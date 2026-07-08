//! Minimal `pgoutput` (logical replication output plugin) message decoder — protocol version 1,
//! text-mode tuples (the `binary` option is deliberately NOT enabled: text values come from the
//! same type output functions Postgres uses everywhere, so they stay byte-identical to the
//! backfill's `::text` casts — see `pg.rs::row_json_expr`).
//!
//! Only the DML-carrying messages are decoded here; `Begin`/`Commit` are parsed by the
//! replication client (`pgwire-replication`) before the raw data reaches us, and `Origin`/`Type`
//! messages carry nothing the engine needs.
//!
//! Reference: PostgreSQL docs, "Logical Replication Message Formats".

use anyhow::{Context, Result, bail};

/// One column of a tuple, in text mode.
#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    /// SQL NULL.
    Null,
    /// TOASTed value unchanged by this UPDATE — the value is not present in the message.
    UnchangedToast,
    /// The value's Postgres text representation.
    Text(String),
}

/// A decoded tuple: cells in the relation's column order.
pub type Tuple = Vec<Cell>;

/// The old-image part of an UPDATE/DELETE: full old row (`REPLICA IDENTITY FULL`) or key-only.
#[derive(Debug, Clone, PartialEq)]
pub enum OldTuple {
    /// `O` — the full old row (REPLICA IDENTITY FULL).
    Full(Tuple),
    /// `K` — replica-identity key columns only (identity is not FULL; a degraded form for us).
    Key(Tuple),
}

/// A decoded pgoutput message the ingestor cares about.
#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    /// `R` — relation metadata; sent before the first DML for a relation on each connection (and
    /// again after schema changes). Maps `rel_id` to a table name + column names.
    Relation { rel_id: u32, namespace: String, name: String, columns: Vec<String> },
    /// `I`
    Insert { rel_id: u32, new: Tuple },
    /// `U` — `old` is present only when the replica identity provides it.
    Update { rel_id: u32, old: Option<OldTuple>, new: Tuple },
    /// `D`
    Delete { rel_id: u32, old: OldTuple },
    /// `T`
    Truncate { rel_ids: Vec<u32> },
    /// A message type the engine ignores (`Y` type metadata, `O` origin, ...).
    Ignored,
}

/// Decode one pgoutput message (the payload of an XLogData frame).
pub fn decode(data: &[u8]) -> Result<Message> {
    let mut r = Reader { b: data, i: 0 };
    let tag = r.u8().context("empty pgoutput message")?;
    Ok(match tag {
        b'R' => {
            let rel_id = r.u32()?;
            let namespace = r.cstr()?;
            let name = r.cstr()?;
            let _replident = r.u8()?;
            let ncols = r.i16()?;
            let mut columns = Vec::with_capacity(ncols.max(0) as usize);
            for _ in 0..ncols {
                let _flags = r.u8()?;
                columns.push(r.cstr()?);
                let _typoid = r.u32()?;
                let _typmod = r.i32()?;
            }
            Message::Relation { rel_id, namespace, name, columns }
        }
        b'I' => {
            let rel_id = r.u32()?;
            let marker = r.u8()?;
            if marker != b'N' {
                bail!("pgoutput INSERT: expected 'N' tuple marker, got {marker:#x}");
            }
            Message::Insert { rel_id, new: r.tuple()? }
        }
        b'U' => {
            let rel_id = r.u32()?;
            let mut old = None;
            let mut marker = r.u8()?;
            if marker == b'K' {
                old = Some(OldTuple::Key(r.tuple()?));
                marker = r.u8()?;
            } else if marker == b'O' {
                old = Some(OldTuple::Full(r.tuple()?));
                marker = r.u8()?;
            }
            if marker != b'N' {
                bail!("pgoutput UPDATE: expected 'N' tuple marker, got {marker:#x}");
            }
            Message::Update { rel_id, old, new: r.tuple()? }
        }
        b'D' => {
            let rel_id = r.u32()?;
            let marker = r.u8()?;
            let old = match marker {
                b'O' => OldTuple::Full(r.tuple()?),
                b'K' => OldTuple::Key(r.tuple()?),
                _ => bail!("pgoutput DELETE: expected 'O'/'K' tuple marker, got {marker:#x}"),
            };
            Message::Delete { rel_id, old }
        }
        b'T' => {
            let n = r.u32()?;
            let _options = r.u8()?;
            let mut rel_ids = Vec::with_capacity(n as usize);
            for _ in 0..n {
                rel_ids.push(r.u32()?);
            }
            Message::Truncate { rel_ids }
        }
        _ => Message::Ignored,
    })
}

struct Reader<'a> {
    b: &'a [u8],
    i: usize,
}

impl Reader<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8]> {
        if self.i + n > self.b.len() {
            bail!("pgoutput message truncated at byte {} (wanted {n} more)", self.i);
        }
        let s = &self.b[self.i..self.i + n];
        self.i += n;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn i16(&mut self) -> Result<i16> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn cstr(&mut self) -> Result<String> {
        let start = self.i;
        while self.i < self.b.len() && self.b[self.i] != 0 {
            self.i += 1;
        }
        if self.i >= self.b.len() {
            bail!("pgoutput message: unterminated string");
        }
        let s = String::from_utf8_lossy(&self.b[start..self.i]).into_owned();
        self.i += 1; // NUL
        Ok(s)
    }
    fn tuple(&mut self) -> Result<Tuple> {
        let ncols = self.i16()?;
        let mut cells = Vec::with_capacity(ncols.max(0) as usize);
        for _ in 0..ncols {
            let kind = self.u8()?;
            cells.push(match kind {
                b'n' => Cell::Null,
                b'u' => Cell::UnchangedToast,
                b't' => {
                    let len = self.i32()?;
                    if len < 0 {
                        bail!("pgoutput tuple: negative text length");
                    }
                    Cell::Text(String::from_utf8_lossy(self.take(len as usize)?).into_owned())
                }
                // 'b' (binary) can only appear when the subscription enables the binary option,
                // which we never do — see the module doc.
                other => bail!("pgoutput tuple: unsupported cell kind {other:#x}"),
            });
        }
        Ok(cells)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cstr(s: &str) -> Vec<u8> {
        let mut v = s.as_bytes().to_vec();
        v.push(0);
        v
    }

    fn text_cell(s: &str) -> Vec<u8> {
        let mut v = vec![b't'];
        v.extend((s.len() as i32).to_be_bytes());
        v.extend(s.as_bytes());
        v
    }

    fn relation_msg() -> Vec<u8> {
        let mut m = vec![b'R'];
        m.extend(42u32.to_be_bytes());
        m.extend(cstr("public"));
        m.extend(cstr("users"));
        m.push(b'f'); // replident
        m.extend(2i16.to_be_bytes());
        // col id: flags, name, typoid, typmod
        m.push(0);
        m.extend(cstr("id"));
        m.extend(23u32.to_be_bytes());
        m.extend((-1i32).to_be_bytes());
        m.push(0);
        m.extend(cstr("name"));
        m.extend(25u32.to_be_bytes());
        m.extend((-1i32).to_be_bytes());
        m
    }

    #[test]
    fn decodes_relation() {
        let msg = decode(&relation_msg()).unwrap();
        assert_eq!(msg, Message::Relation {
            rel_id: 42,
            namespace: "public".into(),
            name: "users".into(),
            columns: vec!["id".into(), "name".into()],
        });
    }

    #[test]
    fn decodes_insert_with_null_toast_and_utf8() {
        let mut m = vec![b'I'];
        m.extend(42u32.to_be_bytes());
        m.push(b'N');
        m.extend(4i16.to_be_bytes());
        m.extend(text_cell("1"));
        m.push(b'n');
        m.push(b'u');
        m.extend(text_cell("café ☃ 北京"));
        let msg = decode(&m).unwrap();
        assert_eq!(msg, Message::Insert {
            rel_id: 42,
            new: vec![
                Cell::Text("1".into()),
                Cell::Null,
                Cell::UnchangedToast,
                Cell::Text("café ☃ 北京".into()),
            ],
        });
    }

    #[test]
    fn decodes_update_with_full_old_image() {
        let mut m = vec![b'U'];
        m.extend(42u32.to_be_bytes());
        m.push(b'O');
        m.extend(1i16.to_be_bytes());
        m.extend(text_cell("old"));
        m.push(b'N');
        m.extend(1i16.to_be_bytes());
        m.extend(text_cell("new"));
        let msg = decode(&m).unwrap();
        assert_eq!(msg, Message::Update {
            rel_id: 42,
            old: Some(OldTuple::Full(vec![Cell::Text("old".into())])),
            new: vec![Cell::Text("new".into())],
        });
    }

    #[test]
    fn decodes_update_without_old_image_and_key_only_delete() {
        let mut m = vec![b'U'];
        m.extend(42u32.to_be_bytes());
        m.push(b'N');
        m.extend(1i16.to_be_bytes());
        m.extend(text_cell("new"));
        assert_eq!(decode(&m).unwrap(), Message::Update {
            rel_id: 42,
            old: None,
            new: vec![Cell::Text("new".into())],
        });

        let mut d = vec![b'D'];
        d.extend(42u32.to_be_bytes());
        d.push(b'K');
        d.extend(1i16.to_be_bytes());
        d.extend(text_cell("1"));
        assert_eq!(decode(&d).unwrap(), Message::Delete {
            rel_id: 42,
            old: OldTuple::Key(vec![Cell::Text("1".into())]),
        });
    }

    #[test]
    fn decodes_truncate_and_ignores_unknown() {
        let mut m = vec![b'T'];
        m.extend(2u32.to_be_bytes());
        m.push(0);
        m.extend(42u32.to_be_bytes());
        m.extend(43u32.to_be_bytes());
        assert_eq!(decode(&m).unwrap(), Message::Truncate { rel_ids: vec![42, 43] });
        assert_eq!(decode(&[b'Y', 0, 0]).unwrap(), Message::Ignored);
    }

    #[test]
    fn truncated_message_errors_instead_of_panicking() {
        let m = relation_msg();
        assert!(decode(&m[..m.len() - 3]).is_err());
        assert!(decode(&[]).is_err());
    }
}
