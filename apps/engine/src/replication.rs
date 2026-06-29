//! Logical-replication ingestor: polls a Postgres `test_decoding` slot and turns each row change
//! into a State-Protocol envelope (carrying old + new and the change's COMMIT LSN), appended to the
//! durable-streams `table/<name>` stream. The engine's existing tailer consumes that stream, so the
//! whole shape fan-out path is unchanged — only the *source* of table changes moves to Postgres.
//!
//! Delivery is read-then-commit: each poll PEEKS the slot (non-consuming), appends to durable-streams,
//! and only then ADVANCES the slot. A failed append leaves the slot unadvanced, so the same changes
//! are re-read next poll rather than lost. (Caveat: if appends to several table streams partially
//! succeed within one poll, the succeeded ones may be re-appended on retry — a real system would need
//! transactional multi-stream append or per-stream cursors.)
//!
//! Each envelope is stamped with its transaction's COMMIT LSN (not the per-change record LSN), so the
//! backfill/replication boundary (`commit_lsn < seed_lsn`, see `engine::process_envelope`) lines up
//! with snapshot *commit* visibility. test_decoding frames each xact as `BEGIN / <changes> / COMMIT`
//! and the COMMIT row's LSN is the commit LSN; we buffer a transaction's changes and stamp them when
//! its COMMIT row arrives.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use serde_json::{Map, Value as Json};

use crate::ds::{DsClient, Envelope, EnvelopeHeaders};
use crate::pg;
use crate::schema::{ColumnType, TableSchema};

const SYNC_TABLE: &str = "__el_sync";
const TOAST_SENTINEL: &str = "unchanged-toast-datum";

/// Long-running ingestor. Reconnects on connection loss. Owns its own Postgres connection (a slot
/// read is session-stateful, so it must not share a connection with backfill transactions).
pub async fn run(
    pg_url: String,
    slot: String,
    poll_ms: u64,
    ds: DsClient,
    tables: Arc<HashMap<String, TableSchema>>,
    last_lsn: Arc<std::sync::Mutex<String>>,
    sync_seq: Arc<AtomicI64>,
) {
    // Outer loop: (re)connect after any connection-level failure.
    loop {
        let client = match pg::connect(&pg_url).await {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("replicator: connect failed: {e:#}; retrying");
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                continue;
            }
        };
        if let Err(e) = poll_loop(&client, &slot, poll_ms, &ds, &tables, &last_lsn, &sync_seq).await {
            tracing::error!("replicator: connection error: {e:#}; reconnecting");
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }
}

/// Inner poll loop; returns `Err` only on a connection-level failure (so the caller reconnects).
async fn poll_loop(
    client: &tokio_postgres::Client,
    slot: &str,
    poll_ms: u64,
    ds: &DsClient,
    tables: &HashMap<String, TableSchema>,
    last_lsn: &Arc<std::sync::Mutex<String>>,
    sync_seq: &Arc<AtomicI64>,
) -> Result<(), tokio_postgres::Error> {
    // PEEK (non-consuming): we only ADVANCE the slot after the batch is durably on the stream.
    let peek = "select lsn::text, data from pg_logical_slot_peek_changes($1, NULL, NULL)";
    loop {
        let rows = client.query(peek, &[&slot]).await?; // connection error -> reconnect
        if !rows.is_empty() {
            let pairs: Vec<(String, String)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
            let batch = decode_batch(&pairs, tables);
            // Append every table's envelopes; advance only if ALL succeed (else re-peek next poll).
            let mut all_ok = true;
            for (table, envs) in &batch.pending {
                if let Err(e) = ds.append(&format!("table/{table}"), envs).await {
                    tracing::error!("replicator: append table/{table} failed: {e:#}; will retry");
                    all_ok = false;
                }
            }
            if all_ok {
                if let Some(ref upto) = batch.advance_to {
                    // Consume everything up to the last commit we processed. The LSN is a PG-produced
                    // hex string (e.g. "0/78912C0"), formatted literally because a bound &str can't be
                    // serialized into a `pg_lsn` parameter; the slot name stays a bound parameter.
                    let q = format!("select pg_replication_slot_advance($1, '{upto}'::pg_lsn)");
                    if let Err(e) = client.execute(&q, &[&slot]).await {
                        tracing::error!("replicator: slot advance to {upto} failed: {e:#}");
                    } else {
                        *last_lsn.lock().unwrap() = upto.clone();
                    }
                }
                if let Some(n) = batch.max_sync {
                    sync_seq.fetch_max(n, Ordering::Relaxed);
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(poll_ms)).await;
    }
}

struct Batch {
    /// Envelopes per table, each stamped with its transaction's commit LSN, in commit order.
    pending: HashMap<String, Vec<Envelope>>,
    /// Highest `__el_sync` counter seen in committed transactions (drain barrier).
    max_sync: Option<i64>,
    /// LSN of the last COMMIT we fully processed; the slot is advanced to here.
    advance_to: Option<String>,
}

/// Decode a peeked batch (rows of `(lsn, data)`) into per-table envelopes, buffering each transaction
/// so its changes can be stamped with the COMMIT LSN. test_decoding only ever emits *complete*
/// transactions, so a `BEGIN` is always matched by a later `COMMIT` within the same batch.
fn decode_batch(rows: &[(String, String)], tables: &HashMap<String, TableSchema>) -> Batch {
    let mut pending: HashMap<String, Vec<Envelope>> = HashMap::new();
    let mut max_sync: Option<i64> = None;
    let mut advance_to: Option<String> = None;

    let mut tx_envs: Vec<Envelope> = Vec::new();
    let mut tx_sync: Option<i64> = None;
    for (lsn, data) in rows {
        let data = data.as_str();
        if data.starts_with("BEGIN") {
            tx_envs.clear();
            tx_sync = None;
        } else if data.starts_with("COMMIT") {
            // The COMMIT row's LSN is the transaction's commit LSN; stamp the buffered changes.
            for mut env in tx_envs.drain(..) {
                env.headers.lsn = Some(lsn.clone());
                pending.entry(env.type_.clone()).or_default().push(env);
            }
            if let Some(n) = tx_sync.take() {
                max_sync = Some(max_sync.map_or(n, |m| m.max(n)));
            }
            advance_to = Some(lsn.clone());
        } else if let Some((table, op, body)) = parse_row(&data) {
            if table == SYNC_TABLE {
                if let Some(n) = parse_sync_body(body) {
                    tx_sync = Some(n);
                }
            } else if let Some(ts) = tables.get(table) {
                if let Some(env) = build_envelope(table, ts, op, body) {
                    tx_envs.push(env); // lsn stamped at COMMIT
                }
            }
        }
    }
    Batch { pending, max_sync, advance_to }
}

/// Split a `test_decoding` change line into `(table, op, body)`, or `None` for BEGIN/COMMIT/other.
/// Handles schema-qualified and double-quoted identifiers (`public."My.Table"`).
fn parse_row(data: &str) -> Option<(&str, &str, &str)> {
    let rest = data.strip_prefix("table ")?; // `public.users: INSERT: ...`
    let (qualified, rest) = rest.split_once(": ")?; // `public.users`, `INSERT: ...`
    let (op, body) = rest.split_once(": ")?; // `INSERT`, `id[integer]:1 ...`
    Some((table_ident(qualified), op, body))
}

/// The table component of a possibly schema-qualified, possibly quoted identifier. Returns a slice
/// into `qualified` for the common unquoted case; quoted identifiers are matched on the unquoted form
/// by callers via `tables.get`, so we strip surrounding quotes here.
fn table_ident(qualified: &str) -> &str {
    // Take the last dot-separated component, treating a trailing quoted segment as atomic.
    if qualified.ends_with('"') {
        // find the opening quote of the final `"..."` segment
        let bytes = qualified.as_bytes();
        let mut i = bytes.len() - 1; // closing quote
        while i > 0 {
            i -= 1;
            if bytes[i] == b'"' {
                // could be an escaped "" — but escaped quotes can't end the string, so the first
                // quote we hit scanning back from the final char is the opener for simple names.
                return &qualified[i + 1..qualified.len() - 1];
            }
        }
        qualified
    } else {
        qualified.rsplit('.').next().unwrap_or(qualified)
    }
}

/// Build an envelope from a parsed change (`op` body); LSN is stamped later at COMMIT.
fn build_envelope(table: &str, ts: &TableSchema, op: &str, body: &str) -> Option<Envelope> {
    let make = |operation: &str, value: Option<Json>, old: Option<Json>, key_src: &Json| Envelope {
        type_: table.to_string(),
        key: key_from_obj(key_src, ts),
        value,
        old,
        headers: EnvelopeHeaders { operation: operation.to_string(), txid: None, offset: None, lsn: None },
    };
    match op {
        "INSERT" => {
            let new = Json::Object(parse_cols(body, ts));
            Some(make("insert", Some(new.clone()), None, &new))
        }
        "UPDATE" => {
            // With REPLICA IDENTITY FULL: `old-key: <cols> new-tuple: <cols>`. The split token is
            // matched OUTSIDE quotes so a text value containing " new-tuple: " can't break it.
            let (old_seg, new_seg) = match find_unquoted(body, " new-tuple: ") {
                Some(idx) => (body[..idx].strip_prefix("old-key: ").unwrap_or(&body[..idx]), &body[idx + " new-tuple: ".len()..]),
                None => ("", body), // no old image (REPLICA IDENTITY DEFAULT) -> new only
            };
            let mut new_map = parse_cols(new_seg, ts);
            let old_map = if old_seg.is_empty() { None } else { Some(parse_cols(old_seg, ts)) };
            // TOASTed-but-unchanged columns are omitted from new (see parse_cols); fill from old.
            if let Some(ref om) = old_map {
                for (k, v) in om {
                    new_map.entry(k.clone()).or_insert_with(|| v.clone());
                }
            }
            let new = Json::Object(new_map);
            let old = old_map.map(Json::Object);
            Some(make("update", Some(new.clone()), old, &new))
        }
        "DELETE" => {
            let old = Json::Object(parse_cols(body, ts));
            Some(make("delete", None, Some(old.clone()), &old))
        }
        _ => None,
    }
}

/// Find `needle` in `haystack` at a position that is not inside a single-quoted string (`''` escape).
fn find_unquoted(haystack: &str, needle: &str) -> Option<usize> {
    let b = haystack.as_bytes();
    let n = needle.as_bytes();
    let mut i = 0;
    let mut in_q = false;
    while i < b.len() {
        if in_q {
            if b[i] == b'\'' {
                if i + 1 < b.len() && b[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_q = false;
            }
            i += 1;
        } else if b[i] == b'\'' {
            in_q = true;
            i += 1;
        } else if b[i..].starts_with(n) {
            return Some(i);
        } else {
            i += 1;
        }
    }
    None
}

/// Extract the new counter value from an `__el_sync` change body (last `n[...]:<int>`).
fn parse_sync_body(body: &str) -> Option<i64> {
    let idx = body.rfind(" n[").or_else(|| if body.starts_with("n[") { Some(0) } else { None })?;
    let rest = &body[idx..];
    let val_start = rest.find("]:")? + 2;
    let rest = &rest[val_start..];
    let end = rest.find(' ').unwrap_or(rest.len());
    rest[..end].parse::<i64>().ok()
}

/// Extract the primary-key string from a parsed row object.
fn key_from_obj(obj: &Json, ts: &TableSchema) -> String {
    match obj.get(&ts.pk_name) {
        Some(Json::Null) | None => "null".to_string(),
        Some(Json::String(s)) => s.clone(),
        Some(Json::Number(n)) => n.to_string(),
        Some(Json::Bool(b)) => b.to_string(),
        Some(v) => v.to_string(),
    }
}

/// Parse a `test_decoding` column segment (`c1[type]:v1 c2[type]:'a b' c3[type]:null`) into a JSON
/// object, converting each value by the column's schema type. Columns reported as the TOAST sentinel
/// (`unchanged-toast-datum`, an unmodified out-of-line value on UPDATE) are OMITTED so the caller can
/// fill them from the old image. Quoted column names are unquoted before lookup.
fn parse_cols(seg: &str, ts: &TableSchema) -> Map<String, Json> {
    let mut out = Map::new();
    let b = seg.as_bytes();
    let mut i = 0;
    while i < b.len() {
        while i < b.len() && b[i] == b' ' {
            i += 1;
        }
        if i >= b.len() {
            break;
        }
        // column name (possibly double-quoted) up to '['
        let name: String;
        if b[i] == b'"' {
            i += 1;
            let mut s: Vec<u8> = Vec::new();
            while i < b.len() {
                if b[i] == b'"' {
                    if i + 1 < b.len() && b[i + 1] == b'"' {
                        s.push(b'"');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                s.push(b[i]);
                i += 1;
            }
            name = String::from_utf8_lossy(&s).into_owned();
        } else {
            let name_start = i;
            while i < b.len() && b[i] != b'[' {
                i += 1;
            }
            name = seg[name_start..i].to_string();
        }
        // skip "[type]"
        while i < b.len() && b[i] != b'[' {
            i += 1;
        }
        while i < b.len() && b[i] != b']' {
            i += 1;
        }
        if i >= b.len() {
            break;
        }
        i += 1; // past ']'
        if i >= b.len() || b[i] != b':' {
            break;
        }
        i += 1; // past ':'
        // value: quoted (handles '' escape) or a non-space run
        let value_text: String;
        let mut is_quoted = false;
        if i < b.len() && b[i] == b'\'' {
            is_quoted = true;
            i += 1;
            // Accumulate raw bytes (not `b[i] as char`, which would mangle multi-byte UTF-8) and
            // decode once; the source segment is valid UTF-8 and `''` is the only escape.
            let mut s: Vec<u8> = Vec::new();
            while i < b.len() {
                if b[i] == b'\'' {
                    if i + 1 < b.len() && b[i + 1] == b'\'' {
                        s.push(b'\'');
                        i += 2;
                        continue;
                    }
                    i += 1; // closing quote
                    break;
                }
                s.push(b[i]);
                i += 1;
            }
            value_text = String::from_utf8(s).unwrap_or_default();
        } else {
            let start = i;
            while i < b.len() && b[i] != b' ' {
                i += 1;
            }
            value_text = seg[start..i].to_string();
        }
        // Omit TOASTed-unchanged columns so the caller fills them from the old image.
        if !is_quoted && value_text == TOAST_SENTINEL {
            continue;
        }
        if let Some(ty) = ts.index.get(&name).map(|&idx| ts.columns[idx].1) {
            out.insert(name, text_to_json(&value_text, is_quoted, ty));
        }
    }
    out
}

/// Convert a `test_decoding` scalar (already unquoted) to JSON per the column type. Unquoted `null`
/// is SQL NULL; quoted values are always present (text).
fn text_to_json(text: &str, is_quoted: bool, ty: ColumnType) -> Json {
    if !is_quoted && text == "null" {
        return Json::Null;
    }
    match ty {
        ColumnType::Int => text.parse::<i64>().map(Json::from).unwrap_or(Json::Null),
        ColumnType::Float => text.parse::<f64>().ok().and_then(serde_json::Number::from_f64).map(Json::Number).unwrap_or(Json::Null),
        ColumnType::Bool => match text {
            "t" | "true" => Json::Bool(true),
            "f" | "false" => Json::Bool(false),
            _ => Json::Null,
        },
        ColumnType::Text => Json::String(text.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnDef, TableDef};
    use std::collections::BTreeMap;

    fn users() -> HashMap<String, TableSchema> {
        let mut columns = BTreeMap::new();
        columns.insert("id".to_string(), ColumnDef { ty: ColumnType::Int });
        columns.insert("tenant".to_string(), ColumnDef { ty: ColumnType::Int });
        columns.insert("name".to_string(), ColumnDef { ty: ColumnType::Text });
        let def = TableDef { columns, primary_key: "id".to_string() };
        let mut m = HashMap::new();
        m.insert("users".to_string(), TableSchema::from_def("users", &def).unwrap());
        m
    }

    // Test helper mirroring the old parse_change: build an envelope directly from a change line.
    fn parse_change(data: &str, tables: &HashMap<String, TableSchema>) -> Option<Envelope> {
        let (table, op, body) = parse_row(data)?;
        let ts = tables.get(table)?;
        build_envelope(table, ts, op, body)
    }

    #[test]
    fn parses_insert_update_delete_with_old() {
        let t = users();
        let ins = parse_change("table public.users: INSERT: id[integer]:1 tenant[integer]:7 name[text]:'a'", &t).unwrap();
        assert_eq!(ins.headers.operation, "insert");
        assert_eq!(ins.key, "1");
        assert_eq!(ins.value.as_ref().unwrap()["name"], "a");
        assert!(ins.old.is_none());

        let upd = parse_change(
            "table public.users: UPDATE: old-key: id[integer]:1 tenant[integer]:7 name[text]:'a' new-tuple: id[integer]:1 tenant[integer]:7 name[text]:'b'",
            &t).unwrap();
        assert_eq!(upd.headers.operation, "update");
        assert_eq!(upd.old.as_ref().unwrap()["name"], "a");
        assert_eq!(upd.value.as_ref().unwrap()["name"], "b");

        let del = parse_change("table public.users: DELETE: id[integer]:1 tenant[integer]:7 name[text]:'b'", &t).unwrap();
        assert_eq!(del.headers.operation, "delete");
        assert_eq!(del.key, "1");
        assert_eq!(del.old.as_ref().unwrap()["tenant"], 7);
        assert!(del.value.is_none());
    }

    #[test]
    fn handles_null_and_quoted_spaces_and_utf8() {
        let t = users();
        let e = parse_change("table public.users: INSERT: id[integer]:5 tenant[integer]:null name[text]:'a b ''c'''", &t).unwrap();
        assert_eq!(e.value.as_ref().unwrap()["tenant"], Json::Null);
        assert_eq!(e.value.as_ref().unwrap()["name"], "a b 'c'");
        let u = parse_change("table public.users: INSERT: id[integer]:6 name[text]:'café ☃ 北京'", &t).unwrap();
        assert_eq!(u.value.as_ref().unwrap()["name"], "café ☃ 北京");
    }

    #[test]
    fn update_split_is_quote_aware() {
        // A text value literally containing " new-tuple: " must not break the old/new split.
        let t = users();
        let line = "table public.users: UPDATE: old-key: id[integer]:1 name[text]:'x' new-tuple: id[integer]:1 name[text]:'evil new-tuple: hack'";
        let upd = parse_change(line, &t).unwrap();
        assert_eq!(upd.old.as_ref().unwrap()["name"], "x");
        assert_eq!(upd.value.as_ref().unwrap()["name"], "evil new-tuple: hack");
    }

    #[test]
    fn toast_unchanged_value_filled_from_old() {
        // An unchanged TOASTed column appears as the sentinel in new-tuple; fill it from old.
        let t = users();
        let line = "table public.users: UPDATE: old-key: id[integer]:1 tenant[integer]:7 name[text]:'big original' new-tuple: id[integer]:1 tenant[integer]:9 name[text]:unchanged-toast-datum";
        let upd = parse_change(line, &t).unwrap();
        assert_eq!(upd.value.as_ref().unwrap()["tenant"], 9); // changed col taken from new
        assert_eq!(upd.value.as_ref().unwrap()["name"], "big original"); // unchanged toast from old
    }

    #[test]
    fn quoted_identifiers() {
        // Quoted, schema-qualified table name + quoted column name resolve to the unquoted form.
        let mut columns = BTreeMap::new();
        columns.insert("id".to_string(), ColumnDef { ty: ColumnType::Int });
        columns.insert("name".to_string(), ColumnDef { ty: ColumnType::Text });
        let def = TableDef { columns, primary_key: "id".to_string() };
        let mut m = HashMap::new();
        m.insert("users".to_string(), TableSchema::from_def("users", &def).unwrap());
        let e = parse_change(r#"table public."users": INSERT: "id"[integer]:1 "name"[text]:'a'"#, &m).unwrap();
        assert_eq!(e.key, "1");
        assert_eq!(e.value.as_ref().unwrap()["name"], "a");
    }

    fn rows(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs.iter().map(|(l, d)| (l.to_string(), d.to_string())).collect()
    }

    #[test]
    fn decode_batch_stamps_commit_lsn() {
        let t = users();
        // A transaction: BEGIN, one insert (record lsn 0/10), COMMIT (commit lsn 0/20).
        let batch = decode_batch(
            &rows(&[
                ("0/A", "BEGIN 700"),
                ("0/10", "table public.users: INSERT: id[integer]:1 name[text]:'a'"),
                ("0/20", "COMMIT 700"),
            ]),
            &t,
        );
        let envs = &batch.pending["users"];
        assert_eq!(envs.len(), 1);
        // Stamped with the COMMIT lsn (0/20), not the per-change record lsn (0/10).
        assert_eq!(envs[0].headers.lsn.as_deref(), Some("0/20"));
        assert_eq!(batch.advance_to.as_deref(), Some("0/20"));
    }

    #[test]
    fn parse_sync_anchored_to_sync_table() {
        let t = users();
        // A data row whose TEXT value contains the sentinel substring must NOT be taken as sync.
        let batch = decode_batch(
            &rows(&[
                ("0/A", "BEGIN 1"),
                ("0/10", "table public.users: INSERT: id[integer]:1 name[text]:'public.__el_sync: n[bigint]:999'"),
                ("0/20", "COMMIT 1"),
                ("0/B", "BEGIN 2"),
                ("0/30", "table public.__el_sync: UPDATE: id[integer]:1 n[bigint]:5"),
                ("0/40", "COMMIT 2"),
            ]),
            &t,
        );
        assert_eq!(batch.max_sync, Some(5)); // only the real sentinel row counts
        assert_eq!(batch.pending["users"].len(), 1); // the data row is still ingested
    }
}
