//! Logical-replication ingestor: polls a Postgres `test_decoding` slot and turns each row change
//! into a State-Protocol envelope (carrying old + new and the commit LSN), appended to the
//! durable-streams `table/<name>` stream. The engine's existing tailer consumes that stream, so the
//! whole shape fan-out path is unchanged — only the *source* of table changes moves to Postgres.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{Map, Value as Json};

use crate::ds::{DsClient, Envelope, EnvelopeHeaders};
use crate::pg;
use crate::schema::{ColumnType, TableSchema};

/// Long-running poll loop. Owns its own Postgres connection (a slot read is session-stateful, so it
/// must not share a connection with backfill transactions).
pub async fn run(
    pg_url: String,
    slot: String,
    poll_ms: u64,
    ds: DsClient,
    tables: Arc<HashMap<String, TableSchema>>,
    last_lsn: Arc<std::sync::Mutex<String>>,
    sync_seq: Arc<std::sync::atomic::AtomicI64>,
) {
    use std::sync::atomic::Ordering;
    let client = match pg::connect(&pg_url).await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("replicator: connect failed: {e:#}");
            return;
        }
    };
    let query = "select lsn::text, data from pg_logical_slot_get_changes($1, NULL, NULL)";
    loop {
        match client.query(query, &[&slot]).await {
            Ok(rows) => {
                // Collect per-table envelopes in commit order, then append once per table. A change to
                // the per-database `__el_sync` sentinel table is not a data table — we only track the
                // highest counter value seen, published *after* the batch is on the stream so a drain
                // barrier can wait for the engine to have caught up to a known point in THIS database
                // (robust under a shared multi-database Postgres where WAL LSNs are server-global).
                let mut pending: HashMap<String, Vec<Envelope>> = HashMap::new();
                let mut max_lsn: Option<String> = None;
                let mut max_sync: Option<i64> = None;
                for r in &rows {
                    let lsn: String = r.get(0);
                    let data: String = r.get(1);
                    max_lsn = Some(lsn.clone());
                    if let Some(n) = parse_sync(&data) {
                        max_sync = Some(max_sync.map_or(n, |m: i64| m.max(n)));
                    } else if let Some(env) = parse_change(&data, &tables, &lsn) {
                        pending.entry(env.type_.clone()).or_default().push(env);
                    }
                }
                for (table, envs) in pending {
                    if let Err(e) = ds.append(&format!("table/{table}"), &envs).await {
                        tracing::error!("replicator: append table/{table} failed: {e:#}");
                    }
                }
                if let Some(l) = max_lsn {
                    *last_lsn.lock().unwrap() = l;
                }
                if let Some(n) = max_sync {
                    sync_seq.fetch_max(n, Ordering::Relaxed);
                }
            }
            Err(e) => {
                tracing::error!("replicator: slot read failed: {e:#}; backing off");
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(poll_ms)).await;
    }
}

/// Recognize a change to the `__el_sync` drain-barrier sentinel and extract its new counter value.
/// The row is `... table public.__el_sync: UPDATE: old-key: id[…]:1 n[bigint]:K0 new-tuple: id[…]:1
/// n[bigint]:K1` (or INSERT with a single `n[…]:K`); we want the LAST `n[...]:` (the new value).
fn parse_sync(data: &str) -> Option<i64> {
    if !data.contains("public.__el_sync:") {
        return None;
    }
    let idx = data.rfind(" n[")?;
    let rest = &data[idx..];
    let val_start = rest.find("]:")? + 2;
    let rest = &rest[val_start..];
    let end = rest.find(' ').unwrap_or(rest.len());
    rest[..end].parse::<i64>().ok()
}

/// Parse one `test_decoding` line into an envelope, or `None` for BEGIN/COMMIT/unwatched tables.
fn parse_change(data: &str, tables: &HashMap<String, TableSchema>, lsn: &str) -> Option<Envelope> {
    let rest = data.strip_prefix("table ")?; // "public.users: INSERT: ..."
    let (qualified, rest) = rest.split_once(": ")?; // "public.users", "INSERT: ..."
    let table = qualified.rsplit('.').next().unwrap_or(qualified).trim_matches('"');
    let ts = tables.get(table)?;
    let (op, body) = rest.split_once(": ")?; // "INSERT", "id[integer]:1 ..."

    let make = |operation: &str, value: Option<Json>, old: Option<Json>, key_src: &Json| {
        let key = key_from_obj(key_src, ts);
        Envelope {
            type_: table.to_string(),
            key,
            value,
            old,
            headers: EnvelopeHeaders {
                operation: operation.to_string(),
                txid: None,
                offset: None,
                lsn: Some(lsn.to_string()),
            },
        }
    };

    match op {
        "INSERT" => {
            let new = Json::Object(parse_cols(body, ts));
            Some(make("insert", Some(new.clone()), None, &new))
        }
        "UPDATE" => {
            // With REPLICA IDENTITY FULL: "old-key: <cols> new-tuple: <cols>".
            let (old_seg, new_seg) = match body.split_once(" new-tuple: ") {
                Some((o, n)) => (o.strip_prefix("old-key: ").unwrap_or(o), n),
                None => ("", body), // no old (REPLICA IDENTITY DEFAULT) -> new only
            };
            let new = Json::Object(parse_cols(new_seg, ts));
            let old = if old_seg.is_empty() { None } else { Some(Json::Object(parse_cols(old_seg, ts))) };
            Some(make("update", Some(new.clone()), old, &new))
        }
        "DELETE" => {
            let old = Json::Object(parse_cols(body, ts));
            Some(make("delete", None, Some(old.clone()), &old))
        }
        _ => None,
    }
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

/// Parse a `test_decoding` column segment ("c1[type]:v1 c2[type]:'a b' c3[type]:null") into a JSON
/// object, converting each value by the column's schema type.
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
        // column name up to '['
        let name_start = i;
        while i < b.len() && b[i] != b'[' {
            i += 1;
        }
        if i >= b.len() {
            break;
        }
        let name = &seg[name_start..i];
        // skip "[type]"
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
        if let Some(ty) = ts.index.get(name).map(|&idx| ts.columns[idx].1) {
            out.insert(name.to_string(), text_to_json(&value_text, is_quoted, ty));
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

    #[test]
    fn parses_insert_update_delete_with_old() {
        let t = users();
        let ins = parse_change("table public.users: INSERT: id[integer]:1 tenant[integer]:7 name[text]:'a'", &t, "0/1").unwrap();
        assert_eq!(ins.headers.operation, "insert");
        assert_eq!(ins.key, "1");
        assert_eq!(ins.value.as_ref().unwrap()["name"], "a");
        assert!(ins.old.is_none());

        let upd = parse_change(
            "table public.users: UPDATE: old-key: id[integer]:1 tenant[integer]:7 name[text]:'a' new-tuple: id[integer]:1 tenant[integer]:7 name[text]:'b'",
            &t, "0/2").unwrap();
        assert_eq!(upd.headers.operation, "update");
        assert_eq!(upd.old.as_ref().unwrap()["name"], "a");
        assert_eq!(upd.value.as_ref().unwrap()["name"], "b");
        assert_eq!(upd.headers.lsn.as_deref(), Some("0/2"));

        let del = parse_change("table public.users: DELETE: id[integer]:1 tenant[integer]:7 name[text]:'b'", &t, "0/3").unwrap();
        assert_eq!(del.headers.operation, "delete");
        assert_eq!(del.key, "1");
        assert_eq!(del.old.as_ref().unwrap()["tenant"], 7);
        assert!(del.value.is_none());
    }

    #[test]
    fn handles_null_and_quoted_spaces() {
        let t = users();
        let e = parse_change("table public.users: INSERT: id[integer]:5 tenant[integer]:null name[text]:'a b ''c'''", &t, "0/4").unwrap();
        assert_eq!(e.value.as_ref().unwrap()["tenant"], Json::Null);
        assert_eq!(e.value.as_ref().unwrap()["name"], "a b 'c'");

        // Multi-byte UTF-8 must round-trip intact (not be split byte-by-byte).
        let u = parse_change("table public.users: INSERT: id[integer]:6 name[text]:'café ☃ 北京'", &t, "0/5").unwrap();
        assert_eq!(u.value.as_ref().unwrap()["name"], "café ☃ 北京");
    }

    #[test]
    fn ignores_begin_commit_and_unwatched() {
        let t = users();
        assert!(parse_change("BEGIN 700", &t, "0/1").is_none());
        assert!(parse_change("COMMIT 700", &t, "0/1").is_none());
        assert!(parse_change("table public.other: INSERT: id[integer]:1", &t, "0/1").is_none());
    }

    #[test]
    fn parse_sync_extracts_new_counter() {
        // UPDATE with REPLICA IDENTITY FULL: take the new-tuple's n, not the old one.
        assert_eq!(
            parse_sync("table public.__el_sync: UPDATE: old-key: id[integer]:1 n[bigint]:4 new-tuple: id[integer]:1 n[bigint]:5"),
            Some(5)
        );
        // INSERT / default identity: single n.
        assert_eq!(parse_sync("table public.__el_sync: UPDATE: id[integer]:1 n[bigint]:9"), Some(9));
        // Unrelated changes are ignored.
        assert_eq!(parse_sync("table public.users: INSERT: id[integer]:1 name[text]:'a'"), None);
        assert_eq!(parse_sync("BEGIN 700"), None);
    }
}
