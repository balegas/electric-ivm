//! Logical-replication ingestor: streams a Postgres `pgoutput` slot (walsender protocol, push
//! delivery — no poll floor) and turns each row change into a State-Protocol envelope (carrying
//! old + new and the change's COMMIT LSN), appended — whole commits, in commit order — to the
//! single durable-streams `changes` stream (the envelope's `type` carries the table). The
//! engine's sequencer consumes that stream, so global transaction order survives end to end.
//!
//! Delivery is append-then-acknowledge: a transaction's changes are buffered between `Begin` and
//! `Commit`, appended to durable-streams as ONE batch, and only then acknowledged to Postgres
//! (`update_applied_lsn` → the slot's `confirmed_flush_lsn`). A failed append tears the
//! replication connection down instead of acknowledging; on reconnect the server resends from the
//! confirmed position, so nothing is lost. (Acknowledgements are flushed on an interval, so a
//! crash can re-deliver whole transactions. Delivery is therefore at-least-once; the sequencer
//! restores exactly-once effect by de-duplicating on the stamped `(lsn, seq)`.)
//!
//! Each envelope is stamped with its transaction's COMMIT LSN (not the per-change record LSN), so
//! the backfill/replication boundary (see `pg::SnapshotGate`) lines up with snapshot *commit*
//! visibility, plus the transaction's xid and the change's position within the transaction.
//!
//! Values are pgoutput **text-mode** tuples (the `binary` option is never enabled): Postgres
//! renders them with the same type output functions the backfill's `::text` casts use, keeping
//! backfilled and replicated representations byte-identical (see `pg.rs::row_json_expr`).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use anyhow::{Context, Result};
use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, ReplicationEvent, TlsConfig};
use serde_json::{Map, Value as Json};

use crate::ds::{DsClient, Envelope, EnvelopeHeaders};
use crate::pgoutput::{self, Cell, Message, OldTuple, Tuple};
use crate::schema::{ColumnType, TableSchema};

const SYNC_TABLE: &str = "__el_sync";

/// Long-running ingestor. Reconnects on any connection-level failure; the server resends
/// everything after the last acknowledged commit.
pub async fn run(
    pg_url: String,
    slot: String,
    publication: String,
    ds: DsClient,
    tables: Arc<HashMap<String, TableSchema>>,
    last_lsn: Arc<std::sync::Mutex<String>>,
    sync_seq: Arc<AtomicI64>,
) {
    loop {
        let cfg = match replication_config(&pg_url, &slot, &publication) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("replicator: bad connection config: {e:#}; retrying");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        };
        match stream_loop(cfg, &ds, &tables, &last_lsn, &sync_seq).await {
            Ok(()) => tracing::warn!("replicator: stream ended; reconnecting"),
            Err(e) => tracing::error!("replicator: {e:#}; reconnecting"),
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

/// Build the walsender connection config from a `postgres://` URL. TLS is disabled — parity with
/// the engine's other connections (`pg::connect` uses `NoTls`).
fn replication_config(pg_url: &str, slot: &str, publication: &str) -> Result<ReplicationConfig> {
    let u = url::Url::parse(pg_url).context("parse postgres url")?;
    let user = match u.username() {
        "" => "postgres".to_string(),
        s => percent_decode(s),
    };
    let database = match u.path().trim_start_matches('/') {
        "" => user.clone(),
        s => percent_decode(s),
    };
    Ok(ReplicationConfig {
        host: u.host_str().unwrap_or("127.0.0.1").to_string(),
        port: u.port().unwrap_or(5432),
        user,
        password: u.password().map(percent_decode).unwrap_or_default(),
        database,
        tls: TlsConfig::default(),
        slot: slot.to_string(),
        publication: publication.to_string(),
        // 0/0: the server streams from the slot's confirmed_flush_lsn when asked for an older
        // position, which is exactly "resume where we left off".
        start_lsn: Lsn::ZERO,
        stop_at_lsn: None,
        // How often acknowledged progress is flushed to the server. Bounds the duplicate window
        // after a reconnect (the tailer de-duplicates anyway).
        status_interval: std::time::Duration::from_secs(1),
        idle_wakeup_interval: std::time::Duration::from_secs(10),
        buffer_events: 8192,
    })
}

fn percent_decode(s: &str) -> String {
    // Minimal %XX decoding for URL userinfo/path segments (connection strings rarely need more).
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() + 1 && i + 2 < b.len() + 1 {
            if let (Some(h), Some(l)) = (
                b.get(i + 1).and_then(|c| (*c as char).to_digit(16)),
                b.get(i + 2).and_then(|c| (*c as char).to_digit(16)),
            ) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// One replication connection's lifetime: decode pgoutput frames, buffer per transaction, append
/// at commit, acknowledge. Returns `Err` on any failure so the caller reconnects (the server then
/// resends from the confirmed position).
async fn stream_loop(
    cfg: ReplicationConfig,
    ds: &DsClient,
    tables: &HashMap<String, TableSchema>,
    last_lsn: &Arc<std::sync::Mutex<String>>,
    sync_seq: &Arc<AtomicI64>,
) -> Result<()> {
    let mut client = ReplicationClient::connect(cfg).await.context("replication connect")?;
    let mut dec = Decoder::new(tables);
    let mut txn: Option<TxnBuf> = None;
    loop {
        let ev = client.recv().await.context("replication stream")?;
        let Some(ev) = ev else { return Ok(()) };
        match ev {
            ReplicationEvent::Begin { xid, .. } => {
                txn = Some(TxnBuf { xid, envs: Vec::new(), sync: None, bytes: 0 });
            }
            ReplicationEvent::XLogData { data, .. } => {
                let msg = pgoutput::decode(&data)?;
                if let Message::Relation { .. } = msg {
                    dec.on_relation(msg);
                    continue;
                }
                let Some(t) = txn.as_mut() else { continue };
                match dec.on_change(msg) {
                    Decoded::Env(env) => {
                        t.bytes += data.len() as u64;
                        t.envs.push(env);
                    }
                    Decoded::Sync(n) => t.sync = Some(n),
                    Decoded::None => {}
                }
            }
            ReplicationEvent::Commit { lsn, end_lsn, .. } => {
                let Some(t) = txn.take() else { continue };
                let t0 = std::time::Instant::now();
                let commit_lsn = lsn.to_string();
                // Stamp the buffered changes with the commit LSN, the transaction's xid (the
                // backfill snapshot's xid-visibility fence), and each change's position within the
                // transaction (the sequencer's de-duplication key).
                let ops = t.envs.len() as u64;
                let mut envs = t.envs;
                for (i, env) in envs.iter_mut().enumerate() {
                    env.headers.lsn = Some(commit_lsn.clone());
                    env.headers.txid = Some(t.xid.to_string());
                    env.headers.seq = Some(i as u64);
                }
                // The whole commit is ONE append to the single ordered log; acknowledge only on
                // success. A failure tears the connection down (re-delivery; the sequencer
                // de-duplicates).
                if !envs.is_empty() {
                    ds.append(crate::CHANGES_STREAM, &envs).await.context("append changes")?;
                }
                client.update_applied_lsn(end_lsn);
                *last_lsn.lock().unwrap() = commit_lsn;
                // Publish the drain-barrier sentinel only after the whole commit is on the streams
                // and acknowledged locally, so the barrier can't claim "drained" early.
                if let Some(n) = t.sync {
                    sync_seq.fetch_max(n, Ordering::Relaxed);
                }
                // Per-txn replication metrics. `receive_lag` here is ingest-side append latency
                // (commit frame received → appended), not source-commit→receipt lag.
                if ops > 0 && crate::statsd::enabled() {
                    let lag_ms = t0.elapsed().as_secs_f64() * 1000.0;
                    crate::statsd::replication_txn(ops, t.bytes, lag_ms);
                }
            }
            ReplicationEvent::KeepAlive { .. }
            | ReplicationEvent::Message { .. }
            | ReplicationEvent::StoppedAt { .. } => {}
        }
    }
}

/// A transaction being buffered between `Begin` and `Commit`.
struct TxnBuf {
    xid: u32,
    envs: Vec<Envelope>,
    /// `__el_sync` counter carried by this transaction (drain barrier).
    sync: Option<i64>,
    /// Raw pgoutput payload bytes of the tracked changes (StatsD).
    bytes: u64,
}

/// What one decoded DML message amounts to for the ingestor.
enum Decoded {
    Env(Envelope),
    Sync(i64),
    None,
}

/// Relation metadata learned from `R` messages on this connection.
struct RelMeta {
    table: String,
    columns: Vec<String>,
}

/// Stateful pgoutput→envelope decoder: tracks relation metadata and builds envelopes for tracked
/// tables (and sync counters for the `__el_sync` bookkeeping table).
struct Decoder<'a> {
    tables: &'a HashMap<String, TableSchema>,
    rels: HashMap<u32, RelMeta>,
}

impl<'a> Decoder<'a> {
    fn new(tables: &'a HashMap<String, TableSchema>) -> Self {
        Decoder { tables, rels: HashMap::new() }
    }

    fn on_relation(&mut self, msg: Message) {
        if let Message::Relation { rel_id, name, columns, .. } = msg {
            self.rels.insert(rel_id, RelMeta { table: name, columns });
        }
    }

    fn on_change(&mut self, msg: Message) -> Decoded {
        let rel_id = match &msg {
            Message::Insert { rel_id, .. }
            | Message::Update { rel_id, .. }
            | Message::Delete { rel_id, .. } => *rel_id,
            Message::Truncate { rel_ids } => {
                for id in rel_ids {
                    if let Some(rel) = self.rels.get(id) {
                        // Not supported: shapes/aggregates/subquery nodes would retain every
                        // truncated row. Degraded and loud.
                        tracing::error!(
                            "replicator: TRUNCATE on {} is not supported — shapes over this table \
                             are now stale and must be recreated",
                            rel.table
                        );
                    }
                }
                return Decoded::None;
            }
            _ => return Decoded::None,
        };
        let Some(rel) = self.rels.get(&rel_id) else {
            tracing::error!("replicator: change for unknown relation id {rel_id} (no R message seen)");
            return Decoded::None;
        };
        if rel.table == SYNC_TABLE {
            if let Message::Insert { new, .. } | Message::Update { new, .. } = &msg {
                if let Some(n) = sync_counter(rel, new) {
                    return Decoded::Sync(n);
                }
            }
            return Decoded::None;
        }
        let Some(ts) = self.tables.get(&rel.table) else { return Decoded::None };
        match build_envelope(&rel.table, ts, &rel.columns, msg) {
            Some(env) => Decoded::Env(env),
            None => Decoded::None,
        }
    }
}

/// Extract the `n` counter from an `__el_sync` tuple.
fn sync_counter(rel: &RelMeta, tuple: &Tuple) -> Option<i64> {
    let idx = rel.columns.iter().position(|c| c == "n")?;
    match tuple.get(idx)? {
        Cell::Text(s) => s.parse().ok(),
        _ => None,
    }
}

/// Build an envelope from a decoded pgoutput DML message; LSN/xid/seq are stamped at `Commit`.
fn build_envelope(table: &str, ts: &TableSchema, columns: &[String], msg: Message) -> Option<Envelope> {
    let make = |operation: &str, value: Option<Json>, old: Option<Json>, key_src: &Json| Envelope {
        type_: table.to_string(),
        key: key_from_obj(key_src, ts),
        value,
        old,
        headers: EnvelopeHeaders { operation: operation.to_string(), txid: None, offset: None, lsn: None, seq: None },
    };
    match msg {
        Message::Insert { new, .. } => {
            let new = Json::Object(tuple_to_map(&new, columns, ts));
            Some(make("insert", Some(new.clone()), None, &new))
        }
        Message::Update { old, new, .. } => {
            let old_map = match old {
                Some(OldTuple::Full(t)) => Some(tuple_to_map(&t, columns, ts)),
                Some(OldTuple::Key(_)) | None => {
                    // No full old image: the table's REPLICA IDENTITY is no longer FULL (e.g. a
                    // migration recreated it). The engine can't retract the prior row, so a change
                    // that moves a row OUT of a shape leaves it stale. Degraded and loud.
                    tracing::error!(
                        "replicator: UPDATE on {table} carries no full old image — REPLICA IDENTITY \
                         is no longer FULL; move-outs will be missed until it is restored and shapes \
                         recreated"
                    );
                    None
                }
            };
            let mut new_map = tuple_to_map(&new, columns, ts);
            // TOASTed-but-unchanged columns are omitted from new (see tuple_to_map); fill from old.
            if let Some(ref om) = old_map {
                for (k, v) in om {
                    new_map.entry(k.clone()).or_insert_with(|| v.clone());
                }
            }
            let new = Json::Object(new_map);
            let old = old_map.map(Json::Object);
            Some(make("update", Some(new.clone()), old, &new))
        }
        Message::Delete { old, .. } => {
            let OldTuple::Full(t) = old else {
                // Key-only image (REPLICA IDENTITY reset): retracting a phantom mostly-NULL row
                // would be wrong either way — skip it, loudly.
                tracing::error!(
                    "replicator: DELETE on {table} carries no full old image — REPLICA IDENTITY is \
                     no longer FULL; the delete cannot be propagated and shapes will retain the row"
                );
                return None;
            };
            let old = Json::Object(tuple_to_map(&t, columns, ts));
            Some(make("delete", None, Some(old.clone()), &old))
        }
        _ => None,
    }
}

/// Zip a pgoutput tuple with its relation's column names into a JSON object, converting each text
/// value by the column's schema type. `UnchangedToast` cells are OMITTED so the caller can fill
/// them from the old image; columns not in the schema are skipped.
fn tuple_to_map(tuple: &Tuple, columns: &[String], ts: &TableSchema) -> Map<String, Json> {
    let mut out = Map::new();
    for (cell, name) in tuple.iter().zip(columns) {
        let Some(ty) = ts.index.get(name).map(|&idx| ts.columns[idx].1) else { continue };
        match cell {
            Cell::UnchangedToast => {}
            Cell::Null => {
                out.insert(name.clone(), Json::Null);
            }
            Cell::Text(text) => {
                out.insert(name.clone(), text_to_json(text, ty));
            }
        }
    }
    out
}

/// Extract the primary-key string from a parsed row object. For composite primary keys the column values
/// are joined by the same separator [`TableSchema::key_string`] uses, so envelope keys match the engine's.
fn key_from_obj(obj: &Json, ts: &TableSchema) -> String {
    let one = |name: &str| -> String {
        match obj.get(name) {
            Some(Json::Null) | None => "null".to_string(),
            Some(Json::String(s)) => s.clone(),
            // Canonicalize through f64 for float pk columns so the envelope key matches the
            // engine's `Value::to_key_string` (serde would print `1.0` where f64 prints `1`).
            Some(Json::Number(n)) => match n.as_f64() {
                Some(f) if ts.index.get(name).is_some_and(|&i| ts.columns[i].1 == ColumnType::Float) => {
                    f.to_string()
                }
                _ => n.to_string(),
            },
            Some(Json::Bool(b)) => b.to_string(),
            Some(v) => v.to_string(),
        }
    };
    if ts.pk_cols.len() == 1 {
        return one(&ts.pk_name);
    }
    ts.pk_cols.iter().map(|&i| one(&ts.columns[i].0)).collect::<Vec<_>>().join("\u{1f}")
}

/// Convert a pgoutput text-mode scalar to JSON per the column type (NULL arrives as its own cell
/// kind, never as text). A value that fails its type's parse (e.g. `NaN`/`Infinity` floats,
/// out-of-range numerics) degrades to NULL — logged, because a real value silently becoming SQL
/// NULL downstream is a corruption, not a convenience.
fn text_to_json(text: &str, ty: ColumnType) -> Json {
    let fail = |ty: &str| {
        tracing::error!("replicator: unparseable {ty} value {text:?} degraded to NULL");
        Json::Null
    };
    match ty {
        ColumnType::Int => text.parse::<i64>().map(Json::from).unwrap_or_else(|_| fail("int")),
        ColumnType::Float => text
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(Json::Number)
            .unwrap_or_else(|| fail("float")),
        ColumnType::Bool => match text {
            "t" | "true" => Json::Bool(true),
            "f" | "false" => Json::Bool(false),
            _ => fail("bool"),
        },
        ColumnType::Text => Json::String(text.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgoutput::{Cell, Message, OldTuple};
    use crate::schema::{ColumnDef, TableDef};
    use std::collections::BTreeMap;

    fn users() -> HashMap<String, TableSchema> {
        let mut columns = BTreeMap::new();
        columns.insert("id".to_string(), ColumnDef { ty: ColumnType::Int, pg_type: None });
        columns.insert("tenant".to_string(), ColumnDef { ty: ColumnType::Int, pg_type: None });
        columns.insert("name".to_string(), ColumnDef { ty: ColumnType::Text, pg_type: None });
        let def = TableDef { columns, primary_key: vec!["id".to_string()] };
        let mut m = HashMap::new();
        m.insert("users".to_string(), TableSchema::from_def("users", &def).unwrap());
        m
    }

    fn decoder(tables: &HashMap<String, TableSchema>) -> Decoder<'_> {
        let mut d = Decoder::new(tables);
        d.on_relation(Message::Relation {
            rel_id: 1,
            namespace: "public".into(),
            name: "users".into(),
            columns: vec!["id".into(), "tenant".into(), "name".into()],
        });
        d.on_relation(Message::Relation {
            rel_id: 2,
            namespace: "public".into(),
            name: SYNC_TABLE.into(),
            columns: vec!["id".into(), "n".into()],
        });
        d
    }

    fn t(s: &str) -> Cell {
        Cell::Text(s.into())
    }

    fn env_of(d: Decoded) -> Envelope {
        match d {
            Decoded::Env(e) => e,
            _ => panic!("expected an envelope"),
        }
    }

    #[test]
    fn builds_insert_update_delete_with_old() {
        let tables = users();
        let mut d = decoder(&tables);

        let ins = env_of(d.on_change(Message::Insert { rel_id: 1, new: vec![t("1"), t("7"), t("a")] }));
        assert_eq!(ins.headers.operation, "insert");
        assert_eq!(ins.key, "1");
        assert_eq!(ins.value.as_ref().unwrap()["name"], "a");
        assert_eq!(ins.value.as_ref().unwrap()["tenant"], 7);
        assert!(ins.old.is_none());

        let upd = env_of(d.on_change(Message::Update {
            rel_id: 1,
            old: Some(OldTuple::Full(vec![t("1"), t("7"), t("a")])),
            new: vec![t("1"), t("7"), t("b")],
        }));
        assert_eq!(upd.headers.operation, "update");
        assert_eq!(upd.old.as_ref().unwrap()["name"], "a");
        assert_eq!(upd.value.as_ref().unwrap()["name"], "b");

        let del = env_of(d.on_change(Message::Delete {
            rel_id: 1,
            old: OldTuple::Full(vec![t("1"), t("7"), t("b")]),
        }));
        assert_eq!(del.headers.operation, "delete");
        assert_eq!(del.key, "1");
        assert_eq!(del.old.as_ref().unwrap()["tenant"], 7);
        assert!(del.value.is_none());
    }

    #[test]
    fn handles_null_and_utf8() {
        let tables = users();
        let mut d = decoder(&tables);
        let e = env_of(d.on_change(Message::Insert {
            rel_id: 1,
            new: vec![t("5"), Cell::Null, t("a b 'c' café ☃ 北京")],
        }));
        assert_eq!(e.value.as_ref().unwrap()["tenant"], Json::Null);
        assert_eq!(e.value.as_ref().unwrap()["name"], "a b 'c' café ☃ 北京");
    }

    #[test]
    fn toast_unchanged_value_filled_from_old() {
        let tables = users();
        let mut d = decoder(&tables);
        let upd = env_of(d.on_change(Message::Update {
            rel_id: 1,
            old: Some(OldTuple::Full(vec![t("1"), t("7"), t("big original")])),
            new: vec![t("1"), t("9"), Cell::UnchangedToast],
        }));
        assert_eq!(upd.value.as_ref().unwrap()["tenant"], 9); // changed col taken from new
        assert_eq!(upd.value.as_ref().unwrap()["name"], "big original"); // unchanged toast from old
    }

    /// A DELETE / UPDATE without the full old image (REPLICA IDENTITY no longer FULL) must degrade
    /// loudly, not fabricate retractions; TRUNCATE produces no envelope.
    #[test]
    fn degraded_forms_are_skipped() {
        let tables = users();
        let mut d = decoder(&tables);
        assert!(matches!(
            d.on_change(Message::Delete { rel_id: 1, old: OldTuple::Key(vec![t("1")]) }),
            Decoded::None
        ));
        assert!(matches!(d.on_change(Message::Truncate { rel_ids: vec![1] }), Decoded::None));
        // Update with key-only old image still emits (new row is valid) but without `old`.
        let upd = env_of(d.on_change(Message::Update {
            rel_id: 1,
            old: Some(OldTuple::Key(vec![t("1"), Cell::Null, Cell::Null])),
            new: vec![t("1"), t("7"), t("b")],
        }));
        assert!(upd.old.is_none());
    }

    #[test]
    fn sync_counter_from_sentinel_table_only() {
        let tables = users();
        let mut d = decoder(&tables);
        // A users row whose TEXT value mentions the sentinel is just data.
        let e = d.on_change(Message::Insert { rel_id: 1, new: vec![t("1"), t("7"), t("__el_sync n:999")] });
        assert!(matches!(e, Decoded::Env(_)));
        // The real sentinel update yields a sync counter, not an envelope.
        let s = d.on_change(Message::Update { rel_id: 2, old: None, new: vec![t("1"), t("5")] });
        assert!(matches!(s, Decoded::Sync(5)));
    }

    /// Changes for relations that are not tracked (and not the sentinel) are ignored.
    #[test]
    fn untracked_relations_are_ignored() {
        let tables = users();
        let mut d = decoder(&tables);
        d.on_relation(Message::Relation {
            rel_id: 9,
            namespace: "public".into(),
            name: "not_tracked".into(),
            columns: vec!["id".into()],
        });
        assert!(matches!(d.on_change(Message::Insert { rel_id: 9, new: vec![t("1")] }), Decoded::None));
    }

    #[test]
    fn float_pk_key_is_canonicalized() {
        let mut columns = BTreeMap::new();
        columns.insert("id".to_string(), ColumnDef { ty: ColumnType::Float, pg_type: None });
        let def = TableDef { columns, primary_key: vec!["id".to_string()] };
        let mut tables = HashMap::new();
        tables.insert("f".to_string(), TableSchema::from_def("f", &def).unwrap());
        let mut d = Decoder::new(&tables);
        d.on_relation(Message::Relation {
            rel_id: 3,
            namespace: "public".into(),
            name: "f".into(),
            columns: vec!["id".into()],
        });
        let e = env_of(d.on_change(Message::Insert { rel_id: 3, new: vec![t("1")] }));
        assert_eq!(e.key, "1"); // f64 canonical form, not "1.0"
    }
}
