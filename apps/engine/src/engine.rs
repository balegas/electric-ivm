//! Engine orchestration: schema/shape registries and one tailer task per table. A tailer owns
//! the table's authoritative `pk -> Row` state, fans each change out to every shape's circuit
//! actor, and appends the filtered deltas (as State-Protocol envelopes) to the shape streams.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use dbsp::ZWeight;
use dbsp::utils::Tup2;
use tokio::sync::{Mutex, mpsc};

use std::sync::atomic::Ordering;

use crate::ds::{DsClient, Envelope, EnvelopeHeaders};
use crate::family::FamilyActor;
use crate::metrics::{Timer, metrics};
use crate::predicate::{CompiledPredicate, PredicateJson};
use crate::schema::{Schema, TableSchema, compile_schema};
use crate::value::{Row, Value};

#[derive(Clone)]
pub struct Engine {
    ds: DsClient,
    state: Arc<Mutex<EngineState>>,
    /// Postgres connection string when running in Postgres mode (logical replication + query-back
    /// backfill, no in-memory `table_state`). `None` keeps the engine usable only as a library shell.
    pg_url: Option<String>,
    /// Last commit LSN the replication ingestor has appended (observability).
    repl_lsn: Arc<std::sync::Mutex<String>>,
    /// Highest `__el_sync` sentinel counter the ingestor has decoded-and-appended. The drain barrier
    /// bumps the sentinel and waits for this to catch up — robust under a shared multi-database
    /// Postgres (per-database, no dependence on server-global WAL LSNs).
    repl_sync: Arc<std::sync::atomic::AtomicI64>,
}

struct EngineState {
    tables: HashMap<String, TableSchema>,
    tailers: HashMap<String, TailerHandle>,
    shapes: HashMap<String, ShapeRecord>,
    next_shape_id: u64,
}

#[derive(Clone, Debug)]
pub struct ShapeRecord {
    pub id: String,
    pub table: String,
    pub stream_path: String,
}

struct TailerHandle {
    cmd_tx: mpsc::UnboundedSender<TailerCmd>,
    /// Offset up to which all table-stream envelopes have been processed AND fanned to every
    /// shape. Published after a batch is fully processed; a harness can poll this to know the
    /// engine has caught up to the stream tail (a sound convergence barrier).
    processed: Arc<std::sync::Mutex<String>>,
    /// Current circuit topology (shared families + standalone count), for tests/observability.
    stats: Arc<std::sync::Mutex<TableStats>>,
}

/// Per-table circuit topology: the shared family circuits (one per equality template) and the
/// count of standalone per-shape circuits. Exposed via `GET /tables/{name}/families` so a test can
/// prove that many same-template shapes share one circuit rather than spawning N.
#[derive(Clone, Default, serde::Serialize)]
pub struct TableStats {
    pub families: Vec<FamilyStat>,
    pub standalone: usize,
}

#[derive(Clone, serde::Serialize)]
pub struct FamilyStat {
    pub key_cols: Vec<usize>,
    pub shapes: usize,
}

enum TailerCmd {
    AddShape { shape_id: String, num_id: u64, stream_path: String, pred: Arc<CompiledPredicate> },
    RemoveShape { shape_id: String },
}

impl Engine {
    pub fn new(ds: DsClient) -> Self {
        Engine {
            ds,
            state: Arc::new(Mutex::new(EngineState {
                tables: HashMap::new(),
                tailers: HashMap::new(),
                shapes: HashMap::new(),
                next_shape_id: 1,
            })),
            pg_url: None,
            repl_lsn: Arc::new(std::sync::Mutex::new("0/0".to_string())),
            repl_sync: Arc::new(std::sync::atomic::AtomicI64::new(0)),
        }
    }

    /// Engine in Postgres mode: data lives in Postgres, ingested via logical replication and read
    /// back for backfill. Call [`setup_postgres`](Self::setup_postgres) before serving.
    pub fn new_pg(ds: DsClient, pg_url: String) -> Self {
        let mut e = Self::new(ds);
        e.pg_url = Some(pg_url);
        e
    }

    /// Introspect the configured tables from Postgres, set `REPLICA IDENTITY FULL`, create the
    /// replication slot, register the schema, and start the replication ingestor. Idempotent.
    pub async fn setup_postgres(&self, tables: &[String], slot: &str, poll_ms: u64) -> Result<()> {
        let url = self.pg_url.clone().context("setup_postgres called without a pg_url")?;
        let client = crate::pg::connect(&url).await?;
        let mut compiled = HashMap::new();
        for t in tables {
            let def = crate::pg::introspect(&client, t).await?;
            let ts = TableSchema::from_def(t, &def)?;
            crate::pg::ensure_replica_identity_full(&client, t).await?;
            self.ds.ensure_stream(&format!("table/{t}")).await?;
            compiled.insert(t.clone(), ts);
        }
        crate::pg::ensure_slot(&client, slot).await?;
        self.state.lock().await.tables = compiled.clone();
        tokio::spawn(crate::replication::run(
            url,
            slot.to_string(),
            poll_ms,
            self.ds.clone(),
            Arc::new(compiled),
            self.repl_lsn.clone(),
            self.repl_sync.clone(),
        ));
        Ok(())
    }

    /// Last commit LSN appended by the replication ingestor (text form, e.g. "0/1A2B3C").
    pub fn replication_lsn(&self) -> String {
        self.repl_lsn.lock().unwrap().clone()
    }

    /// Highest `__el_sync` sentinel counter the ingestor has decoded-and-appended.
    pub fn replication_sync(&self) -> i64 {
        self.repl_sync.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn stream_url(&self, path: &str) -> String {
        self.ds.stream_url(path)
    }

    pub async fn define_schema(&self, schema: &Schema) -> Result<()> {
        let compiled = compile_schema(schema)?;
        for name in compiled.keys() {
            self.ds.ensure_stream(&format!("table/{name}")).await?;
        }
        self.state.lock().await.tables = compiled;
        Ok(())
    }

    pub async fn create_shape(&self, table: &str, where_: Option<PredicateJson>) -> Result<ShapeRecord> {
        let mut st = self.state.lock().await;
        let ts = match st.tables.get(table) {
            Some(ts) => ts.clone(),
            None => bail!("unknown table '{table}'"),
        };
        let pred = Arc::new(CompiledPredicate::compile_opt(where_.as_ref(), &ts)?);

        let num_id = st.next_shape_id;
        let id = format!("s{num_id}");
        st.next_shape_id += 1;
        let stream_path = format!("shape/{id}");
        self.ds.ensure_stream(&stream_path).await?;

        if !st.tailers.contains_key(table) {
            let handle = spawn_tailer(self.ds.clone(), ts.clone(), self.pg_url.clone());
            st.tailers.insert(table.to_string(), handle);
        }
        let tailer = st.tailers.get(table).expect("tailer just inserted");
        tailer
            .cmd_tx
            .send(TailerCmd::AddShape { shape_id: id.clone(), num_id, stream_path: stream_path.clone(), pred })
            .map_err(|_| anyhow::anyhow!("tailer for '{table}' is gone"))?;

        let rec = ShapeRecord { id: id.clone(), table: table.to_string(), stream_path };
        st.shapes.insert(id, rec.clone());
        Ok(rec)
    }

    pub async fn drop_shape(&self, id: &str) -> Result<()> {
        let mut st = self.state.lock().await;
        if let Some(rec) = st.shapes.remove(id) {
            if let Some(t) = st.tailers.get(&rec.table) {
                let _ = t.cmd_tx.send(TailerCmd::RemoveShape { shape_id: id.to_string() });
            }
        }
        Ok(())
    }

    pub async fn get_shape(&self, id: &str) -> Option<ShapeRecord> {
        self.state.lock().await.shapes.get(id).cloned()
    }

    /// The offset up to which the table's tailer has processed, or `None` if no tailer exists
    /// (no shape registered on the table yet).
    pub async fn table_offset(&self, table: &str) -> Option<String> {
        let st = self.state.lock().await;
        st.tailers.get(table).map(|t| t.processed.lock().unwrap().clone())
    }

    /// The table's current circuit topology (shared families + standalone count), or `None` if no
    /// tailer exists.
    pub async fn table_stats(&self, table: &str) -> Option<TableStats> {
        let st = self.state.lock().await;
        st.tailers.get(table).map(|t| t.stats.lock().unwrap().clone())
    }
}

/// A non-shareable shape (range / OR / NOT / inequality / match-all). Its predicate is a stateless
/// filter, so it needs no dbsp circuit or OS thread — it is evaluated directly on each delta. This
/// is what lets standalone shapes scale far past the old one-thread-per-shape ceiling.
struct StandaloneShape {
    pred: Arc<CompiledPredicate>,
    stream_path: String,
    /// WAL LSN of this shape's backfill snapshot; replication changes with `lsn <= seed_lsn` are
    /// already reflected and are skipped.
    seed_lsn: u64,
}

/// Evaluate a stateless WHERE filter directly on a Z-set delta. A filter has no incremental state
/// (unlike a join), so running it in dbsp would only add a thread + channel round-trip + a per-shape
/// clone of the delta. `translate_output` downstream groups by primary key, so emitting the matching
/// `(row, weight)` pairs here is equivalent to what the old per-shape filter circuit produced.
fn eval_standalone(pred: &CompiledPredicate, delta: &[Tup2<Row, ZWeight>]) -> Vec<(Row, ZWeight)> {
    delta
        .iter()
        .filter(|t| pred.matches(&t.0))
        .map(|t| (t.0.clone(), t.1))
        .collect()
}

/// A shared circuit for all shapes whose predicate is the same equality template (see
/// `family::FamilyActor`). `shapes` maps each member's numeric id to its output stream path.
struct Family {
    actor: FamilyActor,
    shapes: HashMap<u64, String>,
    /// WAL LSN of the snapshot the data trace was seeded from; replication changes with
    /// `lsn <= seed_lsn` are already in the trace and are skipped.
    seed_lsn: u64,
}

fn spawn_tailer(ds: DsClient, ts: TableSchema, pg_url: Option<String>) -> TailerHandle {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let processed = Arc::new(std::sync::Mutex::new("-1".to_string()));
    let stats = Arc::new(std::sync::Mutex::new(TableStats::default()));
    tokio::spawn(tailer_loop(ds, ts, pg_url, cmd_rx, processed.clone(), stats.clone()));
    TailerHandle { cmd_tx, processed, stats }
}

fn publish_stats(
    stats: &std::sync::Mutex<TableStats>,
    shapes: &HashMap<String, StandaloneShape>,
    families: &HashMap<Vec<usize>, Family>,
) {
    let mut fams: Vec<FamilyStat> = families
        .iter()
        .map(|(k, f)| FamilyStat { key_cols: k.clone(), shapes: f.shapes.len() })
        .collect();
    fams.sort_by(|a, b| a.key_cols.cmp(&b.key_cols));
    *stats.lock().unwrap() = TableStats { families: fams, standalone: shapes.len() };
}

async fn tailer_loop(
    ds: DsClient,
    ts: TableSchema,
    pg_url: Option<String>,
    mut cmd_rx: mpsc::UnboundedReceiver<TailerCmd>,
    processed: Arc<std::sync::Mutex<String>>,
    stats: Arc<std::sync::Mutex<TableStats>>,
) {
    let table_path = format!("table/{}", ts.name);
    let mut offset = "-1".to_string();
    // Standalone per-shape filter circuits (non-equality predicates), keyed by shape id.
    let mut shapes: HashMap<String, StandaloneShape> = HashMap::new();
    // Shared family circuits, keyed by the equality template's (sorted) column indices.
    let mut families: HashMap<Vec<usize>, Family> = HashMap::new();
    // Reverse lookup for removal: shape id -> (template key cols, numeric id, key tuple).
    let mut family_of: HashMap<String, (Vec<usize>, u64, Row)> = HashMap::new();

    loop {
        let off = offset.clone();
        tokio::select! {
            biased;
            cmd = cmd_rx.recv() => match cmd {
                Some(TailerCmd::AddShape { shape_id, num_id, stream_path, pred }) => {
                    if let Err(e) = add_shape_routed(
                        &ds, &ts, &pg_url, &mut shapes, &mut families, &mut family_of,
                        shape_id, num_id, stream_path, pred,
                    ).await {
                        tracing::error!("add_shape failed: {e:#}");
                    }
                    publish_stats(&stats, &shapes, &families);
                }
                Some(TailerCmd::RemoveShape { shape_id }) => {
                    if shapes.remove(&shape_id).is_none()
                        && let Some((key_cols, num_id, key_tuple)) = family_of.remove(&shape_id)
                        && let Some(fam) = families.get_mut(&key_cols)
                    {
                        // Drop the shape's param so future changes skip it; ignore the removal delta
                        // (the shape stream is being torn down).
                        let _ = fam.actor.step(vec![], vec![Tup2(Tup2(key_tuple, num_id), -1)]).await;
                        fam.shapes.remove(&num_id);
                        if fam.shapes.is_empty() {
                            families.remove(&key_cols); // discard the now-unused family + its trace
                        }
                    }
                    publish_stats(&stats, &shapes, &families);
                }
                None => break,
            },
            res = ds.read(&table_path, &off, true) => match res {
                Ok(rr) => {
                    let next = rr.next_offset.clone();
                    if let Some(n) = rr.next_offset { offset = n; }
                    // Process the whole read batch, collecting shape-stream appends, then flush them
                    // concurrently. Appends (HTTP round-trips) dominate, so coalescing per stream and
                    // parallelizing is the main throughput/latency lever.
                    let mut pending: HashMap<String, Vec<Envelope>> = HashMap::new();
                    for env in rr.envelopes {
                        if let Err(e) = process_envelope(&ts, &shapes, &families, env, &mut pending).await {
                            tracing::error!("process_envelope failed: {e:#}");
                        }
                    }
                    flush_pending(&ds, pending).await;
                    // Publish the processed offset only after the whole batch is fanned out + flushed.
                    if let Some(n) = next {
                        *processed.lock().unwrap() = n;
                    }
                }
                Err(e) => {
                    tracing::warn!("tailer read error on {table_path}: {e:#}; backing off");
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
            },
        }
    }
}

/// Route a new shape to a shared family circuit (pure-equality predicate) or a standalone filter
/// circuit (everything else). For a family, adding the shape is a `Params` insert; its backfill is
/// the join of that param against the family's current data trace.
#[allow(clippy::too_many_arguments)]
async fn add_shape_routed(
    ds: &DsClient,
    ts: &TableSchema,
    pg_url: &Option<String>,
    shapes: &mut HashMap<String, StandaloneShape>,
    families: &mut HashMap<Vec<usize>, Family>,
    family_of: &mut HashMap<String, (Vec<usize>, u64, Row)>,
    shape_id: String,
    num_id: u64,
    stream_path: String,
    pred: Arc<CompiledPredicate>,
) -> Result<()> {
    match pred.equality_template() {
        Some(pairs) => {
            let key_cols: Vec<usize> = pairs.iter().map(|(c, _)| *c).collect();
            let key_tuple = Row(pairs.into_iter().map(|(_, v)| v).collect());
            let param = Tup2(Tup2(key_tuple.clone(), num_id), 1);

            if let Some(fam) = families.get_mut(&key_cols) {
                // Existing family: insert the param; the incremental join backfills from the trace
                // (which already holds the seed + every change applied since).
                let out = fam.actor.step(vec![], vec![param]).await?;
                fam.shapes.insert(num_id, stream_path);
                emit_family_output(ds, ts, fam, out, None).await?;
            } else {
                // New family: seed the data trace from a Postgres snapshot and add the param in one
                // step; the step output is this shape's backfill. `seed_lsn` lets the tailer skip
                // replication changes already reflected in the snapshot.
                let bf = pg_backfill(pg_url, ts).await?;
                let actor = FamilyActor::spawn(Arc::new(key_cols.clone()))?;
                let data: Vec<Tup2<Row, ZWeight>> = bf.rows.iter().map(|r| Tup2(r.clone(), 1)).collect();
                let out = actor.step(data, vec![param]).await?;
                let mut fam = Family {
                    actor,
                    shapes: HashMap::new(),
                    seed_lsn: crate::pg::lsn_to_u64(&bf.seed_lsn),
                };
                fam.shapes.insert(num_id, stream_path);
                emit_family_output(ds, ts, &fam, out, None).await?;
                families.insert(key_cols.clone(), fam);
            }
            family_of.insert(shape_id, (key_cols, num_id, key_tuple));
        }
        None => {
            // Standalone filter: backfill = current matching rows from Postgres (emitted as upserts).
            let bf = pg_backfill(pg_url, ts).await?;
            let out: Vec<(Row, ZWeight)> =
                bf.rows.iter().filter(|r| pred.matches(r)).map(|r| (r.clone(), 1)).collect();
            if !out.is_empty() {
                let envs = translate_output(ts, out, None);
                ds.append(&stream_path, &envs).await?;
            }
            shapes.insert(
                shape_id,
                StandaloneShape { pred, stream_path, seed_lsn: crate::pg::lsn_to_u64(&bf.seed_lsn) },
            );
        }
    }
    Ok(())
}

/// Read a backfill snapshot from Postgres (current rows + snapshot LSN). Without a `pg_url`
/// (library/no-source mode) the shape simply starts empty.
async fn pg_backfill(pg_url: &Option<String>, ts: &TableSchema) -> Result<crate::pg::Backfill> {
    match pg_url {
        Some(url) => {
            let client = crate::pg::connect(url).await?;
            crate::pg::backfill(&client, ts).await
        }
        None => Ok(crate::pg::Backfill { rows: Vec::new(), seed_lsn: "0/0".to_string() }),
    }
}

/// Demultiplex a family circuit's `(shape_id, row, weight)` output by shape and append each shape's
/// rows (translated to envelopes) to its stream.
async fn emit_family_output(
    ds: &DsClient,
    ts: &TableSchema,
    fam: &Family,
    out: Vec<(u64, Row, ZWeight)>,
    txid: Option<String>,
) -> Result<()> {
    let mut by_shape: HashMap<u64, Vec<(Row, ZWeight)>> = HashMap::new();
    for (sid, row, w) in out {
        by_shape.entry(sid).or_default().push((row, w));
    }
    for (sid, rows) in by_shape {
        if let Some(stream_path) = fam.shapes.get(&sid) {
            let envs = translate_output(ts, rows, txid.clone());
            if !envs.is_empty() {
                {
                    let _a = Timer::new(&metrics().append);
                    ds.append(stream_path, &envs).await?;
                }
                metrics().shape_appends.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    Ok(())
}

async fn process_envelope(
    ts: &TableSchema,
    shapes: &HashMap<String, StandaloneShape>,
    families: &HashMap<Vec<usize>, Family>,
    env: Envelope,
    pending: &mut HashMap<String, Vec<Envelope>>,
) -> Result<()> {
    let (delta, txid, lsn) = apply_envelope(ts, &env)?;
    if delta.is_empty() {
        return Ok(());
    }
    let lsn = lsn.as_deref().map(crate::pg::lsn_to_u64).unwrap_or(0);
    metrics().envelopes.fetch_add(1, Ordering::Relaxed);
    let _t = Timer::new(&metrics().process_envelope);
    // Standalone shapes: evaluate each stateless filter directly on the delta (no thread, no clone).
    // Skip changes already reflected in the shape's backfill snapshot. `seed_lsn` is the snapshot's
    // `pg_current_wal_lsn()` (the NEXT byte to be written), so every in-snapshot change has
    // `lsn < seed_lsn` strictly; the first post-snapshot change can have `lsn == seed_lsn` and MUST
    // NOT be skipped (else the first live insert is silently dropped).
    for shape in shapes.values() {
        if lsn != 0 && lsn < shape.seed_lsn {
            continue;
        }
        let out = eval_standalone(&shape.pred, &delta);
        if out.is_empty() {
            continue;
        }
        let envs = translate_output(ts, out, txid.clone());
        pending.entry(shape.stream_path.clone()).or_default().extend(envs);
    }
    // Shared family circuits: one join per template fans the delta to all its shapes. Skip changes
    // already in the family's seed snapshot so the trace's weights stay exact (no double-count).
    for fam in families.values() {
        if lsn != 0 && lsn < fam.seed_lsn {
            continue;
        }
        let out = {
            let _s = Timer::new(&metrics().family_step);
            fam.actor.step(delta.clone(), vec![]).await?
        };
        metrics().family_steps.fetch_add(1, Ordering::Relaxed);
        if out.is_empty() {
            continue;
        }
        collect_family_output(ts, fam, out, txid.clone(), pending);
    }
    Ok(())
}

/// Demultiplex a family circuit's output by shape and stage each shape's envelopes for flushing.
fn collect_family_output(
    ts: &TableSchema,
    fam: &Family,
    out: Vec<(u64, Row, ZWeight)>,
    txid: Option<String>,
    pending: &mut HashMap<String, Vec<Envelope>>,
) {
    let mut by_shape: HashMap<u64, Vec<(Row, ZWeight)>> = HashMap::new();
    for (sid, row, w) in out {
        by_shape.entry(sid).or_default().push((row, w));
    }
    for (sid, rows) in by_shape {
        if let Some(stream_path) = fam.shapes.get(&sid) {
            let envs = translate_output(ts, rows, txid.clone());
            if !envs.is_empty() {
                pending.entry(stream_path.clone()).or_default().extend(envs);
            }
        }
    }
}

/// Flush the batch's staged appends, bounded-concurrently. Each envelope keeps its own txid, so
/// `awaitTxId` semantics are preserved; only the HTTP round-trips are coalesced + parallelized.
async fn flush_pending(ds: &DsClient, pending: HashMap<String, Vec<Envelope>>) {
    const CAP: usize = 32; // bound in-flight appends so we don't swamp the storage server
    let mut items: Vec<(String, Vec<Envelope>)> = pending.into_iter().collect();
    while !items.is_empty() {
        let take = items.len().min(CAP);
        let batch = items.split_off(items.len() - take);
        let mut set = tokio::task::JoinSet::new();
        for (path, envs) in batch {
            let ds = ds.clone();
            set.spawn(async move {
                let _t = Timer::new(&metrics().append);
                let res = ds.append(&path, &envs).await;
                metrics().shape_appends.fetch_add(1, Ordering::Relaxed);
                res
            });
        }
        while let Some(j) = set.join_next().await {
            if let Ok(Err(e)) = j {
                tracing::error!("append failed: {e:#}");
            }
        }
    }
}

/// Turn a table change event into the resulting input Z-set delta, plus the originating txid and
/// commit LSN. The delta is computed entirely from the envelope's `value` (new row) and `old` (prior
/// row, carried by replication under `REPLICA IDENTITY FULL`) — no in-memory `table_state`.
pub(crate) fn apply_envelope(
    ts: &TableSchema,
    env: &Envelope,
) -> Result<(Vec<Tup2<Row, ZWeight>>, Option<String>, Option<String>)> {
    let txid = env.headers.txid.clone();
    let lsn = env.headers.lsn.clone();
    let to_row = |v: &serde_json::Value| -> Result<Row> {
        let obj = v.as_object().ok_or_else(|| anyhow::anyhow!("envelope row is not an object"))?;
        ts.row_from_json(obj)
    };
    let mut delta: Vec<Tup2<Row, ZWeight>> = Vec::new();
    match env.headers.operation.as_str() {
        "insert" => {
            let new = to_row(env.value.as_ref().context("insert envelope missing value")?)?;
            delta.push(Tup2(new, 1));
        }
        "update" | "upsert" => {
            let new = to_row(env.value.as_ref().context("update envelope missing value")?)?;
            match env.old.as_ref() {
                Some(old) => {
                    let old = to_row(old)?;
                    if old != new {
                        delta.push(Tup2(old, -1));
                        delta.push(Tup2(new, 1));
                    }
                }
                // No prior row available -> treat as an insert of the new row.
                None => delta.push(Tup2(new, 1)),
            }
        }
        "delete" => {
            // Replication carries the full old row (REPLICA IDENTITY FULL); retract it.
            if let Some(old) = env.old.as_ref() {
                delta.push(Tup2(to_row(old)?, -1));
            }
        }
        other => bail!("unknown operation '{other}'"),
    }
    Ok((delta, txid, lsn))
}

/// Translate a shape circuit's output Z-set delta into State-Protocol envelopes. Grouped by pk:
/// any positive-weight row -> `upsert` (enter/update); otherwise `delete` (leave).
pub(crate) fn translate_output(ts: &TableSchema, out: Vec<(Row, ZWeight)>, txid: Option<String>) -> Vec<Envelope> {
    let mut pos: HashMap<String, Row> = HashMap::new();
    let mut neg: HashSet<String> = HashSet::new();
    for (row, w) in out {
        let Ok(pk) = ts.pk_of(&row).map(Value::to_key_string) else { continue };
        if w > 0 {
            pos.insert(pk, row);
        } else if w < 0 {
            neg.insert(pk);
        }
    }
    let mut envs = Vec::with_capacity(pos.len() + neg.len());
    for (pk, row) in &pos {
        envs.push(Envelope {
            type_: ts.name.clone(),
            key: pk.clone(),
            value: Some(ts.row_to_json(row)),
            old: None,
            headers: EnvelopeHeaders { operation: "upsert".into(), txid: txid.clone(), offset: None, lsn: None },
        });
    }
    // TEST-ONLY: the `drop_deletes` fault suppresses "leave" envelopes so rows that exit a shape
    // linger in the client. No-op unless ELECTRIC_LITE_FAULT=drop_deletes (see `fault`).
    let drop_deletes = matches!(crate::fault::active(), crate::fault::Fault::DropDeletes);
    for pk in &neg {
        if pos.contains_key(pk) || drop_deletes {
            continue;
        }
        envs.push(Envelope {
            type_: ts.name.clone(),
            key: pk.clone(),
            value: None,
            old: None,
            headers: EnvelopeHeaders { operation: "delete".into(), txid: txid.clone(), offset: None, lsn: None },
        });
    }
    envs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{TableDef, TableSchema};

    fn users() -> TableSchema {
        let def: TableDef = serde_json::from_value(serde_json::json!({
            "columns": { "id": {"type":"int"}, "name": {"type":"text"}, "active": {"type":"bool"} },
            "primaryKey": "id"
        }))
        .unwrap();
        TableSchema::from_def("users", &def).unwrap()
    }

    fn env(op: &str, key: &str, value: Option<serde_json::Value>, old: Option<serde_json::Value>) -> Envelope {
        Envelope {
            type_: "users".into(),
            key: key.into(),
            value,
            old,
            headers: EnvelopeHeaders { operation: op.into(), txid: None, offset: None, lsn: None },
        }
    }

    /// End-to-end (sans HTTP): replication envelope (old+new) -> input delta -> direct filter eval ->
    /// output envelopes, exercising enter / update / leave for a `WHERE active = true` shape.
    #[test]
    fn change_to_shape_envelope_enter_update_leave() {
        let ts = users();
        let pred = CompiledPredicate::compile_opt(
            Some(&serde_json::from_value(serde_json::json!({"col":"active","op":"eq","value":true})).unwrap()),
            &ts,
        ).unwrap();

        // enter: insert an active row -> upsert envelope
        let (delta, _, _) = apply_envelope(&ts, &env("insert", "1", Some(serde_json::json!({"id":1,"name":"a","active":true})), None)).unwrap();
        let envs = translate_output(&ts, eval_standalone(&pred, &delta), None);
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].headers.operation, "upsert");
        assert_eq!(envs[0].key, "1");

        // update within shape (name change, still active) -> upsert with new value
        let (delta, _, _) = apply_envelope(&ts, &env("update", "1", Some(serde_json::json!({"id":1,"name":"a2","active":true})), Some(serde_json::json!({"id":1,"name":"a","active":true})))).unwrap();
        let envs = translate_output(&ts, eval_standalone(&pred, &delta), None);
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].headers.operation, "upsert");
        assert_eq!(envs[0].value.as_ref().unwrap()["name"], "a2");

        // leave: becomes inactive -> delete envelope
        let (delta, _, _) = apply_envelope(&ts, &env("update", "1", Some(serde_json::json!({"id":1,"name":"a2","active":false})), Some(serde_json::json!({"id":1,"name":"a2","active":true})))).unwrap();
        let envs = translate_output(&ts, eval_standalone(&pred, &delta), None);
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].headers.operation, "delete");
        assert_eq!(envs[0].key, "1");

        // a non-matching insert produces no shape envelope
        let (delta, _, _) = apply_envelope(&ts, &env("insert", "2", Some(serde_json::json!({"id":2,"name":"b","active":false})), None)).unwrap();
        let envs = translate_output(&ts, eval_standalone(&pred, &delta), None);
        assert_eq!(envs.len(), 0);
    }
}
