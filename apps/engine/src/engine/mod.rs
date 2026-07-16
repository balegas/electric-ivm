//! Engine orchestration: schema/shape registries and one tailer task per table. A tailer holds only
//! per-shape routing metadata (no table data): it fans each change out to standalone filters and to
//! equality shapes routed by key, and appends the filtered deltas (as State-Protocol envelopes) to
//! the shape streams. Shapes backfill from Postgres on registration; see `add_shape_routed`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use crate::value::{Tup2, ZWeight};
use tokio::sync::{Mutex, mpsc};

use std::sync::atomic::Ordering;

use crate::ds::{DsClient, Envelope, EnvelopeHeaders};
use crate::heap_size::HeapSize;
use crate::metrics::{Timer, metrics};
use crate::predicate::{CompiledPredicate, PredicateJson};
use crate::schema::{Schema, TableSchema, compile_schema};
use crate::retention::{EvictReason, LifeState, RetentionConfig, ShapeLife, SweepShape};
use crate::subquery::{SubqueryRegistry, predicate_has_subquery, referenced_tables};
use crate::value::{Row, Value};

mod catalog;
mod circuit_serving;
pub(crate) mod emission;
mod executors;
mod introspection;
mod lifecycle;
pub(crate) mod membership;
mod output;
mod planning;
mod sequencer;
#[cfg(test)]
mod tests;

use catalog::*;
use circuit_serving::*;
use executors::*;
use introspection::*;
use planning::*;
use sequencer::*;

pub use executors::AggFn;
pub use introspection::{
    AggInfo, ArrConsumer, ArrCounts, ArrIndex, ArrInput, ArrangementGraph, EngineGraph,
    FamilyStat, GraphEdge, GraphNode, GraphShape, NodeIndex, NodeStateSummary, NodeValue,
    OpEdge, OpNode, ShapeRecord, StateSnapshot, TableColumnInfo, TableSchemaInfo, TableStats,
};
pub use planning::CircuitPlacement;
pub(crate) use output::{apply_envelope, delete_envelopes, translate_output};

/// `GET /v1/health` phases (see [`Engine::health`]).
const HEALTH_WAITING: u8 = 0;
const HEALTH_STARTING: u8 = 1;
const HEALTH_ACTIVE: u8 = 2;

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
    /// Set once the replication ingestor has been spawned, so `setup_postgres` stays idempotent.
    replicator_started: Arc<std::sync::atomic::AtomicBool>,
    /// Boot readiness phase driving `GET /v1/health`: 0 = `waiting` (Postgres not connected), 1 =
    /// `starting` (connected; introspecting / creating slot / spawning ingest), 2 = `active` (ingest
    /// loop running). Library mode (no Postgres) is `active` from construction.
    health: Arc<std::sync::atomic::AtomicU8>,
    /// Cross-table subquery registry: maintained inner-set nodes (shared by canonical signature) + the
    /// outer subquery shapes that depend on them. Every tailer routes its deltas here so an inner-table
    /// change moves outer rows. `None`-free; empty until a subquery shape is created.
    subqueries: Arc<Mutex<SubqueryRegistry>>,
    /// Best-effort per-envelope trace broadcast (see [`crate::trace`]). Events are serialized once
    /// and only when someone is subscribed; slow subscribers lag and drop.
    trace_tx: tokio::sync::broadcast::Sender<Arc<String>>,
    /// Sender to the single flip-propagator task: inner-set flips detected by a tailer are handed
    /// off here so their Postgres query-backs run off the tailer hot path (see
    /// [`crate::subquery::propagate_flips`]).
    flip_tx: mpsc::UnboundedSender<FlipWork>,
    /// Flip batches enqueued but not yet fully propagated. Part of the convergence barrier:
    /// drained change log + `pending_flips == 0` ⇒ all subquery effects have landed.
    pending_flips: Arc<std::sync::atomic::AtomicI64>,
    /// Table schemas shared with the sequencer task (updated on `setup_postgres`/`define_schema`).
    tables_shared: SharedTables,
    /// Ordered writer for the durable shape catalog (see [`CATALOG_STREAM`]).
    catalog_tx: mpsc::UnboundedSender<CatalogEvent>,
    /// Change-log offset the sequencer starts from (set by catalog restore before the spawn).
    seq_start: Arc<std::sync::Mutex<String>>,
    /// Per-shape retention lifecycle + last-read instant. A separate sync mutex (not
    /// `EngineState`) so hot read paths can touch it without the async engine lock. Lock order:
    /// when both are held, `state` first, then `lives`; never across `.await`.
    lives: Arc<std::sync::Mutex<HashMap<String, ShapeLife>>>,
    /// Retention policy knobs (see `crate::retention`).
    retention: Arc<RetentionConfig>,
    /// Set once the background retention sweeper has been spawned (lazy, idempotent).
    retention_started: Arc<std::sync::atomic::AtomicBool>,
    /// dbsp arrangement settings (`ELECTRIC_IVM_DBSP*`), set before `setup_postgres`.
    dbsp_cfg: Arc<std::sync::Mutex<Option<crate::config::DbspConfig>>>,
    /// The dbsp arrangement layer, once started (see [`crate::arrangements`]).
    arrangements: Arc<std::sync::Mutex<Option<crate::arrangements::Arrangements>>>,
    /// Per-table seed-snapshot gates fencing the arrangement feed (fresh seeds only; empty
    /// after a checkpoint restore, where the highwater does the fencing instead).
    arr_gates: Arc<std::sync::RwLock<HashMap<String, crate::pg::SnapshotGate>>>,
}

/// One tailer envelope's worth of deferred subquery flips (see [`Engine::flip_tx`]).
pub(crate) struct FlipWork {
    work: std::collections::VecDeque<(crate::predicate::SubquerySig, crate::subquery::Flip)>,
    txid: Option<String>,
    /// The originating write's commit lsn, threaded through to the deferred flip's trace event so
    /// it carries the same lsn/txid as the direct-change event that triggered the propagation —
    /// letting the activity log group them as one write (see `subquery::emit_flip_trace`).
    lsn: Option<String>,
}

/// Everything a tailer needs to route deltas through the subquery layer: the shared registry for
/// the synchronous node-reconcile + outer-emission phases, and the deferral channel + pending
/// counter for flip propagation.
#[derive(Clone)]
struct SubqueryHandle {
    registry: Arc<Mutex<SubqueryRegistry>>,
    flip_tx: mpsc::UnboundedSender<FlipWork>,
    pending_flips: Arc<std::sync::atomic::AtomicI64>,
}

/// Spawn the flip-propagation dispatcher: FlipWork batches run **concurrently**, bounded by a
/// semaphore (`ELECTRIC_IVM_FLIP_WORKERS`, default 8) — the Postgres round-trips are the
/// dominant cost and are independent across batches. Correctness does not depend on
/// propagation order: membership evaluation happens under the registry lock and the resulting
/// envelopes are **enqueued under that same lock** into per-stream FIFO emission lanes
/// (`engine::emission`), so per-shape append order equals eval order regardless of which
/// worker ran the query-back; absolute per-pk emission makes concurrent re-derivations
/// convergent (see `subquery.rs`).
fn spawn_flip_propagator(
    registry: Arc<Mutex<SubqueryRegistry>>,
    mut rx: mpsc::UnboundedReceiver<FlipWork>,
    pending: Arc<std::sync::atomic::AtomicI64>,
    trace_tx: tokio::sync::broadcast::Sender<Arc<String>>,
) {
    let workers: usize = std::env::var("ELECTRIC_IVM_FLIP_WORKERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8)
        .max(1);
    tokio::spawn(async move {
        let sem = Arc::new(tokio::sync::Semaphore::new(workers));
        while let Some(fw) = rx.recv().await {
            let permit = sem.clone().acquire_owned().await.expect("flip semaphore");
            let registry = registry.clone();
            let pending = pending.clone();
            let trace_tx = trace_tx.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    crate::subquery::propagate_flips(&registry, fw.work, fw.txid, fw.lsn, &trace_tx).await
                {
                    tracing::error!("subquery flip propagation failed: {e:#}");
                }
                // Decremented only after propagation finished enqueueing every resulting
                // batch — each batch carries its own pending increment until it lands, so
                // the barrier never reads zero with effects in flight.
                pending.fetch_sub(1, Ordering::SeqCst);
                drop(permit);
            });
        }
    });
}

struct EngineState {
    tables: HashMap<String, TableSchema>,
    sequencer: Option<SequencerHandle>,
    shapes: HashMap<String, ShapeRecord>,
    next_shape_id: u64,
    /// Shape sharing. Any two **equal** shapes — same kind and definition (see `shape_signature` /
    /// `agg_signature`: table + canonical predicate + columns + changes-only, or table + predicate +
    /// func + column for aggregates) — share ONE durable stream + ONE routed/standalone/registry entry,
    /// ref-counted, so the engine maintains + appends once for all subscribers instead of once each. A
    /// joiner positions itself with its own snapshot LSN (client-side `< S` drop), so sharing is safe.
    /// Covers plain, subquery, and aggregate shapes. `feed_by_sig`: signature -> shape_id;
    /// `feed_shares`: shape_id -> (sig, refcount).
    feed_by_sig: HashMap<String, String>,
    /// Circuit-served placement per shape id (label like `all` / `static:project_id` /
    /// `dynamic:project_id` / `counts`), plus the arrangement column serving it — feeds the
    /// graph payload so the visualizer can draw pipeline→shape edges.
    circuit_placement: HashMap<String, CircuitPlacement>,
    feed_shares: HashMap<String, FeedShare>,
}

struct FeedShare {
    sig: String,
    refcount: usize,
    /// Creation outcome, observed by joiners: `None` while the creator's backfill/registration is in
    /// flight, `Some(true)` once the shape is live (its snapshot is readable), `Some(false)` if
    /// creation failed (the entry is removed; joiners must error, not return a dead stream).
    ready: tokio::sync::watch::Receiver<Option<bool>>,
}

/// Wait until a shared shape's creator reports the shape live (or failed). Joining before the
/// backfill lands would hand the caller a stream whose snapshot isn't readable yet.
async fn await_share_ready(mut rx: tokio::sync::watch::Receiver<Option<bool>>, id: &str) -> Result<()> {
    loop {
        let state = *rx.borrow();
        match state {
            Some(true) => return Ok(()),
            Some(false) => bail!("shared shape '{id}' failed to initialize; retry the create"),
            None => {
                if rx.changed().await.is_err() {
                    bail!("shared shape '{id}' creator died before completing; retry the create");
                }
            }
        }
    }
}

/// Canonical signature for feed sharing: table + serialized predicate + sorted projection indices.
/// Two subset feeds with an equal signature are interchangeable and share one stream.
/// Order-insensitive predicate canonicalization (same form used for subquery-node sharing), so
/// `a AND b` and `b AND a` collapse to one shape.
fn canon_where(where_: &Option<PredicateJson>) -> String {
    where_.as_ref().map(crate::predicate::canonical_pred).unwrap_or_default()
}

/// The coarse engine column type as a stable string for the schema endpoint's JSON.
fn col_type_str(ty: crate::schema::ColumnType) -> &'static str {
    use crate::schema::ColumnType::*;
    match ty {
        Int => "int",
        Text => "text",
        Bool => "bool",
        Float => "float",
    }
}

fn canon_cols(out_cols: &Option<Arc<Vec<usize>>>) -> String {
    out_cols
        .as_ref()
        .map(|v| {
            let mut idx = v.as_ref().clone();
            idx.sort_unstable();
            idx.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(",")
        })
        .unwrap_or_default()
}

/// The sharing key for a **row shape** (materialized or changes-only feed, plain or subquery). Two
/// shapes are interchangeable — and so share one maintained stream — iff these all match. `changes_only`
/// is part of the key: a backfilled shape and a no-backfill feed over the same rows are NOT the same
/// stream.
fn shape_signature(
    table: &str,
    where_: &Option<PredicateJson>,
    out_cols: &Option<Arc<Vec<usize>>>,
    changes_only: bool,
) -> String {
    format!("shape\u{1f}{}\u{1f}{table}\u{1f}{}\u{1f}{}", changes_only, canon_where(where_), canon_cols(out_cols))
}

/// The sharing key for an **aggregation shape**: table + predicate + function + column. Namespaced so it
/// never collides with a row shape's key.
fn agg_signature(table: &str, where_: &Option<PredicateJson>, func: &AggFn, col_idx: Option<usize>) -> String {
    format!("agg\u{1f}{table}\u{1f}{}\u{1f}{:?}\u{1f}{:?}", canon_where(where_), func, col_idx)
}

/// Broadcast a graph-lifecycle event on the trace channel (zero cost with no subscribers).
fn trace_lifecycle(tx: &tokio::sync::broadcast::Sender<Arc<String>>, ev: crate::trace::GraphLifecycle) {
    if tx.receiver_count() == 0 {
        return;
    }
    if let Ok(json) = serde_json::to_string(&ev) {
        let _ = tx.send(Arc::new(json));
    }
}

/// The graph/trace node id of a family router: `family:<table>:<col,col>` (column NAMES, matching
/// the hop ids `process_envelope` emits and the ids the visualizer renders).
fn family_node_id(ts: &TableSchema, key_cols: &[usize]) -> String {
    let cols = key_cols
        .iter()
        .map(|i| ts.columns.get(*i).map(|(n, _)| n.clone()).unwrap_or_else(|| format!("col{i}")))
        .collect::<Vec<_>>()
        .join(",");
    format!("family:{}:{cols}", ts.name)
}

/// Shape id (`s<N>`) from its stream path (`shape/s<N>`) — the key `emitted` counters are kept by.
fn sid_of_path(stream_path: &str) -> &str {
    stream_path.strip_prefix("shape/").unwrap_or(stream_path)
}

impl Engine {
    pub fn new(ds: DsClient) -> Self {
        Self::new_inner(ds, None)
    }

    /// Engine in Postgres mode: data lives in Postgres, ingested via logical replication and read
    /// back for backfill. Call [`setup_postgres`](Self::setup_postgres) before serving.
    pub fn new_pg(ds: DsClient, pg_url: String) -> Self {
        let e = Self::new_inner(ds, Some(pg_url));
        // Postgres mode starts `waiting` until the connection + introspection + slot + ingest are up.
        e.health.store(HEALTH_WAITING, std::sync::atomic::Ordering::Relaxed);
        e
    }

    fn new_inner(ds: DsClient, pg_url: Option<String>) -> Self {
        let subqueries = Arc::new(Mutex::new(SubqueryRegistry::new(ds.clone(), pg_url.clone())));
        let trace_tx = tokio::sync::broadcast::channel(crate::trace::CHANNEL_CAP).0;
        let (flip_tx, flip_rx) = mpsc::unbounded_channel();
        let pending_flips = Arc::new(std::sync::atomic::AtomicI64::new(0));
        // Ordered emission lanes for subquery-shape appends (network out from under the
        // registry lock; per-stream FIFO keeps append order = eval order). They share the
        // pendingFlips counter so the convergence barrier covers queued batches.
        let lanes = emission::EmissionLanes::spawn(
            ds.clone(),
            std::env::var("ELECTRIC_IVM_EMIT_LANES").ok().and_then(|v| v.parse().ok()).unwrap_or(8),
            pending_flips.clone(),
        );
        subqueries.try_lock().expect("fresh registry").set_lanes(lanes);
        spawn_flip_propagator(subqueries.clone(), flip_rx, pending_flips.clone(), trace_tx.clone());
        let (catalog_tx, catalog_rx) = mpsc::unbounded_channel();
        spawn_catalog_writer(ds.clone(), catalog_rx);
        Engine {
            ds,
            state: Arc::new(Mutex::new(EngineState {
                tables: HashMap::new(),
                sequencer: None,
                shapes: HashMap::new(),
                next_shape_id: 1,
                feed_by_sig: HashMap::new(),
                feed_shares: HashMap::new(),
                circuit_placement: HashMap::new(),
            })),
            pg_url,
            repl_lsn: Arc::new(std::sync::Mutex::new("0/0".to_string())),
            repl_sync: Arc::new(std::sync::atomic::AtomicI64::new(0)),
            replicator_started: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            // Library mode: no Postgres to wait on, so report `active` immediately.
            health: Arc::new(std::sync::atomic::AtomicU8::new(HEALTH_ACTIVE)),
            subqueries,
            trace_tx,
            flip_tx,
            pending_flips,
            tables_shared: Arc::new(std::sync::RwLock::new(HashMap::new())),
            catalog_tx,
            seq_start: Arc::new(std::sync::Mutex::new("-1".to_string())),
            lives: Arc::new(std::sync::Mutex::new(HashMap::new())),
            retention: Arc::new(RetentionConfig::from_env()),
            retention_started: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            dbsp_cfg: Arc::new(std::sync::Mutex::new(None)),
            arrangements: Arc::new(std::sync::Mutex::new(None)),
            arr_gates: Arc::new(std::sync::RwLock::new(HashMap::new())),
        }
    }

    /// Configure the always-on dbsp arrangement layer (call before
    /// [`setup_postgres`](Self::setup_postgres), which builds and seeds it).
    pub fn set_dbsp_config(&self, cfg: crate::config::DbspConfig) {
        *self.dbsp_cfg.lock().unwrap() = Some(cfg);
    }

    /// Start the dbsp counts layer and seed it, when configured. Seeds each counts pipeline
    /// from one group-aggregated Postgres snapshot per table (capturing the gate that fences the live
    /// feed); restored state skips seeding — the sequencer replays the change-log gap instead.
    async fn maybe_start_arrangements(&self, schemas: &HashMap<String, TableSchema>) -> Result<()> {
        let Some(cfg) = self.dbsp_cfg.lock().unwrap().clone() else { return Ok(()) };
        if !cfg.indexes.is_empty() {
            tracing::warn!(
                "ELECTRIC_IVM_DBSP_INDEXES is deprecated and ignored: row data lives in Postgres \
                 (lookups are pooled queries); the circuit holds counts pipelines only"
            );
        }
        if cfg.cache_mib.is_some() || cfg.max_rss_bytes.is_some() {
            tracing::warn!(
                "ELECTRIC_IVM_DBSP_{{CACHE_MIB,MIN_STORAGE_KB,MAX_RSS_MB,CHECKPOINT_SECS,DIR}} are \
                 deprecated no-ops: the circuit is in-memory counts only (no storage layer)"
            );
        }
        let mut counts: Vec<crate::arrangements::CountSpec> = Vec::new();
        for (t, cols) in &cfg.counts {
            let Some(ts) = schemas.get(t) else {
                tracing::warn!("ELECTRIC_IVM_DBSP_COUNTS: unknown table {t}; skipping");
                continue;
            };
            let resolved: Option<Vec<usize>> =
                cols.iter().map(|c| ts.index.get(c).copied()).collect();
            match resolved {
                Some(group_cols) => {
                    counts.push(crate::arrangements::CountSpec { table: t.clone(), group_cols })
                }
                None => tracing::warn!("ELECTRIC_IVM_DBSP_COUNTS: unknown column in {t}:{cols:?}; skipping"),
            }
        }
        if counts.is_empty() {
            return Ok(()); // nothing for the circuit to maintain
        }
        let arr = crate::arrangements::Arrangements::start(counts.clone())?;
        // Seed each counts pipeline from ONE group-aggregated query per table — O(groups),
        // not O(rows); row data stays in Postgres. State is in-memory only, so this runs on
        // every boot; the seed's SnapshotGate fences change-log replay exactly like a shape
        // backfill.
        let url = self.pg_url.clone().context("counts pipelines need a pg_url to seed")?;
        let client = crate::pg::connect(&url).await?;
        let mut gates = HashMap::new();
        for spec in &counts {
            let ts = schemas.get(&spec.table).expect("resolved above");
            let (groups, gate) =
                crate::pg::backfill_group_counts(&client, ts, &spec.group_cols).await?;
            let total = groups.len();
            arr.seed_groups(&spec.table, groups).await?;
            gates.insert(spec.table.clone(), gate);
            arr.finish_seed(&spec.table);
            tracing::info!("arrangements: seeded counts for '{}' ({total} groups)", spec.table);
        }
        *self.arr_gates.write().unwrap() = gates;
        *self.arrangements.lock().unwrap() = Some(arr);
        Ok(())
    }

    /// Sender for the per-envelope trace broadcast — subscribe via `.subscribe()` (used by the
    /// `/trace` SSE endpoint); tailers publish through a clone.
    pub fn trace_sender(&self) -> tokio::sync::broadcast::Sender<Arc<String>> {
        self.trace_tx.clone()
    }

    /// Flip batches enqueued but not yet propagated (convergence-barrier term; see `flip_tx`).
    pub fn pending_flips(&self) -> i64 {
        self.pending_flips.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn subquery_handle(&self) -> SubqueryHandle {
        SubqueryHandle {
            registry: self.subqueries.clone(),
            flip_tx: self.flip_tx.clone(),
            pending_flips: self.pending_flips.clone(),
        }
    }

    /// Get (or spawn) the single sequencer task consuming the global change log.
    fn ensure_sequencer<'a>(&self, st: &'a mut EngineState) -> &'a SequencerHandle {
        if st.sequencer.is_none() {
            let start = self.seq_start.lock().unwrap().clone();
            st.sequencer = Some(spawn_sequencer(
                self.ds.clone(),
                self.tables_shared.clone(),
                start,
                self.catalog_tx.clone(),
                self.subquery_handle(),
                self.trace_tx.clone(),
                self.arrangements.lock().unwrap().clone(),
                self.arr_gates.read().unwrap().clone(),
            ));
        }
        st.sequencer.as_ref().expect("sequencer just spawned")
    }

    /// Number of tables with a known schema (tables being tailed) — for the boot `consumers_ready` metric.
    pub async fn table_count(&self) -> usize {
        self.state.lock().await.tables.len()
    }

    /// The `/v1/health` status string: `waiting` | `starting` | `active` (exact, no whitespace).
    pub fn health_status(&self) -> &'static str {
        match self.health.load(std::sync::atomic::Ordering::Relaxed) {
            HEALTH_WAITING => "waiting",
            HEALTH_STARTING => "starting",
            _ => "active",
        }
    }

    /// Introspect the configured tables from Postgres, set `REPLICA IDENTITY FULL`, create the
    /// replication slot, register the schema, and start the replication ingestor. Idempotent: a second
    /// call re-introspects but will NOT spawn a second ingestor (two ingestors would fight for the slot).
    pub async fn setup_postgres(&self, tables: &[String], slot: &str) -> Result<()> {
        let url = self.pg_url.clone().context("setup_postgres called without a pg_url")?;
        let client = crate::pg::connect(&url).await?;
        // Postgres connection established: leave `waiting`, enter `starting` (introspection + slot +
        // ingest spawn still ahead). `/v1/health` reports 202 until the ingest loop is running.
        self.health.store(HEALTH_STARTING, std::sync::atomic::Ordering::Relaxed);
        // `*` (or empty) => introspect every public table with a PK (set isn't known up front).
        let discovered;
        let tables: &[String] = if tables.is_empty() || tables == ["*".to_string()] {
            discovered = crate::pg::list_tables(&client).await?;
            tracing::info!("introspect-all: {} tables", discovered.len());
            &discovered
        } else {
            tables
        };
        let mut compiled = HashMap::new();
        for t in tables {
            let def = crate::pg::introspect(&client, t).await?;
            let ts = TableSchema::from_def(t, &def)?;
            crate::pg::ensure_replica_identity_full(&client, t).await?;
            compiled.insert(t.clone(), ts);
        }
        crate::pg::ensure_slot(&client, slot).await?;
        let publication = format!("{slot}_pub");
        crate::pg::ensure_publication(&client, &publication).await?;
        self.ds.ensure_stream(crate::CHANGES_STREAM).await?;
        *self.tables_shared.write().unwrap() = compiled.clone();
        self.state.lock().await.tables = compiled.clone();
        self.subqueries.lock().await.set_schemas(Arc::new(compiled.clone()));
        // Replay the durable shape catalog (restores shapes + the change-log replay offset), then
        // start the sequencer from the restored position. Runs before the ingestor so the restored
        // routing sees every replayed change.
        // Start (and seed or restore) the dbsp arrangement layer BEFORE the catalog restore:
        // the restore spawns the sequencer (which captures the handle + seed gates) and may
        // re-register circuit-served shapes, both of which need the layer up. A failure here
        // degrades to Postgres query-backs (the engine still runs), it does not abort boot.
        if let Err(e) = self.maybe_start_arrangements(&compiled).await {
            tracing::error!("dbsp arrangements failed to start (falling back to Postgres): {e:#}");
        }
        if let Err(e) = self.restore_catalog(&compiled).await {
            tracing::error!("catalog restore failed (continuing empty): {e:#}");
        }
        {
            let mut st = self.state.lock().await;
            self.ensure_sequencer(&mut st);
        }
        // Spawn the ingestor at most once, even if setup_postgres is called again.
        if self.replicator_started.swap(true, std::sync::atomic::Ordering::SeqCst) {
            tracing::warn!("setup_postgres called again; ingestor already running, not spawning another");
            self.health.store(HEALTH_ACTIVE, std::sync::atomic::Ordering::Relaxed);
            return Ok(());
        }
        tokio::spawn(crate::replication::run(
            url,
            slot.to_string(),
            publication,
            self.ds.clone(),
            Arc::new(compiled),
            self.repl_lsn.clone(),
            self.repl_sync.clone(),
        ));
        // Introspection + slot + ingest loop are up: report `active` (200 on `/v1/health`).
        self.health.store(HEALTH_ACTIVE, std::sync::atomic::Ordering::Relaxed);
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
        self.ds.ensure_stream(crate::CHANGES_STREAM).await?;
        self.subqueries.lock().await.set_schemas(Arc::new(compiled.clone()));
        *self.tables_shared.write().unwrap() = compiled.clone();
        {
            let mut st = self.state.lock().await;
            st.tables = compiled;
            self.ensure_sequencer(&mut st);
        }
        Ok(())
    }

    /// Run a one-shot **subset query** (the non-materialized counterpart to a shape): a single
    /// `SELECT … WHERE … ORDER BY … LIMIT … OFFSET …` against Postgres, returning the projected page
    /// rows (as JSON) + the snapshot LSN. Creates no shape, no stream, no live state — paging never
    /// becomes server-side range state, so a change can never fan out across ranges. The caller follows
    /// the live tail separately (a base-predicate feed) to keep the page live.
    pub async fn query_subset(
        &self,
        table: &str,
        where_: Option<PredicateJson>,
        columns: Option<Vec<String>>,
        order_by: Option<(String, bool)>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<(Vec<serde_json::Value>, String)> {
        let (ts, schemas) = {
            let st = self.state.lock().await;
            let ts = st.tables.get(table).cloned().ok_or_else(|| anyhow::anyhow!("unknown table '{table}'"))?;
            // Clone the table schemas so the subquery SQL emitter can cast each leaf's param to its
            // column's native Postgres type (query_subset is one-shot; the clone is off the hot path).
            (ts, st.tables.clone())
        };
        let out_cols = resolve_columns(&ts, columns)?;
        let order = match order_by {
            Some((col, desc)) => Some((ts.column_index(&col)?, desc)),
            None => None,
        };
        // Subquery predicates are evaluated natively by Postgres in the one-shot query-back (no engine
        // subquery state needed for a non-live page); other predicates use the compiled-form emitter.
        let where_sql = match where_.as_ref() {
            Some(p) if crate::subquery::predicate_has_subquery(p) => {
                Some(crate::sql::predicate_json_to_sql(p, 1, &schemas, table))
            }
            Some(p) => {
                let cp = CompiledPredicate::compile_opt(Some(p), &ts)?;
                crate::sql::predicate_to_sql(&cp, &ts)
            }
            None => None,
        };
        let url = self.pg_url.clone().context("query_subset requires postgres mode")?;
        let client = crate::pg::pool_for(&url).get().await?;
        let sq = crate::pg::query_subset_where(&client, &ts, where_sql, order, limit, offset).await?;
        let proj = out_cols.as_deref().map(Vec::as_slice);
        let rows = sq.rows.iter().map(|r| ts.row_to_json_cols(r, proj)).collect();
        Ok((rows, sq.lsn))
    }

    /// The column list + primary key of a replicated table, for the visualizer's add-row form. Reads the
    /// in-memory `TableSchema` (introspected at startup) — no Postgres round-trip.
    pub async fn table_schema_info(&self, table: &str) -> Result<TableSchemaInfo> {
        let ts = {
            let st = self.state.lock().await;
            st.tables.get(table).cloned().ok_or_else(|| anyhow::anyhow!("unknown table '{table}'"))?
        };
        let pk_set: HashSet<usize> = ts.pk_cols.iter().copied().collect();
        let columns = ts
            .columns
            .iter()
            .enumerate()
            .map(|(i, (name, ty))| TableColumnInfo {
                name: name.clone(),
                ty: col_type_str(*ty),
                pg_type: ts.pg_types.get(i).cloned().flatten(),
                pk: pk_set.contains(&i),
                has_default: ts.has_defaults.get(i).copied().unwrap_or(false),
            })
            .collect();
        let primary_key = ts.pk_cols.iter().map(|&i| ts.columns[i].0.clone()).collect();
        Ok(TableSchemaInfo { table: ts.name.clone(), columns, primary_key })
    }

    /// Insert one row into a replicated table's Postgres relation, so the change is captured by logical
    /// replication and flows through the pipeline (backing the visualizer's add-row action). `values`
    /// maps column name → value; only known columns are accepted (unknown ⇒ error), omitted columns take
    /// their Postgres default / NULL. Identifiers are quoted and values are **bound parameters** cast to
    /// each column's native type — no string-concatenated SQL.
    pub async fn insert_row(
        &self,
        table: &str,
        values: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<serde_json::Value> {
        let ts = {
            let st = self.state.lock().await;
            st.tables.get(table).cloned().ok_or_else(|| anyhow::anyhow!("unknown table '{table}'"))?
        };
        if values.is_empty() {
            bail!("no columns provided");
        }
        let mut cols: Vec<String> = Vec::with_capacity(values.len());
        let mut placeholders: Vec<String> = Vec::with_capacity(values.len());
        let mut params: Vec<String> = Vec::new();
        for (col, val) in values {
            // Reject unknown columns (also closes the identifier-injection surface: only catalog columns
            // are ever emitted, each independently quoted).
            if !ts.index.contains_key(col) {
                bail!("unknown column '{col}' on table '{table}'");
            }
            cols.push(crate::pg::quote_ident(col));
            if val.is_null() {
                placeholders.push("NULL".to_string());
                continue;
            }
            // Bind the value as a text parameter, then cast it to the column's native Postgres type
            // (uuid/int8/bool/timestamptz/…). The leading `::text` pins the parameter's inferred type to
            // text so any value serializes as a string; the second cast converts it to the column type
            // (a bare `$n::int8` would instead make Postgres infer the param itself as int8 and reject a
            // String). A JSON string binds its contents; other scalars bind their compact text form.
            let n = params.len() + 1;
            let placeholder = match ts.pg_type_of(col) {
                Some(t) => format!("${n}::text::{}", crate::pg::quote_ident(t)),
                None => format!("${n}::text"),
            };
            placeholders.push(placeholder);
            let s = match val {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            params.push(s);
        }
        let sql = format!(
            "insert into {} ({}) values ({})",
            crate::pg::quote_ident(table),
            cols.join(", "),
            placeholders.join(", "),
        );
        let url = self.pg_url.clone().context("insert_row requires postgres mode")?;
        let client = crate::pg::pool_for(&url).get().await?;
        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            params.iter().map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
        let n = client.execute(&sql, &param_refs).await.with_context(|| format!("insert into {table}"))?;
        Ok(serde_json::json!({ "ok": true, "inserted": n }))
    }

    /// Delete rows from a replicated table's Postgres relation by primary key, so the deletes are
    /// captured by logical replication and flow through the pipeline (backing the visualizer's
    /// delete-rows action). `keys` holds one map per row: primary-key column → value. Every key must
    /// supply exactly the table's primary-key columns, non-NULL. All rows go in one parameterized
    /// statement (identifiers quoted, values bound and cast to the columns' native types), so a
    /// multi-row delete is a single transaction — one replication batch, one pipeline delta.
    pub async fn delete_rows(
        &self,
        table: &str,
        keys: &[serde_json::Map<String, serde_json::Value>],
    ) -> Result<serde_json::Value> {
        const MAX_KEYS: usize = 1000;
        let ts = {
            let st = self.state.lock().await;
            st.tables.get(table).cloned().ok_or_else(|| anyhow::anyhow!("unknown table '{table}'"))?
        };
        if keys.is_empty() {
            bail!("no keys provided");
        }
        if keys.len() > MAX_KEYS {
            bail!("too many keys ({}); at most {MAX_KEYS} rows per delete", keys.len());
        }
        if ts.pk_cols.is_empty() {
            bail!("table '{table}' has no primary key");
        }
        let pk_names: Vec<&str> = ts.pk_cols.iter().map(|&i| ts.columns[i].0.as_str()).collect();
        let mut clauses: Vec<String> = Vec::with_capacity(keys.len());
        let mut params: Vec<String> = Vec::with_capacity(keys.len() * pk_names.len());
        for key in keys {
            // Only primary-key columns are accepted (as with insert, this also closes the
            // identifier-injection surface: every emitted identifier comes from the catalog).
            for col in key.keys() {
                if !pk_names.contains(&col.as_str()) {
                    bail!("column '{col}' is not in table '{table}''s primary key");
                }
            }
            let mut conj: Vec<String> = Vec::with_capacity(pk_names.len());
            for &col in &pk_names {
                let val = key
                    .get(col)
                    .ok_or_else(|| anyhow::anyhow!("key is missing primary-key column '{col}'"))?;
                if val.is_null() {
                    bail!("primary-key column '{col}' must not be NULL");
                }
                // Same bind-as-text-then-cast scheme as insert_row (see the comment there).
                let n = params.len() + 1;
                let placeholder = match ts.pg_type_of(col) {
                    Some(t) => format!("${n}::text::{}", crate::pg::quote_ident(t)),
                    None => format!("${n}::text"),
                };
                conj.push(format!("{} = {placeholder}", crate::pg::quote_ident(col)));
                params.push(match val {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                });
            }
            clauses.push(format!("({})", conj.join(" and ")));
        }
        let sql =
            format!("delete from {} where {}", crate::pg::quote_ident(table), clauses.join(" or "));
        let url = self.pg_url.clone().context("delete_rows requires postgres mode")?;
        let client = crate::pg::pool_for(&url).get().await?;
        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            params.iter().map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
        let n = client.execute(&sql, &param_refs).await.with_context(|| format!("delete from {table}"))?;
        Ok(serde_json::json!({ "ok": true, "deleted": n }))
    }

    /// Number of maintained subquery nodes (for the sharing-topology introspection endpoint).
    pub async fn subquery_node_count(&self) -> usize {
        self.subqueries.lock().await.node_count()
    }

    /// Per-node subquery topology (signature, inner table, distinct values, refcount).
    pub async fn subquery_stats(&self) -> Vec<crate::subquery::NodeStat> {
        self.subqueries.lock().await.stats()
    }

    /// The schema for `table`, if known (used by the Electric-protocol adapter for the schema header and
    /// value encoding).
    pub async fn table_schema(&self, table: &str) -> Option<TableSchema> {
        self.state.lock().await.tables.get(table).cloned()
    }

    /// Read a shape's durable stream (catch-up or long-poll live) — used by the Electric adapter to turn
    /// the engine's shape output into Electric `/v1/shape` change messages.
    pub async fn read_shape_stream(&self, path: &str, offset: &str, live: bool) -> Result<crate::ds::ReadResult> {
        // A data read is a full retention touch: reactivate a dormant shape before reading (so a
        // parked stream is never served stale) and refresh `last_read`. `ensure_active` is a cheap
        // lifecycle-map check when the shape is active (the common case).
        self.ensure_active(sid_of_path(path)).await?;
        self.ds.read(path, offset, live).await
    }

    /// Engine-internal cardinalities for the memory probe — the structures whose growth drives RSS:
    /// registered shapes, per-table tailers, shared **family circuits** (the M× join-trace amplifier:
    /// each holds the base table once), standalone per-shape circuits, and the subquery registry's
    /// nodes/contributor-pks. Read directly from in-memory state (cheap; no tailer round-trip, no
    /// byte-walk, no sequencer round-trip).
    ///
    /// This is the ONLY cardinality path the 500ms background sampler (`mem::spawn_sampler`) is
    /// allowed to call. It deliberately never touches `HeapSize::heap_bytes` or sends
    /// `SequencerCmd::MemBytes` — every `bytes_*` field on the returned [`crate::mem::Cardinalities`]
    /// is left at its `Default` zero. Byte-level self-accounting (Phase 0 of the memory-reduction
    /// effort) lives in the sibling [`Self::mem_bytes`], called only by `GET /memory`; see its doc
    /// comment for why that split exists (a prior regression: this method used to do the walk
    /// inline, which meant a ~100MB recursive walk + a `MemBytes` sequencer round-trip ran twice a
    /// second at 50k+ shapes).
    pub async fn mem_cardinalities(&self) -> crate::mem::Cardinalities {
        let (shapes, tailers, tables, families, family_shapes, standalone) = {
            let st = self.state.lock().await;
            let mut families = 0usize;
            let mut family_shapes = 0usize;
            let mut standalone = 0usize;
            let mut tables_with_execs = 0usize;
            if let Some(seq) = st.sequencer.as_ref()
                && let Ok(per_table) = seq.stats.lock()
            {
                tables_with_execs = per_table.len();
                for s in per_table.values() {
                    families += s.families.len();
                    family_shapes += s.families.iter().map(|f| f.shapes).sum::<usize>();
                    standalone += s.standalone;
                }
            }
            (st.shapes.len(), tables_with_execs, st.tables.len(), families, family_shapes, standalone)
        };
        let sq = {
            let reg = self.subqueries.lock().await;
            reg.mem_totals()
        };
        let shapes_dormant = self
            .lives
            .lock()
            .unwrap()
            .values()
            .filter(|l| matches!(l.state, LifeState::Dormant { .. }))
            .count();
        crate::mem::Cardinalities {
            shapes,
            shapes_dormant,
            tailers,
            tables,
            families,
            family_shapes,
            standalone,
            subquery_nodes: sq.nodes,
            subquery_contributors: sq.contributors,
            subquery_distinct_values: sq.distinct,
            subquery_shapes: sq.shapes,
            subquery_edges: sq.edges,
            subquery_feed_entries: sq.feed_entries,
            ..Default::default()
        }
    }

    /// On-demand byte-level self-accounting (Phase 0 of the memory-reduction effort): a
    /// [`crate::heap_size::HeapSize`] lower-bound owned-heap estimate per major structure. These
    /// are LOWER BOUNDS (owned heap, not allocator slack) — the gap vs. `process.rss_bytes` is the
    /// allocator/pinning term this phase is instrumenting to measure.
    ///
    /// Expensive: locks engine state, round-trips a one-off `SequencerCmd::MemBytes` to the
    /// sequencer task (mirroring the `DumpNode` command's pattern — see `dump_node` below), locks
    /// the subquery registry, and walks roughly the engine's entire owned heap (~100MB at 50k
    /// shapes). Call this ONLY from the `GET /memory` HTTP handler — never from the 500ms
    /// background sampler (`mem::spawn_sampler`), which calls `mem_cardinalities` instead. Mixing
    /// this into the sampler's path was exactly the prior regression (+41%/+52% peak/steady RSS at
    /// 100k subscriptions from twice-a-second byte walks); see `mem::spawn_sampler`'s doc comment.
    ///
    /// `bytes_executors` (standalone shapes + their conjunct index, family routers, aggregate
    /// folds + their index) is the one term this method cannot read out of already-published
    /// state: those structures are privately owned by the sequencer task's `execs` map, never
    /// exposed through a shared mutex (unlike `stats`/`node_states`, which are republished after
    /// every batch specifically so other tasks can read them cheaply). Walking them for real bytes
    /// is not cheap enough to piggyback on every batch (see `sequencer::publish_all`/`stats_of`),
    /// so instead this method round-trips the one-off `SequencerCmd::MemBytes` so the byte-walk
    /// itself only ever runs on this on-demand path, never per batch.
    pub async fn mem_bytes(&self) -> crate::mem::HeapBytes {
        let (bytes_shape_records, cmd_tx) = {
            let st = self.state.lock().await;
            (st.shapes.heap_bytes(), st.sequencer.as_ref().map(|seq| seq.cmd_tx.clone()))
        };
        // Byte-walk every table's live executor state: ask the sequencer task directly, since it
        // privately owns `execs`.
        let bytes_executors = match cmd_tx {
            Some(tx) => {
                let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                if tx.send(SequencerCmd::MemBytes { resp: resp_tx }).is_ok() {
                    resp_rx.await.unwrap_or(0)
                } else {
                    0
                }
            }
            None => 0,
        };
        let (circuit_bytes, bytes_feed_sets, bytes_subquery_registry, bytes_pk_dict) = {
            let reg = self.subqueries.lock().await;
            (reg.circuit_bytes(), reg.feed_sets_bytes(), reg.heap_bytes(), reg.pk_dict_bytes())
        };
        let bytes_retention = self.lives.lock().unwrap().heap_bytes();
        let bytes_electric_adapter = crate::electric::ttl_registry_heap_bytes().await;
        crate::mem::HeapBytes {
            bytes_shape_records,
            bytes_executors,
            bytes_retention,
            bytes_subquery_registry,
            bytes_membership_circuit: circuit_bytes.total_bytes(),
            bytes_circuit_integral: circuit_bytes.integral_bytes(),
            bytes_circuit_snapshots: circuit_bytes.snapshot_bytes(),
            bytes_feed_sets,
            bytes_pk_dict,
            bytes_electric_adapter,
        }
    }

    pub async fn get_shape(&self, id: &str) -> Option<ShapeRecord> {
        self.state.lock().await.shapes.get(id).cloned()
    }

    /// The change-log offset up to which the sequencer has processed (global — all tables share
    /// the single ordered log), or `None` if the sequencer is not running yet.
    pub async fn table_offset(&self, _table: &str) -> Option<String> {
        let st = self.state.lock().await;
        st.sequencer.as_ref().map(|s| s.processed.lock().unwrap().clone())
    }

    /// The table's current circuit topology (shared families + standalone count), or `None` if no
    /// tailer exists.
    pub async fn table_stats(&self, table: &str) -> Option<TableStats> {
        let st = self.state.lock().await;
        st.sequencer.as_ref().and_then(|s| s.stats.lock().unwrap().get(table).cloned())
    }

}
