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
use crate::metrics::{Timer, metrics};
use crate::predicate::{CompiledPredicate, PredicateJson};
use crate::schema::{Schema, TableSchema, compile_schema};
use crate::retention::{EvictReason, LifeState, RetentionConfig, ShapeLife, SweepShape};
use crate::subquery::{SubqueryRegistry, predicate_has_subquery, referenced_tables};
use crate::value::{Row, Value};

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
}

/// One tailer envelope's worth of deferred subquery flips (see [`Engine::flip_tx`]).
pub(crate) struct FlipWork {
    work: std::collections::VecDeque<(crate::predicate::SubquerySig, crate::subquery::Flip)>,
    txid: Option<String>,
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

/// Spawn the engine's single flip-propagator task (one per engine: propagation order and
/// eval+append atomicity are what keep deferred moves convergent — see `subquery.rs`).
fn spawn_flip_propagator(
    registry: Arc<Mutex<SubqueryRegistry>>,
    mut rx: mpsc::UnboundedReceiver<FlipWork>,
    pending: Arc<std::sync::atomic::AtomicI64>,
    trace_tx: tokio::sync::broadcast::Sender<Arc<String>>,
) {
    tokio::spawn(async move {
        while let Some(fw) = rx.recv().await {
            if let Err(e) = crate::subquery::propagate_flips(&registry, fw.work, fw.txid, &trace_tx).await {
                tracing::error!("subquery flip propagation failed: {e:#}");
            }
            pending.fetch_sub(1, Ordering::SeqCst);
        }
    });
}

/// The engine's durable **shape catalog**: an append-only event stream replayed at boot so a
/// restart re-registers every shape itself instead of requiring a client re-registration storm.
/// Plain/routed shapes resume with passthrough gates (the change log replays everything after the
/// persisted offset; re-emission across the crash window is idempotent absolute upserts);
/// aggregates re-seed their fold from a fresh Postgres snapshot (their fresh gate then skips the
/// replayed history). Subquery shapes are NOT restorable without persisted inner-node state (a
/// fresh-seeded node cannot detect downtime flips, which would leave stale move-outs forever) —
/// they are dropped loudly at restore for clients to recreate.
const CATALOG_STREAM: &str = "meta/catalog";

/// One catalog event. `Offset` checkpoints the sequencer's processed change-log position (the
/// replay start after a restart), appended at most every ~2s.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "t", rename_all = "camelCase")]
enum CatalogEvent {
    Created { rec: ShapeRecord, sig: Option<String> },
    /// A subscriber joined a shared feed (refcount +1).
    Joined { id: String },
    /// A subscriber left a shared feed (refcount −1). With retention, reaching refcount 0 keeps
    /// the shape (it goes dormant later), so `Left` never implies teardown.
    Left { id: String },
    /// The shape went dormant: routing state dropped, stream + record retained. `resume_offset`
    /// is the change-log position its stream is complete up to; `gate` is its original
    /// backfill-snapshot fence. Restores as dormant (an improvement over the in-memory-only
    /// lifecycle: a restart no longer forgets dormant shapes).
    Dormant { id: String, resume_offset: String, gate: crate::pg::SnapshotGate },
    /// A dormant shape was reactivated (replayed + re-registered).
    Reactivated { id: String },
    Dropped { id: String },
    Offset { offset: String },
}

/// Spawn the single catalog writer: events are appended strictly in send order (senders enqueue
/// while holding the engine-state lock, so the log order matches the state-mutation order).
fn spawn_catalog_writer(ds: DsClient, mut rx: mpsc::UnboundedReceiver<CatalogEvent>) {
    tokio::spawn(async move {
        let mut ensured = false;
        while let Some(ev) = rx.recv().await {
            if !ensured {
                ensured = self::ensure_catalog(&ds).await;
            }
            let Ok(json) = serde_json::to_value(&ev) else { continue };
            if let Err(e) = ds.append_json(CATALOG_STREAM, &[json]) .await {
                tracing::error!("catalog append failed (event lost; restart may under-restore): {e:#}");
            }
        }
    });
}

async fn ensure_catalog(ds: &DsClient) -> bool {
    match ds.ensure_stream(CATALOG_STREAM).await {
        Ok(()) => true,
        Err(e) => {
            tracing::error!("catalog stream create failed: {e:#}");
            false
        }
    }
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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ShapeRecord {
    pub id: String,
    pub table: String,
    pub stream_path: String,
    /// Graph-introspection metadata (for `GET /graph` / the pipeline visualizer). Filled at creation.
    pub changes_only: bool,
    /// The shape's `where` predicate as raw JSON, for rendering the pipeline. `None` = match-all.
    pub where_json: Option<PredicateJson>,
    /// The columns this shape projects (syncs), as requested at creation. `None` = the full row (all
    /// columns). Surfaced for the visualizer so a shape's SELECT-list is visible.
    pub columns: Option<Vec<String>>,
    /// `Some(key_cols)` iff this shape is an equality template routed by a shared **family** on those
    /// columns; `None` = standalone filter or subquery.
    pub family_key: Option<Vec<String>>,
    /// True iff the predicate contains a `col IN (SELECT …)` leaf (routed via the subquery registry).
    pub is_subquery: bool,
    /// Present iff this shape is a scalar **aggregation** (maintains a running COUNT/SUM/… over `where`,
    /// not the rows). Streams a single value that updates as rows enter/leave the predicate.
    pub aggregate: Option<AggInfo>,
}

/// Aggregation descriptor carried on a shape record + `GET /graph` (for the visualizer).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AggInfo {
    pub func: AggFn,
    pub col: Option<String>,
}

// --- Pipeline-graph introspection (served at `GET /graph` for the visualizer) ---

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphShape {
    pub id: String,
    pub table: String,
    pub stream_path: String,
    pub changes_only: bool,
    #[serde(rename = "where")]
    pub where_: Option<PredicateJson>,
    /// The projected columns (SELECT-list); `null` = the full row (all columns).
    pub columns: Option<Vec<String>>,
    /// Key columns iff this shape routes via a shared equality **family**; else `null` (standalone/subquery).
    pub family_key: Option<Vec<String>>,
    pub is_subquery: bool,
    /// Present iff this shape is a scalar aggregation (COUNT/SUM/…).
    pub aggregate: Option<AggInfo>,
    /// Retention lifecycle: `active` | `deactivating` | `dormant` | `reactivating` (`None` while
    /// the record is mid-create). A dormant shape keeps its stream + record but holds no routing
    /// state — the visualizer renders it parked instead of live.
    pub state: Option<&'static str>,
}

/// A shared maintained inner-set node (`SELECT proj FROM inner WHERE …`), one per distinct subquery.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphNode {
    pub sig: String,
    pub inner_table: String,
    pub proj_col: String,
    pub distinct_values: usize,
    pub refcount: usize,
}

/// A dependency edge from a subquery node to a dependent (an outer shape, or a parent node for nesting).
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphEdge {
    pub node_sig: String,
    pub dependent_kind: String, // "shape" | "node"
    pub dependent_id: String,
    pub connecting_col: String,
    pub negated: bool,
}

/// One operator of the exploded circuit view: the engine's own decomposition of what it
/// executes per node, so the visualizer renders operators the engine declares instead of guessing.
/// `hop` binds the operator to the trace-hop id whose outcomes animate it; `state` (when present)
/// binds it to the state-summary id whose live chips it shows — the operator that actually holds
/// the state, and only that one.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpNode {
    pub id: String,
    /// `source | delta | filter | key | arrange | join | distinct | fold | project | sink`
    pub kind: String,
    /// Trace-hop / graph node id (`table:`, `filter:`, `family:`, `node:`, `shape:`).
    pub hop: String,
    /// State-summary id (`GET /state` key) when this operator is the state-bearing one.
    pub state: Option<String>,
    pub label: String,
}

/// A stream between two operators of the exploded circuit view.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpEdge {
    pub source: String,
    pub target: String,
    /// `flow` (a Z-set stream) | `state` (an arrangement feeding a join) | `subquery` (an
    /// inner-set membership dependency).
    pub kind: String,
    pub label: Option<String>,
}

/// The whole maintained pipeline at an instant: tables, shapes (with their routing placement),
/// the shared subquery node/edge DAG, and the exploded operator decomposition (`operators` /
/// `opEdges`) the circuit view renders. The visualizer derives family + subquery sharing from this.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineGraph {
    pub tables: Vec<String>,
    pub shapes: Vec<GraphShape>,
    pub subquery_nodes: Vec<GraphNode>,
    pub subquery_edges: Vec<GraphEdge>,
    pub operators: Vec<OpNode>,
    pub op_edges: Vec<OpEdge>,
}

/// One entry of a subquery node's live inner-set index.
#[derive(serde::Serialize)]
pub struct NodeValue {
    pub value: serde_json::Value,
    pub contributors: usize,
}

/// The live inner-set index of a subquery node (served at `GET /graph/node?sig=…`).
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeIndex {
    pub sig: String,
    pub distinct_values: usize,
    pub refcount: usize,
    pub values: Vec<NodeValue>,
    pub truncated: bool,
}

/// Live state summary of one pipeline node, keyed by the node's graph/trace id (`table:<t>`,
/// `filter:<sid>`, `family:<t>:<cols>`, `node:<sig>`, `shape:<sid>`). Served in bulk at
/// `GET /state`, pushed as `{"type":"state", "nodes":{…}}` events on the `/trace` channel after
/// each processed batch, and rendered by the visualizer as per-node state chips.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum NodeStateSummary {
    /// A table source: the tailer's convergence offset + envelopes processed since start.
    #[serde(rename_all = "camelCase")]
    Table { processed_offset: String, envelopes: u64 },
    /// A standalone stateless filter (σ + π): envelopes it has emitted downstream.
    #[serde(rename_all = "camelCase")]
    Filter { emitted: u64 },
    /// A shared equality router: cardinality of its routing index (distinct key tuples) and the
    /// number of shapes registered across those keys.
    #[serde(rename_all = "camelCase")]
    Family { keys: usize, shapes: usize },
    /// A shape output stream: envelopes appended to it (backfill + live).
    #[serde(rename_all = "camelCase")]
    Shape { emitted: u64 },
    /// A scalar aggregation fold: its current value and internal fold state.
    #[serde(rename_all = "camelCase")]
    Aggregate { value: serde_json::Value, count: i64, nn_count: i64, multiset_len: usize },
    /// A shared subquery inner-set arrangement: distinct values maintained + dependent refcount.
    #[serde(rename_all = "camelCase")]
    SubqueryNode { distinct_values: usize, refcount: usize },
}

/// Full per-node state snapshot (`GET /state`) — the seed the visualizer loads before applying
/// incremental `state` events from `/trace`.
#[derive(serde::Serialize)]
pub struct StateSnapshot {
    pub nodes: HashMap<String, NodeStateSummary>,
}

/// Handle to the engine's single **sequencer** task — the LSN-ordered executor consuming the
/// global `changes` stream (Electric's `ShapeLogCollector` pattern): one task processes every
/// table's changes in commit order and flushes each transaction's shape appends before the next
/// transaction, restoring per-transaction atomic emission across tables.
struct SequencerHandle {
    cmd_tx: mpsc::UnboundedSender<SequencerCmd>,
    /// Change-log offset up to which every envelope has been processed AND fanned to every shape
    /// (appends landed). A harness polls this against the change log's tail as the convergence
    /// barrier.
    processed: Arc<std::sync::Mutex<String>>,
    /// Per-table circuit topology (shared families + standalone count), for tests/observability.
    stats: Arc<std::sync::Mutex<HashMap<String, TableStats>>>,
    /// Live per-node state summaries, merged across all tables, keyed by graph node id.
    /// Republished after every processed batch and on shape add/remove; read by `GET /state`.
    node_states: Arc<std::sync::Mutex<HashMap<String, NodeStateSummary>>>,
}

/// The tables the sequencer can decode, shared with the `Engine` (which updates it on
/// `setup_postgres` / `define_schema`). A std lock: reads are brief and never held across awaits.
type SharedTables = Arc<std::sync::RwLock<HashMap<String, TableSchema>>>;

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

enum SequencerCmd {
    /// Phase 1 of shape creation: register a PENDING shape that buffers this table's deltas while
    /// the creator runs the Postgres backfill concurrently — the sequencer itself never blocks on
    /// Postgres, so one slow backfill cannot stall the whole change pipeline. Buffer registration
    /// is acknowledged BEFORE the creator takes its snapshot, so no change can fall between the
    /// snapshot and activation.
    BeginShape {
        table: String,
        shape_id: String,
        num_id: u64,
        stream_path: String,
        pred: Arc<CompiledPredicate>,
        /// Output projection (column indices to emit), or `None` for the full row.
        out_cols: Option<Arc<Vec<usize>>>,
        kind: CreateKind,
        ack: tokio::sync::oneshot::Sender<()>,
    },
    /// Phase 2: the creator's backfill snapshot is appended (plain) or carried as `agg_seed`
    /// (aggregates); drain the buffered deltas through the shape's snapshot gate and go live.
    /// `ready` mirrors the old add-shape handshake: `Ok(())` once the shape is live and its
    /// snapshot + gated buffer are on the stream, `Err(reason)` otherwise.
    ActivateShape {
        table: String,
        shape_id: String,
        gate: crate::pg::SnapshotGate,
        /// Backfill rows for seeding an aggregate's fold (empty for plain shapes — the creator
        /// already appended their snapshot envelopes).
        agg_seed: Vec<Row>,
        /// Snapshot envelopes the creator appended (seeds the shape's emit counter).
        emitted_seed: u64,
        ready: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    },
    /// Creation failed after `BeginShape`: drop the pending buffer.
    AbortShape { table: String, shape_id: String },
    /// Retention: unregister a plain row shape's routing and hand back its resume state — the
    /// sequencer's fully-processed change-log offset (the batch preceding this command was fully
    /// fanned out + flushed, so the shape's stream is complete up to here) and the shape's
    /// backfill-snapshot gate. `None` if the shape is unknown (or an aggregate — not parkable).
    DeactivateShape {
        table: String,
        shape_id: String,
        resp: tokio::sync::oneshot::Sender<Option<(String, crate::pg::SnapshotGate)>>,
    },
    RemoveShape { table: String, shape_id: String },
    /// Dump the full internal state of one node (`family:<t>:<cols>` → the routing index
    /// contents; an aggregate `shape:<sid>` → the fold internals incl. the MIN/MAX multiset).
    /// `None` if the node id is unknown. Serves `GET /state/node`.
    DumpNode { table: String, node_id: String, resp: tokio::sync::oneshot::Sender<Option<serde_json::Value>> },
}

/// What kind of shape a pending creation becomes at activation.
#[derive(Clone)]
enum CreateKind {
    Plain,
    Aggregate { func: AggFn, col: Option<usize> },
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
        }
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

    /// `share`: when true, an identical existing shape (same table, canonical predicate, and columns) is
    /// joined by ref-count instead of creating a second stream — so N app clients subscribing to the same
    /// reference shape (e.g. `project_members WHERE user_id = me`) share one maintained output. The
    /// Electric `/v1/shape` path passes `false`: it keys per-request live state by shape id, so each
    /// request needs its own handle.
    pub async fn create_shape(
        &self,
        table: &str,
        where_: Option<PredicateJson>,
        columns: Option<Vec<String>>,
        changes_only: bool,
        share: bool,
    ) -> Result<ShapeRecord> {
        // Whole shape-creation timer (backfill + registration); emitted by the creator on success only
        // (joiners return early before this fires) as `create_snapshot_task.stop.duration`.
        let created_at = std::time::Instant::now();
        let mut st = self.state.lock().await;
        let ts = match st.tables.get(table) {
            Some(ts) => ts.clone(),
            None => bail!("unknown table '{table}'"),
        };
        let col_names = columns.clone();
        let out_cols = resolve_columns(&ts, columns)?;

        // Shape sharing: an identical shape (subset feed, materialized, OR subquery) that already exists
        // is joined (ref-count++), returning the same stream — no second stream, no per-subscriber append
        // fan-out. Subquery shapes share their inner-set nodes in the registry regardless; sharing the
        // *outer* shape here collapses identical subquery shapes fully.
        let feed_sig = if share { Some(shape_signature(table, &where_, &out_cols, changes_only)) } else { None };
        if let Some(sig) = &feed_sig {
            if let Some(existing_id) = st.feed_by_sig.get(sig).cloned() {
                if let Some(rec) = st.shapes.get(&existing_id).cloned() {
                    let share = st.feed_shares.get_mut(&existing_id).expect("share entry for live feed");
                    share.refcount += 1;
                let _ = self.catalog_tx.send(CatalogEvent::Joined { id: existing_id.clone() });
                    let ready = share.ready.clone();
                    // Release the lock, then wait for the creator's backfill to land: a joiner must not
                    // see a stream whose snapshot isn't readable yet, and must surface (not mask) a
                    // failed creation.
                    drop(st);
                    if let Err(e) = await_share_ready(ready, &existing_id).await {
                        // The failed creator already removed the share entries; undo nothing.
                        return Err(e);
                    }
                    // A rejoin is a touch: if the shape went dormant since the last subscriber
                    // left, reactivate it (change-log replay) before handing out the stream.
                    if let Err(e) = self.ensure_active(&existing_id).await {
                        // Roll the failed join back so the dead subscription doesn't pin the shape.
                        self.release_shape(&existing_id).await;
                        return Err(e);
                    }
                    return Ok(rec);
                }
            }
        }

        let num_id = st.next_shape_id;
        let id = format!("s{num_id}");
        st.next_shape_id += 1;
        let stream_path = format!("shape/{id}");
        self.ds.ensure_stream(&stream_path).await?;

        // Subquery shapes (`col IN (SELECT …)`) are maintained by the cross-table registry, not by a
        // tailer's local routing. Ensure a tailer exists for the outer table AND every referenced inner
        // table (so their deltas reach the registry), then register + backfill via the registry.
        if where_.as_ref().is_some_and(predicate_has_subquery) {
            let where_json = where_.expect("subquery predicate present");
            let mut tables = referenced_tables(&where_json);
            tables.push(table.to_string());
            for t in &tables {
                if !st.tables.contains_key(t) {
                    bail!("unknown table '{t}' referenced by subquery");
                }
            }
            // The sequencer feeds every table's deltas to the registry; just make sure it runs.
            self.ensure_sequencer(&mut st);
            let rec = ShapeRecord {
                id: id.clone(),
                table: table.to_string(),
                stream_path: stream_path.clone(),
                changes_only,
                where_json: Some(where_json.clone()),
                columns: col_names.clone(),
                family_key: None,
                is_subquery: true,
                aggregate: None,
            };
            st.shapes.insert(id.clone(), rec.clone());
            let _ = self.catalog_tx.send(CatalogEvent::Created { rec: rec.clone(), sig: feed_sig.clone() });
            self.lives.lock().unwrap().insert(id.clone(), ShapeLife::active());
            self.ensure_retention_sweeper();
            // Register this (first) subquery shape so later identical ones join it by ref-count.
            // Joiners wait on `ready_tx` — the shape isn't live until the registry has seeded its
            // nodes and backfilled the stream.
            let (ready_tx, ready_rx) = tokio::sync::watch::channel(None);
            if let Some(sig) = feed_sig {
                st.feed_by_sig.insert(sig.clone(), id.clone());
                st.feed_shares.insert(id.clone(), FeedShare { sig, refcount: 1, ready: ready_rx });
            }
            // Release the engine-state lock before the registry's PG backfill (so offset polling etc.
            // aren't blocked); the registry has its own lock.
            drop(st);
            let res = self
                .subqueries
                .lock()
                .await
                .create_subquery_shape(&id, table, &stream_path, &where_json, out_cols, changes_only)
                .await;
            match res {
                Ok(()) => {
                    let _ = ready_tx.send(Some(true));
                    trace_lifecycle(
                        &self.trace_tx,
                        crate::trace::GraphLifecycle::ShapeAdded { shape: id, table: table.to_string() },
                    );
                    crate::statsd::create_snapshot_task(created_at.elapsed());
                    return Ok(rec);
                }
                Err(e) => {
                    // Registration failed (the registry rolled its own state back). Remove the shape
                    // record + share entries so later identical creates don't join a dead stream, and
                    // wake any joiners with the failure.
                    let mut st = self.state.lock().await;
                    st.shapes.remove(&id);
                    let _ = self.catalog_tx.send(CatalogEvent::Dropped { id: id.clone() });
                    if let Some(share) = st.feed_shares.remove(&id) {
                        st.feed_by_sig.remove(&share.sig);
                    }
                    drop(st);
                    let _ = ready_tx.send(Some(false));
                    let _ = self.ds.delete_stream(&stream_path).await;
                    return Err(e);
                }
            }
        }

        let pred = Arc::new(CompiledPredicate::compile_opt(where_.as_ref(), &ts)?);
        // Family placement (for graph introspection): an equality template routes by these key columns
        // via a shared family; otherwise it's a standalone filter.
        let family_key = pred
            .equality_template()
            .map(|pairs| pairs.iter().map(|(i, _)| ts.columns[*i].0.clone()).collect::<Vec<_>>());

        let cmd_tx = self.ensure_sequencer(&mut st).cmd_tx.clone();
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        cmd_tx
            .send(SequencerCmd::BeginShape {
                table: table.to_string(),
                shape_id: id.clone(),
                num_id,
                stream_path: stream_path.clone(),
                pred: pred.clone(),
                out_cols: out_cols.clone(),
                kind: CreateKind::Plain,
                ack: ack_tx,
            })
            .map_err(|_| anyhow::anyhow!("sequencer is gone"))?;

        let rec = ShapeRecord {
            id: id.clone(),
            table: table.to_string(),
            stream_path,
            changes_only,
            where_json: where_.clone(),
            columns: col_names,
            family_key,
            is_subquery: false,
            aggregate: None,
        };
        st.shapes.insert(id.clone(), rec.clone());
        let _ = self.catalog_tx.send(CatalogEvent::Created { rec: rec.clone(), sig: feed_sig.clone() });
        self.lives.lock().unwrap().insert(id.clone(), ShapeLife::active());
        self.ensure_retention_sweeper();
        // Register the (first) shared feed so later identical subset feeds join it. Joiners wait on
        // `share_tx` for the backfill outcome.
        let (share_tx, share_rx) = tokio::sync::watch::channel(None);
        if let Some(sig) = feed_sig {
            st.feed_by_sig.insert(sig.clone(), id.clone());
            st.feed_shares.insert(id.clone(), FeedShare { sig, refcount: 1, ready: share_rx });
        }
        // Release the engine-state lock, then run the two-phase backfill+activate so the shape's
        // snapshot is readable when we return (the Electric adapter folds the stream immediately).
        // The sequencer keeps processing all tables meanwhile, buffering this shape's deltas.
        drop(st);
        let outcome = backfill_and_activate(
            &self.ds, &self.pg_url, &cmd_tx, &ts, table, &id, &rec.stream_path, &pred,
            out_cols.as_ref(), changes_only, false, ack_rx,
        )
        .await;
        match outcome {
            Ok(()) => {
                let _ = share_tx.send(Some(true));
                trace_lifecycle(
                    &self.trace_tx,
                    crate::trace::GraphLifecycle::ShapeAdded { shape: rec.id.clone(), table: rec.table.clone() },
                );
                crate::statsd::create_snapshot_task(created_at.elapsed());
                Ok(rec)
            }
            Err(e) => {
                // Backfill/registration failed: remove the record + share entries (no zombie shape a
                // later identical create would join) and surface the error to the caller.
                let mut st = self.state.lock().await;
                st.shapes.remove(&id);
                let _ = self.catalog_tx.send(CatalogEvent::Dropped { id: id.clone() });
                if let Some(share) = st.feed_shares.remove(&id) {
                    st.feed_by_sig.remove(&share.sig);
                }
                if let Some(seq) = st.sequencer.as_ref() {
                    let _ = seq
                        .cmd_tx
                        .send(SequencerCmd::RemoveShape { table: rec.table.clone(), shape_id: id.clone() });
                }
                drop(st);
                let _ = share_tx.send(Some(false));
                let _ = self.ds.delete_stream(&rec.stream_path).await;
                bail!("shape '{id}' creation failed: {e}")
            }
        }
    }

    /// Create a scalar **aggregation** shape (COUNT/SUM/AVG/MIN/MAX over `where`), maintained
    /// incrementally. An electric-ivm extension — not part of the Electric-compatible API. Rejects
    /// subquery predicates (use a plain filter); SUM/AVG/MIN/MAX require a column.
    pub async fn create_aggregate(
        &self,
        table: &str,
        where_: Option<PredicateJson>,
        func: AggFn,
        col: Option<String>,
    ) -> Result<ShapeRecord> {
        let mut st = self.state.lock().await;
        let ts = st.tables.get(table).cloned().ok_or_else(|| anyhow::anyhow!("unknown table '{table}'"))?;
        if where_.as_ref().is_some_and(predicate_has_subquery) {
            bail!("aggregations over subquery predicates are not supported");
        }
        let col_idx = match &col {
            Some(c) => Some(ts.column_index(c)?),
            None => None,
        };
        if matches!(func, AggFn::Sum | AggFn::Avg | AggFn::Min | AggFn::Max) && col_idx.is_none() {
            bail!("aggregation {func:?} requires a column");
        }

        // Aggregate sharing: an identical aggregation (same table, predicate, function, column) is joined
        // by ref-count — one maintained fold feeds every subscriber (e.g. the same live COUNT opened by
        // many clients).
        let agg_sig = agg_signature(table, &where_, &func, col_idx);
        if let Some(existing_id) = st.feed_by_sig.get(&agg_sig).cloned() {
            if let Some(rec) = st.shapes.get(&existing_id).cloned() {
                let share = st.feed_shares.get_mut(&existing_id).expect("share entry for aggregate");
                share.refcount += 1;
                let _ = self.catalog_tx.send(CatalogEvent::Joined { id: existing_id.clone() });
                let ready = share.ready.clone();
                drop(st);
                await_share_ready(ready, &existing_id).await?;
                self.touch_shape(&existing_id); // aggregates never park, but the read is a touch
                return Ok(rec);
            }
        }

        let pred = Arc::new(CompiledPredicate::compile_opt(where_.as_ref(), &ts)?);

        let num_id = st.next_shape_id;
        let id = format!("s{num_id}");
        st.next_shape_id += 1;
        let stream_path = format!("shape/{id}");
        self.ds.ensure_stream(&stream_path).await?;

        let cmd_tx = self.ensure_sequencer(&mut st).cmd_tx.clone();
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        cmd_tx
            .send(SequencerCmd::BeginShape {
                table: table.to_string(),
                shape_id: id.clone(),
                num_id,
                stream_path: stream_path.clone(),
                pred: pred.clone(),
                out_cols: None,
                kind: CreateKind::Aggregate { func, col: col_idx },
                ack: ack_tx,
            })
            .map_err(|_| anyhow::anyhow!("sequencer is gone"))?;

        let stream_path_c = stream_path.clone();
        let rec = ShapeRecord {
            id: id.clone(),
            table: table.to_string(),
            stream_path,
            changes_only: false,
            where_json: where_,
            columns: None,
            family_key: None,
            is_subquery: false,
            aggregate: Some(AggInfo { func, col }),
        };
        st.shapes.insert(id.clone(), rec.clone());
        let _ = self.catalog_tx.send(CatalogEvent::Created { rec: rec.clone(), sig: Some(agg_sig.clone()) });
        self.lives.lock().unwrap().insert(id.clone(), ShapeLife::active());
        self.ensure_retention_sweeper();
        // Register this (first) aggregate so later identical ones join it by ref-count.
        let (share_tx, share_rx) = tokio::sync::watch::channel(None);
        st.feed_by_sig.insert(agg_sig.clone(), id.clone());
        st.feed_shares.insert(id.clone(), FeedShare { sig: agg_sig, refcount: 1, ready: share_rx });
        drop(st);
        let outcome = backfill_and_activate(
            &self.ds, &self.pg_url, &cmd_tx, &ts, table, &id, &stream_path_c, &pred,
            None, false, true, ack_rx,
        )
        .await;
        match outcome {
            Ok(()) => {
                let _ = share_tx.send(Some(true));
                trace_lifecycle(
                    &self.trace_tx,
                    crate::trace::GraphLifecycle::ShapeAdded { shape: rec.id.clone(), table: rec.table.clone() },
                );
                Ok(rec)
            }
            Err(e) => {
                let mut st = self.state.lock().await;
                st.shapes.remove(&id);
                let _ = self.catalog_tx.send(CatalogEvent::Dropped { id: id.clone() });
                if let Some(share) = st.feed_shares.remove(&id) {
                    st.feed_by_sig.remove(&share.sig);
                }
                if let Some(seq) = st.sequencer.as_ref() {
                    let _ = seq
                        .cmd_tx
                        .send(SequencerCmd::RemoveShape { table: rec.table.clone(), shape_id: id.clone() });
                }
                drop(st);
                let _ = share_tx.send(Some(false));
                let _ = self.ds.delete_stream(&rec.stream_path).await;
                bail!("aggregate '{id}' creation failed: {e}")
            }
        }
    }

    /// Snapshot the whole maintained pipeline for the visualizer: tables, every registered shape with
    /// its routing placement (family key / standalone / subquery), the shared subquery node+edge DAG,
    /// and the exploded per-operator decomposition for the circuit view.
    pub async fn graph(&self) -> EngineGraph {
        let (tables, shapes, schemas) = {
            let st = self.state.lock().await;
            // Deterministic output: a consumer diffing consecutive snapshots (the visualizer's
            // "did the structure change" check) must see byte-identical output for an unchanged
            // pipeline.
            let mut tables: Vec<String> = st.tables.keys().cloned().collect();
            tables.sort();
            let lives = self.lives.lock().unwrap();
            let life_of = |id: &str| -> Option<&'static str> {
                lives.get(id).map(|l| match l.state {
                    LifeState::Active => "active",
                    LifeState::Deactivating { .. } => "deactivating",
                    LifeState::Dormant { .. } => "dormant",
                    LifeState::Reactivating { .. } => "reactivating",
                })
            };
            let shapes: Vec<GraphShape> = st
                .shapes
                .values()
                .map(|r| GraphShape {
                    id: r.id.clone(),
                    table: r.table.clone(),
                    stream_path: r.stream_path.clone(),
                    changes_only: r.changes_only,
                    where_: r.where_json.clone(),
                    columns: r.columns.clone(),
                    family_key: r.family_key.clone(),
                    is_subquery: r.is_subquery,
                    aggregate: r.aggregate.clone(),
                    state: life_of(&r.id),
                })
                .collect();
            let mut shapes = shapes;
            shapes.sort_by_key(|s| s.id.strip_prefix('s').and_then(|n| n.parse::<u64>().ok()).unwrap_or(u64::MAX));
            let schemas: HashMap<String, TableSchema> = st.tables.clone();
            (tables, shapes, schemas)
        };
        let col_name = |table: &str, idx: usize| -> String {
            schemas
                .get(table)
                .and_then(|ts| ts.columns.get(idx))
                .map(|(n, _)| n.clone())
                .unwrap_or_else(|| format!("col{idx}"))
        };
        let reg = self.subqueries.lock().await;
        let mut subquery_nodes: Vec<GraphNode> = reg
            .nodes
            .values()
            .map(|n| GraphNode {
                sig: n.sig.clone(),
                inner_table: n.inner_table.clone(),
                proj_col: col_name(&n.inner_table, n.proj_col),
                distinct_values: n.distinct_values(),
                refcount: n.refcount,
            })
            .collect();
        subquery_nodes.sort_by(|a, b| a.sig.cmp(&b.sig));
        let subquery_edges: Vec<GraphEdge> = reg
            .edges
            .iter()
            .map(|e| {
                let (kind, dep_id, dep_table) = match &e.dependent {
                    crate::subquery::Dependent::Shape(id) => (
                        "shape",
                        id.clone(),
                        reg.shapes.get(id).map(|s| s.outer_table.clone()).unwrap_or_default(),
                    ),
                    crate::subquery::Dependent::Node(sig) => (
                        "node",
                        sig.clone(),
                        reg.nodes.get(sig).map(|n| n.inner_table.clone()).unwrap_or_default(),
                    ),
                };
                GraphEdge {
                    node_sig: e.node_sig.clone(),
                    dependent_kind: kind.to_string(),
                    dependent_id: dep_id,
                    connecting_col: col_name(&dep_table, e.connecting_col),
                    negated: e.negated,
                }
            })
            .collect();
        let mut subquery_edges = subquery_edges;
        subquery_edges
            .sort_by(|a, b| (&a.node_sig, &a.dependent_kind, &a.dependent_id).cmp(&(&b.node_sig, &b.dependent_kind, &b.dependent_id)));
        let (operators, op_edges) = circuit_ops(&tables, &shapes, &subquery_nodes, &subquery_edges);
        EngineGraph { tables, shapes, subquery_nodes, subquery_edges, operators, op_edges }
    }

    /// The live inner-set index of one subquery node (values + contributor counts), for the visualizer's
    /// node-detail view. `None` if the signature is unknown.
    pub async fn node_index(&self, sig: &str, cap: usize) -> Option<NodeIndex> {
        let reg = self.subqueries.lock().await;
        let (distinct_values, refcount, values, truncated) = reg.node_value_index(sig, cap)?;
        Some(NodeIndex {
            sig: sig.to_string(),
            distinct_values,
            refcount,
            values: values.into_iter().map(|(value, contributors)| NodeValue { value, contributors }).collect(),
            truncated,
        })
    }

    /// Release one subscription on a shape (extended-API `DELETE /shapes/{id}`, `/v1/shape` handle
    /// eviction). Refcount-0 does **not** tear the shape down: it stays active (a brief reconnect
    /// rejoins it warm), goes dormant after the retention idle timeout, and is eventually evicted
    /// by the layered policy (see `crate::retention`). Releasing is also a touch, so the idle
    /// countdown starts at the disconnect. Infallible: it only adjusts in-memory counters.
    pub async fn release_shape(&self, id: &str) {
        let mut st = self.state.lock().await;
        if let Some(share) = st.feed_shares.get_mut(id) {
            share.refcount = share.refcount.saturating_sub(1);
            let _ = self.catalog_tx.send(CatalogEvent::Left { id: id.to_string() });
        }
        drop(st);
        self.touch_shape(id);
    }

    /// Force-drop a shape NOW, bypassing the retention lifecycle: full teardown (record, share
    /// entries, lifecycle entry, sequencer routing, subquery-registry entry, durable stream)
    /// regardless of refcount or lifecycle state. An admin/debug operation (`DELETE
    /// /shapes/{id}?purge=true`, the visualizer's trash button) — subscribed clients see their
    /// stream vanish and recreate via the normal 404 / must-refetch path. The sequencer command
    /// queue is FIFO, so a purge ordered after an in-flight resume removes whatever the resume
    /// registered.
    pub async fn purge_shape(&self, id: &str) -> Result<()> {
        let mut st = self.state.lock().await;
        self.lives.lock().unwrap().remove(id);
        if let Some(share) = st.feed_shares.remove(id) {
            st.feed_by_sig.remove(&share.sig);
        }
        let removed = st.shapes.remove(id);
        if removed.is_some() {
            let _ = self.catalog_tx.send(CatalogEvent::Dropped { id: id.to_string() });
        }
        if let Some(rec) = &removed {
            if let Some(seq) = st.sequencer.as_ref() {
                let _ = seq
                    .cmd_tx
                    .send(SequencerCmd::RemoveShape { table: rec.table.clone(), shape_id: id.to_string() });
            }
        }
        drop(st);
        // Subquery shapes live in the registry (a no-op for plain shapes).
        self.subqueries.lock().await.drop_subquery_shape(id);
        if let Some(rec) = removed {
            if let Err(e) = self.ds.delete_stream(&rec.stream_path).await {
                tracing::warn!("failed to delete stream {} for purged shape {id}: {e:#}", rec.stream_path);
            }
            trace_lifecycle(&self.trace_tx, crate::trace::GraphLifecycle::ShapeDropped { shape: id.to_string() });
            tracing::info!("purged shape {id} (forced)");
        }
        Ok(())
    }

    /// Record an engine-visible read of a shape (drives the retention idle timer + LRU order).
    fn touch_shape(&self, id: &str) {
        if let Some(life) = self.lives.lock().unwrap().get_mut(id) {
            life.last_read = std::time::Instant::now();
        }
    }

    /// The shape's retention lifecycle, for introspection (`GET /shapes/{id}`).
    pub async fn shape_lifecycle(&self, id: &str) -> Option<&'static str> {
        self.lives.lock().unwrap().get(id).map(|l| match l.state {
            LifeState::Active => "active",
            LifeState::Deactivating { .. } => "deactivating",
            LifeState::Dormant { .. } => "dormant",
            LifeState::Reactivating { .. } => "reactivating",
        })
    }

    /// Make sure a shape is active, reactivating it from dormancy if needed ("any touch
    /// reactivates"): replay the change log from the shape's resume offset through its predicate
    /// onto the retained stream — no Postgres backfill — then re-register it for live routing.
    /// Concurrent touches coalesce onto one replay; a touch during deactivation waits for the
    /// transition to settle first. Also refreshes `last_read`.
    pub async fn ensure_active(&self, id: &str) -> Result<()> {
        loop {
            enum Step {
                Done,
                WaitDeactivate(tokio::sync::watch::Receiver<bool>),
                WaitReactivate(tokio::sync::watch::Receiver<Option<bool>>),
            }
            let step = {
                let mut lives = self.lives.lock().unwrap();
                match lives.get_mut(id) {
                    // Unknown to retention (already evicted, or never tracked): nothing to do here —
                    // the caller's own record lookup decides between 404 and normal service.
                    None => Step::Done,
                    Some(life) => {
                        life.last_read = std::time::Instant::now();
                        match &life.state {
                            LifeState::Active => Step::Done,
                            LifeState::Deactivating { done } => Step::WaitDeactivate(done.clone()),
                            LifeState::Reactivating { done } => Step::WaitReactivate(done.clone()),
                            LifeState::Dormant { resume_offset, gate, .. } => {
                                // Kick off the replay in a DETACHED task: `ensure_active` futures
                                // are dropped when an HTTP client disconnects, and a cancelled
                                // in-place replay would strand the shape in `Reactivating`. The
                                // task always settles the lifecycle state and publishes the
                                // outcome; this caller then awaits THIS attempt's channel like any
                                // concurrent toucher.
                                let resume_offset = resume_offset.clone();
                                let gate = gate.clone();
                                let (tx, rx) = tokio::sync::watch::channel(None);
                                life.state = LifeState::Reactivating { done: rx.clone() };
                                let engine = self.clone();
                                let id = id.to_string();
                                tokio::spawn(async move {
                                    let res = engine.resume_dormant(&id, resume_offset.clone(), gate.clone()).await;
                                    let mut lives = engine.lives.lock().unwrap();
                                    match res {
                                        Ok(()) => {
                                            if let Some(life) = lives.get_mut(&id) {
                                                life.state = LifeState::Active;
                                                life.last_read = std::time::Instant::now();
                                            }
                                            let _ = tx.send(Some(true));
                                        }
                                        Err(e) => {
                                            tracing::warn!("reactivating shape {id} failed: {e:#}");
                                            // Restore the dormant resume state so a later touch retries.
                                            if let Some(life) = lives.get_mut(&id) {
                                                life.state = LifeState::Dormant {
                                                    since: std::time::Instant::now(),
                                                    resume_offset,
                                                    gate,
                                                };
                                            }
                                            let _ = tx.send(Some(false));
                                        }
                                    }
                                });
                                Step::WaitReactivate(rx)
                            }
                        }
                    }
                }
            };
            match step {
                Step::Done => return Ok(()),
                Step::WaitDeactivate(mut rx) => {
                    // Deactivation in flight: wait for it to settle, then loop (we'll see Dormant).
                    while !*rx.borrow_and_update() {
                        if rx.changed().await.is_err() {
                            break; // deactivator vanished; re-inspect the state
                        }
                    }
                }
                Step::WaitReactivate(mut rx) => loop {
                    let outcome = *rx.borrow_and_update();
                    match outcome {
                        Some(true) => return Ok(()),
                        Some(false) => bail!("shape '{id}' reactivation failed; retry the read"),
                        None => {
                            if rx.changed().await.is_err() {
                                bail!("shape '{id}' reactivator died; retry the read");
                            }
                        }
                    }
                },
            }
        }
    }

    /// The replay half of a reactivation: re-register the shape through the sequencer's two-phase
    /// pending-buffer handshake, but replay the change log from the dormant resume offset instead
    /// of taking a Postgres snapshot. Live deltas arriving during the replay buffer in the pending
    /// shape and drain through the same gate at activation; any overlap between the replay and the
    /// buffer double-applies only absolute per-pk upserts/deletes — idempotent for stream readers.
    /// Split from [`ensure_active`] so the lifecycle bookkeeping stays in one place.
    async fn resume_dormant(&self, id: &str, resume_offset: String, gate: crate::pg::SnapshotGate) -> Result<()> {
        let (rec, ts, pred, out_cols, num_id, cmd_tx) = {
            let mut st = self.state.lock().await;
            let rec =
                st.shapes.get(id).cloned().with_context(|| format!("shape '{id}' vanished during reactivation"))?;
            let ts =
                st.tables.get(&rec.table).cloned().with_context(|| format!("unknown table '{}'", rec.table))?;
            let pred = Arc::new(CompiledPredicate::compile_opt(rec.where_json.as_ref(), &ts)?);
            let out_cols = resolve_columns(&ts, rec.columns.clone())?;
            let num_id: u64 =
                id.strip_prefix('s').and_then(|n| n.parse().ok()).context("unparseable shape id")?;
            let cmd_tx = self.ensure_sequencer(&mut st).cmd_tx.clone();
            (rec, ts, pred, out_cols, num_id, cmd_tx)
        };
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        cmd_tx
            .send(SequencerCmd::BeginShape {
                table: rec.table.clone(),
                shape_id: id.to_string(),
                num_id,
                stream_path: rec.stream_path.clone(),
                pred: pred.clone(),
                out_cols: out_cols.clone(),
                kind: CreateKind::Plain,
                ack: ack_tx,
            })
            .map_err(|_| anyhow::anyhow!("sequencer is gone"))?;
        ack_rx.await.map_err(|_| anyhow::anyhow!("sequencer dropped the begin-shape ack"))?;
        // Replay everything the retained stream is missing (buffering live deltas meanwhile).
        let emitted = match replay_changes_for_shape(
            &self.ds,
            &ts,
            &rec.table,
            &pred,
            out_cols.as_ref(),
            &gate,
            &rec.stream_path,
            &resume_offset,
        )
        .await
        {
            Ok(n) => n,
            Err(e) => {
                let _ = cmd_tx
                    .send(SequencerCmd::AbortShape { table: rec.table.clone(), shape_id: id.to_string() });
                return Err(e.context(format!("shape '{id}' reactivation replay failed")));
            }
        };
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        cmd_tx
            .send(SequencerCmd::ActivateShape {
                table: rec.table.clone(),
                shape_id: id.to_string(),
                gate,
                agg_seed: Vec::new(),
                emitted_seed: emitted,
                ready: ready_tx,
            })
            .map_err(|_| anyhow::anyhow!("sequencer is gone"))?;
        ready_rx
            .await
            .unwrap_or_else(|_| Err("sequencer dropped the ready channel".to_string()))
            .map_err(|e| anyhow::anyhow!("shape '{id}' reactivation failed: {e}"))?;
        let _ = self.catalog_tx.send(CatalogEvent::Reactivated { id: id.to_string() });
        metrics().shapes_reactivated.fetch_add(1, Ordering::Relaxed);
        trace_lifecycle(
            &self.trace_tx,
            crate::trace::GraphLifecycle::ShapeReactivated { shape: id.to_string(), table: rec.table.clone() },
        );
        tracing::info!("reactivated dormant shape {id} (table {})", rec.table);
        Ok(())
    }

    /// Move an idle refcount-0 shape from active to dormant: the sequencer unregisters its
    /// routing and hands back the resume state (fully-processed change-log offset + the shape's
    /// snapshot gate); the stream and record are retained. Rechecks eligibility under the locks —
    /// a touch or rejoin racing the sweep wins.
    async fn deactivate_shape(&self, id: &str) -> Result<()> {
        let st = self.state.lock().await;
        let Some(rec) = st.shapes.get(id).cloned() else { return Ok(()) }; // already gone
        if rec.is_subquery || rec.aggregate.is_some() {
            return Ok(()); // never dormant (state not rebuildable from a bounded replay)
        }
        if st.feed_shares.get(id).is_some_and(|s| s.refcount > 0) {
            return Ok(()); // resubscribed since the sweep snapshot
        }
        let Some(cmd_tx) = st.sequencer.as_ref().map(|s| s.cmd_tx.clone()) else { return Ok(()) };
        let (done_tx, done_rx) = tokio::sync::watch::channel(false);
        {
            let mut lives = self.lives.lock().unwrap();
            let Some(life) = lives.get_mut(id) else { return Ok(()) };
            if !matches!(life.state, LifeState::Active)
                || life.last_read.elapsed() < self.retention.idle_timeout
            {
                return Ok(()); // touched or already transitioning since the sweep snapshot
            }
            life.state = LifeState::Deactivating { done: done_rx };
        }
        drop(st);

        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        let sent = cmd_tx
            .send(SequencerCmd::DeactivateShape { table: rec.table.clone(), shape_id: id.to_string(), resp: resp_tx })
            .is_ok();
        let resume = if sent { resp_rx.await.ok().flatten() } else { None };
        let mut lives = self.lives.lock().unwrap();
        let Some(life) = lives.get_mut(id) else { return Ok(()) };
        match resume {
            Some((resume_offset, gate)) => {
                life.state = LifeState::Dormant {
                    since: std::time::Instant::now(),
                    resume_offset: resume_offset.clone(),
                    gate: gate.clone(),
                };
                drop(lives);
                let _ = self.catalog_tx.send(CatalogEvent::Dormant { id: id.to_string(), resume_offset, gate });
                metrics().shapes_dormanted.fetch_add(1, Ordering::Relaxed);
                trace_lifecycle(&self.trace_tx, crate::trace::GraphLifecycle::ShapeDormant { shape: id.to_string() });
                tracing::debug!("shape {id} went dormant (idle)");
            }
            None => {
                // The sequencer didn't know the shape (or is gone): leave it active. Reset the
                // idle clock so the sweep backs off a full idle window instead of re-attempting
                // (and re-warning) every sweep.
                life.state = LifeState::Active;
                life.last_read = std::time::Instant::now();
                drop(lives);
                tracing::warn!("deactivating shape {id}: sequencer returned no resume state; left active");
            }
        }
        let _ = done_tx.send(true);
        Ok(())
    }

    /// Evict a shape: delete its record, share entries, lifecycle entry, and durable stream. A
    /// returning `/v1/shape` client gets `409 must-refetch`; an extended-API client gets `404` and
    /// recreates. Normally only **dormant** shapes are evicted; the exception is non-parkable
    /// shapes (subquery / aggregate — see [`crate::retention`]), which the TTL layer evicts
    /// straight from active with a full teardown. Rechecks eligibility under the locks — a
    /// reactivation or rejoin racing the sweep wins.
    async fn evict_shape(&self, id: &str, reason: EvictReason) -> Result<()> {
        let mut st = self.state.lock().await;
        let Some(rec) = st.shapes.get(id).cloned() else { return Ok(()) };
        let parkable = !rec.is_subquery && rec.aggregate.is_none();
        {
            let mut lives = self.lives.lock().unwrap();
            let evictable = match lives.get(id) {
                Some(life) if matches!(life.state, LifeState::Dormant { .. }) => true,
                // A non-parkable shape is evicted from active only if it is still idle past the
                // full grace window (a touch since the sweep snapshot wins).
                Some(life) if !parkable && matches!(life.state, LifeState::Active) => {
                    life.last_read.elapsed() >= self.retention.idle_timeout + self.retention.dormant_ttl
                }
                _ => false, // transitioning (or already evicted) since the sweep snapshot
            };
            if !evictable {
                return Ok(());
            }
            if st.feed_shares.get(id).is_some_and(|s| s.refcount > 0) {
                return Ok(());
            }
            lives.remove(id);
        }
        if let Some(share) = st.feed_shares.remove(id) {
            st.feed_by_sig.remove(&share.sig);
        }
        let removed = st.shapes.remove(id);
        if removed.is_some() {
            let _ = self.catalog_tx.send(CatalogEvent::Dropped { id: id.to_string() });
        }
        // A dormant shape is already unregistered from the sequencer; a non-parkable one is still
        // live and needs the full teardown (sequencer routing for aggregates, registry for subqueries).
        if !parkable {
            if let Some(seq) = st.sequencer.as_ref() {
                let _ = seq
                    .cmd_tx
                    .send(SequencerCmd::RemoveShape { table: rec.table.clone(), shape_id: id.to_string() });
            }
        }
        drop(st);
        if !parkable {
            self.subqueries.lock().await.drop_subquery_shape(id);
        }
        if let Some(rec) = removed {
            if let Err(e) = self.ds.delete_stream(&rec.stream_path).await {
                tracing::warn!("failed to delete stream {} for evicted shape {id}: {e:#}", rec.stream_path);
            }
            metrics().shapes_evicted.fetch_add(1, Ordering::Relaxed);
            trace_lifecycle(&self.trace_tx, crate::trace::GraphLifecycle::ShapeDropped { shape: id.to_string() });
            tracing::info!("evicted shape {id} ({})", reason.as_str());
        }
        Ok(())
    }

    /// One retention sweep: snapshot every shape's status, run the pure layered policy
    /// ([`crate::retention::plan_sweep`]), then execute the plan. Public so a harness can force a
    /// sweep instead of waiting for the background interval.
    pub async fn retention_sweep(&self) {
        let cfg = self.retention.clone();
        let snapshot: Vec<SweepShape> = {
            let st = self.state.lock().await;
            let bytes = self.ds.appended_bytes_with_prefix("shape/");
            let lives = self.lives.lock().unwrap();
            st.shapes
                .values()
                .map(|rec| {
                    let life = lives.get(&rec.id);
                    let (idle, dormant_for, in_transition) = match life {
                        None => (std::time::Duration::ZERO, None, true), // mid-create; leave alone
                        Some(l) => match &l.state {
                            LifeState::Active => (l.last_read.elapsed(), None, false),
                            LifeState::Dormant { since, .. } => (l.last_read.elapsed(), Some(since.elapsed()), false),
                            LifeState::Deactivating { .. } | LifeState::Reactivating { .. } => {
                                (l.last_read.elapsed(), None, true)
                            }
                        },
                    };
                    SweepShape {
                        id: rec.id.clone(),
                        refcount: st.feed_shares.get(&rec.id).map(|s| s.refcount).unwrap_or(0),
                        idle,
                        dormant_for,
                        in_transition,
                        dormancy_eligible: !rec.is_subquery && rec.aggregate.is_none(),
                        stream_bytes: bytes.get(&rec.stream_path).copied().unwrap_or(0),
                    }
                })
                .collect()
        };
        let plan = crate::retention::plan_sweep(&cfg, &snapshot);
        if plan.over_capacity {
            metrics().retention_pressure.fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                "retention: {} shapes exceed max_shapes={} but nothing dormant is left to evict — \
                 every shape is actively subscribed or recently read; raise ELECTRIC_IVM_MAX_SHAPES or lower the idle timeout",
                snapshot.len(),
                cfg.max_shapes
            );
        }
        if plan.over_budget {
            metrics().retention_pressure.fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                "retention: shape streams exceed the disk budget ({} bytes) but nothing dormant is left to evict — \
                 raise ELECTRIC_IVM_SHAPE_DISK_BUDGET_MB or lower the idle timeout",
                cfg.disk_budget_bytes
            );
        }
        for id in &plan.deactivate {
            if let Err(e) = self.deactivate_shape(id).await {
                tracing::warn!("retention: deactivating shape {id} failed: {e:#}");
            }
        }
        for (id, reason) in &plan.evict {
            if let Err(e) = self.evict_shape(id, *reason).await {
                tracing::warn!("retention: evicting shape {id} failed: {e:#}");
            }
        }
    }

    /// Spawn (once) the background retention sweeper. Started lazily from the shape-create paths
    /// (and after a catalog restore) so library users that never create shapes never run it.
    fn ensure_retention_sweeper(&self) {
        if self.retention_started.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        let engine = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(engine.retention.sweep_interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await; // the first tick fires immediately; skip it
            loop {
                tick.tick().await;
                engine.retention_sweep().await;
            }
        });
    }

    /// Replay the durable shape catalog and re-register every restorable shape with the (not yet
    /// spawned) sequencer — see [`CATALOG_STREAM`] for the restore semantics per shape kind.
    async fn restore_catalog(&self, compiled: &HashMap<String, TableSchema>) -> Result<()> {
        // 1. Fold the event log.
        // (rec, sig, refcount, dormant resume state). The last Dormant/Reactivated event wins.
        type Restored = (ShapeRecord, Option<String>, usize, Option<(String, crate::pg::SnapshotGate)>);
        let mut recs: HashMap<String, Restored> = HashMap::new();
        let mut start_offset = "-1".to_string();
        let mut off = "-1".to_string();
        loop {
            let (events, next, up_to_date) = self.ds.read_json(CATALOG_STREAM, &off).await?;
            for ev in events {
                let Ok(ev) = serde_json::from_value::<CatalogEvent>(ev) else { continue };
                match ev {
                    CatalogEvent::Created { rec, sig } => {
                        recs.insert(rec.id.clone(), (rec, sig, 1, None));
                    }
                    CatalogEvent::Joined { id } => {
                        if let Some(e) = recs.get_mut(&id) {
                            e.2 += 1;
                        }
                    }
                    CatalogEvent::Left { id } => {
                        if let Some(e) = recs.get_mut(&id) {
                            e.2 = e.2.saturating_sub(1);
                        }
                    }
                    CatalogEvent::Dormant { id, resume_offset, gate } => {
                        if let Some(e) = recs.get_mut(&id) {
                            e.3 = Some((resume_offset, gate));
                        }
                    }
                    CatalogEvent::Reactivated { id } => {
                        if let Some(e) = recs.get_mut(&id) {
                            e.3 = None;
                        }
                    }
                    CatalogEvent::Dropped { id } => {
                        recs.remove(&id);
                    }
                    CatalogEvent::Offset { offset } => start_offset = offset,
                }
            }
            match next {
                Some(n) if !up_to_date && n != off => off = n,
                _ => break,
            }
        }
        if recs.is_empty() && start_offset == "-1" {
            return Ok(());
        }
        tracing::info!("catalog restore: {} shape(s), change-log replay from {start_offset}", recs.len());
        *self.seq_start.lock().unwrap() = start_offset;

        // 2. Restore records + shares; subquery shapes are dropped (see CATALOG_STREAM docs).
        let mut resume: Vec<ShapeRecord> = Vec::new();
        let mut dead_streams: Vec<String> = Vec::new();
        {
            let mut st = self.state.lock().await;
            for (id, (rec, sig, refcount, dormant)) in recs {
                if let Ok(num) = id.trim_start_matches('s').parse::<u64>() {
                    st.next_shape_id = st.next_shape_id.max(num + 1);
                }
                if rec.is_subquery {
                    tracing::warn!(
                        "restore: dropping subquery shape {id} (inner-node state is not persisted);                          subscribers observe the deleted stream and recreate"
                    );
                    let _ = self.catalog_tx.send(CatalogEvent::Dropped { id: id.clone() });
                    dead_streams.push(rec.stream_path.clone());
                    continue;
                }
                st.shapes.insert(id.clone(), rec.clone());
                if let Some(sig) = sig {
                    // Restored feeds are live immediately (their streams already hold data).
                    let (ready_tx, ready_rx) = tokio::sync::watch::channel(Some(true));
                    drop(ready_tx); // receivers keep observing Some(true)
                    st.feed_by_sig.insert(sig.clone(), id.clone());
                    st.feed_shares.insert(id.clone(), FeedShare { sig, refcount, ready: ready_rx });
                }
                match dormant {
                    // A dormant shape restores AS dormant: record + stream retained, no routing,
                    // no replay at boot — the first touch reactivates it from its own resume
                    // offset. (Dormancy age restarts at boot; the TTL clock is conservative.)
                    Some((resume_offset, gate)) => {
                        self.lives.lock().unwrap().insert(
                            id.clone(),
                            ShapeLife {
                                last_read: std::time::Instant::now(),
                                state: LifeState::Dormant {
                                    since: std::time::Instant::now(),
                                    resume_offset,
                                    gate,
                                },
                            },
                        );
                    }
                    None => {
                        self.lives.lock().unwrap().insert(id.clone(), ShapeLife::active());
                        resume.push(rec);
                    }
                }
            }
            self.ensure_sequencer(&mut st);
        }
        // Restored dormant shapes still need the TTL/eviction layers running.
        self.ensure_retention_sweeper();
        for path in dead_streams {
            let _ = self.ds.delete_stream(&path).await;
        }

        // 3. Re-register with the sequencer. Plain/routed shapes resume without a backfill and
        // with a passthrough gate (`changes_only = true` path): everything after the restored
        // offset replays, and re-emission across the crash window is idempotent. Aggregates
        // re-seed their fold from a fresh snapshot (fresh gate skips the replayed history).
        let cmd_tx = {
            let st = self.state.lock().await;
            st.sequencer.as_ref().expect("sequencer spawned above").cmd_tx.clone()
        };
        for rec in resume {
            let outcome = self.resume_shape(&cmd_tx, &rec, compiled).await;
            if let Err(e) = outcome {
                tracing::error!("restore: shape {} failed to resume ({e:#}); dropping it", rec.id);
                let mut st = self.state.lock().await;
                st.shapes.remove(&rec.id);
                let _ = self.catalog_tx.send(CatalogEvent::Dropped { id: rec.id.clone() });
                if let Some(share) = st.feed_shares.remove(&rec.id) {
                    st.feed_by_sig.remove(&share.sig);
                }
            }
        }
        Ok(())
    }

    /// Re-register one restored shape with the sequencer (the resume half of `restore_catalog`).
    async fn resume_shape(
        &self,
        cmd_tx: &mpsc::UnboundedSender<SequencerCmd>,
        rec: &ShapeRecord,
        compiled: &HashMap<String, TableSchema>,
    ) -> Result<()> {
        let ts = compiled
            .get(&rec.table)
            .with_context(|| format!("table '{}' no longer exists", rec.table))?;
        let pred = Arc::new(CompiledPredicate::compile_opt(rec.where_json.as_ref(), ts)?);
        let out_cols: Option<Arc<Vec<usize>>> = match &rec.columns {
            Some(names) => {
                let idx: Result<Vec<usize>> = names.iter().map(|n| ts.column_index(n)).collect();
                Some(Arc::new(idx?))
            }
            None => None,
        };
        let num_id: u64 = rec.id.trim_start_matches('s').parse().unwrap_or(0);
        let (kind, changes_only, is_aggregate) = match &rec.aggregate {
            Some(a) => {
                let col = a.col.as_deref().map(|c| ts.column_index(c)).transpose()?;
                (CreateKind::Aggregate { func: a.func, col }, false, true)
            }
            None => (CreateKind::Plain, true, false),
        };
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        cmd_tx
            .send(SequencerCmd::BeginShape {
                table: rec.table.clone(),
                shape_id: rec.id.clone(),
                num_id,
                stream_path: rec.stream_path.clone(),
                pred: pred.clone(),
                out_cols: out_cols.clone(),
                kind,
                ack: ack_tx,
            })
            .map_err(|_| anyhow::anyhow!("sequencer is gone"))?;
        backfill_and_activate(
            &self.ds,
            &self.pg_url,
            cmd_tx,
            ts,
            &rec.table,
            &rec.id,
            &rec.stream_path,
            &pred,
            out_cols.as_ref(),
            changes_only,
            is_aggregate,
            ack_rx,
        )
        .await
        .map_err(|e| anyhow::anyhow!(e))
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
    /// nodes/contributor-pks. Read directly from in-memory state (cheap; no tailer round-trip).
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
        let (sq_nodes, sq_contributors, sq_distinct, sq_shapes, sq_edges) =
            self.subqueries.lock().await.mem_totals();
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
            subquery_nodes: sq_nodes,
            subquery_contributors: sq_contributors,
            subquery_distinct_values: sq_distinct,
            subquery_shapes: sq_shapes,
            subquery_edges: sq_edges,
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

    /// Full per-node state snapshot (`GET /state`): every tailer's published node map merged with
    /// the subquery registry's node/shape summaries. Tables with no tailer yet (no shape registered)
    /// report a default source state so the visualizer can render a chip for every graph node.
    pub async fn state_snapshot(&self) -> StateSnapshot {
        let mut nodes: HashMap<String, NodeStateSummary> = HashMap::new();
        {
            let st = self.state.lock().await;
            for name in st.tables.keys() {
                nodes.insert(
                    format!("table:{name}"),
                    NodeStateSummary::Table { processed_offset: "-1".to_string(), envelopes: 0 },
                );
            }
            if let Some(seq) = st.sequencer.as_ref()
                && let Ok(m) = seq.node_states.lock()
            {
                for (k, v) in m.iter() {
                    nodes.insert(k.clone(), v.clone());
                }
            }
        }
        for (id, s) in self.subqueries.lock().await.state_summaries() {
            nodes.insert(id, s);
        }
        StateSnapshot { nodes }
    }

    /// Deep state dump of one node (`GET /state/node?id=`): a family router's routing-index
    /// contents, an aggregate's fold internals (incl. the MIN/MAX multiset), a subquery node's
    /// inner-set index, or the summary counters for stateless nodes. `None` = unknown node id.
    pub async fn dump_node(&self, id: &str) -> Option<serde_json::Value> {
        if let Some(sig) = id.strip_prefix("node:") {
            let idx = self.node_index(sig, 500).await?;
            return Some(serde_json::json!({
                "kind": "subqueryNode",
                "node": id,
                "distinctValues": idx.distinct_values,
                "refcount": idx.refcount,
                "values": idx.values,
                "truncated": idx.truncated,
            }));
        }
        // Subquery shapes live in the registry, not in a table tailer.
        if let Some(sid) = id.strip_prefix("shape:") {
            let reg = self.subqueries.lock().await;
            if let Some(s) = reg.shapes.get(sid) {
                return Some(serde_json::json!({
                    "kind": "shape",
                    "node": id,
                    "emitted": s.emitted.load(std::sync::atomic::Ordering::Relaxed),
                }));
            }
        }
        // Everything else is owned by a table tailer; resolve the table and round-trip a dump.
        let table = if let Some(rest) = id.strip_prefix("family:") {
            rest.split(':').next().map(str::to_string)
        } else if let Some(rest) = id.strip_prefix("table:") {
            Some(rest.to_string())
        } else if let Some(sid) = id.strip_prefix("shape:").or_else(|| id.strip_prefix("filter:")) {
            self.state.lock().await.shapes.get(sid).map(|r| r.table.clone())
        } else {
            None
        }?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let st = self.state.lock().await;
            st.sequencer
                .as_ref()?
                .cmd_tx
                .send(SequencerCmd::DumpNode { table, node_id: id.to_string(), resp: tx })
                .ok()?;
        }
        rx.await.ok().flatten()
    }
}

/// The exploded operator decomposition of the maintained pipeline — what the engine ACTUALLY
/// executes per node, one operator box per real step, generated from the same registered
/// structures `/graph` reports. Pure over the graph pieces so it is unit-testable and provably
/// consistent with the topology: every operator's `hop` is a trace-hop id and every `state` is a
/// `GET /state` key, so the circuit view animates and shows live state with zero client guessing.
fn circuit_ops(
    tables: &[String],
    shapes: &[GraphShape],
    subquery_nodes: &[GraphNode],
    subquery_edges: &[GraphEdge],
) -> (Vec<OpNode>, Vec<OpEdge>) {
    let mut ops: Vec<OpNode> = Vec::new();
    let mut edges: Vec<OpEdge> = Vec::new();
    let op = |id: &str, kind: &str, hop: &str, state: Option<String>, label: &str| OpNode {
        id: id.to_string(),
        kind: kind.to_string(),
        hop: hop.to_string(),
        state,
        label: label.to_string(),
    };
    let flow = |s: &str, t: &str| OpEdge { source: s.into(), target: t.into(), kind: "flow".into(), label: None };

    // Every table: the stream tailer (source) and the envelope → Z-set delta step it runs.
    for t in tables {
        let hop = format!("table:{t}");
        ops.push(op(&format!("src:{t}"), "source", &hop, Some(hop.clone()), t));
        ops.push(op(&format!("d:{t}"), "delta", &hop, None, "Δ change"));
        edges.push(flow(&format!("src:{t}"), &format!("d:{t}")));
    }

    // Shared family operators are emitted once per (table, key-cols), like the router itself.
    let mut fams_done: HashSet<(String, String)> = HashSet::new();

    for s in shapes {
        let sid = &s.id;
        let t = &s.table;
        let d = format!("d:{t}");
        let shape_hop = format!("shape:{sid}");
        let snk_id = format!("snk:{sid}");

        if let Some(agg) = &s.aggregate {
            // apply(): σ over the delta, then the incremental fold; the sink appends on change.
            let fn_label = format!("Σ {}({})", format!("{:?}", agg.func).to_uppercase(), agg.col.as_deref().unwrap_or("*"));
            ops.push(op(&format!("sigma:{sid}"), "filter", &shape_hop, None, "σ where"));
            ops.push(op(&format!("fold:{sid}"), "fold", &shape_hop, Some(shape_hop.clone()), &fn_label));
            ops.push(op(&snk_id, "sink", &shape_hop, None, &s.stream_path));
            edges.push(flow(&d, &format!("sigma:{sid}")));
            edges.push(flow(&format!("sigma:{sid}"), &format!("fold:{sid}")));
            edges.push(flow(&format!("fold:{sid}"), &snk_id));
            continue;
        }

        if s.is_subquery {
            // The outer predicate evaluates with IN-membership against node arrangements — a
            // semijoin/antijoin; flips arrive on the subquery edges added below.
            ops.push(op(&format!("sj:{sid}"), "join", &shape_hop, None, "⋈ membership"));
            ops.push(op(&format!("pi:{sid}"), "project", &shape_hop, None, "π pk → envelope"));
            ops.push(op(&snk_id, "sink", &shape_hop, Some(shape_hop.clone()), &s.stream_path));
            edges.push(flow(&d, &format!("sj:{sid}")));
            edges.push(flow(&format!("sj:{sid}"), &format!("pi:{sid}")));
            edges.push(flow(&format!("pi:{sid}"), &snk_id));
            continue;
        }

        if let Some(key) = &s.family_key {
            let cols = key.join(",");
            let fam_hop = format!("family:{t}:{cols}");
            let (key_id, arr_id, join_id) =
                (format!("key:{t}:{cols}"), format!("arr:{t}:{cols}"), format!("rjoin:{t}:{cols}"));
            if fams_done.insert((t.clone(), cols.clone())) {
                ops.push(op(&key_id, "key", &fam_hop, None, &format!("↦ key({cols})")));
                ops.push(op(&arr_id, "arrange", &fam_hop, Some(fam_hop.clone()), "params: key → shapes"));
                ops.push(op(&join_id, "join", &fam_hop, None, "⋈ route"));
                edges.push(flow(&d, &key_id));
                edges.push(flow(&key_id, &join_id));
                edges.push(OpEdge { source: arr_id, target: join_id.clone(), kind: "state".into(), label: None });
            }
            ops.push(op(&format!("pi:{sid}"), "project", &shape_hop, None, "π pk → envelope"));
            ops.push(op(&snk_id, "sink", &shape_hop, Some(shape_hop.clone()), &s.stream_path));
            edges.push(OpEdge {
                source: join_id,
                target: format!("pi:{sid}"),
                kind: "flow".into(),
                label: Some(sid.clone()),
            });
            edges.push(flow(&format!("pi:{sid}"), &snk_id));
            continue;
        }

        // Standalone: stateless σ directly on the delta, then group-by-pk into envelopes.
        let filter_hop = format!("filter:{sid}");
        ops.push(op(&format!("sigma:{sid}"), "filter", &filter_hop, Some(filter_hop.clone()), "σ where"));
        ops.push(op(&format!("pi:{sid}"), "project", &shape_hop, None, "π pk → envelope"));
        ops.push(op(&snk_id, "sink", &shape_hop, Some(shape_hop.clone()), &s.stream_path));
        edges.push(flow(&d, &format!("sigma:{sid}")));
        edges.push(flow(&format!("sigma:{sid}"), &format!("pi:{sid}")));
        edges.push(flow(&format!("pi:{sid}"), &snk_id));
    }

    // Shared subquery inner sets: σ inner where → π projected column → distinct arrangement.
    for n in subquery_nodes {
        let sig = &n.sig;
        let hop = format!("node:{sig}");
        ops.push(op(&format!("sqf:{sig}"), "filter", &hop, None, "σ inner where"));
        ops.push(op(&format!("sqp:{sig}"), "project", &hop, None, &format!("π {}", n.proj_col)));
        ops.push(op(&format!("dist:{sig}"), "distinct", &hop, Some(hop.clone()), &format!("distinct {}", n.proj_col)));
        edges.push(flow(&format!("d:{}", n.inner_table), &format!("sqf:{sig}")));
        edges.push(flow(&format!("sqf:{sig}"), &format!("sqp:{sig}")));
        edges.push(flow(&format!("sqp:{sig}"), &format!("dist:{sig}")));
    }
    // Membership dependencies: a node's arrangement feeds each dependent's semijoin (or a parent
    // node's inner filter, for nested IN).
    for e in subquery_edges {
        let src = format!("dist:{}", e.node_sig);
        let (target, label) = if e.dependent_kind == "shape" {
            (format!("sj:{}", e.dependent_id), format!("{} · {}", if e.negated { "NOT IN" } else { "IN" }, e.connecting_col))
        } else {
            (format!("sqf:{}", e.dependent_id), format!("{} · {}", if e.negated { "NOT IN" } else { "IN" }, e.connecting_col))
        };
        edges.push(OpEdge { source: src, target, kind: "subquery".into(), label: Some(label) });
    }

    (ops, edges)
}

/// A non-shareable shape (range / OR / NOT / inequality / match-all). Its predicate is a stateless
/// filter, so it needs no incremental state or OS thread — it is evaluated directly on each delta. This
/// is what lets standalone shapes scale far past the old one-thread-per-shape ceiling.
struct StandaloneShape {
    pred: Arc<CompiledPredicate>,
    stream_path: String,
    /// This shape's backfill-snapshot fence: replicated changes already visible to the backfill are
    /// skipped by xid visibility (LSN fallback) — see [`crate::pg::SnapshotGate`].
    gate: crate::pg::SnapshotGate,
    /// Output projection (column indices), or `None` to emit the full row.
    out_cols: Option<Arc<Vec<usize>>>,
}

/// Evaluate a stateless WHERE filter directly on a Z-set delta. A filter has no incremental state
/// (unlike a join), so wrapping it in a dataflow circuit would only add a thread + channel round-trip
/// + a per-shape clone of the delta. `translate_output` downstream groups by primary key, so emitting
/// the matching `(row, weight)` pairs here is equivalent to what the old per-shape filter circuit produced.
fn eval_standalone(pred: &CompiledPredicate, delta: &[Tup2<Row, ZWeight>]) -> Vec<(Row, ZWeight)> {
    delta
        .iter()
        .filter(|t| pred.matches(&t.0))
        .map(|t| (t.0.clone(), t.1))
        .collect()
}

/// Index over standalone shapes by a **necessary conjunct** (`(column, op)` — see
/// [`CompiledPredicate::access_leaf`]): a change row can only match a shape if the shape's
/// necessary conjunct holds on that row, so per-change candidate lookup replaces the O(K)
/// scan over all standalone shapes with hash lookups (equality conjuncts) + ordered bound
/// scans (range conjuncts), both output-sensitive. Shapes with no indexable conjunct
/// (top-level OR/NOT, LIKE, !=, IS NULL, match-all) stay on the `scan` fallback list.
#[derive(Default)]
struct StandaloneIndex {
    /// `col = v` conjuncts: column -> literal -> shape ids.
    eq: HashMap<usize, HashMap<Value, Vec<String>>>,
    /// `col >/>= v` conjuncts: column -> bound -> (shape id, strict). A row value `x` satisfies
    /// bounds `< x` (any) and `== x` (non-strict only) — an ordered prefix scan.
    lower: HashMap<usize, std::collections::BTreeMap<Value, Vec<(String, bool)>>>,
    /// `col </<= v` conjuncts, mirrored.
    upper: HashMap<usize, std::collections::BTreeMap<Value, Vec<(String, bool)>>>,
    /// Shapes with no indexable conjunct — always candidates.
    scan: Vec<String>,
    /// Where each shape was placed, for removal.
    placed: HashMap<String, Option<crate::predicate::AccessLeaf>>,
}

impl StandaloneIndex {
    fn insert(&mut self, sid: &str, pred: &CompiledPredicate) {
        use crate::predicate::AccessLeaf;
        let leaf = pred.access_leaf();
        match &leaf {
            Some(AccessLeaf::Eq { col, value }) => {
                self.eq.entry(*col).or_default().entry(value.clone()).or_default().push(sid.to_string());
            }
            Some(AccessLeaf::Lower { col, value, strict }) => {
                self.lower.entry(*col).or_default().entry(value.clone()).or_default().push((sid.to_string(), *strict));
            }
            Some(AccessLeaf::Upper { col, value, strict }) => {
                self.upper.entry(*col).or_default().entry(value.clone()).or_default().push((sid.to_string(), *strict));
            }
            None => self.scan.push(sid.to_string()),
        }
        self.placed.insert(sid.to_string(), leaf);
    }

    fn remove(&mut self, sid: &str) {
        use crate::predicate::AccessLeaf;
        let Some(leaf) = self.placed.remove(sid) else { return };
        match leaf {
            Some(AccessLeaf::Eq { col, value }) => {
                if let Some(by_val) = self.eq.get_mut(&col)
                    && let Some(sids) = by_val.get_mut(&value)
                {
                    sids.retain(|s| s != sid);
                    if sids.is_empty() {
                        by_val.remove(&value);
                        if by_val.is_empty() {
                            self.eq.remove(&col);
                        }
                    }
                }
            }
            Some(AccessLeaf::Lower { col, value, .. }) => {
                Self::remove_bound(&mut self.lower, col, &value, sid);
            }
            Some(AccessLeaf::Upper { col, value, .. }) => {
                Self::remove_bound(&mut self.upper, col, &value, sid);
            }
            None => self.scan.retain(|s| s != sid),
        }
    }

    fn remove_bound(
        m: &mut HashMap<usize, std::collections::BTreeMap<Value, Vec<(String, bool)>>>,
        col: usize,
        value: &Value,
        sid: &str,
    ) {
        if let Some(by_val) = m.get_mut(&col)
            && let Some(sids) = by_val.get_mut(value)
        {
            sids.retain(|(s, _)| s != sid);
            if sids.is_empty() {
                by_val.remove(value);
                if by_val.is_empty() {
                    m.remove(&col);
                }
            }
        }
    }

    /// Shape ids whose necessary conjunct is satisfied by at least one row in `delta`, plus the
    /// unconditional `scan` shapes. A superset of the shapes that can match any delta row (each
    /// candidate is still fully evaluated); every non-candidate is guaranteed not to match.
    fn candidates(&self, delta: &[Tup2<Row, ZWeight>]) -> Vec<String> {
        let mut out: HashSet<&str> = self.scan.iter().map(String::as_str).collect();
        for Tup2(row, _) in delta {
            for (col, by_val) in &self.eq {
                if let Some(cell) = row.0.get(*col)
                    && let Some(sids) = by_val.get(cell)
                {
                    out.extend(sids.iter().map(String::as_str));
                }
            }
            for (col, bounds) in &self.lower {
                let Some(cell) = row.0.get(*col) else { continue };
                if matches!(cell, Value::Null) {
                    continue; // cmp with a NULL cell is never TRUE
                }
                for (bound, sids) in bounds.range(..=cell) {
                    let at_bound = bound == cell;
                    out.extend(
                        sids.iter().filter(|(_, strict)| !(at_bound && *strict)).map(|(s, _)| s.as_str()),
                    );
                }
            }
            for (col, bounds) in &self.upper {
                let Some(cell) = row.0.get(*col) else { continue };
                if matches!(cell, Value::Null) {
                    continue;
                }
                for (bound, sids) in bounds.range(cell..) {
                    let at_bound = bound == cell;
                    out.extend(
                        sids.iter().filter(|(_, strict)| !(at_bound && *strict)).map(|(s, _)| s.as_str()),
                    );
                }
            }
        }
        out.into_iter().map(str::to_string).collect()
    }
}

/// One shape registered on an equality template, backfilled from Postgres and routed by key.
struct RoutedShape {
    num_id: u64,
    stream_path: String,
    /// THIS shape's own backfill-snapshot fence (see [`crate::pg::SnapshotGate`]).
    gate: crate::pg::SnapshotGate,
    /// Output projection (column indices), or `None` to emit the full row.
    out_cols: Option<Arc<Vec<usize>>>,
}

/// All equality shapes sharing one key-column set, indexed by key tuple. Holds **no table rows** —
/// only the `key -> shapes` routing. A change is routed by its key to exactly the shapes registered on
/// that key (O(log N), independent of shape count); each shape is backfilled directly from Postgres
/// (`WHERE key = const`), so the engine never keeps a copy of the table.
struct KeyRouter {
    key_cols: Vec<usize>,
    index: HashMap<Row, Vec<RoutedShape>>,
}

impl KeyRouter {
    fn member_count(&self) -> usize {
        self.index.values().map(|v| v.len()).sum()
    }
}

/// Supported scalar aggregation functions. COUNT/SUM/AVG are O(1) running scalars; MIN/MAX keep an
/// ordered multiset of the matching values (so a retraction can restore the previous extreme).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AggFn {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

fn value_f64(v: &Value) -> f64 {
    match v {
        Value::Int(i) => *i as f64,
        Value::Float(f) => f.0,
        Value::Bool(b) => {
            if *b {
                1.0
            } else {
                0.0
            }
        }
        _ => 0.0,
    }
}

/// A scalar aggregation maintained **incrementally** over the rows matching `pred` — a running fold over
/// the Z-set of matching changes. Holds only the running aggregate, never the rows: COUNT is a sum of
/// weights, SUM/AVG add `value·weight`, MIN/MAX keep a `value → net-weight` multiset. O(1) per change
/// (plus a log-factor for MIN/MAX). Evaluated on the delta like a standalone filter, for any
/// non-subquery predicate.
struct AggShape {
    pred: Arc<CompiledPredicate>,
    func: AggFn,
    col: Option<usize>,
    stream_path: String,
    gate: crate::pg::SnapshotGate,
    /// Matching rows (COUNT(*) semantics).
    count: i64,
    /// Matching rows whose aggregated column is non-NULL — SQL aggregates ignore NULLs, so this is
    /// the denominator for AVG, the COUNT(col) value, and the emptiness test for SUM/MIN/MAX.
    nn_count: i64,
    sum: f64,
    multiset: std::collections::BTreeMap<Value, i64>,
    last: Option<serde_json::Value>,
}

impl AggShape {
    /// Fold a Z-set delta into the running aggregate. Returns true if any matching row was seen.
    /// NULL column values are excluded from the fold (SQL semantics: aggregates ignore NULLs).
    fn apply(&mut self, delta: &[Tup2<Row, ZWeight>]) -> bool {
        let mut touched = false;
        for Tup2(row, w) in delta {
            if !self.pred.matches(row) {
                continue;
            }
            touched = true;
            self.count += *w;
            if let Some(ci) = self.col {
                let v = row.0.get(ci).cloned().unwrap_or(Value::Null);
                if matches!(v, Value::Null) {
                    continue; // SQL aggregates skip NULLs entirely
                }
                self.nn_count += *w;
                self.sum += value_f64(&v) * (*w as f64);
                if matches!(self.func, AggFn::Min | AggFn::Max) {
                    let e = self.multiset.entry(v.clone()).or_insert(0);
                    *e += *w;
                    if *e <= 0 {
                        self.multiset.remove(&v);
                    }
                }
            }
        }
        touched
    }

    /// The current aggregate value as JSON, mirroring Postgres: `COUNT(*)` counts rows, `COUNT(col)`
    /// counts non-NULL values, and SUM/AVG/MIN/MAX over zero (non-NULL) values are NULL.
    fn value(&self) -> serde_json::Value {
        match self.func {
            AggFn::Count => {
                if self.col.is_some() {
                    serde_json::json!(self.nn_count)
                } else {
                    serde_json::json!(self.count)
                }
            }
            AggFn::Sum => {
                if self.nn_count > 0 {
                    serde_json::json!(self.sum)
                } else {
                    serde_json::Value::Null
                }
            }
            AggFn::Avg => {
                if self.nn_count > 0 {
                    serde_json::json!(self.sum / self.nn_count as f64)
                } else {
                    serde_json::Value::Null
                }
            }
            AggFn::Min => self.multiset.keys().next().map(Value::to_json).unwrap_or(serde_json::Value::Null),
            AggFn::Max => self.multiset.keys().next_back().map(Value::to_json).unwrap_or(serde_json::Value::Null),
        }
    }

    /// The output envelope carrying the current aggregate (key `"agg"`, so the client materializes one row).
    fn envelope(&self, ts: &TableSchema, txid: Option<String>, lsn: Option<String>) -> Envelope {
        Envelope {
            type_: ts.name.clone(),
            key: "agg".into(),
            value: Some(serde_json::json!({ "value": self.value(), "n": self.count })),
            old: None,
            headers: EnvelopeHeaders { operation: "upsert".into(), txid, offset: None, lsn, seq: None },
        }
    }
}

/// The key tuple for a row given the template's key columns (positional projection). Missing columns
/// project to NULL (defensive; equality-template columns always exist).
fn key_of(row: &Row, cols: &[usize]) -> Row {
    Row(cols.iter().map(|&i| row.0.get(i).cloned().unwrap_or(Value::Null)).collect())
}

/// Resolve an optional column-name projection to sorted, pk-included column indices (the pk is always
/// kept so the client can key rows). `None` => emit the full row. Shared by shapes and subset queries.
fn resolve_columns(ts: &TableSchema, columns: Option<Vec<String>>) -> Result<Option<Arc<Vec<usize>>>> {
    match columns {
        None => Ok(None),
        Some(names) => {
            let mut idxs = Vec::with_capacity(names.len() + 1);
            for name in &names {
                idxs.push(ts.column_index(name)?);
            }
            if !idxs.contains(&ts.pk_index) {
                idxs.push(ts.pk_index);
            }
            idxs.sort_unstable();
            idxs.dedup();
            Ok(Some(Arc::new(idxs)))
        }
    }
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

fn spawn_sequencer(
    ds: DsClient,
    tables: SharedTables,
    start_offset: String,
    catalog_tx: mpsc::UnboundedSender<CatalogEvent>,
    subq: SubqueryHandle,
    trace_tx: tokio::sync::broadcast::Sender<Arc<String>>,
) -> SequencerHandle {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let processed = Arc::new(std::sync::Mutex::new(start_offset.clone()));
    let stats = Arc::new(std::sync::Mutex::new(HashMap::new()));
    let node_states = Arc::new(std::sync::Mutex::new(HashMap::new()));
    tokio::spawn(sequencer_loop(
        ds,
        tables,
        start_offset,
        catalog_tx,
        cmd_rx,
        processed.clone(),
        stats.clone(),
        node_states.clone(),
        subq,
        trace_tx,
    ));
    SequencerHandle { cmd_tx, processed, stats, node_states }
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

/// Rebuild the tailer's full per-node state map from its live structures. Pure so it's unit-testable;
/// cost is O(shapes on this table) small clones, the same order as the fan-out work per batch.
#[allow(clippy::too_many_arguments)]
fn build_node_states(
    ts: &TableSchema,
    offset: &str,
    envelopes: u64,
    shapes: &HashMap<String, StandaloneShape>,
    families: &HashMap<Vec<usize>, KeyRouter>,
    family_of: &HashMap<String, (Vec<usize>, u64, Row)>,
    aggregates: &HashMap<String, AggShape>,
    emitted: &HashMap<String, u64>,
) -> HashMap<String, NodeStateSummary> {
    let mut out = HashMap::new();
    out.insert(
        format!("table:{}", ts.name),
        NodeStateSummary::Table { processed_offset: offset.to_string(), envelopes },
    );
    let emitted_of = |path: &str| emitted.get(sid_of_path(path)).copied().unwrap_or(0);
    for (sid, s) in shapes {
        let n = emitted_of(&s.stream_path);
        out.insert(format!("filter:{sid}"), NodeStateSummary::Filter { emitted: n });
        out.insert(format!("shape:{sid}"), NodeStateSummary::Shape { emitted: n });
    }
    for (key_cols, router) in families {
        out.insert(
            family_node_id(ts, key_cols),
            NodeStateSummary::Family { keys: router.index.len(), shapes: router.member_count() },
        );
    }
    for sid in family_of.keys() {
        out.insert(
            format!("shape:{sid}"),
            NodeStateSummary::Shape { emitted: emitted.get(sid.as_str()).copied().unwrap_or(0) },
        );
    }
    for (sid, agg) in aggregates {
        out.insert(
            format!("shape:{sid}"),
            NodeStateSummary::Aggregate {
                value: agg.value(),
                count: agg.count,
                nn_count: agg.nn_count,
                multiset_len: agg.multiset.len(),
            },
        );
    }
    out
}

/// Cap on entries returned by a `DumpNode` state dump (routing keys / multiset values).
const DUMP_CAP: usize = 500;

/// Full state dump of a family router: the routing index contents (`key tuple -> shape ids`).
fn dump_family_json(ts: &TableSchema, router: &KeyRouter) -> serde_json::Value {
    let mut entries: Vec<serde_json::Value> = router
        .index
        .iter()
        .take(DUMP_CAP)
        .map(|(key, routed)| {
            serde_json::json!({
                "key": key.0.iter().map(Value::to_json).collect::<Vec<_>>(),
                "shapes": routed.iter().map(|rs| format!("s{}", rs.num_id)).collect::<Vec<_>>(),
            })
        })
        .collect();
    entries.sort_by_key(|e| e["key"].to_string());
    serde_json::json!({
        "kind": "family",
        "node": family_node_id(ts, &router.key_cols),
        "keyCols": router.key_cols.iter()
            .map(|i| ts.columns.get(*i).map(|(n, _)| n.clone()).unwrap_or_else(|| format!("col{i}")))
            .collect::<Vec<_>>(),
        "keys": router.index.len(),
        "shapes": router.member_count(),
        "entries": entries,
        "truncated": router.index.len() > DUMP_CAP,
    })
}

/// Full state dump of an aggregation fold: running counters + the MIN/MAX multiset contents.
fn dump_aggregate_json(sid: &str, agg: &AggShape) -> serde_json::Value {
    let multiset: Vec<serde_json::Value> = agg
        .multiset
        .iter()
        .take(DUMP_CAP)
        .map(|(v, w)| serde_json::json!({ "value": v.to_json(), "weight": w }))
        .collect();
    serde_json::json!({
        "kind": "aggregate",
        "node": format!("shape:{sid}"),
        "func": agg.func,
        "value": agg.value(),
        "count": agg.count,
        "nnCount": agg.nn_count,
        "multisetLen": agg.multiset.len(),
        "multiset": multiset,
        "truncated": agg.multiset.len() > DUMP_CAP,
    })
}

fn stats_of(exec: &TableExec) -> TableStats {
    let mut fams: Vec<FamilyStat> = exec
        .families
        .iter()
        .map(|(k, f)| FamilyStat { key_cols: k.clone(), shapes: f.member_count() })
        .collect();
    fams.sort_by(|a, b| a.key_cols.cmp(&b.key_cols));
    TableStats { families: fams, standalone: exec.shapes.len() }
}

/// Rebuild + publish the merged node-state map and per-table stats to the sequencer's shared
/// handles and, when anyone is subscribed to `/trace`, broadcast the merged map (plus the
/// subquery registry's summaries) as a `{"type":"state"}` event.
async fn publish_all(
    execs: &HashMap<String, TableExec>,
    offset: &str,
    emitted: &HashMap<String, u64>,
    stats: &std::sync::Mutex<HashMap<String, TableStats>>,
    node_states: &std::sync::Mutex<HashMap<String, NodeStateSummary>>,
    subqueries: &Arc<Mutex<SubqueryRegistry>>,
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
) {
    let mut stats_map = HashMap::new();
    let mut merged: HashMap<String, NodeStateSummary> = HashMap::new();
    for (t, exec) in execs {
        stats_map.insert(t.clone(), stats_of(exec));
        merged.extend(build_node_states(
            &exec.ts,
            offset,
            exec.envelopes_total,
            &exec.shapes,
            &exec.families,
            &exec.family_of,
            &exec.aggregates,
            emitted,
        ));
    }
    *stats.lock().unwrap() = stats_map;
    *node_states.lock().unwrap() = merged.clone();
    if trace_tx.receiver_count() == 0 {
        return;
    }
    let mut ev_nodes = merged;
    for (id, s) in subqueries.lock().await.state_summaries() {
        ev_nodes.insert(id, s);
    }
    if let Ok(json) = serde_json::to_string(&crate::trace::StateEvent::new(ev_nodes)) {
        let _ = trace_tx.send(Arc::new(json));
    }
}

/// Per-table executor state owned by the sequencer: the routing structures a table's changes fan
/// out through, plus any in-flight (pending) shape creations buffering deltas.
struct TableExec {
    ts: TableSchema,
    shapes: HashMap<String, StandaloneShape>,
    shape_index: StandaloneIndex,
    families: HashMap<Vec<usize>, KeyRouter>,
    family_of: HashMap<String, (Vec<usize>, u64, Row)>,
    aggregates: HashMap<String, AggShape>,
    pending: HashMap<String, PendingShape>,
    envelopes_total: u64,
}

impl TableExec {
    fn new(ts: TableSchema) -> TableExec {
        TableExec {
            ts,
            shapes: HashMap::new(),
            shape_index: StandaloneIndex::default(),
            families: HashMap::new(),
            family_of: HashMap::new(),
            aggregates: HashMap::new(),
            pending: HashMap::new(),
            envelopes_total: 0,
        }
    }
}

/// A shape between `BeginShape` and `ActivateShape`: buffers every processed delta of its table so
/// activation can replay exactly what the backfill snapshot did not see (through the gate).
struct PendingShape {
    num_id: u64,
    stream_path: String,
    pred: Arc<CompiledPredicate>,
    out_cols: Option<Arc<Vec<usize>>>,
    kind: CreateKind,
    buffered: Vec<Envelope>,
}

/// Get (or lazily create) the executor for `table`; `None` if the table has no known schema.
fn exec_for<'a>(
    execs: &'a mut HashMap<String, TableExec>,
    tables: &SharedTables,
    table: &str,
) -> Option<&'a mut TableExec> {
    if !execs.contains_key(table) {
        let ts = tables.read().unwrap().get(table).cloned()?;
        execs.insert(table.to_string(), TableExec::new(ts));
    }
    execs.get_mut(table)
}

/// The engine's single LSN-ordered executor: consumes the global `changes` stream in commit order
/// and dispatches each envelope to its table's executor. Each transaction's shape appends are
/// flushed **before the next transaction is processed**, so every shape stream reflects source
/// transactions atomically and in commit order — cross-table included (Electric's
/// `ShapeLogCollector` pattern; the property the old per-table tailers lost).
#[allow(clippy::too_many_arguments)]
async fn sequencer_loop(
    ds: DsClient,
    tables: SharedTables,
    start_offset: String,
    catalog_tx: mpsc::UnboundedSender<CatalogEvent>,
    mut cmd_rx: mpsc::UnboundedReceiver<SequencerCmd>,
    processed: Arc<std::sync::Mutex<String>>,
    stats: Arc<std::sync::Mutex<HashMap<String, TableStats>>>,
    node_states: Arc<std::sync::Mutex<HashMap<String, NodeStateSummary>>>,
    subq: SubqueryHandle,
    trace_tx: tokio::sync::broadcast::Sender<Arc<String>>,
) {
    let mut execs: HashMap<String, TableExec> = HashMap::new();
    let mut offset = start_offset;
    // Offset checkpointing: persist the processed position (the restart replay start) at most
    // every ~2s of change.
    let mut last_ckpt = std::time::Instant::now();
    let mut ckpt_offset = offset.clone();
    // Envelopes appended per shape id — the counters behind the per-node state summaries.
    let mut emitted: HashMap<String, u64> = HashMap::new();
    // De-duplication highwater: the ingestor's delivery is at-least-once (unacknowledged commits
    // re-deliver after a reconnect), and deltas are NOT idempotent for aggregates/subquery
    // weights. Every ingestor envelope carries (commit lsn, seq = position in txn), strictly
    // increasing on the single ordered log, so anything at/below the highwater has already been
    // applied and is skipped. Envelopes without both stamps (library mode) bypass this.
    let mut highwater: Option<(u64, u64)> = None;

    loop {
        let off = offset.clone();
        tokio::select! {
            biased;
            cmd = cmd_rx.recv() => match cmd {
                Some(SequencerCmd::BeginShape { table, shape_id, num_id, stream_path, pred, out_cols, kind, ack }) => {
                    match exec_for(&mut execs, &tables, &table) {
                        Some(exec) => {
                            exec.pending.insert(
                                shape_id,
                                PendingShape { num_id, stream_path, pred, out_cols, kind, buffered: Vec::new() },
                            );
                        }
                        None => tracing::error!("begin_shape: unknown table '{table}'"),
                    }
                    let _ = ack.send(());
                }
                Some(SequencerCmd::ActivateShape { table, shape_id, gate, agg_seed, emitted_seed, ready }) => {
                    let res = activate_shape(
                        &ds, &mut execs, &table, &shape_id, gate, agg_seed, emitted_seed, &mut emitted,
                    ).await;
                    if let Err(e) = &res {
                        tracing::error!("activate_shape failed: {e:#}");
                    }
                    let _ = ready.send(res.map_err(|e| format!("{e:#}")));
                    publish_all(&execs, &offset, &emitted, &stats, &node_states, &subq.registry, &trace_tx).await;
                }
                Some(SequencerCmd::AbortShape { table, shape_id }) => {
                    if let Some(exec) = execs.get_mut(&table) {
                        exec.pending.remove(&shape_id);
                    }
                }
                Some(SequencerCmd::DeactivateShape { table, shape_id, resp }) => {
                    // Capture-and-unregister is atomic w.r.t. envelope processing (commands run
                    // between fully-flushed transactions), so `offset` is exactly "the shape's
                    // stream is complete up to here".
                    let gate = execs.get_mut(&table).and_then(|exec| {
                        if let Some(shape) = exec.shapes.remove(&shape_id) {
                            exec.shape_index.remove(&shape_id);
                            Some(shape.gate)
                        } else if let Some((key_cols, num_id, key_tuple)) = exec.family_of.remove(&shape_id) {
                            let mut gate = None;
                            if let Some(router) = exec.families.get_mut(&key_cols) {
                                if let Some(routed) = router.index.get_mut(&key_tuple) {
                                    if let Some(pos) = routed.iter().position(|rs| rs.num_id == num_id) {
                                        gate = Some(routed.remove(pos).gate);
                                    }
                                    if routed.is_empty() {
                                        router.index.remove(&key_tuple);
                                    }
                                }
                                if router.index.is_empty() {
                                    exec.families.remove(&key_cols);
                                }
                            }
                            gate
                        } else {
                            None // unknown, pending, or an aggregate — not parkable from here
                        }
                    });
                    if gate.is_some() {
                        emitted.remove(&shape_id);
                    }
                    let _ = resp.send(gate.map(|g| (offset.clone(), g)));
                    publish_all(&execs, &offset, &emitted, &stats, &node_states, &subq.registry, &trace_tx).await;
                }
                Some(SequencerCmd::RemoveShape { table, shape_id }) => {
                    if let Some(exec) = execs.get_mut(&table) {
                        exec.pending.remove(&shape_id);
                        if exec.aggregates.remove(&shape_id).is_some() {
                            // an aggregation shape — nothing else to unwind
                        } else if exec.shapes.remove(&shape_id).map(|_| exec.shape_index.remove(&shape_id)).is_none()
                            && let Some((key_cols, num_id, key_tuple)) = exec.family_of.remove(&shape_id)
                            && let Some(router) = exec.families.get_mut(&key_cols)
                        {
                            // Drop the shape from its key's routing list (the shape stream is torn
                            // down elsewhere); discard the router once it routes to no shapes.
                            if let Some(routed) = router.index.get_mut(&key_tuple) {
                                routed.retain(|rs| rs.num_id != num_id);
                                if routed.is_empty() {
                                    router.index.remove(&key_tuple);
                                }
                            }
                            if router.index.is_empty() {
                                exec.families.remove(&key_cols);
                            }
                        }
                    }
                    emitted.remove(&shape_id);
                    publish_all(&execs, &offset, &emitted, &stats, &node_states, &subq.registry, &trace_tx).await;
                }
                Some(SequencerCmd::DumpNode { table, node_id, resp }) => {
                    let val = execs.get(&table).and_then(|exec| dump_node_json(exec, &offset, &emitted, &node_id));
                    let _ = resp.send(val);
                }
                None => break,
            },
            res = ds.read(crate::CHANGES_STREAM, &off, true) => match res {
                Ok(rr) => {
                    let next = rr.next_offset.clone();
                    if let Some(n) = rr.next_offset { offset = n; }
                    // Split the read batch into transactions (runs of equal (txid, lsn) — the
                    // ingestor appends whole commits contiguously, in commit order) and flush each
                    // transaction's appends before processing the next: atomic per-transaction
                    // emission, across tables.
                    let envs = rr.envelopes;
                    let mut touched = false;
                    let mut i = 0;
                    while i < envs.len() {
                        let txid = envs[i].headers.txid.clone();
                        let lsn = envs[i].headers.lsn.clone();
                        let mut j = i + 1;
                        while j < envs.len() && envs[j].headers.txid == txid && envs[j].headers.lsn == lsn {
                            j += 1;
                        }
                        let mut txn_pending: HashMap<String, Vec<Envelope>> = HashMap::new();
                        for env in envs[i..j].iter() {
                            // Skip redelivered changes (see `highwater` above).
                            let pos = match (env.headers.lsn.as_deref(), env.headers.seq) {
                                (Some(l), Some(seq)) => Some((crate::pg::lsn_to_u64(l), seq)),
                                _ => None,
                            };
                            if let (Some(p), Some(hw)) = (pos, highwater) {
                                if p <= hw {
                                    tracing::debug!("sequencer: skipping duplicate change at {p:?}");
                                    continue;
                                }
                            }
                            let Some(exec) = exec_for(&mut execs, &tables, &env.type_) else {
                                tracing::error!("sequencer: change for unknown table '{}'", env.type_);
                                if let Some(p) = pos { highwater = Some(p); }
                                continue;
                            };
                            // Buffer for in-flight creations on this table: their `BeginShape` was
                            // acknowledged before the creator's snapshot, so everything the
                            // snapshot cannot contain lands in the buffer.
                            for pending in exec.pending.values_mut() {
                                pending.buffered.push(env.clone());
                            }
                            if let Err(e) = process_envelope(
                                &exec.ts, &exec.shapes, &exec.shape_index, &exec.families,
                                &mut exec.aggregates, env.clone(), &mut txn_pending, &subq, &trace_tx,
                            )
                            .await
                            {
                                tracing::error!("process_envelope failed: {e:#}");
                            }
                            exec.envelopes_total += 1;
                            touched = true;
                            if let Some(p) = pos {
                                highwater = Some(p);
                            }
                        }
                        emit_storage_txn_metrics(&txn_pending);
                        for (path, envs) in &txn_pending {
                            *emitted.entry(sid_of_path(path).to_string()).or_insert(0) += envs.len() as u64;
                        }
                        // Transaction boundary: every append of this commit lands before the next
                        // commit is processed.
                        flush_pending(&ds, txn_pending).await;
                        i = j;
                    }
                    // Publish the processed offset only after the whole batch is fanned out + flushed.
                    if let Some(n) = next {
                        *processed.lock().unwrap() = n.clone();
                        if n != ckpt_offset && last_ckpt.elapsed() >= std::time::Duration::from_secs(2) {
                            ckpt_offset = n.clone();
                            last_ckpt = std::time::Instant::now();
                            let _ = catalog_tx.send(CatalogEvent::Offset { offset: n });
                        }
                    }
                    if touched {
                        publish_all(&execs, &offset, &emitted, &stats, &node_states, &subq.registry, &trace_tx).await;
                    }
                }
                Err(e) => {
                    tracing::warn!("sequencer read error on {}: {e:#}; backing off", crate::CHANGES_STREAM);
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
            },
        }
    }
}

/// Make a pending shape live: register its routing, then replay its buffered deltas through the
/// snapshot gate — emitting exactly the changes the backfill snapshot did not see. The buffered
/// replay is appended before the sequencer processes any further change, so the shape stream stays
/// in commit order.
#[allow(clippy::too_many_arguments)]
async fn activate_shape(
    ds: &DsClient,
    execs: &mut HashMap<String, TableExec>,
    table: &str,
    shape_id: &str,
    gate: crate::pg::SnapshotGate,
    agg_seed: Vec<Row>,
    emitted_seed: u64,
    emitted: &mut HashMap<String, u64>,
) -> Result<()> {
    let exec = execs.get_mut(table).with_context(|| format!("no executor for table '{table}'"))?;
    let p = exec
        .pending
        .remove(shape_id)
        .with_context(|| format!("no pending shape '{shape_id}' (aborted?)"))?;
    if emitted_seed > 0 {
        emitted.insert(shape_id.to_string(), emitted_seed);
    }
    match p.kind {
        CreateKind::Plain => {
            // Register routing first (an equality template joins/creates its family's KeyRouter;
            // everything else is a standalone indexed filter)...
            match p.pred.equality_template() {
                Some(pairs) => {
                    let key_cols: Vec<usize> = pairs.iter().map(|(c, _)| *c).collect();
                    let key_tuple = Row(pairs.into_iter().map(|(_, v)| v).collect());
                    let router = exec
                        .families
                        .entry(key_cols.clone())
                        .or_insert_with(|| KeyRouter { key_cols: key_cols.clone(), index: HashMap::new() });
                    router.index.entry(key_tuple.clone()).or_default().push(RoutedShape {
                        num_id: p.num_id,
                        stream_path: p.stream_path.clone(),
                        gate: gate.clone(),
                        out_cols: p.out_cols.clone(),
                    });
                    exec.family_of.insert(shape_id.to_string(), (key_cols, p.num_id, key_tuple));
                }
                None => {
                    exec.shape_index.insert(shape_id, &p.pred);
                    exec.shapes.insert(
                        shape_id.to_string(),
                        StandaloneShape {
                            pred: p.pred.clone(),
                            stream_path: p.stream_path.clone(),
                            gate: gate.clone(),
                            out_cols: p.out_cols.clone(),
                        },
                    );
                }
            }
            // ...then drain the buffer through the gate. `matches()` evaluates equality templates
            // and standalone predicates alike, so one replay path covers both placements.
            let mut outs: Vec<Envelope> = Vec::new();
            for env in &p.buffered {
                let Ok((delta, txid, lsn)) = apply_envelope(&exec.ts, env) else { continue };
                if delta.is_empty() {
                    continue;
                }
                let lsn_u64 = lsn.as_deref().map(crate::pg::lsn_to_u64).unwrap_or(0);
                let xid = txid.as_deref().and_then(|s| s.parse::<u64>().ok());
                if gate.should_skip(lsn_u64, xid) {
                    continue;
                }
                let matched = eval_standalone(&p.pred, &delta);
                if matched.is_empty() {
                    continue;
                }
                outs.extend(translate_output(
                    &exec.ts,
                    matched,
                    txid,
                    lsn,
                    p.out_cols.as_deref().map(Vec::as_slice),
                ));
            }
            if !outs.is_empty() {
                *emitted.entry(shape_id.to_string()).or_insert(0) += outs.len() as u64;
                ds.append_reliable(&p.stream_path, &outs).await;
            }
        }
        CreateKind::Aggregate { func, col } => {
            // Seed the fold from the backfill rows, emit the initial value, then fold the gated
            // buffer (emitting a value envelope whenever the aggregate moves).
            let mut agg = AggShape {
                pred: p.pred.clone(),
                func,
                col,
                stream_path: p.stream_path.clone(),
                gate: gate.clone(),
                count: 0,
                nn_count: 0,
                sum: 0.0,
                multiset: std::collections::BTreeMap::new(),
                last: None,
            };
            let seed: Vec<Tup2<Row, ZWeight>> = agg_seed.iter().map(|r| Tup2(r.clone(), 1)).collect();
            agg.apply(&seed);
            let mut outs = vec![agg.envelope(&exec.ts, None, None)];
            agg.last = Some(agg.value());
            for env in &p.buffered {
                let Ok((delta, txid, lsn)) = apply_envelope(&exec.ts, env) else { continue };
                if delta.is_empty() {
                    continue;
                }
                let lsn_u64 = lsn.as_deref().map(crate::pg::lsn_to_u64).unwrap_or(0);
                let xid = txid.as_deref().and_then(|s| s.parse::<u64>().ok());
                if gate.should_skip(lsn_u64, xid) {
                    continue;
                }
                if agg.apply(&delta) {
                    let val = agg.value();
                    if agg.last.as_ref() != Some(&val) {
                        agg.last = Some(val.clone());
                        outs.push(agg.envelope(&exec.ts, txid, lsn));
                    }
                }
            }
            *emitted.entry(shape_id.to_string()).or_insert(0) += outs.len() as u64;
            ds.append(&p.stream_path, &outs).await?;
            exec.aggregates.insert(shape_id.to_string(), agg);
        }
    }
    Ok(())
}

/// Deep-dump one node's internal state for `GET /state/node` (see `SequencerCmd::DumpNode`).
fn dump_node_json(
    exec: &TableExec,
    offset: &str,
    emitted: &HashMap<String, u64>,
    node_id: &str,
) -> Option<serde_json::Value> {
    if node_id.starts_with("family:") {
        return exec
            .families
            .values()
            .find(|r| family_node_id(&exec.ts, &r.key_cols) == node_id)
            .map(|r| dump_family_json(&exec.ts, r));
    }
    if let Some(sid) = node_id.strip_prefix("shape:").or_else(|| node_id.strip_prefix("filter:")) {
        if let Some(agg) = exec.aggregates.get(sid) {
            return Some(dump_aggregate_json(sid, agg));
        }
        if exec.shapes.contains_key(sid) || exec.family_of.contains_key(sid) {
            return Some(serde_json::json!({
                "kind": if node_id.starts_with("filter:") { "filter" } else { "shape" },
                "node": node_id,
                "emitted": emitted.get(sid).copied().unwrap_or(0),
            }));
        }
        return None;
    }
    if node_id == format!("table:{}", exec.ts.name) {
        return Some(serde_json::json!({
            "kind": "table",
            "node": node_id,
            "processedOffset": offset,
            "envelopes": exec.envelopes_total,
        }));
    }
    None
}

/// Replay the global change log from `from` for one dormant shape: apply each of its table's
/// envelopes through the shape's snapshot gate + predicate + projection and append the matches to
/// the retained stream. Pages until the log reports up-to-date. Appends are direct (`ds.append`):
/// a 404 means the retained stream vanished (evicted/purged mid-replay) and must fail the resume.
#[allow(clippy::too_many_arguments)]
async fn replay_changes_for_shape(
    ds: &DsClient,
    ts: &TableSchema,
    table: &str,
    pred: &CompiledPredicate,
    out_cols: Option<&Arc<Vec<usize>>>,
    gate: &crate::pg::SnapshotGate,
    stream_path: &str,
    from: &str,
) -> Result<u64> {
    let mut off = from.to_string();
    let mut emitted = 0u64;
    loop {
        let rr = ds.read(crate::CHANGES_STREAM, &off, false).await?;
        let mut outs: Vec<Envelope> = Vec::new();
        for env in &rr.envelopes {
            if env.type_ != table {
                continue;
            }
            let Ok((delta, txid, lsn)) = apply_envelope(ts, env) else { continue };
            if delta.is_empty() {
                continue;
            }
            let lsn_u64 = lsn.as_deref().map(crate::pg::lsn_to_u64).unwrap_or(0);
            let xid = txid.as_deref().and_then(|s| s.parse::<u64>().ok());
            if gate.should_skip(lsn_u64, xid) {
                continue;
            }
            let matched = eval_standalone(pred, &delta);
            if matched.is_empty() {
                continue;
            }
            outs.extend(translate_output(ts, matched, txid, lsn, out_cols.map(|c| c.as_slice())));
        }
        if !outs.is_empty() {
            emitted += outs.len() as u64;
            ds.append(stream_path, &outs).await.context("append replay to retained stream")?;
        }
        match rr.next_offset {
            Some(n) if n != off => {
                off = n;
                if rr.up_to_date {
                    break;
                }
            }
            _ => break,
        }
    }
    Ok(emitted)
}

/// Creator-side half of the two-phase shape creation: await the pending-buffer ack, run the
/// Postgres backfill on a pooled connection (appending the snapshot for plain shapes), then
/// activate. The sequencer keeps processing other work the whole time — a slow backfill only
/// delays THIS shape. Returns the creation outcome (`Err(reason)` mirrors the old handshake).
#[allow(clippy::too_many_arguments)]
async fn backfill_and_activate(
    ds: &DsClient,
    pg_url: &Option<String>,
    cmd_tx: &mpsc::UnboundedSender<SequencerCmd>,
    ts: &TableSchema,
    table: &str,
    shape_id: &str,
    stream_path: &str,
    pred: &Arc<CompiledPredicate>,
    out_cols: Option<&Arc<Vec<usize>>>,
    changes_only: bool,
    is_aggregate: bool,
    ack_rx: tokio::sync::oneshot::Receiver<()>,
) -> std::result::Result<(), String> {
    let abort = || {
        let _ = cmd_tx.send(SequencerCmd::AbortShape {
            table: table.to_string(),
            shape_id: shape_id.to_string(),
        });
    };
    if ack_rx.await.is_err() {
        return Err("sequencer dropped the begin-shape ack".to_string());
    }
    // Backfill: current matching rows from a REPEATABLE READ snapshot, predicate pushed into the
    // SELECT; `matches()` is the final authority (a safety net if the SQL is ever a looser
    // superset). A `changes_only` feed skips the backfill and forwards only future matches
    // (passthrough gate) — the non-materialized live tail a subset query follows.
    let (gate, agg_seed, emitted_seed) = if changes_only {
        (crate::pg::SnapshotGate::passthrough(), Vec::new(), 0u64)
    } else {
        let t0 = std::time::Instant::now();
        let bf = match pg_backfill(pg_url, ts, Some(pred.as_ref())).await {
            Ok(bf) => bf,
            Err(e) => {
                abort();
                return Err(format!("{e:#}"));
            }
        };
        let make_new_ms = t0.elapsed().as_secs_f64() * 1000.0;
        if is_aggregate {
            // The sequencer seeds the fold and emits the initial value itself.
            (bf.gate, bf.rows, 0)
        } else {
            let out: Vec<(Row, ZWeight)> =
                bf.rows.iter().filter(|r| pred.matches(r)).map(|r| (r.clone(), 1)).collect();
            let rows = out.len() as u64;
            let mut snapshot_bytes = 0u64;
            let mut emitted_seed = 0u64;
            if !out.is_empty() {
                let envs = translate_output(ts, out, None, None, out_cols.map(|c| c.as_slice()));
                if crate::statsd::enabled() {
                    snapshot_bytes = envs_bytes(&envs);
                }
                if let Err(e) = ds.append(stream_path, &envs).await {
                    abort();
                    return Err(format!("append snapshot: {e:#}"));
                }
                emitted_seed = envs.len() as u64;
            }
            crate::statsd::snapshot_stored(rows, snapshot_bytes, make_new_ms);
            (bf.gate, Vec::new(), emitted_seed)
        }
    };
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    if cmd_tx
        .send(SequencerCmd::ActivateShape {
            table: table.to_string(),
            shape_id: shape_id.to_string(),
            gate,
            agg_seed,
            emitted_seed,
            ready: ready_tx,
        })
        .is_err()
    {
        return Err("sequencer is gone".to_string());
    }
    ready_rx.await.unwrap_or_else(|_| Err("sequencer dropped the ready channel".to_string()))
}

/// Read a backfill snapshot from Postgres (current rows + snapshot LSN). `filter`, when given, is the
/// shape's predicate — backfill reads only matching rows instead of the whole table. Without a
/// `pg_url` (library/no-source mode) the shape simply starts empty.
async fn pg_backfill(
    pg_url: &Option<String>,
    ts: &TableSchema,
    filter: Option<&CompiledPredicate>,
) -> Result<crate::pg::Backfill> {
    match pg_url {
        Some(url) => {
            let client = crate::pg::pool_for(url).get().await?;
            crate::pg::backfill(&client, ts, filter).await
        }
        None => Ok(crate::pg::Backfill {
            rows: Vec::new(),
            seed_lsn: "0/0".to_string(),
            gate: crate::pg::SnapshotGate::passthrough(),
        }),
    }
}


#[allow(clippy::too_many_arguments)]
async fn process_envelope(
    ts: &TableSchema,
    shapes: &HashMap<String, StandaloneShape>,
    shape_index: &StandaloneIndex,
    families: &HashMap<Vec<usize>, KeyRouter>,
    aggregates: &mut HashMap<String, AggShape>,
    env: Envelope,
    pending: &mut HashMap<String, Vec<Envelope>>,
    subq: &SubqueryHandle,
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
) -> Result<()> {
    let (delta, txid, lsn) = apply_envelope(ts, &env)?;
    if delta.is_empty() {
        return Ok(());
    }
    // Per-envelope trace collection (hops, reached shape ids). `None` when nobody is subscribed,
    // so the untraced hot path pays only this one atomic load — see `crate::trace`.
    let mut tr: Option<(Vec<crate::trace::TraceHop>, Vec<String>)> = if trace_tx.receiver_count() > 0 {
        Some((vec![crate::trace::TraceHop::new(format!("table:{}", ts.name), "passed")], Vec::new()))
    } else {
        None
    };
    // `lsn` (the commit-LSN string) is stamped onto output envelopes so a subset client can position
    // its live tail at the page snapshot (drop deltas with `lsn < snapshot_lsn`); `lsn_u64` is the
    // numeric fallback for the per-shape backfill-skip compare, and `xid` (the transaction id the
    // ingestor stamps as `txid`) is the primary fence — see `pg::SnapshotGate` for why xid visibility,
    // not LSN order, is the sound backfill↔replication reconciliation.
    let lsn_u64 = lsn.as_deref().map(crate::pg::lsn_to_u64).unwrap_or(0);
    let xid = txid.as_deref().and_then(|s| s.parse::<u64>().ok());
    metrics().envelopes.fetch_add(1, Ordering::Relaxed);
    let _t = Timer::new(&metrics().process_envelope);
    // Standalone shapes: evaluate each stateless filter directly on the delta (no thread, no clone).
    // Skip changes already visible to the shape's backfill snapshot (xid-visibility gate, LSN
    // fallback for changes without a parseable xid). On the untraced hot path only the index's
    // candidates are visited (a non-candidate's necessary conjunct fails, so it cannot match);
    // with a trace subscriber the full scan is kept so every filter node still reports a hop.
    let candidate_ids;
    let candidates: Box<dyn Iterator<Item = (&String, &StandaloneShape)>> = if tr.is_some() {
        Box::new(shapes.iter())
    } else {
        candidate_ids = shape_index.candidates(&delta);
        Box::new(candidate_ids.iter().filter_map(|sid| shapes.get_key_value(sid)))
    };
    for (sid, shape) in candidates {
        if shape.gate.should_skip(lsn_u64, xid) {
            if let Some((hops, _)) = tr.as_mut() {
                hops.push(crate::trace::TraceHop::new(format!("filter:{sid}"), "dropped"));
            }
            continue;
        }
        let out = eval_standalone(&shape.pred, &delta);
        if out.is_empty() {
            if let Some((hops, _)) = tr.as_mut() {
                hops.push(crate::trace::TraceHop::new(format!("filter:{sid}"), "dropped"));
            }
            continue;
        }
        if let Some((hops, ids)) = tr.as_mut() {
            hops.push(crate::trace::TraceHop::new(format!("filter:{sid}"), "passed"));
            hops.push(crate::trace::TraceHop::new(format!("shape:{sid}"), "passed"));
            ids.push(sid.clone());
        }
        let envs =
            translate_output(ts, out, txid.clone(), lsn.clone(), shape.out_cols.as_deref().map(Vec::as_slice));
        pending.entry(shape.stream_path.clone()).or_default().extend(envs);
    }
    // Equality routers: route each delta row by its key to exactly the shapes registered on that key.
    // No table copy, no join state — membership is the key match (an equality-template predicate matches a
    // row iff its key equals the shape's constants). Each shape's own snapshot gate is applied, so
    // changes already in that shape's backfill are skipped.
    let _s = Timer::new(&metrics().family_step);
    for router in families.values() {
        type ShapeOut<'a> = (&'a str, Option<&'a [usize]>, Vec<(Row, ZWeight)>);
        let mut by_shape: HashMap<u64, ShapeOut> = HashMap::new();
        let mut routed_keys: Vec<Row> = Vec::new();
        for Tup2(row, w) in &delta {
            let key = key_of(row, &router.key_cols);
            let Some(routed) = router.index.get(&key) else { continue };
            if tr.is_some() && !routed_keys.contains(&key) {
                routed_keys.push(key);
            }
            for rs in routed {
                if rs.gate.should_skip(lsn_u64, xid) {
                    continue;
                }
                by_shape
                    .entry(rs.num_id)
                    .or_insert_with(|| (rs.stream_path.as_str(), rs.out_cols.as_deref().map(Vec::as_slice), Vec::new()))
                    .2
                    .push((row.clone(), *w));
            }
        }
        if let Some((hops, ids)) = tr.as_mut() {
            // Node id matches the visualizer's logical graph: family:<table>:<key cols by name>.
            let cols = router
                .key_cols
                .iter()
                .map(|i| ts.columns.get(*i).map(|(n, _)| n.clone()).unwrap_or_else(|| format!("col{i}")))
                .collect::<Vec<_>>()
                .join(",");
            let node = format!("family:{}:{cols}", ts.name);
            if by_shape.is_empty() {
                hops.push(crate::trace::TraceHop::new(node, "dropped"));
            } else {
                for key in &routed_keys {
                    let key_json = serde_json::Value::Array(key.0.iter().map(crate::value::Value::to_json).collect());
                    hops.push(crate::trace::TraceHop::routed(node.clone(), key_json));
                }
                for num_id in by_shape.keys() {
                    let sid = format!("s{num_id}");
                    hops.push(crate::trace::TraceHop::new(format!("shape:{sid}"), "passed"));
                    ids.push(sid);
                }
            }
        }
        if by_shape.is_empty() {
            continue;
        }
        metrics().family_steps.fetch_add(1, Ordering::Relaxed);
        for (_sid, (stream_path, out_cols, rows)) in by_shape {
            let envs = translate_output(ts, rows, txid.clone(), lsn.clone(), out_cols);
            if !envs.is_empty() {
                pending.entry(stream_path.to_string()).or_default().extend(envs);
            }
        }
    }
    // Subquery shapes/nodes: route this delta through the cross-table registry. Under the lock it
    // updates the shared inner-set nodes (in-memory) and emits outer-shape deltas; the flip-driven
    // Postgres query-backs are handed to the engine's flip-propagator task so they never block
    // this tailer. The convergence barrier is processed offsets + a drained flip queue
    // (`pending_flips == 0`).
    {
        let mut work = std::collections::VecDeque::new();
        {
            let mut reg = subq.registry.lock().await;
            if reg.touches(&ts.name) {
                let mut sq_hops: Option<Vec<crate::trace::TraceHop>> = tr.as_ref().map(|_| Vec::new());
                work = reg.on_table_delta(ts, &delta, lsn_u64, xid, txid.clone(), sq_hops.as_mut()).await?;
                if let (Some((hops, ids)), Some(sq)) = (tr.as_mut(), sq_hops) {
                    for h in &sq {
                        if h.outcome == "passed"
                            && let Some(sid) = h.node.strip_prefix("shape:")
                            && !ids.iter().any(|i| i == sid)
                        {
                            ids.push(sid.to_string());
                        }
                    }
                    hops.extend(sq);
                }
            }
        }
        if !work.is_empty() {
            subq.pending_flips.fetch_add(1, Ordering::SeqCst);
            if subq.flip_tx.send(FlipWork { work, txid: txid.clone() }).is_err() {
                // Propagator gone (shutdown) — don't leave the barrier stuck.
                subq.pending_flips.fetch_sub(1, Ordering::SeqCst);
            }
        }
    }
    // Scalar aggregations: fold this delta into each running aggregate; emit the new value when it
    // changes. Skips changes already counted in the seed (the aggregate's snapshot gate).
    for (sid, agg) in aggregates.iter_mut() {
        if agg.gate.should_skip(lsn_u64, xid) {
            if let Some((hops, _)) = tr.as_mut() {
                hops.push(crate::trace::TraceHop::new(format!("shape:{sid}"), "dropped"));
            }
            continue;
        }
        let mut folded = false;
        if agg.apply(&delta) {
            let val = agg.value();
            if agg.last.as_ref() != Some(&val) {
                agg.last = Some(val.clone());
                let env = agg.envelope(ts, txid.clone(), lsn.clone());
                pending.entry(agg.stream_path.clone()).or_default().push(env);
                folded = true;
            }
        }
        if let Some((hops, ids)) = tr.as_mut() {
            hops.push(crate::trace::TraceHop::new(format!("shape:{sid}"), if folded { "folded" } else { "dropped" }));
            if folded {
                ids.push(sid.clone());
            }
        }
    }
    // Publish the trace event (serialize once; lossy send — see `crate::trace`).
    if let Some((hops, shape_ids)) = tr {
        let ev = crate::trace::TraceEvent {
            lsn: lsn.clone(),
            txid: txid.clone(),
            table: ts.name.clone(),
            delta: delta
                .iter()
                .take(crate::trace::DELTA_CAP)
                .map(|Tup2(row, w)| crate::trace::TraceDelta { row: ts.row_to_json(row), w: *w })
                .collect(),
            hops,
            shapes: shape_ids,
        };
        if let Ok(json) = serde_json::to_string(&ev) {
            let _ = trace_tx.send(Arc::new(json));
        }
    }
    Ok(())
}

/// Total serialized byte size of a set of output envelopes (for storage/snapshot byte metrics).
fn envs_bytes(envs: &[Envelope]) -> u64 {
    envs.iter().map(|e| serde_json::to_string(e).map(|s| s.len() as u64).unwrap_or(0)).sum()
}

/// Emit the per-source-transaction storage StatsD metrics from one txn's staged appends.
/// `affected_shape_count` = distinct shape streams the txn touched; `operations`/`bytes` = output
/// envelopes appended + their serialized size. (Subquery-registry appends go out synchronously inside
/// `process_envelope` and are not reflected here.) No-op when the txn produced no appends.
fn emit_storage_txn_metrics(txn_pending: &HashMap<String, Vec<Envelope>>) {
    let ops: u64 = txn_pending.values().map(|v| v.len() as u64).sum();
    if ops == 0 {
        return;
    }
    let bytes: u64 = txn_pending
        .values()
        .flatten()
        .map(|e| serde_json::to_string(e).map(|s| s.len() as u64).unwrap_or(0))
        .sum();
    crate::statsd::storage_txn(ops, bytes, txn_pending.len() as u64);
}

/// Flush the batch's staged appends, bounded-concurrently. Each envelope keeps its own txid, so
/// `awaitTxId` semantics are preserved; only the HTTP round-trips are coalesced + parallelized.
///
/// Appends are **reliable**: transient failures retry with backoff (`append_reliable`) rather than
/// being dropped — a lost shape append is a permanent divergence for that shape's subscribers, and
/// the tailer's processed-offset barrier (published after this returns) must mean "every subscriber
/// stream reflects the batch". The only non-retried case is a 404 (the shape was dropped mid-flush),
/// which discards cleanly.
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
                ds.append_reliable(&path, &envs).await;
                metrics().shape_appends.fetch_add(1, Ordering::Relaxed);
            });
        }
        while set.join_next().await.is_some() {}
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
pub(crate) fn translate_output(
    ts: &TableSchema,
    out: Vec<(Row, ZWeight)>,
    txid: Option<String>,
    lsn: Option<String>,
    out_cols: Option<&[usize]>,
) -> Vec<Envelope> {
    let mut pos: HashMap<String, Row> = HashMap::new();
    let mut neg: HashSet<String> = HashSet::new();
    for (row, w) in out {
        let pk = match ts.key_string(&row) {
            Ok(pk) => pk,
            Err(e) => {
                tracing::warn!("translate_output: dropping row with unextractable pk on table {}: {e:#}", ts.name);
                continue;
            }
        };
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
            value: Some(ts.row_to_json_cols(row, out_cols)),
            old: None,
            headers: EnvelopeHeaders { operation: "upsert".into(), txid: txid.clone(), offset: None, lsn: lsn.clone(), seq: None },
        });
    }
    // TEST-ONLY: the `drop_deletes` fault suppresses "leave" envelopes so rows that exit a shape
    // linger in the client. No-op unless ELECTRIC_IVM_FAULT=drop_deletes (see `fault`).
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
            headers: EnvelopeHeaders { operation: "delete".into(), txid: txid.clone(), offset: None, lsn: lsn.clone(), seq: None },
        });
    }
    envs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{TableDef, TableSchema};

    /// The candidate set must contain every standalone shape that could match any row of the
    /// delta (old or new side), and exclude shapes whose necessary conjunct fails on all rows.
    #[test]
    fn standalone_index_candidates() {
        let def: TableDef = serde_json::from_value(serde_json::json!({
            "columns": { "id": {"type":"int"}, "name": {"type":"text"}, "age": {"type":"int"}, "active": {"type":"bool"} },
            "primaryKey": "id"
        }))
        .unwrap();
        let ts = TableSchema::from_def("users", &def).unwrap();
        let compile = |j: serde_json::Value| {
            Arc::new(
                CompiledPredicate::compile_opt(Some(&serde_json::from_value(j).unwrap()), &ts).unwrap(),
            )
        };
        let mut idx = StandaloneIndex::default();
        idx.insert("eq_a", &compile(serde_json::json!({"col":"name","op":"eq","value":"a"})));
        idx.insert("gt_18", &compile(serde_json::json!({"col":"age","op":"gt","value":18})));
        idx.insert("gte_18", &compile(serde_json::json!({"col":"age","op":"gte","value":18})));
        idx.insert("lt_10", &compile(serde_json::json!({"col":"age","op":"lt","value":10})));
        idx.insert("neq_b", &compile(serde_json::json!({"col":"name","op":"neq","value":"b"}))); // fallback scan

        let row = |name: &str, age: i64| {
            ts.row_from_json(
                serde_json::json!({"id":1,"name":name,"age":age,"active":true}).as_object().unwrap(),
            )
            .unwrap()
        };
        fn cand(idx: &StandaloneIndex, delta: &[Tup2<Row, ZWeight>]) -> Vec<String> {
            let mut c = idx.candidates(delta);
            c.sort();
            c
        }

        // age = 18 satisfies gte (non-strict) but not gt (strict); name 'a' hits the eq bucket;
        // the un-indexable neq shape is always a candidate.
        assert_eq!(cand(&idx, &[Tup2(row("a", 18), 1)]), vec!["eq_a", "gte_18", "neq_b"]);
        // age = 25 satisfies both lower bounds; name 'z' misses the eq bucket.
        assert_eq!(cand(&idx, &[Tup2(row("z", 25), 1)]), vec!["gt_18", "gte_18", "neq_b"]);
        // age = 5 satisfies only the upper bound.
        assert_eq!(cand(&idx, &[Tup2(row("z", 5), 1)]), vec!["lt_10", "neq_b"]);
        // An update whose OLD row matches a shape must surface it (the retraction side).
        assert_eq!(cand(&idx, &[Tup2(row("a", 18), -1), Tup2(row("z", 5), 1)]), vec![
            "eq_a", "gte_18", "lt_10", "neq_b"
        ]);
        // A NULL cell satisfies no comparison conjunct.
        let null_age = ts
            .row_from_json(serde_json::json!({"id":1,"name":null,"age":null,"active":true}).as_object().unwrap())
            .unwrap();
        assert_eq!(cand(&idx, &[Tup2(null_age, 1)]), vec!["neq_b"]);

        // Removal unindexes both indexed and fallback shapes.
        idx.remove("eq_a");
        idx.remove("neq_b");
        assert_eq!(cand(&idx, &[Tup2(row("a", 18), 1)]), vec!["gte_18"]);
    }

    /// A SubqueryHandle over a fresh registry, with a live propagator task (tests run in tokio).
    fn test_subq() -> SubqueryHandle {
        let registry =
            Arc::new(Mutex::new(SubqueryRegistry::new(DsClient::new("http://127.0.0.1:1"), None)));
        let (flip_tx, flip_rx) = mpsc::unbounded_channel();
        let pending_flips = Arc::new(std::sync::atomic::AtomicI64::new(0));
        let (trace_tx, _) = tokio::sync::broadcast::channel(16);
        spawn_flip_propagator(registry.clone(), flip_rx, pending_flips.clone(), trace_tx);
        SubqueryHandle { registry, flip_tx, pending_flips }
    }

    fn agg_shape(func: AggFn, col: Option<usize>, ts: &TableSchema) -> AggShape {
        let pred = Arc::new(CompiledPredicate::compile_opt(None, ts).unwrap());
        AggShape {
            pred,
            func,
            col,
            stream_path: "shape/s9".into(),
            gate: crate::pg::SnapshotGate::passthrough(),
            count: 0,
            nn_count: 0,
            sum: 0.0,
            multiset: std::collections::BTreeMap::new(),
            last: None,
        }
    }

    /// `build_node_states` yields one summary per node in the trace/graph id namespace: the table
    /// source, filter+shape per standalone, the family router under its column-NAME id, family
    /// member shapes, and aggregate folds with their live value.
    #[test]
    fn node_states_cover_every_node_kind() {
        let ts = users();
        let pred = Arc::new(
            CompiledPredicate::compile_opt(
                Some(&serde_json::from_value(serde_json::json!({"col":"active","op":"eq","value":true})).unwrap()),
                &ts,
            )
            .unwrap(),
        );

        let mut shapes = HashMap::new();
        shapes.insert(
            "s1".to_string(),
            StandaloneShape {
                pred: pred.clone(),
                stream_path: "shape/s1".into(),
                gate: crate::pg::SnapshotGate::passthrough(),
                out_cols: None,
            },
        );
        let mut families = HashMap::new();
        let key_cols = vec![ts.column_index("active").unwrap()];
        let mut index = HashMap::new();
        index.insert(
            Row(vec![Value::Bool(true)]),
            vec![RoutedShape {
                num_id: 2,
                stream_path: "shape/s2".into(),
                gate: crate::pg::SnapshotGate::passthrough(),
                out_cols: None,
            }],
        );
        families.insert(key_cols.clone(), KeyRouter { key_cols: key_cols.clone(), index });
        let mut family_of = HashMap::new();
        family_of.insert("s2".to_string(), (key_cols, 2u64, Row(vec![Value::Bool(true)])));

        let mut aggregates = HashMap::new();
        let mut agg = agg_shape(AggFn::Count, None, &ts);
        agg.apply(&[Tup2(Row(vec![Value::Int(1), Value::Text("a".into()), Value::Bool(true)]), 1)]);
        aggregates.insert("s3".to_string(), agg);

        let mut emitted = HashMap::new();
        emitted.insert("s1".to_string(), 4u64);
        emitted.insert("s2".to_string(), 7u64);

        let m = build_node_states(&ts, "12", 42, &shapes, &families, &family_of, &aggregates, &emitted);

        assert_eq!(
            m["table:users"],
            NodeStateSummary::Table { processed_offset: "12".into(), envelopes: 42 }
        );
        assert_eq!(m["filter:s1"], NodeStateSummary::Filter { emitted: 4 });
        assert_eq!(m["shape:s1"], NodeStateSummary::Shape { emitted: 4 });
        assert_eq!(m["family:users:active"], NodeStateSummary::Family { keys: 1, shapes: 1 });
        assert_eq!(m["shape:s2"], NodeStateSummary::Shape { emitted: 7 });
        match &m["shape:s3"] {
            NodeStateSummary::Aggregate { value, count, .. } => {
                assert_eq!(value, &serde_json::json!(1));
                assert_eq!(*count, 1);
            }
            other => panic!("expected aggregate summary, got {other:?}"),
        }
    }

    /// The exploded circuit decomposition is internally consistent: every edge endpoint is an
    /// emitted operator, every hop is a trace-hop id, every `state` is a `GET /state` key, shared
    /// structures (family, subquery node) are emitted once, and each strategy decomposes into its
    /// real steps.
    #[test]
    fn circuit_ops_decompose_every_strategy() {
        let gs = |id: &str, table: &str, fam: Option<Vec<&str>>, sq: bool, agg: Option<AggFn>| GraphShape {
            id: id.into(),
            table: table.into(),
            stream_path: format!("shape/{id}"),
            changes_only: false,
            where_: None,
            columns: None,
            family_key: fam.map(|v| v.iter().map(|s| s.to_string()).collect()),
            is_subquery: sq,
            aggregate: agg.map(|func| AggInfo { func, col: None }),
            state: Some("active"),
        };
        let tables = vec!["users".to_string(), "orders".to_string()];
        let shapes = vec![
            gs("s1", "users", None, false, None),                    // standalone
            gs("s2", "users", Some(vec!["active"]), false, None),    // family member 1
            gs("s3", "users", Some(vec!["active"]), false, None),    // family member 2 (shared ops)
            gs("s4", "users", None, true, None),                     // subquery shape
            gs("s5", "users", None, false, Some(AggFn::Count)),      // aggregate
        ];
        let nodes = vec![GraphNode {
            sig: "orders|user_id|".into(),
            inner_table: "orders".into(),
            proj_col: "user_id".into(),
            distinct_values: 0,
            refcount: 1,
        }];
        let sq_edges = vec![GraphEdge {
            node_sig: "orders|user_id|".into(),
            dependent_kind: "shape".into(),
            dependent_id: "s4".into(),
            connecting_col: "id".into(),
            negated: false,
        }];
        let (ops, edges) = circuit_ops(&tables, &shapes, &nodes, &sq_edges);

        let ids: HashSet<&str> = ops.iter().map(|o| o.id.as_str()).collect();
        // Every edge endpoint exists.
        for e in &edges {
            assert!(ids.contains(e.source.as_str()), "dangling source {}", e.source);
            assert!(ids.contains(e.target.as_str()), "dangling target {}", e.target);
        }
        // Strategy decompositions.
        for want in [
            "src:users", "d:users", // table
            "sigma:s1", "pi:s1", "snk:s1", // standalone
            "key:users:active", "arr:users:active", "rjoin:users:active", "snk:s2", "snk:s3", // family
            "sj:s4", "snk:s4", // subquery shape
            "sigma:s5", "fold:s5", "snk:s5", // aggregate
            "sqf:orders|user_id|", "dist:orders|user_id|", // inner set
        ] {
            assert!(ids.contains(want), "missing operator {want}");
        }
        // Shared family ops emitted once despite two members.
        assert_eq!(ops.iter().filter(|o| o.id == "arr:users:active").count(), 1);
        // Hop ids use the trace namespace; state ids point at real summaries.
        let arr = ops.iter().find(|o| o.id == "arr:users:active").unwrap();
        assert_eq!(arr.hop, "family:users:active");
        assert_eq!(arr.state.as_deref(), Some("family:users:active"));
        let fold = ops.iter().find(|o| o.id == "fold:s5").unwrap();
        assert_eq!(fold.hop, "shape:s5");
        assert_eq!(fold.state.as_deref(), Some("shape:s5"));
        let sigma1 = ops.iter().find(|o| o.id == "sigma:s1").unwrap();
        assert_eq!(sigma1.hop, "filter:s1");
        // The membership edge lands on the dependent's semijoin, dashed as a subquery stream.
        let dep = edges.iter().find(|e| e.source == "dist:orders|user_id|").unwrap();
        assert_eq!(dep.target, "sj:s4");
        assert_eq!(dep.kind, "subquery");
        // The params arrangement feeds the route join as a state edge.
        assert!(edges.iter().any(|e| e.source == "arr:users:active" && e.target == "rjoin:users:active" && e.kind == "state"));
    }

    /// Wire format: summaries are kind-tagged camelCase objects, and a `StateEvent` wraps them
    /// under `{"type":"state","nodes":{…}}` (the tag the visualizer switches on).
    #[test]
    fn state_summary_and_event_serialize_kind_tagged() {
        let s = NodeStateSummary::Aggregate {
            value: serde_json::json!(3.5),
            count: 4,
            nn_count: 2,
            multiset_len: 2,
        };
        let v = serde_json::to_value(&s).unwrap();
        assert_eq!(v["kind"], "aggregate");
        assert_eq!(v["nnCount"], 2);
        assert_eq!(v["multisetLen"], 2);

        let mut nodes = HashMap::new();
        nodes.insert("shape:s1".to_string(), NodeStateSummary::Shape { emitted: 9 });
        let ev = serde_json::to_value(crate::trace::StateEvent::new(nodes)).unwrap();
        assert_eq!(ev["type"], "state");
        assert_eq!(ev["nodes"]["shape:s1"]["kind"], "shape");
        assert_eq!(ev["nodes"]["shape:s1"]["emitted"], 9);
    }

    /// Deep dumps: a family router dumps its routing index (key tuple -> shape ids); a MIN/MAX
    /// aggregate dumps its fold internals including the retraction multiset.
    #[test]
    fn dump_node_family_and_aggregate() {
        let ts = users();
        let mut index = HashMap::new();
        index.insert(
            Row(vec![Value::Bool(true)]),
            vec![RoutedShape {
                num_id: 5,
                stream_path: "shape/s5".into(),
                gate: crate::pg::SnapshotGate::passthrough(),
                out_cols: None,
            }],
        );
        let router = KeyRouter { key_cols: vec![ts.column_index("active").unwrap()], index };
        let v = dump_family_json(&ts, &router);
        assert_eq!(v["kind"], "family");
        assert_eq!(v["node"], "family:users:active");
        assert_eq!(v["keyCols"][0], "active");
        assert_eq!(v["entries"][0]["key"][0], true);
        assert_eq!(v["entries"][0]["shapes"][0], "s5");
        assert_eq!(v["truncated"], false);

        let mut agg = agg_shape(AggFn::Max, Some(0), &ts);
        agg.apply(&[
            Tup2(Row(vec![Value::Int(7), Value::Text("a".into()), Value::Bool(true)]), 1),
            Tup2(Row(vec![Value::Int(3), Value::Text("b".into()), Value::Bool(true)]), 1),
        ]);
        let v = dump_aggregate_json("s9", &agg);
        assert_eq!(v["kind"], "aggregate");
        assert_eq!(v["value"], 7);
        assert_eq!(v["count"], 2);
        assert_eq!(v["multisetLen"], 2);
        assert_eq!(v["multiset"][0]["value"], 3);
        assert_eq!(v["multiset"][0]["weight"], 1);
    }

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
            headers: EnvelopeHeaders { operation: op.into(), txid: None, offset: None, lsn: None, seq: None },
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
        let envs = translate_output(&ts, eval_standalone(&pred, &delta), None, None, None);
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].headers.operation, "upsert");
        assert_eq!(envs[0].key, "1");

        // update within shape (name change, still active) -> upsert with new value
        let (delta, _, _) = apply_envelope(&ts, &env("update", "1", Some(serde_json::json!({"id":1,"name":"a2","active":true})), Some(serde_json::json!({"id":1,"name":"a","active":true})))).unwrap();
        let envs = translate_output(&ts, eval_standalone(&pred, &delta), None, None, None);
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].headers.operation, "upsert");
        assert_eq!(envs[0].value.as_ref().unwrap()["name"], "a2");

        // leave: becomes inactive -> delete envelope
        let (delta, _, _) = apply_envelope(&ts, &env("update", "1", Some(serde_json::json!({"id":1,"name":"a2","active":false})), Some(serde_json::json!({"id":1,"name":"a2","active":true})))).unwrap();
        let envs = translate_output(&ts, eval_standalone(&pred, &delta), None, None, None);
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].headers.operation, "delete");
        assert_eq!(envs[0].key, "1");

        // a non-matching insert produces no shape envelope
        let (delta, _, _) = apply_envelope(&ts, &env("insert", "2", Some(serde_json::json!({"id":2,"name":"b","active":false})), None)).unwrap();
        let envs = translate_output(&ts, eval_standalone(&pred, &delta), None, None, None);
        assert_eq!(envs.len(), 0);
    }

    /// The commit LSN is stamped onto output envelopes (upsert + delete) so a subset client can
    /// position its live tail at the page snapshot (see `docs/ARCHITECTURE.md` §7).
    #[test]
    fn translate_output_stamps_commit_lsn() {
        let ts = users();
        // upsert path: a positive-weight row carries the commit LSN.
        let out = vec![(Row(vec![crate::value::Value::Int(1), crate::value::Value::Text("a".into()), crate::value::Value::Bool(true)]), 1)];
        let envs = translate_output(&ts, out, Some("tx1".into()), Some("0/2A".into()), None);
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].headers.operation, "upsert");
        assert_eq!(envs[0].headers.lsn.as_deref(), Some("0/2A"));

        // delete path (purely negative weight) also carries the LSN.
        let out = vec![(Row(vec![crate::value::Value::Int(2), crate::value::Value::Text("b".into()), crate::value::Value::Bool(true)]), -1)];
        let envs = translate_output(&ts, out, None, Some("0/2A".into()), None);
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].headers.operation, "delete");
        assert_eq!(envs[0].headers.lsn.as_deref(), Some("0/2A"));

        // no LSN (backfill / library mode) -> none stamped.
        let out = vec![(Row(vec![crate::value::Value::Int(3), crate::value::Value::Text("c".into()), crate::value::Value::Bool(true)]), 1)];
        let envs = translate_output(&ts, out, None, None, None);
        assert_eq!(envs[0].headers.lsn, None);
    }

    /// The per-envelope trace reports the actual route: a family router hop (with the key) + the
    /// reached shape for a key match, a `dropped` family hop when no key matches, and a `dropped`
    /// filter hop for a standalone predicate that matches nothing.
    #[tokio::test]
    async fn trace_family_route_and_filter_drop() {
        let ts = users();
        // Columns are stored sorted: active(0), id(1), name(2).
        let name_idx = 2usize;

        // One family router on (name) with a single shape s7 registered on key 'a'.
        let mut families: HashMap<Vec<usize>, KeyRouter> = HashMap::new();
        let mut index: HashMap<Row, Vec<RoutedShape>> = HashMap::new();
        index.insert(
            Row(vec![Value::Text("a".into())]),
            vec![RoutedShape {
                num_id: 7,
                stream_path: "shape/s7".into(),
                gate: crate::pg::SnapshotGate::passthrough(),
                out_cols: None,
            }],
        );
        families.insert(vec![name_idx], KeyRouter { key_cols: vec![name_idx], index });

        // One standalone filter shape s9 whose predicate (active = false) won't match the inserts.
        let mut shapes: HashMap<String, StandaloneShape> = HashMap::new();
        shapes.insert(
            "s9".into(),
            StandaloneShape {
                pred: Arc::new(
                    CompiledPredicate::compile_opt(
                        Some(&serde_json::from_value(serde_json::json!({"col":"active","op":"eq","value":false})).unwrap()),
                        &ts,
                    )
                    .unwrap(),
                ),
                stream_path: "shape/s9".into(),
                gate: crate::pg::SnapshotGate::passthrough(),
                out_cols: None,
            },
        );

        let mut shape_index = StandaloneIndex::default();
        shape_index.insert("s9", &shapes["s9"].pred);

        let mut aggregates: HashMap<String, AggShape> = HashMap::new();
        let subqueries = test_subq();
        let (trace_tx, mut trace_rx) = tokio::sync::broadcast::channel::<Arc<String>>(16);
        let mut pending: HashMap<String, Vec<Envelope>> = HashMap::new();

        // Insert routed to key 'a' -> family hop routed with the key, shape s7 reached, filter s9 drops.
        process_envelope(
            &ts, &shapes, &shape_index, &families, &mut aggregates,
            env("insert", "1", Some(serde_json::json!({"id":1,"name":"a","active":true})), None),
            &mut pending, &subqueries, &trace_tx,
        )
        .await
        .unwrap();
        let ev: serde_json::Value = serde_json::from_str(&trace_rx.try_recv().unwrap()).unwrap();
        assert_eq!(ev["table"], "users");
        let hops = ev["hops"].as_array().unwrap();
        let hop = |node: &str| hops.iter().find(|h| h["node"] == node).unwrap_or_else(|| panic!("missing hop {node}: {hops:?}"));
        assert_eq!(hop("table:users")["outcome"], "passed");
        assert_eq!(hop("family:users:name")["outcome"], "routed");
        assert_eq!(hop("family:users:name")["key"][0], "a");
        assert_eq!(hop("shape:s7")["outcome"], "passed");
        assert_eq!(hop("filter:s9")["outcome"], "dropped");
        assert_eq!(ev["shapes"].as_array().unwrap(), &vec![serde_json::json!("s7")]);
        assert_eq!(ev["delta"][0]["w"], 1);
        assert_eq!(ev["delta"][0]["row"]["name"], "a");

        // Insert whose key matches no routed shape -> family hop dropped, no shapes reached.
        process_envelope(
            &ts, &shapes, &shape_index, &families, &mut aggregates,
            env("insert", "2", Some(serde_json::json!({"id":2,"name":"zzz","active":true})), None),
            &mut pending, &subqueries, &trace_tx,
        )
        .await
        .unwrap();
        let ev: serde_json::Value = serde_json::from_str(&trace_rx.try_recv().unwrap()).unwrap();
        let hops = ev["hops"].as_array().unwrap();
        let hop = |node: &str| hops.iter().find(|h| h["node"] == node).unwrap_or_else(|| panic!("missing hop {node}: {hops:?}"));
        assert_eq!(hop("family:users:name")["outcome"], "dropped");
        assert_eq!(hop("filter:s9")["outcome"], "dropped");
        assert!(ev["shapes"].as_array().unwrap().is_empty());

        // Nobody subscribed -> nothing is built or sent (receiver dropped).
        drop(trace_rx);
        process_envelope(
            &ts, &shapes, &shape_index, &families, &mut aggregates,
            env("insert", "3", Some(serde_json::json!({"id":3,"name":"a","active":true})), None),
            &mut pending, &subqueries, &trace_tx,
        )
        .await
        .unwrap();
        assert_eq!(trace_tx.receiver_count(), 0);
    }

    /// An aggregation shape appears in the trace as a `folded` hop when the delta moves its value,
    /// and `dropped` when the delta doesn't match its predicate.
    #[tokio::test]
    async fn trace_aggregate_fold() {
        let ts = users();
        let shapes: HashMap<String, StandaloneShape> = HashMap::new();
        let shape_index = StandaloneIndex::default();
        let families: HashMap<Vec<usize>, KeyRouter> = HashMap::new();
        let mut aggregates: HashMap<String, AggShape> = HashMap::new();
        aggregates.insert("s4".into(), agg(AggFn::Count, None)); // COUNT(*) WHERE active = true
        let subqueries = test_subq();
        let (trace_tx, mut trace_rx) = tokio::sync::broadcast::channel::<Arc<String>>(16);
        let mut pending: HashMap<String, Vec<Envelope>> = HashMap::new();

        process_envelope(
            &ts, &shapes, &shape_index, &families, &mut aggregates,
            env("insert", "1", Some(serde_json::json!({"id":1,"name":"a","active":true})), None),
            &mut pending, &subqueries, &trace_tx,
        )
        .await
        .unwrap();
        let ev: serde_json::Value = serde_json::from_str(&trace_rx.try_recv().unwrap()).unwrap();
        let hops = ev["hops"].as_array().unwrap();
        assert!(hops.iter().any(|h| h["node"] == "shape:s4" && h["outcome"] == "folded"), "{hops:?}");
        assert_eq!(ev["shapes"].as_array().unwrap(), &vec![serde_json::json!("s4")]);

        process_envelope(
            &ts, &shapes, &shape_index, &families, &mut aggregates,
            env("insert", "2", Some(serde_json::json!({"id":2,"name":"b","active":false})), None),
            &mut pending, &subqueries, &trace_tx,
        )
        .await
        .unwrap();
        let ev: serde_json::Value = serde_json::from_str(&trace_rx.try_recv().unwrap()).unwrap();
        let hops = ev["hops"].as_array().unwrap();
        assert!(hops.iter().any(|h| h["node"] == "shape:s4" && h["outcome"] == "dropped"), "{hops:?}");
    }

    fn agg(func: AggFn, col: Option<usize>) -> AggShape {
        let ts = users();
        let pred = Arc::new(
            CompiledPredicate::compile_opt(
                Some(&serde_json::from_value(serde_json::json!({ "col": "active", "op": "eq", "value": true })).unwrap()),
                &ts,
            )
            .unwrap(),
        );
        AggShape {
            pred,
            func,
            col,
            stream_path: "x".into(),
            gate: crate::pg::SnapshotGate::passthrough(),
            count: 0,
            nn_count: 0,
            sum: 0.0,
            multiset: std::collections::BTreeMap::new(),
            last: None,
        }
    }
    // Columns are stored sorted: active(0), id(1), name(2).
    fn active(id: i64) -> Row {
        Row(vec![Value::Bool(true), Value::Int(id), Value::Text("n".into())])
    }
    fn inactive(id: i64) -> Row {
        Row(vec![Value::Bool(false), Value::Int(id), Value::Text("n".into())])
    }

    /// COUNT over `active = true`, maintained incrementally through inserts, deletes, and predicate-
    /// crossing updates (a row moving in/out of the filter).
    #[test]
    fn aggregate_count_incremental() {
        let mut a = agg(AggFn::Count, None);
        a.apply(&vec![Tup2(active(1), 1), Tup2(active(2), 1), Tup2(inactive(3), 1)]);
        assert_eq!(a.value(), serde_json::json!(2)); // only the two active rows count

        a.apply(&vec![Tup2(active(1), -1), Tup2(active(4), 1)]); // one leaves, one enters
        assert_eq!(a.value(), serde_json::json!(2));

        a.apply(&vec![Tup2(inactive(3), -1), Tup2(active(3), 1)]); // update: crosses INTO the filter
        assert_eq!(a.value(), serde_json::json!(3));

        a.apply(&vec![Tup2(active(2), -1), Tup2(inactive(2), 1)]); // update: crosses OUT of the filter
        assert_eq!(a.value(), serde_json::json!(2));
    }

    /// SQL NULL semantics: aggregates ignore NULL values — COUNT(col) counts non-NULLs (COUNT(*)
    /// counts rows), AVG divides by the non-NULL count, MIN/MAX never surface NULL, and SUM/AVG over
    /// zero non-NULL values are NULL. Mirrors Postgres.
    #[test]
    fn aggregate_null_semantics() {
        // Columns sorted: active(0), id(1), name(2). A row with a NULL name / NULL id.
        let null_name = |id: i64| Row(vec![Value::Bool(true), Value::Int(id), Value::Null]);
        let null_id = Row(vec![Value::Bool(true), Value::Null, Value::Text("n".into())]);

        // COUNT(*) counts all matching rows; COUNT(name) only rows with non-NULL name.
        let mut star = agg(AggFn::Count, None);
        star.apply(&vec![Tup2(active(1), 1), Tup2(null_name(2), 1)]);
        assert_eq!(star.value(), serde_json::json!(2));
        let mut cnt_col = agg(AggFn::Count, Some(2));
        cnt_col.apply(&vec![Tup2(active(1), 1), Tup2(null_name(2), 1)]);
        assert_eq!(cnt_col.value(), serde_json::json!(1));

        // AVG over id where one row's aggregated column is NULL: denominator excludes it.
        let mut avg = agg(AggFn::Avg, Some(1));
        avg.apply(&vec![Tup2(active(10), 1), Tup2(active(20), 1), Tup2(null_id.clone(), 1)]);
        assert_eq!(avg.value(), serde_json::json!(15.0));

        // MIN ignores NULLs (never surfaces NULL as the extreme).
        let mut min = agg(AggFn::Min, Some(1));
        min.apply(&vec![Tup2(active(5), 1), Tup2(null_id.clone(), 1)]);
        assert_eq!(min.value(), serde_json::json!(5));

        // SUM over zero non-NULL values is NULL (not 0), matching SQL.
        let mut sum = agg(AggFn::Sum, Some(1));
        sum.apply(&vec![Tup2(null_id, 1)]);
        assert_eq!(sum.value(), serde_json::Value::Null);
    }

    /// MIN(id) over the filtered set restores the previous extreme on retraction (the multiset).
    #[test]
    fn aggregate_min_with_retraction() {
        let mut a = agg(AggFn::Min, Some(1)); // col 1 = id (sorted: active,id,name)
        a.apply(&vec![Tup2(active(5), 1), Tup2(active(3), 1), Tup2(active(8), 1)]);
        assert_eq!(a.value(), serde_json::json!(3));
        a.apply(&vec![Tup2(active(3), -1)]); // remove the current min → next-smallest surfaces
        assert_eq!(a.value(), serde_json::json!(5));
        let mut mx = agg(AggFn::Max, Some(1));
        mx.apply(&vec![Tup2(active(5), 1), Tup2(active(8), 1)]);
        assert_eq!(mx.value(), serde_json::json!(8));
        mx.apply(&vec![Tup2(active(8), -1)]);
        assert_eq!(mx.value(), serde_json::json!(5));
    }
}
