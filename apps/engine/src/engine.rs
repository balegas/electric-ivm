//! Engine orchestration: schema/shape registries and one tailer task per table. A tailer holds only
//! per-shape routing metadata (no table data): it fans each change out to standalone filters and to
//! equality shapes routed by key, and appends the filtered deltas (as State-Protocol envelopes) to
//! the shape streams. Shapes backfill from Postgres on registration; see `add_shape_routed`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use dbsp::ZWeight;
use dbsp::utils::Tup2;
use tokio::sync::{Mutex, mpsc};

use std::sync::atomic::Ordering;

use crate::ds::{DsClient, Envelope, EnvelopeHeaders};
use crate::metrics::{Timer, metrics};
use crate::predicate::{CompiledPredicate, PredicateJson};
use crate::schema::{Schema, TableSchema, compile_schema};
use crate::subquery::{SubqueryRegistry, predicate_has_subquery, referenced_tables};
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
    /// Set once the replication ingestor has been spawned, so `setup_postgres` stays idempotent.
    replicator_started: Arc<std::sync::atomic::AtomicBool>,
    /// Cross-table subquery registry: maintained inner-set nodes (shared by canonical signature) + the
    /// outer subquery shapes that depend on them. Every tailer routes its deltas here so an inner-table
    /// change moves outer rows. `None`-free; empty until a subquery shape is created.
    subqueries: Arc<Mutex<SubqueryRegistry>>,
}

struct EngineState {
    tables: HashMap<String, TableSchema>,
    tailers: HashMap<String, TailerHandle>,
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

#[derive(Clone, Debug)]
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
#[derive(Clone, Debug, serde::Serialize)]
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

/// The whole maintained dbsp pipeline at an instant: tables, shapes (with their routing placement), and
/// the shared subquery node/edge DAG. The visualizer derives family + subquery sharing from this.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineGraph {
    pub tables: Vec<String>,
    pub shapes: Vec<GraphShape>,
    pub subquery_nodes: Vec<GraphNode>,
    pub subquery_edges: Vec<GraphEdge>,
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
    AddShape {
        shape_id: String,
        num_id: u64,
        stream_path: String,
        pred: Arc<CompiledPredicate>,
        /// Output projection (column indices to emit), or `None` for the full row.
        out_cols: Option<Arc<Vec<usize>>>,
        /// Skip the Postgres backfill and emit only future matching changes (a non-materialized live
        /// "tail" feed). Used by subset queries: the page rows come from a `query_subset`, and this
        /// feed carries just the live deltas the client re-checks against the loaded view.
        changes_only: bool,
        /// Signalled once the shape is registered AND its backfill has been appended to the stream —
        /// `Ok(())` on success, `Err(reason)` if backfill/registration failed — so `create_shape` can
        /// return only after the snapshot is readable (the Electric adapter folds the stream
        /// immediately on create) and can propagate a failure instead of leaving a zombie shape.
        ready: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    },
    /// Register a scalar aggregation (COUNT/SUM/AVG/MIN/MAX) over `pred`, maintained incrementally.
    AddAggregate {
        shape_id: String,
        stream_path: String,
        pred: Arc<CompiledPredicate>,
        func: AggFn,
        col: Option<usize>,
        ready: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    },
    RemoveShape { shape_id: String },
}

impl Engine {
    pub fn new(ds: DsClient) -> Self {
        let subqueries = Arc::new(Mutex::new(SubqueryRegistry::new(ds.clone(), None)));
        Engine {
            ds,
            state: Arc::new(Mutex::new(EngineState {
                tables: HashMap::new(),
                tailers: HashMap::new(),
                shapes: HashMap::new(),
                next_shape_id: 1,
                feed_by_sig: HashMap::new(),
                feed_shares: HashMap::new(),
            })),
            pg_url: None,
            repl_lsn: Arc::new(std::sync::Mutex::new("0/0".to_string())),
            repl_sync: Arc::new(std::sync::atomic::AtomicI64::new(0)),
            replicator_started: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            subqueries,
        }
    }

    /// Engine in Postgres mode: data lives in Postgres, ingested via logical replication and read
    /// back for backfill. Call [`setup_postgres`](Self::setup_postgres) before serving.
    pub fn new_pg(ds: DsClient, pg_url: String) -> Self {
        let mut e = Self::new(ds.clone());
        e.pg_url = Some(pg_url.clone());
        e.subqueries = Arc::new(Mutex::new(SubqueryRegistry::new(ds, Some(pg_url))));
        e
    }

    /// Introspect the configured tables from Postgres, set `REPLICA IDENTITY FULL`, create the
    /// replication slot, register the schema, and start the replication ingestor. Idempotent: a second
    /// call re-introspects but will NOT spawn a second ingestor (two ingestors would fight for the slot).
    pub async fn setup_postgres(&self, tables: &[String], slot: &str, poll_ms: u64) -> Result<()> {
        let url = self.pg_url.clone().context("setup_postgres called without a pg_url")?;
        let client = crate::pg::connect(&url).await?;
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
            self.ds.ensure_stream(&format!("table/{t}")).await?;
            compiled.insert(t.clone(), ts);
        }
        crate::pg::ensure_slot(&client, slot).await?;
        self.state.lock().await.tables = compiled.clone();
        self.subqueries.lock().await.set_schemas(Arc::new(compiled.clone()));
        // Spawn the ingestor at most once, even if setup_postgres is called again.
        if self.replicator_started.swap(true, std::sync::atomic::Ordering::SeqCst) {
            tracing::warn!("setup_postgres called again; ingestor already running, not spawning another");
            return Ok(());
        }
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
        self.subqueries.lock().await.set_schemas(Arc::new(compiled.clone()));
        self.state.lock().await.tables = compiled;
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
        let ts = {
            let st = self.state.lock().await;
            st.tables.get(table).cloned().ok_or_else(|| anyhow::anyhow!("unknown table '{table}'"))?
        };
        let out_cols = resolve_columns(&ts, columns)?;
        let order = match order_by {
            Some((col, desc)) => Some((ts.column_index(&col)?, desc)),
            None => None,
        };
        // Subquery predicates are evaluated natively by Postgres in the one-shot query-back (no engine
        // subquery state needed for a non-live page); other predicates use the compiled-form emitter.
        let where_sql = match where_.as_ref() {
            Some(p) if crate::subquery::predicate_has_subquery(p) => Some(crate::sql::predicate_json_to_sql(p, 1)),
            Some(p) => {
                let cp = CompiledPredicate::compile_opt(Some(p), &ts)?;
                crate::sql::predicate_to_sql(&cp, &ts)
            }
            None => None,
        };
        let url = self.pg_url.clone().context("query_subset requires postgres mode")?;
        let client = crate::pg::connect(&url).await?;
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
                    let ready = share.ready.clone();
                    // Release the lock, then wait for the creator's backfill to land: a joiner must not
                    // see a stream whose snapshot isn't readable yet, and must surface (not mask) a
                    // failed creation.
                    drop(st);
                    if let Err(e) = await_share_ready(ready, &existing_id).await {
                        // The failed creator already removed the share entries; undo nothing.
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
                if !st.tailers.contains_key(t) {
                    let tts = st
                        .tables
                        .get(t)
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!("unknown table '{t}' referenced by subquery"))?;
                    let handle =
                        spawn_tailer(self.ds.clone(), tts, self.pg_url.clone(), self.subqueries.clone());
                    st.tailers.insert(t.clone(), handle);
                }
            }
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
                    return Ok(rec);
                }
                Err(e) => {
                    // Registration failed (the registry rolled its own state back). Remove the shape
                    // record + share entries so later identical creates don't join a dead stream, and
                    // wake any joiners with the failure.
                    let mut st = self.state.lock().await;
                    st.shapes.remove(&id);
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

        if !st.tailers.contains_key(table) {
            let handle = spawn_tailer(self.ds.clone(), ts.clone(), self.pg_url.clone(), self.subqueries.clone());
            st.tailers.insert(table.to_string(), handle);
        }
        let tailer = st.tailers.get(table).expect("tailer just inserted");
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        tailer
            .cmd_tx
            .send(TailerCmd::AddShape {
                shape_id: id.clone(),
                num_id,
                stream_path: stream_path.clone(),
                pred,
                out_cols,
                changes_only,
                ready: ready_tx,
            })
            .map_err(|_| anyhow::anyhow!("tailer for '{table}' is gone"))?;

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
        // Register the (first) shared feed so later identical subset feeds join it. Joiners wait on
        // `share_tx` for the backfill outcome.
        let (share_tx, share_rx) = tokio::sync::watch::channel(None);
        if let Some(sig) = feed_sig {
            st.feed_by_sig.insert(sig.clone(), id.clone());
            st.feed_shares.insert(id.clone(), FeedShare { sig, refcount: 1, ready: share_rx });
        }
        // Release the engine-state lock, then wait for the tailer to finish the backfill so the shape's
        // snapshot is readable when we return (the Electric adapter folds the stream immediately).
        drop(st);
        let outcome = ready_rx.await.unwrap_or_else(|_| Err("tailer dropped the ready channel".to_string()));
        match outcome {
            Ok(()) => {
                let _ = share_tx.send(Some(true));
                Ok(rec)
            }
            Err(e) => {
                // Backfill/registration failed: remove the record + share entries (no zombie shape a
                // later identical create would join) and surface the error to the caller.
                let mut st = self.state.lock().await;
                st.shapes.remove(&id);
                if let Some(share) = st.feed_shares.remove(&id) {
                    st.feed_by_sig.remove(&share.sig);
                }
                if let Some(t) = st.tailers.get(&rec.table) {
                    let _ = t.cmd_tx.send(TailerCmd::RemoveShape { shape_id: id.clone() });
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
                let ready = share.ready.clone();
                drop(st);
                await_share_ready(ready, &existing_id).await?;
                return Ok(rec);
            }
        }

        let pred = Arc::new(CompiledPredicate::compile_opt(where_.as_ref(), &ts)?);

        let num_id = st.next_shape_id;
        let id = format!("s{num_id}");
        st.next_shape_id += 1;
        let stream_path = format!("shape/{id}");
        self.ds.ensure_stream(&stream_path).await?;

        if !st.tailers.contains_key(table) {
            let handle = spawn_tailer(self.ds.clone(), ts.clone(), self.pg_url.clone(), self.subqueries.clone());
            st.tailers.insert(table.to_string(), handle);
        }
        let tailer = st.tailers.get(table).expect("tailer just inserted");
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        tailer
            .cmd_tx
            .send(TailerCmd::AddAggregate {
                shape_id: id.clone(),
                stream_path: stream_path.clone(),
                pred,
                func,
                col: col_idx,
                ready: ready_tx,
            })
            .map_err(|_| anyhow::anyhow!("tailer for '{table}' is gone"))?;

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
        // Register this (first) aggregate so later identical ones join it by ref-count.
        let (share_tx, share_rx) = tokio::sync::watch::channel(None);
        st.feed_by_sig.insert(agg_sig.clone(), id.clone());
        st.feed_shares.insert(id.clone(), FeedShare { sig: agg_sig, refcount: 1, ready: share_rx });
        drop(st);
        let outcome = ready_rx.await.unwrap_or_else(|_| Err("tailer dropped the ready channel".to_string()));
        match outcome {
            Ok(()) => {
                let _ = share_tx.send(Some(true));
                Ok(rec)
            }
            Err(e) => {
                let mut st = self.state.lock().await;
                st.shapes.remove(&id);
                if let Some(share) = st.feed_shares.remove(&id) {
                    st.feed_by_sig.remove(&share.sig);
                }
                if let Some(t) = st.tailers.get(&rec.table) {
                    let _ = t.cmd_tx.send(TailerCmd::RemoveShape { shape_id: id.clone() });
                }
                drop(st);
                let _ = share_tx.send(Some(false));
                let _ = self.ds.delete_stream(&rec.stream_path).await;
                bail!("aggregate '{id}' creation failed: {e}")
            }
        }
    }

    /// Snapshot the whole maintained pipeline for the visualizer: tables, every registered shape with
    /// its routing placement (family key / standalone / subquery), and the shared subquery node+edge DAG.
    pub async fn graph(&self) -> EngineGraph {
        let (tables, shapes, schemas) = {
            let st = self.state.lock().await;
            let tables: Vec<String> = st.tables.keys().cloned().collect();
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
                })
                .collect();
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
        let subquery_nodes: Vec<GraphNode> = reg
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
        EngineGraph { tables, shapes, subquery_nodes, subquery_edges }
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

    pub async fn drop_shape(&self, id: &str) -> Result<()> {
        let mut st = self.state.lock().await;
        // Shared subset feed: decrement the ref-count; only actually tear it down when the last
        // subscriber leaves. (A materialized shape / unshared feed is not in `feed_shares` → drops now.)
        if let Some(share) = st.feed_shares.get_mut(id) {
            share.refcount = share.refcount.saturating_sub(1);
            if share.refcount > 0 {
                return Ok(());
            }
            let sig = share.sig.clone();
            st.feed_shares.remove(id);
            st.feed_by_sig.remove(&sig);
        }
        let removed = st.shapes.remove(id);
        if let Some(rec) = &removed {
            if let Some(t) = st.tailers.get(&rec.table) {
                let _ = t.cmd_tx.send(TailerCmd::RemoveShape { shape_id: id.to_string() });
            }
        }
        drop(st);
        // Subquery shapes live in the registry (a no-op here if `id` was a plain shape).
        self.subqueries.lock().await.drop_subquery_shape(id);
        // Final drop: delete the durable stream. Without this every dropped shape orphans its stream
        // on the storage server forever (disk leak observed under loadgen open/close churn). Any
        // still-in-flight tailer append observes the 404 and discards cleanly (`append_reliable`).
        if let Some(rec) = removed {
            if let Err(e) = self.ds.delete_stream(&rec.stream_path).await {
                tracing::warn!("failed to delete stream {} for dropped shape {id}: {e:#}", rec.stream_path);
            }
        }
        Ok(())
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
            for h in st.tailers.values() {
                if let Ok(s) = h.stats.lock() {
                    families += s.families.len();
                    family_shapes += s.families.iter().map(|f| f.shapes).sum::<usize>();
                    standalone += s.standalone;
                }
            }
            (st.shapes.len(), st.tailers.len(), st.tables.len(), families, family_shapes, standalone)
        };
        let (sq_nodes, sq_contributors, sq_distinct, sq_shapes, sq_edges) =
            self.subqueries.lock().await.mem_totals();
        crate::mem::Cardinalities {
            shapes,
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
    /// This shape's backfill-snapshot fence: replicated changes already visible to the backfill are
    /// skipped by xid visibility (LSN fallback) — see [`crate::pg::SnapshotGate`].
    gate: crate::pg::SnapshotGate,
    /// Output projection (column indices), or `None` to emit the full row.
    out_cols: Option<Arc<Vec<usize>>>,
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

/// A scalar aggregation maintained **incrementally** over the rows matching `pred` — the dbsp fold over
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

fn spawn_tailer(
    ds: DsClient,
    ts: TableSchema,
    pg_url: Option<String>,
    subqueries: Arc<Mutex<SubqueryRegistry>>,
) -> TailerHandle {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let processed = Arc::new(std::sync::Mutex::new("-1".to_string()));
    let stats = Arc::new(std::sync::Mutex::new(TableStats::default()));
    tokio::spawn(tailer_loop(ds, ts, pg_url, cmd_rx, processed.clone(), stats.clone(), subqueries));
    TailerHandle { cmd_tx, processed, stats }
}

fn publish_stats(
    stats: &std::sync::Mutex<TableStats>,
    shapes: &HashMap<String, StandaloneShape>,
    families: &HashMap<Vec<usize>, KeyRouter>,
) {
    let mut fams: Vec<FamilyStat> = families
        .iter()
        .map(|(k, f)| FamilyStat { key_cols: k.clone(), shapes: f.member_count() })
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
    subqueries: Arc<Mutex<SubqueryRegistry>>,
) {
    let table_path = format!("table/{}", ts.name);
    let mut offset = "-1".to_string();
    // Standalone per-shape filter circuits (non-equality predicates), keyed by shape id.
    let mut shapes: HashMap<String, StandaloneShape> = HashMap::new();
    // Shared family circuits, keyed by the equality template's (sorted) column indices.
    let mut families: HashMap<Vec<usize>, KeyRouter> = HashMap::new();
    // Reverse lookup for removal: shape id -> (template key cols, numeric id, key tuple).
    let mut family_of: HashMap<String, (Vec<usize>, u64, Row)> = HashMap::new();
    // Scalar aggregations maintained incrementally over a filter predicate, keyed by shape id.
    let mut aggregates: HashMap<String, AggShape> = HashMap::new();
    // De-duplication highwater: the ingestor's delivery is at-least-once (re-peek after a partial
    // append failure or a crash before the slot advance re-appends whole batches), and deltas are NOT
    // idempotent for aggregates/subquery weights. Every ingestor envelope carries (commit lsn, seq =
    // position in txn), strictly increasing per table stream, so anything at/below the highwater has
    // already been applied and is skipped. Envelopes without both stamps (library mode) bypass this.
    let mut highwater: Option<(u64, u64)> = None;

    loop {
        let off = offset.clone();
        tokio::select! {
            biased;
            cmd = cmd_rx.recv() => match cmd {
                Some(TailerCmd::AddShape { shape_id, num_id, stream_path, pred, out_cols, changes_only, ready }) => {
                    let res = add_shape_routed(
                        &ds, &ts, &pg_url, &mut shapes, &mut families, &mut family_of,
                        shape_id, num_id, stream_path, pred, out_cols, changes_only,
                    ).await;
                    if let Err(e) = &res {
                        tracing::error!("add_shape failed: {e:#}");
                    }
                    // Unblock create_shape with the outcome; a failure propagates so the caller can
                    // remove the shape record instead of leaving a zombie.
                    let _ = ready.send(res.map_err(|e| format!("{e:#}")));
                    publish_stats(&stats, &shapes, &families);
                }
                Some(TailerCmd::AddAggregate { shape_id, stream_path, pred, func, col, ready }) => {
                    let res = add_aggregate(
                        &ds, &ts, &pg_url, &mut aggregates, shape_id, stream_path, pred, func, col,
                    ).await;
                    if let Err(e) = &res {
                        tracing::error!("add_aggregate failed: {e:#}");
                    }
                    let _ = ready.send(res.map_err(|e| format!("{e:#}")));
                }
                Some(TailerCmd::RemoveShape { shape_id }) => {
                    if aggregates.remove(&shape_id).is_some() {
                        // an aggregation shape — nothing else to unwind
                    } else if shapes.remove(&shape_id).is_none()
                        && let Some((key_cols, num_id, key_tuple)) = family_of.remove(&shape_id)
                        && let Some(router) = families.get_mut(&key_cols)
                    {
                        // Drop the shape from its key's routing list (the shape stream is torn down
                        // elsewhere); discard the router once it routes to no shapes.
                        if let Some(routed) = router.index.get_mut(&key_tuple) {
                            routed.retain(|rs| rs.num_id != num_id);
                            if routed.is_empty() {
                                router.index.remove(&key_tuple);
                            }
                        }
                        if router.index.is_empty() {
                            families.remove(&key_cols);
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
                        // Skip redelivered changes (see `highwater` above).
                        let pos = match (env.headers.lsn.as_deref(), env.headers.seq) {
                            (Some(l), Some(s)) => Some((crate::pg::lsn_to_u64(l), s)),
                            _ => None,
                        };
                        if let (Some(p), Some(hw)) = (pos, highwater) {
                            if p <= hw {
                                tracing::debug!("tailer {}: skipping duplicate change at {p:?}", ts.name);
                                continue;
                            }
                        }
                        if let Err(e) = process_envelope(
                            &ts, &shapes, &families, &mut aggregates, env, &mut pending, &subqueries,
                        )
                        .await
                        {
                            tracing::error!("process_envelope failed: {e:#}");
                        }
                        if let Some(p) = pos {
                            highwater = Some(p);
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

/// Seed a scalar aggregation from Postgres (fold the matching rows once), emit the initial value, and
/// register it for incremental maintenance. Steady state holds only the running aggregate — no rows.
#[allow(clippy::too_many_arguments)]
async fn add_aggregate(
    ds: &DsClient,
    ts: &TableSchema,
    pg_url: &Option<String>,
    aggregates: &mut HashMap<String, AggShape>,
    shape_id: String,
    stream_path: String,
    pred: Arc<CompiledPredicate>,
    func: AggFn,
    col: Option<usize>,
) -> Result<()> {
    let bf = pg_backfill(pg_url, ts, Some(pred.as_ref())).await?;
    let mut agg = AggShape {
        pred,
        func,
        col,
        stream_path: stream_path.clone(),
        gate: bf.gate.clone(),
        count: 0,
        nn_count: 0,
        sum: 0.0,
        multiset: std::collections::BTreeMap::new(),
        last: None,
    };
    let seed: Vec<Tup2<Row, ZWeight>> = bf.rows.iter().map(|r| Tup2(r.clone(), 1)).collect();
    agg.apply(&seed);
    let env = agg.envelope(ts, None, None);
    agg.last = Some(agg.value());
    ds.append(&stream_path, &[env]).await?;
    aggregates.insert(shape_id, agg);
    Ok(())
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
    families: &mut HashMap<Vec<usize>, KeyRouter>,
    family_of: &mut HashMap<String, (Vec<usize>, u64, Row)>,
    shape_id: String,
    num_id: u64,
    stream_path: String,
    pred: Arc<CompiledPredicate>,
    out_cols: Option<Arc<Vec<usize>>>,
    changes_only: bool,
) -> Result<()> {
    let proj = out_cols.as_deref().map(Vec::as_slice);
    match pred.equality_template() {
        Some(pairs) => {
            let key_cols: Vec<usize> = pairs.iter().map(|(c, _)| *c).collect();
            let key_tuple = Row(pairs.into_iter().map(|(_, v)| v).collect());

            // Per-shape backfill straight from Postgres: the predicate IS this shape's key equality, so
            // the pushdown reads only the key-matching rows — the engine never holds a table copy. Each
            // shape records its own snapshot gate for the live-change reconciliation. A `changes_only`
            // feed skips the backfill entirely and forwards every future match (passthrough gate).
            let gate = if changes_only {
                crate::pg::SnapshotGate::passthrough()
            } else {
                let bf = pg_backfill(pg_url, ts, Some(pred.as_ref())).await?;
                let out: Vec<(Row, ZWeight)> = bf.rows.iter().map(|r| (r.clone(), 1)).collect();
                if !out.is_empty() {
                    let envs = translate_output(ts, out, None, None, proj);
                    ds.append(&stream_path, &envs).await?;
                }
                bf.gate
            };

            let router = families
                .entry(key_cols.clone())
                .or_insert_with(|| KeyRouter { key_cols: key_cols.clone(), index: HashMap::new() });
            router.index.entry(key_tuple.clone()).or_default().push(RoutedShape {
                num_id,
                stream_path,
                gate,
                out_cols,
            });
            family_of.insert(shape_id, (key_cols, num_id, key_tuple));
        }
        None => {
            // Standalone filter: backfill = current matching rows from Postgres (emitted as upserts).
            // Push the predicate into the SELECT so only matching rows are read; `matches()` below is
            // the final authority (and a safety net if the SQL is ever a looser superset). A
            // `changes_only` feed skips the backfill and forwards only future matches (passthrough
            // gate) — this is the non-materialized live tail a subset query follows.
            let gate = if changes_only {
                crate::pg::SnapshotGate::passthrough()
            } else {
                let bf = pg_backfill(pg_url, ts, Some(pred.as_ref())).await?;
                let out: Vec<(Row, ZWeight)> =
                    bf.rows.iter().filter(|r| pred.matches(r)).map(|r| (r.clone(), 1)).collect();
                if !out.is_empty() {
                    let envs = translate_output(ts, out, None, None, proj);
                    ds.append(&stream_path, &envs).await?;
                }
                bf.gate
            };
            shapes.insert(shape_id, StandaloneShape { pred, stream_path, gate, out_cols });
        }
    }
    Ok(())
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
            let client = crate::pg::connect(url).await?;
            crate::pg::backfill(&client, ts, filter).await
        }
        None => Ok(crate::pg::Backfill {
            rows: Vec::new(),
            seed_lsn: "0/0".to_string(),
            gate: crate::pg::SnapshotGate::passthrough(),
        }),
    }
}


async fn process_envelope(
    ts: &TableSchema,
    shapes: &HashMap<String, StandaloneShape>,
    families: &HashMap<Vec<usize>, KeyRouter>,
    aggregates: &mut HashMap<String, AggShape>,
    env: Envelope,
    pending: &mut HashMap<String, Vec<Envelope>>,
    subqueries: &Arc<Mutex<SubqueryRegistry>>,
) -> Result<()> {
    let (delta, txid, lsn) = apply_envelope(ts, &env)?;
    if delta.is_empty() {
        return Ok(());
    }
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
    // fallback for changes without a parseable xid).
    for shape in shapes.values() {
        if shape.gate.should_skip(lsn_u64, xid) {
            continue;
        }
        let out = eval_standalone(&shape.pred, &delta);
        if out.is_empty() {
            continue;
        }
        let envs =
            translate_output(ts, out, txid.clone(), lsn.clone(), shape.out_cols.as_deref().map(Vec::as_slice));
        pending.entry(shape.stream_path.clone()).or_default().extend(envs);
    }
    // Equality routers: route each delta row by its key to exactly the shapes registered on that key.
    // No table copy, no dbsp — membership is the key match (an equality-template predicate matches a
    // row iff its key equals the shape's constants). Each shape's own snapshot gate is applied, so
    // changes already in that shape's backfill are skipped.
    let _s = Timer::new(&metrics().family_step);
    for router in families.values() {
        type ShapeOut<'a> = (&'a str, Option<&'a [usize]>, Vec<(Row, ZWeight)>);
        let mut by_shape: HashMap<u64, ShapeOut> = HashMap::new();
        for Tup2(row, w) in &delta {
            let key = key_of(row, &router.key_cols);
            let Some(routed) = router.index.get(&key) else { continue };
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
    // Subquery shapes/nodes: route this delta through the cross-table registry. It updates the shared
    // inner-set nodes, emits outer-shape deltas, and propagates inner-set flips to dependents — appending
    // move envelopes synchronously, so this batch's processed-offset barrier still implies convergence.
    {
        let mut reg = subqueries.lock().await;
        if reg.touches(&ts.name) {
            reg.on_table_delta(ts, &delta, lsn_u64, xid, txid.clone()).await?;
        }
    }
    // Scalar aggregations: fold this delta into each running aggregate; emit the new value when it
    // changes. Skips changes already counted in the seed (the aggregate's snapshot gate).
    for agg in aggregates.values_mut() {
        if agg.gate.should_skip(lsn_u64, xid) {
            continue;
        }
        if agg.apply(&delta) {
            let val = agg.value();
            if agg.last.as_ref() != Some(&val) {
                agg.last = Some(val.clone());
                let env = agg.envelope(ts, txid.clone(), lsn.clone());
                pending.entry(agg.stream_path.clone()).or_default().push(env);
            }
        }
    }
    Ok(())
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
