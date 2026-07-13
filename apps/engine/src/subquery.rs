//! Subquery support: shared, incrementally-maintained inner-set **nodes** and the cross-table
//! registry that moves outer rows in/out as inner sets change.
//!
//! A shape whose `WHERE` contains `col IN (SELECT proj FROM inner WHERE pred)` (or `NOT IN`) cannot be
//! evaluated row-locally — membership depends on the inner subquery's result set. We maintain that set
//! once per distinct subquery (keyed by a canonical [`SubquerySig`]) as a [`SubqueryNode`]: a map from
//! projected value to the set of inner-row primary keys producing it. A value is "in the set" iff its
//! contributor set is non-empty; tracking contributor pks (not a bare count) makes maintenance
//! reconcile-by-identity — set a row's presence to equal `match(row)` regardless of history.
//!
//! Identical subqueries share one node (the memory win + the sharing the design calls for). Nodes feed
//! dependents — outer shapes or *parent* nodes (a node whose inner `pred` itself references this node) —
//! along edges recorded by connecting column. When a value flips (a bucket goes empty↔non-empty), the
//! registry queries the dependent rows referencing that value and re-evaluates them (see
//! `on_table_delta`, added in a later step). This file (step 6) is the pure in-memory core: node
//! maintenance + the [`SubqueryEval`] read view. No Postgres, no streams yet.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use anyhow::{Context, Result};
use crate::value::{Tup2, ZWeight};

use crate::ds::{DsClient, Envelope};
use crate::predicate::{
    CompiledPredicate, PredicateJson, SubqueryCollector, SubqueryEval, SubquerySig, subquery_sig,
};
use crate::schema::TableSchema;
use crate::value::{Row, Value};

/// Direction of a value-membership change on a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlipDir {
    /// A value's contributor set went empty → non-empty (the value entered the inner set).
    Enter,
    /// A value's contributor set went non-empty → empty (the value left the inner set).
    Leave,
}

/// A single value-membership change emitted by [`SubqueryNode::reconcile_row`]. `value` may be
/// [`Value::Null`] (the null bucket — relevant to `NOT IN`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Flip {
    pub value: Value,
    pub dir: FlipDir,
}

/// One maintained inner subquery: `SELECT proj_col FROM inner_table WHERE pred`, as a value set.
pub struct SubqueryNode {
    pub sig: SubquerySig,
    pub inner_table: String,
    /// Column index (in `inner_table`) of the projected value.
    pub proj_col: usize,
    /// Column index (in `inner_table`) of the primary key — used to key contributors.
    pub pk_col: usize,
    /// The inner predicate; may reference deeper nodes (evaluated via [`SubqueryEval`]).
    pub pred: Arc<CompiledPredicate>,
    /// The inner `where` as raw JSON (for seeding SQL, which must emit nested `IN (SELECT …)`).
    pub where_json: Option<PredicateJson>,
    /// The node's backfill-snapshot fence: inner deltas already visible to the seed snapshot are
    /// skipped (xid visibility, LSN fallback — see [`crate::pg::SnapshotGate`]).
    pub gate: crate::pg::SnapshotGate,
    /// `Some` while the node is being seeded (three-phase create): raw inner-table deltas that
    /// arrive mid-seed are buffered here and replayed through the seed gate at install — never
    /// applied to a half-seeded set (a snapshot row landing after a fresher delta would be a
    /// stale overwrite). `None` = live.
    pub(crate) seed_buffer: Option<Vec<Tup2<Row, ZWeight>>>,
    /// projected value -> set of contributing inner-row pks (stringified).
    contributors: HashMap<Value, HashSet<String>>,
    /// inner-row pk -> its current projected value (reverse index, for O(1) reconciliation).
    pk_value: HashMap<String, Value>,
    /// Number of dependents (shapes + parent nodes) referencing this node; drop the node at 0.
    pub refcount: usize,
}

impl SubqueryNode {
    pub fn new(
        sig: SubquerySig,
        inner_table: String,
        proj_col: usize,
        pk_col: usize,
        pred: Arc<CompiledPredicate>,
    ) -> Self {
        SubqueryNode {
            sig,
            inner_table,
            proj_col,
            pk_col,
            pred,
            where_json: None,
            gate: crate::pg::SnapshotGate::passthrough(),
            seed_buffer: None,
            contributors: HashMap::new(),
            pk_value: HashMap::new(),
            refcount: 0,
        }
    }

    /// Is `value` currently a member of the inner set?
    pub fn contains(&self, value: &Value) -> bool {
        self.contributors.get(value).is_some_and(|s| !s.is_empty())
    }

    /// Does the inner set currently contain a NULL value? (Makes `x NOT IN set` UNKNOWN.)
    pub fn has_null(&self) -> bool {
        self.contains(&Value::Null)
    }

    /// Number of distinct values currently in the set (for introspection / sharing tests).
    pub fn distinct_values(&self) -> usize {
        self.contributors.len()
    }

    /// Reconcile inner-row `pk`'s contribution so it equals `present_value` (its projected value if the
    /// row currently matches `pred`, else `None`). Returns the resulting value flips (at most a `Leave`
    /// of the old value and an `Enter` of the new). History-independent and idempotent.
    pub fn reconcile_row(&mut self, pk: &str, present_value: Option<Value>) -> Vec<Flip> {
        // No-op if the contribution is unchanged (avoids a spurious Leave+Enter of the same value).
        if self.pk_value.get(pk) == present_value.as_ref() {
            return Vec::new();
        }
        let mut flips = Vec::new();
        // Remove the old contribution.
        if let Some(old_v) = self.pk_value.remove(pk) {
            if let Some(set) = self.contributors.get_mut(&old_v) {
                set.remove(pk);
                if set.is_empty() {
                    self.contributors.remove(&old_v);
                    flips.push(Flip { value: old_v, dir: FlipDir::Leave });
                }
            }
        }
        // Add the new contribution.
        if let Some(v) = present_value {
            let set = self.contributors.entry(v.clone()).or_default();
            let was_empty = set.is_empty();
            set.insert(pk.to_string());
            self.pk_value.insert(pk.to_string(), v.clone());
            if was_empty {
                flips.push(Flip { value: v, dir: FlipDir::Enter });
            }
        }
        flips
    }
}

/// Identifies a dependent of a node: an outer shape or a parent node, plus the connecting column on the
/// dependent's table whose value `= the flipped node value` selects the affected rows.
#[derive(Debug, Clone)]
pub enum Dependent {
    /// An outer subquery shape (by registry shape id).
    Shape(String),
    /// A parent node (by signature) whose inner `pred` references this node.
    Node(SubquerySig),
}

/// An edge from a node to a dependent: when the node flips `value`, rows of the dependent's table with
/// `connecting_col = value` may change membership.
#[derive(Debug, Clone)]
pub struct Edge {
    pub node_sig: SubquerySig,
    pub dependent: Dependent,
    /// Column index (in the dependent's table) connecting to the node.
    pub connecting_col: usize,
    pub negated: bool,
    /// True iff a NULL entering/leaving the node's set can change this dependent's membership. That is
    /// the case when the `IN` leaf is itself negated (`NOT IN` — SQL: a NULL in the set makes it
    /// UNKNOWN) **or** sits under any `Not{…}` wrapper: with no negation anywhere above the leaf, a
    /// NULL only moves the leaf between FALSE and UNKNOWN, and AND/OR are monotone in
    /// FALSE < UNKNOWN < TRUE, so overall TRUE-ness (inclusion) cannot change. Any negation breaks the
    /// monotonicity, so those dependents must be fully re-derived on a NULL flip.
    pub null_sensitive: bool,
}

/// A registered outer subquery shape: an ordinary materialized shape whose predicate contains
/// `IN (SELECT …)`. The engine emits `upsert`/`delete` envelopes to `stream_path` as membership
/// changes (from outer-row deltas and from inner-set flips).
pub struct SubqueryShape {
    pub shape_id: String,
    pub outer_table: String,
    pub stream_path: String,
    /// The outer predicate (with `InSubquery` leaves resolving against this registry's nodes).
    pub pred: Arc<CompiledPredicate>,
    pub out_cols: Option<Arc<Vec<usize>>>,
    /// The shape's backfill-snapshot fence; outer deltas already visible to the backfill are skipped.
    pub gate: crate::pg::SnapshotGate,
    /// Envelopes appended to this shape's stream (backfill + live), for the visualizer's per-node
    /// state. Atomic because the append paths hold `&self`.
    pub emitted: std::sync::atomic::AtomicU64,
    /// Pks this shape has told its stream are members (seeded from the backfill, updated by every
    /// emission below). A delta computing "not a member" for a pk absent here is a true no-op —
    /// this shape never claimed it, so a delete would be spurious, not idempotent — and is
    /// dropped rather than delivered. Without this, every outer-table write wakes every
    /// subquery shape on that table's live long-poll (durable-streams sees a real, non-empty
    /// append), even when the row never matched: the client-side key-set filter in
    /// `electric.rs::apply_changes` suppresses the bogus delete from the visible message, but by
    /// then the long-poll has already resolved with an empty "up-to-date" — burning a full
    /// round-trip per irrelevant write, per concurrently-live shape on the table. Purely local
    /// bookkeeping (this shape's own emission history), so it carries none of the cross-table
    /// race the absolute/idempotent emission scheme (see `emit_shape_delta`) exists to avoid.
    known_members: std::sync::Mutex<std::collections::HashSet<String>>,
}

/// A `TableSchema` lookup shared with the engine's compiled schema.
pub type SchemaMap = Arc<HashMap<String, TableSchema>>;

/// Per-node introspection (served at `GET /subqueries`).
#[derive(Clone, serde::Serialize)]
pub struct NodeStat {
    pub sig: SubquerySig,
    pub inner_table: String,
    pub distinct_values: usize,
    pub refcount: usize,
}

/// A subquery shape between `begin_create` and `finish_create`: registration exists (so its
/// nodes are refcounted, its edges recorded, and its deltas buffered) but seeding/backfill runs
/// outside the registry lock.
pub struct PendingSubqueryShape {
    pub shape_id: String,
    pub outer_table: String,
    pub stream_path: String,
    pub pred: Arc<CompiledPredicate>,
    pub out_cols: Option<Arc<Vec<usize>>>,
    pub changes_only: bool,
    /// This create's node-refcount log (for exact rollback on failure).
    collect_log: Vec<SubquerySig>,
    /// Outer-table deltas buffered while the backfill runs; replayed through the gate at install.
    buffer: Vec<Tup2<Row, ZWeight>>,
}

/// What phase B (Postgres I/O, run WITHOUT the registry lock) needs from `begin_create`.
pub struct BeginCreate {
    /// Fresh nodes this create must seed: `(sig, inner_table, inner where-JSON)`.
    pub seeds: Vec<(SubquerySig, String, Option<PredicateJson>)>,
    /// Schema map snapshot for SQL emission.
    pub schemas: SchemaMap,
}

/// The cross-table registry of subquery nodes + shapes + edges. Implements [`SubqueryEval`] so a
/// predicate's subquery leaves resolve against the maintained node sets. One per engine, behind a
/// `tokio::Mutex`; every table tailer calls [`on_table_delta`](Self::on_table_delta).
pub struct SubqueryRegistry {
    /// Nodes by canonical signature (shared across identical subqueries).
    pub nodes: HashMap<SubquerySig, SubqueryNode>,
    /// Edges from each node to its dependents.
    pub edges: Vec<Edge>,
    /// Registered outer subquery shapes by engine shape id.
    pub shapes: HashMap<String, SubqueryShape>,
    /// Nodes created but not yet seeded from Postgres (deepest-first).
    pending_seed: Vec<SubquerySig>,
    /// Shapes between `begin_create` and `finish_create` (the three-phase create): their
    /// outer-table deltas are buffered here and replayed through the shape's gate at install.
    pending_shapes: Vec<PendingSubqueryShape>,
    /// Every node-refcount increment made by the in-flight `create_subquery_shape` (one entry per
    /// `collect()` call). On failure the create is rolled back exactly: each logged sig is decremented
    /// once, and nodes that return to zero are removed. The registry mutex is held for the whole
    /// create, so the log can't interleave with another create.
    collect_log: Vec<SubquerySig>,
    ds: DsClient,
    pg_url: Option<String>,
    schemas: SchemaMap,
    /// Ordered emission lanes (see `engine::emission`): membership envelopes are enqueued
    /// under this registry's lock — per-stream enqueue order = eval order — and land on their
    /// streams asynchronously, covered by the `pendingFlips` barrier. `None` (unit tests)
    /// falls back to a direct reliable append.
    lanes: Option<crate::engine::emission::EmissionLanes>,
}

impl SubqueryRegistry {
    pub fn new(ds: DsClient, pg_url: Option<String>) -> Self {
        SubqueryRegistry {
            nodes: HashMap::new(),
            edges: Vec::new(),
            shapes: HashMap::new(),
            pending_seed: Vec::new(),
            pending_shapes: Vec::new(),
            collect_log: Vec::new(),
            ds,
            pg_url,
            schemas: Arc::new(HashMap::new()),
            lanes: None,
        }
    }

    pub(crate) fn set_lanes(&mut self, lanes: crate::engine::emission::EmissionLanes) {
        self.lanes = Some(lanes);
    }

    /// Deliver membership envelopes to a shape stream in **evaluation order**: enqueue on the
    /// stream's emission lane while the caller holds this registry's lock (per-stream FIFO ⇒
    /// append order = eval order — the "data in the right place" invariant). Without lanes
    /// (unit tests) this awaits a direct reliable append, the pre-lane behavior.
    async fn deliver(&self, stream_path: &str, envs: Vec<Envelope>) {
        match &self.lanes {
            Some(l) => l.enqueue(stream_path, envs),
            None => {
                self.ds.append_reliable(stream_path, &envs).await;
            }
        }
    }

    /// Check out a pooled Postgres connection. All subquery PG access funnels through the shared
    /// per-URL pool: connections are reused across shapes (connecting per shape exhausts ephemeral
    /// TCP ports when thousands of subquery shapes are created) without serializing query-backs on
    /// a single session.
    async fn pg(&self) -> Result<crate::pg::PooledClient> {
        let url = self.pg_url.as_deref().context("subquery work requires postgres")?;
        crate::pg::pool_for(url).get().await
    }

    pub fn set_schemas(&mut self, schemas: SchemaMap) {
        self.schemas = schemas;
    }

    /// Does any node's inner table or any shape's outer table equal `table`? (Fast skip for tailers of
    /// tables not involved in any subquery.)
    pub fn touches(&self, table: &str) -> bool {
        self.nodes.values().any(|n| n.inner_table == table)
            || self.shapes.values().any(|s| s.outer_table == table)
            || self.pending_shapes.iter().any(|p| p.outer_table == table)
    }

    /// Number of maintained nodes (shared inner sets).
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// The live **inner-set index** of one node (the visualizer's "see the index" view): up to `cap`
    /// `(value, contributor-count)` pairs, most-shared first, plus the true distinct count, refcount, and
    /// whether the list was truncated. This is the actual engine-maintained set, not derivable from topology.
    pub fn node_value_index(
        &self,
        sig: &str,
        cap: usize,
    ) -> Option<(usize, usize, Vec<(serde_json::Value, usize)>, bool)> {
        let n = self.nodes.get(sig)?;
        let mut vals: Vec<(serde_json::Value, usize)> = n
            .contributors
            .iter()
            .filter(|(_, pks)| !pks.is_empty())
            .map(|(v, pks)| (v.to_json(), pks.len()))
            .collect();
        vals.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.to_string().cmp(&b.0.to_string())));
        let truncated = vals.len() > cap;
        vals.truncate(cap);
        Some((n.distinct_values(), n.refcount, vals, truncated))
    }

    /// Memory-relevant registry totals: maintained nodes, total contributor pks across all nodes (the
    /// dominant per-node state — one entry per inner row producing a value), distinct values, shapes,
    /// and edges. Used by the memory probe to attribute subquery state growth.
    pub fn mem_totals(&self) -> (usize, usize, usize, usize, usize) {
        let mut contributors = 0;
        let mut distinct = 0;
        for n in self.nodes.values() {
            contributors += n.contributors.values().map(|s| s.len()).sum::<usize>();
            distinct += n.contributors.len();
        }
        (self.nodes.len(), contributors, distinct, self.shapes.len(), self.edges.len())
    }

    /// Per-node topology for the introspection endpoint: signature, inner table, current distinct value
    /// count, and the dependent refcount. Two shapes referencing the same subquery show one node with
    /// `refcount == 2` (proves sharing).
    pub fn stats(&self) -> Vec<NodeStat> {
        let mut out: Vec<NodeStat> = self
            .nodes
            .values()
            .map(|n| NodeStat {
                sig: n.sig.clone(),
                inner_table: n.inner_table.clone(),
                distinct_values: n.distinct_values(),
                refcount: n.refcount,
            })
            .collect();
        out.sort_by(|a, b| a.sig.cmp(&b.sig));
        out
    }

    /// Live state summaries for every registry-owned graph node (`node:<sig>` inner sets and
    /// subquery `shape:<sid>` sinks), keyed by graph node id — merged into `GET /state` snapshots
    /// and the tailers' SSE `state` events.
    pub fn state_summaries(&self) -> Vec<(String, crate::engine::NodeStateSummary)> {
        let mut out = Vec::with_capacity(self.nodes.len() + self.shapes.len());
        for (sig, n) in &self.nodes {
            out.push((
                format!("node:{sig}"),
                crate::engine::NodeStateSummary::SubqueryNode {
                    distinct_values: n.distinct_values(),
                    refcount: n.refcount,
                },
            ));
        }
        for (sid, s) in &self.shapes {
            out.push((
                format!("shape:{sid}"),
                crate::engine::NodeStateSummary::Shape {
                    emitted: s.emitted.load(std::sync::atomic::Ordering::Relaxed),
                },
            ));
        }
        out
    }

    /// Outgoing edges for a node signature.
    fn edges_of(&self, sig: &SubquerySig) -> Vec<Edge> {
        self.edges.iter().filter(|e| &e.node_sig == sig).cloned().collect()
    }

    // --- registration -------------------------------------------------------------------------

    /// Register an outer subquery shape: compile the outer predicate (creating/deduping nodes + edges),
    /// seed any new nodes from Postgres, backfill the shape, and record it. Idempotent per shape id.
    ///
    /// **Atomic**: on any failure (unknown table, seed error, backfill/append error) every refcount
    /// increment, node, edge, and pending-seed entry made by this call is rolled back, so a failed
    /// create leaves the registry exactly as it was — no half-registered node that would silently
    /// serve wrong (unseeded) membership to a later identical create.
    /// Phase A of the three-phase create (call under the registry lock; brief, in-memory):
    /// compile the predicate (discovering/refcounting nodes — fresh ones start buffering),
    /// record edges, and register a pending shape that buffers its outer-table deltas from this
    /// moment on (no delta can fall between registration and the phase-B snapshot). Returns
    /// what phase B needs, or `Err` **without side effects** if the predicate shares a node
    /// another in-flight create is still seeding (caller retries — evaluating against a
    /// half-seeded set would be unsound).
    pub fn begin_create(
        &mut self,
        shape_id: &str,
        outer_table: &str,
        stream_path: &str,
        where_json: &PredicateJson,
        out_cols: Option<Arc<Vec<usize>>>,
        changes_only: bool,
    ) -> Result<BeginCreate> {
        let outer_ts =
            self.schemas.get(outer_table).cloned().context("subquery shape: unknown outer table")?;
        // Conflict pre-check: compiling refs nodes; a referenced node mid-seed belongs to a
        // concurrent create. Compile on a scratch collector first so a conflict has no effects.
        let edges_checkpoint = self.edges.len();
        self.collect_log.clear();
        let pred = match CompiledPredicate::compile_with(where_json, &outer_ts, self) {
            Ok(p) => Arc::new(p),
            Err(e) => {
                let log = std::mem::take(&mut self.collect_log);
                self.rollback_refs(edges_checkpoint, log);
                return Err(e);
            }
        };
        let log = std::mem::take(&mut self.collect_log);
        // A shared (not fresh-this-create) node still seeding ⇒ conflict: roll back and retry.
        let fresh: Vec<SubquerySig> = std::mem::take(&mut self.pending_seed);
        let conflicted = log.iter().any(|sig| {
            !fresh.contains(sig)
                && self.nodes.get(sig).is_some_and(|n| n.seed_buffer.is_some())
        });
        if conflicted {
            // Put fresh sigs back for the rollback's decref cascade bookkeeping.
            self.rollback_refs(edges_checkpoint, log);
            anyhow::bail!("subquery create conflict: shares a node another create is seeding");
        }
        // Fresh nodes start buffering their inner-table deltas.
        let mut seeds = Vec::with_capacity(fresh.len());
        for sig in &fresh {
            if let Some(n) = self.nodes.get_mut(sig) {
                n.seed_buffer = Some(Vec::new());
                seeds.push((sig.clone(), n.inner_table.clone(), n.where_json.clone()));
            }
        }
        // Shape-level edges.
        for leaf in collect_in_leaves(&pred) {
            self.edges.push(Edge {
                node_sig: leaf.sig,
                dependent: Dependent::Shape(shape_id.to_string()),
                connecting_col: leaf.col,
                negated: leaf.negated,
                null_sensitive: leaf.null_sensitive,
            });
        }
        // Pending shape: outer-table deltas buffer from HERE (before the phase-B snapshot).
        self.pending_shapes.push(PendingSubqueryShape {
            shape_id: shape_id.to_string(),
            outer_table: outer_table.to_string(),
            stream_path: stream_path.to_string(),
            pred,
            out_cols,
            changes_only,
            collect_log: log,
            buffer: Vec::new(),
        });
        Ok(BeginCreate { seeds, schemas: self.schemas.clone() })
    }

    /// Phase C (under the registry lock; brief, in-memory + lane enqueues): install the seeds,
    /// replay every buffered delta through the seed gates, register the shape, and return the
    /// flips the replays produced (the caller enqueues them for propagation). `seeded` counts
    /// the phase-B snapshot envelopes (for the shape's emitted counter). `seeded_pks` is the
    /// backfilled outer rows' pks, seeding `known_members` so a later delta that finds one of
    /// them no longer matching correctly emits a delete (not silently dropped as "never known").
    pub async fn finish_create(
        &mut self,
        shape_id: &str,
        node_seeds: Vec<(SubquerySig, Vec<Row>, crate::pg::SnapshotGate)>,
        outer_gate: crate::pg::SnapshotGate,
        seeded: u64,
        seeded_pks: std::collections::HashSet<String>,
    ) -> Result<VecDeque<(SubquerySig, Flip)>> {
        let idx = self
            .pending_shapes
            .iter()
            .position(|p| p.shape_id == shape_id)
            .context("finish_create: pending shape vanished")?;
        let pending = self.pending_shapes.remove(idx);
        let mut work: VecDeque<(SubquerySig, Flip)> = VecDeque::new();
        // 1. Install node seeds, then replay each node's buffered deltas through its gate.
        for (sig, rows, gate) in node_seeds {
            let (ts, proj_col) = {
                let n = self.nodes.get(&sig).context("finish_create: node vanished")?;
                (
                    self.schemas.get(&n.inner_table).cloned().context("unknown inner table")?,
                    n.proj_col,
                )
            };
            if let Some(n) = self.nodes.get_mut(&sig) {
                n.gate = gate;
                for r in &rows {
                    let pk = ts.key_string(r).unwrap_or_default();
                    let pv = r.0.get(proj_col).cloned().unwrap_or(Value::Null);
                    n.reconcile_row(&pk, Some(pv)); // initial state: flips are meaningless
                }
            }
            let buffered = self
                .nodes
                .get_mut(&sig)
                .and_then(|n| n.seed_buffer.take())
                .unwrap_or_default();
            if !buffered.is_empty() {
                // Replay through the gate: only deltas the snapshot could NOT contain apply.
                // (Buffered stamps aren't retained; the gate's xid test is per-eval at the
                // node phase — here we re-evaluate membership by identity, which is idempotent
                // against the seed for snapshot-visible rows, so replaying all is convergent.)
                let evals = self.node_present_values(&sig, &ts, &buffered);
                for f in self.apply_node_flips(&sig, evals) {
                    work.push_back((sig.clone(), f));
                }
            }
        }
        // 2. Register the shape, then replay its buffered outer deltas through the gate
        //    (absolute emission; idempotent against the backfill for snapshot-visible rows).
        let ts = self
            .schemas
            .get(&pending.outer_table)
            .cloned()
            .context("finish_create: unknown outer table")?;
        self.shapes.insert(
            shape_id.to_string(),
            SubqueryShape {
                shape_id: shape_id.to_string(),
                outer_table: pending.outer_table.clone(),
                stream_path: pending.stream_path.clone(),
                pred: pending.pred.clone(),
                out_cols: pending.out_cols.clone(),
                gate: outer_gate,
                emitted: std::sync::atomic::AtomicU64::new(seeded),
                known_members: std::sync::Mutex::new(seeded_pks),
            },
        );
        if !pending.buffer.is_empty() {
            self.emit_shape_delta(shape_id, &ts, &pending.buffer, None).await?;
        }
        Ok(work)
    }

    /// Abort an in-flight create (phase B failed): drop the pending entry and roll back the
    /// registration exactly (edges, refcounts, fresh nodes with their buffers).
    pub fn abort_create(&mut self, shape_id: &str) {
        let Some(idx) = self.pending_shapes.iter().position(|p| p.shape_id == shape_id) else {
            return;
        };
        let pending = self.pending_shapes.remove(idx);
        self.edges
            .retain(|e| !matches!(&e.dependent, Dependent::Shape(id) if id == pending.shape_id.as_str()));
        for sig in pending.collect_log {
            if let Some(n) = self.nodes.get_mut(&sig) {
                n.refcount = n.refcount.saturating_sub(1);
                if n.refcount == 0 {
                    self.nodes.remove(&sig);
                    self.pending_seed.retain(|s| s != &sig);
                    self.edges.retain(|e| {
                        e.node_sig != sig && !matches!(&e.dependent, Dependent::Node(s) if s == &sig)
                    });
                }
            }
        }
    }

    /// Rollback helper for a failed/conflicted `begin_create` compile: undo edge appends and
    /// node refs made by the aborted compile.
    fn rollback_refs(&mut self, edges_checkpoint: usize, log: Vec<SubquerySig>) {
        self.edges.truncate(edges_checkpoint);
        for sig in log {
            if let Some(n) = self.nodes.get_mut(&sig) {
                n.refcount = n.refcount.saturating_sub(1);
                if n.refcount == 0 {
                    self.nodes.remove(&sig);
                    self.pending_seed.retain(|s| s != &sig);
                }
            }
        }
    }

    /// Remove a subquery shape: drop its edges and decref the nodes it referenced (removing nodes whose
    /// refcount hits zero, and their edges, recursively).
    pub fn drop_subquery_shape(&mut self, shape_id: &str) {
        let Some(shape) = self.shapes.remove(shape_id) else { return };
        // Sigs this shape pointed at, then drop the shape's edges.
        let sigs: Vec<SubquerySig> = collect_in_leaves(&shape.pred).into_iter().map(|l| l.sig).collect();
        self.edges.retain(|e| !matches!(&e.dependent, Dependent::Shape(id) if id == shape_id));
        for sig in sigs {
            self.decref_node(&sig);
        }
    }

    fn decref_node(&mut self, sig: &SubquerySig) {
        let Some(node) = self.nodes.get_mut(sig) else { return };
        node.refcount = node.refcount.saturating_sub(1);
        if node.refcount > 0 {
            return;
        }
        // Refcount hit zero: gather child sigs, remove the node + its incoming/outgoing edges, recurse.
        let child_sigs: Vec<SubquerySig> =
            collect_in_leaves(&node.pred).into_iter().map(|l| l.sig).collect();
        self.nodes.remove(sig);
        self.edges
            .retain(|e| &e.node_sig != sig && !matches!(&e.dependent, Dependent::Node(s) if s == sig));
        for c in child_sigs {
            self.decref_node(&c);
        }
    }

    // --- live maintenance ---------------------------------------------------------------------

    /// Process one table delta: update affected nodes (in-memory) and emit outer-shape deltas
    /// synchronously, then **return** the inner-set flips for deferred propagation (the caller
    /// hands them to the engine's flip-propagator task — see [`propagate_flips`]). Deferring the
    /// flip-driven Postgres query-backs is safe because outer membership is emitted absolutely
    /// (upsert-if-matches-now / idempotent delete), so cross-table convergence is order-independent;
    /// the convergence barrier is the processed offset **plus** a drained flip queue. `lsn` is the
    /// change's commit LSN (0 = unknown/never skip).
    pub async fn on_table_delta(
        &mut self,
        ts: &TableSchema,
        delta: &[Tup2<Row, ZWeight>],
        lsn: u64,
        xid: Option<u64>,
        txid: Option<String>,
        mut trace: Option<&mut Vec<crate::trace::TraceHop>>,
    ) -> Result<VecDeque<(SubquerySig, Flip)>> {
        let table = ts.name.clone();
        // Work queue of (node sig, flip) pairs to propagate (BFS up the dependency DAG).
        let mut work: VecDeque<(SubquerySig, Flip)> = VecDeque::new();
        // Trace helper: record a hop once per node id (a shape reached via several flips is one hop).
        let hop = |trace: &mut Option<&mut Vec<crate::trace::TraceHop>>, node: String, outcome: &'static str| {
            if let Some(t) = trace.as_mut() {
                if let Some(prev) = t.iter_mut().find(|h| h.node == node) {
                    if outcome == "passed" {
                        prev.outcome = "passed"; // an earlier dropped hop upgraded by a later emit
                    }
                } else {
                    t.push(crate::trace::TraceHop::new(node, outcome));
                }
            }
        };

        // 1. Nodes whose inner table is this table: reconcile from the delta, collect flips.
        let node_sigs: Vec<SubquerySig> =
            self.nodes.iter().filter(|(_, n)| n.inner_table == table).map(|(s, _)| s.clone()).collect();
        for sig in node_sigs {
            // Mid-seed: buffer the raw delta for gated replay at install (a half-seeded set
            // must not be reconciled — the snapshot could stale-overwrite a fresher delta).
            if let Some(buf) = self.nodes.get_mut(&sig).and_then(|n| n.seed_buffer.as_mut()) {
                buf.extend(delta.iter().cloned());
                hop(&mut trace, format!("node:{sig}"), "buffered");
                continue;
            }
            if self.nodes.get(&sig).is_some_and(|n| n.gate.should_skip(lsn, xid)) {
                hop(&mut trace, format!("node:{sig}"), "dropped");
                continue;
            }
            let evals = self.node_present_values(&sig, ts, delta);
            let flips = self.apply_node_flips(&sig, evals);
            hop(&mut trace, format!("node:{sig}"), if flips.is_empty() { "dropped" } else { "passed" });
            for f in flips {
                work.push_back((sig.clone(), f));
            }
        }

        // 2. Subquery shapes whose outer table is this table: evaluate the filter on the delta + append.
        let shape_ids: Vec<String> = self
            .shapes
            .iter()
            .filter(|(_, s)| s.outer_table == table)
            .map(|(id, _)| id.clone())
            .collect();
        for id in shape_ids {
            if self.shapes.get(&id).is_some_and(|s| s.gate.should_skip(lsn, xid)) {
                continue;
            }
            let emitted = self.emit_shape_delta(&id, ts, delta, txid.clone()).await?;
            hop(&mut trace, format!("shape:{id}"), if emitted { "passed" } else { "dropped" });
        }

        // 2b. Pending shapes (mid-create) on this table: buffer for gated replay at install.
        for p in self.pending_shapes.iter_mut().filter(|p| p.outer_table == table) {
            p.buffer.extend(delta.iter().cloned());
        }

        // 3. Flip propagation (the Postgres query-backs) is deferred: the caller enqueues `work`
        // onto the engine's flip-propagator task, which runs [`propagate_flips`] without holding
        // this registry lock across round-trips.
        Ok(work)
    }

    /// For each inner-row pk touched by `delta`, its desired contribution (`Some(proj)` if the row now
    /// matches the node predicate, else `None`). Immutable (reads node sets for `matches_ctx`).
    fn node_present_values(
        &self,
        sig: &SubquerySig,
        ts: &TableSchema,
        delta: &[Tup2<Row, ZWeight>],
    ) -> Vec<(String, Option<Value>)> {
        let (pred, proj) = match self.nodes.get(sig) {
            Some(n) => (n.pred.clone(), n.proj_col),
            None => return Vec::new(),
        };
        // The +1 row (if any) is the row's new state; a pk seen only with -1 was deleted.
        let mut newrow: HashMap<String, Row> = HashMap::new();
        let mut seen: Vec<String> = Vec::new();
        for Tup2(row, w) in delta {
            let pk = ts.key_string(row).unwrap_or_default();
            if !seen.contains(&pk) {
                seen.push(pk.clone());
            }
            if *w > 0 {
                newrow.insert(pk, row.clone());
            }
        }
        seen.into_iter()
            .map(|pk| match newrow.get(&pk) {
                Some(r) => {
                    let pv = if pred.matches_ctx(r, self) {
                        Some(r.0.get(proj).cloned().unwrap_or(Value::Null))
                    } else {
                        None
                    };
                    (pk, pv)
                }
                None => (pk, None),
            })
            .collect()
    }

    /// Apply reconciliations to a node, returning the resulting flips. Mutable.
    fn apply_node_flips(&mut self, sig: &SubquerySig, evals: Vec<(String, Option<Value>)>) -> Vec<Flip> {
        let Some(node) = self.nodes.get_mut(sig) else { return Vec::new() };
        let mut flips = Vec::new();
        for (pk, pv) in evals {
            flips.extend(node.reconcile_row(&pk, pv));
        }
        flips
    }

    /// Deferred-propagation helper: snapshot what a query-back needs (brief lock scope at the
    /// call site — see the free functions below).
    fn snapshot_for_table(&self, table: &str) -> Result<TableSchema> {
        self.schemas.get(table).cloned().with_context(|| format!("unknown table '{table}'"))
    }

    /// Evaluate a subquery shape over a delta on its own (outer) table and append the resulting
    /// enter/leave envelopes. Emission is **absolute, not delta-based**: for each touched pk we emit the
    /// row's *current* membership (`upsert` if its latest row matches, else `delete` by pk). This is what
    /// makes the outer path independent of cross-table processing order — a per-table tailer may apply an
    /// inner-set change before an earlier-committed outer change, so a delta-based "delete only if the
    /// *old* row matched" misses move-outs once the inner set is already ahead. Emitting on the *new*
    /// row's membership (delete is idempotent by pk) converges regardless of order; a value the inner set
    /// hasn't caught up to yet is reconciled later by the flip-driven move query.
    async fn emit_shape_delta(
        &self,
        shape_id: &str,
        ts: &TableSchema,
        delta: &[Tup2<Row, ZWeight>],
        txid: Option<String>,
    ) -> Result<bool> {
        let Some(shape) = self.shapes.get(shape_id) else { return Ok(false) };
        let pred = shape.pred.clone();
        // Per touched pk, take the row's latest state (shared kernel fold): `is_new`
        // distinguishes "row still exists" from "row was deleted".
        let out: Vec<(Row, ZWeight)> = crate::engine::membership::latest_rows_by_pk(ts, delta)
            .into_iter()
            .map(|(row, is_new)| {
                let member = is_new && pred.matches_ctx(&row, self);
                (row, if member { 1 } else { -1 })
            })
            .collect();
        let out = filter_known_members(ts, out, &shape.known_members);
        if out.is_empty() {
            return Ok(false);
        }
        let envs = crate::engine::translate_output(ts, out, txid, None, shape.out_cols.as_deref().map(Vec::as_slice));
        if envs.is_empty() {
            return Ok(false);
        }
        shape.emitted.fetch_add(envs.len() as u64, std::sync::atomic::Ordering::Relaxed);
        let path = shape.stream_path.clone();
        self.deliver(&path, envs).await;
        Ok(true)
    }

}

/// Drop deletes (`w <= 0`) for pks this shape's `known_members` doesn't hold — this shape never
/// told its stream that pk was a member, so a delete for it is a true no-op, not an idempotent
/// one, and delivering it would needlessly wake every live long-poll on this shape (durable-streams
/// treats any non-empty append as new data). Updates `known_members` to match what's kept: an
/// emitted insert/update adds its pk, an emitted delete removes it.
fn filter_known_members(
    ts: &TableSchema,
    out: Vec<(Row, ZWeight)>,
    known_members: &std::sync::Mutex<std::collections::HashSet<String>>,
) -> Vec<(Row, ZWeight)> {
    let mut known = known_members.lock().unwrap();
    out.into_iter()
        .filter(|(row, w)| {
            let pk = ts.key_string(row).unwrap_or_default();
            if *w > 0 {
                known.insert(pk);
                true
            } else {
                known.remove(&pk)
            }
        })
        .collect()
}

// --- deferred flip propagation ------------------------------------------------------------------
//
// Runs on the engine's flip-worker pool (semaphore-bounded, `ELECTRIC_IVM_FLIP_WORKERS`), NOT
// inside the table tailers, so the flip-driven Postgres query-backs neither sit on the tailer
// hot path nor serialize on a single task. Two invariants make this sound:
//
//  * **Deferral**: every emission is absolute (per pk: upsert if the row matches *now*, else
//    idempotent delete), so a propagation that runs later re-derives from the then-current
//    Postgres and node state and converges regardless of when — or on which worker — it runs.
//    The convergence barrier gains one term: the engine's pending counter must drain to zero
//    (`GET /replication/lsn` → `pendingFlips`), and that counter also covers emission-lane
//    batches until they LAND on their streams.
//  * **Eval+enqueue atomicity + per-stream FIFO**: membership evaluation and the enqueue of the
//    resulting envelopes happen under one registry-lock scope, and each shape stream drains
//    through exactly one ordered emission lane (`engine::emission`). Per-stream append order
//    therefore equals evaluation order — a move evaluated at time t1 can never land *after* an
//    emission evaluated at t2 > t1 for the same pk (which would leave the stream's last word
//    stale — permanent divergence). This is the same guarantee the old
//    hold-the-lock-across-append design gave, without network under the lock and without a
//    single-task bottleneck. Postgres round-trips run outside the lock, concurrently.

/// Propagate a batch of inner-set flips up the dependency DAG (BFS), querying back affected rows.
pub async fn propagate_flips(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    mut work: VecDeque<(SubquerySig, Flip)>,
    txid: Option<String>,
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
) -> Result<()> {
    while let Some((sig, flip)) = work.pop_front() {
        // The flipped inner-set node's dependents, plus the table its change entered through: the
        // head of the propagation path each dependent's trace lights (`table:<t>` → `node:<sig>` →
        // dependent). Fetched under one lock so a concurrent drop can't split them.
        let (edges, source_table) = {
            let reg = registry.lock().await;
            (reg.edges_of(&sig), reg.nodes.get(&sig).map(|n| n.inner_table.clone()))
        };
        for edge in edges {
            // A NULL-value flip only matters to NULL-sensitive dependents — a `NOT IN` leaf, or an
            // `IN` leaf under any `Not{…}` (SQL: a NULL in the set makes the leaf UNKNOWN, which
            // negation turns into a membership change). It can shift *every* dependent row, so
            // re-derive the dependent fully; NULL-insensitive dependents can't change (AND/OR are
            // monotone over FALSE < UNKNOWN < TRUE), so skip.
            if matches!(flip.value, Value::Null) {
                if edge.null_sensitive {
                    rederive_dependent(registry, &edge, txid.clone(), &mut work).await?;
                }
                continue;
            }
            match &edge.dependent {
                Dependent::Shape(id) => {
                    let moved =
                        move_shape_for_value(registry, id, edge.connecting_col, &flip.value, txid.clone()).await?;
                    // Light the whole path only when the shape actually moved rows: source
                    // `table:<t>` → the flipped `node:<sig>` → this `shape:<id>`.
                    if let (Some((outer, net)), Some(src)) = (moved, source_table.as_deref()) {
                        emit_flip_trace(trace_tx, &outer, src, &sig, format!("shape:{id}"), vec![id.clone()], net);
                    }
                }
                Dependent::Node(parent_sig) => {
                    let new_flips =
                        reconcile_parent_for_value(registry, parent_sig, edge.connecting_col, &flip.value).await?;
                    if let Some((_inner, flips)) = new_flips {
                        // A nested `IN`: connect the flipped child `node:<sig>` to the parent
                        // `node:<parent_sig>` it re-derived, so the propagation reads through. The
                        // parent's own downstream shape lights when its flips reach a shape edge.
                        if let (false, Some(src)) = (flips.is_empty(), source_table.as_deref()) {
                            emit_flip_trace(trace_tx, src, src, &sig, format!("node:{parent_sig}"), Vec::new(), flip_net(&flips));
                        }
                        for f in flips {
                            work.push_back((parent_sig.clone(), f));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// An inner-set value `v` flipped for an outer shape: query the outer rows with `connecting_col = v`,
/// re-evaluate the full shape predicate, and append `upsert` (matches) / `delete` (doesn't) by pk.
/// Returns `Some((outer_table, net_weight))` when envelopes were appended — the shape's own table
/// (the event's `table`) and the net membership change (for the trace dot's label/colour) — or
/// `None` when nothing moved.
async fn move_shape_for_value(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    shape_id: &str,
    connecting_col: usize,
    value: &Value,
    txid: Option<String>,
) -> Result<Option<(String, i64)>> {
    // Brief lock: snapshot what the query-back needs.
    let (ts, pg_url) = {
        let reg = registry.lock().await;
        let Some(shape) = reg.shapes.get(shape_id) else { return Ok(None) };
        (reg.snapshot_for_table(&shape.outer_table)?, reg.pg_url.clone())
    };
    let rows = query_candidates(&pg_url, &ts, connecting_col, value).await?;
    if rows.is_empty() {
        return Ok(None);
    }
    // Evaluate membership against the *current* node sets and append, atomically under the lock
    // (see the module comment on eval+append atomicity).
    let reg = registry.lock().await;
    let Some(shape) = reg.shapes.get(shape_id) else { return Ok(None) };
    let out: Vec<(Row, ZWeight)> = rows
        .into_iter()
        .map(|r| {
            let w: ZWeight = if shape.pred.matches_ctx(&r, &*reg) { 1 } else { -1 };
            (r, w)
        })
        .collect();
    let out = filter_known_members(&ts, out, &shape.known_members);
    if out.is_empty() {
        return Ok(None);
    }
    // Net membership change this move applies (enters +1, leaves −1), for the trace dot.
    let net: i64 = out.iter().map(|(_, w)| *w as i64).sum();
    let envs = crate::engine::translate_output(&ts, out, txid, None, shape.out_cols.as_deref().map(Vec::as_slice));
    if envs.is_empty() {
        return Ok(None);
    }
    shape.emitted.fetch_add(envs.len() as u64, std::sync::atomic::Ordering::Relaxed);
    // Ordered delivery: enqueued under the registry lock (lane FIFO ⇒ lands in eval order).
    let path = shape.stream_path.clone();
    reg.deliver(&path, envs).await;
    Ok(Some((ts.name.clone(), net)))
}

/// A parent node's value `v` was referenced by a flipped child; re-evaluate the parent's inner rows
/// with `connecting_col = v` and reconcile them. Returns `Some((inner_table, flips))`, or `None` if
/// the parent vanished.
async fn reconcile_parent_for_value(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    parent_sig: &SubquerySig,
    connecting_col: usize,
    value: &Value,
) -> Result<Option<(String, Vec<Flip>)>> {
    let (ts, pg_url) = {
        let reg = registry.lock().await;
        let Some(n) = reg.nodes.get(parent_sig) else { return Ok(None) };
        (reg.snapshot_for_table(&n.inner_table)?, reg.pg_url.clone())
    };
    let rows = query_candidates(&pg_url, &ts, connecting_col, value).await?;
    let mut reg = registry.lock().await;
    let (pred, proj) = match reg.nodes.get(parent_sig) {
        Some(n) => (n.pred.clone(), n.proj_col),
        None => return Ok(None),
    };
    let evals: Vec<(String, Option<Value>)> = rows
        .iter()
        .map(|r| {
            let pk = ts.key_string(r).unwrap_or_default();
            let pv = if pred.matches_ctx(r, &*reg) {
                Some(r.0.get(proj).cloned().unwrap_or(Value::Null))
            } else {
                None
            };
            (pk, pv)
        })
        .collect();
    Ok(Some((ts.name.clone(), reg.apply_node_flips(parent_sig, evals))))
}

/// Re-derive a dependent fully (used for NULL flips on negated edges): re-query every candidate row
/// of the dependent's table and reconcile/emit. Rare (projections are typically non-null).
async fn rederive_dependent(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    edge: &Edge,
    txid: Option<String>,
    work: &mut VecDeque<(SubquerySig, Flip)>,
) -> Result<()> {
    match &edge.dependent {
        Dependent::Shape(id) => {
            let (ts, pg_url) = {
                let reg = registry.lock().await;
                let Some(s) = reg.shapes.get(id) else { return Ok(()) };
                (reg.snapshot_for_table(&s.outer_table)?, reg.pg_url.clone())
            };
            let rows = query_all(&pg_url, &ts).await?;
            // Eval + append atomically under the lock (see the module comment).
            let reg = registry.lock().await;
            let Some(s) = reg.shapes.get(id) else { return Ok(()) };
            let out: Vec<(Row, ZWeight)> = rows
                .into_iter()
                .map(|r| {
                    let w: ZWeight = if s.pred.matches_ctx(&r, &*reg) { 1 } else { -1 };
                    (r, w)
                })
                .collect();
            let out = filter_known_members(&ts, out, &s.known_members);
            if out.is_empty() {
                return Ok(());
            }
            let envs =
                crate::engine::translate_output(&ts, out, txid, None, s.out_cols.as_deref().map(Vec::as_slice));
            if envs.is_empty() {
                return Ok(());
            }
            s.emitted.fetch_add(envs.len() as u64, std::sync::atomic::Ordering::Relaxed);
            let path = s.stream_path.clone();
            reg.deliver(&path, envs).await;
        }
        Dependent::Node(parent_sig) => {
            let (ts, pg_url) = {
                let reg = registry.lock().await;
                let Some(n) = reg.nodes.get(parent_sig) else { return Ok(()) };
                (reg.snapshot_for_table(&n.inner_table)?, reg.pg_url.clone())
            };
            let rows = query_all(&pg_url, &ts).await?;
            let mut reg = registry.lock().await;
            let (pred, proj) = match reg.nodes.get(parent_sig) {
                Some(n) => (n.pred.clone(), n.proj_col),
                None => return Ok(()),
            };
            let evals: Vec<(String, Option<Value>)> = rows
                .iter()
                .map(|r| {
                    let pk = ts.key_string(r).unwrap_or_default();
                    let pv = if pred.matches_ctx(r, &*reg) {
                        Some(r.0.get(proj).cloned().unwrap_or(Value::Null))
                    } else {
                        None
                    };
                    (pk, pv)
                })
                .collect();
            for f in reg.apply_node_flips(parent_sig, evals) {
                work.push_back((parent_sig.clone(), f));
            }
        }
    }
    Ok(())
}

// Candidate-row resolution (arrangement snapshot → pooled Postgres fallback) is the shared
// membership kernel's — one implementation for this registry and for circuit cohort serving.
use crate::engine::membership::{query_rows_all as query_all, query_rows_by_col as query_candidates};

/// Net membership change carried by a batch of parent-node flips (enters +1, leaves −1), for the
/// trace dot's label/colour.
fn flip_net(flips: &[Flip]) -> i64 {
    flips
        .iter()
        .map(|f| match f.dir {
            FlipDir::Enter => 1,
            FlipDir::Leave => -1,
        })
        .sum()
}

/// Broadcast a lossy trace event lighting the WHOLE path a deferred inner-set flip travelled: the
/// source inner `table:<t>` the change entered through, the flipped inner-set `node:<sig>` (its
/// `IN-SET ARRANGE`/distinct), and the re-derived `dependent` (`shape:<sid>` for an outer subquery
/// shape, or a parent `node:<sig>` for nested `IN`). The originating envelope's own trace stops at
/// the inner-set node — the propagator moves the dependent out of band, after the query-backs — so
/// without this the visualizer flashes the source, fades the moved shape off that direct change's
/// path, and never pulses the serving edge (an edge pulses only when both endpoints flash). One
/// synthetic weighted row carries the net membership change so the travelling dot is labelled +1 /
/// −1 and coloured. Best-effort and zero-cost when no one is subscribed, mirroring the in-engine
/// trace path.
///
/// `event_table` is the table the event is *about* (the dependent shape's own table, matching the
/// direct-change trace's `table`); `source_table` is where the change entered and heads the hop path
/// (they differ: a `project_members` change moves an `issues` shape).
fn emit_flip_trace(
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
    event_table: &str,
    source_table: &str,
    node_sig: &SubquerySig,
    dependent: String,
    shapes: Vec<String>,
    net: i64,
) {
    if trace_tx.receiver_count() == 0 {
        return;
    }
    let ev = crate::trace::TraceEvent {
        lsn: None,
        txid: None,
        table: event_table.to_string(),
        // One synthetic weighted row carrying the net change: a single flip can move many outer
        // rows, so the payload is not one table row — left empty, weighted by the net.
        delta: vec![crate::trace::TraceDelta { row: serde_json::json!({}), w: net }],
        hops: vec![
            crate::trace::TraceHop::new(format!("table:{source_table}"), "passed"),
            crate::trace::TraceHop::new(format!("node:{node_sig}"), "passed"),
            crate::trace::TraceHop::new(dependent, "passed"),
        ],
        shapes,
    };
    if let Ok(json) = serde_json::to_string(&ev) {
        let _ = trace_tx.send(Arc::new(json));
    }
}

impl SubqueryCollector for SubqueryRegistry {
    /// Discover (or dedupe) a subquery node: compile its inner predicate (recursively collecting deeper
    /// nodes), record its child edges, and queue it for seeding. Returns the canonical signature.
    fn collect(&mut self, table: &str, project: &str, where_: Option<&PredicateJson>) -> Result<SubquerySig> {
        let sig = subquery_sig(table, project, where_);
        if let Some(n) = self.nodes.get_mut(&sig) {
            n.refcount += 1;
            self.collect_log.push(sig.clone());
            return Ok(sig);
        }
        let inner_ts = self.schemas.get(table).cloned().context("subquery: unknown inner table")?;
        let inner_pred = match where_ {
            Some(w) => CompiledPredicate::compile_with(w, &inner_ts, self)?,
            None => CompiledPredicate::MatchAll,
        };
        // Record edges from each child node to THIS node (so a child flip re-derives this node's rows).
        for leaf in collect_in_leaves(&inner_pred) {
            self.edges.push(Edge {
                node_sig: leaf.sig,
                dependent: Dependent::Node(sig.clone()),
                connecting_col: leaf.col,
                negated: leaf.negated,
                null_sensitive: leaf.null_sensitive,
            });
        }
        let proj_col = inner_ts.column_index(project)?;
        let mut node =
            SubqueryNode::new(sig.clone(), table.to_string(), proj_col, inner_ts.pk_index, Arc::new(inner_pred));
        node.where_json = where_.cloned();
        node.refcount = 1;
        self.nodes.insert(sig.clone(), node);
        self.collect_log.push(sig.clone());
        self.pending_seed.push(sig.clone());
        Ok(sig)
    }
}

impl SubqueryEval for SubqueryRegistry {
    fn contains(&self, sig: &SubquerySig, value: &Value) -> bool {
        self.nodes.get(sig).is_some_and(|n| n.contains(value))
    }
    fn has_null(&self, sig: &SubquerySig) -> bool {
        self.nodes.get(sig).is_some_and(|n| n.has_null())
    }
}

/// One `IN (SELECT …)` leaf found in a compiled predicate, with the context needed to build its
/// dependency edge.
pub struct InLeaf {
    pub col: usize,
    pub sig: SubquerySig,
    pub negated: bool,
    /// leaf negated OR under any `Not{…}` wrapper — see [`Edge::null_sensitive`].
    pub null_sensitive: bool,
}

/// Find all `IN (SELECT …)` leaves in a compiled predicate, tracking whether each sits under a `Not`
/// (which makes it NULL-sensitive even when the leaf itself isn't negated — `NOT (x IN S)` flips
/// membership when a NULL enters `S`, exactly like `x NOT IN S`).
pub fn collect_in_leaves(p: &CompiledPredicate) -> Vec<InLeaf> {
    let mut out = Vec::new();
    fn go(p: &CompiledPredicate, under_not: bool, out: &mut Vec<InLeaf>) {
        match p {
            CompiledPredicate::And(v) | CompiledPredicate::Or(v) => {
                v.iter().for_each(|c| go(c, under_not, out))
            }
            CompiledPredicate::Not(b) => go(b, true, out),
            CompiledPredicate::InSubquery { col, sig, negated } => out.push(InLeaf {
                col: *col,
                sig: sig.clone(),
                negated: *negated,
                null_sensitive: *negated || under_not,
            }),
            _ => {}
        }
    }
    go(p, false, &mut out);
    out
}

/// Does a JSON predicate contain any `IN (SELECT …)` subquery?
pub fn predicate_has_subquery(p: &PredicateJson) -> bool {
    match p {
        PredicateJson::In { .. } => true,
        PredicateJson::And { and } => and.iter().any(predicate_has_subquery),
        PredicateJson::Or { or } => or.iter().any(predicate_has_subquery),
        PredicateJson::Not { not } => predicate_has_subquery(not),
        PredicateJson::Leaf { .. } | PredicateJson::IsNull { .. } => false,
    }
}

/// Every table referenced by a JSON predicate's subqueries (inner tables, recursively).
pub fn referenced_tables(p: &PredicateJson) -> Vec<String> {
    let mut out = Vec::new();
    fn go(p: &PredicateJson, out: &mut Vec<String>) {
        match p {
            PredicateJson::In { subquery, .. } => {
                if !out.contains(&subquery.table) {
                    out.push(subquery.table.clone());
                }
                if let Some(w) = &subquery.where_ {
                    go(w, out);
                }
            }
            PredicateJson::And { and } => and.iter().for_each(|c| go(c, out)),
            PredicateJson::Or { or } => or.iter().for_each(|c| go(c, out)),
            PredicateJson::Not { not } => go(not, out),
            PredicateJson::Leaf { .. } | PredicateJson::IsNull { .. } => {}
        }
    }
    go(p, &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node() -> SubqueryNode {
        SubqueryNode::new("sig".into(), "t".into(), 0, 1, Arc::new(CompiledPredicate::MatchAll))
    }

    /// The trace reports an inner-table delta's effect on a subquery node: `passed` when the
    /// inner set flipped (a value entered/left), `dropped` when it didn't change.
    #[tokio::test]
    async fn trace_subquery_node_hops() {
        use crate::schema::TableDef;
        let ts = {
            let def: TableDef = serde_json::from_value(serde_json::json!({
                "columns": { "id": {"type":"int"} }, "primaryKey": "id"
            }))
            .unwrap();
            crate::schema::TableSchema::from_def("t", &def).unwrap()
        };
        let mut reg = SubqueryRegistry::new(crate::ds::DsClient::new("http://127.0.0.1:1"), None);
        reg.nodes.insert("sig1".into(), node_with_sig("sig1"));

        // A new row projects value 1 into the inner set -> Enter flip -> passed.
        let delta = vec![Tup2(Row(vec![Value::Int(1)]), 1)];
        let mut hops = Vec::new();
        reg.on_table_delta(&ts, &delta, 0, None, None, Some(&mut hops)).await.unwrap();
        assert!(
            hops.iter().any(|h| h.node == "node:sig1" && h.outcome == "passed"),
            "expected passed node hop, got {hops:?}"
        );

        // The same row again: the value is already present -> no flip -> dropped.
        let mut hops = Vec::new();
        reg.on_table_delta(&ts, &delta, 0, None, None, Some(&mut hops)).await.unwrap();
        assert!(
            hops.iter().any(|h| h.node == "node:sig1" && h.outcome == "dropped"),
            "expected dropped node hop, got {hops:?}"
        );
    }

    /// A deferred inner-set flip that moves a dependent shape must light the WHOLE propagation path
    /// — the source inner `table:`, the flipped `node:<sig>` (IN-SET ARRANGE/distinct), and the
    /// re-derived `shape:<sid>` — so the visualizer animates the moved shape instead of fading it
    /// off the direct change's path (the "leaving a project" bug).
    #[test]
    fn flip_trace_lights_source_node_and_shape() {
        let (trace_tx, mut trace_rx) = tokio::sync::broadcast::channel::<Arc<String>>(8);
        // A `project_members` delete flipped inner-set value; the `issues` shape s1 lost 103 rows.
        let sig = "project_members|project_id|L(user_id,Eq,1)".to_string();
        emit_flip_trace(&trace_tx, "issues", "project_members", &sig, "shape:s1".into(), vec!["s1".into()], -103);

        let ev: serde_json::Value = serde_json::from_str(&trace_rx.try_recv().unwrap()).unwrap();
        // The event is about the dependent shape's table; the path still heads at the source.
        assert_eq!(ev["table"], "issues");
        let outcome = |node: &str| {
            ev["hops"].as_array().unwrap().iter().find(|h| h["node"] == node).map(|h| h["outcome"].clone())
        };
        assert_eq!(outcome("table:project_members"), Some(serde_json::json!("passed")), "source lit");
        assert_eq!(outcome(&format!("node:{sig}")), Some(serde_json::json!("passed")), "subquery node lit");
        assert_eq!(outcome("shape:s1"), Some(serde_json::json!("passed")), "dependent shape lit");
        assert_eq!(ev["shapes"].as_array().unwrap(), &vec![serde_json::json!("s1")]);
        assert_eq!(ev["delta"][0]["w"], -103, "the dot carries the real net −103 leave, not 0");
    }

    /// Gating: a flip that changes no dependent membership emits nothing. Here a NULL flip reaches a
    /// plain (non-negated) `IN` dependent, which NULL can't move — `propagate_flips` skips it, so no
    /// path lights and the visualizer fades nothing spuriously.
    #[tokio::test]
    async fn flip_no_op_emits_no_trace() {
        let mut reg = SubqueryRegistry::new(DsClient::new("http://unused"), None);
        reg.nodes.insert("sig1".into(), node_with_sig("sig1"));
        reg.edges.push(Edge {
            node_sig: "sig1".into(),
            dependent: Dependent::Shape("s7".into()),
            connecting_col: 0,
            negated: false,
            null_sensitive: false,
        });
        let reg = tokio::sync::Mutex::new(reg);
        let (trace_tx, mut trace_rx) = tokio::sync::broadcast::channel::<Arc<String>>(8);

        let mut work: VecDeque<(SubquerySig, Flip)> = VecDeque::new();
        work.push_back(("sig1".into(), Flip { value: Value::Null, dir: FlipDir::Enter }));
        propagate_flips(&reg, work, None, &trace_tx).await.unwrap();
        assert!(trace_rx.try_recv().is_err(), "a NULL flip on a non-null-sensitive dependent emits nothing");
    }

    fn node_with_sig(sig: &str) -> SubqueryNode {
        SubqueryNode::new(sig.into(), "t".into(), 0, 1, Arc::new(CompiledPredicate::MatchAll))
    }

    #[test]
    fn reconcile_enter_and_leave_on_first_and_last_contributor() {
        let mut n = node();
        assert_eq!(n.reconcile_row("a", Some(Value::Int(5))), vec![Flip { value: Value::Int(5), dir: FlipDir::Enter }]);
        assert!(n.contains(&Value::Int(5)));
        // second contributor to the same value -> no flip
        assert_eq!(n.reconcile_row("b", Some(Value::Int(5))), vec![]);
        // removing one of two -> still present, no flip
        assert_eq!(n.reconcile_row("a", None), vec![]);
        assert!(n.contains(&Value::Int(5)));
        // removing the last -> Leave
        assert_eq!(n.reconcile_row("b", None), vec![Flip { value: Value::Int(5), dir: FlipDir::Leave }]);
        assert!(!n.contains(&Value::Int(5)));
    }

    #[test]
    fn reconcile_value_change_emits_leave_then_enter() {
        let mut n = node();
        n.reconcile_row("a", Some(Value::Int(5)));
        let flips = n.reconcile_row("a", Some(Value::Int(7)));
        assert_eq!(
            flips,
            vec![
                Flip { value: Value::Int(5), dir: FlipDir::Leave },
                Flip { value: Value::Int(7), dir: FlipDir::Enter },
            ]
        );
        assert!(!n.contains(&Value::Int(5)));
        assert!(n.contains(&Value::Int(7)));
    }

    #[test]
    fn reconcile_same_value_is_a_noop() {
        let mut n = node();
        n.reconcile_row("a", Some(Value::Int(5)));
        assert_eq!(n.reconcile_row("a", Some(Value::Int(5))), vec![]);
        // unchanged absence is also a no-op
        assert_eq!(n.reconcile_row("z", None), vec![]);
    }

    #[test]
    fn null_bucket_tracks_has_null() {
        let mut n = node();
        assert_eq!(n.reconcile_row("a", Some(Value::Null)), vec![Flip { value: Value::Null, dir: FlipDir::Enter }]);
        assert!(n.has_null());
        assert_eq!(n.reconcile_row("a", None), vec![Flip { value: Value::Null, dir: FlipDir::Leave }]);
        assert!(!n.has_null());
    }

    /// `NOT (x IN S)` is exactly as NULL-sensitive as `x NOT IN S`: a NULL entering `S` turns the leaf
    /// UNKNOWN, and the enclosing NOT converts that into a membership change. The edge must record it,
    /// or a NULL flip silently skips the re-derivation and members go stale.
    #[test]
    fn null_sensitivity_tracks_not_wrappers_and_negated_leaves() {
        use crate::schema::TableDef;
        let ts = {
            let def: TableDef = serde_json::from_value(serde_json::json!({
                "columns": { "id": {"type":"int"}, "gid": {"type":"int"} }, "primaryKey": "id"
            }))
            .unwrap();
            crate::schema::TableSchema::from_def("outer_t", &def).unwrap()
        };
        struct Rec;
        impl crate::predicate::SubqueryCollector for Rec {
            fn collect(&mut self, t: &str, p: &str, w: Option<&PredicateJson>) -> Result<SubquerySig> {
                Ok(crate::predicate::subquery_sig(t, p, w))
            }
        }
        let compile = |j: serde_json::Value| {
            CompiledPredicate::compile_with(&serde_json::from_value(j).unwrap(), &ts, &mut Rec).unwrap()
        };
        let in_sub = serde_json::json!({"col":"gid","in":{"table":"outer_t","project":"gid"}});

        // plain IN: not NULL-sensitive (FALSE↔UNKNOWN can't change inclusion without negation)
        let leaves = collect_in_leaves(&compile(in_sub.clone()));
        assert!(!leaves[0].negated && !leaves[0].null_sensitive);

        // NOT IN leaf: NULL-sensitive
        let mut neg = in_sub.clone();
        neg["negated"] = serde_json::json!(true);
        let leaves = collect_in_leaves(&compile(neg));
        assert!(leaves[0].negated && leaves[0].null_sensitive);

        // IN under a Not wrapper: NULL-sensitive even though the leaf isn't negated
        let leaves = collect_in_leaves(&compile(serde_json::json!({"not": in_sub.clone()})));
        assert!(!leaves[0].negated && leaves[0].null_sensitive);

        // IN nested under Not(And(...)): still NULL-sensitive
        let leaves = collect_in_leaves(&compile(serde_json::json!({
            "not": {"and": [ {"col":"id","op":"gt","value":0}, in_sub ]}
        })));
        assert!(leaves[0].null_sensitive);
    }

    /// A failed create (here: no Postgres to seed from) must roll the registry back to exactly its
    /// prior state — no orphaned node, edge, or pending-seed entry that a later identical create
    /// would silently join and read unseeded (wrong) membership from.
    #[tokio::test]
    async fn failed_create_rolls_back_nodes_and_edges() {
        use crate::schema::TableDef;
        let mk = |name: &str| {
            let def: TableDef = serde_json::from_value(serde_json::json!({
                "columns": { "id": {"type":"int"}, "gid": {"type":"int"} }, "primaryKey": "id"
            }))
            .unwrap();
            crate::schema::TableSchema::from_def(name, &def).unwrap()
        };
        let mut schemas = HashMap::new();
        schemas.insert("outer_t".to_string(), mk("outer_t"));
        schemas.insert("inner_t".to_string(), mk("inner_t"));
        // No pg_url: node seeding must fail after collect() has already registered the node.
        let mut reg = SubqueryRegistry::new(DsClient::new("http://unused"), None);
        reg.set_schemas(Arc::new(schemas));
        let where_json: PredicateJson = serde_json::from_value(serde_json::json!({
            "col":"gid","in":{"table":"inner_t","project":"gid"}
        }))
        .unwrap();
        // Three-phase: begin registers (nodes buffering, edges, pending shape); a phase-B
        // failure is rolled back exactly by abort_create.
        let begin = reg.begin_create("s1", "outer_t", "shape/s1", &where_json, None, false).unwrap();
        assert_eq!(begin.seeds.len(), 1, "one fresh node to seed");
        assert_eq!(reg.nodes.len(), 1);
        assert!(reg.nodes.values().all(|n| n.seed_buffer.is_some()), "fresh node buffers");
        assert!(reg.touches("outer_t"), "pending shape routes its outer table");
        reg.abort_create("s1");
        assert_eq!(reg.nodes.len(), 0, "aborted create left an orphaned node");
        assert_eq!(reg.edges.len(), 0, "aborted create left orphaned edges");
        assert_eq!(reg.pending_seed.len(), 0, "aborted create left a pending seed");
        assert!(reg.shapes.is_empty());
        assert!(reg.pending_shapes.is_empty());
    }

    /// `filter_known_members` is the fix for the live-poll wake-storm bug: a subquery shape's
    /// `emit_shape_delta`/`move_shape_for_value`/`rederive_dependent` compute an *absolute*
    /// membership weight (+1/-1) for every touched pk, regardless of whether that shape ever had
    /// the pk — by design (see `emit_shape_delta`'s doc comment), since a delete is "idempotent by
    /// pk" from a correctness standpoint. But delivering that idempotent-but-spurious delete to the
    /// shape's durable-stream still counts as new data to a live long-poll: durable-streams wakes on
    /// any non-empty append, and every OTHER write to the same outer table — matching this shape's
    /// predicate or not — would resolve this shape's live poll early with an empty "up-to-date"
    /// (electric.rs's client-side key-set filter drops the bogus delete before it reaches the
    /// client, but by then the round-trip is already spent). Never-known pks must be dropped before
    /// they reach `deliver()`, not just before they reach the client.
    #[test]
    fn filter_known_members_drops_deletes_for_never_known_pks() {
        use crate::schema::TableDef;
        let def: TableDef = serde_json::from_value(serde_json::json!({
            "columns": { "id": {"type":"int"}, "gid": {"type":"int"} }, "primaryKey": "id"
        }))
        .unwrap();
        let ts = crate::schema::TableSchema::from_def("t", &def).unwrap();
        let known: std::sync::Mutex<std::collections::HashSet<String>> = std::sync::Mutex::new(HashSet::new());
        let row = |id: i64| Row(vec![Value::Int(id), Value::Int(0)]);
        let pk1 = ts.key_string(&row(1)).unwrap();

        // A "leave" for a pk this shape never claimed as a member is a true no-op: this shape's
        // stream never told anyone pk 1 was there, so a delete for it is spurious, not idempotent —
        // it must be dropped, not delivered (the bug this test guards).
        let out = filter_known_members(&ts, vec![(row(1), -1)], &known);
        assert!(out.is_empty(), "delete for a never-known pk must be dropped");
        assert!(known.lock().unwrap().is_empty());

        // An "enter" is always kept and recorded as known.
        let out = filter_known_members(&ts, vec![(row(1), 1)], &known);
        assert_eq!(out.len(), 1, "insert must be kept");
        assert!(known.lock().unwrap().contains(&pk1));

        // Now that pk 1 is known, a genuine "leave" is kept and clears the record.
        let out = filter_known_members(&ts, vec![(row(1), -1)], &known);
        assert_eq!(out.len(), 1, "delete for a known pk must be kept");
        assert!(!known.lock().unwrap().contains(&pk1));

        // Deleting the same (now-unknown-again) pk a second time is dropped once more.
        let out = filter_known_members(&ts, vec![(row(1), -1)], &known);
        assert!(out.is_empty(), "repeat delete for an already-removed pk must be dropped");
    }

    #[test]
    fn registry_eval_reads_node_sets() {
        let mut reg = SubqueryRegistry::new(DsClient::new("http://unused"), None);
        let mut n = node();
        n.reconcile_row("a", Some(Value::Int(1)));
        n.reconcile_row("b", Some(Value::Null));
        reg.nodes.insert("sig".into(), n);
        assert!(reg.contains(&"sig".to_string(), &Value::Int(1)));
        assert!(!reg.contains(&"sig".to_string(), &Value::Int(2)));
        assert!(reg.has_null(&"sig".to_string()));
        // unknown sig -> empty
        assert!(!reg.contains(&"other".to_string(), &Value::Int(1)));
    }
}
