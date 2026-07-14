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
///
/// The set itself lives in the **membership circuit** (`crate::subq_circuit`) as the
/// `(node_id, value)` slice of one shared relation; the node keeps only the host-side reverse
/// index (`pk_value`) that makes maintenance reconcile-by-identity — evaluation can depend on
/// *other* nodes' current sets (nested `IN`), so a row's tuple is not a pure function of the
/// row and exact retraction needs the remembered old value.
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
    /// The node's key in the membership circuit (registry-assigned, unique per live node).
    pub node_id: i64,
    /// The template this node is a bind of (see [`crate::predicate::subquery_template`]).
    pub template_key: String,
    /// The lifted parameter literals, positionally aligned with the template's `param_cols`.
    pub(crate) bind: Row,
    /// inner-row pk -> its current projected value (reverse index, for O(1) reconciliation
    /// and exact circuit retractions).
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
        node_id: i64,
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
            node_id,
            template_key: String::new(),
            bind: Row(Vec::new()),
            pk_value: HashMap::new(),
            refcount: 0,
        }
    }

    /// Number of contributing inner-row pks (the dominant per-node state term).
    pub fn contributor_count(&self) -> usize {
        self.pk_value.len()
    }

    /// Reconcile inner-row `pk`'s contribution so it equals `present_value` (its projected
    /// value if the row currently matches `pred`, else `None`). Returns the **circuit tuples**
    /// realizing the change (retract the old contribution, insert the new — at most two);
    /// flips come back from the circuit when the tuples are applied. History-independent and
    /// idempotent: an unchanged contribution produces no tuples.
    pub fn reconcile_row_tuples(&mut self, pk: &str, present_value: Option<Value>) -> Vec<Tup2<Row, ZWeight>> {
        if self.pk_value.get(pk) == present_value.as_ref() {
            return Vec::new();
        }
        let mut tuples = Vec::new();
        if let Some(old_v) = self.pk_value.remove(pk) {
            tuples.push(Tup2(Row(vec![Value::Int(self.node_id), old_v, Value::Text(pk.to_string())]), -1));
        }
        if let Some(v) = present_value {
            self.pk_value.insert(pk.to_string(), v.clone());
            tuples.push(Tup2(Row(vec![Value::Int(self.node_id), v, Value::Text(pk.to_string())]), 1));
        }
        tuples
    }
}

/// One subquery template: the shared evaluation structure for every node (bind) whose inner
/// query differs only in the lifted equality literals — the KeyRouter factoring applied to
/// subqueries. A delta on the inner table is evaluated ONCE per template (residual + param
/// projection), then routed to the single affected bind by hash lookup, instead of one full
/// predicate eval per literal-keyed node.
pub(crate) struct TemplateGroup {
    pub(crate) inner_table: String,
    /// Column index (in `inner_table`) of the projected value (same for every bind).
    proj_col: usize,
    /// The compiled residual (lifted equalities removed, other literals baked in). May contain
    /// nested `IN` leaves, resolved via [`SubqueryEval`] against already-collected child nodes.
    residual: Arc<CompiledPredicate>,
    /// Column indices of the lifted parameters, aligned with each bind `Row`.
    param_cols: Vec<usize>,
    /// bind literals -> the node serving that bind.
    pub(crate) binds: HashMap<Row, SubquerySig>,
    /// pk -> nodes of this template currently holding a contribution from that pk. An exact
    /// inverted index over the nodes' `pk_value` maps (maintained in lockstep by
    /// `reconcile_node_row`), so a row that stops matching finds its old bind in O(1) instead
    /// of scanning every bind.
    pk_nodes: HashMap<String, HashSet<SubquerySig>>,
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
    /// The template this node is a bind of — equal across nodes that differ only in lifted
    /// equality literals (template-level sharing).
    pub template: String,
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
    /// Edges from each node to its dependents, keyed by the node's signature — flip
    /// propagation looks up ONE node's dependents per flip, so this must not be a scan over
    /// every edge in the registry (the propagation-side analogue of template-grouped eval).
    edges: HashMap<SubquerySig, Vec<Edge>>,
    /// Edges appended by the in-flight `begin_create` compile, committed into `edges` only
    /// when the whole registration succeeds (a failed/conflicted compile just clears this —
    /// exact rollback without index bookkeeping).
    staged_edges: Vec<Edge>,
    /// Registered outer subquery shapes by engine shape id.
    pub shapes: HashMap<String, SubqueryShape>,
    /// The membership circuit: every node's value set as one dbsp relation; flip detection is
    /// the circuit's incremental distinct (see `crate::subq_circuit`).
    circuit: crate::subq_circuit::MembershipCircuit,
    /// Next circuit node id (monotonic; ids of dropped nodes are never reused, so a stale
    /// snapshot read can never alias a new node's slice).
    next_node_id: i64,
    /// circuit node id -> node signature (maps circuit flip deltas back to nodes).
    node_by_id: HashMap<i64, SubquerySig>,
    /// Shared evaluation templates (see [`TemplateGroup`]), keyed by
    /// [`crate::predicate::subquery_template`]'s key.
    pub(crate) templates: HashMap<String, TemplateGroup>,
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
            edges: HashMap::new(),
            staged_edges: Vec::new(),
            shapes: HashMap::new(),
            circuit: crate::subq_circuit::MembershipCircuit::start()
                .expect("membership circuit failed to start"),
            next_node_id: 1,
            node_by_id: HashMap::new(),
            templates: HashMap::new(),
            pending_seed: Vec::new(),
            pending_shapes: Vec::new(),
            collect_log: Vec::new(),
            ds,
            pg_url,
            schemas: Arc::new(HashMap::new()),
            lanes: None,
        }
    }

    /// Apply contributor tuples to the membership circuit and map its flip deltas back to
    /// node signatures. Callers hold the registry lock across the await — the circuit thread
    /// never takes this lock, and awaiting the step is what gives every later membership read
    /// (`contains`/`has_null`) read-your-writes over this batch.
    async fn apply_tuples(&mut self, tuples: Vec<Tup2<Row, ZWeight>>) -> Vec<(SubquerySig, Flip)> {
        if tuples.is_empty() {
            return Vec::new();
        }
        self.circuit
            .apply(tuples)
            .await
            .into_iter()
            .filter_map(|d| {
                let sig = self.node_by_id.get(&d.node_id)?.clone();
                let dir = if d.delta > 0 { FlipDir::Enter } else { FlipDir::Leave };
                Some((sig, Flip { value: d.value, dir }))
            })
            .collect()
    }

    /// Reconcile one node's contribution for `pk` and keep the template's `pk_nodes` inverted
    /// index in lockstep. ALL `pk_value` mutations must go through here.
    fn reconcile_node_row(
        &mut self,
        sig: &SubquerySig,
        pk: &str,
        present: Option<Value>,
    ) -> Vec<Tup2<Row, ZWeight>> {
        let Some(node) = self.nodes.get_mut(sig) else { return Vec::new() };
        let had = node.pk_value.contains_key(pk);
        let tuples = node.reconcile_row_tuples(pk, present);
        let has = node.pk_value.contains_key(pk);
        if had != has {
            let tkey = node.template_key.clone();
            if let Some(tpl) = self.templates.get_mut(&tkey) {
                if has {
                    tpl.pk_nodes.entry(pk.to_string()).or_default().insert(sig.clone());
                } else if let Some(set) = tpl.pk_nodes.get_mut(pk) {
                    set.remove(sig);
                    if set.is_empty() {
                        tpl.pk_nodes.remove(pk);
                    }
                }
            }
        }
        tuples
    }

    /// Reconcile a batch of per-pk evaluations against ONE node and return the resulting
    /// flips (used by seeding replay and flip-driven parent re-derivations, where the caller
    /// already evaluated the node's full predicate per row).
    async fn apply_node_evals(
        &mut self,
        sig: &SubquerySig,
        evals: Vec<(String, Option<Value>)>,
    ) -> Vec<Flip> {
        let mut tuples = Vec::new();
        for (pk, pv) in evals {
            tuples.extend(self.reconcile_node_row(sig, &pk, pv));
        }
        // Every tuple belongs to `sig`, so the sig on each flip is redundant here.
        self.apply_tuples(tuples).await.into_iter().map(|(_, f)| f).collect()
    }

    /// The node's current distinct-value count, read from the circuit snapshot.
    pub(crate) fn circuit_distinct(&self, node_id: i64) -> usize {
        self.circuit.values_for_node(node_id, 0).0
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
        let (distinct, vals) = self.circuit.values_for_node(n.node_id, cap);
        let vals: Vec<(serde_json::Value, usize)> =
            vals.into_iter().map(|(v, c)| (v.to_json(), c)).collect();
        Some((distinct, n.refcount, vals, distinct > cap))
    }

    /// Memory-relevant registry totals: maintained nodes, total contributor pks across all nodes (the
    /// dominant per-node state — one entry per inner row producing a value), distinct values, shapes,
    /// and edges. Used by the memory probe to attribute subquery state growth.
    pub fn mem_totals(&self) -> (usize, usize, usize, usize, usize) {
        let mut contributors = 0;
        let mut distinct = 0;
        for n in self.nodes.values() {
            contributors += n.contributor_count();
            distinct += self.circuit_distinct(n.node_id);
        }
        (self.nodes.len(), contributors, distinct, self.shapes.len(), self.edges_count())
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
                distinct_values: self.circuit_distinct(n.node_id),
                refcount: n.refcount,
                template: n.template_key.clone(),
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
                    distinct_values: self.circuit_distinct(n.node_id),
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

    /// Outgoing edges for a node signature. O(that node's own edge list).
    fn edges_of(&self, sig: &SubquerySig) -> Vec<Edge> {
        self.edges.get(sig).cloned().unwrap_or_default()
    }

    /// Every edge in the registry (introspection only — hot paths use [`edges_of`]).
    pub(crate) fn all_edges(&self) -> impl Iterator<Item = &Edge> {
        self.edges.values().flatten()
    }

    /// Total edge count (memory probe + tests).
    pub fn edges_count(&self) -> usize {
        self.edges.values().map(Vec::len).sum()
    }

    /// Commit one edge (staged during creates; direct in tests).
    fn add_edge(&mut self, e: Edge) {
        self.edges.entry(e.node_sig.clone()).or_default().push(e);
    }

    /// Remove a dying node's edge entries: its outgoing list, plus the incoming edges that
    /// point at it from its children's lists (a dependent edge lives under the CHILD's key).
    fn remove_node_edges(&mut self, sig: &SubquerySig, child_sigs: &[SubquerySig]) {
        self.edges.remove(sig);
        for c in child_sigs {
            if let Some(v) = self.edges.get_mut(c) {
                v.retain(|e| !matches!(&e.dependent, Dependent::Node(s) if s == sig));
                if v.is_empty() {
                    self.edges.remove(c);
                }
            }
        }
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
        self.staged_edges.clear();
        self.collect_log.clear();
        let pred = match CompiledPredicate::compile_with(where_json, &outer_ts, self) {
            Ok(p) => Arc::new(p),
            Err(e) => {
                let log = std::mem::take(&mut self.collect_log);
                self.rollback_refs(log);
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
            self.rollback_refs(log);
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
        // Shape-level edges (staged with the compile's child edges; committed below).
        for leaf in collect_in_leaves(&pred) {
            self.staged_edges.push(Edge {
                node_sig: leaf.sig,
                dependent: Dependent::Shape(shape_id.to_string()),
                connecting_col: leaf.col,
                negated: leaf.negated,
                null_sensitive: leaf.null_sensitive,
            });
        }
        // Registration is definitely happening: commit the staged edges.
        for e in std::mem::take(&mut self.staged_edges) {
            self.add_edge(e);
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
            }
            let mut seed_tuples = Vec::new();
            for r in &rows {
                let pk = ts.key_string(r).unwrap_or_default();
                let pv = r.0.get(proj_col).cloned().unwrap_or(Value::Null);
                seed_tuples.extend(self.reconcile_node_row(&sig, &pk, Some(pv)));
            }
            // Initial state: the seed's flips are meaningless (every dependent's backfill
            // already reflects the seeded set), so this step's deltas are discarded — only
            // the replay below propagates.
            let _ = self.apply_tuples(seed_tuples).await;
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
                for f in self.apply_node_evals(&sig, evals).await {
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
        // Shape edges live under the pred's leaf sigs — remove only there, not a global scan.
        self.remove_shape_edges(&pending.pred, &pending.shape_id);
        for sig in pending.collect_log {
            if let Some(n) = self.nodes.get_mut(&sig) {
                n.refcount = n.refcount.saturating_sub(1);
                if n.refcount == 0 {
                    if let Some(node) = self.nodes.remove(&sig) {
                        let child_sigs: Vec<SubquerySig> =
                            collect_in_leaves(&node.pred).into_iter().map(|l| l.sig).collect();
                        self.remove_node_edges(&sig, &child_sigs);
                        self.remove_node_entry(&node);
                    }
                    self.pending_seed.retain(|s| s != &sig);
                }
            }
        }
    }

    /// Remove a shape's dependency edges: they live under the keys of the predicate's IN
    /// leaves, so removal touches only those nodes' lists.
    fn remove_shape_edges(&mut self, pred: &CompiledPredicate, shape_id: &str) {
        for leaf in collect_in_leaves(pred) {
            if let Some(v) = self.edges.get_mut(&leaf.sig) {
                v.retain(|e| !matches!(&e.dependent, Dependent::Shape(id) if id == shape_id));
                if v.is_empty() {
                    self.edges.remove(&leaf.sig);
                }
            }
        }
    }

    /// Drop a removed node's id/template bookkeeping (the circuit retraction, when the node
    /// has state, is the caller's job — pre-seed nodes have none).
    fn remove_node_entry(&mut self, node: &SubqueryNode) {
        self.node_by_id.remove(&node.node_id);
        if let Some(tpl) = self.templates.get_mut(&node.template_key) {
            tpl.binds.remove(&node.bind);
            for pk in node.pk_value.keys() {
                if let Some(set) = tpl.pk_nodes.get_mut(pk) {
                    set.remove(&node.sig);
                    if set.is_empty() {
                        tpl.pk_nodes.remove(pk);
                    }
                }
            }
            if tpl.binds.is_empty() {
                self.templates.remove(&node.template_key);
            }
        }
    }

    /// Rollback helper for a failed/conflicted `begin_create` compile: drop the staged edges
    /// and undo the node refs made by the aborted compile.
    fn rollback_refs(&mut self, log: Vec<SubquerySig>) {
        self.staged_edges.clear();
        for sig in log {
            if let Some(n) = self.nodes.get_mut(&sig) {
                n.refcount = n.refcount.saturating_sub(1);
                if n.refcount == 0 {
                    if let Some(node) = self.nodes.remove(&sig) {
                        self.remove_node_entry(&node);
                    }
                    self.pending_seed.retain(|s| s != &sig);
                }
            }
        }
    }

    /// Remove a subquery shape: drop its edges and decref the nodes it referenced (removing nodes whose
    /// refcount hits zero, and their edges, recursively).
    pub async fn drop_subquery_shape(&mut self, shape_id: &str) {
        let Some(shape) = self.shapes.remove(shape_id) else { return };
        // Sigs this shape pointed at, then drop the shape's edges.
        let sigs: Vec<SubquerySig> = collect_in_leaves(&shape.pred).into_iter().map(|l| l.sig).collect();
        self.remove_shape_edges(&shape.pred, shape_id);
        self.decref_nodes(sigs).await;
    }

    /// Decrement each sig's refcount, removing (and cascading into the children of) nodes
    /// that reach zero. Removed nodes retract their contributor tuples from the circuit in
    /// one batch at the end; the resulting Leave flips are discarded — a refcount-0 node has
    /// no dependents left to move.
    async fn decref_nodes(&mut self, sigs: Vec<SubquerySig>) {
        let mut stack = sigs;
        let mut tuples: Vec<Tup2<Row, ZWeight>> = Vec::new();
        while let Some(sig) = stack.pop() {
            let Some(node) = self.nodes.get_mut(&sig) else { continue };
            node.refcount = node.refcount.saturating_sub(1);
            if node.refcount > 0 {
                continue;
            }
            // Refcount hit zero: gather child sigs, retract state, remove node + edges, recurse.
            let child_sigs: Vec<SubquerySig> =
                collect_in_leaves(&node.pred).into_iter().map(|l| l.sig).collect();
            let node = self.nodes.remove(&sig).expect("node fetched above");
            for (pk, v) in &node.pk_value {
                tuples.push(Tup2(
                    Row(vec![Value::Int(node.node_id), v.clone(), Value::Text(pk.clone())]),
                    -1,
                ));
            }
            self.remove_node_entry(&node);
            self.remove_node_edges(&sig, &child_sigs);
            stack.extend(child_sigs);
        }
        if !tuples.is_empty() {
            let _ = self.circuit.apply(tuples).await;
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

        // 1. Templates whose inner table is this table: one residual eval + one bind lookup
        // per touched pk (instead of one full-predicate eval per literal-keyed node), then one
        // circuit step for the whole delta — the circuit's distinct reports the flips.
        let tkeys: Vec<String> = self
            .templates
            .iter()
            .filter(|(_, t)| t.inner_table == table)
            .map(|(k, _)| k.clone())
            .collect();
        let mut tuples: Vec<Tup2<Row, ZWeight>> = Vec::new();
        let mut live_sigs: Vec<SubquerySig> = Vec::new();
        for tkey in &tkeys {
            let sigs: Vec<SubquerySig> =
                self.templates.get(tkey).map(|t| t.binds.values().cloned().collect()).unwrap_or_default();
            for sig in sigs {
                // Mid-seed: buffer the raw delta for gated replay at install (a half-seeded
                // set must not be reconciled — the snapshot could stale-overwrite a fresher
                // delta).
                if let Some(buf) = self.nodes.get_mut(&sig).and_then(|n| n.seed_buffer.as_mut()) {
                    buf.extend(delta.iter().cloned());
                    hop(&mut trace, format!("node:{sig}"), "buffered");
                } else if self.nodes.get(&sig).is_some_and(|n| n.gate.should_skip(lsn, xid)) {
                    hop(&mut trace, format!("node:{sig}"), "dropped");
                } else {
                    live_sigs.push(sig);
                }
            }
            let evals = self.template_present(tkey, ts, delta);
            tuples.extend(self.template_reconcile(tkey, evals, lsn, xid));
        }
        let flips = self.apply_tuples(tuples).await;
        for sig in live_sigs {
            let flipped = flips.iter().any(|(s, _)| s == &sig);
            hop(&mut trace, format!("node:{sig}"), if flipped { "passed" } else { "dropped" });
        }
        for f in flips {
            work.push_back(f);
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

    /// For each touched pk, the row's target contribution under one template: `Some((node
    /// sig, projected value))` when the latest row matches the residual AND its projected
    /// params hit a registered bind, else `None`. One residual eval + one hash lookup per pk —
    /// the template-sharing eval win over per-node full-predicate evaluation.
    fn template_present(
        &self,
        tkey: &str,
        ts: &TableSchema,
        delta: &[Tup2<Row, ZWeight>],
    ) -> Vec<(String, Option<(SubquerySig, Value)>)> {
        let Some(tpl) = self.templates.get(tkey) else { return Vec::new() };
        crate::engine::membership::latest_rows_by_pk(ts, delta)
            .into_iter()
            .map(|(row, is_new)| {
                let pk = ts.key_string(&row).unwrap_or_default();
                let target = if is_new && tpl.residual.matches_ctx(&row, self) {
                    let params = Row(
                        tpl.param_cols
                            .iter()
                            .map(|&i| row.0.get(i).cloned().unwrap_or(Value::Null))
                            .collect(),
                    );
                    tpl.binds.get(&params).map(|sig| {
                        (sig.clone(), row.0.get(tpl.proj_col).cloned().unwrap_or(Value::Null))
                    })
                } else {
                    None
                };
                (pk, target)
            })
            .collect()
    }

    /// Turn one template's per-pk targets into circuit tuples: retract from nodes that held
    /// the pk but are no longer its target, insert into the new target. Per node, the delta
    /// is skipped when the node is mid-seed (its raw buffer replays at install) or when the
    /// node's seed gate says the snapshot already contains this change — in both cases the
    /// node's seed is (or will be) the authority, and reconcile-by-identity absorbs any
    /// overlap idempotently.
    fn template_reconcile(
        &mut self,
        tkey: &str,
        evals: Vec<(String, Option<(SubquerySig, Value)>)>,
        lsn: u64,
        xid: Option<u64>,
    ) -> Vec<Tup2<Row, ZWeight>> {
        let node_applies = |reg: &Self, sig: &SubquerySig| {
            reg.nodes
                .get(sig)
                .is_some_and(|n| n.seed_buffer.is_none() && !n.gate.should_skip(lsn, xid))
        };
        let mut tuples = Vec::new();
        for (pk, target) in evals {
            let holders: Vec<SubquerySig> = self
                .templates
                .get(tkey)
                .and_then(|t| t.pk_nodes.get(&pk))
                .map(|s| s.iter().cloned().collect())
                .unwrap_or_default();
            for sig in holders {
                if target.as_ref().is_some_and(|(tsig, _)| tsig == &sig) {
                    continue; // still the target; the insert below reconciles the value
                }
                if node_applies(self, &sig) {
                    tuples.extend(self.reconcile_node_row(&sig, &pk, None));
                }
            }
            if let Some((sig, v)) = target {
                if node_applies(self, &sig) {
                    tuples.extend(self.reconcile_node_row(&sig, &pk, Some(v)));
                }
            }
        }
        tuples
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
    lsn: Option<String>,
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
                        emit_flip_trace(
                            trace_tx,
                            &outer,
                            src,
                            &sig,
                            format!("shape:{id}"),
                            vec![id.clone()],
                            net,
                            lsn.clone(),
                            txid.clone(),
                        );
                    }
                }
                Dependent::Node(parent_sig) => {
                    let new_flips =
                        requery_and_reconcile_parent(registry, parent_sig, Some((edge.connecting_col, &flip.value))).await?;
                    if let Some((_inner, flips)) = new_flips {
                        // A nested `IN`: connect the flipped child `node:<sig>` to the parent
                        // `node:<parent_sig>` it re-derived, so the propagation reads through. The
                        // parent's own downstream shape lights when its flips reach a shape edge.
                        if let (false, Some(src)) = (flips.is_empty(), source_table.as_deref()) {
                            emit_flip_trace(
                                trace_tx,
                                src,
                                src,
                                &sig,
                                format!("node:{parent_sig}"),
                                Vec::new(),
                                flip_net(&flips),
                                lsn.clone(),
                                txid.clone(),
                            );
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

/// Re-query a parent node's inner rows — `Some((col, v))` = only rows with
/// `connecting_col = v` (a value flip), `None` = every row (a NULL re-derive) — then
/// re-evaluate the parent's full predicate and reconcile. Returns `Some((inner_table,
/// flips))`, or `None` if the parent vanished. The shared body of both flip-driven parent
/// paths: the fetch differs, the eval+reconcile never does.
async fn requery_and_reconcile_parent(
    registry: &tokio::sync::Mutex<SubqueryRegistry>,
    parent_sig: &SubquerySig,
    filter: Option<(usize, &Value)>,
) -> Result<Option<(String, Vec<Flip>)>> {
    let (ts, pg_url) = {
        let reg = registry.lock().await;
        let Some(n) = reg.nodes.get(parent_sig) else { return Ok(None) };
        (reg.snapshot_for_table(&n.inner_table)?, reg.pg_url.clone())
    };
    let rows = match filter {
        Some((col, value)) => query_candidates(&pg_url, &ts, col, value).await?,
        None => query_all(&pg_url, &ts).await?,
    };
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
    Ok(Some((ts.name.clone(), reg.apply_node_evals(parent_sig, evals).await)))
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
            // Full re-derive of the parent: same eval+reconcile as a value flip, fetching
            // every row instead of one connecting value's candidates.
            if let Some((_table, flips)) =
                requery_and_reconcile_parent(registry, parent_sig, None).await?
            {
                for f in flips {
                    work.push_back((parent_sig.clone(), f));
                }
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
    lsn: Option<String>,
    txid: Option<String>,
) {
    if trace_tx.receiver_count() == 0 {
        return;
    }
    let ev = crate::trace::TraceEvent {
        lsn,
        txid,
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
            self.staged_edges.push(Edge {
                node_sig: leaf.sig,
                dependent: Dependent::Node(sig.clone()),
                connecting_col: leaf.col,
                negated: leaf.negated,
                null_sensitive: leaf.null_sensitive,
            });
        }
        let proj_col = inner_ts.column_index(project)?;
        let node_id = self.next_node_id;
        self.next_node_id += 1;
        // Template registration: lift the equality literals into a bind (coerced to the
        // column types, same as leaf compilation) and share the residual across binds.
        let (tkey, bind_literals, residual_json) =
            crate::predicate::subquery_template(table, project, where_);
        let mut param_cols = Vec::with_capacity(bind_literals.len());
        let mut bind_vals = Vec::with_capacity(bind_literals.len());
        for (col, lit) in &bind_literals {
            let idx = inner_ts.column_index(col)?;
            param_cols.push(idx);
            bind_vals.push(Value::literal_from_json(lit, inner_ts.column_type(idx))?);
        }
        let bind = Row(bind_vals);
        if !self.templates.contains_key(&tkey) {
            // The residual is compiled with a sig-only collector: any nested IN inside it was
            // already collected (and refcounted) by the full-pred compile above; collecting
            // again would double-count.
            struct SigOnly;
            impl SubqueryCollector for SigOnly {
                fn collect(&mut self, t: &str, p: &str, w: Option<&PredicateJson>) -> Result<SubquerySig> {
                    Ok(subquery_sig(t, p, w))
                }
            }
            let residual = if residual_json.is_empty() {
                CompiledPredicate::MatchAll
            } else {
                CompiledPredicate::compile_with(
                    &PredicateJson::And { and: residual_json },
                    &inner_ts,
                    &mut SigOnly,
                )?
            };
            self.templates.insert(
                tkey.clone(),
                TemplateGroup {
                    inner_table: table.to_string(),
                    proj_col,
                    residual: Arc::new(residual),
                    param_cols: param_cols.clone(),
                    binds: HashMap::new(),
                    pk_nodes: HashMap::new(),
                },
            );
        }
        if let Some(tpl) = self.templates.get_mut(&tkey) {
            tpl.binds.insert(bind.clone(), sig.clone());
        }
        let mut node = SubqueryNode::new(
            sig.clone(), table.to_string(), proj_col, inner_ts.pk_index, Arc::new(inner_pred), node_id,
        );
        node.where_json = where_.cloned();
        node.template_key = tkey;
        node.bind = bind;
        node.refcount = 1;
        self.nodes.insert(sig.clone(), node);
        self.node_by_id.insert(node_id, sig.clone());
        self.collect_log.push(sig.clone());
        self.pending_seed.push(sig.clone());
        Ok(sig)
    }
}

impl SubqueryEval for SubqueryRegistry {
    fn contains(&self, sig: &SubquerySig, value: &Value) -> bool {
        self.nodes.get(sig).is_some_and(|n| self.circuit.contains(n.node_id, value))
    }
    fn has_null(&self, sig: &SubquerySig) -> bool {
        self.nodes.get(sig).is_some_and(|n| self.circuit.contains(n.node_id, &Value::Null))
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

    /// A registry with one MatchAll node registered the way `collect()` would: node map,
    /// node_by_id, and a unit-bind template — so `on_table_delta`, `apply_node_evals`, and the
    /// `SubqueryEval` reads all work.
    fn registry_with_node(sig: &str) -> SubqueryRegistry {
        let mut reg = SubqueryRegistry::new(DsClient::new("http://unused"), None);
        insert_test_node(&mut reg, sig);
        reg
    }

    fn insert_test_node(reg: &mut SubqueryRegistry, sig: &str) {
        let node_id = reg.next_node_id;
        reg.next_node_id += 1;
        let mut node = SubqueryNode::new(
            sig.into(), "t".into(), 0, 1, Arc::new(CompiledPredicate::MatchAll), node_id,
        );
        let tkey = format!("tpl:{sig}");
        node.template_key = tkey.clone();
        node.refcount = 1;
        reg.nodes.insert(sig.into(), node);
        reg.node_by_id.insert(node_id, sig.into());
        reg.templates.insert(
            tkey,
            TemplateGroup {
                inner_table: "t".into(),
                proj_col: 0,
                residual: Arc::new(CompiledPredicate::MatchAll),
                param_cols: Vec::new(),
                binds: [(Row(Vec::new()), sig.to_string())].into_iter().collect(),
                pk_nodes: HashMap::new(),
            },
        );
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
        insert_test_node(&mut reg, "sig1");

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
        emit_flip_trace(
            &trace_tx,
            "issues",
            "project_members",
            &sig,
            "shape:s1".into(),
            vec!["s1".into()],
            -103,
            Some("0/1A2B3C".into()),
            Some("777".into()),
        );

        let ev: serde_json::Value = serde_json::from_str(&trace_rx.try_recv().unwrap()).unwrap();
        // The event is about the dependent shape's table; the path still heads at the source.
        assert_eq!(ev["table"], "issues");
        // Carries the originating write's lsn/txid, so the activity log can group this deferred
        // flip event together with the direct-change event that triggered it (same commit).
        assert_eq!(ev["lsn"], "0/1A2B3C");
        assert_eq!(ev["txid"], "777");
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
        let mut reg = registry_with_node("sig1");
        reg.add_edge(Edge {
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
        propagate_flips(&reg, work, None, None, &trace_tx).await.unwrap();
        assert!(trace_rx.try_recv().is_err(), "a NULL flip on a non-null-sensitive dependent emits nothing");
    }



    #[tokio::test(flavor = "multi_thread")]
    async fn reconcile_enter_and_leave_on_first_and_last_contributor() {
        let sig: SubquerySig = "sig".into();
        let mut reg = registry_with_node(&sig);
        let evals = |pk: &str, v: Option<Value>| vec![(pk.to_string(), v)];
        assert_eq!(
            reg.apply_node_evals(&sig, evals("a", Some(Value::Int(5)))).await,
            vec![Flip { value: Value::Int(5), dir: FlipDir::Enter }]
        );
        assert!(reg.contains(&sig, &Value::Int(5)));
        // second contributor to the same value -> no flip
        assert_eq!(reg.apply_node_evals(&sig, evals("b", Some(Value::Int(5)))).await, vec![]);
        // removing one of two -> still present, no flip
        assert_eq!(reg.apply_node_evals(&sig, evals("a", None)).await, vec![]);
        assert!(reg.contains(&sig, &Value::Int(5)));
        // removing the last -> Leave
        assert_eq!(
            reg.apply_node_evals(&sig, evals("b", None)).await,
            vec![Flip { value: Value::Int(5), dir: FlipDir::Leave }]
        );
        assert!(!reg.contains(&sig, &Value::Int(5)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reconcile_value_change_emits_leave_then_enter() {
        let sig: SubquerySig = "sig".into();
        let mut reg = registry_with_node(&sig);
        reg.apply_node_evals(&sig, vec![("a".into(), Some(Value::Int(5)))]).await;
        let mut flips = reg.apply_node_evals(&sig, vec![("a".into(), Some(Value::Int(7)))]).await;
        flips.sort_by(|a, b| a.value.cmp(&b.value));
        assert_eq!(
            flips,
            vec![
                Flip { value: Value::Int(5), dir: FlipDir::Leave },
                Flip { value: Value::Int(7), dir: FlipDir::Enter },
            ]
        );
        assert!(!reg.contains(&sig, &Value::Int(5)));
        assert!(reg.contains(&sig, &Value::Int(7)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reconcile_same_value_is_a_noop() {
        let sig: SubquerySig = "sig".into();
        let mut reg = registry_with_node(&sig);
        reg.apply_node_evals(&sig, vec![("a".into(), Some(Value::Int(5)))]).await;
        assert_eq!(reg.apply_node_evals(&sig, vec![("a".into(), Some(Value::Int(5)))]).await, vec![]);
        // unchanged absence is also a no-op
        assert_eq!(reg.apply_node_evals(&sig, vec![("z".into(), None)]).await, vec![]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn null_bucket_tracks_has_null() {
        let sig: SubquerySig = "sig".into();
        let mut reg = registry_with_node(&sig);
        assert_eq!(
            reg.apply_node_evals(&sig, vec![("a".into(), Some(Value::Null))]).await,
            vec![Flip { value: Value::Null, dir: FlipDir::Enter }]
        );
        assert!(reg.has_null(&sig));
        assert_eq!(
            reg.apply_node_evals(&sig, vec![("a".into(), None)]).await,
            vec![Flip { value: Value::Null, dir: FlipDir::Leave }]
        );
        assert!(!reg.has_null(&sig));
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
        assert_eq!(reg.edges_count(), 0, "aborted create left orphaned edges");
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

    #[tokio::test(flavor = "multi_thread")]
    async fn registry_eval_reads_node_sets() {
        let sig: SubquerySig = "sig".into();
        let mut reg = registry_with_node(&sig);
        reg.apply_node_evals(&sig, vec![("a".into(), Some(Value::Int(1))), ("b".into(), Some(Value::Null))])
            .await;
        assert!(reg.contains(&sig, &Value::Int(1)));
        assert!(!reg.contains(&sig, &Value::Int(2)));
        assert!(reg.has_null(&sig));
        // unknown sig -> empty
        assert!(!reg.contains(&"other".to_string(), &Value::Int(1)));
    }
}
