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
use crate::heap_size::HeapSize;
use crate::pk_dict::PkDict;
use crate::subq_circuit::{Assert, Assertions, FeedDelta, PkKey};
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
    /// Number of dependents (shapes + parent nodes) referencing this node; drop the node at 0.
    pub refcount: usize,
}

impl HeapSize for SubqueryNode {
    /// `pred` (`Arc<CompiledPredicate>`) is shared with the registry's compiled evaluators, not
    /// uniquely owned by this node — skipped, like every other `Arc<...>` field in this module.
    fn heap_bytes(&self) -> usize {
        self.sig.heap_bytes()
            + self.inner_table.heap_bytes()
            + self.where_json.heap_bytes()
            + self.gate.heap_bytes()
            + self.seed_buffer.heap_bytes()
            + self.template_key.heap_bytes()
            + self.bind.heap_bytes()
    }
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
            refcount: 0,
        }
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
    /// pk id -> nodes of this template currently holding a contribution from that pk. An exact
    /// inverted index over the nodes' contributor sets (maintained in lockstep by
    /// `reconcile_node_row`), so a row that stops matching finds its old bind in O(1) instead
    /// of scanning every bind. Keyed by the pk's dictionary id (see [`crate::pk_dict::PkDict`]),
    /// not the pk string — the string lives once in the shared dictionary.
    pk_nodes: HashMap<u32, HashSet<SubquerySig>>,
}

impl HeapSize for TemplateGroup {
    /// `residual` (`Arc<CompiledPredicate>`) is shared across every bind of this template —
    /// skipped, like other `Arc<...>` fields in this module.
    fn heap_bytes(&self) -> usize {
        self.inner_table.heap_bytes() + self.param_cols.heap_bytes() + self.binds.heap_bytes() + self.pk_nodes.heap_bytes()
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

impl HeapSize for Dependent {
    fn heap_bytes(&self) -> usize {
        match self {
            Dependent::Shape(id) => id.heap_bytes(),
            Dependent::Node(sig) => sig.heap_bytes(),
        }
    }
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

impl HeapSize for Edge {
    /// `connecting_col`/`negated`/`null_sensitive` are inline.
    fn heap_bytes(&self) -> usize {
        self.node_sig.heap_bytes() + self.dependent.heap_bytes()
    }
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
    /// This shape's key in the circuit's per-feed relation (`(feed_id, pk)` upsert map). The
    /// relation replaces the old `known_members` set: a delete is delivered iff the relation
    /// actually retracts, so a "not a member" verdict for a pk the stream never contained is
    /// structurally a no-op — the wake-storm gate (PR #30) with no filter to keep in sync.
    pub(crate) feed_id: i64,
}

impl HeapSize for SubqueryShape {
    /// `pred`/`out_cols` are `Arc`-shared, not uniquely owned; `emitted` (`AtomicU64`) and
    /// `feed_id` (`i64`) are inline.
    fn heap_bytes(&self) -> usize {
        self.shape_id.heap_bytes() + self.outer_table.heap_bytes() + self.stream_path.heap_bytes() + self.gate.heap_bytes()
    }
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

impl HeapSize for PendingSubqueryShape {
    /// `pred`/`out_cols` are `Arc`-shared, not uniquely owned; `changes_only` is inline.
    fn heap_bytes(&self) -> usize {
        self.shape_id.heap_bytes()
            + self.outer_table.heap_bytes()
            + self.stream_path.heap_bytes()
            + self.collect_log.heap_bytes()
            + self.buffer.heap_bytes()
    }
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
    /// The global pk dictionary shared by the circuit tier: every contributor / feed key is
    /// `(id, pk_id)` where `pk_id = pk_dict.get_or_insert(pk)`. Ids are minted here (when building
    /// assertions) and resolved back to pk strings here (at the emission seam), so the circuit and
    /// its indexes never store a heap pk string. One instance per engine (per registry).
    pk_dict: Arc<PkDict>,
    /// Next circuit node id (monotonic; ids of dropped nodes are never reused, so a stale
    /// snapshot read can never alias a new node's slice).
    next_node_id: i64,
    /// circuit node id -> node signature (maps circuit flip deltas back to nodes).
    node_by_id: HashMap<i64, SubquerySig>,
    /// Next feed id (per-shape circuit key; monotonic, never reused).
    next_feed_id: i64,
    /// circuit feed id -> shape id (maps feed deltas back to shapes).
    feed_by_id: HashMap<i64, String>,
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

impl HeapSize for SubqueryRegistry {
    /// `circuit` (the dbsp membership relation) is accounted separately — see
    /// [`SubqueryRegistry::circuit_bytes`]'s `bytes_membership_circuit` measurement — so it is
    /// deliberately excluded here to avoid double-counting the same state under two `/memory`
    /// fields. The `pk_dict` is likewise accounted separately (`bytes_pk_dict`, see
    /// [`SubqueryRegistry::pk_dict_bytes`]) — `Arc`-shared and reported once. `ds` (a client
    /// handle) and `schemas` (`Arc`-shared with the engine's compiled
    /// schema) are not uniquely owned; `lanes` holds channel senders, not owned data;
    /// `next_node_id`/`next_feed_id` are inline counters.
    fn heap_bytes(&self) -> usize {
        self.nodes.heap_bytes()
            + self.edges.heap_bytes()
            + self.staged_edges.heap_bytes()
            + self.shapes.heap_bytes()
            + self.node_by_id.heap_bytes()
            + self.feed_by_id.heap_bytes()
            + self.templates.heap_bytes()
            + self.pending_seed.heap_bytes()
            + self.pending_shapes.heap_bytes()
            + self.collect_log.heap_bytes()
            + self.pg_url.heap_bytes()
    }
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
            pk_dict: Arc::new(PkDict::new()),
            next_node_id: 1,
            node_by_id: HashMap::new(),
            next_feed_id: 1,
            feed_by_id: HashMap::new(),
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

    /// Apply an assertion batch to the membership circuit and map its member deltas back to
    /// node signatures (feed deltas pass through by feed id). Callers hold the registry lock
    /// across the await — the circuit thread never takes this lock, and awaiting the step is
    /// what gives every later membership read read-your-writes over this batch.
    async fn apply_asserts(
        &mut self,
        asserts: Assertions,
    ) -> (Vec<(SubquerySig, Flip)>, Vec<FeedDelta>) {
        if asserts.is_empty() {
            return (Vec::new(), Vec::new());
        }
        let (member_deltas, feed_deltas) = self.circuit.apply(asserts).await;
        let flips = member_deltas
            .into_iter()
            .filter_map(|d| {
                let sig = self.node_by_id.get(&d.node_id)?.clone();
                let dir = if d.delta > 0 { FlipDir::Enter } else { FlipDir::Leave };
                Some((sig, Flip { value: d.value, dir }))
            })
            .collect();
        (flips, feed_deltas)
    }

    /// Build one node's contributor assertion for `pk` and keep the template's `pk_nodes`
    /// inverted index in lockstep. `pk_nodes` IS the record of presence per template (the
    /// circuit's upsert map is the record of the value), so all presence transitions go
    /// through here. Inserts always assert (idempotent; a changed value must flow); absent
    /// stays quiet unless the node actually held the pk.
    fn assert_node_row(
        &mut self,
        sig: &SubquerySig,
        pk: &str,
        present: Option<Value>,
    ) -> Option<Tup2<PkKey, Assert>> {
        let (node_id, tkey) = {
            let node = self.nodes.get(sig)?;
            (node.node_id, node.template_key.clone())
        };
        let pk_id = self.pk_dict.get_or_insert(pk);
        let key = PkKey { id: node_id, pk: pk_id };
        match present {
            Some(v) => {
                if let Some(tpl) = self.templates.get_mut(&tkey) {
                    tpl.pk_nodes.entry(pk_id).or_default().insert(sig.clone());
                }
                Some(Tup2(key, Assert::Insert(v)))
            }
            None => {
                let had = self
                    .templates
                    .get_mut(&tkey)
                    .and_then(|tpl| {
                        let set = tpl.pk_nodes.get_mut(&pk_id)?;
                        let had = set.remove(sig);
                        if set.is_empty() {
                            tpl.pk_nodes.remove(&pk_id);
                        }
                        Some(had)
                    })
                    .unwrap_or(false);
                had.then_some(Tup2(key, Assert::Delete))
            }
        }
    }

    /// Assert a batch of per-pk evaluations against ONE node and return the resulting flips
    /// (seeding replay and flip-driven parent re-derivations, where the caller already
    /// evaluated the node's full predicate per row).
    async fn apply_node_evals(
        &mut self,
        sig: &SubquerySig,
        evals: Vec<(String, Option<Value>)>,
    ) -> Vec<Flip> {
        let mut asserts = Assertions::default();
        for (pk, pv) in evals {
            asserts.contributors.extend(self.assert_node_row(sig, &pk, pv));
        }
        // Every assertion belongs to `sig`, so the sig on each flip is redundant here.
        let (flips, _) = self.apply_asserts(asserts).await;
        flips.into_iter().map(|(_, f)| f).collect()
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
    /// and edges. Used by the memory probe to attribute subquery state growth — cheap enough for the
    /// 500ms background sampler (`mem::spawn_sampler`): everything here is already published/derivable
    /// per-node index state, the same walk this method did before the byte-level accounting phase.
    ///
    /// Does NOT include the membership-circuit byte measurement — see [`Self::circuit_bytes`], the
    /// on-demand-only (`GET /memory`) counterpart that adds the FEEDS-map walk on top of this.
    pub fn mem_totals(&self) -> (usize, usize, usize, usize, usize) {
        let mut contributors = 0;
        let mut distinct = 0;
        for n in self.nodes.values() {
            let (d, vals) = self.circuit.values_for_node(n.node_id, usize::MAX);
            contributors += vals.iter().map(|(_, c)| c).sum::<usize>();
            distinct += d;
        }
        (self.nodes.len(), contributors, distinct, self.shapes.len(), self.edges_count())
    }

    /// Measured owned/on-disk bytes of the membership circuit's published snapshots
    /// (`bytes_membership_circuit` and its `bytes_circuit_integral` / `bytes_circuit_snapshots`
    /// split) — the on-demand-only (`GET /memory`) counterpart to [`Self::mem_totals`]. Never
    /// called from the 500ms background sampler.
    ///
    /// Replaces the former `key_count × 88 B` estimate with dbsp's exact per-batch
    /// `approximate_byte_size` (columnar bytes when resident; on-disk file size when spilled —
    /// see `subq_circuit`'s `SpillConfig`). Cheap: reads the three snapshot slots the circuit
    /// already publishes, no circuit round-trip. See [`crate::subq_circuit::CircuitBytes`] for
    /// what each term covers and the (profiler-only) non-published state it does not.
    pub fn circuit_bytes(&self) -> crate::subq_circuit::CircuitBytes {
        self.circuit.snapshot_bytes()
    }

    /// Estimated owned heap of the global pk dictionary (`bytes_pk_dict` in `GET /memory`) — the
    /// once-per-distinct-pk string storage plus its forward/reverse index. Accounted separately
    /// from the registry's own `heap_bytes` (the dictionary is `Arc`-shared and append-only; this
    /// makes the string-interning trade visible). On-demand only — never on the sampler path.
    pub fn pk_dict_bytes(&self) -> usize {
        self.pk_dict.heap_bytes()
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
            let mut seed = Assertions::default();
            for r in &rows {
                let pk = ts.key_string(r).unwrap_or_default();
                let pv = r.0.get(proj_col).cloned().unwrap_or(Value::Null);
                seed.contributors.extend(self.assert_node_row(&sig, &pk, Some(pv)));
            }
            // Initial state: the seed's flips are meaningless (every dependent's backfill
            // already reflects the seeded set), so this step's deltas are discarded — only
            // the replay below propagates.
            let _ = self.apply_asserts(seed).await;
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
        let feed_id = self.next_feed_id;
        self.next_feed_id += 1;
        self.feed_by_id.insert(feed_id, shape_id.to_string());
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
                feed_id,
            },
        );
        // Seed the feed relation with the backfilled pks (deltas discarded: the stream
        // already carries the snapshot) — replaces the old known_members hand-off.
        let mut feed_seed = Assertions::default();
        for pk in seeded_pks {
            let pk_id = self.pk_dict.get_or_insert(&pk);
            feed_seed.feeds.push(Tup2(PkKey { id: feed_id, pk: pk_id }, true));
        }
        let _ = self.apply_asserts(feed_seed).await;
        if !pending.buffer.is_empty() {
            let candidates = crate::engine::membership::latest_rows_by_pk(&ts, &pending.buffer);
            self.emit_for_shapes(&ts, vec![(shape_id.to_string(), candidates)], None).await?;
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
        // Retract the feed's key slice from the circuit (deltas discarded — the stream is
        // being torn down) and drop the id mapping.
        self.feed_by_id.remove(&shape.feed_id);
        let feeds: Vec<Tup2<PkKey, bool>> = self
            .circuit
            .feed_pk_ids(shape.feed_id)
            .into_iter()
            .map(|pk_id| Tup2(PkKey { id: shape.feed_id, pk: pk_id }, false))
            .collect();
        if !feeds.is_empty() {
            let _ = self.circuit.apply(Assertions { contributors: Vec::new(), feeds }).await;
        }
        self.decref_nodes(sigs).await;
    }

    /// Decrement each sig's refcount, removing (and cascading into the children of) nodes
    /// that reach zero. Removed nodes retract their contributor tuples from the circuit in
    /// one batch at the end; the resulting Leave flips are discarded — a refcount-0 node has
    /// no dependents left to move.
    async fn decref_nodes(&mut self, sigs: Vec<SubquerySig>) {
        let mut stack = sigs;
        let mut asserts = Assertions::default();
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
            // The node's contributor slice comes from the circuit's own integral (prefix
            // scan) — there is no host pk list to drain anymore. Keys are pk ids; the retraction
            // re-asserts the same id, so no dictionary round-trip is needed here.
            for (pk_id, _v) in self.circuit.contributor_entries(node.node_id) {
                if let Some(tpl) = self.templates.get_mut(&node.template_key) {
                    if let Some(set) = tpl.pk_nodes.get_mut(&pk_id) {
                        set.remove(&sig);
                        if set.is_empty() {
                            tpl.pk_nodes.remove(&pk_id);
                        }
                    }
                }
                asserts
                    .contributors
                    .push(Tup2(PkKey { id: node.node_id, pk: pk_id }, Assert::Delete));
            }
            self.remove_node_entry(&node);
            self.remove_node_edges(&sig, &child_sigs);
            stack.extend(child_sigs);
        }
        // Refcount-0 removal ⇒ no dependents remain; the flips are discarded.
        if !asserts.is_empty() {
            let _ = self.circuit.apply(asserts).await;
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
        let mut asserts = Assertions::default();
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
            asserts.contributors.extend(self.template_assertions(tkey, evals, lsn, xid));
        }
        let (flips, _) = self.apply_asserts(asserts).await;
        for sig in live_sigs {
            let flipped = flips.iter().any(|(s, _)| s == &sig);
            hop(&mut trace, format!("node:{sig}"), if flipped { "passed" } else { "dropped" });
        }
        for f in flips {
            work.push_back(f);
        }

        // 2. Subquery shapes whose outer table is this table: one batch of candidates across
        // every shape — assertions feed the circuit in ONE step, its feed deltas are the
        // deletes, matching candidates are the upserts.
        let shape_ids: Vec<String> = self
            .shapes
            .iter()
            .filter(|(_, s)| s.outer_table == table)
            .map(|(id, _)| id.clone())
            .collect();
        let mut groups: Vec<(String, Vec<(Row, bool)>)> = Vec::new();
        for id in shape_ids {
            if self.shapes.get(&id).is_some_and(|s| s.gate.should_skip(lsn, xid)) {
                continue;
            }
            groups.push((id, crate::engine::membership::latest_rows_by_pk(ts, delta)));
        }
        for (id, emitted, _net) in self.emit_for_shapes(ts, groups, txid.clone()).await? {
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

    /// Turn one template's per-pk targets into contributor assertions: absent for nodes that
    /// held the pk but are no longer its target, present for the new target. Per node, the
    /// delta is skipped when the node is mid-seed (its raw buffer replays at install) or when
    /// the node's seed gate says the snapshot already contains this change — in both cases
    /// the node's seed is (or will be) the authority, and absolute assertion absorbs any
    /// overlap idempotently.
    fn template_assertions(
        &mut self,
        tkey: &str,
        evals: Vec<(String, Option<(SubquerySig, Value)>)>,
        lsn: u64,
        xid: Option<u64>,
    ) -> Vec<Tup2<PkKey, Assert>> {
        let node_applies = |reg: &Self, sig: &SubquerySig| {
            reg.nodes
                .get(sig)
                .is_some_and(|n| n.seed_buffer.is_none() && !n.gate.should_skip(lsn, xid))
        };
        let mut asserts = Vec::new();
        for (pk, target) in evals {
            // A pk with no interned id was never asserted, so it can hold no contribution — probe
            // without minting (a never-member delete must not grow the dictionary).
            let holders: Vec<SubquerySig> = self
                .pk_dict
                .get(&pk)
                .and_then(|pk_id| self.templates.get(tkey).and_then(|t| t.pk_nodes.get(&pk_id)))
                .map(|s| s.iter().cloned().collect())
                .unwrap_or_default();
            for sig in holders {
                if target.as_ref().is_some_and(|(tsig, _)| tsig == &sig) {
                    continue; // still the target; the insert below carries the fresh value
                }
                if node_applies(self, &sig) {
                    asserts.extend(self.assert_node_row(&sig, &pk, None));
                }
            }
            if let Some((sig, v)) = target {
                if node_applies(self, &sig) {
                    asserts.extend(self.assert_node_row(&sig, &pk, Some(v)));
                }
            }
        }
        asserts
    }

    /// Deferred-propagation helper: snapshot what a query-back needs (brief lock scope at the
    /// call site — see the free functions below).
    fn snapshot_for_table(&self, table: &str) -> Result<TableSchema> {
        self.schemas.get(table).cloned().with_context(|| format!("unknown table '{table}'"))
    }

    /// The ONE emission tail (spec §5): evaluate each shape's candidates against the current
    /// membership snapshot, assert their feed presence absolutely, step the circuit once for
    /// the whole batch, and deliver — **upserts for every matching candidate** (an update to a
    /// continuing member must flow; upserts are always safe for readers), **deletes only from
    /// the feed relation's retractions** (a "not a member" verdict for a pk the stream never
    /// contained nets to nothing in the map, so the spurious delete that used to wake idle
    /// long-polls is structurally impossible). Emission is absolute per pk, exactly as before:
    /// deferred flip propagation converges regardless of timing.
    ///
    /// Callers hold the registry lock for the whole call (eval + circuit await + lane
    /// enqueue), which is what keeps per-stream append order = evaluation order.
    /// `candidates` are each shape's touched rows: `(latest row, still-exists)`.
    /// Returns per shape whether anything was delivered (trace hops).
    async fn emit_for_shapes(
        &mut self,
        ts: &TableSchema,
        groups: Vec<(String, Vec<(Row, bool)>)>,
        txid: Option<String>,
    ) -> Result<Vec<(String, bool, i64)>> {
        // Phase 1: evaluate + build assertions and the member upserts, per shape.
        let mut asserts = Assertions::default();
        // shape id -> (member rows to upsert, candidate pk -> row for delete construction)
        let mut staged: Vec<(String, Vec<Row>)> = Vec::new();
        for (shape_id, candidates) in groups {
            let Some(shape) = self.shapes.get(&shape_id) else { continue };
            let (pred, feed_id) = (shape.pred.clone(), shape.feed_id);
            let mut members: Vec<Row> = Vec::new();
            for (row, exists) in candidates {
                let pk = match ts.key_string(&row) {
                    Ok(pk) => pk,
                    Err(_) => continue,
                };
                let member = exists && pred.matches_ctx(&row, self);
                let pk_id = self.pk_dict.get_or_insert(&pk);
                asserts.feeds.push(Tup2(PkKey { id: feed_id, pk: pk_id }, member));
                if member {
                    members.push(row);
                }
            }
            staged.push((shape_id, members));
        }
        // Phase 2: one circuit step for the whole batch; retractions are the deletes. Resolve
        // each retracted pk id back to its pk string HERE — still under the registry lock, before
        // the delta leaves the circuit tier — so the wire protocol carries the pk string
        // unchanged and per-stream emission order is preserved (the order-preserving seam).
        let (_, feed_deltas) = self.apply_asserts(asserts).await;
        let mut deletes: HashMap<String, Vec<String>> = HashMap::new();
        for d in feed_deltas {
            if d.delta < 0 {
                if let Some(shape_id) = self.feed_by_id.get(&d.feed_id) {
                    let pk = self.pk_dict.resolve(d.pk_id).to_string();
                    deletes.entry(shape_id.clone()).or_default().push(pk);
                }
            }
        }
        // Phase 3: build + deliver per shape (still under the caller's lock — enqueue order
        // on each stream's FIFO lane is evaluation order).
        let mut results = Vec::with_capacity(staged.len());
        for (shape_id, members) in staged {
            let Some(shape) = self.shapes.get(&shape_id) else { continue };
            let dels = deletes.remove(&shape_id).unwrap_or_default();
            let net = members.len() as i64 - dels.len() as i64;
            let mut envs = crate::engine::translate_output(
                ts,
                members.into_iter().map(|r| (r, 1)).collect(),
                txid.clone(),
                None,
                shape.out_cols.as_deref().map(Vec::as_slice),
            );
            envs.extend(crate::engine::delete_envelopes(ts, dels, txid.clone()));
            if envs.is_empty() {
                results.push((shape_id, false, 0));
                continue;
            }
            shape.emitted.fetch_add(envs.len() as u64, std::sync::atomic::Ordering::Relaxed);
            let path = shape.stream_path.clone();
            self.deliver(&path, envs).await;
            results.push((shape_id, true, net));
        }
        Ok(results)
    }
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
    // Evaluate + assert + deliver atomically under the lock, through the ONE emission tail
    // (candidates from a query-back all still exist).
    let mut reg = registry.lock().await;
    let candidates: Vec<(Row, bool)> = rows.into_iter().map(|r| (r, true)).collect();
    let results =
        reg.emit_for_shapes(&ts, vec![(shape_id.to_string(), candidates)], txid).await?;
    Ok(match results.first() {
        Some((_, true, net)) => Some((ts.name.clone(), *net)),
        _ => None,
    })
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
            // Full re-derive: every row is a candidate; the ONE emission tail decides.
            let mut reg = registry.lock().await;
            let candidates: Vec<(Row, bool)> = rows.into_iter().map(|r| (r, true)).collect();
            reg.emit_for_shapes(&ts, vec![(id.clone(), candidates)], txid).await?;
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

    /// The per-feed relation is the fix for the live-poll wake-storm bug, now structural:
    /// `emit_for_shapes` computes an *absolute* membership verdict for every touched pk, but a
    /// delete envelope is built ONLY from the feed relation's retraction — a "not a member"
    /// verdict for a pk the stream never contained nets to nothing in the circuit's upsert
    /// map, so the spurious delete that used to wake every idle long-poll cannot be emitted
    /// at all (there is no filter left to get out of sync).
    #[tokio::test(flavor = "multi_thread")]
    async fn feed_relation_drops_deletes_for_never_known_pks() {
        use crate::schema::TableDef;
        let def: TableDef = serde_json::from_value(serde_json::json!({
            "columns": { "id": {"type":"int"}, "gid": {"type":"int"} }, "primaryKey": "id"
        }))
        .unwrap();
        let ts = crate::schema::TableSchema::from_def("t", &def).unwrap();
        let mut reg = SubqueryRegistry::new(DsClient::new("http://unused"), None);
        // A shape whose predicate never matches (Not(MatchAll)): every candidate verdict is
        // "not a member".
        let feed_id = reg.next_feed_id;
        reg.next_feed_id += 1;
        reg.feed_by_id.insert(feed_id, "s1".into());
        reg.shapes.insert(
            "s1".into(),
            SubqueryShape {
                shape_id: "s1".into(),
                outer_table: "t".into(),
                stream_path: "shape/s1".into(),
                pred: Arc::new(CompiledPredicate::Not(Box::new(CompiledPredicate::MatchAll))),
                out_cols: None,
                gate: crate::pg::SnapshotGate::passthrough(),
                emitted: std::sync::atomic::AtomicU64::new(0),
                feed_id,
            },
        );
        let row = |id: i64| Row(vec![Value::Int(id), Value::Int(0)]);

        // A "leave" for a pk this feed never contained: nothing is emitted (no lanes are
        // configured, so an emission would attempt a real append and fail loudly; emitted
        // stays 0 and the result reports nothing delivered).
        let results = reg
            .emit_for_shapes(&ts, vec![("s1".to_string(), vec![(row(1), true)])], None)
            .await
            .unwrap();
        assert_eq!(results, vec![("s1".to_string(), false, 0)], "never-member delete must be dropped");
        assert_eq!(reg.shapes["s1"].emitted.load(std::sync::atomic::Ordering::Relaxed), 0);

        // Seed pk 1 as a member (backfill hand-off), then the same verdict is a GENUINE leave.
        // The pk id must match the one `emit_for_shapes` minted for row(1)'s pk string above, so
        // build the key through the SAME dictionary.
        let pk_id = reg.pk_dict.get_or_insert("1");
        let mut seed = Assertions::default();
        seed.feeds.push(Tup2(PkKey { id: feed_id, pk: pk_id }, true));
        reg.apply_asserts(seed).await;
        let (_, fd) = reg
            .circuit
            .apply(Assertions {
                contributors: Vec::new(),
                feeds: vec![Tup2(PkKey { id: feed_id, pk: pk_id }, false)],
            })
            .await;
        assert_eq!(fd.len(), 1, "a known member's delete must produce the retraction");
        assert_eq!(fd[0].delta, -1);

        // And once retracted, a repeat delete nets nothing again.
        let (_, fd) = reg
            .circuit
            .apply(Assertions {
                contributors: Vec::new(),
                feeds: vec![Tup2(PkKey { id: feed_id, pk: pk_id }, false)],
            })
            .await;
        assert!(fd.is_empty(), "repeat delete for an already-removed pk must net nothing");
    }

    // --- Task 0.3 harness: a minimal fake durable-streams server ------------------------------
    //
    // `emit_for_shapes`'s `deliver()` falls back to `DsClient::append_reliable` when no lanes are
    // configured (unit tests): a genuinely non-empty emission would otherwise retry forever
    // against `http://unused`. `tests/live_poll_deadline.rs` and `tests/params_shape.rs` solve
    // this the same way — spawn a real (local) axum server and point a real `DsClient` at it —
    // reused verbatim here so appends actually land and can be inspected, instead of asserting
    // only the `emitted` counter.

    #[derive(Clone, Default)]
    struct DsStore(Arc<std::sync::Mutex<HashMap<String, Vec<Envelope>>>>);

    async fn fake_ds_handler(
        axum::extract::State(store): axum::extract::State<DsStore>,
        req: axum::extract::Request,
    ) -> axum::response::Response {
        use axum::http::{Method, StatusCode};
        use axum::response::IntoResponse;
        match *req.method() {
            Method::PUT | Method::DELETE => StatusCode::OK.into_response(),
            Method::POST => {
                let path = req.uri().path().trim_start_matches('/').to_string();
                let body = axum::body::to_bytes(req.into_body(), 1024 * 1024).await.unwrap_or_default();
                if let Ok(envs) = serde_json::from_slice::<Vec<Envelope>>(&body) {
                    store.0.lock().unwrap().entry(path).or_default().extend(envs);
                }
                StatusCode::OK.into_response()
            }
            Method::GET => {
                ([("stream-next-offset", "tip"), ("stream-up-to-date", "1")], "[]").into_response()
            }
            _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
        }
    }

    /// Boots the fake server on an ephemeral port; returns its base URL and the shared store of
    /// every envelope POSTed to it, keyed by stream path (e.g. `"shape/s1"`).
    async fn spawn_fake_ds() -> (String, DsStore) {
        let store = DsStore::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = axum::Router::new().fallback(fake_ds_handler).with_state(store.clone());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}"), store)
    }

    /// The operations (`"upsert"`/`"delete"`) delivered to one stream path so far, in append order.
    fn ops_for(store: &DsStore, path: &str) -> Vec<String> {
        store.0.lock().unwrap().get(path).map(|v| v.iter().map(|e| e.headers.operation.clone()).collect()).unwrap_or_default()
    }

    /// `issues(id, project_id)`, matching the brief's example shape:
    /// `issues WHERE project_id IN (SELECT project_id FROM project_members WHERE user_id = ...)`.
    fn issues_ts() -> TableSchema {
        use crate::schema::TableDef;
        let def: TableDef = serde_json::from_value(serde_json::json!({
            "columns": { "id": {"type":"int"}, "project_id": {"type":"int"} },
            "primaryKey": "id"
        }))
        .unwrap();
        TableSchema::from_def("issues", &def).unwrap()
    }

    fn issue(id: i64, project_id: i64) -> Row {
        Row(vec![Value::Int(id), Value::Int(project_id)])
    }

    /// Registers a shape whose predicate is `project_id IN (node sig)` — the outer half of the
    /// brief's example, wired to a membership node inserted with [`insert_test_node`].
    fn insert_membership_shape(reg: &mut SubqueryRegistry, shape_id: &str, sig: &SubquerySig, project_col: usize) {
        let feed_id = reg.next_feed_id;
        reg.next_feed_id += 1;
        reg.feed_by_id.insert(feed_id, shape_id.to_string());
        reg.shapes.insert(
            shape_id.to_string(),
            SubqueryShape {
                shape_id: shape_id.to_string(),
                outer_table: "issues".into(),
                stream_path: format!("shape/{shape_id}"),
                pred: Arc::new(CompiledPredicate::InSubquery {
                    col: project_col,
                    sig: sig.clone(),
                    negated: false,
                }),
                out_cols: None,
                gate: crate::pg::SnapshotGate::passthrough(),
                emitted: std::sync::atomic::AtomicU64::new(0),
                feed_id,
            },
        );
    }

    /// G2 loop test 1/2 — the delete-gate's OPEN-failure half: a delete for a pk that was
    /// **never** a member of the feed must be dropped (zero appends, zero wake), never a
    /// spurious delete. `project_members` (user 1) only ever contains project 100; an issue in
    /// project 999 is written then deleted without ever entering the `issues` shape's feed.
    #[tokio::test(flavor = "multi_thread")]
    async fn never_member_delete_is_dropped() {
        let ts = issues_ts();
        let (ds_url, store) = spawn_fake_ds().await;
        let mut reg = SubqueryRegistry::new(DsClient::new(&ds_url), None);
        let sig: SubquerySig = "project_members|project_id|L(user_id,Eq,1)".into();
        insert_test_node(&mut reg, &sig);
        // The only project user 1 is a member of.
        reg.apply_node_evals(&sig, vec![("pm-1".into(), Some(Value::Int(100)))]).await;
        insert_membership_shape(&mut reg, "s1", &sig, 1);

        // Write an issue in a NON-matching project (999): never a member, so nothing is emitted.
        let work = reg.on_table_delta(&ts, &[Tup2(issue(1, 999), 1)], 1, None, None, None).await.unwrap();
        assert!(work.is_empty(), "an outer-table delta never queues node-flip propagation");
        assert_eq!(reg.shapes["s1"].emitted.load(std::sync::atomic::Ordering::Relaxed), 0);

        // Delete it: still never a member -> the delete-gate must drop it (no spurious wake).
        reg.on_table_delta(&ts, &[Tup2(issue(1, 999), -1)], 2, None, None, None).await.unwrap();
        assert_eq!(
            reg.shapes["s1"].emitted.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "never-member delete must be dropped"
        );
        assert!(
            ops_for(&store, "shape/s1").is_empty(),
            "the shape's stream must receive ZERO appends for a never-member pk"
        );
    }

    /// G2 loop test 2/2 — the delete-gate's CLOSED-failure half: a delete for a pk that WAS a
    /// genuine member must never be dropped, on either exit path — (a) the row itself is
    /// deleted, (b) the row survives but the membership node it depended on flips the pk out
    /// (`project_members`'s row for user 1 is removed: "the user loses the project") — and a pk
    /// that re-enters after leaving must re-emit (the feed relation, not a one-shot latch).
    #[tokio::test(flavor = "multi_thread")]
    async fn genuine_member_delete_is_never_dropped() {
        let ts = issues_ts();
        let (ds_url, store) = spawn_fake_ds().await;
        let mut reg = SubqueryRegistry::new(DsClient::new(&ds_url), None);
        let sig: SubquerySig = "project_members|project_id|L(user_id,Eq,1)".into();
        insert_test_node(&mut reg, &sig);
        reg.apply_node_evals(&sig, vec![("pm-1".into(), Some(Value::Int(100)))]).await;
        insert_membership_shape(&mut reg, "s2", &sig, 1);
        fn emitted(reg: &SubqueryRegistry) -> u64 {
            reg.shapes["s2"].emitted.load(std::sync::atomic::Ordering::Relaxed)
        }

        // Enter: an issue in the matching project.
        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), 1)], 1, None, None, None).await.unwrap();
        assert_eq!(emitted(&reg), 1);
        assert_eq!(ops_for(&store, "shape/s2"), vec!["upsert"]);

        // Exit (a): the row itself is deleted -> exactly one delete emission.
        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), -1)], 2, None, None, None).await.unwrap();
        assert_eq!(emitted(&reg), 2, "exactly one more envelope: the row-delete emission");
        assert_eq!(ops_for(&store, "shape/s2"), vec!["upsert", "delete"]);

        // A pk that re-enters after leaving must re-emit — the feed relation gates on current
        // membership, it is not a one-shot "already told you" latch.
        reg.on_table_delta(&ts, &[Tup2(issue(1, 100), 1)], 3, None, None, None).await.unwrap();
        assert_eq!(emitted(&reg), 3, "a re-entering pk must re-emit");
        assert_eq!(ops_for(&store, "shape/s2"), vec!["upsert", "delete", "upsert"]);

        // Exit (b): the row is untouched, but the user loses the project — the membership node's
        // only contributor is withdrawn, flipping project 100 out of the inner set. In
        // production `propagate_flips`/`move_shape_for_value` discovers this and queries
        // Postgres back for the outer rows with `project_id = 100` to re-evaluate; this harness
        // has no Postgres, so the flip is driven directly and the still-present candidate row is
        // handed to the same `emit_for_shapes` re-derivation tail that query-back would call —
        // exactly the "membership flip" exit path, distinct from a row delete.
        let flips = reg.apply_node_evals(&sig, vec![("pm-1".into(), None)]).await;
        assert_eq!(flips, vec![Flip { value: Value::Int(100), dir: FlipDir::Leave }]);
        let results =
            reg.emit_for_shapes(&ts, vec![("s2".to_string(), vec![(issue(1, 100), true)])], None).await.unwrap();
        assert_eq!(
            results,
            vec![("s2".to_string(), true, -1)],
            "exactly one delete emission for the membership-flip exit path"
        );
        assert_eq!(emitted(&reg), 4);
        assert_eq!(ops_for(&store, "shape/s2"), vec!["upsert", "delete", "upsert", "delete"]);
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
