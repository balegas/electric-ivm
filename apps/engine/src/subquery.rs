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
use dbsp::ZWeight;
use dbsp::utils::Tup2;

use crate::ds::DsClient;
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
    /// Every node-refcount increment made by the in-flight `create_subquery_shape` (one entry per
    /// `collect()` call). On failure the create is rolled back exactly: each logged sig is decremented
    /// once, and nodes that return to zero are removed. The registry mutex is held for the whole
    /// create, so the log can't interleave with another create.
    collect_log: Vec<SubquerySig>,
    ds: DsClient,
    pg_url: Option<String>,
    schemas: SchemaMap,
    /// A single reused Postgres connection for node seeding / backfill / query-back. Subquery work is
    /// serialized under the registry mutex, so one connection suffices — and reusing it is essential:
    /// connecting per shape exhausts ephemeral TCP ports when thousands of subquery shapes are created.
    pg_client: tokio::sync::Mutex<Option<Arc<tokio_postgres::Client>>>,
}

impl SubqueryRegistry {
    pub fn new(ds: DsClient, pg_url: Option<String>) -> Self {
        SubqueryRegistry {
            nodes: HashMap::new(),
            edges: Vec::new(),
            shapes: HashMap::new(),
            pending_seed: Vec::new(),
            collect_log: Vec::new(),
            ds,
            pg_url,
            schemas: Arc::new(HashMap::new()),
            pg_client: tokio::sync::Mutex::new(None),
        }
    }

    /// Lazily connect to Postgres and cache the client, reconnecting if the cached connection has closed.
    /// All subquery PG access funnels through here so we hold one connection, not one per shape.
    async fn pg(&self) -> Result<Arc<tokio_postgres::Client>> {
        let url = self.pg_url.clone().context("subquery work requires postgres")?;
        let mut guard = self.pg_client.lock().await;
        if let Some(c) = guard.as_ref() {
            if !c.is_closed() {
                return Ok(c.clone());
            }
        }
        let client = Arc::new(crate::pg::connect(&url).await?);
        *guard = Some(client.clone());
        Ok(client)
    }

    pub fn set_schemas(&mut self, schemas: SchemaMap) {
        self.schemas = schemas;
    }

    /// Does any node's inner table or any shape's outer table equal `table`? (Fast skip for tailers of
    /// tables not involved in any subquery.)
    pub fn touches(&self, table: &str) -> bool {
        self.nodes.values().any(|n| n.inner_table == table)
            || self.shapes.values().any(|s| s.outer_table == table)
    }

    /// Number of maintained nodes (shared inner sets).
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// The live **inner-set index** of one node (the visualizer's "see the index" view): up to `cap`
    /// `(value, contributor-count)` pairs, most-shared first, plus the true distinct count, refcount, and
    /// whether the list was truncated. This is the actual dbsp-maintained set, not derivable from topology.
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
    pub async fn create_subquery_shape(
        &mut self,
        shape_id: &str,
        outer_table: &str,
        stream_path: &str,
        where_json: &PredicateJson,
        out_cols: Option<Arc<Vec<usize>>>,
        changes_only: bool,
    ) -> Result<()> {
        let edges_checkpoint = self.edges.len();
        self.collect_log.clear();
        let res = self
            .create_subquery_shape_inner(shape_id, outer_table, stream_path, where_json, out_cols, changes_only)
            .await;
        if res.is_err() {
            self.rollback_create(edges_checkpoint);
        }
        res
    }

    /// Undo an in-flight create: drop the edges it appended and decrement each logged node refcount
    /// once, removing nodes that return to zero (with their pending-seed entries and any stray edges).
    fn rollback_create(&mut self, edges_checkpoint: usize) {
        self.edges.truncate(edges_checkpoint);
        let log = std::mem::take(&mut self.collect_log);
        for sig in log {
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

    async fn create_subquery_shape_inner(
        &mut self,
        shape_id: &str,
        outer_table: &str,
        stream_path: &str,
        where_json: &PredicateJson,
        out_cols: Option<Arc<Vec<usize>>>,
        changes_only: bool,
    ) -> Result<()> {
        let outer_ts =
            self.schemas.get(outer_table).cloned().context("subquery shape: unknown outer table")?;
        // 1. Compile the outer predicate, discovering/deduping nodes (deepest-first) + node edges.
        let pred = Arc::new(CompiledPredicate::compile_with(where_json, &outer_ts, self)?);
        // 2. Record shape-level edges (the outer predicate's `IN` leaves).
        for leaf in collect_in_leaves(&pred) {
            self.edges.push(Edge {
                node_sig: leaf.sig,
                dependent: Dependent::Shape(shape_id.to_string()),
                connecting_col: leaf.col,
                negated: leaf.negated,
                null_sensitive: leaf.null_sensitive,
            });
        }
        // 3. Seed newly-created nodes from Postgres (Postgres evaluates nested subqueries natively).
        //    Nodes are seeded even for a `changes_only` feed — `matches_ctx` needs the inner sets to
        //    evaluate live membership.
        self.seed_pending_nodes().await?;
        // 4. Backfill the outer shape and append its initial members — UNLESS this is a `changes_only`
        //    feed (a subset's live tail), which forwards only future membership deltas (passthrough).
        let gate = if changes_only {
            crate::pg::SnapshotGate::passthrough()
        } else {
            let (wsql, params) = crate::sql::predicate_json_to_sql(where_json, 1);
            let bf = {
                let client = self.pg().await?;
                crate::pg::backfill_where(&client, &outer_ts, Some((wsql, params))).await?
            };
            let out: Vec<(Row, ZWeight)> = bf.rows.iter().map(|r| (r.clone(), 1)).collect();
            if !out.is_empty() {
                let envs = crate::engine::translate_output(
                    &outer_ts,
                    out,
                    None,
                    None,
                    out_cols.as_deref().map(Vec::as_slice),
                );
                self.ds.append(stream_path, &envs).await?;
            }
            bf.gate
        };
        // 5. Record the shape.
        self.shapes.insert(
            shape_id.to_string(),
            SubqueryShape {
                shape_id: shape_id.to_string(),
                outer_table: outer_table.to_string(),
                stream_path: stream_path.to_string(),
                pred,
                out_cols,
                gate,
            },
        );
        Ok(())
    }

    /// Seed every node queued in `pending_seed` (deepest-first) from a Postgres snapshot.
    async fn seed_pending_nodes(&mut self) -> Result<()> {
        let pending = std::mem::take(&mut self.pending_seed);
        if pending.is_empty() {
            return Ok(());
        }
        let client = self.pg().await?;
        for sig in pending {
            let (inner_table, where_json, proj_col) = {
                let n = self.nodes.get(&sig).context("seed: node vanished")?;
                (n.inner_table.clone(), n.where_json.clone(), n.proj_col)
            };
            let ts = self.schemas.get(&inner_table).cloned().context("seed: unknown inner table")?;
            let wsql = where_json.as_ref().map(|w| crate::sql::predicate_json_to_sql(w, 1));
            let bf = crate::pg::backfill_where(&client, &ts, wsql).await?;
            let node = self.nodes.get_mut(&sig).context("seed: node vanished")?;
            node.gate = bf.gate;
            for r in &bf.rows {
                let pk = ts.key_string(r).unwrap_or_default();
                let pv = r.0.get(proj_col).cloned().unwrap_or(Value::Null);
                node.reconcile_row(&pk, Some(pv));
            }
        }
        Ok(())
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

    /// Process one table delta: update affected nodes, emit outer-shape deltas, and propagate inner-set
    /// flips to dependents (querying back the affected rows). Appends move envelopes synchronously, so
    /// the caller's processed-offset barrier still implies convergence. `lsn` is the change's commit
    /// LSN (0 = unknown/never skip).
    pub async fn on_table_delta(
        &mut self,
        ts: &TableSchema,
        delta: &[Tup2<Row, ZWeight>],
        lsn: u64,
        xid: Option<u64>,
        txid: Option<String>,
        mut trace: Option<&mut Vec<crate::trace::TraceHop>>,
    ) -> Result<()> {
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

        // 3. Propagate flips up the DAG.
        while let Some((sig, flip)) = work.pop_front() {
            for edge in self.edges_of(&sig) {
                // A NULL-value flip only matters to NULL-sensitive dependents — a `NOT IN` leaf, or an
                // `IN` leaf under any `Not{…}` (SQL: a NULL in the set makes the leaf UNKNOWN, which
                // negation turns into a membership change). It can shift *every* dependent row, so
                // re-derive the dependent fully; NULL-insensitive dependents can't change (AND/OR are
                // monotone over FALSE < UNKNOWN < TRUE), so skip. (Not traced: rare path, and it
                // re-derives whole dependents rather than following this envelope's delta.)
                if matches!(flip.value, Value::Null) {
                    if edge.null_sensitive {
                        self.rederive_dependent(&edge, txid.clone(), &mut work).await?;
                    }
                    continue;
                }
                match &edge.dependent {
                    Dependent::Shape(id) => {
                        let moved =
                            self.move_shape_for_value(id, edge.connecting_col, &flip.value, txid.clone()).await?;
                        if moved {
                            hop(&mut trace, format!("shape:{id}"), "passed");
                        }
                    }
                    Dependent::Node(parent_sig) => {
                        let new_flips = self
                            .reconcile_parent_for_value(parent_sig, edge.connecting_col, &flip.value)
                            .await?;
                        hop(
                            &mut trace,
                            format!("node:{parent_sig}"),
                            if new_flips.is_empty() { "dropped" } else { "passed" },
                        );
                        for f in new_flips {
                            work.push_back((parent_sig.clone(), f));
                        }
                    }
                }
            }
        }
        Ok(())
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

    /// A parent node's value `v` was referenced by a flipped child; re-evaluate the parent's inner rows
    /// with `connecting_col = v` and reconcile them, returning the parent's resulting flips.
    async fn reconcile_parent_for_value(
        &mut self,
        parent_sig: &SubquerySig,
        connecting_col: usize,
        value: &Value,
    ) -> Result<Vec<Flip>> {
        let inner_table = match self.nodes.get(parent_sig) {
            Some(n) => n.inner_table.clone(),
            None => return Ok(Vec::new()),
        };
        let ts = self.schemas.get(&inner_table).cloned().context("parent node: unknown table")?;
        let rows = self.query_candidates(&ts, connecting_col, value).await?;
        let (pred, proj) = {
            let n = self.nodes.get(parent_sig).context("parent node vanished")?;
            (n.pred.clone(), n.proj_col)
        };
        let evals: Vec<(String, Option<Value>)> = rows
            .iter()
            .map(|r| {
                let pk = ts.key_string(r).unwrap_or_default();
                let pv = if pred.matches_ctx(r, self) {
                    Some(r.0.get(proj).cloned().unwrap_or(Value::Null))
                } else {
                    None
                };
                (pk, pv)
            })
            .collect();
        Ok(self.apply_node_flips(parent_sig, evals))
    }

    /// An inner-set value `v` flipped for an outer shape: query the outer rows with `connecting_col = v`,
    /// re-evaluate the full shape predicate, and append `upsert` (matches) / `delete` (doesn't) by pk.
    async fn move_shape_for_value(
        &self,
        shape_id: &str,
        connecting_col: usize,
        value: &Value,
        txid: Option<String>,
    ) -> Result<bool> {
        let Some(shape) = self.shapes.get(shape_id) else { return Ok(false) };
        let ts = self.schemas.get(&shape.outer_table).cloned().context("shape: unknown table")?;
        let rows = self.query_candidates(&ts, connecting_col, value).await?;
        if rows.is_empty() {
            return Ok(false);
        }
        let pred = shape.pred.clone();
        let out: Vec<(Row, ZWeight)> = rows
            .into_iter()
            .map(|r| {
                let w: ZWeight = if pred.matches_ctx(&r, self) { 1 } else { -1 };
                (r, w)
            })
            .collect();
        let envs = crate::engine::translate_output(&ts, out, txid, None, shape.out_cols.as_deref().map(Vec::as_slice));
        if envs.is_empty() {
            return Ok(false);
        }
        // Reliable: a dropped move envelope is permanent divergence for the shape's subscribers.
        self.ds.append_reliable(&shape.stream_path, &envs).await;
        Ok(true)
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
        // Per touched pk, take the row's latest state: the `+1` row if present (insert/update), else the
        // `-1` row (delete). `is_new` distinguishes "row still exists" from "row was deleted".
        let mut by_pk: HashMap<String, (Row, bool)> = HashMap::new();
        for Tup2(row, w) in delta {
            let pk = ts.key_string(row).unwrap_or_default();
            if *w > 0 {
                by_pk.insert(pk, (row.clone(), true));
            } else {
                by_pk.entry(pk).or_insert_with(|| (row.clone(), false));
            }
        }
        let out: Vec<(Row, ZWeight)> = by_pk
            .into_values()
            .map(|(row, is_new)| {
                let member = is_new && pred.matches_ctx(&row, self);
                (row, if member { 1 } else { -1 })
            })
            .collect();
        if out.is_empty() {
            return Ok(false);
        }
        let envs = crate::engine::translate_output(ts, out, txid, None, shape.out_cols.as_deref().map(Vec::as_slice));
        if envs.is_empty() {
            return Ok(false);
        }
        self.ds.append_reliable(&shape.stream_path, &envs).await;
        Ok(true)
    }

    /// Re-derive a dependent fully (used for NULL flips on negated edges): re-query every candidate row
    /// of the dependent's table and reconcile/emit. Rare (projections are typically non-null).
    async fn rederive_dependent(
        &mut self,
        edge: &Edge,
        txid: Option<String>,
        work: &mut VecDeque<(SubquerySig, Flip)>,
    ) -> Result<()> {
        match &edge.dependent {
            Dependent::Shape(id) => {
                let (outer_table, pred, stream_path, out_cols) = match self.shapes.get(id) {
                    Some(s) => (s.outer_table.clone(), s.pred.clone(), s.stream_path.clone(), s.out_cols.clone()),
                    None => return Ok(()),
                };
                let ts = self.schemas.get(&outer_table).cloned().context("rederive: unknown table")?;
                let rows = self.query_all(&ts).await?;
                let out: Vec<(Row, ZWeight)> = rows
                    .into_iter()
                    .map(|r| { let w: ZWeight = if pred.matches_ctx(&r, self) { 1 } else { -1 }; (r, w) })
                    .collect();
                let envs = crate::engine::translate_output(&ts, out, txid, None, out_cols.as_deref().map(Vec::as_slice));
                if !envs.is_empty() {
                    self.ds.append_reliable(&stream_path, &envs).await;
                }
            }
            Dependent::Node(parent_sig) => {
                let inner_table = match self.nodes.get(parent_sig) {
                    Some(n) => n.inner_table.clone(),
                    None => return Ok(()),
                };
                let ts = self.schemas.get(&inner_table).cloned().context("rederive: unknown table")?;
                let rows = self.query_all(&ts).await?;
                let (pred, proj) = {
                    let n = self.nodes.get(parent_sig).context("rederive: node vanished")?;
                    (n.pred.clone(), n.proj_col)
                };
                let evals: Vec<(String, Option<Value>)> = rows
                    .iter()
                    .map(|r| {
                        let pk = ts.key_string(r).unwrap_or_default();
                        let pv = if pred.matches_ctx(r, self) { Some(r.0.get(proj).cloned().unwrap_or(Value::Null)) } else { None };
                        (pk, pv)
                    })
                    .collect();
                for f in self.apply_node_flips(parent_sig, evals) {
                    work.push_back((parent_sig.clone(), f));
                }
            }
        }
        Ok(())
    }

    /// Query candidate rows of `ts` where `col = value` (current Postgres state).
    async fn query_candidates(&self, ts: &TableSchema, col: usize, value: &Value) -> Result<Vec<Row>> {
        let client = self.pg().await?;
        let where_sql = value_eq_sql(&ts.columns[col].0, value);
        let bf = crate::pg::backfill_where(&client, ts, Some(where_sql)).await?;
        Ok(bf.rows)
    }

    /// Query all rows of `ts` (for full re-derive).
    async fn query_all(&self, ts: &TableSchema) -> Result<Vec<Row>> {
        let client = self.pg().await?;
        let bf = crate::pg::backfill_where(&client, ts, None).await?;
        Ok(bf.rows)
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

/// Build a `WHERE col = value` fragment + params for a move query-back. Text is parameterized; other
/// scalars are inlined (mirrors the SQL emitter). NULL never reaches here (handled by full re-derive).
fn value_eq_sql(col: &str, value: &Value) -> (String, Vec<String>) {
    let name = crate::pg::quote_ident(col);
    match value {
        Value::Null => (format!("{name} IS NULL"), Vec::new()),
        Value::Int(i) => (format!("{name} = {i}"), Vec::new()),
        Value::Float(f) => (format!("{name} = {}", f.0), Vec::new()),
        Value::Bool(b) => (format!("{name} = {}", if *b { "true" } else { "false" }), Vec::new()),
        Value::Text(s) => (format!("{name} = $1"), vec![s.clone()]),
    }
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
        let res = reg
            .create_subquery_shape("s1", "outer_t", "shape/s1", &where_json, None, false)
            .await;
        assert!(res.is_err());
        assert_eq!(reg.nodes.len(), 0, "failed create left an orphaned node");
        assert_eq!(reg.edges.len(), 0, "failed create left orphaned edges");
        assert_eq!(reg.pending_seed.len(), 0, "failed create left a pending seed");
        assert!(reg.shapes.is_empty());
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
