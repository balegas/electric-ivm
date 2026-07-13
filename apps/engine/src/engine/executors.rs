//! Per-shape executor structures: standalone filters + their conjunct index, key routers,
//! aggregate folds, and the circuit-shape wrappers. Pure data + delta math; no engine state.

use super::*;

/// Live group membership of a circuit-served shape. (`All`/`Static` mirror the planner's
/// reserved variants; `inner_table` is kept for dumps/introspection.)
#[allow(dead_code)]
pub(crate) enum CohortGroups {
    All,
    Static {
        col: usize,
        keys: std::collections::HashSet<Value>,
    },
    Dynamic {
        col: usize,
        inner_table: String,
        inner_proj: usize,
        inner_col: usize,
        inner_key: Value,
        /// Projected value → number of contributing inner rows (refcounted: two membership
        /// rows yielding the same value must both leave before the group does).
        groups: HashMap<Value, i64>,
    },
}

impl CohortGroups {
    /// Does `row` fall in this shape's groups? (`NULL` cohort values never match.)
    pub(crate) fn admits(&self, row: &Row) -> bool {
        match self {
            CohortGroups::All => true,
            CohortGroups::Static { col, keys } => {
                row.0.get(*col).is_some_and(|v| v != &Value::Null && keys.contains(v))
            }
            CohortGroups::Dynamic { col, groups, .. } => {
                row.0.get(*col).is_some_and(|v| v != &Value::Null && groups.contains_key(v))
            }
        }
    }
}

/// A shape served from the circuit: seeded from arrangement snapshots, updated by routing each
/// transaction's deltas through (cohort groups ∧ residual). No Postgres, no snapshot gate —
/// consistency comes from creating/reading inside the sequencer, between transactions.
pub(crate) struct CircuitShape {
    #[allow(dead_code)] // kept for dumps/introspection parity with the other executors
    pub(crate) num_id: u64,
    pub(crate) stream_path: String,
    pub(crate) groups: CohortGroups,
    pub(crate) residual: Option<Arc<CompiledPredicate>>,
    pub(crate) out_cols: Option<Arc<Vec<usize>>>,
}

impl CircuitShape {
    pub(crate) fn matches(&self, row: &Row) -> bool {
        self.groups.admits(row) && self.residual.as_ref().is_none_or(|p| p.matches(row))
    }
}

/// A COUNT aggregate served from the counts pipeline: `value` = Σ counts of matching groups.
pub(crate) struct CircuitAgg {
    pub(crate) stream_path: String,
    /// Aligned with the table's count group columns; `None` = unconstrained dimension.
    pub(crate) constraints: Vec<Option<std::collections::HashSet<Value>>>,
    pub(crate) value: i64,
}

impl CircuitAgg {
    pub(crate) fn group_matches(&self, group: &Row) -> bool {
        self.constraints.iter().enumerate().all(|(i, c)| match c {
            None => true,
            Some(keys) => group.0.get(i).is_some_and(|v| keys.contains(v)),
        })
    }

    /// The shared aggregate wire envelope (see [`super::output::agg_envelope`]).
    pub(crate) fn envelope(&self, table: &str, txid: Option<String>, lsn: Option<String>) -> Envelope {
        super::output::agg_envelope(table, serde_json::json!(self.value), self.value, txid, lsn)
    }
}

/// A non-shareable shape (range / OR / NOT / inequality / match-all). Its predicate is a stateless
/// filter, so it needs no incremental state or OS thread — it is evaluated directly on each delta. This
/// is what lets standalone shapes scale far past the old one-thread-per-shape ceiling.
pub(crate) struct StandaloneShape {
    pub(crate) pred: Arc<CompiledPredicate>,
    pub(crate) stream_path: String,
    /// This shape's backfill-snapshot fence: replicated changes already visible to the backfill are
    /// skipped by xid visibility (LSN fallback) — see [`crate::pg::SnapshotGate`].
    pub(crate) gate: crate::pg::SnapshotGate,
    /// Output projection (column indices), or `None` to emit the full row.
    pub(crate) out_cols: Option<Arc<Vec<usize>>>,
}

/// Evaluate a stateless WHERE filter directly on a Z-set delta. A filter has no incremental state
/// (unlike a join), so wrapping it in a dataflow circuit would only add a thread + channel round-trip
/// + a per-shape clone of the delta. `translate_output` downstream groups by primary key, so emitting
/// the matching `(row, weight)` pairs here is equivalent to what the old per-shape filter circuit produced.
pub(crate) fn eval_standalone(pred: &CompiledPredicate, delta: &[Tup2<Row, ZWeight>]) -> Vec<(Row, ZWeight)> {
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
pub(crate) struct StandaloneIndex {
    /// `col = v` conjuncts: column -> literal -> shape ids.
    pub(crate) eq: HashMap<usize, HashMap<Value, Vec<String>>>,
    /// `col >/>= v` conjuncts: column -> bound -> (shape id, strict). A row value `x` satisfies
    /// bounds `< x` (any) and `== x` (non-strict only) — an ordered prefix scan.
    pub(crate) lower: HashMap<usize, std::collections::BTreeMap<Value, Vec<(String, bool)>>>,
    /// `col </<= v` conjuncts, mirrored.
    pub(crate) upper: HashMap<usize, std::collections::BTreeMap<Value, Vec<(String, bool)>>>,
    /// Shapes with no indexable conjunct — always candidates.
    pub(crate) scan: Vec<String>,
    /// Where each shape was placed, for removal.
    pub(crate) placed: HashMap<String, Option<crate::predicate::AccessLeaf>>,
}

impl StandaloneIndex {
    pub(crate) fn insert(&mut self, sid: &str, pred: &CompiledPredicate) {
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

    pub(crate) fn remove(&mut self, sid: &str) {
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

    pub(crate) fn remove_bound(
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
    pub(crate) fn candidates(&self, delta: &[Tup2<Row, ZWeight>]) -> Vec<String> {
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
pub(crate) struct RoutedShape {
    pub(crate) num_id: u64,
    pub(crate) stream_path: String,
    /// THIS shape's own backfill-snapshot fence (see [`crate::pg::SnapshotGate`]).
    pub(crate) gate: crate::pg::SnapshotGate,
    /// Output projection (column indices), or `None` to emit the full row.
    pub(crate) out_cols: Option<Arc<Vec<usize>>>,
}

/// All equality shapes sharing one key-column set, indexed by key tuple. Holds **no table rows** —
/// only the `key -> shapes` routing. A change is routed by its key to exactly the shapes registered on
/// that key (O(log N), independent of shape count); each shape is backfilled directly from Postgres
/// (`WHERE key = const`), so the engine never keeps a copy of the table.
pub(crate) struct KeyRouter {
    pub(crate) key_cols: Vec<usize>,
    pub(crate) index: HashMap<Row, Vec<RoutedShape>>,
}

impl KeyRouter {
    pub(crate) fn member_count(&self) -> usize {
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

pub(crate) fn value_f64(v: &Value) -> f64 {
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
pub(crate) struct AggShape {
    pub(crate) pred: Arc<CompiledPredicate>,
    pub(crate) func: AggFn,
    pub(crate) col: Option<usize>,
    pub(crate) stream_path: String,
    pub(crate) gate: crate::pg::SnapshotGate,
    /// Matching rows (COUNT(*) semantics).
    pub(crate) count: i64,
    /// Matching rows whose aggregated column is non-NULL — SQL aggregates ignore NULLs, so this is
    /// the denominator for AVG, the COUNT(col) value, and the emptiness test for SUM/MIN/MAX.
    pub(crate) nn_count: i64,
    pub(crate) sum: f64,
    pub(crate) multiset: std::collections::BTreeMap<Value, i64>,
    pub(crate) last: Option<serde_json::Value>,
}

impl AggShape {
    /// Fold a Z-set delta into the running aggregate. Returns true if any matching row was seen.
    /// NULL column values are excluded from the fold (SQL semantics: aggregates ignore NULLs).
    pub(crate) fn apply(&mut self, delta: &[Tup2<Row, ZWeight>]) -> bool {
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
    pub(crate) fn value(&self) -> serde_json::Value {
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

    /// The shared aggregate wire envelope (see [`super::output::agg_envelope`]) carrying the
    /// current value (key `"agg"`, so the client materializes one row).
    pub(crate) fn envelope(&self, ts: &TableSchema, txid: Option<String>, lsn: Option<String>) -> Envelope {
        super::output::agg_envelope(&ts.name, self.value(), self.count, txid, lsn)
    }
}

/// The key tuple for a row given the template's key columns (positional projection). Missing columns
/// project to NULL (defensive; equality-template columns always exist).
pub(crate) fn key_of(row: &Row, cols: &[usize]) -> Row {
    Row(cols.iter().map(|&i| row.0.get(i).cloned().unwrap_or(Value::Null)).collect())
}

/// Resolve an optional column-name projection to sorted, pk-included column indices (the pk is always
/// kept so the client can key rows). `None` => emit the full row. Shared by shapes and subset queries.
pub(crate) fn resolve_columns(ts: &TableSchema, columns: Option<Vec<String>>) -> Result<Option<Arc<Vec<usize>>>> {
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
