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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::predicate::{CompiledPredicate, SubqueryEval, SubquerySig};
use crate::schema::TableSchema;
use crate::value::Value;

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
    /// `pg_current_wal_lsn()` at the node's backfill snapshot; inner deltas with commit LSN strictly
    /// `< seed_lsn` are already counted and are skipped.
    pub seed_lsn: u64,
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
        seed_lsn: u64,
    ) -> Self {
        SubqueryNode {
            sig,
            inner_table,
            proj_col,
            pk_col,
            pred,
            seed_lsn,
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
}

/// The cross-table registry of subquery nodes + shapes + edges. Implements [`SubqueryEval`] so a
/// predicate's subquery leaves resolve against the maintained node sets.
#[derive(Default)]
pub struct SubqueryRegistry {
    /// Nodes by canonical signature (shared across identical subqueries).
    pub nodes: HashMap<SubquerySig, SubqueryNode>,
    /// Edges from each node to its dependents.
    pub edges: Vec<Edge>,
}

impl SubqueryRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Outgoing edges for a node signature.
    pub fn edges_of<'a>(&'a self, sig: &'a SubquerySig) -> impl Iterator<Item = &'a Edge> + 'a {
        self.edges.iter().filter(move |e| &e.node_sig == sig)
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

/// Build a `TableSchema` lookup that the registry uses for seeding/SQL/evaluation. Shared with the
/// engine's compiled schema (`Arc<HashMap<String, TableSchema>>`).
pub type SchemaMap = Arc<HashMap<String, TableSchema>>;

#[cfg(test)]
mod tests {
    use super::*;

    fn node() -> SubqueryNode {
        SubqueryNode::new("sig".into(), "t".into(), 0, 1, Arc::new(CompiledPredicate::MatchAll), 0)
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

    #[test]
    fn registry_eval_reads_node_sets() {
        let mut reg = SubqueryRegistry::new();
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
