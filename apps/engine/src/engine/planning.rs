//! Circuit placement planning: which COUNT aggregates the counts pipelines can serve.
//! (Membership shapes are served by the subquery registry — row data lives in Postgres, so
//! there is no cohort/arrangement tier to plan for.)

use super::*;

/// Where a circuit-served shape's data comes from, for the graph payload.
#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CircuitPlacement {
    /// `counts` (the only circuit-served class).
    pub label: String,
    /// Unused for counts placements; kept for payload stability.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub col: Option<usize>,
    /// True for counts-served aggregates.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub counts: bool,
}

/// Decompose an aggregate's WHERE into per-group-column constraints over the table's counts
/// pipeline: a conjunction of equalities / IN-lists over the group columns ONLY (any leftover
/// conjunct would make group sums wrong). `None` = not servable from counts.
pub(crate) fn plan_circuit_agg(
    where_: Option<&PredicateJson>,
    ts: &TableSchema,
    group_cols: &[usize],
) -> Option<Vec<Option<std::collections::HashSet<Value>>>> {
    let mut constraints: Vec<Option<std::collections::HashSet<Value>>> = vec![None; group_cols.len()];
    let Some(p) = where_ else { return Some(constraints) };
    let children: Vec<&PredicateJson> = match p {
        PredicateJson::And { and } => and.iter().collect(),
        other => vec![other],
    };
    for child in children {
        let (idx, keys) = match child {
            PredicateJson::Leaf { col, op: crate::predicate::LeafOp::Eq, value } => {
                let i = *ts.index.get(col)?;
                let v = Value::literal_from_json(value, ts.columns.get(i)?.1).ok()?;
                (i, std::iter::once(v).collect::<std::collections::HashSet<_>>())
            }
            PredicateJson::Or { or } if !or.is_empty() => {
                let mut keys = std::collections::HashSet::new();
                let mut idx: Option<usize> = None;
                for c in or {
                    let PredicateJson::Leaf { col, op: crate::predicate::LeafOp::Eq, value } = c
                    else {
                        return None;
                    };
                    let i = *ts.index.get(col)?;
                    if *idx.get_or_insert(i) != i {
                        return None;
                    }
                    keys.insert(Value::literal_from_json(value, ts.columns.get(i)?.1).ok()?);
                }
                (idx?, keys)
            }
            _ => return None,
        };
        let pos = group_cols.iter().position(|&g| g == idx)?;
        if constraints[pos].is_some() {
            return None;
        }
        constraints[pos] = Some(keys);
    }
    Some(constraints)
}
