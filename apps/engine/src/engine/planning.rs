//! Circuit placement planning: which shapes/aggregates the always-on circuit can serve.

use super::*;

/// Where a circuit-served shape's data comes from, for the graph payload.
#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CircuitPlacement {
    /// `all` | `static:<col>` | `dynamic:<col>` | `counts`.
    pub label: String,
    /// The arrangement column serving the cohort (absent for `all`/`counts`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub col: Option<usize>,
    /// True for counts-served aggregates.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub counts: bool,
}

/// Planner output: the shape's cohort constraint (see [`CohortGroups`] for the live form).
/// `All`/`Static` are classified (so a cohort leaf is recognized and kept out of the
/// residual) but currently never planned — the legacy tiers route those by index; they are
/// the seams for a future group-indexed static tier.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) enum PlannedConstraint {
    All,
    Static { col: usize, keys: std::collections::HashSet<Value> },
    Dynamic { col: usize, inner_table: String, inner_proj: usize, inner_col: usize, inner_key: Value },
}

pub(crate) struct CircuitPlan {
    pub constraint: PlannedConstraint,
    pub residual: Option<PredicateJson>,
}

/// Decompose `where_` into a cohort constraint plus a residual conjunction. Returns a plan
/// only for **dynamic** (membership-subquery) constraints: static equality and match-all
/// shapes stay on the KeyRouter/standalone tiers, whose routing indexes make them
/// output-sensitive — serving them from the circuit would replace an indexed route with a
/// linear per-delta scan. `None` therefore means "serve on the legacy tier" (which for
/// unplannable subqueries — nested/negated/multiple, or unindexed columns — is the registry).
pub(crate) fn plan_circuit_shape(
    where_: Option<&PredicateJson>,
    ts: &TableSchema,
    schemas: &HashMap<String, TableSchema>,
    arr: &crate::arrangements::Arrangements,
) -> Option<CircuitPlan> {
    // Match-all shapes are already optimal on the legacy tier (a bare fan-out, no state).
    let p = where_?;
    let children: Vec<PredicateJson> = match p {
        PredicateJson::And { and } => and.clone(),
        other => vec![other.clone()],
    };
    let mut constraint: Option<PlannedConstraint> = None;
    let mut residual: Vec<PredicateJson> = Vec::new();
    for child in children {
        if constraint.is_none() {
            if let Some(c) = as_cohort_constraint(&child, ts, schemas, arr) {
                constraint = Some(c);
                continue;
            }
        }
        if predicate_has_subquery(&child) {
            // A subquery leaf the constraint slot cannot take: only the registry can serve it.
            return None;
        }
        residual.push(child);
    }
    let constraint = match constraint {
        Some(c @ PlannedConstraint::Dynamic { .. }) => c,
        // Static/match-all shapes: the legacy tiers already route them by index; a circuit
        // shape would be a linear scan per delta. Put any classified leaf back in the
        // residual conceptually by declining the plan altogether.
        _ => return None,
    };
    let residual = match residual.len() {
        0 => None,
        1 => residual.into_iter().next(),
        _ => Some(PredicateJson::And { and: residual }),
    };
    Some(CircuitPlan { constraint, residual })
}

/// Classify one AND-child as a cohort constraint, if its column(s) are arrangement-indexed:
/// `col = lit`, an OR of same-column equalities (the IN-list form), or one non-negated
/// single-level `col IN (SELECT proj FROM inner WHERE inner_col = lit)`.
pub(crate) fn as_cohort_constraint(
    p: &PredicateJson,
    ts: &TableSchema,
    schemas: &HashMap<String, TableSchema>,
    arr: &crate::arrangements::Arrangements,
) -> Option<PlannedConstraint> {
    match p {
        PredicateJson::Leaf { col, op: crate::predicate::LeafOp::Eq, value } => {
            let idx = *ts.index.get(col)?;
            if !arr.has_index(&ts.name, &[idx]) {
                return None;
            }
            let v = Value::literal_from_json(value, ts.columns.get(idx)?.1).ok()?;
            if v == Value::Null {
                return None; // `col = NULL` never matches; leave it to the residual
            }
            Some(PlannedConstraint::Static { col: idx, keys: std::iter::once(v).collect() })
        }
        PredicateJson::Or { or } if !or.is_empty() => {
            let mut keys = std::collections::HashSet::new();
            let mut idx: Option<usize> = None;
            for c in or {
                let PredicateJson::Leaf { col, op: crate::predicate::LeafOp::Eq, value } = c else {
                    return None;
                };
                let i = *ts.index.get(col)?;
                if *idx.get_or_insert(i) != i {
                    return None;
                }
                let v = Value::literal_from_json(value, ts.columns.get(i)?.1).ok()?;
                if v == Value::Null {
                    return None;
                }
                keys.insert(v);
            }
            let i = idx?;
            if !arr.has_index(&ts.name, &[i]) {
                return None;
            }
            Some(PlannedConstraint::Static { col: i, keys })
        }
        PredicateJson::In { col, subquery, negated: false } => {
            let outer_idx = *ts.index.get(col)?;
            if !arr.has_index(&ts.name, &[outer_idx]) {
                return None;
            }
            let its = schemas.get(&subquery.table)?;
            let proj = *its.index.get(&subquery.project)?;
            // Single level only: the inner where must be one equality leaf on an indexed column.
            let PredicateJson::Leaf { col: icol, op: crate::predicate::LeafOp::Eq, value } =
                subquery.where_.as_deref()?
            else {
                return None;
            };
            let inner_idx = *its.index.get(icol)?;
            if !arr.has_index(&subquery.table, &[inner_idx]) {
                return None;
            }
            let key = Value::literal_from_json(value, its.columns.get(inner_idx)?.1).ok()?;
            Some(PlannedConstraint::Dynamic {
                col: outer_idx,
                inner_table: subquery.table.clone(),
                inner_proj: proj,
                inner_col: inner_idx,
                inner_key: key,
            })
        }
        _ => None,
    }
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
