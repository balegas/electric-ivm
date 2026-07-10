import type { GraphShape, Predicate, SubqueryRef } from './types'

const OPS: Record<string, string> = {
  eq: '=',
  neq: '≠',
  lt: '<',
  lte: '≤',
  gt: '>',
  gte: '≥',
  like: 'LIKE',
}

function val(v: unknown): string {
  if (v === null || v === undefined) return 'NULL'
  if (typeof v === 'string') return `'${v}'`
  return String(v)
}

/** Render a predicate JSON AST as a readable SQL-ish string for labels. */
export function predicateLabel(p: Predicate | null | undefined): string {
  if (!p) return 'match all'
  if ('and' in p) return p.and.map((c) => wrap(c)).join(' AND ')
  if ('or' in p) return p.or.map((c) => wrap(c)).join(' OR ')
  if ('not' in p) return `NOT (${predicateLabel(p.not)})`
  if ('in' in p) return `${p.col} ${p.negated ? 'NOT IN' : 'IN'} ${subqueryLabel(p.in)}`
  return `${p.col} ${OPS[p.op] ?? p.op} ${val(p.value)}`
}

function wrap(p: Predicate): string {
  const s = predicateLabel(p)
  return 'and' in p || 'or' in p ? `(${s})` : s
}

export function subqueryLabel(s: SubqueryRef): string {
  const w = s.where ? ` WHERE ${predicateLabel(s.where)}` : ''
  return `(SELECT ${s.project} FROM ${s.table}${w})`
}

/** Canonical TEMPLATE signature of a predicate: its structure with every leaf VALUE dropped, and
 *  AND/OR children sorted so equivalent predicates share one key (mirrors the engine's
 *  `canonical_pred`). Two subquery shapes that differ only in a bound parameter — `owner = 5` vs
 *  `owner = 8` — share this key, which is what lets the circuit view stack their identical
 *  pipelines while their materialized contents differ. */
export function predicateTemplate(p: Predicate | null | undefined): string {
  if (!p) return '*'
  if ('and' in p) return `A(${p.and.map(predicateTemplate).sort().join(',')})`
  if ('or' in p) return `O(${p.or.map(predicateTemplate).sort().join(',')})`
  if ('not' in p) return `N(${predicateTemplate(p.not)})`
  // A subquery leaf: its structure (table, projection, negation, inner-where TEMPLATE) is part of
  // the template; the inner bound values are dropped just like the outer ones.
  if ('in' in p)
    return `I(${p.col},${p.negated ? 1 : 0},${p.in.table},${p.in.project},${predicateTemplate(p.in.where)})`
  return `L(${p.col},${p.op})`
}

/** Human-readable rendering of a predicate's TEMPLATE — like `predicateLabel`, but every literal is
 *  shown as `?` (a bound parameter). Used as the headline of a stacked subquery group, which stands
 *  in for several instances that share this shape and differ only in their bindings. */
export function predicateTemplateLabel(p: Predicate | null | undefined): string {
  if (!p) return 'match all'
  if ('and' in p) return p.and.map((c) => wrapTemplate(c)).join(' AND ')
  if ('or' in p) return p.or.map((c) => wrapTemplate(c)).join(' OR ')
  if ('not' in p) return `NOT (${predicateTemplateLabel(p.not)})`
  if ('in' in p) return `${p.col} ${p.negated ? 'NOT IN' : 'IN'} ${subqueryTemplateLabel(p.in)}`
  return `${p.col} ${OPS[p.op] ?? p.op} ?`
}

function wrapTemplate(p: Predicate): string {
  const s = predicateTemplateLabel(p)
  return 'and' in p || 'or' in p ? `(${s})` : s
}

function subqueryTemplateLabel(s: SubqueryRef): string {
  const w = s.where ? ` WHERE ${predicateTemplateLabel(s.where)}` : ''
  return `(SELECT ${s.project} FROM ${s.table}${w})`
}

/** Short equality-key summary for a family member, e.g. `status = 'todo'`. Falls back to the predicate. */
export function keyLabel(where: Predicate | null): string {
  return predicateLabel(where)
}

// ---------------------------------------------------------------------------------------------
// Shared "is this a subquery-bearing shape, and what template does it share" helpers. The logical
// view (`build-graph`) and the circuit view (`build-circuit`) BOTH group repeated subquery shapes,
// and they must agree exactly on which shapes are subquery-bearing and which of them share a
// pipeline — otherwise one view stacks a pair the other leaves apart. These two functions are that
// single shared definition; both views import them rather than re-deriving the test inline.
// ---------------------------------------------------------------------------------------------

/** Does a predicate use an `IN (SELECT …)` membership test anywhere in its tree? Mirrors the
 *  engine's `predicate_has_subquery` (subquery.rs) so the visualizer's notion of "subquery-bearing"
 *  never drifts from the executor's. */
export function predicateHasSubquery(p: Predicate | null | undefined): boolean {
  if (!p) return false
  if ('and' in p) return p.and.some(predicateHasSubquery)
  if ('or' in p) return p.or.some(predicateHasSubquery)
  if ('not' in p) return predicateHasSubquery(p.not)
  if ('in' in p) return true
  return false
}

/** Is this shape subquery-bearing? The engine stamps this on the OUTER shape that USES the subquery
 *  (`GraphShape.isSubquery`) — NOT on the inner subquery nodes — so keying grouping on the flag is
 *  correct. We additionally fall back to inspecting the predicate so a shape from an older/mislabeled
 *  engine that forgot the flag still groups. Both views group on THIS definition. */
export function isSubqueryShape(s: GraphShape): boolean {
  return s.isSubquery || predicateHasSubquery(s.where)
}

/** The structural template key of a subquery shape's maintained pipeline: its outer table, its
 *  predicate TEMPLATE (every bound value dropped — see `predicateTemplate`), and its projection as an
 *  ORDER-INSENSITIVE column set. Two instances of the same query that differ only in their bound
 *  parameter (`user_id = 5` vs `user_id = 8`) produce the SAME key, which is what lets both views
 *  stack their structurally identical pipelines while their materialized inner sets differ. The
 *  projection is sorted so two callers that request the same columns in a different order still
 *  share the key (a SELECT-list is a set for grouping purposes, and the engine emits `columns` in
 *  request order — it does not canonicalize it the way it canonicalizes the predicate). */
export function subqueryTemplateKey(s: GraphShape): string {
  const cols = (s.columns ?? []).slice().sort().join(',')
  return `${s.table}|${predicateTemplate(s.where)}|${cols}`
}
