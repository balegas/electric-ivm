import type { Predicate, SubqueryRef } from './types'

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

/** Short equality-key summary for a family member, e.g. `status = 'todo'`. Falls back to the predicate. */
export function keyLabel(where: Predicate | null): string {
  return predicateLabel(where)
}
