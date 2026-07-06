// Render the SQL statement a shape corresponds to — the query Postgres would run to compute the
// shape's initial contents. Values are inlined as literals (not `$n` placeholders) so the statement
// reads as something you could paste into psql. Mirrors the engine's predicate → SQL compilation
// (packages/protocol/src/sql.ts), specialized for readable display.

import type { EngineGraph, GraphShape, Predicate, SubqueryRef } from './types'

const OP: Record<string, string> = {
  eq: '=',
  neq: '<>',
  lt: '<',
  lte: '<=',
  gt: '>',
  gte: '>=',
  like: 'LIKE',
}

/** Quote a SQL identifier. */
function qi(id: string): string {
  return `"${id.replace(/"/g, '""')}"`
}

/** Format a value as a SQL literal. */
function lit(v: unknown): string {
  if (v === null || v === undefined) return 'NULL'
  if (typeof v === 'number') return String(v)
  if (typeof v === 'boolean') return v ? 'TRUE' : 'FALSE'
  return `'${String(v).replace(/'/g, "''")}'`
}

/** Compile a predicate AST to an inline-literal SQL boolean expression. */
export function predicateSql(p: Predicate | null | undefined): string {
  if (!p) return ''
  if ('and' in p) return p.and.map(wrap).join(' AND ')
  if ('or' in p) return p.or.map(wrap).join(' OR ')
  if ('not' in p) return `NOT (${predicateSql(p.not)})`
  if ('in' in p) return `${qi(p.col)} ${p.negated ? 'NOT IN' : 'IN'} ${subquerySql(p.in)}`
  return `${qi(p.col)} ${OP[p.op] ?? p.op} ${lit(p.value)}`
}

function wrap(p: Predicate): string {
  const s = predicateSql(p)
  return 'and' in p || 'or' in p ? `(${s})` : s
}

/** `(SELECT proj FROM inner [WHERE …])` for an IN-subquery reference. */
export function subquerySql(s: SubqueryRef): string {
  const w = s.where ? ` WHERE ${predicateSql(s.where)}` : ''
  return `(SELECT ${qi(s.project)} FROM ${qi(s.table)}${w})`
}

/** The `SELECT`-list projection for a shape: the aggregate, the explicit column list, or `*`. */
function projection(shape: GraphShape): string {
  if (shape.aggregate) {
    const fn = shape.aggregate.func.toUpperCase()
    return fn === 'COUNT' ? 'COUNT(*)' : `${fn}(${shape.aggregate.col ? qi(shape.aggregate.col) : '*'})`
  }
  return shape.columns && shape.columns.length ? shape.columns.map(qi).join(', ') : '*'
}

/**
 * The SQL statement a shape corresponds to. The top-level ANDs of the predicate are broken onto their
 * own lines so a compound visibility+filter predicate stays readable.
 */
export function shapeSql(shape: GraphShape): string {
  const head = `SELECT ${projection(shape)} FROM ${qi(shape.table)}`
  const w = shape.where
  if (!w) return `${head};`
  const conjuncts = 'and' in w ? w.and : [w]
  const lines = conjuncts.map((c, i) => `${i === 0 ? 'WHERE' : '  AND'} ${predicateSql(c)}`)
  return `${head}\n${lines.join('\n')};`
}

/**
 * Just the WHERE clause (no `SELECT … FROM`, no trailing `;`) — the form the engine's Electric
 * `/v1/shape` endpoint expects in its `where` query param. Empty string = match all.
 */
export function whereSql(shape: GraphShape): string {
  return shape.where ? predicateSql(shape.where) : ''
}

/** Find the IN-subquery reference within a predicate that targets a given (table, projection). */
function findInRef(p: Predicate | null | undefined, table: string, project: string): SubqueryRef | null {
  if (!p) return null
  if ('and' in p) return firstMatch(p.and, table, project)
  if ('or' in p) return firstMatch(p.or, table, project)
  if ('not' in p) return findInRef(p.not, table, project)
  if ('in' in p) {
    if (p.in.table === table && p.in.project === project) return p.in
    return findInRef(p.in.where, table, project)
  }
  return null
}

function firstMatch(ps: Predicate[], table: string, project: string): SubqueryRef | null {
  for (const c of ps) {
    const r = findInRef(c, table, project)
    if (r) return r
  }
  return null
}

/**
 * The SQL a shared subquery node maintains: `SELECT DISTINCT proj FROM inner [WHERE …]`. The node's
 * own predicate isn't carried on the node DTO, so we recover it from a dependent shape (tied to this
 * exact node by a subquery edge) — this keeps per-user nodes (`user_id = 1` vs `= 2`) distinct.
 */
export function nodeInnerSql(graph: EngineGraph, sig: string, innerTable: string, projCol: string): string {
  const edge = graph.subqueryEdges.find((e) => e.nodeSig === sig && e.dependentKind === 'shape')
  const shape = edge ? graph.shapes.find((s) => s.id === edge.dependentId) : undefined
  const ref = shape ? findInRef(shape.where, innerTable, projCol) : null
  const where = ref?.where ? ` WHERE ${predicateSql(ref.where)}` : ''
  return `SELECT DISTINCT ${qi(projCol)} FROM ${qi(innerTable)}${where};`
}
