import {
  type ChangeEvent,
  type ColumnType,
  isAnd,
  isLeaf,
  isNot,
  isOr,
  type LeafOp,
  type Predicate,
  type TableDef,
  type Value,
} from './types.js'

export interface SqlFragment {
  text: string
  params: Value[]
}

/** Quote a SQL identifier (column/table name). */
function q(id: string): string {
  return `"${id.replace(/"/g, '""')}"`
}

const OP_SQL: Record<LeafOp, string> = {
  eq: '=',
  neq: '<>',
  lt: '<',
  lte: '<=',
  gt: '>',
  gte: '>=',
}

const TYPE_SQL: Record<ColumnType, string> = {
  int: 'INTEGER',
  float: 'DOUBLE PRECISION',
  text: 'TEXT',
  bool: 'BOOLEAN',
}

/**
 * Compile a predicate to a parameterized SQL boolean expression.
 * `startIndex` is the first `$n` placeholder number to use (1-based).
 */
export function predicateToSql(pred: Predicate, startIndex = 1): SqlFragment {
  const params: Value[] = []
  let next = startIndex
  const text = build(pred)
  return { text, params }

  function ph(value: Value): string {
    params.push(value)
    return `$${next++}`
  }

  function build(p: Predicate): string {
    if (isLeaf(p)) {
      return `${q(p.col)} ${OP_SQL[p.op]} ${ph(p.value)}`
    }
    if (isAnd(p)) {
      if (p.and.length === 0) return 'TRUE'
      return `(${p.and.map(build).join(' AND ')})`
    }
    if (isOr(p)) {
      if (p.or.length === 0) return 'FALSE'
      return `(${p.or.map(build).join(' OR ')})`
    }
    if (isNot(p)) {
      return `(NOT ${build(p.not)})`
    }
    throw new Error(`unknown predicate node: ${JSON.stringify(p)}`)
  }
}

/** `CREATE TABLE` DDL for one table. */
export function tableDDL(name: string, def: TableDef): string {
  const cols = Object.entries(def.columns).map(([col, c]) => `${q(col)} ${TYPE_SQL[c.type]}`)
  cols.push(`PRIMARY KEY (${q(def.primaryKey)})`)
  return `CREATE TABLE ${q(name)} (\n  ${cols.join(',\n  ')}\n)`
}

/**
 * Compile a change event to a parameterized DML statement.
 * insert/update -> upsert by pk; delete -> delete by pk.
 */
export function changeEventToDML(name: string, def: TableDef, ev: ChangeEvent): SqlFragment {
  const pk = def.primaryKey
  if (ev.op === 'delete') {
    return { text: `DELETE FROM ${q(name)} WHERE ${q(pk)} = $1`, params: [ev.pk] }
  }
  if (!ev.row) throw new Error(`change event op="${ev.op}" requires a row`)
  const columns = Object.keys(def.columns)
  const params: Value[] = columns.map((c) => ev.row![c] ?? null)
  const placeholders = columns.map((_, i) => `$${i + 1}`)
  const updates = columns
    .filter((c) => c !== pk)
    .map((c) => `${q(c)} = EXCLUDED.${q(c)}`)
  const colList = columns.map(q).join(', ')
  const text =
    updates.length === 0
      ? `INSERT INTO ${q(name)} (${colList}) VALUES (${placeholders.join(', ')}) ` +
        `ON CONFLICT (${q(pk)}) DO NOTHING`
      : `INSERT INTO ${q(name)} (${colList}) VALUES (${placeholders.join(', ')}) ` +
        `ON CONFLICT (${q(pk)}) DO UPDATE SET ${updates.join(', ')}`
  return { text, params }
}

/** `SELECT * ... WHERE <pred>` for a shape, parameterized. */
export function shapeSelectSql(name: string, where?: Predicate): SqlFragment {
  if (!where) return { text: `SELECT * FROM ${q(name)}`, params: [] }
  const frag = predicateToSql(where, 1)
  return { text: `SELECT * FROM ${q(name)} WHERE ${frag.text}`, params: frag.params }
}
