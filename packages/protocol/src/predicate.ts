import {
  isAnd,
  isLeaf,
  isNot,
  isOr,
  type LeafOp,
  type Predicate,
  type Row,
  type TableDef,
  type Value,
} from './types.js'

/**
 * Reference evaluator: does `row` satisfy `pred`?
 *
 * SQL three-valued logic (TRUE / FALSE / UNKNOWN). Any comparison with a NULL/absent operand is
 * UNKNOWN; AND/OR follow the SQL truth tables; `NOT UNKNOWN = UNKNOWN`. A row is included iff the
 * predicate is TRUE — matching Postgres `WHERE` exactly, including the `NOT (col = x)` over NULL
 * case. Mirrors the Rust engine's `predicate::CompiledPredicate::matches`.
 */
export function evaluate(pred: Predicate, row: Row): boolean {
  return evalTri(pred, row) === true
}

/** Three-valued result: `true`, `false`, or `null` (UNKNOWN). */
type Tri = boolean | null

function evalTri(pred: Predicate, row: Row): Tri {
  if (isLeaf(pred)) return compare(row[pred.col], pred.op, pred.value)
  if (isAnd(pred)) {
    // FALSE dominates; else UNKNOWN if any UNKNOWN; else TRUE (empty AND => TRUE).
    let acc: Tri = true
    for (const p of pred.and) {
      const r = evalTri(p, row)
      if (r === false) return false
      if (r === null) acc = null
    }
    return acc
  }
  if (isOr(pred)) {
    // TRUE dominates; else UNKNOWN if any UNKNOWN; else FALSE (empty OR => FALSE).
    let acc: Tri = false
    for (const p of pred.or) {
      const r = evalTri(p, row)
      if (r === true) return true
      if (r === null) acc = null
    }
    return acc
  }
  if (isNot(pred)) {
    const r = evalTri(pred.not, row)
    return r === null ? null : !r
  }
  throw new Error(`unknown predicate node: ${JSON.stringify(pred)}`)
}

function compare(cell: Value | undefined, op: LeafOp, value: Value): Tri {
  // Any NULL operand => UNKNOWN.
  if (cell === null || cell === undefined || value === null) return null
  switch (op) {
    case 'eq':
      return cell === value
    case 'neq':
      return cell !== value
    case 'lt':
      return cell < value
    case 'lte':
      return cell <= value
    case 'gt':
      return cell > value
    case 'gte':
      return cell >= value
  }
}

export class PredicateError extends Error {}

/**
 * Validate a predicate against a table definition: every referenced column must exist and
 * every literal must be type-compatible with its column. Throws `PredicateError` on failure.
 */
export function validatePredicate(pred: Predicate, table: TableDef): void {
  if (isLeaf(pred)) {
    const col = table.columns[pred.col]
    if (!col) throw new PredicateError(`unknown column "${pred.col}"`)
    if (pred.value !== null && !valueMatchesType(pred.value, col.type)) {
      throw new PredicateError(
        `value ${JSON.stringify(pred.value)} is not compatible with column "${pred.col}" of type ${col.type}`,
      )
    }
    return
  }
  if (isAnd(pred)) {
    pred.and.forEach((p) => validatePredicate(p, table))
    return
  }
  if (isOr(pred)) {
    pred.or.forEach((p) => validatePredicate(p, table))
    return
  }
  if (isNot(pred)) {
    validatePredicate(pred.not, table)
    return
  }
  throw new PredicateError(`unknown predicate node: ${JSON.stringify(pred)}`)
}

function valueMatchesType(value: Value, type: TableDef['columns'][string]['type']): boolean {
  switch (type) {
    case 'int':
      return typeof value === 'number' && Number.isInteger(value)
    case 'float':
      return typeof value === 'number'
    case 'text':
      return typeof value === 'string'
    case 'bool':
      return typeof value === 'boolean'
  }
}
