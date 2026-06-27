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
 * Two-valued logic with the convention that any comparison against a `null`/absent cell
 * is `false`. This matches Postgres `WHERE` semantics as long as cells are non-null, which
 * is the M1/M2 contract (the simulator populates every column). Null three-valued logic is
 * a deliberate, documented gap deferred past M2.
 */
export function evaluate(pred: Predicate, row: Row): boolean {
  if (isLeaf(pred)) {
    const cell = row[pred.col]
    return compare(cell, pred.op, pred.value)
  }
  if (isAnd(pred)) return pred.and.every((p) => evaluate(p, row))
  if (isOr(pred)) return pred.or.some((p) => evaluate(p, row))
  if (isNot(pred)) return !evaluate(pred.not, row)
  // Exhaustiveness guard.
  throw new Error(`unknown predicate node: ${JSON.stringify(pred)}`)
}

function compare(cell: Value | undefined, op: LeafOp, value: Value): boolean {
  if (cell === null || cell === undefined) return false
  switch (op) {
    case 'eq':
      return cell === value
    case 'neq':
      return cell !== value
    case 'lt':
      return value !== null && cell < value
    case 'lte':
      return value !== null && cell <= value
    case 'gt':
      return value !== null && cell > value
    case 'gte':
      return value !== null && cell >= value
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
