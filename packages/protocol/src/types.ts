// Cross-language contract for electric-lite.
//
// These JSON shapes are the single source of truth shared by the TS API/oracle/client
// and the Rust dbsp engine (which mirrors them with serde). Keep them minimal and stable.

/** Supported column types. Maps to a JS `Value` and a Postgres type. */
export type ColumnType = 'int' | 'text' | 'bool' | 'float'

/** A scalar cell value. `null` is permitted for absent values. */
export type Value = number | string | boolean | null

export interface ColumnDef {
  type: ColumnType
}

export interface TableDef {
  columns: Record<string, ColumnDef>
  /** Name of the primary-key column. Must be a key of `columns`. */
  primaryKey: string
}

export interface Schema {
  tables: Record<string, TableDef>
}

/** A row is a flat record of column name -> value. */
export type Row = Record<string, Value>

// --- Predicate AST -----------------------------------------------------------

/** Comparison operators for a leaf predicate. */
export type LeafOp = 'eq' | 'neq' | 'lt' | 'lte' | 'gt' | 'gte'

export interface LeafPredicate {
  col: string
  op: LeafOp
  value: Value
}

export interface AndPredicate {
  and: Predicate[]
}

export interface OrPredicate {
  or: Predicate[]
}

export interface NotPredicate {
  not: Predicate
}

/**
 * A restricted boolean predicate over a single table's columns.
 * M1 uses only `eq` leaves; M2 adds the other ops plus and/or/not.
 */
export type Predicate = LeafPredicate | AndPredicate | OrPredicate | NotPredicate

export function isLeaf(p: Predicate): p is LeafPredicate {
  return 'col' in p && 'op' in p
}
export function isAnd(p: Predicate): p is AndPredicate {
  return 'and' in p
}
export function isOr(p: Predicate): p is OrPredicate {
  return 'or' in p
}
export function isNot(p: Predicate): p is NotPredicate {
  return 'not' in p
}

// --- Shapes ------------------------------------------------------------------

/** A shape is one table + an optional predicate over that table's columns. */
export interface ShapeDef {
  table: string
  /** Omitted/undefined predicate means "all rows of the table". */
  where?: Predicate
  /**
   * Output projection: the columns to sync to the client. Omitted = the full row. The primary key is
   * always included (the client keys rows by it). Use this to keep large unused columns out of a
   * shape's stream (e.g. a list view that never reads a big `description`). The predicate may still
   * reference columns outside this set — projection only affects what is emitted, not what is matched.
   */
  columns?: string[]
}

/** Handle returned when a shape is registered; the client materializes from `streamPath`. */
export interface ShapeHandle {
  shapeId: string
  table: string
  /** Stream path on the durable-streams server, e.g. `shape/<shapeId>`. */
  streamPath: string
}

// --- Change events (the unit on every stream) --------------------------------

/**
 * Write operations. `insert` and `update` are both UPSERT (set row by pk); this keeps
 * the engine and the oracle trivially in sync regardless of which label a caller uses.
 * `delete` removes by pk (idempotent no-op if absent).
 */
export type Op = 'insert' | 'update' | 'delete'

/**
 * A change on a table or shape stream.
 * - insert/update: `row` is the full new row (includes the pk column).
 * - delete: `row` may be omitted; only `pk` is required.
 *
 * On a shape stream the same envelope is reused, where `op` reflects enter (insert),
 * leave (delete), or update.
 */
export interface ChangeEvent {
  op: Op
  pk: Value
  row?: Row
}

export function tableStreamPath(table: string): string {
  return `table/${table}`
}

export function shapeStreamPath(shapeId: string): string {
  return `shape/${shapeId}`
}
