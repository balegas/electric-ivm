// Cross-language contract for electric-circuits.
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

/**
 * SQL null test: `col IS NULL` (`isNull: true`) / `col IS NOT NULL` (`isNull: false`). A separate
 * leaf because it is the one predicate that is TRUE on a NULL cell — no comparison can express it
 * under three-valued logic. Two-valued (never UNKNOWN), so it composes soundly under `not`.
 */
export interface IsNullPredicate {
  col: string
  isNull: boolean
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
 * Reference to an inner subquery: the set of `project` values over `table`'s rows matching `where`.
 * `where` may itself contain `InSubqueryPredicate` leaves (nested subqueries). Single column only —
 * composite `(a,b) IN (…)` is out of scope.
 */
export interface SubqueryRef {
  table: string
  project: string
  where?: Predicate
}

/**
 * `outer.col IN (SELECT project FROM table WHERE where)` (or `NOT IN` when `negated`). Outer membership
 * is `row[col] ∈ innerSet` / `∉`; the engine maintains the inner set incrementally (shared across shapes
 * referencing the same subquery). `col` references the *outer* table; `in.project`/`in.where` the inner.
 */
export interface InSubqueryPredicate {
  col: string
  in: SubqueryRef
  negated?: boolean
}

/**
 * A restricted boolean predicate over a single table's columns, plus single-column `IN`/`NOT IN`
 * subqueries. M1 used only `eq` leaves; M2 added the other ops plus and/or/not; subqueries add `in`.
 */
export type Predicate =
  | LeafPredicate
  | IsNullPredicate
  | AndPredicate
  | OrPredicate
  | NotPredicate
  | InSubqueryPredicate

export function isLeaf(p: Predicate): p is LeafPredicate {
  return 'col' in p && 'op' in p
}
export function isIsNull(p: Predicate): p is IsNullPredicate {
  return 'col' in p && 'isNull' in p
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
export function isInSubquery(p: Predicate): p is InSubqueryPredicate {
  return 'in' in p && 'col' in p
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

// --- Subset queries ----------------------------------------------------------
// A **subset query** is the deliberate opposite of a shape. A shape is *materialized and live-tailed*
// (the engine backfills it, stores it as a durable stream, and maintains the whole matching set). A
// subset query is *ephemeral and one-shot*: the engine runs a single `SELECT … WHERE … ORDER BY …
// LIMIT … OFFSET …` straight against Postgres and returns the rows + the snapshot LSN. Nothing is
// stored server-side, so paging/ranges never become live state — which is exactly why a change can
// never fan out across ranges (ranges are simply never live-tailed). This mirrors Electric's
// Shape-vs-Subset split. To keep a subset *live*, follow the table's tail and re-check view membership
// (see `SubsetDef.where`), rather than materializing a per-page shape.

/** Order key for a subset query. The engine appends the primary key as a tiebreaker (total order). */
export interface SubsetOrderBy {
  col: string
  desc?: boolean
}

/**
 * A subset query: one table + an optional `where`, projected `columns`, and an ordered window
 * (`orderBy` + `limit` + `offset`). Ephemeral and non-live — run it again (e.g. with a moved cursor in
 * `where`, or a higher `offset`) to page. Compare with [`ShapeDef`], which is materialized + live.
 */
export interface SubsetDef {
  table: string
  /** Filter over the table's columns. Omitted = all rows. */
  where?: Predicate
  /** Output projection (pk always included). Omitted = the full row. */
  columns?: string[]
  /** Order for the window; required when `limit`/`offset` are set (for a deterministic page). */
  orderBy?: SubsetOrderBy
  /** Max rows to return (the page size). */
  limit?: number
  /** Rows to skip before the page (keyset cursors via `where` are preferred over large offsets). */
  offset?: number
}

/** Result of a subset query: the page rows, plus the Postgres snapshot LSN they were read at (so a
 * live tail can be followed from exactly that point with no gap or duplicate). */
export interface SubsetResult {
  rows: Row[]
  /** `pg_current_wal_lsn()` at the read snapshot. */
  lsn: string
}

/** Scalar aggregation functions (an electric-circuits extension — not part of the Electric protocol). */
export type AggFn = 'count' | 'sum' | 'avg' | 'min' | 'max'

/** A scalar aggregation over a filtered set, maintained incrementally by the engine and streamed as a
 * single value that updates as rows enter/leave the predicate. `col` is required for all but `count`. */
export interface AggregateDef {
  table: string
  /** Filter over the table's columns (no subqueries). Omitted = all rows. */
  where?: Predicate
  fn: AggFn
  /** The column to aggregate — required for sum/avg/min/max, ignored for count. */
  col?: string
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

/** The single ordered change log every table write rides on (replication and library mode). */
export const CHANGES_STREAM = 'changes'


export function shapeStreamPath(shapeId: string): string {
  return `shape/${shapeId}`
}
