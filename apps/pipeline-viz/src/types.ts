// Mirrors the engine's `GET /graph` response (`EngineGraph` in apps/engine/src/engine.rs).

export type Predicate =
  | { col: string; op: 'eq' | 'neq' | 'lt' | 'lte' | 'gt' | 'gte' | 'like'; value: unknown }
  | { and: Predicate[] }
  | { or: Predicate[] }
  | { not: Predicate }
  | { col: string; in: SubqueryRef; negated?: boolean }

export interface SubqueryRef {
  table: string
  project: string
  where?: Predicate | null
}

export interface GraphShape {
  id: string
  table: string
  streamPath: string
  changesOnly: boolean
  where: Predicate | null
  /** The projected columns (SELECT-list); null = the full row (all columns). */
  columns: string[] | null
  /** Key columns iff routed via a shared equality family; null = standalone filter or subquery. */
  familyKey: string[] | null
  isSubquery: boolean
  /** Present iff this shape is a scalar aggregation (COUNT/SUM/AVG/MIN/MAX over `where`). */
  aggregate: { func: string; col: string | null } | null
}

export interface GraphNode {
  sig: string
  innerTable: string
  projCol: string
  distinctValues: number
  refcount: number
}

export interface GraphEdge {
  nodeSig: string
  dependentKind: 'shape' | 'node'
  dependentId: string
  connectingCol: string
  negated: boolean
}

export interface EngineGraph {
  tables: string[]
  shapes: GraphShape[]
  subqueryNodes: GraphNode[]
  subqueryEdges: GraphEdge[]
}

/** `GET /graph/node?sig=…` — the live inner-set index of a subquery node. */
export interface NodeIndex {
  sig: string
  distinctValues: number
  refcount: number
  values: { value: unknown; contributors: number }[]
  truncated: boolean
}

/** `GET /trace` (SSE) — one event per processed change envelope (crate::trace::TraceEvent).
 *  Hop node ids use the logical graph's namespace (`table:`, `family:`, `filter:`, `node:`,
 *  `shape:`), matching build-graph ids directly. */
export type HopOutcome = 'passed' | 'dropped' | 'routed' | 'folded'

export interface TraceHop {
  node: string
  outcome: HopOutcome
  key?: unknown[]
}

export interface TraceEvent {
  lsn?: string
  txid?: string
  table: string
  delta: { row: Record<string, unknown>; w: number }[]
  hops: TraceHop[]
  shapes: string[]
}

/** Graph-lifecycle event on the same `/trace` feed (crate::trace::GraphLifecycle): the pipeline's
 *  structure changed. Distinguished from data TraceEvents by the `type` field. */
export interface TraceLifecycle {
  type: 'shapeAdded' | 'shapeDropped'
  shape: string
  table?: string
}
