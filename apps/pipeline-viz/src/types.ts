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

/** One operator of the engine-emitted circuit decomposition (crate::engine::OpNode). `hop` is the
 *  trace-hop id whose outcomes animate this operator; `state` (when set) is the `GET /state` key
 *  whose live chips it shows — only the operator that actually holds the state carries one. */
export interface OpNode {
  id: string
  kind: 'source' | 'delta' | 'filter' | 'key' | 'arrange' | 'join' | 'distinct' | 'fold' | 'project' | 'sink'
  hop: string
  state: string | null
  label: string
}

/** A stream between two operators (crate::engine::OpEdge). */
export interface OpEdge {
  source: string
  target: string
  kind: 'flow' | 'state' | 'subquery'
  label: string | null
}

export interface EngineGraph {
  tables: string[]
  shapes: GraphShape[]
  subqueryNodes: GraphNode[]
  subqueryEdges: GraphEdge[]
  /** The exploded operator decomposition (engine-emitted) the circuit view renders. Absent on
   *  engines older than the decomposition — consumers must treat missing as empty. */
  operators?: OpNode[]
  opEdges?: OpEdge[]
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

/** Live state summary of one pipeline node (crate::engine::NodeStateSummary), keyed by the same
 *  node-id namespace as the graph and trace hops. Rendered as the state chips on every node. */
export type NodeStateSummary =
  | { kind: 'table'; processedOffset: string; envelopes: number }
  | { kind: 'filter'; emitted: number }
  | { kind: 'family'; keys: number; shapes: number }
  | { kind: 'shape'; emitted: number }
  | { kind: 'aggregate'; value: unknown; count: number; nnCount: number; multisetLen: number }
  | { kind: 'subqueryNode'; distinctValues: number; refcount: number }

/** `GET /state` — the full per-node state snapshot the store seeds from. */
export interface StateSnapshot {
  nodes: Record<string, NodeStateSummary>
}

/** Per-node state push on the `/trace` feed (crate::trace::StateEvent): the current summaries of
 *  every node the last batch touched. Applied incrementally over the `GET /state` seed. */
export interface TraceState {
  type: 'state'
  nodes: Record<string, NodeStateSummary>
}

/** Any message on the `/trace` SSE feed. Data events carry no `type` field. */
export type TraceMessage = TraceEvent | TraceLifecycle | TraceState

/** `GET /state/node?id=family:…` — a family router's full routing-index contents. */
export interface FamilyDump {
  kind: 'family'
  node: string
  keyCols: string[]
  keys: number
  shapes: number
  entries: { key: unknown[]; shapes: string[] }[]
  truncated: boolean
}

/** `GET /state/node?id=shape:…` for an aggregation — the fold internals. */
export interface AggregateDump {
  kind: 'aggregate'
  node: string
  func: string
  value: unknown
  count: number
  nnCount: number
  multisetLen: number
  multiset: { value: unknown; weight: number }[]
  truncated: boolean
}
