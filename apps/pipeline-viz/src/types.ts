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
  /** Present iff this shape is circuit-served (seeded + maintained by the dbsp pipeline).
   *  `label` says which cohort form serves it — `all` / `static:<col>` / `dynamic:<col>` /
   *  `counts`; `col` is the serving arrangement column's index; `counts` marks a counts-served
   *  aggregate. Omitted by the engine when the shape is not circuit-served. */
  circuit?: { label: string; col?: number; counts?: boolean }
  /** Retention lifecycle: a dormant shape keeps its stream + record but holds no routing state. */
  state: 'active' | 'deactivating' | 'dormant' | 'reactivating' | null
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

/** The compiled dbsp arrangement pipeline (crate::engine::ArrangementGraph): static
 *  infrastructure built once at boot — one input per table, one map_index→integrate_trace
 *  pipeline per index — plus its live consumers and the layer's lookup counters. Present
 *  whenever the always-on circuit is running (Postgres mode). */
export interface ArrangementGraph {
  /** Lookups served from arrangement snapshots. */
  served: number
  /** Lookups that fell back to Postgres (missing index, or table not seeded yet). */
  fallback: number
  inputs: { id: string; table: string; seeded: boolean }[]
  indexes: { id: string; input: string; table: string; cols: string[]; seeded: boolean }[]
  /** Counts pipelines (`map_index(group cols) → weighted_count`), one per counted table.
   *  Omitted by the engine when no table has one. */
  counts?: { id: string; input: string; table: string; groupCols: string[]; seeded: boolean }[]
  /** `shape`/`node` are lookup consumers (a subquery dependent whose flip re-derivations read the
   *  index). `circuit-shape`/`circuit-agg` are SERVING consumers — the dependent shape's data
   *  comes from the circuit (its `index` is the serving index, counts pipeline, or table input). */
  consumers: {
    index: string
    dependentKind: 'shape' | 'node' | 'circuit-shape' | 'circuit-agg'
    dependentId: string
    connectingCol: string
  }[]
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
  /** The compiled dbsp arrangement pipeline; absent when the layer is off. */
  arrangements?: ArrangementGraph
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
  type: 'shapeAdded' | 'shapeDropped' | 'shapeDormant' | 'shapeReactivated'
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
