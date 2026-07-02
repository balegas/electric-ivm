// The playground's client ↔ server contract. The browser talks ONLY to the playground server;
// everything engine-facing (shape registration, /graph, /trace, rows) is proxied and scoped to a
// workspace here.

import type { EngineGraph, Predicate } from '@viz/types'

// ── Domain ────────────────────────────────────────────────────────────────────────────────────

export const ORDER_STATUSES = ['new', 'cooking', 'riding', 'delivered', 'cancelled'] as const
export type OrderStatus = (typeof ORDER_STATUSES)[number]

export interface Restaurant {
  id: number
  workspace_id: string
  name: string
  emoji: string
  city: string
}

export interface Order {
  id: number
  workspace_id: string
  restaurant_id: number
  status: OrderStatus
  dish: string
  total: number
}

/** One-click domain writes. Each maps to fixed parameterized SQL, always scoped to the workspace. */
export type Verb =
  | { verb: 'place_order'; restaurantId: number }
  | { verb: 'start_cooking'; orderId: number }
  | { verb: 'pickup'; orderId: number }
  | { verb: 'deliver'; orderId: number }
  | { verb: 'cancel'; orderId: number }
  | { verb: 'move_restaurant'; restaurantId: number; city: string }

// ── Workspaces ────────────────────────────────────────────────────────────────────────────────

export interface WorkspaceRef {
  id: string
  /** Bumped when the operator wipes the server; a mismatch tells the client to re-provision. */
  epoch: number
}

export interface WorkspaceState {
  workspace: WorkspaceRef
  restaurants: Restaurant[]
  orders: Order[]
  shapes: PlaygroundShape[]
}

// ── Shapes ────────────────────────────────────────────────────────────────────────────────────

/** What the guided builder can express. The server appends `AND workspace_id = $ws` on top. */
export interface ShapeSpec {
  table: 'orders' | 'restaurants'
  /** Simple conjuncts, e.g. [{col:'status',op:'eq',value:'cooking'}]. */
  where: { col: string; op: 'eq' | 'neq' | 'lt' | 'lte' | 'gt' | 'gte'; value: unknown }[]
  /** Optional `col IN (SELECT project FROM table WHERE …)` clause. */
  subquery?: {
    col: string
    negated?: boolean
    inner: { table: string; project: string; where: { col: string; op: 'eq'; value: unknown }[] }
  }
  /** Optional scalar aggregation over the predicate. */
  aggregate?: { func: 'count' | 'sum' | 'avg' | 'min' | 'max'; col: string | null }
}

/** How a shape's subscriber is rendered on the right-hand side. */
export type DeviceRole = 'orders' | 'kitchen' | 'rider' | 'customer' | 'dashboard' | 'custom'

export interface PlaygroundShape {
  /** Engine shape id (`s3`, …) — the id used in /graph, trace events, and rows lookups. */
  id: string
  workspaceId: string
  /** Which scene provisioned it, or null for user-built shapes. */
  scene: number | null
  role: DeviceRole
  label: string
  spec: ShapeSpec
  /** The full predicate as registered (including the workspace conjunct) for honest display. */
  where: Predicate
}

// ── Scenes ────────────────────────────────────────────────────────────────────────────────────

export interface SceneShapeResult {
  scene: number
  shapes: PlaygroundShape[]
}

// ── Trace (SSE) ───────────────────────────────────────────────────────────────────────────────

export type HopOutcome = 'passed' | 'dropped' | 'routed' | 'folded'

export interface TraceHop {
  /** Node id in the LOGICAL graph's namespace: `table:orders`, `family:orders:a,b`, `filter:s7`,
   *  `node:<sig>`, `shape:s7` — so the UI can animate without translation. */
  node: string
  outcome: HopOutcome
  /** Routing key values for `routed` hops (JSON array). */
  key?: unknown[] | undefined
}

export interface TraceEvent {
  lsn?: string
  txid?: string
  table: string
  /** Weighted delta rows, e.g. an update = [(old,-1),(new,+1)]. Rows may be truncated server-side. */
  delta: { row: Record<string, unknown>; w: 1 | -1 }[]
  hops: TraceHop[]
  /** Shape ids whose logs got appends. */
  shapes: string[]
  /** True iff any destination shape (or the source rows) belongs to the caller's workspace.
   *  Foreign events are stripped to shared-node hops and rowless deltas — ambient pulses only. */
  yours: boolean
}

// ── HTTP surface ──────────────────────────────────────────────────────────────────────────────
//
//   POST   /api/workspace                { existingId? }            → WorkspaceState (idempotent)
//   GET    /api/workspace/:id            —                          → WorkspaceState | 404
//   POST   /api/action                   { workspace } & Verb       → { ok, order? }
//   POST   /api/scene                    { workspace, scene }       → SceneShapeResult (idempotent)
//   POST   /api/shape                    { workspace, spec, label?, role? } → PlaygroundShape
//   DELETE /api/shape/:id?workspace=…    —                          → { ok }
//   GET    /api/graph?workspace=…        —                          → { graph: EngineGraph, mine: string[] }
//   GET    /api/shapes/:id/rows?workspace=…&limit=…                 → engine rows payload (proxied)
//   GET    /api/trace?workspace=…        SSE of TraceEvent

export interface GraphResponse {
  graph: EngineGraph
  /** Shape ids belonging to the caller's workspace (drives selection + device cards). */
  mine: string[]
}
