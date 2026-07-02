// The playground's client ↔ server contract. The browser talks ONLY to the playground server;
// everything engine-facing (shape registration, /graph, /trace, rows) is proxied and scoped to a
// workspace here. Workspace scoping is SILENT in the UI (v2): the server always applies it, the
// client strips it from display unless "under the hood" is on.

import type { EngineGraph, Predicate } from '@viz/types'

// ── Domain: a tiny issue tracker ─────────────────────────────────────────────────────────────

export const STATUSES = ['todo', 'in_progress', 'done'] as const
export type Status = (typeof STATUSES)[number]
export const TEAMS = ['web', 'mobile', 'infra'] as const

export interface Project {
  id: number
  workspace_id: string
  name: string
  team: string
}

export interface Issue {
  id: number
  workspace_id: string
  project_id: number
  title: string
  status: Status
  priority: number // 1 (low) … 4 (urgent)
}

/** Grid edits — each maps to fixed parameterized SQL, always scoped to the workspace. */
export type Verb =
  | { verb: 'add_issue'; projectId: number }
  | { verb: 'set_status'; issueId: number; status: Status }
  | { verb: 'set_priority'; issueId: number; priority: number }
  | { verb: 'delete_issue'; issueId: number }
  | { verb: 'add_project' }
  | { verb: 'set_team'; projectId: number; team: string }

// ── Workspaces (server-side concept only; the UI does not surface it) ────────────────────────

export interface WorkspaceRef {
  id: string
  /** Bumped when the operator wipes the server; a mismatch tells the client to re-provision. */
  epoch: number
}

export interface WorkspaceState {
  workspace: WorkspaceRef
  projects: Project[]
  issues: Issue[]
  shapes: PlaygroundShape[]
}

// ── Shapes ────────────────────────────────────────────────────────────────────────────────────

/** What the composer can express. The server appends `AND workspace_id = $ws` on top. */
export interface ShapeSpec {
  table: 'issues' | 'projects'
  where: { col: string; op: 'eq' | 'neq' | 'lt' | 'lte' | 'gt' | 'gte'; value: unknown }[]
  subquery?: {
    col: string
    negated?: boolean
    inner: { table: string; project: string; where: { col: string; op: 'eq'; value: unknown }[] }
  }
  aggregate?: { func: 'count' | 'sum' | 'avg' | 'min' | 'max'; col: string | null }
}

export interface PlaygroundShape {
  /** Engine shape id (`s3`, …) — the id used in /graph, trace events, and rows lookups. */
  id: string
  workspaceId: string
  scene: number | null
  label: string
  spec: ShapeSpec
  /** The full predicate as registered (workspace conjunct included — display scrubs it). */
  where: Predicate
}

export interface SceneShapeResult {
  scene: number
  shapes: PlaygroundShape[]
}

// ── Trace (SSE) ───────────────────────────────────────────────────────────────────────────────

export type HopOutcome = 'passed' | 'dropped' | 'routed' | 'folded'

export interface TraceHop {
  /** Node id in the LOGICAL graph's namespace: `table:issues`, `family:issues:a,b`, `filter:s7`,
   *  `node:<sig>`, `shape:s7` — so the UI can animate without translation. */
  node: string
  outcome: HopOutcome
  key?: unknown[] | undefined
}

export interface TraceEvent {
  lsn?: string
  txid?: string
  table: string
  delta: { row: Record<string, unknown>; w: 1 | -1 }[]
  hops: TraceHop[]
  shapes: string[]
  yours: boolean
}

// ── HTTP surface (unchanged from v1 apart from the domain payloads) ──────────────────────────

export interface GraphResponse {
  graph: EngineGraph
  /** Shape ids belonging to the caller's workspace (drives selection + result cards). */
  mine: string[]
}

/** Legacy alias kept for the server tests' imports. */
export type DeviceRole = 'custom'
