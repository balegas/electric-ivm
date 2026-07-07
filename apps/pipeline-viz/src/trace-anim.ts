// Map a TraceEvent onto the rendered graph: which nodes flash (pass/drop/fold) and which edges
// pulse (a dot travels along them). Trace hops carry the engine's node ids (table:/family:/
// filter:/node:/shape:). In the logical view these ARE the rendered ids (identity mapping); in
// the circuit view each hop expands to the operator ids the ENGINE stamped with that hop
// (`OpNode.hop` via build-circuit's hopIndex) — declared, not guessed.
//
// The decoration is STAGED: the change propagates through the pipeline sequentially, the way the
// engine actually processes it — the source flashes first, a dot travels each edge, and each
// downstream node flashes only when the dot arrives. Stages come from the longest path over the
// traced sub-DAG (depth 0 = the source), one STEP_MS per rank.

import type { Edge } from '@xyflow/react'

import type { TraceEvent } from './types'

export type FlashKind = 'pass' | 'drop' | 'fold'

/** One dot-travel / one rank of node flashes. */
export const STEP_MS = 420

export interface NodeFlash {
  kind: FlashKind
  /** Stage offset: when this node's flash animation begins. */
  delayMs: number
}

export interface Decor {
  /** node id -> flash */
  nodes: Map<string, NodeFlash>
  /** edge id -> pulse (color + weight label + stage timing) */
  edges: Map<string, EdgePulse>
  /** monotonically increasing (diagnostics; pulses carry their own id) */
  id: number
  /** Total staged duration — the caller keeps the decor alive at least this long. */
  totalMs: number
}

export interface EdgePulse {
  /** Id of the EVENT that created this pulse — keys the SVG animation, so merging a later event
   *  into the decor never restarts other events' running dots (that read as a double render). */
  id: number
  color: string
  label: string
  /** Stage offset: when the dot starts travelling this edge (source node's rank). */
  delayMs: number
  /** Dot travel time along this edge. */
  durMs: number
}

let decorSeq = 1

const outcomeFlash: Record<string, FlashKind> = {
  passed: 'pass',
  routed: 'pass',
  folded: 'fold',
  dropped: 'drop',
}

/** Expand a hop id to rendered node ids: identity for the logical view, or the engine-declared
 *  operator group for the circuit view. */
export type HopExpand = (hop: string) => string[]

/** Longest-path rank of every flashed node over the traced sub-DAG (edges whose both endpoints
 *  flashed). Roots (no traced in-edge — the sources) are rank 0. The pipeline graph is acyclic;
 *  a defensive iteration cap keeps a malformed input from spinning. */
function stageRanks(flashed: Set<string>, edges: Edge[]): Map<string, number> {
  const out = new Map<string, number>()
  const adj: Array<[string, string]> = []
  for (const e of edges) {
    if (flashed.has(e.source) && flashed.has(e.target)) adj.push([e.source, e.target])
  }
  for (const id of flashed) out.set(id, 0)
  // Bellman-Ford-style relaxation to the longest path; ranks are tiny (pipeline depth ≤ ~6).
  for (let pass = 0; pass < 12; pass++) {
    let changed = false
    for (const [u, v] of adj) {
      const d = out.get(u)! + 1
      if (d > out.get(v)! && d < 24) {
        out.set(v, d)
        changed = true
      }
    }
    if (!changed) break
  }
  return out
}

/** Compute the staged flash/pulse decoration for one trace event against the rendered edge list.
 *  Nodes not present in the rendered graph are silently skipped (e.g. other selections). */
export function eventDecor(ev: TraceEvent, edges: Edge[], present: Set<string>, expand: HopExpand): Decor {
  const kinds = new Map<string, FlashKind>()
  for (const hop of ev.hops) {
    const flash: FlashKind = outcomeFlash[hop.outcome] ?? 'pass'
    for (const id of expand(hop.node)) {
      if (!present.has(id)) continue
      // keep the strongest signal: drop > fold > pass
      const prev = kinds.get(id)
      const rank = (k: FlashKind) => ({ drop: 2, fold: 1, pass: 0 })[k]
      if (prev === undefined || rank(flash) > rank(prev)) kinds.set(id, flash)
    }
  }

  const w = ev.delta.reduce((acc, d) => acc + d.w, 0)
  const label = ev.delta.length === 0 ? '' : ev.delta.length > 1 && w === 0 ? '±1' : w > 0 ? '+1' : '−1'
  const color = w > 0 ? '#16a34a' : w < 0 ? '#dc2626' : '#0ea5e9'

  const ranks = stageRanks(new Set(kinds.keys()), edges)
  const nodes = new Map<string, NodeFlash>()
  let maxRank = 0
  for (const [id, kind] of kinds) {
    const r = ranks.get(id) ?? 0
    maxRank = Math.max(maxRank, r)
    nodes.set(id, { kind, delayMs: r * STEP_MS })
  }

  const id = decorSeq++
  const pulses = new Map<string, EdgePulse>()
  for (const e of edges) {
    if (nodes.has(e.source) && nodes.has(e.target)) {
      // The dot leaves when its source rank flashes and arrives at the target's rank.
      pulses.set(e.id, { id, color, label, delayMs: (ranks.get(e.source) ?? 0) * STEP_MS, durMs: STEP_MS })
    }
  }
  return { nodes, edges: pulses, id, totalMs: (maxRank + 1) * STEP_MS }
}

/** Merge b over a (later events win per node/edge). */
export function mergeDecor(a: Decor | null, b: Decor): Decor {
  if (!a) return b
  const nodes = new Map(a.nodes)
  for (const [k, v] of b.nodes) nodes.set(k, v)
  const edges = new Map(a.edges)
  for (const [k, v] of b.edges) edges.set(k, v)
  return { nodes, edges, id: b.id, totalMs: Math.max(a.totalMs, b.totalMs) }
}
