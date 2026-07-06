// Map a TraceEvent onto the rendered graph: which nodes flash (pass/drop/fold) and which edges
// pulse (a dot travels along them). Trace hops carry the engine's node ids (table:/family:/
// filter:/node:/shape:). In the logical view these ARE the rendered ids (identity mapping); in
// the circuit view each hop expands to the operator ids the ENGINE stamped with that hop
// (`OpNode.hop` via build-circuit's hopIndex) — declared, not guessed.

import type { Edge } from '@xyflow/react'

import type { TraceEvent } from './types'

export type FlashKind = 'pass' | 'drop' | 'fold'

export interface Decor {
  /** node id -> flash kind */
  nodes: Map<string, FlashKind>
  /** edge id -> pulse (color + weight label) */
  edges: Map<string, EdgePulse>
  /** monotonically increasing (diagnostics; pulses carry their own id) */
  id: number
}

export interface EdgePulse {
  /** Id of the EVENT that created this pulse — keys the SVG animation, so merging a later event
   *  into the decor never restarts other events' running dots (that read as a double render). */
  id: number
  color: string
  label: string
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

/** Compute the flash/pulse decoration for one trace event against the rendered edge list. Nodes
 *  not present in the rendered graph are silently skipped (e.g. other selections). */
export function eventDecor(ev: TraceEvent, edges: Edge[], present: Set<string>, expand: HopExpand): Decor {
  const nodes = new Map<string, FlashKind>()
  for (const hop of ev.hops) {
    const flash: FlashKind = outcomeFlash[hop.outcome] ?? 'pass'
    for (const id of expand(hop.node)) {
      if (!present.has(id)) continue
      // keep the strongest signal: drop > fold > pass
      const prev = nodes.get(id)
      const rank = (k: FlashKind) => ({ drop: 2, fold: 1, pass: 0 })[k]
      if (prev === undefined || rank(flash) > rank(prev)) nodes.set(id, flash)
    }
  }

  const w = ev.delta.reduce((acc, d) => acc + d.w, 0)
  const label = ev.delta.length === 0 ? '' : ev.delta.length > 1 && w === 0 ? '±1' : w > 0 ? '+1' : '−1'
  const color = w > 0 ? '#16a34a' : w < 0 ? '#dc2626' : '#0ea5e9'

  const id = decorSeq++
  const pulses = new Map<string, EdgePulse>()
  for (const e of edges) {
    if (nodes.has(e.source) && nodes.has(e.target)) {
      pulses.set(e.id, { id, color, label })
    }
  }
  return { nodes, edges: pulses, id }
}

/** Merge b over a (later events win per node/edge). */
export function mergeDecor(a: Decor | null, b: Decor): Decor {
  if (!a) return b
  const nodes = new Map(a.nodes)
  for (const [k, v] of b.nodes) nodes.set(k, v)
  const edges = new Map(a.edges)
  for (const [k, v] of b.edges) edges.set(k, v)
  return { nodes, edges, id: b.id }
}
