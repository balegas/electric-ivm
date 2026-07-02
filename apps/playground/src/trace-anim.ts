// Map a TraceEvent onto the currently rendered graph: which nodes flash (pass/drop/fold) and
// which edges pulse (a dot travels along them). Trace hops carry LOGICAL-view node ids
// (table:/family:/filter:/node:/shape:); the dbsp view expands each into its operator chain.

import type { Edge } from '@xyflow/react'

import type { TraceEvent } from '../shared/types.ts'

export type FlashKind = 'pass' | 'drop' | 'fold' | 'foreign'

export interface Decor {
  /** node id -> flash kind */
  nodes: Map<string, FlashKind>
  /** edge id -> pulse (color + weight label) */
  edges: Map<string, EdgePulse>
  /** monotonically increasing — keys the SVG animation restarts */
  id: number
}

export interface EdgePulse {
  color: string
  label: string
  foreign: boolean
}

let decorSeq = 1

const outcomeFlash: Record<string, FlashKind> = {
  passed: 'pass',
  routed: 'pass',
  folded: 'fold',
  dropped: 'drop',
}

/** Expand a logical hop node id to the ids used by the current view. */
function viewNodes(node: string, view: 'logical' | 'dbsp'): string[] {
  if (view === 'logical') return [node]
  const [kind, ...rest] = node.split(':')
  const id = rest.join(':')
  switch (kind) {
    case 'table':
      return [`src:${id}`, `d:${id}`]
    case 'filter':
      return [`f:${id}`, `m:${id}`]
    case 'family':
      // family:<table>:<cols> -> ix/pa/j keyed `${table}:${cols}` in build-dbsp
      return [`ix:${id}`, `j:${id}`]
    case 'node':
      return [`sf:${id}`, `si:${id}`, `dist:${id}`]
    case 'shape':
      return [`m:${id}`, `snk:${id}`, `fold:${id}`, `sj:${id}`]
    default:
      return [node]
  }
}

/** Compute the flash/pulse decoration for one trace event against the rendered edge list. Nodes
 *  not present in the rendered graph are silently skipped (e.g. other selections). */
export function eventDecor(ev: TraceEvent, view: 'logical' | 'dbsp', edges: Edge[], present: Set<string>): Decor {
  const nodes = new Map<string, FlashKind>()
  for (const hop of ev.hops) {
    const flash: FlashKind = ev.yours ? (outcomeFlash[hop.outcome] ?? 'pass') : 'foreign'
    for (const id of viewNodes(hop.node, view)) {
      if (!present.has(id)) continue
      // keep the strongest signal: drop > fold > pass > foreign
      const prev = nodes.get(id)
      const rank = (k: FlashKind) => ({ drop: 3, fold: 2, pass: 1, foreign: 0 })[k]
      if (prev === undefined || rank(flash) > rank(prev)) nodes.set(id, flash)
    }
  }

  const w = ev.delta.reduce((acc, d) => acc + d.w, 0)
  const label = ev.delta.length === 0 ? '' : ev.delta.length > 1 && w === 0 ? '±1' : w > 0 ? '+1' : '−1'
  const color = ev.yours ? (w > 0 ? '#16a34a' : w < 0 ? '#dc2626' : '#0ea5e9') : '#94a3b8'

  const pulses = new Map<string, EdgePulse>()
  for (const e of edges) {
    if (nodes.has(e.source) && nodes.has(e.target)) {
      pulses.set(e.id, { color, label, foreign: !ev.yours })
    }
  }
  return { nodes, edges: pulses, id: decorSeq++ }
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
