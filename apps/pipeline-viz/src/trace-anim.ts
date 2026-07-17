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

/** One dot-travel / one rank of node flashes, at the default 1× speed. Deliberately unhurried —
 *  the point of the animation is READING the propagation, not signalling that something happened.
 *  The sidebar speed scrubber scales this per call via `eventDecor`'s `speed` param. */
export const BASE_STEP_MS = 750

/** The per-rank stage duration at a given speed multiplier (2× speed == half the step time). */
export function rankDelayMs(rank: number, speed: number): number {
  return (rank * BASE_STEP_MS) / speed
}

export interface NodeFlash {
  kind: FlashKind
  /** Stage offset: when this node's flash animation begins. */
  delayMs: number
  /** The rank this delay was computed from — lets a later replay re-derive delayMs at whatever
   *  speed is active THEN, instead of replaying frozen at the speed captured live. */
  rank: number
  /** This flash is a query-back-derived move-in/out landing on the outer Δ node — rendered with a
   *  distinct amber ring + "query-back" marker, since the change entered via a Postgres query-back,
   *  not this table's replication stream (so no source→change edge pulses into it). */
  derived?: boolean
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
  /** The whole event is a query-back-derived move-in/out (§ isDerivedEvent) — rendered dashed so
   *  the derived propagation reads distinctly from a table's own replication stream. */
  derived?: boolean
}

/** True when this event's causal root is a DIFFERENT table than the one it is "about" — i.e. a
 *  subquery membership move-in/out: the engine roots the hop path at the inner/membership table
 *  (`hops[0] = table:<inner>`) while `ev.table` names the OUTER table the moved rows belong to. The
 *  outer table's own replication stream did NOT change; the rows arrived via a pooled Postgres
 *  query-back. Same signal the delta peek uses to tag the outer Δ node "via query-back". */
export function isDerivedEvent(ev: TraceEvent): boolean {
  const first = ev.hops[0]?.node
  return !!first && first.startsWith('table:') && first !== `table:${ev.table}`
}

/** The table the change actually entered through for a derived event (the inner/membership table),
 *  or null for a normal same-table change. */
export function derivedVia(ev: TraceEvent): string | null {
  return isDerivedEvent(ev) ? ev.hops[0]!.node.slice('table:'.length) : null
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
 *  Nodes not present in the rendered graph are silently skipped (e.g. other selections). `speed`
 *  scales every stage's timing (2× runs twice as fast); defaults to the normal 1× pace. */
export function eventDecor(ev: TraceEvent, edges: Edge[], present: Set<string>, expand: HopExpand, speed = 1): Decor {
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
    nodes.set(id, { kind, delayMs: rankDelayMs(r, speed), rank: r })
  }

  const id = decorSeq++
  const derived = isDerivedEvent(ev)
  const stepMs = rankDelayMs(1, speed)
  const pulses = new Map<string, EdgePulse>()
  for (const e of edges) {
    // A `state` edge is a READ (a join/filter consulting an arrangement), not a data stream — its
    // endpoints share a trace hop and both flash, but a travelling delta dot along it reads as data
    // flowing from the arrangement, which it isn't. Flash the nodes, don't pulse the edge.
    if ((e.data as { kind?: string } | undefined)?.kind === 'state') continue
    if (nodes.has(e.source) && nodes.has(e.target)) {
      // The dot leaves when its source rank flashes and arrives at the target's rank.
      pulses.set(e.id, { id, color, label, delayMs: rankDelayMs(ranks.get(e.source) ?? 0, speed), durMs: stepMs, derived: derived || undefined })
    }
  }
  return { nodes, edges: pulses, id, totalMs: rankDelayMs(maxRank + 1, speed) }
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
