// The exploded dbsp-circuit view. Unlike the logical view (which renders `/graph`'s node set
// directly), this renders the engine-emitted operator decomposition (`operators` / `opEdges`):
// every box is an operator the engine declares it executes, bound by the engine to its trace-hop
// id (for animation) and its state-summary id (for live chips). Nothing here is reconstructed
// client-side — this file only lays out what the engine reports.

import type { Edge, Node } from '@xyflow/react'

import { layout, type BuildOpts, type NodeKind, type RawEdge, type RawNode } from './build-graph'
import type { EngineGraph, OpNode } from './types'

const OP_KIND: Record<OpNode['kind'], NodeKind> = {
  source: 'op-source',
  delta: 'op-delta',
  filter: 'op-filter',
  key: 'op-key',
  arrange: 'op-arrange',
  join: 'op-join',
  distinct: 'op-distinct',
  fold: 'op-fold',
  project: 'op-project',
  sink: 'op-sink',
}

function buildFull(g: EngineGraph): { nodes: Map<string, RawNode>; edges: RawEdge[] } {
  const nodes = new Map<string, RawNode>()
  // An engine older than the decomposition omits operators/opEdges — render an empty circuit
  // rather than crashing (the logical view still works against such an engine).
  for (const o of g.operators ?? []) {
    nodes.set(o.id, {
      id: o.id,
      data: {
        kind: OP_KIND[o.kind],
        label: o.label,
        ...(o.state ? { stateId: o.state } : null),
        ref: { kind: 'op', opKind: OP_KIND[o.kind], hop: o.hop, label: o.label },
      },
    })
  }
  const edges: RawEdge[] = (g.opEdges ?? []).map((e) => ({
    id: `${e.source}~>${e.target}~${e.label ?? ''}`,
    source: e.source,
    target: e.target,
    label: e.label ?? undefined,
    kind: e.kind,
  }))
  addArrangements(g, nodes, edges)
  return { nodes, edges }
}

/** The compiled dbsp arrangement pipeline (present iff the engine runs with ELECTRIC_IVM_DBSP=1):
 *  static infrastructure — one input per table, one map_index→integrate_trace pipeline per index,
 *  one map_index(group)→weighted_count pipeline per counted table — rendered permanently. Two
 *  consumer-edge kinds hang off it: dashed LOOKUP edges to subquery dependents whose flip
 *  re-derivations read an index, and solid animated SERVING edges to circuit-served shapes and
 *  aggregates whose data comes from the circuit itself. Ids come from the engine (`arr:input:…` /
 *  `arr:index:…` / `arr:counts:…`), stable across snapshots, so sticky layout keeps the lane
 *  parked while shapes come and go. */
function addArrangements(g: EngineGraph, nodes: Map<string, RawNode>, edges: RawEdge[]) {
  const arr = g.arrangements
  if (!arr) return
  for (const inp of arr.inputs) {
    nodes.set(inp.id, {
      id: inp.id,
      data: {
        kind: 'arr-input',
        label: inp.table,
        sub: inp.seeded ? 'seeded' : 'seeding…',
        ref: { kind: 'op', opKind: 'arr-input', hop: inp.id, label: inp.table },
      },
    })
  }
  for (const ix of arr.indexes) {
    const label = `map_index(${ix.cols.join(', ')})`
    nodes.set(ix.id, {
      id: ix.id,
      data: {
        kind: 'arr-index',
        label,
        sub: `integrate_trace · ${ix.seeded ? 'seeded' : 'seeding…'}`,
        ref: { kind: 'op', opKind: 'arr-index', hop: ix.id, label },
      },
    })
    edges.push({ id: `${ix.input}~>${ix.id}~`, source: ix.input, target: ix.id, kind: 'flow' })
  }
  for (const ct of arr.counts ?? []) {
    // A counts pipeline COMPUTES (a maintained weighted_count per group), where an index REMEMBERS
    // (rows) — its card carries the reduction as the headline, the grouping step as the sub line.
    const label = `weighted_count(${ct.groupCols.join(', ')})`
    nodes.set(ct.id, {
      id: ct.id,
      data: {
        kind: 'arr-counts',
        label,
        sub: `map_index(${ct.groupCols.join(', ')}) · ${ct.seeded ? 'seeded' : 'seeding…'}`,
        ref: { kind: 'op', opKind: 'arr-counts', hop: ct.id, label },
      },
    })
    edges.push({ id: `${ct.input}~>${ct.id}~`, source: ct.input, target: ct.id, kind: 'flow' })
  }
  for (const c of arr.consumers) {
    if (c.dependentKind === 'circuit-shape' || c.dependentKind === 'circuit-agg') {
      // A SERVING edge: the dependent's data comes FROM the circuit (seeded there, maintained
      // there) — not an occasional read. It lands on the dependent's own operator: the fold of a
      // counts-served aggregate, the membership semijoin (or standalone σ) of a served shape.
      const candidates =
        c.dependentKind === 'circuit-agg'
          ? [`fold:${c.dependentId}`, `snk:${c.dependentId}`]
          : [`sj:${c.dependentId}`, `sigma:${c.dependentId}`, `snk:${c.dependentId}`]
      const target = candidates.find((t) => nodes.has(t))
      if (!target || !nodes.has(c.index)) continue
      edges.push({
        id: `${c.index}~>${target}~serves`,
        source: c.index,
        target,
        label: c.connectingCol ? `serves · ${c.connectingCol}` : 'serves',
        kind: 'serve',
      })
      continue
    }
    // A LOOKUP edge feeds the dependent's own operator — a shape's membership semijoin, or a
    // nested node's inner filter: the exact operators whose flip re-derivations the index serves.
    const target = c.dependentKind === 'shape' ? `sj:${c.dependentId}` : `sqf:${c.dependentId}`
    if (!nodes.has(target)) continue
    edges.push({
      id: `${c.index}~>${target}~lookup`,
      source: c.index,
      target,
      label: `lookup · ${c.connectingCol}`,
      kind: 'state',
    })
  }
}

/** Keep only the upstream closure of the selected shapes' sinks (`snk:<id>`). */
function restrictToSelection(
  full: { nodes: Map<string, RawNode>; edges: RawEdge[] },
  selectedShapeIds: Set<string>,
): { nodes: Map<string, RawNode>; edges: RawEdge[] } {
  const rev = new Map<string, string[]>()
  for (const e of full.edges) {
    if (!rev.has(e.target)) rev.set(e.target, [])
    rev.get(e.target)!.push(e.source)
  }
  const keep = new Set<string>()
  const queue: string[] = []
  for (const id of selectedShapeIds) {
    const nid = `snk:${id}`
    if (full.nodes.has(nid)) {
      keep.add(nid)
      queue.push(nid)
    }
  }
  while (queue.length) {
    const cur = queue.shift()!
    for (const up of rev.get(cur) ?? []) {
      if (!keep.has(up)) {
        keep.add(up)
        queue.push(up)
      }
    }
  }
  const nodes = new Map([...full.nodes].filter(([id]) => keep.has(id)))
  const edges = full.edges.filter((e) => keep.has(e.source) && keep.has(e.target))
  return { nodes, edges }
}

/** Build laid-out React Flow nodes+edges for the circuit view. Same selection/focus semantics as
 *  the logical view. */
export function buildCircuit(
  g: EngineGraph,
  selection: 'all' | Set<string>,
  focus: string | null = null,
  opts?: BuildOpts,
): { nodes: Node[]; edges: Edge[] } {
  const full = buildFull(g)
  const restricted = selection === 'all' ? full : restrictToSelection(full, selection)
  return layout(restricted, focus, opts)
}

/** hop id → operator ids. The engine stamps each operator with its trace-hop id, so expanding a
 *  trace hop (or a graph-diff id) to circuit nodes is a lookup, not a guess. */
export function hopIndex(g: EngineGraph): Map<string, string[]> {
  const m = new Map<string, string[]>()
  for (const o of g.operators ?? []) {
    if (!m.has(o.hop)) m.set(o.hop, [])
    m.get(o.hop)!.push(o.id)
  }
  return m
}
