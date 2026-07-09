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

/** The table an operator acts over, when naming it aids reading — and only then. Derived from the
 *  engine's stable id/hop conventions (`d:<table>`, `sj:<shapeId>`, route join hop `family:<table>:…`,
 *  `dist:<sig>`). Returns null for operators whose table is either obvious (the source, already named)
 *  or one adjacent edge away (per-shape σ / π / sink), where a label would just add clutter. */
function opTable(o: OpNode, shapeTable: Map<string, string>, innerTable: Map<string, string>): string | null {
  if (o.kind === 'delta') return o.id.startsWith('d:') ? o.id.slice(2) : null
  if (o.kind === 'join') {
    if (o.id.startsWith('sj:')) return shapeTable.get(o.id.slice(3)) ?? null // membership: the outer (delta) table
    if (o.hop.startsWith('family:')) return o.hop.slice('family:'.length).split(':')[0] || null // route join
    return null
  }
  if (o.kind === 'distinct') return o.id.startsWith('dist:') ? (innerTable.get(o.id.slice(5)) ?? null) : null
  return null
}

function buildFull(g: EngineGraph): { nodes: Map<string, RawNode>; edges: RawEdge[] } {
  const nodes = new Map<string, RawNode>()
  const shapeTable = new Map((g.shapes ?? []).map((s) => [s.id, s.table]))
  const innerTable = new Map((g.subqueryNodes ?? []).map((n) => [n.sig, n.innerTable]))
  // An engine older than the decomposition omits operators/opEdges — render an empty circuit
  // rather than crashing (the logical view still works against such an engine).
  for (const o of g.operators ?? []) {
    const t = opTable(o, shapeTable, innerTable)
    // Delta streams are one-per-table and otherwise identical ("Δ change"); name them by the table
    // like the source above them. Joins and the subquery distinct append the table to their label.
    const label = t === null ? o.label : o.kind === 'delta' ? t : `${o.label} · ${t}`
    nodes.set(o.id, {
      id: o.id,
      data: {
        kind: OP_KIND[o.kind],
        label,
        ...(o.state ? { stateId: o.state } : null),
        ref: { kind: 'op', opKind: OP_KIND[o.kind], hop: o.hop, label },
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

/** The compiled dbsp arrangement pipeline (always present once the always-on circuit is running):
 *  static infrastructure — one input per table, one map_index→integrate_trace arrangement per
 *  index, one weighted_count pipeline per counted table. Drawn as a separate lane it swamps the
 *  canvas (three-plus nodes per table), so instead it is FOLDED onto each table's SOURCE node: the
 *  source carries an "indexed" treatment + an index/counts count badge, and the detail panel
 *  expands the full list. The two consumer-edge kinds still hang off it, re-anchored to the source
 *  node: dashed LOOKUP edges to subquery dependents whose flip re-derivations read an index, and
 *  solid animated SERVING edges to circuit-served shapes and aggregates whose data comes from the
 *  circuit itself. Every arrangement id maps back to `src:<table>` so those edges land there. */
function addArrangements(g: EngineGraph, nodes: Map<string, RawNode>, edges: RawEdge[]) {
  const arr = g.arrangements
  if (!arr) return

  // Map every arrangement id (input / index / counts) back to its table, and tally per table how
  // many indexes and counts pipelines it carries and whether they are all seeded.
  const idTable = new Map<string, string>()
  const fold = new Map<string, { indexes: number; counts: number; seeded: boolean }>()
  const bump = (table: string, seeded: boolean, isCount: boolean) => {
    const cur = fold.get(table) ?? { indexes: 0, counts: 0, seeded: true }
    if (isCount) cur.counts += 1
    else cur.indexes += 1
    cur.seeded = cur.seeded && seeded
    fold.set(table, cur)
  }
  for (const inp of arr.inputs) idTable.set(inp.id, inp.table)
  for (const ix of arr.indexes) {
    idTable.set(ix.id, ix.table)
    bump(ix.table, ix.seeded, false)
  }
  for (const ct of arr.counts ?? []) {
    idTable.set(ct.id, ct.table)
    bump(ct.table, ct.seeded, true)
  }

  // Stamp the fold summary onto each table's source node — that is what the indexed treatment and
  // the count badge read, and the detail panel re-derives the full list from `graph.arrangements`.
  for (const [table, sum] of fold) {
    const src = nodes.get(`src:${table}`)
    if (src) src.data.arr = sum
  }

  // Consumer edges now hang off the table's source node (its arrangements are folded there). The
  // edge id carries the arrangement id so two indexes on one table serving two shapes stay distinct.
  for (const c of arr.consumers) {
    const table = idTable.get(c.index)
    if (table === undefined) continue
    const src = `src:${table}`
    if (!nodes.has(src)) continue
    if (c.dependentKind === 'circuit-shape' || c.dependentKind === 'circuit-agg') {
      // A SERVING edge: the dependent's data comes FROM the circuit (seeded there, maintained
      // there) — not an occasional read. It lands on the dependent's own operator: the fold of a
      // counts-served aggregate, the membership semijoin (or standalone σ) of a served shape.
      const candidates =
        c.dependentKind === 'circuit-agg'
          ? [`fold:${c.dependentId}`, `snk:${c.dependentId}`]
          : [`sj:${c.dependentId}`, `sigma:${c.dependentId}`, `snk:${c.dependentId}`]
      const target = candidates.find((t) => nodes.has(t))
      if (!target) continue
      edges.push({
        id: `${src}~>${target}~serves~${c.index}`,
        source: src,
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
      id: `${src}~>${target}~lookup~${c.index}`,
      source: src,
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
