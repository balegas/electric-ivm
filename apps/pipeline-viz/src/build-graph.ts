import dagre from '@dagrejs/dagre'
import type { Edge, Node } from '@xyflow/react'

import { keyLabel, predicateLabel } from './predicate-label'
import type { EngineGraph } from './types'

export type NodeKind =
  // logical view
  | 'table'
  | 'family'
  | 'filter'
  | 'sqnode'
  | 'shape'
  | 'agg'
  // raw dbsp operator view
  | 'source'
  | 'delta'
  | 'op-filter'
  | 'op-index'
  | 'op-arrange'
  | 'op-join'
  | 'op-map'
  | 'op-agg'
  | 'sink'

/** Identity of the underlying engine entity a graph node represents (used by the detail panel). */
export type NodeRef =
  | { kind: 'table'; name: string }
  | { kind: 'family'; table: string; keyCols: string[] }
  | { kind: 'filter'; shapeId: string }
  | { kind: 'sqnode'; sig: string; innerTable: string; projCol: string }
  | { kind: 'shape'; shapeId: string }
  | { kind: 'aggshape'; shapeId: string }
  /** A dbsp operator (raw view): its symbol, formula, and an explanatory note. */
  | { kind: 'op'; op: string; formula: string; note: string }

export interface VizNodeData extends Record<string, unknown> {
  kind: NodeKind
  label: string
  sub?: string
  /** How many things share this node (family members / subquery refcount) — drives the "shared" badge. */
  shared?: number
  /** Small count badge, e.g. "5 keys" / "3 values". */
  index?: string
  /** An id shown inline in the header tag row (e.g. the shape id next to "SHAPE OUTPUT"). */
  idTag?: string
  /** Render `label` as a highlighted expression (used for a shape's filter predicate). */
  highlight?: boolean
  ref: NodeRef
  selected?: boolean
  dimmed?: boolean
}

interface RawNode {
  id: string
  data: VizNodeData
}
interface RawEdge {
  id: string
  source: string
  target: string
  label?: string
  kind: 'flow' | 'route' | 'subquery'
}

/**
 * Turn the engine's `/graph` snapshot into the full pipeline graph (all nodes + edges). Shared
 * structure collapses naturally: family routers are keyed by (table, key-cols) and subquery nodes by
 * signature, so two shapes that share one underneath connect to the SAME node here.
 */
function buildFull(g: EngineGraph): { nodes: Map<string, RawNode>; edges: RawEdge[] } {
  const nodes = new Map<string, RawNode>()
  const edges: RawEdge[] = []
  const familyMembers = new Map<string, number>()

  const add = (id: string, data: VizNodeData) => {
    const existing = nodes.get(id)
    if (existing) {
      if (data.shared && (!existing.data.shared || data.shared > existing.data.shared)) existing.data.shared = data.shared
      if (data.index && !existing.data.index) existing.data.index = data.index
      return
    }
    nodes.set(id, { id, data })
  }
  const edge = (source: string, target: string, kind: RawEdge['kind'], label?: string) => {
    const id = `${source}~>${target}~${label ?? ''}`
    if (!edges.some((e) => e.id === id)) edges.push({ id, source, target, label, kind })
  }
  const tableId = (t: string) => `table:${t}`

  for (const t of g.tables) add(tableId(t), { kind: 'table', label: t, ref: { kind: 'table', name: t } })

  for (const s of g.shapes) {
    if (s.familyKey && !s.isSubquery) {
      const fid = `family:${s.table}:${s.familyKey.join(',')}`
      familyMembers.set(fid, (familyMembers.get(fid) ?? 0) + 1)
    }
  }

  for (const s of g.shapes) {
    const shapeId = `shape:${s.id}`
    add(tableId(s.table), { kind: 'table', label: s.table, ref: { kind: 'table', name: s.table } })

    // Aggregation shape: a scalar COUNT/SUM/… over the filter — a terminal node, not row output.
    if (s.aggregate) {
      const fn = s.aggregate.func.toUpperCase()
      add(shapeId, {
        kind: 'agg',
        label: `${fn}(${s.aggregate.col ?? '*'})`,
        sub: predicateLabel(s.where),
        index: 'live scalar',
        ref: { kind: 'aggshape', shapeId: s.id },
      })
      edge(tableId(s.table), shapeId, 'flow', predicateLabel(s.where) === 'match all' ? undefined : 'filter')
      continue
    }

    add(shapeId, {
      kind: 'shape',
      // The filter predicate is the node's headline content (highlighted); the shape id moves inline
      // into the header tag row.
      label: predicateLabel(s.where),
      idTag: s.id,
      highlight: true,
      ref: { kind: 'shape', shapeId: s.id },
    })

    if (s.isSubquery) {
      edge(tableId(s.table), shapeId, 'flow', 'filter + moves')
    } else if (s.familyKey) {
      const fid = `family:${s.table}:${s.familyKey.join(',')}`
      const shared = familyMembers.get(fid) ?? 1
      add(fid, {
        kind: 'family',
        label: `route by (${s.familyKey.join(', ')})`,
        sub: shared > 1 ? `shared by ${shared} shapes` : undefined,
        shared,
        index: `${shared} ${shared === 1 ? 'key' : 'keys'}`,
        ref: { kind: 'family', table: s.table, keyCols: s.familyKey },
      })
      edge(tableId(s.table), fid, 'flow')
      edge(fid, shapeId, 'route', keyLabel(s.where))
    } else {
      const fid = `filter:${s.id}`
      add(fid, {
        kind: 'filter',
        label: 'filter',
        sub: predicateLabel(s.where),
        ref: { kind: 'filter', shapeId: s.id },
      })
      edge(tableId(s.table), fid, 'flow')
      edge(fid, shapeId, 'flow')
    }
  }

  for (const n of g.subqueryNodes) {
    add(`node:${n.sig}`, {
      kind: 'sqnode',
      label: `SELECT ${n.projCol} FROM ${n.innerTable}`,
      sub: `${n.distinctValues} values · refcount ${n.refcount}`,
      shared: n.refcount,
      index: `${n.distinctValues} ${n.distinctValues === 1 ? 'value' : 'values'}`,
      ref: { kind: 'sqnode', sig: n.sig, innerTable: n.innerTable, projCol: n.projCol },
    })
    add(tableId(n.innerTable), { kind: 'table', label: n.innerTable, ref: { kind: 'table', name: n.innerTable } })
    edge(tableId(n.innerTable), `node:${n.sig}`, 'flow')
  }
  for (const e of g.subqueryEdges) {
    const src = `node:${e.nodeSig}`
    const rel = `${e.negated ? 'NOT IN' : 'IN'} · ${e.connectingCol}`
    if (e.dependentKind === 'shape') edge(src, `shape:${e.dependentId}`, 'subquery', rel)
    else edge(src, `node:${e.dependentId}`, 'subquery', rel)
  }

  return { nodes, edges }
}

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
    const nid = `shape:${id}`
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

const KIND_SIZE: Partial<Record<NodeKind, { w: number; h: number }>> = {
  table: { w: 150, h: 52 },
  family: { w: 220, h: 64 },
  filter: { w: 240, h: 64 },
  sqnode: { w: 250, h: 66 },
  shape: { w: 200, h: 64 },
  agg: { w: 210, h: 64 },
}

function layout(
  raw: { nodes: Map<string, RawNode>; edges: RawEdge[] },
  focus: string | null,
): { nodes: Node[]; edges: Edge[] } {
  const g = new dagre.graphlib.Graph()
  g.setGraph({ rankdir: 'LR', nodesep: 24, ranksep: 90, marginx: 24, marginy: 24 })
  g.setDefaultEdgeLabel(() => ({}))
  for (const [id, n] of raw.nodes) {
    const s = KIND_SIZE[n.data.kind] ?? { w: 200, h: 60 }
    g.setNode(id, { width: s.w, height: s.h })
  }
  for (const e of raw.edges) g.setEdge(e.source, e.target)
  dagre.layout(g)

  // Connection highlight: when a node is focused, neighbours stay lit and the rest dims.
  let lit: Set<string> | null = null
  if (focus && raw.nodes.has(focus)) {
    lit = new Set([focus])
    for (const e of raw.edges) {
      if (e.source === focus) lit.add(e.target)
      if (e.target === focus) lit.add(e.source)
    }
  }

  const nodes: Node[] = [...raw.nodes.values()].map((n) => {
    const p = g.node(n.id)
    const s = KIND_SIZE[n.data.kind] ?? { w: 200, h: 60 }
    return {
      id: n.id,
      type: 'pipeline',
      position: { x: p.x - s.w / 2, y: p.y - s.h / 2 },
      data: { ...n.data, selected: n.id === focus, dimmed: lit ? !lit.has(n.id) : false },
      width: s.w,
      height: s.h,
    }
  })
  const edges: Edge[] = raw.edges.map((e) => {
    const dim = lit ? !(lit.has(e.source) && lit.has(e.target)) : false
    return {
      id: e.id,
      source: e.source,
      target: e.target,
      animated: e.kind === 'subquery' && !dim,
      style: {
        stroke: e.kind === 'subquery' ? '#a855f7' : e.kind === 'route' ? '#0ea5e9' : '#94a3b8',
        strokeWidth: e.kind === 'flow' ? 1.5 : 2,
        opacity: dim ? 0.12 : 1,
      },
    }
  })
  return { nodes, edges }
}

/** Build laid-out React Flow nodes+edges for a selection ('all' or a set of shape ids), with an
 *  optional focused node id (for connection highlighting). */
export function buildGraph(
  g: EngineGraph,
  selection: 'all' | Set<string>,
  focus: string | null = null,
): { nodes: Node[]; edges: Edge[] } {
  const full = buildFull(g)
  const restricted = selection === 'all' ? full : restrictToSelection(full, selection)
  return layout(restricted, focus)
}
