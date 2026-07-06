import dagre from '@dagrejs/dagre'
import type { Edge, Node } from '@xyflow/react'

import { keyLabel, predicateLabel } from './predicate-label'
import type { EngineGraph } from './types'

// The node kinds are the engine's real maintained structures — the same set `GET /graph` reports
// and `/trace` hops name. Every logical id (`table:`, `family:`, `filter:`, `node:`, `shape:`) is
// an engine id: trace hops and `state` events key on them directly, no translation. The `op-*`
// kinds are the circuit view's operators — also engine-emitted (`/graph` `operators`), each bound
// to its hop and state id by the engine.
export type NodeKind =
  | 'table'
  | 'family'
  | 'filter'
  | 'sqnode'
  | 'shape'
  | 'agg'
  | 'op-source'
  | 'op-delta'
  | 'op-filter'
  | 'op-key'
  | 'op-arrange'
  | 'op-join'
  | 'op-distinct'
  | 'op-fold'
  | 'op-project'
  | 'op-sink'

/** Identity of the underlying engine entity a graph node represents (used by the detail panel). */
export type NodeRef =
  | { kind: 'table'; name: string }
  | { kind: 'family'; table: string; keyCols: string[] }
  | { kind: 'filter'; shapeId: string }
  | { kind: 'sqnode'; sig: string; innerTable: string; projCol: string }
  | { kind: 'shape'; shapeId: string }
  | { kind: 'aggshape'; shapeId: string }
  /** A circuit-view operator: its kind, the trace-hop it animates under, and its display label. */
  | { kind: 'op'; opKind: NodeKind; hop: string; label: string }

export interface VizNodeData extends Record<string, unknown> {
  kind: NodeKind
  label: string
  sub?: string
  /** How many things share this node (family members / subquery refcount) — drives the "shared" badge. */
  shared?: number
  /** An id shown inline in the header tag row (e.g. the shape id next to "SHAPE OUTPUT"). */
  idTag?: string
  /** Render `label` as a highlighted expression (used for a shape's filter predicate). */
  highlight?: boolean
  /** `GET /state` key whose live chips this node shows (absent = no state row). */
  stateId?: string
  ref: NodeRef
  selected?: boolean
  dimmed?: boolean
}

export interface RawNode {
  id: string
  data: VizNodeData
}
export interface RawEdge {
  id: string
  source: string
  target: string
  label?: string
  kind: 'flow' | 'route' | 'subquery' | 'state'
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
      return
    }
    // Logical node ids ARE the engine's state-summary ids, so every node gets live chips.
    nodes.set(id, { id, data: { stateId: id, ...data } })
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
      // No query expression on the canvas — the detail panel carries the full SQL.
      label: n.innerTable,
      sub: `distinct ${n.projCol}`,
      shared: n.refcount,
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

// Boxes handed to dagre — these are also the boxes the nodes RENDER at (the node fills its
// laid-out box), so the height must cover the tallest kind's content (tag row + label + optional
// sub line + live state row) or it clips. ONE default height for every kind: a connected row of
// mixed kinds reads as a single centered line with level edges.
export const KIND_H = 88
const KIND_SIZE: Partial<Record<NodeKind, { w: number; h: number }>> = {
  table: { w: 150, h: KIND_H },
  family: { w: 220, h: KIND_H },
  filter: { w: 240, h: KIND_H },
  sqnode: { w: 250, h: KIND_H },
  shape: { w: 200, h: KIND_H },
  agg: { w: 210, h: KIND_H },
  // circuit-view operators (denser boxes, same height so ranks stay level)
  'op-source': { w: 150, h: KIND_H },
  'op-delta': { w: 120, h: KIND_H },
  'op-filter': { w: 150, h: KIND_H },
  'op-key': { w: 150, h: KIND_H },
  'op-arrange': { w: 180, h: KIND_H },
  'op-join': { w: 150, h: KIND_H },
  'op-distinct': { w: 180, h: KIND_H },
  'op-fold': { w: 180, h: KIND_H },
  'op-project': { w: 150, h: KIND_H },
  'op-sink': { w: 180, h: KIND_H },
}

/** Optional layout hooks: `measure` overrides a node's box (return null to keep the default) —
 *  lets an embedder size nodes to fit full multi-line labels. */
export interface BuildOpts {
  measure?: (data: VizNodeData) => { w: number; h: number } | null
  /** Force all replication-source nodes into one rank (one aligned column). Done inside dagre via
   *  a hidden root, so downstream placement still avoids node/edge overlaps. */
  alignSources?: boolean
}

export function layout(
  raw: { nodes: Map<string, RawNode>; edges: RawEdge[] },
  focus: string | null,
  opts?: BuildOpts,
): { nodes: Node[]; edges: Edge[] } {
  const sizeOf = (n: RawNode) => opts?.measure?.(n.data) ?? KIND_SIZE[n.data.kind] ?? { w: 200, h: KIND_H }
  const g = new dagre.graphlib.Graph()
  g.setGraph({ rankdir: 'LR', nodesep: 24, ranksep: 90, marginx: 24, marginy: 24 })
  g.setDefaultEdgeLabel(() => ({}))
  for (const [id, n] of raw.nodes) {
    const s = sizeOf(n)
    g.setNode(id, { width: s.w, height: s.h })
  }
  for (const e of raw.edges) g.setEdge(e.source, e.target)
  if (opts?.alignSources) {
    g.setNode('__align_root', { width: 1, height: 1 })
    for (const [id, n] of raw.nodes) {
      // High weight: the ranker otherwise trades a longer align edge for a shorter flow edge and
      // lets a source drift into a deeper rank (e.g. a table whose only consumer sits far right).
      if (n.data.kind === 'table' || n.data.kind === 'op-source') g.setEdge('__align_root', id, { weight: 100 })
    }
  }
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
    const s = sizeOf(n)
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
        stroke: e.kind === 'subquery' ? '#a855f7' : e.kind === 'route' ? '#0ea5e9' : e.kind === 'state' ? '#7e22ce' : '#94a3b8',
        strokeWidth: e.kind === 'flow' ? 1.5 : 2,
        // A dashed edge = a stateful arrangement feeding a join (not a Z-set stream).
        ...(e.kind === 'state' ? { strokeDasharray: '6 4' } : null),
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
  opts?: BuildOpts,
): { nodes: Node[]; edges: Edge[] } {
  const full = buildFull(g)
  const restricted = selection === 'all' ? full : restrictToSelection(full, selection)
  return layout(restricted, focus, opts)
}
