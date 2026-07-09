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
  | 'arr-input'
  | 'arr-index'
  | 'arr-counts'

/** Identity of the underlying engine entity a graph node represents (used by the detail panel). */
export type NodeRef =
  | { kind: 'table'; name: string }
  | { kind: 'family'; table: string; keyCols: string[] }
  | { kind: 'filter'; shapeId: string }
  | { kind: 'sqnode'; sig: string; innerTable: string; projCol: string }
  | { kind: 'shape'; shapeId: string }
  | { kind: 'aggshape'; shapeId: string }
  /** A collapsed family of shapes (the "group shapes" toggle): every equality shape on this
   *  (table, key-cols) route join, shown as one node. The detail panel lists the members. */
  | { kind: 'shapegroup'; table: string; keyCols: string[] }
  /** A circuit-view operator: its kind, the trace-hop it animates under, and its display label. */
  | { kind: 'op'; opKind: NodeKind; hop: string; label: string }

export interface VizNodeData extends Record<string, unknown> {
  kind: NodeKind
  label: string
  sub?: string
  /** How many things share this node (family members / subquery refcount) — drives the "shared" badge. */
  shared?: number
  /** Render as a stack of cards — a collapsed `shapegroup` standing in for its N member shapes. */
  stack?: boolean
  /** An id shown inline in the header tag row (e.g. the shape id next to "SHAPE OUTPUT"). */
  idTag?: string
  /** Render `label` as a highlighted expression (used for a shape's filter predicate). */
  highlight?: boolean
  /** `GET /state` key whose live chips this node shows (absent = no state row). */
  stateId?: string
  /** Retention lifecycle of a shape node — `dormant`/`deactivating`/`reactivating` render parked. */
  life?: string | null
  /** Circuit placement label (`dynamic:<col>` / `counts` / …) when the shape is circuit-served —
   *  rendered as a chip in the card's tag row. */
  serve?: string
  /** Circuit view: the compiled dbsp arrangements folded onto a table's SOURCE node — the
   *  arrangement lane (inputs, indexes, counts) is collapsed onto the source to declutter. Present
   *  only on `op-source` nodes whose table has arrangements; drives the indexed treatment + count
   *  badge, and the detail panel expands the full list from `graph.arrangements`. */
  arr?: { indexes: number; counts: number; seeded: boolean }
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
  /** `serve` = a circuit SERVING edge (the target's data comes from the dbsp circuit), as opposed
   *  to `state` (a stateful arrangement an operator occasionally reads — e.g. a lookup). */
  kind: 'flow' | 'route' | 'subquery' | 'state' | 'serve'
}

/**
 * Turn the engine's `/graph` snapshot into the full pipeline graph (all nodes + edges). Shared
 * structure collapses naturally: family routers are keyed by (table, key-cols) and subquery nodes by
 * signature, so two shapes that share one underneath connect to the SAME node here.
 *
 * With `group` on, the fan-out of equality shapes on a shared route join collapses too: instead of
 * one output node per shape, a family with >1 member gets ONE `shapegroup` node badged with the
 * count. (Only in the whole-graph view; a selection always expands to individual shapes so their
 * sinks stay reachable — see `buildGraph`.)
 */
function buildFull(g: EngineGraph, group: boolean): { nodes: Map<string, RawNode>; edges: RawEdge[] } {
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
        life: s.state,
        serve: s.circuit?.label,
        ref: { kind: 'aggshape', shapeId: s.id },
      })
      edge(tableId(s.table), shapeId, 'flow', predicateLabel(s.where) === 'match all' ? undefined : 'filter')
      continue
    }

    // A family with >1 member collapses to a single `shapegroup` node when grouping — so skip the
    // per-shape node for those, and skip its route edge below.
    const fid = s.familyKey && !s.isSubquery ? `family:${s.table}:${s.familyKey.join(',')}` : null
    const shared = fid ? (familyMembers.get(fid) ?? 1) : 1
    const grouped = group && fid !== null && shared > 1

    if (!grouped) {
      add(shapeId, {
        kind: 'shape',
        // The filter predicate is the node's headline content (highlighted); the shape id moves inline
        // into the header tag row, and the source table names the relation the predicate is over.
        label: predicateLabel(s.where),
        idTag: s.id,
        highlight: true,
        sub: s.table,
        life: s.state,
        serve: s.circuit?.label,
        ref: { kind: 'shape', shapeId: s.id },
      })
    }

    if (s.isSubquery) {
      edge(tableId(s.table), shapeId, 'flow', 'filter + moves')
    } else if (fid) {
      add(fid, {
        kind: 'family',
        label: `route by (${s.familyKey!.join(', ')})`,
        sub: shared > 1 ? `shared by ${shared} shapes` : undefined,
        shared,
        ref: { kind: 'family', table: s.table, keyCols: s.familyKey! },
      })
      edge(tableId(s.table), fid, 'flow')
      if (grouped) {
        // One collapsed output node for the whole family (deduped by id across its members).
        const gid = `shapegroup:${fid}`
        add(gid, {
          kind: 'shape',
          // The query template the members share — the key predicate with the value parameterized
          // (`issue_id = ?`), shown like a concrete shape's predicate but abstracted over its value.
          label: s.familyKey!.map((c) => `${c} = ?`).join(' AND '),
          highlight: true,
          sub: `${s.table} · ${shared} shapes`,
          stack: true,
          ref: { kind: 'shapegroup', table: s.table, keyCols: s.familyKey! },
        })
        edge(fid, gid, 'route')
      } else {
        edge(fid, shapeId, 'route', keyLabel(s.where))
      }
    } else {
      const filterId = `filter:${s.id}`
      add(filterId, {
        kind: 'filter',
        label: 'filter',
        sub: predicateLabel(s.where),
        ref: { kind: 'filter', shapeId: s.id },
      })
      edge(tableId(s.table), filterId, 'flow')
      edge(filterId, shapeId, 'flow')
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
  // circuit-view operators (denser boxes, same height so ranks stay level). op-source is a touch
  // wider than the rest: it carries the folded-arrangement count badge (`⧉ N idx · M cnt`) beside
  // its SOURCE tag, and the widest table names (`project_members`).
  'op-source': { w: 176, h: KIND_H },
  'op-delta': { w: 120, h: KIND_H },
  'op-filter': { w: 150, h: KIND_H },
  'op-key': { w: 150, h: KIND_H },
  'op-arrange': { w: 180, h: KIND_H },
  'op-join': { w: 150, h: KIND_H },
  'op-distinct': { w: 180, h: KIND_H },
  'op-fold': { w: 180, h: KIND_H },
  'op-project': { w: 150, h: KIND_H },
  'op-sink': { w: 180, h: KIND_H },
  // the compiled dbsp arrangement pipeline (static infrastructure lane)
  'arr-input': { w: 160, h: KIND_H },
  'arr-index': { w: 220, h: KIND_H },
  'arr-counts': { w: 260, h: KIND_H },
}

/** Optional layout hooks: `measure` overrides a node's box (return null to keep the default) —
 *  lets an embedder size nodes to fit full multi-line labels. */
export interface BuildOpts {
  measure?: (data: VizNodeData) => { w: number; h: number } | null
  /** Force all replication-source nodes into one rank (one aligned column). Done inside dagre via
   *  a hidden root, so downstream placement still avoids node/edge overlaps. */
  alignSources?: boolean
  /** Sticky positions across publishes (keyed by node id): a node already in the map keeps its
   *  coordinates — adding/removing shapes never shifts the rest of the canvas. Only genuinely
   *  new nodes are placed, anchored to their already-placed neighbours (their fresh-dagre offset
   *  relative to a neighbour is applied to that neighbour's sticky position, averaged), so they
   *  appear in the right slot of the STABLE frame instead of the re-ranked one. Final positions
   *  are written back. Clear the map to re-tidy the whole layout (the refresh button). */
  positions?: Map<string, { x: number; y: number }>
  /** Collapse each family's fan-out of equality shapes into one `shapegroup` node (default on in
   *  the app). Consumed by `buildGraph`; ignored by the circuit view. */
  groupShapes?: boolean
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
      if (n.data.kind === 'table' || n.data.kind === 'op-source' || n.data.kind === 'arr-input')
        g.setEdge('__align_root', id, { weight: 100 })
    }
  }
  dagre.layout(g)

  // Sticky placement: `placed` starts as the caller's cache and grows as this pass assigns
  // positions, so a chain of new nodes (table → new family → new shape) anchors transitively.
  const sticky = opts?.positions
  const placed = new Map(sticky)
  const dagrePos = (id: string): { x: number; y: number } => {
    const p = g.node(id)
    const s = sizeOf(raw.nodes.get(id)!)
    return { x: p.x - s.w / 2, y: p.y - s.h / 2 }
  }
  const neighbours = new Map<string, string[]>()
  if (sticky) {
    for (const e of raw.edges) {
      neighbours.set(e.source, [...(neighbours.get(e.source) ?? []), e.target])
      neighbours.set(e.target, [...(neighbours.get(e.target) ?? []), e.source])
    }
  }
  // A new node anchored to its sticky neighbours can still land on top of a sibling: two shapes that
  // share one anchor (e.g. both hanging off `route by (issue_id)`) were placed in different publishes'
  // dagre frames, so the same anchor offset drops them at the same spot. After anchoring, nudge a new
  // node DOWN past any already-placed box it overlaps — never touching the sticky nodes, so the rest
  // of the canvas stays put while the fresh shape falls into free space instead of stacking.
  const NODE_GAP = 16
  const deOverlap = (id: string, pos: { x: number; y: number }): { x: number; y: number } => {
    const s = sizeOf(raw.nodes.get(id)!)
    let y = pos.y
    for (let guard = 0; guard < 400; guard++) {
      let bumped = false
      for (const [oid, op] of placed) {
        if (oid === id || !raw.nodes.has(oid)) continue
        const os = sizeOf(raw.nodes.get(oid)!)
        const xOverlap = pos.x < op.x + os.w && pos.x + s.w > op.x
        if (!xOverlap) continue
        if (y < op.y + os.h + NODE_GAP && y + s.h + NODE_GAP > op.y) {
          y = op.y + os.h + NODE_GAP // drop below the box we hit, then re-scan
          bumped = true
        }
      }
      if (!bumped) break
    }
    return { x: pos.x, y }
  }
  const positionOf = (id: string): { x: number; y: number } => {
    if (!sticky) return dagrePos(id)
    const hit = placed.get(id)
    if (hit) return hit
    const base = dagrePos(id)
    const anchors = (neighbours.get(id) ?? []).filter((o) => placed.has(o) && raw.nodes.has(o))
    let pos = base
    if (anchors.length) {
      let dx = 0
      let dy = 0
      for (const o of anchors) {
        const op = dagrePos(o)
        const cp = placed.get(o)!
        dx += cp.x - op.x
        dy += cp.y - op.y
      }
      pos = { x: base.x + dx / anchors.length, y: base.y + dy / anchors.length }
    }
    pos = deOverlap(id, pos)
    placed.set(id, pos)
    sticky.set(id, pos)
    return pos
  }

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
    const s = sizeOf(n)
    return {
      id: n.id,
      type: 'pipeline',
      position: positionOf(n.id),
      data: { ...n.data, selected: n.id === focus, dimmed: lit ? !lit.has(n.id) : false },
      width: s.w,
      height: s.h,
      // Pre-seed the measured size: React Flow's adoptUserNodes treats a node object WITHOUT
      // `measured` as re-initialized and resets its measured handle bounds — and since every
      // graph publish rebuilds these node objects, a freshly added node could lose the race and
      // stay unmeasured forever, silently rendering NO edges (its edge positions stay null).
      // With `measured` present, previously measured handle bounds survive republishing and only
      // genuinely new nodes take the one initial ResizeObserver measurement.
      measured: { width: s.w, height: s.h },
    }
  })
  const edges: Edge[] = raw.edges.map((e) => {
    const dim = lit ? !(lit.has(e.source) && lit.has(e.target)) : false
    return {
      id: e.id,
      source: e.source,
      target: e.target,
      // Serving edges animate like subquery flips: data continuously flows FROM the circuit.
      animated: (e.kind === 'subquery' || e.kind === 'serve') && !dim,
      style: {
        stroke:
          e.kind === 'subquery'
            ? '#a855f7'
            : e.kind === 'route'
              ? '#0ea5e9'
              : e.kind === 'serve'
                ? '#4338ca'
                : e.kind === 'state'
                  ? '#7e22ce'
                  : '#94a3b8',
        strokeWidth: e.kind === 'flow' ? 1.5 : e.kind === 'serve' ? 2.5 : 2,
        // A dashed edge = a stateful arrangement an operator READS (a lookup, not a stream);
        // a solid animated indigo edge = the circuit SERVING a shape (its data source).
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
  // Grouping only applies to the whole-graph overview: a selection restricts to shape SINKS
  // (`shape:<id>`), which a collapsed family node wouldn't carry, so we always expand there.
  const full = buildFull(g, opts?.groupShapes !== false && selection === 'all')
  const restricted = selection === 'all' ? full : restrictToSelection(full, selection)
  return layout(restricted, focus, opts)
}
