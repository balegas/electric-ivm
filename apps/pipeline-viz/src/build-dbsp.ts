import dagre from '@dagrejs/dagre'
import type { Edge, Node } from '@xyflow/react'

import type { BuildOpts, NodeKind, NodeRef, VizNodeData } from './build-graph'
import { predicateLabel } from './predicate-label'
import type { EngineGraph } from './types'

// The RAW dbsp operator view: each shape is expanded into the incremental dataflow that maintains it —
// Z-sets flowing through Δ (change), σ (filter), ↦ (index/arrange), ⋈ (join), distinct/params
// (stateful arrangements), and π (map to upsert/delete). Operators shared underneath (a table's Δ, a
// family's params arrangement, a subquery's distinct node) appear once, exactly as the engine shares
// them. It is reconstructed from the query semantics the engine reports — this engine hand-rolls these
// operators over dbsp's Z-set types rather than running a compiled circuit, but the dataflow is the
// same, annotated with the real maintained state.

interface RawNode {
  id: string
  data: VizNodeData
}
interface RawEdge {
  id: string
  source: string
  target: string
  label?: string
  kind: 'z' | 'arr'
}

const op = (op: string, formula: string, note: string): NodeRef => ({ kind: 'op', op, formula, note })

function buildFull(g: EngineGraph): { nodes: Map<string, RawNode>; edges: RawEdge[] } {
  const nodes = new Map<string, RawNode>()
  const edges: RawEdge[] = []
  const add = (id: string, data: VizNodeData) => {
    const ex = nodes.get(id)
    if (ex) {
      if (data.shared && (!ex.data.shared || data.shared > ex.data.shared)) ex.data.shared = data.shared
      return
    }
    nodes.set(id, { id, data })
  }
  const edge = (source: string, target: string, kind: RawEdge['kind'], label?: string) => {
    const id = `${source}~>${target}~${label ?? ''}`
    if (!edges.some((e) => e.id === id)) edges.push({ id, source, target, label, kind })
  }

  const srcId = (t: string) => `src:${t}`
  const dId = (t: string) => `d:${t}`
  const ensureTable = (t: string) => {
    add(srcId(t), {
      kind: 'source',
      label: `table/${t}`,
      sub: 'Z-set of row changes',
      ref: { kind: 'table', name: t },
    })
    add(dId(t), {
      kind: 'delta',
      label: 'Δ  upsert → ±1',
      sub: '[(old,−1),(new,+1)]',
      ref: op(
        'Δ  (change → Z-set)',
        'insert (r,+1) · delete (r,−1) · update (old,−1)(new,+1)',
        'Turns each replicated envelope into a weighted delta (REPLICA IDENTITY FULL supplies old+new). One per table change — shared by every operator downstream on this table.',
      ),
    })
    edge(srcId(t), dId(t), 'z', `Z⟨${t}⟩`)
  }

  const famMembers = new Map<string, number>()
  for (const s of g.shapes) {
    if (s.familyKey && !s.isSubquery) {
      const fk = `${s.table}:${s.familyKey.join(',')}`
      famMembers.set(fk, (famMembers.get(fk) ?? 0) + 1)
    }
  }

  for (const s of g.shapes) {
    ensureTable(s.table)
    const sink = `snk:${s.id}`

    // Aggregation shape: source → Δ → σ(filter) → fold (stateful running scalar) → sink.
    if (s.aggregate) {
      const fn = s.aggregate.func.toUpperCase()
      add(sink, { kind: 'sink', label: `shape/${s.id}`, sub: 'scalar out', ref: { kind: 'shape', shapeId: s.id } })
      const fold = `fold:${s.id}`
      add(fold, {
        kind: 'op-agg',
        label: `fold ${fn}`,
        sub: s.aggregate.col ? `over ${s.aggregate.col}` : 'Σ weights',
        index: 'scalar · state',
        ref: op(
          `Σ  fold / aggregate (${fn})`,
          s.aggregate.col ? `${fn}(${s.aggregate.col}) = Σ value·weight` : 'COUNT = Σ weight',
          'A stateful incremental fold over the matching Z-set: each change adds weight·value to the running aggregate (COUNT is Σ weights). MIN/MAX keep an ordered multiset so a retraction restores the previous extreme. Emits a single scalar that updates as rows enter/leave.',
        ),
      })
      const noFilter = predicateLabel(s.where) === 'match all'
      if (noFilter) {
        edge(dId(s.table), fold, 'z', `Z⟨${s.table}⟩`)
      } else {
        const f = `f:${s.id}`
        add(f, {
          kind: 'op-filter',
          label: `σ  ${predicateLabel(s.where)}`,
          sub: 'stateless',
          ref: op('σ  filter', `keep rows where ${predicateLabel(s.where)}`, 'Stateless filter before the fold.'),
        })
        edge(dId(s.table), f, 'z')
        edge(f, fold, 'z')
      }
      edge(fold, sink, 'z', 'scalar')
      continue
    }

    add(sink, { kind: 'sink', label: `shape/${s.id}`, sub: 'upsert / delete out', ref: { kind: 'shape', shapeId: s.id } })
    const mapId = `m:${s.id}`
    add(mapId, {
      kind: 'op-map',
      label: 'π  → upsert/delete',
      sub: 'group by pk',
      ref: op(
        'π  map / translate_output',
        'Z-set → per-pk: +weight ⇒ upsert, −only ⇒ delete',
        'Groups the output Z-set by primary key into shape-stream envelopes (carrying the txid + commit LSN).',
      ),
    })
    edge(mapId, sink, 'z', `Z⟨${s.id}⟩`)

    if (s.isSubquery) {
      const sj = `sj:${s.id}`
      add(sj, {
        kind: 'op-join',
        label: '⋈  subquery join',
        sub: 'outer ∈/∉ inner set',
        ref: op(
          '⋈  semijoin / antijoin',
          'outer ⋉ node-set on connecting col (IN = semijoin, NOT IN = antijoin)',
          'Keeps outer rows whose connecting column is (IN) / is not (NOT IN) the maintained inner set. When a value enters/leaves the set, the affected outer rows move in/out live.',
        ),
      })
      edge(dId(s.table), sj, 'z', `Z⟨${s.table}⟩`)
      edge(sj, mapId, 'z')
    } else if (s.familyKey) {
      const fk = `${s.table}:${s.familyKey.join(',')}`
      const members = famMembers.get(fk) ?? 1
      const ix = `ix:${fk}`
      const pa = `pa:${fk}`
      const j = `j:${fk}`
      add(ix, {
        kind: 'op-index',
        label: `↦ index by (${s.familyKey.join(', ')})`,
        sub: 'key = template cols',
        ref: op('↦  map_index', 'row ↦ (key(row), row)', 'Arranges the delta by the template key so the routing join can dispatch it.'),
      })
      add(pa, {
        kind: 'op-arrange',
        label: 'params  key → shapes',
        sub: `${members} keys · stateful`,
        index: `${members} keys`,
        shared: members,
        ref: { kind: 'family', table: s.table, keyCols: s.familyKey },
      })
      add(j, {
        kind: 'op-join',
        label: '⋈  route (join)',
        sub: 'delta ⋈ params',
        shared: members,
        ref: op(
          '⋈  incremental join (route)',
          '(key,row) ⋈ (key,shape) → (shape,row)',
          'The shared routing join: a change is emitted to exactly the shapes registered on its key. Here it is a hashmap lookup; the dbsp-equivalent is a semijoin against the params arrangement — one join for the whole family, independent of shape count.',
        ),
      })
      edge(dId(s.table), ix, 'z')
      edge(ix, j, 'z')
      edge(pa, j, 'arr', 'arrangement')
      edge(j, mapId, 'z', `→ shape ${s.id}`)
    } else {
      const f = `f:${s.id}`
      add(f, {
        kind: 'op-filter',
        label: `σ  ${predicateLabel(s.where)}`,
        sub: 'stateless',
        ref: op('σ  filter', `keep rows where ${predicateLabel(s.where)}`, 'Stateless: applied directly to each delta tuple under three-valued logic (a NULL operand ⇒ exclude). No state, O(1) per change.'),
      })
      edge(dId(s.table), f, 'z')
      edge(f, mapId, 'z')
    }
  }

  for (const n of g.subqueryNodes) {
    ensureTable(n.innerTable)
    const sf = `sf:${n.sig}`
    const si = `si:${n.sig}`
    const dist = `dist:${n.sig}`
    add(sf, {
      kind: 'op-filter',
      label: 'σ  inner where',
      sub: 'subquery predicate',
      ref: op('σ  filter (inner)', 'keep inner rows matching the subquery WHERE', 'The subquery inner predicate (may itself join deeper nodes for nested IN).'),
    })
    add(si, {
      kind: 'op-index',
      label: `↦ index by ${n.projCol}`,
      sub: `project ${n.projCol}`,
      ref: op('↦  map_index', `row ↦ (${n.projCol}, pk)`, 'Arranges inner rows by the projected column so distinct can maintain the value set.'),
    })
    add(dist, {
      kind: 'op-arrange',
      label: `distinct ${n.projCol}`,
      sub: `${n.distinctValues} values · refcount ${n.refcount}`,
      index: `${n.distinctValues} values`,
      shared: n.refcount,
      ref: { kind: 'sqnode', sig: n.sig, innerTable: n.innerTable, projCol: n.projCol },
    })
    edge(dId(n.innerTable), sf, 'z')
    edge(sf, si, 'z')
    edge(si, dist, 'z')
  }
  for (const e of g.subqueryEdges) {
    const dist = `dist:${e.nodeSig}`
    if (e.dependentKind === 'shape') {
      edge(dist, `sj:${e.dependentId}`, 'arr', `${e.negated ? 'NOT IN' : 'IN'} · ${e.connectingCol}`)
    } else {
      edge(dist, `sf:${e.dependentId}`, 'arr', 'nested IN')
    }
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

const OP_SIZE: Partial<Record<NodeKind, { w: number; h: number }>> = {
  source: { w: 150, h: 54 },
  delta: { w: 170, h: 56 },
  'op-filter': { w: 220, h: 54 },
  'op-index': { w: 200, h: 54 },
  'op-arrange': { w: 210, h: 60 },
  'op-join': { w: 180, h: 56 },
  'op-map': { w: 180, h: 54 },
  'op-agg': { w: 200, h: 60 },
  sink: { w: 160, h: 54 },
}
const defaultSize = (k: NodeKind) => OP_SIZE[k] ?? { w: 190, h: 54 }

function layout(
  raw: { nodes: Map<string, RawNode>; edges: RawEdge[] },
  focus: string | null,
  opts?: BuildOpts,
): { nodes: Node[]; edges: Edge[] } {
  const sizeOfNode = (n: RawNode) => opts?.measure?.(n.data) ?? defaultSize(n.data.kind)
  const g = new dagre.graphlib.Graph()
  g.setGraph({ rankdir: 'LR', nodesep: 18, ranksep: 70, marginx: 24, marginy: 24 })
  g.setDefaultEdgeLabel(() => ({}))
  for (const [id, n] of raw.nodes) {
    const s = sizeOfNode(n)
    g.setNode(id, { width: s.w, height: s.h })
  }
  for (const e of raw.edges) g.setEdge(e.source, e.target)
  if (opts?.alignSources) {
    g.setNode('__align_root', { width: 1, height: 1 })
    for (const [id, n] of raw.nodes) {
      if (n.data.kind === 'table' || n.data.kind === 'source') g.setEdge('__align_root', id)
    }
  }
  dagre.layout(g)

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
    const s = sizeOfNode(n)
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
      animated: e.kind === 'arr' && !dim,
      style: {
        stroke: e.kind === 'arr' ? '#a855f7' : '#64748b',
        strokeWidth: e.kind === 'arr' ? 2 : 1.5,
        strokeDasharray: e.kind === 'arr' ? '5 4' : undefined,
        opacity: dim ? 0.12 : 1,
      },
    }
  })
  return { nodes, edges }
}

/** Build the laid-out raw dbsp operator graph for a selection ('all' or shape ids), with optional focus. */
export function buildDbspGraph(
  g: EngineGraph,
  selection: 'all' | Set<string>,
  focus: string | null = null,
  opts?: BuildOpts,
): { nodes: Node[]; edges: Edge[] } {
  const full = buildFull(g)
  const restricted = selection === 'all' ? full : restrictToSelection(full, selection)
  return layout(restricted, focus, opts)
}
