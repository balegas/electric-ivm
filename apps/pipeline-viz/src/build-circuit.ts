// The exploded dbsp-circuit view. Unlike the logical view (which renders `/graph`'s node set
// directly), this renders the engine-emitted operator decomposition (`operators` / `opEdges`):
// every box is an operator the engine declares it executes, bound by the engine to its trace-hop
// id (for animation) and its state-summary id (for live chips). Nothing here is reconstructed
// client-side — this file only lays out what the engine reports.

import type { Edge, Node } from '@xyflow/react'

import { layout, type BuildOpts, type NodeKind, type NodeRef, type RawEdge, type RawNode } from './build-graph'
import { isSubqueryShape, predicateTemplateLabel, subqueryTemplateKey } from './predicate-label'
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
        // The family params arrangement has no incoming data edge: it is populated by shape
        // create/drop (the control plane), not by any stream. Annotate rather than leave it looking
        // disconnected. (Only the family arrange — a subquery shape's feed set is also kind
        // `arrange` but IS fed by a data edge, the π assertions.)
        ...(o.kind === 'arrange' && o.hop.startsWith('family:') ? { note: '← shape create / drop' } : null),
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

/** The compiled dbsp counts pipelines (present whenever the counts circuit is running): one input
 *  per counted table, one map_index(group)→weighted_count pipeline each. Drawn as a separate lane
 *  it swamps the canvas, so instead it is FOLDED onto each table's SOURCE node: the source carries
 *  an "indexed" treatment + a counts count badge, and the detail panel expands the full list.
 *  Consumer edges hang off it, re-anchored to the source node — today that is the solid animated
 *  SERVING edge to each counts-served aggregate's fold. (The `indexes` field and the dashed LOOKUP
 *  edges below are legacy: the row-arrangement layer was removed — flip re-derivations go to
 *  Postgres — so current engines emit `indexes: []` and only `circuit-agg` consumers. The branches
 *  stay for older engine payloads.) Every pipeline id maps back to `src:<table>`. */
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
      // counts-served aggregate, or — legacy `circuit-shape` payloads only — the membership
      // semijoin (or standalone σ) of a cohort-served shape.
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
    // LEGACY (older engine payloads only — current engines never emit shape/node consumers):
    // a LOOKUP edge feeds the dependent's own operator — a shape's membership semijoin, or a
    // nested node's inner filter: the operators whose flip re-derivations the index served.
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

// ---------------------------------------------------------------------------------------------
// Shape grouping (the "group shapes" toggle) for the circuit view.
//
// The engine reports one per-shape operator chain per shape, so a real app — which opens the same
// handful of query templates once per user/value — swamps the circuit with N near-identical
// parallel chains. Under the toggle (whole-graph view only) those repeated chains collapse to a
// single STACKED representative badged with the member count, mirroring build-graph's `shapegroup`.
// Two dimensions collapse:
//   1. Route-join families: the N per-shape `pi`→`snk` chains hanging off one shared route join
//      (same table + key columns) fold into one stacked SINK — the exact families build-graph folds.
//   2. Subquery templates: subquery shapes whose maintained pipeline is structurally identical
//      (same outer table, predicate template, projection) — differing only in their bound parameter
//      and thus their materialized inner set — fold their inner-set chain (`sqf`/`sqp`/`dist`) into
//      one stacked IN-SET ARRANGE and their outer chain (`sj`/`pi`/`snk`) into one stacked SINK.
// Both dimensions reduce to the same mechanism: a `redirect` map (collapsed operator id → the
// representative node that stands in for it) plus the representative nodes themselves. `collapse`
// then drops the collapsed operators, adds the representatives, and rewrites every edge endpoint
// through the redirect (deduping, and dropping edges that become self-loops on a representative).
// ---------------------------------------------------------------------------------------------

interface GroupPlan {
  /** collapsed per-shape operator id → the representative node that stands in for it. */
  redirect: Map<string, string>
  /** the stacked representative nodes to add in place of the collapsed operators. */
  reps: RawNode[]
}

/** Enumerate the circuit's collapsible shape groups (see the block comment above) and return the
 *  representative nodes plus the operator-id redirect they imply. Derived purely from `/graph` — the
 *  same shape data build-graph groups on — so grouping is deterministic and matches the logical
 *  view's family folding. Empty (no reps) when nothing collapses. */
function planGroups(g: EngineGraph): GroupPlan {
  const redirect = new Map<string, string>()
  const reps: RawNode[] = []

  // (1) Route-join families: >1 equality shape sharing one (table, key-cols) route join. Aggregates
  // and subquery shapes never flow through the route join (they compile to their own chains), so
  // they are excluded from the count — only the `pi`/`snk` chains fed by the route join collapse.
  const fam = new Map<string, { table: string; cols: string[]; ids: string[] }>()
  for (const s of g.shapes ?? []) {
    if (!s.familyKey || isSubqueryShape(s) || s.aggregate) continue
    const key = `${s.table}:${s.familyKey.join(',')}`
    const e = fam.get(key) ?? { table: s.table, cols: s.familyKey, ids: [] }
    e.ids.push(s.id)
    fam.set(key, e)
  }
  for (const [key, e] of fam) {
    if (e.ids.length <= 1) continue
    const repId = `snk:group:${key}`
    reps.push({
      id: repId,
      data: {
        kind: 'op-sink',
        // The query template the members share (`issue_id = ?`), highlighted like a shape's
        // predicate — the same headline the logical view's `shapegroup` node carries.
        label: e.cols.map((c) => `${c} = ?`).join(' AND '),
        highlight: true,
        sub: `${e.table} · ${e.ids.length} shapes`,
        stack: true,
        ref: { kind: 'shapegroup', table: e.table, keyCols: e.cols },
      },
    })
    for (const sid of e.ids) {
      redirect.set(`pi:${sid}`, repId)
      redirect.set(`snk:${sid}`, repId)
    }
  }

  // (2) Subquery templates: subquery shapes keyed by their structural template (outer table +
  // predicate template + projection), values dropped. Members of a >1 group share one maintained
  // pipeline shape and differ only in their binding — so the whole per-instance pipeline collapses.
  const sigsOfShape = new Map<string, string[]>() // shape id → the subquery node sigs it depends on
  for (const e of g.subqueryEdges ?? []) {
    if (e.dependentKind !== 'shape') continue
    const arr = sigsOfShape.get(e.dependentId) ?? []
    arr.push(e.nodeSig)
    sigsOfShape.set(e.dependentId, arr)
  }
  const nodeBySig = new Map((g.subqueryNodes ?? []).map((n) => [n.sig, n]))
  const tpl = new Map<
    string,
    { outerTable: string; where: EngineGraph['shapes'][number]['where']; ids: string[]; sigs: Set<string> }
  >()
  for (const s of g.shapes ?? []) {
    if (!isSubqueryShape(s)) continue
    const key = subqueryTemplateKey(s)
    const e = tpl.get(key) ?? { outerTable: s.table, where: s.where, ids: [], sigs: new Set<string>() }
    e.ids.push(s.id)
    for (const sig of sigsOfShape.get(s.id) ?? []) e.sigs.add(sig)
    tpl.set(key, e)
  }
  for (const [key, e] of tpl) {
    if (e.ids.length <= 1) continue
    const sigs = [...e.sigs]
    // Labels come from a representative inner node — the members are structurally identical, so any
    // one of them names the inner table / projected column for the whole group.
    const firstNode = sigs.map((sig) => nodeBySig.get(sig)).find((n) => n !== undefined)
    const innerTable = firstNode?.innerTable ?? '?'
    const projCol = firstNode?.projCol ?? '?'
    const distId = `dist:sqgroup:${key}`
    const snkId = `snk:sqgroup:${key}`
    const ref: NodeRef = { kind: 'sqgroup', outerTable: e.outerTable, innerTable, projCol, shapeIds: e.ids, sigs }
    reps.push({
      id: distId,
      data: {
        kind: 'op-distinct',
        label: `distinct ${projCol}`,
        sub: `${innerTable} · ${e.ids.length} instances`,
        stack: true,
        ref,
      },
    })
    reps.push({
      id: snkId,
      data: {
        kind: 'op-sink',
        // The outer predicate template (`project_id IN (SELECT id FROM projects WHERE …)`), values
        // shown as `?` — the shape all members share.
        label: predicateTemplateLabel(e.where),
        highlight: true,
        sub: `${e.outerTable} · ${e.ids.length} shapes`,
        stack: true,
        ref,
      },
    })
    // The inner-set chain (σ inner where → π proj → distinct) collapses to the IN-SET ARRANGE rep…
    for (const sig of sigs) {
      redirect.set(`sqf:${sig}`, distId)
      redirect.set(`sqp:${sig}`, distId)
      redirect.set(`dist:${sig}`, distId)
    }
    // …and the outer chain (membership semijoin → π → feed set → sink) to the SINK rep.
    for (const sid of e.ids) {
      redirect.set(`sj:${sid}`, snkId)
      redirect.set(`pi:${sid}`, snkId)
      redirect.set(`feed:${sid}`, snkId)
      redirect.set(`snk:${sid}`, snkId)
    }
  }

  return { redirect, reps }
}

/** Collapse the repeated per-shape operator chains into their stacked representatives (see the
 *  block comment above `planGroups`). Returns the full circuit untouched when nothing groups. */
function collapse(
  full: { nodes: Map<string, RawNode>; edges: RawEdge[] },
  g: EngineGraph,
): { nodes: Map<string, RawNode>; edges: RawEdge[] } {
  const plan = planGroups(g)
  if (plan.reps.length === 0) return full

  // Nodes: drop every collapsed per-shape operator, add the representatives in their place.
  const nodes = new Map<string, RawNode>()
  for (const [id, n] of full.nodes) {
    if (!plan.redirect.has(id)) nodes.set(id, n)
  }
  for (const rep of plan.reps) nodes.set(rep.id, rep)

  // Edges: redirect each endpoint through the collapse map. An edge whose two ends land on the SAME
  // representative was an internal step of a collapsed chain (`pi`→`snk`, `sqf`→`sqp`→`dist`) — it
  // becomes a self-loop and is dropped. A redirected edge drops its per-shape label (the route join
  // fans one labelled edge per member; they must merge into one), and the whole set is deduped by
  // its rebuilt id (which carries the edge kind, so serve / lookup / flow between the same pair stay
  // distinct). Every representative endpoint exists, so no edge dangles.
  const edges: RawEdge[] = []
  const seen = new Set<string>()
  for (const e of full.edges) {
    const source = plan.redirect.get(e.source) ?? e.source
    const target = plan.redirect.get(e.target) ?? e.target
    if (source === target) continue
    const collapsed = source !== e.source || target !== e.target
    const label = collapsed ? undefined : e.label
    const id = `${source}~>${target}~${e.kind}~${label ?? ''}`
    if (seen.has(id)) continue
    seen.add(id)
    edges.push({ id, source, target, label, kind: e.kind })
  }
  return { nodes, edges }
}

/** Build laid-out React Flow nodes+edges for the circuit view. Same selection/focus semantics as
 *  the logical view: grouping (the shared `groupShapes` toggle) applies only to the whole-graph
 *  view — a selection restricts to shape SINKS, which a collapsed representative wouldn't carry, so
 *  it always expands to the individual operators (exactly like `buildGraph`). */
export function buildCircuit(
  g: EngineGraph,
  selection: 'all' | Set<string>,
  focus: string | null = null,
  opts?: BuildOpts,
): { nodes: Node[]; edges: Edge[] } {
  const full = buildFull(g)
  const grouped = opts?.groupShapes !== false && selection === 'all'
  const shown = selection === 'all' ? (grouped ? collapse(full, g) : full) : restrictToSelection(full, selection)
  return layout(shown, focus, opts)
}

/** hop id → rendered node ids. The engine stamps each operator with its trace-hop id, so expanding a
 *  trace hop (or a graph-diff id) to circuit nodes is a lookup, not a guess. With `group` on, an
 *  operator that a collapsed chain swallowed resolves to the representative that stands in for it —
 *  so a trace hop into any collapsed member still flashes the stacked group node (and pulses its
 *  edges). Must be called with the SAME `group` flag `buildCircuit` grouped under. */
export function hopIndex(g: EngineGraph, group = false): Map<string, string[]> {
  const redirect = group ? planGroups(g).redirect : null
  const m = new Map<string, string[]>()
  for (const o of g.operators ?? []) {
    const id = redirect?.get(o.id) ?? o.id
    if (!m.has(o.hop)) m.set(o.hop, [])
    const ids = m.get(o.hop)!
    // A hop can own several operators that all collapse to one representative (a member's `pi` and
    // `snk` both map to its group sink) — dedupe so the representative flashes once.
    if (!ids.includes(id)) ids.push(id)
  }
  return m
}
