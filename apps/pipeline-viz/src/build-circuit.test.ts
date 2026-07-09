// Circuit-view shape grouping (the "group shapes" toggle). The engine reports one operator chain
// per shape; grouping collapses the repeated parallel chains — route-join families and structurally
// identical subquery pipelines — into stacked representatives, and ungrouping must reproduce the
// full circuit exactly. These tests drive the public surface (`buildCircuit` + `hopIndex`) against a
// hand-built `/graph` fixture whose operators mirror the engine's decomposition.

import { describe, expect, it } from 'vitest'

import { buildCircuit, hopIndex } from './build-circuit'
import type { VizNodeData } from './build-graph'
import type { EngineGraph, GraphEdge, GraphShape, OpEdge, OpNode } from './types'

const op = (id: string, kind: OpNode['kind'], hop: string, state: string | null = null): OpNode => ({
  id,
  kind,
  hop,
  state,
  label: id,
})
const flow = (source: string, target: string, label: string | null = null): OpEdge => ({
  source,
  target,
  kind: 'flow',
  label,
})

/** A fixture with all three per-shape strategies: a standalone shape (never grouped), a two-member
 *  route-join family, and two subquery shapes that share one pipeline template (owner = ?) with
 *  different bindings. The operators/opEdges reproduce what `build_circuit` emits in engine.rs. */
function fixture(): EngineGraph {
  const sigA = 'projects|id|owner=5'
  const sigB = 'projects|id|owner=8'
  const shapes: GraphShape[] = [
    {
      id: 's1',
      table: 'users',
      streamPath: 'shape/s1',
      changesOnly: false,
      where: { col: 'status', op: 'neq', value: 'done' },
      columns: null,
      familyKey: null,
      isSubquery: false,
      aggregate: null,
      state: 'active',
    },
    {
      id: 's2',
      table: 'users',
      streamPath: 'shape/s2',
      changesOnly: false,
      where: { col: 'active', op: 'eq', value: true },
      columns: null,
      familyKey: ['active'],
      isSubquery: false,
      aggregate: null,
      state: 'active',
    },
    {
      id: 's3',
      table: 'users',
      streamPath: 'shape/s3',
      changesOnly: false,
      where: { col: 'active', op: 'eq', value: false },
      columns: null,
      familyKey: ['active'],
      isSubquery: false,
      aggregate: null,
      state: 'active',
    },
    {
      id: 's55',
      table: 'issues',
      streamPath: 'shape/s55',
      changesOnly: false,
      where: { col: 'project_id', in: { table: 'projects', project: 'id', where: { col: 'owner', op: 'eq', value: 5 } } },
      columns: null,
      familyKey: null,
      isSubquery: true,
      aggregate: null,
      state: 'active',
    },
    {
      id: 's68',
      table: 'issues',
      streamPath: 'shape/s68',
      changesOnly: false,
      where: { col: 'project_id', in: { table: 'projects', project: 'id', where: { col: 'owner', op: 'eq', value: 8 } } },
      columns: null,
      familyKey: null,
      isSubquery: true,
      aggregate: null,
      state: 'active',
    },
  ]
  const subqueryNodes = [
    { sig: sigA, innerTable: 'projects', projCol: 'id', distinctValues: 3, refcount: 1 },
    { sig: sigB, innerTable: 'projects', projCol: 'id', distinctValues: 0, refcount: 1 },
  ]
  const subqueryEdges: GraphEdge[] = [
    { nodeSig: sigA, dependentKind: 'shape', dependentId: 's55', connectingCol: 'project_id', negated: false },
    { nodeSig: sigB, dependentKind: 'shape', dependentId: 's68', connectingCol: 'project_id', negated: false },
  ]
  const operators: OpNode[] = [
    op('src:users', 'source', 'table:users', 'table:users'),
    op('d:users', 'delta', 'table:users'),
    op('src:issues', 'source', 'table:issues', 'table:issues'),
    op('d:issues', 'delta', 'table:issues'),
    op('src:projects', 'source', 'table:projects', 'table:projects'),
    op('d:projects', 'delta', 'table:projects'),
    // standalone s1
    op('sigma:s1', 'filter', 'filter:s1', 'filter:s1'),
    op('pi:s1', 'project', 'shape:s1'),
    op('snk:s1', 'sink', 'shape:s1', 'shape:s1'),
    // route-join family (s2, s3)
    op('key:users:active', 'key', 'family:users:active'),
    op('arr:users:active', 'arrange', 'family:users:active', 'family:users:active'),
    op('rjoin:users:active', 'join', 'family:users:active'),
    op('pi:s2', 'project', 'shape:s2'),
    op('snk:s2', 'sink', 'shape:s2', 'shape:s2'),
    op('pi:s3', 'project', 'shape:s3'),
    op('snk:s3', 'sink', 'shape:s3', 'shape:s3'),
    // subquery shapes (s55, s68)
    op('sj:s55', 'join', 'shape:s55'),
    op('pi:s55', 'project', 'shape:s55'),
    op('snk:s55', 'sink', 'shape:s55', 'shape:s55'),
    op('sj:s68', 'join', 'shape:s68'),
    op('pi:s68', 'project', 'shape:s68'),
    op('snk:s68', 'sink', 'shape:s68', 'shape:s68'),
    // subquery inner-set nodes
    op('sqf:' + sigA, 'filter', 'node:' + sigA),
    op('sqp:' + sigA, 'project', 'node:' + sigA),
    op('dist:' + sigA, 'distinct', 'node:' + sigA, 'node:' + sigA),
    op('sqf:' + sigB, 'filter', 'node:' + sigB),
    op('sqp:' + sigB, 'project', 'node:' + sigB),
    op('dist:' + sigB, 'distinct', 'node:' + sigB, 'node:' + sigB),
  ]
  const opEdges: OpEdge[] = [
    flow('src:users', 'd:users'),
    flow('src:issues', 'd:issues'),
    flow('src:projects', 'd:projects'),
    flow('d:users', 'sigma:s1'),
    flow('sigma:s1', 'pi:s1'),
    flow('pi:s1', 'snk:s1'),
    flow('d:users', 'key:users:active'),
    flow('key:users:active', 'rjoin:users:active'),
    { source: 'arr:users:active', target: 'rjoin:users:active', kind: 'state', label: null },
    flow('rjoin:users:active', 'pi:s2', 's2'),
    flow('pi:s2', 'snk:s2'),
    flow('rjoin:users:active', 'pi:s3', 's3'),
    flow('pi:s3', 'snk:s3'),
    flow('d:issues', 'sj:s55'),
    flow('sj:s55', 'pi:s55'),
    flow('pi:s55', 'snk:s55'),
    flow('d:issues', 'sj:s68'),
    flow('sj:s68', 'pi:s68'),
    flow('pi:s68', 'snk:s68'),
    flow('d:projects', 'sqf:' + sigA),
    flow('sqf:' + sigA, 'sqp:' + sigA),
    flow('sqp:' + sigA, 'dist:' + sigA),
    flow('d:projects', 'sqf:' + sigB),
    flow('sqf:' + sigB, 'sqp:' + sigB),
    flow('sqp:' + sigB, 'dist:' + sigB),
    { source: 'dist:' + sigA, target: 'sj:s55', kind: 'subquery', label: 'IN · project_id' },
    { source: 'dist:' + sigB, target: 'sj:s68', kind: 'subquery', label: 'IN · project_id' },
  ]
  return { tables: ['users', 'issues', 'projects'], shapes, subqueryNodes, subqueryEdges, operators, opEdges }
}

const idset = (r: { nodes: { id: string }[] }) => new Set(r.nodes.map((n) => n.id))
const refKinds = (r: { nodes: { data: unknown }[] }) => r.nodes.map((n) => (n.data as VizNodeData).ref.kind)
const dataOf = (r: { nodes: { id: string; data: unknown }[] }, id: string) =>
  r.nodes.find((n) => n.id === id)!.data as VizNodeData

function assertWellFormed(r: { nodes: { id: string }[]; edges: { id: string; source: string; target: string }[] }) {
  const ids = idset(r)
  // No node appears twice.
  expect(ids.size).toBe(r.nodes.length)
  // No edge id collides, and every endpoint resolves to a rendered node (nothing dangles).
  expect(new Set(r.edges.map((e) => e.id)).size).toBe(r.edges.length)
  for (const e of r.edges) {
    expect(ids.has(e.source)).toBe(true)
    expect(ids.has(e.target)).toBe(true)
  }
}

describe('circuit grouping', () => {
  it('collapses a route-join family to one stacked sink', () => {
    const g = fixture()
    const grouped = buildCircuit(g, 'all', null, { groupShapes: true })
    const ids = idset(grouped)

    expect(ids.has('snk:group:users:active')).toBe(true)
    for (const gone of ['pi:s2', 'snk:s2', 'pi:s3', 'snk:s3']) expect(ids.has(gone)).toBe(false)

    const rep = dataOf(grouped, 'snk:group:users:active')
    expect(rep.stack).toBe(true)
    expect(rep.sub).toBe('users · 2 shapes')
    expect(rep.ref).toEqual({ kind: 'shapegroup', table: 'users', keyCols: ['active'] })
    // The shared family operators are NOT collapsed.
    expect(ids.has('rjoin:users:active')).toBe(true)
    expect(ids.has('arr:users:active')).toBe(true)
    assertWellFormed(grouped)
  })

  it('collapses same-template subquery pipelines to one stacked inner-set + one stacked sink', () => {
    const g = fixture()
    const grouped = buildCircuit(g, 'all', null, { groupShapes: true })
    const ids = idset(grouped)

    // Exactly one sqgroup dist (IN-SET ARRANGE) and one sqgroup sink, standing in for two instances.
    expect(refKinds(grouped).filter((k) => k === 'sqgroup')).toHaveLength(2)
    for (const gone of [
      'sj:s55', 'pi:s55', 'snk:s55', 'sj:s68', 'pi:s68', 'snk:s68',
      'dist:projects|id|owner=5', 'sqf:projects|id|owner=5', 'dist:projects|id|owner=8',
    ]) {
      expect(ids.has(gone)).toBe(false)
    }
    const distRep = grouped.nodes.find(
      (n) => (n.data as VizNodeData).ref.kind === 'sqgroup' && (n.data as VizNodeData).kind === 'op-distinct',
    )!
    const snkRep = grouped.nodes.find(
      (n) => (n.data as VizNodeData).ref.kind === 'sqgroup' && (n.data as VizNodeData).kind === 'op-sink',
    )!
    expect((distRep.data as VizNodeData).sub).toBe('projects · 2 instances')
    expect((snkRep.data as VizNodeData).sub).toBe('issues · 2 shapes')
    // The membership edge survives, redirected between the two representatives (no dangle).
    expect(grouped.edges.some((e) => e.source === distRep.id && e.target === snkRep.id)).toBe(true)
    assertWellFormed(grouped)
  })

  it('leaves the standalone shape untouched', () => {
    const grouped = buildCircuit(fixture(), 'all', null, { groupShapes: true })
    const ids = idset(grouped)
    expect(ids.has('sigma:s1')).toBe(true)
    expect(ids.has('snk:s1')).toBe(true)
  })

  it('ungrouping reproduces the full circuit exactly', () => {
    const g = fixture()
    const full = buildCircuit(g, 'all', null, { groupShapes: false })
    const ids = idset(full)
    // Every per-shape operator is back; no representative remains.
    for (const want of ['pi:s2', 'snk:s2', 'pi:s3', 'snk:s3', 'sj:s55', 'snk:s68', 'dist:projects|id|owner=5']) {
      expect(ids.has(want)).toBe(true)
    }
    expect(refKinds(full).some((k) => k === 'sqgroup' || k === 'shapegroup')).toBe(false)
    // The ungrouped node set matches the engine's operator count exactly.
    expect(full.nodes).toHaveLength(g.operators!.length)
    assertWellFormed(full)
    // Grouping strictly reduces the node count.
    const grouped = buildCircuit(g, 'all', null, { groupShapes: true })
    expect(grouped.nodes.length).toBeLessThan(full.nodes.length)
  })

  it('a selection always expands, ignoring the group toggle', () => {
    const grouped = buildCircuit(fixture(), new Set(['s2']), null, { groupShapes: true })
    const ids = idset(grouped)
    expect(ids.has('snk:s2')).toBe(true) // individual sink kept…
    expect(ids.has('snk:group:users:active')).toBe(false) // …not the group representative
  })

  it('maps a hop into a collapsed member onto its stacked representative', () => {
    const g = fixture()
    const grouped = buildCircuit(g, 'all', null, { groupShapes: true })
    const groupedIds = idset(grouped)
    const idx = hopIndex(g, true)

    // A route-family member's shape hop resolves to the one family sink, deduped (its pi + snk both
    // collapse there) — and that node is actually present in the grouped render, so it flashes.
    expect(idx.get('shape:s2')).toEqual(['snk:group:users:active'])
    expect(groupedIds.has('snk:group:users:active')).toBe(true)

    // A subquery member's shape hop resolves to the single sqgroup sink; its inner node hop to the
    // single sqgroup dist. Both are present in the grouped render.
    const shapeHop = idx.get('shape:s55')!
    expect(shapeHop).toHaveLength(1)
    expect(shapeHop[0]).toMatch(/^snk:sqgroup:/)
    expect(groupedIds.has(shapeHop[0]!)).toBe(true)

    const nodeHop = idx.get('node:projects|id|owner=5')!
    expect(nodeHop).toHaveLength(1)
    expect(nodeHop[0]).toMatch(/^dist:sqgroup:/)
    expect(groupedIds.has(nodeHop[0]!)).toBe(true)
  })

  it('without grouping, a hop expands to the individual operators', () => {
    const idx = hopIndex(fixture(), false)
    expect(idx.get('shape:s2')).toEqual(['pi:s2', 'snk:s2'])
    expect(idx.get('node:projects|id|owner=5')).toEqual([
      'sqf:projects|id|owner=5',
      'sqp:projects|id|owner=5',
      'dist:projects|id|owner=5',
    ])
  })
})
