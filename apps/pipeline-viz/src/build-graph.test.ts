// Logical-view shape grouping (the "group shapes" toggle). `buildGraph` renders the engine's
// logical node set directly; grouping collapses the repeated per-shape fan-outs — route-join
// families AND structurally identical subquery pipelines — into stacked representatives, and
// ungrouping must restore every individual node. These tests drive the public surface (`buildGraph`)
// against a hand-built `/graph` fixture, focusing on the subquery-template fold (the circuit view's
// fold is covered in build-circuit.test.ts; both key on the shared `subqueryTemplateKey`).

import { describe, expect, it } from 'vitest'

import { type VizNodeData, buildGraph } from './build-graph'
import { predicateTemplate, subqueryTemplateKey } from './predicate-label'
import type { EngineGraph, GraphEdge, GraphShape } from './types'

const sigA = 'project_members|project_id|user_id=208'
const sigB = 'project_members|project_id|user_id=0'

/** Two subquery shapes on `issues` that share one pipeline template
 *  (`project_id IN (SELECT project_id FROM project_members WHERE user_id = ?)`) but differ in their
 *  bound `user_id` — the exact s55/s68 case from the bug report — plus a two-member route-join
 *  family so grouping of the two dimensions can be checked independently. */
function fixture(): EngineGraph {
  const membership = (uid: number): GraphShape['where'] => ({
    col: 'project_id',
    in: { table: 'project_members', project: 'project_id', where: { col: 'user_id', op: 'eq', value: uid } },
  })
  const shapes: GraphShape[] = [
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
      where: membership(208),
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
      where: membership(0),
      columns: null,
      familyKey: null,
      isSubquery: true,
      aggregate: null,
      state: 'active',
    },
  ]
  const subqueryNodes = [
    { sig: sigA, innerTable: 'project_members', projCol: 'project_id', distinctValues: 3, refcount: 1 },
    { sig: sigB, innerTable: 'project_members', projCol: 'project_id', distinctValues: 0, refcount: 1 },
  ]
  const subqueryEdges: GraphEdge[] = [
    { nodeSig: sigA, dependentKind: 'shape', dependentId: 's55', connectingCol: 'project_id', negated: false },
    { nodeSig: sigB, dependentKind: 'shape', dependentId: 's68', connectingCol: 'project_id', negated: false },
  ]
  return { tables: ['users', 'issues', 'project_members'], shapes, subqueryNodes, subqueryEdges }
}

const idset = (r: { nodes: { id: string }[] }) => new Set(r.nodes.map((n) => n.id))
const refKinds = (r: { nodes: { data: unknown }[] }) => r.nodes.map((n) => (n.data as VizNodeData).ref.kind)

function assertWellFormed(r: { nodes: { id: string }[]; edges: { id: string; source: string; target: string }[] }) {
  const ids = idset(r)
  expect(ids.size).toBe(r.nodes.length) // no node appears twice
  expect(new Set(r.edges.map((e) => e.id)).size).toBe(r.edges.length) // no edge id collides
  for (const e of r.edges) {
    expect(ids.has(e.source)).toBe(true) // nothing dangles
    expect(ids.has(e.target)).toBe(true)
  }
}

describe('logical grouping', () => {
  it('collapses same-template subquery shapes to one stacked output + one stacked inner-set', () => {
    const grouped = buildGraph(fixture(), 'all', null, { groupShapes: true })
    const ids = idset(grouped)

    // The two per-instance nodes (outer shape + inner set) are gone for both members…
    for (const gone of [`shape:s55`, `shape:s68`, `node:${sigA}`, `node:${sigB}`]) {
      expect(ids.has(gone)).toBe(false)
    }
    // …replaced by exactly two `sqgroup` representatives: the stacked output and the stacked inner set.
    expect(refKinds(grouped).filter((k) => k === 'sqgroup')).toHaveLength(2)

    const outRep = grouped.nodes.find(
      (n) => (n.data as VizNodeData).ref.kind === 'sqgroup' && (n.data as VizNodeData).kind === 'shape',
    )!
    const inRep = grouped.nodes.find(
      (n) => (n.data as VizNodeData).ref.kind === 'sqgroup' && (n.data as VizNodeData).kind === 'sqnode',
    )!
    expect((outRep.data as VizNodeData).stack).toBe(true)
    expect((outRep.data as VizNodeData).sub).toBe('issues · 2 shapes')
    expect((inRep.data as VizNodeData).stack).toBe(true)
    expect((inRep.data as VizNodeData).sub).toBe('distinct project_id · 2 instances')

    // The membership edge survives, redirected between the two representatives (no dangle).
    expect(grouped.edges.some((e) => e.source === inRep.id && e.target === outRep.id)).toBe(true)
    // The detail panel re-derives members + inner instances from the ref — both members are carried.
    const ref = (outRep.data as VizNodeData).ref as Extract<VizNodeData['ref'], { kind: 'sqgroup' }>
    expect(new Set(ref.shapeIds)).toEqual(new Set(['s55', 's68']))
    expect(new Set(ref.sigs)).toEqual(new Set([sigA, sigB]))
    assertWellFormed(grouped)
  })

  it('ungrouping restores every individual subquery node', () => {
    const full = buildGraph(fixture(), 'all', null, { groupShapes: false })
    const ids = idset(full)
    for (const want of [`shape:s55`, `shape:s68`, `node:${sigA}`, `node:${sigB}`]) {
      expect(ids.has(want)).toBe(true)
    }
    expect(refKinds(full).some((k) => k === 'sqgroup')).toBe(false)
    assertWellFormed(full)
    // Grouping strictly reduces the node count (two per-instance pairs → two reps).
    const grouped = buildGraph(fixture(), 'all', null, { groupShapes: true })
    expect(grouped.nodes.length).toBeLessThan(full.nodes.length)
  })

  it('a selection always expands, ignoring the group toggle', () => {
    const sel = buildGraph(fixture(), new Set(['s55']), null, { groupShapes: true })
    const ids = idset(sel)
    expect(ids.has('shape:s55')).toBe(true) // the individual shape is kept…
    expect(refKinds(sel).some((k) => k === 'sqgroup')).toBe(false) // …not a group representative
  })

  it('leaves a lone subquery shape untouched (needs >1 member to group)', () => {
    const g = fixture()
    g.shapes = g.shapes.filter((s) => s.id !== 's68')
    g.subqueryNodes = g.subqueryNodes.filter((n) => n.sig !== sigB)
    g.subqueryEdges = g.subqueryEdges.filter((e) => e.nodeSig !== sigB)
    const grouped = buildGraph(g, 'all', null, { groupShapes: true })
    const ids = idset(grouped)
    expect(ids.has('shape:s55')).toBe(true)
    expect(ids.has(`node:${sigA}`)).toBe(true)
    expect(refKinds(grouped).some((k) => k === 'sqgroup')).toBe(false)
  })
})

describe('subquery template key', () => {
  it('two bindings of one subquery predicate share the template', () => {
    const g = fixture()
    const s55 = g.shapes.find((s) => s.id === 's55')!
    const s68 = g.shapes.find((s) => s.id === 's68')!
    // The value is dropped from the template, so both bindings collapse to one key.
    expect(predicateTemplate(s55.where)).toBe(predicateTemplate(s68.where))
    expect(subqueryTemplateKey(s55)).toBe(subqueryTemplateKey(s68))
    // A different projection ORDER must not split the group (the key sorts columns).
    expect(subqueryTemplateKey({ ...s55, columns: ['a', 'b'] })).toBe(
      subqueryTemplateKey({ ...s68, columns: ['b', 'a'] }),
    )
  })
})
