// Outer-shape de-duplication: two *equal* shapes (same kind + definition) must collapse to ONE engine
// shape (one stream, one maintained circuit), ref-counted — regardless of kind. Covers materialized
// row shapes, subquery shapes, and aggregations. Complements conformance-subquery-sharing.test.ts,
// which asserts inner-node sharing. Asserted via the engine's GET /graph introspection.

import type { AggregateDef, Predicate, Schema, ShapeDef } from '@electric-lite/protocol'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { bootHarness, drainEngine, type Harness } from './harness.js'

interface GraphShape {
  id: string
  table: string
  where: Predicate | null
  columns: string[] | null
  changesOnly: boolean
  isSubquery: boolean
  aggregate: { func: string; col: string | null } | null
}

async function graphShapes(h: Harness): Promise<GraphShape[]> {
  const res = await fetch(`${h.engineUrl}/graph`)
  return ((await res.json()) as { shapes: GraphShape[] }).shapes
}
const sameWhere = (a: Predicate | null, b: Predicate) => JSON.stringify(a) === JSON.stringify(b)

const schema: Schema = {
  tables: {
    parent: { columns: { id: { type: 'int' }, active: { type: 'bool' }, score: { type: 'float' } }, primaryKey: 'id' },
    child: { columns: { id: { type: 'int' }, parent_id: { type: 'int' } }, primaryKey: 'id' },
  },
}
const activeParents = { table: 'parent', project: 'id', where: { col: 'active', op: 'eq', value: true } } as const

describe('conformance: equal shapes share one engine shape', () => {
  let h: Harness
  beforeAll(async () => {
    h = await bootHarness(schema)
  }, 60000)
  afterAll(async () => await h?.shutdown())

  it('two identical materialized row shapes collapse to one', async () => {
    const where: Predicate = { col: 'active', op: 'eq', value: true }
    const def: ShapeDef = { table: 'parent', where }
    await h.client.shape(def)
    await h.client.shape(def) // byte-identical -> must join the first
    await drainEngine(h)
    const matching = (await graphShapes(h)).filter(
      (s) => s.table === 'parent' && !s.aggregate && !s.isSubquery && sameWhere(s.where, where),
    )
    expect(matching.length).toBe(1)
  }, 60000)

  it('two identical subquery shapes collapse to one', async () => {
    const where: Predicate = { col: 'parent_id', in: activeParents }
    const def: ShapeDef = { table: 'child', where }
    await h.client.shape(def)
    await h.client.shape(def)
    await drainEngine(h)
    // Only this test creates `child` shapes; the two identical subquery shapes must collapse to one.
    const matching = (await graphShapes(h)).filter((s) => s.table === 'child' && s.isSubquery)
    expect(matching.length).toBe(1)
  }, 60000)

  it('two identical aggregations collapse to one', async () => {
    const def: AggregateDef = { table: 'parent', where: { col: 'active', op: 'eq', value: true }, fn: 'count' }
    await h.client.aggregate(def)
    await h.client.aggregate(def)
    await drainEngine(h)
    const aggs = (await graphShapes(h)).filter((s) => s.aggregate?.func === 'count' && s.table === 'parent')
    expect(aggs.length).toBe(1)
  }, 60000)

  it('shapes that differ (predicate / columns / kind) are NOT shared', async () => {
    // distinct predicate
    await h.client.shape({ table: 'parent', where: { col: 'active', op: 'eq', value: false } })
    // distinct projection (columns) over an already-registered predicate
    await h.client.shape({ table: 'parent', where: { col: 'active', op: 'eq', value: true }, columns: ['id'] })
    // an aggregate is not the same as a row shape over the same predicate
    await h.client.aggregate({ table: 'parent', where: { col: 'active', op: 'eq', value: false }, fn: 'count' })
    await drainEngine(h)
    const shapes = await graphShapes(h)
    expect(shapes.filter((s) => s.table === 'parent' && !s.aggregate && !s.isSubquery && sameWhere(s.where, { col: 'active', op: 'eq', value: false })).length).toBe(1)
    expect(shapes.filter((s) => s.table === 'parent' && s.columns?.length === 1).length).toBe(1)
    expect(shapes.filter((s) => s.aggregate?.func === 'count' && sameWhere(s.where, { col: 'active', op: 'eq', value: false })).length).toBe(1)
  }, 60000)
})
