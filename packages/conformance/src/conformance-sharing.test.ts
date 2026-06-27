// Pipeline-sharing conformance: many shapes that differ only in an equality constant must (a) each
// still match pglite exactly, and (b) share ONE dbsp circuit per template rather than spawning N.
// Sharing is verified via the engine's `GET /tables/:name/families` introspection endpoint.
// See docs/superpowers/specs/2026-06-27-shape-pipeline-sharing-design.md.

import type { Schema, ShapeDef } from '@electric-lite/protocol'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { formatCompare } from './compare.js'
import { applyOp, bootHarness, drainEngine, type Harness, waitForConvergence } from './harness.js'

const schema: Schema = {
  tables: {
    users: {
      columns: {
        id: { type: 'int' },
        tenant: { type: 'int' },
        active: { type: 'bool' },
        age: { type: 'int' },
      },
      primaryKey: 'id',
    },
  },
}
const COLUMNS = ['id', 'tenant', 'active', 'age']
const N = 20 // number of `tenant = k` shapes sharing one family

interface Families {
  families: { key_cols: number[]; shapes: number }[]
  standalone: number
}
async function families(h: Harness): Promise<Families> {
  const res = await fetch(`${h.engineUrl}/tables/users/families`)
  if (!res.ok) throw new Error(`families endpoint -> ${res.status}`)
  return (await res.json()) as Families
}

describe('conformance: equality shapes share one family circuit', () => {
  let h: Harness
  beforeAll(async () => {
    h = await bootHarness(schema)
  }, 60000)
  afterAll(async () => {
    await h?.shutdown()
  })

  it('N tenant=k shapes share ONE circuit; each shape still matches the oracle', async () => {
    // N single-column equality shapes (one template) ...
    const eqDefs: ShapeDef[] = Array.from({ length: N }, (_, k) => ({
      table: 'users',
      where: { col: 'tenant', op: 'eq', value: k },
    }))
    // ... two range shapes (must stay standalone) ...
    const rangeDefs: ShapeDef[] = [
      { table: 'users', where: { col: 'age', op: 'gt', value: 30 } },
      { table: 'users', where: { col: 'age', op: 'lte', value: 10 } },
    ]
    // ... and two two-column equality shapes (a second, distinct template/family).
    const twoColDefs: ShapeDef[] = [3, 7].map((k) => ({
      table: 'users',
      where: { and: [{ col: 'tenant', op: 'eq', value: k }, { col: 'active', op: 'eq', value: true }] },
    }))
    const allDefs = [...eqDefs, ...rangeDefs, ...twoColDefs]
    const shapes = [] as { def: ShapeDef; shape: Awaited<ReturnType<Harness['client']['shape']>> }[]
    for (const def of allDefs) shapes.push({ def, shape: await h.client.shape(def) })

    // Deterministic data: 60 rows across tenants 0..N-1, then move some between tenants + a delete.
    for (let pk = 1; pk <= 60; pk++) {
      await applyOp(h, 'users', {
        op: 'insert',
        pk,
        row: { id: pk, tenant: pk % N, active: pk % 2 === 0, age: pk },
      })
    }
    for (let pk = 1; pk <= 20; pk++) {
      await applyOp(h, 'users', {
        op: 'update',
        pk,
        row: { id: pk, tenant: (pk + 5) % N, active: pk % 3 === 0, age: pk },
      })
    }
    await applyOp(h, 'users', { op: 'delete', pk: 7 })
    await drainEngine(h)

    // (a) correctness: every shape — shared family member, standalone, or two-column — matches pglite.
    for (const { def, shape } of shapes) {
      const res = await waitForConvergence(h, { shape, def, columns: COLUMNS, pk: 'id' })
      expect(res.equal, `${JSON.stringify(def.where)} -> ${formatCompare(res)}`).toBe(true)
    }

    // (b) sharing: 24 shapes are served by just 2 family circuits + 2 standalone circuits.
    const stats = await families(h)
    const counts = stats.families.map((f) => f.shapes).sort((a, b) => b - a)
    expect(counts).toContain(N) // the tenant=? template holds all N shapes in one circuit
    expect(counts).toContain(twoColDefs.length) // the (tenant,active) template holds both
    expect(stats.families.length).toBe(2) // exactly two equality templates -> two family circuits
    expect(stats.standalone).toBe(rangeDefs.length) // range shapes remain standalone
  }, 120000)
})
