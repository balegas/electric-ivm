// End-to-end conformance: drive electric-lite through the real tRPC API + streamdb client and
// assert the materialized shape set equals the pglite oracle for the same op stream.

import type { Schema, ShapeDef } from '@electric-lite/protocol'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { compareShapeSets, formatCompare } from './compare.js'
import { applyOp, bootHarness, drainEngine, type Harness, waitForConvergence } from './harness.js'
import { createSimulator, randomSeed } from './simulator.js'

const schema: Schema = {
  tables: {
    users: {
      columns: {
        id: { type: 'int' },
        name: { type: 'text' },
        age: { type: 'int' },
        active: { type: 'bool' },
        score: { type: 'float' },
      },
      primaryKey: 'id',
    },
  },
}
const COLUMNS = ['id', 'name', 'age', 'active', 'score']

describe('conformance: equality filters (M1)', () => {
  let h: Harness
  beforeAll(async () => {
    h = await bootHarness(schema)
  }, 60000)
  afterAll(async () => {
    await h?.shutdown()
  })

  it('multiple eq shapes converge to the oracle after a random op stream', async () => {
    const seed = process.env.SEED ? Number(process.env.SEED) : randomSeed()
    const defs: ShapeDef[] = [
      { table: 'users', where: { col: 'active', op: 'eq', value: true } },
      { table: 'users', where: { col: 'name', op: 'eq', value: 'alpha' } },
      { table: 'users' }, // match-all
    ]
    const shapes = await Promise.all(defs.map((d) => h.client.shape(d)))

    for (const { table, ev } of createSimulator(schema, { seed }).take(150)) {
      await applyOp(h, table, ev)
    }
    await drainEngine(h)

    for (let i = 0; i < defs.length; i++) {
      const res = await waitForConvergence(h, { shape: shapes[i]!, def: defs[i]!, columns: COLUMNS, pk: 'id' })
      expect(
        res.equal,
        `seed=${seed} shape=${JSON.stringify(defs[i]!.where ?? 'ALL')}\n${formatCompare(res)}`,
      ).toBe(true)
    }
  }, 60000)

  it('propagates live enter/leave through Postgres logical replication', async () => {
    const def: ShapeDef = { table: 'users', where: { col: 'active', op: 'eq', value: true } }
    const shape = await h.client.shape(def)

    // Seed a known-correct state: one active row in the shape.
    await applyOp(h, 'users', { op: 'insert', pk: 101, row: { id: 101, name: 'alpha', age: 30, active: true, score: 1.5 } })
    await drainEngine(h)
    let res = await waitForConvergence(h, { shape, def, columns: COLUMNS, pk: 'id' })
    expect(res.equal, formatCompare(res)).toBe(true)
    expect(shape.currentRows().some((r) => String(r.id) === '101')).toBe(true)

    // Live ENTER: a new active row appears once replication propagates the insert.
    await applyOp(h, 'users', { op: 'insert', pk: 102, row: { id: 102, name: 'bravo', age: 22, active: true, score: 2.5 } })
    await drainEngine(h)
    res = await waitForConvergence(h, { shape, def, columns: COLUMNS, pk: 'id' })
    expect(res.equal, formatCompare(res)).toBe(true)
    expect(shape.currentRows().some((r) => String(r.id) === '102')).toBe(true)

    // Live LEAVE: making row 101 inactive removes it (update carries old+new via replication).
    await applyOp(h, 'users', { op: 'update', pk: 101, row: { id: 101, name: 'alpha', age: 30, active: false, score: 1.5 } })
    await drainEngine(h)
    res = await waitForConvergence(h, { shape, def, columns: COLUMNS, pk: 'id' })
    expect(res.equal, formatCompare(res)).toBe(true)
    expect(shape.currentRows().some((r) => String(r.id) === '101')).toBe(false)
  }, 60000)
})
