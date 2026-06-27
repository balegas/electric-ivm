// Late shape registration: a client can define a new shape at any time, including after data
// already exists. The new shape must backfill to match the oracle. Two paths:
//   A) the FIRST shape on a table created after writes  -> tailer reads the backlog,
//   B) a shape added to a table whose tailer already ran -> backfill from current table state.

import type { Schema, ShapeDef } from '@electric-lite/protocol'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { formatCompare } from './compare.js'
import { applyOp, bootHarness, type Harness, waitForConvergence } from './harness.js'
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

describe('conformance: late shape registration (backfill)', () => {
  let h: Harness
  beforeAll(async () => {
    h = await bootHarness(schema)
  }, 60000)
  afterAll(async () => {
    await h?.shutdown()
  })

  it('A) the first shape on a table, created after writes, is populated from the backlog', async () => {
    const seed = process.env.SEED ? Number(process.env.SEED) : randomSeed()
    // No shape yet -> no tailer running. Writes accumulate in the table stream.
    for (const { table, ev } of createSimulator(schema, { seed }).take(120)) {
      await applyOp(h, table, ev)
    }
    const def: ShapeDef = { table: 'users', where: { col: 'score', op: 'gt', value: 500 } }
    const shape = await h.client.shape(def) // first shape -> tailer reads the backlog
    const res = await waitForConvergence(h, { shape, def, columns: COLUMNS, pk: 'id' })
    expect(res.equal, `seed=${seed}\n${formatCompare(res)}`).toBe(true)
  }, 60000)

  it('B) a shape added after a tailer has run backfills from current table state', async () => {
    const seed = (process.env.SEED ? Number(process.env.SEED) : randomSeed()) + 1
    // Warm-up shape starts the tailer; converge it so the backlog is fully consumed.
    const warmDef: ShapeDef = { table: 'users', where: { col: 'active', op: 'eq', value: true } }
    const warm = await h.client.shape(warmDef)
    for (const { table, ev } of createSimulator(schema, { seed }).take(120)) {
      await applyOp(h, table, ev)
    }
    expect((await waitForConvergence(h, { shape: warm, def: warmDef, columns: COLUMNS, pk: 'id' })).equal).toBe(true)

    // Now register a NEW shape after data exists -> must backfill from current table state.
    const lateDef: ShapeDef = { table: 'users', where: { col: 'age', op: 'gte', value: 18 } }
    const late = await h.client.shape(lateDef)
    const res = await waitForConvergence(h, { shape: late, def: lateDef, columns: COLUMNS, pk: 'id' })
    expect(res.equal, `seed=${seed}\n${formatCompare(res)}`).toBe(true)
  }, 60000)
})
