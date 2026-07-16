// System / op-coverage conformance: deterministic sequences that stress the change-application
// machinery — enter/leave churn, pk-changing "updates", re-insert of a deleted pk, idempotent and
// redundant ops, high-churn over a tiny pk space, and multiple shapes across multiple tables.

import type { Schema, ShapeDef } from '@electric-circuits/protocol'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { formatCompare } from './compare.js'
import { applyOp, bootHarness, drainEngine, type Harness, waitForConvergence } from './harness.js'
import { createSimulator } from './simulator.js'

const usersSchema: Schema = {
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
const mk = (id: number, active: boolean, age = 20): import('@electric-circuits/protocol').Row => ({
  id,
  name: 'alpha',
  age,
  active,
  score: 1.0,
})

describe('conformance: op-application transitions', () => {
  let h: Harness
  beforeAll(async () => {
    h = await bootHarness(usersSchema)
  }, 60000)
  afterAll(async () => {
    await h?.shutdown()
  })

  it('IN->OUT->IN... churn of one pk converges to correct final membership', async () => {
    const def: ShapeDef = { table: 'users', where: { col: 'active', op: 'eq', value: true } }
    const shape = await h.client.shape(def)
    // 12 toggles (IN/OUT/IN/...), then one explicit final flip to active=true -> ends IN the shape.
    for (let i = 0; i < 12; i++) {
      await applyOp(h, 'users', { op: i === 0 ? 'insert' : 'update', pk: 1, row: mk(1, i % 2 === 0) })
    }
    await applyOp(h, 'users', { op: 'update', pk: 1, row: mk(1, true) })
    await drainEngine(h)
    const res = await waitForConvergence(h, { shape, def, columns: COLUMNS, pk: 'id' })
    expect(res.equal, formatCompare(res)).toBe(true)
    expect(shape.currentRows().some((r) => String(r.id) === '1')).toBe(true)
  }, 60000)

  it('an "update" carrying a different pk upserts a new key; the old key remains', async () => {
    const def: ShapeDef = { table: 'users' } // match-all
    const shape = await h.client.shape(def)
    await applyOp(h, 'users', { op: 'insert', pk: 10, row: mk(10, true) })
    // "update" whose row has a different pk -> a new row at pk 11 (keyed by the event pk).
    await applyOp(h, 'users', { op: 'update', pk: 11, row: mk(11, true) })
    await drainEngine(h)
    const res = await waitForConvergence(h, { shape, def, columns: COLUMNS, pk: 'id' })
    expect(res.equal, formatCompare(res)).toBe(true)
    const ids = shape.currentRows().map((r) => String(r.id))
    expect(ids).toContain('10')
    expect(ids).toContain('11')
  }, 60000)

  it('re-insert of a deleted pk brings the row back', async () => {
    const def: ShapeDef = { table: 'users', where: { col: 'active', op: 'eq', value: true } }
    const shape = await h.client.shape(def)
    await applyOp(h, 'users', { op: 'insert', pk: 20, row: mk(20, true) })
    await applyOp(h, 'users', { op: 'delete', pk: 20 })
    await applyOp(h, 'users', { op: 'insert', pk: 20, row: mk(20, true) })
    await drainEngine(h)
    const res = await waitForConvergence(h, { shape, def, columns: COLUMNS, pk: 'id' })
    expect(res.equal, formatCompare(res)).toBe(true)
    expect(shape.currentRows().some((r) => String(r.id) === '20')).toBe(true)
  }, 60000)

  it('idempotent inserts and redundant deletes are no-ops', async () => {
    const def: ShapeDef = { table: 'users' }
    const shape = await h.client.shape(def)
    // Same row inserted 3x -> one row.
    for (let i = 0; i < 3; i++) await applyOp(h, 'users', { op: 'insert', pk: 30, row: mk(30, true) })
    // Delete a key that never existed, twice -> nothing happens.
    await applyOp(h, 'users', { op: 'delete', pk: 999 })
    await applyOp(h, 'users', { op: 'delete', pk: 999 })
    await drainEngine(h)
    const res = await waitForConvergence(h, { shape, def, columns: COLUMNS, pk: 'id' })
    expect(res.equal, formatCompare(res)).toBe(true)
    expect(shape.currentRows().filter((r) => String(r.id) === '30').length).toBe(1)
  }, 60000)

  it('high-churn over a tiny pk space converges across several shapes', async () => {
    const defs: ShapeDef[] = [
      { table: 'users', where: { col: 'active', op: 'eq', value: true } },
      { table: 'users', where: { col: 'age', op: 'gte', value: 500 } },
      { table: 'users' },
    ]
    const shapes = await Promise.all(defs.map((d) => h.client.shape(d)))
    // pkSpace=3 -> massive upsert/delete overlap on the same handful of keys.
    for (const { table, ev } of createSimulator(usersSchema, { seed: 4242, pkSpace: 3 }).take(400)) {
      await applyOp(h, table, ev)
    }
    await drainEngine(h)
    for (let i = 0; i < defs.length; i++) {
      const res = await waitForConvergence(h, { shape: shapes[i]!, def: defs[i]!, columns: COLUMNS, pk: 'id' })
      expect(res.equal, `shape=${JSON.stringify(defs[i]!.where ?? 'ALL')}\n${formatCompare(res)}`).toBe(true)
    }
  }, 90000)
})

const multiSchema: Schema = {
  tables: {
    users: {
      columns: { id: { type: 'int' }, name: { type: 'text' }, age: { type: 'int' }, active: { type: 'bool' }, score: { type: 'float' } },
      primaryKey: 'id',
    },
    items: {
      columns: { sku: { type: 'int' }, tag: { type: 'text' }, qty: { type: 'int' } },
      primaryKey: 'sku',
    },
  },
}

describe('conformance: multiple tables and shapes', () => {
  let h: Harness
  beforeAll(async () => {
    h = await bootHarness(multiSchema)
  }, 60000)
  afterAll(async () => {
    await h?.shutdown()
  })

  it('interleaved ops over two tables each converge per shape', async () => {
    const userDefs: ShapeDef[] = [
      { table: 'users', where: { col: 'active', op: 'eq', value: true } },
      { table: 'users' },
    ]
    const itemDefs: ShapeDef[] = [
      { table: 'items', where: { col: 'qty', op: 'gt', value: 500 } },
      { table: 'items', where: { col: 'tag', op: 'eq', value: 'alpha' } },
    ]
    const userShapes = await Promise.all(userDefs.map((d) => h.client.shape(d)))
    const itemShapes = await Promise.all(itemDefs.map((d) => h.client.shape(d)))

    // The simulator randomly targets either table.
    for (const { table, ev } of createSimulator(multiSchema, { seed: 7, pkSpace: 12 }).take(300)) {
      await applyOp(h, table, ev)
    }
    await drainEngine(h)

    for (let i = 0; i < userDefs.length; i++) {
      const res = await waitForConvergence(h, { shape: userShapes[i]!, def: userDefs[i]!, columns: COLUMNS, pk: 'id' })
      expect(res.equal, `users shape#${i}\n${formatCompare(res)}`).toBe(true)
    }
    const itemCols = ['sku', 'tag', 'qty']
    for (let i = 0; i < itemDefs.length; i++) {
      const res = await waitForConvergence(h, { shape: itemShapes[i]!, def: itemDefs[i]!, columns: itemCols, pk: 'sku' })
      expect(res.equal, `items shape#${i}\n${formatCompare(res)}`).toBe(true)
    }
  }, 90000)
})
