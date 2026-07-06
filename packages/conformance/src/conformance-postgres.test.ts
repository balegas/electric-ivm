// Postgres-mode conformance: scenarios that specifically exercise the logical-replication ingestion
// path and the query-back backfill, beyond the generic op-stream coverage. Postgres is the system of
// record, so we also write to it directly (bypassing the applyOp helper) to prove the engine tracks
// the database itself — and still compare the materialized shape to the oracle (the same Postgres).

import type { Schema, ShapeDef } from '@electric-ivm/protocol'
import pgpkg from 'pg'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { formatCompare } from './compare.js'
import { applyOp, bootHarness, drainEngine, type Harness, waitForConvergence } from './harness.js'

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

describe('conformance: Postgres logical replication scenarios', () => {
  let h: Harness
  beforeAll(async () => {
    h = await bootHarness(schema)
  }, 60000)
  afterAll(async () => {
    await h?.shutdown()
  })

  // test_decoding emits text with `'`-escaping; the ingestor's column parser must round-trip quotes,
  // commas, spaces, backslashes and unicode exactly, or a predicate over `name` would diverge.
  it('decodes text values with quotes, commas, spaces and unicode', async () => {
    const tricky = [
      { id: 1, name: "O'Brien, Jr.", age: 40, active: true, score: 1.0 },
      { id: 2, name: 'a b  c', age: 41, active: true, score: 1.0 },
      { id: 3, name: 'back\\slash', age: 42, active: true, score: 1.0 },
      { id: 4, name: 'café ☃ 北京', age: 43, active: true, score: 1.0 },
      { id: 5, name: "it''s tricky", age: 44, active: false, score: 1.0 },
    ]
    const def: ShapeDef = { table: 'users', where: { col: 'active', op: 'eq', value: true } }
    const shape = await h.client.shape(def)
    for (const row of tricky) await applyOp(h, 'users', { op: 'insert', pk: row.id, row })
    await drainEngine(h)
    const res = await waitForConvergence(h, { shape, def, columns: COLUMNS, pk: 'id' })
    expect(res.equal, formatCompare(res)).toBe(true)
    // The exact tricky string must survive the round-trip into the client.
    const obrien = shape.currentRows().find((r) => String(r.id) === '1')
    expect(obrien?.name).toBe("O'Brien, Jr.")
  }, 60000)

  // Query-back backfill: rows that already exist in Postgres before a shape is created must be loaded
  // by the SELECT snapshot (not just live replication), including the very first row (LSN boundary).
  it('backfills pre-existing Postgres rows when a shape is created later', async () => {
    for (let id = 100; id < 130; id++) {
      await applyOp(h, 'users', {
        op: 'insert',
        pk: id,
        row: { id, name: id % 2 === 0 ? 'even' : 'odd', age: id, active: id % 2 === 0, score: 1.0 },
      })
    }
    await drainEngine(h)
    // Shape created AFTER the rows exist -> its initial state comes from the backfill snapshot.
    const def: ShapeDef = { table: 'users', where: { col: 'name', op: 'eq', value: 'even' } }
    const shape = await h.client.shape(def)
    const res = await waitForConvergence(h, { shape, def, columns: COLUMNS, pk: 'id' })
    expect(res.equal, formatCompare(res)).toBe(true)
    expect(shape.currentRows().length).toBe(15)
  }, 60000)

  // A multi-statement Postgres transaction commits atomically; logical decoding delivers the whole
  // commit as one batch, so the shape must reflect all rows together (never a partial commit).
  it('reflects a multi-row transaction atomically', async () => {
    const def: ShapeDef = { table: 'users', where: { col: 'age', op: 'gte', value: 200 } }
    const shape = await h.client.shape(def)

    const c = new pgpkg.Client({ connectionString: h.pgUrl })
    await c.connect()
    try {
      await c.query('BEGIN')
      for (let id = 200; id < 210; id++) {
        await c.query('INSERT INTO "users" (id, name, age, active, score) VALUES ($1,$2,$3,$4,$5)', [
          id,
          `txn-${id}`,
          id,
          true,
          1.0,
        ])
      }
      await c.query('COMMIT')
    } finally {
      await c.end()
    }

    await drainEngine(h)
    const res = await waitForConvergence(h, { shape, def, columns: COLUMNS, pk: 'id' })
    expect(res.equal, formatCompare(res)).toBe(true)
    expect(shape.currentRows().length).toBe(10)
  }, 60000)

  // Postgres is the source of record: an UPDATE/DELETE issued straight to the database (no API, no
  // applyOp) must flow through to the shape, with the engine deriving the delta from the replicated
  // old+new tuples (REPLICA IDENTITY FULL).
  it('tracks direct Postgres UPDATE/DELETE as the source of record', async () => {
    const def: ShapeDef = { table: 'users', where: { col: 'active', op: 'eq', value: true } }
    const shape = await h.client.shape(def)

    const c = new pgpkg.Client({ connectionString: h.pgUrl })
    await c.connect()
    try {
      await c.query('INSERT INTO "users" (id, name, age, active, score) VALUES (300,$1,30,true,1.0)', ['raw'])
      await c.query('INSERT INTO "users" (id, name, age, active, score) VALUES (301,$1,31,true,1.0)', ['raw'])
      await drainEngine(h)
      let res = await waitForConvergence(h, { shape, def, columns: COLUMNS, pk: 'id' })
      expect(res.equal, formatCompare(res)).toBe(true)
      expect(shape.currentRows().some((r) => String(r.id) === '300')).toBe(true)

      // Direct UPDATE -> row 300 leaves the shape; direct DELETE -> row 301 disappears.
      await c.query('UPDATE "users" SET active = false WHERE id = 300')
      await c.query('DELETE FROM "users" WHERE id = 301')
      await drainEngine(h)
      res = await waitForConvergence(h, { shape, def, columns: COLUMNS, pk: 'id' })
      expect(res.equal, formatCompare(res)).toBe(true)
      expect(shape.currentRows().some((r) => String(r.id) === '300' || String(r.id) === '301')).toBe(false)
    } finally {
      await c.end()
    }
  }, 60000)
})
