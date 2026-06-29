// Concurrency conformance: shapes created WHILE writes are in flight from several independent
// Postgres connections. This guards the backfill <-> live-replication reconciliation: the backfill
// takes a snapshot at some LSN and the engine skips replicated changes already in that snapshot.
// The boundary must be the transaction COMMIT lsn (not the per-change record lsn), or a transaction
// whose change record precedes the snapshot but which COMMITS after it would be in neither the
// backfill nor the live stream -> a silently lost row. We assert every shape converges to the oracle.

import type { Schema, ShapeDef } from '@electric-lite/protocol'
import pgpkg from 'pg'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { formatCompare } from './compare.js'
import { bootHarness, drainEngine, type Harness, waitForConvergence } from './harness.js'

const schema: Schema = {
  tables: {
    users: {
      columns: {
        id: { type: 'int' },
        name: { type: 'text' },
        active: { type: 'bool' },
        n: { type: 'int' },
      },
      primaryKey: 'id',
    },
  },
}
const COLUMNS = ['id', 'name', 'active', 'n']
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

describe('conformance: shapes created during concurrent writes', () => {
  let h: Harness
  beforeAll(async () => {
    h = await bootHarness(schema)
  }, 60000)
  afterAll(async () => {
    await h?.shutdown()
  })

  it('every shape converges to the oracle despite in-flight concurrent writers', async () => {
    const WRITERS = 4
    const PER = 200

    // Independent connections so their transactions genuinely overlap (in-flight at snapshot time).
    const writers = Array.from({ length: WRITERS }, () => new pgpkg.Client({ connectionString: h.pgUrl }))
    await Promise.all(writers.map((c) => c.connect()))

    // Each writer owns a disjoint id range; one row per transaction maximizes commit interleaving.
    const writeAll = Promise.all(
      writers.map(async (c, w) => {
        for (let i = 0; i < PER; i++) {
          const id = 1_000_000 + w * PER + i
          await c.query('INSERT INTO "users" (id, name, active, n) VALUES ($1,$2,$3,$4)', [id, `w${w}`, i % 2 === 0, i])
        }
      }),
    )

    // Create shapes mid-flight: their backfill snapshots race the writers' uncommitted transactions.
    await sleep(25)
    const defs: ShapeDef[] = [
      { table: 'users' }, // match-all
      { table: 'users', where: { col: 'active', op: 'eq', value: true } },
      { table: 'users', where: { col: 'n', op: 'gte', value: 100 } },
    ]
    const shapes = await Promise.all(defs.map((d) => h.client.shape(d)))

    await writeAll
    await Promise.all(writers.map((c) => c.end()))
    await drainEngine(h)

    for (let i = 0; i < defs.length; i++) {
      const res = await waitForConvergence(h, { shape: shapes[i]!, def: defs[i]!, columns: COLUMNS, pk: 'id' }, 25000)
      expect(res.equal, `shape=${JSON.stringify(defs[i]!.where ?? 'ALL')}\n${formatCompare(res)}`).toBe(true)
    }
    // Sanity: the match-all shape must hold every written row (no silent loss).
    expect(shapes[0]!.currentRows().length).toBe(WRITERS * PER)
  }, 90000)
})
