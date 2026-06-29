// Property-style subquery convergence: register a matrix of subquery shapes (1/2/3-level, tag
// subqueries, NOT IN, and compositions with AND/OR/NOT + atomics) over the multi-level schema, drive a
// deterministic FK-respecting mutation stream, and assert every shape converges to the pg oracle (which
// evaluates the subquery natively). This is the engine's analog of Electric's oracle property test.

import type { Predicate, ShapeDef } from '@electric-lite/protocol'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { formatCompare } from './compare.js'
import { applyOp, bootHarness, drainEngine, type Harness, waitForConvergence } from './harness.js'
import { seedOps, subqueryMutations, subquerySchema } from './subquery-schema.js'

const L4_COLS = ['id', 'level_3_id', 'value']

// Subquery building blocks over the level_N hierarchy (all shapes are on level_4).
const activeL3: Predicate = { col: 'level_3_id', in: { table: 'level_3', project: 'id', where: { col: 'active', op: 'eq', value: true } } }
const l3ViaActiveL2: Predicate = {
  col: 'level_3_id',
  in: { table: 'level_3', project: 'id', where: { col: 'level_2_id', in: { table: 'level_2', project: 'id', where: { col: 'active', op: 'eq', value: true } } } },
}
const l3ViaActiveL1: Predicate = {
  col: 'level_3_id',
  in: {
    table: 'level_3',
    project: 'id',
    where: {
      col: 'level_2_id',
      in: {
        table: 'level_2',
        project: 'id',
        where: { col: 'level_1_id', in: { table: 'level_1', project: 'id', where: { col: 'active', op: 'eq', value: true } } },
      },
    },
  },
}
const l3WithAlphaTag: Predicate = { col: 'level_3_id', in: { table: 'level_3_tags', project: 'level_3_id', where: { col: 'tag', op: 'eq', value: 'alpha' } } }
const l3ViaL2AlphaTag: Predicate = {
  col: 'level_3_id',
  in: { table: 'level_3', project: 'id', where: { col: 'level_2_id', in: { table: 'level_2_tags', project: 'level_2_id', where: { col: 'tag', op: 'eq', value: 'alpha' } } } },
}
const notActiveL3: Predicate = { col: 'level_3_id', negated: true, in: { table: 'level_3', project: 'id', where: { col: 'active', op: 'eq', value: true } } }

const CASES: { label: string; where: Predicate }[] = [
  { label: '1-level active', where: activeL3 },
  { label: '2-level active', where: l3ViaActiveL2 },
  { label: '3-level active', where: l3ViaActiveL1 },
  { label: 'tag (1-level)', where: l3WithAlphaTag },
  { label: 'tag (2-level)', where: l3ViaL2AlphaTag },
  { label: 'NOT IN active', where: notActiveL3 },
  { label: 'subquery AND value range', where: { and: [activeL3, { col: 'value', op: 'gte', value: 'v2' }] } },
  { label: 'subquery OR atomic', where: { or: [activeL3, { col: 'value', op: 'eq', value: 'v0' }] } },
  { label: 'NOT (subquery)', where: { not: activeL3 } },
  { label: 'two subqueries ANDed', where: { and: [activeL3, l3WithAlphaTag] } },
]

describe('conformance: subquery matrix', () => {
  let h: Harness
  beforeAll(async () => {
    h = await bootHarness(subquerySchema)
  }, 60000)
  afterAll(async () => {
    await h?.shutdown()
  })

  it('all subquery shapes converge through a deterministic mutation stream', async () => {
    const seed = process.env.SEED ? Number(process.env.SEED) : 12345
    const { ops, state } = seedOps()
    for (const { table, ev } of ops) await applyOp(h, table, ev)

    const defs: ShapeDef[] = CASES.map((c) => ({ table: 'level_4', where: c.where }))
    const shapes = await Promise.all(defs.map((d) => h.client.shape(d)))
    await drainEngine(h)

    // Initial convergence.
    for (let i = 0; i < CASES.length; i++) {
      const res = await waitForConvergence(h, { shape: shapes[i]!, def: defs[i]!, columns: L4_COLS, pk: 'id' })
      expect(res.equal, `seed=${seed} initial case="${CASES[i]!.label}"\n${formatCompare(res)}`).toBe(true)
    }

    // Drive mutations, checking convergence periodically.
    const gen = subqueryMutations(state, seed)
    for (let round = 0; round < 6; round++) {
      for (const { table, ev } of gen.take(15)) await applyOp(h, table, ev)
      await drainEngine(h)
      for (let i = 0; i < CASES.length; i++) {
        const res = await waitForConvergence(h, { shape: shapes[i]!, def: defs[i]!, columns: L4_COLS, pk: 'id' })
        expect(res.equal, `seed=${seed} round=${round} case="${CASES[i]!.label}"\n${formatCompare(res)}`).toBe(true)
      }
    }
  }, 120000)
})
