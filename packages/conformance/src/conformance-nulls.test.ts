// NULL / three-valued-logic conformance. pglite is the ground truth (real Postgres WHERE), so each
// case asserts the client-materialized set equals the oracle even when cells are NULL. This is the
// gap the rest of the suite deliberately avoided (no nulls generated): a comparison with a NULL
// operand is UNKNOWN, AND/OR follow the SQL truth tables, and `NOT (col = x)` over a NULL cell
// stays UNKNOWN -> excluded (the case that diverged under the old two-valued engine).
//
// Non-pk columns are nullable by contract (the oracle DDL emits no NOT NULL; the engine stores
// Value::Null; the client zod schema allows null cells). The pk is never null.

import type { Row, Schema, ShapeDef } from '@electric-ivm/protocol'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { formatCompare } from './compare.js'
import { applyOp, bootHarness, drainEngine, type Harness, waitForConvergence } from './harness.js'
import { createSimulator, randomShapeDefs } from './simulator.js'

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

// Fixture peppered with NULLs across every non-pk column, including an all-null row (id 5).
const FIXTURE: Row[] = [
  { id: 1, name: 'alpha', age: 20, active: true, score: 1.0 },
  { id: 2, name: null, age: 20, active: true, score: 1.0 },
  { id: 3, name: 'alpha', age: null, active: true, score: 1.0 },
  { id: 4, name: 'alpha', age: 20, active: null, score: 1.0 },
  { id: 5, name: null, age: null, active: null, score: null },
  { id: 6, name: 'bravo', age: 10, active: false, score: 5.0 },
  { id: 7, name: 'alpha', age: 30, active: true, score: null },
]

const CASES: Array<{ label: string; where: ShapeDef['where'] }> = [
  { label: 'match-all materializes null cells', where: undefined },
  { label: 'eq over null name -> excluded', where: { col: 'name', op: 'eq', value: 'alpha' } },
  { label: 'neq over null name -> excluded', where: { col: 'name', op: 'neq', value: 'alpha' } },
  // The headline fix: NOT(eq) over a null cell must NOT leak the row.
  { label: 'NOT(eq) over null name -> excluded', where: { not: { col: 'name', op: 'eq', value: 'alpha' } } },
  { label: 'gt over null age -> excluded', where: { col: 'age', op: 'gt', value: 18 } },
  { label: 'NOT(gt) over null age -> excluded', where: { not: { col: 'age', op: 'gt', value: 18 } } },
  { label: 'neq over null age -> excluded', where: { col: 'age', op: 'neq', value: 20 } },
  // AND: TRUE AND UNKNOWN = UNKNOWN; FALSE AND UNKNOWN = FALSE.
  { label: 'AND with null operand', where: { and: [{ col: 'active', op: 'eq', value: true }, { col: 'age', op: 'gt', value: 18 }] } },
  // OR: TRUE OR UNKNOWN = TRUE; FALSE OR UNKNOWN = UNKNOWN.
  { label: 'OR with null operand', where: { or: [{ col: 'active', op: 'eq', value: true }, { col: 'age', op: 'gt', value: 100 }] } },
  // Deep nesting with NOT over a conjunction that hits nulls.
  { label: 'NOT(and) over nulls', where: { not: { and: [{ col: 'name', op: 'eq', value: 'alpha' }, { col: 'age', op: 'gte', value: 20 }] } } },
  // The native null-test leaf — the one predicate that is TRUE on a NULL cell.
  { label: 'IS NULL selects null cells', where: { col: 'name', isNull: true } },
  { label: 'IS NOT NULL excludes null cells', where: { col: 'name', isNull: false } },
  { label: 'NOT(IS NULL) = IS NOT NULL', where: { not: { col: 'age', isNull: true } } },
  { label: 'IS NULL composed under AND', where: { and: [{ col: 'active', op: 'eq', value: true }, { col: 'age', isNull: true }] } },
  { label: 'IS NULL composed under OR', where: { or: [{ col: 'name', isNull: true }, { col: 'age', op: 'gt', value: 25 }] } },
]

describe('conformance: NULL three-valued logic (deterministic fixtures)', () => {
  let h: Harness
  const shapes: Awaited<ReturnType<Harness['client']['shape']>>[] = []

  beforeAll(async () => {
    h = await bootHarness(schema)
    for (const c of CASES) shapes.push(await h.client.shape({ table: 'users', where: c.where }))
    for (const row of FIXTURE) await applyOp(h, 'users', { op: 'insert', pk: row.id as number, row })
    await drainEngine(h)
  }, 90000)
  afterAll(async () => {
    await h?.shutdown()
  })

  for (let i = 0; i < CASES.length; i++) {
    const c = CASES[i]!
    it(`${c.label} matches the oracle`, async () => {
      const def: ShapeDef = { table: 'users', where: c.where }
      const res = await waitForConvergence(h, { shape: shapes[i]!, def, columns: COLUMNS, pk: 'id' })
      expect(res.equal, `case="${c.label}" where=${JSON.stringify(c.where)}\n${formatCompare(res)}`).toBe(true)
    }, 30000)
  }
})

// Fuzz with NULLs ON: random predicates over an op stream that injects nulls into ~35% of cells,
// so NOT-over-null and AND/OR-with-null arise constantly. Still compared row-for-row to pglite.
const SEEDS = Number(process.env.NULL_FUZZ_SEEDS ?? 4)
const SHAPES = Number(process.env.NULL_FUZZ_SHAPES ?? 14)
const OPS = Number(process.env.NULL_FUZZ_OPS ?? 300)

describe('conformance: NULL fuzz vs oracle', () => {
  it(
    `holds the oracle invariant with nulls across ${SEEDS} scenarios`,
    async () => {
      const base = process.env.SEED ? Number(process.env.SEED) : 90210
      for (let i = 0; i < SEEDS; i++) {
        const seed = base + i * 7919
        const h = await bootHarness(schema)
        try {
          const defs = randomShapeDefs(schema, seed, SHAPES, { maxDepth: 3, edgeLiterals: true })
          const shapes = await Promise.all(defs.map((d) => h.client.shape(d)))
          for (const { table, ev } of createSimulator(schema, { seed, nullProb: 0.35 }).take(OPS)) {
            await applyOp(h, table, ev)
          }
          await drainEngine(h)
          for (let s = 0; s < defs.length; s++) {
            const def = defs[s]!
            const res = await waitForConvergence(h, { shape: shapes[s]!, def, columns: COLUMNS, pk: 'id' })
            expect(
              res.equal,
              `FAILED seed=${seed} shape#${s}=${JSON.stringify(def.where ?? 'ALL')}\n${formatCompare(res)}`,
            ).toBe(true)
          }
        } finally {
          await h.shutdown()
        }
      }
    },
    300000,
  )
})
