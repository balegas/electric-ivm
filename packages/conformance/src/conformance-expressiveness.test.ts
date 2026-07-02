// Query-expressiveness conformance: a deterministic fixture dataset with known edge values lets
// us assert exact behaviour for every comparison op on every column type, boundary literals,
// edge values (empty string, negatives), contradiction/tautology, deep nesting, and predicates
// touching every column. Each shape is registered before data, drained, then compared to pglite.
//
// Text ordering uses lowercase-ASCII + empty string only, matching the collation-safe domain the
// existing fuzz already proves agrees between Rust byte ordering and pglite.

import type { Row, Schema, ShapeDef } from '@electric-ivm/protocol'
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

// Edge-laden fixture: empty string, negative + boundary ages (18 appears twice), duplicate names,
// negative/zero/large floats.
const FIXTURE: Row[] = [
  { id: 1, name: 'alpha', age: 17, active: true, score: -5.5 },
  { id: 2, name: 'bravo', age: 18, active: false, score: 0.0 },
  { id: 3, name: 'charlie', age: 19, active: true, score: 100.25 },
  { id: 4, name: '', age: 0, active: false, score: 0.5 },
  { id: 5, name: 'delta', age: 65, active: true, score: 999.9999 },
  { id: 6, name: 'alpha', age: 18, active: true, score: 42.0 },
  { id: 7, name: 'zeta', age: -3, active: false, score: -100.0 },
  { id: 8, name: 'echo', age: 30, active: true, score: 18.0 },
]

const CASES: Array<{ label: string; where: ShapeDef['where'] }> = [
  // bool: eq / neq
  { label: 'bool eq', where: { col: 'active', op: 'eq', value: true } },
  { label: 'bool neq', where: { col: 'active', op: 'neq', value: true } },
  // int: every op, with age=18 sitting exactly on the boundary
  { label: 'int eq (boundary)', where: { col: 'age', op: 'eq', value: 18 } },
  { label: 'int neq', where: { col: 'age', op: 'neq', value: 18 } },
  { label: 'int lt (boundary)', where: { col: 'age', op: 'lt', value: 18 } },
  { label: 'int lte (boundary)', where: { col: 'age', op: 'lte', value: 18 } },
  { label: 'int gt (boundary)', where: { col: 'age', op: 'gt', value: 18 } },
  { label: 'int gte (boundary)', where: { col: 'age', op: 'gte', value: 18 } },
  { label: 'int eq negative', where: { col: 'age', op: 'eq', value: -3 } },
  { label: 'int lt zero (negatives)', where: { col: 'age', op: 'lt', value: 0 } },
  // text: every op (lowercase + empty), plus empty-string equality
  { label: 'text eq', where: { col: 'name', op: 'eq', value: 'alpha' } },
  { label: 'text neq', where: { col: 'name', op: 'neq', value: 'alpha' } },
  { label: 'text lt', where: { col: 'name', op: 'lt', value: 'charlie' } },
  { label: 'text lte', where: { col: 'name', op: 'lte', value: 'charlie' } },
  { label: 'text gt', where: { col: 'name', op: 'gt', value: 'charlie' } },
  { label: 'text gte', where: { col: 'name', op: 'gte', value: 'charlie' } },
  { label: 'text eq empty string', where: { col: 'name', op: 'eq', value: '' } },
  // float: every op, incl. exactly-representable boundary and negatives
  { label: 'float eq', where: { col: 'score', op: 'eq', value: 0.5 } },
  { label: 'float neq', where: { col: 'score', op: 'neq', value: 0.5 } },
  { label: 'float lt zero', where: { col: 'score', op: 'lt', value: 0.0 } },
  { label: 'float lte zero', where: { col: 'score', op: 'lte', value: 0.0 } },
  { label: 'float gt (boundary)', where: { col: 'score', op: 'gt', value: 100.25 } },
  { label: 'float gte (boundary)', where: { col: 'score', op: 'gte', value: 100.25 } },
  // contradiction / tautology, incl. empty and/or
  { label: 'contradiction (and)', where: { and: [{ col: 'age', op: 'gte', value: 100 }, { col: 'age', op: 'lt', value: 0 }] } },
  { label: 'tautology (or covers all)', where: { or: [{ col: 'age', op: 'gte', value: 0 }, { col: 'age', op: 'lt', value: 0 }] } },
  { label: 'empty and == TRUE (all)', where: { and: [] } },
  { label: 'empty or == FALSE (none)', where: { or: [] } },
  // deep nesting (depth >= 3)
  {
    label: 'deep and/or/not (depth 3)',
    where: {
      and: [
        { col: 'active', op: 'eq', value: true },
        { or: [{ col: 'age', op: 'gte', value: 18 }, { not: { col: 'name', op: 'eq', value: '' } }] },
        { not: { col: 'score', op: 'lt', value: 0 } },
      ],
    },
  },
  // every column referenced in one conjunction
  {
    label: 'all columns referenced',
    where: {
      and: [
        { col: 'id', op: 'gte', value: 1 },
        { col: 'name', op: 'neq', value: 'nobody' },
        { col: 'age', op: 'gte', value: -100 },
        { col: 'active', op: 'eq', value: true },
        { col: 'score', op: 'gte', value: -1000 },
      ],
    },
  },
]

describe('conformance: query expressiveness (deterministic fixtures)', () => {
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
