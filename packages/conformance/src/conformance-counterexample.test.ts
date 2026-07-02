// Negative control: the conformance suite is only trustworthy if a deliberately-WRONG engine
// makes it go red. We boot the engine with a test-only injected fault (ELECTRIC_IVM_FAULT) and
// assert the oracle comparison DETECTS the divergence — and that the identical scenario on a
// normal engine still converges. A pure-TS unit control guards the comparator itself.
//
// The faulted run must be detected within a bounded time and must NOT hang: under `drop_deletes`
// the "leave" envelope is never emitted, so we rely on `drainEngine` (the offset barrier still
// advances) instead of awaitTxId, then take a single snapshot comparison.

import type { Schema, ShapeDef } from '@electric-ivm/protocol'
import { afterEach, describe, expect, it } from 'vitest'
import { compareShapeSets, formatCompare } from './compare.js'
import { applyOp, bootHarness, drainEngine, type Harness, snapshotCompare, waitForConvergence } from './harness.js'

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
const def: ShapeDef = { table: 'users', where: { col: 'active', op: 'eq', value: true } }

// A scenario that forces a row to LEAVE the shape: it enters (active=true), then exits
// (active=false). A correct engine ends with an empty shape; `drop_deletes` keeps the stale row.
async function runLeaveScenario(h: Harness) {
  const shape = await h.client.shape(def)
  // Enter: the "enter" envelope is emitted even under the fault, so the client observes the row.
  await applyOp(h, 'users', {
    op: 'insert',
    pk: 1,
    row: { id: 1, name: 'alpha', age: 30, active: true, score: 1.0 },
  })
  await drainEngine(h)
  await waitForConvergence(h, { shape, def, columns: COLUMNS, pk: 'id' })
  // Leave: under `drop_deletes` no envelope is emitted; drainEngine proves the engine processed it.
  await applyOp(h, 'users', {
    op: 'update',
    pk: 1,
    row: { id: 1, name: 'alpha', age: 30, active: false, score: 1.0 },
  })
  await drainEngine(h)
  return shape
}

describe('negative control: the harness catches a deliberately broken engine', () => {
  let h: Harness | undefined
  afterEach(async () => {
    await h?.shutdown()
    h = undefined
  })

  it('FAULTED engine (drop_deletes) is DETECTED as divergent (not equal)', async () => {
    h = await bootHarness(schema, { fault: 'drop_deletes' })
    const shape = await runLeaveScenario(h)

    // Oracle: row 1 is inactive -> shape empty. Faulted client: still holds the stale row 1.
    const res = await snapshotCompare(h, { shape, def, columns: COLUMNS, pk: 'id' })
    expect(res.equal, `expected divergence but got: ${formatCompare(res)}`).toBe(false)
    expect(res.extra, 'the stale leaver should appear as an extra client row').toContain('1')
    expect(shape.currentRows().some((r) => String(r.id) === '1')).toBe(true)
  }, 60000)

  it('CONTROL: the identical scenario on a NORMAL engine converges (equal)', async () => {
    h = await bootHarness(schema) // no fault
    const shape = await runLeaveScenario(h)

    const res = await waitForConvergence(h, { shape, def, columns: COLUMNS, pk: 'id' })
    expect(res.equal, `normal engine should converge: ${formatCompare(res)}`).toBe(true)
    expect(shape.currentRows().some((r) => String(r.id) === '1')).toBe(false)
  }, 60000)

  it('FAULTED engine (off_by_one_cmp) misclassifies a boundary row -> DETECTED', async () => {
    h = await bootHarness(schema, { fault: 'off_by_one_cmp' })
    // Shape `age >= 18`. A row exactly at the boundary (age=18) must be IN per the oracle, but the
    // faulted engine treats >= as strict > and excludes it.
    const boundaryDef: ShapeDef = { table: 'users', where: { col: 'age', op: 'gte', value: 18 } }
    const shape = await h.client.shape(boundaryDef)
    await applyOp(h, 'users', { op: 'insert', pk: 7, row: { id: 7, name: 'edge', age: 18, active: true, score: 0.0 } })
    await drainEngine(h)

    const res = await snapshotCompare(h, { shape, def: boundaryDef, columns: COLUMNS, pk: 'id' })
    expect(res.equal, `expected divergence but got: ${formatCompare(res)}`).toBe(false)
    expect(res.missing, 'the boundary row should be missing from the faulted client').toContain('7')
  }, 60000)

  it('UNIT control: compareShapeSets flags missing/extra/mismatched on a mutated set', () => {
    const oracle = [
      { id: 1, name: 'a', active: true },
      { id: 2, name: 'b', active: false },
    ]
    const cols = ['id', 'name', 'active']

    // Identical sets are equal.
    expect(compareShapeSets(cols, 'id', oracle, oracle).equal).toBe(true)

    // Drop a row -> missing.
    expect(compareShapeSets(cols, 'id', oracle, [oracle[0]!]).missing).toContain('2')
    // Add a row -> extra.
    const extra = compareShapeSets(cols, 'id', oracle, [...oracle, { id: 3, name: 'c', active: true }])
    expect(extra.extra).toContain('3')
    // Flip a value -> mismatched.
    const mutated = compareShapeSets(cols, 'id', oracle, [oracle[0]!, { id: 2, name: 'b', active: true }])
    expect(mutated.equal).toBe(false)
    expect(mutated.mismatched.map((m) => m.key)).toContain('2')
  })
})
