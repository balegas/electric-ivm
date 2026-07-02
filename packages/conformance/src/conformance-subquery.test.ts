// Subquery conformance: a shape whose WHERE is `col IN (SELECT … )` must converge to the Postgres
// oracle (which evaluates the subquery natively) through inner- and outer-table mutations. The engine
// maintains a shared inner-set node and moves outer rows in/out; the oracle is `SELECT … WHERE <sub>`.

import type { InSubqueryPredicate, Schema, ShapeDef } from '@electric-ivm/protocol'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { formatCompare } from './compare.js'
import { applyOp, bootHarness, drainEngine, type Harness, waitForConvergence } from './harness.js'

const schema: Schema = {
  tables: {
    parent: {
      columns: { id: { type: 'int' }, active: { type: 'bool' } },
      primaryKey: 'id',
    },
    child: {
      columns: { id: { type: 'int' }, parent_id: { type: 'int' }, label: { type: 'text' } },
      primaryKey: 'id',
    },
  },
}
const CHILD_COLS = ['id', 'parent_id', 'label']

const inActiveParents: InSubqueryPredicate = {
  col: 'parent_id',
  in: { table: 'parent', project: 'id', where: { col: 'active', op: 'eq', value: true } },
}

describe('conformance: subquery (child IN active parents)', () => {
  let h: Harness
  beforeAll(async () => {
    h = await bootHarness(schema)
  }, 60000)
  afterAll(async () => {
    await h?.shutdown()
  })

  it('converges through inner + outer mutations (move-in / move-out / re-parent)', async () => {
    const def: ShapeDef = { table: 'child', where: inActiveParents }

    // Seed: parents 1,3 active / 2 inactive; children round-robin across parents 1,2,3 plus a 4th on 1.
    for (const [id, active] of [[1, true], [2, false], [3, true]] as const) {
      await applyOp(h, 'parent', { op: 'insert', pk: id, row: { id, active } })
    }
    for (const [id, parent_id] of [[1, 1], [2, 2], [3, 3], [4, 1]] as const) {
      await applyOp(h, 'child', { op: 'insert', pk: id, row: { id, parent_id, label: `c${id}` } })
    }

    const shape = await h.client.shape(def)
    await drainEngine(h)
    // Initial: children of active parents (1,3) => 1,3,4.
    let res = await waitForConvergence(h, { shape, def, columns: CHILD_COLS, pk: 'id' })
    expect(res.equal, `initial\n${formatCompare(res)}`).toBe(true)
    expect(shape.currentRows().map((r) => Number(r.id)).sort((a, b) => a - b)).toEqual([1, 3, 4])

    // MOVE-OUT: deactivate parent 1 -> children 1 and 4 leave (synthetic deletes from an inner change).
    await applyOp(h, 'parent', { op: 'update', pk: 1, row: { id: 1, active: false } })
    await drainEngine(h)
    res = await waitForConvergence(h, { shape, def, columns: CHILD_COLS, pk: 'id' })
    expect(res.equal, `after deactivate parent 1\n${formatCompare(res)}`).toBe(true)
    expect(shape.currentRows().map((r) => Number(r.id)).sort((a, b) => a - b)).toEqual([3])

    // MOVE-IN: activate parent 2 -> child 2 enters.
    await applyOp(h, 'parent', { op: 'update', pk: 2, row: { id: 2, active: true } })
    await drainEngine(h)
    res = await waitForConvergence(h, { shape, def, columns: CHILD_COLS, pk: 'id' })
    expect(res.equal, `after activate parent 2\n${formatCompare(res)}`).toBe(true)
    expect(shape.currentRows().map((r) => Number(r.id)).sort((a, b) => a - b)).toEqual([2, 3])

    // RE-PARENT: move child 4 to active parent 2 -> child 4 enters (outer-row change).
    await applyOp(h, 'child', { op: 'update', pk: 4, row: { id: 4, parent_id: 2, label: 'c4' } })
    await drainEngine(h)
    res = await waitForConvergence(h, { shape, def, columns: CHILD_COLS, pk: 'id' })
    expect(res.equal, `after re-parent child 4 -> 2\n${formatCompare(res)}`).toBe(true)
    expect(shape.currentRows().map((r) => Number(r.id)).sort((a, b) => a - b)).toEqual([2, 3, 4])

    // DELETE inner: delete parent 2 -> children 2 and 4 leave.
    await applyOp(h, 'parent', { op: 'delete', pk: 2 })
    await drainEngine(h)
    res = await waitForConvergence(h, { shape, def, columns: CHILD_COLS, pk: 'id' })
    expect(res.equal, `after delete parent 2\n${formatCompare(res)}`).toBe(true)
    expect(shape.currentRows().map((r) => Number(r.id)).sort((a, b) => a - b)).toEqual([3])
  }, 60000)
})
