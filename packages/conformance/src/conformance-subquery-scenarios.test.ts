// Deterministic subquery move-in/move-out scenarios, ported from Electric's subquery integration tests
// (subquery_move_out_test.exs / subquery_dependency_update_test.exs). Each: seed a known state, apply a
// specific mutation, drain, and assert the client materialization equals the pg oracle (and check the
// specific row entered/left). Convergence is the contract — not Electric's exact control-message stream.

import type { Predicate, Schema, ShapeDef } from '@electric-lite/protocol'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { formatCompare } from './compare.js'
import { applyOp, bootHarness, drainEngine, type Harness, waitForConvergence } from './harness.js'

async function ids(h: Harness, shape: { currentRows(): { id: unknown }[] }, def: ShapeDef, cols: string[]) {
  const res = await waitForConvergence(h, { shape: shape as never, def, columns: cols, pk: 'id' })
  expect(res.equal, formatCompare(res)).toBe(true)
  return (shape.currentRows() as { id: unknown }[]).map((r) => Number(r.id)).sort((a, b) => a - b)
}

describe('conformance: subquery — NOT IN move-in/out', () => {
  const schema: Schema = {
    tables: {
      inner_table: { columns: { id: { type: 'int' }, active: { type: 'bool' } }, primaryKey: 'id' },
      outer_table: { columns: { id: { type: 'int' }, inner_id: { type: 'int' } }, primaryKey: 'id' },
    },
  }
  const COLS = ['id', 'inner_id']
  const where: Predicate = { col: 'inner_id', negated: true, in: { table: 'inner_table', project: 'id', where: { col: 'active', op: 'eq', value: true } } }
  let h: Harness
  beforeAll(async () => { h = await bootHarness(schema) }, 60000)
  afterAll(async () => await h?.shutdown())

  it('negated move-in (inner becomes inactive) and move-out (inner becomes active)', async () => {
    const def: ShapeDef = { table: 'outer_table', where }
    await applyOp(h, 'inner_table', { op: 'insert', pk: 1, row: { id: 1, active: true } })
    await applyOp(h, 'outer_table', { op: 'insert', pk: 1, row: { id: 1, inner_id: 1 } })
    const shape = await h.client.shape(def)
    await drainEngine(h)
    // inner-1 active -> outer-1 NOT in (1 IN active set) -> absent.
    expect(await ids(h, shape, def, COLS)).toEqual([])

    // NEGATED MOVE-IN: inner-1 becomes inactive -> 1 leaves the active set -> outer-1 enters NOT IN.
    await applyOp(h, 'inner_table', { op: 'update', pk: 1, row: { id: 1, active: false } })
    await drainEngine(h)
    expect(await ids(h, shape, def, COLS)).toEqual([1])

    // NEGATED MOVE-OUT: inner-1 becomes active again -> outer-1 leaves.
    await applyOp(h, 'inner_table', { op: 'update', pk: 1, row: { id: 1, active: true } })
    await drainEngine(h)
    expect(await ids(h, shape, def, COLS)).toEqual([])
  }, 60000)
})

describe('conformance: subquery AND a non-subquery condition', () => {
  const schema: Schema = {
    tables: {
      parents: { columns: { id: { type: 'int' }, active: { type: 'bool' } }, primaryKey: 'id' },
      children: { columns: { id: { type: 'int' }, parent_id: { type: 'int' }, status: { type: 'text' } }, primaryKey: 'id' },
    },
  }
  const COLS = ['id', 'parent_id', 'status']
  const where: Predicate = {
    and: [
      { col: 'parent_id', in: { table: 'parents', project: 'id', where: { col: 'active', op: 'eq', value: true } } },
      { col: 'status', op: 'eq', value: 'published' },
    ],
  }
  let h: Harness
  beforeAll(async () => { h = await bootHarness(schema) }, 60000)
  afterAll(async () => await h?.shutdown())

  it('a subquery move-in must not mask a failing sibling condition', async () => {
    const def: ShapeDef = { table: 'children', where }
    // parent-a active, parent-b inactive; child-1 under parent-a, published -> in shape.
    await applyOp(h, 'parents', { op: 'insert', pk: 1, row: { id: 1, active: true } })
    await applyOp(h, 'parents', { op: 'insert', pk: 2, row: { id: 2, active: false } })
    await applyOp(h, 'children', { op: 'insert', pk: 1, row: { id: 1, parent_id: 1, status: 'published' } })
    const shape = await h.client.shape(def)
    await drainEngine(h)
    expect(await ids(h, shape, def, COLS)).toEqual([1])

    // Make parent-b active AND move child-1 to parent-b with status='draft'. The subquery part now
    // matches (parent-b active), but status fails -> child-1 must be DELETED (move-in must not mask it).
    await applyOp(h, 'parents', { op: 'update', pk: 2, row: { id: 2, active: true } })
    await applyOp(h, 'children', { op: 'update', pk: 1, row: { id: 1, parent_id: 2, status: 'draft' } })
    await drainEngine(h)
    expect(await ids(h, shape, def, COLS)).toEqual([])

    // Re-publish under the active parent-b -> re-enters.
    await applyOp(h, 'children', { op: 'update', pk: 1, row: { id: 1, parent_id: 2, status: 'published' } })
    await drainEngine(h)
    expect(await ids(h, shape, def, COLS)).toEqual([1])
  }, 60000)
})

describe('conformance: multi-level subquery — no spurious delete on dependency move', () => {
  // tasks -> projects -> teams -> organizations(+org tags). A task stays in the shape as long as SOME
  // path keeps its org premium; moving a team between premium orgs (or the old org losing the tag after
  // the move) must NOT delete the task.
  const schema: Schema = {
    tables: {
      organizations: { columns: { id: { type: 'int' } }, primaryKey: 'id' },
      org_tags: { columns: { id: { type: 'int' }, org_id: { type: 'int' }, tag: { type: 'text' } }, primaryKey: 'id' },
      teams: { columns: { id: { type: 'int' }, org_id: { type: 'int' } }, primaryKey: 'id' },
      projects: { columns: { id: { type: 'int' }, team_id: { type: 'int' } }, primaryKey: 'id' },
      tasks: { columns: { id: { type: 'int' }, project_id: { type: 'int' } }, primaryKey: 'id' },
    },
  }
  const COLS = ['id', 'project_id']
  // task.project_id IN (projects whose team's org has a 'premium' tag)
  const where: Predicate = {
    col: 'project_id',
    in: {
      table: 'projects',
      project: 'id',
      where: {
        col: 'team_id',
        in: {
          table: 'teams',
          project: 'id',
          where: {
            col: 'org_id',
            in: { table: 'org_tags', project: 'org_id', where: { col: 'tag', op: 'eq', value: 'premium' } },
          },
        },
      },
    },
  }
  let h: Harness
  beforeAll(async () => { h = await bootHarness(schema) }, 60000)
  afterAll(async () => await h?.shutdown())

  it('team moves between premium orgs, old org loses tag -> task remains', async () => {
    const def: ShapeDef = { table: 'tasks', where }
    // orgs 1 (acme) and 2 (globex), both premium.
    await applyOp(h, 'organizations', { op: 'insert', pk: 1, row: { id: 1 } })
    await applyOp(h, 'organizations', { op: 'insert', pk: 2, row: { id: 2 } })
    await applyOp(h, 'org_tags', { op: 'insert', pk: 1, row: { id: 1, org_id: 1, tag: 'premium' } })
    await applyOp(h, 'org_tags', { op: 'insert', pk: 2, row: { id: 2, org_id: 2, tag: 'premium' } })
    await applyOp(h, 'teams', { op: 'insert', pk: 1, row: { id: 1, org_id: 1 } }) // team in acme
    await applyOp(h, 'projects', { op: 'insert', pk: 1, row: { id: 1, team_id: 1 } })
    await applyOp(h, 'tasks', { op: 'insert', pk: 1, row: { id: 1, project_id: 1 } })
    const shape = await h.client.shape(def)
    await drainEngine(h)
    expect(await ids(h, shape, def, COLS)).toEqual([1])

    // Move the team acme(1) -> globex(2), both premium: NO delete.
    await applyOp(h, 'teams', { op: 'update', pk: 1, row: { id: 1, org_id: 2 } })
    await drainEngine(h)
    expect(await ids(h, shape, def, COLS)).toEqual([1])

    // Now remove premium from the OLD org acme(1): still NO delete (path is via globex).
    await applyOp(h, 'org_tags', { op: 'delete', pk: 1 })
    await drainEngine(h)
    expect(await ids(h, shape, def, COLS)).toEqual([1])

    // Finally remove premium from globex(2) too: now the task leaves.
    await applyOp(h, 'org_tags', { op: 'delete', pk: 2 })
    await drainEngine(h)
    expect(await ids(h, shape, def, COLS)).toEqual([])
  }, 60000)
})
