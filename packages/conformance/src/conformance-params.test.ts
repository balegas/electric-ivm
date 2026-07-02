// Params conformance: `GET /v1/shape` with a subquery `where` + `params` (the benchmarking-fleet's
// load_generator_subqueries form, e.g. `project_id IN (SELECT id FROM projects WHERE owner_id = $1)`).
// The engine substitutes `$N` from `params` before parsing, so the backfill returns exactly the rows
// matching the substituted predicate. Asserts correct rows for two different param values (proving
// distinct params never collide onto one shape) and the Electric-style 400s.

import type { Schema } from '@electric-ivm/protocol'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { applyOp, bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: {
    projects: { columns: { id: { type: 'text' }, owner_id: { type: 'text' } }, primaryKey: 'id' },
    issues: { columns: { id: { type: 'text' }, project_id: { type: 'text' } }, primaryKey: 'id' },
  },
}

/** Fetch a snapshot from /v1/shape and return the inserted row keys, sorted. */
async function shapeKeys(engineUrl: string, params: Record<string, string>): Promise<string[]> {
  const q = new URLSearchParams(params)
  const res = await fetch(`${engineUrl}/v1/shape?${q.toString()}`)
  if (res.status !== 200) throw new Error(`expected 200, got ${res.status}: ${await res.text()}`)
  const msgs = (await res.json()) as Array<{ headers: { operation?: string; control?: string }; key?: string }>
  return msgs
    .filter((m) => m.headers.operation === 'insert')
    .map((m) => m.key as string)
    .sort()
}

async function shapeStatus(engineUrl: string, params: Record<string, string>): Promise<{ status: number; message?: string }> {
  const q = new URLSearchParams(params)
  const res = await fetch(`${engineUrl}/v1/shape?${q.toString()}`)
  const body = (await res.json().catch(() => ({}))) as { message?: string }
  return { status: res.status, message: body.message }
}

const WHERE = 'project_id IN (SELECT id FROM projects WHERE owner_id = $1)'

describe('conformance: /v1/shape params ($N substitution)', () => {
  let h: Harness
  beforeAll(async () => {
    h = await bootHarness(schema)
    // projects p1,p3 owned by u1; p2 owned by u2. issues fan across them.
    for (const [id, owner_id] of [['p1', 'u1'], ['p2', 'u2'], ['p3', 'u1']] as const) {
      await applyOp(h, 'projects', { op: 'insert', pk: id, row: { id, owner_id } })
    }
    for (const [id, project_id] of [['i1', 'p1'], ['i2', 'p2'], ['i3', 'p3'], ['i4', 'p2']] as const) {
      await applyOp(h, 'issues', { op: 'insert', pk: id, row: { id, project_id } })
    }
    await drainEngine(h)
  }, 60000)
  afterAll(async () => {
    await h?.shutdown()
  })

  it('returns exactly the rows matching the substituted subquery param', async () => {
    // owner u1 -> projects p1,p3 -> issues i1,i3.
    const u1 = await shapeKeys(h.engineUrl, { table: 'issues', offset: '-1', where: WHERE, 'params[1]': 'u1' })
    expect(u1).toEqual(['i1', 'i3'])
  })

  it('distinct param values yield distinct row sets (no collision)', async () => {
    // owner u2 -> project p2 -> issues i2,i4. Same where string, different param value.
    const u2 = await shapeKeys(h.engineUrl, { table: 'issues', offset: '-1', where: WHERE, 'params[1]': 'u2' })
    expect(u2).toEqual(['i2', 'i4'])
  })

  it('accepts the JSON params form too', async () => {
    const u1 = await shapeKeys(h.engineUrl, { table: 'issues', offset: '-1', where: WHERE, params: '{"1":"u1"}' })
    expect(u1).toEqual(['i1', 'i3'])
  })

  it('400s when a referenced $N has no param', async () => {
    const r = await shapeStatus(h.engineUrl, { table: 'issues', offset: '-1', where: WHERE })
    expect(r.status).toBe(400)
    expect(r.message).toContain('parameter $1 was not provided')
  })

  it('400s on non-sequential param keys', async () => {
    const r = await shapeStatus(h.engineUrl, { table: 'issues', offset: '-1', where: WHERE, 'params[2]': 'u1' })
    expect(r.status).toBe(400)
    expect(r.message).toContain('Parameters must be numbered sequentially')
  })
})
