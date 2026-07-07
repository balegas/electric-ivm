// Params conformance: `GET /v1/shape` with a subquery `where` + `params` (the benchmarking-fleet's
// load_generator_subqueries form, e.g. `project_id IN (SELECT id FROM projects WHERE owner_id = $1)`).
// Uses real UUID columns like the fleet's issue_tracker schema (uuid PK + uuid FK) — the exact case
// that 500'd ("cannot convert String -> uuid") before the backfill bound params as `col::text = $n`.
// Asserts correct rows for two param values (proving distinct params never collide onto one shape),
// a nested/depth-2 subquery hitting a second table's backfill, and the Electric-style 400s.

import type { Schema } from '@electric-ivm/protocol'
import pgpkg from 'pg'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { applyOp, bootHarness, drainEngine, type Harness } from './harness.js'

// The engine coarsens uuid -> text on introspection, so the protocol schema declares these as text;
// the real Postgres columns are uuid (created by the ddl below), which is what triggered the bug.
const schema: Schema = {
  tables: {
    projects: { columns: { id: { type: 'text' }, owner_id: { type: 'text' } }, primaryKey: 'id' },
    issues: { columns: { id: { type: 'text' }, project_id: { type: 'text' } }, primaryKey: 'id' },
    comments: { columns: { id: { type: 'text' }, issue_id: { type: 'text' } }, primaryKey: 'id' },
  },
}

const ddl = `
  CREATE TABLE projects (id uuid PRIMARY KEY, owner_id uuid NOT NULL);
  CREATE INDEX projects_owner_idx ON projects (owner_id);
  ALTER TABLE projects REPLICA IDENTITY FULL;
  CREATE TABLE issues (id uuid PRIMARY KEY, project_id uuid NOT NULL REFERENCES projects(id));
  ALTER TABLE issues REPLICA IDENTITY FULL;
  CREATE TABLE comments (id uuid PRIMARY KEY, issue_id uuid NOT NULL REFERENCES issues(id));
  ALTER TABLE comments REPLICA IDENTITY FULL;
`

const uuid = () => crypto.randomUUID()

async function shapeKeys(engineUrl: string, params: Record<string, string>): Promise<string[]> {
  const q = new URLSearchParams(params)
  const res = await fetch(`${engineUrl}/v1/shape?${q.toString()}`)
  if (res.status !== 200) throw new Error(`expected 200, got ${res.status}: ${await res.text()}`)
  const msgs = (await res.json()) as Array<{ headers: { operation?: string }; key?: string }>
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

// owners u1,u2; projects p1,p3 -> u1, p2 -> u2; issues fan across projects; comments on issues.
const u1 = uuid()
const u2 = uuid()
const p1 = uuid()
const p2 = uuid()
const p3 = uuid()
const i1 = uuid()
const i2 = uuid()
const i3 = uuid()
const i4 = uuid()
const c1 = uuid()
const c2 = uuid()
const c3 = uuid()

const SUB = 'project_id IN (SELECT id FROM projects WHERE owner_id = $1)'
const NESTED = 'issue_id IN (SELECT id FROM issues WHERE project_id IN (SELECT id FROM projects WHERE owner_id = $1))'

describe('conformance: /v1/shape params over uuid columns', () => {
  let h: Harness
  beforeAll(async () => {
    h = await bootHarness(schema, { ddl })
    for (const [id, owner_id] of [[p1, u1], [p2, u2], [p3, u1]] as const) {
      await applyOp(h, 'projects', { op: 'insert', pk: id, row: { id, owner_id } })
    }
    for (const [id, project_id] of [[i1, p1], [i2, p2], [i3, p3], [i4, p2]] as const) {
      await applyOp(h, 'issues', { op: 'insert', pk: id, row: { id, project_id } })
    }
    // comments c1,c2 on issues of u1's projects (i1,i3); c3 on i2 (u2's).
    for (const [id, issue_id] of [[c1, i1], [c2, i3], [c3, i2]] as const) {
      await applyOp(h, 'comments', { op: 'insert', pk: id, row: { id, issue_id } })
    }
    await drainEngine(h)
  }, 60000)
  afterAll(async () => {
    await h?.shutdown()
  })

  it('returns rows (not 500) for a uuid subquery param', async () => {
    // owner u1 -> projects p1,p3 -> issues i1,i3.
    const rows = await shapeKeys(h.engineUrl, { table: 'issues', offset: '-1', where: SUB, 'params[1]': u1 })
    expect(rows).toEqual([i1, i3].sort())
  })

  it('distinct param values yield distinct row sets (no collision)', async () => {
    const rows = await shapeKeys(h.engineUrl, { table: 'issues', offset: '-1', where: SUB, 'params[1]': u2 })
    expect(rows).toEqual([i2, i4].sort())
  })

  it('nested/depth-2 subquery hits a second table backfill', async () => {
    // comments whose issue belongs to a project owned by u1 -> c1 (i1), c2 (i3).
    const rows = await shapeKeys(h.engineUrl, { table: 'comments', offset: '-1', where: NESTED, 'params[1]': u1 })
    expect(rows).toEqual([c1, c2].sort())
  })

  it('accepts the JSON params form', async () => {
    const rows = await shapeKeys(h.engineUrl, { table: 'issues', offset: '-1', where: SUB, params: JSON.stringify({ 1: u1 }) })
    expect(rows).toEqual([i1, i3].sort())
  })

  it('400s when a referenced $N has no param', async () => {
    const r = await shapeStatus(h.engineUrl, { table: 'issues', offset: '-1', where: SUB })
    expect(r.status).toBe(400)
    expect(r.message).toContain('parameter $1 was not provided')
  })

  it('400s on non-sequential param keys', async () => {
    const r = await shapeStatus(h.engineUrl, { table: 'issues', offset: '-1', where: SUB, 'params[2]': u1 })
    expect(r.status).toBe(400)
    expect(r.message).toContain('Parameters must be numbered sequentially')
  })

  // The point of casting `$n::text::uuid` (vs `owner_id::text = $n`) is to keep the inner select
  // index-eligible. EXPLAIN the two forms with seqscan disabled: the cast form can use the owner_id
  // btree index; the text-cast form cannot (the `::text` expression doesn't match the index).
  it('the $n::text::uuid cast keeps the inner select on an index scan', async () => {
    const c = new pgpkg.Client({ connectionString: h.pgUrl })
    await c.connect()
    try {
      await c.query('SET enable_seqscan = off')
      const plan = async (sql: string) => {
        const r = await c.query(`EXPLAIN (FORMAT JSON) ${sql}`, [u1])
        return JSON.stringify(r.rows[0]['QUERY PLAN'])
      }
      const cast = await plan('SELECT id FROM projects WHERE owner_id = $1::text::uuid')
      const textCast = await plan('SELECT id FROM projects WHERE owner_id::text = $1')
      expect(cast).toContain('Index') // Index Scan / Bitmap Index Scan — uses projects_owner_idx
      expect(textCast).toContain('Seq Scan') // the ::text expression can't use the btree index
    } finally {
      await c.end()
    }
  })
})

// A LIVE inner change that re-derives outer membership via a Postgres query-back over uuid columns —
// the path that failed SILENTLY (process_envelope: "backfill select comments: cannot convert String ->
// uuid"), dropping move-in rows while the client stream stayed clean. Own harness so the mutation is
// isolated from the read-only snapshot tests above.
describe('conformance: /v1/shape params LIVE move-in over uuid columns', () => {
  let h: Harness
  const uOwner = uuid()
  const uOther = uuid()
  const pl = uuid()
  const il = uuid()
  const cl = uuid()

  beforeAll(async () => {
    h = await bootHarness(schema, { ddl })
    // Pre-shape: project pl owned by uOther (NOT uOwner), issue il on pl, comment cl on il.
    await applyOp(h, 'projects', { op: 'insert', pk: pl, row: { id: pl, owner_id: uOther } })
    await applyOp(h, 'issues', { op: 'insert', pk: il, row: { id: il, project_id: pl } })
    await applyOp(h, 'comments', { op: 'insert', pk: cl, row: { id: cl, issue_id: il } })
    await drainEngine(h)
  }, 60000)
  afterAll(async () => {
    await h?.shutdown()
  })

  it('re-derives a uuid move-in on a live inner change (no silent drop, no engine error)', async () => {
    // Snapshot the depth-2 comments shape for uOwner: empty (pl is owned by uOther).
    const q = new URLSearchParams({ table: 'comments', offset: '-1', where: NESTED, 'params[1]': uOwner })
    const snap = await fetch(`${h.engineUrl}/v1/shape?${q.toString()}`)
    expect(snap.status).toBe(200)
    const handle = snap.headers.get('electric-handle') as string
    const snapMsgs = (await snap.json()) as Array<{ headers: { operation?: string }; key?: string }>
    expect(snapMsgs.filter((m) => m.headers.operation === 'insert').map((m) => m.key)).toEqual([])

    const errBefore = h.engineStderr().length

    // LIVE inner change: pl's owner becomes uOwner -> pl enters -> il enters -> the engine re-derives
    // `comments WHERE issue_id = il` (uuid query-back, the previously-failing path) -> cl must move in.
    await applyOp(h, 'projects', { op: 'update', pk: pl, row: { id: pl, owner_id: uOwner } })
    await drainEngine(h)

    // (a) the move-in row actually arrives on the shape stream (silently dropped before the fix).
    const arrived = await waitShapeContains(h.engineUrl, handle, cl)
    expect(arrived, 'comment did not move in — the live uuid subquery re-derive dropped it').toBe(true)

    // (b) no engine-side process_envelope failure during the re-derive.
    const errNew = h.engineStderr().slice(errBefore)
    expect(errNew).not.toContain('process_envelope failed')
    expect(errNew).not.toContain('cannot convert')
  }, 60000)
})

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

/** A /v1/shape handle is `<shapeId>h<seq>` — a per-client handle over a shared engine shape
 * (retention lifecycle); strip the suffix to address the underlying shape on the extended API. */
const shapeIdOfHandle = (handle: string) => handle.replace(/h\d+$/, '')

/** Fold a shape's current contents (via /shapes/{id}/rows) and report whether `key` is present. */
async function shapeContains(engineUrl: string, handle: string, key: string): Promise<boolean> {
  const res = await fetch(`${engineUrl}/shapes/${shapeIdOfHandle(handle)}/rows`)
  if (res.status !== 200) return false
  const body = (await res.json()) as { rows: Array<{ key: string }> }
  return body.rows.some((r) => r.key === key)
}

async function waitShapeContains(engineUrl: string, handle: string, key: string, ms = 8000): Promise<boolean> {
  const end = Date.now() + ms
  while (Date.now() < end) {
    if (await shapeContains(engineUrl, handle, key)) return true
    await sleep(150)
  }
  return false
}
