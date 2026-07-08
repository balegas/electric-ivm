// Engine-restart persistence: shapes survive an engine crash without any client re-registration.
// The engine replays its durable shape catalog (`meta/catalog`) at boot, re-registers plain/routed
// shapes with passthrough gates, resumes the change log from the persisted offset, and the SAME
// shape streams keep receiving changes. Subquery shapes are deliberately NOT restored (their
// inner-node state is not persisted) — their streams are deleted loudly so clients recreate.

import type { Schema } from '@electric-ivm/protocol'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { applyOp, bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: {
    users: {
      columns: { id: { type: 'int' }, name: { type: 'text' }, active: { type: 'bool' } },
      primaryKey: 'id',
    },
  },
}

/** Fold a shape stream's raw envelopes into the current row set (upsert/delete by key). */
async function streamRows(dsUrl: string, streamPath: string): Promise<Map<string, unknown>> {
  const rows = new Map<string, unknown>()
  let offset = '-1'
  for (;;) {
    const res = await fetch(`${dsUrl}/${streamPath}?offset=${offset}`)
    if (!res.ok) throw new Error(`read ${streamPath} -> ${res.status}`)
    const next = res.headers.get('stream-next-offset')
    const body = res.status === 204 ? [] : ((await res.json()) as Array<{ key: string; value?: unknown; headers: { operation: string } }>)
    for (const env of body) {
      if (env.headers.operation === 'delete') rows.delete(env.key)
      else rows.set(env.key, env.value)
    }
    if (!next || next === offset || res.headers.get('stream-up-to-date')) break
    offset = next
  }
  return rows
}

describe('conformance: engine restart restores shapes from the durable catalog', () => {
  let h: Harness
  beforeAll(async () => {
    h = await bootHarness(schema)
  }, 60000)
  afterAll(async () => {
    await h?.shutdown()
  })

  it('a shape keeps receiving changes across a crash, with no re-registration', async () => {
    // Create a shape and land one matching row pre-crash.
    const res = await fetch(`${h.engineUrl}/shapes`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ table: 'users', where: { col: 'active', op: 'eq', value: true } }),
    })
    expect(res.ok).toBe(true)
    const rec = (await res.json()) as { shapeId: string; streamPath: string }
    await applyOp(h, 'users', { op: 'insert', pk: 1, row: { id: 1, name: 'before', active: true } })
    await drainEngine(h)
    expect((await streamRows(h.dsUrl, rec.streamPath)).has('1')).toBe(true)

    // Crash + reboot the engine against the same streams/Postgres/slot. Nothing re-creates the
    // shape: the new process must restore it from the catalog.
    await h.restartEngine()

    // A post-restart matching write must reach the SAME stream; a non-matching one must not.
    await applyOp(h, 'users', { op: 'insert', pk: 2, row: { id: 2, name: 'after', active: true } })
    await applyOp(h, 'users', { op: 'insert', pk: 3, row: { id: 3, name: 'skip', active: false } })
    await drainEngine(h)
    const rows = await streamRows(h.dsUrl, rec.streamPath)
    expect(rows.has('1'), 'pre-crash row survives').toBe(true)
    expect(rows.has('2'), 'post-restart matching write flows to the restored shape').toBe(true)
    expect(rows.has('3'), 'post-restart non-matching write is filtered').toBe(false)

    // The restored engine also knows the shape (its record came from the catalog).
    const got = await fetch(`${h.engineUrl}/shapes/${rec.shapeId}`)
    expect(got.status).toBe(200)

    // And a post-restart update that moves row 1 out must emit its delete (retraction still works).
    await applyOp(h, 'users', { op: 'update', pk: 1, row: { id: 1, name: 'before', active: false } })
    await drainEngine(h)
    expect((await streamRows(h.dsUrl, rec.streamPath)).has('1')).toBe(false)
  }, 60000)
})
