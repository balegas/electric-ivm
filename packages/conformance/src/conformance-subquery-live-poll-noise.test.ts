// Regression: a subquery shape must not receive stream traffic for a write it was never a member
// of. `emit_shape_delta` computes an *absolute* membership weight for every touched pk on a
// shape's outer table (see its doc comment) — a row that never matched still produces a "delete
// by pk", which the comment calls idempotent-and-therefore-harmless. It IS harmless to the
// shape's *content* (a delete for a pk the client never saw is a no-op once classified against
// the tracked key set) — but delivering it still counts as new data on the shape's durable
// stream, and durable-streams wakes any live long-poll on a stream the moment it sees a
// non-empty append. Two shapes filtering the *same* outer table by different subquery
// parameters would therefore both wake — and both burn a live-poll round-trip reporting
// "up-to-date" with nothing to show — on every write to that table, matching or not. Asserted via
// the `emitted` counter (`GET /state/node?id=shape:<id>`) rather than long-poll timing: a write
// that matches only shape A must not move shape B's counter at all, not "eventually settle".

import type { Schema } from '@electric-circuits/protocol'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { applyOp, bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: {
    parent: { columns: { id: { type: 'int' }, owner: { type: 'int' } }, primaryKey: 'id' },
    child: { columns: { id: { type: 'int' }, parent_id: { type: 'int' } }, primaryKey: 'id' },
  },
}

const whereOwnerA = 'parent_id IN (SELECT id FROM parent WHERE owner = 100)'
const whereOwnerB = 'parent_id IN (SELECT id FROM parent WHERE owner = 200)'

/** A `/v1/shape` handle is `<shapeId>h<seq>` — strip the per-client suffix to address the
 * underlying shape via the engine's introspection routes. */
const shapeIdOfHandle = (handle: string) => handle.replace(/h\d+$/, '')

async function snapshotHandle(engineUrl: string, where: string): Promise<string> {
  const q = new URLSearchParams({ table: 'child', offset: '-1', where })
  const res = await fetch(`${engineUrl}/v1/shape?${q.toString()}`)
  expect(res.status).toBe(200)
  return res.headers.get('electric-handle') as string
}

async function emittedCount(engineUrl: string, shapeId: string): Promise<number> {
  const res = await fetch(`${engineUrl}/state/node?id=${encodeURIComponent(`shape:${shapeId}`)}`)
  expect(res.status).toBe(200)
  const body = (await res.json()) as { emitted: number }
  return body.emitted
}

describe('conformance: subquery shapes do not emit for writes they never matched', () => {
  let h: Harness
  beforeAll(async () => {
    h = await bootHarness(schema)
    await applyOp(h, 'parent', { op: 'insert', pk: '1', row: { id: 1, owner: 100 } })
    await applyOp(h, 'parent', { op: 'insert', pk: '2', row: { id: 2, owner: 200 } })
    await drainEngine(h)
  }, 60000)
  afterAll(async () => await h?.shutdown())

  it("a write matching only shape A never moves shape B's emitted counter", async () => {
    const handleA = await snapshotHandle(h.engineUrl, whereOwnerA)
    const handleB = await snapshotHandle(h.engineUrl, whereOwnerB)
    const idA = shapeIdOfHandle(handleA)
    const idB = shapeIdOfHandle(handleB)

    const beforeA = await emittedCount(h.engineUrl, idA)
    const beforeB = await emittedCount(h.engineUrl, idB)

    // Matches shape A's outer predicate (parent_id=1, owned by 100) only; shape B (owner=200)
    // was never a member of this row and must see nothing at all — not even a spurious delete.
    await applyOp(h, 'child', { op: 'insert', pk: '10', row: { id: 10, parent_id: 1 } })
    await drainEngine(h)

    const afterA = await emittedCount(h.engineUrl, idA)
    const afterB = await emittedCount(h.engineUrl, idB)

    expect(afterA, 'the matching shape must still emit normally').toBeGreaterThan(beforeA)
    expect(afterB, 'a non-matching shape must not receive stream traffic for a row it never had').toBe(beforeB)
  }, 60000)
})
