// Unit tests for subset LSN positioning — the no-double-count invariant and the merge decision
// table. Pure (no stack): drives `mergeFeedDelta` directly. See
// `docs/ARCHITECTURE.md` §7 (subset queries and client positioning).
// Plus subscription lifecycle (one-shot close, feed cleanup on error) against a fake tRPC.

import { describe, expect, it, vi } from 'vitest'
import type { Row, Schema } from '@electric-circuits/protocol'
import { createSubset, deleteShapeWithRetry, lsnToU64, mergeFeedDelta, type SubsetDeps, type SubsetView } from './subset.js'

// The lifecycle tests never need a real durable stream; an empty tail is enough.
vi.mock('@durable-streams/client', () => ({
  stream: async () => ({ jsonStream: async function* () {} }),
}))

type Env = Parameters<typeof mergeFeedDelta>[1]

function upsert(id: number, lsn: string | undefined, extra: Record<string, unknown> = {}): Env {
  return {
    type: 'issues',
    key: String(id),
    value: { id, ...extra } as Row,
    headers: { operation: 'upsert', ...(lsn ? { lsn } : {}) },
  }
}
function del(id: number, lsn: string | undefined): Env {
  return { type: 'issues', key: String(id), headers: { operation: 'delete', ...(lsn ? { lsn } : {}) } }
}

/** A fresh view seeded with a page read at `snapshotLsn`, window = everything (inView always true). */
function viewSeededWith(pageIds: number[], snapshotLsn: bigint, inView: (r: Row) => boolean = () => true): SubsetView {
  const present = new Set<string>()
  const applied = new Map<string, bigint>()
  for (const id of pageIds) {
    present.add(String(id))
    applied.set(String(id), snapshotLsn)
  }
  return { snapshotLsn, present, applied, inView }
}

describe('lsnToU64', () => {
  it('mirrors the engine pg::lsn_to_u64 ((hi<<32)|lo, hex)', () => {
    expect(lsnToU64('0/0')).toBe(0n)
    expect(lsnToU64('0/2A')).toBe(42n)
    expect(lsnToU64('1/0')).toBe(1n << 32n)
    expect(lsnToU64('0/FF')).toBe(255n)
    expect(lsnToU64('16/0')).toBe(0x16n << 32n)
    expect(lsnToU64(undefined)).toBeNull()
    expect(lsnToU64('')).toBeNull()
  })

  it('returns null (never throws) on a malformed LSN — a throw would kill the feed tail', () => {
    expect(lsnToU64('garbage')).toBeNull() // no slash
    expect(lsnToU64('zz/zz')).toBeNull() // non-hex halves
    expect(lsnToU64('0/')).toBeNull() // empty lo
    expect(lsnToU64('/0')).toBeNull() // empty hi
    // and mergeFeedDelta treats such deltas as fresh (the null-LSN library-mode path)
    const view = viewSeededWith([1], lsnToU64('0/100')!)
    expect(mergeFeedDelta(view, upsert(1, 'not-an-lsn', { t: 'x' }))).toEqual({
      type: 'update',
      value: { id: 1, t: 'x' },
    })
  })
})

describe('mergeFeedDelta — LSN positioning', () => {
  const S = lsnToU64('0/100')! // snapshot at LSN 0x100

  it('drops an overlap delta already reflected in the page (commit LSN < snapshot) — no double-count', () => {
    const view = viewSeededWith([1], S)
    // A delta for the page row that committed BEFORE the snapshot is already in the page → drop.
    expect(mergeFeedDelta(view, upsert(1, '0/80'))).toBeNull()
    // A genuine live update AFTER the snapshot is applied as an update (not a second insert).
    expect(mergeFeedDelta(view, upsert(1, '0/120', { title: 'new' }))).toEqual({
      type: 'update',
      value: { id: 1, title: 'new' },
    })
  })

  it('never re-inserts a row already in the page; only inserts when absent (no double-count under churn)', () => {
    // A single feed is strictly monotonic in commit LSN (one tailer, commit-ordered table stream), so
    // the realistic sequence is non-decreasing. The invariant: no insert is ever emitted for a pk that
    // is currently present (that would double-count the page row); inserts happen only when absent.
    const view = viewSeededWith([1], S)
    const seq: Env[] = [
      upsert(1, '0/90'), // overlap (< snapshot) → already in page → drop
      upsert(1, '0/110'), // live update
      del(1, '0/130'), // leaves
      upsert(1, '0/140'), // genuinely re-created after the delete → one insert
    ]
    const wasPresent: boolean[] = []
    const actions = seq.map((e) => {
      wasPresent.push(view.present.has(e.key))
      return mergeFeedDelta(view, e)
    })
    // No insert was emitted while the key was already present.
    actions.forEach((a, i) => {
      if (a && a.type === 'insert') expect(wasPresent[i]).toBe(false)
    })
    expect(actions).toEqual([
      null,
      { type: 'update', value: { id: 1 } },
      { type: 'delete', key: '1' },
      { type: 'insert', value: { id: 1 } },
    ])
  })

  it('does not admit a pre-snapshot row that was outside the first page (floor = snapshot)', () => {
    const view = viewSeededWith([1], S)
    // id=2 not in page; a delta from before the snapshot belongs to a later page (loadMore), not live.
    expect(mergeFeedDelta(view, upsert(2, '0/80'))).toBeNull()
    expect(view.present.has('2')).toBe(false)
    // A move-in after the snapshot is admitted exactly once.
    expect(mergeFeedDelta(view, upsert(2, '0/140'))).toEqual({ type: 'insert', value: { id: 2 } })
    expect(mergeFeedDelta(view, upsert(2, '0/150'))).toEqual({ type: 'update', value: { id: 2 } })
  })

  it('drops a stale delete older than the row watermark (loadMore-vs-feed race)', () => {
    const L2 = lsnToU64('0/200')!
    const view = viewSeededWith([1], S)
    view.applied.set('1', L2) // row was refreshed by a loadMore page at 0x200
    expect(mergeFeedDelta(view, del(1, '0/180'))).toBeNull() // stale delete → drop, row stays
    expect(view.present.has('1')).toBe(true)
    expect(mergeFeedDelta(view, del(1, '0/210'))).toEqual({ type: 'delete', key: '1' }) // newer → applied
    expect(view.present.has('1')).toBe(false)
  })

  it('emits a move-out delete when an in-view row updates out of the window', () => {
    let cutoff = 1000
    const view = viewSeededWith([1], S, (r) => Number(r.id) <= cutoff)
    // id=1 stays in view on a normal update
    expect(mergeFeedDelta(view, upsert(1, '0/110'))).toEqual({ type: 'update', value: { id: 1 } })
    // now shrink the window; an update that places id=1 outside → move-out delete
    cutoff = 0
    expect(mergeFeedDelta(view, upsert(1, '0/120', { id: 1 }))).toEqual({ type: 'delete', key: '1' })
    expect(view.present.has('1')).toBe(false)
  })

  it('keeps a tombstone watermark on delete so a stale in-flight loadMore page cannot resurrect the row', () => {
    const view = viewSeededWith([1], S)
    const D = lsnToU64('0/150')!
    expect(mergeFeedDelta(view, del(1, '0/150'))).toEqual({ type: 'delete', key: '1' })
    expect(view.present.has('1')).toBe(false)
    // The watermark survives as a tombstone: absent + watermark w = "deleted at ≥ w".
    expect(view.applied.get('1')).toBe(D)
    // The loadMore guard (`pageLsn < w` → skip) then drops a page snapshotted before the delete…
    const stalePageLsn = lsnToU64('0/120')!
    const w = view.applied.get('1')
    expect(w !== undefined && stalePageLsn < w).toBe(true) // row stays deleted
    // …while a page at/after the delete (row genuinely re-created) is admitted.
    const freshPageLsn = lsnToU64('0/160')!
    expect(w !== undefined && freshPageLsn < w).toBe(false)
  })

  it('records a tombstone for a delete of a never-seen pk (no write, but no ghost from a stale page)', () => {
    const view = viewSeededWith([1], S)
    // pk 2 was never loaded: the delete emits nothing, but the watermark is recorded so an in-flight
    // older page (pageLsn < 0x150) cannot insert the row the feed already saw deleted.
    expect(mergeFeedDelta(view, del(2, '0/150'))).toBeNull()
    expect(view.present.has('2')).toBe(false)
    expect(view.applied.get('2')).toBe(lsnToU64('0/150')!)
    // A pre-snapshot delete for an unseen pk is not fresh → still dropped with no tombstone.
    expect(mergeFeedDelta(view, del(3, '0/80'))).toBeNull()
    expect(view.applied.has('3')).toBe(false)
  })

  it('keeps a tombstone watermark on a move-out delete too', () => {
    const view = viewSeededWith([1], S, () => false) // window rejects everything
    expect(mergeFeedDelta(view, upsert(1, '0/120'))).toEqual({ type: 'delete', key: '1' })
    expect(view.present.has('1')).toBe(false)
    expect(view.applied.get('1')).toBe(lsnToU64('0/120')!)
  })

  it('library mode (no LSN on deltas) falls back to idempotent-by-pk apply', () => {
    const view = viewSeededWith([1], 0n)
    expect(mergeFeedDelta(view, upsert(1, undefined, { t: 'x' }))).toEqual({ type: 'update', value: { id: 1, t: 'x' } })
    expect(mergeFeedDelta(view, upsert(2, undefined))).toEqual({ type: 'insert', value: { id: 2 } })
    expect(mergeFeedDelta(view, del(2, undefined))).toEqual({ type: 'delete', key: '2' })
  })
})

// --- subscription lifecycle (fake tRPC; the durable-streams client is mocked to an empty tail) ---

const testSchema: Schema = {
  tables: { issues: { columns: { id: { type: 'int' }, title: { type: 'text' } }, primaryKey: 'id' } },
}

function fakeDeps(opts: { queryError?: Error } = {}) {
  const deleted: string[] = []
  const trpc = {
    subset: {
      live: {
        // The HEAD probe against this unroutable URL fails fast and falls back to the origin offset.
        mutate: async () => ({ shapeId: 'feed-1', streamPath: 'streams/feed-1', streamUrl: 'http://127.0.0.1:9/streams/feed-1' }),
      },
      query: {
        query: async () => {
          if (opts.queryError) throw opts.queryError
          return { rows: [], lsn: '0/100' }
        },
      },
    },
    shapes: {
      delete: {
        mutate: async ({ id }: { id: string }) => {
          deleted.push(id)
          return { ok: true as const }
        },
      },
    },
  }
  const deps: SubsetDeps = {
    trpc: trpc as unknown as SubsetDeps['trpc'],
    schema: testSchema,
    resolveStreamUrl: (h) => h.streamUrl,
  }
  return { deps, deleted }
}

describe('createSubset lifecycle', () => {
  it('close() deletes the server-side feed exactly once (double/concurrent close is a no-op)', async () => {
    const { deps, deleted } = fakeDeps()
    const sub = await createSubset(deps, { table: 'issues' })
    await Promise.all([sub.close(), sub.close()]) // concurrent double-close
    await sub.close() // and a late third
    expect(deleted).toEqual(['feed-1']) // exactly one refcount decrement
  })

  it('deletes the feed before rethrowing when the page query-back fails (no feed leak)', async () => {
    const { deps, deleted } = fakeDeps({ queryError: new Error('boom') })
    await expect(createSubset(deps, { table: 'issues' })).rejects.toThrow('boom')
    expect(deleted).toEqual(['feed-1'])
  })
})

describe('deleteShapeWithRetry', () => {
  it('treats "not found" as success (shape already dropped) without retrying', async () => {
    let calls = 0
    const trpc = {
      shapes: {
        delete: {
          mutate: async () => {
            calls++
            throw Object.assign(new Error('shape x not found'), { data: { code: 'NOT_FOUND', httpStatus: 404 } })
          },
        },
      },
    }
    await deleteShapeWithRetry(trpc as never, 'x')
    expect(calls).toBe(1)
  })

  it('retries a transient failure with backoff until the delete lands', async () => {
    let calls = 0
    const trpc = {
      shapes: {
        delete: {
          mutate: async () => {
            calls++
            if (calls === 1) throw new Error('ECONNRESET')
          },
        },
      },
    }
    await deleteShapeWithRetry(trpc as never, 'x')
    expect(calls).toBe(2)
  })
})
