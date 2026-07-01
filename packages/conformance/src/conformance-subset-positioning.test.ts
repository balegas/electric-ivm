// Subset LSN positioning — end-to-end against the live engine (real Postgres replication + commit-LSN
// stamping). Validates the no-double-count guarantee: a change committing in the overlap window
// [feed-open, page-snapshot] is reflected in the page AND emitted on the live feed, and the client's
// LSN positioning drops the feed copy (commit LSN < snapshot LSN) so the row is counted exactly once.
// See docs/superpowers/specs/2026-07-01-subset-lsn-positioning-design.md.

import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import pgpkg from 'pg'
import type { Row, Schema, StreamEnvelope } from '@electric-lite/protocol'
import { lsnToU64, mergeFeedDelta, type SubsetView } from '@electric-lite/client'
import { bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: { items: { columns: { id: { type: 'int' }, n: { type: 'int' } }, primaryKey: 'id' } },
}

let h: Harness
beforeEach(async () => {
  h = await bootHarness(schema)
})
afterEach(async () => {
  await h.shutdown()
})

async function pg(sql: string, params: unknown[] = []): Promise<Row[]> {
  const c = new pgpkg.Client({ connectionString: h.pgUrl })
  await c.connect()
  try {
    return (await c.query(sql, params)).rows as Row[]
  } finally {
    await c.end().catch(() => {})
  }
}

async function postJson<T>(url: string, body: unknown): Promise<T> {
  const res = await fetch(url, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) })
  if (!res.ok) throw new Error(`POST ${url} -> ${res.status} ${await res.text()}`)
  return (await res.json()) as T
}

/** Read every envelope currently on a stream (catch-up only; mirrors the engine's non-live ds.read). */
async function readFeed(streamUrl: string): Promise<StreamEnvelope[]> {
  const out: StreamEnvelope[] = []
  let offset = '-1'
  for (let i = 0; i < 100; i++) {
    const res = await fetch(`${streamUrl}?offset=${encodeURIComponent(offset)}`)
    if (res.status === 204) break
    if (!res.ok) throw new Error(`feed read -> ${res.status}`)
    const next = res.headers.get('stream-next-offset')
    const upToDate = res.headers.get('stream-up-to-date') != null
    const text = (await res.text()).trim()
    if (text) out.push(...(JSON.parse(text) as StreamEnvelope[]))
    if (next) offset = next
    if (upToDate || !text) break
  }
  return out
}

describe('subset LSN positioning (end-to-end)', () => {
  it('drops the overlap-window delta already in the page — exactly-once, no double-count', async () => {
    // 1. Seed the table and let the engine ingest it.
    await pg('INSERT INTO items (id, n) VALUES (1, 10), (2, 20), (3, 30)')
    await drainEngine(h)

    // 2. Open the live feed FIRST (changes-only, match-all). The engine now forwards every future match.
    const feed = await postJson<{ shapeId: string; streamUrl: string }>(`${h.engineUrl}/shapes`, {
      table: 'items',
      where: null,
      changesOnly: true,
    })

    // 3. OVERLAP write: committed after feed-open but before the page snapshot. It will appear BOTH in
    //    the page (the snapshot sees n=11) and on the feed (commit LSN < snapshot LSN).
    await pg('UPDATE items SET n = 11 WHERE id = 1')
    await drainEngine(h)

    // 4. Page snapshot — the engine returns the rows + the snapshot LSN S.
    const page = await postJson<{ rows: Row[]; lsn: string }>(`${h.engineUrl}/query`, {
      table: 'items',
      where: null,
      orderBy: { col: 'id' },
      limit: 100,
    })
    const S = lsnToU64(page.lsn)!
    expect(page.rows.find((r) => r.id === 1)!.n).toBe(11) // page reflects the overlap write

    // 5. POST-snapshot write: a genuinely new change after S.
    await pg('INSERT INTO items (id, n) VALUES (4, 40)')
    await drainEngine(h)

    // 6. Read the feed. The engine must stamp the commit LSN on every envelope, and the overlap delta
    //    for id=1 must carry an LSN strictly below S (so positioning has real work to do).
    const envs = await readFeed(feed.streamUrl)
    expect(envs.length).toBeGreaterThan(0)
    for (const e of envs) expect(e.headers.lsn, `envelope ${e.key} missing lsn`).toBeDefined()
    const overlap = envs.filter((e) => e.key === '1')
    expect(overlap.length).toBeGreaterThan(0)
    for (const e of overlap) expect(lsnToU64(e.headers.lsn)! < S).toBe(true)
    expect(envs.some((e) => e.key === '4' && lsnToU64(e.headers.lsn)! >= S)).toBe(true)

    // 7. Replay the feed through the REAL client merge, seeded with the page (present + watermark = S).
    const present = new Set<string>()
    const applied = new Map<string, bigint>()
    const collection = new Map<string, Row>()
    for (const r of page.rows) {
      const k = String(r.id)
      present.add(k)
      applied.set(k, S)
      collection.set(k, r)
    }
    const view: SubsetView = { snapshotLsn: S, present, applied, inView: () => true }

    let id1FeedWrites = 0
    for (const e of envs) {
      const action = mergeFeedDelta(view, e)
      if (e.key === '1' && action) id1FeedWrites++
      if (!action) continue
      if (action.type === 'delete') collection.delete(e.key)
      else collection.set(e.key, action.value)
    }

    // 8a. No double-count: the page already holds id=1; the only feed delta for it is the overlap copy
    //     (LSN < S), which positioning drops — so the feed performs ZERO writes for id=1.
    expect(id1FeedWrites).toBe(0)

    // 8b. The post-snapshot insert (id=4) was applied exactly once; final set equals Postgres.
    const oracle = await pg('SELECT id, n FROM items ORDER BY id')
    const got = [...collection.values()].sort((a, b) => Number(a.id) - Number(b.id))
    expect(got).toEqual(oracle)
    expect(collection.size).toBe(4)
  })

  it('keeps the live subset convergent and duplicate-free under a write burst (real client path)', async () => {
    // Seed 30 rows; open a live subset over a window of 10 ordered by n.
    const seed: string[] = []
    for (let i = 1; i <= 30; i++) seed.push(`(${i}, ${i * 10})`)
    await pg(`INSERT INTO items (id, n) VALUES ${seed.join(',')}`)
    await drainEngine(h)

    const sub = await h.client.subset({ table: 'items', orderBy: { col: 'n' }, limit: 10 })
    try {
      // Burst of writes that touch in-window rows (update, delete, move-in), exercising the feed.
      await pg('UPDATE items SET n = 5 WHERE id = 25') // move a far row into the front window
      await pg('DELETE FROM items WHERE id = 3')
      await pg('UPDATE items SET n = 15 WHERE id = 2') // value change within window
      await pg('INSERT INTO items (id, n) VALUES (40, 1)') // new front row
      await drainEngine(h)
      await new Promise((r) => setTimeout(r, 300)) // let the live tail apply

      const arr = sub.collection.toArray as unknown as Row[] // TanStack DB collection -> array

      // No duplicate primary keys in the materialized window.
      const ids = arr.map((r) => String(r.id))
      expect(new Set(ids).size).toBe(ids.length)

      // Every materialized row matches its CURRENT Postgres value (no stale / regressed rows).
      const live = await pg('SELECT id, n FROM items')
      const byId = new Map(live.map((r) => [String(r.id), r.n]))
      for (const r of arr) expect(r.n, `row ${r.id} stale`).toBe(byId.get(String(r.id)))
    } finally {
      await sub.close()
    }
  })
})

describe('subset feed sharing (dedup + refcount)', () => {
  const createFeed = (where: unknown) =>
    postJson<{ shapeId: string; streamUrl: string; streamPath: string }>(`${h.engineUrl}/shapes`, {
      table: 'items',
      where,
      changesOnly: true,
    })
  const shapeExists = async (id: string) => (await fetch(`${h.engineUrl}/shapes/${encodeURIComponent(id)}`)).status === 200

  it('identical changes-only feeds share one stream; distinct predicates do not; ref-count governs teardown', async () => {
    await pg('INSERT INTO items (id, n) VALUES (1, 10), (2, 20)')
    await drainEngine(h)

    const whereA = { col: 'n', op: 'gte', value: 10 }
    const a1 = await createFeed(whereA)
    const a2 = await createFeed(whereA) // identical predicate → JOIN the same feed
    const b = await createFeed({ col: 'n', op: 'lt', value: 5 }) // different predicate → separate feed

    // Sharing: the two identical feeds are the SAME server object (one stream, one routed entry).
    expect(a2.shapeId).toBe(a1.shapeId)
    expect(a2.streamPath).toBe(a1.streamPath)
    expect(b.shapeId).not.toBe(a1.shapeId)

    // Ref-count: dropping ONE holder keeps the shared feed alive for the other.
    await fetch(`${h.engineUrl}/shapes/${a1.shapeId}`, { method: 'DELETE' })
    expect(await shapeExists(a1.shapeId)).toBe(true)

    // Dropping the last holder tears it down.
    await fetch(`${h.engineUrl}/shapes/${a2.shapeId}`, { method: 'DELETE' })
    expect(await shapeExists(a1.shapeId)).toBe(false)

    // The distinct feed is unaffected.
    expect(await shapeExists(b.shapeId)).toBe(true)
  })
})
