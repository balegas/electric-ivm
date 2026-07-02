// Subset queries — the non-materialized counterpart to a shape. Rows come from one-shot Postgres
// query-backs (`subset.query`); the loaded page is kept live by following a changes-only tail feed
// (`subset.live`) and re-checking each delta's membership in the loaded window *client-side*. The
// engine never holds per-page/per-range state, so a change is matched against one predicate (the base
// filter) and never fans out across ranges. This is our extension of Electric's static Subset: same
// non-materialized query-back, plus a single-range live tail.

import type { AppRouter } from '@electric-lite/api'
import type { Predicate, Row, Schema, StreamEnvelope, SubsetDef, SubsetResult, Value } from '@electric-lite/protocol'
import { stream } from '@durable-streams/client'
import { createCollection, type Collection } from '@tanstack/db'
import type { createTRPCClient } from '@trpc/client'

type Trpc = ReturnType<typeof createTRPCClient<AppRouter>>

export interface SubsetSubscription<T extends Row = Row> {
  /** Live collection: the query-back rows within the loaded window, kept current by the live tail. */
  collection: Collection<T, string>
  /** Fetch the next page from Postgres and append it; resolves to the rows added (0 once exhausted). */
  loadMore(pageSize?: number): Promise<number>
  /** False once a page returned fewer rows than requested (the set is fully loaded). */
  hasMore(): boolean
  /** Tear down the live feed (drops the server-side changes-only feed) and stop following the tail. */
  close(): Promise<void>
}

export interface SubsetDeps {
  trpc: Trpc
  schema: Schema
  /** Resolve a feed handle to a readable stream URL (honoring any dev-proxy base). */
  resolveStreamUrl(handle: { streamPath: string; streamUrl: string }): string
  liveMode?: boolean | 'sse' | 'long-poll'
}

/** Compare two cell values (numbers numerically, everything else lexically; null sorts first). */
function cmpVal(a: Value, b: Value): number {
  if (a === b) return 0
  if (a == null) return b == null ? 0 : -1
  if (b == null) return 1
  if (typeof a === 'number' && typeof b === 'number') return a - b
  const as = String(a)
  const bs = String(b)
  return as < bs ? -1 : as > bs ? 1 : 0
}

/** Row comparator matching the engine's `ORDER BY <col> <dir>, <pk> <dir>` (pk tiebreaker, same dir). */
function makeCmp(pk: string, orderBy?: { col: string; desc?: boolean }): (a: Row, b: Row) => number {
  const dir = orderBy?.desc ? -1 : 1
  const col = orderBy?.col
  return (a, b) => {
    if (col) {
      const d = cmpVal(a[col], b[col])
      if (d !== 0) return dir * d
    }
    return dir * cmpVal(a[pk], b[pk])
  }
}

/** Keyset predicate for rows strictly *after* `b` in the order — the cursor for the next page. */
function cursorPredicate(pk: string, orderBy: { col: string; desc?: boolean } | undefined, b: Row): Predicate {
  const pkOp = orderBy?.desc ? 'lt' : 'gt'
  if (!orderBy?.col) return { col: pk, op: pkOp, value: b[pk] }
  const colOp = orderBy.desc ? 'lt' : 'gt'
  return {
    or: [
      { col: orderBy.col, op: colOp, value: b[orderBy.col] },
      { and: [{ col: orderBy.col, op: 'eq', value: b[orderBy.col] }, { col: pk, op: pkOp, value: b[pk] }] },
    ],
  }
}

function andPredicate(base: Predicate | undefined, cursor: Predicate): Predicate {
  return base ? { and: [base, cursor] } : cursor
}

/** Does this error mean the shape/feed is already gone? Then a delete has nothing left to do. */
function isNotFoundError(e: unknown): boolean {
  const data = (e as { data?: { code?: string; httpStatus?: number } } | null)?.data
  if (data?.code === 'NOT_FOUND' || data?.httpStatus === 404) return true
  return /not[ _-]?found|404/i.test(e instanceof Error ? e.message : String(e))
}

/**
 * Drop a server-side shape/feed subscriber ref, retrying transient failures. The engine refcounts
 * per identical create and there is NO server-side reaper — a swallowed delete leaks the shape (and
 * its stream) forever — so retry with backoff and only warn once if the delete never lands. "Not
 * found" counts as success (the shape was already dropped, e.g. its stream reaped after the final
 * drop).
 */
export async function deleteShapeWithRetry(trpc: Trpc, id: string): Promise<void> {
  const attempts = 5
  for (let i = 0; i < attempts; i++) {
    try {
      await trpc.shapes.delete.mutate({ id })
      return
    } catch (e) {
      if (isNotFoundError(e)) return
      if (i === attempts - 1) {
        console.warn(`client: failed to delete shape ${id} after ${attempts} attempts:`, e)
        return
      }
      await new Promise((r) => setTimeout(r, 200 * 2 ** i))
    }
  }
}

/**
 * Parse a Postgres LSN (`"HI/LO"` hex) into a comparable bigint — mirrors the engine's
 * `pg::lsn_to_u64` (`(hi << 32) | lo`). `null`/empty/malformed → null (library/no-Postgres mode, or
 * an unparseable header): callers treat a null LSN as "apply fresh". Never throws — a throw here
 * would propagate out of the feed's async iterator and silently kill the live tail.
 */
export function lsnToU64(lsn: string | undefined | null): bigint | null {
  if (!lsn) return null
  const slash = lsn.indexOf('/')
  if (slash < 0) return null
  const hi = Number.parseInt(lsn.slice(0, slash), 16)
  const lo = Number.parseInt(lsn.slice(slash + 1), 16)
  if (Number.isNaN(hi) || Number.isNaN(lo)) return null
  return (BigInt(hi) << 32n) | BigInt(lo)
}

/** The loaded subset window's membership + per-row LSN watermark (the merge state). */
export interface SubsetView {
  snapshotLsn: bigint
  present: Set<string>
  applied: Map<string, bigint>
  /** Is the row within the currently-loaded keyset window? */
  inView: (row: Row) => boolean
}

/** A collection write to emit, or `null` to drop the delta. */
export type MergeAction =
  | { type: 'insert' | 'update'; value: Row }
  | { type: 'delete'; key: string }
  | null

/**
 * Decide how one live-feed delta updates the loaded subset view, applying **LSN positioning** +
 * **last-writer-wins**. Mutates `view.present`/`view.applied`. Returns the write to emit, or `null`
 * to drop the delta because it is: already reflected in the page (commit LSN < the row's watermark /
 * the snapshot floor), stale w.r.t. a newer page/delta, or out of the loaded window. Exported so the
 * no-double-count invariant can be unit-tested without the full stack.
 */
export function mergeFeedDelta(view: SubsetView, env: StreamEnvelope): MergeAction {
  const key = env.key
  const deltaLsn = lsnToU64(env.headers.lsn)
  // A null LSN (library/no-Postgres mode) always applies — the old idempotent-by-pk behaviour.
  const fresh = (): boolean => {
    if (deltaLsn === null) return true
    const w = view.applied.get(key)
    return w === undefined ? deltaLsn >= view.snapshotLsn : deltaLsn >= w
  }
  if (env.headers.operation === 'delete') {
    if (!fresh()) return null
    const wasPresent = view.present.has(key)
    view.present.delete(key)
    // Keep a tombstone watermark instead of clearing it: absence from `present` + watermark w means
    // "deleted at ≥ w", so an in-flight loadMore page snapshotted before the delete (pageLsn < w)
    // is skipped by the loadMore guard rather than resurrecting the row. Recorded even for a
    // never-seen pk — otherwise a stale page could insert a ghost row the feed already deleted.
    if (deltaLsn !== null) view.applied.set(key, deltaLsn)
    else view.applied.delete(key)
    return wasPresent ? { type: 'delete', key } : null
  }
  const value = env.value
  if (!value || !fresh()) return null
  if (view.inView(value)) {
    const type = view.present.has(key) ? 'update' : 'insert'
    view.present.add(key)
    if (deltaLsn !== null) view.applied.set(key, deltaLsn)
    return { type, value }
  }
  if (view.present.has(key)) {
    // Moved out of the loaded window (e.g. its sort key dropped below the boundary). Same tombstone
    // treatment as a delete: a stale in-flight page must not re-insert the pre-move version.
    view.present.delete(key)
    if (deltaLsn !== null) view.applied.set(key, deltaLsn)
    else view.applied.delete(key)
    return { type: 'delete', key }
  }
  return null
}

/** Manual-write handles captured from the collection's sync callback (used by load-more + the feed). */
interface SyncCtl {
  begin: () => void
  write: (m: { type: 'insert' | 'update' | 'delete'; value?: Row; key?: string }) => void
  commit: () => void
}

export async function createSubset<T extends Row = Row>(
  deps: SubsetDeps,
  def: SubsetDef,
): Promise<SubsetSubscription<T>> {
  const tableDef = deps.schema.tables[def.table]
  if (!tableDef) throw new Error(`client: unknown table "${def.table}"`)
  const pk = tableDef.primaryKey
  const cmp = makeCmp(pk, def.orderBy)
  const limit = def.limit ?? 100
  // The order column + pk must be present on every row so membership/cursoring can be evaluated, even
  // when the caller projects a narrower column set.
  const cols = def.columns
    ? Array.from(new Set([pk, ...(def.orderBy ? [def.orderBy.col] : []), ...def.columns]))
    : undefined

  // 1. Open the live tail FIRST so it captures every change from ~now. The feed may be SHARED with other
  //    subscriptions on the same predicate (the engine ref-counts identical changes-only feeds).
  const feed = await deps.trpc.subset.live.mutate({ table: def.table, where: def.where as never, columns: cols })
  const feedUrl = deps.resolveStreamUrl(feed)

  const ac = new AbortController()
  let closed = false

  // The feed above is the only server-side state we hold; if any of the remaining setup (offset
  // capture, page query-back, preload) throws, delete it before rethrowing — nobody else will.
  try {
    // 1b. Capture the feed's current tail offset BEFORE the page snapshot. Reading the live tail from
    //     here (rather than the stream origin) means a joiner to a SHARED, long-lived feed does not
    //     replay the whole backlog — it starts at "≈now". Everything at/before this offset committed
    //     before the snapshot LSN below and is already in the page; the `< snapshotLsn` drop covers the
    //     small [thisOffset, snapshot] overlap. Falls back to the stream origin if HEAD is unavailable.
    let feedOffset = '-1'
    try {
      const head = await fetch(feedUrl, { method: 'HEAD' })
      feedOffset = head.headers.get('stream-next-offset') ?? '-1'
    } catch {
      /* proxy/env without HEAD support → read from origin; correctness unaffected (only backlog). */
    }

    // 2. Query-back page 1 straight from Postgres (no stream, no materialization).
    const first = (await deps.trpc.subset.query.query({
      table: def.table,
      where: def.where as never,
      columns: cols,
      orderBy: def.orderBy,
      limit,
    })) as SubsetResult

    // `boundary` = the last (lowest-in-order) loaded row; the loaded window is everything sorting <= it.
    let boundary: Row | null = first.rows.length ? first.rows[first.rows.length - 1]! : null
    let ended = first.rows.length < limit
    const present = new Set<string>()
    // LSN positioning: `snapshotLsn` is the page's read point in the engine's replication timeline.
    // `applied` is a per-present-row watermark — the snapshot LSN the row's current value was read at
    // (page or loadMore), bumped to a feed delta's LSN when applied. A feed delta is accepted only if
    // its commit LSN is at/after the relevant watermark, so deltas already reflected in the page (commit
    // LSN < snapshotLsn) are dropped — exactly-once after the snapshot, no double-count.
    const snapshotLsn = lsnToU64(first.lsn) ?? 0n
    const applied = new Map<string, bigint>()
    const inView = (row: Row): boolean => ended || boundary == null || cmp(row, boundary) <= 0

    let ctl: SyncCtl | null = null
    let loadsInFlight = 0

    const view: SubsetView = { snapshotLsn, present, applied, inView }
    const applyEnvelope = (env: StreamEnvelope): void => {
      if (!ctl || env.type !== def.table) return
      const action = mergeFeedDelta(view, env)
      if (!action) return
      ctl.begin()
      ctl.write(action)
      ctl.commit()
    }

    const collection = createCollection<T>({
      id: `subset:${def.table}:${feed.shapeId}`,
      getKey: (r) => String((r as Row)[pk]),
      sync: {
        sync: (params: SyncCtl & { markReady: () => void }) => {
          ctl = params
          // Seed the query-back page.
          params.begin()
          for (const r of first.rows) {
            const k = String(r[pk])
            params.write({ type: 'insert', value: r })
            present.add(k)
            applied.set(k, snapshotLsn)
          }
          params.commit()
          params.markReady()
          // 3. Follow the raw live tail from the offset captured before the snapshot, applying each change
          //    filtered by membership + LSN positioning (deltas already in the page are dropped).
          void (async () => {
            try {
              const resp = await stream<StreamEnvelope>({
                url: feedUrl,
                offset: feedOffset,
                live: deps.liveMode ?? 'long-poll',
                contentType: 'application/json',
                signal: ac.signal,
              })
              for await (const env of resp.jsonStream()) applyEnvelope(env)
            } catch (e) {
              // After close() the feed's durable stream may already be gone (the engine deletes it on
              // the final drop), so a racing read 404s — normal termination, not an error.
              if (!ac.signal.aborted && !closed) console.error('subset feed error', e)
            }
          })()
          return () => ac.abort()
        },
      },
    })
    await collection.preload()

    return {
      collection: collection as Collection<T, string>,
      hasMore: () => !ended,

      async loadMore(pageSize = limit) {
        if (closed || ended || !boundary || !ctl) return 0
        const where = andPredicate(def.where, cursorPredicate(pk, def.orderBy, boundary))
        loadsInFlight++
        try {
          const page = (await deps.trpc.subset.query.query({
            table: def.table,
            where: where as never,
            columns: cols,
            orderBy: def.orderBy,
            limit: pageSize,
          })) as SubsetResult
          if (page.rows.length) {
            // This page is a fresh Postgres snapshot at `pageLsn`; its rows are the authoritative state as
            // of that LSN. Don't let a stale page regress a row already advanced past `pageLsn` by the live
            // feed (the loadMore-vs-feed race), and set each row's watermark so older feed deltas drop.
            // Tombstoned rows (watermark without membership) are skipped the same way — a page older than
            // the delete must not resurrect the row.
            const pageLsn = lsnToU64(page.lsn) ?? snapshotLsn
            ctl.begin()
            for (const r of page.rows) {
              const k = String(r[pk])
              const w = applied.get(k)
              if (w !== undefined && pageLsn < w) continue
              ctl.write({ type: present.has(k) ? 'update' : 'insert', value: r })
              present.add(k)
              applied.set(k, pageLsn)
            }
            ctl.commit()
            boundary = page.rows[page.rows.length - 1]!
          }
          if (page.rows.length < pageSize) ended = true
          return page.rows.length
        } finally {
          loadsInFlight--
          // Tombstone watermarks only exist to guard in-flight loadMore pages; once none are in
          // flight, prune them so delete churn doesn't grow `applied` unboundedly.
          if (loadsInFlight === 0) {
            for (const k of applied.keys()) if (!present.has(k)) applied.delete(k)
          }
        }
      },

      async close() {
        // One-shot: the engine DELETE decrements a shared refcount per call, so a double close must
        // not steal another subscriber's reference on a shared feed.
        if (closed) return
        closed = true
        ac.abort()
        await deleteShapeWithRetry(deps.trpc, feed.shapeId)
      },
    }
  } catch (e) {
    closed = true
    ac.abort()
    await deleteShapeWithRetry(deps.trpc, feed.shapeId)
    throw e
  }
}
