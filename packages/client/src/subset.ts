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

  // 1. Open the live tail FIRST so it captures every change from ~now; any overlap with the query-back
  //    snapshot is reconciled idempotently below (upsert/delete by pk).
  const feed = await deps.trpc.subset.live.mutate({ table: def.table, where: def.where as never, columns: cols })

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
  const inView = (row: Row): boolean => ended || boundary == null || cmp(row, boundary) <= 0

  let ctl: SyncCtl | null = null
  const ac = new AbortController()

  const applyEnvelope = (env: StreamEnvelope): void => {
    if (!ctl || env.type !== def.table) return
    const key = env.key
    const op = env.headers.operation
    if (op === 'delete') {
      if (present.has(key)) {
        ctl.begin()
        ctl.write({ type: 'delete', key })
        ctl.commit()
        present.delete(key)
      }
      return
    }
    const value = env.value
    if (!value) return
    if (inView(value)) {
      const type = present.has(key) ? 'update' : 'insert'
      ctl.begin()
      ctl.write({ type, value })
      ctl.commit()
      present.add(key)
    } else if (present.has(key)) {
      // The row moved out of the loaded window (e.g. its sort key dropped below the boundary).
      ctl.begin()
      ctl.write({ type: 'delete', key })
      ctl.commit()
      present.delete(key)
    }
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
          params.write({ type: 'insert', value: r })
          present.add(String(r[pk]))
        }
        params.commit()
        params.markReady()
        // 3. Follow the raw live tail and apply each change, filtered by membership. Reading from the
        //    start of the freshly-created feed stream ('-1') == reading from feed creation onward.
        const url = deps.resolveStreamUrl(feed)
        void (async () => {
          try {
            const resp = await stream<StreamEnvelope>({
              url,
              offset: '-1',
              live: deps.liveMode ?? 'long-poll',
              contentType: 'application/json',
              signal: ac.signal,
            })
            for await (const env of resp.jsonStream()) applyEnvelope(env)
          } catch (e) {
            if (!ac.signal.aborted) console.error('subset feed error', e)
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
      if (ended || !boundary || !ctl) return 0
      const where = andPredicate(def.where, cursorPredicate(pk, def.orderBy, boundary))
      const page = (await deps.trpc.subset.query.query({
        table: def.table,
        where: where as never,
        columns: cols,
        orderBy: def.orderBy,
        limit: pageSize,
      })) as SubsetResult
      if (page.rows.length) {
        ctl.begin()
        for (const r of page.rows) {
          const k = String(r[pk])
          ctl.write({ type: present.has(k) ? 'update' : 'insert', value: r })
          present.add(k)
        }
        ctl.commit()
        boundary = page.rows[page.rows.length - 1]!
      }
      if (page.rows.length < pageSize) ended = true
      return page.rows.length
    },

    async close() {
      ac.abort()
      try {
        await deps.trpc.shapes.delete.mutate({ id: feed.shapeId })
      } catch {
        /* best effort — the feed stream is also reaped server-side when idle */
      }
    },
  }
}
