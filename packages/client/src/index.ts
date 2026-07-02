// electric-ivm client: a thin wrapper over a typed tRPC client plus stream-db
// (`@durable-streams/state/db`) for materializing a shape into a live TanStack DB collection.

import type { AppRouter } from '@electric-ivm/api'
import type {
  AggregateDef,
  Op,
  Row,
  Schema,
  ShapeDef,
  StreamEnvelope,
  SubsetDef,
  SubsetResult,
  TableDef,
  Value,
} from '@electric-ivm/protocol'
import { stream } from '@durable-streams/client'
import { createStateSchema, createStreamDB } from '@durable-streams/state/db'
import { createTRPCClient, httpBatchLink } from '@trpc/client'
import { z } from 'zod'

import { createSubset, deleteShapeWithRetry, type SubsetSubscription } from './subset.js'

export type { SubsetSubscription } from './subset.js'
// LSN-positioning primitives (also unit-tested in subset.test.ts) — exported so integration tests can
// exercise the real merge logic against the live engine.
export { lsnToU64, mergeFeedDelta, type SubsetView, type MergeAction } from './subset.js'

export interface ShapeHandle {
  shapeId: string
  table: string
  streamPath: string
  streamUrl: string
}

export interface ShapeMaterialization {
  handle: ShapeHandle
  /** The underlying TanStack DB collection (usable with @tanstack/react-db's useLiveQuery). */
  collection: unknown
  /** Current materialized rows (declared columns + virtual props). */
  currentRows(): Row[]
  /** Resolve once an event bearing `txid` has been consumed (append-then-read determinism). */
  awaitTxId(txid: string, timeoutMs?: number): Promise<void>
  /** Subscribe to live change batches; returns an unsubscribe fn. */
  subscribe(cb: (changes: Array<{ type: string; key: unknown; value?: unknown }>) => void): () => void
  close(): Promise<void>
}

/** Per-table ingestion helpers derived from the schema (pk read from the row's pk column). */
export interface TableApi {
  insert(row: Row, txid?: string): Promise<{ txid: string }>
  update(row: Row, txid?: string): Promise<{ txid: string }>
  delete(pk: Value, txid?: string): Promise<{ txid: string }>
}

/** A live scalar aggregation (COUNT/SUM/AVG/MIN/MAX) maintained by the engine. */
export interface AggregateSubscription {
  /** Current aggregate value (null before the first value, or empty avg/min/max). */
  value(): number | null
  /** Count of rows matching the predicate (available for every aggregation). */
  count(): number
  subscribe(cb: (value: number | null) => void): () => void
  close(): Promise<void>
}

export interface ElectricIvmClient {
  defineSchema(schema: Schema): Promise<unknown>
  write(input: { table: string; op: Op; pk: Value; row?: Row; txid?: string }): Promise<{ txid: string }>
  /** Schema-derived typed ingestion API, one entry per table. */
  tables: Record<string, TableApi>
  /** Register a **materialized, live** shape (backfilled + maintained as a durable stream). */
  shape(def: ShapeDef): Promise<ShapeMaterialization>
  /**
   * Run a one-shot **subset query** — the non-materialized counterpart to {@link shape}. Returns the
   * page rows + the Postgres snapshot LSN directly, with no stream and no server-side state. Page by
   * moving a keyset cursor in `where` (preferred) or bumping `offset`; keep it live by following the
   * table's tail and re-checking view membership rather than materializing a per-page shape.
   */
  query(def: SubsetDef): Promise<SubsetResult>
  /**
   * Open a **live subset**: query-back the first page, then follow the table's tail to keep the loaded
   * window current (paging via {@link SubsetSubscription.loadMore}). Non-materialized — the engine
   * never stores the page; a change is matched against one base predicate, never fanned across ranges.
   */
  subset(def: SubsetDef): Promise<SubsetSubscription>
  /** Open a live scalar **aggregation** over a filtered set (electric-ivm extension). */
  aggregate(def: AggregateDef): Promise<AggregateSubscription>
  close(): Promise<void>
}

function zodRowSchema(def: TableDef, cols?: string[]): z.ZodType {
  // When the shape projects a column subset, validate only those columns (+ pk) — the projected rows
  // genuinely omit the rest, so requiring them would reject every row. The pk is always present.
  const names = cols ? Array.from(new Set([def.primaryKey, ...cols])) : Object.keys(def.columns)
  const shape: Record<string, z.ZodTypeAny> = {}
  for (const col of names) {
    const c = def.columns[col]
    if (!c) continue
    // pk is validated as its declared type here, then the dispatcher stringifies it on the row.
    const base = c.type === 'bool' ? z.boolean() : c.type === 'text' ? z.string() : z.number()
    // Non-pk columns are nullable (the pk is never null); allow null cells to materialize.
    shape[col] = col === def.primaryKey ? base : base.nullable()
  }
  // be permissive about extra/loose fields the stream layer may add
  return z.object(shape).loose()
}

export function createClient(opts: {
  apiUrl: string
  schema: Schema
  /** Override the durable-streams base URL for shape reads (e.g. '/ds' behind a dev proxy). */
  dsBaseUrl?: string
  /** Live mode passed to stream-db. 'long-poll' is the most proxy-friendly. Default true (SSE). */
  liveMode?: boolean | 'sse' | 'long-poll'
}): ElectricIvmClient {
  const trpc = createTRPCClient<AppRouter>({ links: [httpBatchLink({ url: opts.apiUrl })] })
  // Everything the client opens (shape materializations, subset subscriptions AND aggregate
  // subscriptions) so `close()` can tear them all down — otherwise a live stream leaks and blocks
  // shutdown. `track` wraps each close with a one-shot guard and prunes the entry on completion:
  // the engine DELETE decrements a shared refcount per call, so every subscription must be closed
  // exactly once (a double close would steal another subscriber's reference on a shared shape).
  const open: { close: () => Promise<void> }[] = []
  function track<T extends { close(): Promise<void> }>(item: T): T {
    const inner = item.close.bind(item)
    let closing: Promise<void> | undefined
    item.close = () => {
      closing ??= inner().finally(() => {
        const i = open.indexOf(item)
        if (i >= 0) open.splice(i, 1)
      })
      return closing
    }
    open.push(item)
    return item
  }

  const write = (input: { table: string; op: Op; pk: Value; row?: Row; txid?: string }) =>
    trpc.ingest.write.mutate(input)

  // Derive a typed ingestion helper per table from the schema.
  const tables: Record<string, TableApi> = {}
  for (const [table, tdef] of Object.entries(opts.schema.tables)) {
    const pkCol = tdef.primaryKey
    tables[table] = {
      insert: (row, txid) => write({ table, op: 'insert', pk: row[pkCol] ?? null, row, txid }),
      update: (row, txid) => write({ table, op: 'update', pk: row[pkCol] ?? null, row, txid }),
      delete: (pk, txid) => write({ table, op: 'delete', pk, txid }),
    }
  }

  return {
    defineSchema: (schema) => trpc.schema.define.mutate({ schema }),

    write,
    tables,

    async shape(def) {
      const tableDef = opts.schema.tables[def.table]
      if (!tableDef) throw new Error(`client: unknown table "${def.table}"`)

      const handle = (await trpc.shapes.create.mutate({
        table: def.table,
        where: def.where as never,
        columns: def.columns,
      })) as ShapeHandle

      const state = createStateSchema({
        [def.table]: { schema: zodRowSchema(tableDef, def.columns), type: def.table, primaryKey: tableDef.primaryKey },
      })
      const streamUrl = opts.dsBaseUrl
        ? `${opts.dsBaseUrl.replace(/\/$/, '')}/${handle.streamPath}`
        : handle.streamUrl
      const db = createStreamDB({
        streamOptions: { url: streamUrl, contentType: 'application/json' },
        state,
        live: opts.liveMode ?? true,
      })
      await db.preload()
      const collection = db.collections[def.table]

      const mat: ShapeMaterialization = {
        handle,
        collection,
        currentRows: () => collection.toArray as Row[],
        awaitTxId: (txid, timeoutMs) => db.utils.awaitTxId(txid, timeoutMs),
        subscribe: (cb) => {
          const sub = collection.subscribeChanges(cb as never, { includeInitialState: false })
          return () => sub.unsubscribe()
        },
        close: async () => {
          await db.close?.()
          // Drop our subscriber ref: creates are refcounted server-side (share=true), so every
          // shape() must delete exactly once or the shape (and its stream) leaks forever.
          await deleteShapeWithRetry(trpc, handle.shapeId)
        },
      }
      return track(mat)
    },

    async query(def) {
      const result = await trpc.subset.query.query({
        table: def.table,
        where: def.where as never,
        columns: def.columns,
        orderBy: def.orderBy,
        limit: def.limit,
        offset: def.offset,
      })
      return result as SubsetResult
    },

    async subset(def) {
      const sub = await createSubset(
        {
          trpc,
          schema: opts.schema,
          liveMode: opts.liveMode === true ? 'long-poll' : (opts.liveMode ?? 'long-poll'),
          resolveStreamUrl: (handle) =>
            opts.dsBaseUrl ? `${opts.dsBaseUrl.replace(/\/$/, '')}/${handle.streamPath}` : handle.streamUrl,
        },
        def,
      )
      return track(sub)
    },

    async aggregate(def) {
      const handle = (await trpc.aggregate.create.mutate({
        table: def.table,
        where: def.where as never,
        fn: def.fn,
        col: def.col,
      })) as ShapeHandle
      const url = opts.dsBaseUrl
        ? `${opts.dsBaseUrl.replace(/\/$/, '')}/${handle.streamPath}`
        : handle.streamUrl
      let current: number | null = null
      let n = 0
      const subs = new Set<(v: number | null) => void>()
      const ac = new AbortController()
      // The engine streams the running aggregate as `{ value, n }` envelopes (keyed "agg"); keep the latest.
      void (async () => {
        try {
          const resp = await stream<StreamEnvelope>({
            url,
            offset: '-1',
            live: opts.liveMode === true ? 'long-poll' : (opts.liveMode ?? 'long-poll'),
            contentType: 'application/json',
            signal: ac.signal,
          })
          for await (const env of resp.jsonStream()) {
            const v = env.value as { value?: number | null; n?: number } | undefined
            if (v && 'value' in v) {
              current = (v.value ?? null) as number | null
              n = v.n ?? 0
              for (const cb of subs) cb(current)
            }
          }
        } catch (e) {
          // After close() the aggregate's durable stream may already be gone (the engine deletes it
          // on the final drop), so a racing read 404s — normal termination, not an error.
          if (!ac.signal.aborted) console.error('aggregate stream error', e)
        }
      })()
      const sub: AggregateSubscription = {
        value: () => current,
        count: () => n,
        subscribe: (cb) => {
          subs.add(cb)
          return () => {
            subs.delete(cb)
          }
        },
        close: async () => {
          ac.abort()
          await deleteShapeWithRetry(trpc, handle.shapeId)
        },
      }
      return track(sub)
    },

    async close() {
      // Iterate a copy: each close() prunes itself from `open`, and anything the caller already
      // closed is gone — so teardown is exactly-once per subscription.
      for (const m of [...open]) await m.close()
    },
  }
}
