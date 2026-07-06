# @electric-ivm/client

The browser/Node client for the extended electric-ivm API: a typed tRPC client over
[`@electric-ivm/api`](../../apps/api/README.md) plus `@durable-streams/state` for materializing
shape streams into live TanStack DB collections. (ElectricSQL clients don't use this package —
they sync straight from the engine's `/v1/shape`.)

```ts
import { createClient } from '@electric-ivm/client'

const client = createClient({
  apiUrl,            // the tRPC API server
  schema,            // Schema from @electric-ivm/protocol
  dsBaseUrl,         // optional: durable-streams base override (e.g. '/ds' behind a dev proxy)
  liveMode,          // true (SSE, default) | 'sse' | 'long-poll'
})
```

## `shape(def)` — materialized, live

Registers a shape (backfilled + maintained server-side) and materializes its stream into a
TanStack DB collection. Identical definitions from any number of clients share **one** maintained
stream, ref-counted.

```ts
const shape = await client.shape({
  table: 'issues',
  where: { col: 'status', op: 'eq', value: 'open' },   // Predicate AST (see packages/protocol)
  columns: ['id', 'title', 'status'],                  // optional projection; pk always included
})
shape.currentRows()                    // Row[]
shape.collection                       // TanStack DB collection (usable with useLiveQuery)
const unsub = shape.subscribe((changes) => { /* live change batches */ })
await shape.awaitTxId(txid)            // resolve once the write bearing txid is materialized
await shape.close()
```

## `subset(def)` / `query(def)` — ordered pages, shared live tail

`query()` is one-shot: the engine runs a single `SELECT … ORDER BY … LIMIT/OFFSET` against
Postgres and returns `{ rows, lsn }` — nothing is stored server-side. `subset()` builds on it:
first page + a **changes-only** live tail on the base predicate, merged client-side by per-pk LSN
watermarks (a stale page can never resurrect a deleted row).

```ts
const page = await client.subset({
  table: 'issues',
  orderBy: { col: 'created', desc: true },   // pk appended as tiebreaker
  limit: 50,
  where,
})
page.collection                 // live collection of the loaded window
await page.loadMore(50)         // next keyset page; resolves to rows added (0 when exhausted)
page.hasMore()
await page.close()
```

## `aggregate(def)` — live scalar

```ts
const agg = await client.aggregate({ table: 'issues', fn: 'count', where })
agg.value()                     // number | null (null before first value / empty avg-min-max)
agg.count()                     // matching-row count, available for every fn
agg.subscribe((v) => { … })
await agg.close()
```

`fn` is `'count' | 'sum' | 'avg' | 'min' | 'max'`; `col` is required for all but `count`.

## Writes and lifecycle

Writes go to Postgres with ordinary SQL (the engine ingests via replication). In library mode
(no Postgres), use `client.write(...)` or the schema-derived helpers
`client.tables.<t>.insert/update/delete(row, txid?)`.

**`close()` is one-shot and deletes server-side.** Every `shape()`/`subset()`/`aggregate()` holds
one reference on a ref-counted server object; its `close()` releases exactly that reference (with
retry — there is no server-side reaper) and is guarded against double-close. `client.close()`
tears down everything still open. Design context: [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md).
