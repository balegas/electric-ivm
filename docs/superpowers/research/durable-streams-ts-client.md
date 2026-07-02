# Durable Streams TypeScript client: materializing a stream into a reactive collection

Research brief for electric-ivm. Goal: subscribe to a per-shape durable stream
(a feed of `{op, pk, row}` change-events), materialize the current set + receive
live updates, then read the current materialized state in **Node (no React)** and
assert set-equality vs an oracle.

Date: 2026-06-27. Sources: npm registry, the published package tarballs
(`@durable-streams/state@0.3.1`, `@durable-streams/client@0.2.6`,
`@tanstack/db@0.6.12`), the package READMEs, and `STATE-PROTOCOL.md` shipped in
the `@durable-streams/state` tarball. Facts below are verified against the actual
shipped `.d.ts` / `src/*.ts` unless explicitly marked UNVERIFIED.

> Naming note: there is **no npm package literally named `stream-db`**
> (`npm view stream-db` → 404, name was unpublished in 2020). "StreamDB" /
> "stream-db" is the *feature name* for the TanStack-DB-backed layer that lives
> in **`@durable-streams/state`** under its **`./db` subpath**
> (`@durable-streams/state/db`), exporting `createStreamDB`. The
> https://durablestreams.com/stream-db docs page describes exactly this module.

---

## 1. Packages, versions, peer deps

| Package | Latest | Role | Notes |
|---|---|---|---|
| `@durable-streams/state` | **0.3.1** | Schema protocol + StreamDB layer | Apache-2.0. deps: `@durable-streams/client@0.2.6` (pinned exact), `@standard-schema/spec@^1.0.0`. **peerDependency: `@tanstack/db` `>=0.6.0 <1.0.0`** |
| `@durable-streams/client` | **0.2.6** | Low-level durable-stream HTTP client | deps: `@microsoft/fetch-event-source@^2.0.1`, `fastq@^1.19.1`. Pulled in transitively by state. |
| `@tanstack/db` | **0.6.12** | Reactive collections / differential dataflow | peerDependency: `typescript >=4.7`. **You must install this yourself** (peer of state). |
| `@standard-schema/spec` | ^1.x | Standard Schema type contract | Transitive; only types. |

Install:

```bash
pnpm add @durable-streams/state @tanstack/db
# zod (or valibot/arktype) if you want a real validator:
pnpm add zod
# @durable-streams/client comes in transitively, but you can add it explicitly
# if you want to consume the raw stream yourself:
pnpm add @durable-streams/client
```

Two import surfaces:

- `@durable-streams/state` — **db-free**. Schema definition + event construction
  + a plain in-memory `MaterializedState` class. Does NOT import `@tanstack/db`.
- `@durable-streams/state/db` — **superset**. Everything above PLUS
  `createStreamDB`, and re-exports of `@tanstack/db` helpers (`createCollection`,
  `eq`, `and`, `count`, `createLiveQueryCollection`, `queryOnce`, etc.). Importing
  this pulls in the `@tanstack/db` peer.

```ts
import { createStreamDB, createStateSchema } from "@durable-streams/state/db"
// or, db-free:
import { createStateSchema, MaterializedState } from "@durable-streams/state"
```

---

## 2. Pointing the client at a server URL + stream path

There is **no separate "server URL" + "path" split** — the durable stream is
identified by a single absolute `url` (server base + stream path concatenated).
You pass it via `streamOptions.url`.

```ts
const db = createStreamDB({
  streamOptions: {
    url: "https://api.example.com/streams/my-shape-id", // full stream URL
    contentType: "application/json",                     // REQUIRED for the state protocol
    headers: { Authorization: "Bearer <token>" },        // optional
    batching: true,                                      // optional (append batching)
  },
  state: schema,
  live: true, // see live modes below; default true
})
```

`streamOptions` is the `@durable-streams/client` `DurableStreamOptions`
(`url: string | URL`, `contentType?`, `headers?`, `batching?`). The state
protocol REQUIRES `Content-Type: application/json` streams.

`live` (`LiveMode`) accepts: `true` (default), `false`, `"sse"`, `"long-poll"`.
- `false` → read once up to current head, do not stay live.
- `true` / `"sse"` / `"long-poll"` → stay connected for live updates.

Alternative: pass a pre-built `stream` instance instead of `streamOptions` to
reuse one connection:

```ts
import { DurableStream } from "@durable-streams/client"
const stream = new DurableStream({ url, contentType: "application/json" })
const db = createStreamDB({ stream, state: schema })
```

The stream is created **lazily**: `createStreamDB` is synchronous and does NOT
connect. The connection opens on the first `db.preload()`.

---

## 3. Schema / collection definition, preload, and event→row mapping

### 3.1 Schema (Standard Schema — Zod/Valibot/ArkType all work)

`createStateSchema` maps a logical collection name → `{ schema, type, primaryKey }`.

```ts
import { z } from "zod"
import { createStateSchema } from "@durable-streams/state/db"

const userSchema = z.object({
  id: z.string(),
  name: z.string(),
  email: z.string(),
})

const schema = createStateSchema({
  users: {
    schema: userSchema,   // any StandardSchemaV1 validator (zod/valibot/arktype/manual)
    type: "user",         // the `type` discriminator carried in each stream event
    primaryKey: "id",     // property name used as the row key
  },
})
```

- `type` is the **event discriminator**: events whose `type` field === `"user"`
  route to the `users` collection. Duplicate `type` values across collections
  throw at schema-creation time.
- Reserved collection names (throw): `collections`, `preload`, `close`, `utils`,
  `actions`.
- `createStateSchema` also decorates each collection with **producer-side event
  helpers** (`.insert/.update/.delete/.upsert`) — see §5.

### 3.2 preload()

```ts
await db.preload()
```

`preload()` → `startConsumer()` (opens the stream, begins consuming batches) →
`await dispatcher.waitForUpToDate()`. It resolves once the stream signals
**up-to-date** (the snapshot/backlog has been fully materialized). After that the
consumer stays attached for live updates (when `live !== false`), continuously
applying new batches to the collections. `db.close()` aborts the connection and
rejects pending waiters.

`db.offset` (string getter) exposes the last consumed stream offset.

### 3.3 Event envelope → collection row mapping (THE CRUX)

A change event on the wire (the "ChangeEvent" of the State Protocol) has this
shape (verified in `src/types.ts`):

```ts
type ChangeEvent<T> = {
  type: string          // discriminator → selects collection (must match schema `type`)
  key: string           // the PRIMARY KEY value, as a string
  value?: T             // the row (required for insert/update/upsert; omitted/null for delete)
  old_value?: T         // optional previous row (audit/conflict; NOT used for materialization)
  headers: {
    operation: "insert" | "update" | "delete" | "upsert"  // REQUIRED
    txid?: string
    timestamp?: string
    from?: string
    offset?: string
  }
}
```

How an appended event becomes an insert/update/delete row in the materialized
TanStack collection (verified in `EventDispatcher.dispatchChange`):

1. **Discrimination**: `event.type` selects the handler/collection. Unknown
   types are silently ignored.
2. **Operation**: read from `event.headers.operation`. This is the `op` encoding
   — it lives in `headers.operation`, NOT a top-level field.
3. **Primary key**: the dispatcher takes `event.key` (a string) and **writes it
   onto the row** at the schema `primaryKey` field: `value[primaryKey] = event.key`.
   So the row's PK is authoritatively the event `key`. The collection `getKey`
   is `String(item[primaryKey])`.
4. **Value requirement**: for non-delete ops, `event.value` MUST be a non-null
   object, else it throws (`StreamDB collections require object values`).
5. **`upsert`** is resolved to insert-or-update based on whether the key already
   exists.
6. **Idempotency normalisation**: an `insert` whose key already exists (or whose
   row deep-equals the existing row) is downgraded to `update`, so live-stream
   reconnects/replays don't trip TanStack's duplicate-key path.
7. **Batching/commit**: writes are buffered per batch; on the up-to-date /
   batch boundary all handlers `commit()` and (first time) `markReady()`.
8. A `_seq` field is stamped on each row for cross-collection insertion ordering
   (internal; strip it if you compare rows — see Open questions).

Control events (`headers.control`): `reset` truncates all collections;
`snapshot-start` / `snapshot-end` are boundary hints.

> So: to make an append materialize correctly, each appended JSON item must be
> `{ type, key, value, headers: { operation } }`. The producer helpers in §5
> build exactly this shape and validate `value`.

---

## 4. Reading CURRENT materialized state in Node (NO React)

`db.collections[name]` is a plain **`@tanstack/db` `Collection`** — fully usable
headless. `useLiveQuery` is just a React wrapper over the same primitives. The
headless API (verified in `@tanstack/db@0.6.12` `collection/index.d.ts`):

**Synchronous reads (valid after `preload()` since the collection is ready):**

```ts
const c = db.collections.users
c.toArray            // getter → CurrentRows[]  (snapshot array of current rows)
c.state              // getter → Map<key, row>
c.size               // number of rows
c.get(key)           // row | undefined   (SYNC — not a Promise)
c.has(key)           // boolean
c.keys() / c.values() / c.entries()   // iterators
c.forEach((row, key, i) => ...)
```

> Important correction: the marketing/README snippets that show
> `await db.collections.users.get("1")` and `.query().toArray()` are **wrong /
> aspirational**. `get` is synchronous and there is no `.query()` on a
> collection. Use `toArray` / `state` / `get`, and `createLiveQueryCollection`
> or `queryOnce` for queries.

**Await-ready variants (use if you read before/around preload):**

```ts
await c.toArrayWhenReady()   // Promise<row[]>
await c.stateWhenReady()     // Promise<Map<key,row>>
```

**Subscribe to live changes (the headless equivalent of useLiveQuery):**

```ts
const sub = c.subscribeChanges(
  (changes) => {
    // changes: Array<ChangeMessage>
    // ChangeMessage = { key, value, previousValue?, type: "insert"|"update"|"delete", metadata? }
    for (const ch of changes) {
      console.log(ch.type, ch.key, ch.value)
    }
  },
  { includeInitialState: true } // emit current rows immediately on subscribe
)
// later:
sub.unsubscribe()
```

`subscribeChanges` options: `includeInitialState?: boolean`, and an optional
`where` filter built from the re-exported operators (`eq`, `and`, `gt`, …).
It returns a `CollectionSubscription` with `.unsubscribe()`.

`currentStateAsChanges({ where? })` returns the current state as an array of
change messages (one-shot snapshot, no subscription).

**For queries/joins/aggregates headless**, use the re-exported
`createLiveQueryCollection` (a derived live collection you can also `.toArray`)
or `queryOnce`. Both are re-exported from `@durable-streams/state/db`.

### 4.1 Simplest path for a test: bypass TanStack entirely

For the electric-ivm test (read current set, compare to oracle) you don't even
need the reactive layer. The db-free entry ships `MaterializedState`, a plain
in-memory reducer over `ChangeEvent`s (verified `src/materialized-state.ts`):

```ts
import { MaterializedState, isChangeEvent } from "@durable-streams/state"
import { DurableStream } from "@durable-streams/client"

const state = new MaterializedState()
// feed it events you read off the stream yourself:
state.applyBatch(events.filter(isChangeEvent))

state.get("user", "1")          // row | undefined
state.getType("user")           // Map<key, row>  → current set for that type
Array.from(state.getType("user").values())  // current rows array for set-equality
```

`MaterializedState.apply` honours `insert/update/upsert` (set key→value) and
`delete` (delete key), keyed by `event.type` then `event.key`. It does NOT
connect to a stream — you supply events (e.g. by consuming
`@durable-streams/client` `stream.stream({ json: true })` batches). Use this if
you want a dependency-light oracle/materializer without `@tanstack/db`.

For the production reactive path, prefer `createStreamDB` + `collection.toArray`.

---

## 5. Appends / optimistic mutations (lower priority)

Producer-side event construction (validates `value` against the schema, derives
`key` from `value[primaryKey]` if not given, sets `headers.operation`):

```ts
schema.users.insert({ value: { id: "1", name: "Alice", email: "a@x.com" } })
// → { type:"user", key:"1", value:{...}, headers:{ operation:"insert" } }
schema.users.update({ value: updated, oldValue: prev })
schema.users.delete({ key: "1" })          // or { oldValue }
schema.users.upsert({ value: row })
// extra headers (e.g. txid) merge in:
schema.users.insert({ value: row, headers: { txid: crypto.randomUUID() } })
```

Append raw (your own API path — what electric-ivm does):

```ts
await db.stream.append(JSON.stringify(schema.users.insert({ value: row })))
```

Optimistic actions (TanStack `createOptimisticAction` under the hood):

```ts
const db = createStreamDB({
  streamOptions: { url, contentType: "application/json" },
  state: schema,
  actions: ({ db, stream }) => ({
    addUser: {
      onMutate: (user) => db.collections.users.insert(user), // immediate local
      mutationFn: async (user) => {
        const txid = crypto.randomUUID()
        await stream.append(JSON.stringify(schema.users.insert({ value: user, headers: { txid } })))
        await db.utils.awaitTxId(txid) // resolves when the txid round-trips through the stream
      },
    },
  }),
})
await db.actions.addUser({ id: "1", name: "Alice", email: "a@x.com" })
```

`db.utils.awaitTxId(txid, timeoutMs = 5000)` resolves once an event bearing that
`headers.txid` has been consumed and committed (or rejects on timeout) — useful
to make an append-then-read deterministic in a test.

---

## 6. Minimal Node snippet: connect, preload, read, subscribe

```ts
import { createStreamDB, createStateSchema } from "@durable-streams/state/db"
import { z } from "zod"

const rowSchema = z.object({ id: z.string(), name: z.string() })

const schema = createStateSchema({
  items: { schema: rowSchema, type: "item", primaryKey: "id" },
})

const db = createStreamDB({
  streamOptions: {
    url: "https://my-durable-streams-server.example/streams/shape-42",
    contentType: "application/json",
  },
  state: schema,
  live: true,
})

// 1. connect + materialize backlog until up-to-date
await db.preload()

// 2. read CURRENT materialized set (synchronous, no React)
const items = db.collections.items
console.log("current rows:", items.toArray)          // Row[]
console.log("as map:", items.state)                  // Map<id, Row>
console.log("one:", items.get("1"))                  // Row | undefined

// helper for set-equality vs an oracle:
const currentSet = new Map(items.state)              // copy of id -> row

// 3. subscribe to live updates (headless equivalent of useLiveQuery)
const sub = items.subscribeChanges(
  (changes) => {
    for (const ch of changes) {
      // ch.type: "insert" | "update" | "delete"; ch.key; ch.value
      if (ch.type === "delete") currentSet.delete(ch.key)
      else currentSet.set(ch.key, ch.value)
    }
  },
  { includeInitialState: true },
)

// ... run assertions ...

// 4. teardown
sub.unsubscribe()
db.close()
```

For a dependency-light test materializer, see §4.1 (`MaterializedState`).

---

## Open questions / unverified

- **`_seq` pollution of rows.** The StreamDB dispatcher stamps each materialized
  row with an internal `_seq: number` for ordering. `collection.toArray` rows
  will therefore likely carry an extra `_seq` field not present in your oracle.
  Confirm whether `_seq` is stripped on read (it is only stripped internally for
  the deep-equals dedupe via `comparableRow`). For set-equality, compare on your
  schema fields / strip `_seq`. UNVERIFIED whether it surfaces in `toArray`.
- **Does `delete` need `value`?** Protocol says delete `value` is omitted/null
  and the dispatcher skips the object-type check for deletes — but the producer
  `delete()` helper emits `old_value` only (no `value`). Confirm your server
  emits deletes as `{ type, key, headers:{operation:"delete"} }` (no `value`).
- **`toArray` after `preload` truly ready?** `preload()` awaits up-to-date which
  triggers `markReady()`, so `toArray` should be populated synchronously. If you
  ever read before preload resolves, use `toArrayWhenReady()`. Low risk.
- **Live reconnect replays.** On SSE/long-poll reconnect the server may replay
  already-seen inserts; StreamDB normalises insert→update for known keys. If your
  oracle counts ops (not final set) this matters; for set-equality it's fine.
- **`@durable-streams/client` low-level read API** (`stream.stream({ live, json })`,
  `.subscribeJson(batch => ...)`, `batch.upToDate`, `batch.offset`, `batch.items`)
  is used internally and is available if you want to drive `MaterializedState`
  yourself, but its exact public surface (offset resumption, `from` param) was
  not exhaustively documented here — read `C/package/dist/index.d.ts` if needed.
- **Exact `@tanstack/db` version float.** state peer is `>=0.6.0 <1.0.0`; 0.6.12
  is current. The TanStack DB collection API (`toArray`, `subscribeChanges`,
  `state`) is stable across 0.6.x but could shift before 1.0.
- **Server availability.** This brief assumes a running durable-streams server
  speaking the base protocol + State Protocol (JSON content-type, batch reads,
  up-to-date signal, control messages). electric-ivm must provide/point at one.
