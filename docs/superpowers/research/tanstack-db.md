# @tanstack/db — Headless (non-React) usage in Node

Research date: 2026-06-27. All API facts below were verified against the
published package `@tanstack/db@0.6.12` (type defs in `dist/esm`) and by running
a real Node ESM script (no React, no DOM). Items that were executed are marked
**[verified]**; items read only from `.d.ts`/docs are marked **[from types]**.

## 1. Version + install

- Latest stable: **`@tanstack/db@0.6.12`** (`dist-tags.latest = 0.6.12`). **[verified]**
- Runtime deps: `@standard-schema/spec ^1.1.0`, `@tanstack/pacer-lite ^0.2.1`,
  `@tanstack/db-ivm 0.1.18`. No React dependency. **[verified]**
- Install (core only, headless):

  ```bash
  npm i @tanstack/db
  # or: pnpm add @tanstack/db
  ```

- The package is ESM-first and ships `dist/esm` + `dist/cjs`. `createCollection`
  and friends import cleanly under Node `"type": "module"`. **[verified]**
- Do NOT need `@tanstack/react-db` for headless use — that is only the React
  adapter (hooks like `useLiveQuery`). The core `@tanstack/db` package is
  framework-agnostic. **[verified]**

## 2. Reading current state synchronously (no hooks)

After the collection is ready (see "readiness" below), all of these are
synchronous getters/methods on the `Collection` instance:

| API | Returns | Notes |
| --- | --- | --- |
| `collection.toArray` | `T[]` | **Getter, not a function** — use `collection.toArray`, not `collection.toArray()`. **[verified]** |
| `collection.state` | `Map<TKey, T>` | Getter. `instanceof Map === true`. **[verified]** |
| `collection.entries()` | `IterableIterator<[TKey, T]>` | **[verified]** |
| `collection.values()` | `IterableIterator<T>` | **[from types]** |
| `collection.keys()` | `IterableIterator<TKey>` | **[from types]** |
| `collection.get(key)` | `T \| undefined` | **[from types]** |
| `collection.has(key)` | `boolean` | **[from types]** |
| `collection.size` | `number` | Getter (cached). **[verified]** |
| `collection.forEach(fn)` / `collection.map(fn)` | — / `U[]` | Array-like helpers. **[from types]** |
| `collection.currentStateAsChanges(opts?)` | `ChangeMessage<T>[]` | Snapshot expressed as `insert` change messages; supports `where`/`orderBy`/`limit`. **[from types]** |

Async (wait-for-first-commit) variants, useful in tests to avoid racing sync:

- `await collection.toArrayWhenReady()` → `Promise<T[]>`
- `await collection.stateWhenReady()` → `Promise<Map<TKey, T>>`
- `await collection.preload()` → `Promise<void>` (starts sync if lazy, resolves when ready)

### IMPORTANT caveat: virtual props on every row

Rows read back from the collection are **not byte-identical to what you wrote**.
The collection augments each row with virtual props:

```
{ id: '1', text: 'a', $synced: true, $origin: 'remote', $key: '1', $collectionId: 'todos' }
```

**[verified]** Your own fields are preserved; `$synced`, `$origin`, `$key`,
`$collectionId` are added. In tests, compare on your own fields (or strip `$`
keys) rather than deep-equaling the whole object. The type is
`WithVirtualProps<T, TKey>`; `hasVirtualProps()` / `WithoutVirtualProps` helpers
are exported if you need to strip them.

## 3. Subscribing to changes programmatically

```ts
const subscription = collection.subscribeChanges(
  (changes /* Array<ChangeMessage<T>> */) => {
    for (const c of changes) {
      // c.type: 'insert' | 'update' | 'delete'
      // c.key, c.value, c.previousValue
    }
  },
  { includeInitialState: true } // optional, see semantics below
)
// later:
subscription.unsubscribe()
```

- `subscribeChanges(callback, options?)` returns a **`CollectionSubscription`**
  object with an `.unsubscribe()` method (it is NOT a bare unsubscribe
  function). **[verified]**
- The callback receives a **batch** (`Array<ChangeMessage>`) per committed sync
  transaction, not one event per row. **[verified]**
- `options.includeInitialState?: boolean` — when true, the current contents are
  delivered immediately (synchronously, within the `subscribeChanges` call) as
  `insert` change messages, before any future changes. **[verified]**
- `options.where` / `options.whereExpression` — filter the stream to matching
  rows using query-builder expressions (`eq`, `gt`, `and`, …). **[from types]**
- There is also a lower-level event API: `collection.on(event, cb)` /
  `collection.once(...)` for lifecycle/status events, and `collection.status`
  (`CollectionStatus`). For data changes, prefer `subscribeChanges`. **[from types]**

### ChangeMessage shape **[verified]**

```ts
interface ChangeMessage<T, TKey> {
  key: TKey
  value: T                 // current value (for delete: the last value)
  previousValue?: T        // present on 'update'
  type: 'insert' | 'update' | 'delete'
  metadata?: Record<string, unknown>
}
```

### Subscriber-baseline semantics — IMPORTANT **[verified]**

The change stream is made *consistent for each subscriber's own view*, which
affects the `type` you see:

- **With `includeInitialState: true`** the subscriber's baseline = current
  contents, so subsequent ops report their true type. Example run, after an
  initial `{1:'a', 2:'b'}` then a transaction `update 1→'a2'; insert 3; delete 2`:

  ```
  initial delivery: [insert 1 'a', insert 2 'b']
  live changes:     [update 1 'a2' (prev 'a'), insert 3 'c', delete 2 'b']
  ```

- **Without `includeInitialState`** the subscriber starts from an *empty* view.
  Updates to rows it has never seen are coerced to `insert`, and deletes of
  unseen rows are **dropped entirely**. Same scenario produced:

  ```
  live changes: [insert 1 'a2', insert 3 'c']   // no delete, update became insert
  ```

  This is by design (keeps each subscriber's reconstructed state valid) but is a
  trap. **For tests that assert on operation type, pass
  `includeInitialState: true`.** If you only need to detect "an update was
  applied" (any event fired), either form works — you will get at least one
  callback per committed transaction touching visible rows.

## 4. Keying rows & how mutations are represented

- Every collection requires `getKey: (item) => TKey` in its config; `TKey` is
  `string | number`. `collection.getKeyFromItem(item)` exposes it. **[from types/verified]**
- In the change stream and in `state`/`entries`, the key is this derived key.
- Operation types in the stream are exactly `'insert' | 'update' | 'delete'`
  (`OperationType`). `update` carries `previousValue`; `delete` carries the last
  `value` and key. **[verified]**

## 5. Standalone in Node — viability & caveats

**Yes, fully usable headless in Node with no framework.** **[verified]** A
collection is created with `createCollection({...})` and is driven by a **sync
config** — this is the integration point a durable-stream adapter (stream-db)
plugs into:

```ts
interface SyncConfig<T, TKey> {
  sync: (params: {
    collection
    begin: (opts?: { immediate?: boolean }) => void
    write: (msg: ChangeMessageOrDeleteKeyMessage<T, TKey>) => void  // {type, value} / {type:'delete', key}
    commit: () => void
    markReady: () => void
    truncate: () => void
    metadata?: SyncMetadataApi
  }) => void | CleanupFn | SyncConfigRes
  rowUpdateMode?: 'partial' | 'full'   // default 'partial'
}
```

The `sync` function is invoked lazily on first access / `preload()`. You call
`begin()` → one or more `write({type, value})` → `commit()`, and `markReady()`
once initial load is done. `subscribeChanges` fires once per `commit()`.
**[verified]**

Caveats / gotchas for tests:

1. **`toArray` and `state` are getters** — no parens. Easy to misuse.
2. **Virtual props** are added to every row (see §2). Don't deep-equal raw rows.
3. **Readiness/lazy sync:** sync does not start until the collection is accessed
   or `preload()`/`startSyncImmediate()` is called. In tests, `await
   collection.preload()` (or `toArrayWhenReady()`) before asserting initial
   state, otherwise `state` may be empty. `collection.isReady()` /
   `onFirstReady(cb)` are available. **[verified]**
4. **The Node process does not exit on its own.** After using a collection the
   event loop stays alive (internal scheduler/timers keep handles open); a plain
   script hangs at the end. In one-shot scripts call `process.exit(0)`. Under a
   test runner (vitest/jest) this is normally fine, but ensure you
   `subscription.unsubscribe()` and consider an explicit teardown. **[verified —
   script timed out at 2 min until `process.exit(0)` was added]**
5. **`rowUpdateMode`** defaults to `'partial'` (writes may contain only changed
   fields). If your stream always emits full rows, set `rowUpdateMode: 'full'`
   to avoid partial-merge surprises. **[from types]**

### Alternative: live query collections (headless)

`createLiveQueryCollection(...)` / `liveQueryCollectionOptions(...)` (exported
from `@tanstack/db`) build a derived collection from a query over other
collections and are usable headlessly — the result is itself a `Collection`, so
you read it with the same `toArray`/`subscribeChanges` APIs. For simply
observing one source collection, plain `subscribeChanges` is sufficient and
simpler. **[from types]**

## Minimal Node snippet (current rows + subscribe) — [verified to run]

```js
// package.json: { "type": "module" }
import { createCollection } from '@tanstack/db'

let syncApi
const collection = createCollection({
  id: 'todos',
  getKey: (item) => item.id,
  sync: {
    sync: ({ begin, write, commit, markReady }) => {
      syncApi = { begin, write, commit }     // keep handle so the stream can push later
      begin()
      write({ type: 'insert', value: { id: '1', text: 'a' } })
      write({ type: 'insert', value: { id: '2', text: 'b' } })
      commit()
      markReady()
    },
  },
})

// 1. Read current rows synchronously (after ready)
await collection.preload()
const rows = collection.toArray            // getter, no parens
console.log(rows.map(r => r.id))           // ['1','2']  (+ $synced/$origin/... virtual props)

// 2. Subscribe to changes; includeInitialState:true => correct op types + baseline
const applied = []
const sub = collection.subscribeChanges(
  (changes) => {
    for (const c of changes) applied.push(`${c.type}:${c.key}`)
  },
  { includeInitialState: true },
)

// 3. A live update arriving from the durable stream:
syncApi.begin()
syncApi.write({ type: 'update', value: { id: '1', text: 'a2' } })
syncApi.write({ type: 'insert', value: { id: '3', text: 'c' } })
syncApi.write({ type: 'delete', value: { id: '2', text: 'b' } })
syncApi.commit()

// subscribeChanges fires synchronously on commit in this path; a microtask
// await is a safe guard in tests:
await Promise.resolve()
console.log(applied)
// e.g. ['insert:1','insert:2', 'update:1','insert:3','delete:2']

sub.unsubscribe()
process.exit(0)   // needed: collection keeps the event loop alive otherwise
```

To detect "a live update has been applied" in a test, the robust pattern is a
promise that resolves on the next relevant change:

```js
function waitForChange(collection, predicate = () => true) {
  return new Promise((resolve) => {
    const sub = collection.subscribeChanges((changes) => {
      const hit = changes.find(predicate)
      if (hit) { sub.unsubscribe(); resolve(hit) }
    })
  })
}
// const change = await waitForChange(collection, c => c.key === '1' && c.type === 'update')
```

## Open questions

- **Callback timing:** in the verified run, `subscribeChanges` fired
  synchronously during `syncApi.commit()` (a bare `await Promise.resolve()` was
  enough). Whether delivery is *always* synchronous on commit, or can be
  deferred (e.g. via the internal `@tanstack/pacer-lite` scheduler or under
  optimistic mutations), is unconfirmed. Tests should await a change promise
  rather than assume synchronous delivery.
- **Event-loop hang:** confirmed the process needs `process.exit(0)` in a plain
  script. The exact handle keeping it alive (scheduler timer?) and whether a
  full `cleanup`/teardown API stops it cleanly was not traced. For long-lived
  test suites, verify the sync `cleanup` function and `subscription.unsubscribe`
  fully release resources.
- **`rowUpdateMode: 'partial'` merge semantics:** how a partial `update` write
  merges into the stored row (shallow vs deep, handling of removed keys) was not
  exercised. If stream-db emits partial diffs, validate this explicitly.
- **`metadata` on ChangeMessage / SyncMetadataApi:** present in types but not
  exercised; relevant if stream-db wants to carry stream offsets/LSNs per row.
- **`truncate()`** (clear-all in a sync transaction) exists in the sync API but
  was not tested; relevant for stream resets/snapshots.
- **Backpressure / large streams:** no investigation into throughput or memory
  behavior for high-volume durable streams.
