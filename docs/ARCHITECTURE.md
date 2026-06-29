# electric-lite — architecture

A reactive read-path: clients declare **shapes** (filtered views of a table); the system keeps each
shape's result set live as the base table changes, delivering incremental updates. Built on
incremental dataflow (dbsp), durable streams for transport/persistence, and a stream-db + TanStack DB
client for materialization.

This document describes the system as built. For the narrower design records see
`docs/superpowers/specs/` (electric-lite-decisions, shape-pipeline-sharing-design, conformance-expansion,
benchmark-findings, postgres-logical-replication).

> **Postgres mode (current default for deployment).** electric-lite now runs with **Postgres as the
> system of record**: applications write to Postgres, the engine ingests changes via **logical
> replication** (built-in `test_decoding`), and shape **backfill reads rows back from Postgres** with a
> snapshot `SELECT`. The in-memory `table_state` described in §4.1–4.2 has been **removed entirely** (in
> *all* modes): delta computation now uses the replicated/envelope **old + new** tuples
> (`REPLICA IDENTITY FULL` in Postgres mode) instead of a local table copy, so §4.1–4.2 (and the
> `table_state` row of the §8 trade-off table) describe the original design and no longer reflect the
> code. The API write path (§3) now applies only to the legacy/library mode (no `ELECTRIC_LITE_PG_URL`).
>
> The ingestor (`replication.rs`) reads the slot non-consuming (`peek`), buffers each transaction, and
> stamps every change with its transaction's **COMMIT LSN** (taken from the `COMMIT` record), then
> appends to durable-streams and only **then** advances the slot (so a failed append re-reads rather
> than loses data). Backfill (`pg.rs`) records the snapshot's `pg_current_wal_lsn()` as `seed_lsn`, and
> the engine skips replicated changes whose **commit LSN is strictly `< seed_lsn`** — transactions that
> committed before the snapshot are already in the backfill; those committing at/after it
> (`>= seed_lsn`) are taken from the live stream. Comparing the *commit* LSN (not the per-change record
> LSN) is what keeps rows of transactions in flight during the snapshot from being dropped, so each row
> counts exactly once (guarded by `conformance-concurrency.test.ts`). The output translation (§4.4) and
> the durable-streams transport are unchanged. See `docs/deployment-postgres.md` to run it and
> `docs/superpowers/specs/2026-06-29-postgres-logical-replication.md` for the design.

> **Routing model (supersedes the dbsp family circuit, §4.3a / §5 / §8 / §10).** The engine no longer
> keeps any table data in memory. Equality-template shapes are no longer a shared dbsp **join** whose
> data trace is a full copy of the table; they are a **key routing index** (`key_tuple -> {shapes}`,
> one per template) holding only per-shape metadata. A new equality shape **backfills directly from
> Postgres** (`SELECT … WHERE key = const`, the Phase-1 predicate pushdown) and registers in the index;
> a live change is routed by its old/new key to exactly the shapes on that key (membership = key match)
> and emits delete(old)/upsert(new), each shape applying its own `seed_lsn`. Standalone shapes are
> unchanged (stateless filters; their backfill now also pushes the predicate into the `SELECT`). So the
> engine holds **O(#shapes)** routing metadata and **zero table copies** (was `O(#templates × table)`);
> `FamilyActor` and the per-template OS thread are gone (dbsp remains only for the `Row`/`Tup2`/`ZWeight`
> Z-set value types). §4.3(a), the family rows of §5 and §8, and §10 (dbsp trace storage) describe the
> superseded design. Backfill never reads the whole table any more. See
> `docs/superpowers/specs/2026-06-29-reduce-engine-memory-design.md`.

```
  Postgres mode:
   app ──writes──▶  Postgres  ──logical replication──▶  engine  ──append──▶  durable-streams
                      ▲                                   │                    (shape/<id>)
                      └──────────── backfill SELECT ──────┘                         │
                                                                                    ▼
                                                                           client (live rows)
```

---

## 1. Components

```
   ┌─────────┐   write (tRPC)   ┌─────────┐   append    ┌──────────────────┐
   │ client  │ ───────────────▶ │  API    │ ──────────▶ │  durable-streams  │
   │ (TS)    │                  │ (core)  │  table/<t>  │  (table + shape   │
   │         │ ◀─ subscribe ─── │         │             │   streams)        │
   └─────────┘   shape/<id>     └─────────┘             └──────────────────┘
        ▲                                                   │        ▲
        │ live rows (TanStack DB)                      tail │        │ append shape/<id>
        │                                                   ▼        │
        │                                            ┌──────────────────┐
        └──────────────────────────────────────────  │   engine (Rust)  │
                     (reads shape streams)            │  dbsp + tailer   │
                                                      └──────────────────┘
```

- **durable-streams** — append-only, offset-addressed JSON streams with long-poll/SSE tailing. One
  `table/<name>` stream per table (the write log) and one `shape/<id>` stream per shape (the result
  feed). Source of truth and the decoupling boundary between write and read paths.
- **API / core** (`apps/api`) — thin tRPC surface. Writes are translated to State-Protocol envelopes
  and appended **directly to the table stream** (it does *not* go through the engine). Schema and shape
  lifecycle are forwarded to the engine over HTTP.
- **engine** (`apps/engine`, Rust) — tails each table stream, maintains authoritative table state,
  computes per-change deltas, fans them out to all shapes via two execution strategies, and appends the
  resulting upsert/delete envelopes to shape streams. Holds all dataflow state.
- **client** (`packages/client`) — `createClient` exposes typed writes (tRPC) and `shape()` which
  creates a shape and returns a **TanStack DB collection** kept live by a stream-db reader on the shape
  stream. `awaitTxId` resolves when a given write's txid is observed in the shape stream. The collection
  is consumed reactively via `@tanstack/react-db`'s `useLiveQuery` (see §12, the client query layer).

The write path and read path are **decoupled through durable-streams**: the API never blocks on the
engine, and the engine is a stateless-restartable consumer of the durable log.

---

## 2. Data model

- **`Value`** (`value.rs`) — dynamically-typed scalar: `Int | Float | Text | Bool | Null`. NULL is
  first-class for three-valued predicate logic.
- **`Row`** = `Vec<Value>` — positional. The schema names the positions; the engine works positionally.
- **Z-set delta** — `Vec<Tup2<Row, ZWeight>>` where `ZWeight` is a signed `i64` multiplicity. A change
  is expressed as weights: insert = `(row, +1)`; delete = `(row, −1)`; update = `(old, −1), (new, +1)`.
  This is the dbsp incremental-computation currency.
- **State-Protocol envelope** (`ds.rs::Envelope`) — what lives on streams: `{ type, key, value,
  headers{ operation, txid, offset } }`. Table-stream ops are `insert/update/upsert/delete`; shape-stream
  ops are `upsert` (row enters/updates the result) or `delete` (row leaves).

---

## 3. Write path

1. `client.tables.<t>.update(row)` → tRPC `write`.
2. `core.write` builds a table envelope (`toTableEnvelope`) with a generated `txid` and **POSTs it to
   `durable-streams` `table/<t>`**. Returns `{ txid }` immediately.
3. The engine's tailer observes the appended envelope on its next read.

The txid threads through: the same txid is copied onto every shape envelope produced from this change,
so the client's `awaitTxId(txid)` can detect when the write has been fully materialized into a shape.

---

## 4. Engine internals

### 4.1 One tailer task per table

`spawn_tailer` runs `tailer_loop` (a tokio task) per table. It owns, single-threaded:

- `table_state: HashMap<Value, Row>` — authoritative current rows keyed by primary key. The source for
  delta computation (old row on update/delete) and for backfilling newly-registered shapes.
- `shapes: HashMap<String, StandaloneShape>` — non-shareable shapes (direct eval).
- `families: HashMap<Vec<usize>, Family>` — shared circuits, keyed by the template's sorted key columns.
- `family_of` — reverse index for shape removal.

The loop `select!`s (biased) between control commands (`AddShape`/`RemoveShape`) and a long-poll
`ds.read` on the table stream. Processing a change is **serial within a table** — ordering and state
mutation are trivially correct; the only intra-batch parallelism is the append flush (§4.5).

### 4.2 Delta computation — `apply_envelope`

Converts a table envelope into a Z-set delta against `table_state`:

- insert/upsert of a brand-new pk → `[(row, +1)]`
- update of an existing pk → `[(old, −1), (row, +1)]` (or empty if the row is byte-identical — a no-op,
  which is why the firehose's same-value writes don't count as envelopes)
- delete → `[(old, −1)]`

`table_state` is updated in place; the delta + txid are returned.

### 4.3 Two execution strategies

A shape's predicate decides how it runs. `predicate.equality_template()` returns `Some(key_cols)` iff
the predicate is a **conjunction of non-null equality leaves on distinct columns** (e.g.
`tenant = 7 AND region = 'eu'`); otherwise `None`.

**(a) Shared family circuits — equality templates** (`family.rs`)

All shapes with the *same set of equality columns* (regardless of the constants) share **one** dbsp
circuit:

```
data_s   ── map_index(row -> (key_of(row, key_cols), row)) ──┐
                                                             ├─ join ─▶ (shape_id, row) ─▶ output
params_s ── map_index((key,shape) -> (key, shape)) ──────────┘
```

The table is indexed once by the template's key columns; a `Params{(key_tuple, shape_id)}` collection
holds one entry per member shape. An **equi-join** emits `(shape_id, row)` for every row whose key
matches a shape's constants. So a write touching key `k` is routed only to the shapes registered on `k`
— in O(log N) over the params arrangement, *independent of the number of shapes*.

- **Adding a shape** = insert `(key_tuple, shape_id)` into Params. The incremental join emits the
  shape's backfill automatically (joining the new param against the accumulated data trace). A brand-new
  family is primed with the current `table_state` in the same step.
- **Dropping a shape** = delete the param; the join emits negative weights (its rows leave). When a
  family's last shape leaves, the family (and its trace) is discarded.

`collect_family_output` demultiplexes the `(shape_id, row, weight)` output back to per-shape streams.

**(b) Standalone direct evaluation — everything else** (ranges, OR, NOT, inequality, match-all)

A `WHERE` filter is **stateless**, so it needs no circuit and no thread:

```rust
fn eval_standalone(pred, delta) -> Vec<(Row, ZWeight)> {
    delta.iter().filter(|t| pred.matches(&t.0)).map(|t| (t.0.clone(), t.1)).collect()
}
```

Backfill on registration evaluates the predicate over `table_state`. Cost is O(K) predicate evals per
write for K standalone shapes — CPU-bound, but with **no thread, no circuit, no per-shape delta clone**.

### 4.4 Output translation — `translate_output`

A shape's output delta is grouped by pk: any positive-weight row → `upsert` envelope (the row entered or
updated the shape); a purely negative pk → `delete` (it left). The originating txid is stamped on each.

### 4.5 Batched, concurrent append flush

Per read batch, all shape envelopes are staged in `pending: HashMap<stream_path, Vec<Envelope>>`, then
`flush_pending` appends them, **bounded-concurrently (CAP=32)** via a `JoinSet`. Appends (HTTP
round-trips to storage) dominate engine time, so coalescing per stream and parallelizing across streams
is the primary throughput/latency lever. Envelopes are never merged, so each keeps its own txid and
`awaitTxId` still works.

### 4.6 Convergence barrier

After a batch is fully fanned out *and* flushed, the tailer publishes the processed offset
(`GET /tables/<t>/offset`). A harness can poll this against the stream tail to know the engine has
caught up — a sound (no false-green) convergence check used by the conformance suite.

---

## 5. Threading model

| unit | threads | notes |
|------|---------|-------|
| API / core | tokio (Node) | stateless |
| engine main | tokio multi-thread | tailer tasks + append flush run here |
| tailer (per table) | 1 task | serial change processing |
| standalone shapes | **0** | direct eval on the tailer task |
| family circuit (per template) | **1 OS thread** | dbsp join needs a runtime; `FamilyActor` owns a `std::thread` with `blocking_recv` |

So engine threads ≈ `1 + #templates + tokio workers` — flat in the number of *shapes* (benchmarks show
~16–18 threads at 100k shapes). It grows with the number of distinct equality **templates**, not shapes.

---

## 6. Predicate compilation & NULL

`predicate.rs` compiles the JSON predicate to a `CompiledPredicate` tree (column indices resolved
against the schema). `matches(row)` evaluates with SQL **three-valued logic** (`Tri{True,False,Unknown}`):
a NULL operand yields UNKNOWN, AND/OR follow SQL truth tables, `NOT UNKNOWN = UNKNOWN`, and a row is
included only when the predicate is TRUE. The same evaluator backs both strategies (standalone eval and,
implicitly, equality routing), so they agree by construction.

---

## 7. Telemetry

`GET /metrics` (+ `POST /metrics/reset`): lock-free atomic counters (`envelopes_processed`,
`shape_appends`, `family_steps`) and log-bucket latency histograms (`process_envelope`, `family_step`,
`append`) with p50/p99/p999/max. `GET /tables/<t>/families` exposes the live topology (families + their
member counts, standalone count) for tests and capacity analysis.

---

## 8. Trade-offs

| decision | benefit | cost / risk |
|----------|---------|-------------|
| **Shared family per equality template** | N same-template shapes → 1 circuit; per-write routing O(log N), independent of shape count | each family holds a **full copy of the table** in its dbsp data trace → memory `O(#templates × table)`. Bounded by template count (small), not shapes. |
| **Standalone direct eval (no dbsp) for non-equality** | no thread, no circuit, no per-shape delta clone; scales to many shapes | **O(K) predicate evals per write** (K standalone shapes) — CPU-bound; no pruning, every shape is tested on every change. |
| **Per-envelope processing** | each shape envelope carries the exact originating txid (clean `awaitTxId`) | more channel round-trips / family steps than batching the whole read-batch into one step. |
| **Per-family delta clone** (`step(delta.clone())`) | dbsp input consumes its Vec; clone gives each family its own | one delta clone per family per write. Cheap (few families) but real. |
| **`table_state` separate from family traces** | one authoritative state for delta computation + standalone backfill; works even with no families | the table is materialized `1 + #families` times. |
| **Single tailer per table** | trivial ordering & state correctness | change processing (delta + family steps + standalone evals) is **serial** on one task; only the append flush parallelizes. |
| **Decoupled write path (API → table stream, engine tails)** | durable, replayable, API never blocks on engine; engine restart-safe | adds a storage round-trip of latency between write and materialization. |
| **One durable stream per shape** | clean per-shape subscription, independent offsets | creating/touching N streams = N storage round-trips; under a non-keep-alive local server this is bounded by ephemeral ports (see benchmark-findings). |
| **Family circuit = 1 OS thread** | dbsp join gets its required runtime; deterministic, isolated | threads grow with template count; a workload with thousands of *distinct* templates would need a worker pool. |

---

## 9. Potential speedups

Ordered roughly by expected impact. Telemetry (§7) shows the engine's internal per-change cost is
already <1ms p99 at 100k shapes; the current end-to-end ceiling under max load is **storage throughput**,
not the engine. So the highest-leverage items target the append/storage path and the standalone O(K).

**Storage / append path (current throughput ceiling)**
1. **Multi-stream append** — one request appending to many shape streams at once (if storage supports
   it) collapses the per-stream round-trip; today fan-out to M streams = M requests even when batched
   concurrently.
2. **HTTP/2 multiplexing or persistent pipelined connections** to storage — removes per-append
   connection setup and the socket churn that bounds local scale.
3. **Sharded / parallel tailers** — partition a table's shapes (or key space) across multiple tailer
   tasks so fan-out and flush use multiple cores; today one tailer per table is the serial point.
4. A production durable-streams backend (vs. the single-process test server) — the benchmarks are
   storage-bound, not engine-bound.

**Standalone evaluation (O(K) per write)**
5. **Predicate indexing** — instead of testing all K standalone predicates per change, index them by
   `(column, op)`: an interval/segment tree for range predicates on a column finds matching shapes in
   `O(log K + matches)`; equality-ish leaves can route like families. Turns O(K) into output-sensitive.
6. **Promote more predicates into shared circuits** — e.g. single-column range *templates* sharing one
   indexed circuit, the same way equality templates share a join. Widens the "shared" class beyond pure
   equality.

**Engine compute**
7. **Batch the whole read-batch into one family step** — accumulate all envelopes' deltas and step each
   family once per batch instead of per envelope; cuts channel round-trips and `transaction()` calls.
   Trade-off: per-row txid attribution is lost in the combined output, so either carry a txid *set* per
   shape envelope or keep per-envelope stepping for subscribed/probe streams only.
8. **Drop the per-family driver thread** — run the circuit via `spawn_blocking` or inline so a family is
   0 dedicated OS threads, removing the thread-per-template growth.
9. **Avoid the per-family delta clone** — share the delta as `Arc<[Tup2]>` and clone only at the dbsp
   input boundary; or feed families from a single shared indexed arrangement (also removes the
   table-copy-per-family amplification — the biggest remaining memory item).
10. **Subsume `table_state` into a shared arrangement** — keep one indexed copy of the table that both
    delta computation and family joins read, instead of `table_state` + one trace per family.

**Representation**
11. **Intern / `Arc<str>` stream paths and txids** to cut String clones in the hot `pending` map and
    `translate_output`.
12. **Columnar / packed `Value`** (smaller enum, string interning) to shrink `Row` and speed predicate
    evaluation and joins.

---

## 10. Running dbsp from disk (memory beyond RAM)

dbsp 0.299 has a full storage subsystem: on-disk columnar batch files (`trace/ord/file/`) paged
through a cache, with a per-batch spill threshold and memory-pressure-driven spilling. It is wired
into `FamilyActor` (`family.rs::circuit_config`) and **disabled by default** (in-memory). When enabled,
a family's join data trace + Params arrangement spill to disk instead of living wholly in RAM — this is
the lever for the table-copy-per-family amplification (§8). It does **not** touch `table_state` (our own
`HashMap`), which stays in RAM until separately moved to an embedded KV.

Enabled via env (per-family subdirs are created under the base dir, scoped by pid):

| env var | effect |
|---------|--------|
| `ELECTRIC_LITE_STORAGE_DIR` | base dir; **presence enables storage**. Unset = in-memory. |
| `ELECTRIC_LITE_STORAGE_CACHE` | `page` (default, OS page cache) or `feldera` (dbsp's internal LRU) |
| `ELECTRIC_LITE_STORAGE_CACHE_MIB` | LRU budget in MiB (FelderaCache only) |
| `ELECTRIC_LITE_STORAGE_MIN_BYTES` | per-batch spill threshold (dbsp default 10 MiB; `0` spills everything) |
| `ELECTRIC_LITE_MAX_RSS_MB` | process memory target driving pressure-based spilling |

**Verified:** conformance stays 84-green with storage forced fully on-disk (`MIN_BYTES=0`); `.feldera`
batch files + dirlocks are written. Output is identical — storage is transparent.

**A/B at 5,000 shapes, moderate load (measured):**

| config | e2e p99 | RSS peak | notes |
|--------|---------|----------|-------|
| in-memory (default) | 8.2ms | 42MB | baseline |
| on-disk, PageCache, `MIN_BYTES=0` | 7.4ms | **556MB** | pathological — forcing every tiny batch to disk makes mmap'd pages + write buffers count toward RSS |
| on-disk, FelderaCache 64 MiB, default threshold | 7.5ms | **34MB** | nothing spills (working set < 10 MiB); behaves like in-memory |

**Large-scale test (`packages/bench/src/memtest.ts`, `pnpm --filter @electric-lite/bench memtest`).**
8 distinct equality templates (8 families, so the table is held 8× in trace memory — the §8
amplification) + a growing table of ~130k wide rows (512B payload). Measured engine RSS peak:

| config | RSS peak | spilled to disk |
|--------|----------|-----------------|
| in-memory (default) | 401MB | — |
| on-disk, FelderaCache 128 MiB, `MIN_BYTES=1MiB` | **1538MB** | 41MB |
| on-disk, PageCache, `MAX_RSS_MB=300`, `MIN_BYTES=0` | 469MB | **2MB** |

**Conclusion (honest negative result):** in dbsp 0.299, enabling storage on our **ephemeral,
hand-built** circuit does **not** offload the join's steady-state data trace to disk for this workload.
`MIN_BYTES=0` wrote only ~2MB while the ~570MB of 8× trace data stayed resident; `FelderaCache` made
RSS *worse* (cache + write buffers + un-spilled batches, and it is documented as under-development).
Effective spill appears tied to dbsp's persistent-id / checkpoint machinery, which our circuit does not
use (`Mode::Ephemeral`; `Mode::Persistent` requires compiler-assigned operator ids we don't have). So
the wiring is correct and transparent (conformance green), but it is **not** a usable memory-bounding
lever as-is.

**Caveats / next steps:**
- Don't ship `MIN_BYTES=0` or `FelderaCache` — both inflate RSS here.
- Getting real spill likely needs deeper dbsp work (persistent mode + checkpoint cadence), validation on
  Linux (where the storage path is primarily developed and file-I/O RSS accounting differs), and/or a
  newer dbsp release. Treat the env flags as experimental until that's done.
- The higher-confidence path to "run from disk" for *this* engine is the part we fully control:
  back `table_state` (and optionally the shape index) with an embedded KV (redb/lmdb/sled), independent
  of dbsp's trace storage.
- Spilled family subdirs are not yet garbage-collected on family drop (families are long-lived; minor).

## 11. File map

| path | role |
|------|------|
| `apps/engine/src/engine.rs` | tailer, delta computation, key routing + standalone fan-out, flush, backfill↔replication LSN reconciliation |
| `apps/engine/src/replication.rs` | Postgres logical-replication ingestor: peek slot, buffer per-txn, stamp commit LSN, append, advance |
| `apps/engine/src/pg.rs` | Postgres connect/introspect, `REPLICA IDENTITY FULL`, slot create, predicate-pushdown backfill (`seed_lsn`) |
| `apps/engine/src/sql.rs` | predicate → parameterized SQL `WHERE` (backfill pushdown; mirrors the TS compiler / oracle) |
| `apps/engine/src/predicate.rs` | predicate compilation, three-valued `matches`, `equality_template` |
| `apps/engine/src/ds.rs` | durable-streams HTTP client (`ensure_stream`/`append`/`read`) |
| `apps/engine/src/value.rs` | `Value`, `Row` |
| `apps/engine/src/schema.rs` | schema compilation, JSON⇄Row, pk handling |
| `apps/engine/src/metrics.rs` | counters + latency histograms |
| `apps/engine/src/http.rs` | engine control-plane HTTP |
| `apps/api/src/core.ts` | write path + schema/shape forwarding |
| `packages/client/src/index.ts` | typed writes + `shape()` live collections |
| `packages/bench/src/run.ts` | stress benchmark harness |
| `examples/linearlite/src/lib/useShape.ts` | client query layer: shape→collection + `useLiveQuery` (§12) |
```

---

## 12. Client query layer (two-level querying)

There are **two query layers** with different jobs, and keeping them distinct is what lets the system
sync a bounded set yet still present it flexibly:

1. **Server-side shape predicate** (the engine, §4) — *what crosses the network*. One table + a
   `WHERE` over its columns (eq/neq/lt/lte/gt/gte + and/or/not). The engine maintains exactly this set
   on the client's `shape/<id>` stream, incrementally, as Postgres changes. This is the sync boundary:
   rows outside the predicate are never materialized.
2. **Client-side live query** (`@tanstack/react-db`'s `useLiveQuery` over the materialized TanStack DB
   collection) — *how the synced set is presented*. Ordering (`orderBy`), text search (`ilike`/`like`),
   projection (`select`), and any finer filtering run here. TanStack DB maintains the query result
   **incrementally** over the collection (it is not re-computed in JS on every delta), so a client-only
   refinement — e.g. typing in a search box — updates the rendered rows **without changing the
   engine-side shape or re-syncing**.

The split is deliberate: ordering and `LIKE`-style search are intentionally **not** part of the shape
model (§ the shape predicate is an unordered-set filter), because they are presentation concerns over
an already-synced set, and pushing them server-side would couple sync to view state. It is also the
natural seam for windowed / infinite-scroll sync: a value-range shape bounds *what* syncs while the
live query handles ordering within the loaded window.

**Binding pattern (the example apps).** `useShapeCollection(def)` creates the engine-side shape and
returns its live collection (async; `null` while loading or when `def` is `null`), recreating it when
`def` changes and closing it on unmount. `useShapeRows(def, build?, deps?)` runs `useLiveQuery` over
that collection, where `build` pushes `.where()/.orderBy()/.select()` into the query and `deps` carry
any values `build` closes over (so a search term re-runs the *client* query without re-syncing). A
shared always-ready empty placeholder collection is queried while the real one is still loading, so
`useLiveQuery` can be called unconditionally (React rules of hooks). LinearLite filters by
status/priority/id server-side (the shape) and orders by date/kanban-order and searches by text in the
live query; the one exception is priority ordering, which is a rank over a *text* enum (no integer
column) and so is applied as a small client-side sort over the live-query result.
