# electric-lite

A minimal, [Electric](https://electric-sql.com/)-style **reactive database**. Your app writes to
**Postgres**; a query engine built on [`dbsp`](https://crates.io/crates/dbsp) turns those writes into
**live shapes**; and [durable streams](https://durablestreams.com) is the log that carries everything
in between. A client defines a **shape** (a query over the schema) and receives every change to the
database that matches that query, live.

Deliberately simpler than Electric:

- A shape is exactly **one table + a `WHERE` clause over that table's own columns**. No joins,
  no cross-table queries.
- Query expressivity is limited and grows pragmatically: column comparisons (`eq neq lt lte gt
  gte`) combined with `and` / `or` / `not`.

## Design in three layers

The system is three layers stacked on one idea — *incrementally maintain query results as the
database changes* — each with a single responsibility and a clean seam to the next.

```
   ┌─────────────────────────────────────────────────────────────────────────┐
   │  app  ──writes──►  POSTGRES  (system of record)                          │
   │                       │  logical replication (test_decoding slot)        │
   │                       │  + REPEATABLE READ snapshot for backfill         │
   │                       ▼                                                   │
   │   INGESTOR  ─decode commits→ envelopes (old+new, commit LSN)─►            │
   │                       │                                                   │
   │                       ▼  append                                           │
   │  DURABLE STREAMS   table/<name>  (the log)                                │
   │                       │  tail                                             │
   │                       ▼                                                   │
   │  DBSP ENGINE   one filter circuit per shape ──matched deltas──►           │
   │                       │  append                                           │
   │                       ▼                                                   │
   │  DURABLE STREAMS   shape/<id>  (the feed)                                 │
   │                       │  read / long-poll                                 │
   │                       ▼                                                   │
   │  CLIENT   stream-db + TanStack DB  →  live materialized set               │
   └─────────────────────────────────────────────────────────────────────────┘
```

### 1. Postgres — the system of record

Apps write to Postgres with ordinary SQL; Postgres owns durability, constraints, and transactions.
electric-lite observes it two ways, reconciled by LSN:

- **Live changes** come from **logical replication**. The engine creates a `test_decoding`
  replication slot and polls it (`pg_logical_slot_peek_changes`), decoding each committed transaction
  into change envelopes. `REPLICA IDENTITY FULL` makes Postgres emit the **old and new** tuple for
  updates/deletes, so the engine can retract the prior row precisely. Changes are read, appended to
  durable-streams, and only **then** is the slot advanced — a failed append re-reads rather than
  loses data.
- **Backfill** for a newly-created shape reads the table's current rows in a single
  `REPEATABLE READ` snapshot (`SELECT to_jsonb(t) …`) and records the snapshot's
  `pg_current_wal_lsn()` as the shape's `seed_lsn`.

**The reconciliation that makes this sound:** every change envelope is stamped with its
transaction's **COMMIT LSN** (not the per-change record LSN — the ingestor buffers a transaction's
changes and stamps them when the `COMMIT` record arrives). The engine then skips any replicated
change whose commit LSN is strictly `< seed_lsn`. A transaction visible to the backfill snapshot
committed before it (commit LSN `< seed_lsn` → already in the backfill, skip); a transaction
committing at/after the snapshot has commit LSN `>= seed_lsn` → kept from the live stream. Using the
commit LSN (rather than the record LSN) is what prevents silently dropping rows of transactions that
were in flight while the snapshot was taken. (Regression-guarded by
`packages/conformance/src/conformance-concurrency.test.ts`.)

### 2. dbsp — the incremental query engine

A shape is *one table + a predicate*. The predicate is a **JSON AST** (not SQL); the engine compiles
it to a Rust closure. Each row change arrives as a **Z-set delta** (rows with `+1`/`-1` weights), and
the engine maintains the shape's result incrementally:

- **Standalone shapes** are a stateless filter applied directly to the delta — no per-shape thread,
  no state. The matching rows (and their enter/leave/update transitions) fall straight out of
  filtering the delta Z-set.
- **Shape families** (many shapes over the same table/columns) share a `dbsp` circuit so the
  incremental work is done once and fanned out.

dbsp guarantees the incremental result equals a full recompute, so the live set is always exactly the
query result — never an approximation that drifts.

### 3. Durable Streams — the log and the shape feed

Everything is a stream, which is what decouples the layers:

- **`table/<name>`** is the per-table change log. The ingestor appends decoded commits here; the
  engine tails it. The engine and the source of writes never talk directly.
- **`shape/<id>`** is the per-shape feed. The engine appends matched deltas (enter/leave/update);
  the client reads/long-polls it.
- A **client** materializes `shape/<id>` with `@durable-streams/state` (stream-db) into a **TanStack
  DB collection** and re-renders on every delta.

Because the predicate has a single definition, it has two derivations with no drift: the engine
compiles it to a Rust `dbsp.filter` closure; the oracle translates it to a SQL `WHERE`. The
conformance suite asserts the two always agree.

### Two-level querying: server shape vs client live query

There are two query layers, and they do different jobs:

- The **server-side shape predicate** (engine) decides *what crosses the network* — one table + a
  `WHERE` over its columns. It's the sync boundary: only matching rows are materialized on the client,
  and the engine maintains that set incrementally as Postgres changes.
- A **client-side live query** (TanStack DB `useLiveQuery`) runs *over the already-materialized
  collection* for the things the shape predicate can't express — ordering (`ORDER BY`), text search
  (`ilike`/`LIKE`), and any finer filtering — without re-syncing. Because it's a live query, it's
  maintained **incrementally** (not re-run in JS on every delta), and a client-only refinement (e.g.
  typing in a search box) changes the rendered result without touching the engine-side shape.

So: shape = *what you sync*, live query = *how you present it*. The example apps follow this split —
the engine filters by status/priority/id; the client orders by date/kanban-order and searches by text
in the live query. (Ordering/`LIKE` are deliberately not part of the shape model; this is also the
seam where windowed/infinite-scroll sync would slot in — see the partial-sync notes.)

> Postgres is the default source of record. The engine can also run **without** Postgres — writes
> append directly to `table/<name>` through the tRPC `ingest.write` API — which is how the
> non-Postgres unit paths and the `pnpm demo` todos example run. The dbsp + durable-streams layers
> are identical either way; only the source of `table/<name>` changes. See
> **[docs/deployment-postgres.md](docs/deployment-postgres.md)** for the Postgres setup and config.

## Layout

| Path | Lang | Responsibility |
|---|---|---|
| `apps/engine` | Rust | dbsp filter circuit per shape, per-table tailer, Postgres ingestor + backfill, control-plane HTTP |
| `apps/api` | TS | tRPC server: `schema.define`, `ingest.write`, `shapes.create/get` |
| `packages/protocol` | TS | shared contract: schema/predicate/change-event types, predicate→SQL / schema→DDL / change→DML compilers |
| `packages/client` | TS | `createClient()`: typed tRPC client + stream-db materialization (`currentRows`, `awaitTxId`) |
| `packages/oracle` | TS | Postgres/pglite oracle (DDL/DML/SELECT from the protocol compilers) |
| `packages/conformance` | TS | seeded faker simulator, set comparator, harness, and the conformance tests |
| `examples/web`, `examples/linearlite` | TS | live demo apps on the Postgres backend |

Design and research notes live in `docs/superpowers/`; architecture in
**[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**.

## Requirements

Node ≥ 22, [pnpm](https://pnpm.io) 10, a Rust toolchain (stable; the engine uses `dbsp`), and (for the
Postgres path and the demos) a local **PostgreSQL 16** with `wal_level=logical` — the demos boot their
own ephemeral cluster, so you only need `initdb`/`pg_ctl` on `PATH`.

```bash
pnpm install            # JS deps; allows native builds (lmdb for the test server)
pnpm engine:build       # cargo build -p electric-lite-engine
```

## Try it

```bash
pnpm demo            # headless: a live "active high-priority todos" shape over the schema-derived API
pnpm demo:web        # Todos web app on the Postgres backend; open the printed Local URL
pnpm demo:linearlite # a LinearLite clone on the Postgres backend
```

- **`pnpm demo`** (`examples/todos-demo.ts`) boots the stack and runs a live
  `done = false AND priority >= 3` shape, writing through the schema-derived API. Rows enter and
  leave the shape live as todos are completed, re-prioritised, and deleted.
- **`pnpm demo:web`** (`examples/web/`) is a React app that materializes shapes live with stream-db +
  TanStack DB. `start.ts` boots an ephemeral Postgres, the engine in Postgres mode, durable-streams,
  and the API on **ephemeral ports**, then Vite proxies `/api` and `/ds` to them (no CORS, no
  fixed-port clashes). Edit todos on the left; the right panel is the live shape. Set
  `DEMO_SEED_COUNT` / `DEMO_CHURN_MS` to size the initial workload and drive continuous writes.
- **`pnpm demo:linearlite`** (`examples/linearlite/`) ports ElectricSQL's LinearLite issue tracker —
  board (drag-and-drop), list, filters, detail, comments — onto electric-lite. See
  **[examples/linearlite/README.md](examples/linearlite/README.md)**.

### Using it in code

```ts
import { createClient } from '@electric-lite/client'

const client = createClient({ apiUrl, schema })   // schema: { tables: { todos: { columns, primaryKey } } }

// 1. define a live shape (one table + a WHERE over its columns)
const shape = await client.shape({
  table: 'todos',
  where: { and: [{ col: 'done', op: 'eq', value: false }, { col: 'priority', op: 'gte', value: 3 }] },
})

// 2. read the current set, and subscribe to live changes
shape.currentRows()                                  // Row[]
const off = shape.subscribe((changes) => { /* insert/update/delete batches */ })

// 3. write to Postgres (the system of record); the engine ingests via replication and the shape updates
//    live. (Without Postgres, writes can instead go through client.tables.<table>.insert/update/delete.)
```

In production you run the real `durable-streams-server`, the engine binary, and the API server as
separate processes pointed at your Postgres, and point the client at the API URL; the demos colocate
them for convenience.

## Tests

```bash
pnpm test               # all unit + e2e tests (TS)
pnpm engine:test        # Rust engine unit tests (parser, circuits, commit-LSN stamping)
pnpm test:conformance   # the conformance suite (boots the full stack on a real Postgres)
pnpm test:fuzz          # one batch of oracle-driven random scenarios
```

The conformance harness boots the whole stack — an in-process `DurableStreamTestServer`, the Rust
engine as a child process (in Postgres mode), the tRPC API, the Postgres oracle, and the stream-db
client — applies the **same** op stream to electric-lite and the oracle, and asserts set-equality per
shape. It drives the system through the **real API + stream-db client**, including live propagation,
and includes a concurrent-writer test that creates shapes while several connections write, guarding
the backfill↔replication reconciliation.

## The conformance loop (for an agent)

The invariant: *for any shape and any op stream, the client-materialized set equals the oracle's
`SELECT … WHERE <predicate>` result set.*

```bash
pnpm loop            # run the fuzz test repeatedly until it fails (default 50 iterations)
pnpm loop 200        # more iterations
```

On failure the output prints `FAILED seed=<n>`; replay it exactly with:

```bash
SEED=<n> pnpm exec vitest run packages/conformance/src/conformance-fuzz.test.ts
```

Tunables (env): `FUZZ_SEEDS`, `FUZZ_SHAPES`, `FUZZ_OPS`, `SEED`.

## Status

- **M1 — equality filters:** done. Engine, API, client, oracle, and the live propagation path.
- **M2 — richer predicates:** comparisons + `and/or/not`, validated by the fuzz loop.
- **M3 — robustness:** done — late-shape backfill, a sound convergence barrier (`drainEngine`), and
  the schema-derived per-table ingestion API.
- **Postgres source of record:** done — logical-replication ingest (`test_decoding`), snapshot
  backfill, and commit-LSN reconciliation, exercised by the conformance suite and the demo apps.
- **Deferred (documented in `docs/superpowers/specs/`):** engine restart idempotency
  (deterministic `Producer-Seq`) and three-valued NULL logic (the simulator generates no nulls).
