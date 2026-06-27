# electric-lite

A minimal, [Electric](https://electric-sql.com/)-style **reactive database**: an ingestion API
and a query engine based on [`dbsp`](https://crates.io/crates/dbsp), with storage on
[durable streams](https://durablestreams.com). A client defines a **shape** (a query over the
schema) and receives every change to the database that matches that query, live.

Deliberately simpler than Electric:

- A shape is exactly **one table + a `WHERE` clause over that table's own columns**. No joins,
  no cross-table queries.
- Query expressivity is limited and grows pragmatically: column comparisons (`eq neq lt lte gt
  gte`) combined with `and` / `or` / `not`.

## How it works

Durable Streams is both the **write-ahead log** and the **shape feed** — everything is a stream.

```
                  (control plane: define schema, create/drop shape)
   client ──tRPC──►  API (TS, Node) ──HTTP──► engine (Rust, dbsp)
     │                   │                          ▲   │
     │   write(op,row)   │ append change            │   │ tail table streams
     │                   ▼                          │   │ run one dbsp circuit per shape
     │            durable-streams server  ◄─────────┘   ▼ append matched deltas
     │            table/<name>  (the WAL)        shape/<id> (the feed)
     │                                                  │
     └──── stream-db + TanStack DB ◄────read/SSE────────┘  (materialize + live)
```

1. **Writes** (`ingest.write`) append a change event to `table/<name>` — the authoritative log.
   The API and engine are decoupled through this stream.
2. The **engine** tails every `table/<name>`, feeds changes into one **dbsp filter circuit per
   shape**, and appends the matching deltas to `shape/<id>` with correct enter / leave / update
   semantics (filter of the delta Z-set).
3. A **client** materializes `shape/<id>` with `@durable-streams/state` (stream-db) + TanStack DB
   and receives live updates.

Predicates are a **JSON AST** (not SQL). One predicate definition has two derivations: the engine
compiles it to a Rust closure for `dbsp.filter`; the pglite oracle translates it to a SQL `WHERE`.
Single source of truth ⇒ no drift between the system under test and the oracle.

## Layout

| Path | Lang | Responsibility |
|---|---|---|
| `apps/engine` | Rust | dbsp filter circuit per shape (on its own thread), per-table tailer, control-plane HTTP |
| `apps/api` | TS | tRPC server: `schema.define`, `ingest.write`, `shapes.create/get` |
| `packages/protocol` | TS | shared contract: schema/predicate/change-event types, predicate→SQL / schema→DDL compilers |
| `packages/client` | TS | `createClient()`: typed tRPC client + stream-db materialization (`currentRows`, `awaitTxId`) |
| `packages/oracle` | TS | pglite oracle (DDL/DML/SELECT from the protocol compilers) |
| `packages/conformance` | TS | seeded faker simulator, set comparator, harness, and the conformance tests |

Design and research notes live in `docs/superpowers/`.

## Requirements

Node ≥ 22, [pnpm](https://pnpm.io) 10, and a Rust toolchain (stable; the engine uses `dbsp`).

```bash
pnpm install            # JS deps; allows native builds (lmdb for the test server)
pnpm engine:build       # cargo build -p electric-lite-engine
```

## Tests

```bash
pnpm test               # all unit + e2e tests (TS)
pnpm engine:test        # Rust engine unit tests
pnpm test:conformance   # the conformance suite (boots the full stack)
pnpm test:fuzz          # one batch of oracle-driven random scenarios
```

The conformance harness boots the whole stack — an in-process `DurableStreamTestServer`, the Rust
engine as a child process, the tRPC API, the pglite oracle, and the stream-db client — applies the
**same** op stream to electric-lite and the oracle, and asserts set-equality per shape. Tests drive
the system through the **real API + stream-db client**, including live propagation (`awaitTxId`).

## The conformance loop (for an agent)

The invariant: *for any shape and any op stream, the client-materialized set equals the pglite
`SELECT … WHERE <predicate>` result set.*

```bash
pnpm loop            # run the fuzz test repeatedly until it fails (default 50 iterations)
pnpm loop 200        # more iterations
```

Each iteration generates random-predicate shapes and a random op stream from a fresh seed. On
failure the output prints `FAILED seed=<n>`; replay it exactly with:

```bash
SEED=<n> pnpm exec vitest run packages/conformance/src/conformance-fuzz.test.ts
```

Tunables (env): `FUZZ_SEEDS`, `FUZZ_SHAPES`, `FUZZ_OPS`, `SEED`.

## Status

- **M1 — equality filters:** done. Engine, API, client, oracle, and the live propagation path.
- **M2 — richer predicates:** comparisons + `and/or/not`, validated by the fuzz loop.
- **M3 — robustness:** done — late-shape backfill (both paths), a sound convergence barrier
  (`drainEngine` via the engine's processed-offset endpoint), the schema-derived per-table
  ingestion API (`client.tables.<table>.insert/update/delete`), and long fuzz runs.
- **Deferred (documented in `docs/superpowers/specs/`):** engine restart idempotency
  (deterministic `Producer-Seq`) and three-valued NULL logic (the simulator generates no nulls).
