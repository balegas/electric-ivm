# @electric-ivm/conformance

The end-to-end conformance suite. The invariant it asserts, through the **real** stack (Postgres →
replication → engine → durable-streams → client), against the
[oracle](../oracle/README.md):

> For any shape and any op stream, the client-materialized set equals the oracle's
> `SELECT … WHERE <predicate>`.

## What the harness boots (`src/harness.ts`)

A vitest `globalSetup` builds the engine once and starts **one ephemeral Postgres** with
`wal_level=logical` (each harness then creates its own database + replication slot inside it, so
test files stay isolated). Per harness:

- a per-test Postgres database (the system of record *and*, via `createPgOracle`, the truth),
- a `DurableStreamTestServer` (the log),
- the Rust engine as a child process in Postgres mode (spawned from `target/debug/`, discovered
  via its `ENGINE_LISTENING` stdout line),
- the tRPC API (`createApiServer`) and a real `@electric-ivm/client`.

Comparison (`src/compare.ts`) is set equality over declared columns, keyed by stringified pk.

## Test areas

| Files | What they pin down |
|---|---|
| `conformance.test.ts`, `-postgres`, `-backfill` | core invariant: live replication, batched mutations, backfill ↔ live fencing |
| `conformance-concurrency.test.ts` | concurrent writers |
| `conformance-nulls.test.ts` | NULL three-valued logic (`NOT (col = x)` over NULL, `IS [NOT] NULL`) |
| `conformance-subquery*.test.ts` | `IN (SELECT …)`: scenarios, cross-table matrix, nested subqueries, inner-node sharing |
| `conformance-sharing`, `-shape-sharing` | identical shapes/feeds collapse to one stream, refcounted create/drop |
| `conformance-subset-positioning.test.ts` | subset LSN positioning: a change in the page/feed overlap window is counted exactly once |
| `conformance-expressiveness`, `-transitions` | full predicate grammar, rows entering/leaving shapes |
| `conformance-fuzz`, `-fuzz-wide`, `-counterexample` | random-predicate fuzz vs the oracle + pinned counterexample replays |
| `harness-mechanics.test.ts` | the harness itself (boot, teardown, engine discovery) |

## Running

Requires PostgreSQL 16 binaries on `PATH` (`initdb`/`pg_ctl`) — the suite boots its own cluster.
From the repo root:

```bash
pnpm test:conformance     # the whole suite
pnpm test:fuzz            # the random-predicate fuzz test
pnpm loop [N]             # run the fuzz test repeatedly until failure (default 50 iterations)

# a fuzz failure prints `FAILED seed=<n>`; replay it exactly:
SEED=<n> pnpm exec vitest run packages/conformance/src/conformance-fuzz.test.ts
```

Fuzz tunables: `FUZZ_SEEDS` (scenarios per run, default 5), `FUZZ_SHAPES`, `FUZZ_OPS`, `SEED`
(base seed). `src/simulator.ts` generates the random schemas/predicates/op streams.

This suite tests the **extended** API; Electric's own protocol tests run separately from
[`electric-conformance/`](../../electric-conformance/README.md). Consistency model:
[docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md).
