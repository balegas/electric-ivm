# electric-ivm

A reactive sync engine in the style of [Electric](https://electric-sql.com/), built on incremental
view maintenance. Your app writes to **Postgres**; a Rust engine turns logical-replication changes
into **live shapes** (incrementally-maintained query results); [durable streams](https://durablestreams.com)
is the log that carries everything in between. It speaks **two client protocols**:

- **The Electric protocol** — `GET /v1/shape` on the engine, compatible with the ElectricSQL TS
  client and validated against Electric's own oracle/property/integration tests
  ([`electric-conformance/`](electric-conformance/README.md)).
- **The extended API** (`@electric-ivm/client`) — shapes plus the pieces the Electric API doesn't
  cover today: **subset queries** (ordered, windowed pages with a shared live tail) and **live
  aggregations** (COUNT/SUM/AVG/MIN/MAX maintained incrementally). This surface is where the API is
  headed; the Electric endpoint is the compatibility layer.

## The design in three moves

```
  app ──SQL writes──▶ POSTGRES (system of record; wal_level=logical)
                         │  logical replication (commit LSN + xid + seq stamped)
                         ▼
                      DURABLE STREAMS   table/<name>       (the change log)
                         │  one tailer per table, (lsn,seq) de-duplicated
                         ▼
                      ENGINE   Z-set delta → shared routing/filters/subqueries/aggregations
                         │  reliable append
                         ▼
                      DURABLE STREAMS   shape/<id>         (ONE feed per DISTINCT shape)
                         │  read / long-poll
                         ▼
                      CLIENTS   Electric client (/v1/shape)  or  @electric-ivm/client
```

1. **The engine holds no copy of any table.** Row-count-scale state lives in Postgres; shapes
   backfill just their matching rows in a `REPEATABLE READ` snapshot. Engine RSS is ~19 MiB whether
   the database has 1k or 100k rows, +~0.8 KiB per shape
   ([measurements](docs/bench/shape-memory-matrix.md)).
2. **Every change is a Z-set delta** (rows with ±1 weights, from the replication old+new tuples), and
   the *shape of the predicate* picks a shared execution strategy: equality templates route by key
   through one shared router per template; ranges/OR/NOT run as stateless filters; subqueries flow
   through shared inner-set nodes; aggregations fold the delta into a running scalar. No per-shape
   circuit, no per-shape thread.
3. **Everything equal is de-duplicated.** Two identical shapes (same table, canonical predicate,
   projection, kind) share one maintained stream, ref-counted — as do identical subquery inner-sets
   and identical aggregations. A thousand clients opening the same shape cost the engine one
   maintenance path and one append per change.

Full architecture: **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**; engine execution + cost model:
**[docs/ivm-engine-internals.md](docs/ivm-engine-internals.md)**.

## Consistency model (the short version)

- **Backfill ↔ live is fenced by transaction visibility**, not WAL position: the backfill snapshot
  records `pg_current_snapshot()` and the engine skips a replicated change iff its xid was visible to
  that snapshot. (An LSN-only fence silently drops rows committed-but-not-yet-visible during the
  snapshot — a race we hit and closed; see ARCHITECTURE §4.)
- **Ingest is at-least-once, effect is exactly-once**: the ingestor appends before advancing the
  slot; tailers de-duplicate by the stamped `(commit LSN, seq)`.
- **Shape appends never drop silently**: transient storage failures retry with backoff and the
  convergence barrier only advances once every subscriber stream reflects the batch.
- **Subquery membership is emitted absolutely** (current membership per touched pk, never a
  history-dependent delta), so cross-table tailer interleaving cannot miss move-outs — no
  LSN-buffering protocol needed.
- **Shape creation is atomic**: a failed backfill/registration rolls everything back and surfaces the
  error — never a zombie shape that pins its signature and streams nothing.

The conformance invariant, asserted end-to-end through the real API/streams/client: *for any shape
and any op stream, the client-materialized set equals the oracle's `SELECT … WHERE <predicate>`* —
including live replication, batched mutations, NULL three-valued logic, and concurrent writers.

## What a shape is

One table + a `WHERE` over its columns — comparisons (`eq neq lt lte gt gte`, `LIKE`), null tests
(`col IS [NOT] NULL`), combined with `and/or/not`, **plus single-column subqueries**
`col [NOT] IN (SELECT proj FROM other WHERE …)` (recursive) as the one cross-table form. An optional **`columns`** projection bounds what is synced
(never what is matched). Ordered windows (`orderBy`+`limit`) are deliberately **not** a shape knob —
they live in subset queries, so ranges are never live-tailed and a change never fans out across
pages.

On top of shapes, the extended API adds:

- **Subset queries** — one-shot `SELECT … ORDER BY … LIMIT/OFFSET` pages + a shared changes-only live
  feed, merged client-side by per-pk LSN watermarks (with delete tombstones, so a stale page can
  never resurrect a deleted row). The basis for infinite scroll / keyset pagination.
- **Aggregations** — live scalar COUNT/SUM/AVG/MIN/MAX over a predicate, SQL NULL semantics,
  retraction-correct MIN/MAX. One maintained fold feeds every subscriber of the same aggregate.

## Try it

Requirements: Node ≥ 22, pnpm 10, Rust stable, and (for Postgres mode/demos) PostgreSQL 16 binaries
on `PATH` (`initdb`/`pg_ctl` — the demos boot their own ephemeral cluster).

```bash
pnpm install
pnpm engine:build

pnpm demo               # headless live-shape demo
pnpm demo:linearlite    # LinearLite (issue tracker) on electric-ivm — the flagship demo
scripts/linearlite.sh start large   # same, at a 100k-issue workload + the pipeline explorer
```

The **pipeline explorer** (`apps/pipeline-viz`, printed URL on demo start) shows the live maintained
pipeline — shapes, shared families, shared subquery nodes, per-node indexes, live shape contents.

### Docker

```bash
pnpm docker:up    # Postgres + durable-streams + engine (+ extended API) — see docker/README.md
```

Point an ElectricSQL client at `http://localhost:7010/v1/shape`, or `@electric-ivm/client` at
`http://localhost:8790`.

### Using the extended client

```ts
import { createClient } from '@electric-ivm/client'
const client = createClient({ apiUrl, schema })

// a live shape (materialized TanStack DB collection)
const shape = await client.shape({
  table: 'issues',
  where: { col: 'project_id', in: { table: 'project_members', project: 'project_id',
           where: { col: 'user_id', op: 'eq', value: 42 } } },   // visibility subquery
})
shape.currentRows(); shape.subscribe(cb); await shape.close()

// an ordered page + live tail (infinite scroll)
const page = await client.subset({ table: 'issues', orderBy: { col: 'created', desc: true }, limit: 50, where })

// a live aggregate
const count = await client.aggregate({ table: 'issues', fn: 'count', where })
```

Writes go to Postgres (the system of record) with ordinary SQL; the engine ingests via replication.
Without Postgres (library mode), writes can go through `client.tables.<t>.insert/update/delete`.

## Electric protocol conformance

`electric-conformance/` runs **Electric's own tests** against our `/v1/shape`: the oracle harness
(levels 1–4), the PROPERTY test over the full schema+grammar, and the subquery integration tests.
Known scope gaps are documented in its README (e.g. row `tags` are not emitted — absolute membership
emission makes them unnecessary for convergence). The adapter parses Electric's SQL `where` grammar,
serves snapshot + live long-poll with handles/offsets, and evicts idle handles after a TTL.

## Benchmarks

```bash
pnpm bench:fleet    # clones electric-sql/benchmarking-fleet and runs its byo_electric
                    # benchmarks (unmodified .exs) against our /v1/shape — results in
                    # docs/bench/electric-fleet-results.md
```

Load/observability companions: `packages/loadgen` (state-machine users; memory/CPU/disk vs workload,
Docker-scalable clients) and `docs/bench/shape-memory-matrix.md` (memory vs shapes × deployment size).

## Tests

```bash
pnpm engine:test        # Rust engine unit tests (parsers, gates, dedup, aggregation semantics)
pnpm test               # full TS suite incl. conformance (boots its own Postgres)
pnpm test:fuzz          # random-predicate fuzz vs the oracle
pnpm loop [N]           # run the fuzz loop until failure; replay with SEED=<n>
```

## Layout

| Path | Lang | Responsibility |
|---|---|---|
| `apps/engine` | Rust | replication ingest, shape/subquery/aggregation maintenance, control HTTP, `/v1/shape` |
| `apps/api` | TS | extended tRPC API (schema, writes, shapes, subsets, aggregations) |
| `packages/protocol` | TS | shared contract: schema/predicate/envelope types + compilers |
| `packages/client` | TS | `shape()` / `subset()` / `aggregate()` + tracked lifecycles |
| `packages/oracle` / `packages/conformance` | TS | reference implementation + the conformance suite |
| `packages/bench` / `packages/loadgen` | TS | fleet-benchmark runner, memory matrix, load generator |
| `electric-conformance/` | Elixir | Electric's own tests, pointed at our adapter |
| `examples/linearlite`, `examples/web` | TS | demo apps |
| `docker/` | — | containerized stack |
| `apps/pipeline-viz` | TS | live pipeline explorer |

Design records live in `docs/superpowers/specs/` (one per feature). Agent guidance: **AGENTS.md**.
