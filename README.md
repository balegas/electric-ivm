# electric-ivm

A reactive sync engine in the style of [Electric](https://electric-sql.com/), built on **incremental
view maintenance**. Your app writes to Postgres with ordinary SQL; clients subscribe to **shapes** —
queries whose result sets stay live — and receive every change that affects them, incrementally. A
Rust engine sits between the two, turning the Postgres logical-replication stream into per-shape
change feeds. It speaks the Electric wire protocol (`GET /v1/shape`, works with the unmodified
ElectricSQL client) plus an extended API that adds subset queries and live aggregations.

## What is a shape?

A shape is a query over one table whose **result set is maintained for you as the database
changes**:

```sql
SELECT * FROM issues
WHERE status = 'todo' AND priority >= 3
```

Subscribe to that shape and you first receive its current rows (the *snapshot*), then a live feed of
exactly three kinds of message, forever:

- `upsert` — a row entered the result set, or changed while inside it;
- `delete` — a row left the result set (deleted, **or updated so it no longer matches**);
- nothing — the change didn't affect this shape.

That last bullet is the point. A shape is a **sync boundary**: only matching rows (and only the
columns you project) ever cross the network, and the client's local copy is always exactly the
query's result — never a cache to invalidate, never an approximation to refresh.

Shape predicates are comparisons (`= <> < <= > >=`, `LIKE`), null tests (`IS [NOT] NULL`),
`AND/OR/NOT`, and one cross-table form: single-column subqueries,
`col [NOT] IN (SELECT proj FROM other WHERE …)`, recursively. Ordering and windowing are
deliberately *not* shape features — they live in **subset queries** (below), so a shape's
maintenance never involves range state.

## What is DBSP?

[DBSP](https://docs.rs/dbsp) is a theory (and Rust library) of incremental computation from the
Feldera project. Its core idea:

1. Represent data as **Z-sets** — collections where every row carries a signed weight
   (multiplicity). A table is a Z-set where present rows have weight `+1`.
2. Represent *change* as a **delta** — a tiny Z-set: an insert is `(row, +1)`, a delete is
   `(row, −1)`, an update is both — `(old, −1), (new, +1)`.
3. Build queries as **operator pipelines** over Z-sets (filter, map, join, aggregate…), where each
   operator has an *incremental* form: feed it a delta and it emits the delta of its output.

The consequence: to keep a query result up to date you never re-run the query — you push each
change's delta through the pipeline and apply the (usually tiny) output delta to the result. Cost
scales with the size of the *change*, not the size of the *data*.

electric-ivm is built on this model. Every replicated change becomes a Z-set delta (Postgres's
`REPLICA IDENTITY FULL` supplies the old row, so updates retract precisely), and every shape,
subquery, and aggregation is an incremental operator over those deltas. The engine implements the
operators directly in Rust — stateless filters, key routers, shared inner-set nodes, running
folds — rather than running a general dbsp circuit, so it keeps **no copy of any table**: engine
RSS is ~19 MiB whether the database has 1k or 100k rows, +~0.8 KiB per shape
([measurements](docs/bench/shape-memory-matrix.md)).

## A shape as a DBSP pipeline

Take the shape above and one write:

```sql
UPDATE issues SET status = 'done' WHERE id = 42;   -- was: status = 'todo', priority = 4
```

```
                    Postgres logical replication (old + new tuple)
                                      │
                                      ▼
              Z-set delta:   (id=42, status='todo', prio=4)  → −1
                             (id=42, status='done', prio=4)  → +1
                                      │
                                      ▼
              σ  status='todo' AND priority>=3        (incremental filter:
                                      │                keep matching weighted rows)
                                      ▼
                             (id=42, status='todo', prio=4)  → −1
                                      │                       (new row filtered out)
                                      ▼
              group by pk → net weight negative → emit
                                      │
                                      ▼
              shape feed:   delete id=42          ← the row leaves every subscriber, live
```

No table scan, no diffing, no re-query — the update's own delta carried everything needed to know
that row 42 must *leave* the shape.

The one cross-table operator works the same way. A per-user visibility shape:

```sql
SELECT * FROM issues
WHERE project_id IN (SELECT project_id FROM project_members WHERE user_id = 42)
```

compiles to a two-input pipeline with a small piece of shared state — the maintained **inner set**
(user 42's project ids, a handful of values, not any issues):

```
project_members deltas ──▶ [ inner-set node: {project_id | user_id = 42} ]
                                      │  value enters/leaves the set ("flip")
                                      ▼
                           re-evaluate the affected outer rows (issues WHERE project_id = P)
                                      │
issues deltas ────────────▶ [ membership test against the node ]
                                      │
                                      ▼
                           shape feed: upserts / deletes
```

Add user 42 to a project and every issue of that project upserts into their shape; remove them and
the issues delete — driven entirely by the membership table's delta. The pipeline for any running
engine is inspectable live in the **pipeline explorer** (`apps/pipeline-viz`).

**Everything equal is de-duplicated.** Two identical shapes (same table, canonical predicate,
projection) share one maintained pipeline and one output stream, ref-counted — as do identical
subquery inner sets and identical aggregations. A thousand clients opening the same shape cost the
engine one maintenance path and one append per change.

## The system

```
  app ──SQL writes──▶ POSTGRES (system of record; wal_level=logical)
                         │  logical replication
                         ▼
                      DURABLE STREAMS   table/<name>       (the change log)
                         │  one tailer per table
                         ▼
                      ENGINE   Z-set deltas → shared filters/routers/subqueries/aggregations
                         │
                         ▼
                      DURABLE STREAMS   shape/<id>         (one feed per DISTINCT shape)
                         │  read / long-poll
                         ▼
                      CLIENTS   Electric client (/v1/shape)  or  @electric-ivm/client
```

Postgres owns durability and transactions; [durable streams](https://durablestreams.com) is the log
that decouples every layer (the engine is a restartable consumer in the middle); the engine holds
only per-shape routing metadata and the shared inner sets. Backfills read just a shape's matching
rows in a `REPEATABLE READ` snapshot, fenced against the live stream by transaction visibility.
Full design: **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**; execution strategies + cost model:
**[docs/ivm-engine-internals.md](docs/ivm-engine-internals.md)**.

## Two client surfaces

- **The Electric protocol** — `GET /v1/shape` on the engine, compatible with the ElectricSQL TS
  client and validated against Electric's own oracle/property/integration tests
  ([`electric-conformance/`](electric-conformance/README.md)).
- **The extended API** (`@electric-ivm/client`) — shapes plus the pieces the Electric API doesn't
  cover today; this surface is where the API is headed:
  - **Subset queries** — one-shot `SELECT … ORDER BY … LIMIT` pages + a shared live tail, merged
    client-side; the basis for infinite scroll / keyset pagination.
  - **Aggregations** — live scalar COUNT/SUM/AVG/MIN/MAX over a predicate, maintained as an
    incremental fold with SQL NULL semantics and retraction-correct MIN/MAX.

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

### The apps in this repo

Each is self-contained with its own README; every demo boots everything it needs (ephemeral
Postgres, durable-streams, the engine) in one command.

**`apps/engine` — the sync engine (Rust).** The core: ingests Postgres logical replication and
maintains every shape/subquery/aggregation incrementally; serves the control-plane HTTP API,
the Electric wire protocol (`/v1/shape`), and the `/trace` SSE feed. You rarely run it by hand —
the demos and Docker do — but standalone:

```bash
cargo build -p electric-ivm-engine
ELECTRIC_IVM_DS_URL=<durable-streams url> ELECTRIC_IVM_PG_URL=<postgres url> \
ELECTRIC_IVM_PG_TABLES='*' target/debug/electric-ivm-engine   # prints ENGINE_LISTENING <url>
```

**`apps/api` — the extended API (TS).** The tRPC façade for the extended client surface (schema,
writes, shapes, subset queries, aggregations). Run via the demos or `docker/api-server.ts`.

**`apps/pipeline-viz` — the pipeline explorer (TS).** A developer/debugging GUI that attaches to
any running engine and renders the maintained dbsp pipeline (logical + circuit views, node
details, live shape contents). Auto-launched by `pnpm demo:linearlite`, or standalone:

```bash
ELECTRIC_IVM_ENGINE_URL=http://127.0.0.1:<engine-port> VIZ_PORT=5180 \
  pnpm --filter @electric-ivm/pipeline-viz dev
```

**`apps/playground` — the dbsp playground (TS).** The audience-facing interactive demo: a
food-delivery world whose writes animate live through the real engine's pipeline to subscriber
device cards, with a six-scene walkthrough, per-visitor workspaces, and click-to-inspect node
details. Built for demo videos and public hosting.

```bash
pnpm demo:playground      # ephemeral PG + engine + server + app (+ HTTPS/2 front if caddy is installed)
pnpm docker:playground    # the hosted stack (compose overlay), app on :5199
```

**`examples/linearlite` — the flagship demo app (TS).** A Linear-style issue tracker synced
entirely through shapes (visibility subqueries, live counts, subset pagination):
`pnpm demo:linearlite`, or `scripts/linearlite.sh start large` for a 100k-issue workload.

**`examples/web` — the minimal example (TS).** The smallest end-to-end app using the extended
client: `pnpm demo:web`. (`pnpm demo` runs the even smaller headless todos walkthrough.)

### Docker

```bash
pnpm docker:up    # Postgres + durable-streams + engine (+ extended API) — see docker/README.md
```

Point an ElectricSQL client at `http://localhost:7010/v1/shape`, or `@electric-ivm/client` at
`http://localhost:8790`. Prebuilt images: `ghcr.io/balegas/electric-ivm/{engine,node}`.

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
(levels 1–4), the PROPERTY test over the full schema+grammar, and the subquery integration tests —
one command via `electric-conformance/run.sh`. Known scope gaps are documented in its README (e.g.
row `tags` are not emitted — absolute membership emission makes them unnecessary for convergence).

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

The conformance invariant, asserted end-to-end through the real API/streams/client: *for any shape
and any op stream, the client-materialized set equals a Postgres oracle's
`SELECT … WHERE <predicate>`* — including live replication, batched mutations, NULL three-valued
logic, and concurrent writers.

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
| `apps/pipeline-viz` | TS | live pipeline explorer (developer tool) |
| `apps/playground` | TS | interactive dbsp playground (public demo: scenes, workspaces, trace animation) |

Each package has its own README. Agent guidance for working in this repo: **AGENTS.md**.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE), at your option.
