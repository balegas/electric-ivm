# Electric Circuits

**Electric Circuits make your app's queries live.** Write the queries your app already runs — joins,
aggregates, subqueries — and every result becomes a live primitive your code programs against: bind
it to a component, sync it into a local collection, feed it to an agent. No fetch, poll, refetch,
invalidate.

Your app writes to Postgres with ordinary SQL. A Rust engine ingests the logical-replication stream
and keeps every result in sync. Behind every query is a **circuit** — one of a small, fixed set of
shared dataflows, one per *kind* of query, that maintains every result incrementally and never holds
a copy of your data. A new user, a new parameter, a whole new query is data flowing through a
dataflow that's already there.

The engine speaks the Electric wire protocol (`GET /v1/shape`, works with the unmodified ElectricSQL
client) plus an extended API (`@electric-circuits/client`) that adds subset queries and live
aggregations.

## What is a live query?

A live query is a query whose **result set is maintained for you as the database changes**:

```sql
SELECT * FROM issues
WHERE status = 'todo' AND priority >= 3
```

Run it and you first receive its current rows (the *snapshot*), then a live feed of exactly three
kinds of message, forever: `upsert` (a row entered the result, or changed while inside it),
`delete` (a row left it), or nothing (the change didn't affect this query). A live query is a
**sync boundary**: only matching rows and projected columns cross the network, and the client's
local copy is always exactly the query's result — never a cache to invalidate.

Predicates cover comparisons, `LIKE`, null tests, `AND/OR/NOT`, and one cross-table form:
`col [NOT] IN (SELECT … WHERE …)` subqueries, recursively. Ordering and windowing live in **subset
queries** instead, so live-query maintenance never involves range state. Full semantics:
[docs/live-queries-guide.md](docs/live-queries-guide.md).

## Under the hood: DBSP

Electric Circuits is built on [DBSP](https://docs.rs/dbsp), Feldera's theory and Rust library of
incremental computation: data is **Z-sets** (rows with signed weights), change is a **delta**
(insert `+1`, delete `−1`, update both), and queries are operator pipelines where each operator
consumes a delta and emits the delta of its output. Keeping a result up to date never re-runs the
query — cost scales with the size of the *change*, not the *data*. The engine keeps **no copy of
any table**: memory scales with the kinds of query your app runs, not with table size or query
count ([docs/memory-model.md](docs/memory-model.md)).

One write through the query above:

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
              live query feed:   delete id=42     ← the row leaves every subscriber, live
```

No table scan, no diffing, no re-query — the update's own delta carried everything needed to know
that row 42 must *leave* the result. Cross-table subqueries work the same way: the engine maintains
the small inner set (say, a user's project ids — never any issues), and a value entering or leaving
it drives upserts/deletes into every affected result. Identical live queries share one maintained
pipeline and one output stream, ref-counted. Watch any running engine's pipeline live in the
**pipeline explorer** (`apps/pipeline-viz`).

How a query registers onto its circuit, end to end:
[docs/how-queries-become-live.md](docs/how-queries-become-live.md). Engine internals and cost
model: [docs/ivm-engine-internals.md](docs/ivm-engine-internals.md).

## The system

```
  app ──SQL writes──▶ POSTGRES (system of record; wal_level=logical)
                         │  logical replication
                         ▼
                      DURABLE STREAMS   changes            (the single ordered change log)
                         │  one LSN-ordered sequencer (all tables, commit order)
                         ▼
                      ENGINE   Z-set deltas → shared circuits (membership, aggregation, routing)
                         │
                         ▼
                      DURABLE STREAMS   shape/<id>         (one feed per DISTINCT live query)
                         │  read / long-poll
                         ▼
                      CLIENTS   Electric client (/v1/shape)  or  @electric-circuits/client
```

Postgres owns durability and transactions; [durable streams](https://durablestreams.com) is the log
that decouples every layer; the engine is a restartable consumer in the middle holding only routing
metadata and the shared inner sets. Full design:
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Two client surfaces

- **The Electric protocol** — `GET /v1/shape`, compatible with the ElectricSQL TS client and
  validated against Electric's own oracle/property/integration tests
  ([`electric-conformance/`](electric-conformance/README.md)).
- **The extended API** (`@electric-circuits/client`) — live queries plus **subset queries**
  (ordered pages + a shared live tail; infinite scroll) and **aggregations** (live
  COUNT/SUM/AVG/MIN/MAX with SQL NULL semantics).

## Try it

Requirements: Node ≥ 22, pnpm 10, Rust stable, and PostgreSQL 16 binaries on `PATH`
(`initdb`/`pg_ctl` — the demos boot their own ephemeral cluster).

```bash
pnpm install
pnpm demo:linearlite    # builds the engine on first run
```

One command boots everything (ephemeral Postgres, durable streams, the engine) and serves the
**LinearLite app and the pipeline explorer side by side** — write in one, watch the engine maintain
your live queries in the other:

| | URL |
|---|---|
| **LinearLite** (issue tracker) | https://localhost:8443 |
| **Pipeline explorer** | https://localhost:5443 |

The certs come from Caddy's local CA — run `caddy trust` once before first start, or just click
through the browser warning. `scripts/linearlite.sh start large` runs a 100k-issue workload; stop
everything with `scripts/linearlite.sh stop`. Other entry points: `pnpm demo` (headless
walkthrough), `pnpm demo:web` (minimal end-to-end app).

### Docker

```bash
pnpm docker:up    # Postgres + durable-streams + engine (+ extended API) — see docker/README.md
```

Point an ElectricSQL client at `http://localhost:7010/v1/shape`, or `@electric-circuits/client` at
`http://localhost:8790`.

### Using the extended client

```ts
import { createClient } from '@electric-circuits/client'
const client = createClient({ apiUrl, schema })

// a live query (materialized TanStack DB collection)
const liveQuery = await client.shape({
  table: 'issues',
  where: { col: 'project_id', in: { table: 'project_members', project: 'project_id',
           where: { col: 'user_id', op: 'eq', value: 42 } } },   // visibility subquery
})
liveQuery.currentRows(); liveQuery.subscribe(cb); await liveQuery.close()

// an ordered page + live tail (infinite scroll)
const page = await client.subset({ table: 'issues', orderBy: { col: 'created', desc: true }, limit: 50, where })

// a live aggregate
const count = await client.aggregate({ table: 'issues', fn: 'count', where })
```

Writes go to Postgres (the system of record) with ordinary SQL; the engine ingests via replication.

## Tests & benchmarks

```bash
pnpm engine:test        # Rust engine unit tests
pnpm test               # full TS suite incl. conformance (boots its own Postgres)
pnpm test:fuzz          # random-predicate fuzz vs the oracle
pnpm bench:fleet        # electric-sql/benchmarking-fleet against our /v1/shape
```

The conformance invariant, asserted end-to-end through the real stack: *for any live query and any
op stream, the client-materialized set equals a Postgres oracle's `SELECT … WHERE <predicate>`*.
`electric-conformance/` additionally runs Electric's own test suites against our `/v1/shape`.

## Layout

| Path | Lang | Responsibility |
|---|---|---|
| `apps/engine` | Rust | replication ingest, live-query/subquery/aggregation maintenance, control HTTP, `/v1/shape` |
| `apps/api` | TS | extended tRPC API (schema, writes, live queries, subsets, aggregations) |
| `apps/pipeline-viz` | TS | live pipeline explorer (developer tool) |
| `packages/protocol` | TS | shared contract: schema/predicate/envelope types + compilers |
| `packages/client` | TS | `shape()` / `subset()` / `aggregate()` + tracked lifecycles |
| `packages/oracle` / `packages/conformance` | TS | reference implementation + the conformance suite |
| `packages/bench` / `packages/loadgen` | TS | fleet-benchmark runner, memory matrix, load generator |
| `electric-conformance/` | Elixir | Electric's own tests, pointed at our adapter |
| `examples/linearlite`, `examples/web` | TS | demo apps |
| `docker/` | — | containerized stack |

Each package has its own README. The docs index — architecture, query semantics, engine internals,
memory model — lives in [docs/](docs/) (start with
[docs/getting-started.md](docs/getting-started.md)). Agent guidance for working in this repo:
**AGENTS.md**.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE), at your option.
