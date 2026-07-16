# Electric Circuits

**Electric Circuits make your app's queries live.** Write the queries your app already runs — joins,
aggregates, subqueries — and every result becomes a live primitive your code programs against: bind
it to a component, sync it into a local collection, feed it to an agent. No fetch, poll, refetch,
invalidate.

Your app writes to Postgres with ordinary SQL. A Rust engine ingests the logical-replication stream
and turns every change into **live queries** — results that stay in sync as the database changes.
Behind every one is a **circuit**: a small, fixed set of shared dataflows, one per *kind* of query
(membership, aggregation, filtering and routing), that maintains every result incrementally and
never holds a copy of your data. Add a user, a parameter, a whole new query, and nothing new gets
built — it's data flowing through a dataflow that's already there.

The engine speaks the Electric wire protocol (`GET /v1/shape`, works with the unmodified ElectricSQL
client) plus an extended API (`@electric-circuits/client`) that adds subset queries and live
aggregations.

## What is a live query?

A live query is a query over one table whose **result set is maintained for you as the database
changes**:

```sql
SELECT * FROM issues
WHERE status = 'todo' AND priority >= 3
```

In the API these are created with `client.shape()` and served at `/v1/shape` (the Electric protocol
name); conceptually we call them **live queries**.

Run that query and you first receive its current rows (the *snapshot*), then a live feed of exactly
three kinds of message, forever:

- `upsert` — a row entered the result set, or changed while inside it;
- `delete` — a row left the result set (deleted, **or updated so it no longer matches**);
- nothing — the change didn't affect this query.

That last bullet is the point. A live query is a **sync boundary**: only matching rows (and only the
columns you project) ever cross the network, and the client's local copy is always exactly the
query's result — never a cache to invalidate, never an approximation to refresh.

Live-query predicates are comparisons (`= <> < <= > >=`, `LIKE`), null tests (`IS [NOT] NULL`),
`AND/OR/NOT`, and one cross-table form: single-column subqueries,
`col [NOT] IN (SELECT proj FROM other WHERE …)`, recursively. Ordering and windowing are
deliberately *not* live-query features — they live in **subset queries** (below), so a live query's
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

Electric Circuits is built on this model. Every replicated change becomes a Z-set delta (Postgres's
`REPLICA IDENTITY FULL` supplies the old row, so updates retract precisely), and every live query,
subquery, and aggregation registers onto a **circuit** — one of a small, fixed set of shared,
always-on dataflows, one per *kind* of query (membership/visibility, aggregation, filtering and
routing), never one per query and never one per user. A new user, a new parameter, a whole new
query is data flowing through a dataflow that's already there — nothing new gets built, so a
circuit's size never grows with query or user count. (Which dataflow a query registers onto, and
how the engine routes deltas to it, is an implementation detail documented for engine developers in
`docs/ivm-engine-internals.md`.)

The engine keeps **no copy of any table**: its memory scales with the *kinds* of query your app
runs and the relationships they watch, not with table size or query count. Measured at ~50,000
distinct live queries (100,000 subscriptions): **~645 MiB total engine RSS, about 13 KiB per live
query**; and memory is flat with database size — 100× the rows moves total RSS by about 1%. See
`docs/bench/mem-reduction-log.md` and `docs/memory-model.md` §5 for the full figures, and
`docs/ARCHITECTURE.md` §6b for the circuit's in-memory state model.

## A live query as a DBSP pipeline

Take the query above and one write:

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
that row 42 must *leave* the result.

The one cross-table operator works the same way. A per-user visibility query:

```sql
SELECT * FROM issues
WHERE project_id IN (SELECT project_id FROM project_members WHERE user_id = 42)
```

runs as a two-input pipeline with a small piece of shared state — the maintained **inner set**
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
                           live query feed: upserts / deletes
```

Add user 42 to a project and every issue of that project upserts into their result; remove them and
the issues delete — driven entirely by the membership table's delta. The pipeline for any running
engine is inspectable live in the **pipeline explorer** (`apps/pipeline-viz`).

**Everything equal is de-duplicated.** Two identical live queries (same table, canonical predicate,
projection) share one maintained pipeline and one output stream, ref-counted — as do identical
subquery inner sets and identical aggregations. A thousand clients opening the same live query cost
the engine one maintenance path and one append per change.

## How your queries become live

You don't design or compile a pipeline for your app. You write the query — a filter, a visibility
subquery, a live aggregate — and it registers onto the circuit that already handles its *kind*: one
shared dataflow for membership/visibility, one for aggregation, one for filtering and routing.
Registering a new query, a new parameter value, or a new user is cheap and immediate — a key into a
structure that's already running, never a new pipeline stage. Sharing is automatic: identical live
queries (same table, canonical predicate, projection) fold onto the same maintained result.

The walkthrough of what actually happens, end to end, when your query becomes live is in
**[docs/how-queries-become-live.md](docs/how-queries-become-live.md)**. The engine-internal
routing/fallback machinery and cost model behind that — for people working on the engine itself,
not on apps — lives in **[docs/ivm-engine-internals.md](docs/ivm-engine-internals.md)**.

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
that decouples every layer (the engine is a restartable consumer in the middle); the engine holds
only per-live-query routing metadata and the shared inner sets. Backfills read just a live query's
matching rows in a `REPEATABLE READ` snapshot, fenced against the live stream by transaction
visibility.
Full design: **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**; execution strategies + cost model:
**[docs/ivm-engine-internals.md](docs/ivm-engine-internals.md)**.

## Two client surfaces

- **The Electric protocol** — `GET /v1/shape` on the engine, compatible with the ElectricSQL TS
  client and validated against Electric's own oracle/property/integration tests
  ([`electric-conformance/`](electric-conformance/README.md)).
- **The extended API** (`@electric-circuits/client`) — live queries plus the pieces the Electric API
  doesn't cover today; this surface is where the API is headed:
  - **Subset queries** — one-shot `SELECT … ORDER BY … LIMIT` pages + a shared live tail, merged
    client-side; the basis for infinite scroll / keyset pagination.
  - **Aggregations** — live scalar COUNT/SUM/AVG/MIN/MAX over a predicate, maintained as an
    incremental fold with SQL NULL semantics and retraction-correct MIN/MAX.

## Try it

Requirements: Node ≥ 22, pnpm 10, Rust stable, and (for Postgres mode/demos) PostgreSQL 16 binaries
on `PATH` (`initdb`/`pg_ctl` — the demos boot their own ephemeral cluster).

```bash
pnpm install
pnpm demo:linearlite    # the flagship demo — builds the engine on first run
```

One command boots everything (ephemeral Postgres, durable-streams, the engine) and serves the
**LinearLite app and the pipeline explorer side by side** — write in one, watch the engine maintain
your live queries live in the other:

| | URL |
|---|---|
| **LinearLite** (issue tracker) | https://localhost:8443 |
| **Pipeline explorer** | https://localhost:5443 |

The HTTPS/HTTP-2 fronts multiplex the live-query streams over one connection (past the ~6-per-origin
HTTP/1.1 cap); the certs come from Caddy's local CA, so run `caddy trust` **before** starting the
demo — once the demo is up, its own Caddy instance runs with the admin API disabled, so `caddy trust`
will fail with `connection refused`. On most dev machines the CA is already trusted from a prior
`caddy trust` run; if not, just click through the browser's certificate warning instead. Plain HTTP
is also served on `:5174` (app) and `:5180` (explorer). `DEMO_VIZ=0` skips the explorer;
`scripts/linearlite.sh start large` runs the same demo at a 100k-issue workload. Other entry points:
`pnpm demo` (headless live-query walkthrough), `pnpm demo:web` (minimal end-to-end app).

Stop the demo with `scripts/linearlite.sh stop` (or `Ctrl+C` if you ran `pnpm demo:linearlite`
directly in the foreground) — this tears down Postgres, durable-streams, the engine, and the
visualizer, and is safe to run even if nothing is up.

### The apps in this repo

Each is self-contained with its own README; every demo boots everything it needs (ephemeral
Postgres, durable-streams, the engine) in one command.

**`apps/engine` — the sync engine (Rust).** The core: ingests Postgres logical replication and
maintains every live query/subquery/aggregation incrementally; serves the control-plane HTTP API,
the Electric wire protocol (`/v1/shape`), and the `/trace` SSE feed. You rarely run it by hand —
the demos and Docker do — but standalone:

```bash
cargo build -p electric-circuits-engine
ELECTRIC_CIRCUITS_DS_URL=<durable-streams url> ELECTRIC_CIRCUITS_PG_URL=<postgres url> \
ELECTRIC_CIRCUITS_PG_TABLES='*' target/debug/electric-circuits-engine   # prints ENGINE_LISTENING <url>
```

**`apps/api` — the extended API (TS).** The tRPC façade for the extended client surface (schema,
writes, live queries, subset queries, aggregations). Run via the demos or `docker/api-server.ts`.

**`apps/pipeline-viz` — the pipeline explorer (TS).** A developer/debugging GUI that attaches to
any running engine and renders the maintained pipeline — one graph in the engine's own node
namespace, with the **live state of every node** on its card (routing-index sizes, subquery
inner-set sizes, fold values, emit counters — pushed over the `/trace` SSE stream, not polled).
Node details dump full operator state (routing indexes, aggregation multisets, inner sets), live
query contents / change logs, and paginated table browsing; live trace animation pulses every
replicated change through the graph, and live-query creation/removal highlights the new paths.
Includes live-query management (drop one / sweep all). Auto-launched by `pnpm demo:linearlite`,
or standalone:

```bash
ELECTRIC_CIRCUITS_ENGINE_URL=http://127.0.0.1:<engine-port> VIZ_PORT=5180 \
  pnpm --filter @electric-circuits/pipeline-viz dev
```

**`examples/linearlite` — the flagship demo app (TS).** A Linear-style issue tracker synced
entirely through live queries (visibility subqueries, live counts, subset pagination). The demo
launches the engine with the circuit's counts pipeline configured by default (the live
browse-header COUNT): `pnpm demo:linearlite`, or
`scripts/linearlite.sh start large` for a 100k-issue workload.

**`examples/web` — the minimal example (TS).** The smallest end-to-end app using the extended
client: `pnpm demo:web`. (`pnpm demo` runs the even smaller headless todos walkthrough.)

### Docker

```bash
pnpm docker:up    # Postgres + durable-streams + engine (+ extended API) — see docker/README.md
```

Point an ElectricSQL client at `http://localhost:7010/v1/shape`, or `@electric-circuits/client` at
`http://localhost:8790`. Prebuilt images: `ghcr.io/balegas/electric-circuits/{engine,node}`.

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
Docker-scalable clients) and the shape-memory matrix runner in `packages/bench` (memory vs live
queries × deployment size, written to `docs/bench/shape-memory-matrix.md`).

## Tests

```bash
pnpm engine:test        # Rust engine unit tests (parsers, gates, dedup, aggregation semantics)
pnpm test               # full TS suite incl. conformance (boots its own Postgres)
pnpm test:fuzz          # random-predicate fuzz vs the oracle
pnpm loop [N]           # run the fuzz loop until failure; replay with SEED=<n>
```

The conformance invariant, asserted end-to-end through the real API/streams/client: *for any live
query and any op stream, the client-materialized set equals a Postgres oracle's
`SELECT … WHERE <predicate>`* — including live replication, batched mutations, NULL three-valued
logic, and concurrent writers.

## Layout

| Path | Lang | Responsibility |
|---|---|---|
| `apps/engine` | Rust | replication ingest, live-query/subquery/aggregation maintenance, control HTTP, `/v1/shape` |
| `apps/api` | TS | extended tRPC API (schema, writes, live queries, subsets, aggregations) |
| `packages/protocol` | TS | shared contract: schema/predicate/envelope types + compilers |
| `packages/client` | TS | `shape()` / `subset()` / `aggregate()` + tracked lifecycles |
| `packages/oracle` / `packages/conformance` | TS | reference implementation + the conformance suite |
| `packages/bench` / `packages/loadgen` | TS | fleet-benchmark runner, memory matrix, load generator |
| `electric-conformance/` | Elixir | Electric's own tests, pointed at our adapter |
| `examples/linearlite`, `examples/web` | TS | demo apps |
| `docker/` | — | containerized stack |
| `apps/pipeline-viz` | TS | live pipeline explorer (developer tool) |

Each package has its own README. Agent guidance for working in this repo: **AGENTS.md**.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE), at your option.
