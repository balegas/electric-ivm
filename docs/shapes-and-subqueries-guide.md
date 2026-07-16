# Guide: shapes and subqueries

Audience: people integrating against electric-circuits — defining shapes and subqueries, wiring the
engine to Postgres, and sizing a deployment. For how the engine works internally and the full
analytical cost model, see the companion `docs/ivm-engine-internals.md`.

---

## 1. What a shape is

A **shape** is a live, filtered view of one table:

> **one table + an optional `WHERE` over that table's own columns + an optional `columns`
> projection.**

You declare it once; the engine keeps its result set live as Postgres changes, delivering
incremental `upsert`/`delete` updates to a per-shape feed. There are **no general joins** — the
one cross-table form is a **single-column subquery** (`col [NOT] IN (SELECT …)`, §4).

Two things a shape's predicate is *not*:

- It has no `ORDER BY` / `LIMIT`. Ordering and windowing are either a **client-side live query**
  (presentation) or a **subset query** (pagination) — see §6.
- It has no text search. `LIKE`/`ilike` runs client-side over the already-synced set.

### Two-level querying

Keep these two layers distinct — it's what lets you sync a bounded set yet present it flexibly:

| layer | runs where | decides |
|---|---|---|
| **Shape predicate** | engine (server) | *what crosses the network* — the sync boundary |
| **Live query** (`@tanstack/react-db` `useLiveQuery`) | client | *how the synced set is presented* — ordering, text search, finer filtering |

The example apps filter by status/priority/id in the **shape**, and order by date and search by
text in the **live query** — no re-sync when you type in a search box.

---

## 2. Setting up the engine against Postgres

Postgres is the system of record; the engine observes it via logical replication and backfills
via snapshot reads. (The engine can also run without Postgres — writes go through the tRPC
`ingest.write` API — which is how the headless `pnpm demo` runs. The shape/subquery model is
identical either way.)

### Postgres prerequisites

- **PostgreSQL 16** with `wal_level = logical`.
- Each replicated table needs `REPLICA IDENTITY FULL` so updates/deletes carry the **old** row
  (the engine sets this during setup; see `docs/deployment-postgres.md`).
- Each replicated table needs a **primary key** (single or composite — composite keys are
  supported and used by Electric's `*_tags` tables).

### Engine configuration (environment)

| env var | meaning |
|---|---|
| `ELECTRIC_CIRCUITS_PG_URL` | Postgres connection string. Its presence selects Postgres mode. |
| `ELECTRIC_CIRCUITS_PG_TABLES` | comma-separated table list, or `*`/empty to **introspect every public table that has a primary key** (skipping the engine's `__el_sync` bookkeeping table). |
| `ELECTRIC_CIRCUITS_PG_SLOT` | replication slot name (default `electric_circuits`; the slot uses the `pgoutput` plugin). |
| `ELECTRIC_CIRCUITS_PG_POLL_MS` | slot poll interval. |

On boot the engine introspects the configured tables (columns, types, primary key — composite
keys ordered by index position), sets `REPLICA IDENTITY FULL`, creates the replication slot,
ensures the `changes` durable stream, and starts the ingestor.

In production you run three processes — the `durable-streams-server`, the engine binary, and the
API server — pointed at your Postgres, and point the client at the API URL. The demos colocate
them on ephemeral ports for convenience.

---

## 3. Defining shapes

### Predicate grammar

A predicate is a JSON AST (not SQL):

- **Leaf:** `{ "col": "<name>", "op": "<op>", "value": <literal> }`, where `op` ∈
  `eq neq lt lte gt gte`.
- **Null test:** `{ "col": "<name>", "isNull": true|false }` — `col IS NULL` / `col IS NOT NULL`.
  The one predicate that is TRUE on a NULL cell (two-valued, so it composes soundly under `not`).
- **Boolean:** `{ "and": [ … ] }`, `{ "or": [ … ] }`, `{ "not": <pred> }`.
- **Subquery:** `{ "col": "<name>", "in": { "table": …, "project": …, "where": … }, "negated": <bool> }`
  (§4).

NULL follows SQL three-valued logic — a comparison against NULL is UNKNOWN, and only TRUE rows
are included. This matches Postgres exactly (the conformance suite asserts the engine and a
Postgres oracle always agree).

### Via the client (tRPC + stream-db)

```ts
import { createClient } from '@electric-circuits/client'

const client = createClient({ apiUrl, schema })

// one table + a WHERE over its columns
const shape = await client.shape({
  table: 'todos',
  where: { and: [
    { col: 'done',     op: 'eq',  value: false },
    { col: 'priority', op: 'gte', value: 3 },
  ] },
})

shape.currentRows()                              // Row[] — current matching set
const off = shape.subscribe((changes) => { /* insert/update/delete batches */ })
```

`client.shape()` creates the engine-side shape and returns a **TanStack DB collection** kept
live by a stream-db reader on the shape's feed; it re-renders on every delta.

### Via the Electric `/v1/shape` HTTP protocol

The engine also speaks Electric's wire protocol (`apps/engine/src/electric.rs`), so existing
Electric clients/tools work:

- **Snapshot:** `GET /v1/shape?table=todos&where=<SQL WHERE>` (or `offset=-1`) → the current rows
  as `insert` messages + an `up-to-date` control message. Response carries `electric-handle` and
  `electric-offset` headers. (Schema-qualified names like `public.todos` are accepted.)
- **Live:** `GET /v1/shape?...&handle=<h>&offset=<o>&live=true` long-polls for `insert`/`update`/
  `delete` from that offset. An unknown handle returns `must-refetch`.

Here the `where` is a **SQL string** (`status = 'active' AND priority > 2`, `BETWEEN`, `IN (…)`,
`IN (SELECT …)`, `NOT IN`), parsed by the engine's WHERE parser.

### The `columns` projection

Add `columns: ['id', 'title', 'status']` to sync only the columns a view needs (the primary key
is always included). This cuts both the backfill working set and the synced payload — e.g. a
browse list that never renders a large `description` should drop it. It affects *what is synced*,
not *what is matched*.

---

## 4. Subqueries

The single cross-table form: a column is `IN` (or `NOT IN`) the result of a single-column
subquery, which may itself be nested.

```jsonc
{ "col": "project_id",
  "in": { "table": "project_members",
          "project": "project_id",
          "where": { "col": "user_id", "op": "eq", "value": "u" } },
  "negated": false }
```

Read as: *issues whose `project_id` is one of the `project_id`s this user is a member of.*

- **`NOT IN`** is `"negated": true` (SQL NULL semantics apply — if the inner set contains a NULL,
  `NOT IN` is UNKNOWN for every row, as in Postgres).
- **Nesting:** the inner `where` may itself contain an `in` leaf, recursively (the test suite
  goes 3–4 levels deep).
- **Sharing is automatic:** two shapes with an identical inner subquery share **one** maintained
  inner-set node (the engine dedupes by a canonical signature). You don't configure this; it's
  why per-user fleets stay cheap (§7).

### Supported vs out of scope

| supported | out of scope |
|---|---|
| `col IN (SELECT proj FROM t WHERE …)` | general joins |
| `col NOT IN (…)` | `EXISTS` / `= (SELECT …)` / `< ANY` |
| nested / multi-level subqueries | correlated subqueries |
| "tag" subqueries through composite-PK `*_tags` side tables | composite-key `(a,b) IN (…)` |

If you need something out of scope, model it as a subquery chain through a side table, push the
filter into the inner `where`, or do it as a client-side live query over a broader synced set.

---

## 5. Practical examples

### Active high-priority todos (`pnpm demo`)

```ts
const shape = await client.shape({
  table: 'todos',
  where: { and: [
    { col: 'done',     op: 'eq',  value: false },
    { col: 'priority', op: 'gte', value: 3 },
  ] },
})
```

Rows enter and leave live as todos are completed, re-prioritised, and deleted. This is a
**standalone** shape (it has a range leaf), so the engine keeps no state for it — it filters the
change stream directly.

### Per-user visibility (`pnpm demo:linearlite`)

The visibility subquery from §4 makes each user see only issues in their projects. It is a
**subquery** shape: a tiny shared node holds the user's membership rows; when membership changes,
the affected issues move in/out of the shape automatically. Verified in-browser at 100k issues.

### Tenant / equality filters

```ts
where: { and: [ { col: 'tenant', op: 'eq', value: 7 },
                { col: 'region', op: 'eq', value: 'eu' } ] }
```

This is an **equality template**. All shapes with the same key columns (`tenant`, `region`),
whatever the constants, share **one** key-routing index — so thousands of per-tenant shapes cost
a handful of routers plus tiny per-shape metadata.

### Pagination / infinite scroll

Use a **subset query** (not a shape) for ordered pages: `orderBy` + `limit` (+ `offset`, or a
keyset cursor folded into the `where`: `col < lastSeen OR (col = lastSeen AND id < lastId)`).
Each page is a bounded range query — the engine never holds a stateful top-N. The example apps
pair this with a virtualized render layer so a 20k-row view stays a few dozen DOM nodes.

---

## 6. What's cheap, what's expensive

The full analysis is in `docs/ivm-engine-internals.md` §4; the practical summary:

**Cheap (do freely):**

- **Many shapes.** Per-shape registration is ~0.8 KiB and constant regardless of table size
  (the engine keeps no table copy; baseline RSS ~19 MiB at 1k or 100k rows).
- **Equality/tenant shapes.** They collapse onto a few shared routers — flat in shape count.
- **Per-user visibility subqueries.** Each user's node holds only that user's membership rows
  (e.g. ~6), not any issues; identical subqueries are shared. 1,000 users ≈ +8 MiB.
- **`changes-only` and subset feeds.** They skip backfill entirely — registration + live deltas
  only.

**The thing to budget for:**

- **Concurrent large materialized backfills.** A materialized shape's *initial* backfill working
  set is ~2 KiB per visible row (transient, released after sync). Budget memory by the **peak
  concurrent backfill working set** (visible-rows-per-shape × 2 KiB, summed over shapes
  backfilling at once) — *not* by total shape count or total rows. Narrow it with the `columns`
  projection, or avoid it with `changes-only`/subset.

**Watch as you scale:**

- **Many distinct *range* shapes on one table.** Standalone shapes are tested on every change to
  that table (`O(K)` per change). Cheap per eval, but it grows with shape count on the live path
  — the one term that isn't shared. Equality and subquery shapes don't have this property.

To read retained state directly (independent of allocator noise), scrape the OTel gauges:
`engine_shapes`, `engine_family_circuits`, `engine_subquery_nodes`,
`engine_subquery_contributors`, `engine_subquery_edges` (`GET /memory`, or
`/metrics/prometheus`).

---

## 7. Quick reference

| you want | use | notes |
|---|---|---|
| a live filtered view of one table | **shape** with `where` | incremental upsert/delete feed |
| only some columns synced | shape `columns` | pk always included |
| cross-table membership | **subquery** `col IN (SELECT …)` | single column; nestable; auto-shared |
| exclusion | subquery `negated: true` | SQL `NOT IN` NULL semantics |
| an ordered page / infinite scroll | **subset query** (`orderBy`+`limit`) | not a shape; no top-N state |
| a live count / sum / avg / min / max | **aggregation** (`client.aggregate({ table, fn, col?, where })`) | one maintained fold shared by all subscribers; SQL NULL semantics; extended API only |
| ordering / text search of a synced set | **client live query** | no re-sync |
| Electric-compatible HTTP client | `GET /v1/shape` | snapshot + `live=true` long-poll |

Identical shapes are **de-duplicated end to end**: two `shape()`/`subset()`/`aggregate()` calls with
the same definition (predicate order doesn't matter) share one maintained stream on the engine,
ref-counted — always `close()` what you open (close is one-shot and safe to call twice).

## 8. See also

- `docs/getting-started.md` — from-zero setup against a new database, with bare-HTTP examples
  for every request in this guide (shapes, subqueries, aggregations).
- `docs/ivm-engine-internals.md` — engine internals + full analytical cost model.
- `docs/deployment-postgres.md` — running with Postgres as system of record.
- `docs/ARCHITECTURE.md` §6 — the subquery node/edge/flip model and its correctness argument.
- `examples/linearlite/README.md` — the end-to-end visibility example.
