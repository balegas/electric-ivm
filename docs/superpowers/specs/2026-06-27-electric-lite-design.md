# electric-lite — Design Spec

**Status:** Approved (engine fork decided: Rust `dbsp` service)
**Date:** 2026-06-27
**Author:** Victor Balegas (balegas@electric-sql.com)

## 1. Goal

A minimal, Electric-style **reactive database**: a data-ingestion API and a query
engine based on [`dbsp`](https://crates.io/crates/dbsp), with storage on
[`durable-streams`](https://crates.io/crates/durable-streams). A client defines a
**shape** (a query over the schema) and receives every change to the database that
matches that query, live.

This is modelled on [ElectricSQL](https://electric-sql.com/) but deliberately
simpler. The system is called **electric-lite**. All documents are written in English.

### Non-goals (deliberate constraints)
- **No cross-table queries.** A shape is exactly *one table* + a *WHERE clause over
  that table's own columns* (Electric's shape constraint).
- Limited query expressivity. Start with column equality; grow to comparisons and
  boolean combinators. Never general SQL.
- Durability/restart hardening is deferred (see Milestone M3).

## 2. Building blocks (verified)

| Block | What it actually is | Role |
|---|---|---|
| `durable-streams` | Standalone **Rust HTTP server** (also npm/Docker). Append via `POST`, read/subscribe via `GET` with `offset`/`live`/SSE, metadata via `HEAD`. Stores stream as literal wire bytes; WAL durability; idempotent producers via `Producer-Id`/`Producer-Epoch`/`Stream-Seq`. | The log **and** the shape feed |
| `dbsp` | **Rust** crate (Feldera). Incremental dataflow: `RootCircuit`, `Stream`, `ZSet`, `InputHandle`/`OutputHandle`, operators (`filter`, …). No JS/WASM port. | Query engine (per-shape circuits) |
| `stream-db` + `@durable-streams/state` + `@tanstack/db` | **TS** client. Materialize a stream into reactive TanStack DB collections: `preload()`, `useLiveQuery()`, optimistic mutations. | Client materialization + live |
| `tRPC` | **TS** | Public write/admin API |
| `pglite` | **TS** (Postgres in WASM) | Oracle |
| `faker` | **TS** | Ingestion simulator |

## 3. Architecture

Durable Streams is **both the write-ahead log and the shape feed**. Everything is a stream.

```
                  (control plane: define schema, create/drop shape)
   client ──tRPC──►  API (TS, Node) ──HTTP──► engine (Rust, dbsp)
     │                   │                          ▲   │
     │   write(op,row)   │ append change            │   │ tail table streams
     │                   ▼                          │   │ run dbsp circuit per shape
     │            durable-streams server  ◄─────────┘   ▼ append matched deltas
     │            table/<name>  (the WAL)        shape/<id> (the feed)
     │                                                  │
     └──── stream-db + TanStack DB ◄────read/SSE────────┘  (materialize + live)
```

- **Writes** (`tRPC ingest`) append a change event `{op, pk, row}` to `table/<name>` —
  the authoritative log. API and engine are decoupled through this stream.
- **Engine** tails every `table/<name>`, feeds changes into one **dbsp circuit per
  shape** (a `filter` over a dynamic `Row` Z-set), and appends output deltas to
  `shape/<id>`. dbsp yields correct **enter / leave / update** semantics for free
  (filter of the delta Z-set: `filter(Δin) = Δout`).
- **Read handle**: `shapes.create({table, where})` returns `{shapeId, streamUrl}`. The
  client materializes it with stream-db (`preload()` + TanStack DB collection + live SSE).

### 3.1 Key decision — predicates are a JSON AST, not SQL

A shape's `where` is a restricted **JSON predicate**:

```jsonc
// leaf
{ "col": "status", "op": "eq", "value": "active" }
// later: combinators
{ "and": [ {leaf}, { "or": [ {leaf}, {leaf} ] } ] }
{ "not": {leaf} }
```

Ops: `eq` (M1); `neq lt lte gt gte` and `and/or/not` (M2). **One predicate definition,
two derivations:**
- engine compiles it to a Rust closure for `dbsp.filter`;
- oracle translates it to a SQL `WHERE`.

Single source of truth ⇒ no drift between system-under-test and oracle. Single table,
columns of that table only. No raw SQL, no injection surface, trivial to parse.

### 3.2 Data model
- **Schema**: `{ tables: { <name>: { columns: { <col>: {type: "int"|"text"|"bool"|"float"}, ... }, primaryKey: <col> } } }`.
- **Row**: dynamic record `{ [col]: Value }`. In Rust: `Row = BTreeMap<String, Value>`
  (newtype implementing dbsp's data traits). In TS: validated against the schema (Zod).
- **Change event** (the unit on every stream): `{ op: "insert"|"update"|"delete", pk, row }`.
  On a shape stream the same envelope is reused; `op` reflects enter/leave/update.

## 4. Components (monorepo: pnpm workspaces + Cargo workspace)

| Path | Lang | Responsibility |
|---|---|---|
| `apps/engine` | Rust | dbsp circuit per shape; tail table streams; append shape deltas; HTTP control plane (`POST /schema`, `POST /shapes`, `DELETE /shapes/:id`, `GET /shapes/:id`). Swappable behind HTTP. |
| `apps/api` | TS | tRPC server: `schema.define`, `ingest.write` (→ table stream), `shapes.create/get` (→ engine). Derives Zod validators per table from schema. |
| `packages/client` | TS | `createClient()`: tRPC client + `client.shape()` → materialized TanStack DB handle via stream-db; `client.write()`. |
| `packages/oracle` | TS | pglite: schema→DDL, change events→DML, predicate→SQL `SELECT`. |
| `packages/conformance` | TS | faker simulator + seeded op-generator + vitest runner. Boots everything ephemeral, drives both systems, diffs results. |
| `packages/protocol` | TS | Shared types: schema, predicate AST, change event, handle. Rust mirrors these (serde). |

The predicate AST + change-event envelope are the cross-language contract; the Rust
engine and TS packages each (de)serialize the same JSON shapes.

## 5. Conformance harness — the heart (built for an agent loop)

**Invariant:** for any shape and any op-sequence, the **client-materialized set equals
the pglite `SELECT … WHERE <predicate>` result-set**.

- Boots durable-streams + engine + api on random ports / temp dirs; tears down clean.
- **Snapshot test:** seeded faker generates a schema, shapes, and a random op stream →
  apply the *same* ops to electric-lite (via tRPC) and the oracle (pglite) → wait for
  convergence → assert per-shape set-equality. On failure, print the seed for replay and
  shrink toward a minimal failing trace.
- **Live-propagation test:** materialize a known-correct shape via the real client; issue
  a targeted write known to cause enter / leave / update; await the SSE update; assert the
  client's state equals the oracle. Exercised **through the real client + durable-streams**,
  never via engine internals.
- Single command `pnpm test:conformance` → clear pass/fail + failing seed. This is the loop
  an agent iterates against.

## 6. Milestones (pragmatic, start tiny)

- **M0 — Skeleton:** monorepo + boot all processes; one hardcoded schema; `insert → table
  stream → client reads one row` e2e. Proves the plumbing.
- **M1 — Equality filter (core deliverable):** `where col = value`; dbsp filter circuit;
  insert/update/delete; snapshot conformance + one live-propagation test.
- **M2 — Richer predicates:** `neq lt lte gt gte`, `and/or/not`, multiple columns/shapes;
  expand oracle + property tests.
- **M3 — Robustness:** backfill on late shape registration; engine restart idempotency
  (deterministic `Stream-Seq`); typed derived per-table API; long faker fuzz runs.

## 7. Risks to verify during build (not blockers)
1. dbsp trait bounds for dynamic `Row = BTreeMap<String,Value>` Z-set element
   (`Clone/Ord/Hash/serde` + dbsp `DBData`).
2. Exact durable-streams HTTP surface (append/read/offset/SSE framing) and
   `@durable-streams/state` / stream-db API — confirm against docs before wiring.
3. Engine restart/idempotency to shape streams — deferred to M3; M1–M2 use fresh
   streams per run.
