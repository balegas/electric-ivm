# Reduce engine memory: Postgres-backed shapes (no in-engine table copies)

Design record — 2026-06-29. Status: **proposed (awaiting review)**.

## Context & problem

The engine currently holds table *data* in memory in one place: **equality-template family circuits**.
Each family (`apps/engine/src/family.rs`) keeps a dbsp join whose data trace is a **full copy of the
table**, indexed by the template's key columns. Memory is therefore `O(#templates × table)` — the §8
amplification in `ARCHITECTURE.md`. That trace exists for exactly one reason: to **backfill a new shape
that joins the family** via the incremental join (confirmed by `family.rs:178-185`). Live routing of a
change to the shapes watching its key does *not* need the historical trace — only the params map
(key → shapes) does.

Separately, **backfill reads the whole table every time**. `pg::backfill` runs
`SELECT to_jsonb(t) FROM tbl t` (no `WHERE`) for every standalone shape and every new family
(`engine.rs:384,401`), then filters in-engine and discards non-matching rows. This is wasted read /
transfer / CPU, and for families it is also what seeds the full-table trace.

Standalone shapes already hold **no** table data — they filter the change delta directly
(`eval_standalone`) and their backfill rows are released after emission. So the steady-state memory
target is the family trace; the whole-table read is a separate (transient) cost.

## Goal & non-goals

**Goal.** Stop holding table data in the engine. Lean on Postgres as the state store: read only what a
shape needs at backfill time, and route live changes by key without a resident table copy. Reduce
steady-state engine memory from `O(#templates × table)` to `~O(#shapes)` (routing metadata only).

**Non-goals.** No change to the shape model (one table + `WHERE`), the predicate language, the
durable-streams transport, the client, or the commit-LSN reconciliation. Output envelopes are
byte-for-byte unchanged (conformance must stay green). Not addressing windowed/infinite-scroll sync
(separate effort), though this work is a prerequisite for it.

## Design

Two independently-shippable phases. After both, the engine is a uniform **stateless delta router
backed by Postgres for backfill** — it holds per-shape metadata (predicate or key, `seed_lsn`, stream
path), never table rows.

### Shared enabler: a Rust predicate→SQL compiler

`packages/protocol/src/sql.ts` already has `predicateToSql`/`shapeSelectSql` (parameterized,
three-valued-NULL-correct, used by the oracle). The engine needs a **Rust** equivalent that turns a
shape's `where` AST into a parameterized `WHERE` fragment.

- Unit: `apps/engine/src/sql.rs` — `predicate_to_sql(&Predicate) -> (String /* WHERE */, Vec<Value> /* params */)`.
- Generated from the **JSON predicate AST** (which carries column *names*), not `CompiledPredicate`
  (which carries indices). The engine already receives the `where` AST at shape creation.
- Must match the engine's `matches()` three-valued semantics exactly — same truth tables as the TS
  `predicateToSql`. Parity is guarded by the existing conformance fuzz loop (engine result vs oracle
  `SELECT … WHERE`), plus direct Rust unit tests covering each op + NULL.

### Phase 1 — Predicate-pushdown backfill (standalone shapes)

`pg::backfill(client, ts)` → `pg::backfill(client, ts, where: Option<&Predicate>)`, emitting
`SELECT to_jsonb(t) FROM tbl t WHERE <predicate_to_sql>` when a predicate is given.

- **Standalone path** (`engine.rs:401`) passes the shape's predicate → reads only matching rows; the
  in-engine `pred.matches` filter becomes a redundant safety net (kept; cheap on the already-small set).
- **Family path** (`engine.rs:384`) passes `None` in Phase 1 — the trace still needs the whole table
  because members can be on any key constant. (Phase 2 removes this.)
- **Wins:** satisfies "avoid querying the entire table every time" for standalone shapes — cuts the
  read, the PG→engine transfer, and the discarded-row CPU. Steady-state memory is unchanged here (the
  win there is Phase 2).
- **Ships independently:** conformance green (output identical), measurable read reduction.

### Phase 2 — Drop the family trace (routing model)

Replace each equality-template family's full-table dbsp trace with a **routing index** + **per-shape
Postgres backfill**.

- **Routing index** (per template key-column set): `key_tuple → { shape_id → (stream_path, seed_lsn) }`.
  Size is `O(#shapes)` entries — **no table rows**.
- **Per-shape backfill on join:** `SELECT to_jsonb(t) FROM tbl t WHERE <key_col> = <const> …` (the
  template's equality constants) → emit upserts. Each shape captures its **own** `seed_lsn` at its
  backfill snapshot (per-shape now, not per-family).
- **Live routing:** for a change with old/new rows (REPLICA IDENTITY FULL gives both), compute
  `old_key`/`new_key` over the template columns. Since an equality-template predicate matches a row iff
  its key equals the shape's constants, key membership *is* shape membership:
  - insert → upsert to shapes on `new_key`;
  - delete → delete from shapes on `old_key`;
  - update, `old_key == new_key` → upsert (update) to shapes on that key;
  - update, `old_key != new_key` → delete from shapes on `old_key`, upsert to shapes on `new_key`.
  Per matching shape, skip if `commit_lsn < shape.seed_lsn` (the existing reconciliation, now per-shape).
- **Consequence — the dbsp family circuit is removed.** `FamilyActor` (and its per-template OS thread,
  §5, and the per-family delta clone, §8) go away. Standalone shapes are already non-dbsp. So after
  Phase 2 **neither shape path uses dbsp**, and the engine holds zero table copies. This changes the
  headline "one dbsp circuit per equality template" architecture. *(Decision flagged for review — see
  Open questions.)*
- **Memory:** `O(#templates × table)` → `~O(#shapes)` routing entries + per-shape metadata.
- **Ships after Phase 1:** conformance green (output identical), measurable RSS drop.

## Correctness

- **Consistency under concurrent writes** is unchanged in mechanism: each per-shape backfill takes a
  `REPEATABLE READ` snapshot and records `seed_lsn = pg_current_wal_lsn()`; the ingestor stamps each
  change with its COMMIT LSN; the router skips `commit_lsn < seed_lsn`. Guarded by
  `conformance-concurrency.test.ts`. (Per-shape `seed_lsn` is strictly finer-grained than the old
  per-family one — each shape's boundary matches its own snapshot.)
- **Cross-key updates** are handled by routing old-key vs new-key (above), using the envelope's
  old+new — exactly how standalone shapes already handle moves across the shape boundary.
- **NULL / predicate parity** is the Phase-1 risk surface (general predicate → SQL). Equality templates
  are non-null by construction, so Phase-2 family WHERE is trivial; the fuzz loop + Rust unit tests
  guard the general compiler.

## Memory & Postgres-load trade-off

| | Before | After |
|---|---|---|
| Engine memory (table data) | `O(#templates × table)` (family traces) | **0** table rows; `~O(#shapes)` routing/metadata |
| Backfill read | whole table per standalone shape & per new family | only matching rows (standalone) / only key-matching rows (family member) |
| PG queries at shape creation | 1 full read per template (members after the first are free from the trace) | 1 indexed read per shape |
| PG load on the live hot path | unchanged (engine consumes the replication stream) | unchanged |

Net: memory ↓↓, shape-creation PG queries ↑ (indexed, cheap), hot-path PG load flat. This is the
explicit trade the goal asks for ("data could live on postgres side").

## Component boundaries (units)

1. `apps/engine/src/sql.rs` — `predicate_to_sql` (+ a key-equality helper). Pure, unit-tested, no I/O.
2. `apps/engine/src/pg.rs` — `backfill` gains an optional `where`/key filter; otherwise unchanged.
3. `apps/engine/src/engine.rs` — standalone path passes its predicate (Phase 1). Phase 2 replaces the
   `families: HashMap<Vec<usize>, Family>` (dbsp) with a key router; `process_envelope` routes
   equality shapes by key and applies per-shape `seed_lsn`.
4. `apps/engine/src/family.rs` — **removed** in Phase 2 (along with the dbsp dependency on the shape
   path, if nothing else uses it).

## Testing

- Rust unit tests for `predicate_to_sql` (every op, and/or/not, NULL three-valued cases) and the
  key-equality SQL.
- Conformance suite stays green unchanged (output is identical) across both phases — the primary
  safety net. Fuzz loop guards predicate→SQL parity.
- A new assertion that the engine holds **no table rows** (e.g. a metrics/topology field for routing
  entry count and a check that family traces are gone).
- Rerun `packages/bench/src/memtest.ts` (the §10 8-template / ~130k-row workload) before/after to
  quantify the RSS drop (expected: from ~400MB resident table-trace memory toward routing-only).
  Folds into the already-parked benchmark task (#25).

## Phasing / shippability

- **Phase 1** ships alone: Rust predicate→SQL + standalone pushdown. Conformance green; read reduction.
- **Phase 2** ships next: routing model + family-trace removal. Conformance green; RSS drop.

## Open questions / decisions

1. **Remove the dbsp family circuit (Phase 2)?** This is the core decision — the engine would hold no
   table data and no longer use dbsp on the shape path. It is entailed by "drop the family trace," but
   it changes the headline architecture, so confirm before implementing Phase 2. *(Phase 1 is
   independent of this and can proceed regardless.)*
2. **Standalone O(K) per write** is unchanged by this work (each standalone predicate is still tested
   per change). Out of scope here; the `ARCHITECTURE.md` §9 predicate-indexing idea would address it
   and composes cleanly with the routing index.
3. **dbsp retained anywhere?** If no other path uses dbsp after Phase 2, the dependency can be dropped
   entirely; if we anticipate stateful operators (aggregations, top-K for windowing), keep it as a
   library. Decide at Phase 2.
