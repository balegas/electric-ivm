# AGENTS.md

Guidance for AI agents working in **electric-lite** — a minimal, Electric-style reactive database.
App writes to **Postgres**; a Rust **dbsp** engine turns logical-replication changes into **live
shapes**; **durable streams** is the log between them; a TanStack-DB client materializes shapes.

## Layout

| Path | What |
|---|---|
| `apps/engine` | Rust query engine (dbsp). Postgres-backed: logical replication in, rows read back for backfill. Key files: `engine.rs` (tailer + shape routing), `pg.rs` (backfill + subset query-back), `http.rs` (HTTP API), `predicate.rs`/`sql.rs` (WHERE AST → match + SQL pushdown), `subquery.rs` (cross-table subquery registry: shared inner-set nodes + move-queries), `replication.rs`. |
| `apps/api` | tRPC API (`router.ts`) over the engine + durable-streams (`core.ts`). The public read/write/shape/subset surface. |
| `packages/protocol` | Shared types + the change-event envelope (`types.ts`, `envelope.ts`, `predicate.ts`, `sql.ts`). |
| `packages/client` | Browser client: `shape()` (materialized), `query()`/`subset()` (subset queries — see `subset.ts`). |
| `packages/conformance` | The real test suite — engine vs an oracle, incl. live Postgres replication, fuzz, nulls, concurrency. |
| `packages/oracle` | Reference implementation shapes are checked against. |
| `packages/bench` | Throughput/memory benchmarks. |
| `packages/loadgen` | Headless load generator: state-machine "users" drive the client for reads + Postgres for writes; boots/teardowns infra; samples engine RSS/CPU + PG/ds disk vs workload size (`src/run.ts`, `src/user.ts`, `src/infra.ts`, Docker in `docker/`). |
| `apps/pipeline-viz` | Web GUI attached to a running engine — the shape/dbsp pipeline explorer. Two views (logical topology + raw dbsp operator circuit), node-click details incl. live indexes. Reads `GET /graph` + `GET /graph/node`. |
| `examples/linearlite` | The flagship demo (LinearLite on electric-lite). `start.ts` boots the whole stack. |
| `scripts/linearlite.sh` | All-inclusive control script: `start <size>` / `stop` / `status` for the demo at a chosen workload size (see **Running the stack**). |

## Docs (read these before designing)

- `README.md` — the three-layer model + shape semantics.
- `docs/ivm-engine-internals.md` — the **as-built** engine model (routing + stateless filters +
  subquery registry) and the analytical cost model. Prefer this over `ARCHITECTURE.md` §4–§10.
- `docs/shapes-and-subqueries-guide.md` — user/integrator guide: defining shapes/subqueries, setup, sizing.
- `docs/ARCHITECTURE.md` — system architecture (note: §4–§5/§8/§10 describe the superseded
  `table_state`/dbsp-circuit model, corrected in place by the blockquotes at its top).
- `docs/deployment-postgres.md` — Postgres-as-source-of-record (slot, REPLICA IDENTITY, backfill).
- `docs/superpowers/specs/` — design records, one per feature. Most relevant:
  - `2026-06-29-subset-queries-design.md` — **shapes vs subset queries** (the current pagination model).
  - `2026-06-29-postgres-logical-replication.md` — replication + snapshot↔live handoff.
  - `2026-06-29-reduce-engine-memory-design.md` — virtualization, projection, routing.
  - `2026-06-27-electric-lite-decisions.md` / `-design.md` — foundational decisions.

New designs go in `docs/superpowers/specs/YYYY-MM-DD-<topic>-design.md` and get committed.

## Build & test

```bash
pnpm engine:build          # cargo build -p electric-lite-engine
pnpm engine:test           # cargo test  -p electric-lite-engine   (36 tests, fast)
pnpm test                  # vitest run — full suite incl. conformance (114 tests, ~40s; spins up its own PG)
pnpm test:conformance      # just the conformance package
pnpm test:fuzz             # random-predicate fuzz vs oracle
pnpm demo:linearlite       # boot the LinearLite demo (ephemeral PG + engine + ds + api + vite + caddy)
```

**There is no `tsc` typecheck gate** — `@types/node` isn't installed and CI uses vitest (esbuild,
transpile-only). To check TS: run `pnpm test`, transpile-load a module with `npx tsx -e "import(...)"`,
or have the running Vite server transform it (`curl localhost:5174/src/<file>` → 500 on error). Always
run `pnpm engine:test` + `pnpm test` before claiming done.

## Running the stack (workload sizes, explorer, load testing)

**`scripts/linearlite.sh` — all-inclusive demo at a chosen workload size.** One command boots the whole
stack (ephemeral PG + logical replication, durable-streams, engine, API, the LinearLite web UI, **and**
the shape/dbsp pipeline explorer) and prints both URLs; another tears it all down cleanly (graceful
`start.ts` shutdown that drops the slot + stops PG, then force-cleans remnants).

```bash
scripts/linearlite.sh start <size>   # size = small | medium | large | xlarge | <number-of-issues>
scripts/linearlite.sh stop
scripts/linearlite.sh status
```

The **workload size is a number of issues**; the seeded **users and projects scale with it** (users
~√issues, projects ~users/2.5) — `small`=1k→8 users, `medium`=20k→35, `large`=100k→79, `xlarge`=500k→177.
It sets `DEMO_SEED_COUNT` / `DEMO_USERS` / `DEMO_PROJECTS`; `start.ts` generates the roster (classic
names first, faker beyond) and the app reads it back from the DB, so the "Viewing as" switcher adapts.
The default `pnpm demo:linearlite` (no `DEMO_USERS`) is unchanged (6 users / classic memberships).
It manages **one instance at a time** (its `stop` matches by process pattern + ports). Ports:
`DEMO_HTTPS_PORT` (web UI, 8443), `DEMO_VIZ_PORT` (explorer, 5180), `DEMO_VIZ=0` to skip the explorer.

**Demo env knobs** (`examples/linearlite/start.ts`): `DEMO_SEED_COUNT` (issues), `DEMO_USERS`,
`DEMO_PROJECTS`, `DEMO_HTTPS`/`DEMO_HTTPS_PORT` (Caddy), `DEMO_VIZ`/`DEMO_VIZ_PORT` (pipeline explorer).

**`packages/loadgen` — observe memory/CPU/disk vs workload.** Headless state-machine users drive the
real client for reads (subset feeds + COUNT aggregation + board subquery-shapes) and write to Postgres.
Boots/tears down its own infra, samples metrics → CSV + summary.

```bash
USERS=100 SEED_ISSUES=20000 DURATION_S=90 pnpm --filter @electric-lite/loadgen loadgen  # single run
SWEEP_USERS=10,50,150 pnpm --filter @electric-lite/loadgen sweep                        # comparison table
```

Modes: `all` (self-contained), `infra` (boot + keep + sample; for docker clients), `client` (connect to
an existing infra + run users). Docker scales client "nodes" (`packages/loadgen/docker/`). Connection
budget ≈ `USERS × FEEDS_PER_USER`; it checks `ulimit -n`. Use `DS_MEMORY=1` for high concurrency (the
file-backed durable-streams fsync-per-append is the bottleneck at scale, not the engine). See
`packages/loadgen/README.md`.

## Conventions

- **Postgres is the system of record.** The engine holds *no* table copy — it backfills via a
  `REPEATABLE READ` snapshot and tails logical replication. Snapshot↔live dedup is by **commit LSN**:
  skip changes with `commit_lsn < seed_lsn` (strict `<`). Match this when adding read paths.
- **Shapes vs subset queries** (keep them distinct in any new API):
  - *Shape* = materialized + live (backfill stored as a durable stream; whole `WHERE` set maintained).
  - *Subset query* = ephemeral, non-materialized (one-shot PG `SELECT … ORDER BY … LIMIT … OFFSET`,
    plus an optional **changes-only feed** — a shape created with no backfill that forwards only future
    matching deltas). Ranges/limits live *only* here. This is how range fanout is avoided: ranges are
    never live-tailed, so one change is matched against one base predicate, never split across ranges.
- Predicates are a JSON AST: `Leaf{col,op,value}` / `And` / `Or` / `Not`; ops `eq neq lt lte gt gte`.
  One table + WHERE over its own columns, **plus single-column subqueries**
  `{col, in:{table,project,where?}, negated?}` = `col [NOT] IN (SELECT project FROM table WHERE …)`
  (recursive; no other join form). Subquery shapes are maintained by a cross-table registry
  (`apps/engine/src/subquery.rs`): each distinct inner subquery is one **shared node** (a value→
  contributor-pk multiset, keyed by a canonical signature, refcounted, `GET /subqueries`); an inner-set
  flip query-backs the affected outer rows. **Outer membership is emitted absolutely** (upsert if the
  new row matches else delete-by-pk), never delta-based — per-table tailers process tables out of global
  commit order, so a delta-based emit misses move-outs. See
  `docs/superpowers/specs/2026-06-29-subqueries-design.md`.
- Commit messages end with the two trailers from the harness (`Co-Authored-By:` Claude + a
  `Claude-Session:` link). Branch before committing if on the default branch.

## Lessons learned (hard-won — don't relearn these)

- **Read raw stream envelopes, not stream-db's reconciled view, when you need every delta.** A subset's
  live feed must apply *move-outs* (a row whose update leaves the predicate → engine emits a `delete`
  for the *old* row). stream-db's collection no-ops a delete for a key it never inserted, so
  `subscribeChanges` silently drops it and the row sticks. The client reads `@durable-streams/client`
  `stream().jsonStream()` (raw `StreamEnvelope`s) and applies membership itself. (`packages/client/subset.ts`.)
- **The engine computes move-in/move-out from the WAL alone** (old+new rows via `REPLICA IDENTITY FULL`),
  no Postgres round-trip — same as Electric. A standalone predicate filter over `[(old,-1),(new,+1)]`
  deltas yields the right insert/delete.
- **Subqueries: emit outer membership *absolutely*, not as a delta.** A subquery shape's outer table and
  its inner tables flow through *independent per-table tailers*, so an inner-set node can be updated
  *before* an earlier-committed outer change. A delta-based "delete only if the *old* row matched" then
  misses move-outs (the inner set is already ahead) and a stale backfill row sticks. Emit each touched
  pk's *current* membership — `upsert` if the new row matches else `delete` by pk (idempotent) — and let
  the flip-driven move-query reconcile values the inner set hasn't caught up to yet. This converges
  regardless of cross-table order, so Electric's LSN-buffering/tag protocol isn't needed.
  (`apps/engine/src/subquery.rs::emit_shape_delta`.) Symptom when wrong: convergence holds op-by-op but
  fails on *batched* mutations (the interleaving that exposes the race only happens under load).
- **A `changes_only` feed must use `seed_lsn = 0`** (no backfill ⇒ forward all future matches) and the
  client reads its fresh stream from offset `-1` (= from feed creation). Create the feed *before* the
  query-back so the live tail can't miss a delta in the gap; overlap is reconciled idempotently by pk.
- **The demo boots an _ephemeral_ Postgres each run** (`mkdtemp`), seeded by `DEMO_SEED_COUNT` (default
  512). Data does not persist between runs; don't expect a previous run's rows. **Kill stale demos before
  restarting** — a leftover `tsx start.ts` + `caddy` from a prior run keeps `:8443`/`:5174` and serves
  OLD code (an engine without new tables, an API zod without new predicate branches), which reads as a
  mysterious "unknown table" / "invalid_union" mismatch vs source. `pkill -f electric-lite-engine`,
  `pkill -f "tsx start.ts"`, `pkill -f caddy` first.
- **Shape rows stringify the primary key.** A materialized shape's row arrives with its pk column coerced
  to a *string* (TanStack DB collection keys are strings), while non-pk int columns and the subset
  query-back path stay numbers. Cross-id joins (e.g. `issue.project_id` number vs `projects.id` string)
  silently miss — normalize reference-data ids to numbers when reading from shapes
  (`examples/linearlite/src/lib/CurrentUser.tsx`).
- **A subset whose predicate folds in the live UI filters re-creates the engine feed on every filter
  click** (`useSubset` keys on the predicate JSON → teardown + query-back = a visible delay). For
  permissioned/faceted lists prefer **per-facet feeds reused across filter changes** + a client merge:
  LinearLite's browse list mounts one `project_id = P` subset per member project (identical predicate
  across users ⇒ shared engine family; bounded memory at 100k issues) and merges/filters on the client,
  so switching project/status is instant. The visibility *subquery* stays the declarative form for the
  bounded Board/Search views.
- **Vite binds IPv6 `[::1]:5174` only.** `http://localhost:5174` can fail to resolve to it; prefer the
  **`https://localhost:8443`** Caddy proxy (HTTP/2 — also dodges the browser's ~6-connection HTTP/1.1
  cap that freezes multi-stream apps). `DEMO_HTTPS=0` disables the proxy. Caddy's local CA is trusted.
- **Reverting code ≠ reverting docs.** When you revert a feature, realign README/specs in the same pass.
  (The README's old orderBy/limit-as-a-shape-knob paragraph was one such casualty — now fixed.)
- **Verify against the live stack, not just types.** A headless `tsx` script driving the real
  `client.subset()` against a running demo caught behavior the (absent) typechecker never could.
- **Under load the durable-streams *test server* is the ceiling, not the engine.** In loadgen runs the
  engine stayed ~30–48 MB RSS / <1 core even at ~180 concurrent users, while the single-process ds
  server saturated: file-backed mode fsyncs every append (throttles shape creation → the ramp crawls),
  and even in-memory an aggressive ramp burst gives `ECONNRESET` on appends. Mitigate with `DS_MEMORY=1`
  + a staggered ramp; the real fix for scale is a production durable-streams backend (matches
  `ARCHITECTURE.md §9`: storage-bound, not engine-bound). Open finding: under heavy subset/board
  open-close churn, engine shape count grows over time at a *fixed* user count (drops lag creates —
  likely the shared changes-only feed refcount or client `subset.close → shapes.delete`); memory impact
  is tiny (~0.8 KB/shape) but it's a real leak worth fixing.
- **One ephemeral demo at a time — teardown is pattern-based.** `scripts/linearlite.sh stop` and the
  manual `pkill -f "start.ts"` / `pgrep -f el-linearlite-pg` cleanups match *all* demos. If two are
  running (e.g. a load test beside a live demo), scope kills precisely: identify a demo's `start.ts` by
  the parent of its Caddy (`ps -o ppid= -p $(lsof -ti :<httpsPort>)`) or its `DEMO_HTTPS_PORT` env, and
  `SIGTERM` that pid so its own `shutdown()` drops the slot + `pg_ctl -m immediate` stops PG. A
  SIGKILL/premature force-kill mid-shutdown leaks the ephemeral Postgres (postmaster ignores plain
  SIGTERM while a client is attached).
