# AGENTS.md

Guidance for AI agents working in **electric-lite** — a minimal, Electric-style reactive database.
App writes to **Postgres**; a Rust **dbsp** engine turns logical-replication changes into **live
shapes**; **durable streams** is the log between them; a TanStack-DB client materializes shapes.

## Layout

| Path | What |
|---|---|
| `apps/engine` | Rust query engine (dbsp). Postgres-backed: logical replication in, rows read back for backfill. Key files: `engine.rs` (tailer + shape routing), `pg.rs` (backfill + subset query-back), `http.rs` (HTTP API), `predicate.rs`/`sql.rs` (WHERE AST → match + SQL pushdown), `replication.rs`. |
| `apps/api` | tRPC API (`router.ts`) over the engine + durable-streams (`core.ts`). The public read/write/shape/subset surface. |
| `packages/protocol` | Shared types + the change-event envelope (`types.ts`, `envelope.ts`, `predicate.ts`, `sql.ts`). |
| `packages/client` | Browser client: `shape()` (materialized), `query()`/`subset()` (subset queries — see `subset.ts`). |
| `packages/conformance` | The real test suite — engine vs an oracle, incl. live Postgres replication, fuzz, nulls, concurrency. |
| `packages/oracle` | Reference implementation shapes are checked against. |
| `packages/bench` | Throughput/memory benchmarks. |
| `examples/linearlite` | The flagship demo (LinearLite on electric-lite). `start.ts` boots the whole stack. |

## Docs (read these before designing)

- `README.md` — the three-layer model + shape semantics. **Stale spot:** it still lists `orderBy + limit`
  as a *shape* knob; that was reverted — ranges/limits now live only in **subset queries**, never shapes.
- `docs/ARCHITECTURE.md` — system architecture.
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
pnpm engine:test           # cargo test  -p electric-lite-engine   (19 tests, fast)
pnpm test                  # vitest run — full suite incl. conformance (~30s, spins up its own PG)
pnpm test:conformance      # just the conformance package
pnpm test:fuzz             # random-predicate fuzz vs oracle
pnpm demo:linearlite       # boot the LinearLite demo (ephemeral PG + engine + ds + api + vite + caddy)
```

**There is no `tsc` typecheck gate** — `@types/node` isn't installed and CI uses vitest (esbuild,
transpile-only). To check TS: run `pnpm test`, transpile-load a module with `npx tsx -e "import(...)"`,
or have the running Vite server transform it (`curl localhost:5174/src/<file>` → 500 on error). Always
run `pnpm engine:test` + `pnpm test` before claiming done.

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
  One table + WHERE over its own columns only — no joins.
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
- **A `changes_only` feed must use `seed_lsn = 0`** (no backfill ⇒ forward all future matches) and the
  client reads its fresh stream from offset `-1` (= from feed creation). Create the feed *before* the
  query-back so the live tail can't miss a delta in the gap; overlap is reconciled idempotently by pk.
- **The demo boots an _ephemeral_ Postgres each run** (`mkdtemp`), seeded by `DEMO_SEED_COUNT` (default
  512). Data does not persist between runs; don't expect a previous run's rows.
- **Vite binds IPv6 `[::1]:5174` only.** `http://localhost:5174` can fail to resolve to it; prefer the
  **`https://localhost:8443`** Caddy proxy (HTTP/2 — also dodges the browser's ~6-connection HTTP/1.1
  cap that freezes multi-stream apps). `DEMO_HTTPS=0` disables the proxy. Caddy's local CA is trusted.
- **Reverting code ≠ reverting docs.** When you revert a feature, realign README/specs in the same pass
  (the README's orderBy/limit paragraph is the current casualty).
- **Verify against the live stack, not just types.** A headless `tsx` script driving the real
  `client.subset()` against a running demo caught behavior the (absent) typechecker never could.
