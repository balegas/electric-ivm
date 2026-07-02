# AGENTS.md

Guidance for AI agents working in **electric-ivm** — an Electric-style reactive sync engine. App
writes go to **Postgres**; a Rust engine turns logical-replication changes into **live shapes**
(incrementally maintained, fully de-duplicated); **durable streams** is the log between them. Two
client surfaces: the Electric-compatible `GET /v1/shape` (works with the ElectricSQL TS client) and
the extended `@electric-ivm/client` API (shapes + subset queries + live aggregations — the surface
the project is growing toward).

## Layout

| Path | What |
|---|---|
| `apps/engine` | Rust engine. Key files: `engine.rs` (tailers, routing, shape sharing/lifecycle, aggregations), `subquery.rs` (cross-table registry: shared inner-set nodes, flips, absolute emission), `replication.rs` (ingestor), `pg.rs` (backfill + `SnapshotGate`), `electric.rs` (`/v1/shape`), `where_sql.rs`/`sql.rs` (SQL⇄predicate), `ds.rs` (streams client incl. `append_reliable`). |
| `apps/api` | tRPC API (`router.ts`) over the engine + durable-streams (`core.ts`). |
| `packages/protocol` | Shared types + the change-event envelope (`types.ts`, `envelope.ts`). |
| `packages/client` | Browser client: `shape()`, `subset()` (see `subset.ts` — LSN watermarks + tombstones), `aggregate()`. All lifecycles tracked; `close()` is one-shot and deletes server-side with retry. |
| `packages/conformance` | The real test suite — engine vs oracle, incl. live replication, fuzz, NULLs, concurrency, shape sharing. |
| `packages/oracle` | Reference implementation shapes are checked against. |
| `packages/bench` | Benchmarks incl. the **benchmarking-fleet runner** (`electric-bench-runner.ts`, `pnpm bench:fleet` — auto-clones electric-sql/benchmarking-fleet). |
| `packages/loadgen` | Headless load generator (state-machine users; memory/CPU/disk sampling; Docker-scalable clients). |
| `electric-conformance/` | Electric's own oracle/property/integration tests pointed at our `/v1/shape`. |
| `docker/` | Containerized stack: `compose.yaml` (postgres + ds + engine + api), `Dockerfile.engine`, `Dockerfile.node`. `pnpm docker:up`. |
| `apps/pipeline-viz` | Live pipeline explorer (shapes, shared families/nodes, per-node indexes) over `GET /graph`. |
| `examples/linearlite` | The flagship demo. `scripts/linearlite.sh start <size>` boots everything. |

## Docs (read these before designing)

- `README.md` — the system in one page + the consistency model summary.
- `docs/ARCHITECTURE.md` — the as-built architecture: ingest, `SnapshotGate` fencing, sharing,
  subquery registry, reliability model, Electric adapter, client layer.
- `docs/ivm-engine-internals.md` — engine execution strategies + the analytical cost model.
- `docs/shapes-and-subqueries-guide.md` — user/integrator guide.
- `docs/deployment-postgres.md` — Postgres-as-source-of-record setup.
- Each package has its own `README.md` (surface, commands, env knobs).

## Build & test

```bash
pnpm engine:build          # cargo build -p electric-ivm-engine
pnpm engine:test           # cargo test  -p electric-ivm-engine   (fast)
pnpm test                  # vitest run — full suite incl. conformance (~60s; boots its own PG)
pnpm test:conformance      # just the conformance package
pnpm test:fuzz             # random-predicate fuzz vs oracle
pnpm loop [N]              # fuzz until failure; replay with SEED=<n>
pnpm demo:linearlite       # LinearLite demo (ephemeral PG + engine + ds + api + vite + caddy)
pnpm bench:fleet           # ElectricSQL benchmarking-fleet vs our /v1/shape (auto-clones)
pnpm docker:up             # containerized stack
```

**There is no `tsc` typecheck gate** — CI is vitest (esbuild, transpile-only). To check TS: run
`pnpm test`, or transpile-load a module with `npx tsx -e "import(...)"`. Always run
`pnpm engine:test` + `pnpm test` before claiming done.

## Running the stack (sizes, explorer, load testing)

`scripts/linearlite.sh start <size>` — size = `small|medium|large|xlarge|<issue count>`; users and
projects scale with it (users ~√issues). Boots PG + ds + engine + API + web UI + the pipeline
explorer; `stop` tears down cleanly; `status` reports. One instance at a time (teardown is
pattern-based). Ports: `DEMO_HTTPS_PORT` (8443), `DEMO_VIZ_PORT` (5180), `DEMO_VIZ=0` to skip.

`packages/loadgen` — `USERS=100 SEED_ISSUES=20000 DURATION_S=90 pnpm --filter @electric-ivm/loadgen
loadgen`; `SWEEP_USERS=…` for comparison tables; Docker client scaling in `packages/loadgen/docker/`.
Use `DS_MEMORY=1` at high concurrency (the file-backed test server fsyncs per append — the ceiling is
storage, not the engine).

## Invariants (violate these and conformance will catch you — eventually)

- **Postgres is the system of record; the engine holds no table copy.** Backfills read matching rows
  in a `REPEATABLE READ` snapshot.
- **Backfill↔live is fenced by xid visibility, NOT by LSN.** Every seeded structure carries a
  `pg::SnapshotGate` (from `pg_current_snapshot()`); a replicated change is skipped iff its xid was
  visible to that snapshot. `commit_lsn < seed_lsn` is only the fallback for changes without an xid.
  If you add a read path, use the gate. (Why: a commit's WAL record exists before it becomes
  snapshot-visible; LSN comparison drops rows in that window and duplicates at the boundary.)
- **Ingest is at-least-once; consumers restore exactly-once effect.** The ingestor stamps
  `(commit lsn, xid, seq)`; tailers de-duplicate by `(lsn, seq)`. Aggregates and subquery contributor
  weights are NOT idempotent under duplicates — never bypass the highwater.
- **Live shape appends must not drop.** Use `ds.append_reliable` (retry/backoff; 404 = shape dropped,
  discard). The tailer's processed offset is published only after the whole batch landed.
- **Subqueries: emit outer membership *absolutely*** — per touched pk, `upsert` if the row matches
  *now* else idempotent `delete`. Per-table tailers interleave arbitrarily; delta-based emission
  misses move-outs. Symptom when wrong: op-by-op converges, *batched* mutations diverge.
- **NULL flips re-derive any dependent whose `IN` leaf is negated OR under a `Not{…}`** (edge
  `null_sensitive`). Plain-`IN` dependents can't change (monotonicity over FALSE<UNKNOWN<TRUE).
- **Shape creation is atomic.** On any failure, everything (record, share entries, registry
  refcounts/edges, stream) rolls back and the error propagates — including to joiners waiting on the
  share's ready-watch. Never leave a signature pointing at a dead stream.
- **Sharing lifecycle:** equal shapes share one id+stream, ref-counted; N joiners each delete exactly
  once; the final drop deletes the durable stream. Client `close()` must be one-shot (a double
  delete steals another subscriber's refcount).
- **Shapes vs subset queries stay distinct.** Ranges/`orderBy`/`limit` live ONLY in subset queries
  (never live-tailed); a `changes_only` feed uses a passthrough gate and the client reads from the
  offset captured *before* the page snapshot.
- **Aggregations follow SQL NULL semantics** (ignore NULLs; `COUNT(col)` = non-NULLs; empty
  SUM/AVG/MIN/MAX = NULL). Extended API only — the Electric surface doesn't cover them.
- Branch before committing if on the default branch.

## Gotchas (know these before touching the respective areas)

- **`pg_current_wal_lsn()` is not a visibility fence.** A commit's WAL record exists (and the LSN
  moves past it) before the transaction becomes visible to snapshots — the gap includes a WAL fsync.
  The xid gate (`pg_current_snapshot()`) decides both the dropped-row and boundary-duplicate cases
  exactly. See `pg.rs::SnapshotGate`.
- **The client must delete what it creates — there is no server-side reaper.** Every create path
  needs a guaranteed, retried, one-shot delete; `track()` in `packages/client/src/index.ts` is the
  pattern. An unpaired create pins a shared feed's refcount forever.
- **Backfill and replication must produce byte-identical text values.** `to_jsonb(t)` renders
  timestamps ISO-`T`-style; `test_decoding` uses Postgres text output. Same cell, different string →
  broken retractions/routing/MIN-MAX. Backfill casts text-mapped columns with `::text`
  (`pg.rs::row_json_expr`). If you add a read path, match it.
- **test_decoding array types nest brackets** (`tags[integer[]]:`) — the type-name skip in
  `parse_cols` is bracket-depth aware; keep it that way (a first-`]` scan reads every later column
  in the row as NULL).
- **Read raw stream envelopes, not stream-db's reconciled view, when you need every delta.** A
  subset's live feed must apply *move-outs*; stream-db no-ops a delete for a key it never inserted.
  (`packages/client/subset.ts` reads raw `StreamEnvelope`s.)
- **Deletes must leave tombstones across the page/live seam.** An in-flight `loadMore` whose
  snapshot predates a delete would resurrect the row (or insert a ghost for a never-seen pk) unless
  the per-pk watermark survives the delete. (`subset.ts` keeps LSN tombstones, pruned when no page
  is in flight.)
- **Shape rows stringify the primary key** (TanStack DB keys are strings); non-pk ints stay numbers.
  Normalize ids when cross-referencing shape rows against query-back rows.
- **A subset whose predicate folds in live UI filters re-creates the engine feed on every click.**
  Prefer per-facet feeds reused across filter changes + a client merge (identical predicates across
  users ⇒ shared engine families). LinearLite's browse list does this.
- **The demo boots an _ephemeral_ Postgres each run** (`mkdtemp`); data does not persist. **Kill
  stale demos before restarting** — a leftover `tsx start.ts`/`caddy` keeps the ports and serves
  stale code, which reads as a mysterious schema mismatch. `scripts/linearlite.sh stop`, or
  `pkill -f electric-ivm-engine`, `pkill -f "tsx start.ts"`, `pkill -f caddy`. If two demos run,
  scope kills by port (`ps -o ppid= -p $(lsof -ti :<httpsPort>)`) — a SIGKILL mid-shutdown leaks
  the ephemeral Postgres.
- **Vite binds IPv6 `[::1]` only** — prefer the `https://localhost:8443` Caddy proxy (HTTP/2 also
  dodges the browser's ~6-connection HTTP/1.1 cap that freezes multi-stream apps).
- **Under load the durable-streams *test server* is the ceiling, not the engine** (fsync per append
  file-backed; `ECONNRESET` under burst). `DS_MEMORY=1` + staggered ramps; a production
  durable-streams backend lifts the ceiling.
- **Docker + pnpm:** scripts that import workspace deps must live in a workspace package
  (`docker/package.json`) — running `tsx docker/x.ts` from the repo root can't resolve them.
- **Verify against the live stack, not just types.** A headless `tsx` script driving the real client
  against a running demo catches what the (absent) typechecker can't. **Changing code means
  realigning docs in the same pass.**
