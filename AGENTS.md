# AGENTS.md

Guidance for AI agents working in **electric-circuits** — an Electric-style reactive sync engine. App
writes go to **Postgres**; a Rust engine turns logical-replication changes into **live shapes**
(incrementally maintained, fully de-duplicated); **durable streams** is the log between them. Two
client surfaces: the Electric-compatible `GET /v1/shape` (works with the ElectricSQL TS client) and
the extended `@electric-circuits/client` API (shapes + subset queries + live aggregations — the surface
the project is growing toward).

## Layout

| Path | What |
|---|---|
| `apps/engine` | Rust engine. Key files: `engine/` (the engine module — `sequencer.rs` the LSN-ordered sequencer, `lifecycle.rs` shape creation/sharing/retention, `circuit_serving.rs` circuit-tier serving, `executors.rs` routers/filters/folds, `planning.rs` circuit placement, `catalog.rs` durable catalog, `introspection.rs` graph/state, `membership.rs` the shared membership kernel (flips, query-backs), `output.rs` envelope codec, `mod.rs` the `Engine` handle), `arrangements.rs` (the circuit: in-memory counts pipelines, group-aggregated boot seeding), `subquery.rs` (cross-table registry: shared inner-set nodes, flips, absolute emission), `replication.rs` (streaming pgoutput ingestor) + `pgoutput.rs` (message decoder), `pg.rs` (backfill + `SnapshotGate`), `electric.rs` (`/v1/shape`), `where_sql.rs`/`sql.rs` (SQL⇄predicate), `ds.rs` (streams client incl. `append_reliable`). |
| `apps/api` | tRPC API (`router.ts`) over the engine + durable-streams (`core.ts`). |
| `packages/protocol` | Shared types + the change-event envelope (`types.ts`, `envelope.ts`). |
| `packages/client` | Browser client: `shape()`, `subset()` (see `subset.ts` — LSN watermarks + tombstones), `aggregate()`. All lifecycles tracked; `close()` is one-shot and deletes server-side with retry. |
| `packages/conformance` | The real test suite — engine vs oracle, incl. live replication, fuzz, NULLs, concurrency, shape sharing. |
| `packages/oracle` | Reference implementation shapes are checked against. |
| `packages/bench` | Benchmarks incl. the **benchmarking-fleet runner** (`electric-bench-runner.ts`, `pnpm bench:fleet` — auto-clones electric-sql/benchmarking-fleet). |
| `packages/loadgen` | Headless load generator (state-machine users; memory/CPU/disk sampling; Docker-scalable clients). |
| `electric-conformance/` | Electric's own oracle/property/integration tests pointed at our `/v1/shape`. |
| `docker/` | Containerized stack: `compose.yaml` (postgres + ds + engine + api), `Dockerfile.engine`, `Dockerfile.node`. `pnpm docker:up`. |
| `apps/pipeline-viz` | Live pipeline explorer (shapes, shared families/nodes, reactive per-node state + index dumps) over `GET /graph` + `/state` + `/trace`. |
| `examples/linearlite` | The flagship demo. `scripts/linearlite.sh start <size>` boots everything. |

## Docs (read these before designing)

- `README.md` — the system in one page + the consistency model summary.
- `docs/ARCHITECTURE.md` — the as-built architecture: ingest, `SnapshotGate` fencing, sharing,
  subquery registry, reliability model, Electric adapter, client layer.
- `docs/ivm-engine-internals.md` — engine execution strategies + the analytical cost model,
  including the three-tier serving model (circuit/routing/fallback): see
  [`docs/ivm-engine-internals.md#serving-tiers-compiled-routed-fallback`](docs/ivm-engine-internals.md#serving-tiers-compiled-routed-fallback).
- `docs/live-queries-guide.md` — user/integrator guide.
- `docs/deployment-postgres.md` — Postgres-as-source-of-record setup.
- Each package has its own `README.md` (surface, commands, env knobs).

## Designing dbsp circuits: pipelines vs shapes

The load-bearing mental model: **pipelines are few and fixed; shapes are many and dynamic —
and the fan-out between them lives outside the circuit.** A pipeline's output is keyed by
*cohort groups* (project, (project, status), aggregate group, …). A shape is a selection or
union over those groups, materialized as a per-shape stream at the delivery edge. Shape
cardinality can vastly exceed pipeline cardinality: a subquery shape filtering issues exists
per *combination* of projects a client asks for, yet every combination is fed from the same
`issues_by_project` pipeline — the circuit never grows with shape count, only the routing
table does. If a design makes the circuit's structure scale with shapes, users, or parameter
combinations, it is wrong (the circuit-per-shape trap: structure must never scale with
subscriptions).

The recipe for capturing an app's query set in one circuit:

1. **Enumerate call sites → collapse to templates.** Parameters become *data* (keys in the
   output index, rows in an input relation) — never circuit structure.
2. **Find the access cohort** (LinearLite: the project) and key every pipeline output by it,
   never by user or shape. Per-shape work happens only at the fan-out edge: a shape = the set
   of cohort groups its parameters select, unioned by delivery. The union is correct only when
   the cohort key **partitions** the table (a row lives in exactly one group) — overlapping
   groups would double-emit and need dedup at the edge. Genuinely per-user predicates
   (`username = $me`) get their own keyed feed — same pattern, cohort of size one.
3. **Visibility relations drive membership through the registry**: a membership table's
   deltas flip shared inner-set nodes, and move-in/move-out are parallel pooled Postgres
   query-backs with absolute per-pk emission through ordered lanes (row data lives in
   Postgres — there are no local row snapshots to read).
4. **Linear operators are free** (filter / project / `map_index`); **aggregates knowingly** —
   counts pipelines hold O(distinct groups) in memory (`ARCHITECTURE.md` §6b). Aggregate at the
   finest useful group grain and let the reader sum groups, so one pipeline serves every
   filter combination.
5. **Structure ships with deploys.** New templates = circuit rebuild + reseed (layout
   fingerprint) or `Mode::Persistent` bootstrap. Ad-hoc predicates that match no template fall
   back to the dynamic-shape path (standalone evaluator / KeyRouter / registry) — the circuit
   is an optimization tier, never a correctness dependency.

## Build & test

```bash
pnpm engine:build          # cargo build -p electric-circuits-engine
pnpm engine:test           # cargo test  -p electric-circuits-engine   (fast)
pnpm test                  # vitest run — full suite incl. conformance (~60s; boots its own PG)
pnpm test:conformance      # just the conformance package
pnpm test:fuzz             # random-predicate fuzz vs oracle
pnpm loop [N]              # fuzz until failure; replay with SEED=<n>
pnpm demo:linearlite       # LinearLite demo (ephemeral PG + engine + ds + api + vite + caddy)
pnpm bench:fleet           # ElectricSQL benchmarking-fleet vs our /v1/shape (auto-clones)
pnpm docker:up             # containerized stack
```

**Benchmarking against other Electric versions.** The fleet runner also drives any
Electric-compatible server instead of our stack — use this to baseline stock Electric releases
against the same workloads:

```bash
# 1. Boot the target, e.g. stock Electric:
docker run -d --name electric-baseline -p 3000:3000 \
  -e DATABASE_URL=postgresql://postgres:password@host.docker.internal:54321/electric \
  -e ELECTRIC_INSECURE=true electricsql/electric:latest

# 2. Point the fleet at it (both vars required together; tables are dropped/recreated in that DB):
EXTERNAL_ELECTRIC_URL=http://localhost:3000 \
EXTERNAL_DATABASE_URL=postgresql://postgres:password@localhost:54321/electric \
BENCH_OUT=docs/bench/electric-fleet-results-baseline.md pnpm bench:fleet
```

Use a distinct `BENCH_OUT` per target and diff the reports; `BENCH_ONLY`/`BENCH_SCALE` apply the
same way. (Our own image can also be the target — `pnpm docker:up`, then point at port 7010.)

**There is no `tsc` typecheck gate** — CI is vitest (esbuild, transpile-only). To check TS: run
`pnpm test`, or transpile-load a module with `npx tsx -e "import(...)"`. Always run
`pnpm engine:test` + `pnpm test` before claiming done.

## Running the stack (sizes, explorer, load testing)

`scripts/linearlite.sh start <size>` — size = `small|medium|large|xlarge|<issue count>`; users and
projects scale with it (users ~√issues). Boots PG + ds + engine + API + web UI + the pipeline
explorer; `stop` tears down cleanly; `status` reports. One instance at a time (teardown is
pattern-based). Ports: `DEMO_HTTPS_PORT` (8443), `DEMO_VIZ_PORT` (5180), `DEMO_VIZ=0` to skip.

`packages/loadgen` — `USERS=100 SEED_ISSUES=20000 DURATION_S=90 pnpm --filter @electric-circuits/loadgen
loadgen`; `SWEEP_USERS=…` for comparison tables; Docker client scaling in `packages/loadgen/docker/`.
The streams layer is the Rust durable-streams server (group-commit WAL — appends batch under
concurrency); `DS_MEMORY=1` still removes durability entirely for max-throughput runs
(`--durability memory`, Linux-only).

## Demo + visualizer: start, drive, verify (agent runbook)

**Start everything with one command** (rebuilds the engine, boots an ephemeral throwaway Postgres
with `wal_level=logical`, durable-streams, the API, LinearLite, and the pipeline visualizer wired
to the engine):

```bash
pnpm demo:linearlite > /tmp/demo.log 2>&1 &     # agents: run in background, tail the log
# ready when the log prints "👉 Open a URL above"
```

Fixed URLs: LinearLite `http://localhost:5174` (HTTPS/HTTP-2 `https://localhost:8443`), visualizer
`http://localhost:5180` (`https://localhost:5443`). Ephemeral ports for the rest — grep the log:
`postgres →`, `engine →`, `api →`. `DEMO_SEED_COUNT=<n>` scales the faker seed (default 512 issues).
Data resets every run. **Restarting:** kill the previous run first or Vite silently binds 5175 —
`pkill -f electric-circuits-engine; pkill -f caddy; pkill -f linearlite/start.ts`, then relaunch (if a
port lingers: `lsof -ti :5174 -ti :5180 | xargs kill`).

The **visualizer** can also attach to any running engine on its own:
`ELECTRIC_CIRCUITS_ENGINE_URL=http://127.0.0.1:<port> pnpm --filter @electric-circuits/pipeline-viz dev`.
Its dev server proxies `/engine/*` → the engine control plane, so browser-side `fetch('/engine/graph')`
etc. work from the page — the backbone of the verification workflow below. A third way is the
containerized visualizer (`docker/Dockerfile.viz`): `docker build -f docker/Dockerfile.viz -t
electric-circuits-viz . && docker run -p 5180:5180 -p 5443:5443 electric-circuits-viz` serves
`http://localhost:5180` with Caddy proxying `/engine/*` to the engine; set `ENGINE_UPSTREAM` to
point it at another engine.

### Typical verification workflow (Playwright MCP)

Use the Playwright MCP browser to drive both apps; keep LinearLite and the visualizer in two tabs
(`browser_tabs` to create/select, `browser_navigate` to each URL).

1. **Make the pipeline do something.** Drive LinearLite: switch the "Viewing as" user
   (`browser_select_option` on the sidebar `<select>`) to create/join that user's shapes; open the
   Board view or an issue detail for more; drag cards / edit issues for live writes. For surgical
   writes, `psql` straight at the demo Postgres (URL from the log) — replication picks it up.
2. **Verify engine state from the viz page** with `browser_evaluate` — no CORS friction thanks to
   the proxy: `await (await fetch('/engine/graph')).json()` (shapes/nodes/edges),
   `/engine/shapes/{id}` (incl. retention `state`), `/engine/metrics`, `/engine/state`.
3. **Verify the canvas against the engine.** Count DOM vs graph:
   `document.querySelectorAll('.react-flow__node').length` / `'.react-flow__edge'` vs
   `graph.shapes` — nodes render immediately, edges must too (regression test: clear shapes via the
   trash button, switch user, edges must appear without a reload).
4. **Verify animations deterministically** via the sidebar **Activity** log (last 50 replicated
   changes): click an entry with `browser_evaluate` and sample in the same script — dot positions
   over time (`.react-flow__edge g circle` → `getBoundingClientRect()`), staged flash delays
   (`.flash` → `--flash-delay`), pulse stagger. Replay beats racing a live write's timing.
5. **Eyeball it.** `browser_take_screenshot` after driving — a blank or stale frame is a failure
   even when the DOM probes pass. Check the browser console output for React/engine errors.

Retention interplay while testing: an open LinearLite tab holds subscriptions (refcount ≥ 1), which
blocks dormancy for its shapes; `GET /shapes/{id}` is deliberately NOT a retention touch, so
polling it never keeps a shape alive. To exercise dormancy/eviction fast, boot with second-scale
knobs (`ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS=1 ELECTRIC_CIRCUITS_RETENTION_SWEEP_SECS=1 …`) — see
`packages/conformance/src/conformance-retention.test.ts` for the canonical sequence.

### Testing checklist before claiming done

**This is a task-completion requirement, not a suggestion: an engine-touching task is not
"done" until all three suites below have run green, and agents must run them (or report exactly
which ran and why the rest could not) before closing the task.**

```bash
pnpm engine:test                          # Rust unit + integration (fast)
ELECTRIC_CIRCUITS_ENGINE_PREBUILT=1 pnpm test  # full vitest suite incl. oracle conformance (set the var iff you already built)
ASDF_ELIXIR_VERSION=1.18.4-otp-28 ASDF_ERLANG_VERSION=28.1 \
  ./electric-conformance/run.sh oracle    # Electric's own oracle vs /v1/shape (needs elixir + ../electric)
```

The vitest suite includes `packages/conformance` — the engine-vs-oracle harness — and runs
against the always-on circuit on every run (there is no off mode). The
`ELECTRIC_CIRCUITS_DBSP_INDEXES`/`_COUNTS` tunables decide which shapes the circuit actually serves
versus which fall through to the routing/fallback tiers. The `electric-conformance` line is
Electric's *own* oracle suite pointed at our `/v1/shape` — a separate tier from our conformance
package; run both. (The ASDF pins matter: `../electric` asks for an Elixir that may not be
installed locally.)

**E2E (browser) tier:** for anything touching the engine's live path, shapes, or the visualizer,
finish by **driving the demo as above** (Playwright MCP runbook, §"Demo + visualizer") — the
suites don't render a canvas or exercise the browser, so a green run does not prove the live
UI path. A quick pass = boot `pnpm demo:linearlite`, drive a write, verify the shape stream and
canvas update, screenshot.

## Invariants (violate these and conformance will catch you — eventually)

- **Postgres is the system of record; the engine's hot path holds no table copy.** Backfills read
  matching rows in a `REPEATABLE READ` snapshot. (The always-on circuit tier
  holds disk-spillable *derived* state — table arrangements + counts pipelines — rebuildable,
  with Postgres fallback for lookups, never the record of truth; see `ARCHITECTURE.md` §6b.)
- **Backfill↔live is fenced by xid visibility, NOT by LSN.** Every seeded structure carries a
  `pg::SnapshotGate` (from `pg_current_snapshot()`); a replicated change is skipped iff its xid was
  visible to that snapshot. `commit_lsn < seed_lsn` is only the fallback for changes without an xid.
  If you add a read path, use the gate. (Why: a commit's WAL record exists before it becomes
  snapshot-visible; LSN comparison drops rows in that window and duplicates at the boundary.)
- **Ingest is at-least-once; consumers restore exactly-once effect.** The ingestor stamps
  `(commit lsn, xid, seq)`; the sequencer de-duplicates by `(lsn, seq)`. Aggregates and subquery contributor
  weights are NOT idempotent under duplicates — never bypass the highwater.
- **Live shape appends must not drop.** Use `ds.append_reliable` (retry/backoff; 404 = shape dropped,
  discard). The sequencer's processed offset is published only after the whole batch landed, and
  each source transaction's appends are flushed before the next transaction is processed
  (per-transaction atomic emission).
- **Subqueries: emit outer membership *absolutely*** — per touched pk, `upsert` if the row matches
  *now* else idempotent `delete`. Flip-driven query-backs run deferred on the flip-propagator task
  (out of commit order relative to the sequencer), so delta-based emission would miss move-outs.
  Symptom when wrong: op-by-op converges, *batched* mutations diverge.
- **NULL flips re-derive any dependent whose `IN` leaf is negated OR under a `Not{…}`** (edge
  `null_sensitive`). Plain-`IN` dependents can't change (monotonicity over FALSE<UNKNOWN<TRUE).
- **Shape creation is atomic.** On any failure, everything (record, share entries, registry
  refcounts/edges, stream) rolls back and the error propagates — including to joiners waiting on the
  share's ready-watch. Never leave a signature pointing at a dead stream.
- **Sharing lifecycle:** equal shapes share one id+stream, ref-counted; N joiners each release
  exactly once. The final release does NOT delete anything: the shape stays active/warm and is
  retired by the retention lifecycle (idle → dormant → evicted; `engine/src/retention.rs`). Client
  `close()` must still be one-shot (a double delete steals another subscriber's refcount).
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
- **The client should still release what it creates** (`track()` in `packages/client/src/index.ts`
  is the pattern), but the engine now has a server-side reaper: an unpaired create pins the shape
  active only until the retention sweeper's idle/dormancy/eviction layers retire it
  (`engine/src/retention.rs`). A leaked refcount (double create, missed release) DOES still pin a
  shape active forever — releases must stay paired one-to-one with creates.
- **Backfill and replication must produce byte-identical text values.** `to_jsonb(t)` renders
  timestamps ISO-`T`-style; pgoutput text-mode tuples use Postgres text output. Same cell, different string →
  broken retractions/routing/MIN-MAX. Backfill casts text-mapped columns with `::text`
  (`pg.rs::row_json_expr`). If you add a read path, match it.
- **Ingest is streaming pgoutput** (walsender protocol via `pgwire-replication` + our own
  `pgoutput.rs` decoder, text-mode tuples, never the `binary` option — binary values would break
  the byte-identity above). The slot is `pgoutput`-plugin; a publication `<slot>_pub` (FOR ALL
  TABLES, needs superuser) is created at setup.
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
  `pkill -f electric-circuits-engine`, `pkill -f "tsx start.ts"`, `pkill -f caddy`. If two demos run,
  scope kills by port (`ps -o ppid= -p $(lsof -ti :<httpsPort>)`) — a SIGKILL mid-shutdown leaks
  the ephemeral Postgres.
- **Vite binds IPv6 `[::1]` only** — prefer the `https://localhost:8443` Caddy proxy (HTTP/2 also
  dodges the browser's ~6-connection HTTP/1.1 cap that freezes multi-stream apps). **The pipeline
  visualizer needs its Caddy front too** (`https://localhost:5443`, auto-started by the demo): its
  `/trace` SSE + engine polling compete for the same connection budget, so open it over HTTPS in a
  browser — plain `http://localhost:5180` is for `curl` only. See `.claude/skills/run-linearlite`.
- **The durable-streams server is the Rust binary** (crates.io `durable-streams`, spawned by the
  drop-in wrapper `packages/ds-rust`; self-provisions via `cargo install`, override with
  `DS_RUST_BIN`). Appends are group-commit WAL `fdatasync` — no per-append fsync ceiling like the
  old Node test server. `DS_MEMORY=1` still works (ephemeral data dir; `--durability memory` is
  Linux-only, macOS falls back to `wal`).
- **Docker + pnpm:** scripts that import workspace deps must live in a workspace package
  (`docker/package.json`) — running `tsx docker/x.ts` from the repo root can't resolve them.
- **Verify against the live stack, not just types.** A headless `tsx` script driving the real client
  against a running demo catches what the (absent) typechecker can't. **Changing code means
  realigning docs in the same pass.**

## Git Policy

Do not commit or push unless explicitly asked. At handoff, report changed files, validation run,
and suggested next commands.
