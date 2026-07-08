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
| `apps/engine` | Rust engine. Key files: `engine.rs` (the LSN-ordered sequencer, routing, shape sharing/lifecycle, aggregations), `subquery.rs` (cross-table registry: shared inner-set nodes, flips, absolute emission), `replication.rs` (streaming pgoutput ingestor) + `pgoutput.rs` (message decoder), `pg.rs` (backfill + `SnapshotGate`), `electric.rs` (`/v1/shape`), `where_sql.rs`/`sql.rs` (SQL⇄predicate), `ds.rs` (streams client incl. `append_reliable`). |
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
| `tutorials/` | Tutorial series: one compose stack (postgres+ds+engine+api+viz) + per-episode walkthroughs (episodes/01-first-shape, 02-inside-the-pipeline). |
| `examples/linearlite` | The flagship demo. `scripts/linearlite.sh start <size>` boots everything. |

## Docs (read these before designing)

- `README.md` — the system in one page + the consistency model summary.
- `docs/ARCHITECTURE.md` — the as-built architecture: ingest, `SnapshotGate` fencing, sharing,
  subquery registry, reliability model, Electric adapter, client layer.
- `docs/ivm-engine-internals.md` — engine execution strategies + the analytical cost model.
- `docs/shapes-and-subqueries-guide.md` — user/integrator guide.
- `docs/deployment-postgres.md` — Postgres-as-source-of-record setup.
- Each package has its own `README.md` (surface, commands, env knobs).
- `docs/linearlite-circuit-design.md` — design study: one dbsp circuit for an entire app's
  query set; source of the recipe below.

## Designing dbsp circuits: pipelines vs shapes

The load-bearing mental model: **pipelines are few and fixed; shapes are many and dynamic —
and the fan-out between them lives outside the circuit.** A pipeline's output is keyed by
*cohort groups* (project, (project, status), aggregate group, …). A shape is a selection or
union over those groups, materialized as a per-shape stream at the delivery edge. Shape
cardinality can vastly exceed pipeline cardinality: a subquery shape filtering issues exists
per *combination* of projects a client asks for, yet every combination is fed from the same
`issues_by_project` pipeline — the circuit never grows with shape count, only the routing
table does. If a design makes the circuit's structure scale with shapes, users, or parameter
combinations, it is wrong (that is the circuit-per-shape mistake this repo already made and
removed; see the git history around `75488b6`/`c1aa075`).

The recipe for capturing an app's query set in one circuit:

1. **Enumerate call sites → collapse to templates.** Parameters become *data* (keys in the
   output index, rows in an input relation) — never circuit structure.
2. **Find the access cohort** (LinearLite: the project) and key every pipeline output by it,
   never by user or shape. Per-shape work happens only at the fan-out edge: a shape = the set
   of cohort groups its parameters select, unioned by delivery. Genuinely per-user predicates
   (`username = $me`) get their own keyed feed — same pattern, cohort of size one.
3. **Visibility relations become the delivery router**, not a join input: a membership feed's
   deltas drive subscribe/unsubscribe to cohort feeds, and backfill = replaying the cohort
   feed's own durable log (no Postgres snapshot, no xmin fencing, no flip query-backs).
4. **Linear operators are free** (filter / project / `map_index`); **joins and aggregates
   knowingly** — a join stores both inputs (acceptable with storage-backed spilling; see
   `ARCHITECTURE.md` §6b), and `aggregate_linear` (COUNT/SUM) is cheap. Aggregate at the
   finest useful group grain and let the reader sum groups, so one pipeline serves every
   filter combination.
5. **Structure ships with deploys.** New templates = circuit rebuild + reseed (layout
   fingerprint) or `Mode::Persistent` bootstrap. Ad-hoc predicates that match no template fall
   back to the dynamic-shape path (standalone evaluator / KeyRouter / registry) — the circuit
   is an optimization tier, never a correctness dependency.

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

`packages/loadgen` — `USERS=100 SEED_ISSUES=20000 DURATION_S=90 pnpm --filter @electric-ivm/loadgen
loadgen`; `SWEEP_USERS=…` for comparison tables; Docker client scaling in `packages/loadgen/docker/`.
Use `DS_MEMORY=1` at high concurrency (the file-backed test server fsyncs per append — the ceiling is
storage, not the engine).

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
`pkill -f electric-ivm-engine; pkill -f caddy; pkill -f linearlite/start.ts`, then relaunch (if a
port lingers: `lsof -ti :5174 -ti :5180 | xargs kill`).

The **visualizer** can also attach to any running engine on its own:
`ELECTRIC_IVM_ENGINE_URL=http://127.0.0.1:<port> pnpm --filter @electric-ivm/pipeline-viz dev`.
Its dev server proxies `/engine/*` → the engine control plane, so browser-side `fetch('/engine/graph')`
etc. work from the page — the backbone of the verification workflow below. A third way is the
containerized visualizer (`docker/Dockerfile.viz`): `cd tutorials && docker compose up --build` serves
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
knobs (`ELECTRIC_IVM_SHAPE_IDLE_SECS=1 ELECTRIC_IVM_RETENTION_SWEEP_SECS=1 …`) — see
`packages/conformance/src/conformance-retention.test.ts` for the canonical sequence.

### Testing checklist before claiming done

```bash
pnpm engine:test                          # Rust unit + integration (fast)
ELECTRIC_IVM_ENGINE_PREBUILT=1 pnpm test  # full vitest suite (set the var iff you already built)
./electric-conformance/run.sh oracle      # Electric's own oracle vs /v1/shape (needs elixir + ../electric)
```

Then, for anything touching the engine's live path, shapes, or the visualizer: **drive the demo as
above** — the suites don't render a canvas or exercise the browser.

## Invariants (violate these and conformance will catch you — eventually)

- **Postgres is the system of record; the engine's hot path holds no table copy.** Backfills read
  matching rows in a `REPEATABLE READ` snapshot. (The optional dbsp arrangement layer,
  `ELECTRIC_IVM_DBSP=1`, holds disk-backed *derived* table indexes — rebuildable state with
  Postgres fallback, never the record of truth; see `ARCHITECTURE.md` §6b.)
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
  `pkill -f electric-ivm-engine`, `pkill -f "tsx start.ts"`, `pkill -f caddy`. If two demos run,
  scope kills by port (`ps -o ppid= -p $(lsof -ti :<httpsPort>)`) — a SIGKILL mid-shutdown leaks
  the ephemeral Postgres.
- **Vite binds IPv6 `[::1]` only** — prefer the `https://localhost:8443` Caddy proxy (HTTP/2 also
  dodges the browser's ~6-connection HTTP/1.1 cap that freezes multi-stream apps). **The pipeline
  visualizer needs its Caddy front too** (`https://localhost:5443`, auto-started by the demo): its
  `/trace` SSE + engine polling compete for the same connection budget, so open it over HTTPS in a
  browser — plain `http://localhost:5180` is for `curl` only. See `.claude/skills/run-linearlite`.
- **Under load the durable-streams *test server* is the ceiling, not the engine** (fsync per append
  file-backed; `ECONNRESET` under burst). `DS_MEMORY=1` + staggered ramps; a production
  durable-streams backend lifts the ceiling.
- **Docker + pnpm:** scripts that import workspace deps must live in a workspace package
  (`docker/package.json`) — running `tsx docker/x.ts` from the repo root can't resolve them.
- **Verify against the live stack, not just types.** A headless `tsx` script driving the real client
  against a running demo catches what the (absent) typechecker can't. **Changing code means
  realigning docs in the same pass.**

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:970c3bf2 -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

**Architecture in one line:** issues live in a local Dolt DB; sync uses `refs/dolt/data` on your git remote; `.beads/issues.jsonl` is a passive export. See https://github.com/gastownhall/beads/blob/main/docs/SYNC_CONCEPTS.md for details and anti-patterns.

## Agent Context Profiles

The managed Beads block is task-tracking guidance, not permission to override repository, user, or orchestrator instructions.

- **Conservative (default)**: Use `bd` for task tracking. Do not run git commits, git pushes, or Dolt remote sync unless explicitly asked. At handoff, report changed files, validation, and suggested next commands.
- **Minimal**: Keep tool instruction files as pointers to `bd prime`; use the same conservative git policy unless active instructions say otherwise.
- **Team-maintainer**: Only when the repository explicitly opts in, agents may close beads, run quality gates, commit, and push as part of session close. A current "do not commit" or "do not push" instruction still wins.

## Session Completion

This protocol applies when ending a Beads implementation workflow. It is subordinate to explicit user, repository, and orchestrator instructions.

1. **File issues for remaining work** - Create beads for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **Handle git/sync by active profile**:
   ```bash
   # Conservative/minimal/default: report status and proposed commands; wait for approval.
   git status

   # Team-maintainer opt-in only, unless current instructions forbid it:
   git pull --rebase
   bd dolt push
   git push
   git status
   ```
5. **Hand off** - Summarize changes, validation, issue status, and any blocked sync/commit/push step

**Critical rules:**
- Explicit user or orchestrator instructions override this Beads block.
- Do not commit or push without clear authority from the active profile or the current user request.
- If a required sync or push is blocked, stop and report the exact command and error.
<!-- END BEADS INTEGRATION -->

<!-- BEGIN BEADS CODEX SETUP: generated by bd setup codex -->
## Beads Issue Tracker

Use Beads (`bd`) for durable task tracking in repositories that include it. Use the `beads` skill at `.agents/skills/beads/SKILL.md` (project install) or `~/.agents/skills/beads/SKILL.md` (global install) for Beads workflow guidance, then use the `bd` CLI for issue operations.

### Quick Reference

```bash
bd ready                # Find available work
bd show <id>            # View issue details
bd update <id> --claim  # Claim work
bd close <id>           # Complete work
bd prime                # Refresh Beads context
```

### Rules

- Use `bd` for all task tracking; do not create markdown TODO lists.
- Run `bd prime` when Beads context is missing or stale. Codex 0.129.0+ can load Beads context automatically through native hooks; use `/hooks` to inspect or toggle them.
- Keep persistent project memory in Beads via `bd remember`; do not create ad hoc memory files.

**Architecture in one line:** issues live in a local Dolt DB; sync uses `refs/dolt/data` on your git remote; `.beads/issues.jsonl` is a passive export. See https://github.com/gastownhall/beads/blob/main/docs/SYNC_CONCEPTS.md for details and anti-patterns.
<!-- END BEADS CODEX SETUP -->
