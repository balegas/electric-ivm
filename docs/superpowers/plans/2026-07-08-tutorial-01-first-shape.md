# Tutorial Episodes 1–2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A self-contained `tutorials/` stack (one compose for the whole series, seeded Postgres, containerized pipeline visualizer) plus the episode-1 walkthrough ("Your first live shape") and episode-2 walkthrough ("Inside the pipeline — the DBSP circuit"), with every visual claim verified against the live engine.

**Architecture:** Reuse the existing `docker/Dockerfile.engine` and `docker/Dockerfile.node` images; add one new image (`docker/Dockerfile.viz`: Vite static build served by Caddy, reverse-proxying `/engine/*` → `engine:7010` — same URL contract as the Vite dev server). `tutorials/compose.yaml` boots all five services with episode 1's schema+seed auto-applied by Postgres initdb. The episode README drives everything over bare `curl` + `psql` inside the postgres container.

**Tech Stack:** Docker Compose, Caddy 2, Vite (existing app, no code changes), Postgres 16, the Rust engine as-is.

**Spec:** `docs/superpowers/specs/2026-07-08-tutorial-01-first-shape-design.md`
**Bead:** `dbsp-ds-0bi`

## Global Constraints

- **Git policy is conservative** (CLAUDE.md): propose each commit and wait for user approval before running it. Branch: work continues on `shape-retention` unless the user redirects.
- **No app code changes** to `apps/pipeline-viz` or the engine in this plan — the viz container must work with the app exactly as built (`fetch('/engine/…')` relative paths).
- Fixed tutorial ports: postgres `5432`, ds `8791`, engine `7010`, api `8790`, viz `5180` (all overridable via env like `docker/compose.yaml`).
- The engine introspects the table set **at startup** — any future episode script that adds tables must be documented with `docker compose restart engine`. Episode 1 needs no restart (seed runs before the engine's first successful boot; `restart: unless-stopped` covers ordering).
- Repo-root build context with the existing `.dockerignore` (excludes `**/node_modules`, `target`, `**/dist`).
- Docs realignment happens in the same pass as the code (AGENTS.md gotcha: "Changing code means realigning docs").

---

### Task 1: The viz container image

**Files:**
- Create: `docker/Dockerfile.viz`
- Create: `docker/viz.Caddyfile`

**Interfaces:**
- Consumes: `apps/pipeline-viz` (existing `pnpm build` → `dist/`), repo-root build context.
- Produces: image serving the built app on `:5180`, proxying `/engine/*` (prefix stripped) to `{$ENGINE_UPSTREAM}` (default `engine:7010`). Task 2's compose references `dockerfile: docker/Dockerfile.viz`.

- [ ] **Step 1: Write `docker/viz.Caddyfile`**

```caddyfile
# Serves the built pipeline visualizer and proxies /engine/* to the engine's control plane —
# the same URL contract as the app's Vite dev server (vite.config.ts), so the app code is
# identical in dev and in the container. ENGINE_UPSTREAM overrides the target (host:port).
:5180 {
	handle_path /engine/* {
		reverse_proxy {$ENGINE_UPSTREAM:engine:7010}
	}
	handle {
		root * /srv
		try_files {path} /index.html
		file_server
	}
}
```

(`handle_path` strips the `/engine` prefix — the exact equivalent of the dev proxy's `rewrite`.)

- [ ] **Step 2: Write `docker/Dockerfile.viz`**

```dockerfile
# electric-ivm VIZ image: the pipeline visualizer as static assets behind Caddy, which also
# reverse-proxies /engine/* to the engine control plane (same contract as the Vite dev proxy).
#
# Build context is the repo root:
#   docker build -f docker/Dockerfile.viz -t electric-ivm-viz .

FROM node:22-slim AS build
RUN corepack enable
WORKDIR /repo
COPY . .
RUN pnpm install --filter @electric-ivm/pipeline-viz...
RUN pnpm --filter @electric-ivm/pipeline-viz build

FROM caddy:2-alpine
COPY docker/viz.Caddyfile /etc/caddy/Caddyfile
COPY --from=build /repo/apps/pipeline-viz/dist /srv
EXPOSE 5180
```

- [ ] **Step 3: Build the image**

Run: `docker build -f docker/Dockerfile.viz -t electric-ivm-viz /Users/vbalegas/workspace/dbsp-ds`
Expected: builds to completion; the `vite build` step prints `✓ built in …` and emits `dist/index.html`.
(If `pnpm install --filter @electric-ivm/pipeline-viz...` fails on workspace resolution, fall back to plain `RUN pnpm install` — same as `Dockerfile.node`.)

- [ ] **Step 4: Smoke-test the image standalone**

Run:
```bash
docker run -d --name viz-smoke -p 5181:5180 -e ENGINE_UPSTREAM=127.0.0.1:9 electric-ivm-viz
sleep 1
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:5181/          # expect 200
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:5181/engine/health  # expect 502 (no engine — proves the proxy route exists)
docker rm -f viz-smoke
```
Expected: `200` then `502`.

- [ ] **Step 5: Propose commit (wait for approval)**

```bash
git add docker/Dockerfile.viz docker/viz.Caddyfile
git commit -m "feat(docker): containerize the pipeline visualizer (static build behind Caddy /engine proxy)"
```

---

### Task 2: The series compose + episode 1 seed

**Files:**
- Create: `tutorials/compose.yaml`
- Create: `tutorials/seed/01-init.sql`

**Interfaces:**
- Consumes: `docker/Dockerfile.{engine,node,viz}` (Task 1), repo-root contexts.
- Produces: `docker compose up` from `tutorials/` boots postgres(seeded)+ds+engine+api+viz; Task 3's README instructs exactly this. Reset contract: `docker compose down -v` → pristine episode-1 state.

- [ ] **Step 1: Write `tutorials/seed/01-init.sql`**

```sql
-- Episode 1 initial state: a single issue-tracker table, small enough that the reader can
-- predict every shape result by looking at it. Applied automatically by Postgres initdb on
-- first boot (docker-entrypoint-initdb.d); `docker compose down -v` resets to this state.

CREATE TABLE issues (
  id        bigint PRIMARY KEY,
  title     text   NOT NULL,
  status    text   NOT NULL DEFAULT 'todo',   -- 'todo' | 'doing' | 'done'
  priority  bigint NOT NULL DEFAULT 0
);

INSERT INTO issues VALUES
  (1, 'fix the flaky test',      'todo',  3),
  (2, 'write the release notes', 'doing', 2),
  (3, 'ship the login page',     'todo',  5),
  (4, 'triage the inbox',        'done',  1),
  (5, 'update the onboarding',   'todo',  1),
  (6, 'refactor the tailer',     'done',  4);
```

- [ ] **Step 2: Write `tutorials/compose.yaml`**

```yaml
# The tutorial-series stack — every service any episode needs, boots with episode 1's data:
#
#   postgres — system of record (wal_level=logical), auto-seeded from ./seed on first boot
#   ds       — durable-streams server (the log)
#   engine   — the Rust engine: replication ingest + shape maintenance + GET /v1/shape (7010)
#   api      — the extended tRPC API (unused until episode 2, present so this file never changes)
#   viz      — the pipeline visualizer (http://localhost:5180), proxying /engine/* to the engine
#
#   cd tutorials && docker compose up --build
#
# Reset to episode 1's initial state from anywhere in the series:
#   docker compose down -v && docker compose up
#
# Later episodes change database state with SQL scripts (episodes/0N-…/setup.sql), not by
# editing this file. The engine introspects the table set at startup — an episode script that
# ADDS tables is always followed by `docker compose restart engine` in the episode text.

name: electric-ivm-tutorials

services:
  postgres:
    image: postgres:16
    command: ["postgres", "-c", "wal_level=logical"]
    environment:
      POSTGRES_PASSWORD: password
      POSTGRES_DB: electric
    ports:
      - "${PG_PORT:-5432}:5432"
    volumes:
      - pg-data:/var/lib/postgresql/data
      - ./seed:/docker-entrypoint-initdb.d:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d electric"]
      interval: 3s
      timeout: 3s
      retries: 20

  ds:
    build:
      context: ..
      dockerfile: docker/Dockerfile.node
    command: ["pnpm", "run", "ds"]
    environment:
      DS_PORT: 8791
      DS_DATA_DIR: /data
    ports:
      - "${DS_PORT:-8791}:8791"
    volumes:
      - ds-data:/data
    healthcheck:
      test: ["CMD-SHELL", "node -e \"fetch('http://127.0.0.1:8791').then(()=>process.exit(0),()=>process.exit(1))\""]
      interval: 3s
      timeout: 3s
      retries: 20

  engine:
    build:
      context: ..
      dockerfile: docker/Dockerfile.engine
    environment:
      ELECTRIC_IVM_DS_URL: http://ds:8791
      ELECTRIC_IVM_PG_URL: postgres://postgres:password@postgres:5432/electric
      ELECTRIC_IVM_PG_TABLES: "*"
      ELECTRIC_IVM_BIND: 0.0.0.0:7010
    ports:
      - "${ENGINE_PORT:-7010}:7010"
    depends_on:
      postgres:
        condition: service_healthy
      ds:
        condition: service_healthy
    restart: unless-stopped

  api:
    build:
      context: ..
      dockerfile: docker/Dockerfile.node
    command: ["pnpm", "run", "api"]
    environment:
      DS_URL: http://ds:8791
      ENGINE_URL: http://engine:7010
      API_PORT: 8790
    ports:
      - "${API_PORT:-8790}:8790"
    depends_on:
      ds:
        condition: service_healthy
      engine:
        condition: service_started

  viz:
    build:
      context: ..
      dockerfile: docker/Dockerfile.viz
    environment:
      ENGINE_UPSTREAM: engine:7010
    ports:
      - "${VIZ_PORT:-5180}:5180"
    depends_on:
      engine:
        condition: service_started

volumes:
  pg-data:
  ds-data:
```

- [ ] **Step 3: Boot and verify the whole stack**

Run: `cd /Users/vbalegas/workspace/dbsp-ds/tutorials && docker compose up --build -d && sleep 20`
Then:
```bash
curl -s http://localhost:7010/health                                   # → ok
docker compose exec postgres psql -U postgres -d electric -tAc 'SELECT count(*) FROM issues'   # → 6
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:5180/        # → 200
curl -s http://localhost:5180/engine/health                            # → ok   (proxy through Caddy)
curl -s 'http://localhost:5180/engine/graph' | head -c 200             # → JSON with "tables"
```
Expected: all five outputs as annotated.

- [ ] **Step 4: Verify the reset contract**

Run: `docker compose down -v && docker compose up -d && sleep 20 && docker compose exec postgres psql -U postgres -d electric -tAc 'SELECT count(*) FROM issues'`
Expected: `6` (re-seeded from scratch).

- [ ] **Step 5: Propose commit (wait for approval)**

```bash
git add tutorials/compose.yaml tutorials/seed/01-init.sql
git commit -m "feat(tutorials): series compose stack with episode-1 seeded Postgres and viz container"
```

---

### Task 3: Episode 1 README (full walkthrough text)

**Files:**
- Create: `tutorials/episodes/01-first-shape/README.md`

**Interfaces:**
- Consumes: the running Task-2 stack; `docs/getting-started.md` §3 for the `/v1/shape` protocol details it links to.
- Produces: the reader-facing walkthrough; Task 4 executes it verbatim as the acceptance test.

- [ ] **Step 1: Write the README**

Write `tutorials/episodes/01-first-shape/README.md` with exactly this structure and content (copy edits welcome, structural changes are not — the spec's six sections):

````markdown
# Episode 1 — Your first live shape

## 1. What is a shape?

A **shape** is a live query result: "the rows matching this predicate", kept up to date
forever. You ask for it once; from then on, every change that affects it is pushed to you as a
delta — no polling, no re-running the query.

Your data doesn't move anywhere to make this work. It lives in **Postgres**, the system of
record, and your apps keep writing to it with ordinary SQL. The **electric-ivm engine** tails
Postgres logical replication and maintains every shape incrementally: each committed change
flows once through a small pipeline of operators, and only the shapes it affects hear about it.
In this episode you'll create one shape, watch its pipeline get built, and watch one write flow
through it — live, on screen.

## 2. Start the stack

From the `tutorials/` directory:

```sh
docker compose up --build
```

Five containers come up: **postgres** (already seeded with a tiny `issues` table), **ds** (the
durable-streams log), the **engine**, an **api** server (ignore it until episode 2), and the
**visualizer**. Check the engine is up and the data is there:

```sh
curl http://localhost:7010/health
# → ok
docker compose exec postgres psql -U postgres -d electric -c 'TABLE issues'
# → 6 issues: three 'todo', one 'doing', two 'done'
```

## 3. Open the visualizer — and keep it open

Open **http://localhost:5180**. The canvas is **empty**.

That's the first lesson: the engine maintains nothing until someone asks. There are no
pipelines for tables nobody is watching — a shape is what brings a pipeline into existence.

Keep this tab open (and visible) for the rest of the episode. The live animation you'll see in
§5 plays as changes happen — it's the pipeline working, not a replay.

## 4. Create a shape

Ask for every issue that's still open — anything not `done`. This one request creates the shape
and returns its current rows:

```sh
RES=$(curl -si -G 'http://localhost:7010/v1/shape' \
  --data-urlencode "table=issues" \
  --data-urlencode "offset=-1" \
  --data-urlencode "where=status <> 'done'")
printf '%s\n' "$RES"
```

You get back four insert messages — issues 1, 2, 3, and 5, exactly the not-done rows in the
seed — followed by an `up-to-date` control message. Two response headers matter; save them:

```sh
HANDLE=$(printf '%s' "$RES" | awk 'tolower($1)=="electric-handle:"{print $2}' | tr -d '\r')
OFFSET=$(printf '%s' "$RES" | awk 'tolower($1)=="electric-offset:"{print $2}' | tr -d '\r')
```

Now look at the visualizer: three nodes appeared.

- **`issues` (table · Δ source)** — the replication source; its chips count envelopes processed.
- **σ filter** — your predicate, `status <> 'done'`, evaluated on every delta from the table.
- **shape out · π** — the shape's output stream; its chip counts envelopes emitted (your four
  backfill rows are already on it).

This *is* the pipeline the engine built for your request — not a diagram of it. The node ids,
counters, and edges come from the engine's own introspection endpoints.

## 5. Go live — one write, watched end to end

Two terminals. In **terminal A**, start a long-poll on the shape's tail — it will hang, waiting
for the next change:

```sh
curl -i -G 'http://localhost:7010/v1/shape' \
  --data-urlencode "table=issues" \
  --data-urlencode "handle=$HANDLE" \
  --data-urlencode "offset=$OFFSET" \
  --data-urlencode "live=true"
```

Arrange your windows so you can see the **visualizer** and terminal A at the same time. Then,
in **terminal B**, insert one matching row with plain SQL:

```sh
docker compose exec postgres psql -U postgres -d electric \
  -c "INSERT INTO issues VALUES (7, 'review this tutorial', 'todo', 2)"
```

Watch the same event arrive three ways at once:

1. **On the canvas**, a green `+1` dot travels `issues → σ → shape` and each node flashes as it
   passes; the chips increment.
2. **Terminal A's long-poll returns** with the insert delta for issue 7 and fresh
   `electric-handle` / `electric-offset` headers (loop with the new offset to keep tailing).
3. **The sidebar Activity log** records the change — click it to replay the animation if you
   looked away at the wrong moment.

Nothing re-queried Postgres. The engine took one replicated change, evaluated your predicate
against it once, and forwarded it to the one shape that cared.

**Before you go, try the reverse** *(optional)*: mark the new issue done —
`UPDATE issues SET status = 'done' WHERE id = 7` — and the next long-poll returns a **delete**:
the row didn't vanish from Postgres, it left *your shape*.

## 6. What just happened

You created a live query with one HTTP request; the engine built a dataflow pipeline for it;
and a plain SQL write was evaluated **incrementally** — one delta through one filter, not a
re-run of the query. That is the whole idea this series builds on: the engine is a DBSP-style
incremental view maintenance system, and everything you'll see later — shared pipelines,
cross-table subqueries, live aggregations — is this same picture with more operators.

**Next — Episode 2, Inside the pipeline:** the same shape, exploded into the DBSP circuit the
engine actually executes — and why an update is secretly a retraction plus an insertion.
````

- [ ] **Step 2: Dry-run every command block**

Run each `sh` block from the README verbatim against the running stack (fresh `down -v && up`
first). Expected: outputs match the prose (4 inserts for §4; §5's long-poll returns issue 7's
insert; optional UPDATE returns a delete for key `7`).

- [ ] **Step 3: Propose commit (wait for approval)**

```bash
git add tutorials/episodes/01-first-shape/README.md
git commit -m "docs(tutorials): episode 1 — your first live shape"
```

---

### Task 4: Paired end-to-end verification (visualizer accuracy)

**Files:**
- Modify: `tutorials/episodes/01-first-shape/README.md` (only if checks force copy changes)
- No other file changes expected; mismatches become beads, not inline fixes.

**Interfaces:**
- Consumes: the full Task-2 stack + Task-3 README; browser MCP; `/engine/graph`, `/engine/state`, `/engine/trace`.
- Produces: a verified episode (the spec's goal #2) and zero-or-more beads for visualizer bugs.

This task is interactive — done live with the user watching where useful.

- [ ] **Step 1: Fresh boot** — `docker compose down -v && docker compose up -d`, wait healthy.
- [ ] **Step 2: Empty-canvas check** — browser MCP to `http://localhost:5180`; assert `document.querySelectorAll('.react-flow__node').length === 0` and `/engine/graph` reports no shapes.
- [ ] **Step 3: Shape-creation check** — run §4's curl; assert canvas gains exactly 3 nodes / 2 edges without a reload; cross-check ids against `await (await fetch('/engine/graph')).json()` (one table, one standalone filter shape); screenshot.
- [ ] **Step 4: Chip-accuracy check** — compare each node's chip values against `GET /engine/state` (shape emit count must be 4 after backfill).
- [ ] **Step 5: Live-write check** — start §5's long-poll, run the INSERT; verify (a) trace SSE emits one data event with hops `table → filter(passed) → shape`, (b) the dot/flash plays (sample `.react-flow__edge g circle` positions via the Activity-log replay for determinism), (c) chips increment by exactly 1, (d) long-poll body carries the issue-7 insert.
- [ ] **Step 6: Leave-the-shape check (the optional aside)** — run the UPDATE; verify the delete delta and the filter's outcome on the trace.
- [ ] **Step 7: File beads for any mismatch** — one bead per discrepancy (`bd create … --type=bug`), linked to `dbsp-ds-0bi`; fix separately.
- [ ] **Step 8: Close the loop** — re-run any README step whose text changed; screenshot set saved to the scratchpad and shared in the session summary.

---

### Task 5: Docs realignment

**Files:**
- Modify: `AGENTS.md` (Layout table + the demo/visualizer section)
- Modify: `apps/pipeline-viz/README.md` ("Run it" section)
- Modify: `docs/getting-started.md` (one pointer line in the intro)

**Interfaces:**
- Consumes: everything above, merged.
- Produces: docs that mention the tutorials stack wherever the demo stack is mentioned today.

- [ ] **Step 1: AGENTS.md** — add a Layout row: `| tutorials/ | Tutorial series: one compose stack (postgres+ds+engine+api+viz) + per-episode walkthroughs (episodes/01-first-shape, 02-inside-the-pipeline). |` and one sentence in the demo/visualizer runbook noting the containerized viz (`docker/Dockerfile.viz`, Caddy `/engine/*` proxy) as a third way to run the visualizer.
- [ ] **Step 2: apps/pipeline-viz/README.md** — extend "Run it" with the containerized form: `cd tutorials && docker compose up` → `http://localhost:5180`, noting `ENGINE_UPSTREAM` for pointing the container at another engine.
- [ ] **Step 3: docs/getting-started.md** — add one line to the intro: hands-on learners should start with `tutorials/episodes/01-first-shape/README.md`.
- [ ] **Step 4: Mirror substantive AGENTS.md edits into CLAUDE.md** if the touched sections overlap (per the repo note that they are independent files).
- [ ] **Step 5: Propose commit (wait for approval)**

```bash
git add AGENTS.md CLAUDE.md apps/pipeline-viz/README.md docs/getting-started.md
git commit -m "docs: point AGENTS/getting-started/viz README at the tutorials stack"
```

---

### Task 6: Episode 2 README ("Inside the pipeline — the DBSP circuit")

**Files:**
- Create: `tutorials/episodes/02-inside-the-pipeline/README.md`

**Interfaces:**
- Consumes: the Task-2 stack unchanged (no new seed, no setup.sql — the episode opens with a reset); episode 1's shape request.
- Produces: the reader-facing episode 2; Task 7 executes it verbatim.

- [ ] **Step 1: Write the README**

````markdown
# Episode 2 — Inside the pipeline: the DBSP circuit

In episode 1 you created a live shape and watched one write flow through
`issues → σ → shape`. Those three boxes are the *logical* view — honest, but summarized. This
episode opens the hood: the operator circuit the engine actually executes, and the two ideas
that make incremental view maintenance work — **deltas** and **weights**.

## 1. Where we left off

Reset to episode 1's starting state and re-create the open-issues shape:

```sh
docker compose down -v && docker compose up -d --wait

RES=$(curl -si -G 'http://localhost:7010/v1/shape' \
  --data-urlencode "table=issues" \
  --data-urlencode "offset=-1" \
  --data-urlencode "where=status <> 'done'")
HANDLE=$(printf '%s' "$RES" | awk 'tolower($1)=="electric-handle:"{print $2}' | tr -d '\r')
OFFSET=$(printf '%s' "$RES" | awk 'tolower($1)=="electric-offset:"{print $2}' | tr -d '\r')
```

Open **http://localhost:5180** — the three familiar nodes are back. Keep the tab open.

## 2. Two views of one pipeline

Switch the canvas to the **dbsp circuit** view (the view toggle in the top bar). The same shape
explodes into five boxes:

- **source** — the replication tap on `issues`: every committed change enters here.
- **Δ (delta)** — changes leave the source as a *delta stream*: not rows, but row *changes*.
- **σ (filter)** — your predicate, applied to each delta.
- **π (project)** — trims each surviving delta to the shape's columns.
- **sink** — groups the result by primary key into the upsert/delete envelopes on your feed.

One box per real execution step: this decomposition is **reported by the engine itself**
(`GET /graph` returns the operator list), not drawn by the UI. You are looking at the execution
plan, live.

## 3. Deltas and weights

In a second terminal, insert a matching row — and watch the circuit while it lands:

```sh
docker compose exec postgres psql -U postgres -d electric \
  -c "INSERT INTO issues VALUES (7, 'review this tutorial', 'todo', 2)"
```

The green dot is labeled **`+1`**, and that label is the core of DBSP: a change is a **weighted
row**. `+1` means "this row is now present"; `−1` means "this row is no longer present". Every
operator in the circuit consumes a stream of weighted rows and emits one — σ passes or drops
them, π reshapes them, the sink turns them into feed envelopes. Nothing anywhere re-reads the
table.

## 4. An update is a retraction plus an insertion

Here is the trick the whole engine turns on. In SQL you think of an update as "the row changed
in place". A delta stream has no such thing — an update is **two weighted rows**:
`−1 × (old row)` and `+1 × (new row)`.

Watch it. First, an update that *keeps* the row in the shape:

```sh
docker compose exec postgres psql -U postgres -d electric \
  -c "UPDATE issues SET title = 'review this tutorial twice' WHERE id = 7"
```

Both halves survive σ (old and new row are open issues), and the sink collapses them into a
single **upsert** on your feed — on canvas the dot runs blue, `±1`.

Now the update you already saw in episode 1, re-explained:

```sh
docker compose exec postgres psql -U postgres -d electric \
  -c "UPDATE issues SET status = 'done' WHERE id = 7"
```

σ passes the `−1` (the old row matched) and **drops** the `+1` (the new row doesn't). Only the
retraction reaches the sink — and that is *exactly* why your long-poll gets a `delete`: not
because anything was deleted in Postgres, but because the surviving half of the update says
"this row is no longer present *in this shape*".

## 5. Stateless vs stateful

Click the σ box: its detail panel shows what it evaluates, and note what it *doesn't* have —
stored rows. σ and π are pure per-delta functions; this entire circuit keeps **no state**,
which is why the engine can maintain a shape like this for next to nothing.

The interesting DBSP machinery starts when a circuit *must* remember things: equality routing
uses a shared index, joins and subqueries keep **arrangements** (the dashed edges in the
legend), and aggregations keep folds. That state — and how it stays small — is where the series
goes next.

## 6. What you now know

Shapes compile to circuits of operators that pass weighted row-changes; an update is a
retraction plus an insertion; and a predicate like yours needs no state at all. When a reader
asks you "how does the engine know a row *left* a query result without re-running it?", you now
know the answer: the `−1` told it.

**Next — Episode 3, Shapes as resources:** creating shapes with the extended API, reading
feeds straight from the durable-streams log, and what happens when two clients ask for the
same shape (spoiler: one pipeline, on screen).
````

- [ ] **Step 2: Dry-run every command block**

Fresh `down -v && up`, then run each `sh` block verbatim. Expected: §1 returns 4 inserts; §3's
INSERT animates in circuit view; §4's first UPDATE yields an upsert envelope on the long-poll,
the second yields a delete for key `7`.

- [ ] **Step 3: Propose commit (wait for approval)**

```bash
git add tutorials/episodes/02-inside-the-pipeline/README.md
git commit -m "docs(tutorials): episode 2 — inside the pipeline (dbsp circuit view)"
```

---

### Task 7: Episode 2 paired verification (circuit-view accuracy)

**Files:**
- Modify: `tutorials/episodes/02-inside-the-pipeline/README.md` (copy fixes only; visualizer bugs become beads)

**Interfaces:**
- Consumes: Task-6 README, the running stack, browser MCP, `/engine/graph` (`operators`/`opEdges`), `/engine/trace`.
- Produces: a verified episode 2; beads for any circuit-view inaccuracy.

This task is interactive — done live with the user where useful.

- [ ] **Step 1: Fresh boot + shape** — run §1 verbatim.
- [ ] **Step 2: Circuit topology check** — switch to circuit view via browser MCP; assert canvas operator boxes == `graph.operators.length` for this shape's closure and every edge in `opEdges` is rendered; record the actual operator ids (`src:`/`d:`/`sigma:`/`pi:`/`snk:`) and fix §2's box list if the engine emits a different set.
- [ ] **Step 3: Hop-expansion check** — run §3's INSERT; on the trace SSE, capture the data event and verify the circuit animation visits exactly the operator boxes bound to each hop (`hop` field on `OpNode`); dot label must read `+1`.
- [ ] **Step 4: Update-in-place check** — §4 first UPDATE: verify the trace outcome at σ, the blue `±1` rendering, and a single upsert envelope on the long-poll. **If the on-screen rendering does not show the two-halves story the text tells, adjust the text to what actually renders — or file a viz bead if the engine reports the halves and the UI hides them.**
- [ ] **Step 5: Update-out check** — §4 second UPDATE: verify σ's dropped/passed outcomes on the trace and the delete envelope.
- [ ] **Step 6: Detail-panel check** — click σ and π; §5's "no stored rows" claim must match what the panels actually show.
- [ ] **Step 7: File beads / fix copy** — mismatches in engine-vs-canvas become beads linked to `dbsp-ds-0bi`; prose mismatches are fixed inline and the affected steps re-run.

---

## Self-review notes

- **Spec coverage:** series compose (Task 2), initdb seed (Task 2), viz container (Task 1), episode-1 narrative with all six sections incl. single-write §5 + teaser §6 (Task 3), episode-1 verification (Task 4), docs (Task 5), episode-2 narrative per the spec's episode-2 section (Task 6), episode-2 circuit verification (Task 7). The spec's out-of-scope items stay out (`dbsp-ds-58p`).
- **No placeholders:** all file contents are written out above; the only intentionally open text is copy-editing of the READMEs during Tasks 4/7 (explicitly bounded to what verification observes).
- **Consistency:** `ENGINE_UPSTREAM` is the single new env name, used identically in the Caddyfile, Dockerfile.viz smoke test, and compose. The predicate is `status <> 'done'` everywhere (spec §4 rationale: equality predicates compile to the route-join family, `engine.rs:846-850`; the inequality yields the standalone stateless σ pipeline both episodes rely on). Episode 1's teaser matches episode 2's title; episode 2's teaser matches the series map's episode 3. Ports match the Global Constraints everywhere.
