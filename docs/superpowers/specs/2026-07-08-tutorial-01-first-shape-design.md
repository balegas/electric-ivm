# Tutorial series + Episodes 1–2 — design

**Date:** 2026-07-08 (episode 2 added same day)
**Status:** approved design, pre-implementation
**Audience for the tutorials:** developers curious about sync engines; educative enough that, over
the series, a reader understands how DBSP-style incremental view maintenance works.

## Goals

1. A tutorial series that teaches electric-circuits hands-on: shell + bare HTTP for shapes, `psql` for
   writes, the pipeline visualizer as the window into the engine.
2. Each episode doubles as an **end-to-end accuracy test of the visualizer**: every visual claim in
   the text is verified against the engine's own `/graph`, `/state`, and `/trace` output while the
   episode is written (pairing workflow, below). Episode 1 certifies the simplest pipeline
   (table → filter → shape); later episodes progressively certify sharing, subqueries, and
   aggregations.

## Series infrastructure (common to all episodes)

- **One compose file for the whole series:** `tutorials/compose.yaml`. It boots **all** services
  any episode needs, from the images in `docker/`:
  - `postgres` (16, `wal_level=logical`) — episode 1's schema+seed auto-applied via
    `docker-entrypoint-initdb.d`, so the database has data the moment the stack is up.
  - `ds` — durable-streams server (port 8791).
  - `engine` — Rust engine, control plane + `/v1/shape` (port 7010).
  - `api` — extended API (port 8790). Unused in episode 1; present so the compose never changes
    across episodes.
  - `viz` — **new**: the pipeline visualizer, containerized (see below). One fixed port
    (default 5180).
- **Episode state changes are SQL scripts, not compose changes.** Each episode directory ships the
  script that brings the database to its starting state
  (`psql <url> -f tutorials/episodes/0N-…/setup.sql`). When a script **adds tables**, the episode
  text must include `docker compose restart engine` (the engine introspects the table set at
  startup — documented behavior of the docker stack).
- **Layout:**

  ```
  tutorials/
    compose.yaml            # the series stack, used by every episode
    seed/01-init.sql        # episode 1 schema + seed, mounted into postgres initdb
    episodes/
      01-first-shape/README.md
      02-…/ (later)         # README + setup.sql per episode
  ```

- **Reset story:** `docker compose down -v && docker compose up` returns to episode 1's initial
  state from anywhere in the series.

### New build artifact: the viz container

`apps/pipeline-viz` is currently dev-server-only (Vite dev + `/engine/*` proxy). Add
`docker/Dockerfile.viz`: multi-stage build (pnpm build of the Vite app → static assets) served by
**Caddy** with a reverse-proxy rule `/engine/* → engine:7010`. Same URL contract as the dev
server, so the app code does not change. Compose wires `viz` to depend on `engine`.

## Episode 1 schema + seed (`tutorials/seed/01-init.sql`)

Single table, issue-tracker themed. Small enough that the reader can predict a shape's contents
before running the request:

```sql
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

(Exact rows may be tuned during writing; the invariant is: ~6 rows, a mix of statuses, so
`status = 'todo'` selects an obvious, predictable subset.)

## Episode 1 narrative (six short sections)

1. **What is a shape?** Lead with the concept: a shape is a live, incrementally maintained query
   result — "the rows matching this predicate, kept up to date forever". From that it follows
   where the data lives: your data stays in **Postgres**, the system of record; apps keep writing
   ordinary SQL; the engine turns logical replication into live shapes. Two paragraphs, no
   architecture diagrams.
2. **Start the stack.** `docker compose up` in `tutorials/`; one line on what each container is;
   verify with `curl :7010/health` and a `psql` `SELECT count(*) FROM issues` to see the seed.
3. **Open the visualizer** (`http://localhost:5180`). The canvas is **empty** — teaching moment:
   the engine maintains nothing until someone asks for a shape. Tell the reader to **keep this
   tab open** for the rest of the episode (it is the observer; the live animation in §5 only
   plays while it is watching).
4. **Create a shape.** The `/v1/shape` snapshot request with a SQL where-string:

   ```sh
   curl -i -G 'http://localhost:7010/v1/shape' \
     --data-urlencode "table=issues" \
     --data-urlencode "offset=-1" \
     --data-urlencode "where=status <> 'done'"
   ```

   **Predicate choice is deliberate:** a pure-equality predicate (`status = 'todo'`) compiles to
   the shared route-join family (`equality_template()`, `engine.rs:846-850`) — a stateful
   arrangement, and a sharing story that belongs later in the series. The inequality
   `status <> 'done'` ("open issues") is a **standalone σ filter**: the simplest, fully
   stateless pipeline, and the gentlest possible circuit for episode 2.

   Read the response together (change messages, `electric-handle` / `electric-offset` headers —
   save both). Then look back at the canvas: `table:issues → σ filter → shape out` has appeared.
   Explain each node card and its live chips (envelopes processed / emitted). Logical view only —
   the circuit view is deliberately not opened in this episode.
5. **Go live — one write, watched end to end.** Terminal A: the long-poll request
   (`…&handle=$HANDLE&offset=$OFFSET&live=true`). Terminal B: one `psql` INSERT of a new `todo`
   issue. With the visualizer open, the reader sees the same event three ways at once:
   - the green `+1` dot travels table → filter → shape and each node flashes **passed**;
   - the node chips increment;
   - the pending long-poll returns the insert delta.
   Close the loop explicitly: nothing re-queried Postgres — the engine evaluated one delta
   against the predicate and forwarded it.
   *Optional one-liner ("before you go, try it"):* `UPDATE … SET status='done'` on that row and
   watch it **leave** the shape as a delete. Marked optional; cut if it dilutes.
6. **What just happened + next episode.** Three-sentence recap of incremental maintenance (no
   polling, no re-query; every replicated change flows the pipeline once). Then the teaser:
   **Episode 2 — inside the pipeline**: the same shape, exploded into its real DBSP circuit —
   what the engine actually executes, and why an update is secretly a retraction plus an
   insertion.

## Episode 2 narrative: "Inside the pipeline — the DBSP circuit"

Same database initial state as episode 1 (no `setup.sql`; the episode opens with
`docker compose down -v && docker compose up` and re-creates the open-issues shape with the
episode-1 curl). The subject is the **Circuit view** of that one shape. Six sections:

1. **Recap + setup.** Reset the stack, re-run episode 1's shape request in one block (snapshot +
   handle/offset capture). Canvas shows the familiar three logical nodes.
2. **Flip to the Circuit view.** The same pipeline, exploded into the operators the engine
   actually executes: `source → Δ → σ → π → sink`, one box per real execution step. Key teaching
   point: this decomposition is **emitted by the engine** (`/graph`'s `operators`/`opEdges`),
   not drawn by the UI — what you see is the execution plan, and the episode says so.
3. **Deltas and weights.** With the visualizer open, one `psql` INSERT of a matching row. The
   green `+1` dot now travels *operator to operator*. Introduce the DBSP vocabulary gently: a
   change is a **weighted row** — `+1` means "now present", `−1` means "no longer present" — and
   every operator consumes and emits streams of these deltas. No math notation; the weights are
   already on screen.
4. **An update is a retraction + an insertion.** The episode's aha. First
   `UPDATE … SET title = …` on a row inside the shape (stays matching): at the delta level this
   is `−1` old row, `+1` new row; the shape output collapses it into one upsert (blue `±1` dot).
   Then `UPDATE … SET status = 'done'`: σ passes the `−1` (old row matched) and drops the `+1`
   (new row doesn't) — the net effect is the delete the reader saw in episode 1, now explained
   operator by operator. (Exact on-screen rendering of the two halves is confirmed during the
   verification pass; the text is written to match what the trace actually reports.)
5. **Stateless vs stateful.** Click each operator: σ and π are pure per-delta functions — this
   whole circuit keeps **no state**, which is why the engine can maintain the shape for free.
   Name what's coming: joins, subqueries, and aggregations need *arrangements* (state), and
   that's where the interesting DBSP machinery lives — pointing at the dashed-edge legend.
6. **Recap + teaser.** Recap: shapes compile to circuits of delta-operators; updates are
   retract+insert pairs; stateless operators are the easy case. Teaser for **episode 3 — shapes
   as resources**: the extended API, feeds from durable streams, and two clients sharing one
   pipeline.

**Episode 2 verification additions** (on top of the episode-1 checks): circuit-view DOM node
count vs `graph.operators` length; every operator id present and edged per `opEdges`; hop →
operator expansion during the INSERT animation (the dot must visit `src/d/sigma/pi/snk` boxes,
matching `hopIndex()` bindings); the update-in-place vs update-out writes produce the trace
outcomes the text claims.

## Verification pairing workflow (how each episode is built)

Every visual claim in the narrative becomes a check executed against the live stack before the
text ships:

1. Boot the episode's stack (`docker compose up` in `tutorials/`).
2. Drive the reader's exact steps with `curl` + `psql`, and the visualizer with the browser MCP.
3. Cross-check canvas vs engine at each step:
   - node/edge count on canvas vs `GET /engine/graph`;
   - chip values vs `GET /engine/state`;
   - animation outcomes (passed/dropped/routed/folded) vs the `/trace` SSE events;
   - shape contents vs `psql` ground truth.
4. Any mismatch is filed as a bead and fixed before the episode is considered done.

This makes each episode a reproducible acceptance test for the visualizer, which is the second
goal of the series.

## Tentative series map (for the teasers; each episode designed when reached)

1. **Your first live shape** — this spec.
2. **Inside the pipeline — the DBSP circuit** — this spec (added by user direction): the same
   shape exploded into operators; deltas, weights, update = retract+insert, stateless vs
   stateful.
3. **Shapes as resources** — extended API (`shapes.create` + JSON predicate AST), reading feeds
   from durable streams, shape sharing/de-duplication (incl. the equality route-join family —
   why `status = 'todo'` compiles to an index entry, not a filter).
4. **Cross-table shapes** — `IN (SELECT …)` subqueries, the shared inner-set node, live
   membership changes.
5. **Live aggregations** — count/sum/avg/min/max as incremental folds, the retraction multiset.

(Order/content of 3–5 are placeholders except where episode 2's teaser commits to episode 3's
topic.)

## Out of scope for episode 1 (tracked as beads)

- `ELECTRIC_CIRCUITS_TRACE=0` hard kill switch for the trace/graph/state endpoints (today tracing is
  near-zero-cost when unobserved, but there is no way to force it off; endpoints are also
  unauthenticated — production note belongs in a later episode or deployment docs).
- Auth story for the engine control plane.

## Open questions / risks

- **Long-poll ergonomics in a tutorial**: the reader must copy `electric-handle`/`electric-offset`
  from headers into the next request. The episode text uses a small shell snippet to capture them
  (`curl -si … | awk …` or similar) — keep it copy-pasteable and POSIX-sh friendly.
- **Timing**: the dot animation plays when the write lands; if the reader writes before looking
  at the tab, they miss it. §5's text orders the steps so the visualizer is watched at write time,
  and the sidebar Activity log (last 50 changes) is mentioned as the replay fallback.
