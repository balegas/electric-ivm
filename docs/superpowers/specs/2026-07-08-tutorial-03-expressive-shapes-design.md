# Tutorial Episode 3 — "Expressive shapes: subqueries & live aggregations" — design

**Date:** 2026-07-08
**Status:** approved design (arc + choices confirmed by user), pre-implementation
**Series doc:** `docs/superpowers/specs/2026-07-08-tutorial-01-first-shape-design.md`
**Tracking:** bd `dbsp-ds-unt`

## Why this episode, and why combined

Episodes 1–2 taught a single-table filter: `issues → σ → shape`, a stateless circuit.
Real queries do two things that a where-string on one table cannot: they **reach across
tables** (subqueries) and they **summarize** (aggregations). This episode covers both in one
combined episode (user's explicit choice), because together they make one point — this is where
the engine stops being stateless and starts keeping **arrangements** (the state ep2's teaser
promised). The two new nodes are already greyed in the visualizer legend: **`IN-set arrange`**
and **`Σ fold`**.

Both need a richer shape definition than ep1's `where=<sql-string>`. That is the *light-touch*
reason to introduce the **extended API** (`api` service, port 8790) and its **JSON predicate
AST**. The API is the **vehicle**, not the subject — the "shapes as resources / sharing /
route-join" material stays deferred to a later episode.

## Verified mechanics (smoke-tested against the live stack 2026-07-08)

Everything below was executed end-to-end before this spec was written; values are real.

### Extended API surface (port 8790, tRPC-over-HTTP, bare-HTTP form)

- **Create a shape:** `POST http://localhost:8790/shapes.create` with JSON body
  `{ table, where?: <predicate-AST>, columns?: string[] }`.
- **Create an aggregate:** `POST http://localhost:8790/aggregate.create` with
  `{ table, where?, fn: 'count'|'sum'|'avg'|'min'|'max', col? }`.
- Both return the tRPC envelope `{"result":{"data":{ shapeId, table, streamPath, streamUrl }}}`.
- **Predicate AST nodes** (from `packages/protocol/src/types.ts`):
  - leaf: `{"col":"status","op":"neq","value":"done"}` — ops `eq|neq|lt|lte|gt|gte`
  - null test: `{"col":"c","isNull":true}`
  - boolean: `{"and":[…]}` · `{"or":[…]}` · `{"not":…}`
  - subquery: `{"col":"project_id","in":{"table":"projects","project":"id","where":<pred>},"negated":false}`

### GOTCHA to surface in the episode — the returned `streamUrl` uses the container hostname

`shapes.create`/`aggregate.create` return `"streamUrl":"http://ds:8791/shape/s3"` — `ds` is the
**docker-internal** hostname. A reader on the host reads the feed at
**`http://localhost:8791/shape/<id>`** (same `streamPath`, host-reachable port). The episode must
say this explicitly, or the reader's first feed read fails. (Feed read semantics mirror ep1:
`?offset=-1` for backfill, `?offset=<next>&live=long-poll` to tail, `stream-next-offset` header
to resume.)

## Episode schema (`tutorials/episodes/03-expressive-shapes/setup.sql`)

Adds one table + one column so the subquery is genuinely cross-table. **Adds a table ⇒ episode
text must run `docker compose restart engine`** (series rule; the engine introspects the table
set at startup).

```sql
-- projects, and which project each issue belongs to
ALTER TABLE issues ADD COLUMN project_id bigint;

CREATE TABLE projects (
  id       bigint  PRIMARY KEY,
  name     text    NOT NULL,
  archived boolean NOT NULL DEFAULT false
);

INSERT INTO projects VALUES
  (10, 'Website',    false),
  (20, 'Mobile app', false),
  (30, 'Legacy',     true);   -- archived

UPDATE issues SET project_id = 10 WHERE id IN (1, 2);  -- Website  (active)
UPDATE issues SET project_id = 20 WHERE id IN (4, 5);  -- Mobile   (active)
UPDATE issues SET project_id = 30 WHERE id IN (3, 6);  -- Legacy   (archived)
```

Resulting open issues (`status <> 'done'`): 1 (p3, Website), 2 (p2, Website), 3 (p5, **Legacy**),
5 (p1, Mobile). Issue 3 is open **but its project is archived** — so the subquery shape excludes
it from the start, making the cross-table filter visible immediately (not only on churn).

## Six sections, one beat each

### 1. Setup + meet the extended API
Reset (`docker compose down -v && docker compose up -d --wait`), apply `setup.sql`, **restart the
engine**. Open the visualizer — now **two** table cards (`issues`, `projects`). Introduce port
8790 and the JSON-AST create by re-making ep1's open-issues shape through the new front door:
`POST :8790/shapes.create {"table":"issues","where":{"col":"status","op":"neq","value":"done"}}`.
Same shape, richer definition language. One line on the `{"result":{"data":…}}` envelope and the
returned `streamUrl` → read it at `localhost:8791`.

### 2. A cross-table shape (subquery)
Create **open issues in active projects**:
```json
{"table":"issues","where":{"and":[
  {"col":"status","op":"neq","value":"done"},
  {"col":"project_id","in":{"table":"projects","project":"id",
                            "where":{"col":"archived","op":"eq","value":false}}}
]}}
```
Read the feed once (`localhost:8791/shape/<id>?offset=-1`): **issues 1, 2, 5** — issue 3 is
absent (open, but Legacy is archived). On the canvas a new stateful node appears — **`IN-SET
ARRANGE · STATE`** (`projects · distinct id`), feeding the shape through a **`⋈ membership`**
semijoin. Explain: the engine materializes the *inner set* (active project ids) as a small
arrangement and routes each outer row's `project_id` against it. Verified `/graph`: one
`subqueryNode` `projects|id|L(archived,Eq,false)` (`distinctValues:2`, `refcount:1`), a
`subqueryEdge` to the shape on `connectingCol: project_id`, operators `sj:<id>` (`⋈ membership`)
+ `pi` + `snk`.

### 3. Live membership churn — the aha
With the viz open, archive a project: `UPDATE projects SET archived = true WHERE id = 10`.
Issues **1 and 2 leave** the shape — verified feed deltas `delete key 1`, `delete key 2` — even
though **neither issue row changed**. The `/trace` shows it in two hops: the `projects` update
flows `table:projects → node:projects|id|archived=false` (the inner set drops project 10), then a
second event moves issues 1, 2 out of `shape:s<id>`. Close the loop: **a write to one table moved
rows in another table's shape.** Single-table filters can't do this. (Optional reverse:
`UPDATE projects SET archived = false WHERE id = 30` → issue 3 *enters*.)

### 4. Live aggregations
Different question — not "which rows" but "how many". `POST :8790/aggregate.create
{"table":"issues","where":{status≠done},"fn":"count"}`. The canvas shows a **`Σ FOLD · STATE`**
node reading **`COUNT(*) · = 4 · n=4`**. Insert an open issue / close one and the scalar updates
**incrementally** — a fold over the delta stream, no recount. (Feed form:
`{"key":"agg","value":{"n":4,"value":4}}`.)

### 5. The retraction multiset — why folds are stateful
The sharp case is `MIN`. `aggregate.create {…,"fn":"min","col":"priority"}` → node
**`Σ FOLD · STATE · MIN(priority) · = 1 · 4 in multiset`**. The current min is 1 (issue 5). Now
**close issue 5** (`UPDATE issues SET status='done' WHERE id=5`): the min can't just "look at the
removed row" — it must know the *next* smallest. It does, because the fold keeps a **multiset** of
contributing values (the node literally shows `N in multiset`, `/state` shows
`multisetLen`/`nnCount`); the `−1` retraction removes priority 1 and the min recomputes to **2**
(issue 2). This is the arrangement/state ep2 promised — contrast the stateless `σ` of ep2, which
stored nothing.

### 6. Recap + teaser
Subqueries = cross-table membership via a **shared inner-set arrangement**; aggregations =
**incremental folds**, stateful because a retraction has to reveal the new result. Both are the
engine *remembering just enough*. Teaser → next episode: **shapes as resources** — the extended
API as a resource model, reading feeds straight from the log, and what happens when two clients
ask for the *same* shape (one shared pipeline: the `↦⋈ route join` family).

## Verification additions (pairing workflow, on top of ep1/2 checks)

- `/graph` `subqueryNodes`/`subqueryEdges` present and wired to the subquery shape; operator set
  includes `⋈ membership` + shared inner-set node; viz renders **`IN-SET ARRANGE`**.
- Cross-table churn: archiving/among-projects writes produce the exact `delete`/`upsert` feed
  deltas the text claims; `/trace` shows the two-table propagation.
- Aggregates: `Σ FOLD` node value == `psql` ground truth for COUNT and MIN; after closing the min
  row, the recomputed MIN matches; `/state` `multisetLen` corroborates §5's multiset claim.
- The `streamUrl` host-name caveat is stated before the first feed read.

## Consequences handled as part of the work

1. **Ep2 teaser rewrite.** `tutorials/episodes/02-inside-the-pipeline/README.md` §6 currently
   promises "Episode 3, Shapes as resources." Re-point it to subqueries + aggregations.
2. **Series map update.** In the series design doc, swap episode 3 ("Shapes as resources") to this
   topic and push the resources/sharing material to a later slot (it becomes the new teaser
   target). Episodes 4–5 of the old map (cross-table / aggregations) are absorbed here.

## Open questions / risks

- **Density.** Two topics in one episode is more than ep1/2 carried. Mitigation: strictly one beat
  per section, viz-primary observation (psql for writes, one feed read to prove the stream),
  aggregates reuse the `issues` table (no second schema hop).
- **`streamUrl` hostname** (see gotcha) — must be called out or the first read fails.
- **Restart timing.** `docker compose restart engine` must complete (health check) before the
  first `shapes.create`; the text should tell the reader to wait for `curl :7010/health → ok`.
