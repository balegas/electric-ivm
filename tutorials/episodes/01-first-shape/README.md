# Episode 1 — Your first live shape

## 1. What is a shape?

A **shape** is a live query result: "the rows matching this predicate", kept up to date forever. You ask for it once; from then on, every change that affects it is pushed to you as a delta — no polling, no re-running the query.

Your data doesn't move anywhere to make this work. It lives in **Postgres**, the system of record, and your apps keep writing to it with ordinary SQL. The **electric-circuits engine** tails Postgres logical replication and maintains every shape incrementally: each committed change flows once through a small pipeline of operators, and only the shapes it affects hear about it. In this episode you'll create one shape, watch its pipeline get built, and watch one write flow through it — live, on screen.

## 2. Start the stack

From the `tutorials/` directory:

```sh
docker compose up --build
```

Five containers come up: **postgres** (already seeded with a tiny `issues` table), **ds** (the durable-streams log), the **engine**, an **api** server (the extended tRPC API — this series never needs it; it's here so the stack file never changes), and the **visualizer**. (If a port is already taken on your machine — a local Postgres on 5432 is the usual culprit — override it: `PG_PORT=15432 docker compose up --build`; same idea for `DS_PORT`, `ENGINE_PORT`, `API_PORT`, `VIZ_PORT`, and `VIZ_HTTPS_PORT`.)

In a second terminal, check the engine is up and the data is there:

```sh
curl http://localhost:7010/health
# → ok
psql "postgres://postgres:password@localhost:5432/electric" -c 'TABLE issues'
# → 6 issues: three 'todo', one 'doing', two 'done'
```

(If you overrode `PG_PORT`, use that port in the connection URL. No `psql` installed? The series [README](../../README.md) shows the in-container equivalent.)

## 3. Open the visualizer — and keep it open

Open **https://localhost:5543** (click through the certificate warning the first time, or trust the local CA once — see [Trusting the visualizer's certificate](../../README.md#trusting-the-visualizers-certificate)). The canvas shows a single card — the `issues` table, with `0 env` on its counter — and nothing else.

That's the first lesson: the engine is already tailing the table's replication stream (that's the card), but it maintains **nothing** beyond that until someone asks. There are no pipelines for data nobody is watching — a shape is what brings a pipeline into existence.

Keep this tab open (and visible) for the rest of the episode. The live animation you'll see in §5 plays as changes happen — it's the pipeline working, not a replay.

## 4. Create a shape

Ask for every issue that's still open — anything not `done`. This one request creates the shape and returns its current rows:

```sh
RES=$(curl -si -G 'http://localhost:7010/v1/shape' \
  --data-urlencode "table=issues" \
  --data-urlencode "offset=-1" \
  --data-urlencode "where=status <> 'done'")
printf '%s\n' "$RES"
```

You get back four insert messages — issues 1, 2, 3, and 5, exactly the not-done rows in the seed — followed by an `up-to-date` control message. Two response headers matter; save them:

```sh
HANDLE=$(printf '%s' "$RES" | awk 'tolower($1)=="electric-handle:"{print $2}' | tr -d '\r')
OFFSET=$(printf '%s' "$RES" | awk 'tolower($1)=="electric-offset:"{print $2}' | tr -d '\r')
```

Now look at the visualizer: two new nodes just appeared next to the table card, making three.

- **`issues` (table · Δ source)** — the replication source; its chips count envelopes processed.
- **σ filter** — your predicate, `status <> 'done'`, evaluated on every delta from the table (the canvas pretty-prints `<>` as the equivalent `≠`).
- **shape out · π** — the shape's output stream; its chip counts envelopes emitted (your four backfill rows are already on it).

This *is* the pipeline the engine built for your request — not a diagram of it. The node ids, counters, and edges come from the engine's own introspection endpoints.

> **You can also create a shape without leaving the browser.** Click **`+ new shape`** in the left sidebar, pick `issues` from the table picker, and type `status <> 'done'` into the `WHERE` editor — it autocompletes column names, operators (`=`, `<>`, `<`, `IN`, …), even `IN (SELECT …)` subqueries. Submit, and the same pipeline appears on the canvas. It fires the exact `GET /v1/shape?table=…&offset=-1&where=…` request you just ran by hand. We keep `curl` as the primary path here for one reason: the create response carries the `electric-handle` and `electric-offset` headers, and §5's long-poll needs them — a shape born in the browser never hands those to your terminal.

## 5. Go live — one write, watched end to end

Two terminals. In **terminal A** — the one where you saved `$HANDLE` and `$OFFSET` — start a long-poll on the shape's tail — it will hang, waiting for the next change:

```sh
curl -i -G 'http://localhost:7010/v1/shape' \
  --data-urlencode "table=issues" \
  --data-urlencode "handle=$HANDLE" \
  --data-urlencode "offset=$OFFSET" \
  --data-urlencode "live=true"
```

Arrange your windows so you can see the **visualizer** and terminal A at the same time. Then, in **terminal B**, insert one matching row with plain SQL:

```sh
psql "postgres://postgres:password@localhost:5432/electric" \
  -c "INSERT INTO issues VALUES (7, 'review this tutorial', 'todo', 2)"
```

> **Prefer to stay in the browser for this one?** Click the **`issues` table node** on the canvas and use the **add-row** form in its detail panel — it's schema-driven, and it runs the same insert (`POST /table/issues/rows`) against Postgres. You write the row and watch the `+1` leave that very node, all in one window. (Where an episode only needs to *cause a change and watch it flow*, this is a genuine alternative to `psql`; §4's create stayed on `curl` only because it needed the returned headers.)

Watch the same event arrive three ways at once:

1. **On the canvas**, a green `+1` dot travels `issues → σ → shape` and each node flashes as it passes; the chips increment.
2. **Terminal A's long-poll returns** with the insert delta for issue 7 and fresh `electric-handle` / `electric-offset` headers (loop with the new offset to keep tailing).
3. **The sidebar Activity log** records the change — click it to replay the animation if you looked away at the wrong moment.

Nothing re-queried Postgres. The engine took one replicated change, evaluated your predicate against it once, and forwarded it to the one shape that cared.

**Before you go, try the reverse** *(optional)*: mark the new issue done — `UPDATE issues SET status = 'done' WHERE id = 7` — and the next long-poll returns a **delete**: the row didn't vanish from Postgres, it left *your shape*.

> The browser can make issue 7 leave your shape too: open the `issues` table node, tick issue 7's row in its detail panel, and delete it (`DELETE /table/issues/rows`, by primary key). But that's the *stronger* statement — it removes the row from Postgres outright, so of course it leaves the shape. The mark-done `UPDATE` above is the subtler, more interesting version: the row lives on in Postgres, it just stops matching *your* predicate. Same `delete` on the feed, two different reasons — hold that distinction; episode 2 is built on it.

## 6. What just happened

You created a live query with one HTTP request; the engine built a dataflow pipeline for it; and a plain SQL write was evaluated **incrementally** — one delta through one filter, not a re-run of the query. That is the whole idea this series builds on: the engine is a DBSP-style incremental view maintenance system, and everything you'll see later — shared pipelines, cross-table subqueries, live aggregations — is this same picture with more operators.

**Next — Episode 2, Inside the pipeline:** the same shape, exploded into the DBSP circuit the engine actually executes — and why an update is secretly a retraction plus an insertion.
