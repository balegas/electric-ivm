# Episode 1 — Your first live query

## 1. What is a live query?

A **live query** is "the rows matching this predicate", kept up to date forever. You ask for it once;
from then on, every change that affects it is pushed to you as a delta — no polling, no re-running the
query. In the API these are created with `client.shape()` and served at `/v1/shape` (the Electric
protocol name); conceptually we call them **live queries**.

Your data doesn't move anywhere to make this work. It lives in **Postgres**, the system of record, and
your apps keep writing to it with ordinary SQL. The **Electric Circuits engine** tails Postgres logical
replication and maintains every live query incrementally: each committed change flows once through a
small **circuit** of operators, and only the live queries it affects hear about it. In this episode
you'll create one live query, watch its circuit get built, and watch one write flow through it — live,
on screen.

## 2. Start the stack

From the `tutorials/` directory:

```sh
docker compose up --build
```

Five containers come up: **postgres** (already seeded with a tiny `issues` table), **ds** (the
durable-streams server — every live query's Stream lives here), the **engine**, an **api** server (the
extended API — this series doesn't need it until episode 4; it's here so the stack file never
changes), and the **viz** (pipeline explorer). If a port is already taken on your machine — a local
Postgres on 5432 is the usual culprit — override it: `PG_PORT=15432 docker compose up --build`; same
idea for `DS_PORT`, `ENGINE_PORT`, `API_PORT`, `VIZ_PORT`, and `VIZ_HTTPS_PORT`.

In a second terminal, check the engine is up and the data is there:

```sh
curl http://localhost:7010/health
# → ok
psql "postgres://postgres:password@localhost:5432/electric" -c 'TABLE issues'
# → 6 issues: three 'todo', one 'doing', two 'done'
```

(If you overrode `PG_PORT`, use that port in the connection URL. No `psql` installed? The series
[README](../../README.md) shows the in-container equivalent.)

## 3. Open the pipeline explorer — and keep it open

Open **https://localhost:5543** (click through the certificate warning the first time, or trust the
local CA once — see [Trusting the explorer's certificate](../../README.md#trusting-the-explorers-certificate)).
The canvas shows a single card — the `issues` table, with a `0` counter — and nothing else.

That's the first lesson: the engine is already tailing the table's replication stream (that's the
card), but it maintains **nothing** beyond that until someone asks. There's no circuit for data nobody
is watching — a live query is what brings one into existence.

Keep this tab open (and visible) for the rest of the episode. The live animation you'll see in §5
plays as changes happen — it's the engine working, not a replay.

## 4. Create a live query

Ask for every issue that's still open — anything not `done`. This one request creates the live query
and returns its current rows:

```sh
RES=$(curl -si -G 'http://localhost:7010/v1/shape' \
  --data-urlencode "table=issues" \
  --data-urlencode "offset=-1" \
  --data-urlencode "where=status <> 'done'")
printf '%s\n' "$RES"
```

You get back four insert messages — issues 1, 2, 3, and 5, exactly the not-done rows in the seed —
followed by an `up-to-date` control message. Two response headers matter; save them:

```sh
HANDLE=$(printf '%s' "$RES" | awk 'tolower($1)=="electric-handle:"{print $2}' | tr -d '\r')
OFFSET=$(printf '%s' "$RES" | awk 'tolower($1)=="electric-offset:"{print $2}' | tr -d '\r')
```

Now look at the pipeline explorer: two new nodes just appeared next to the table card, making three —
a **filter** node (your predicate) and a **live query output** node (its result feed). This *is* the
circuit the engine built for your request — not a diagram of it. The node ids, counters, and edges
come from the engine's own introspection endpoints (`GET /graph`, `GET /state`), the same ones you'll
read by hand in episode 2.

## 5. Go live — one write, watched end to end

Two terminals. In **terminal A** — the one where you saved `$HANDLE` and `$OFFSET` — start a long-poll
on the live query's tail — it will hang, waiting for the next change:

```sh
curl -i -G 'http://localhost:7010/v1/shape' \
  --data-urlencode "table=issues" \
  --data-urlencode "handle=$HANDLE" \
  --data-urlencode "offset=$OFFSET" \
  --data-urlencode "live=true"
```

Arrange your windows so you can see the **pipeline explorer** and terminal A at the same time. Then,
in **terminal B**, insert one matching row with plain SQL:

```sh
psql "postgres://postgres:password@localhost:5432/electric" \
  -c "INSERT INTO issues VALUES (7, 'review this tutorial', 'todo', 2)"
```

Watch the same event arrive two ways at once:

1. **On the canvas**, a change travels the circuit and each node it passes through flashes; the
   counters increment.
2. **Terminal A's long-poll returns** with the insert delta for issue 7 and fresh `electric-handle` /
   `electric-offset` headers (loop with the new offset to keep tailing).

Nothing re-queried Postgres. The engine took one replicated change, evaluated your predicate against
it once, and forwarded it to the one live query that cared.

**Before you go, try the reverse** *(optional)*: mark the new issue done —
`UPDATE issues SET status = 'done' WHERE id = 7` — and the next long-poll returns a **delete**: the row
didn't vanish from Postgres, it left *your live query*. Same delta on the feed, two different reasons
you might see one — hold that distinction; episode 2 is built on it.

## 6. What just happened

You created a live query with one HTTP request; the engine built a circuit for it; and a plain SQL
write was evaluated **incrementally** — one delta through one filter, not a re-run of the query. That
is the whole idea this series builds on: Electric Circuits is a DBSP-style incremental view
maintenance system, and everything you'll see later — cross-table subqueries, live aggregations — is
this same picture with more operators.

**Next — Episode 2, Inside the circuit:** the same live query, exploded into the DBSP circuit the
engine actually executes — and why an update is secretly a retraction plus an insertion.
