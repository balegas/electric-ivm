# Episode 2 — Inside the circuit: deltas and weights

In episode 1 you created a live query and watched one write flow through it. What you saw on the
canvas was the *logical* view — honest, but summarized. This episode opens the hood: the operators
your live query's circuit actually runs, and the two ideas that make incremental view maintenance
work — **deltas** and **weights**.

## 1. Where we left off

From the `tutorials/` directory, reset to episode 1's starting state and re-create the open-issues
live query:

```sh
docker compose down -v && docker compose up -d --wait

RES=$(curl -si -G 'http://localhost:7010/v1/shape' \
  --data-urlencode "table=issues" \
  --data-urlencode "offset=-1" \
  --data-urlencode "where=status <> 'done'")
HANDLE=$(printf '%s' "$RES" | awk 'tolower($1)=="electric-handle:"{print $2}' | tr -d '\r')
OFFSET=$(printf '%s' "$RES" | awk 'tolower($1)=="electric-offset:"{print $2}' | tr -d '\r')
```

And, like in episode 1, keep a long-poll running on the live query's tail — this episode watches the
feed as much as the canvas. Re-issue this after each response, with the fresh `electric-offset` it
returns:

```sh
curl -i -G 'http://localhost:7010/v1/shape' \
  --data-urlencode "table=issues" \
  --data-urlencode "handle=$HANDLE" \
  --data-urlencode "offset=$OFFSET" \
  --data-urlencode "live=true"
```

Open **https://localhost:5543** — the familiar nodes are back. Keep the tab open.

## 2. Two views of one circuit

Switch the canvas from **Logical** to **dbsp circuit** (the two tabs at the top of the sidebar). The
same live query explodes into five operator boxes:

- **source** — the table's slice of the engine's replication tailer: every committed change on
  `issues` enters here.
- **Δ (delta)** — turns one replicated envelope into a *weighted* change: not a row, but a row
  *change*.
- **σ filter** — your predicate, applied to each delta.
- **π project** — groups the surviving delta by primary key into an upsert/delete envelope.
- **sink** — appends those envelopes to your live query's Stream: the feed your long-poll reads.

This isn't a diagram someone drew. `GET /graph` reports this exact decomposition — try it yourself:

```sh
curl -s http://localhost:7010/graph | jq '.operators[] | {kind, label}'
```

You'll see one `source`/`delta` pair for `issues`, and — for your live query — a `filter` (`σ
where`), a `project` (`π pk → envelope`), and a `sink` (labeled with your live query's stream path).
One box per step the engine performs on every change: you're looking at the execution plan, live.

## 3. Deltas and weights

In a second terminal, insert a matching row — and watch the circuit while it lands:

```sh
psql "postgres://postgres:password@localhost:5432/electric" \
  -c "INSERT INTO issues VALUES (7, 'review this tutorial', 'todo', 2)"
```

The change that travels the circuit is labeled **`+1`**, and that label is the core of DBSP: a
change is a **weighted row**. `+1` means "this row is now present"; `−1` means "this row is no longer
present". Every operator in the circuit consumes a stream of weighted rows and emits one — σ passes
or drops them, π folds them into feed envelopes, the sink appends those to your live query's Stream.
Nothing anywhere re-reads the table.

## 4. An update is a retraction plus an insertion

Here is the trick the whole engine turns on. In SQL you think of an update as "the row changed in
place". A delta stream has no such thing — an update is **two weighted rows**: `−1 × (old row)` and
`+1 × (new row)`.

Watch it. First, an update that *keeps* the row in the live query:

```sh
psql "postgres://postgres:password@localhost:5432/electric" \
  -c "UPDATE issues SET title = 'review this tutorial twice' WHERE id = 7"
```

Both halves survive σ (old and new row are open issues), and π collapses them into a single
**upsert** on your feed.

Now the update you already saw in episode 1, re-explained:

```sh
psql "postgres://postgres:password@localhost:5432/electric" \
  -c "UPDATE issues SET status = 'done' WHERE id = 7"
```

σ passes the `−1` (the old row matched) and **drops** the `+1` (the new row doesn't). Only the
retraction reaches the sink — and that is *exactly* why your long-poll gets a `delete`: not because
anything was deleted in Postgres, but because the surviving half of the update says "this row is no
longer present *in this live query*".

## 5. Stateless

Click the **σ** box on the canvas: its detail panel shows what it evaluates, and note what it
*doesn't* have — stored rows. σ and π are pure per-delta functions; this stretch of the circuit keeps
**no state**, which is why the engine can maintain a live query like this for next to nothing.

Your predicate (a range on `status`) needs no state at all to serve. Not every query is this simple —
a cross-table membership check needs to remember *something* (which values are currently visible),
and a live COUNT needs to remember a running total. That's where the series goes next: episode 3
picks up a live query that spans two tables, and episode 4 a live query that's a running aggregate.
Both are still built from the same idea — operators over weighted deltas — just with a small,
shared, maintained bit of state added in. (For the fuller picture of what a circuit is and why
registering a new query onto one is cheap, see `docs/how-queries-become-live.md`.)

## 6. What you now know

Live queries compile to circuits of operators that pass weighted row-changes; an update is a
retraction plus an insertion; and a predicate like yours needs no state at all. When someone asks you
"how does the engine know a row *left* a query result without re-running it?", you now know the
answer: the `−1` told it.

**Next — Episode 3, Cross-table live queries: subqueries are dynamic:** a live query that spans two
tables, served immediately with zero configuration — and what changes (and doesn't) when a whole
circuit has to remember something shared across queries.
