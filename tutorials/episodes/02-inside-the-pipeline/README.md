# Episode 2 — Inside the pipeline: the DBSP circuit

In episode 1 you created a live shape and watched one write flow through `issues → σ → shape`. Those three boxes are the *logical* view — honest, but summarized. This episode opens the hood: the operator pipeline every change actually flows through, and the two ideas that make incremental view maintenance work — **deltas** and **weights**.

## 1. Where we left off

From the `tutorials/` directory, reset to episode 1's starting state and re-create the open-issues shape:

```sh
docker compose down -v && docker compose up -d --wait

RES=$(curl -si -G 'http://localhost:7010/v1/shape' \
  --data-urlencode "table=issues" \
  --data-urlencode "offset=-1" \
  --data-urlencode "where=status <> 'done'")
HANDLE=$(printf '%s' "$RES" | awk 'tolower($1)=="electric-handle:"{print $2}' | tr -d '\r')
OFFSET=$(printf '%s' "$RES" | awk 'tolower($1)=="electric-offset:"{print $2}' | tr -d '\r')
```

And, like in episode 1, keep a long-poll running on the shape's tail — this episode watches the feed as much as the canvas. Re-issue this after each response, with the fresh `electric-offset` it returns:

```sh
curl -i -G 'http://localhost:7010/v1/shape' \
  --data-urlencode "table=issues" \
  --data-urlencode "handle=$HANDLE" \
  --data-urlencode "offset=$OFFSET" \
  --data-urlencode "live=true"
```

Open **https://localhost:5543** — the three familiar nodes are back. Keep the tab open.

## 2. Two views of one pipeline

Switch the canvas to the **dbsp circuit** view (the **Logical / dbsp circuit** toggle at the top of the left sidebar). The same shape explodes into five boxes:

- **source** — the replication tap on `issues`: every committed change enters here.
- **Δ (delta)** — changes leave the source as a *delta stream*: not rows, but row *changes*.
- **σ (filter)** — your predicate, applied to each delta.
- **π (project)** — reshapes each surviving delta: trims it to the shape's columns and groups by primary key into an upsert/delete envelope.
- **sink** — appends those envelopes to the shape's stream: the feed your long-poll reads.

One box per step the engine performs on every change: this decomposition is **reported by the engine itself** (`GET /graph` returns the operator list), not drawn by the UI. You are looking at the execution plan, live.

## 3. Deltas and weights

In a second terminal, insert a matching row — and watch the circuit while it lands:

```sh
psql "postgres://postgres:password@localhost:5432/electric" \
  -c "INSERT INTO issues VALUES (7, 'review this tutorial', 'todo', 2)"
```

> As in episode 1, you can drive this write from the canvas instead — the `issues` table node's **add-row** form does the same insert. (The updates in §4 stay on `psql`: the table node writes and deletes rows, but changing a column in place is `UPDATE`'s job — which is exactly the point §4 makes.)

The green dot is labeled **`+1`**, and that label is the core of DBSP: a change is a **weighted row**. `+1` means "this row is now present"; `−1` means "this row is no longer present". Every operator in the circuit consumes a stream of weighted rows and emits one — σ passes or drops them, π folds them into feed envelopes, the sink appends those to your shape's stream. Nothing anywhere re-reads the table.

## 4. An update is a retraction plus an insertion

Here is the trick the whole engine turns on. In SQL you think of an update as "the row changed in place". A delta stream has no such thing — an update is **two weighted rows**: `−1 × (old row)` and `+1 × (new row)`.

Watch it. First, an update that *keeps* the row in the shape:

```sh
psql "postgres://postgres:password@localhost:5432/electric" \
  -c "UPDATE issues SET title = 'review this tutorial twice' WHERE id = 7"
```

Both halves survive σ (old and new row are open issues), and π collapses them into a single **upsert** on your feed — on canvas the dot runs blue, `±1`.

Now the update you already saw in episode 1, re-explained:

```sh
psql "postgres://postgres:password@localhost:5432/electric" \
  -c "UPDATE issues SET status = 'done' WHERE id = 7"
```

σ passes the `−1` (the old row matched) and **drops** the `+1` (the new row doesn't). Only the retraction reaches the sink — and that is *exactly* why your long-poll gets a `delete`: not because anything was deleted in Postgres, but because the surviving half of the update says "this row is no longer present *in this shape*".

## 5. Stateless vs stateful

Click the σ box: its detail panel shows what it evaluates, and note what it *doesn't* have — stored rows. σ and π are pure per-delta functions; this entire circuit keeps **no state**, which is why the engine can maintain a shape like this for next to nothing.

The interesting DBSP machinery starts when a circuit *must* remember things: equality routing uses a shared index, joins and subqueries keep **arrangements** (stateful indexes — when one feeds a join, the canvas draws that edge dashed), and aggregations keep folds. That state — and how it stays small — is where the series goes next.

## 6. What you now know

Shapes compile to circuits of operators that pass weighted row-changes; an update is a retraction plus an insertion; and a predicate like yours needs no state at all. When a reader asks you "how does the engine know a row *left* a query result without re-running it?", you now know the answer: the `−1` told it.

**Next — Episode 3, Pipelines, shapes, and strangers:** step out of one shape and into the app's whole query graph — the engine's static compiled pipeline, the three-tier serving model, and shapes latching onto that pipeline (and letting go) live on the canvas. That's where "two clients, one pipeline" turns out to live.
