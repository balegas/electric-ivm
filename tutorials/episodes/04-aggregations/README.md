# Episode 4 — Aggregations: a live COUNT

Every live query so far was served the moment you wrote it — no configuration, no restart. This
episode is the one exception: a **live aggregation** — a running COUNT, SUM, AVG, MIN, or MAX — can
be served straight from a small **counts circuit**, but only for groupings you tell the engine about
*ahead of time*. That's a real, honest piece of static configuration in today's engine, and this
episode shows you exactly what it costs and what you get for it.

You need episode 3's todo model still applied. If you reset since then, bring it back:

```sh
docker compose down -v && docker compose up -d --wait
psql "postgres://postgres:password@localhost:5432/electric" \
  -f episodes/03-subqueries-are-dynamic/setup.sql
docker compose restart engine
```

## 1. Configure a counts grouping, and restart

Ask the circuit to maintain a live count of `todos`, grouped by `(list_id, done)` — exactly the
grouping a browse-header COUNT in a real todo app would need (open-todo count, per list):

```sh
ELECTRIC_CIRCUITS_DBSP_COUNTS=todos:list_id+done docker compose up -d engine
curl http://localhost:7010/health
# → ok
```

This is genuinely a **configuration change plus a restart** — the one thing in this series that is.
Unlike episode 3's subqueries and equality live queries, a counts grouping is fixed when the circuit
is built; asking for a different grouping means changing this environment variable and restarting
the engine. The restart is cheap, though: the counts circuit holds no on-disk state to restore. It
**reseeds from Postgres** every time — one `GROUP BY` per configured table, not a scan of every row:

```sh
curl -s http://localhost:7010/graph | jq '.arrangements.counts'
```

```json
[{"id":"arr:counts:todos","input":"arr:input:todos","table":"todos",
  "groupCols":["list_id","done"],"seeded":true}]
```

`seeded: true` means the initial `SELECT list_id, done, count(*) FROM todos GROUP BY list_id, done`
has already loaded. Open the pipeline explorer: the `todos` source card now carries a small badge —
`⧉ 0 idx · 1 cnt`. Ignore the `idx` half; it's a leftover field from a row-indexing layer the engine
no longer has, and it's always zero today. The `1 cnt` half is the real thing: one live counts
pipeline, folded onto the table it counts.

## 2. Create the live count — the app-facing way

The extended API (port 8790) is how an app creates a live aggregation:

```sh
AGG=$(curl -s -X POST http://localhost:8790/aggregate.create \
  -H 'content-type: application/json' \
  -d '{
    "table": "todos",
    "fn": "count",
    "where": { "and": [
      { "col": "list_id", "op": "eq", "value": 1 },
      { "col": "done",    "op": "eq", "value": false }
    ] }
  }')
printf '%s\n' "$AGG"
```

```json
{"result":{"data":{"shapeId":"<id>","table":"todos",
  "streamPath":"shape/<id>","streamUrl":"http://ds:8791/shape/<id>"}}}
```

(`streamUrl` names the durable-streams server by its in-cluster hostname `ds:8791`; from your host
you reach the same feed at the published port, `http://localhost:8791`.) Read its feed:

```sh
SHAPE_ID=$(printf '%s' "$AGG" | sed -n 's/.*"shapeId":"\([^"]*\)".*/\1/p')
curl -s "http://localhost:8791/shape/$SHAPE_ID?offset=-1"
```

```json
[{"type":"todos","key":"agg","value":{"n":2,"value":2},"headers":{"operation":"upsert"}}]
```

Groceries (list 1) has two open todos — *buy milk*, *buy eggs* — so the count is `2` over `n=2`
matching rows.

## 3. The same thing, under the hood

This predicate — `list_id = 1 AND done = false` — is exactly a conjunction of equalities over the
counts pipeline's group columns (`list_id`, `done`). That's what makes it **circuit-served**: its
value is seeded by summing the matching group and kept live from the circuit's own group deltas, not
by a per-row fold. You can create the same aggregate straight against the engine's control-plane
HTTP (port 7010), the surface the extended API sits on top of:

```sh
AGG=$(curl -s -X POST http://localhost:7010/aggregate \
  -H 'content-type: application/json' \
  -d '{"table":"todos","fn":"count",
       "where":{"and":[{"col":"list_id","op":"eq","value":1},
                       {"col":"done","op":"eq","value":false}]}}')
AGG_ID=$(printf '%s' "$AGG" | sed -n 's/.*"shapeId":"\([^"]*\)".*/\1/p')
curl -s "http://localhost:7010/shapes/$AGG_ID/rows"
```

```json
{"id":"<id>","table":"todos","changesOnly":false,"count":1,"truncated":false,
 "rows":[{"key":"agg","value":{"n":2,"value":2}}]}
```

On the pipeline explorer, this aggregate's row in the sidebar wears a **`circuit · counts`** badge,
and a `serves` edge runs from the `todos` source straight into its fold — no `σ` runs for this one at
all. Its value lives in the counts pipeline itself.

## 4. Watch it move, live

Close one of Groceries' open todos:

```sh
psql "postgres://postgres:password@localhost:5432/electric" \
  -c "UPDATE todos SET done = true WHERE id = 1"
```

Re-read either feed and the count is **1**:

```sh
curl -s "http://localhost:7010/shapes/$AGG_ID/rows"
# → {"n":1,"value":1}} for the (1, false) group
```

The `(list_id=1, done=false)` group's weighted count dropped by one on that step, and the aggregate
followed it — one maintained integer, not a re-scan of `todos`. The same counts pipeline serves
every list: ask for `list_id = 2 AND done = false` and you get Launch plan's count from the exact
same circuit, no new structure.

## 5. What you now know — and what the whole series adds up to

A live COUNT can be served straight from a small circuit, but only for the group columns you
configured ahead of time (`ELECTRIC_CIRCUITS_DBSP_COUNTS`) — a config change plus a restart, and the
restart always reseeds from Postgres. That is the one piece of this engine that works the way the
old static-pipeline model described everything: fixed at construction, changed by a deploy.
Everything else in this series — filters, membership subqueries, equality live queries — you saw
work the opposite way: write the query, and it runs immediately on structure that was already there.

That's the whole arc: **Streams** carry every live query's changes; **circuits** are the small, fixed,
shared dataflows your live queries register onto; and a **live query** is what you actually get back —
current rows, then everything that changes, forever. Most of what makes a query expensive elsewhere —
more users, more parameters, a brand-new predicate — costs nothing new here. The one thing that still
does is a COUNT grouping nobody configured yet, and now you know exactly what that costs, too: one
environment variable and a restart.
