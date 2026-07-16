# Episode 3 — Cross-table live queries with subqueries

Episodes 1 and 2 lived inside one table. A real app's live queries usually don't stop at one table —
"todos of the lists I belong to" needs to know about `list_members` too. Electric Circuits has one
cross-table form for exactly this — a **subquery** — and you write it just like any other live query.
The interesting part is what the engine does behind it: it maintains a small **shared set**, part of
the **circuit**, that you can watch appear and update live on the canvas.

## 1. The app: a tiny todo model

From the `tutorials/` directory, apply this episode's setup on top of episode 1's `issues` table,
then restart the engine so it picks up the new tables (the engine introspects the table set at
startup):

```sh
psql "postgres://postgres:password@localhost:5432/electric" \
  -f episodes/03-subqueries-are-dynamic/setup.sql
docker compose restart engine
curl http://localhost:7010/health
# → ok
```

Three tables: `lists` group `todos`, and `list_members` says who may see which list.

```text
lists(id, name)
todos(id, list_id, done, title, assignee)
list_members(id, list_id, user_id)
```

alice is a member of lists 1 (Groceries) and 2 (Launch plan); bob is a member of lists 2 and 3
(Reading). Seven todos are split across the three lists. Small enough that you can predict every
result by reading [`setup.sql`](setup.sql).

## 2. A membership live query

Ask for *all the todos of alice's lists*. The `where` carries a subquery — a column `IN` the result
of another `SELECT`:

```sh
RES=$(curl -si -G 'http://localhost:7010/v1/shape' \
  --data-urlencode "table=todos" \
  --data-urlencode "offset=-1" \
  --data-urlencode "where=list_id IN (SELECT list_id FROM list_members WHERE user_id = 'alice')")
printf '%s\n' "$RES"

HANDLE=$(printf '%s' "$RES" | awk 'tolower($1)=="electric-handle:"{print $2}' | tr -d '\r')
OFFSET=$(printf '%s' "$RES" | awk 'tolower($1)=="electric-offset:"{print $2}' | tr -d '\r')
```

alice is in lists 1 and 2, so you get back **five inserts** — todos 1–5 — then `up-to-date`. You
wrote a query that reaches across two tables and got its live result — from where you sit, no
different than episode 1's single-table query.

Look at the pipeline explorer. Alongside your live query's output node, a new node appears — a
**shared inner set** — holding the *distinct `list_id` values alice belongs to*, not the todos
themselves. An animated edge labeled `IN · list_id` connects it to your live query. That node is
what makes this query live: `/graph` reports it directly —

```sh
curl -s http://localhost:7010/graph | jq '.subqueryNodes, .subqueryEdges'
```

— one entry in `subqueryNodes` (`innerTable: "list_members"`, `projCol: "list_id"`,
`distinctValues: 2`), and one entry in `subqueryEdges` connecting it to your live query on
`list_id`. The node holds two values (alice's two list ids), **not five rows of `todos`** — the
todos themselves stay in Postgres until something changes for them.

## 3. Membership changes flow through the same node — move-in, move-out

The most important thing about a shared inner set is that *changing who can see what* costs no
recomputation of the todos themselves. Start a long-poll on your live query's tail (reissue with
each fresh `electric-offset`):

```sh
curl -i -G 'http://localhost:7010/v1/shape' \
  --data-urlencode "table=todos" \
  --data-urlencode "handle=$HANDLE" \
  --data-urlencode "offset=$OFFSET" \
  --data-urlencode "live=true"
```

Now add alice to a list she isn't in yet — the Reading list (id 3) — with a plain write to the
**membership** table, not `todos`:

```sh
psql "postgres://postgres:password@localhost:5432/electric" \
  -c "INSERT INTO list_members (list_id, user_id) VALUES (3, 'alice')"
```

Watch it two ways:

1. **On the canvas**, the change enters at `list_members`. The inner set's value `3` just went from
   absent to present — a **flip** — and the engine queries back exactly the `todos` rows where
   `list_id = 3` (todos 6 and 7) to bring them into your live query.
2. **The long-poll returns two upserts** — todos 6 and 7 **moved in**. Your live query now holds
   seven rows.

Nothing re-queried the `todos` table as a whole — one membership row flipped one value in one small
set, and that value's dependents moved.

Reverse it and the todos **move out**:

```sh
psql "postgres://postgres:password@localhost:5432/electric" \
  -c "DELETE FROM list_members WHERE list_id = 3 AND user_id = 'alice'"
```

The next long-poll returns two **deletes** for todos 6 and 7 — they didn't leave Postgres, they left
*alice's live query* when she left the list.

## 4. Identical subqueries share one node

Open a second, different live query — narrower columns, but the **same inner subquery**:

```sh
curl -si -G 'http://localhost:7010/v1/shape' \
  --data-urlencode "table=todos" \
  --data-urlencode "offset=-1" \
  --data-urlencode "where=list_id IN (SELECT list_id FROM list_members WHERE user_id = 'alice')" \
  --data-urlencode "columns=id,title" >/dev/null
```

This is a genuinely different live query (a different column projection means a different stream),
so it gets its own output node — but no *second* inner-set node appears. The existing one now shows
a **`shared ×2`** badge, and its detail panel's refcount climbs to 2: two live queries, one
maintained membership set. This is why per-user fleets stay cheap — a thousand identical visibility
queries share one small set, not a thousand copies of it.

## 5. Equality live queries share a router, too

Ask for one list's todos directly — an equality predicate, no subquery:

```sh
curl -si -G 'http://localhost:7010/v1/shape' \
  --data-urlencode "table=todos" \
  --data-urlencode "offset=-1" \
  --data-urlencode "where=list_id = 2" >/dev/null
```

Two todos come back (4 and 5). On the canvas this live query connects to a **route join** node
(labeled `route by (list_id)`) instead of an inner-set node — a change is routed to the live queries
whose key matches, in `O(log N)`, rather than a subquery's move-in/move-out dance. Now ask for a
*different* list:

```sh
curl -si -G 'http://localhost:7010/v1/shape' \
  --data-urlencode "table=todos" \
  --data-urlencode "offset=-1" \
  --data-urlencode "where=list_id = 1" >/dev/null
```

No second route-join node appears — this live query shares the same one, keyed on the same column
(`list_id`), just a different value. Every equality live query on `list_id`, however many, routes
through this one shared structure.

## 6. Under the hood, if you're curious

(If you want the engine-internals view of how live queries land on the router versus the subquery
registry — the terminology engine developers use for this split — see the "Serving tiers" section of
`docs/ivm-engine-internals.md`. This episode deliberately doesn't use that vocabulary: from your
app's side, it's all just "write the query.")

## 7. What you now know

A cross-table membership live query is served by a shared inner set the engine maintains for you —
holding the values that decide membership, not the rows. Identical subqueries share one node;
identical column-sets on an equality predicate share one router. Adding a new user, a new list, or a
new equality value is data flowing through structure that was already there — not new structure.

**Next — Episode 4, Aggregations: a live COUNT:** live totals like an open-todo count per list, and
the one way they work a little differently from everything you've seen so far.
