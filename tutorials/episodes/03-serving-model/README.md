# Episode 3 — Pipelines, shapes, and strangers: the three-tier serving model

Episodes 1 and 2 lived inside one shape — `issues → σ → π → sink`, a stateless filter the engine
spins up on demand and tears down when the last reader leaves. That is the whole story for a
*stateless* predicate. But a real app doesn't open one shape; it opens a *query graph* — the same
handful of query shapes, over and over, once per user, once per screen. Building a fresh circuit
per shape would be madness, and the engine doesn't. It has a **static compiled dbsp pipeline** that
is decided at deploy time and serves the whole family of shapes an app asks for.

This episode is about the split that makes that work — the engine's **three-tier serving model**:

- **Pipelines serve query _families_.** A pipeline is compiled once, at deploy, and its output is
  keyed by *cohort group* (per list, per (list, done) count group, …). Its structure never grows
  with shapes, users, or parameter combinations.
- **Routing serves query _instances_.** A shape is a *selection or union* of cohort groups from a
  pipeline's keyed output, materialized at the delivery edge. Shape cardinality is unbounded over
  the *same* pipeline — the fan-out happens outside the circuit.
- **The fallback serves query _strangers_.** Any predicate that matches no template runs on the
  always-on stateless path. The pipeline is an optimization in front of it, never a correctness
  dependency: a brand-new query pattern works immediately.

You'll deploy a real pipeline for a tiny todo app, then watch — on the canvas — shapes connect to
it and disconnect as you create and remove them. The canonical write-up is
[`docs/building-app-pipelines.md`](../../../docs/building-app-pipelines.md); this episode drives
its todo model by hand.

## 1. The app: a tiny todo model

Three tables (the recipe's running example): `lists` group `todos`, and `list_members` says who may
see which list.

```text
lists(id, name)
todos(id, list_id, done, title, assignee)
list_members(id, list_id, user_id)
```

Its queries are a *family*: "todos of my lists", "todos of list L", "open-todo count per list". The
unit at which visibility is granted is the **list** — every member of a list sees the same todos —
so `list_id` is the **cohort key**, and it *partitions* `todos` (each todo is in exactly one list).
That partitioning is the load-bearing property; hold onto it.

## 2. Deploy the pipeline

From the `tutorials/` directory, reset to episode 1's clean slate, add the todo tables, then
**configure the circuit to serve the todo model** — three steps, because that is genuinely what a
deploy is here:

```sh
# 1. clean slate: postgres with just the `issues` table; the engine's circuit runs, but has
#    nothing app-specific to serve yet (only per-table primary-key arrangements)
docker compose down -v && docker compose up -d --wait

# 2. add the todo model (this is not part of the seed — a reset drops it)
psql "postgres://postgres:password@localhost:5432/electric" \
  -f episodes/03-serving-model/setup.sql

# 3. recreate the engine WITH the circuit's serving templates configured for those tables
docker compose -f compose.yaml -f episodes/03-serving-model/compose.circuit.yaml \
  up -d --force-recreate engine
```

Step 3 is the point. The circuit is always running — but *what it serves* is configured by static
environment variables, not by introspection, and a dbsp circuit is fixed at construction. Unlike a
shape — which the engine builds the instant you ask — a new serving template ships with a restart.
The overlay ([`compose.circuit.yaml`](compose.circuit.yaml)) declares exactly the todo-model
serving config from the recipe:

```sh
# the circuit is always on; these declare which shapes it serves end to end
ELECTRIC_IVM_DBSP_INDEXES=todos.list_id,list_members.user_id,list_members.list_id
ELECTRIC_IVM_DBSP_COUNTS=todos:list_id+done
```

(This is the same shape of config the flagship demo runs — see `examples/linearlite/start.ts`,
where the real app declares `issues.project_id`, the `project_members` columns, and a
`issues:project_id+status+priority+username` count.)

Check the engine came back up with the circuit:

```sh
curl http://localhost:7010/health
# → ok
```

## 3. Look at the static pipeline — before any shape exists

Open **https://localhost:5543** and switch the canvas to the **dbsp circuit** view (the
**Logical / dbsp circuit** toggle at the top of the left sidebar). In episode 2 this view exploded
*one shape* into operators. Now it shows something new: a **permanent, dashed lane** — the compiled
circuit itself — with **no shape attached to it yet**.

The sidebar counter reads it out:

```
dbsp: 3 indexes · 1 counts · 0 served · 0 fallback
```

On the lane you'll find:

- **an input per replicated table** — `todos`, `lists`, `list_members`, and `issues` (episode 1's
  table is still here; nobody configured a pipeline for it, so it sits with only its primary-key
  input — a table the circuit tier ignores). Each input's chip says `seeded` once its
  `REPEATABLE READ` snapshot has loaded.
- **three index pipelines** — `map_index(list_id)` on `todos`, `map_index(user_id)` and
  `map_index(list_id)` on `list_members`, each `→ integrate_trace`. These are the cohort
  arrangements shapes will be *served from*.
- **one counts pipeline** — `weighted_count(list_id, done)` over `todos`: a live COUNT per
  `(list, done)` group.

This lane doesn't come and go. It was built at boot and it stays, whether zero shapes or ten
thousand are open — **structure never scales with subscriptions**. Keep the tab open; the rest of
the episode is shapes latching onto this lane and letting go.

## 4. Tier 1 — the pipeline serves a family (a membership shape)

Ask for *the todos of all of alice's lists*. This is a **membership subquery**: the cohort constraint
is "list_id is one of the lists alice belongs to". Create it with the same `/v1/shape` request you
used in episodes 1 and 2 — the `where` just carries a subquery now:

```sh
RES=$(curl -si -G 'http://localhost:7010/v1/shape' \
  --data-urlencode "table=todos" \
  --data-urlencode "offset=-1" \
  --data-urlencode "where=list_id IN (SELECT list_id FROM list_members WHERE user_id = 'alice')")
printf '%s\n' "$RES"

HANDLE=$(printf '%s' "$RES" | awk 'tolower($1)=="electric-handle:"{print $2}' | tr -d '\r')
OFFSET=$(printf '%s' "$RES" | awk 'tolower($1)=="electric-offset:"{print $2}' | tr -d '\r')
```

alice is in lists 1 and 2, so you get back **four inserts** — todos 1, 2, 3 (Groceries) and 4
(Launch plan) — then `up-to-date`. Nothing surprising in the *result*. The surprise is *where it
came from*.

> The `+ new shape` form handles this subquery gracefully: pick `todos`, and the `WHERE` editor
> autocompletes the whole `list_id IN (SELECT list_id FROM list_members WHERE user_id = 'alice')`
> predicate — column names, the `IN (SELECT …)` scaffold, and all. Submit and the same shape latches
> onto the lane on the canvas. We stay on `curl` here because §5 long-polls this shape's tail and
> needs the `electric-handle` / `electric-offset` the create response returns — the browser keeps
> those to itself.

Look at the canvas. A shape node just appeared and **latched onto the static lane with a solid,
animated `serves` edge** — from the `todos map_index(list_id)` arrangement into the shape's
membership operator. The sidebar counter ticked to `1 served`, and the shape's row in the list wears
a **`circuit`** badge. That backfill of four rows did **not** query Postgres: the shape was *seeded
from the arrangement snapshots* the circuit already holds — no backfill `SELECT`, no snapshot gate.
The pipeline was already maintaining `todos` keyed by list; the shape just selected alice's cohort
groups {1, 2} out of it.

That is tier one in one picture: **the pipeline is the family, the shape is a selection of its cohort
groups, and the selection is resolved at the delivery edge** — the edge you're looking at.

## 5. The membership feed — move-in and move-out

The most important thing a cohort key buys you is that *changing who can see what* costs no
computation. Start a long-poll on the shape's tail (as in episodes 1 and 2 — reissue it with each
fresh `electric-offset`):

```sh
curl -i -G 'http://localhost:7010/v1/shape' \
  --data-urlencode "table=todos" \
  --data-urlencode "handle=$HANDLE" \
  --data-urlencode "offset=$OFFSET" \
  --data-urlencode "live=true"
```

Now add alice to a list she isn't in — the Reading list (id 3) — with a plain write to the
*membership* table, not the todos table:

```sh
psql "postgres://postgres:password@localhost:5432/electric" \
  -c "INSERT INTO list_members (list_id, user_id) VALUES (3, 'alice')"
```

> This is a pure "cause a change and watch it flow" write, so the canvas is a fine place to make it:
> click the **`list_members` table node**, and its **add-row** form will insert `(3, alice)` for you
> (`POST /table/list_members/rows`) — you write the membership row and watch two todos move into
> alice's shape in the same window.

Watch it three ways:

1. **On the canvas**, the change enters at `list_members`, not `todos`. The membership delta
   subscribes alice's shape to cohort group 3, and the two todos of list 3 (`finish chapter 3`,
   `return library book`) are read straight out of the `todos` arrangement's post-transaction
   snapshot.
2. **The long-poll returns two upserts** — todos 6 and 7 **moved in**. The shape now holds six.
3. **Nothing re-queried the todos table.** A row that grants *visibility* moved a whole cohort into
   the result. This is "dynamic" of the second kind — a **time-varying membership shape**, driven by
   the membership feed.

Reverse it and the todos **move out**:

```sh
psql "postgres://postgres:password@localhost:5432/electric" \
  -c "DELETE FROM list_members WHERE list_id = 3 AND user_id = 'alice'"
```

> Or do it from the canvas: open the `list_members` node, tick the `(3, alice)` row in its detail
> panel, and delete it (`DELETE /table/list_members/rows`, by primary key). Same move-out, watched on
> the lane — the membership row leaves, and alice's cohort group 3 unsubscribes.

The next long-poll returns two **deletes** for todos 6 and 7 — they didn't leave Postgres, they left
*alice's shape* when she left the list. Subscribe/unsubscribe, not recompute.

### The counts pipeline, live

The other thing on the lane is the `weighted_count(list_id, done)` pipeline. Any COUNT whose
predicate **decomposes over those group columns** is served straight from it — no per-shape fold.
Ask for the open-todo count of the Groceries list (`list_id = 1 AND done = false`) with the
extended API's `/aggregate` endpoint (a COUNT isn't an Electric shape):

```sh
AGG=$(curl -s -X POST http://localhost:7010/aggregate \
  -H 'content-type: application/json' \
  -d '{"table":"todos","fn":"count",
       "where":{"and":[{"col":"list_id","op":"eq","value":1},
                       {"col":"done","op":"eq","value":false}]}}')
AGG_ID=$(printf '%s' "$AGG" | sed 's/.*"shapeId":"\([^"]*\)".*/\1/')
curl -s "http://localhost:7010/shapes/$AGG_ID/rows"
# → the count is 2  (buy milk, buy eggs)
```

On the canvas this aggregate latches onto the **counts** pipeline with a `serves` edge (its badge
reads `circuit`). Its value was *seeded by summing the matching groups* — here just the `(1, false)`
group. Now close one of those todos:

```sh
psql "postgres://postgres:password@localhost:5432/electric" \
  -c "UPDATE todos SET done = true WHERE id = 1"
```

Re-read `http://localhost:7010/shapes/$AGG_ID/rows`: the count is **1**. The `(1, false)` group's
weighted count dropped by one on that step, and the aggregate followed it — one maintained integer,
not a re-scan. Aggregate at the finest useful grain and one pipeline serves every filter
combination: the badge for one list is one group; a dashboard across lists sums the user's groups.

## 6. Tier 2 — routing serves instances (and shares them)

Not every shape needs the circuit. Ask for one list's todos directly — an **equality** predicate:

```sh
curl -si -G 'http://localhost:7010/v1/shape' \
  --data-urlencode "table=todos" \
  --data-urlencode "offset=-1" \
  --data-urlencode "where=list_id = 2" >/dev/null
```

Two todos come back (4 and 5). On the canvas this shape does **not** attach to the circuit lane — it
is routed by a `KeyRouter` on `list_id` instead, because an indexed route finds a change's shapes in
`O(log N)`, whereas a circuit shape would scan every delta. Equality shapes are *deliberately* not
circuit-served.

Now the punchline of routing — run that **exact** request again, as if a second client asked for the
same view:

```sh
curl -si -G 'http://localhost:7010/v1/shape' \
  --data-urlencode "table=todos" \
  --data-urlencode "offset=-1" \
  --data-urlencode "where=list_id = 2" >/dev/null
```

No second node appears. Two identical requests collapse onto **one** maintained shape and one durable
stream, ref-counted — *two clients, one pipeline, on screen*. And a shape for a *different* list
(`list_id = 5`), or for *several* lists (`list_id IN (2, 3)`), is just another entry in the same
router: **shape cardinality is unbounded over one template**, and every new combination is pure
routing — a routing-table entry, never a change to the circuit. This is "dynamic" of the first kind:
**runtime-created combination shapes**.

Two notes the recipe insists on, both visible here:

- **The union is only correct because `list_id` partitions `todos`.** `list_id IN (2, 3)` is the
  union of groups 2 and 3, and each todo lives in exactly one — so no todo is counted twice. Overlapping
  cohorts (a row in two groups) would need de-duplication at the edge.
- **Union-at-edge vs client-side merge is a real choice.** The engine can materialize the union into
  one feed (what you get above), or hand a client one feed per cohort group and let it merge them —
  cheaper to fan out, but the merge moves to the client. Same cohort feeds either way; only the seam
  moves.

## 7. Tier 3 — the fallback serves strangers

Finally, ask something the pipeline was never built for — a substring match:

```sh
curl -si -G 'http://localhost:7010/v1/shape' \
  --data-urlencode "table=todos" \
  --data-urlencode "offset=-1" \
  --data-urlencode "where=title LIKE '%milk%'" >/dev/null
```

One row (`buy milk`). This predicate decomposes over **no** cohort key — it cuts across every list —
so it matches no template. It doesn't error and it doesn't wait for a redeploy: the **fallback** picks
it up on the spot, evaluating the predicate statelessly on every delta (episode 2's `σ`, exactly).
On the canvas it stands alone, attached to no lane. This is "dynamic" of the third kind: a
**cross-key predicate**. The circuit sits *in front of* the fallback as an optimization — if this
pattern turned out to matter, you'd promote it into the circuit at the next deploy. Until then it
just works, at fallback cost.

> By now the sidebar's shape list has a handful of entries — the membership shape, the aggregate, the
> routed equality and union shapes, the fallback. Each has a **delete** button, and there's a
> **delete-all-shapes** control alongside; use them to watch shapes **let go** of the lane in reverse.
> Delete the membership shape and its `serves` edge detaches and the `served` counter ticks back down;
> clear them all and the static lane sits there alone again, exactly as in §3 — structure that never
> came and went with the subscriptions in the first place.

## 8. What you now know

The engine doesn't build a circuit per shape — it compiles a **static pipeline per query family**,
decided at deploy time, and everything dynamic happens *outside* it. Pipelines serve **families**
(the compiled circuit, keyed by cohort group); routing serves **instances** (selections and unions
of those groups, unbounded over the same pipeline, fanned out at the delivery edge); and the fallback
serves **strangers** (any predicate, always on). The three flavors of "dynamic" map cleanly onto the
tiers: combination shapes are pure routing, membership shapes are routing driven by a feed, and
cross-key predicates are fallback. When someone asks "does adding a user, or a new filter
combination, grow the engine?" — you now know it doesn't: only new *templates* do, and those ship
with a deploy.

For the full case study — the same three tiers carrying the flagship app's entire query graph, the
one you've been syncing since episode 1 — read
[`docs/linearlite-circuit-design.md`](../../../docs/linearlite-circuit-design.md): which of its nine
call sites goes to which tier, and why ordering and pagination deliberately stay in Postgres.
