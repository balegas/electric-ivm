# Episode 4 — Extending the pipeline (and rebuilding it)

Episode 3 left you with a slogan: *only new templates grow the engine, and templates ship with a
deploy.* You watched shapes latch onto a static pipeline and let go, you watched the fallback catch
a stranger the pipeline was never built for — and the closer promised that when you finally *add* a
template, you'd do it with your hands. This episode is that: open a shape the pipeline can't serve,
watch it drop to the fallback tier, then add its dimension to the config and rebuild the circuit —
and see the circuit **reseed**, the shapes **replay** from the durable catalog, and that fallback
shape get **promoted** to circuit-served, live on the canvas.

You need the todo pipeline from episode 3 still deployed and serving. If you tore it down, bring it
back — clean slate, todo model, episode-3 serving config — in three steps:

```sh
docker compose down -v && docker compose up -d --wait
psql "postgres://postgres:password@localhost:5432/electric" \
  -f episodes/03-serving-model/setup.sql
docker compose -f compose.yaml -f episodes/03-serving-model/compose.circuit.yaml \
  up -d --force-recreate engine
```

Open **https://localhost:5543**, switch to the **Logical / dbsp circuit** view, and confirm the
sidebar reads the episode-3 circuit back to you:

```
dbsp: 7 indexes · 1 counts · 0 served · 0 fallback
```

Seven indexes — four automatic primary-key arrangements plus the three app cohort indexes the
episode-3 overlay declared (`todos.list_id`, `list_members.user_id`, `list_members.list_id`). Hold
that number; you're about to change it.

## 1. A shape the pipeline can't serve yet

Episode 3's cohort key was **`list_id`** — *who may see a todo*. But a todo carries a second, entirely
different cohort axis the pipeline was never told about: **`assignee`** — *who is on the hook for it*.
The `todos` table has the column (`assignee text NOT NULL`, seeded in
[`03-serving-model/setup.sql`](../03-serving-model/setup.sql)), but the episode-3 config indexed
`todos` by `list_id` alone. Ask for a cohort keyed on `assignee` and the pipeline has nothing to
serve it from.

Ask for *every todo assigned to a member of the Reading list* — a **membership subquery**, exactly
the shape of episode 3's alice query, but keyed on `assignee` instead of `list_id`:

```sh
curl -si -G 'http://localhost:7010/v1/shape' \
  --data-urlencode "table=todos" \
  --data-urlencode "offset=-1" \
  --data-urlencode "where=assignee IN (SELECT user_id FROM list_members WHERE list_id = 3)" >/dev/null
```

The Reading list's only member is bob, so `assignee IN ('bob')` — you get back **three** todos (5,
6, 7). The *result* is correct. Look at *where it came from*, and that is the whole point of this
step.

Look at the canvas. The shape node appears, but it **draws no `serves · assignee` edge back to the
`todos` source** — because there *is* no `assignee` arrangement folded onto that source for the edge
to originate from. Its row wears no `circuit` badge. The circuit's cohort machinery is keyed on
`list_id`; this predicate decomposes over `assignee`, a key the pipeline doesn't compile, so it
matches no template. It doesn't error and it doesn't wait for a redeploy: the **fallback** picks it
up on the spot, re-deriving it statelessly by querying Postgres — episode 3's tier three, exactly.
The sidebar's `served` counter — index lookups answered from the circuit's snapshots — doesn't move
on this shape's account; it isn't served from any snapshot. This is the "stranger" tier, and the
todo app just grew a query that lands on it.

> The `+ new shape` form takes this predicate too: pick `todos` and the `WHERE` editor autocompletes
> the whole `assignee IN (SELECT user_id FROM list_members WHERE list_id = 3)` subquery. Submit and
> the same standalone node appears — no serving edge, latched onto no source. We stay on `curl` so
> the shape has a durable stream you can watch survive the rebuild in §3.

That is the setup. In episode 3 the lesson was *leave the stranger on the fallback; promote it at the
next deploy if it matters*. This dimension matters — "my assigned work" is a first-class view — so
now you deploy.

## 2. Extend the config, rebuild the circuit

Adding a cohort dimension the circuit already knows how to compile — *another lookup index* — is a
**configuration change, not a code change**. You append one column to the serving config and
force-recreate the engine. The overlay
([`compose.circuit.yaml`](compose.circuit.yaml)) is episode 3's config plus that one line:

```sh
# the episode-3 config, verbatim, plus todos.assignee — one new lookup arrangement
ELECTRIC_CIRCUITS_DBSP_INDEXES=todos.list_id,list_members.user_id,list_members.list_id,todos.assignee
ELECTRIC_CIRCUITS_DBSP_COUNTS=todos:list_id+done
ELECTRIC_CIRCUITS_DBSP_DIR=/tmp/dbsp
```

It is a strict superset of episode 3's overlay: the same three indexes and the same count, with
`todos.assignee` appended. Apply it the same way you applied the episode-3 config — a
force-recreate of the engine, on top of the base stack:

```sh
docker compose -f compose.yaml -f episodes/04-extending-the-pipeline/compose.circuit.yaml \
  up -d --force-recreate engine
```

You changed no Rust and wrote no SQL. A new lookup arrangement is a column plugged into a template
kind the circuit already has — the `INDEXES`/`COUNTS` half of "ship structure with deploys". (A
template kind the circuit *can't* express from config — a derived-visibility join, a new reduction —
would be new operators in `apps/engine/src/arrangements.rs`; that is the other, code-change kind of
"extend", and it is not this.)

Wait for the engine's health check, then look:

```sh
curl http://localhost:7010/health
# → ok
```

## 3. What happens on rebuild — watch it three ways

The force-recreate isn't a plain restart. The circuit's **layout fingerprint** — a hash of its index
and count specs (`layout_fingerprint` in `apps/engine/src/arrangements.rs`) — just changed: it went
from three app indexes to four. On boot the engine reads the old dbsp state's recorded fingerprint,
sees the mismatch, and **discards the checkpoint** proactively (`arrangements.rs`: *"index layout
changed; discarding dbsp state"*) rather than letting dbsp refuse it. Every arrangement then
**reseeds** from a fresh Postgres `REPEATABLE READ` snapshot — automatic, no error, one snapshot scan
per table. Watch it three ways:

1. **The sidebar's index count ticks up** — `7 indexes` becomes **`8 indexes`**, and the counter's
   `served`/`fallback` lookup tallies reset with the fresh circuit. The **`todos` source node** now
   wears the badge **`⧉ 3 idx · 1 cnt`** (its primary key, the `list_id` cohort index, and the new
   `assignee` cohort index, plus the `(list_id, done)` counts pipeline) — one tick up from episode
   3's `⧉ 2 idx · 1 cnt`. Click it: the **compiled dbsp arrangements** section now lists a
   `map_index(assignee)` alongside `map_index(list_id)`, each marked *seeded* once its snapshot loads.

2. **Your shapes are still there.** The arrangements were thrown away and rebuilt, but the shapes
   were not. They live in the engine's **durable shape catalog** — an append-only event stream
   (`meta/catalog`, the `CATALOG_STREAM` in `apps/engine/src/engine.rs`) kept in the durable-streams
   `ds` volume, which a `--force-recreate engine` does not touch. On boot the engine replays that
   catalog and re-registers every open shape, each replaying from its own durable stream. Nothing
   you subscribed to was lost across the rebuild.

3. **The fallback shape got promoted.** This is the one to watch. As the catalog restores each shape
   it **re-plans** it against the *new* arrangement set (`plan_circuit_shape` in `engine.rs`). Your
   Reading-list shape, keyed on `assignee`, could not be planned yesterday — `todos.assignee` wasn't
   arranged, so it fell to the fallback. Today it can: a `serves · assignee` edge **attaches** from
   the `todos` source into the shape's membership operator, its row lights up with the **`circuit`**
   badge, and its reads now answer from the `assignee` snapshot — the sidebar's `served` counter
   climbs on its account where before it didn't. Same shape, same `where` clause, same three rows —
   promoted from stranger to circuit-served the moment its dimension shipped.

Meanwhile every episode-3 shape that was circuit-served *stays* circuit-served — the overlay is a
superset, so `list_id` cohorts and the `(list_id, done)` count are untouched. You extended the
pipeline without disturbing what it already served.

### Restore vs. reseed — why this rebuild reseeds

Not every engine restart throws its state away. The fork is the layout fingerprint:

- **Unchanged config + a persistent `ELECTRIC_CIRCUITS_DBSP_DIR`** → the fingerprint matches, and the
  restart is a fast **checkpoint restore**: arrangements come back from disk and replay resumes from
  the checkpointed change-log offset. No snapshot scan.
- **Changed config (fingerprint mismatch) — or an ephemeral state dir** → the restart **reseeds**
  from Postgres.

This rebuild takes the reseed path *twice over*. The config changed (`7 → 8` indexes), which alone
forces a reseed; and the tutorial stack's `ELECTRIC_CIRCUITS_DBSP_DIR` is `/tmp/dbsp` — a container-local
path with no volume behind it — so a `--force-recreate` starts every arrangement from an empty
directory regardless. The tutorial stack **always reseeds**; that is deliberate, so this episode's
lesson is never masked by a lucky checkpoint hit. A reseed costs one snapshot scan per table, not an
outage: your shapes keep serving from their durable streams the whole time.

## 4. The boundary that makes this safe

Step back and the rule is simple, and it is the rule the whole three-tier model rests on:

- **Which columns you index or count is the _program_.** `todos.list_id`, `todos.assignee`,
  `todos:list_id+done` — these define the circuit's *shape*, they are fixed at construction, and
  changing them rebuilds it. They ship with a **restart**, exactly as source would. Adding
  `assignee` to the cohort set was editing the program.
- **Which _values_ flow through those columns is _data_.** `assignee = 'bob'`, list 3's roster,
  the individual todos — these are runtime. New users, new lists, new assignees, new filter
  combinations pour through the standing circuit and never rebuild anything; that was episode 3's
  entire lesson.

Cross the boundary the wrong way and you get the two classic mistakes: rebuilding the circuit for a
new *value* (you don't — that's just routing), or expecting a new *column* to appear without a deploy
(it can't — a dbsp circuit is fixed at construction). Keep index/group columns on the program side of
the line and runtime stays cheap, deploys stay rare, and "extending the pipeline" stays a
config-and-restart, not a rewrite.

## 5. What you now know

Extending a deployed pipeline is a **config change plus a rebuild**, and the engine makes the rebuild
safe. Adding a cohort dimension the circuit can already compile — another lookup index, another count
group — changes the **layout fingerprint**, so the old checkpoint is discarded and every arrangement
**reseeds** from a fresh Postgres snapshot; your **shapes survive** the whole thing in the durable
catalog and replay from their own streams; and any shape whose predicate now decomposes over the
new dimension is **re-planned on restore and promoted** from the fallback tier to circuit-served,
live. The dividing line under all of it: index and group **columns** are the program (they ship with
a restart), the **values** are data (they flow at runtime). "Only new templates grow the engine, and
templates ship with a deploy" is no longer a slogan — you just did it, and watched a stranger become
a first-class cohort in the process.

**Next → the full case study, [`docs/linearlite-circuit-design.md`](../../../docs/linearlite-circuit-design.md):**
the same three tiers — and this same extend-and-rebuild boundary — carrying the flagship
application's entire query graph, the one you've been syncing since episode 1. Which of its nine call
sites goes to which tier, which dimensions are compiled into the circuit versus left to routing and
the fallback, and why ordering and pagination deliberately stay in Postgres.
