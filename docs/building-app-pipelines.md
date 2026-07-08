# Building the pipeline for your app

How to go from an application's queries to a deploy-time dbsp pipeline, and how that pipeline
relates to the shapes clients actually open. Companion docs: `linearlite-circuit-design.md`
(the full worked example), `ARCHITECTURE.md` §6b (the arrangement layer that exists today),
and the recipe summary in `AGENTS.md`.

## The serving model: three tiers

The load-bearing separation is between what is *compiled* and what is *routed*:

| Tier | Cardinality | Cost of adding one | Serves |
|------|-------------|--------------------|--------|
| **Pipelines** | few, fixed at deploy | circuit rebuild + reseed | query **families** (templates) |
| **Routing** | unbounded, changes at runtime | a routing-table entry | query **instances** (parameter combinations) |
| **Fallback** | unbounded | nothing | query **strangers** (predicates matching no template) |

- A **pipeline** computes one delta stream per *cohort group* (per project, per
  (project, status), per aggregate group…). Its structure never grows with shapes, users, or
  parameter combinations — if a design makes it do so, the design is wrong (that is the
  circuit-per-shape mistake this repo made and removed; see the history around `75488b6`).
- A **shape** is a selection or union of cohort groups from one pipeline's keyed output,
  materialized at the delivery edge. Shape cardinality can vastly exceed pipeline cardinality:
  an issues filter exists per *combination* of projects clients ask for, all fed from the same
  `issues_by_project` pipeline. The union is correct only when the cohort key **partitions**
  the table (each row in exactly one group); overlapping groups need dedup at the edge.
- The **fallback** is the engine's existing dynamic path — standalone three-valued predicate
  evaluation (with the `AccessLeaf` index), `KeyRouter` equality families, and the subquery
  registry. It serves *any* predicate. The pipeline tier is an optimization in front of it,
  never a correctness dependency: a brand-new query pattern works immediately at fallback
  cost, and is promoted into the circuit at the next deploy if it matters.

### Three kinds of "dynamic", and who serves them

1. **Dynamically created shapes** whose predicate decomposes over a pipeline key
   (`project_id IN (3,7,9)`) → routing. No circuit change, ever.
2. **Time-varying membership** (`project_id IN (SELECT … WHERE user_id = $me)`) → routing
   *driven by a feed*: the membership pipeline's deltas subscribe/unsubscribe the shape to
   cohort groups; move-in is served by replaying the newly subscribed group's log, move-out by
   unsubscribing. The dynamism moves from computation into routing.
3. **Predicates that cut across every pipeline key** (`title LIKE '%foo%'`,
   `priority = 'high'` with no priority-keyed pipeline) → fallback.

## The recipe

1. **Enumerate the app's call sites and collapse them to templates.** Parameters become
   *data* — keys in an output index, rows in an input relation — never circuit structure.
2. **Find the access cohort** — the unit at which visibility is granted (project, workspace,
   channel, tenant). Key every pipeline output by it, never by user or shape. Verify the
   cohort key partitions the table. Genuinely per-user predicates (`assignee = $me`) get their
   own keyed feed: same pattern, cohort of size one.
3. **Make visibility relations the delivery router, not a join input.** A membership feed's
   deltas drive subscribe/unsubscribe; backfill = replaying the cohort feed's own durable log
   (no source snapshot, no fencing, no re-query).
4. **Compose each template from operators, pricing state honestly.** Linear operators
   (filter, project, `map_index`) are free — no state. Joins store *both* inputs
   (acceptable with disk spilling — `ARCHITECTURE.md` §6b); use them for *derived* visibility
   (a comment inherits its issue's project) and deep hierarchies. `aggregate_linear`
   (COUNT/SUM) is cheap: aggregate at the finest useful group grain and let the reader sum
   groups, so one pipeline serves every filter combination.
5. **Ship structure with deploys.** New templates = circuit rebuild + reseed (the layout
   fingerprint handles discard/reseed automatically) or `Mode::Persistent` bootstrap. Leave
   ordering/pagination to the database (keyset pages + a changes-only live tail from the
   cohort feed) — don't force topk into the circuit to reproduce what SQL already does well.

## A simple model, end to end

A todo app: `lists(id, name)`, `todos(id, list_id, done, title, assignee)`,
`list_members(id, list_id, user_id)`. Its queries: "my lists", "todos of a list", "todos of
all my lists", "my assigned todos", "open-todo count per list".

Step 1 — templates: five (already listed). Step 2 — the cohort is the **list**; `list_id`
partitions `todos`. Step 3 — `list_members` is the router. Steps 4–5 — the circuit:

```text
lists ────────────────────────────────────────▶ (A) lists_all            one global feed
list_members ─ map_index(user_id) ────────────▶ (B) memberships_by_user  per-user feed = THE ROUTER
todos ─┬─ map_index(list_id) ─────────────────▶ (C) todos_by_list        per-list cohort feed
       ├─ map_index(assignee) ────────────────▶ (D) todos_by_assignee    per-user feed (cohort of one)
       └─ map_index((list_id, done))
          .aggregate_linear(count) ───────────▶ (E) open_counts          per-(list, done) live counts
```

How every query is served, with zero per-user engine state:

- **"my lists"** → feed B keyed by the user (also drives everything below).
- **"todos of list L"** → feed C, group L. One shape per list, shared by all members.
- **"todos of all my lists"** → the union of C's groups for the user's memberships — resolved
  at the delivery edge (or client-side). When the user joins a list, B emits a delta, delivery
  subscribes them to that group and replays its log: move-in with no computation.
- **"my assigned todos"** → feed D, group = the user. A genuinely per-user predicate as its
  own small pipeline.
- **"open count per list"** → feed E: the badge for list L = the `(L, false)` group; a
  dashboard across lists sums the user's groups. COUNT is linear, so each group is one
  maintained integer.

A shape like `todos WHERE list_id IN (3,7,9) AND done = false` needs no new pipeline: it is
groups {3,7,9} of C filtered on `done` client-side — or, if that filter must be server-side,
key C by `(list_id, done)` instead (same table copy, finer groups). A shape like
`title LIKE '%urgent%'` matches no template: fallback path, works immediately.

State: `todos` ×2–3 (by list, by assignee, count groups), `lists`/`list_members` ×1 — a small
constant factor over one table copy, on disk, **independent of user count**. A thousand users
cost a thousand subscriptions.

## Where this lives in the code

Today the engine ships the tier-3 fallback (always on) and a tier-1 *degenerate* pipeline —
the arrangement layer (`src/arrangements.rs`, `ELECTRIC_IVM_DBSP=1`): passive indexes
(`input → map_index → integrate_trace`) that serve subquery re-derivations locally. The
constructor closure in `Arrangements::start` is where a fuller app pipeline slots in: each
recipe template is a handful of operators between the existing inputs and a new output, using
the same feed, stepping, checkpoint, and snapshot plumbing. `linearlite-circuit-design.md`
works this through for a complete application.
