# Building the pipeline for your app

How to go from an application's queries to a deploy-time dbsp pipeline, and how that pipeline
relates to the shapes clients actually open. Companion docs: `linearlite-circuit-design.md`
(how the flagship app's query graph is served), `ARCHITECTURE.md` §6b (the circuit),
and the recipe summary in `AGENTS.md`.

## The serving model: three tiers

The load-bearing separation is between what is *compiled* and what is *routed*:

| Tier | Cardinality | Cost of adding one | Serves |
|------|-------------|--------------------|--------|
| **The circuit** | few counts pipelines, fixed at deploy | a config change + restart (counts reseed on boot) | COUNT **families** (templates) |
| **Routing** | unbounded, changes at runtime | a routing-table entry | query **instances** (parameter combinations) |
| **Fallback** | unbounded | nothing | query **strangers** (predicates matching no template) |

- The **circuit** compiles one counts pipeline per aggregate family —
  `map_index(group) → weighted_count` — keyed by *cohort group* (per project, per
  (project, status), per aggregate group…). Its state is O(distinct groups), held in memory;
  **row data lives in Postgres, never in the circuit**. Its structure never grows with shapes,
  users, or parameter combinations — if a design makes it do so, the design is wrong (the
  circuit-per-shape trap: structure must never scale with subscriptions).
- A **circuit-served shape** (a COUNT aggregate) is a selection or sum of cohort groups from
  one pipeline's keyed output, materialized at the delivery edge. Shape cardinality can
  vastly exceed pipeline cardinality: a count exists per *combination* of projects clients
  ask for, all fed from the same per-(project, status) pipeline. The sum is correct only when
  the cohort key **partitions** the table (each row in exactly one group); overlapping groups
  need dedup at the edge.
- The **routing tier and fallback** are the engine's dynamic path — `KeyRouter` equality
  families, the conjunct-indexed standalone path and conjunct-indexed aggregates, standalone
  three-valued predicate evaluation (with the `AccessLeaf` index), and the subquery registry,
  which serves **every** membership subquery: single-level and nested, negated or not. Together
  they serve *any* predicate. The circuit is an optimization in front of them, never a
  correctness dependency: a brand-new query pattern works immediately at fallback cost, and a
  COUNT that matters is promoted into the circuit at the next deploy.

### Three kinds of "dynamic", and who serves them

1. **Dynamically created shapes** whose predicate decomposes over a template key
   (`project_id IN (3,7,9)`, `issue_id = X`) → the routing tier: `KeyRouter` families and the
   conjunct-indexed standalone path. These are deliberately *not* circuit-served — an indexed
   route finds a change's shapes in `O(log N)`, whereas a circuit shape would scan every
   delta linearly.
2. **Time-varying membership** (`project_id IN (SELECT … WHERE user_id = $me)`) → the
   subquery registry: one shared inner-set node per distinct subquery, created in two phases
   (a Postgres backfill under a `REPEATABLE READ` snapshot, `SnapshotGate`-fenced against the
   live log). Membership-table deltas reconcile the node's contributors; a value flip queries
   the affected outer rows back from Postgres on the parallel flip-worker pool and emits them
   absolutely, per pk, through ordered per-stream emission lanes.
3. **Predicates that cut across every template key** (`title LIKE '%foo%'`, nested or negated
   subqueries) → fallback.

## The recipe

1. **Enumerate the app's call sites and collapse them to templates.** Parameters become
   *data* — keys in an output index, rows in an input relation — never circuit structure.
2. **Find the access cohort** — the unit at which visibility is granted (project, workspace,
   channel, tenant). Key every counts group by it, never by user or shape. Verify the
   cohort key partitions the table. Genuinely per-user predicates (`assignee = $me`) get their
   own group column: same pattern, cohort of size one.
3. **Write visibility as a membership subquery; the registry serves it.** The membership
   table feeds one shared inner-set node per distinct subquery (two-phase creation: Postgres
   backfill under `REPEATABLE READ`, `SnapshotGate`-fenced against the live log). Membership
   deltas reconcile the node; a flip queries the affected outer rows back from Postgres on
   the parallel flip-worker pool and emits them absolutely, per pk, through ordered
   per-stream emission lanes — move-in/move-out is a bounded query-back, not a
   re-computation of anything else.
4. **Compose each counts template from linear operators, pricing state honestly.** Linear
   operators (filter, project, `map_index`) are free — no state — and `weighted_count` is one
   integer per group, so circuit state stays O(distinct groups), in memory. Aggregate at the
   finest useful group grain and let the reader sum groups, so one pipeline serves every
   filter combination. Anything that would integrate row data engine-side (a join stores
   *both* inputs) reintroduces a table copy — derived visibility belongs in the registry's
   subqueries, with row lookups going to Postgres.
5. **Ship structure with deploys.** A new counts template = a config change
   (`ELECTRIC_CIRCUITS_DBSP_COUNTS`) + a restart; counts state is in-memory and reseeds on every
   boot from one group-aggregated Postgres snapshot per table (O(groups), not O(rows)). Leave
   ordering/pagination to the database (keyset pages + a changes-only live tail) — don't
   force topk into the circuit to reproduce what SQL already does well.

## A simple model, end to end

A todo app: `lists(id, name)`, `todos(id, list_id, done, title, assignee)`,
`list_members(id, list_id, user_id)`. Its queries: "my lists", "todos of a list", "todos of
all my lists", "my assigned todos", "open-todo count per list".

Step 1 — templates: five (already listed). Step 2 — the cohort is the **list**; `list_id`
partitions `todos`. Step 3 — `list_members` is the visibility subquery's inner relation.
Steps 4–5 — the logical pipeline decomposition:

```text
lists ────────────────────────────────────────▶ (A) lists_all            one global feed
list_members ─ map_index(user_id) ────────────▶ (B) memberships_by_user  per-user feed = the visibility relation
todos ─┬─ map_index(list_id) ─────────────────▶ (C) todos_by_list        per-list cohort feed
       ├─ map_index(assignee) ────────────────▶ (D) todos_by_assignee    per-user feed (cohort of one)
       └─ map_index((list_id, done))
          .aggregate_linear(count) ───────────▶ (E) open_counts          per-(list, done) live counts
```

How every query is served, with per-user engine state bounded by the inner sets (a handful
of list ids each), never by todo rows:

- **"my lists"** → feed B keyed by the user (also drives everything below).
- **"todos of list L"** → feed C, group L. One shape per list, shared by all members.
- **"todos of all my lists"** → the union of C's groups for the user's memberships — resolved
  at the delivery edge (or client-side). On the engine this union is the registry shape
  `todos WHERE list_id IN (SELECT …)`: joining a list flips the list into the user's inner
  set, and the flip query-back pulls that list's todos from Postgres — one bounded query,
  no re-computation of anything else.
- **"my assigned todos"** → feed D, group = the user. A genuinely per-user predicate as its
  own small pipeline.
- **"open count per list"** → feed E: the badge for list L = the `(L, false)` group; a
  dashboard across lists sums the user's groups. COUNT is linear, so each group is one
  maintained integer.

A shape like `todos WHERE list_id IN (3,7,9) AND done = false` needs no new pipeline: it is
groups {3,7,9} of C filtered on `done` client-side — or, if that filter must be server-side,
key C by `(list_id, done)` instead (same feed, finer groups). A shape like
`title LIKE '%urgent%'` matches no template: fallback path, works immediately.

State, engine-side: E's count groups (one integer per (list, done) group, in memory) and the
membership subqueries' inner sets (list ids + contributing membership pks) — **no copy of
`todos` or `lists` anywhere**; row data stays in Postgres. A thousand users cost a thousand
subscriptions and a thousand small inner sets.

## Setting up a pipeline in code

All three tiers ship in the engine and are always on. The circuit
(`src/arrangements.rs`) compiles one template kind from env configuration: **counts
pipelines** (`ELECTRIC_CIRCUITS_DBSP_COUNTS=table:col+col`) that maintain a live COUNT per group —
O(distinct groups), in memory. The sequencer serves decomposable COUNT aggregates from that
state end to end (`engine/circuit_serving.rs`; `ARCHITECTURE.md` §6b); membership shapes are
served by the subquery registry against Postgres. `ELECTRIC_CIRCUITS_DBSP_INDEXES` is deprecated
and ignored (warn-and-ignore): row data lives in Postgres, and row lookups are pooled queries
(`engine/membership.rs`). New template kinds extend the same skeleton; the feed and stepping
plumbing in `arrangements.rs` is reused unchanged.

### The skeleton (arrangements.rs, abridged)

A dbsp circuit is built once, inside the constructor closure of `Runtime::init_circuit`; the
closure declares inputs and wires operators, and returns the handles the feeder and the step
loop use. There is no storage config, spill, or checkpoint — the counts state is in-memory
and reseeded on every boot (`seed_groups` feeds one synthetic weighted row per group):

```rust
let (dbsp, (inputs, count_outputs)) =
    Runtime::init_circuit(CircuitConfig::with_workers(1), move |circuit| {
        let mut handles: HashMap<String, ZSetHandle<Row>> = HashMap::new();
        let mut count_handles: HashMap<String, CountOutput> = HashMap::new();
        for spec in &ctor_specs {
            // One Z-set input per COUNTED table: deltas are Vec<Tup2<Row, ZWeight>>.
            let (stream, handle) = circuit.add_input_zset::<Row>();
            let (gcols, cslot) = ctor_counts.get(&spec.table).expect("count slot").clone();
            let counted = stream
                .map_index(move |row| (project(row, &gcols), ()))
                .weighted_count();
            // `apply`, not `inspect`: `inspect` re-emits the `Spine` downstream, which
            // clones it (unimplemented for spines). `apply` produces the snapshot only.
            counted.integrate_trace().apply(move |spine| {
                *cslot.write().expect("count slot lock") = Some(spine.ro_snapshot());
            });
            // `accumulate_output`, not `output`: a transaction can span several
            // microsteps, and the plain mailbox only holds the last one's delta.
            count_handles.insert(spec.table.clone(), counted.accumulate_output());
            handles.insert(spec.table.clone(), handle);
        }
        Ok((handles, count_handles))
    })?;

// Feeding (the sequencer does this per change-log batch):
inputs["todos"].append(&mut delta);   // Vec<Tup2<Row, ZWeight>>
dbsp.transaction()?;                  // one atomic step; snapshots update, and
                                      // count_outputs hold the step's group deltas
```

Two dbsp API traps, learned the hard way: tap a trace stream with **`apply`, never
`inspect`** (`inspect` re-emits the `Spine` downstream, and `Spine::clone()` is
`unimplemented!()`); and drain delta-driven outputs via **`accumulate_output()`, never
`output()`** — a transaction can span several microsteps, and the plain mailbox only holds
the last microstep's delta.

### The todo-model templates on the engine

The todo circuit above maps onto the engine's compiled template kinds directly:

- **(E) open_counts is a counts pipeline** — `ELECTRIC_CIRCUITS_DBSP_COUNTS=todos:list_id+done`
  compiles exactly `map_index((list_id, done)).weighted_count()` (`arrangements.rs`). Its
  per-step group deltas are drained after each transaction (`apply_count_deltas`,
  `engine/circuit_serving.rs`) and any COUNT aggregate whose predicate decomposes over the group columns is
  seeded by summing groups and updated live. The badge for list L is the `(L, false)` group;
  a dashboard sums the user's groups — one pipeline serves every filter combination.
- **(B)+(C) is a registry-served membership shape** — the shape `todos WHERE list_id IN
  (SELECT list_id FROM list_members WHERE user_id = $me)` is served by the subquery registry,
  like every membership subquery: two-phase creation (a Postgres backfill under a
  `REPEATABLE READ` snapshot, `SnapshotGate`-fenced against the live log), with one shared
  inner-set node per distinct subquery (`subquery.rs`). `list_members` deltas reconcile the
  node's contributor sets; a flip (the user joins/leaves a list) queries that list's todos
  back from Postgres on the parallel flip-worker pool (`ELECTRIC_CIRCUITS_FLIP_WORKERS`,
  `engine/mod.rs`) and emits them absolutely, per pk, through ordered per-stream emission
  lanes (`engine/emission.rs`).
- **(A) and (D)** are equality/match-all templates: the routing tier serves them by index,
  by design.

### Extending a deployed pipeline

Two operations wear the word "extend"; keep them apart.

- **Extend the config — a deploy, not a code change.** Adding a *count group the circuit already
  knows how to compile* is a configuration change: append the table/columns to
  `ELECTRIC_CIRCUITS_DBSP_COUNTS` and restart the engine. Counts state is in-memory and reseeds on
  **every** boot from one `SELECT <group_cols>, count(*) … GROUP BY` per table under a
  `REPEATABLE READ` snapshot — O(groups), not O(rows) — with a `SnapshotGate` fencing change-log
  replay (`maybe_start_arrangements`, `engine/mod.rs`). There is no layout fingerprint, no
  checkpoint, and nothing to migrate. Shapes are not lost: they replay from the durable shape
  catalog (`meta/catalog` in `engine/catalog.rs`), and any COUNT aggregate whose predicate
  decomposes over the freshly-added group columns is circuit-served the moment the config ships.
  (`ELECTRIC_CIRCUITS_DBSP_INDEXES` is deprecated and ignored — row data lives in Postgres, and
  membership shapes are registry-served regardless of config.) This is the `COUNTS` half of
  "ship structure with deploys" (recipe step 5).
- **Add a new template *kind* — a code change.** A pipeline the circuit cannot express from config
  (a new delta-driven reduction) is new operators in `arrangements.rs`;
  see "Adding a new template kind" below. Config knobs plug *columns* into existing template kinds;
  a new kind is new operators.

**Every restart reseeds.** The circuit holds no durable state: a restart — config change or
not — rebuilds the circuit and reseeds each counts pipeline from a fresh group-aggregated
Postgres snapshot. That costs one `GROUP BY` scan per counted table, not a service outage —
shapes keep serving from their durable streams throughout, and each seed's `SnapshotGate`
fences the replayed change log against double-apply.

### Adding a new template kind

A new template is a few operators between an existing input and a new handle. Pull-style
reads use `integrate_trace` + `apply(ro_snapshot)` (how the count snapshots publish);
delta-driven templates use `.accumulate_output()` — an `OutputHandle` whose per-transaction
delta is drained after each step, exactly as the counts pipelines are today. Price the state
honestly: the circuit is in-memory, so an operator that integrates row data (a join stores
*both* inputs) reintroduces engine-side table copies — prefer group-grain state, and keep
row lookups in Postgres (`engine/membership.rs`):

```rust
// Derived visibility needs ONE join (both sides are integrated by dbsp — priced, in-memory,
// row-holding state — weigh it against a registry subquery before shipping):
// comments keyed by their issue's project, re-homed automatically if the issue moves.
let issue_project = issues.map_index(|i: &Row| (i.get(PK).clone(), i.get(PROJECT_ID).clone()));
let comments_by_project = comments
    .map_index(|c: &Row| (c.get(ISSUE_ID).clone(), c.clone()))
    .join_index(&issue_project, |_issue, c, project| Some((project.clone(), c.clone())))
    .accumulate_output();
```

Drain the handle in `circuit_thread`'s `Cmd::Batch` arm (the step loop) and fan the delta out
by cohort key — this is the pipeline→shape edge, and the only per-shape work in the system:

```rust
let delta = comments_by_project.consolidate();    // this step's delta, merged
let mut cursor = delta.cursor();
while cursor.key_valid() {                        // key = the cohort (project_id)
    let group = cursor.key().clone();
    // route this group's (row, weight) pairs to the shapes subscribed to the cohort
    ...
    cursor.step_key();
}
```

Routing — which shapes subscribe to which groups — stays outside the circuit. Membership
itself is the registry's job: a membership delta flips values in the shared inner set, and
move-in is a pooled Postgres query-back for the flipped group's rows, not a recomputation of
the pipeline.

### Checklist for a new template

1. Decide its cohort key (§recipe step 2) and check it partitions the table.
2. Add the operators in the constructor closure; linear ops free, joins/aggregates priced.
3. `.accumulate_output()` for a delta-driven template (drained per step) or
   `integrate_trace()+apply(ro_snapshot)` for a pull-style snapshot.
4. Drain the new handle in the step loop; route by group at the delivery edge.
5. State is in-memory: every boot reseeds, so a new template needs a group-aggregated
   seeding query (like `backfill_group_counts`) and a `SnapshotGate`.
   `cargo test -p electric-circuits-engine` and the conformance suite
   (`pnpm test:conformance` — always exercises the circuit) are the safety net.

`linearlite-circuit-design.md` maps the full flagship application onto these tiers.
