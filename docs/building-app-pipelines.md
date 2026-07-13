# Building the pipeline for your app

How to go from an application's queries to a deploy-time dbsp pipeline, and how that pipeline
relates to the shapes clients actually open. Companion docs: `linearlite-circuit-design.md`
(how the flagship app's query graph is served), `ARCHITECTURE.md` §6b (the circuit),
and the recipe summary in `AGENTS.md`.

## The serving model: three tiers

The load-bearing separation is between what is *compiled* and what is *routed*:

| Tier | Cardinality | Cost of adding one | Serves |
|------|-------------|--------------------|--------|
| **The circuit** | few pipelines, fixed at deploy | circuit rebuild + reseed | query **families** (templates) |
| **Routing** | unbounded, changes at runtime | a routing-table entry | query **instances** (parameter combinations) |
| **Fallback** | unbounded | nothing | query **strangers** (predicates matching no template) |

- The **circuit** compiles one pipeline per query family — a table arrangement, a counts
  pipeline, a cohort feed — keyed by *cohort group* (per project, per (project, status), per
  aggregate group…). Its structure never grows with shapes, users, or parameter
  combinations — if a design makes it do so, the design is wrong (the circuit-per-shape trap:
  structure must never scale with subscriptions).
- A **shape** is a selection or union of cohort groups from one pipeline's keyed output,
  materialized at the delivery edge. Shape cardinality can vastly exceed pipeline cardinality:
  an issues filter exists per *combination* of projects clients ask for, all fed from the same
  `issues_by_project` pipeline. The union is correct only when the cohort key **partitions**
  the table (each row in exactly one group); overlapping groups need dedup at the edge.
- The **fallback** is the engine's dynamic path — standalone three-valued predicate
  evaluation (with the `AccessLeaf` index), `KeyRouter` equality families, and the subquery
  registry. It serves *any* predicate. The circuit is an optimization in front of it,
  never a correctness dependency: a brand-new query pattern works immediately at fallback
  cost, and is promoted into the circuit at the next deploy if it matters.

### Three kinds of "dynamic", and who serves them

1. **Dynamically created shapes** whose predicate decomposes over a template key
   (`project_id IN (3,7,9)`, `issue_id = X`) → the routing tier: `KeyRouter` families and the
   conjunct-indexed standalone path. These are deliberately *not* circuit-served — an indexed
   route finds a change's shapes in `O(log N)`, whereas a circuit shape would scan every
   delta linearly.
2. **Time-varying membership** (`project_id IN (SELECT … WHERE user_id = $me)`) → the
   always-on circuit: the membership table's deltas subscribe/unsubscribe
   the shape's cohort groups, and move-in/move-out are emitted from the post-transaction
   arrangement snapshots — no Postgres backfill, no snapshot gate. The dynamism moves from
   computation into routing.
3. **Predicates that cut across every template key** (`title LIKE '%foo%'`, nested or negated
   subqueries) → fallback.

## The recipe

1. **Enumerate the app's call sites and collapse them to templates.** Parameters become
   *data* — keys in an output index, rows in an input relation — never circuit structure.
2. **Find the access cohort** — the unit at which visibility is granted (project, workspace,
   channel, tenant). Key every pipeline output by it, never by user or shape. Verify the
   cohort key partitions the table. Genuinely per-user predicates (`assignee = $me`) get their
   own keyed feed: same pattern, cohort of size one.
3. **Make visibility relations the delivery router, not a join input.** A membership table's
   deltas drive subscribe/unsubscribe; move-in reads the cohort's post-transaction snapshot
   (no Postgres backfill, no snapshot gate, no re-query).
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
  subscribes them to that group and emits its current rows from the post-transaction
  snapshot: move-in with no computation.
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

## Setting up a pipeline in code

All three tiers ship in the engine and are always on. The circuit
(`src/arrangements.rs`) compiles two template kinds from env
configuration: **lookup arrangements** (`ELECTRIC_IVM_DBSP_INDEXES`) that serve subquery
re-derivations and shape seeding from local snapshots, and **counts pipelines**
(`ELECTRIC_IVM_DBSP_COUNTS`) that maintain a live COUNT per group. The sequencer serves
membership shapes and decomposable COUNT aggregates from that state end to end, for shapes whose
connecting columns are arrangement-indexed (`engine/circuit_serving.rs`; `ARCHITECTURE.md` §6b). New template
kinds extend the same skeleton; the feed, stepping, checkpoint, and restore plumbing in
`arrangements.rs` is reused unchanged.

### The skeleton (arrangements.rs, abridged)

A dbsp circuit is built once, inside the constructor closure of `Runtime::init_circuit`; the
closure declares inputs and wires operators, and returns the input handles the feeder uses:

```rust
let storage = CircuitStorageConfig::for_config(
    StorageConfig { path: dir, cache: StorageCacheConfig::default() },
    StorageOptions { min_storage_bytes, cache_mib, ..Default::default() },
)?;
let mut config = CircuitConfig::with_workers(1).with_storage(Some(storage));
config.max_rss_bytes = max_rss_bytes;

let (mut dbsp, inputs) = Runtime::init_circuit(config, move |circuit| {
    let mut handles = HashMap::new();
    for (table, table_specs) in &tables {
        // One Z-set input per replicated table: deltas are Vec<Tup2<Row, ZWeight>>.
        let (stream, handle) = circuit.add_input_zset::<Row>();
        for spec in table_specs {
            // A passive index: key by the projected columns, keep the full row,
            // integrate into a (disk-spillable) trace, publish a read snapshot.
            let slot = slot_for(spec);
            stream
                .map_index(move |row| (project(row, &spec.cols), row.clone()))
                .integrate_trace()
                .apply(move |spine| { *slot.write().unwrap() = Some(spine.ro_snapshot()); });
        }
        handles.insert(table.clone(), handle);
    }
    Ok(handles)
})?;

// Feeding (the sequencer does this per transaction):
inputs["todos"].append(&mut delta);   // Vec<Tup2<Row, ZWeight>>
dbsp.transaction()?;                  // one atomic step; snapshots update
```

Two dbsp API traps, learned the hard way: tap a trace stream with **`apply`, never
`inspect`** (`inspect` re-emits the `Spine` downstream, and `Spine::clone()` is
`unimplemented!()`); and snapshots publish only when a step runs, so run one empty
`transaction()` after a checkpoint restore before serving reads.

### The todo-model templates on the engine

The todo circuit above maps onto the engine's compiled template kinds directly:

- **(E) open_counts is a counts pipeline** — `ELECTRIC_IVM_DBSP_COUNTS=todos:list_id+done`
  compiles exactly `map_index((list_id, done)).weighted_count()` (`arrangements.rs`). Its
  per-step group deltas are drained after each transaction (`apply_count_deltas`,
  `engine/circuit_serving.rs`) and any COUNT aggregate whose predicate decomposes over the group columns is
  seeded by summing groups and updated live. The badge for list L is the `(L, false)` group;
  a dashboard sums the user's groups — one pipeline serves every filter combination.
- **(B)+(C) is a circuit-served membership shape** — with
  `ELECTRIC_IVM_DBSP_INDEXES=todos.list_id,list_members.user_id,list_members.list_id`
  declared, the shape `todos WHERE list_id IN (SELECT list_id FROM list_members WHERE
  user_id = $me)` is seeded from the arrangement snapshots and maintained by cohort routing
  in the sequencer: the `list_members` deltas refcount the shape's cohort groups, and
  move-in/move-out read the post-transaction snapshots (`CohortGroups`, `engine/executors.rs`).
- **(A) and (D)** are equality/match-all templates: the routing tier serves them by index,
  by design.

### Extending a deployed pipeline

Two operations wear the word "extend"; keep them apart.

- **Extend the config — a deploy, not a code change.** Adding a *cohort dimension the circuit already
  knows how to compile* — another lookup index or another count group — is a configuration change.
  Append the column to `ELECTRIC_IVM_DBSP_INDEXES` (a new membership/lookup cohort) or
  `ELECTRIC_IVM_DBSP_COUNTS` (a new count group) and force-recreate the engine. On boot the circuit's
  **layout fingerprint** changes, so the previous checkpoint is discarded and the arrangements
  **reseed** from a fresh Postgres `REPEATABLE READ` snapshot (`arrangements.rs`) — automatic, no
  error. Shapes are not lost: they replay from the durable shape catalog (`meta/catalog` in
  `engine/catalog.rs`), and any shape whose predicate now decomposes over the freshly-added dimension is
  **promoted** from the fallback tier to circuit-served on the spot — the catalog restore re-plans
  each shape against the new arrangement set (`plan_circuit_shape`), so a membership shape keyed on
  `todos.assignee` that fell to fallback yesterday is circuit-served the moment
  `ELECTRIC_IVM_DBSP_INDEXES=…,todos.assignee` ships. This is the `INDEXES`/`COUNTS` half of "ship
  structure with deploys" (recipe step 5).
- **Add a new template *kind* — a code change.** A pipeline the circuit cannot express from config
  (a derived-visibility join, a new delta-driven reduction) is new operators in `arrangements.rs`;
  see "Adding a new template kind" below. Config knobs plug *columns* into existing template kinds;
  a new kind is new operators.

**Checkpoint restore vs. reseed.** With config *unchanged* and a persistent `ELECTRIC_IVM_DBSP_DIR`,
a restart is a fast **checkpoint restore**: the layout fingerprint matches, state comes back from
disk, and replay resumes from the checkpointed change-log offset (offset + de-dup highwater intact).
With config *changed* (fingerprint mismatch) — or an ephemeral state dir — the restart **reseeds**
from Postgres instead. A config change always takes the reseed path; that is the intended deploy
story, and it costs one snapshot scan per table, not a service outage — shapes keep serving from
their durable streams throughout.

### Adding a new template kind

A new template is a few operators between an existing input and a new handle. Passive lookup
indexes use pull-style snapshots (`integrate_trace` + `apply(ro_snapshot)`); delta-driven
templates use `.output()` — an `OutputHandle` whose per-step delta is drained after each
transaction, exactly as the counts pipelines are today:

```rust
// Derived visibility needs ONE join (both sides are integrated by dbsp — priced state):
// comments keyed by their issue's project, re-homed automatically if the issue moves.
let issue_project = issues.map_index(|i: &Row| (i.get(PK).clone(), i.get(PROJECT_ID).clone()));
let comments_by_project = comments
    .map_index(|c: &Row| (c.get(ISSUE_ID).clone(), c.clone()))
    .join_index(&issue_project, |_issue, c, project| Some((project.clone(), c.clone())))
    .output();
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

Routing — which shapes subscribe to which groups — stays outside the circuit, driven by the
membership relation: a membership delta subscribes/unsubscribes a shape's cohort groups, and
move-in is served from the group's post-transaction snapshot, not by computing anything.

### Checklist for a new template

1. Decide its cohort key (§recipe step 2) and check it partitions the table.
2. Add the operators in the constructor closure; linear ops free, joins/aggregates priced.
3. `.output()` for a delta-driven template (drained per step) or
   `integrate_trace()+apply(ro_snapshot)` for a lookup index (pull).
4. Drain the new handle in the step loop; route by group at the delivery edge.
5. The layout fingerprint changes → state is discarded and reseeded on next boot; that is
   the intended deploy story. `cargo test -p electric-ivm-engine` and the conformance suite
   (`pnpm test:conformance` — always exercises the on circuit) are the safety net.

`linearlite-circuit-design.md` maps the full flagship application onto these tiers.
