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

## Setting up a pipeline in code

Today the engine ships the tier-3 fallback (always on) and a tier-1 *degenerate* pipeline —
the arrangement layer (`src/arrangements.rs`, `ELECTRIC_IVM_DBSP=1`): passive indexes that
serve subquery re-derivations locally. Everything below extends the same skeleton; the feed,
stepping, checkpoint, and restore plumbing in `arrangements.rs` is reused unchanged.

### The skeleton that exists (arrangements.rs, abridged)

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

### Adding the todo-model templates

Each recipe template is a few operators between an existing input and a new **output
handle**. Where the passive indexes use pull-style snapshots (`integrate_trace` +
`apply(ro_snapshot)`), cohort *feeds* want push-style deltas: `.output()` returns an
`OutputHandle` whose per-step delta you drain after each transaction and route to per-cohort
durable streams.

```rust
// (B) memberships_by_user — THE ROUTER: per-user membership deltas.
let router = members
    .map_index(|m: &Row| (m.get(USER_ID).clone(), m.clone()))
    .output();                              // OutputHandle<OrdIndexedZSet<Value, Row>>

// (C) todos_by_list — the cohort feed. list_id partitions todos, so a shape for
// lists {3,7,9} is the union of three groups of THIS one pipeline.
let todos_by_list = todos
    .map_index(|t: &Row| (t.get(LIST_ID).clone(), t.clone()))
    .output();

// (E) open_counts — COUNT is linear: one maintained integer per (list_id, done) group.
let open_counts = todos
    .map_index(|t: &Row| (Row(vec![t.get(LIST_ID).clone(), t.get(DONE).clone()]), ()))
    .weighted_count()                       // or .aggregate_linear(|_| 1)
    .output();

// Derived visibility needs ONE join (both sides are integrated by dbsp — priced state):
// comments keyed by their issue's project, re-homed automatically if the issue moves.
let issue_project = issues.map_index(|i: &Row| (i.get(PK).clone(), i.get(PROJECT_ID).clone()));
let comments_by_project = comments
    .map_index(|c: &Row| (c.get(ISSUE_ID).clone(), c.clone()))
    .join_index(&issue_project, |_issue, c, project| Some((project.clone(), c.clone())))
    .output();
```

### Draining outputs into shape streams

After each `dbsp.transaction()` (in `circuit_thread`'s `Cmd::Batch` arm), drain each output
handle and fan the delta out by cohort key — this is the pipeline→shape edge, and the only
per-shape work in the system:

```rust
let delta = todos_by_list.consolidate();          // this step's delta, merged
let mut cursor = delta.cursor();
while cursor.key_valid() {                        // key = the cohort (list_id)
    let group = cursor.key().clone();
    // collect this group's (row, weight) pairs and append them, as envelopes,
    // to every stream routed to this cohort group (`shape/list:<id>`, unions, …)
    ...
    cursor.step_key();
}
```

Routing — which shapes subscribe to which groups — stays outside the circuit, driven by the
router feed (B): a membership delta subscribes/unsubscribes a client to cohort streams, and
move-in is served by replaying the group's log, not by computing anything.

### Checklist for a new template

1. Decide its cohort key (§recipe step 2) and check it partitions the table.
2. Add the operators in the constructor closure; linear ops free, joins/aggregates priced.
3. `.output()` for a feed (push deltas → durable streams) or
   `integrate_trace()+apply(ro_snapshot)` for a lookup index (pull).
4. Drain the new handle in the step loop; route by group at the delivery edge.
5. The layout fingerprint changes → state is discarded and reseeded on next boot; that is
   the intended deploy story. `cargo test -p electric-ivm-engine` and the conformance suite
   (`pnpm test:conformance`, with and without `ELECTRIC_IVM_DBSP=1`) are the safety net.

`linearlite-circuit-design.md` works the full application through this process.
