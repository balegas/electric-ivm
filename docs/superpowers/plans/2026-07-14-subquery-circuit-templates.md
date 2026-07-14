# Subqueries in the DBSP Circuit as Parameterized Templates — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Replace the subquery registry's hand-rolled contributor-set kernel with a dbsp
circuit that maintains membership sets and emits flips, grouped by parameterized templates.

**Architecture:** Per the approved spec (`docs/superpowers/specs/2026-07-14-subquery-circuit-templates-design.md`,
incl. §11b as-built amendments): the registry evaluates templates host-side (under its lock,
per envelope — same ordering as today) and feeds exact weighted `(node_id, value, pk)` tuples
into a dedicated always-on membership circuit (`input → map(drop pk) → integrate_trace
snapshot + distinct → accumulate_output`). Distinct output deltas are the flips; the
integrated trace serves `contains`/`has_null`/introspection. Reverse index (`pk → value`)
stays host-side per node (reconcile-by-identity; nested-IN residuals make row→tuple impure).
Everything above flip detection (edges, absolute emission, known_members, flip workers,
emission lanes, three-phase create) is preserved.

**Tech Stack:** Rust, dbsp 0.318 (Feldera), tokio; existing test suites (cargo test, vitest
oracle conformance, electric-conformance oracle, LinearLite demo).

## Global Constraints

- Direct replacement: no feature flag; conformance suites gate the branch.
- Bind-gated: memory proportional to subscriptions; each new node still seeds from PG.
- No new env vars. `ELECTRIC_IVM_FLIP_WORKERS` / `ELECTRIC_IVM_EMIT_LANES` unchanged.
- Node identity = existing literal-level `SubquerySig` (edges/sharing unchanged); template
  layer sits above it for eval grouping.
- Counts circuit (`arrangements.rs`) untouched.
- Work on branch `feat/subquery-circuit-templates`; commit per task; never push.
- PR #31 threaded `lsn: Option<String>` through `FlipWork` and `propagate_flips` — keep it.

---

### Task 1: MembershipCircuit (`apps/engine/src/subq_circuit.rs`)

**Files:**
- Create: `apps/engine/src/subq_circuit.rs`
- Modify: `apps/engine/src/lib.rs` (add `pub mod subq_circuit;`)

**Interfaces (Produces):**
```rust
pub struct MembershipCircuit { /* tx, snapshot slot */ }  // Clone
pub struct MemberDelta { pub node_id: i64, pub value: Value, pub delta: i64 } // ±1 net per key
impl MembershipCircuit {
    pub fn start() -> Result<MembershipCircuit>;
    /// Feed tuples Row([Int(node_id), value, Text(pk)]) weighted ±1; step; return flips.
    pub async fn apply(&self, tuples: Vec<Tup2<Row, ZWeight>>) -> Vec<MemberDelta>;
    pub fn contains(&self, node_id: i64, value: &Value) -> bool;      // snapshot seek
    /// (value, contributor_count) pairs for a node, up to cap; plus total distinct count.
    pub fn values_for_node(&self, node_id: i64, cap: usize) -> (usize, Vec<(Value, usize)>);
    pub async fn shutdown(&self);
}
```

Pipeline inside `Runtime::init_circuit(CircuitConfig::with_workers(1), …)`:
`add_input_zset::<Row>()` → `map(|t| Row(t.0[..t.0.len()-1].to_vec()))` (drop pk) →
publish `integrate_trace().apply(ro_snapshot → slot)` (weight = contributor count) →
`.distinct().accumulate_output()` (drain per step: net weight per key = flip).
Dedicated OS thread `dbsp-subq`, bounded mpsc(256), one `dbsp.transaction()` per apply —
mirror `arrangements.rs::circuit_thread` minus the highwater (host tuples are already exact:
reconcile-by-identity makes duplicates impossible).

**Steps:**
- [x] 1. Write failing unit tests in `subq_circuit.rs` `#[cfg(test)]`:
  - `flips_on_zero_crossings`: two contributors to value 5 on node 1 → one Enter; remove one
    → no flip; remove last → Leave. Assert parity with
    `engine::membership::fold_refcount_flips` on the same contribution stream.
  - `nodes_are_isolated`: same value on node 1 and node 2 → independent flips/contains.
  - `contains_and_null_bucket`: `contains(1, &Value::Null)` true after a NULL-value tuple.
  - `values_for_node_reports_contributor_counts`.
  - `retract_insert_same_step_nets`: (v_old,-1),(v_new,+1) in one apply → Leave(v_old)+Enter(v_new).
- [x] 2. `cargo test -p electric-ivm-engine subq_circuit` → FAIL (module missing).
- [x] 3. Implement. Snapshot point-read: `cursor.seek_key(key.erase())` via
  `dbsp::dynamic::Erase` (compare `count_groups`'s downcast style). If `seek_key` fights the
  dynamic API, fallback is a linear cursor scan bounded by node prefix — but prefer seek.
- [x] 4. Tests pass. 5. Commit `feat: membership circuit (dbsp distinct flip detection)`.

### Task 2: Template extraction (`apps/engine/src/predicate.rs`)

**Interfaces (Produces):**
```rust
/// Lift top-level AND non-NULL equality conjuncts (distinct cols) from a subquery's inner
/// WHERE into parameters. Returns (template_key, bind) — bind sorted by column name.
/// No liftable conjuncts ⇒ empty bind, residual = whole where.
pub fn subquery_template(
    table: &str, project: &str, where_: Option<&PredicateJson>,
) -> (String, Vec<(String, serde_json::Value)>);
```
Template key: `format!("{table}|{project}|P({cols})|{residual_canon}")`, residual = remaining
conjuncts via `canonical_pred` (AND of the rest, order-insensitive; `MatchAll` → empty).
Lifting mirrors `equality_template`'s rules on the JSON AST: only top-level `And` chains
(recursively flattened), `Leaf{op: Eq, value != null}`, distinct columns; a duplicate column
or non-eq leaf stays in the residual (do NOT reject the template — residual absorbs it).

**Steps:**
- [x] 1. Failing tests: same-shape-different-literal → same key, different binds;
  `A(user=1,status='x')` vs `A(user=2,status='y')` → same key, binds `[status,user]` sorted;
  range/OR/nested-IN stay residual; no-where → key with empty P and empty residual;
  nested-IN inner where produces stable key (recursion via canonical_pred only).
- [x] 2. FAIL → 3. implement → 4. PASS → 5. Commit `feat: subquery template extraction`.

### Task 3: Registry on the circuit (`apps/engine/src/subquery.rs`)

The core swap. **Node changes:** remove `contributors`; add `node_id: i64`; keep `pk_value`.
`reconcile_row` → `reconcile_row_tuples(&mut self, pk, present: Option<Value>) -> Vec<Tup2<Row, ZWeight>>`
(retract `[Int(node_id), old_v, Text(pk)]` −1 / insert new +1; no-op if unchanged).
`contains`/`has_null`/`distinct_values` on the node are DELETED (serve from circuit).

**Registry changes:**
- Fields: `circuit: crate::subq_circuit::MembershipCircuit` (started in `new()`),
  `next_node_id: i64`, `node_by_id: HashMap<i64, SubquerySig>`,
  `templates: HashMap<String, TemplateGroup>` where
  ```rust
  struct TemplateGroup {
      inner_table: String,
      residual: Arc<CompiledPredicate>,      // compiled residual (literals baked)
      param_cols: Vec<usize>,                // positional, sorted by column name order of key
      binds: HashMap<Row, SubquerySig>,      // bind tuple -> node
      pk_node: HashMap<String, SubquerySig>, // pk -> node currently holding it (≤1 per template)
  }
  ```
- `collect()`: also compute `subquery_template(...)`, compile the residual once per fresh
  template, register bind→sig; store `template_key` + `bind: Row` on the node.
- `SubqueryEval for SubqueryRegistry`: `contains(sig,v)` → node → `circuit.contains(node_id, v)`;
  same `has_null` via `Value::Null`.
- New async kernel replacing `apply_node_flips`:
  ```rust
  async fn apply_tuples(&mut self, tuples: Vec<Tup2<Row, ZWeight>>) -> Vec<(SubquerySig, Flip)>
  // circuit.apply(tuples).await → MemberDelta{node_id,value,delta} → (sig, Flip{value, Enter/Leave})
  ```
- `on_table_delta` step 1 becomes template-grouped: for each template with
  `inner_table == table` — for each touched pk (latest-row fold as today):
  residual eval once (`matches_ctx` for nested), project params → bind → target node
  (mid-seed node ⇒ buffer raw delta on that node as today and treat as unregistered;
  gate-check the target node's gate per envelope). Reconcile via `pk_node` (old node
  retract / new node insert, updating both nodes' `pk_value` and the template `pk_node`).
  Collect all tuples across templates → ONE `apply_tuples` await → work queue. Step 2
  (outer shape emission) unchanged — it now reads post-step membership (same as today's
  post-reconcile reads).
- `finish_create`: per node seed — install gate, build seed tuples from snapshot rows
  (reconcile against empty), then replay `seed_buffer` through the gate producing more
  tuples (per-envelope stamps are gone in the buffer — keep today's replay-all-idempotent
  semantics: re-evaluate rows, reconcile; gate applied exactly as today, i.e. NOT re-checked
  at replay since buffered deltas carried no stamps — preserve current behavior). One
  `apply_tuples` for seed+replay → returned work.
- `reconcile_parent_for_value` / `rederive_dependent` (Node arm): evals → reconcile tuples
  → `apply_tuples` → push flips.
- `drop_subquery_shape`/`decref_node` → async; on node removal build retraction tuples from
  `pk_value`, `circuit.apply`, discard flips (refcount 0 ⇒ no dependents), clean
  `node_by_id`, template binds, `pk_node` entries, remove empty templates.
- `node_value_index`/`mem_totals`/`stats`/`state_summaries`: `distinct_values` and value
  lists from `circuit.values_for_node`; contributor total from Σ `pk_value.len()`.
  `NodeStat` gains `template: String`.

**Steps:**
- [x] 1. Port/adapt the module's unit tests FIRST (they pin semantics): reconcile
  enter/leave/value-change/no-op + null bucket now assert on registry+circuit
  (`apply_tuples` results and `SubqueryEval` reads); keep `filter_known_members`,
  rollback, trace, null-sensitivity tests compiling (async where needed).
- [x] 2. Red → 3. implement registry swap → 4. `cargo test -p electric-ivm-engine` green
  (engine/tests.rs integration tests included — fix fallout in mod.rs/lifecycle.rs/
  sequencer.rs call sites: async drops, `propagate_flips` unchanged signature).
- [x] 5. Commit `feat: subquery registry served by the membership circuit`.

### Task 4: Wiring, introspection, docs surface

- [x] `engine/mod.rs`: `mem_cardinalities` uses new `mem_totals`; drop-shape call sites await.
- [x] `engine/introspection.rs` + `http.rs`: `/subqueries` includes `template`; `/graph`
  subquery node ids unchanged (`node:<sig>`), no breaking viz changes.
- [x] `docs/ARCHITECTURE.md`: §6 (registry → circuit-backed flip detection), §6b (membership
  circuit alongside counts), §10 threading table (+1 row `dbsp-subq`).
- [x] `cargo clippy -p electric-ivm-engine` clean; commit `docs+introspection`.

### Task 5: Quality gates (all must pass)

- [x] `pnpm engine:test`
- [x] `ELECTRIC_IVM_ENGINE_PREBUILT=1 pnpm test` (vitest incl. oracle conformance)
- [x] `ASDF_ELIXIR_VERSION=1.18.4-otp-28 ASDF_ERLANG_VERSION=28.1 ./electric-conformance/run.sh oracle`
  — **goal gate: all subquery tests pass**
- [x] Fix regressions until green; commit fixes individually.

### Task 6: LinearLite manual verification (goal gate)

- [x] `pnpm demo:linearlite` per the run-linearlite skill; drive with Playwright MCP:
  create/move issues across projects, membership churn (join/leave project), verify live
  updates + no console errors; check the pipeline visualizer shows subquery node state.
- [x] Teardown; record evidence in handoff.

### Task 7: Close-out

- [x] Update spec §11b if further deviations emerged; `bd close dbsp-ds-jq6` only after all
  gates; `git status`; conservative handoff (branch name, commits, validation evidence,
  proposed PR command — no push).
