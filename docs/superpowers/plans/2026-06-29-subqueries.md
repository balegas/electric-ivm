# Subqueries Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `outer.col IN (SELECT inner.proj FROM inner WHERE …)` (and `NOT IN`) shape predicates to the engine, with the inner subquery maintained once and shared across shapes referencing the same inner shape; port Electric's subquery oracle tests and make them pass.

**Architecture:** A new predicate leaf carries a (recursive) subquery. Postgres evaluates the subquery natively, so the pglite/PG oracle is ground truth with only SQL-emission changes. The engine maintains shared inner-set **nodes** (value → contributor-pk sets, keyed by a canonical signature) in a cross-table `Arc<Mutex<SubqueryRegistry>>`; every table tailer calls `registry.on_table_delta` so an inner-table change moves outer-table rows via keyed query-back + full re-evaluation. Convergence-after-drain is the correctness contract (the conformance harness), not Electric's streaming protocol.

**Tech Stack:** Rust (apps/engine, dbsp/tokio), TypeScript (packages/protocol, packages/oracle, packages/conformance, apps/api), Postgres/pglite, vitest, cargo test.

## Global Constraints

- One table + WHERE over its own columns, **plus** single-column `IN`/`NOT IN` subqueries. Composite `(a,b) IN (…)`, `EXISTS`, `= (SELECT)` are **out of scope** — reject at validation.
- Predicate ops unchanged: `eq neq lt lte gt gte`; combinators `and/or/not`; new leaf `in`.
- SQL three-valued logic must match Postgres (the oracle), including `NULL`/`NOT IN`.
- Engine holds **no table copy**; reads back from Postgres (REPEATABLE READ + `seed_lsn`, strict-`<` commit-LSN reconciliation).
- Run `pnpm engine:test` (cargo, fast) and `pnpm test` (vitest incl. conformance) before claiming done. No `tsc` gate.
- Commit messages end with the two harness trailers (`Co-Authored-By:` Claude + `Claude-Session:`).
- Existing equality/standalone shape fast paths must stay byte-identical (no regression in the 88-test suite).

---

## File Structure

- `packages/protocol/src/types.ts` — add `SubqueryRef`, `InSubqueryPredicate`, `isInSubquery`, extend `Predicate`.
- `packages/protocol/src/predicate.ts` — `validatePredicate` recurse into inner table; `evaluate` throws on subquery (unused for subquery shapes).
- `packages/protocol/src/sql.ts` — `predicateToSql` emits `[NOT] IN (SELECT … )`; needs a schema arg for the inner table.
- `packages/protocol/src/protocol.test.ts` — AST/SQL unit tests.
- `apps/api/src/router.ts` — extend recursive `predicateSchema` zod with the `in` node.
- `apps/engine/src/predicate.rs` — `PredicateJson::In`, `CompiledPredicate::InSubquery`, canonical signature, `matches_ctx`, `SubqueryEval` trait.
- `apps/engine/src/sql.rs` — `predicate_to_sql` emits subqueries.
- `apps/engine/src/subquery.rs` (new) — `SubqueryRegistry`, `SubqueryNode`, edges, `on_table_delta`, move-query + append, backfill seeding, introspection.
- `apps/engine/src/engine.rs` — wire the registry into `create_shape` (discover/register nodes; subquery shapes route to registry), spawn tailers for inner tables, call `registry.on_table_delta` from `tailer_loop`, add `/tables/:name/subqueries` stat.
- `apps/engine/src/http.rs` — expose the subquery-node introspection endpoint.
- `apps/engine/src/lib.rs` — `mod subquery;`.
- `packages/conformance/src/subquery-schema.ts` (new) — the multi-level schema + seed + mutation generator shared by subquery tests.
- `packages/conformance/src/conformance-subquery.test.ts` (new) — property-style convergence matrix.
- `packages/conformance/src/conformance-subquery-scenarios.test.ts` (new) — deterministic move-in/out, NOT IN, combined, multi-level.
- `packages/conformance/src/conformance-subquery-sharing.test.ts` (new) — node-sharing topology.

---

## Task 1: Protocol AST — the `in` subquery leaf (TS)

**Files:**
- Modify: `packages/protocol/src/types.ts`
- Modify: `packages/protocol/src/predicate.ts`
- Test: `packages/protocol/src/protocol.test.ts`

**Interfaces:**
- Produces: `SubqueryRef { table: string; project: string; where?: Predicate }`,
  `InSubqueryPredicate { col: string; in: SubqueryRef; negated?: boolean }`,
  `isInSubquery(p): p is InSubqueryPredicate`. `Predicate` union gains `InSubqueryPredicate`.
  `validatePredicate(pred, table, schema?)` — when validating an `in` leaf it looks up `in.table` in `schema.tables` and recurses into `in.where` against the inner table.

- [ ] **Step 1: Write failing tests** in `protocol.test.ts`:

```ts
import { type Schema, isInSubquery, validatePredicate } from './index.js'
const schema: Schema = { tables: {
  parent: { columns: { id: { type: 'int' }, active: { type: 'bool' } }, primaryKey: 'id' },
  child:  { columns: { id: { type: 'int' }, parent_id: { type: 'int' } }, primaryKey: 'id' },
} }
it('recognizes an in-subquery leaf', () => {
  const p = { col: 'parent_id', in: { table: 'parent', project: 'id', where: { col: 'active', op: 'eq', value: true } } }
  expect(isInSubquery(p as any)).toBe(true)
})
it('validates inner where against the inner table', () => {
  const ok = { col: 'parent_id', in: { table: 'parent', project: 'id', where: { col: 'active', op: 'eq', value: true } } }
  expect(() => validatePredicate(ok as any, schema.tables.child!, schema)).not.toThrow()
  const badCol = { col: 'parent_id', in: { table: 'parent', project: 'id', where: { col: 'nope', op: 'eq', value: true } } }
  expect(() => validatePredicate(badCol as any, schema.tables.child!, schema)).toThrow()
  const badProject = { col: 'parent_id', in: { table: 'parent', project: 'nope' } }
  expect(() => validatePredicate(badProject as any, schema.tables.child!, schema)).toThrow()
})
```

- [ ] **Step 2: Run** `pnpm --filter @electric-ivm/protocol test` → FAIL (isInSubquery undefined).

- [ ] **Step 3:** In `types.ts` add the interfaces + guard and extend `Predicate`:

```ts
export interface SubqueryRef { table: string; project: string; where?: Predicate }
export interface InSubqueryPredicate { col: string; in: SubqueryRef; negated?: boolean }
export type Predicate = LeafPredicate | AndPredicate | OrPredicate | NotPredicate | InSubqueryPredicate
export function isInSubquery(p: Predicate): p is InSubqueryPredicate {
  return 'in' in p && 'col' in p
}
```

(Note: `isLeaf` already checks `'col' in p && 'op' in p`; an `in` leaf has `col` but not `op`, so `isLeaf` stays false for it — verify order in `predicate.ts` dispatch: test `isInSubquery` before `isLeaf` is unnecessary since `isLeaf` requires `op`.)

- [ ] **Step 4:** In `predicate.ts`, change `validatePredicate` signature to `(pred, table, schema?)` and add the branch (place the `isInSubquery` check **before** `isLeaf`):

```ts
export function validatePredicate(pred: Predicate, table: TableDef, schema?: Schema): void {
  if (isInSubquery(pred)) {
    const col = table.columns[pred.col]
    if (!col) throw new PredicateError(`unknown column "${pred.col}"`)
    if (!schema) throw new PredicateError('subquery validation requires a schema')
    const inner = schema.tables[pred.in.table]
    if (!inner) throw new PredicateError(`unknown subquery table "${pred.in.table}"`)
    if (!inner.columns[pred.in.project]) throw new PredicateError(`unknown subquery column "${pred.in.project}"`)
    if (pred.in.where) validatePredicate(pred.in.where, inner, schema)
    return
  }
  // ...existing isLeaf/isAnd/isOr/isNot branches, threading `schema` into recursive calls...
}
```

Import `Schema`, `isInSubquery` into `predicate.ts`. Thread `schema` through the and/or/not recursions.

- [ ] **Step 5:** In `predicate.ts` `evalTri`, add an `isInSubquery` branch that throws: `throw new Error('evaluate() cannot resolve a subquery; subquery shapes are evaluated via SQL')` (so the row evaluator fails loud rather than silently wrong — the oracle uses SQL).

- [ ] **Step 6:** Export `isInSubquery`, `SubqueryRef`, `InSubqueryPredicate` from `index.ts`.

- [ ] **Step 7: Run** the protocol tests → PASS. **Commit** `feat(protocol): in-subquery predicate AST + validation`.

---

## Task 2: Protocol SQL — emit `[NOT] IN (SELECT …)` (TS)

**Files:**
- Modify: `packages/protocol/src/sql.ts`
- Test: `packages/protocol/src/protocol.test.ts`

**Interfaces:**
- Consumes: Task 1 types.
- Produces: `predicateToSql(pred, startIndex?)` handles the `in` node, recursively emitting the inner SELECT. `shapeSelectSql(table, where)` continues to work and now supports subqueries (it calls `predicateToSql`).

- [ ] **Step 1: Failing test:**

```ts
import { predicateToSql, shapeSelectSql } from './index.js'
it('emits IN (SELECT …) sql', () => {
  const p = { col: 'parent_id', in: { table: 'parent', project: 'id', where: { col: 'active', op: 'eq', value: true } } }
  const { text } = predicateToSql(p as any, 1)
  expect(text.replace(/\s+/g, ' ')).toContain('"parent_id" IN (SELECT "id" FROM "parent" WHERE "active" = true)')
})
it('emits NOT IN', () => {
  const p = { col: 'parent_id', negated: true, in: { table: 'parent', project: 'id' } }
  expect(predicateToSql(p as any, 1).text.replace(/\s+/g, ' ')).toContain('"parent_id" NOT IN (SELECT "id" FROM "parent")')
})
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3:** In `sql.ts` add the `in` branch in `predicateToSql` (mirror existing param-threading; text literals in the inner where bind to `$n` continuing the same index counter). Inner where omitted ⇒ no `WHERE`. Emit `"col" [NOT] IN (SELECT "project" FROM "table"[ WHERE <inner>])`. Quote identifiers with the file's existing quoting helper.

- [ ] **Step 4: Run** → PASS. Verify `shapeSelectSql` composes (add one assertion using `shapeSelectSql('child', subqueryWhere)`).

- [ ] **Step 5: Commit** `feat(protocol): predicateToSql emits IN/NOT IN subqueries`.

---

## Task 3: Oracle supports subquery shapes (TS)

**Files:**
- Modify: `packages/oracle/src/index.ts` (only if `queryShape`/`shapeSelectSql` needs the schema for inner-table validation; the SQL path already works once Task 2 lands)
- Test: `packages/oracle/src/oracle.test.ts`

**Interfaces:**
- Consumes: Tasks 1–2.
- Produces: `oracle.queryShape({ table, where: <subquery> })` returns the correct rows (Postgres/pglite evaluates the subquery). No interface change expected.

- [ ] **Step 1: Failing test** in `oracle.test.ts`: create a 2-table schema (parent/child), insert parents (some active) + children, then `queryShape({ table: 'child', where: { col: 'parent_id', in: { table: 'parent', project: 'id', where: { col: 'active', op: 'eq', value: true } } } })` and assert it equals the children whose parent is active.

- [ ] **Step 2: Run** `pnpm --filter @electric-ivm/oracle test` → expect PASS if Task 2 is correct (pglite runs the subquery). If FAIL, fix `shapeSelectSql` plumbing.

- [ ] **Step 3: Commit** `test(oracle): subquery shape via SELECT … IN (SELECT …)`.

---

## Task 4: Rust predicate AST + canonical signature

**Files:**
- Modify: `apps/engine/src/predicate.rs`
- Test: inline `#[cfg(test)]` in `predicate.rs`

**Interfaces:**
- Produces:
  - `PredicateJson::In { col: String, r#in: SubqueryJson, #[serde(default)] negated: bool }` where `SubqueryJson { table: String, project: String, #[serde(default)] r#where: Option<Box<PredicateJson>> }`. (serde rename: the JSON key is `in` / `where`; use `#[serde(rename="in")]` / `#[serde(rename="where")]`.)
  - `CompiledPredicate::InSubquery { col: usize, sig: SubquerySig, negated: bool }`.
  - `SubquerySig` = a `String` canonical key `format!("{table}|{project}|{canonical_where}")`, where `canonical_where` is a stable serialization of the inner predicate (sorted/normalized JSON). Two equal subqueries ⇒ equal sig.
  - `CompiledPredicate::compile` cannot resolve `col`/inner without **both** the outer table schema and a way to compile/register the node. Split: `compile` (and `compile_opt`) gain a `&mut dyn SubqueryCollector` parameter OR a simpler approach — see Step 3.

- [ ] **Step 1: Failing test** — compile a JSON `{col,in:{table,project,where},negated}` and assert the `sig` is stable and equals that of an identical subquery, and differs when the inner where differs.

- [ ] **Step 2: Run** `cargo test -p electric-ivm-engine predicate` → FAIL.

- [ ] **Step 3:** Implement. Because compiling the inner predicate needs the **inner** table's schema (for column indices) and must register a node, thread a collector:

```rust
pub trait SubqueryCollector {
    /// Register (or dedupe) a subquery node and return its signature. `inner_ts` lookup is the
    /// collector's responsibility (it holds the full schema). Returns the canonical sig string.
    fn collect(&mut self, table: &str, project: &str, where_json: Option<&PredicateJson>) -> Result<SubquerySig>;
}
pub type SubquerySig = String;
```

Change `compile`/`compile_opt` to `compile_with(p, ts, collector)`. For `PredicateJson::In`, resolve `col` via `ts.column_index`, call `collector.collect(table, project, where.as_deref())` to get `sig`, build `InSubquery { col, sig, negated }`. Keep a thin `compile`/`compile_opt` that pass a `NoSubqueries` collector which errors if an `in` node is hit (used by existing non-subquery call sites / tests). Provide `canonical_where(p: Option<&PredicateJson>) -> String` (stable JSON: serialize a normalized form; for AND/OR sort children by their canonical string so order doesn't split the cache).

- [ ] **Step 4: matches_ctx + SubqueryEval.** Add:

```rust
pub trait SubqueryEval {
    /// Does `value` belong to the node's set? Returns `Tri` so NULL/NOT-IN obey SQL.
    fn contains(&self, sig: &SubquerySig, value: &Value) -> bool;
    fn has_null(&self, sig: &SubquerySig) -> bool;
}
impl CompiledPredicate {
    pub fn matches_ctx(&self, row: &Row, ev: &dyn SubqueryEval) -> bool { self.eval_ctx(row, ev) == Tri::True }
    fn eval_ctx(&self, row, ev) -> Tri { /* like eval, plus: */ }
}
```

The `InSubquery` arm of `eval_ctx`:
```rust
CompiledPredicate::InSubquery { col, sig, negated } => {
    let cell = row.0.get(*col).unwrap_or(&Value::Null);
    if matches!(cell, Value::Null) { return Tri::Unknown; }       // NULL IN / NOT IN -> UNKNOWN
    let present = ev.contains(sig, cell);
    if !negated { Tri::from_bool(present) }
    else if ev.has_null(sig) { Tri::Unknown }                     // x NOT IN (set with NULL) -> UNKNOWN
    else { Tri::from_bool(!present) }
}
```
Keep the existing `matches`/`eval` for non-subquery predicates (they `unreachable!()` or error on `InSubquery`, since plain `matches` has no eval context — but better: make `eval` route `InSubquery` to a panic-free `Tri::Unknown` only inside `#[cfg(test)]`? No: standalone non-subquery shapes never contain `InSubquery`. Add a debug_assert.)

- [ ] **Step 5: Run** predicate tests → PASS. **Commit** `feat(engine): subquery predicate AST, signature, matches_ctx`.

---

## Task 5: Rust SQL emits subqueries

**Files:**
- Modify: `apps/engine/src/sql.rs`
- Test: inline tests in `sql.rs`

**Interfaces:**
- Consumes: `PredicateJson::In` (Task 4). `predicate_to_sql` works on `PredicateJson` (pre-compile) or on a structure carrying table/project — confirm which the file uses. The engine builds backfill SQL from the **compiled** predicate today; for subqueries we need the inner table/project/where, which live in `PredicateJson`. Decision: emit SQL from `PredicateJson` (raw), or carry inner SQL in the node. **Use the node:** the registry stores each node's prebuilt inner `WHERE` SQL; outer-shape backfill SQL references `(SELECT proj FROM t WHERE <inner-sql>)`. So `sql.rs` gains a helper `subquery_sql(col, table, project, inner_where_sql, negated)`.

- [ ] **Step 1: Failing test:** assert `predicate_to_sql` (or the new helper) produces `"parent_id" IN (SELECT "id" FROM "parent" WHERE …)` matching the TS emitter.

- [ ] **Step 2–4:** Implement to mirror `packages/protocol/src/sql.ts` exactly (same quoting, same param semantics). **Run** → PASS.

- [ ] **Step 5: Commit** `feat(engine): predicate_to_sql emits IN/NOT IN subqueries`.

---

## Task 6: SubqueryRegistry — node maintenance core (Rust, unit-tested without PG)

**Files:**
- Create: `apps/engine/src/subquery.rs`
- Modify: `apps/engine/src/lib.rs` (`mod subquery;`)
- Test: inline tests in `subquery.rs` (pure in-memory; no Postgres)

**Interfaces:**
- Produces:
```rust
pub struct SubqueryRegistry { /* nodes: HashMap<SubquerySig, SubqueryNode>, edges, schemas: Arc<HashMap<String,TableSchema>> */ }
pub struct SubqueryNode {
    pub sig: SubquerySig, pub inner_table: String, pub proj_col: usize,
    pub pred: Arc<CompiledPredicate>, pub seed_lsn: u64,
    contributors: HashMap<Value, HashSet<String /*pk*/>>, has_null_count: usize, refcount: usize,
}
// Reconcile one inner row's contribution; returns the set of value-flips on this node.
enum Flip { Enter(Value), Leave(Value), NullEnter, NullLeave }
impl SubqueryNode {
    fn reconcile_row(&mut self, pk: &str, present_value: Option<Value>) -> Vec<Flip>;
    // present_value = Some(proj) if the row currently matches the node pred (so it should contribute that
    // value); None if it should not contribute. Adds/removes pk from contributors[value] / null bucket.
}
impl SubqueryEval for SubqueryRegistry { fn contains(..); fn has_null(..); }
```
- The registry also implements `SubqueryEval` so `matches_ctx` can consult it.

- [ ] **Step 1: Failing tests** (pure, no PG) for `reconcile_row` flip detection:
  - Add pk "a" with value 5 to an empty bucket ⇒ `[Enter(5)]`; add pk "b" value 5 ⇒ `[]` (already non-empty); remove "a" ⇒ `[]`; remove "b" ⇒ `[Leave(5)]`.
  - NULL value: add pk with `present_value=Some(Null)` ⇒ `[NullEnter]` (and bucket tracked); removing last ⇒ `[NullLeave]`.
  - Changing a row's value (reconcile with new value) removes from old bucket, adds to new — caller passes the new `present_value`; provide a `reconcile_row` that first removes pk from **all** buckets it's in (track a reverse `pk -> value` map per node) then adds to the new bucket. Test: pk "a" 5 then reconcile "a" 7 ⇒ `[Leave(5), Enter(7)]`.

- [ ] **Step 2: Run** `cargo test -p electric-ivm-engine subquery` → FAIL.

- [ ] **Step 3:** Implement `SubqueryNode` with `contributors: HashMap<Value, HashSet<String>>` + `pk_value: HashMap<String, Value>` (reverse map, Value::Null for null bucket) so `reconcile_row` is O(1)-ish and history-independent. `contains(value)` = bucket non-empty; `has_null` = null bucket non-empty.

- [ ] **Step 4: Run** → PASS. **Commit** `feat(engine): subquery node contributor-set maintenance`.

---

## Task 7: SubqueryRegistry — backfill seeding + create/register (Rust, integration with PG via engine)

**Files:**
- Modify: `apps/engine/src/subquery.rs`, `apps/engine/src/engine.rs`, `apps/engine/src/pg.rs` (if a `SELECT proj, pk FROM t WHERE …` helper is needed)

**Interfaces:**
- Produces on the registry:
```rust
impl SubqueryRegistry {
  // get-or-create a node by sig; seed it from PG (SELECT proj,pk FROM inner WHERE inner-sql). Recurses
  // for nested subqueries (collect inner nodes first, deepest seeded first). Increments refcount.
  async fn ensure_node(&mut self, sig, inner_table, project, inner_where_json, pg_url, schemas) -> Result<()>;
  // register a subquery shape: store (shape_id, outer_table, stream_path, compiled outer pred, out_cols,
  // seed_lsn) and one edge per IN leaf: (dependent=Shape(shape_id), connecting_col, node_sig, negated).
  fn register_shape(&mut self, ...);
  fn drop_shape(&mut self, shape_id);  // decref nodes, remove edges/empty nodes
}
```
- Node-to-node edges: when `ensure_node`'s inner pred itself contains `IN` leaves, register edges `(dependent=Node(parent_sig), connecting_col, child_sig, negated)`.

- [ ] **Step 1:** Add `pg.rs` helper `pub async fn project_rows(client, ts, project_idx, filter_sql) -> Vec<(pk_string, Value /*proj*/)>` plus the snapshot LSN — or reuse `backfill` and project in Rust. Prefer reuse: `backfill(client, ts, Some(pred))` returns full rows + seed_lsn; project `proj_col` and `pk` in Rust. **But** the node pred may contain nested subqueries that PG must evaluate — so backfill SQL must include them. Build the inner-SQL via Task 5 helper from the node's `PredicateJson`. Store the raw `PredicateJson` (or prebuilt SQL) on the node for this.

- [ ] **Step 2: Failing test (engine-level, needs PG):** defer the heavy PG test to Task 10 conformance. Here add a unit test that `ensure_node` on a non-PG engine (pg_url None) seeds an empty node and `register_shape` builds the right edges (assert edge count + sigs).

- [ ] **Step 3:** Implement. Recurse deepest-first so a parent node's seeding sees child nodes already populated (the parent's backfill SQL uses nested SELECTs, so PG handles correctness regardless; the in-memory child set is needed only for live `matches_ctx`). Record `seed_lsn` per node.

- [ ] **Step 4: Run** → PASS. **Commit** `feat(engine): subquery node seeding + shape/edge registration`.

---

## Task 8: SubqueryRegistry — `on_table_delta` propagation + appends (Rust)

**Files:**
- Modify: `apps/engine/src/subquery.rs`, `apps/engine/src/engine.rs`

**Interfaces:**
- Produces:
```rust
impl SubqueryRegistry {
  // Called by every tailer for every delta batch. Fast-returns if `table` is neither an inner table of
  // a node nor an outer table of a shape. Performs node updates, recursive flip propagation, move
  // queries, and appends to shape streams. Holds &self mutably; ds + pg_url passed in.
  pub async fn on_table_delta(&mut self, table: &str, ts: &TableSchema, delta: &[Tup2<Row,ZWeight>],
                              lsn: u64, txid: Option<String>, ds: &DsClient, pg_url: &Option<String>) -> Result<()>;
}
```
- Algorithm (per the spec "Maintenance — one rule"):
  1. If `table` is an inner table of node(s): for each node with `inner_table==table`, for each delta tuple, reconcile the row pk: compute `match = node.pred.matches_ctx(row, self)`, `present_value = match ? Some(row[proj]) : None`. Use commit-LSN guard vs `node.seed_lsn`. Collect flips.
  2. For each flip `(node_sig, value, direction)`: for each edge whose `node_sig` matches, query `SELECT * FROM dep_table WHERE connecting_col = value` (REPEATABLE READ snapshot via a fresh connection, or reuse one). For a **Node** dependent: reconcile each candidate row in the parent node → new flips → push to the work queue (BFS). For a **Shape** dependent: `matches_ctx(candidate, self)` → append `upsert`/`delete` envelope (via `translate_output`-equivalent or a single-row builder) to the shape stream.
  3. If `table` is a SubqueryShape's outer table: evaluate the shape's outer pred on the delta with `matches_ctx`, build enter/update/leave envelopes (reuse `engine::translate_output` with a context-aware filter), append.
- Edge case (has_null flip): on `NullEnter`/`NullLeave` of a node referenced by a **negated** edge, re-derive that shape's full candidate set (query all rows of the shape's outer table where the rest-of-predicate could match — simplest: re-run the shape backfill and reconcile). Implement minimally; covered by a defensive test only.

- [ ] **Step 1: Implementation note** — `match_ctx` re-entrancy: `on_table_delta` mutates `self` (nodes) and also calls `self.contains` via `matches_ctx`. Resolve borrow conflict by splitting state: keep node **sets** queryable while collecting flips into a separate `Vec`, applying mutations through indices, or snapshot the needed node membership before the mutable walk. Concretely: process one node at a time; for `matches_ctx` of a parent/shape pred, the registry exposes an immutable `view: SubqueryView<'_>` borrowing only the `contributors` maps (not the whole registry), so reconciliation of node N can read other nodes' views. Use `RefCell`/indices if the borrow checker fights; document the chosen approach.

- [ ] **Step 2:** Wire into `engine.rs`:
  - `Engine` gains `subqueries: Arc<TokioMutex<SubqueryRegistry>>` constructed with `Arc<HashMap<String,TableSchema>>` from `setup_postgres`/`define_schema`.
  - `create_shape`: compile the outer predicate **with a collector** that calls `registry.ensure_node(...)` for each subquery (so nodes are created/seeded and edges registered) and yields the compiled `CompiledPredicate` (with `InSubquery{sig}` leaves). If the predicate contains any subquery, route the shape to the registry (`register_shape`) **instead of** the per-table standalone/family path, AND backfill the outer shape via `backfill(WHERE <full subquery SQL>)` → append. Also ensure a tailer exists for the outer table **and every inner table** in the dependency tree (so their deltas reach the registry). The subquery shape is NOT added to the tailer's local `shapes`/`families`.
  - `tailer_loop`/`process_envelope`: after the existing standalone+family fan-out, call `registry.on_table_delta(table, &ts, &delta, lsn, txid, &ds, &pg_url)` (lock the registry). The registry decides if this table matters. Appends happen inside, before the processed offset is published.
  - Pass the registry handle + ds + pg_url into `tailer_loop` (extend `spawn_tailer` signature).

- [ ] **Step 3:** Build: `pnpm engine:build`. Fix compile errors. Run `pnpm engine:test` (existing 19 tests must stay green — no subquery PG test yet).

- [ ] **Step 4: Commit** `feat(engine): subquery registry on_table_delta propagation + engine wiring`.

---

## Task 9: API zod schema + node introspection endpoint

**Files:**
- Modify: `apps/api/src/router.ts` (recursive `predicateSchema` gains the `in` node)
- Modify: `apps/engine/src/http.rs` + `engine.rs` (`GET /tables/:name/subqueries` → list node sigs + sizes; or a global `GET /subqueries`)

**Interfaces:**
- Produces: `predicateSchema` accepts `{ col, in: { table, project, where? }, negated? }`. Engine endpoint returns `{ nodes: [{ sig, inner_table, project, values: <distinct count>, shapes: <refcount> }] }`.

- [ ] **Step 1:** Extend the zod recursive predicate (use `z.lazy`) with the `in` shape. Add a quick API-package test if one exists; else rely on conformance.
- [ ] **Step 2:** Add the engine introspection endpoint + `Engine::subquery_stats()`.
- [ ] **Step 3:** `pnpm engine:test` green. **Commit** `feat(api,engine): subquery predicate schema + node introspection`.

---

## Task 10: Conformance — multi-level schema + property convergence

**Files:**
- Create: `packages/conformance/src/subquery-schema.ts` (schema mirroring Electric `level_1..4` + tag side-tables; seed helper; deterministic mutation generator).
- Create: `packages/conformance/src/conformance-subquery.test.ts`

**Interfaces:**
- Consumes: `bootHarness`, `applyOp`, `drainEngine`, `waitForConvergence`, `createSimulator`-style seeding.
- The schema: `level_1(id text pk, active bool)`, `level_2(id text pk, level_1_id text, active bool)`, `level_3(id, level_2_id, active)`, `level_4(id, level_3_id, value text)`, plus `level_1_tags(level_1_id, tag)` … (pk handling: tag tables need a synthetic pk — give them `id` serial or composite; the engine requires a single pk column, so use `id text pk` and carry `(level_n_id, tag)`).

- [ ] **Step 1:** Write `subquery-schema.ts`: the `Schema`, a `seed(h)` that inserts the standard rows (l1-1..5 etc., active alternating, children round-robin, l4 value `v{i}`), and `mutationsGen(seed)` yielding `{ table, ev }` ops (toggle active, move parent, add/remove tag, update value) — deterministic via a seeded RNG (reuse `simulator.ts`'s RNG).

- [ ] **Step 2: Failing test:** register a 1-level subquery shape on `level_4`:
```ts
const def = { table: 'level_4', where: { col: 'level_3_id', in: { table: 'level_3', project: 'id', where: { col: 'active', op: 'eq', value: true } } } }
```
Seed, drive ~80 mutations, drain, `waitForConvergence` vs oracle. (Columns: all of level_4; pk `id`.)

- [ ] **Step 3: Run** `pnpm test:conformance` (or vitest filtered). Expect FAIL initially if engine bugs; debug with systematic-debugging until convergence.

- [ ] **Step 4:** Add a `CASES` matrix (like `conformance-expressiveness.test.ts`): 1/2/3-level subqueries, tag subqueries, `NOT IN`, `(A) AND (B)`, `(A) OR (B)`, `NOT (sub)`, subquery `AND` atomic. Each case: register, drive the shared mutation stream, drain, assert convergence. Loop over cases.

- [ ] **Step 5: Run** → all green. **Commit** `test(conformance): subquery convergence matrix vs pg oracle`.

---

## Task 11: Conformance — deterministic move scenarios + NOT IN + combined + multi-level

**Files:**
- Create: `packages/conformance/src/conformance-subquery-scenarios.test.ts`

**Interfaces:** Consumes the harness. Each test: small fixed schema, fixed ops, assert exact convergence (and where meaningful, assert specific row presence/absence after the mutation).

- [ ] **Step 1:** Port these scenarios (each = seed → mutate → drain → assert client rows == oracle, plus a targeted presence assertion):
  - **move-out on parent deactivate:** `child` where `parent_id IN (SELECT id FROM parent WHERE active=true)`; deactivate parent-1 ⇒ its children leave.
  - **move-out on parent delete:** delete parent-1 ⇒ children leave.
  - **move-in via new parent:** deactivate parent-1 (child leaves), then move child to active parent-2 ⇒ child re-enters.
  - **NOT IN move-in:** `outer` where `inner_id NOT IN (SELECT id FROM inner WHERE active=true)`; inner active (outer absent) → set inner inactive ⇒ outer enters.
  - **NOT IN move-out:** inner inactive (outer present) → set inner active ⇒ outer leaves.
  - **combined condition:** `child` where `parent_id IN (SELECT id FROM parent WHERE active=true) AND status='published'`; in one txn make parent-b active + move child to parent-b with status='draft' ⇒ child deleted (sub move-in must not mask failing status).
  - **multi-level no spurious delete:** `tasks` where 3-level org/team/premium-tag chain; move team between premium orgs ⇒ no delete; remove premium tag from old org ⇒ still no delete.
- [ ] **Step 2: Run** → debug to green (systematic-debugging for any divergence). **Commit** `test(conformance): subquery move-in/out, NOT IN, combined, multi-level`.

---

## Task 12: Conformance — node sharing topology

**Files:**
- Create: `packages/conformance/src/conformance-subquery-sharing.test.ts`

- [ ] **Step 1:** Register K shapes whose subquery has the **same** inner shape (e.g. different outer tables/cols but identical `(SELECT id FROM level_3 WHERE active=true)`), plus some with a different inner. Drive mutations, assert convergence for all, AND assert `GET /subqueries` reports **one** node for the shared sig (refcount == K) and separate nodes for distinct sigs.
- [ ] **Step 2: Run** → green. **Commit** `test(conformance): identical subqueries share one inner node`.

---

## Task 13: Full suite + docs realignment

- [ ] **Step 1:** Run `pnpm engine:test` (all Rust) and `pnpm test` (full vitest incl. fuzz). All green.
- [ ] **Step 2:** Update `AGENTS.md` (predicate AST now includes `in`; note subqueries + node sharing) and flip the spec status to **implemented**. Add a short note to `README.md` if it enumerates predicate ops.
- [ ] **Step 3: Commit** `docs: subqueries implemented (AST, sharing, tests)`.

---

## Self-Review

**Spec coverage:** grammar/AST (T1,T4,T9), SQL emission/oracle (T2,T3,T5), shared nodes (T6,T7,T12), maintenance/move-in-out (T8,T11), property convergence (T10), NOT IN/null (T4,T8,T11), combined/multi-level (T11), restart/restore — **deferred/optional** (noted in spec as stretch; not a separate task — add if time permits after T13). Sharing introspection (T9,T12). Covered.

**Placeholder scan:** Borrow-checker resolution in T8 is described as a decision-with-options (acceptable — it's a known-hard Rust detail the implementer resolves against the compiler), not a TODO. No "add error handling" hand-waving.

**Type consistency:** `SubquerySig` is `String` throughout; `InSubquery { col, sig, negated }` consistent T4↔T6↔T8; `matches_ctx`/`SubqueryEval`/`contains`/`has_null` consistent T4↔T6↔T8; `on_table_delta` signature consistent T8↔engine wiring. `in.where`/`in.project`/`in.table` consistent T1↔T2↔T10.
