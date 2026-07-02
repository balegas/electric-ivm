# Subqueries: `col IN (SELECT … )` shapes (shared inner-set nodes)

Design record — 2026-06-29. Status: **implemented + verified**. The engine maintains shared inner-set
nodes and converges to the Postgres oracle across the ported subquery suite (multi-level convergence
matrix over many seeds, deterministic move-in/out, `NOT IN`, combined-condition, multi-level
no-spurious-delete, and node-sharing topology); full suite 103 tests green. One non-obvious correctness
fix during implementation: outer-table membership is emitted **absolutely** (upsert if the new row
matches, else idempotent delete by pk), not as a delta — because per-table tailers can apply an
inner-set change ahead of an earlier-committed outer change, and a delta-based "delete only if the *old*
row matched" then misses move-outs. Absolute emission + flip-driven move-queries converge regardless of
cross-table order (so Electric's LSN buffering/tag machinery stays unnecessary). Code:
`apps/engine/src/subquery.rs` (registry), `predicate.rs`/`sql.rs` (AST + SQL), `engine.rs`/`http.rs`
(wiring + `GET /subqueries`); tests `packages/conformance/src/conformance-subquery*.test.ts`.

Goal: add subquery support to the dbsp engine so a
shape's `WHERE` can be `outer.col IN (SELECT inner.proj FROM inner WHERE …)` (and `NOT IN`), with the
inner subquery maintained **once** and **shared** across shapes that reference the same inner shape.
Port Electric sync-service's subquery oracle tests; stop when they pass.

## What Electric does (the model we copy)

(`../electric/packages/sync-service`, gated behind `allow_subqueries`.)

- **Grammar:** only `col IN (SELECT proj FROM t WHERE …)` and `col NOT IN (…)`. Left side is a single
  column reference (composite `(a,b) IN …` exists but is unused by the oracle generator — out of scope
  here). Inner `WHERE` may itself contain `IN (SELECT …)`, recursively (the oracle goes 3–4 levels deep).
- **Semantics:** the inner subquery is a maintained **set of values** (`MapSet` per `["$sublink", n]`).
  Outer membership is `outer.col ∈ innerSet` (positive) / `∉` (negated). When a value **enters** the
  inner set, outer rows referencing it **move in**; when a value **leaves**, they **move out**. A
  subquery `AND`-ed with other conditions re-evaluates the *whole* `WHERE` per outer row, so a row whose
  other condition fails is not admitted by a bare subquery move-in.
- **Sharing:** identical subqueries are deduped into one dependency sub-shape (`comparable_shape`).
- **Oracle tests:** drive mutations against a multi-level schema; after each batch assert the
  client-materialized set equals `SELECT … WHERE <subquery-where>` from real Postgres. Plus deterministic
  move-in/move-out, `NOT IN`, combined-condition, and multi-level-dependency scenarios.

Electric streams exact control messages (move-in snapshot splice, move-out via row *tags*) with an LSN
buffering state machine. **We do not need that protocol.** Our conformance harness asserts *convergence
after drain* (final-state equality vs the Postgres oracle), so we implement correct eventual membership,
not Electric's streaming choreography. Postgres evaluates subqueries natively, so the oracle is free; the
work is the engine maintaining shared inner-set state and emitting correct move-in/out envelopes.

## Architecture: shared inner-set **nodes** + a cross-table registry

Today each table has its own tailer with **local** routing state (`shapes`, `families`). Subqueries are
inherently cross-table (an inner-table change moves outer-table rows), so they live in a new shared
`Arc<Mutex<SubqueryRegistry>>` that every tailer calls into. The existing equality/standalone fast paths
are untouched — only subquery shapes and their inner tables route through the registry.

### Node (the shared, maintained inner set)

A `SubqueryNode` materializes `SELECT proj FROM inner WHERE pred` as a value set, keyed by a **canonical
signature** `sig = (inner_table, proj_col, canonical(pred))`. Two subqueries with equal `sig` share one
node (the sharing the goal asks for; exposed via introspection for a sharing test).

```
SubqueryNode {
  sig, inner_table, proj_col, pred: CompiledPredicate,   // pred may reference deeper nodes
  contributors: HashMap<Value, HashSet<pk>>,   // value -> set of inner-row pks producing it
  has_null: bool,                              // any contributor whose proj value is NULL
  seed_lsn: u64,                               // backfill snapshot; skip inner deltas with commit lsn < seed
  refcount: usize,                             // shapes/parent-nodes depending on this node
}
```

`value ∈ set ⟺ contributors[value]` is non-empty. Tracking contributor **pks** (not a bare count) makes
maintenance reconcile-by-identity: set a row's presence to equal `match(row)` regardless of history. This
is O(inner result size) per node — inherent to incremental maintenance, and shared. Backfill seeds it
with `SELECT proj, <pk> FROM inner WHERE <pred-sql>` (Postgres computes nested subqueries natively).

### Dependency edges

Each `col IN node` leaf in a dependent's predicate is an edge `(dependent, connecting_col, node,
negated)`. A *dependent* is either an outer **SubqueryShape** or a **parent node** (whose `pred`
contains the leaf). Edges form a DAG (subquery nesting is acyclic).

### Maintenance — one rule, applied recursively

`registry.on_table_delta(table, ts, delta)` (called by the tailer for every delta, fast-returns if
`table` is irrelevant):

1. **`table` is some node's `inner_table`:** for each delta tuple `(row, ±1)` reconcile that inner row's
   pk in the node — compute `match(row, nodes)` and its `proj` bucket, add/remove pk so presence ==
   match. Record per-value **flips** (bucket `∅→nonempty` = *enter*, `nonempty→∅` = *leave*).
2. **For each flipped value `v` of a node `N`:** for every edge `(dep, col, N, negated)`, the dependent
   rows that could change are exactly those with `col = v`. Query them
   (`SELECT … FROM dep_table WHERE col = v`) and:
   - **dep is a parent node:** reconcile each candidate row's pk (recompute `match` against current
     nodes) → may flip parent values → **recurse** to step 2.
   - **dep is a SubqueryShape:** re-evaluate the *full* shape predicate against current nodes; emit
     `upsert` if it matches, `delete` if not (idempotent on the client by pk).
3. **`table` is a SubqueryShape's outer table:** evaluate the shape filter on the delta with
   `matches_ctx(row, nodes)` (subquery leaves consult node sets) — the normal enter/update/leave path,
   plus node awareness.

All node updates, move queries, and appends happen **synchronously inside `on_table_delta`**, before the
tailer publishes its processed offset — so the existing drain barrier (and thus `drainEngine`) still
guarantees convergence. The registry mutex serializes subquery processing globally (fine for correctness
and test scale; per-table parallelism is a later optimization).

### NULL / `NOT IN`

`col IN set` with `col = NULL` is UNKNOWN (excluded), as today. `col NOT IN set` is UNKNOWN whenever the
set contains NULL (SQL semantics) — tracked by `has_null`. A node's `has_null` flip re-derives its
`NOT IN` dependents by re-querying their full candidate set (rare; the oracle generator's projections are
non-null pks/tags, so this path is exercised only defensively).

## AST & SQL

New predicate leaf (single-column `IN`/`NOT IN`), added to `@electric-ivm/protocol` and mirrored in Rust:

```jsonc
{ "col": "parent_id",
  "in": { "table": "parent", "project": "id", "where": { /* nested Predicate, optional */ } },
  "negated": false }
```

- **TS** `types.ts`: `InSubqueryPredicate { col; in: SubqueryRef; negated? }`; guard `isInSubquery`.
  `validatePredicate` recurses into `in.where` against the *inner* table. `sql.ts`
  `predicateToSql` emits `"col" [NOT] IN (SELECT "project" FROM "table" WHERE <inner>)`. `shapeSelectSql`
  gets subqueries for free → the **Postgres oracle is ground truth with no extra code**. The TS reference
  `evaluate()` is unused for subquery shapes (oracle goes through SQL); it throws a clear error if asked.
- **Rust** `predicate.rs`: `PredicateJson::In { col, r#in: SubqueryJson, negated }`;
  `CompiledPredicate::InSubquery { col, sig, negated }` (compiled inner pred lives in the node, keyed by
  `sig`). `matches` → `matches_ctx` with a `SubqueryEval` that resolves `sig → (contains(value),
  has_null)`. `sql.rs` `predicate_to_sql` mirrors the TS emitter (for outer/inner backfill).

## API & client

- `apps/api/router.ts`: extend the recursive `predicateSchema` zod with the `in` node. No new endpoint —
  a subquery shape is just a `ShapeDef` with a subquery `where`, created via `shapes.create`.
- **Client: no change.** A subquery shape is a normal *materialized* shape; the engine emits `upsert`/
  `delete` by pk, and `client.shape()` (stream-db reconciled view) materializes it. Move-out is a
  `delete` of a row the client actually holds (inserted via backfill/move-in), so stream-db applies it —
  unlike the subset move-out caveat.

## Tests to port (the stop condition)

Ported into `packages/conformance` (TS, against the real engine + pg oracle) + Rust unit tests:

1. **Grammar/AST** (Rust `predicate.rs`, TS `protocol.test.ts`): compile/validate `IN`/`NOT IN`,
   `predicateToSql` round-trips, reject unsupported sublink shapes.
2. **`conformance-subquery.test.ts`** — the property-test analog. A multi-level schema mirroring
   Electric's `level_1..4` (+ tag side-tables); a `CASES` matrix of subquery predicates (1/2/3-level,
   tag subqueries, `NOT IN`, compositions with `AND`/`OR`/`NOT` and atomics); register as shapes, drive a
   deterministic/seeded mutation stream (toggle active, move parent, add/remove tag, update value), drain,
   assert convergence vs `oracle.queryShape`.
3. **Deterministic move scenarios** (`subquery_move_out_test.exs` / `subquery_dependency_update_test.exs`
   analogs): parent deactivation → move-out; move-in via new parent; `DELETE parent` → move-out; `NOT IN`
   move-in/out; **combined condition** (subquery `AND status='published'`) — move-in must not mask a
   failing condition; **multi-level** (team moves between premium orgs; old org loses tag) → no spurious
   delete. Each asserts convergence after the mutation + drain.
4. **Sharing topology** (`conformance-sharing.test.ts` analog): N shapes sharing one inner subquery use
   **one** node — asserted via a `GET /tables/:name/subqueries` (or `/subquery-nodes`) introspection
   endpoint, like the families endpoint.
5. **Restart/restore** (optional, `oracle_restore_test.exs` analog): restart the engine mid-stream; node
   state rebuilds from backfill; assert convergence. Stretch — included if time permits.

## Memory & sharing summary

| | cost |
|---|---|
| Inner node | O(inner result size) values × contributor pks; **shared** across identical subqueries |
| Outer shape | stores nothing extra; affected rows fetched by keyed query-back on flip |
| Per outer delta | `matches_ctx` = node lookups (O(1) per subquery leaf) |
| Per inner flip | one keyed `SELECT … WHERE col = v` per dependent edge + re-eval |

The win vs storing each outer set: nodes are shared and sized to the inner set, not the outer × shapes
cross-product. Pathological only for very low-selectivity inner sets with huge fan-out (a value referenced
by many outer rows) — same inherent cost any correct incremental subquery maintenance pays.

## Out of scope

Composite-key `(a,b) IN (…)`; `EXISTS` / `= (SELECT …)` / `< ANY`; Electric's exact tag/buffering
streaming protocol (we assert convergence, not control messages); cross-table parallelism of the registry.
