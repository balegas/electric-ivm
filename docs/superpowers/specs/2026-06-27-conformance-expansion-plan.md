# Conformance Suite Expansion Plan

**Goal:** verify the correctness of electric-lite across a much wider slice of its query
expressiveness and system behaviour, and *prove* the oracle approach actually catches bugs with a
negative-control (counter-example) test. Everything runs through the real tRPC API + stream-db
client against the pglite oracle, with the `drainEngine` soundness barrier before every comparison.

## Invariant under test

For any shape `S` and any op stream applied to **both** electric-lite and pglite:
`client.materialize(S)  ==  pglite.SELECT * FROM table WHERE <S.where>`
(set equality keyed by stringified pk, comparing declared non-pk columns by value).

The predicate AST is the single source of truth, compiled to a Rust dbsp closure
(`apps/engine/src/predicate.rs`) **and** a SQL `WHERE` (`packages/protocol/src/sql.ts`). Expanding
expressiveness means expanding the generator that exercises both sides — never one alone.

## Coverage dimensions

### A. Query expressiveness (`conformance-expressiveness.test.ts`, deterministic fixtures)

A hand-built fixture dataset with known edge values lets us assert exact boundary behaviour
(faker can't guarantee a row sits *exactly* on a literal). All shapes registered before data, then
`drainEngine`, then compare each shape to the oracle.

1. **Every op × every type.** `eq`/`neq` on `bool`; `eq/neq/lt/lte/gt/gte` on `int`, `float`,
   `text` (lexicographic). One shape per (op, column).
2. **Boundary literals.** Rows placed exactly *on* the literal, just below, and just above, with
   `lt` vs `lte` and `gt` vs `gte` — catches off-by-one comparison bugs (`<` vs `<=`).
3. **Edge values.** empty string `''`; negative ints/floats; large ints; float fractional values;
   `text` ordering with mixed case/length.
4. **Contradiction / tautology.** `and:[gte 100, lt 0]` → always empty; `or:[gte 0, lt 0]` over a
   non-null column → always all. Confirms empty/all shapes converge soundly (drainEngine matters).
5. **Deep nesting (depth ≥ 3).** Mixed `and`/`or`/`not` trees referencing several columns.
6. **All columns referenced.** A conjunction touching every column of the table.

### B. System / op coverage (`conformance-transitions.test.ts`, deterministic)

1. **Enter/leave churn.** One pk toggled IN→OUT→IN→OUT many times against a selective shape;
   converges to the correct final membership.
2. **pk-changing "update".** An update whose row carries a different pk is an upsert of a new key;
   the old key remains. Asserted against the oracle's identical upsert semantics.
3. **Re-insert of a deleted pk.** insert → delete → insert same pk ⇒ present.
4. **Idempotent / duplicate ops.** Repeated identical inserts ⇒ no spurious change; redundant
   deletes of absent keys ⇒ no-op.
5. **High-churn, tiny pk space.** `pkSpace = 3`, hundreds of ops ⇒ heavy upsert/delete overlap.
6. **Multiple shapes over one table** and **multiple tables** in one schema, each converging.
7. **Backfill + churn** is already covered by `conformance-backfill.test.ts`; the live path
   (`awaitTxId`) by `conformance.test.ts`. Not duplicated here.

### C. Wider fuzz (`simulator.ts` + heavier scenario in `conformance-fuzz.test.ts`)

Extend `randomShapeDefs` with options: deeper trees, occasional empty `and`/`or` (tautology/
contradiction), and edge literals (boundary, empty string, negatives) — defaults preserve the
existing behaviour so current fuzz is unchanged. A heavier deterministic scenario (more shapes ×
ops, fixed seed) runs in CI without env tuning; env tunables still scale it further.

### D. Negative control — the counter-example (`conformance-counterexample.test.ts`)

The whole suite is only trustworthy if a *wrong* engine makes it go red. We add **test-only fault
injection** to the Rust engine, gated by `ELECTRIC_LITE_FAULT` (zero effect when unset):

- `drop_deletes` — never emit shape "leave" (delete) envelopes, so a row that exits a shape
  lingers in the client forever (`apps/engine/src/engine.rs::translate_output`).
- `off_by_one_cmp` — treat `>=`/`<=` as strict `>`/`<` (`apps/engine/src/predicate.rs::cmp`).

Tests:
1. **End-to-end negative control.** Boot the engine with `fault: 'drop_deletes'`, run a scenario
   that forces a leave (insert active → update inactive). After `drainEngine`, assert the
   comparison is **not equal** (the stale row shows as `extra`) within a bounded time — the test
   passes by *detecting* the divergence, and never hangs (it deliberately avoids `awaitTxId` on the
   dropped event).
2. **Control.** The identical scenario on a normal (non-faulted) engine converges (`equal`).
3. **Pure-TS unit control.** `compareShapeSets` on a deliberately mutated set returns
   `equal:false` with the expected `missing`/`extra`/`mismatched` — guards the comparator itself.

## Covered: NULL three-valued logic

Previously deferred, now closed. Non-pk columns are nullable by contract (the oracle DDL emits no
`NOT NULL`; the engine stores `Value::Null`; the client zod schema makes non-pk columns
`.nullable()`). The engine evaluator (`predicate.rs`) and the protocol reference evaluator
(`predicate.ts`) both implement SQL three-valued logic:

- any comparison with a NULL operand → UNKNOWN (row excluded),
- `AND`/`OR` follow the SQL truth tables (FALSE dominates AND, TRUE dominates OR, UNKNOWN otherwise),
- `NOT UNKNOWN = UNKNOWN` — so `NOT (col = x)` over a NULL cell keeps the row out, matching Postgres.

Coverage: `conformance-nulls.test.ts` (deterministic fixtures with NULLs in every column incl. an
all-null row, the headline `NOT(eq)`/`NOT(gt)`-over-null cases, AND/OR with null operands, and
match-all materializing null cells) plus a NULL-enabled fuzz (simulator `nullProb` ~35%, depth-3
predicates) — all compared row-for-row to pglite. Unit proofs: `predicate.rs`
`three_valued_null_logic` and `protocol.test.ts` "uses SQL three-valued logic".

## Deferred (explicitly out of scope)

- **Engine restart idempotency** (deterministic `Producer-Seq`) — unchanged from prior status.

## Harness changes

`bootHarness(schema, { fault })` threads `ELECTRIC_LITE_FAULT` into the spawned engine's env. All
existing call sites keep the no-arg form (normal, fault-free). No other harness changes; new tests
reuse `applyOp` / `drainEngine` / `waitForConvergence` / `compareShapeSets`.
