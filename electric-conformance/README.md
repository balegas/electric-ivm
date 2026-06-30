# Electric protocol conformance

Runs ElectricSQL's **own** oracle harness (`Support.OracleHarness` / `ShapeChecker` — its
comparison-against-Postgres logic) against electric-lite's `GET /v1/shape` adapter, driven by Electric's
official Elixir `Electric.Client`. This proves electric-lite speaks Electric's wire protocol.

## Run
1. `cargo build --release -p electric-lite-engine`
2. Copy `electric_lite_oracle_test.exs` and `electric_lite_oracle_property_test.exs` into
   `../electric/packages/sync-service/test/integration/`.
3. In `../electric/packages/sync-service`: `mix deps.get` then
   - hand-written scenarios: `mix test test/integration/electric_lite_oracle_test.exs`
   - **property test** (the real one): `mix test test/integration/electric_lite_oracle_property_test.exs`

Both boot our stack (durable-streams + engine + adapter) via `packages/bench/src/electric-adapter.ts`,
point Electric's official `Electric.Client` at it, and run Electric's own `OracleHarness`/`ShapeChecker`.
The property test additionally reuses Electric's `WhereClauseGenerator` + `StandardSchema` (DDL/seed/
mutation generators); the launcher's two-phase mode (`ADAPTER_WAIT_TABLE`) lets the test apply
StandardSchema's exact schema+seed before the engine introspects. `ELECTRIC_LITE_DIR` overrides the path.
Tunables: `ORACLE_RUNS`, `ORACLE_SHAPE_COUNT`, `ORACLE_BATCH_COUNT`, `ORACLE_MUTATIONS_PER_TXN`.

## Electric's subquery integration tests (`subquery_move_out_test.exs`, `subquery_dependency_update_test.exs`)

These are Electric's **hand-written** subquery integration tests, run against electric-lite via a setup
swap: `el_lite_setup.ex` (`el_lite_pg` + `el_lite_client`) replaces `with_unique_db` + `with_complete_stack`
+ `with_electric_client` with our launcher-booted stack (engine introspects all tables via
`ELECTRIC_LITE_PG_TABLES=*`); **the test bodies and assertions are unchanged**. Copy all three files into
`test/integration/` (the two test files) and `test/support/` (`el_lite_setup.ex`), then
`mix test test/integration/subquery_move_out_test.exs test/integration/subquery_dependency_update_test.exs`.

**Result: 13 / 15 pass.** The 13 cover the real subquery behaviors — synthetic move-out deletes (parent
deactivation/deletion), move-in via a different parent, negated move-in/out, dependency tracking (team
moves between premium orgs with no spurious deletes), combined-condition move-in, resume-preserves-move-out,
and the stale-tag no-spurious-delete case. The **2 failures both assert Electric's row-`tags` mechanism**
(`assert %{headers: %{tags: [_]}}` / `assert new_tags != initial_tags`) — an Electric-internal protocol
detail electric-lite deliberately doesn't emit (we use absolute membership emission, not row tags). They
are not membership/correctness failures.

## Status — ✅ passing (oracle)
The Electric oracle property test passes against electric-lite across the **full** standard schema
(`level_1..4` + composite-PK `*_tags`) and the full generated grammar: comparisons (`= <> < > <= >=`),
`LIKE`/`NOT LIKE`, `BETWEEN`/`NOT BETWEEN`, `IN (list)`, 1/2/3-level `IN (SELECT …)` subqueries, tag
subqueries, `NOT IN`, and `AND`/`OR`/`NOT` compositions — all converging vs the Postgres oracle through
Electric's official client. Verified at `ORACLE_RUNS=25 ORACLE_SHAPE_COUNT=5 ORACLE_BATCH_COUNT=4`
(`1 property, 0 failures`).
