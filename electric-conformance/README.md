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

## Status — ✅ passing
The Electric oracle property test passes against electric-lite across the **full** standard schema
(`level_1..4` + composite-PK `*_tags`) and the full generated grammar: comparisons (`= <> < > <= >=`),
`LIKE`/`NOT LIKE`, `BETWEEN`/`NOT BETWEEN`, `IN (list)`, 1/2/3-level `IN (SELECT …)` subqueries, tag
subqueries, `NOT IN`, and `AND`/`OR`/`NOT` compositions — all converging vs the Postgres oracle through
Electric's official client. Verified at `ORACLE_RUNS=25 ORACLE_SHAPE_COUNT=5 ORACLE_BATCH_COUNT=4`
(`1 property, 0 failures`).
