# Electric protocol conformance

Runs ElectricSQL's **own** oracle harness (`Support.OracleHarness` / `ShapeChecker` — its
comparison-against-Postgres logic) against electric-lite's `GET /v1/shape` adapter, driven by Electric's
official Elixir `Electric.Client`. This proves electric-lite speaks Electric's wire protocol.

## Run
1. `cargo build --release -p electric-lite-engine`
2. Copy `electric_lite_oracle_test.exs` into `../electric/packages/sync-service/test/integration/`.
3. In `../electric/packages/sync-service`: `mix deps.get` then
   `mix test test/integration/electric_lite_oracle_test.exs`.

The test boots our stack (durable-streams + engine + adapter) via `packages/bench/src/electric-adapter.ts`
(which seeds a single-PK subset of Electric's `level_1..4` standard schema), points `Electric.Client` at
it, and runs Electric's oracle checks across mutation batches. `ELECTRIC_LITE_DIR` overrides the repo path.

## Status
- ✅ Single-PK schema (`level_1..4`): comparisons, `LIKE`, `IN (list)`, 1/2-level `IN (SELECT …)`
  subqueries, all op types — converge vs the Postgres oracle through Electric's real client.
- ⏳ Composite-PK `*_tags` tables (tag subqueries) + the full random `oracle_property_test` — in progress
  (needs composite-PK support in the engine).
