# Electric protocol conformance

Runs ElectricSQL's **own** oracle harness (`Support.OracleHarness` / `ShapeChecker` — its
comparison-against-Postgres logic) against electric-ivm's `GET /v1/shape` adapter, driven by Electric's
official Elixir `Electric.Client`. This proves electric-ivm speaks Electric's wire protocol.

## Run
1. `cargo build --release -p electric-ivm-engine`
2. Copy `electric_ivm_oracle_test.exs` and `electric_ivm_oracle_property_test.exs` into
   `../electric/packages/sync-service/test/integration/`.
3. In `../electric/packages/sync-service`: `mix deps.get` then
   - hand-written scenarios: `mix test test/integration/electric_ivm_oracle_test.exs`
   - **property test** (the real one): `mix test test/integration/electric_ivm_oracle_property_test.exs`

Both boot our stack (durable-streams + engine + adapter) via `packages/bench/src/electric-adapter.ts`,
point Electric's official `Electric.Client` at it, and run Electric's own `OracleHarness`/`ShapeChecker`.
The property test additionally reuses Electric's `WhereClauseGenerator` + `StandardSchema` (DDL/seed/
mutation generators); the launcher's two-phase mode (`ADAPTER_WAIT_TABLE`) lets the test apply
StandardSchema's exact schema+seed before the engine introspects. `ELECTRIC_IVM_DIR` overrides the path.
Tunables: `ORACLE_RUNS`, `ORACLE_SHAPE_COUNT`, `ORACLE_BATCH_COUNT`, `ORACLE_MUTATIONS_PER_TXN`.

## Adapter behavior notes (`/v1/shape`)

- **Handle idle-TTL.** Every initial `GET /v1/shape` (offset=-1) creates one engine shape + durable
  stream keyed by the returned `electric-handle`. Handles idle longer than **`ELECTRIC_HANDLE_TTL`**
  seconds (default `600`) are evicted by a background task: the shape and its stream are dropped, and a
  later request on that handle gets the standard `409` + `must-refetch` control (the client
  re-snapshots). Set the env var on the engine process to tune it (e.g. `ELECTRIC_HANDLE_TTL=60`).
- **Errors.** Validation failures (bad `where`, unknown table/column, `offset` without a `handle`,
  table not matching the handle's shape) are `400` with an Electric-style `{"message": …}` body;
  transient/internal failures (durable-streams hiccups, backfill errors) are `500` so the client
  retries instead of killing its sync loop.
- **Live long-poll deadline** is the adapter's own **`ELECTRIC_LIVE_TIMEOUT_MS`** (default `20000`,
  matching Electric's ~20s live long-poll), decoupled from the durable-streams server's long-poll
  timeout (which just paces the adapter's re-poll loop). At the deadline the response is `204` with
  the electric headers (handle/offset unchanged, `electric-up-to-date`), matching Electric, instead
  of a `200 []` the client would busy-loop on. The launcher (`electric-adapter.ts`) sets it from
  `ADAPTER_LIVE_TIMEOUT_MS`, defaulting to `ADAPTER_LONGPOLL_MS` (1000) so the oracle harness keeps
  its fast up-to-date detection; the fleet benchmark runner sets `ADAPTER_LIVE_TIMEOUT_MS=20000`.
- **Live-request coalescing.** Concurrent `live=true` requests at the same (handle, offset) are
  identical, so one leader performs the serialized read+apply and every concurrent request at that
  offset receives the same response (write-fanout: N clients long-poll one handle at one offset —
  serializing them behind the per-handle mutex would hand each a full long-poll timeout in turn).
  Non-live requests and other offsets keep the serialized per-handle-mutex path; only the leader
  mutates per-handle state.
- **`IS NULL` / `IS NOT NULL`** in `where` are fully supported: they map to the engine's native
  null-test predicate leaf (`PredicateJson::IsNull` — the one leaf that evaluates TRUE on a NULL
  cell), so they compose correctly under `NOT`/`AND`/`OR` and match Postgres semantics exactly.

## Electric's subquery integration tests (`subquery_move_out_test.exs`, `subquery_dependency_update_test.exs`)

These are Electric's **hand-written** subquery integration tests, run against electric-ivm via a setup
swap: `el_ivm_setup.ex` (`el_ivm_pg` + `el_ivm_client`) replaces `with_unique_db` + `with_complete_stack`
+ `with_electric_client` with our launcher-booted stack (engine introspects all tables via
`ELECTRIC_IVM_PG_TABLES=*`); **the test bodies and assertions are unchanged**. Copy all three files into
`test/integration/` (the two test files) and `test/support/` (`el_ivm_setup.ex`), then
`mix test test/integration/subquery_move_out_test.exs test/integration/subquery_dependency_update_test.exs`.

**Result: 13 / 15 pass.** The 13 cover the real subquery behaviors — synthetic move-out deletes (parent
deactivation/deletion), move-in via a different parent, negated move-in/out, dependency tracking (team
moves between premium orgs with no spurious deletes), combined-condition move-in, resume-preserves-move-out,
and the stale-tag no-spurious-delete case. The **2 failures both assert Electric's row-`tags` mechanism**
(`assert %{headers: %{tags: [_]}}` / `assert new_tags != initial_tags`) — an Electric-internal protocol
detail electric-ivm deliberately doesn't emit (we use absolute membership emission, not row tags). They
are not membership/correctness failures.

## Status — ✅ passing (oracle)
The Electric oracle property test passes against electric-ivm across the **full** standard schema
(`level_1..4` + composite-PK `*_tags`) and the full generated grammar: comparisons (`= <> < > <= >=`),
`LIKE`/`NOT LIKE`, `BETWEEN`/`NOT BETWEEN`, `IN (list)`, 1/2/3-level `IN (SELECT …)` subqueries, tag
subqueries, `NOT IN`, and `AND`/`OR`/`NOT` compositions — all converging vs the Postgres oracle through
Electric's official client. Verified at `ORACLE_RUNS=25 ORACLE_SHAPE_COUNT=5 ORACLE_BATCH_COUNT=4`
(`1 property, 0 failures`).
