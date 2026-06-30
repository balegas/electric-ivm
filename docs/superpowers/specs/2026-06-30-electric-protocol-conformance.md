# Electric protocol conformance: run Electric's oracle tests against our engine

Design record — 2026-06-30. Status: **✅ done — Electric's oracle property test passes against our engine.**
Branch `electric-protocol-conformance`. Goal: make electric-lite speak Electric's `GET /v1/shape` HTTP
protocol faithfully enough that Electric's **own** oracle tests (driven by the real Elixir
`Electric.Client`) pass against our engine.

**Result:** Electric's oracle property test — its real `OracleHarness`/`ShapeChecker` + official
`Electric.Client` + `WhereClauseGenerator`/`StandardSchema` generators — runs against our `/v1/shape`
adapter and converges vs the Postgres oracle across the full standard schema (`level_1..4` +
composite-PK `*_tags`) and full grammar (comparisons, `LIKE`/`NOT LIKE`, `BETWEEN`/`NOT BETWEEN`,
`IN (list)`, 1/2/3-level `IN (SELECT …)` + tag subqueries, `NOT IN`, `AND`/`OR`/`NOT`).
`1 property, 0 failures` at 25 runs × 5 shapes × 4 batches. Tests + reproduction in `electric-conformance/`.
Engine work delivered: the `/v1/shape` adapter (`electric.rs`), a SQL `where` parser (`where_sql.rs`),
the `LIKE` operator, `create_shape` backfill-await, and composite-primary-key support.

## How the oracle tests work (reverse-engineered from ../electric/packages/sync-service)

- The oracle harness (`test/support/oracle_harness*`, `oracle_property_test.exs`) drives shapes **only over
  HTTP** through `Electric.Client` (`shape_checker.ex:161` `Client.poll(...)`). No in-process calls.
- The "oracle" is a live Postgres query (`SELECT cols FROM table WHERE <where> ORDER BY pk`) on the same DB
  the harness mutates. The assertion is **final-state convergence**: the client's materialized row map ==
  the oracle result set, compared as **stringified values** (`to_string_value`: `true`→`"true"`,
  `false`→`"false"`, int→`"5"`, text as-is, NULL→nil).
- The only seam to target an external server: `with_electric_client` (`integration_setup.ex:23-49`) computes
  `base_url` from a locally-started Bandit listener. We patch it to read `ORACLE_TARGET_BASE_URL` from env
  (and run with `restart_server_every: 0`) so the client polls our server instead.

## Wire contract our server must satisfy (`GET /v1/shape`)

- **Params:** `table`, `where` (SQL string), `columns`, `offset` (required; `-1` initial), `handle`
  (required when `offset != -1`), `live` (`"true"`), `cursor`, `replica` (`full`).
- **Response headers:** always `electric-handle` + `electric-offset`; `electric-schema` on the initial
  non-live `200`; `electric-cursor` on every `live=true` `200`; `electric-up-to-date` when caught up. The
  Elixir client *raises* if these are missing — they are mandatory.
- **Body:** JSON array of change messages `{"headers":{"operation":"insert"|"update"|"delete"},"key":..,
  "value":{col→text}}` terminated by a control message `{"headers":{"control":"up-to-date",
  "global_last_seen_lsn":".."}}` when caught up. `409` with `[{"headers":{"control":"must-refetch"}}]`
  forces a resync.
- **Live:** `live=true` long-polls up to a timeout, then returns `200` (data + up-to-date control). Status
  is always `200`/`409` — never rely on `204`.
- Offsets/handles/cursor are **opaque** to the client (it echoes them back). We can use our durable-stream
  offset string as `electric-offset`, our shape id as `electric-handle`, a monotonic int as `electric-cursor`.

## Architecture: an Electric-protocol adapter inside the engine (axum)

The engine already creates materialized shapes (backfill + live deltas → a durable stream). The adapter is
a protocol-translating reader over that stream:

1. **`offset=-1` (snapshot):** parse `where` → predicate, `engine.create_shape` (materialized), read the
   shape's durable stream to its tail, materialize to the current key→row map, emit every row as an
   `insert`, set `electric-handle`=shape id, `electric-offset`=tail, `electric-schema` from the table
   schema, then the `up-to-date` control. Cache (handle → {where, columns, stream_path, schema}).
2. **`live=true` (offset=X, handle):** long-poll the durable stream from X; map our `upsert`→`insert`
   (key new to the materialized set) or `update` (key already present), `delete`→`delete`; emit
   `electric-cursor`, then `up-to-date`. The insert/update distinction is reconstructed by re-deriving the
   materialized key-set up to X (our engine emits absolute `upsert`, not insert/update).
3. **Unknown/stale handle:** `409` must-refetch.

### Value encoding
Each `Value` → Postgres text: `Int`→decimal, `Bool`→`"true"`/`"false"`, `Text`→raw, `Float`→shortest,
`Null`→JSON `null`. Matches the oracle's `to_string_value`. `electric-schema` = `{col: {type: pg_type}}`.

### `where` SQL → predicate (Electric's generated grammar)
Recursive-descent parser for the oracle generator's grammar (`where_clause_generator.ex`): `col <op> 'lit'`
(`= <> < > <= >=`), `col [NOT] LIKE 'pat'`, `col [NOT] BETWEEN 'a' AND 'b'`, `col [NOT] IN ('a',..)`,
`col [NOT] IN (SELECT proj FROM t WHERE …)` (recursive), `AND`/`OR`/`NOT`, parenthesization, `active = true`
booleans. Desugar: `BETWEEN`→`AND(gte,lte)`, `IN (list)`→`OR(eq…)`, `NOT …`→`Not`. New engine op needed:
**`LIKE`** (added to AST/compile/matches/SQL-emit); `ILIKE` not in grammar.

## Driving it
An Elixir setup (`with_external_electric_client`) + a launcher (`scripts/electric-adapter-up`) that, given the
test PG URL, boots durable-streams + engine (PG mode) + the adapter and prints its base URL; the setup
points `Electric.Client` at it. The harness's StandardSchema/WhereClauseGenerator/ShapeChecker are reused
unchanged.

## Scope / order
1. Adapter happy-path (eq `where`) + value/schema encoding → Electric.Client reads one shape end-to-end.
2. `where` grammar parser + `LIKE`/`BETWEEN`/`IN-list` + subqueries (already supported).
3. Elixir glue; run `oracle_property_test` with `restart_*_every: 0` → green across seeds.
4. Stretch: `oracle_restore_test` (server restart/resume).

## Out of scope (initial)
Server-restart/restore (needs our engine to persist + resume); compaction; `must-refetch` beyond
unknown-handle; SSE mode; subset/`live_sse`; replica modes other than `full`.
