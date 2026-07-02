# DBSP Playground Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A hosted playground app that visualizes Electric shapes as live DBSP pipelines over a real engine — scenes, one-click writes, per-envelope trace animation, per-visitor workspaces.

**Architecture:** Three pieces. (1) The Rust engine gains a `GET /trace` SSE endpoint fed by a `tokio::sync::broadcast` channel; `process_envelope` (apps/engine/src/engine.rs:1388) publishes one JSON event per envelope with the hops taken. (2) A thin Node server (`apps/playground/server`) owns workspaces/seeds/actions/shape-building, writes plain SQL to Postgres, registers shapes via the engine's `POST /shapes` / `POST /aggregate` (JSON predicate AST), and fans `/trace` out per client tagged yours/other. (3) A Vite React app (`apps/playground/src`) reuses pipeline-viz's graph builders via the `@viz` alias and adds scenes, action buttons, device cards, and trace-driven edge animation.

**Tech Stack:** Rust (axum 0.8, tokio broadcast), Node 22 + tsx + `pg` (no framework — `node:http` like docker/api-server), React 18 + @xyflow/react + dagre, vitest, Playwright MCP for E2E debugging.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-02-dbsp-playground-design.md`. Domain: food delivery; statuses `new|cooking|riding|delivered|cancelled`.
- Every playground row carries `workspace_id`; every shape predicate gets `AND workspace_id = $ws` appended server-side; display is HONEST (never strip the conjunct).
- The browser talks only to the playground server (`/api/*`). No raw SQL from clients. Engine untouched except the trace feature.
- Client↔server contract is `apps/playground/shared/types.ts`; scene copy+shape defs are `apps/playground/shared/scenes.ts` (both already written).
- Trace is best-effort animation; device cards poll `GET /shapes/{id}/rows` as ground truth.
- Trace node ids use the logical-graph namespace from `apps/pipeline-viz/src/build-graph.ts`: `table:<t>`, `filter:<sid>`, `family:<t>:<col,col>`, `node:<sig>`, `shape:<sid>`.

---

### Task 1: Engine trace types, broadcast channel, `/trace` SSE route

**Files:**
- Create: `apps/engine/src/trace.rs`
- Modify: `apps/engine/src/main.rs` (module decl), `apps/engine/src/engine.rs:23-42` (Engine struct + accessor), `apps/engine/src/http.rs:14-36` (route)

**Interfaces:**
- Produces: `TraceEvent { lsn: Option<String>, txid: Option<String>, table: String, delta: Vec<TraceDelta>, hops: Vec<TraceHop>, shapes: Vec<String> }`, `TraceDelta { row: serde_json::Value, w: i64 }`, `TraceHop { node: String, outcome: &'static str, key: Option<serde_json::Value> }`; `Engine::trace_sender() -> broadcast::Sender<Arc<String>>` (events pre-serialized once); `GET /trace` → `text/event-stream`, one `data:` line per event.

- [ ] Write `trace.rs`: serde structs above (`#[serde(rename_all = "camelCase")]`, skip-none), `pub const CHANNEL_CAP: usize = 1024`, plus unit test `trace_event_serializes_camel_case` asserting a sample event's JSON keys.
- [ ] Add `trace_tx: tokio::sync::broadcast::Sender<Arc<String>>` to `Engine` (created in `Engine::new` with `broadcast::channel(CHANNEL_CAP).0`); `pub fn trace_sender(&self)` accessor. Publishing rule everywhere: skip serialization when `trace_tx.receiver_count() == 0`; `let _ = tx.send(...)` (lagging receivers drop, hot path never blocks).
- [ ] Add `/trace` to `http.rs` using `axum::response::sse::{Sse, Event, KeepAlive}` over `tokio_stream::wrappers::BroadcastStream` (add `tokio-stream = { version = "0.1", features = ["sync"] }` to Cargo.toml), mapping `Ok(json)` → `Event::default().data(&*json)` and dropping `Lagged` errors.
- [ ] `cargo test -p electric-ivm-engine trace_` → PASS; `cargo build` clean. Commit `feat(engine): trace event types + /trace SSE endpoint`.

### Task 2: Emit trace from the standalone/family/aggregate fan-out

**Files:**
- Modify: `apps/engine/src/engine.rs` — `process_envelope` (1388), `tailer_loop`/`spawn_tailer` (pass the sender + table column names for key labels), the callsite constructing tailers
- Test: `apps/engine/src/engine.rs` `#[cfg(test)]` additions (the crate's existing test style)

**Interfaces:**
- Consumes: Task 1's types/sender. `shapes: HashMap<String, StandaloneShape>` keys and `aggregates` keys are shape ids; `RoutedShape.num_id` → id `format!("s{num_id}")`; family node id cols from `router.key_cols` mapped through `ts` column names joined with `,` — MUST match `Engine::graph()`'s `familyKey` derivation (engine.rs:718-785; verify order and reuse the same helper).
- Produces: per envelope exactly one broadcast event. Hops: `table:<t>` always (`outcome: "passed"`); per standalone shape either `filter:<sid>` `dropped` (pred matched nothing / gate skip) or `filter:<sid>` `passed` + shape id in `shapes` + `shape:<sid>` hop `passed`; per family router one `family:<t>:<cols>` hop — `routed` with `key` when ≥1 shape received it, `dropped` when no key matched; each reached routed shape adds `shape:<sid>` hop and id; per agg shape `shape:<sid>` hop `folded` + id when the value changed, `dropped` when gate-skipped or the delta didn't match.
- Delta payload: rows via the same JSON conversion `translate_output`/rows endpoint uses, capped at 8 entries with a `truncated` implied by cap (just cap; the UI only animates a few dots).

- [ ] Write failing test `trace_family_route_and_filter_drop`: build a tailer fixture like existing engine tests (find an existing `process_envelope`-level test to copy setup from; if none exists, test through the smallest public seam the tests already use), subscribe a receiver, process an UPDATE envelope, assert hop sequence + `shapes` for (a) an equality shape on the routed key, (b) a standalone filter that drops.
- [ ] Thread `trace_tx` + build the event in `process_envelope`: collect hops/shape-ids into locals alongside the existing loops (guarded by `receiver_count() > 0` so the untraced path stays zero-cost), then serialize once and send after the aggregate loop.
- [ ] `cargo test -p electric-ivm-engine trace_` → PASS. Commit `feat(engine): per-envelope trace emission (standalone, family, aggregate)`.

### Task 3: Subquery trace collection

**Files:**
- Modify: `apps/engine/src/subquery.rs` — `on_table_delta` (499-568), `apply_node_flips`, `emit_shape_delta`, `move_shape_for_value`; `apps/engine/src/engine.rs:1461-1466` callsite

**Interfaces:**
- Consumes: Task 1 `TraceHop`.
- Produces: `on_table_delta(..., trace: Option<&mut Vec<TraceHop>>)`; when `Some`, pushes `node:<sig>` hops (`passed` when the inner set changed, `dropped` when the inner delta didn't change it), `shape:<sid>` `passed` hops for outer shapes that emitted (including flip-driven moves), and returns shape ids via the hop list (engine.rs merges them into `shapes`).

- [ ] Failing test `trace_subquery_cascade`: inner-table change flips a value; assert hops contain the node sig and the dependent shape, and that an unrelated inner change yields a `dropped` node hop.
- [ ] Implement the collector param (pass `None` from non-traced callers); merge into `process_envelope`'s event before send.
- [ ] `cargo test -p electric-ivm-engine` (full suite — regression) → PASS. Commit `feat(engine): subquery hops in trace events`.

### Task 4: Playground server — bootstrap, workspaces, seeds

**Files:**
- Create: `apps/playground/server/main.ts` (http server + router), `apps/playground/server/db.ts` (pool, `ensureTables`, seed SQL), `apps/playground/server/workspace.ts`
- Test: `apps/playground/server/__tests__/workspace.test.ts` (vitest, real Postgres via `TEST_PG_URL`, skipped when unset)

**Interfaces:**
- Consumes: `shared/types.ts` (`WorkspaceState`, `Restaurant`, `Order`), env `PLAYGROUND_PG_URL`, `PLAYGROUND_ENGINE_URL`, `PLAYGROUND_PORT` (default 5199), `PLAYGROUND_EPOCH` (default 1).
- Produces: `ensureTables(pool)` creating `playground_restaurants` / `playground_orders` (int GENERATED ALWAYS AS IDENTITY pk, `workspace_id text NOT NULL`, cols per shared types, `REPLICA IDENTITY FULL`) and meta table `playground_workspaces(id text pk, epoch int, created_at, last_seen)`; `provisionWorkspace(deps, existingId?) -> WorkspaceState` (idempotent: existing id + matching epoch returns current state; unknown/stale id mints fresh); routes `POST /api/workspace`, `GET /api/workspace/:id` (404 when unknown → client re-provisions). Seed: 6 restaurants (4 Lisbon, 2 Porto, names+emoji fixed array), 5 orders in mixed statuses.
- Table names: prefixed `playground_*` so the engine's `PG_TABLES=*` story stays tidy next to other demos; the UI labels them `orders`/`restaurants` — NO: honest display. Decision: name them exactly `restaurants` and `orders` inside a dedicated database (dev: ephemeral PG; docker: `POSTGRES_DB=playground`) so display needs no aliasing.

- [ ] Failing vitest: provision → 6 restaurants + 5 orders rows with the workspace id; re-provision same id → identical ids (idempotent); provision after epoch bump → fresh workspace.
- [ ] Implement db.ts/workspace.ts/main.ts (plain `node:http` + tiny path router; JSON body helper; all errors → `{error}` with status).
- [ ] `pnpm --filter @electric-ivm/playground test` → PASS (against a scratch PG started by the test setup via `initdb`/docker — copy the repo's existing test-PG pattern from vitest.global-setup.ts if present, else document `TEST_PG_URL` requirement).
- [ ] Commit `feat(playground): server bootstrap, workspace provisioning + seeds`.

### Task 5: Actions (domain verbs)

**Files:**
- Create: `apps/playground/server/actions.ts`
- Test: `apps/playground/server/__tests__/actions.test.ts`

**Interfaces:**
- Consumes: `Verb` union from shared/types.
- Produces: `applyAction(pool, ws, verb) -> { ok: true, order?: Order }`; route `POST /api/action`. SQL per verb (all `AND workspace_id = $ws`): place_order → INSERT status `new` with random dish/total from fixed arrays (server-side rand ok); start_cooking `new→cooking`; pickup `cooking→riding`; deliver `riding→delivered`; cancel (any non-terminal → `cancelled`); move_restaurant → UPDATE restaurants SET city. Illegal transition → 409 `{error}`.

- [ ] Failing vitest covering the full lifecycle, an illegal transition (deliver a `new` order → 409), and cross-workspace denial (verb with other workspace's orderId → 404).
- [ ] Implement; wire route. Tests PASS. Commit `feat(playground): domain action verbs`.

### Task 6: Shapes + scenes (engine registration)

**Files:**
- Create: `apps/playground/server/engine-client.ts` (POST /shapes, /aggregate, DELETE /shapes/:id, GET /graph, /shapes/:id/rows, POST /query), `apps/playground/server/shapes.ts` (spec→PredicateJson composition + registry), `apps/playground/server/scenes.ts` (idempotent provisioning from `shared/scenes.ts`)
- Test: `apps/playground/server/__tests__/shapes.test.ts`

**Interfaces:**
- Consumes: engine API — `POST /shapes {table, where?: PredicateJson, columns?}` → `{shapeId, streamPath}`; `POST /aggregate {table, where?, fn, col?}`; predicate AST per `packages/protocol/src/types.ts`.
- Produces: `specToWhere(spec, ws) -> Predicate` — conjuncts + optional `{col, in:{...}, negated}` + ALWAYS `{col:'workspace_id', op:'eq', value: ws}` appended at top level AND (and inside the subquery inner where too — inner tables are also workspace rows); `createShape(deps, ws, spec, label, role, scene) -> PlaygroundShape` persisting to meta table `playground_shapes(shape_id, workspace_id, scene, role, label, spec jsonb, where_json jsonb)`; `provisionScene(deps, ws, n) -> SceneShapeResult` (per SceneShapeDef.key idempotent); routes `POST /api/shape`, `DELETE /api/shape/:id`, `POST /api/scene`, plus `GET /api/graph` returning `{graph, mine}` (engine /graph passthrough + this workspace's shape ids), `GET /api/shapes/:id/rows` proxy (404 unless shape ∈ workspace), `POST /api/subset {orderBy, limit}` → engine `/query` with the workspace conjunct.

- [ ] Failing vitest (needs a running engine — reuse/copy the boot helper the conformance/vitest global setup uses): specToWhere unit cases (no conjuncts → just ws eq; subquery inner gets ws conjunct; agg spec passes through), scene 2 provision idempotency (two calls → one engine shape), graph `mine` filtering, rows proxy denial for foreign shape.
- [ ] Implement; wire routes. Tests PASS. Commit `feat(playground): shape builder, scene provisioning, graph/rows proxies`.

### Task 7: Trace fan-out with yours/other tagging

**Files:**
- Create: `apps/playground/server/trace.ts`
- Test: `apps/playground/server/__tests__/trace-tag.test.ts` (pure unit — no engine)

**Interfaces:**
- Consumes: engine `GET /trace` SSE (Task 1 event JSON), meta registry of workspace→shape ids.
- Produces: one upstream SSE connection (lazy, reconnect w/ backoff), fanned to N client responses on `GET /api/trace?workspace=…`. Tagging: event `yours` iff `event.shapes ∩ workspaceShapes ≠ ∅` OR any delta row has `row.workspace_id === ws`. Foreign events: strip `delta` rows (keep weights), keep only hops on shared node kinds (`table:`, `family:`, `node:`), drop `filter:`/`shape:` hops and `shapes`, set `yours:false`. Heartbeat comment every 15s.

- [ ] Failing unit tests for `tagAndStrip(event, ws, myShapes)` covering both branches + strip behavior.
- [ ] Implement fan-out + route. Tests PASS. Commit `feat(playground): trace SSE fan-out with workspace tagging`.

### Task 8: Defenses + static serving

**Files:**
- Modify: `apps/playground/server/main.ts`, `apps/playground/server/workspace.ts`
- Test: `apps/playground/server/__tests__/defense.test.ts`

**Interfaces:**
- Produces: token bucket (per workspace, 5 rps burst 15) on `/api/action` + `/api/shape` → 429; caps: MAX_SHAPES_PER_WS=12 (413), MAX_OPEN_ORDERS=30 (oldest `new` auto-cancelled on overflow); idle TTL sweep (default 24h, env `PLAYGROUND_WS_TTL_H`) deleting rows + dropping engine shapes; production mode serves `dist/` (SPA fallback) when `PLAYGROUND_STATIC=dist` set.

- [ ] Failing tests: rate limit trips; 13th shape rejected; sweep deletes an aged workspace's rows and engine shapes.
- [ ] Implement. Tests PASS. Commit `feat(playground): rate limits, caps, TTL sweep, static serving`.

### Task 9: App shell — API client, workspace lifecycle, layout

**Files:**
- Create: `apps/playground/src/main.tsx`, `src/api.ts`, `src/useWorkspace.ts`, `src/App.tsx`, `src/styles.css`
- (scaffold already present: package.json, vite.config.ts w/ `@viz` alias + `/api` proxy, tsconfig, index.html, shared/*)

**Interfaces:**
- Consumes: `/api/*` contract from shared/types.
- Produces: `api.ts` typed fetch helpers (throwing `ApiError{status}`); `useWorkspace()` → `{ state: WorkspaceState | null, status: 'booting'|'ready'|'reset-needed'|'error', refresh(), newWorkspace(), act(verb), createShape(spec,label,role), enterScene(n) }` — id in `localStorage['playground-ws']`, on 404/epoch-mismatch → `reset-needed` with a modal offering re-provision; polls `GET /api/workspace/:id` every 2.5s for orders/shapes. `App.tsx` grid: left world panel (280px) / center canvas / right device rail (320px) / bottom scene strip; renders placeholders for panels built in Tasks 10-11.
- [ ] Implement; `pnpm --filter @electric-ivm/playground build` (tsc via vite) clean; commit `feat(playground): app shell, workspace lifecycle`.

### Task 10: Pipeline canvas + trace animation

**Files:**
- Create: `src/PipelineCanvas.tsx`, `src/useTrace.ts`, `src/trace-anim.ts`, `src/edges.tsx`

**Interfaces:**
- Consumes: `buildGraph` from `@viz/build-graph`, `buildDbspGraph` from `@viz/build-dbsp`, `nodeTypes` from `@viz/nodes`, `GraphResponse`, `TraceEvent` SSE via `EventSource('/api/trace?workspace=…')`.
- Produces: `<PipelineCanvas graph mine view={'logical'|'dbsp'} pulses />`; selection = the workspace's shapes (`restrictToSelection` via buildGraph's `Set(mine)`); `useTrace(ws)` → ring buffer of recent `TraceEvent` + subscription callback. Animation model (`trace-anim.ts`): map a TraceEvent to `EdgePulse[] { edgeId, t0, dur, color(+green/−red/foreign-gray), label('+1'|'−1') }` + `NodeFlash[] { nodeId, kind: 'pass'|'drop'|'fold' }` by walking hops in order against the current edge list (edge id format from build-graph: `${source}~>${target}~${label??''}` — match by source/target prefix instead of exact id). Custom edge type (`edges.tsx`) renders BaseEdge + an SVG circle with `animateMotion` when a pulse targets it; drop = red ✕ badge flashed on the filter node, no downstream pulse. dbsp view: reuse same hops (map `filter:` → `f:`, `family:` → `ix:/pa:/j:` chain, `shape:` → `snk:`, `node:` → `dist:`, `table:` → `src:/d:`).
- [ ] Implement; verify types compile; commit `feat(playground): pipeline canvas with trace-driven delta animation`.

### Task 11: World panel, device cards, scene strip, shape builder, top-5 board

**Files:**
- Create: `src/WorldPanel.tsx`, `src/DeviceCards.tsx`, `src/useShapeRows.ts` (poll `/api/shapes/:id/rows`, 2s — port of pipeline-viz `useShapeContents` against the playground proxy), `src/SceneStrip.tsx`, `src/ShapeBuilder.tsx`, `src/SubsetBoard.tsx`

**Interfaces:**
- Consumes: `useWorkspace` API, `SCENES` from shared/scenes, `PlaygroundShape.role` for card chrome.
- Produces: WorldPanel — restaurant cards (name/emoji/city + move-city select in scene≥4) with per-order status buttons (the legal next verbs only) + Place order; DeviceCards — one card per shape: role icon + label + honest predicate (reuse `predicateLabel` from `@viz/predicate-label`), rows table (or big scalar for aggregates), flash on change, expandable raw feed (last N row diffs computed client-side between polls), delete button for custom shapes; SceneStrip — scene tabs + explainer card + try-chips, `enterScene(n)` on select, persists current scene in localStorage; ShapeBuilder — modal implementing `ShapeSpec` (column/op/value selects constrained per table, optional subquery + aggregate sections); SubsetBoard (scene 6 card) — `POST /api/subset` top-5 by total + shows pinned `lsn`, refresh button.
- [ ] Implement all five; wire into App; `pnpm build` clean; commit `feat(playground): world panel, device cards, scenes, shape builder, subset board`.

### Task 12: Orchestration — one-command dev boot, docker, README

**Files:**
- Create: `apps/playground/start.ts` (pattern-copy from `examples/linearlite/start.ts`: ephemeral Postgres, create tables via `ensureTables`, start engine binary, start playground server, start vite with env wired), `apps/playground/README.md`
- Modify: root `package.json` (script `demo:playground`), `docker/compose.yaml` (+ `playground` service on Dockerfile.node running `pnpm --filter @electric-ivm/playground start`, `POSTGRES_DB` note)

**Interfaces:**
- Produces: `pnpm demo:playground` → prints app URL; docker compose profile for hosted deployment.
- [ ] Implement; boot locally; verify `/health`, provisioning, one action, trace event visible via `curl -N`. Commit `feat(playground): one-command boot + docker service + README`.

### Task 13: End-to-end debugging with Playwright MCP

- [ ] Boot the stack; with Playwright MCP: fresh profile → workspace provisions; walk scenes 1→6 asserting: scene shapes appear in canvas + device rail; place order animates table→…→shape; start cooking pulses into kitchen card and the card updates within 2.5s; scene 2 drop case shows the ✕; scene 3 family router shows shared badge; scene 4 move-city cascades; scene 5 numbers tick; scene 6 board pins an LSN. Reset flow: bump `PLAYGROUND_EPOCH`, reload → reset modal → re-provision works.
- [ ] Fix every bug found (systematic-debugging skill per bug); re-run the failing step after each fix.
- [ ] Final: `cargo test`, `pnpm -r test`, `pnpm -r build` all green. Commit fixes; update spec if behavior diverged.

## Self-review notes

- Spec coverage: scenes 1-6 (Tasks 6/11), honest workspace display (Task 6 specToWhere + Task 11 predicateLabel), trace+animation incl. drop case and foreign pulses (Tasks 2/3/7/10), defenses/epoch (Tasks 4/8/9), subset queries (Tasks 6/11), deployment local+hosted (Task 12), testing (each task + 13).
- Type consistency: contract lives in shared/types.ts; server and app import it. Engine JSON is duck-typed against `TraceEvent` — Task 7's unit test pins the mapping.
- Known judgment call: dedicated `POSTGRES_DB=playground` database instead of table prefixes, so honest display shows clean table names.
