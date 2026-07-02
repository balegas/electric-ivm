# DBSP Playground — design spec

**Date:** 2026-07-02
**Status:** approved design, pre-implementation
**Audience for the app:** Twitter viewers (short videos) and visitors to a hosted live demo.

## Purpose

A standalone example app that makes visible how Electric shapes are implemented as DBSP
pipelines: you create shapes, you write data with one-click domain actions, and you *watch the
delta travel* through the real engine's pipeline — through family routers, filters, subquery
nodes, and aggregation folds — down to subscriber "devices" that update the instant it lands.

It walks you through and explains things scene by scene, but it is a playground, not a tutorial:
free experimentation is always available.

## Non-goals

- Not a general-purpose admin UI or debugging tool (that's `apps/pipeline-viz`).
- Not a tutorial with exercises or progress tracking.
- No raw SQL from visitors; no arbitrary query building beyond the guided shape builder.
- No accounts/auth. Workspace identity is a localStorage token.

## Decisions made (with rationale)

| Decision | Choice |
| --- | --- |
| Backend | Real Rust engine + Postgres, run via docker locally and a Fly machine hosted. Same code path both ways. |
| Multi-tenancy | Shared tables with a `workspace_id` column; every shape predicate carries `AND workspace_id = $ws`. |
| Workspace display | **Honest**: the `workspace_id` conjunct is shown in predicates and the family key. It becomes part of the story — your shapes and other visitors' shapes genuinely share one family router keyed `(workspace_id, status)`. |
| Workspace concept | First-class and explained in scene 1. Identical in local (docker) and hosted deployments — localhost is just a one-visitor instance. |
| Walkthrough | Scenes + free play: curated scenes each pre-create shapes and show a short explainer card; within any scene the user can freely act and build shapes. |
| Domain | Food delivery: `restaurants` (id, workspace_id, name, city) and `orders` (id, workspace_id, restaurant_id, status: new/cooking/riding/delivered/cancelled, total). Both tables are per-workspace rows in shared tables, so scene 4's “move a restaurant to another city” only affects your workspace. |
| Shape coverage | Equality + family sharing, subqueries (`IN (SELECT …)`), live aggregations, subset queries. |
| Writes | Domain action buttons only (place order / start cooking / rider picks up / delivered / cancel) — parameterized SQL server-side. |
| Delta flow | New engine **SSE trace endpoint** emitting the actual per-envelope route (nodes hit, drops, shapes reached). The UI animates exactly what the engine did. |
| Shape creation | Guided builder (table, column/op/value, optional subquery clause, optional aggregation) — constrained to engine-supported predicates. |
| Shape outputs | Device cards: each shape's subscriber rendered as a small app card (rider phone, kitchen screen, manager dashboard tile), expandable to the raw upsert/delete feed. |
| Code strategy | New `apps/playground` importing pipeline-viz's graph-building modules (`build-graph`, `build-dbsp`, node renderers); own UI shell/styling/scene system. Thin Node "playground server" between browser and engine/Postgres. |

## Architecture

```
browser (React app)
   │  static assets + JSON + SSE
   ▼
playground server (Node/TS)          ←  the only thing the browser talks to
   │  parameterized SQL              │  engine HTTP API
   ▼                                 ▼
Postgres  ── logical replication ──► engine (Rust)
                                       └─ new: GET /trace (SSE)
```

### Engine (Rust) — one new feature

`GET /trace` (SSE): streams one event per replicated envelope processed:

```jsonc
{
  "lsn": "0/1A2B3C",
  "table": "orders",
  "delta": [{ "row": { …cols… }, "w": -1 }, { "row": { … }, "w": 1 }],
  "hops": [
    { "node": "family:orders:(workspace_id,status)", "outcome": "routed", "key": ["w_ab12", "cooking"] },
    { "node": "filter:s7", "outcome": "dropped" },
    { "node": "sq:restaurants:city", "outcome": "passed" },
    { "node": "agg:s9", "outcome": "folded" }
  ],
  "shapes": ["s3", "s9"]
}
```

Semantics: `hops` is the set of pipeline nodes the envelope's delta visited with the outcome at
each; `shapes` are the shape ids whose logs got appends. Trace events are best-effort (bounded
broadcast channel; slow consumers miss events — the UI treats trace as animation, never as truth).
This endpoint is a general engine debugging feature, not playground-specific.

### Playground server (Node/TS) — thin, the only client-facing surface

- `POST /workspace` → mint workspace id + epoch, seed ~6 restaurants and a handful of orders,
  all rows carrying this `workspace_id`; idempotent per id.
- `GET /workspace/:id/health` → `{ ok, epoch }`; 404/epoch-mismatch ⇒ client re-provisions.
- `POST /action` → `{ workspace, verb, orderId?, restaurantId?, total? }`; verbs map to fixed
  parameterized SQL (INSERT order / UPDATE status transitions / DELETE=cancel). All writes scoped
  `WHERE workspace_id = $ws`.
- `POST /shape` → guided-builder payload; server composes the where clause, appends
  `AND workspace_id = $ws`, registers the shape with the engine, returns shape id. Also
  `DELETE /shape/:id` (only shapes belonging to the workspace).
- Scene provisioning: `POST /scene/:n` creates that scene's shapes idempotently for the workspace.
- Proxies to the engine: `/graph`, `/metrics`, shape subscription (wire protocol), and `/trace`
  fanned out per client with each event tagged `yours` / `other` (by intersecting the event's
  `shapes` with the workspace's shape ids; pure-`other` events are stripped to shared-node hops).
- Defenses: caps (max shapes/workspace, max open orders/workspace with oldest auto-completed),
  simple token-bucket rate limit on `/action` and `/shape`, TTL cleanup of idle workspaces
  (drop their rows + shapes), global epoch bumped on operator wipe.

### Playground app (React/Vite)

Layout (one consistent screen; scenes change focus, not chrome):

- **Left — the world**: restaurant cards with their open orders and verb buttons; “Place order”
  picks a restaurant + randomized total.
- **Center — the pipeline**: ReactFlow canvas reusing pipeline-viz's graph builders; logical view
  default, dbsp-circuit toggle. Trace events animate a dot per weighted row along the hop path
  (green `+1`, red `−1`); a dropped delta visibly stops at the filter. Other-workspace events
  render as a faint pulse through shared nodes only.
- **Right — the subscribers**: one device card per shape (rider phone, kitchen screen, dashboard
  tile with live count/sum). Cards are driven by real shape subscriptions (ground truth); trace is
  animation only. Expand a card for the raw upsert/delete message feed.
- **Scene strip** (bottom): titles + explainer card, next/prev. Entering a scene provisions its
  shapes idempotently; data and workspace persist across scenes; free play everywhere.

### Scenes

1. **Your workspace** — provisions + explains the model: shared tables, your `workspace_id`,
   honesty about multi-tenancy, wipe/reset semantics, “New workspace” button. One trivial shape
   (`orders WHERE workspace_id = $ws`) so the first button click already animates.
2. **A shape is a filter** — `status = 'cooking' AND workspace_id = $ws`. Start cooking → delta
   flows to the kitchen screen; delivered → `−1` retraction; place order (status `new`) → dropped
   at the filter.
3. **Shapes share machinery** — add `riding` and `delivered` shapes; three shapes collapse into
   one family router keyed `(workspace_id, status)`, `shared ×N` badge counts other visitors too.
4. **Subqueries** — `restaurant_id IN (SELECT id FROM restaurants WHERE city = 'Lisbon' AND
   workspace_id = $ws)`; the shared inner-set node + join; a “move restaurant to another city”
   action lets you watch orders enter/leave the shape with no order row changing. Restaurants are
   per-workspace rows, so this never affects other visitors.
5. **Live aggregations** — `count(*)` per status + `sum(total)` for one restaurant as running
   folds; deltas arrive as `+/-` adjustments to a number.
6. **Subset queries** — “top 5 orders by total” as an LSN-positioned one-shot over a shape;
   ordering/windowing lives outside shape maintenance; card shows the page pinned at its LSN while
   the shape keeps moving.

### Defensive behavior (server wipes, restarts)

- Workspace id + epoch in localStorage; every server response carries the epoch. Mismatch or
  health 404 ⇒ “This workspace was reset” + one-click re-provision. “New workspace” always
  available.
- Scene provisioning idempotent; on engine/Postgres restart the client re-runs it on reconnect.
- The app never trusts trace for state: device cards resubscribe from the shape's log offset.

## Testing

- **Rust**: unit tests for trace hop sequences — family route, filter drop, subquery cascade,
  aggregation fold.
- **Server**: vitest integration tests against docker engine+Postgres — provisioning idempotency,
  action SQL transitions, workspace scoping (cannot touch another workspace's rows/shapes),
  epoch/reset recovery, caps/rate limits.
- **App**: component tests for trace→animation mapping and scene idempotency; Playwright smoke:
  provision → place order → device card updates.
- **Manual/driven debugging**: Playwright MCP session driving every scene end-to-end.

## Deployment

- Local: docker compose (Postgres + engine) + `pnpm --filter playground dev` — or one
  `pnpm demo:playground` script in the style of `demo:linearlite`.
- Hosted: one Fly machine running Postgres, engine, and the playground server serving built
  assets. Operator wipe = reset DB + bump epoch; clients self-heal.
