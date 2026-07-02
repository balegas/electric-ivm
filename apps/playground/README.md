# @electric-ivm/playground

An interactive playground that shows **how Electric shapes are implemented as DBSP pipelines** —
built for sharing (demo videos, a hosted live demo), on a real engine and a real Postgres.

It drives the **Shape API** directly against a tiny issue tracker: `issues` and `projects`. Every
cell you edit in the data grids is one SQL write to Postgres, and you watch the resulting **delta**
replicate into the engine and travel the maintained pipeline in the middle — through routers,
filters, subquery nodes, and aggregation folds — down to the per-shape **live result cards** on the
right (each shows its query, its exact API request, and its maintained result set). Scenes walk you
through the ideas (shapes, deltas and drops, shared machinery, subqueries, live aggregations,
subset queries); the shape composer and grids work in every scene.

## What's real

Everything. Writes go to Postgres; the engine ingests them via logical replication; shapes are
registered through the engine's control-plane API; device cards poll the engine's materialized
shape rows; the delta animation is driven by the engine's own `GET /trace` SSE feed (one event per
processed envelope, with the actual hops taken — including drops). Nothing is simulated.

## Workspaces

Every visitor gets a **workspace**: rows in the shared `issues`/`projects` tables stamped with
their `workspace_id`, and every shape predicate carries `AND workspace_id = <yours>` server-side.
The UI keeps this **silent** (predicates and router keys render scrubbed) — the "under the hood"
toggle reveals the real predicates, the `shared ×N` badges, and other visitors' pulses. The
workspace id lives in localStorage; if the operator wipes the server, clients notice (epoch/404)
and offer fresh data in one click.

## Run it

Local dev (ephemeral Postgres + engine + server + Vite, one command):

```bash
pnpm demo:playground        # prints the app URL (default http://localhost:5190)
```

Docker (base stack + playground overlay; app served at :5199):

```bash
pnpm docker:playground
```

Hosted: run the docker stack on one machine (e.g. Fly); wipe by resetting the database and/or
bumping `PLAYGROUND_EPOCH` — clients self-heal.

## Layout

- `server/` — the playground server: the ONLY thing browsers talk to. Workspace provisioning +
  seeds (one restaurant to start; an `add_restaurant` verb grows the world), the action verbs
  (fixed parameterized SQL — no raw SQL surface), a shape endpoint (spec → engine predicate AST,
  always workspace-scoped; scenes are its only UI today), scene provisioning (idempotent,
  self-healing), engine proxies (`/graph`, shape rows, subset queries), and the `/trace` fan-out
  that tags events `yours`/foreign and strips foreign events to shared-node pulses. Defenses:
  per-workspace rate limiting, shape/order/restaurant caps, idle-workspace TTL sweep.
- `src/` — the React app. Reuses pipeline-viz's graph builders + node renderer (`@viz` alias);
  adds the first-visit welcome/intro, the scene strip, world panel, device cards, and the
  trace-driven animation (travelling delta dots, pass/drop/fold node flashes, red ✕ where a delta
  dies).
- `shared/` — the client↔server contract (`types.ts`) and the scene definitions (`scenes.ts`,
  copy + shape specs in one module so the story and the provisioning can't drift).
- `start.ts` — the one-command dev boot (pattern from `examples/linearlite`).

## Tests

```bash
pnpm --filter @electric-ivm/playground test   # unit + integration (boots a real engine + Postgres)
```

The integration suite covers provisioning idempotency, the action lifecycle + illegal transitions,
cross-workspace denial, rate limiting, scene idempotency + engine-restart self-healing, graph/rows
ownership guards, feed-sharing dedup, subset pinning, and a live end-to-end trace event.
