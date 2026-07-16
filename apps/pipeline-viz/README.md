# @electric-circuits/pipeline-viz

A web GUI **attached to a running electric-circuits engine** that visualizes the dbsp pipeline it is
maintaining — a learning tool for seeing how shapes are executed and shared, with the **live state
of every node** on the canvas.

## Two views, both engine-truthful

- **Logical** — the engine's node set as `GET /graph` reports it: every node id (`table:`,
  `filter:`, `family:`, `node:`, `shape:`) is an engine id. Trace hops and state updates key on
  the same ids, so nothing is reconstructed client-side — what flashes is what ran, and the
  counts you see are the engine's counters.
- **dbsp circuit** — the exploded operator dataflow, **emitted by the engine** (`/graph`'s
  `operators`/`opEdges`): source → Δ → σ/↦/arrange/⋈/distinct/Σ → π → sink, one box per real
  execution step. Each operator carries the trace-hop id it animates under and (for the
  state-bearing operator only) the state-summary id its chips show — declared bindings, not
  client-side guesses. Dashed edges are stateful arrangements feeding joins; shared structure
  (a table's Δ, a family's params arrangement, a subquery's distinct node) appears once, exactly
  as the engine shares it.

In the Logical view each node card carries its dbsp identity and its live state:

- **table · Δ source** — a replication source. Chips: envelopes processed + the convergence offset.
- **σ filter · stateless** — a standalone predicate (range / OR / NOT / inequality), evaluated
  directly on each delta under three-valued logic. Chip: envelopes emitted.
- **↦⋈ route join · STATE** — the shared equality router: an arrangement of predicate keys →
  shapes, one per family (`WHERE k = const` compiles to an index entry, not a circuit). Chips:
  live routing-index keys + routed shapes.
- **IN-set arrange · STATE** — a shared subquery inner set (`value → contributing pks`), one per
  distinct `IN (SELECT …)`. Chips: distinct values + refcount.
- **Σ fold · STATE** — a scalar aggregation maintained as an incremental fold. Chips: the current
  value (live), matching-row count, and the MIN/MAX retraction-multiset size.
- **shape out · π** — the per-shape output stream (grouped by pk into upsert/delete envelopes).
  Chip: envelopes emitted (backfill + live).

## Reactive state, one connection

The app seeds from `GET /state` (every node's summary in one response) and then applies the
`{"type":"state"}` events the engine pushes on its `GET /trace` SSE stream after each processed
batch, with a slow (10s) full re-seed as a safety net — the trace broadcast is lossy by design.
State chips update in place — no polling per node, no graph rebuild, at most three concurrent
connections per tab (the SSE stream, an occasional `/graph`/`/state` fetch, and one detail-panel
poll while a panel is open).

## Live trace animation

Every replicated change animates through the canvas as it flows through the real pipeline: a delta
dot travels the edges (green `+1` insert, red `−1` delete, blue `±1` update) and each visited node
flashes with its outcome — passed, routed, **dropped** (filter mismatch or snapshot-gate skip), or
**folded** (absorbed into an aggregation). Hop ids match node ids 1:1. Shape creations light up
too: new nodes and the paths into them flash purple with a `★ new` badge
(`shapeAdded`/`shapeDropped` lifecycle events trigger an immediate, settled graph refresh instead
of waiting for the next poll).

## Inspecting state and data

Click any node for its detail panel — each kind gets an "inside this operator" explainer plus its
full live state:

- **Route joins** dump their actual routing index (`key tuple → shape ids`) from
  `GET /state/node?id=family:…`.
- **Aggregations** show the live value (pushed on the state stream) plus the fold internals —
  matching rows, non-NULL count, and the MIN/MAX **retraction multiset** contents.
- **Subquery nodes** show the live inner-set index (`value → contributor count`) and their
  dependents.
- **Materialized shapes** show their live contents (a folding of the shape's stream);
  **feed shapes** (`changesOnly`) show a **live change log** instead — every insert / update /
  delete on the tail, newest first, with deletes carrying the departed row.
- **Tables** show envelopes/offset and a paginated **browse data** view (one-shot subset queries,
  a page at a time — nothing is materialized and no shape is created).

## Selection & shape tools

- **List shapes** (left panel, grouped by table) with their predicate and routing kind.
- **Click a shape** → just its pipeline (the upstream closure). **⌘/Ctrl-click** to select several
  — anything they **share underneath** (a route join, a subquery node) appears **once**.
- **Entire graph** → the whole maintained pipeline at once.
- **✕ on a shape row** force-drops that shape; **🗑 Delete all** sweeps every shape (shared shapes
  are ref-counted, so the sweep repeats until the graph drains). Live clients holding those shapes
  will need to resubscribe.
- The sidebar collapses (bottom-left ☰) and is drag-resizable at its right edge.

## Run it

Against the LinearLite demo, it launches automatically:

```bash
pnpm demo:linearlite        # prints:  🔬 Pipeline visualizer → http://localhost:5180/
# DEMO_VIZ=0 to skip it, DEMO_VIZ_PORT=NNNN to change the port
# With HTTPS on (default when caddy is installed) it is also fronted at https://localhost:5443/
# over HTTP/2 — DEMO_VIZ_HTTPS_PORT to change, DEMO_HTTPS=0 to skip.
```

Against any engine, standalone:

```bash
ELECTRIC_CIRCUITS_ENGINE_URL=http://127.0.0.1:<engine-port> VIZ_PORT=5180 \
  pnpm --filter @electric-circuits/pipeline-viz dev
```

The Vite dev server proxies `/engine/*` to that engine (no CORS needed). `VIZ_HOST` pins the bind
address when a TLS proxy needs a deterministic upstream.

Or containerized via the tutorials stack:

```bash
cd tutorials && docker compose up --build    # serves http://localhost:5180
# ENGINE_UPSTREAM=other-engine:7010 to point at another engine
```

## Backed by

- `GET /graph` (`apps/engine/src/http.rs` → `Engine::graph`) returns tables, every shape with its
  routing placement (`familyKey` / standalone / `isSubquery`) and predicate, plus the shared
  subquery node + edge DAG. It reads in-memory topology only — no cost to the hot path.
- `GET /state` (→ `Engine::state_snapshot`) returns the live summary of every node — offsets,
  emit counters, index cardinalities, fold values — assembled from per-tailer published maps.
- `GET /state/node?id=<node-id>` (→ `Engine::dump_node`) deep-dumps one node's state: a route
  join's index contents, an aggregate's fold internals (incl. the multiset), a subquery node's
  inner set.
- `GET /trace` (SSE, `apps/engine/src/trace.rs`) streams per-envelope pipeline traces (hops with
  per-node outcomes), `shapeAdded`/`shapeDropped` lifecycle events, and `state` summary updates
  after each batch. Hop and state ids share the graph's namespace, so everything maps without
  translation. Lossy by design; zero cost when nobody subscribes.
- `GET /shapes/{id}/rows` (fold to current set), `GET /shapes/{id}/log` (exact-op change log), and
  `POST /query` (one-shot paginated subset) drive the detail panel's data views.
