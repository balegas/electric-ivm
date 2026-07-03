# @electric-ivm/pipeline-viz

A web GUI **attached to a running electric-ivm engine** that visualizes the dbsp query pipeline it is
maintaining — a learning tool for seeing how shapes are executed and shared.

## Two views (toggle top-left)

- **Logical** — the routing topology: tables → family routers / filters / subquery nodes → shape
  outputs. Shows *what shares what*.
- **dbsp circuit** — the **raw operator dataflow** that maintains each shape: Z-sets flowing through
  **Δ** (change), **σ** (filter), **↦** (index/arrange), **⋈** (join), **distinct/params** (stateful
  arrangements), and **π** (map → upsert/delete). Operators shared underneath — a table's Δ, a
  family's params arrangement, a subquery's distinct node — appear once, exactly as the engine shares
  them. (This engine hand-rolls these operators over dbsp's Z-set types rather than running a compiled
  circuit; the dataflow is the same, annotated with the real maintained state.)

Both views support single/multi/all selection, node-click details, and the live inner-set / routing
index.

## Logical view

It reads the engine's `GET /graph` introspection endpoint and renders, as an interactive left-to-right
dataflow graph:

- **tables** (replication sources) →
- **family routers** (one shared node per equality template — e.g. `route by (status)` — with a
  `shared ×N` badge), **standalone filters** (one per non-equality shape), and **subquery nodes**
  (one shared inner-set node per distinct `IN (SELECT …)`, with its refcount) →
- **shape outputs** (the per-shape streams clients subscribe to).

## What you can do

- **List shapes** (left panel, grouped by table) with their predicate and routing kind.
- **Click a shape** → just its pipeline (the upstream closure).
- **⌘/Ctrl-click** to select several → they render together, and anything they **share underneath**
  (a family router, a subquery node) appears **once**, with edges to each shape.
- **Entire graph** → the whole maintained pipeline at once.
- **live** toggle re-polls `/graph` every ~2.5s and subscribes to the engine's `/trace` SSE feed, so
  you watch the pipeline grow/shrink as shapes come and go.

## Live trace animation

With **live** on, every replicated change animates through the canvas as it flows through the real
pipeline: a delta dot travels the edges (green `+1` insert, red `−1` delete, blue `±1` update) and
each visited node flashes with its outcome — passed, routed, **dropped** (filter mismatch), or
**folded** (absorbed into an aggregation). Shape creations light up too: new nodes and the paths
into them flash purple with a `★ new` badge (`shapeAdded`/`shapeDropped` lifecycle events trigger an
immediate, settled graph refresh instead of waiting for the next poll).

## Inspecting data

- **Materialized shapes** show their live contents (a folding of the shape's stream, polled).
- **Feed shapes** (`changesOnly`) show a **live change log** instead — every insert / update /
  delete seen on the tail, newest first, with deletes carrying the departed row.
- **Aggregations** show their running scalar on the node chip and as a stat card in the panel.
- **Tables** get a paginated **browse data** view (one-shot subset queries, a page at a time —
  nothing is materialized and no shape is created).

## Shape tools

- **✕ on a shape row** force-drops that shape from the engine.
- **🗑 Delete all** sweeps every shape (shared shapes are ref-counted, so the sweep repeats until
  the graph drains). Both are force-drops: live clients holding those shapes will need to
  resubscribe.
- The sidebar collapses (bottom-left ☰) and is drag-resizable at its right edge.

## Run it

Against the LinearLite demo, it launches automatically:

```bash
pnpm demo:linearlite        # prints:  🔬 Pipeline visualizer → http://localhost:5180/
# DEMO_VIZ=0 to skip it, DEMO_VIZ_PORT=NNNN to change the port
```

Against any engine, standalone:

```bash
ELECTRIC_IVM_ENGINE_URL=http://127.0.0.1:<engine-port> VIZ_PORT=5180 \
  pnpm --filter @electric-ivm/pipeline-viz dev
```

The Vite dev server proxies `/engine/*` to that engine (no CORS needed).

## Backed by

- `GET /graph` (`apps/engine/src/http.rs` → `Engine::graph`) returns tables, every shape with its
  routing placement (`familyKey` / standalone / `isSubquery`) and predicate, plus the shared
  subquery node + edge DAG. It reads in-memory topology only — no cost to the hot path.
- `GET /trace` (SSE, `apps/engine/src/trace.rs`) streams per-envelope pipeline traces — the hops a
  change took with per-node outcomes — and `shapeAdded`/`shapeDropped` lifecycle events. Hop node
  ids use the same namespace as the logical view, so events animate onto the graph without
  translation. Lossy by design; zero cost when nobody subscribes.
- `GET /shapes/{id}/rows` (fold to current set), `GET /shapes/{id}/log` (stream tail as-is), and
  `POST /query` (one-shot paginated subset) drive the detail panel's data views.
