# Pipeline visualizer rebuild: one truthful graph, reactive per-node state

Date: 2026-07-06 · Issue: dbsp-ds-6tv · Status: approved-by-goal (autonomous session)

## Problem

The visualizer (`apps/pipeline-viz`) has two views. The **Logical** view is faithful — its
node ids (`table:`, `filter:`, `family:`, `node:`, `shape:`) are the engine's own trace-hop
namespace, so structure and animation map 1:1. The **dbsp circuit** view is a client-side
fiction: `build-dbsp.ts` synthesizes an operator chain (Δ → σ → ↦ → arrange → ⋈ → π → sink)
the engine never emits, so:

- trace hops are smeared one-to-many across synthetic operators (`trace-anim.ts:39-58`),
  lighting boxes that didn't act;
- edge pulses fire by coincidence of that expansion;
- refcounts, shared-counts and subquery predicates are re-derived client-side and can
  disagree with the engine;
- none of the boxes labelled "STATE" show any state.

Separately, per-node **state is mostly invisible**: the engine exposes the subquery inner
set (`GET /graph/node?sig=`) but *not* the family router's routing index nor the aggregate
fold internals (both live in tailer-task locals). And "live" today means one SSE trace
stream plus four independent 2–2.5 s polling hooks — the connection pressure that motivated
the in-flight Caddy HTTP/2 front for the demo (`examples/linearlite/start.ts`, uncommitted).

## Goals

1. The rendered graph **is** the engine's pipeline — emitted by the engine, zero client
   reconstruction. Node identity = trace-hop identity.
2. Every node shows **live state on the canvas** (index sizes, fold values, emit counters,
   offsets), pushed over the existing SSE channel, not polled.
3. Clicking a node dumps its **full state** (routing index contents, agg multiset, inner
   set, materialized rows).
4. Connection budget ≤ 3 per tab (SSE + occasional topology fetch + one panel poll), so the
   HTTP/1.1 cap is no longer structural; the Caddy front stays as demo polish.
5. All explainer copy (in-app + READMEs + architecture mentions) rewritten to match reality.

## Non-goals

- No compiled dbsp circuit in the engine; we visualize the hand-rolled strategies honestly.
- No trace history/timeline UI (future work).
- No changes to shape/aggregate creation APIs or the Electric adapter.

## Design

### A. One unified view

Drop the synthetic circuit view (`build-dbsp.ts` deleted). One graph = the engine's logical
topology, but each node card is rendered with its dbsp identity: operator glyph + tag
(e.g. family = "↦⋈ ROUTE JOIN · STATE", subquery node = "distinct ARRANGEMENT · STATE",
aggregate = "Σ FOLD · STATE", filter = "σπ stateless"), a formula line, and a **live state
chip row**. The pedagogical "what happens inside this operator" prose moves to the detail
panel, keyed by node kind — honest about what the engine actually executes
(`docs/ivm-engine-internals.md` §3 is the source of truth). Trace animation becomes a
trivial 1:1 hop → node mapping; the smear machinery goes away.

### B. Engine: state publication (apps/engine)

State summaries are cheap numbers; deep dumps are on demand.

1. **`NodeStates` summary map** — `HashMap<String /*node id*/, NodeStateSummary>` published
   behind the existing `TailerHandle`-style `Arc<Mutex<…>>` pattern (`engine.rs:228-236`),
   refreshed by the tailer after every processed batch and on shape add/remove:
   - `table:<t>` → `{ processedOffset, envelopes }` (running counter)
   - `filter:<sid>` → `{ emitted }` (envelopes emitted to the shape stream)
   - `family:<t>:<cols>` → `{ keys, shapes }` (router index cardinality)
   - `shape:<sid>` → `{ emitted }`
   - agg `shape:<sid>` → `{ value, count, nnCount, multisetLen }`
   The subquery registry (already engine-global) contributes `node:<sig>` →
   `{ distinctValues, refcount, emitted }`.
2. **SSE `state` events** — after each batch, when `trace_tx.receiver_count() > 0`, emit
   `{ "type": "state", "nodes": { <id>: <summary>… } }` for nodes touched by the batch, on
   the *existing* `/trace` channel (no new connection). Also emitted once after each
   lifecycle event so adds show state immediately.
3. **`GET /state`** — full snapshot `{ nodes: {…} }` for initial load / SSE reconnect.
4. **`GET /state/node?id=<nodeId>`** — deep dump for the detail panel, via a new
   `TailerCmd::DumpNode { id, resp: oneshot }` round-trip into the owning tailer task:
   - family → routing table `[{ key, shapeIds }]` (capped, `truncated` flag)
   - agg → `{ value, count, nnCount, multiset: [[value, weight]…] }` (capped)
   - subquery node → delegates to the existing `node_value_index`
   - filter/shape/table → summary + pointer fields (rows/log stay on existing endpoints)
5. `/graph`, `/trace` hop semantics unchanged — the viz keys on them as today.

### C. Viz rebuild (apps/pipeline-viz)

- `state-store.ts`: module-level store keyed by node id; seeded from `GET /state`, updated
  by SSE `state` events; safety re-seed on SSE reconnect. Exposed via
  `useSyncExternalStore` with per-node-id subscription so a state tick re-renders only the
  touched node chips, never rebuilding the React Flow graph.
- `build-graph.ts` slimmed: topology only (poll stays, 2.5 s + lifecycle settle, unchanged
  guard on raw-JSON identity); per-kind metadata (glyph, formula, stateful flag) in a
  static table `node-meta.ts`.
- `nodes.tsx`: card renders label + formula + `StateChips` (live). Flash/pulse animation
  kept, mapping now direct.
- `DetailPanel.tsx`: per-kind "inside the operator" explainer + deep state view backed by
  `/state/node` (family routing table, agg internals), existing rows/log/table-browser
  views kept; `useAggValues.ts` deleted (values arrive on the state stream).
- Deleted: `build-dbsp.ts`, view toggle, `useAggValues.ts`, `viewNodes` expansion in
  `trace-anim.ts`.
- Connection budget: 1 SSE + intermittent `/graph`/`/state` fetches + at most one panel
  poll. Fits HTTP/1.1; HTTP/2 front unaffected.

### D. Caddy / demo work

The uncommitted `start.ts` + `vite.config.ts` changes (VIZ_HOST pin + second Caddy front on
5443) are kept verbatim — they remain correct and useful. The rebuild only reduces the
pressure that motivated them.

### E. Copy & docs refresh

- `apps/pipeline-viz/README.md`: rewritten (single view, state model, endpoint table;
  removes the stale "live toggle" narrative).
- Root `README.md` §pipeline-viz and `docs/ARCHITECTURE.md` telemetry/explorer mentions:
  updated endpoint list (+`/state`, `/state/node`) and one-view description.
- In-app copy: `node-meta.ts` formulas/notes, detail-panel prose, legends, empty states —
  all rewritten against `docs/ivm-engine-internals.md` §3 so every sentence describes what
  the engine actually does.

### F. Testing & verification

- Rust: unit tests for `NodeStateSummary` assembly (graph+state coherence: every graph node
  has a state entry), the `/state` handler shape, and `DumpNode` round-trip (family + agg).
- The `/graph`/`/trace` surface is currently untested; the new tests anchor the contract
  the viz depends on.
- E2E: launch the linearlite demo on private ports, drive it with Playwright MCP: create
  shapes, mutate rows via `/pg/write`, assert canvas chips update without page refresh,
  screenshot before/after. Verify connection count stays ≤ 3.

## Risks

- Publishing state summaries on the hot path: mitigation — plain counters + `try_lock`-free
  single `Mutex` swap per batch (same cost class as the existing `publish_stats`).
- SSE `state` payload size under bursty writes: only touched nodes, summaries are scalars.
- Removing the circuit view loses the operator-alphabet pedagogy: recovered in the detail
  panel per node, where it can be honest.
