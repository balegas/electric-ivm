import { Background, Controls, MiniMap, ReactFlow, type Edge, type Node, type NodeProps } from '@xyflow/react'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import { buildGraph, type NodeRef, type VizNodeData } from './build-graph'
import { buildDbspGraph } from './build-dbsp'
import { DetailPanel } from './DetailPanel'
import { edgeTypes, type PulseEdgeData } from './edges'
import { PipelineNode } from './nodes'
import { predicateLabel } from './predicate-label'
import { shapeSql } from './shape-sql'
import { eventDecor, mergeDecor, viewNodes, type Decor, type FlashKind } from './trace-anim'
import type { EngineGraph, GraphShape, TraceEvent, TraceLifecycle } from './types'
import { useAggValues } from './useAggValues'
import { useTrace } from './useTrace'

type Mode = 'all' | 'select'
type View = 'logical' | 'dbsp'

interface Metrics {
  counters: { envelopes_processed: number; shape_appends: number; family_steps: number }
  append_us: { p99_us: number }
}

/** How long a trace decoration (flash + pulse) stays on screen after the last event. */
const DECOR_TTL_MS = 1100
/** How long newly created nodes/paths stay highlighted after a graph change. */
const FRESH_TTL_MS = 2500
/** How long the graph structure must be quiet before a lifecycle-triggered refresh. Clients
 *  create short-lived subset-feed shapes around each interaction (add + drop ~0.7s apart);
 *  settling past that renders one net change instead of thrashing the layout twice. */
const LIFECYCLE_SETTLE_MS = 1000

/** Node wrapper adding the trace flash overlay around the base renderer. */
function FlashNode(props: NodeProps) {
  const d = props.data as VizNodeData & { flash?: FlashKind | 'new' }
  return (
    <div className={d.flash ? `flash flash-${d.flash}` : undefined}>
      {d.flash === 'drop' ? <span className="flash-x">✕ dropped</span> : null}
      {d.flash === 'new' ? <span className="flash-star">★ new</span> : null}
      <PipelineNode {...props} />
    </div>
  )
}
const nodeTypes = { pipeline: FlashNode }

/** Logical-namespace ids that appear in `next` but not `prev` — the structure a create added. */
function graphDiff(prev: EngineGraph, next: EngineGraph): Set<string> {
  const added = new Set<string>()
  const famKey = (s: GraphShape) => (s.familyKey ? `${s.table}:${s.familyKey.join(',')}` : null)
  const prevShapes = new Set(prev.shapes.map((s) => s.id))
  for (const s of next.shapes) {
    if (prevShapes.has(s.id)) continue
    added.add(`shape:${s.id}`)
    added.add(`filter:${s.id}`)
  }
  const prevFams = new Set(prev.shapes.map(famKey).filter(Boolean) as string[])
  for (const s of next.shapes) {
    const k = famKey(s)
    if (k && !prevFams.has(k)) added.add(`family:${k}`)
  }
  const prevNodes = new Set(prev.subqueryNodes.map((n) => n.sig))
  for (const n of next.subqueryNodes) if (!prevNodes.has(n.sig)) added.add(`node:${n.sig}`)
  const prevTables = new Set(prev.tables)
  for (const t of next.tables) if (!prevTables.has(t)) added.add(`table:${t}`)
  return added
}

function kindOf(s: GraphShape): { label: string; cls: string } {
  if (s.aggregate) return { label: `agg · ${s.aggregate.func}`, cls: 'k-agg' }
  if (s.isSubquery) return { label: 'subquery', cls: 'k-sq' }
  if (s.familyKey) return { label: `family(${s.familyKey.join(',')})`, cls: 'k-fam' }
  return { label: 'standalone', cls: 'k-std' }
}

export default function App() {
  const [graph, setGraph] = useState<EngineGraph | null>(null)
  const [metrics, setMetrics] = useState<Metrics | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [mode, setMode] = useState<Mode>('all')
  const [auto, setAuto] = useState(true)
  const [loadedAt, setLoadedAt] = useState<number>(0)
  const [search, setSearch] = useState('')
  const [focus, setFocus] = useState<{ id: string; ref: NodeRef } | null>(null)
  const [view, setView] = useState<View>('logical')
  const [sidebarOpen, setSidebarOpen] = useState(true)

  const lastGraphJson = useRef<string>('')
  const load = useCallback(async () => {
    try {
      const [gr, mr] = await Promise.all([fetch('/engine/graph'), fetch('/engine/metrics')])
      if (!gr.ok) throw new Error(`engine /graph → ${gr.status}`)
      const text = await gr.text()
      // Only publish a new graph when the CONTENT changed: a fresh object identity per poll makes
      // React Flow rebuild every edge each 2.5s, which kills in-flight pulse animations.
      if (text !== lastGraphJson.current) {
        lastGraphJson.current = text
        setGraph(JSON.parse(text) as EngineGraph)
      }
      if (mr.ok) setMetrics((await mr.json()) as Metrics)
      setErr(null)
      setLoadedAt(Date.now())
    } catch (e) {
      setErr(String(e))
    }
  }, [])

  useEffect(() => {
    void load()
  }, [load])
  useEffect(() => {
    if (!auto) return
    // Hold the poll while a lifecycle settle is pending — it must not publish the intermediate
    // state (e.g. a transient subset-feed shape) that the settle exists to skip.
    const t = setInterval(() => {
      if (!lifecycleTimer.current) void load()
    }, 2500)
    return () => clearInterval(t)
  }, [auto, load])

  const { nodes, edges } = useMemo<{ nodes: Node[]; edges: Edge[] }>(() => {
    if (!graph) return { nodes: [], edges: [] }
    if (mode === 'select' && selected.size === 0) return { nodes: [], edges: [] }
    const sel = mode === 'all' ? 'all' : selected
    const f = focus?.id ?? null
    return view === 'dbsp' ? buildDbspGraph(graph, sel, f) : buildGraph(graph, sel, f)
  }, [graph, mode, selected, focus, view])

  // Live trace decoration: flashes on nodes, travelling delta dots on edges. Refs let the trace
  // callback map events against the CURRENT render without re-subscribing.
  const [decor, setDecor] = useState<Decor | null>(null)
  const decorTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
  const edgesRef = useRef(edges)
  edgesRef.current = edges
  const presentRef = useRef(new Set<string>())
  presentRef.current = useMemo(() => new Set(nodes.map((n) => n.id)), [nodes])
  const viewRef = useRef(view)
  viewRef.current = view

  const lifecycleTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
  const onTrace = useCallback(
    (ev: TraceEvent | TraceLifecycle) => {
      if ('type' in ev) {
        // Structure changed (shape created/dropped) — refresh once the churn settles instead of
        // waiting for the next poll; the graph-diff effect below highlights what appeared.
        // Lifecycle events arrive in bursts (a client interaction creates several shapes at once,
        // and transient subset feeds drop again within ~0.7s) — one refresh per settled burst,
        // or the canvas re-layouts several times in a row, which reads as flicker.
        if (lifecycleTimer.current) clearTimeout(lifecycleTimer.current)
        lifecycleTimer.current = setTimeout(() => {
          lifecycleTimer.current = null
          void load()
        }, LIFECYCLE_SETTLE_MS)
        return
      }
      const d = eventDecor(ev, viewRef.current, edgesRef.current, presentRef.current)
      if (d.nodes.size === 0 && d.edges.size === 0) return
      setDecor((prev) => mergeDecor(prev, d))
      if (decorTimer.current) clearTimeout(decorTimer.current)
      decorTimer.current = setTimeout(() => setDecor(null), DECOR_TTL_MS)
    },
    [load],
  )
  useTrace(auto, onTrace)
  useEffect(
    () => () => {
      if (decorTimer.current) clearTimeout(decorTimer.current)
      if (freshTimer.current) clearTimeout(freshTimer.current)
      if (lifecycleTimer.current) clearTimeout(lifecycleTimer.current)
    },
    [],
  )

  // Newly created structure: diff each graph load against the previous one and highlight what
  // appeared (logical-namespace ids, expanded to the current view's ids at render time).
  const [fresh, setFresh] = useState<Set<string> | null>(null)
  const freshTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
  const prevGraphRef = useRef<EngineGraph | null>(null)
  useEffect(() => {
    if (!graph) return
    const prev = prevGraphRef.current
    prevGraphRef.current = graph
    if (!prev) return // first load — nothing is "new"
    const added = graphDiff(prev, graph)
    if (added.size === 0) return
    setFresh(added)
    if (freshTimer.current) clearTimeout(freshTimer.current)
    freshTimer.current = setTimeout(() => setFresh(null), FRESH_TTL_MS)
  }, [graph])

  // Live scalar per aggregation shape (e.g. the current COUNT(*)), shown on the agg nodes below.
  const aggValues = useAggValues(graph)

  // Keep the detail panel meaningful across shape churn: clients drop + recreate identical shapes
  // under new ids (e.g. every LinearLite navigation), which would orphan a panel pinned to the old
  // id. When the focused shape vanishes, retarget to the same-query replacement if one exists.
  const focusShapeSig = useRef<{ id: string; sig: string } | null>(null)
  useEffect(() => {
    if (!graph || !focus) return
    const m = focus.id.match(/^(?:shape|snk):(.+)$/)
    if (!m) return
    const id = m[1]!
    const sigOf = (s: GraphShape) =>
      JSON.stringify([s.table, s.where ?? null, s.changesOnly, s.aggregate ?? null, s.columns ?? null])
    const cur = graph.shapes.find((s) => s.id === id)
    if (cur) {
      focusShapeSig.current = { id, sig: sigOf(cur) }
      return
    }
    const want = focusShapeSig.current
    const repl = want && want.id === id ? graph.shapes.find((s) => sigOf(s) === want.sig) : undefined
    if (repl) {
      focusShapeSig.current = { id: repl.id, sig: sigOf(repl) }
      setFocus({
        id: focus.id.startsWith('snk:') ? `snk:${repl.id}` : `shape:${repl.id}`,
        ref: repl.aggregate ? { kind: 'aggshape', shapeId: repl.id } : { kind: 'shape', shapeId: repl.id },
      })
    } else {
      setFocus(null)
    }
  }, [graph, focus])

  const freshIds = useMemo(() => {
    if (!fresh) return null
    const s = new Set<string>()
    for (const l of fresh) for (const id of viewNodes(l, view)) s.add(id)
    return s
  }, [fresh, view])

  const decorated = useMemo(() => {
    const dn =
      decor || freshIds || aggValues.size > 0
        ? nodes.map((n) => {
            const d = n.data as VizNodeData
            const flash = decor?.nodes.get(n.id) ?? (freshIds?.has(n.id) ? ('new' as const) : undefined)
            // Aggregation nodes show their live scalar in the index chip: the logical Σ node is
            // `shape:{id}` (kind 'agg'), the dbsp fold is `fold:{id}` (kind 'op-agg') — either way
            // the shape id is the part after the first ':'.
            const agg =
              d.kind === 'agg' || d.kind === 'op-agg'
                ? aggValues.get(n.id.slice(n.id.indexOf(':') + 1))
                : undefined
            if (!flash && !agg) return n
            return { ...n, data: { ...d, ...(agg ? { index: agg } : null), ...(flash ? { flash } : null) } }
          })
        : nodes
    // Edges ALWAYS use the pulse type — flipping an edge's `type` when a decoration appears would
    // remount every edge component at once, which flickers the whole canvas.
    const de = edges.map((e) => {
      // A "new path": an edge touching a newly created node. Goes through baseStyle — PulseEdge
      // renders from data.baseStyle, not the edge's style prop (an active pulse still wins).
      const isFresh = freshIds != null && (freshIds.has(e.source) || freshIds.has(e.target))
      const baseStyle = isFresh ? { ...e.style, stroke: '#7c3aed', strokeWidth: 2.5 } : e.style
      // The pulse keeps the id of the event that created it — re-rendering after a merge must not
      // restart dots already in flight on other edges.
      const data: PulseEdgeData = { pulse: decor?.edges.get(e.id), baseStyle }
      return { ...e, type: 'pulse', data, style: undefined }
    })
    return { nodes: dn, edges: de }
  }, [nodes, edges, decor, freshIds, aggValues])

  // Force-drop a shape from the engine. The resulting shapeDropped lifecycle event refreshes the
  // canvas via the settled path; selection/focus are pruned so the view doesn't dangle.
  const deleteShape = useCallback(async (id: string) => {
    await fetch(`/engine/shapes/${encodeURIComponent(id)}`, { method: 'DELETE' }).catch(() => {})
    setSelected((prev) => {
      if (!prev.has(id)) return prev
      const next = new Set(prev)
      next.delete(id)
      return next
    })
    setFocus((f) => (f && (f.id === `shape:${id}` || f.id === `snk:${id}`) ? null : f))
  }, [])

  // Shared shapes are ref-counted (one DELETE = one decrement), so sweep in passes until the
  // graph reports no shapes (bounded — a client re-creating shapes concurrently can win).
  const deleteAll = useCallback(async () => {
    for (let pass = 0; pass < 5; pass++) {
      const r = await fetch('/engine/graph').catch(() => null)
      if (!r?.ok) break
      const g = (await r.json()) as EngineGraph
      if (g.shapes.length === 0) break
      await Promise.all(
        g.shapes.map((s) => fetch(`/engine/shapes/${encodeURIComponent(s.id)}`, { method: 'DELETE' }).catch(() => {})),
      )
    }
    setSelected(new Set())
    setMode('all')
    setFocus(null)
  }, [])

  const toggle = (id: string, additive: boolean) => {
    setMode('select')
    setFocus(null)
    setSelected((prev) => {
      const next = additive ? new Set(prev) : new Set<string>()
      if (additive && prev.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  const shapesByTable = useMemo(() => {
    const q = search.trim().toLowerCase()
    const m = new Map<string, GraphShape[]>()
    for (const s of graph?.shapes ?? []) {
      if (q && !`${s.id} ${s.table} ${predicateLabel(s.where)}`.toLowerCase().includes(q)) continue
      if (!m.has(s.table)) m.set(s.table, [])
      m.get(s.table)!.push(s)
    }
    for (const arr of m.values()) arr.sort((a, b) => Number(a.id.slice(1)) - Number(b.id.slice(1)))
    return [...m.entries()].sort((a, b) => a[0].localeCompare(b[0]))
  }, [graph, search])

  return (
    <div className={sidebarOpen ? 'app' : 'app sidebar-collapsed'}>
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-title">electric-ivm</div>
          <div className="brand-sub">dbsp pipeline visualizer</div>
        </div>

        <div className="viewtabs">
          <button
            className={view === 'logical' ? 'vtab vtab-on' : 'vtab'}
            onClick={() => {
              setView('logical')
              setFocus(null)
            }}
          >
            Logical
          </button>
          <button
            className={view === 'dbsp' ? 'vtab vtab-on' : 'vtab'}
            onClick={() => {
              setView('dbsp')
              setFocus(null)
            }}
          >
            dbsp circuit
          </button>
        </div>

        {metrics ? (
          <div className="metrics">
            <div className="metric">
              <span className="metric-n">{metrics.counters.envelopes_processed.toLocaleString()}</span>
              <span className="metric-l">changes</span>
            </div>
            <div className="metric">
              <span className="metric-n">{metrics.counters.shape_appends.toLocaleString()}</span>
              <span className="metric-l">appends</span>
            </div>
            <div className="metric">
              <span className="metric-n">{(metrics.append_us.p99_us / 1000).toFixed(1)}ms</span>
              <span className="metric-l">append p99</span>
            </div>
          </div>
        ) : null}

        <div className="toolbar">
          <button
            className={mode === 'all' ? 'btn btn-on' : 'btn'}
            onClick={() => {
              setMode('all')
              setFocus(null)
            }}
          >
            ▦ Entire graph
          </button>
          <button
            className="btn"
            disabled={selected.size === 0}
            onClick={() => {
              setSelected(new Set())
              setMode('all')
            }}
          >
            Clear
          </button>
        </div>
        <div className="toolbar">
          <button className="btn" onClick={() => void load()}>
            ↻ Refresh
          </button>
          <label className="auto">
            <input type="checkbox" checked={auto} onChange={(e) => setAuto(e.target.checked)} /> live
          </label>
          <button
            className="btn btn-danger"
            disabled={!graph || graph.shapes.length === 0}
            title="Drop every shape from the engine (shared feeds are swept until their refcounts drain)"
            onClick={() => void deleteAll()}
          >
            🗑 Delete all
          </button>
        </div>

        {graph ? (
          <div className="counts">
            {graph.shapes.length} shapes · {graph.tables.length} tables · {graph.subqueryNodes.length} subquery
            nodes
          </div>
        ) : null}
        {err ? <div className="err">{err}</div> : null}

        <input
          className="search"
          placeholder="filter shapes… (id, table, predicate)"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />

        <div className="list">
          {shapesByTable.map(([table, shapes]) => (
            <div key={table} className="tgroup">
              <div className="tgroup-h">{table}</div>
              {shapes.map((s) => {
                const k = kindOf(s)
                const on = selected.has(s.id)
                return (
                  <button
                    key={s.id}
                    className={`shape-row${on ? ' shape-on' : ''}`}
                    onClick={(e) => {
                      const additive = e.metaKey || e.ctrlKey || e.shiftKey
                      toggle(s.id, additive)
                      // A plain click also opens the detail panel for this shape (SQL + live contents);
                      // additive clicks just build up the multi-select without stealing focus.
                      if (!additive) {
                        setFocus({
                          id: `shape:${s.id}`,
                          ref: s.aggregate ? { kind: 'aggshape', shapeId: s.id } : { kind: 'shape', shapeId: s.id },
                        })
                      }
                    }}
                    title={shapeSql(s)}
                  >
                    <div className="shape-row-top">
                      <span className="shape-id">{s.id}</span>
                      <span className={`badge ${k.cls}`}>{k.label}</span>
                      {s.changesOnly ? <span className="badge k-feed">feed</span> : null}
                      <span
                        className="shape-del"
                        role="button"
                        title="Delete shape"
                        onClick={(e) => {
                          e.stopPropagation()
                          void deleteShape(s.id)
                        }}
                      >
                        ✕
                      </span>
                    </div>
                    <div className="shape-pred">{predicateLabel(s.where)}</div>
                  </button>
                )
              })}
            </div>
          ))}
        </div>

        {view === 'logical' ? (
          <div className="legend">
            <span className="lg lg-table">table</span>
            <span className="lg lg-family">family router</span>
            <span className="lg lg-filter">filter</span>
            <span className="lg lg-sqnode">subquery node</span>
            <span className="lg lg-agg">Σ aggregation</span>
            <span className="lg lg-shape">shape</span>
          </div>
        ) : (
          <div className="legend">
            <span className="lg lg-table">source</span>
            <span className="lg lg-delta">Δ change</span>
            <span className="lg lg-filter">σ filter</span>
            <span className="lg lg-index">↦ index</span>
            <span className="lg lg-sqnode">arrange (state)</span>
            <span className="lg lg-join">⋈ join</span>
            <span className="lg lg-agg">Σ fold</span>
            <span className="lg lg-shape">sink</span>
          </div>
        )}

        <button className="sidebar-collapse" title="Collapse sidebar" onClick={() => setSidebarOpen(false)}>
          ☰
        </button>
      </aside>

      {!sidebarOpen ? (
        <button className="sidebar-reopen" title="Open sidebar" onClick={() => setSidebarOpen(true)}>
          ☰
        </button>
      ) : null}

      <main className="canvas">
        {mode === 'select' && selected.size === 0 ? (
          <div className="empty">Select one or more shapes to see their maintained pipeline.</div>
        ) : (
          <ReactFlow
            nodes={decorated.nodes}
            edges={decorated.edges}
            nodeTypes={nodeTypes}
            edgeTypes={edgeTypes}
            fitView
            minZoom={0.1}
            onNodeClick={(_e, n) => setFocus({ id: n.id, ref: (n.data as VizNodeData).ref })}
            onPaneClick={() => setFocus(null)}
            proOptions={{ hideAttribution: true }}
          >
            <Background gap={20} color="#eef2f7" />
            <MiniMap position="bottom-left" pannable zoomable nodeStrokeWidth={2} />
            <Controls />
          </ReactFlow>
        )}
        <div className="stamp">
          {loadedAt ? `updated ${new Date(loadedAt).toLocaleTimeString()}` : ''}
          {focus ? ' · click a node for details' : ''}
        </div>
        {focus && graph ? (
          <DetailPanel
            node={focus.ref}
            graph={graph}
            onClose={() => setFocus(null)}
            onSelectShape={(id) => toggle(id, false)}
          />
        ) : null}
      </main>
    </div>
  )
}
