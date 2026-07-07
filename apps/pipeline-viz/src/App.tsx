import { Background, Controls, MiniMap, ReactFlow, type Edge, type Node, type NodeProps } from '@xyflow/react'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import { buildCircuit, hopIndex } from './build-circuit'
import { buildGraph, type NodeRef, type VizNodeData } from './build-graph'
import { DetailPanel } from './DetailPanel'
import { edgeTypes, type PulseEdgeData } from './edges'
import { PipelineNode } from './nodes'
import { predicateLabel } from './predicate-label'
import { shapeSql } from './shape-sql'
import { applyState, seedState } from './state-store'
import { eventDecor, mergeDecor, type Decor, type FlashKind } from './trace-anim'
import type { EngineGraph, GraphShape, TraceEvent, TraceMessage } from './types'
import { useTrace } from './useTrace'

type Mode = 'all' | 'select'
type View = 'logical' | 'circuit'

/** How long a trace decoration (flash + pulse) stays on screen past its last stage. */
const DECOR_TTL_MS = 1100
/** How many trace events the activity log retains (newest first). */
const LOG_CAP = 50

/** One activity-log entry: a captured trace event, replayable on click. */
interface LogEntry {
  key: number
  at: number
  ev: TraceEvent
}
/** How long newly created nodes/paths stay highlighted after a graph change. */
const FRESH_TTL_MS = 2500
/** How long the graph structure must be quiet before a lifecycle-triggered refresh. Clients
 *  create short-lived subset-feed shapes around each interaction (add + drop ~0.7s apart);
 *  settling past that renders one net change instead of thrashing the layout twice. */
const LIFECYCLE_SETTLE_MS = 1000

/** Node wrapper adding the trace flash overlay around the base renderer. The flash is staged
 *  (`flashDelay`): downstream nodes light up only when the travelling delta reaches them. */
function FlashNode(props: NodeProps) {
  const d = props.data as VizNodeData & { flash?: FlashKind | 'new'; flashDelay?: number }
  const style = d.flashDelay ? ({ '--flash-delay': `${d.flashDelay}ms` } as React.CSSProperties) : undefined
  return (
    <div className={d.flash ? `flash flash-${d.flash}` : undefined} style={style}>
      {d.flash === 'drop' ? <span className="flash-x">✕ dropped</span> : null}
      {d.flash === 'new' ? <span className="flash-star">★ new</span> : null}
      <PipelineNode {...props} />
    </div>
  )
}
const nodeTypes = { pipeline: FlashNode }

/** Node ids that appear in `next` but not `prev` — the structure a create added. The ids are the
 *  engine's own namespace, so they match rendered node ids directly. */
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

/** Compact one-line summary of a trace event for the activity log. */
function logSummary(ev: TraceEvent): { op: string; cls: string; hint: string } {
  const w = ev.delta.reduce((a, d) => a + d.w, 0)
  const op = ev.delta.length === 0 ? '·' : ev.delta.length > 1 && w === 0 ? '± update' : w > 0 ? '+ insert' : '− delete'
  const cls = w > 0 ? 'lop-ins' : w < 0 ? 'lop-del' : 'lop-upd'
  const row = ev.delta.find((d) => d.w > 0)?.row ?? ev.delta[0]?.row
  const id = row && 'id' in row ? String(row.id) : null
  const reached = ev.shapes.length
  const hint = `${id ? `id ${id} · ` : ''}${reached} shape${reached === 1 ? '' : 's'}`
  return { op, cls, hint }
}

function kindOf(s: GraphShape): { label: string; cls: string } {
  if (s.aggregate) return { label: `agg · ${s.aggregate.func}`, cls: 'k-agg' }
  if (s.isSubquery) return { label: 'subquery', cls: 'k-sq' }
  if (s.familyKey) return { label: `family(${s.familyKey.join(',')})`, cls: 'k-fam' }
  return { label: 'standalone', cls: 'k-std' }
}

export default function App() {
  const [graph, setGraph] = useState<EngineGraph | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [mode, setMode] = useState<Mode>('all')
  const [loadedAt, setLoadedAt] = useState<number>(0)
  const [search, setSearch] = useState('')
  const [focus, setFocus] = useState<{ id: string; ref: NodeRef } | null>(null)
  const [view, setView] = useState<View>('logical')
  const [sidebarOpen, setSidebarOpen] = useState(true)
  const [sidebarW, setSidebarW] = useState(340)
  const [resizing, setResizing] = useState(false)
  // Drag the sidebar's right edge to resize; the width feeds both the grid column and (via a CSS
  // variable) the fixed child width that keeps content from rewrapping during the collapse.
  const startResize = (e: React.MouseEvent) => {
    e.preventDefault()
    setResizing(true)
    const move = (ev: MouseEvent) => setSidebarW(Math.min(640, Math.max(240, ev.clientX)))
    const up = () => {
      setResizing(false)
      window.removeEventListener('mousemove', move)
      window.removeEventListener('mouseup', up)
    }
    window.addEventListener('mousemove', move)
    window.addEventListener('mouseup', up)
  }

  const lastGraphJson = useRef<string>('')
  const lastLoadAt = useRef(0)
  const load = useCallback(async () => {
    lastLoadAt.current = Date.now()
    try {
      const gr = await fetch('/engine/graph')
      if (!gr.ok) throw new Error(`engine /graph → ${gr.status}`)
      const text = await gr.text()
      // Only publish a new graph when the CONTENT changed: a fresh object identity per poll makes
      // React Flow rebuild every edge each poll, which kills in-flight pulse animations.
      if (text !== lastGraphJson.current) {
        lastGraphJson.current = text
        setGraph(JSON.parse(text) as EngineGraph)
      }
      setErr(null)
      setLoadedAt(Date.now())
    } catch (e) {
      setErr(String(e))
    }
  }, [])

  useEffect(() => {
    void load()
    void seedState()
    // Slow safety re-seed: the trace broadcast is lossy by design (a lagging subscriber drops
    // events), and some fronts buffer SSE — a periodic full snapshot bounds any staleness.
    const t = setInterval(() => void seedState(), 10_000)
    return () => clearInterval(t)
  }, [load])
  useEffect(() => {
    // Hold the poll while a lifecycle settle is pending — it must not publish the intermediate
    // state (e.g. a transient subset-feed shape) that the settle exists to skip. But sustained
    // shape churn re-arms the settle forever, so force a refresh anyway once we've gone STARVED_MS
    // without one — a slightly noisy canvas beats a frozen stale one.
    const STARVED_MS = 6000
    const t = setInterval(() => {
      if (!lifecycleTimer.current || Date.now() - lastLoadAt.current > STARVED_MS) void load()
    }, 2500)
    return () => clearInterval(t)
  }, [load])

  const { nodes, edges } = useMemo<{ nodes: Node[]; edges: Edge[] }>(() => {
    if (!graph) return { nodes: [], edges: [] }
    if (mode === 'select' && selected.size === 0) return { nodes: [], edges: [] }
    const sel = mode === 'all' ? 'all' : selected
    // alignSources pins every replication-source node into the leftmost rank.
    const opts = { alignSources: true }
    return view === 'circuit' ? buildCircuit(graph, sel, focus?.id ?? null, opts) : buildGraph(graph, sel, focus?.id ?? null, opts)
  }, [graph, mode, selected, focus, view])

  // hop id → rendered node ids: identity in the logical view; in the circuit view, the operator
  // group the ENGINE stamped with that hop (OpNode.hop) — trace flashes and fresh-structure
  // highlights expand through this, never through client-side guessing.
  const expandHop = useMemo(() => {
    if (view === 'logical' || !graph) return (h: string) => [h]
    const idx = hopIndex(graph)
    return (h: string) => idx.get(h) ?? []
  }, [view, graph])
  const expandRef = useRef(expandHop)
  expandRef.current = expandHop

  // Live trace decoration: flashes on nodes, travelling delta dots on edges. Refs let the trace
  // callback map events against the CURRENT render without re-subscribing.
  const [decor, setDecor] = useState<Decor | null>(null)
  const decorTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
  const edgesRef = useRef(edges)
  edgesRef.current = edges
  const presentRef = useRef(new Set<string>())
  presentRef.current = useMemo(() => new Set(nodes.map((n) => n.id)), [nodes])

  // Apply one trace event's staged decoration against the CURRENT render (used by both the live
  // stream and activity-log replays). The decor stays up for the whole staged run + a tail.
  const applyEventDecor = useCallback((ev: TraceEvent) => {
    const d = eventDecor(ev, edgesRef.current, presentRef.current, expandRef.current)
    if (d.nodes.size === 0 && d.edges.size === 0) return
    setDecor((prev) => mergeDecor(prev, d))
    if (decorTimer.current) clearTimeout(decorTimer.current)
    decorTimer.current = setTimeout(() => setDecor(null), d.totalMs + DECOR_TTL_MS)
  }, [])

  // Activity log: the last LOG_CAP trace events, newest first — each entry replays its animation
  // on click. Collapsed by default (a sidebar section header toggles it).
  const [log, setLog] = useState<LogEntry[]>([])
  const [logOpen, setLogOpen] = useState(false)
  const logSeq = useRef(1)

  const lifecycleTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
  const onTrace = useCallback(
    (ev: TraceMessage) => {
      if ('type' in ev) {
        if (ev.type === 'state') {
          // Per-node state push — feed the store; subscribed chips re-render, the graph doesn't.
          applyState(ev.nodes)
          return
        }
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
      setLog((prev) => [{ key: logSeq.current++, at: Date.now(), ev }, ...prev].slice(0, LOG_CAP))
      applyEventDecor(ev)
    },
    [load, applyEventDecor],
  )
  // On every (re)connect, re-seed the state store — state events pushed while disconnected are gone.
  const onTraceOpen = useCallback(() => {
    void seedState()
  }, [])
  useTrace(true, onTrace, onTraceOpen)
  useEffect(
    () => () => {
      if (decorTimer.current) clearTimeout(decorTimer.current)
      if (freshTimer.current) clearTimeout(freshTimer.current)
      if (lifecycleTimer.current) clearTimeout(lifecycleTimer.current)
    },
    [],
  )

  // Newly created structure: diff each graph load against the previous one and highlight what
  // appeared. Diff ids are engine node ids, so they match rendered ids directly.
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

  // Keep the detail panel meaningful across shape churn: clients drop + recreate identical shapes
  // under new ids (e.g. every LinearLite navigation), which would orphan a panel pinned to the old
  // id. When the focused shape vanishes, retarget to the same-query replacement if one exists.
  const focusShapeSig = useRef<{ id: string; sig: string } | null>(null)
  useEffect(() => {
    if (!graph || !focus) return
    const m = focus.id.match(/^shape:(.+)$/)
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
        id: `shape:${repl.id}`,
        ref: repl.aggregate ? { kind: 'aggshape', shapeId: repl.id } : { kind: 'shape', shapeId: repl.id },
      })
    } else {
      setFocus(null)
    }
  }, [graph, focus])

  // Fresh-structure ids are logical hop ids; expand them for the current view.
  const freshIds = useMemo(() => {
    if (!fresh) return null
    const out = new Set<string>()
    for (const h of fresh) for (const id of expandHop(h)) out.add(id)
    return out
  }, [fresh, expandHop])

  const decorated = useMemo(() => {
    const dn =
      decor || freshIds
        ? nodes.map((n) => {
            const df = decor?.nodes.get(n.id)
            const flash = df?.kind ?? (freshIds?.has(n.id) ? ('new' as const) : undefined)
            if (!flash) return n
            return { ...n, data: { ...(n.data as VizNodeData), flash, flashDelay: df?.delayMs ?? 0 } }
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
      const pulse = decor?.edges.get(e.id)
      const data: PulseEdgeData = { pulse, baseStyle }
      // A pulsing edge is lifted above the node cards (each edge is its own stacked svg): the
      // travelling dot + weight label must never disappear behind a component it passes.
      return { ...e, type: 'pulse', data, style: undefined, zIndex: pulse ? 1000 : undefined }
    })
    return { nodes: dn, edges: de }
  }, [nodes, edges, decor, freshIds])

  // Force-drop a shape from the engine (`?purge=true` bypasses the retention lifecycle — a bare
  // DELETE only releases a subscription and the shape would stay on the canvas as active/dormant).
  // The resulting shapeDropped lifecycle event refreshes the canvas via the settled path;
  // selection/focus are pruned so the view doesn't dangle.
  const deleteShape = useCallback(async (id: string) => {
    await fetch(`/engine/shapes/${encodeURIComponent(id)}?purge=true`, { method: 'DELETE' }).catch(() => {})
    setSelected((prev) => {
      if (!prev.has(id)) return prev
      const next = new Set(prev)
      next.delete(id)
      return next
    })
    setFocus((f) => (f && f.id === `shape:${id}` ? null : f))
  }, [])

  // Purge every shape (force-drop, bypassing retention refcounts/lifecycle). Sweep in passes
  // until the graph reports no shapes (bounded — a client re-creating shapes concurrently can win).
  const deleteAll = useCallback(async () => {
    for (let pass = 0; pass < 5; pass++) {
      const r = await fetch('/engine/graph').catch(() => null)
      if (!r?.ok) break
      const g = (await r.json()) as EngineGraph
      if (g.shapes.length === 0) break
      await Promise.all(
        g.shapes.map((s) => fetch(`/engine/shapes/${encodeURIComponent(s.id)}?purge=true`, { method: 'DELETE' }).catch(() => {})),
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
    <div
      className={`app${sidebarOpen ? '' : ' sidebar-collapsed'}${resizing ? ' resizing' : ''}`}
      style={
        {
          gridTemplateColumns: sidebarOpen ? `${sidebarW}px 1fr` : '0 1fr',
          '--sidebar-w': `${sidebarW}px`,
        } as React.CSSProperties
      }
    >
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
            className={view === 'circuit' ? 'vtab vtab-on' : 'vtab'}
            onClick={() => {
              setView('circuit')
              setFocus(null)
            }}
          >
            dbsp circuit
          </button>
        </div>

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
          <button className="btn btn-icon" title="Refresh" onClick={() => void load()}>
            ↻
          </button>
          <button
            className="btn btn-icon btn-danger"
            disabled={!graph || graph.shapes.length === 0}
            title="Delete all shapes (shared feeds are swept until their refcounts drain)"
            onClick={() => void deleteAll()}
          >
            🗑
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

        <div className="logsec">
          <button className="logsec-h" onClick={() => setLogOpen((o) => !o)} title="Recent replicated changes — click one to replay its animation">
            <span>{logOpen ? '▾' : '▸'} Activity</span>
            {log.length > 0 ? <span className="logsec-count">{log.length}</span> : null}
          </button>
          {logOpen ? (
            <div className="loglist">
              {log.length === 0 ? (
                <div className="log-empty">No replicated changes seen yet — write to Postgres and they land here.</div>
              ) : (
                log.map((e) => {
                  const s = logSummary(e.ev)
                  return (
                    <button
                      key={e.key}
                      className="log-row"
                      title="Replay this change's animation on the canvas"
                      onClick={() => applyEventDecor(e.ev)}
                    >
                      <span className="log-time">{new Date(e.at).toLocaleTimeString()}</span>
                      <span className="log-table">{e.ev.table}</span>
                      <span className={`log-op ${s.cls}`}>{s.op}</span>
                      <span className="log-hint">{s.hint}</span>
                    </button>
                  )
                })
              )}
            </div>
          ) : null}
        </div>

        {view === 'logical' ? (
          <div className="legend">
            <span className="lg lg-table">table · Δ source</span>
            <span className="lg lg-filter">σ filter</span>
            <span className="lg lg-family">↦⋈ route join</span>
            <span className="lg lg-sqnode">IN-set arrange</span>
            <span className="lg lg-agg">Σ fold</span>
            <span className="lg lg-shape">shape out</span>
          </div>
        ) : (
          <div className="legend">
            <span className="lg lg-table">source</span>
            <span className="lg lg-delta">Δ change</span>
            <span className="lg lg-filter">σ filter</span>
            <span className="lg lg-index">↦ key</span>
            <span className="lg lg-sqnode">arrange (state)</span>
            <span className="lg lg-join">⋈ join</span>
            <span className="lg lg-agg">Σ fold</span>
            <span className="lg lg-shape">π · sink</span>
          </div>
        )}

        <button className="sidebar-collapse" title="Collapse sidebar" onClick={() => setSidebarOpen(false)}>
          ☰
        </button>
        <div className="sidebar-resize" title="drag to resize" onMouseDown={startResize} />
      </aside>

      {!sidebarOpen ? (
        <button className="sidebar-reopen" title="Open sidebar" onClick={() => setSidebarOpen(true)}>
          ☰
        </button>
      ) : null}

      <main className="canvas">
        {mode === 'select' && selected.size === 0 ? (
          <div className="empty">Select one or more shapes to see their maintained pipeline.</div>
        ) : view === 'circuit' && graph && !graph.operators ? (
          <div className="empty">
            This engine doesn&apos;t emit the operator decomposition (<code>/graph</code> has no{' '}
            <code>operators</code>) — restart it with the current build to use the circuit view.
          </div>
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
            <MiniMap position="bottom-right" pannable zoomable nodeStrokeWidth={2} />
            <Controls position="bottom-right" />
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
