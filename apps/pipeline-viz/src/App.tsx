import { Background, Controls, MiniMap, ReactFlow, type Edge, type Node } from '@xyflow/react'
import { useCallback, useEffect, useMemo, useState } from 'react'

import { buildGraph, type NodeRef, type VizNodeData } from './build-graph'
import { buildDbspGraph } from './build-dbsp'
import { DetailPanel } from './DetailPanel'
import { nodeTypes } from './nodes'
import { predicateLabel } from './predicate-label'
import { shapeSql } from './shape-sql'
import type { EngineGraph, GraphShape } from './types'

type Mode = 'all' | 'select'
type View = 'logical' | 'dbsp'

interface Metrics {
  counters: { envelopes_processed: number; shape_appends: number; family_steps: number }
  append_us: { p99_us: number }
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

  const load = useCallback(async () => {
    try {
      const [gr, mr] = await Promise.all([fetch('/engine/graph'), fetch('/engine/metrics')])
      if (!gr.ok) throw new Error(`engine /graph → ${gr.status}`)
      setGraph((await gr.json()) as EngineGraph)
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
    const t = setInterval(() => void load(), 2500)
    return () => clearInterval(t)
  }, [auto, load])

  const { nodes, edges } = useMemo<{ nodes: Node[]; edges: Edge[] }>(() => {
    if (!graph) return { nodes: [], edges: [] }
    if (mode === 'select' && selected.size === 0) return { nodes: [], edges: [] }
    const sel = mode === 'all' ? 'all' : selected
    const f = focus?.id ?? null
    return view === 'dbsp' ? buildDbspGraph(graph, sel, f) : buildGraph(graph, sel, f)
  }, [graph, mode, selected, focus, view])

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
    <div className="app">
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
      </aside>

      <main className="canvas">
        {mode === 'select' && selected.size === 0 ? (
          <div className="empty">Select one or more shapes to see their maintained pipeline.</div>
        ) : (
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            fitView
            minZoom={0.1}
            onNodeClick={(_e, n) => setFocus({ id: n.id, ref: (n.data as VizNodeData).ref })}
            onPaneClick={() => setFocus(null)}
            proOptions={{ hideAttribution: true }}
          >
            <Background gap={20} color="#eef2f7" />
            <MiniMap pannable zoomable nodeStrokeWidth={2} />
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
