// The center pane: the workspace's maintained pipeline (reusing pipeline-viz's graph builders and
// node renderer), decorated live by trace events — travelling delta dots on edges, pass/drop/fold
// flashes on nodes, faint gray pulses for other visitors' traffic through shared nodes.

import { Background, Controls, ReactFlow, type Edge, type Node, type NodeProps } from '@xyflow/react'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import { buildDbspGraph } from '@viz/build-dbsp'
import { buildGraph, type BuildOpts, type NodeRef, type VizNodeData } from '@viz/build-graph'
import type { EngineGraph } from '@viz/types'

import { PipelineNode } from './nodes.tsx'

import type { TraceEvent } from '../shared/types.ts'
import { DetailPanel } from './DetailPanel.tsx'
import { edgeTypes, type PulseEdgeData } from './edges.tsx'
import { scrubText } from './scrub.ts'
import { eventDecor, mergeDecor, type Decor, type FlashKind } from './trace-anim.ts'
import { useTrace } from './useTrace.ts'

export type View = 'logical' | 'dbsp'

const DECOR_TTL_MS = 1100

/** Node wrapper adding the flash overlay around pipeline-viz's renderer. */
function FlashNode(props: NodeProps) {
  const d = props.data as VizNodeData & { flash?: FlashKind }
  return (
    <div className={d.flash ? `flash flash-${d.flash}` : undefined}>
      {d.flash === 'drop' ? <span className="flash-x">✕ dropped</span> : null}
      <PipelineNode {...props} />
    </div>
  )
}
const nodeTypes = { pipeline: FlashNode }

export function PipelineCanvas({
  workspaceId,
  graph,
  mine,
  view,
  underHood,
  onViewChange,
}: {
  workspaceId: string | undefined
  graph: EngineGraph | null
  mine: string[]
  view: View
  /** Reveal the multi-tenancy plumbing (full predicates, shared badges, foreign pulses). */
  underHood: boolean
  onViewChange: (v: View) => void
}) {
  const [decor, setDecor] = useState<Decor | null>(null)
  const decorTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
  // Click-to-inspect: the focused node's id (for connection highlighting) and its entity ref.
  const [focus, setFocus] = useState<{ id: string; ref: NodeRef } | null>(null)

  const { nodes, edges } = useMemo<{ nodes: Node[]; edges: Edge[] }>(() => {
    if (!graph || mine.length === 0) return { nodes: [], edges: [] }
    const sel = new Set(mine)
    const f = focus?.id ?? null
    // Uniform node widths + heights sized to the FULL (wrapped) query text, measured on what will
    // actually be displayed (scrubbed unless under-the-hood).
    const opts: BuildOpts = {
      measure: (d) => {
        const label = underHood ? d.label : scrubText(d.label)
        const sub = d.sub ? (underHood ? d.sub : scrubText(d.sub)) : ''
        if (d.kind === 'table' || d.kind === 'source') return { w: 160, h: 52 }
        const w = 260
        const charsPerLine = Math.floor((w - 26) / 7.3) // 12px ui-monospace ≈ 7.3px/char
        const lines = Math.max(1, Math.ceil(label.length / charsPerLine))
        const subLines = sub ? Math.max(1, Math.ceil(sub.length / charsPerLine)) : 0
        return { w, h: 30 + lines * 17 + subLines * 14 + (d.kind === 'shape' ? 8 : 4) }
      },
    }
    opts.alignSources = true
    return view === 'dbsp' ? buildDbspGraph(graph, sel, f, opts) : buildGraph(graph, sel, f, opts)
  }, [graph, mine, view, focus, underHood])

  // Refs so the trace callback maps events against the CURRENT render without re-subscribing.
  const edgesRef = useRef(edges)
  edgesRef.current = edges
  const presentRef = useRef(new Set<string>())
  presentRef.current = useMemo(() => new Set(nodes.map((n) => n.id)), [nodes])
  const viewRef = useRef(view)
  viewRef.current = view
  const hoodRef = useRef(underHood)
  hoodRef.current = underHood

  const onTrace = useCallback((ev: TraceEvent) => {
    // Foreign pulses are an "under the hood" lesson — hidden while scoping is silent.
    if (!ev.yours && !hoodRef.current) return
    const d = eventDecor(ev, viewRef.current, edgesRef.current, presentRef.current)
    if (d.nodes.size === 0 && d.edges.size === 0) return
    setDecor((prev) => mergeDecor(prev, d))
    if (decorTimer.current) clearTimeout(decorTimer.current)
    decorTimer.current = setTimeout(() => setDecor(null), DECOR_TTL_MS)
  }, [])
  useTrace(workspaceId, onTrace)
  useEffect(() => () => {
    if (decorTimer.current) clearTimeout(decorTimer.current)
  }, [])

  const decorated = useMemo(() => {
    const dn = nodes.map((n) => {
      let d = n.data as VizNodeData & { flash?: FlashKind }
      // Display-only scrub: node ids stay real (trace animation matches); labels hide the
      // workspace conjunct and the cross-tenant shared badge unless under-the-hood is on.
      if (!underHood) {
        d = {
          ...d,
          label: scrubText(d.label),
          sub: d.sub ? scrubText(d.sub) : d.sub,
          shared: undefined,
          index: d.index ? scrubText(d.index) : d.index,
        }
        // Router counts would leak other tenants' shape counts — recompute from YOUR shapes.
        if ((n.id.startsWith('family:') || n.id.startsWith('pa:') || n.id.startsWith('j:')) && graph) {
          const mineSet = new Set(mine)
          const key = n.id.replace(/^(family:|pa:|j:)/, '')
          const members = graph.shapes.filter(
            (sh) => mineSet.has(sh.id) && sh.familyKey && `${sh.table}:${sh.familyKey.join(',')}` === key,
          ).length
          d = {
            ...d,
            index: `${members} ${members === 1 ? 'key' : 'keys'}`,
            sub: members > 1 ? `shared by ${members} shapes` : undefined,
          }
        }
      }
      if (decor?.nodes.has(n.id)) d = { ...d, flash: decor.nodes.get(n.id) }
      return d === n.data ? n : { ...n, data: d }
    })
    const de = edges.map((e) => {
      // The pulse keeps the id of the event that created it — re-rendering after a merge must not
      // restart dots already in flight on other edges.
      const data: PulseEdgeData = { pulse: decor?.edges.get(e.id), baseStyle: e.style }
      return { ...e, type: 'pulse', data, style: data.pulse ? undefined : e.style }
    })
    return { nodes: dn, edges: de }
  }, [nodes, edges, decor, underHood])

  return (
    <div className="canvas">
      <div className="viewtoggle">
        <button
          className={view === 'logical' ? 'on' : ''}
          onClick={() => {
            onViewChange('logical')
            setFocus(null)
          }}
        >
          Logical
        </button>
        <button
          className={view === 'dbsp' ? 'on' : ''}
          onClick={() => {
            onViewChange('dbsp')
            setFocus(null)
          }}
        >
          dbsp circuit
        </button>
      </div>
      {decorated.nodes.length === 0 ? (
        <div className="canvas-empty">
          <div className="canvas-empty-t">Nothing is syncing yet</div>
          <div>This pane will show the engine's pipeline. Open scene 1 below to create your first live query.</div>
        </div>
      ) : (
        <ReactFlow
          nodes={decorated.nodes}
          edges={decorated.edges}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          fitView
          minZoom={0.15}
          onNodeClick={(_e, n) => setFocus({ id: n.id, ref: (n.data as VizNodeData).ref })}
          onPaneClick={() => setFocus(null)}
          proOptions={{ hideAttribution: true }}
        >
          <Background gap={20} color="#eef2f7" />
          <Controls />
        </ReactFlow>
      )}
      {focus && graph && workspaceId ? (
        <DetailPanel
          node={focus.ref}
          graph={graph}
          workspaceId={workspaceId}
          mine={mine}
          onClose={() => setFocus(null)}
          onSelectShape={(id) => {
            const nid = view === 'dbsp' ? `snk:${id}` : `shape:${id}`
            const ref: NodeRef = graph.shapes.find((s) => s.id === id)?.aggregate
              ? { kind: 'aggshape', shapeId: id }
              : { kind: 'shape', shapeId: id }
            setFocus({ id: nid, ref })
          }}
        />
      ) : null}
    </div>
  )
}
