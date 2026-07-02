// The center pane: the workspace's maintained pipeline (reusing pipeline-viz's graph builders and
// node renderer), decorated live by trace events — travelling delta dots on edges, pass/drop/fold
// flashes on nodes, faint gray pulses for other visitors' traffic through shared nodes.

import { Background, Controls, ReactFlow, type Edge, type Node, type NodeProps } from '@xyflow/react'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import { buildDbspGraph } from '@viz/build-dbsp'
import { buildGraph, type VizNodeData } from '@viz/build-graph'
import { PipelineNode } from '@viz/nodes'
import type { EngineGraph } from '@viz/types'

import type { TraceEvent } from '../shared/types.ts'
import { edgeTypes, type PulseEdgeData } from './edges.tsx'
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
  onViewChange,
}: {
  workspaceId: string | undefined
  graph: EngineGraph | null
  mine: string[]
  view: View
  onViewChange: (v: View) => void
}) {
  const [decor, setDecor] = useState<Decor | null>(null)
  const decorTimer = useRef<ReturnType<typeof setTimeout> | null>(null)

  const { nodes, edges } = useMemo<{ nodes: Node[]; edges: Edge[] }>(() => {
    if (!graph || mine.length === 0) return { nodes: [], edges: [] }
    const sel = new Set(mine)
    return view === 'dbsp' ? buildDbspGraph(graph, sel) : buildGraph(graph, sel)
  }, [graph, mine, view])

  // Refs so the trace callback maps events against the CURRENT render without re-subscribing.
  const edgesRef = useRef(edges)
  edgesRef.current = edges
  const presentRef = useRef(new Set<string>())
  presentRef.current = useMemo(() => new Set(nodes.map((n) => n.id)), [nodes])
  const viewRef = useRef(view)
  viewRef.current = view

  const onTrace = useCallback((ev: TraceEvent) => {
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
    const dn = nodes.map((n) =>
      decor?.nodes.has(n.id) ? { ...n, data: { ...n.data, flash: decor.nodes.get(n.id) } } : n,
    )
    const de = edges.map((e) => {
      const pulse = decor?.edges.get(e.id)
      const data: PulseEdgeData = { pulse: pulse ? { ...pulse, id: decor!.id } : undefined, baseStyle: e.style }
      return { ...e, type: 'pulse', data, style: pulse ? undefined : e.style }
    })
    return { nodes: dn, edges: de }
  }, [nodes, edges, decor])

  return (
    <div className="canvas">
      <div className="viewtoggle">
        <button className={view === 'logical' ? 'on' : ''} onClick={() => onViewChange('logical')}>
          Logical
        </button>
        <button className={view === 'dbsp' ? 'on' : ''} onClick={() => onViewChange('dbsp')}>
          dbsp circuit
        </button>
      </div>
      {decorated.nodes.length === 0 ? (
        <div className="canvas-empty">Your pipeline appears here once the scene creates its shapes.</div>
      ) : (
        <ReactFlow
          nodes={decorated.nodes}
          edges={decorated.edges}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          fitView
          minZoom={0.15}
          proOptions={{ hideAttribution: true }}
        >
          <Background gap={20} color="#eef2f7" />
          <Controls />
        </ReactFlow>
      )}
    </div>
  )
}
