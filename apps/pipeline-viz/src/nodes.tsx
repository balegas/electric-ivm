import { Handle, Position, type NodeProps } from '@xyflow/react'

import type { NodeKind, VizNodeData } from './build-graph'

const KIND_META: Record<NodeKind, { color: string; bg: string; tag: string }> = {
  // logical view
  table: { color: '#334155', bg: '#e2e8f0', tag: 'TABLE' },
  family: { color: '#0369a1', bg: '#e0f2fe', tag: 'FAMILY ROUTER' },
  filter: { color: '#b45309', bg: '#fef3c7', tag: 'FILTER' },
  sqnode: { color: '#7e22ce', bg: '#f3e8ff', tag: 'SUBQUERY NODE' },
  shape: { color: '#166534', bg: '#dcfce7', tag: 'SHAPE OUTPUT' },
  agg: { color: '#0d9488', bg: '#ccfbf1', tag: 'Σ AGGREGATION' },
  // raw dbsp operator view
  source: { color: '#334155', bg: '#e2e8f0', tag: 'Z-SET SOURCE' },
  delta: { color: '#c2410c', bg: '#ffedd5', tag: 'Δ CHANGE' },
  'op-filter': { color: '#b45309', bg: '#fef3c7', tag: 'σ FILTER' },
  'op-index': { color: '#0f766e', bg: '#ccfbf1', tag: '↦ INDEX' },
  'op-arrange': { color: '#7e22ce', bg: '#f3e8ff', tag: 'ARRANGE · STATE' },
  'op-join': { color: '#1d4ed8', bg: '#dbeafe', tag: '⋈ JOIN' },
  'op-map': { color: '#475569', bg: '#e2e8f0', tag: 'π MAP' },
  'op-agg': { color: '#0d9488', bg: '#ccfbf1', tag: 'Σ FOLD · STATE' },
  sink: { color: '#166534', bg: '#dcfce7', tag: 'SINK · shape out' },
}

export function PipelineNode({ data }: NodeProps) {
  const d = data as VizNodeData
  const meta = KIND_META[d.kind]
  return (
    <div
      className={`pnode pnode-${d.kind}${d.selected ? ' pnode-selected' : ''}${d.dimmed ? ' pnode-dimmed' : ''}`}
      style={{ borderColor: meta.color, background: meta.bg }}
    >
      <Handle type="target" position={Position.Left} />
      <div className="pnode-tag" style={{ color: meta.color }}>
        <span>
          {meta.tag}
          {d.idTag ? <span className="pnode-idtag">{d.idTag}</span> : null}
        </span>
        <span className="pnode-tag-r">
          {d.index ? <span className="pnode-index">{d.index}</span> : null}
          {d.shared && d.shared > 1 ? <span className="pnode-shared">shared ×{d.shared}</span> : null}
        </span>
      </div>
      <div className={`pnode-label${d.highlight ? ' pnode-highlight' : ''}`} title={d.label}>
        {d.label}
      </div>
      {d.sub ? (
        <div className="pnode-sub" title={d.sub}>
          {d.sub}
        </div>
      ) : null}
      <Handle type="source" position={Position.Right} />
    </div>
  )
}

export const nodeTypes = { pipeline: PipelineNode }
