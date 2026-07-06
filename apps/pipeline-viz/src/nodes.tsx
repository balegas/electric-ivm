import { Handle, Position, type NodeProps } from '@xyflow/react'

import type { VizNodeData } from './build-graph'
import { KIND_META, fmtScalar } from './node-meta'
import { useNodeState } from './state-store'
import type { NodeStateSummary } from './types'

/** The live state row of one node card. Chips come straight from the engine's state summaries
 *  (seeded by `GET /state`, updated by SSE `state` events) — each card subscribes to its own node
 *  id, so state ticks re-render only the touched chips, never the graph. */
function StateChips({ id }: { id: string }) {
  const s = useNodeState(id)
  if (!s) return <div className="pnode-state pnode-state-empty">—</div>
  return <div className="pnode-state">{chips(s)}</div>
}

function chips(s: NodeStateSummary): React.ReactNode {
  switch (s.kind) {
    case 'table':
      return (
        <>
          <span className="chip" title="table-stream envelopes processed since start">
            {s.envelopes.toLocaleString()} env
          </span>
          <span className="chip chip-dim" title="processed offset (the convergence barrier)">
            @{s.processedOffset}
          </span>
        </>
      )
    case 'filter':
      return (
        <span className="chip" title="envelopes this filter has emitted downstream">
          {s.emitted.toLocaleString()} out
        </span>
      )
    case 'family':
      return (
        <>
          <span className="chip chip-state" title="distinct key tuples in the routing index">
            {s.keys.toLocaleString()} {s.keys === 1 ? 'key' : 'keys'}
          </span>
          <span className="chip" title="shapes registered across those keys">
            {s.shapes.toLocaleString()} {s.shapes === 1 ? 'shape' : 'shapes'}
          </span>
        </>
      )
    case 'shape':
      return (
        <span className="chip" title="envelopes appended to this shape stream (backfill + live)">
          {s.emitted.toLocaleString()} env
        </span>
      )
    case 'aggregate':
      return (
        <>
          <span className="chip chip-value" title="current aggregate value (live)">
            = {fmtScalar(s.value)}
          </span>
          <span className="chip chip-dim" title="matching rows (Σ of Z-set weights)">
            n={s.count.toLocaleString()}
          </span>
          {s.multisetLen > 0 ? (
            <span className="chip chip-state" title="values held in the MIN/MAX retraction multiset">
              {s.multisetLen.toLocaleString()} in multiset
            </span>
          ) : null}
        </>
      )
    case 'subqueryNode':
      return (
        <>
          <span className="chip chip-state" title="distinct values in the maintained inner set">
            {s.distinctValues.toLocaleString()} {s.distinctValues === 1 ? 'value' : 'values'}
          </span>
          <span className="chip" title="dependents referencing this node">
            ref {s.refcount}
          </span>
        </>
      )
  }
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
          {d.shared && d.shared > 1 ? <span className="pnode-shared">shared ×{d.shared}</span> : null}
        </span>
      </div>
      <div className={`pnode-label${d.highlight ? ' pnode-highlight' : ''}`} title={d.label}>
        {d.label}
      </div>
      {/* Operators carry their dbsp formula as the sub line; logical nodes their own sub text. */}
      {d.sub ?? (d.kind.startsWith('op-') ? meta.formula : undefined) ? (
        <div className="pnode-sub" title={d.sub ?? meta.formula}>
          {d.sub ?? meta.formula}
        </div>
      ) : null}
      {d.stateId ? <StateChips id={d.stateId} /> : null}
      <Handle type="source" position={Position.Right} />
    </div>
  )
}

export const nodeTypes = { pipeline: PipelineNode }
