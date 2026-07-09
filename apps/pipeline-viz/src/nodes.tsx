import { Handle, Position, type NodeProps } from '@xyflow/react'

import type { VizNodeData } from './build-graph'
import { useLatestDelta } from './delta-store'
import { KIND_META, fmtScalar } from './node-meta'
import { useNodeState } from './state-store'
import type { NodeStateSummary } from './types'

/** Inline Z-set peek on a Δ change operator: the weights of the most recent change on its table
 *  (`+1` / `−1`, colored), a compact echo of what the detail panel spells out in full. Empty
 *  until the first change on the table. */
function DeltaPeek({ table }: { table: string }) {
  const cap = useLatestDelta(table)
  if (!cap) return <div className="pnode-state pnode-state-empty">—</div>
  return (
    <div className="pnode-state pnode-delta" title="most recent Z-set delta (weights)">
      {cap.rows.map((r, i) => (
        <span key={i} className={`chip pnode-zw ${r.w > 0 ? 'pnode-zw-pos' : 'pnode-zw-neg'}`}>
          {r.w > 0 ? `+${r.w}` : `−${Math.abs(r.w)}`}
        </span>
      ))}
    </div>
  )
}

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

// The indigo of the compiled dbsp arrangement lane (KIND_META['arr-index']): a source node whose
// arrangements are folded onto it borrows this treatment so "indexed" reads at a glance.
const ARR_COLOR = '#4338ca'
const ARR_BG = '#e0e7ff'

export function PipelineNode({ id, data }: NodeProps) {
  const d = data as VizNodeData
  const meta = KIND_META[d.kind]
  const parked = d.life === 'dormant' || d.life === 'deactivating' || d.life === 'reactivating'
  // A table source with compiled arrangements folded onto it: indigo treatment + a count badge,
  // standing in for the (decluttered-away) arrangement lane. The detail panel expands the list.
  const indexed = d.kind === 'op-source' && d.arr && d.arr.indexes + d.arr.counts > 0 ? d.arr : null
  const color = indexed ? ARR_COLOR : meta.color
  return (
    <div
      className={`pnode pnode-${d.kind}${indexed ? ' pnode-indexed' : ''}${d.stack ? ' pnode-stacked' : ''}${d.selected ? ' pnode-selected' : ''}${d.dimmed ? ' pnode-dimmed' : ''}${parked ? ' pnode-parked' : ''}`}
      style={{ borderColor: color, background: indexed ? ARR_BG : meta.bg }}
    >
      <Handle type="target" position={Position.Left} />
      <div className="pnode-tag" style={{ color }}>
        <span>
          {meta.tag}
          {d.idTag ? <span className="pnode-idtag">{d.idTag}</span> : null}
        </span>
        <span className="pnode-tag-r">
          {indexed ? (
            <span
              className="pnode-arr"
              title={`compiled dbsp arrangements on this table — click the source to see the ${indexed.indexes} index${indexed.indexes === 1 ? '' : 'es'}${indexed.counts ? ` and ${indexed.counts} counts pipeline${indexed.counts === 1 ? '' : 's'}` : ''}. ${indexed.seeded ? 'seeded' : 'seeding…'}`}
            >
              ⧉ {indexed.indexes} idx{indexed.counts ? ` · ${indexed.counts} cnt` : ''}
            </span>
          ) : null}
          {parked ? <span className={`pnode-life pnode-life-${d.life}`}>{d.life}</span> : null}
          {d.serve ? (
            <span
              className="pnode-serve"
              title="circuit-served — this shape's data is seeded and maintained by the dbsp circuit"
            >
              circuit · {d.serve}
            </span>
          ) : null}
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
      {/* The Δ change operator (d:<t>) holds no state chip — show its latest Z-set delta instead. */}
      {d.kind === 'op-delta' && id.startsWith('d:') ? <DeltaPeek table={id.slice('d:'.length)} /> : null}
      <Handle type="source" position={Position.Right} />
    </div>
  )
}

export const nodeTypes = { pipeline: PipelineNode }
