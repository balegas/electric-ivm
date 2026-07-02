// The right pane — live results. One card per shape: scrubbed predicate, the live result table
// (or scalar for aggregations), and a collapsible "API request" block showing the exact POST
// /shapes body that created it. No app metaphors: a shape's card IS its result set.

import { useMemo, useState } from 'react'

import { predicateLabel } from '@viz/predicate-label'
import type { Predicate } from '@viz/types'

import type { PlaygroundShape } from '../shared/types.ts'
import { scrubPredicate, scrubRow } from './scrub.ts'
import { useShapeRows } from './useShapeRows.ts'

function ApiRequest({ shape, underHood }: { shape: PlaygroundShape; underHood: boolean }) {
  const [open, setOpen] = useState(false)
  const body = useMemo(() => {
    const where = underHood ? shape.where : scrubPredicate(shape.where as Predicate)
    const req: Record<string, unknown> = { table: shape.spec.table, ...(where ? { where } : {}) }
    if (shape.spec.aggregate) {
      req.fn = shape.spec.aggregate.func
      if (shape.spec.aggregate.col) req.col = shape.spec.aggregate.col
    }
    const path = shape.spec.aggregate ? '/aggregate' : '/shapes'
    return `POST ${path}\n${JSON.stringify(req, null, 2)}`
  }, [shape, underHood])
  return (
    <>
      <button className="device-feed-toggle" onClick={() => setOpen((o) => !o)}>
        {open ? '▾ hide' : '▸ API request'}
      </button>
      {open ? <pre className="api-block">{body}</pre> : null}
    </>
  )
}

function ShapeCard({
  workspaceId,
  shape,
  tick,
  underHood,
  onDelete,
}: {
  workspaceId: string | undefined
  shape: PlaygroundShape
  tick: number
  underHood: boolean
  onDelete?: (() => void) | undefined
}) {
  const { rows, changedAt, error } = useShapeRows(workspaceId, shape.id, tick)
  const isAgg = !!shape.spec.aggregate
  const flash = Date.now() - changedAt < 1200

  const payload = isAgg ? (rows[0]?.value as { value?: unknown } | undefined) : undefined
  const scalar = payload && 'value' in payload ? payload.value : null
  const scalarText =
    scalar === null || scalar === undefined
      ? '—'
      : typeof scalar === 'number' && !Number.isInteger(scalar)
        ? scalar.toFixed(2)
        : String(scalar)

  const pred = underHood ? (shape.where as Predicate) : scrubPredicate(shape.where as Predicate)

  return (
    <div className={`device${flash ? ' device-flash' : ''}`}>
      <div className="device-h">
        <span className="device-title">{shape.label}</span>
        <span className="device-tools">
          <span className="device-id">{shape.id}</span>
          {onDelete ? (
            <button className="device-del" title="Delete this shape" onClick={onDelete}>
              🗑
            </button>
          ) : null}
        </span>
      </div>
      <div className="device-pred" title="The shape's predicate">
        {shape.spec.aggregate
          ? `${shape.spec.aggregate.func.toUpperCase()}(${shape.spec.aggregate.col ?? '*'}) WHERE ${pred ? predicateLabel(pred) : 'match all'}`
          : pred
            ? predicateLabel(pred)
            : 'match all'}
      </div>
      {error ? <div className="device-err">{error}</div> : null}
      {isAgg ? (
        <div className="device-scalar" title="SQL semantics: an empty SUM/AVG/MIN/MAX is NULL (—)">
          {scalarText}
        </div>
      ) : (
        <div className="device-rows">
          {rows.length === 0 ? <div className="device-empty">no rows match</div> : null}
          {rows.slice(0, 8).map((r) => {
            const v = scrubRow(r.value) as { title?: string; status?: string; priority?: number; name?: string; team?: string }
            return (
              <div key={r.key} className="device-row">
                {v.title ? (
                  <>
                    <span>{v.title}</span>
                    <span className="device-row-r">
                      {v.status} · P{v.priority}
                    </span>
                  </>
                ) : (
                  <span>{v.name ? `${v.name} · ${v.team}` : JSON.stringify(v)}</span>
                )}
              </div>
            )
          })}
          {rows.length > 8 ? <div className="device-more">+{rows.length - 8} more</div> : null}
        </div>
      )}
      <ApiRequest shape={shape} underHood={underHood} />
    </div>
  )
}

export function ShapeCards({
  workspaceId,
  shapes,
  tick,
  underHood,
  deleteShape,
}: {
  workspaceId: string | undefined
  shapes: PlaygroundShape[]
  tick: number
  underHood: boolean
  deleteShape: (id: string) => void
}) {
  return (
    <div className="devices">
      <div className="devices-h">
        Live results <span className="devices-sub">one card per shape — maintained, never re-queried</span>
      </div>
      {shapes.length === 0 ? (
        <div className="device-empty">No shapes yet — open scene 1 below to create your first one.</div>
      ) : null}
      {shapes.map((s) => (
        <ShapeCard
          key={s.id}
          workspaceId={workspaceId}
          shape={s}
          tick={tick}
          underHood={underHood}
          onDelete={s.scene === null ? () => deleteShape(s.id) : undefined}
        />
      ))}
    </div>
  )
}
