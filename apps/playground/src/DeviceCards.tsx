// The right pane — the subscribers. One device card per shape: a small "app" (kitchen screen,
// rider phone, dashboard tile…) whose contents ARE the shape's live result set. The predicate is
// shown honestly — workspace_id conjunct and all. Expand for the raw upsert/delete feed.

import { useState } from 'react'

import { predicateLabel } from '@viz/predicate-label'
import type { Predicate } from '@viz/types'

import type { PlaygroundShape } from '../shared/types.ts'
import { useShapeRows } from './useShapeRows.ts'

const ROLE_META: Record<string, { icon: string; chrome: string }> = {
  orders: { icon: '📋', chrome: 'device-list' },
  kitchen: { icon: '🍳', chrome: 'device-kitchen' },
  rider: { icon: '🛵', chrome: 'device-phone' },
  customer: { icon: '🧑‍🍳', chrome: 'device-list' },
  dashboard: { icon: '📈', chrome: 'device-tile' },
  custom: { icon: '🧩', chrome: 'device-list' },
}

function DeviceCard({
  workspaceId,
  shape,
  tick,
  onDelete,
}: {
  workspaceId: string | undefined
  shape: PlaygroundShape
  tick: number
  onDelete?: (() => void) | undefined
}) {
  const { rows, feed, changedAt, error } = useShapeRows(workspaceId, shape.id, tick)
  const [open, setOpen] = useState(false)
  const meta = ROLE_META[shape.role] ?? ROLE_META.custom!
  const isAgg = !!shape.spec.aggregate
  const flash = Date.now() - changedAt < 1200

  // An aggregation stream materializes to a single `{ value, n }` row; `value` is SQL-null for an
  // empty SUM/AVG/MIN/MAX (no fallback to the envelope — that renders "[object Object]").
  const payload = isAgg ? (rows[0]?.value as { value?: unknown } | undefined) : undefined
  const scalar = payload && 'value' in payload ? payload.value : null
  const scalarText =
    scalar === null || scalar === undefined
      ? '—'
      : typeof scalar === 'number' && !Number.isInteger(scalar)
        ? scalar.toFixed(2)
        : String(scalar)

  return (
    <div className={`device ${meta.chrome}${flash ? ' device-flash' : ''}`}>
      <div className="device-h">
        <span className="device-title">
          {meta.icon} {shape.label}
        </span>
        <span className="device-tools">
          <span className="device-id">{shape.id}</span>
          {onDelete ? (
            <button className="device-del" title="Delete this shape" onClick={onDelete}>
              🗑
            </button>
          ) : null}
        </span>
      </div>
      <div className="device-pred" title="The exact predicate the engine maintains — workspace conjunct included">
        {predicateLabel(shape.where as Predicate)}
      </div>
      {error ? <div className="device-err">{error}</div> : null}
      {isAgg ? (
        <div className="device-scalar" title="SQL semantics: an empty SUM/AVG/MIN/MAX is NULL (—)">
          {scalarText}
        </div>
      ) : (
        <div className="device-rows">
          {rows.length === 0 ? <div className="device-empty">no rows in this shape</div> : null}
          {rows.slice(0, 8).map((r) => {
            const v = r.value as { dish?: string; total?: number; status?: string; name?: string; city?: string }
            return (
              <div key={r.key} className="device-row">
                {v.dish ? (
                  <>
                    <span>{v.dish}</span>
                    <span className="device-row-r">
                      {v.status} · €{Number(v.total ?? 0).toFixed(2)}
                    </span>
                  </>
                ) : (
                  <span>{v.name ? `${v.name} · ${v.city}` : JSON.stringify(r.value)}</span>
                )}
              </div>
            )
          })}
          {rows.length > 8 ? <div className="device-more">+{rows.length - 8} more</div> : null}
        </div>
      )}
      <button className="device-feed-toggle" onClick={() => setOpen((o) => !o)}>
        {open ? '▾ hide' : '▸ raw feed'} ({feed.length})
      </button>
      {open ? (
        <div className="device-feed">
          {feed.length === 0 ? <div className="device-empty">no messages yet — make a change</div> : null}
          {feed.map((f, i) => (
            <div key={`${f.key}-${f.at}-${i}`} className={`feed-${f.kind}`}>
              <b>{f.kind}</b> {f.key}
              {f.value ? ` ${JSON.stringify(f.value).slice(0, 80)}` : ''}
            </div>
          ))}
        </div>
      ) : null}
    </div>
  )
}

export function DeviceCards({
  workspaceId,
  shapes,
  tick,
  deleteShape,
}: {
  workspaceId: string | undefined
  shapes: PlaygroundShape[]
  tick: number
  deleteShape: (id: string) => void
}) {
  return (
    <div className="devices">
      <div className="devices-h">
        Subscribers <span className="devices-sub">each card is a live shape</span>
      </div>
      {shapes.length === 0 ? (
        <div className="device-empty">No live queries yet — open scene 1 below to sync your first one.</div>
      ) : null}
      {shapes.map((s) => (
        <DeviceCard
          key={s.id}
          workspaceId={workspaceId}
          shape={s}
          tick={tick}
          onDelete={s.scene === null ? () => deleteShape(s.id) : undefined}
        />
      ))}
    </div>
  )
}
