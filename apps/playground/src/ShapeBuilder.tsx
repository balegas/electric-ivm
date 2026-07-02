// Guided shape builder: everything the engine supports for this schema, nothing it doesn't.
// Compose conjuncts (column/op/value), optionally a subquery clause, optionally an aggregation.
// The server appends the workspace conjunct — the preview shows the full honest predicate.

import { useMemo, useState } from 'react'

import { predicateLabel } from '@viz/predicate-label'
import type { Predicate } from '@viz/types'

import type { DeviceRole, ShapeSpec } from '../shared/types.ts'

const COLUMNS: Record<'orders' | 'restaurants', { col: string; type: 'text' | 'number'; values?: string[] }[]> = {
  orders: [
    { col: 'status', type: 'text', values: ['new', 'cooking', 'riding', 'delivered', 'cancelled'] },
    { col: 'total', type: 'number' },
    { col: 'dish', type: 'text' },
  ],
  restaurants: [
    { col: 'city', type: 'text', values: ['Lisbon', 'Porto', 'Faro'] },
    { col: 'name', type: 'text' },
  ],
}

const OPS = ['eq', 'neq', 'lt', 'lte', 'gt', 'gte'] as const

interface Conjunct {
  col: string
  op: (typeof OPS)[number]
  value: string
}

export function ShapeBuilder({
  onCreate,
  onClose,
}: {
  onCreate: (spec: ShapeSpec, label: string, role: DeviceRole) => void
  onClose: () => void
}) {
  const [table, setTable] = useState<'orders' | 'restaurants'>('orders')
  const [conjuncts, setConjuncts] = useState<Conjunct[]>([{ col: 'status', op: 'eq', value: 'cooking' }])
  const [withSubquery, setWithSubquery] = useState(false)
  const [subqueryCity, setSubqueryCity] = useState('Lisbon')
  const [withAgg, setWithAgg] = useState(false)
  const [aggFunc, setAggFunc] = useState<'count' | 'sum' | 'avg' | 'min' | 'max'>('count')
  const [label, setLabel] = useState('')

  const spec = useMemo((): ShapeSpec => {
    const cols = COLUMNS[table]
    const where = conjuncts
      .filter((c) => c.value !== '')
      .map((c) => ({
        col: c.col,
        op: c.op,
        value: cols.find((x) => x.col === c.col)?.type === 'number' ? Number(c.value) : c.value,
      }))
    const s: ShapeSpec = { table, where }
    if (withSubquery && table === 'orders') {
      s.subquery = {
        col: 'restaurant_id',
        inner: { table: 'restaurants', project: 'id', where: [{ col: 'city', op: 'eq', value: subqueryCity }] },
      }
    }
    if (withAgg) {
      s.aggregate = { func: aggFunc, col: aggFunc === 'count' ? null : 'total' }
    }
    return s
  }, [table, conjuncts, withSubquery, subqueryCity, withAgg, aggFunc])

  // Preview mirrors the server's composition (shared/scenes stays the source of truth for scenes;
  // this is just display).
  const preview = useMemo(() => {
    const parts: Predicate[] = spec.where.map((c) => c as unknown as Predicate)
    if (spec.subquery) {
      parts.push({
        col: spec.subquery.col,
        in: {
          table: spec.subquery.inner.table,
          project: spec.subquery.inner.project,
          where: {
            and: [
              ...(spec.subquery.inner.where as unknown as Predicate[]),
              { col: 'workspace_id', op: 'eq', value: '<you>' },
            ],
          } as Predicate,
        },
      } as Predicate)
    }
    parts.push({ col: 'workspace_id', op: 'eq', value: '<you>' } as Predicate)
    return predicateLabel(parts.length === 1 ? parts[0]! : ({ and: parts } as Predicate))
  }, [spec])

  const cols = COLUMNS[table]

  return (
    <div className="modal-back" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal-h">Create a shape</div>

        <label className="fld">
          table
          <select
            value={table}
            onChange={(e) => {
              const t = e.target.value as 'orders' | 'restaurants'
              setTable(t)
              setConjuncts([{ col: COLUMNS[t][0]!.col, op: 'eq', value: '' }])
              if (t !== 'orders') setWithSubquery(false)
            }}
          >
            <option>orders</option>
            <option>restaurants</option>
          </select>
        </label>

        {conjuncts.map((c, i) => {
          const def = cols.find((x) => x.col === c.col)
          return (
            <div key={i} className="conjunct">
              <select
                value={c.col}
                onChange={(e) => setConjuncts((cs) => cs.map((x, j) => (j === i ? { ...x, col: e.target.value, value: '' } : x)))}
              >
                {cols.map((x) => (
                  <option key={x.col}>{x.col}</option>
                ))}
              </select>
              <select
                value={c.op}
                onChange={(e) => setConjuncts((cs) => cs.map((x, j) => (j === i ? { ...x, op: e.target.value as Conjunct['op'] } : x)))}
              >
                {OPS.map((o) => (
                  <option key={o}>{o}</option>
                ))}
              </select>
              {def?.values ? (
                <select
                  value={c.value}
                  onChange={(e) => setConjuncts((cs) => cs.map((x, j) => (j === i ? { ...x, value: e.target.value } : x)))}
                >
                  <option value="">—</option>
                  {def.values.map((v) => (
                    <option key={v}>{v}</option>
                  ))}
                </select>
              ) : (
                <input
                  value={c.value}
                  placeholder={def?.type === 'number' ? '20' : 'value'}
                  onChange={(e) => setConjuncts((cs) => cs.map((x, j) => (j === i ? { ...x, value: e.target.value } : x)))}
                />
              )}
              <button className="mini" onClick={() => setConjuncts((cs) => cs.filter((_, j) => j !== i))}>
                ✕
              </button>
            </div>
          )
        })}
        <button className="mini" onClick={() => setConjuncts((cs) => [...cs, { col: cols[0]!.col, op: 'eq', value: '' }])}>
          ＋ condition
        </button>

        {table === 'orders' ? (
          <label className="fld fld-check">
            <input type="checkbox" checked={withSubquery} onChange={(e) => setWithSubquery(e.target.checked)} />
            restaurant_id IN (SELECT id FROM restaurants WHERE city =
            <select value={subqueryCity} onChange={(e) => setSubqueryCity(e.target.value)}>
              {['Lisbon', 'Porto', 'Faro'].map((c) => (
                <option key={c}>{c}</option>
              ))}
            </select>
            )
          </label>
        ) : null}

        <label className="fld fld-check">
          <input type="checkbox" checked={withAgg} onChange={(e) => setWithAgg(e.target.checked)} />
          aggregate
          {withAgg ? (
            <select value={aggFunc} onChange={(e) => setAggFunc(e.target.value as typeof aggFunc)}>
              {['count', 'sum', 'avg', 'min', 'max'].map((f) => (
                <option key={f}>{f}</option>
              ))}
            </select>
          ) : null}
          {withAgg && aggFunc !== 'count' ? <span className="fld-note">over total</span> : null}
        </label>

        <label className="fld">
          label
          <input value={label} placeholder="My shape" onChange={(e) => setLabel(e.target.value)} />
        </label>

        <div className="preview">
          <span className="preview-l">the engine will maintain</span>
          SELECT {withAgg ? `${aggFunc.toUpperCase()}(${aggFunc === 'count' ? '*' : 'total'})` : '*'} FROM {table}{' '}
          WHERE {preview}
        </div>

        <div className="modal-actions">
          <button className="mini" onClick={onClose}>
            cancel
          </button>
          <button
            className="primary"
            onClick={() => {
              onCreate(spec, label || 'Custom shape', withAgg ? 'dashboard' : 'custom')
              onClose()
            }}
          >
            Create shape
          </button>
        </div>
      </div>
    </div>
  )
}
