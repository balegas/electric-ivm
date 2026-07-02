// The shape composer: everything the Shape API supports for this schema, nothing it doesn't.
// Compose conjuncts (column/op/value), optionally an IN-subquery, optionally an aggregation. The
// preview shows the API request that will be sent (workspace scoping is applied silently
// server-side and doesn't appear here).

import { useMemo, useState } from 'react'

import type { ShapeSpec } from '../shared/types.ts'

const COLUMNS: Record<'issues' | 'projects', { col: string; type: 'text' | 'number'; values?: string[] }[]> = {
  issues: [
    { col: 'status', type: 'text', values: ['todo', 'in_progress', 'done'] },
    { col: 'priority', type: 'number', values: ['1', '2', '3', '4'] },
    { col: 'title', type: 'text' },
  ],
  projects: [
    { col: 'team', type: 'text', values: ['web', 'mobile', 'infra'] },
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
  onCreate: (spec: ShapeSpec, label: string) => void
  onClose: () => void
}) {
  const [table, setTable] = useState<'issues' | 'projects'>('issues')
  const [conjuncts, setConjuncts] = useState<Conjunct[]>([{ col: 'status', op: 'eq', value: 'todo' }])
  const [withSubquery, setWithSubquery] = useState(false)
  const [subqueryTeam, setSubqueryTeam] = useState('web')
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
    if (withSubquery && table === 'issues') {
      s.subquery = {
        col: 'project_id',
        inner: { table: 'projects', project: 'id', where: [{ col: 'team', op: 'eq', value: subqueryTeam }] },
      }
    }
    if (withAgg) {
      s.aggregate = { func: aggFunc, col: aggFunc === 'count' ? null : 'priority' }
    }
    return s
  }, [table, conjuncts, withSubquery, subqueryTeam, withAgg, aggFunc])

  const preview = useMemo(() => {
    const where: unknown[] = spec.where.map((c) => c)
    if (spec.subquery) {
      where.push({
        col: spec.subquery.col,
        in: { table: spec.subquery.inner.table, project: spec.subquery.inner.project, where: spec.subquery.inner.where },
      })
    }
    const req: Record<string, unknown> = {
      table: spec.table,
      ...(where.length ? { where: where.length === 1 ? where[0] : { and: where } } : {}),
      ...(spec.aggregate ? { fn: spec.aggregate.func, ...(spec.aggregate.col ? { col: spec.aggregate.col } : {}) } : {}),
    }
    return `POST ${spec.aggregate ? '/aggregate' : '/shapes'}\n${JSON.stringify(req, null, 2)}`
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
              const t = e.target.value as 'issues' | 'projects'
              setTable(t)
              setConjuncts([{ col: COLUMNS[t][0]!.col, op: 'eq', value: '' }])
              if (t !== 'issues') setWithSubquery(false)
            }}
          >
            <option>issues</option>
            <option>projects</option>
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
                  className="conjunct-value"
                  value={c.value}
                  placeholder="value"
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

        {table === 'issues' ? (
          <label className="fld fld-check">
            <input type="checkbox" checked={withSubquery} onChange={(e) => setWithSubquery(e.target.checked)} />
            project_id IN (SELECT id FROM projects WHERE team =
            <select value={subqueryTeam} onChange={(e) => setSubqueryTeam(e.target.value)}>
              {['web', 'mobile', 'infra'].map((t) => (
                <option key={t}>{t}</option>
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
          {withAgg && aggFunc !== 'count' ? <span className="fld-note">over priority</span> : null}
        </label>

        <label className="fld">
          label
          <input className="fld-label" value={label} placeholder="My shape" onChange={(e) => setLabel(e.target.value)} />
        </label>

        <div className="preview">
          <span className="preview-l">API request</span>
          <pre className="api-block api-block-flat">{preview}</pre>
        </div>

        <div className="modal-actions">
          <button className="mini" onClick={onClose}>
            cancel
          </button>
          <button
            className="primary"
            onClick={() => {
              onCreate(spec, label || 'My shape')
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
