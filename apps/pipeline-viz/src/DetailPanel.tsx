import { useEffect, useMemo, useState, type ReactNode } from 'react'

import type { NodeRef } from './build-graph'
import { predicateLabel } from './predicate-label'
import { nodeInnerSql, shapeSql } from './shape-sql'
import type { EngineGraph, GraphShape, NodeIndex } from './types'
import { fmtScalar } from './useAggValues'
import { useShapeContents } from './useShapeContents'
import { useShapeLog } from './useShapeLog'

const CONTENTS_LIMIT = 50

/** Render a shape's projected columns as chips, or note that it syncs the full row. */
function ColumnList({ columns }: { columns: string[] | null }) {
  if (!columns || columns.length === 0) return <span className="dp-cols-all">all columns</span>
  return (
    <span className="dp-cols">
      {columns.map((c) => (
        <code key={c} className="dp-col-chip">
          {c}
        </code>
      ))}
    </span>
  )
}

function fmtCell(v: unknown): string {
  if (v === null || v === undefined) return '∅'
  const s = String(v)
  return s.length > 48 ? `${s.slice(0, 48)}…` : s
}

function fmtLogRow(row?: Record<string, unknown>): string {
  if (!row) return '∅'
  const s = Object.entries(row)
    .map(([k, v]) => `${k}: ${fmtCell(v)}`)
    .join(' · ')
  return s.length > 160 ? `${s.slice(0, 160)}…` : s
}

const LOG_OPS: Record<string, { sym: string; cls: string }> = {
  insert: { sym: '+', cls: 'dp-op-ins' },
  update: { sym: '~', cls: 'dp-op-upd' },
  delete: { sym: '−', cls: 'dp-op-del' },
}

/** Live change log of a feed shape (polls `GET /shapes/{id}/log`): the flow of insert/update/
 *  delete envelopes on the live tail, newest first — a feed has no materialized set to show. */
function ShapeLogView({ shapeId }: { shapeId: string }) {
  const { entries, total, live, loading, error } = useShapeLog(true, shapeId, CONTENTS_LIMIT)
  // The endpoint walks the whole stream, so ops arrive as exact insert/update/delete and a delete
  // entry's `old` carries the row it removed — newest first for display.
  const shown = useMemo(() => entries.map((e) => ({ ...e, row: e.value ?? e.old })).reverse(), [entries])
  return (
    <div className="dp-contents">
      <div className="dp-sec dp-contents-h">
        <span>
          live change log {live ? <span className="dp-live-dot" title="polling every 2s" /> : null}
        </span>
        <span className="dp-contents-n">
          {loading ? 'loading…' : `${total.toLocaleString()} change${total === 1 ? '' : 's'}`}
        </span>
      </div>
      <div className="dp-note">
        changes-only feed — every insert / update / delete seen on the live tail, newest first.
      </div>
      {error ? <div className="dp-err">{error}</div> : null}
      {!error && !loading && total === 0 ? <div className="dp-empty-idx">no changes seen yet</div> : null}
      {shown.length > 0 ? (
        <div className="dp-table-wrap">
          <table className="dp-table dp-log">
            <tbody>
              {shown.map((e, i) => {
                const op = LOG_OPS[e.op] ?? { sym: e.op, cls: '' }
                return (
                  <tr key={total - i} className={op.cls}>
                    <td className="dp-log-op" title={e.op}>
                      {op.sym}
                    </td>
                    <td className="dp-log-key">{e.key}</td>
                    <td className="dp-log-row" title={e.row ? JSON.stringify(e.row) : undefined}>
                      {fmtLogRow(e.row)}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      ) : null}
      {total > shown.length ? (
        <div className="dp-note">
          showing latest {shown.length} of {total.toLocaleString()} — updates live
        </div>
      ) : null}
    </div>
  )
}

/** A shape's live view: materialized shapes show their current set, feeds show the change log. */
function ShapeLiveView({ shape }: { shape: GraphShape }) {
  return shape.changesOnly ? <ShapeLogView shapeId={shape.id} /> : <ShapeContentsView shapeId={shape.id} />
}

const BROWSE_PAGE = 25

/** Paginated browser over a table's rows via the engine's one-shot subset query
 *  (`POST /query` with limit/offset) — each page is fetched on demand, no shape is
 *  created and nothing is materialized, so large tables never load upfront. */
function TableBrowser({ table }: { table: string }) {
  const [page, setPage] = useState(0)
  const [rows, setRows] = useState<Record<string, unknown>[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [nonce, setNonce] = useState(0)

  useEffect(() => {
    setPage(0)
  }, [table])

  useEffect(() => {
    let stopped = false
    const ac = new AbortController()
    setLoading(true)
    const run = async () => {
      // Keyset-stable paging needs an order; try `id` first (the common case), fall back to
      // the table's natural order for tables without an id column.
      const body = (withOrder: boolean) =>
        JSON.stringify({
          table,
          limit: BROWSE_PAGE,
          offset: page * BROWSE_PAGE,
          ...(withOrder ? { orderBy: { col: 'id', desc: false } } : {}),
        })
      try {
        let r = await fetch('/engine/query', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: body(true),
          signal: ac.signal,
        })
        if (!r.ok) {
          r = await fetch('/engine/query', {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: body(false),
            signal: ac.signal,
          })
        }
        if (!r.ok) throw new Error(`query → ${r.status}`)
        const data = (await r.json()) as { rows: Record<string, unknown>[] }
        if (stopped) return
        setRows(data.rows)
        setError(null)
      } catch (e) {
        if (!ac.signal.aborted && !stopped) setError(String(e))
      } finally {
        if (!stopped) setLoading(false)
      }
    }
    void run()
    return () => {
      stopped = true
      ac.abort()
    }
  }, [table, page, nonce])

  const columns = rows.length ? Object.keys(rows[0]!) : []
  return (
    <div className="dp-contents">
      <div className="dp-sec dp-contents-h">
        <span>browse data</span>
        <span className="dp-browse-nav">
          <button className="dp-page-btn" disabled={page === 0 || loading} onClick={() => setPage((p) => p - 1)}>
            ‹ prev
          </button>
          <span className="dp-page-n">page {page + 1}</span>
          <button
            className="dp-page-btn"
            disabled={rows.length < BROWSE_PAGE || loading}
            onClick={() => setPage((p) => p + 1)}
          >
            next ›
          </button>
          <button className="dp-page-btn" disabled={loading} title="reload this page" onClick={() => setNonce((n) => n + 1)}>
            ↻
          </button>
        </span>
      </div>
      <div className="dp-note">
        one-shot subset query, {BROWSE_PAGE} rows per page — pages load on demand, nothing is materialized.
      </div>
      {error ? <div className="dp-err">{error}</div> : null}
      {!error && !loading && rows.length === 0 ? (
        <div className="dp-empty-idx">{page === 0 ? 'table is empty' : 'no more rows'}</div>
      ) : null}
      {rows.length > 0 ? (
        <div className="dp-table-wrap">
          <table className="dp-table">
            <thead>
              <tr>
                {columns.map((c) => (
                  <th key={c}>{c}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((r, i) => (
                <tr key={`${page}:${i}`}>
                  {columns.map((c) => (
                    <td key={c} title={String(r[c] ?? '')}>
                      {fmtCell(r[c])}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
    </div>
  )
}

/** Live-preview a shape's rows (polls `GET /shapes/{id}/rows`) as a compact, updating table. */
function ShapeContentsView({ shapeId }: { shapeId: string }) {
  const { rows, columns, count, changesOnly, live, loading, error } = useShapeContents(true, shapeId, CONTENTS_LIMIT)
  const shown = rows.slice(0, CONTENTS_LIMIT)
  return (
    <div className="dp-contents">
      <div className="dp-sec dp-contents-h">
        <span>
          live contents {live ? <span className="dp-live-dot" title="polling every 2s" /> : null}
        </span>
        <span className="dp-contents-n">
          {loading ? 'loading…' : `${count.toLocaleString()} row${count === 1 ? '' : 's'}`}
        </span>
      </div>
      {changesOnly ? (
        <div className="dp-note">changes-only feed — shows rows seen on the live tail (no backfill).</div>
      ) : null}
      {error ? <div className="dp-err">{error}</div> : null}
      {!error && !loading && count === 0 ? <div className="dp-empty-idx">set is empty</div> : null}
      {shown.length > 0 ? (
        <div className="dp-table-wrap">
          <table className="dp-table">
            <thead>
              <tr>{columns.map((c) => <th key={c}>{c}</th>)}</tr>
            </thead>
            <tbody>
              {shown.map((r) => (
                <tr key={r.key}>
                  {columns.map((c) => (
                    <td key={c} title={String(r.value[c] ?? '')}>
                      {fmtCell(r.value[c])}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
      {count > shown.length ? <div className="dp-note">showing first {shown.length} of {count.toLocaleString()} — updates live</div> : null}
    </div>
  )
}

/** Prominent live scalar for an aggregation shape. Polls the same `GET /shapes/{id}/rows`
 *  endpoint as row previews — for an aggregation the single "agg" row's `value` field IS the
 *  running aggregate. Only mounted while an aggshape is focused, so nothing double-polls. */
function AggValueView({ shapeId, expr }: { shapeId: string; expr: string }) {
  const { rows, live, loading, error } = useShapeContents(true, shapeId, 1)
  const v = rows[0]?.value['value']
  return (
    <div className="dp-agg">
      <div className="dp-agg-l">
        current value {live ? <span className="dp-live-dot" title="polling every 2s" /> : null}
      </div>
      <div className="dp-agg-n">{v === undefined || v === null ? (loading ? '…' : '—') : fmtScalar(v)}</div>
      <div className="dp-agg-e">{expr}</div>
      {error ? <div className="dp-err">{error}</div> : null}
    </div>
  )
}

function Row({ k, v }: { k: string; v: ReactNode }) {
  return (
    <div className="dp-row">
      <span className="dp-k">{k}</span>
      <span className="dp-v">{v}</span>
    </div>
  )
}

/** The SQL statement an entity corresponds to, with a copy button. */
function SqlBlock({ sql, label = 'SQL' }: { sql: string; label?: string }) {
  const [copied, setCopied] = useState(false)
  return (
    <div className="dp-sql">
      <div className="dp-sec dp-sql-h">
        <span>{label}</span>
        <button
          className="dp-copy"
          onClick={() => {
            void navigator.clipboard?.writeText(sql)
            setCopied(true)
            setTimeout(() => setCopied(false), 1200)
          }}
        >
          {copied ? '✓ copied' : 'copy'}
        </button>
      </div>
      <pre className="dp-sql-code">{sql}</pre>
    </div>
  )
}

function fmtValue(v: unknown): string {
  if (v === null || v === undefined) return 'NULL'
  if (typeof v === 'string') return `'${v}'`
  return String(v)
}

function ShapeLink({ id, onSelect }: { id: string; onSelect: (id: string) => void }) {
  return (
    <button className="dp-shape-link" onClick={() => onSelect(id)}>
      {id}
    </button>
  )
}

export function DetailPanel({
  node,
  graph,
  onClose,
  onSelectShape,
}: {
  node: NodeRef
  graph: EngineGraph
  onClose: () => void
  onSelectShape: (id: string) => void
}) {
  const [index, setIndex] = useState<NodeIndex | null>(null)
  const [indexErr, setIndexErr] = useState<string | null>(null)

  // For subquery nodes, fetch the live inner-set index and keep it fresh while the panel is open.
  useEffect(() => {
    if (node.kind !== 'sqnode') {
      setIndex(null)
      return
    }
    let alive = true
    const fetchIdx = async () => {
      try {
        const r = await fetch(`/engine/graph/node?sig=${encodeURIComponent(node.sig)}`)
        if (!r.ok) throw new Error(`node index → ${r.status}`)
        if (alive) {
          setIndex((await r.json()) as NodeIndex)
          setIndexErr(null)
        }
      } catch (e) {
        if (alive) setIndexErr(String(e))
      }
    }
    void fetchIdx()
    const t = setInterval(fetchIdx, 2500)
    return () => {
      alive = false
      clearInterval(t)
    }
  }, [node])

  let title = ''
  let body: ReactNode = null

  if (node.kind === 'table') {
    title = `Table · ${node.name}`
    const onTable = graph.shapes.filter((s) => s.table === node.name)
    const innerFor = graph.subqueryNodes.filter((n) => n.innerTable === node.name)
    body = (
      <>
        <Row k="role" v="replication source (table/<name> stream → tailer)" />
        <Row k="shapes on it" v={onTable.length} />
        {innerFor.length > 0 ? <Row k="feeds subquery nodes" v={innerFor.length} /> : null}
        <div className="dp-sec">shapes reading this table</div>
        <div className="dp-list">
          {onTable.map((s) => (
            <div key={s.id} className="dp-item">
              <ShapeLink id={s.id} onSelect={onSelectShape} />
              <span className="dp-item-sub">{predicateLabel(s.where)}</span>
            </div>
          ))}
        </div>
        <TableBrowser table={node.name} />
      </>
    )
  } else if (node.kind === 'family') {
    title = `Family router · (${node.keyCols.join(', ')})`
    const members = graph.shapes.filter(
      (s) => s.table === node.table && s.familyKey && s.familyKey.join(',') === node.keyCols.join(','),
    )
    body = (
      <>
        <Row k="table" v={node.table} />
        <Row k="key columns" v={node.keyCols.join(', ')} />
        <Row k="routing" v="key tuple → shapes (O(log N), no table copy)" />
        <Row k="member shapes" v={members.length} />
        <div className="dp-sec">routing index · key → shape</div>
        <div className="dp-list">
          {members.map((s) => (
            <div key={s.id} className="dp-item">
              <code className="dp-key">{predicateLabel(s.where)}</code>
              <span className="dp-arrow">→</span>
              <ShapeLink id={s.id} onSelect={onSelectShape} />
            </div>
          ))}
        </div>
        <div className="dp-note">
          All {members.length} shapes above share this one router — a change is routed by its key to
          exactly the matching shape(s).
        </div>
      </>
    )
  } else if (node.kind === 'filter') {
    const s = graph.shapes.find((x) => x.id === node.shapeId)
    title = `Filter · ${node.shapeId}`
    body = (
      <>
        <Row k="table" v={s?.table} />
        <Row k="predicate" v={<code>{predicateLabel(s?.where ?? null)}</code>} />
        <Row k="columns" v={<ColumnList columns={s?.columns ?? null} />} />
        <Row k="type" v="standalone — stateless (no index)" />
        {s ? <SqlBlock sql={shapeSql(s)} /> : null}
        {s ? <ShapeLiveView shape={s} /> : null}
        <div className="dp-note">
          Non-equality predicate: evaluated directly on each change delta. It holds no state, so there is
          no index to maintain — the cost is O(1) predicate evals per change.
        </div>
      </>
    )
  } else if (node.kind === 'sqnode') {
    title = 'Subquery node'
    const deps = graph.subqueryEdges.filter((e) => e.nodeSig === node.sig)
    body = (
      <>
        <SqlBlock sql={nodeInnerSql(graph, node.sig, node.innerTable, node.projCol)} label="maintains" />
        <Row k="inner table" v={node.innerTable} />
        <Row k="distinct values" v={index ? index.distinctValues : '…'} />
        <Row k="refcount (dependents)" v={index ? index.refcount : '…'} />
        <div className="dp-sec">
          inner-set index · value → contributors
          {index?.truncated ? ' (top 500)' : ''}
        </div>
        {indexErr ? <div className="dp-err">{indexErr}</div> : null}
        <div className="dp-list dp-index">
          {index?.values.map((v, i) => (
            <div key={i} className="dp-item">
              <code className="dp-key">{fmtValue(v.value)}</code>
              <span className="dp-badge-n">{v.contributors} row{v.contributors === 1 ? '' : 's'}</span>
            </div>
          ))}
          {index && index.values.length === 0 ? <div className="dp-empty-idx">set is empty</div> : null}
        </div>
        <div className="dp-sec">dependents</div>
        <div className="dp-list">
          {deps.map((e, i) => (
            <div key={i} className="dp-item">
              {e.dependentKind === 'shape' ? (
                <ShapeLink id={e.dependentId} onSelect={onSelectShape} />
              ) : (
                <span className="dp-item-sub">node {e.dependentId.slice(0, 24)}…</span>
              )}
              <span className="dp-item-sub">
                {e.negated ? 'NOT IN' : 'IN'} via {e.connectingCol}
              </span>
            </div>
          ))}
        </div>
        <div className="dp-note">
          This one maintained set is shared by every dependent above (the sharing the engine gives you for
          free) — when a value enters/leaves it, the affected outer rows move in/out live.
        </div>
      </>
    )
  } else if (node.kind === 'aggshape') {
    const s = graph.shapes.find((x) => x.id === node.shapeId)
    const fn = s?.aggregate?.func.toUpperCase() ?? '?'
    title = `Aggregation · ${node.shapeId}`
    body = (
      <>
        <AggValueView shapeId={node.shapeId} expr={`${fn}(${s?.aggregate?.col ?? '*'})`} />
        <Row k="function" v={<code>{`${fn}(${s?.aggregate?.col ?? '*'})`}</code>} />
        <Row k="table" v={s?.table} />
        <Row k="output" v="a single live scalar (streamed)" />
        {s ? <SqlBlock sql={shapeSql(s)} /> : null}
        <div className="dp-note">
          Maintained incrementally as a dbsp <b>fold</b>: each change that enters/leaves the filter
          adjusts the running aggregate — no rows are stored. COUNT is Σ of the Z-set weights. This drives
          the app's top-of-list counter (the true total, live) rather than a client-side length of the
          loaded window.
        </div>
      </>
    )
  } else if (node.kind === 'op') {
    title = `Operator · ${node.op}`
    body = (
      <>
        <Row k="operator" v={node.op} />
        <Row k="computes" v={<code>{node.formula}</code>} />
        <div className="dp-note">{node.note}</div>
        <div className="dp-note">
          In the <b>dbsp circuit</b> view every box is one incremental operator and every arrow is a
          Z-set stream (dashed = a stateful arrangement feeding a join). Operators shared underneath —
          a table's Δ, a family's params, a subquery's distinct — appear once.
        </div>
      </>
    )
  } else {
    // shape
    const s = graph.shapes.find((x) => x.id === node.shapeId)
    title = `Shape · ${node.shapeId}`
    const kind = s?.isSubquery ? 'subquery' : s?.familyKey ? `family(${s.familyKey.join(',')})` : 'standalone'
    body = (
      <>
        <Row k="table" v={s?.table} />
        <Row k="routing" v={kind} />
        <Row k="columns" v={<ColumnList columns={s?.columns ?? null} />} />
        <Row k="changes-only feed" v={s?.changesOnly ? 'yes (no backfill)' : 'no (materialized)'} />
        <Row k="stream" v={<code>{s?.streamPath}</code>} />
        {s ? <SqlBlock sql={shapeSql(s)} /> : null}
        {s ? <ShapeLiveView shape={s} /> : null}
        <div className="dp-note">
          {s?.isSubquery
            ? 'Membership is driven by the shared subquery node(s) upstream plus the outer-row filter.'
            : s?.familyKey
              ? 'Routed by key through a shared family — the engine keeps only per-shape metadata, no table rows.'
              : 'A stateless filter over the change stream — enter/leave falls out of filtering each delta.'}
        </div>
      </>
    )
  }

  return (
    <div className="detail">
      <div className="detail-h">
        <span className="detail-title">{title}</span>
        <button className="detail-x" onClick={onClose}>
          ✕
        </button>
      </div>
      <div className="detail-body">{body}</div>
    </div>
  )
}

export type { GraphShape }
