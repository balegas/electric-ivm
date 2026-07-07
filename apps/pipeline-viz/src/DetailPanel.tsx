import { useEffect, useMemo, useState, type ReactNode } from 'react'

import type { NodeKind, NodeRef } from './build-graph'
import { KIND_META, fmtScalar } from './node-meta'
import { predicateLabel } from './predicate-label'
import { nodeInnerSql, shapeSql } from './shape-sql'
import { useNodeState } from './state-store'
import type { AggregateDump, EngineGraph, FamilyDump, GraphShape, NodeIndex } from './types'
import { useShapeContents } from './useShapeContents'
import { useShapeLog } from './useShapeLog'

const CONTENTS_LIMIT = 50
const DUMP_POLL_MS = 2500

/** Poll the engine's deep state dump for one node (`GET /state/node?id=`) while the panel shows
 *  it. Deep contents (routing keys, multisets) are on-demand — only summaries stream over SSE. */
function useNodeDump<T>(id: string | null): { dump: T | null; error: string | null } {
  const [dump, setDump] = useState<T | null>(null)
  const [error, setError] = useState<string | null>(null)
  useEffect(() => {
    setDump(null)
    setError(null)
    if (!id) return
    let alive = true
    const fetchDump = async () => {
      try {
        const r = await fetch(`/engine/state/node?id=${encodeURIComponent(id)}`)
        if (!r.ok) throw new Error(`state/node → ${r.status}`)
        if (alive) {
          setDump((await r.json()) as T)
          setError(null)
        }
      } catch (e) {
        if (alive) setError(String(e))
      }
    }
    void fetchDump()
    const t = setInterval(fetchDump, DUMP_POLL_MS)
    return () => {
      alive = false
      clearInterval(t)
    }
  }, [id])
  return { dump, error }
}

/** The per-kind "inside this operator" explainer — what the engine actually executes here. */
function InsideNote({ kind }: { kind: NodeKind }) {
  return (
    <div className="dp-note dp-inside">
      <span className="dp-inside-h">inside this operator</span>
      {KIND_META[kind].inside}
    </div>
  )
}

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

/** The live routing index of a family router (`GET /state/node?id=family:…`) — the actual key
 *  tuples the engine holds, not a client-side reconstruction. */
function FamilyIndexView({ nodeId, onSelectShape }: { nodeId: string; onSelectShape: (id: string) => void }) {
  const { dump, error } = useNodeDump<FamilyDump>(nodeId)
  return (
    <>
      <div className="dp-sec">
        routing index · key tuple → shapes{dump?.truncated ? ' (top 500)' : ''}
        <span className="dp-live-dot" title={`live dump, every ${DUMP_POLL_MS / 1000}s`} />
      </div>
      {error ? <div className="dp-err">{error}</div> : null}
      <div className="dp-list dp-index">
        {dump?.entries.map((e, i) => (
          <div key={i} className="dp-item">
            <code className="dp-key">({e.key.map(fmtValue).join(', ')})</code>
            <span className="dp-arrow">→</span>
            <span>
              {e.shapes.map((sid) => (
                <ShapeLink key={sid} id={sid} onSelect={onSelectShape} />
              ))}
            </span>
          </div>
        ))}
        {dump && dump.entries.length === 0 ? <div className="dp-empty-idx">index is empty</div> : null}
      </div>
    </>
  )
}

/** An aggregate's fold internals (`GET /state/node?id=shape:…`): running counters and, for
 *  MIN/MAX, the value → net-weight retraction multiset the engine actually keeps. */
function AggInternalsView({ nodeId }: { nodeId: string }) {
  const { dump, error } = useNodeDump<AggregateDump>(nodeId)
  return (
    <>
      <div className="dp-sec">
        fold state <span className="dp-live-dot" title={`live dump, every ${DUMP_POLL_MS / 1000}s`} />
      </div>
      {error ? <div className="dp-err">{error}</div> : null}
      {dump ? (
        <>
          <Row k="matching rows (Σ weights)" v={dump.count.toLocaleString()} />
          <Row k="non-NULL values" v={dump.nnCount.toLocaleString()} />
          {dump.multisetLen > 0 || dump.multiset.length > 0 ? (
            <>
              <div className="dp-sec">
                retraction multiset · value → net weight{dump.truncated ? ' (top 500)' : ''}
              </div>
              <div className="dp-list dp-index">
                {dump.multiset.map((m, i) => (
                  <div key={i} className="dp-item">
                    <code className="dp-key">{fmtValue(m.value)}</code>
                    <span className="dp-badge-n">×{m.weight}</span>
                  </div>
                ))}
                {dump.multiset.length === 0 ? <div className="dp-empty-idx">multiset is empty</div> : null}
              </div>
            </>
          ) : null}
        </>
      ) : null}
    </>
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
    const t = setInterval(fetchIdx, DUMP_POLL_MS)
    return () => {
      alive = false
      clearInterval(t)
    }
  }, [node])

  // Live summary of the focused node (fed by the SSE state stream; same data as the canvas chips).
  // Operator refs carry their trace-hop id, which is also the state-summary id of the underlying
  // structure — so an operator's panel shows its owner's live state.
  const stateId =
    node.kind === 'op'
      ? node.hop
      : node.kind === 'table'
        ? `table:${node.name}`
        : node.kind === 'family'
          ? `family:${node.table}:${node.keyCols.join(',')}`
          : node.kind === 'filter'
            ? `filter:${node.shapeId}`
            : node.kind === 'sqnode'
              ? `node:${node.sig}`
              : `shape:${node.shapeId}`
  const live = useNodeState(stateId)

  let title = ''
  let body: ReactNode = null

  if (node.kind === 'table') {
    title = `Table · ${node.name}`
    const onTable = graph.shapes.filter((s) => s.table === node.name)
    const innerFor = graph.subqueryNodes.filter((n) => n.innerTable === node.name)
    body = (
      <>
        <Row k="role" v="replication source (table/<name> stream → tailer)" />
        {live?.kind === 'table' ? (
          <>
            <Row k="envelopes processed" v={live.envelopes.toLocaleString()} />
            <Row k="processed offset" v={<code>{live.processedOffset}</code>} />
          </>
        ) : null}
        <Row k="shapes on it" v={onTable.length} />
        {innerFor.length > 0 ? <Row k="feeds subquery nodes" v={innerFor.length} /> : null}
        <InsideNote kind="table" />
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
    title = `Route join · (${node.keyCols.join(', ')})`
    const members = graph.shapes.filter(
      (s) => s.table === node.table && s.familyKey && s.familyKey.join(',') === node.keyCols.join(','),
    )
    body = (
      <>
        <Row k="table" v={node.table} />
        <Row k="key columns" v={node.keyCols.join(', ')} />
        {live?.kind === 'family' ? (
          <>
            <Row k="index keys (live)" v={live.keys.toLocaleString()} />
            <Row k="routed shapes (live)" v={live.shapes.toLocaleString()} />
          </>
        ) : null}
        <Row k="member shapes (graph)" v={members.length} />
        <InsideNote kind="family" />
        <FamilyIndexView nodeId={stateId} onSelectShape={onSelectShape} />
        <div className="dp-sec">registered predicates</div>
        <div className="dp-list">
          {members.map((s) => (
            <div key={s.id} className="dp-item">
              <code className="dp-key">{predicateLabel(s.where)}</code>
              <span className="dp-arrow">→</span>
              <ShapeLink id={s.id} onSelect={onSelectShape} />
            </div>
          ))}
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
        {live?.kind === 'filter' ? <Row k="envelopes emitted (live)" v={live.emitted.toLocaleString()} /> : null}
        <InsideNote kind="filter" />
        {s ? <SqlBlock sql={shapeSql(s)} /> : null}
        {s ? <ShapeLiveView shape={s} /> : null}
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
        <InsideNote kind="sqnode" />
        <div className="dp-sec">
          inner-set index · value → contributors
          {index?.truncated ? ' (top 500)' : ''}
          <span className="dp-live-dot" title={`live index, every ${DUMP_POLL_MS / 1000}s`} />
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
      </>
    )
  } else if (node.kind === 'op') {
    const meta = KIND_META[node.opKind]
    title = `Operator · ${node.label}`
    body = (
      <>
        <Row k="operator" v={<code>{meta.tag}</code>} />
        <Row k="computes" v={<code>{meta.formula}</code>} />
        <Row k="part of" v={<code>{node.hop}</code>} />
        {live?.kind === 'table' ? (
          <>
            <Row k="envelopes processed" v={live.envelopes.toLocaleString()} />
            <Row k="processed offset" v={<code>{live.processedOffset}</code>} />
          </>
        ) : null}
        {live?.kind === 'filter' ? <Row k="envelopes emitted (live)" v={live.emitted.toLocaleString()} /> : null}
        {live?.kind === 'shape' ? <Row k="envelopes emitted (live)" v={live.emitted.toLocaleString()} /> : null}
        {live?.kind === 'family' ? (
          <>
            <Row k="index keys (live)" v={live.keys.toLocaleString()} />
            <Row k="routed shapes (live)" v={live.shapes.toLocaleString()} />
          </>
        ) : null}
        {live?.kind === 'subqueryNode' ? (
          <>
            <Row k="distinct values (live)" v={live.distinctValues.toLocaleString()} />
            <Row k="refcount" v={live.refcount} />
          </>
        ) : null}
        {live?.kind === 'aggregate' ? (
          <>
            <Row k="current value (live)" v={<code>{fmtScalar(live.value)}</code>} />
            <Row k="matching rows" v={live.count.toLocaleString()} />
          </>
        ) : null}
        <InsideNote kind={node.opKind} />
      </>
    )
  } else if (node.kind === 'aggshape') {
    const s = graph.shapes.find((x) => x.id === node.shapeId)
    const fn = s?.aggregate?.func.toUpperCase() ?? '?'
    title = `Aggregation · ${node.shapeId}`
    body = (
      <>
        <div className="dp-agg">
          <div className="dp-agg-l">
            current value <span className="dp-live-dot" title="pushed on the state stream" />
          </div>
          <div className="dp-agg-n">{live?.kind === 'aggregate' ? fmtScalar(live.value) : '…'}</div>
          <div className="dp-agg-e">{`${fn}(${s?.aggregate?.col ?? '*'})`}</div>
        </div>
        <Row k="function" v={<code>{`${fn}(${s?.aggregate?.col ?? '*'})`}</code>} />
        <Row k="table" v={s?.table} />
        <Row k="output" v="a single live scalar (streamed)" />
        <InsideNote kind="agg" />
        <AggInternalsView nodeId={stateId} />
        {s ? <SqlBlock sql={shapeSql(s)} /> : null}
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
        {live?.kind === 'shape' ? <Row k="envelopes emitted (live)" v={live.emitted.toLocaleString()} /> : null}
        <InsideNote kind="shape" />
        {s ? <SqlBlock sql={shapeSql(s)} /> : null}
        {s ? <ShapeLiveView shape={s} /> : null}
        <div className="dp-note">
          {s?.isSubquery
            ? 'Membership is driven by the shared subquery node(s) upstream plus the outer-row filter — when an inner value flips, the affected rows move in/out of this stream.'
            : s?.familyKey
              ? 'Fed by the shared route join upstream — the engine keeps only this shape’s routing entry and snapshot gate, no table rows.'
              : 'Fed by its standalone filter — enter/leave falls out of filtering each delta.'}
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
