import { useEffect, useMemo, useState, type ReactNode } from 'react'

import type { NodeKind, NodeRef } from './build-graph'
import { useLatestDelta } from './delta-store'
import { KIND_META, fmtScalar, servingTier } from './node-meta'
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

/** An aggregate shape's live output: its SINK emits a single scalar row (`{key:"agg",
 *  value:{n,value}}`), so render the scalar big rather than as a one-cell table. Fed by the same
 *  event-driven rows fetch as every other shape, so it updates as changes arrive. */
function AggregateScalarView({ shape }: { shape: GraphShape }) {
  const { rows, live, loading, error } = useShapeContents(true, shape.id, 1)
  const fn = shape.aggregate?.func.toUpperCase() ?? '?'
  const expr = `${fn}(${shape.aggregate?.col ?? '*'})`
  const val = rows[0]?.value as { value?: unknown } | undefined
  const scalar = val && typeof val === 'object' && 'value' in val ? val.value : (val as unknown)
  return (
    <div className="dp-contents">
      <div className="dp-sec dp-contents-h">
        <span>live output {live ? <span className="dp-live-dot" title="refetched on each change" /> : null}</span>
      </div>
      <div className="dp-note">This sink emits a single scalar — the aggregate’s current value.</div>
      {error ? <div className="dp-err">{error}</div> : null}
      <div className="dp-agg">
        <div className="dp-agg-l">current value</div>
        <div className="dp-agg-n">{loading && rows.length === 0 ? '…' : fmtScalar(scalar)}</div>
        <div className="dp-agg-e">{expr}</div>
      </div>
    </div>
  )
}

/** The live materialized output of a SINK operator (`snk:<id>`): the shape's current rows,
 *  refetched as changes arrive. Aggregates render their scalar; a removed shape clears gracefully. */
function SinkView({ shape }: { shape: GraphShape | undefined }) {
  if (!shape) return <div className="dp-note">shape removed — nothing to show.</div>
  return shape.aggregate ? <AggregateScalarView shape={shape} /> : <ShapeLiveView shape={shape} />
}

const BROWSE_PAGE = 25

/** The compiled dbsp arrangements folded onto a table's SOURCE node, as a compact list. On the
 *  canvas the arrangement lane is collapsed onto the source (a count badge) to keep the graph
 *  legible; here, where a click affords the room, the indexes and counts pipelines are spelled out.
 *  The prose that explains what each KIND does lives in [`SourceArrangementNotes`], rendered lower
 *  so the row browser stays near the top. Read straight from `graph.arrangements`. */
function SourceArrangements({ table, arr }: { table: string; arr: EngineGraph['arrangements'] }) {
  if (!arr) return null
  const indexes = arr.indexes.filter((i) => i.table === table)
  const counts = (arr.counts ?? []).filter((c) => c.table === table)
  if (indexes.length === 0 && counts.length === 0) return null
  return (
    <>
      <div className="dp-sec">compiled dbsp arrangements</div>
      <div className="dp-list dp-index">
        {indexes.map((i) => (
          <div key={i.id} className="dp-item">
            <code>map_index({i.cols.join(', ')})</code>
            <span className="dp-item-sub">integrate_trace · {i.seeded ? 'seeded' : 'seeding…'}</span>
          </div>
        ))}
        {counts.map((c) => (
          <div key={c.id} className="dp-item">
            <code>weighted_count({c.groupCols.join(', ')})</code>
            <span className="dp-item-sub">counts pipeline · {c.seeded ? 'seeded' : 'seeding…'}</span>
          </div>
        ))}
      </div>
    </>
  )
}

/** The "what this kind does" prose for a source's folded arrangements — one headed card per kind
 *  the table actually has. Rendered BELOW the row browser (verbose reference, not the headline), and
 *  headed like `InsideNote` so the panel's explanation cards read consistently. */
function SourceArrangementNotes({ table, arr }: { table: string; arr: EngineGraph['arrangements'] }) {
  if (!arr) return null
  const hasIndex = arr.indexes.some((i) => i.table === table)
  const hasCounts = (arr.counts ?? []).some((c) => c.table === table)
  if (!hasIndex && !hasCounts) return null
  return (
    <>
      {hasIndex ? (
        <div className="dp-note dp-inside">
          <span className="dp-inside-h">what an index remembers</span>
          {KIND_META['arr-index'].inside}
        </div>
      ) : null}
      {hasCounts ? (
        <div className="dp-note dp-inside">
          <span className="dp-inside-h">what a counts pipeline computes</span>
          {KIND_META['arr-counts'].inside}
        </div>
      ) : null}
    </>
  )
}

/** Paginated browser over a table's rows via the engine's one-shot subset query
 *  (`POST /query` with limit/offset) — each page is fetched on demand, no shape is
 *  created and nothing is materialized, so large tables never load upfront.
 *
 *  Insert lives here too: a `+ add row` affordance below the rows reveals an inline
 *  editable row aligned to the table's columns; submitting inserts into Postgres via
 *  the engine (`POST /engine/table/{table}/rows`), then reloads the current page so the
 *  new row shows. The insert is captured by logical replication, so the pipeline animates
 *  on its own. Blank inputs are omitted (column default / NULL).
 *
 *  Delete too: when the schema exposes a primary key, each row gets a checkbox (plus a
 *  page-wide one in the header); `− delete n rows` removes the selected rows from Postgres
 *  in one request (`DELETE /engine/table/{table}/rows` with their primary keys), so the
 *  deletes replicate and flow through the pipeline like any other write. */
function TableBrowser({ table }: { table: string }) {
  const [page, setPage] = useState(0)
  const [rows, setRows] = useState<Record<string, unknown>[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [nonce, setNonce] = useState(0)

  // Table schema (columns + PK) — the authoritative column list for the header and the
  // inline editor, so the add-row lines up even when the table (and thus the query) is empty.
  const [schema, setSchema] = useState<TableSchemaInfo | null>(null)

  // Inline add-row editor state.
  const [adding, setAdding] = useState(false)
  const [values, setValues] = useState<Record<string, string>>({})
  const [inserting, setInserting] = useState(false)
  const [insertErr, setInsertErr] = useState<string | null>(null)
  const [okFlash, setOkFlash] = useState(false)

  // Row-selection + delete state. Each selected row is its serialized primary key — a JSON
  // object built in the schema's pk-column order, so the string is stable and parses straight
  // back into the DELETE request's key format.
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [deleting, setDeleting] = useState(false)
  const [deleteErr, setDeleteErr] = useState<string | null>(null)
  const [delFlash, setDelFlash] = useState(0)

  useEffect(() => {
    setPage(0)
    setAdding(false)
    setValues({})
    setInsertErr(null)
    setSelected(new Set())
    setDeleteErr(null)
  }, [table])

  // Selection is per page: rows off-screen should never be part of a delete.
  useEffect(() => {
    setSelected(new Set())
    setDeleteErr(null)
  }, [page])

  // Fetch the column list once per table (used for the header and the inline editor).
  useEffect(() => {
    let alive = true
    setSchema(null)
    void (async () => {
      try {
        const r = await fetch(`/engine/table/${encodeURIComponent(table)}/schema`)
        if (!r.ok) throw new Error(`schema → ${r.status}`)
        const s = (await r.json()) as TableSchemaInfo
        if (alive) setSchema(s)
      } catch {
        // Non-fatal: fall back to deriving columns from the fetched rows.
      }
    })()
    return () => {
      alive = false
    }
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

  // Prefer the schema's columns (order + PK); fall back to whatever the rows expose.
  const columns = schema ? schema.columns.map((c) => c.name) : rows.length ? Object.keys(rows[0]!) : []
  const colInfo = useMemo(() => {
    const m = new Map<string, TableColumn>()
    schema?.columns.forEach((c) => m.set(c.name, c))
    return m
  }, [schema])

  // Selection needs the primary key: without one there is no safe row identity to delete by.
  const pkCols = useMemo(() => schema?.primaryKey ?? [], [schema])
  const canSelect = pkCols.length > 0

  /** A row's selection key: its primary key as JSON, in pk-column order (null ⇒ not selectable). */
  const rowKey = (r: Record<string, unknown>): string | null => {
    const key: Record<string, unknown> = {}
    for (const c of pkCols) {
      const v = r[c]
      if (v === null || v === undefined) return null
      key[c] = v
    }
    return JSON.stringify(key)
  }

  const pageKeys = canSelect ? rows.map(rowKey).filter((k): k is string => k !== null) : []
  const allSelected = pageKeys.length > 0 && pageKeys.every((k) => selected.has(k))

  const toggleRow = (key: string) => {
    setSelected((s) => {
      const next = new Set(s)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }
  const toggleAll = () => {
    setSelected((s) => {
      const next = new Set(s)
      if (allSelected) pageKeys.forEach((k) => next.delete(k))
      else pageKeys.forEach((k) => next.add(k))
      return next
    })
  }

  const deleteSelected = async () => {
    if (selected.size === 0 || deleting) return
    setDeleting(true)
    setDeleteErr(null)
    try {
      // One request for the whole selection — the engine deletes all keys in a single
      // statement, so a multi-row delete reaches the pipeline as one replication batch.
      const keys = [...selected].map((k) => JSON.parse(k) as Record<string, unknown>)
      const r = await fetch(`/engine/table/${encodeURIComponent(table)}/rows`, {
        method: 'DELETE',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ keys }),
      })
      if (!r.ok) {
        const eb = (await r.json().catch(() => null)) as { error?: string } | null
        throw new Error(eb?.error ?? `delete → ${r.status}`)
      }
      setSelected(new Set())
      setDelFlash(keys.length)
      setTimeout(() => setDelFlash(0), 1600)
      setNonce((n) => n + 1)
    } catch (e) {
      setDeleteErr(e instanceof Error ? e.message : String(e))
    } finally {
      setDeleting(false)
    }
  }

  const openEditor = () => {
    setValues({})
    setInsertErr(null)
    setAdding(true)
  }
  const closeEditor = () => {
    setAdding(false)
    setValues({})
    setInsertErr(null)
  }

  const submit = async () => {
    setInserting(true)
    setInsertErr(null)
    // Only send filled-in fields; blank inputs fall through to the column default / NULL.
    const cols: Record<string, string> = {}
    for (const [k, v] of Object.entries(values)) if (v !== '') cols[k] = v
    try {
      const r = await fetch(`/engine/table/${encodeURIComponent(table)}/rows`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ columns: cols }),
      })
      if (!r.ok) {
        const eb = (await r.json().catch(() => null)) as { error?: string } | null
        throw new Error(eb?.error ?? `insert → ${r.status}`)
      }
      // Success: clear the editor and reload the current page so the new row shows.
      setValues({})
      setAdding(false)
      setOkFlash(true)
      setTimeout(() => setOkFlash(false), 1600)
      setNonce((n) => n + 1)
    } catch (e) {
      setInsertErr(e instanceof Error ? e.message : String(e))
    } finally {
      setInserting(false)
    }
  }

  const hasCols = columns.length > 0
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
      {!error && !loading && rows.length === 0 && !adding ? (
        <div className="dp-empty-idx">{page === 0 ? 'table is empty' : 'no more rows'}</div>
      ) : null}
      {hasCols && (rows.length > 0 || adding) ? (
        <div className="dp-table-wrap">
          <table className="dp-table">
            <thead>
              <tr>
                {canSelect ? (
                  <th className="dp-selcol">
                    <input
                      type="checkbox"
                      className="dp-sel-cb"
                      title="select all rows on this page"
                      checked={allSelected}
                      disabled={deleting || pageKeys.length === 0}
                      onChange={toggleAll}
                    />
                  </th>
                ) : null}
                {columns.map((c) => (
                  <th key={c}>{c}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((r, i) => {
                const key = canSelect ? rowKey(r) : null
                const isSel = key !== null && selected.has(key)
                return (
                  <tr key={`${page}:${i}`} className={isSel ? 'dp-row-sel' : undefined}>
                    {canSelect ? (
                      <td className="dp-selcol">
                        {key !== null ? (
                          <input
                            type="checkbox"
                            className="dp-sel-cb"
                            title="select row for delete"
                            checked={isSel}
                            disabled={deleting}
                            onChange={() => toggleRow(key)}
                          />
                        ) : null}
                      </td>
                    ) : null}
                    {columns.map((c) => (
                      <td key={c} title={String(r[c] ?? '')}>
                        {fmtCell(r[c])}
                      </td>
                    ))}
                  </tr>
                )
              })}
              {adding ? (
                <tr className="dp-addrow-tr">
                  {canSelect ? <td className="dp-selcol" /> : null}
                  {columns.map((c, i) => {
                    const info = colInfo.get(c)
                    return (
                      <td key={c}>
                        <input
                          className="dp-cell-i"
                          value={values[c] ?? ''}
                          placeholder={info?.hasDefault ? 'auto' : info?.pk ? 'required' : 'default / null'}
                          title={
                            info
                              ? `${c} · ${info.pgType ?? info.type}${info.pk ? ' · pk' : ''}${info.hasDefault ? ' · auto' : ''}`
                              : c
                          }
                          autoFocus={i === 0}
                          disabled={inserting}
                          onChange={(e) => setValues((v) => ({ ...v, [c]: e.target.value }))}
                          onKeyDown={(e) => {
                            if (e.key === 'Enter') void submit()
                            else if (e.key === 'Escape') closeEditor()
                          }}
                        />
                      </td>
                    )
                  })}
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      ) : null}
      {insertErr ? <div className="dp-err">{insertErr}</div> : null}
      {deleteErr ? <div className="dp-err">{deleteErr}</div> : null}
      <div className="dp-addrow-bar">
        {adding ? (
          <>
            <button className="dp-copy" disabled={inserting} onClick={() => void submit()}>
              {inserting ? 'inserting…' : '✓ insert'}
            </button>
            <button className="dp-copy" disabled={inserting} onClick={closeEditor}>
              cancel
            </button>
          </>
        ) : (
          <>
            <button className="dp-addrow-open" disabled={loading} title="insert a row into this table" onClick={openEditor}>
              + add row
            </button>
            {selected.size > 0 ? (
              <button
                className="dp-delrow-btn"
                disabled={deleting}
                title="delete the selected rows from this table"
                onClick={() => void deleteSelected()}
              >
                {deleting ? 'deleting…' : `− delete ${selected.size} row${selected.size === 1 ? '' : 's'}`}
              </button>
            ) : null}
            {okFlash ? <span className="dp-addrow-ok">✓ inserted</span> : null}
            {delFlash > 0 ? (
              <span className="dp-delrow-ok">
                ✓ deleted {delFlash} row{delFlash === 1 ? '' : 's'}
              </span>
            ) : null}
          </>
        )}
      </div>
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
          live contents {live ? <span className="dp-live-dot" title="refetched on each change" /> : null}
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

interface TableColumn {
  name: string
  type: string
  pgType: string | null
  pk: boolean
  /** Postgres auto-supplies the value when omitted (IDENTITY / DEFAULT) → optional in the add-row form. */
  hasDefault?: boolean
}
interface TableSchemaInfo {
  table: string
  columns: TableColumn[]
  primaryKey: string[]
}

/** Format a Z-set weight with its sign (`+1`, `−1`, `+2`) using the app's unicode minus. */
function fmtWeight(w: number): string {
  return w > 0 ? `+${w}` : `−${Math.abs(w)}`
}

/** The reconstructed Z-set delta on a Δ change operator: the most-recent change for this table as
 *  weighted rows — insert (row,+1), delete (row,−1), update (old,−1)+(new,+1). The old row of an
 *  update strikes through (it is retracted). Captured client-side from the `/trace` data event the
 *  animation already consumes — no engine call. Empty until the first change on this table. */
function DeltaView({ table }: { table: string }) {
  const cap = useLatestDelta(table)
  const op = useMemo(() => {
    if (!cap) return null
    const w = cap.rows.reduce((a, r) => a + r.w, 0)
    return cap.rows.length > 1 && w === 0 ? 'update' : w > 0 ? 'insert' : w < 0 ? 'delete' : 'no-op'
  }, [cap])
  return (
    <div className="dp-contents">
      <div className="dp-sec dp-contents-h">
        <span>latest Z-set delta</span>
        {cap ? <span className="dp-contents-n">{op}</span> : null}
      </div>
      <div className="dp-note">
        The weighted rows this change becomes — insert (new,+1), delete (old,−1), update
        (old,−1)+(new,+1). This one Z-set is shared by every operator downstream.
      </div>
      {!cap ? (
        <div className="dp-empty-idx">no change seen yet — write to {table} and its delta lands here.</div>
      ) : (
        <>
          <div className="dp-table-wrap">
            <table className="dp-table dp-log dp-zset">
              <tbody>
                {cap.rows.map((r, i) => (
                  <tr key={i} className={r.w > 0 ? 'dp-op-ins' : 'dp-op-del'}>
                    <td className="dp-log-op dp-zw">{fmtWeight(r.w)}</td>
                    <td className="dp-log-row" title={JSON.stringify(r.row)}>
                      {fmtLogRow(r.row)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="dp-note">captured {new Date(cap.at).toLocaleTimeString()}</div>
        </>
      )}
    </div>
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
    // Both the source (src:<t>) and Δ change (d:<t>) operators animate under the `table:<t>` hop,
    // so the table name is the hop's suffix — used to wire the add-row form and the Z-set view.
    const opTable = node.hop.startsWith('table:') ? node.hop.slice('table:'.length) : null
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
        {/* Source operator: the table's compiled arrangements (folded onto this node on the canvas)
            as a compact list, then the row browser near the top where it is most useful, and the
            verbose per-kind explanation cards last. */}
        {node.opKind === 'op-source' && opTable ? (
          <SourceArrangements table={opTable} arr={graph.arrangements} />
        ) : null}
        {node.opKind === 'op-source' && opTable ? (
          <TableBrowser table={opTable} />
        ) : null}
        {node.opKind === 'op-source' && opTable ? (
          <SourceArrangementNotes table={opTable} arr={graph.arrangements} />
        ) : null}
        {/* Δ change operator: the reconstructed Z-set of the most recent change on this table. */}
        {node.opKind === 'op-delta' && opTable ? <DeltaView table={opTable} /> : null}
        {/* Sink operator: the shape's live materialized output (its hop is `shape:<id>`). */}
        {node.opKind === 'op-sink' ? (
          <SinkView shape={graph.shapes.find((x) => x.id === (node.hop.startsWith('shape:') ? node.hop.slice('shape:'.length) : ''))} />
        ) : null}
      </>
    )
  } else if (node.kind === 'aggshape') {
    const s = graph.shapes.find((x) => x.id === node.shapeId)
    const fn = s?.aggregate?.func.toUpperCase() ?? '?'
    const tier = s ? servingTier(s) : null
    // A circuit-served COUNT's value lives in the circuit's counts pipeline — surface which one.
    const countsPipe = s?.circuit?.counts
      ? (graph.arrangements?.counts ?? []).find((c) => c.table === s.table)
      : undefined
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
        {tier ? <Row k="serving tier" v={<code>{tier.label}</code>} /> : null}
        {countsPipe ? (
          <Row
            k="counts pipeline"
            v={<code>{`${countsPipe.id} · group by (${countsPipe.groupCols.join(', ')})`}</code>}
          />
        ) : null}
        <Row k="output" v="a single live scalar (streamed)" />
        {tier ? <div className="dp-note">{tier.note}</div> : null}
        <InsideNote kind="agg" />
        {/* Fold internals exist only for fold-maintained aggregates — a circuit-served COUNT has
            no fold executor (the deep dump 404s), its value lives in the counts pipeline. */}
        {s && !s.circuit ? <AggInternalsView nodeId={stateId} /> : null}
        {s ? <SqlBlock sql={shapeSql(s)} /> : null}
      </>
    )
  } else {
    // shape
    const s = graph.shapes.find((x) => x.id === node.shapeId)
    title = `Shape · ${node.shapeId}`
    const tier = s ? servingTier(s) : null
    body = (
      <>
        <Row k="table" v={s?.table} />
        {tier ? <Row k="serving tier" v={<code>{tier.label}</code>} /> : null}
        {s?.circuit && s.isSubquery ? <Row k="registered as" v="subquery (membership relation)" /> : null}
        <Row k="columns" v={<ColumnList columns={s?.columns ?? null} />} />
        <Row k="changes-only feed" v={s?.changesOnly ? 'yes (no backfill)' : 'no (materialized)'} />
        <Row k="stream" v={<code>{s?.streamPath}</code>} />
        {live?.kind === 'shape' ? <Row k="envelopes emitted (live)" v={live.emitted.toLocaleString()} /> : null}
        <InsideNote kind="shape" />
        {s ? <SqlBlock sql={shapeSql(s)} /> : null}
        {s ? <ShapeLiveView shape={s} /> : null}
        {tier ? <div className="dp-note">{tier.note}</div> : null}
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
