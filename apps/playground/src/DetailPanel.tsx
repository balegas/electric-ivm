// Click-to-inspect panel for pipeline nodes — the playground edition of pipeline-viz's
// DetailPanel. Same explanations and SQL; data access goes through the playground server's
// workspace-scoped API, and live contents are shown only for the caller's own shapes (other
// visitors' shape metadata is public introspection, their rows are not).

import { useEffect, useState, type ReactNode } from 'react'

import type { NodeRef } from '@viz/build-graph'
import { predicateLabel } from '@viz/predicate-label'
import { nodeInnerSql, shapeSql } from '@viz/shape-sql'
import type { EngineGraph } from '@viz/types'

import { api } from './api.ts'

const CONTENTS_LIMIT = 50

function fmtCell(v: unknown): string {
  if (v === null || v === undefined) return '∅'
  const s = String(v)
  return s.length > 48 ? `${s.slice(0, 48)}…` : s
}

/** Live-preview one of OUR shapes' rows (polls the workspace-scoped rows proxy). */
function ShapeContentsView({ workspaceId, shapeId }: { workspaceId: string; shapeId: string }) {
  const [state, setState] = useState<{
    rows: { key: string; value: Record<string, unknown> }[]
    count: number
    changesOnly: boolean
    error: string | null
    loading: boolean
  }>({ rows: [], count: 0, changesOnly: false, error: null, loading: true })

  useEffect(() => {
    let alive = true
    const poll = async () => {
      try {
        const data = await api.shapeRows(workspaceId, shapeId, CONTENTS_LIMIT)
        if (alive) setState({ rows: data.rows, count: data.count, changesOnly: data.changesOnly, error: null, loading: false })
      } catch (e) {
        if (alive) setState((s) => ({ ...s, error: String((e as Error).message ?? e), loading: false }))
      }
    }
    void poll()
    const t = setInterval(() => void poll(), 2000)
    return () => {
      alive = false
      clearInterval(t)
    }
  }, [workspaceId, shapeId])

  const columns = state.rows.length ? Object.keys(state.rows[0]!.value) : []
  return (
    <div className="dp-contents">
      <div className="dp-sec dp-contents-h">
        <span>
          live contents {!state.error ? <span className="dp-live-dot" title="polling every 2s" /> : null}
        </span>
        <span className="dp-contents-n">
          {state.loading ? 'loading…' : `${state.count.toLocaleString()} row${state.count === 1 ? '' : 's'}`}
        </span>
      </div>
      {state.error ? <div className="dp-err">{state.error}</div> : null}
      {!state.error && !state.loading && state.count === 0 ? <div className="dp-empty-idx">set is empty</div> : null}
      {state.rows.length > 0 ? (
        <div className="dp-table-wrap">
          <table className="dp-table">
            <thead>
              <tr>{columns.map((c) => <th key={c}>{c}</th>)}</tr>
            </thead>
            <tbody>
              {state.rows.map((r) => (
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
      {state.count > state.rows.length ? (
        <div className="dp-note">showing first {state.rows.length} of {state.count.toLocaleString()} — updates live</div>
      ) : null}
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

interface NodeIndexData {
  distinctValues: number
  refcount: number
  values: { value: unknown; contributors: number }[]
  truncated: boolean
}

export function DetailPanel({
  node,
  graph,
  workspaceId,
  mine,
  onClose,
  onSelectShape,
}: {
  node: NodeRef
  graph: EngineGraph
  workspaceId: string
  /** Shape ids belonging to this workspace — only these get live contents / links. */
  mine: string[]
  onClose: () => void
  onSelectShape: (id: string) => void
}) {
  const [index, setIndex] = useState<NodeIndexData | null>(null)
  const [indexErr, setIndexErr] = useState<string | null>(null)
  const isMine = (id: string) => mine.includes(id)

  // For subquery nodes, fetch the live inner-set index and keep it fresh while the panel is open.
  useEffect(() => {
    if (node.kind !== 'sqnode') {
      setIndex(null)
      return
    }
    let alive = true
    const fetchIdx = async () => {
      try {
        const idx = await api.nodeIndex(workspaceId, node.sig)
        if (alive) {
          setIndex(idx)
          setIndexErr(null)
        }
      } catch (e) {
        if (alive) setIndexErr(String((e as Error).message ?? e))
      }
    }
    void fetchIdx()
    const t = setInterval(() => void fetchIdx(), 2500)
    return () => {
      alive = false
      clearInterval(t)
    }
  }, [node, workspaceId])

  /** A shape id: a focus link for your own shapes, plain text for other visitors'. */
  const shapeRef = (id: string) =>
    isMine(id) ? (
      <button className="dp-shape-link" onClick={() => onSelectShape(id)}>
        {id}
      </button>
    ) : (
      <code className="dp-key" title="another visitor's shape">
        {id}
      </code>
    )

  let title = ''
  let body: ReactNode = null

  if (node.kind === 'table') {
    title = `Table · ${node.name}`
    const onTable = graph.shapes.filter((s) => s.table === node.name)
    const mineOnTable = onTable.filter((s) => isMine(s.id))
    body = (
      <>
        <Row k="role" v="replication source — every committed write becomes a change event" />
        <Row k="shapes on it" v={`${onTable.length} engine-wide · ${mineOnTable.length} yours`} />
        <div className="dp-sec">your shapes reading this table</div>
        <div className="dp-list">
          {mineOnTable.map((s) => (
            <div key={s.id} className="dp-item">
              {shapeRef(s.id)}
              <span className="dp-item-sub">{predicateLabel(s.where)}</span>
            </div>
          ))}
        </div>
      </>
    )
  } else if (node.kind === 'family') {
    title = `Router · (${node.keyCols.join(', ')})`
    const members = graph.shapes.filter(
      (s) => s.table === node.table && s.familyKey && s.familyKey.join(',') === node.keyCols.join(','),
    )
    body = (
      <>
        <Row k="table" v={node.table} />
        <Row k="key columns" v={node.keyCols.join(', ')} />
        <Row k="routing" v="key tuple → shapes (one lookup per change, no table copy)" />
        <Row k="member shapes" v={members.length} />
        <div className="dp-sec">routing index · key → shape</div>
        <div className="dp-list">
          {members.map((s) => (
            <div key={s.id} className="dp-item">
              <code className="dp-key">{predicateLabel(s.where)}</code>
              <span className="dp-arrow">→</span>
              {shapeRef(s.id)}
            </div>
          ))}
        </div>
        <div className="dp-note">
          All {members.length} shapes above share this ONE router — including other visitors' (that's
          the honest multi-tenancy: same columns, different key values). A change is routed by its key
          to exactly the matching shape(s).
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
        <Row k="type" v="standalone — stateless (no index)" />
        {s ? <SqlBlock sql={shapeSql(s)} /> : null}
        {s && isMine(s.id) ? <ShapeContentsView workspaceId={workspaceId} shapeId={s.id} /> : null}
        <div className="dp-note">
          Non-equality predicate: evaluated directly on each change delta. It holds no state — the
          cost is one predicate check per change.
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
          {index?.truncated ? ' (truncated)' : ''}
        </div>
        {indexErr ? <div className="dp-err">{indexErr}</div> : null}
        <div className="dp-list dp-index">
          {index?.values.map((v, i) => (
            <div key={i} className="dp-item">
              <code className="dp-key">{fmtValue(v.value)}</code>
              <span className="dp-badge-n">
                {v.contributors} row{v.contributors === 1 ? '' : 's'}
              </span>
            </div>
          ))}
          {index && index.values.length === 0 ? <div className="dp-empty-idx">set is empty</div> : null}
        </div>
        <div className="dp-sec">dependents</div>
        <div className="dp-list">
          {deps.map((e, i) => (
            <div key={i} className="dp-item">
              {e.dependentKind === 'shape' ? shapeRef(e.dependentId) : (
                <span className="dp-item-sub">node {e.dependentId.slice(0, 24)}…</span>
              )}
              <span className="dp-item-sub">
                {e.negated ? 'NOT IN' : 'IN'} via {e.connectingCol}
              </span>
            </div>
          ))}
        </div>
        <div className="dp-note">
          This one maintained set is shared by every dependent above. When a value enters or leaves
          it, the affected outer rows move in/out of the dependent shapes live — that's the scene-4
          cascade.
        </div>
      </>
    )
  } else if (node.kind === 'aggshape') {
    const s = graph.shapes.find((x) => x.id === node.shapeId)
    const fn = s?.aggregate?.func.toUpperCase() ?? '?'
    title = `Aggregation · ${node.shapeId}`
    body = (
      <>
        <Row k="function" v={<code>{`${fn}(${s?.aggregate?.col ?? '*'})`}</code>} />
        <Row k="table" v={s?.table} />
        <Row k="output" v="a single live scalar (streamed)" />
        {s ? <SqlBlock sql={shapeSql(s)} /> : null}
        <div className="dp-note">
          Maintained incrementally as a <b>fold</b>: each change that enters/leaves the filter adjusts
          the running value — no rows are stored, and the number is never re-computed from scratch.
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
          stream of weighted changes (dashed = a stateful arrangement feeding a join). Anything shared
          underneath appears once.
        </div>
      </>
    )
  } else {
    // shape
    const s = graph.shapes.find((x) => x.id === node.shapeId)
    title = `Live query · ${node.shapeId}`
    const kind = s?.isSubquery
      ? 'subquery'
      : s?.familyKey
        ? `routed by (${s.familyKey.join(', ')})`
        : 'standalone filter'
    body = (
      <>
        <Row k="table" v={s?.table} />
        <Row k="routing" v={kind} />
        <Row k="owner" v={s && isMine(s.id) ? 'you' : 'another visitor'} />
        {s ? <SqlBlock sql={shapeSql(s)} /> : null}
        {s && isMine(s.id) ? (
          <ShapeContentsView workspaceId={workspaceId} shapeId={s.id} />
        ) : (
          <div className="dp-note">Another visitor's shape — its metadata is public introspection, its rows are private.</div>
        )}
        <div className="dp-note">
          {s?.isSubquery
            ? 'Membership is driven by the shared subquery node(s) upstream plus the outer-row filter.'
            : s?.familyKey
              ? 'Routed by key through a shared router — the engine keeps only per-query metadata, no table rows.'
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
