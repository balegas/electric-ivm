import { ilike, or } from '@tanstack/db'
import { useCallback, useEffect, useMemo, useState } from 'react'

import type { Filters } from '../App'
import { navigate } from '../App'
import { type Cursor, type Issue, issuesShapeDef, LIST_COLUMNS, updateIssue } from '../electric'
import { PRIORITY_RANK, type Priority, type Status } from '../schema'
import { useShapeRows } from '../lib/useShape'
import { Virtual } from '../lib/Virtual'
import { Avatar, displayId, formatDate, PriorityMenu, StatusMenu } from './ui'
import { TopFilter } from './TopFilter'

const PAGE_SIZE = 200

function IssueRow({ issue }: { issue: Issue }): JSX.Element {
  return (
    <div className="issue-row">
      <span onClick={(e) => e.stopPropagation()}>
        <PriorityMenu value={issue.priority} onChange={(priority) => updateIssue(issue, { priority })} />
      </span>
      <span onClick={(e) => e.stopPropagation()}>
        <StatusMenu value={issue.status} onChange={(status) => updateIssue(issue, { status })} />
      </span>
      <span className="issue-id">{displayId(issue.id)}</span>
      <button type="button" className="issue-title" onClick={() => navigate(`#/issue/${issue.id}`)}>
        {issue.title}
      </button>
      <span className="issue-date">{formatDate(issue.created)}</span>
      <Avatar name={issue.username} />
    </div>
  )
}

function listTitle(filters: Filters, showSearch?: boolean): string {
  if (showSearch) return 'Search'
  const s = filters.statuses
  if (s.length === 1 && s[0] === 'backlog') return 'Backlog'
  if (s.length === 2 && s.includes('todo') && s.includes('in_progress')) return 'Active'
  if (s.length === 0 && filters.priorities.length === 0) return 'All Issues'
  return 'Issues'
}

/** Sort already-loaded rows by the active order. created/modified are numeric columns; priority is a
 * rank over a TEXT enum. The pk breaks ties so the order is stable. */
function applySort(rows: Issue[], filters: Filters): Issue[] {
  const sign = filters.dir === 'asc' ? 1 : -1
  if (filters.orderBy === 'priority')
    return [...rows].sort((a, b) => sign * (PRIORITY_RANK[a.priority] - PRIORITY_RANK[b.priority]) || a.id - b.id)
  const col = filters.orderBy === 'modified' ? 'modified' : 'created'
  return [...rows].sort((a, b) => sign * (a[col] - b[col]) || a.id - b.id)
}

/** Shared presentation: fixed header + a virtualized, internally-scrolled row list. */
function ListView({
  filters,
  setFilters,
  rows,
  loading,
  showSearch,
  onEndReached,
}: {
  filters: Filters
  setFilters: (f: Filters) => void
  rows: Issue[]
  loading: boolean
  showSearch?: boolean
  onEndReached?: () => void
}): JSX.Element {
  return (
    <div className="list-pane">
      <TopFilter title={listTitle(filters, showSearch)} count={rows.length} filters={filters} setFilters={setFilters} showSearch={showSearch} />
      {loading && <div className="empty">Loading shape…</div>}
      {!loading && rows.length === 0 && <div className="empty">No issues match.</div>}
      {!loading && rows.length > 0 && (
        <Virtual
          className="issue-list-viewport"
          items={rows}
          getKey={(issue) => issue.id}
          estimateSize={41}
          renderItem={(issue) => <IssueRow issue={issue} />}
          onEndReached={onEndReached}
        />
      )}
    </div>
  )
}

// --- Search: one full-row shape (needs `description` for ilike), no pagination -----------------------
function SearchList({ filters, setFilters }: { filters: Filters; setFilters: (f: Filters) => void }): JSX.Element {
  // Strip `%`/`_`: TanStack ilike treats them as wildcards and has no ESCAPE (we want substring search).
  const q = filters.q.trim().replace(/[%_]/g, '')
  const { rows, loading } = useShapeRows<Issue>(
    issuesShapeDef(filters.statuses, filters.priorities), // full row — search matches title + description
    (b) => {
      let query = b
      if (q) query = query.where(({ t }: { t: Issue }) => or(ilike(t.title, `%${q}%`), ilike(t.description, `%${q}%`)))
      return query.select(({ t }: { t: Issue }) => t)
    },
    [q],
  )
  return <ListView filters={filters} setFilters={setFilters} rows={applySort(rows, filters)} loading={loading} showSearch />
}

// --- Browse: cursor/range pagination — sync one page at a time, load more on scroll -----------------

/** The (window-column value, pk) of the row that ends a page's window order — the next page's cursor.
 * The collection stringifies the pk, so coerce to Number: the cursor predicate targets the int `id`
 * column (a string would be rejected) and the boundary must compare ids numerically, not lexically. */
function pageBoundary(rows: Issue[], col: 'created' | 'modified', desc: boolean): Cursor | undefined {
  if (rows.length === 0) return undefined
  let bv = Number(rows[0]![col])
  let bid = Number(rows[0]!.id)
  for (const r of rows) {
    const rv = Number(r[col])
    const rid = Number(r.id)
    const better = desc ? rv < bv || (rv === bv && rid < bid) : rv > bv || (rv === bv && rid > bid)
    if (better) {
      bv = rv
      bid = rid
    }
  }
  return { col, val: bv, pk: bid }
}

/** A single page: subscribes to one windowed shape and reports its rows up. Renders nothing. */
function IssuePage({
  index,
  statuses,
  priorities,
  windowCol,
  windowDesc,
  after,
  report,
}: {
  index: number
  statuses: Status[]
  priorities: Priority[]
  windowCol: 'created' | 'modified'
  windowDesc: boolean
  after: Cursor | undefined
  report: (index: number, rows: Issue[]) => void
}): null {
  const def = useMemo(
    () => issuesShapeDef(statuses, priorities, LIST_COLUMNS, { col: windowCol, desc: windowDesc, limit: PAGE_SIZE, after }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [JSON.stringify([statuses, priorities, windowCol, windowDesc, after])],
  )
  const { rows } = useShapeRows<Issue>(def)
  useEffect(() => report(index, rows), [report, index, rows])
  return null
}

function BrowseList({ filters, setFilters }: { filters: Filters; setFilters: (f: Filters) => void }): JSX.Element {
  // Window/scroll axis: the active date column when sorting by one, else `created`. (Priority has no
  // numeric column to page by, so we page by created and sort the loaded rows by priority for display.)
  const dateSort = filters.orderBy === 'created' || filters.orderBy === 'modified'
  const windowCol: 'created' | 'modified' = filters.orderBy === 'modified' ? 'modified' : 'created'
  const windowDesc = dateSort ? filters.dir === 'desc' : true
  const resetKey = JSON.stringify([filters.statuses, filters.priorities, windowCol, windowDesc])

  // One cursor per loaded page (page 0 has none); `pages[i]` holds page i's rows once it reports.
  const [cursors, setCursors] = useState<(Cursor | undefined)[]>([undefined])
  const [pages, setPages] = useState<Issue[][]>([])
  useEffect(() => {
    setCursors([undefined])
    setPages([])
  }, [resetKey])

  const report = useCallback((i: number, rows: Issue[]) => {
    setPages((prev) => {
      if (prev[i] === rows) return prev
      const next = prev.slice()
      next[i] = rows
      return next
    })
  }, [])

  // Merge pages (dedupe by id — a row can shift across a boundary on a live edit), then sort for display.
  const merged = useMemo(() => {
    const byId = new Map<number, Issue>()
    for (const pg of pages) if (pg) for (const r of pg) byId.set(r.id, r)
    return [...byId.values()]
  }, [pages])
  const sorted = applySort(merged, filters)

  const loadedPages = pages.filter(Boolean).length
  const lastPage = pages[cursors.length - 1]
  const hasMore = !!lastPage && lastPage.length >= PAGE_SIZE

  const loadMore = useCallback(() => {
    setCursors((prev) => {
      // Advance only when every requested page has reported and the last page was full (more to come).
      if (pages.length < prev.length) return prev
      const last = pages[prev.length - 1]
      if (!last || last.length < PAGE_SIZE) return prev
      const next = pageBoundary(last, windowCol, windowDesc)
      return next ? [...prev, next] : prev
    })
  }, [pages, windowCol, windowDesc])

  return (
    <>
      {cursors.map((c, i) => (
        <IssuePage
          key={i}
          index={i}
          statuses={filters.statuses}
          priorities={filters.priorities}
          windowCol={windowCol}
          windowDesc={windowDesc}
          after={c}
          report={report}
        />
      ))}
      <ListView
        filters={filters}
        setFilters={setFilters}
        rows={sorted}
        loading={loadedPages === 0}
        onEndReached={hasMore ? loadMore : undefined}
      />
    </>
  )
}

export function IssueList({
  filters,
  setFilters,
  showSearch,
}: {
  filters: Filters
  setFilters: (f: Filters) => void
  showSearch?: boolean
  onNewIssue: () => void
}): JSX.Element {
  return showSearch ? (
    <SearchList filters={filters} setFilters={setFilters} />
  ) : (
    <BrowseList filters={filters} setFilters={setFilters} />
  )
}
