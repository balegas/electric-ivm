import { ilike, or } from '@tanstack/db'
import { useMemo } from 'react'

import type { Filters } from '../App'
import { navigate } from '../App'
import { type Issue, issuesShapeDef, issuesSubsetDef, LIST_COLUMNS, updateIssue } from '../electric'
import { PRIORITY_RANK } from '../schema'
import { useShapeRows, useSubset } from '../lib/useShape'
import { Virtual } from '../lib/Virtual'
import { Avatar, displayId, formatDate, PriorityMenu, StatusMenu } from './ui'
import { TopFilter } from './TopFilter'

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

/** Shared list chrome: header + virtualized rows. `onEndReached` drives subset "load more". */
function ListChrome({
  filters,
  setFilters,
  showSearch,
  rows,
  loading,
  onEndReached,
}: {
  filters: Filters
  setFilters: (f: Filters) => void
  showSearch?: boolean
  rows: Issue[]
  loading: boolean
  onEndReached?: () => void
}): JSX.Element {
  return (
    <div className="list-pane">
      <TopFilter title={listTitle(filters, showSearch)} count={rows.length} filters={filters} setFilters={setFilters} showSearch={showSearch} />
      {loading && <div className="empty">Loading…</div>}
      {!loading && rows.length === 0 && <div className="empty">No issues match.</div>}
      {!loading && rows.length > 0 && (
        // Only the visible rows are mounted (see Virtual): renders ~30 nodes instead of one per issue.
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

/**
 * Browse the issues as a **subset query**: the first page is query-backed from Postgres and the loaded
 * window stays live by following the table's tail — no materialized shape, so the engine never holds
 * the 20k rows. Scrolling to the bottom pages in the next chunk (another query-back). Ordering pages by
 * a real column (`created`/`modified`); a `priority` selection re-sorts the loaded window client-side.
 */
function BrowseList({ filters, setFilters }: { filters: Filters; setFilters: (f: Filters) => void }): JSX.Element {
  const dir = filters.dir
  const orderCol: 'created' | 'modified' = filters.orderBy === 'modified' ? 'modified' : 'created'
  const { rows, loading, loadMore, hasMore } = useSubset<Issue>(
    issuesSubsetDef(filters.statuses, filters.priorities, { col: orderCol, desc: dir === 'desc' }, LIST_COLUMNS),
  )

  // The subset collection is an unordered set; impose the display order over the loaded window.
  const sign = dir === 'asc' ? 1 : -1
  const sorted = useMemo(() => {
    const arr = [...rows]
    if (filters.orderBy === 'priority') {
      arr.sort((a, b) => sign * (PRIORITY_RANK[a.priority] - PRIORITY_RANK[b.priority]) || a.id - b.id)
    } else {
      arr.sort((a, b) => sign * ((a[orderCol] as number) - (b[orderCol] as number)) || a.id - b.id)
    }
    return arr
  }, [rows, filters.orderBy, sign, orderCol])

  return (
    <ListChrome
      filters={filters}
      setFilters={setFilters}
      rows={sorted}
      loading={loading}
      onEndReached={() => {
        if (hasMore) loadMore()
      }}
    />
  )
}

/**
 * Search across issues. Search must match on `description`, so it uses the full materialized shape
 * (not the projected subset) and refines client-side. This is the deliberate counterpart to browse:
 * shapes for "sync this set", subset queries for "page through this set".
 */
function SearchList({ filters, setFilters }: { filters: Filters; setFilters: (f: Filters) => void }): JSX.Element {
  // Strip `%`/`_` from the search term: TanStack DB's ilike treats them as wildcards and has no
  // ESCAPE, so a literal `%`/`_` would otherwise match everything. (We want plain substring search.)
  const q = filters.q.trim().replace(/[%_]/g, '')
  const dir = filters.dir
  const dateSort = filters.orderBy === 'created' || filters.orderBy === 'modified'
  const { rows, loading } = useShapeRows<Issue>(
    issuesShapeDef(filters.statuses, filters.priorities),
    (b) => {
      let query = b
      if (q) query = query.where(({ t }: { t: Issue }) => or(ilike(t.title, `%${q}%`), ilike(t.description, `%${q}%`)))
      if (dateSort)
        query = query
          .orderBy(({ t }: { t: Issue }) => t[filters.orderBy as 'created' | 'modified'], dir)
          .orderBy(({ t }: { t: Issue }) => t.id, 'asc')
      return query.select(({ t }: { t: Issue }) => t)
    },
    [q, filters.orderBy, dir],
  )

  const sign = dir === 'asc' ? 1 : -1
  const sorted =
    filters.orderBy === 'priority'
      ? [...rows].sort((a, b) => sign * (PRIORITY_RANK[a.priority] - PRIORITY_RANK[b.priority]) || a.id - b.id)
      : rows

  return <ListChrome filters={filters} setFilters={setFilters} showSearch rows={sorted} loading={loading} />
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
