import { ilike, or } from '@tanstack/db'

import type { Filters } from '../App'
import { navigate } from '../App'
import { type Issue, issuesShapeDef, updateIssue } from '../electric'
import { PRIORITY_RANK } from '../schema'
import { useShapeRows } from '../lib/useShape'
import { Avatar, displayId, formatDate, PriorityMenu, StatusMenu } from './ui'
import { TopFilter } from './TopFilter'

function listTitle(filters: Filters, showSearch?: boolean): string {
  if (showSearch) return 'Search'
  const s = filters.statuses
  if (s.length === 1 && s[0] === 'backlog') return 'Backlog'
  if (s.length === 2 && s.includes('todo') && s.includes('in_progress')) return 'Active'
  if (s.length === 0 && filters.priorities.length === 0) return 'All Issues'
  return 'Issues'
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
  // The engine evaluates the status/priority predicate (the shape). Search and ordering are pushed
  // into the live query — incrementally maintained over the synced shape, not re-run in JS each change.
  // (Search is a client-side query refinement, so it doesn't re-sync the engine-side shape.)
  // Strip `%`/`_` from the search term: TanStack DB's ilike treats them as wildcards and has no
  // ESCAPE, so a literal `%`/`_` would otherwise match everything. (We want plain substring search.)
  const q = filters.q.trim().replace(/[%_]/g, '')
  const dir = filters.dir
  // created/modified are real numeric columns → order in-query; priority is a rank over a TEXT enum
  // (no integer column), so that one ordering is applied client-side below.
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

  return (
    <>
      <TopFilter title={listTitle(filters, showSearch)} count={sorted.length} filters={filters} setFilters={setFilters} showSearch={showSearch} />
      <div className="issue-list">
        {loading && <div className="empty">Loading shape…</div>}
        {!loading && sorted.length === 0 && <div className="empty">No issues match.</div>}
        {sorted.map((issue) => (
          <div key={issue.id} className="issue-row">
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
        ))}
      </div>
    </>
  )
}
