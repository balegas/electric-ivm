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
  // The engine evaluates the status/priority predicate; search is applied client-side (no LIKE here).
  const { rows, loading } = useShapeRows<Issue>(issuesShapeDef(filters.statuses, filters.priorities))

  const q = filters.q.trim().toLowerCase()
  const filtered = q
    ? rows.filter((r) => r.title.toLowerCase().includes(q) || r.description.toLowerCase().includes(q))
    : rows

  const sign = filters.dir === 'asc' ? 1 : -1
  const sorted = [...filtered].sort((a, b) => {
    let d: number
    if (filters.orderBy === 'priority') d = PRIORITY_RANK[a.priority] - PRIORITY_RANK[b.priority]
    else d = a[filters.orderBy] - b[filters.orderBy]
    return d !== 0 ? sign * d : a.id - b.id
  })

  return (
    <>
      <TopFilter title={listTitle(filters, showSearch)} count={filtered.length} filters={filters} setFilters={setFilters} showSearch={showSearch} />
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
