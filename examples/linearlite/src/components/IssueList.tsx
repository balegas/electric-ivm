import { ilike, or } from '@tanstack/db'
import { useMemo } from 'react'

import type { AggregateDef, Predicate } from '@electric-circuits/protocol'

import type { Filters } from '../App'
import { navigate } from '../App'
import {
  type Issue,
  type IssueQuery,
  issuesShapeDef,
  issuesSubsetDef,
  LIST_COLUMNS,
  moveIssue,
  projectIssuesSubsetDef,
  updateIssue,
} from '../electric'
import { PRIORITY_RANK } from '../schema'
import { useCurrentUser } from '../lib/CurrentUser'
import { useAggregate, useShapeRows, useSubset } from '../lib/useShape'

/** Build a browse-view filter predicate (project visibility + status/priority/my-tasks) for the counter. */
function inList(col: string, vals: (string | number)[]): Predicate {
  return vals.length === 1 ? { col, op: 'eq', value: vals[0] } : { or: vals.map((v) => ({ col, op: 'eq', value: v })) }
}
function buildBrowseWhere(projectIds: number[], filters: Filters, userName: string): Predicate {
  const clauses: Predicate[] = [inList('project_id', projectIds)]
  if (filters.statuses.length) clauses.push(inList('status', filters.statuses))
  if (filters.priorities.length) clauses.push(inList('priority', filters.priorities))
  if (filters.myTasksOnly) clauses.push({ col: 'username', op: 'eq', value: userName })
  return clauses.length === 1 ? clauses[0] : { and: clauses }
}
import { Virtual } from '../lib/Virtual'
import { Avatar, displayId, formatDate, PriorityMenu, ProjectMenu, StatusMenu } from './ui'
import { TopFilter } from './TopFilter'

/** Derive the engine-side issue query (visibility + filters) from the UI filters and the current user. */
function toIssueQuery(filters: Filters, userId: number, userName: string): IssueQuery {
  return {
    userId,
    userName,
    statuses: filters.statuses,
    priorities: filters.priorities,
    projectId: filters.projectId,
    myTasksOnly: filters.myTasksOnly,
  }
}

function IssueRow({ issue }: { issue: Issue }): JSX.Element {
  const { projects } = useCurrentUser()
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
      <span onClick={(e) => e.stopPropagation()}>
        <ProjectMenu value={issue.project_id} projects={projects} onChange={(project_id) => moveIssue(issue, project_id)} />
      </span>
      <span className="issue-date">{formatDate(issue.created)}</span>
      <Avatar name={issue.username} />
    </div>
  )
}

function listTitle(filters: Filters, showSearch?: boolean): string {
  if (showSearch) return 'Search'
  if (filters.myTasksOnly) return 'My Tasks'
  if (filters.projectId !== null) return 'Project'
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
  count,
}: {
  filters: Filters
  setFilters: (f: Filters) => void
  showSearch?: boolean
  rows: Issue[]
  loading: boolean
  onEndReached?: () => void
  /** Server-maintained COUNT of the matching set; falls back to the loaded-rows length when absent. */
  count?: number | null
}): JSX.Element {
  return (
    <div className="list-pane">
      <TopFilter
        title={listTitle(filters, showSearch)}
        count={count ?? rows.length}
        filters={filters}
        setFilters={setFilters}
        showSearch={showSearch}
      />
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
 * Browse the visible issues. The all-projects view is **one subquery-backed subset** — the engine
 * evaluates `project_id IN (SELECT project_id FROM project_members WHERE user_id = me)` through its
 * shared subquery node, so visibility lives server-side and membership changes re-scope the feed.
 * Selecting a single project uses that project's plain `project_id = P` subset (shared across users).
 * Status/priority/my-tasks selection and ordering happen on the client over the loaded window, so
 * switching filters is instant with no new engine request.
 *
 * Exactly one subset feed drives the view at a time, so it is consumed directly with `useSubset`
 * (the hook re-keys on the def, so switching project/user/order swaps the engine feed). Do NOT
 * mirror the feed's rows into component state via an effect: the live tail applies row changes
 * one-by-one, and an effect→setState per row forms an unbroken nested-update chain that trips
 * React's "Maximum update depth exceeded" warning during membership Join/Leave churn.
 */
function BrowseList({ filters, setFilters }: { filters: Filters; setFilters: (f: Filters) => void }): JSX.Element {
  const { myProjectIds, currentUserId, currentUserName } = useCurrentUser()
  const memberIds = useMemo(() => [...myProjectIds].sort((a, b) => a - b), [myProjectIds])
  const orderCol: 'created' | 'modified' = filters.orderBy === 'modified' ? 'modified' : 'created'
  const desc = filters.dir === 'desc'

  // The active feed: the selected project's subset, else the all-visible-issues subquery subset.
  const def = filters.projectId
    ? projectIssuesSubsetDef(filters.projectId, { col: orderCol, desc })
    : issuesSubsetDef(
        { userId: currentUserId, userName: currentUserName, statuses: [], priorities: [], projectId: null, myTasksOnly: false },
        { col: orderCol, desc },
        LIST_COLUMNS,
      )
  const { rows, loading, loadMore, hasMore } = useSubset<Issue>(
    def,
    (b) =>
      b
        .orderBy(({ t }: { t: Issue }) => t[orderCol], desc ? 'desc' : 'asc')
        .orderBy(({ t }: { t: Issue }) => t.id, 'asc')
        .select(({ t }: { t: Issue }) => t),
    [orderCol, desc],
  )

  const rendered = useMemo(() => {
    let all: Issue[] = [...rows]
    if (filters.statuses.length) all = all.filter((i) => filters.statuses.includes(i.status))
    if (filters.priorities.length) all = all.filter((i) => filters.priorities.includes(i.priority))
    if (filters.myTasksOnly) all = all.filter((i) => i.username === currentUserName)
    all.sort((a, b) => {
      const d =
        filters.orderBy === 'priority'
          ? PRIORITY_RANK[a.priority] - PRIORITY_RANK[b.priority]
          : (a[orderCol] as number) - (b[orderCol] as number)
      return (desc ? -d : d) || a.id - b.id
    })
    return all
  }, [rows, filters.statuses, filters.priorities, filters.myTasksOnly, filters.orderBy, orderCol, desc, currentUserName])

  // The header count is a real **server-maintained COUNT aggregation** over the visible+filtered set —
  // the true total, updating live on writes — not the length of the client-loaded (paginated) window.
  // Aggregations don't take subquery predicates, so the count keeps the expanded member-project list.
  const aggProjects = filters.projectId ? [filters.projectId] : memberIds
  const aggDef: AggregateDef | null = aggProjects.length
    ? { table: 'issues', where: buildBrowseWhere(aggProjects, filters, currentUserName), fn: 'count' }
    : null
  const agg = useAggregate(aggDef)

  return (
    <ListChrome
      filters={filters}
      setFilters={setFilters}
      rows={rendered}
      count={aggDef ? agg.value : 0}
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
  const { currentUserId, currentUserName } = useCurrentUser()
  const q = filters.q.trim().replace(/[%_]/g, '')
  const dir = filters.dir
  const dateSort = filters.orderBy === 'created' || filters.orderBy === 'modified'
  const { rows, loading } = useShapeRows<Issue>(
    issuesShapeDef(toIssueQuery(filters, currentUserId, currentUserName)),
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
