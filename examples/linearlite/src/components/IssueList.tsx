import { ilike, or } from '@tanstack/db'
import { useCallback, useEffect, useMemo, useState } from 'react'

import type { Filters } from '../App'
import { navigate } from '../App'
import { type Issue, type IssueQuery, issuesShapeDef, projectIssuesSubsetDef, updateIssue } from '../electric'
import { PRIORITY_RANK } from '../schema'
import { useCurrentUser } from '../lib/CurrentUser'
import { useShapeRows, useSubset } from '../lib/useShape'
import { Virtual } from '../lib/Virtual'
import { Avatar, displayId, formatDate, PriorityMenu, ProjectBadge, StatusMenu } from './ui'
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
  const { projectById } = useCurrentUser()
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
      <ProjectBadge project={projectById.get(issue.project_id)} />
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

interface FeedState {
  rows: Issue[]
  loadMore: () => void
  hasMore: boolean
}

/**
 * One project's subset feed (`project_id = P`), paginated + live. Renders nothing — it reports its
 * loaded rows + paging controls up to {@link BrowseList} via `register`. Kept mounted for every project
 * the user belongs to (regardless of the active filter) so switching project/status is instant client
 * work and the underlying engine feed is reused across users and filter changes.
 */
function ProjectSubsetFeed({
  projectId,
  orderCol,
  desc,
  register,
}: {
  projectId: number
  orderCol: 'created' | 'modified'
  desc: boolean
  register: (projectId: number, state: FeedState | null) => void
}): JSX.Element | null {
  const { rows, loadMore, hasMore } = useSubset<Issue>(
    projectIssuesSubsetDef(projectId, { col: orderCol, desc }),
    (b) =>
      b
        .orderBy(({ t }: { t: Issue }) => t[orderCol], desc ? 'desc' : 'asc')
        .orderBy(({ t }: { t: Issue }) => t.id, 'asc')
        .select(({ t }: { t: Issue }) => t),
    [orderCol, desc],
  )
  // Report state up; on unmount (lost membership) clear this feed from the merge.
  useEffect(() => {
    register(projectId, { rows, loadMore, hasMore })
    return () => register(projectId, null)
  }, [projectId, rows, loadMore, hasMore, register])
  return null
}

/**
 * Browse the visible issues by **merging one per-project subset feed per project the user belongs to**.
 * Each feed is a paginated query-back (never materialized — scales to 100k+ issues), reused across users
 * and filter changes. Project/status/priority/my-tasks selection and ordering happen on the client over
 * the merged loaded window, so switching filters is instant with no new engine request. Scrolling pages
 * every active feed forward together.
 */
function BrowseList({ filters, setFilters }: { filters: Filters; setFilters: (f: Filters) => void }): JSX.Element {
  const { myProjectIds, currentUserName } = useCurrentUser()
  const memberIds = useMemo(() => [...myProjectIds].sort((a, b) => a - b), [myProjectIds])
  const orderCol: 'created' | 'modified' = filters.orderBy === 'modified' ? 'modified' : 'created'
  const desc = filters.dir === 'desc'

  const [feeds, setFeeds] = useState<Map<number, FeedState>>(new Map())
  const register = useCallback((projectId: number, state: FeedState | null) => {
    setFeeds((prev) => {
      const next = new Map(prev)
      if (state) next.set(projectId, state)
      else next.delete(projectId)
      return next
    })
  }, [])

  // Which projects feed the current view: one when a project is selected, else all member projects.
  const activeIds = filters.projectId ? [filters.projectId] : memberIds

  const rendered = useMemo(() => {
    let all: Issue[] = []
    for (const pid of activeIds) all = all.concat(feeds.get(pid)?.rows ?? [])
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
  }, [feeds, activeIds, filters.statuses, filters.priorities, filters.myTasksOnly, filters.orderBy, orderCol, desc, currentUserName])

  const hasMore = activeIds.some((pid) => feeds.get(pid)?.hasMore)
  const loadMore = () => {
    for (const pid of activeIds) {
      const f = feeds.get(pid)
      if (f?.hasMore) f.loadMore()
    }
  }

  return (
    <>
      {memberIds.map((pid) => (
        <ProjectSubsetFeed key={pid} projectId={pid} orderCol={orderCol} desc={desc} register={register} />
      ))}
      <ListChrome
        filters={filters}
        setFilters={setFilters}
        rows={rendered}
        loading={feeds.size === 0 && memberIds.length > 0}
        onEndReached={() => {
          if (hasMore) loadMore()
        }}
      />
    </>
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
