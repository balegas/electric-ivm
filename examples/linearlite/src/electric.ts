import { createClient } from '@electric-ivm/client'
import type { Predicate, ShapeDef, SubsetDef } from '@electric-ivm/protocol'
import { type Priority, schema, type Status } from './schema'

// The browser talks to the API and reads shape streams through the Vite dev proxy
// (/api -> tRPC API, /ds -> durable-streams). long-poll is the most proxy-friendly live mode.
const origin = window.location.origin
export const client = createClient({
  apiUrl: `${origin}/api`,
  schema,
  dsBaseUrl: `${origin}/ds`,
  liveMode: 'long-poll',
})

export interface Issue {
  id: number
  title: string
  description: string
  status: Status
  priority: Priority
  username: string
  project_id: number
  created: number
  modified: number
  kanbanorder: number
}

export interface Project {
  id: number
  name: string
  color: string
}

export interface User {
  id: number
  name: string
}

export interface ProjectMember {
  id: number
  project_id: number
  user_id: number
}

export interface Comment {
  id: number
  issue_id: number
  body: string
  username: string
  created: number
}

// Collision-free ids: seeded rows are 1..N, and Date.now() is far above that. Monotonic within the
// session so rapid creates never collide.
let _id = Date.now()
export const genId = () => ++_id

// --- Writes: everything goes to Postgres (the system of record) via the dev /pg/write middleware ---
// Writes are fire-and-forget from the UI (the live shape reflects the result), but failures are
// surfaced rather than silently swallowed.
async function pgWrite(body: { table: string; op: 'insert' | 'update' | 'delete'; pk: number; row?: object }) {
  try {
    const res = await fetch('/pg/write', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
    })
    if (!res.ok) {
      const detail = await res.text().catch(() => '')
      console.error(`pg/write ${body.op} ${body.table} failed: ${res.status} ${detail}`)
    }
  } catch (e) {
    console.error(`pg/write ${body.op} ${body.table} failed:`, e)
  }
}

export function createIssue(
  fields: Pick<Issue, 'title' | 'description' | 'status' | 'priority' | 'project_id'>,
  username: string,
): Issue {
  const now = Date.now()
  const issue: Issue = {
    id: genId(),
    username,
    created: now,
    modified: now,
    kanbanorder: now, // append to the end of its column; reorders adjust this
    ...fields,
  }
  void pgWrite({ table: 'issues', op: 'insert', pk: issue.id, row: issue })
  return issue
}

/** Join/leave a project: insert/delete a `project_members` row. Drives live visibility changes. */
export function joinProject(memberId: number, projectId: number, userId: number) {
  void pgWrite({ table: 'project_members', op: 'insert', pk: memberId, row: { id: memberId, project_id: projectId, user_id: userId } })
}

export function leaveProject(memberId: number) {
  void pgWrite({ table: 'project_members', op: 'delete', pk: memberId })
}

export function updateIssue(issue: Issue, patch: Partial<Issue>) {
  // Send only the changed columns (a partial update). List/board rows sync a projected subset of
  // columns (no `description`), so writing the whole row back would null the omitted columns.
  void pgWrite({ table: 'issues', op: 'update', pk: issue.id, row: { ...patch, modified: Date.now() } })
}

/** Move an issue to another project. This changes the issue's **visibility** (the per-project subset
 * feeds and the `project_id IN (SELECT …)` visibility subquery), so it live-moves in/out of members'
 * views — a good demonstration of incremental move-in/move-out. */
export function moveIssue(issue: Issue, projectId: number) {
  updateIssue(issue, { project_id: projectId })
}

export function deleteIssue(id: number) {
  void pgWrite({ table: 'issues', op: 'delete', pk: id })
}

export function addComment(issueId: number, body: string, username: string) {
  const comment: Comment = { id: genId(), issue_id: issueId, body, username, created: Date.now() }
  void pgWrite({ table: 'comments', op: 'insert', pk: comment.id, row: comment })
}

export function deleteComment(id: number) {
  void pgWrite({ table: 'comments', op: 'delete', pk: id })
}

// --- Shape definitions (the engine evaluates these predicates) -------------------------------------
const anyOf = (col: string, values: string[]): Predicate =>
  values.length === 1 ? { col, op: 'eq', value: values[0]! } : { or: values.map((v) => ({ col, op: 'eq', value: v })) }

// The list/board never render `description` (a large lorem blob — ~55% of each issue's bytes); it's
// only needed by search. Projecting it out of the browse shapes cuts the synced payload roughly in
// half. Search syncs the full row (columns omitted) so it can match on description. `project_id` is
// included so rows can render their project badge.
export const LIST_COLUMNS = ['id', 'title', 'status', 'priority', 'username', 'project_id', 'created', 'modified', 'kanbanorder']
const BOARD_COLUMNS = ['id', 'title', 'priority', 'username', 'project_id', 'kanbanorder']

const andAll = (preds: (Predicate | undefined)[]): Predicate | undefined => {
  const cs = preds.filter((p): p is Predicate => p !== undefined)
  return cs.length === 0 ? undefined : cs.length === 1 ? cs[0] : { and: cs }
}

/**
 * **Visibility = a subquery.** A user sees an issue only if they belong to its project: the issue's
 * `project_id` must be IN the set of projects the user is a member of. This single subquery is reused
 * across every issue read path (list, board, search, my-tasks); identical instances share one inner
 * registry node in the engine.
 */
export function visibleIssues(userId: number): Predicate {
  return {
    col: 'project_id',
    in: { table: 'project_members', project: 'project_id', where: { col: 'user_id', op: 'eq', value: userId } },
  }
}

/** Everything an issue query needs: who's asking (visibility) + the active filters/view. */
export interface IssueQuery {
  userId: number
  userName: string
  statuses: Status[]
  priorities: Priority[]
  projectId?: number | null // restrict to one project (sidebar project link)
  myTasksOnly?: boolean // only issues assigned to the current user
}

/** The full issue predicate: visibility ∧ status ∧ priority ∧ [project] ∧ [assigned-to-me]. */
function issuesWhere(q: IssueQuery): Predicate | undefined {
  return andAll([
    visibleIssues(q.userId),
    q.statuses.length ? anyOf('status', q.statuses) : undefined,
    q.priorities.length ? anyOf('priority', q.priorities) : undefined,
    q.projectId ? { col: 'project_id', op: 'eq', value: q.projectId } : undefined,
    q.myTasksOnly ? { col: 'username', op: 'eq', value: q.userName } : undefined,
  ])
}

/**
 * Build the search list's shape from the active query. `columns` restricts which columns sync (the pk
 * is always included); omit it for the full row (search needs `description`).
 */
export function issuesShapeDef(q: IssueQuery, columns?: string[]): ShapeDef {
  return { table: 'issues', where: issuesWhere(q), columns }
}

/** Page size for the browse subset (query-back chunk + live-tail window growth on scroll). */
export const SUBSET_PAGE = 200

/**
 * The browse view as a **subset query**: the visibility-subquery predicate, ordered + paged, fetched by
 * query-back from Postgres (never materialized — the subquery is evaluated natively by Postgres) with a
 * changes-only live tail. Ordering must be a real column for keyset paging, so the demo pages by
 * `created`/`modified`; the engine appends the pk as a tiebreaker.
 */
export function issuesSubsetDef(
  q: IssueQuery,
  orderBy: { col: 'created' | 'modified'; desc?: boolean },
  columns?: string[],
): SubsetDef {
  return { table: 'issues', where: issuesWhere(q), columns, orderBy, limit: SUBSET_PAGE }
}

/**
 * One **subset** (paginated query-back, never materialized) per project the current user belongs to. The
 * browse list mounts one of these per member project and merges/filters them on the client, so switching
 * project/status/sort is instant (no new engine feed per filter combination) AND it scales — a member of
 * a 100k-issue workspace never holds more than the loaded pages. The `project_id = P` predicate is
 * identical across users, so the engine reuses one feed family per project rather than a per-user subquery
 * subset per filter combination.
 */
export const projectIssuesSubsetDef = (projectId: number, orderBy: { col: 'created' | 'modified'; desc?: boolean }): SubsetDef => ({
  table: 'issues',
  where: { col: 'project_id', op: 'eq', value: projectId },
  columns: LIST_COLUMNS,
  orderBy,
  limit: SUBSET_PAGE,
})

/** A board column: visible issues with a given status (and optional project filter), as a shape. */
export const statusShapeDef = (q: Pick<IssueQuery, 'userId' | 'projectId'>, status: Status): ShapeDef => ({
  table: 'issues',
  where: andAll([
    visibleIssues(q.userId),
    { col: 'status', op: 'eq', value: status },
    q.projectId ? { col: 'project_id', op: 'eq', value: q.projectId } : undefined,
  ]),
  columns: BOARD_COLUMNS,
})

export const commentsShapeDef = (issueId: number): ShapeDef => ({
  table: 'comments',
  where: { col: 'issue_id', op: 'eq', value: issueId },
})

// --- Reference-data shapes (small, fully materialized): the user roster, projects, and the current
// user's memberships. The UI reads these live so the switcher/badges/sidebar reflect real engine state.
export const usersShapeDef: ShapeDef = { table: 'users' }
export const projectsShapeDef: ShapeDef = { table: 'projects' }
export const myMembershipsShapeDef = (userId: number): ShapeDef => ({
  table: 'project_members',
  where: { col: 'user_id', op: 'eq', value: userId },
})
