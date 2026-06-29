import { createClient } from '@electric-lite/client'
import type { Predicate, ShapeDef } from '@electric-lite/protocol'
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
  created: number
  modified: number
  kanbanorder: number
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

export function createIssue(fields: Pick<Issue, 'title' | 'description' | 'status' | 'priority'>): Issue {
  const now = Date.now()
  const issue: Issue = {
    id: genId(),
    username: 'testuser',
    created: now,
    modified: now,
    kanbanorder: now, // append to the end of its column; reorders adjust this
    ...fields,
  }
  void pgWrite({ table: 'issues', op: 'insert', pk: issue.id, row: issue })
  return issue
}

export function updateIssue(issue: Issue, patch: Partial<Issue>) {
  const next = { ...issue, ...patch, modified: Date.now() }
  void pgWrite({ table: 'issues', op: 'update', pk: issue.id, row: next })
}

export function deleteIssue(id: number) {
  void pgWrite({ table: 'issues', op: 'delete', pk: id })
}

export function addComment(issueId: number, body: string) {
  const comment: Comment = { id: genId(), issue_id: issueId, body, username: 'testuser', created: Date.now() }
  void pgWrite({ table: 'comments', op: 'insert', pk: comment.id, row: comment })
}

export function deleteComment(id: number) {
  void pgWrite({ table: 'comments', op: 'delete', pk: id })
}

// --- Shape definitions (the engine evaluates these predicates) -------------------------------------
const anyOf = (col: string, values: string[]): Predicate =>
  values.length === 1 ? { col, op: 'eq', value: values[0]! } : { or: values.map((v) => ({ col, op: 'eq', value: v })) }

/** Build the list view's shape from the active status/priority filters. Empty filters => match-all. */
export function issuesShapeDef(statuses: Status[], priorities: Priority[]): ShapeDef {
  const clauses: Predicate[] = []
  if (statuses.length) clauses.push(anyOf('status', statuses))
  if (priorities.length) clauses.push(anyOf('priority', priorities))
  const where = clauses.length === 0 ? undefined : clauses.length === 1 ? clauses[0] : { and: clauses }
  return { table: 'issues', where }
}

export const statusShapeDef = (status: Status): ShapeDef => ({
  table: 'issues',
  where: { col: 'status', op: 'eq', value: status },
})

export const commentsShapeDef = (issueId: number): ShapeDef => ({
  table: 'comments',
  where: { col: 'issue_id', op: 'eq', value: issueId },
})
