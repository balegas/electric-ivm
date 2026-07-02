// Workspace lifecycle: minting, seeding, lookup, and reset semantics. A workspace is a row in
// playground_workspaces plus its projects/issues rows (all stamped workspace_id) and its
// registered shapes (playground_shapes meta + the engine-side shape). A workspace is valid only
// for the epoch it was created under: the operator bumps PLAYGROUND_EPOCH (or wipes the DB) and
// every client re-provisions on the resulting 404. The UI never surfaces any of this — scoping is
// silent.

import type { Issue, Project, WorkspaceState } from '../shared/types.ts'
import { type Db, mintId, num } from './db.ts'
import { SEED_ISSUES, SEED_PROJECTS } from './schema.ts'

export interface WorkspaceDeps {
  db: Db
  epoch: number
  /** List a workspace's shapes for the state payload (wired to shapes.ts; injected to avoid a cycle). */
  listShapes(ws: string): Promise<WorkspaceState['shapes']>
}

function mintWorkspaceId(): string {
  const alphabet = 'abcdefghjkmnpqrstuvwxyz23456789'
  let s = 'w_'
  for (let i = 0; i < 6; i++) s += alphabet[Math.floor(Math.random() * alphabet.length)]
  return s
}

export async function workspaceExists(deps: WorkspaceDeps, id: string): Promise<boolean> {
  const r = await deps.db.query('SELECT 1 FROM playground_workspaces WHERE id = $1 AND epoch = $2', [id, deps.epoch])
  return r.rowCount === 1
}

/** Current state (projects, issues, shapes) of a valid workspace; null if unknown/stale. */
export async function getWorkspaceState(deps: WorkspaceDeps, id: string): Promise<WorkspaceState | null> {
  if (!(await workspaceExists(deps, id))) return null
  await deps.db.query('UPDATE playground_workspaces SET last_seen = $2 WHERE id = $1', [id, Date.now()])
  const projects = await deps.db.query('SELECT * FROM projects WHERE workspace_id = $1 ORDER BY id', [id])
  const issues = await deps.db.query('SELECT * FROM issues WHERE workspace_id = $1 ORDER BY id', [id])
  return {
    workspace: { id, epoch: deps.epoch },
    projects: projects.rows.map((r) => ({ ...r, id: num(r.id) }) as Project),
    issues: issues.rows.map(
      (r) => ({ ...r, id: num(r.id), project_id: num(r.project_id), priority: num(r.priority) }) as Issue,
    ),
    shapes: await deps.listShapes(id),
  }
}

/** Idempotent provisioning: an existing valid id returns its current state untouched; otherwise a
 *  fresh workspace is minted and seeded (2 projects, 4 issues in mixed statuses). */
export async function provisionWorkspace(deps: WorkspaceDeps, existingId?: string): Promise<WorkspaceState> {
  if (existingId) {
    const state = await getWorkspaceState(deps, existingId)
    if (state) return state
  }
  const id = mintWorkspaceId()
  const now = Date.now()
  await deps.db.query('INSERT INTO playground_workspaces (id, epoch, created_at, last_seen) VALUES ($1,$2,$3,$4)', [
    id,
    deps.epoch,
    now,
    now,
  ])
  const projectIds: number[] = []
  for (const p of SEED_PROJECTS) {
    const pid = mintId()
    projectIds.push(pid)
    await deps.db.query('INSERT INTO projects (id, workspace_id, name, team) VALUES ($1,$2,$3,$4)', [
      pid,
      id,
      p.name,
      p.team,
    ])
  }
  for (const i of SEED_ISSUES) {
    await deps.db.query(
      'INSERT INTO issues (id, workspace_id, project_id, title, status, priority) VALUES ($1,$2,$3,$4,$5,$6)',
      [mintId(), id, projectIds[i.pi], i.title, i.status, i.priority],
    )
  }
  const state = await getWorkspaceState(deps, id)
  if (!state) throw new Error('provisioned workspace vanished')
  return state
}

/** Delete a workspace's rows + meta. Engine-side shape teardown is the caller's job (shapes.ts). */
export async function deleteWorkspaceRows(db: Db, id: string): Promise<void> {
  await db.query('DELETE FROM issues WHERE workspace_id = $1', [id])
  await db.query('DELETE FROM projects WHERE workspace_id = $1', [id])
  await db.query('DELETE FROM playground_shapes WHERE workspace_id = $1', [id])
  await db.query('DELETE FROM playground_workspaces WHERE id = $1', [id])
}

/** Workspaces idle beyond `ttlMs` (by last_seen), any epoch. */
export async function idleWorkspaces(db: Db, ttlMs: number): Promise<string[]> {
  const r = await db.query('SELECT id FROM playground_workspaces WHERE last_seen < $1', [Date.now() - ttlMs])
  return r.rows.map((x) => x.id as string)
}
