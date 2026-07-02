// The grid-edit verbs — the ONLY way playground visitors write data. Each verb is fixed,
// parameterized SQL scoped to the caller's workspace; there is no raw SQL surface.

import type { Issue, Project, Verb } from '../shared/types.ts'
import { STATUSES, TEAMS } from '../shared/types.ts'
import { type Db, mintId, num } from './db.ts'
import { ISSUE_TITLES, PROJECT_NAMES } from './schema.ts'

export class ActionError extends Error {
  constructor(
    public status: number,
    msg: string,
  ) {
    super(msg)
  }
}

const MAX_ISSUES = 40
const MAX_PROJECTS = 6

const toIssue = (r: Record<string, unknown>): Issue =>
  ({ ...r, id: num(r.id), project_id: num(r.project_id), priority: num(r.priority) }) as Issue

export async function applyAction(
  db: Db,
  ws: string,
  verb: Verb,
): Promise<{ ok: true; issue?: Issue; project?: Project }> {
  switch (verb.verb) {
    case 'add_issue': {
      const p = await db.query('SELECT 1 FROM projects WHERE id = $1 AND workspace_id = $2', [verb.projectId, ws])
      if (p.rowCount !== 1) throw new ActionError(404, 'unknown project')
      const n = await db.query('SELECT COUNT(*)::int AS n FROM issues WHERE workspace_id = $1', [ws])
      if ((n.rows[0].n as number) >= MAX_ISSUES) throw new ActionError(413, 'issue cap reached')
      const title = ISSUE_TITLES[Math.floor(Math.random() * ISSUE_TITLES.length)]
      const priority = 1 + Math.floor(Math.random() * 4)
      const ins = await db.query(
        `INSERT INTO issues (id, workspace_id, project_id, title, status, priority)
         VALUES ($1,$2,$3,$4,'todo',$5) RETURNING *`,
        [mintId(), ws, verb.projectId, title, priority],
      )
      return { ok: true, issue: toIssue(ins.rows[0]) }
    }
    case 'set_status': {
      if (!STATUSES.includes(verb.status)) throw new ActionError(400, 'bad status')
      const r = await db.query(
        'UPDATE issues SET status = $3 WHERE id = $1 AND workspace_id = $2 RETURNING *',
        [verb.issueId, ws, verb.status],
      )
      if (r.rowCount !== 1) throw new ActionError(404, 'unknown issue')
      return { ok: true, issue: toIssue(r.rows[0]) }
    }
    case 'set_priority': {
      const p = Math.round(Number(verb.priority))
      if (!(p >= 1 && p <= 4)) throw new ActionError(400, 'priority is 1–4')
      const r = await db.query(
        'UPDATE issues SET priority = $3 WHERE id = $1 AND workspace_id = $2 RETURNING *',
        [verb.issueId, ws, p],
      )
      if (r.rowCount !== 1) throw new ActionError(404, 'unknown issue')
      return { ok: true, issue: toIssue(r.rows[0]) }
    }
    case 'delete_issue': {
      const r = await db.query('DELETE FROM issues WHERE id = $1 AND workspace_id = $2', [verb.issueId, ws])
      if (r.rowCount !== 1) throw new ActionError(404, 'unknown issue')
      return { ok: true }
    }
    case 'add_project': {
      const existing = await db.query('SELECT name FROM projects WHERE workspace_id = $1', [ws])
      if ((existing.rowCount ?? 0) >= MAX_PROJECTS) throw new ActionError(413, 'project cap reached')
      const used = new Set(existing.rows.map((r) => r.name as string))
      const name = PROJECT_NAMES.find((x) => !used.has(x)) ?? `Project ${existing.rowCount! + 1}`
      const team = TEAMS[Math.floor(Math.random() * TEAMS.length)]!
      const ins = await db.query(
        'INSERT INTO projects (id, workspace_id, name, team) VALUES ($1,$2,$3,$4) RETURNING *',
        [mintId(), ws, name, team],
      )
      const p = ins.rows[0]
      return { ok: true, project: { ...p, id: num(p.id) } }
    }
    case 'set_team': {
      const r = await db.query(
        'UPDATE projects SET team = $3 WHERE id = $1 AND workspace_id = $2 RETURNING *',
        [verb.projectId, ws, verb.team],
      )
      if (r.rowCount !== 1) throw new ActionError(404, 'unknown project')
      const p = r.rows[0]
      return { ok: true, project: { ...p, id: num(p.id) } }
    }
    default:
      throw new ActionError(400, `unknown verb ${(verb as { verb: string }).verb}`)
  }
}
