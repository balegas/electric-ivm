// The left pane — the data. Two plain editable grids (issues, projects); every cell edit is one
// SQL write to Postgres, which is what sets the pipeline in motion. No app chrome: the point is
// the data and what the engine does with it.

import type { Issue, Project, Verb } from '../shared/types.ts'
import { STATUSES, TEAMS } from '../shared/types.ts'

const PRIORITIES = [1, 2, 3, 4]

export function DataPanel({
  projects,
  issues,
  pending,
  act,
}: {
  projects: Project[]
  issues: Issue[]
  /** Writes in flight — the whole panel shows pending feedback so clicks feel acknowledged. */
  pending: boolean
  act: (verb: Verb) => void
}) {
  const projectName = new Map(projects.map((p) => [p.id, p.name]))
  return (
    <div className={`world${pending ? ' world-pending' : ''}`}>
      <div className="world-h">
        Issues {pending ? <span className="world-spin" title="write in flight" /> : null}
      </div>
      <table className="grid">
        <thead>
          <tr>
            <th>title</th>
            <th>status</th>
            <th>prio</th>
            <th>project</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {issues.map((i) => (
            <tr key={i.id}>
              <td className="grid-title" title={i.title}>
                {i.title}
              </td>
              <td>
                <select value={i.status} onChange={(e) => act({ verb: 'set_status', issueId: i.id, status: e.target.value as Issue['status'] })}>
                  {STATUSES.map((s) => (
                    <option key={s}>{s}</option>
                  ))}
                </select>
              </td>
              <td>
                <select value={i.priority} onChange={(e) => act({ verb: 'set_priority', issueId: i.id, priority: Number(e.target.value) })}>
                  {PRIORITIES.map((p) => (
                    <option key={p}>{p}</option>
                  ))}
                </select>
              </td>
              <td className="grid-proj">{projectName.get(i.project_id) ?? i.project_id}</td>
              <td>
                <button className="mini grid-del" title="Delete issue" onClick={() => act({ verb: 'delete_issue', issueId: i.id })}>
                  ✕
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {projects.map((p) => (
        <button key={p.id} className="order-btn" onClick={() => act({ verb: 'add_issue', projectId: p.id })}>
          ＋ Issue in {p.name}
        </button>
      ))}

      <div className="world-h" style={{ marginTop: 16 }}>
        Projects
      </div>
      <table className="grid">
        <thead>
          <tr>
            <th>name</th>
            <th>team</th>
          </tr>
        </thead>
        <tbody>
          {projects.map((p) => (
            <tr key={p.id}>
              <td>{p.name}</td>
              <td>
                <select value={p.team} onChange={(e) => act({ verb: 'set_team', projectId: p.id, team: e.target.value })}>
                  {TEAMS.map((t) => (
                    <option key={t}>{t}</option>
                  ))}
                </select>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <button className="add-rest" onClick={() => act({ verb: 'add_project' })}>
        ＋ Add project
      </button>
    </div>
  )
}
