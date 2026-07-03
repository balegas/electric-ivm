import { EMPTY_FILTERS, type Filters, navigate } from '../App'
import { genId, joinProject, leaveProject } from '../electric'
import { useCurrentUser } from '../lib/CurrentUser'

function NavItem({
  label,
  active,
  onClick,
  trailing,
}: {
  label: React.ReactNode
  active: boolean
  onClick: () => void
  trailing?: React.ReactNode
}): JSX.Element {
  return (
    <button type="button" className={`nav-item ${active ? 'active' : ''}`} onClick={onClick}>
      <span className="nav-item-label">{label}</span>
      {trailing}
    </button>
  )
}

export function Sidebar({
  onNewIssue,
  filters,
  setFilters,
  activeHash,
  onCollapse,
}: {
  onNewIssue: () => void
  filters: Filters
  setFilters: (f: Filters) => void
  activeHash: string
  onCollapse: () => void
}): JSX.Element {
  const { users, projects, myProjectIds, myMemberships, currentUserId, setCurrentUserId } = useCurrentUser()

  const isList = !activeHash.startsWith('#/board') && !activeHash.startsWith('#/issue')
  const eq = (a: string[], b: string[]) => a.length === b.length && a.every((x) => b.includes(x))
  const noScope = isList && filters.projectId === null && !filters.myTasksOnly
  const isAll = noScope && filters.statuses.length === 0
  const isActive = noScope && eq(filters.statuses, ['todo', 'in_progress'])
  const isBacklog = noScope && eq(filters.statuses, ['backlog'])
  const isMyTasks = isList && filters.myTasksOnly

  const go = (statuses: Filters['statuses']) => {
    setFilters({ ...EMPTY_FILTERS, statuses })
    navigate('#/')
  }
  const goProject = (projectId: number) => {
    setFilters({ ...EMPTY_FILTERS, projectId })
    navigate('#/')
  }
  const goMyTasks = () => {
    setFilters({ ...EMPTY_FILTERS, myTasksOnly: true })
    navigate('#/')
  }

  // Join/leave toggles a project_members row for the current user — visibility changes live.
  const toggleMembership = (projectId: number) => {
    const existing = myMemberships.find((m) => m.project_id === projectId)
    if (existing) leaveProject(existing.id)
    else joinProject(genId(), projectId, currentUserId)
  }

  return (
    <aside className="sidebar">
      <div className="brand">
        <span className="brand-mark">◆</span>
        <div>
          <div className="brand-name">LinearLite</div>
          <div className="brand-sub">on electric-ivm</div>
        </div>
      </div>

      <div className="user-switcher">
        <label htmlFor="user-select" className="nav-group-title">
          Viewing as
        </label>
        <select id="user-select" value={currentUserId} onChange={(e) => setCurrentUserId(Number(e.target.value))}>
          {users.map((u) => (
            <option key={u.id} value={u.id}>
              {u.name}
            </option>
          ))}
        </select>
      </div>

      <div className="sidebar-actions">
        <button type="button" className="btn primary" onClick={onNewIssue}>
          + New Issue
        </button>
        <button type="button" className="btn" onClick={() => navigate('#/search')}>
          Search
        </button>
      </div>

      <div className="nav-group">
        <div className="nav-group-title">Your Issues</div>
        <NavItem label="All Issues" active={isAll} onClick={() => go([])} />
        <NavItem label="Active" active={isActive} onClick={() => go(['todo', 'in_progress'])} />
        <NavItem label="Backlog" active={isBacklog} onClick={() => go(['backlog'])} />
        <NavItem label="My Tasks" active={isMyTasks} onClick={goMyTasks} />
        <NavItem label="Board" active={activeHash.startsWith('#/board')} onClick={() => navigate('#/board')} />
      </div>

      <div className="nav-group">
        <div className="nav-group-title">Projects</div>
        {projects.map((p) => {
          const member = myProjectIds.has(p.id)
          return (
            <NavItem
              key={p.id}
              label={
                <span className="project-row">
                  <span className="project-dot" style={{ background: p.color, opacity: member ? 1 : 0.3 }} />
                  <span style={{ opacity: member ? 1 : 0.5 }}>{p.name}</span>
                </span>
              }
              active={isList && filters.projectId === p.id}
              onClick={() => (member ? goProject(p.id) : toggleMembership(p.id))}
              trailing={
                <span
                  className="project-join"
                  title={member ? 'Leave project' : 'Join project'}
                  onClick={(e) => {
                    e.stopPropagation()
                    toggleMembership(p.id)
                  }}
                >
                  {member ? 'Leave' : 'Join'}
                </span>
              }
            />
          )
        })}
      </div>

      <button type="button" className="sidebar-collapse" title="Collapse sidebar" onClick={onCollapse}>
        ☰
      </button>
    </aside>
  )
}
