import { EMPTY_FILTERS, type Filters, navigate } from '../App'

function NavItem({
  label,
  active,
  onClick,
}: {
  label: string
  active: boolean
  onClick: () => void
}): JSX.Element {
  return (
    <button type="button" className={`nav-item ${active ? 'active' : ''}`} onClick={onClick}>
      {label}
    </button>
  )
}

export function Sidebar({
  onNewIssue,
  filters,
  setFilters,
  activeHash,
}: {
  onNewIssue: () => void
  filters: Filters
  setFilters: (f: Filters) => void
  activeHash: string
}): JSX.Element {
  const isList = !activeHash.startsWith('#/board') && !activeHash.startsWith('#/issue')
  const eq = (a: string[], b: string[]) => a.length === b.length && a.every((x) => b.includes(x))
  const isAll = isList && filters.statuses.length === 0
  const isActive = isList && eq(filters.statuses, ['todo', 'in_progress'])
  const isBacklog = isList && eq(filters.statuses, ['backlog'])

  const go = (statuses: Filters['statuses']) => {
    setFilters({ ...EMPTY_FILTERS, statuses })
    navigate('#/')
  }

  return (
    <aside className="sidebar">
      <div className="brand">
        <span className="brand-mark">◆</span>
        <div>
          <div className="brand-name">LinearLite</div>
          <div className="brand-sub">on electric-lite</div>
        </div>
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
        <NavItem label="Board" active={activeHash.startsWith('#/board')} onClick={() => navigate('#/board')} />
      </div>

      <div className="sidebar-footer">
        <a href="https://github.com/electric-sql/electric/tree/main/examples/linearlite" target="_blank" rel="noreferrer">
          Ported from ElectricSQL LinearLite
        </a>
        <span className="muted">Postgres → logical replication → live shapes</span>
      </div>
    </aside>
  )
}
