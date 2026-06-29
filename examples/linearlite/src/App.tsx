import { useEffect, useState } from 'react'

import { Board } from './components/Board'
import { IssueDetail } from './components/IssueDetail'
import { IssueList } from './components/IssueList'
import { IssueModal } from './components/IssueModal'
import { Sidebar } from './components/Sidebar'
import type { Priority, Status } from './schema'

export interface Filters {
  statuses: Status[]
  priorities: Priority[]
  q: string
  orderBy: 'created' | 'modified' | 'priority'
  dir: 'asc' | 'desc'
}

export const EMPTY_FILTERS: Filters = { statuses: [], priorities: [], q: '', orderBy: 'created', dir: 'desc' }

// Minimal hash router: '#/', '#/board', '#/search', '#/issue/<id>'.
function useHashRoute(): string {
  const [hash, setHash] = useState(() => window.location.hash || '#/')
  useEffect(() => {
    const on = () => setHash(window.location.hash || '#/')
    window.addEventListener('hashchange', on)
    return () => window.removeEventListener('hashchange', on)
  }, [])
  return hash
}

export const navigate = (hash: string) => {
  window.location.hash = hash
}

export function App(): JSX.Element {
  const hash = useHashRoute()
  const [filters, setFilters] = useState<Filters>(EMPTY_FILTERS)
  const [createOpen, setCreateOpen] = useState(false)

  const issueMatch = hash.match(/^#\/issue\/(\d+)/)
  const route = issueMatch ? 'issue' : hash.startsWith('#/board') ? 'board' : 'list'
  const showSearch = hash.startsWith('#/search')

  return (
    <div className="layout">
      <Sidebar onNewIssue={() => setCreateOpen(true)} filters={filters} setFilters={setFilters} activeHash={hash} />
      <main className="main">
        {route === 'list' && (
          <IssueList filters={filters} setFilters={setFilters} showSearch={showSearch} onNewIssue={() => setCreateOpen(true)} />
        )}
        {route === 'board' && <Board onNewIssue={() => setCreateOpen(true)} />}
        {route === 'issue' && <IssueDetail id={Number(issueMatch![1])} />}
      </main>
      {createOpen && <IssueModal onClose={() => setCreateOpen(false)} />}
    </div>
  )
}
