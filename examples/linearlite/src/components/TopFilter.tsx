import { useEffect, useRef, useState } from 'react'

import type { Filters } from '../App'
import { PRIORITY_LABEL, STATUS_LABEL, type Priority, type Status } from '../schema'
import { PriorityIcon, StatusIcon } from './ui'
import { PRIORITIES, STATUSES } from './ui'

function toggle<T>(arr: T[], v: T): T[] {
  return arr.includes(v) ? arr.filter((x) => x !== v) : [...arr, v]
}

function FilterPopover({ filters, setFilters }: { filters: Filters; setFilters: (f: Filters) => void }): JSX.Element {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)
  useEffect(() => {
    if (!open) return
    const onDoc = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', onDoc)
    return () => document.removeEventListener('mousedown', onDoc)
  }, [open])

  return (
    <div className="menu" ref={ref}>
      <button type="button" className="btn small" onClick={() => setOpen((o) => !o)}>
        + Filter
      </button>
      {open && (
        <div className="menu-popover wide">
          <div className="menu-section-title">Status</div>
          {STATUSES.map((s) => (
            <label key={s} className="check-item">
              <input
                type="checkbox"
                checked={filters.statuses.includes(s)}
                onChange={() => setFilters({ ...filters, statuses: toggle<Status>(filters.statuses, s) })}
              />
              <StatusIcon status={s} /> {STATUS_LABEL[s]}
            </label>
          ))}
          <div className="menu-section-title">Priority</div>
          {PRIORITIES.map((p) => (
            <label key={p} className="check-item">
              <input
                type="checkbox"
                checked={filters.priorities.includes(p)}
                onChange={() => setFilters({ ...filters, priorities: toggle<Priority>(filters.priorities, p) })}
              />
              <PriorityIcon priority={p} /> {PRIORITY_LABEL[p]}
            </label>
          ))}
        </div>
      )}
    </div>
  )
}

export function TopFilter({
  title,
  count,
  filters,
  setFilters,
  showSearch,
}: {
  title: string
  count: number
  filters: Filters
  setFilters: (f: Filters) => void
  showSearch?: boolean
}): JSX.Element {
  return (
    <div className="topfilter">
      <div className="topfilter-row">
        <h1 className="page-title">
          {title} <span className="count-pill">{count}</span>
        </h1>
        <div className="spacer" />
        <FilterPopover filters={filters} setFilters={setFilters} />
        <button
          type="button"
          className="btn small"
          title="Toggle sort direction"
          onClick={() => setFilters({ ...filters, dir: filters.dir === 'asc' ? 'desc' : 'asc' })}
        >
          {filters.dir === 'desc' ? '↓' : '↑'} {filters.orderBy}
        </button>
      </div>

      {showSearch && (
        <input
          className="search-input"
          placeholder="Search title & description…"
          value={filters.q}
          autoFocus
          onChange={(e) => setFilters({ ...filters, q: e.target.value })}
        />
      )}

      {(filters.statuses.length > 0 || filters.priorities.length > 0) && (
        <div className="chips">
          {filters.statuses.map((s) => (
            <span key={s} className="chip">
              <StatusIcon status={s} /> {STATUS_LABEL[s]}
              <button type="button" onClick={() => setFilters({ ...filters, statuses: filters.statuses.filter((x) => x !== s) })}>
                ✕
              </button>
            </span>
          ))}
          {filters.priorities.map((p) => (
            <span key={p} className="chip">
              <PriorityIcon priority={p} /> {PRIORITY_LABEL[p]}
              <button type="button" onClick={() => setFilters({ ...filters, priorities: filters.priorities.filter((x) => x !== p) })}>
                ✕
              </button>
            </span>
          ))}
        </div>
      )}
    </div>
  )
}
