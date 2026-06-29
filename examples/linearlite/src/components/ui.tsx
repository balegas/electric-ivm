import { useEffect, useRef, useState, type ReactNode } from 'react'
import {
  PRIORITIES,
  PRIORITY_LABEL,
  type Priority,
  STATUS_LABEL,
  STATUSES,
  type Status,
} from '../schema'

export function formatDate(ms: number): string {
  const d = new Date(ms)
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })
}

// Collision-free display id. Seeded rows (small ints) read as EL-1..; client-minted ids (Date.now()
// based, ~13 digits) are base36-compressed so they stay short yet never alias (the old `id % 100000`
// could collide between two ids created close together, or a seeded id vs a minted one).
export function displayId(id: number): string {
  return id < 1_000_000 ? `EL-${id}` : `EL-${id.toString(36).toUpperCase()}`
}

const AVATAR_COLORS = ['#6e56cf', '#0ea5e9', '#10b981', '#f59e0b', '#ef4444', '#ec4899', '#14b8a6']
export function Avatar({ name, size = 22 }: { name: string; size?: number }): JSX.Element {
  const initials = name.slice(0, 2).toUpperCase()
  let h = 0
  for (const c of name) h = (h * 31 + c.charCodeAt(0)) >>> 0
  const bg = AVATAR_COLORS[h % AVATAR_COLORS.length]
  return (
    <span
      className="avatar"
      title={name}
      style={{ width: size, height: size, background: bg, fontSize: size * 0.42 }}
    >
      {initials}
    </span>
  )
}

// --- Status icon: a colored ring/disc per status -------------------------------------------------
const STATUS_COLOR: Record<Status, string> = {
  backlog: '#9ca3af',
  todo: '#9aa4b2',
  in_progress: '#f2c94c',
  done: '#6e56cf',
  canceled: '#6b7280',
}
export function StatusIcon({ status, size = 14 }: { status: Status; size?: number }): JSX.Element {
  const c = STATUS_COLOR[status]
  const r = size / 2
  const cx = r
  return (
    <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`} className="status-icon" aria-label={STATUS_LABEL[status]}>
      {status === 'backlog' && (
        <circle cx={cx} cy={cx} r={r - 1.5} fill="none" stroke={c} strokeWidth="1.5" strokeDasharray="2 2" />
      )}
      {status === 'todo' && <circle cx={cx} cy={cx} r={r - 1.5} fill="none" stroke={c} strokeWidth="1.5" />}
      {status === 'in_progress' && (
        <>
          <circle cx={cx} cy={cx} r={r - 1.5} fill="none" stroke={c} strokeWidth="1.5" />
          <path d={`M ${cx} ${cx} L ${cx} 1.5 A ${r - 1.5} ${r - 1.5} 0 0 1 ${cx} ${size - 1.5} Z`} fill={c} />
        </>
      )}
      {status === 'done' && (
        <>
          <circle cx={cx} cy={cx} r={r - 1} fill={c} />
          <path d={`M ${cx - 3} ${cx} l 2 2 l 4 -4`} fill="none" stroke="#fff" strokeWidth="1.5" />
        </>
      )}
      {status === 'canceled' && (
        <>
          <circle cx={cx} cy={cx} r={r - 1} fill={c} />
          <path d={`M ${cx - 2.5} ${cx - 2.5} l 5 5 M ${cx + 2.5} ${cx - 2.5} l -5 5`} stroke="#fff" strokeWidth="1.3" />
        </>
      )}
    </svg>
  )
}

// --- Priority icon: signal bars (urgent = filled box) --------------------------------------------
export function PriorityIcon({ priority, size = 14 }: { priority: Priority; size?: number }): JSX.Element {
  if (priority === 'urgent') {
    return (
      <svg width={size} height={size} viewBox="0 0 14 14" className="priority-icon" aria-label="Urgent">
        <rect x="0" y="0" width="14" height="14" rx="3" fill="#f59e0b" />
        <rect x="6" y="3" width="2" height="5" fill="#fff" />
        <rect x="6" y="9.5" width="2" height="2" fill="#fff" />
      </svg>
    )
  }
  const level = { none: 0, low: 1, medium: 2, high: 3 }[priority]
  const bars = [3, 6, 9] // heights
  return (
    <svg width={size} height={size} viewBox="0 0 14 14" className="priority-icon" aria-label={PRIORITY_LABEL[priority]}>
      {bars.map((h, i) => (
        <rect
          key={i}
          x={1 + i * 4.5}
          y={11 - h}
          width="3"
          height={h}
          rx="1"
          fill={i < level ? '#d1d5db' : '#3a3f4b'}
        />
      ))}
      {priority === 'none' && <rect x="2" y="6.5" width="10" height="1.5" rx="0.75" fill="#6b7280" />}
    </svg>
  )
}

// --- Generic dropdown menu -----------------------------------------------------------------------
export function Menu({ trigger, children }: { trigger: ReactNode; children: (close: () => void) => ReactNode }): JSX.Element {
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
      <button
        type="button"
        className="menu-trigger"
        onClick={(e) => {
          e.stopPropagation()
          setOpen((o) => !o)
        }}
      >
        {trigger}
      </button>
      {open && <div className="menu-popover">{children(() => setOpen(false))}</div>}
    </div>
  )
}

export function StatusMenu({ value, onChange }: { value: Status; onChange: (s: Status) => void }): JSX.Element {
  return (
    <Menu trigger={<StatusIcon status={value} />}>
      {(close) =>
        STATUSES.map((s) => (
          <button
            key={s}
            type="button"
            className={`menu-item ${s === value ? 'active' : ''}`}
            onClick={(e) => {
              e.stopPropagation()
              onChange(s)
              close()
            }}
          >
            <StatusIcon status={s} /> {STATUS_LABEL[s]}
          </button>
        ))
      }
    </Menu>
  )
}

export function PriorityMenu({ value, onChange }: { value: Priority; onChange: (p: Priority) => void }): JSX.Element {
  const order: Priority[] = ['none', 'urgent', 'high', 'medium', 'low']
  return (
    <Menu trigger={<PriorityIcon priority={value} />}>
      {(close) =>
        order.map((p) => (
          <button
            key={p}
            type="button"
            className={`menu-item ${p === value ? 'active' : ''}`}
            onClick={(e) => {
              e.stopPropagation()
              onChange(p)
              close()
            }}
          >
            <PriorityIcon priority={p} /> {PRIORITY_LABEL[p]}
          </button>
        ))
      }
    </Menu>
  )
}

export { PRIORITIES, STATUSES }
