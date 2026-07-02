import { useEffect, useRef, useState } from 'react'

import type { Filters } from '../App'
import { navigate } from '../App'
import { type Issue, statusShapeDef, updateIssue } from '../electric'
import { STATUS_LABEL, STATUSES, type Status } from '../schema'
import { useCurrentUser } from '../lib/CurrentUser'
import { useShapeRows } from '../lib/useShape'
import { Virtual } from '../lib/Virtual'
import { Avatar, displayId, PriorityIcon, StatusIcon } from './ui'

function BoardCard({ issue }: { issue: Issue }): JSX.Element {
  return (
    <div
      className="board-card"
      draggable
      onDragStart={(e) => e.dataTransfer.setData('issue-id', String(issue.id))}
      onClick={() => navigate(`#/issue/${issue.id}`)}
    >
      <div className="board-card-title">{issue.title}</div>
      <div className="board-card-meta">
        <PriorityIcon priority={issue.priority} />
        <span className="issue-id">{displayId(issue.id)}</span>
        <span className="spacer" />
        <Avatar name={issue.username} size={20} />
      </div>
    </div>
  )
}

function BoardColumn({
  status,
  userId,
  projectId,
  onDropIssue,
  register,
}: {
  status: Status
  userId: number
  projectId: number | null
  onDropIssue: (id: number, target: Status, maxOrder: number) => void
  register: (rows: Issue[]) => void
}): JSX.Element {
  // Each column is a visibility subquery ∧ status shape; all five share one inner registry node. Ordering
  // is pushed into the live query (kanbanorder, then id) — no client-side sort.
  const { rows } = useShapeRows<Issue>(statusShapeDef({ userId, projectId }, status), (b) =>
    b
      .orderBy(({ t }: { t: Issue }) => t.kanbanorder, 'asc')
      .orderBy(({ t }: { t: Issue }) => t.id, 'asc')
      .select(({ t }: { t: Issue }) => t),
  )
  const [over, setOver] = useState(false)
  const maxOrder = rows.reduce((m, r) => Math.max(m, r.kanbanorder), 0)
  // Feed the board-level registry so a drop reads the FRESHEST row (not a snapshot from dragstart).
  useEffect(() => {
    register(rows)
  }, [rows, register])

  return (
    <div
      className={`board-col ${over ? 'over' : ''}`}
      onDragOver={(e) => {
        e.preventDefault()
        setOver(true)
      }}
      onDragLeave={() => setOver(false)}
      onDrop={(e) => {
        e.preventDefault()
        setOver(false)
        // Carry only the id; the freshest row is looked up at drop time (avoids LWW-clobbering any
        // edit that landed between dragstart and drop).
        const id = Number(e.dataTransfer.getData('issue-id'))
        if (id) onDropIssue(id, status, maxOrder)
      }}
    >
      <div className="board-col-head">
        <StatusIcon status={status} /> <span>{STATUS_LABEL[status]}</span>
        <span className="count-pill">{rows.length}</span>
      </div>
      {/* Only the cards in view are mounted (see Virtual): a 4k-card column renders ~20 nodes. */}
      <Virtual
        className="board-col-body"
        items={rows}
        getKey={(issue) => issue.id}
        estimateSize={76}
        gap={8}
        renderItem={(issue) => <BoardCard issue={issue} />}
      />
    </div>
  )
}

export function Board({ filters, onNewIssue }: { filters: Filters; onNewIssue: () => void }): JSX.Element {
  const { currentUserId } = useCurrentUser()
  // id -> freshest known row, fed by every column's live shape (see BoardColumn).
  const registry = useRef<Map<number, Issue>>(new Map())
  const register = useRef((rows: Issue[]) => {
    for (const r of rows) registry.current.set(r.id, r)
  }).current

  const onDropIssue = (id: number, target: Status, maxOrder: number) => {
    const fresh = registry.current.get(id)
    if (!fresh || fresh.status === target) return
    updateIssue(fresh, { status: target, kanbanorder: maxOrder + 1 })
  }

  return (
    <>
      <div className="topfilter">
        <div className="topfilter-row">
          <h1 className="page-title">Board</h1>
          <div className="spacer" />
          <button type="button" className="btn small primary" onClick={onNewIssue}>
            + New Issue
          </button>
        </div>
      </div>
      <div className="board">
        {STATUSES.map((s) => (
          <BoardColumn
            key={s}
            status={s}
            userId={currentUserId}
            projectId={filters.projectId}
            onDropIssue={onDropIssue}
            register={register}
          />
        ))}
      </div>
    </>
  )
}
