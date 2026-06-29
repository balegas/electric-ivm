import { useEffect, useRef, useState } from 'react'

import { navigate } from '../App'
import { type Issue, statusShapeDef, updateIssue } from '../electric'
import { STATUS_LABEL, STATUSES, type Status } from '../schema'
import { useShapeRows } from '../lib/useShape'
import { Avatar, displayId, PriorityIcon, StatusIcon } from './ui'

function BoardColumn({
  status,
  onDropIssue,
  register,
}: {
  status: Status
  onDropIssue: (id: number, target: Status, maxOrder: number) => void
  register: (rows: Issue[]) => void
}): JSX.Element {
  const { rows } = useShapeRows<Issue>(statusShapeDef(status))
  const [over, setOver] = useState(false)
  const sorted = [...rows].sort((a, b) => a.kanbanorder - b.kanbanorder || a.id - b.id)
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
      <div className="board-col-body">
        {sorted.map((issue) => (
          <div
            key={issue.id}
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
        ))}
      </div>
    </div>
  )
}

export function Board({ onNewIssue }: { onNewIssue: () => void }): JSX.Element {
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
          <BoardColumn key={s} status={s} onDropIssue={onDropIssue} register={register} />
        ))}
      </div>
    </>
  )
}
