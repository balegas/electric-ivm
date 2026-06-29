import { useState } from 'react'

import { createIssue } from '../electric'
import type { Priority, Status } from '../schema'
import { PriorityMenu, StatusMenu } from './ui'

export function IssueModal({ onClose }: { onClose: () => void }): JSX.Element {
  const [title, setTitle] = useState('')
  const [description, setDescription] = useState('')
  const [status, setStatus] = useState<Status>('backlog')
  const [priority, setPriority] = useState<Priority>('none')

  const submit = () => {
    const t = title.trim()
    if (!t) return
    createIssue({ title: t, description, status, priority })
    onClose()
  }

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal-head">
          <span className="muted">New issue</span>
          <button type="button" className="icon-btn" onClick={onClose}>
            ✕
          </button>
        </div>
        <input
          className="modal-title-input"
          placeholder="Issue title"
          value={title}
          autoFocus
          onChange={(e) => setTitle(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) submit()
          }}
        />
        <textarea
          className="modal-desc-input"
          placeholder="Add description…"
          value={description}
          rows={5}
          onChange={(e) => setDescription(e.target.value)}
        />
        <div className="modal-foot">
          <div className="modal-controls">
            <StatusMenu value={status} onChange={setStatus} />
            <PriorityMenu value={priority} onChange={setPriority} />
          </div>
          <button type="button" className="btn primary" onClick={submit} disabled={!title.trim()}>
            Create issue
          </button>
        </div>
      </div>
    </div>
  )
}
