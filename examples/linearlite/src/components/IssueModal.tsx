import { useState } from 'react'

import { createIssue } from '../electric'
import { useCurrentUser } from '../lib/CurrentUser'
import type { Priority, Status } from '../schema'
import { PriorityMenu, StatusMenu } from './ui'

export function IssueModal({ onClose }: { onClose: () => void }): JSX.Element {
  const { projects, myProjectIds, currentUserName } = useCurrentUser()
  // Only projects the current user belongs to (creating into an invisible project would hide the issue).
  const myProjects = projects.filter((p) => myProjectIds.has(p.id))
  const [title, setTitle] = useState('')
  const [description, setDescription] = useState('')
  const [status, setStatus] = useState<Status>('backlog')
  const [priority, setPriority] = useState<Priority>('none')
  const [projectId, setProjectId] = useState<number | null>(null)
  const effectiveProjectId = projectId ?? myProjects[0]?.id ?? null

  const submit = () => {
    const t = title.trim()
    if (!t || effectiveProjectId === null) return
    createIssue({ title: t, description, status, priority, project_id: effectiveProjectId }, currentUserName)
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
            <select
              className="project-select"
              value={effectiveProjectId ?? ''}
              onChange={(e) => setProjectId(Number(e.target.value))}
            >
              {myProjects.map((p) => (
                <option key={p.id} value={p.id}>
                  {p.name}
                </option>
              ))}
            </select>
          </div>
          <button type="button" className="btn primary" onClick={submit} disabled={!title.trim() || effectiveProjectId === null}>
            Create issue
          </button>
        </div>
      </div>
    </div>
  )
}
