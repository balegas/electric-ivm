import { useEffect, useState } from 'react'

import { navigate } from '../App'
import { addComment, type Comment, commentsShapeDef, deleteComment, deleteIssue, type Issue, updateIssue } from '../electric'
import { useCurrentUser } from '../lib/CurrentUser'
import { useShapeRows } from '../lib/useShape'
import { Avatar, displayId, formatDate, PriorityMenu, ProjectBadge, StatusMenu } from './ui'

export function IssueDetail({ id }: { id: number }): JSX.Element {
  const { currentUserName, projectById } = useCurrentUser()
  // Live single-issue shape + a live per-issue comments shape (created/closed with this view).
  const { rows: issues, loading } = useShapeRows<Issue>({ table: 'issues', where: { col: 'id', op: 'eq', value: id } })
  // Comments ordered oldest-first in the live query.
  const { rows: comments } = useShapeRows<Comment>(commentsShapeDef(id), (b) =>
    b.orderBy(({ t }: { t: Comment }) => t.created, 'asc').select(({ t }: { t: Comment }) => t),
  )
  const issue = issues[0]

  const [title, setTitle] = useState('')
  const [description, setDescription] = useState('')
  const [body, setBody] = useState('')
  // Sync local edit fields when the issue first loads / changes identity. Reset the comment draft too
  // so an unposted comment doesn't leak onto the next issue when navigating between them.
  useEffect(() => {
    if (issue) {
      setTitle(issue.title)
      setDescription(issue.description)
    }
    setBody('')
  }, [issue?.id]) // eslint-disable-line react-hooks/exhaustive-deps

  if (loading) return <div className="detail"><div className="empty">Loading…</div></div>
  if (!issue) {
    return (
      <div className="detail">
        <div className="empty">Issue not found (it may have been deleted).</div>
        <button type="button" className="btn" onClick={() => navigate('#/')}>
          ← Back to issues
        </button>
      </div>
    )
  }


  return (
    <div className="detail">
      <div className="detail-head">
        <button type="button" className="btn small" onClick={() => navigate('#/')}>
          ←
        </button>
        <span className="issue-id">{displayId(issue.id)}</span>
        <span className="spacer" />
        <button
          type="button"
          className="btn small danger"
          onClick={() => {
            deleteIssue(issue.id)
            navigate('#/')
          }}
        >
          Delete
        </button>
      </div>

      <div className="detail-body">
        <div className="detail-main">
          <input
            className="detail-title"
            value={title}
            onChange={(e) => setTitle(e.target.value)}
            onBlur={() => title !== issue.title && updateIssue(issue, { title })}
          />
          <textarea
            className="detail-desc"
            value={description}
            rows={8}
            placeholder="Add description…"
            onChange={(e) => setDescription(e.target.value)}
            onBlur={() => description !== issue.description && updateIssue(issue, { description })}
          />

          <h3 className="comments-title">Comments ({comments.length})</h3>
          <div className="comments">
            {comments.map((c) => (
              <div key={c.id} className="comment">
                <Avatar name={c.username} />
                <div className="comment-body">
                  <div className="comment-meta">
                    <strong>{c.username}</strong> <span className="muted">{formatDate(c.created)}</span>
                    <button type="button" className="icon-btn tiny" title="Delete comment" onClick={() => deleteComment(c.id)}>
                      ✕
                    </button>
                  </div>
                  <div>{c.body}</div>
                </div>
              </div>
            ))}
            {comments.length === 0 && <div className="empty">No comments yet.</div>}
          </div>

          <div className="comment-add">
            <textarea
              placeholder="Leave a comment…"
              value={body}
              rows={3}
              onChange={(e) => setBody(e.target.value)}
            />
            <button
              type="button"
              className="btn primary"
              disabled={!body.trim()}
              onClick={() => {
                addComment(issue.id, body.trim(), currentUserName)
                setBody('')
              }}
            >
              Post comment
            </button>
          </div>
        </div>

        <aside className="detail-side">
          <div className="side-field">
            <span className="side-label">Status</span>
            <StatusMenu value={issue.status} onChange={(status) => updateIssue(issue, { status })} />
          </div>
          <div className="side-field">
            <span className="side-label">Priority</span>
            <PriorityMenu value={issue.priority} onChange={(priority) => updateIssue(issue, { priority })} />
          </div>
          <div className="side-field">
            <span className="side-label">Project</span>
            <span className="side-value">
              <ProjectBadge project={projectById.get(issue.project_id)} />
            </span>
          </div>
          <div className="side-field">
            <span className="side-label">Assignee</span>
            <span className="side-value">
              <Avatar name={issue.username} /> {issue.username}
            </span>
          </div>
          <div className="side-field">
            <span className="side-label">Created</span>
            <span className="side-value">{formatDate(issue.created)}</span>
          </div>
        </aside>
      </div>
    </div>
  )
}
