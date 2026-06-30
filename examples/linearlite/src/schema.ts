import type { Schema } from '@electric-lite/protocol'

// LinearLite mapped onto electric-lite's model: a shape is one table + a WHERE over that table's own
// columns, with value types int | float | text | bool and a single-column primary key. The original
// uses uuid ids and timestamptz; we use integer ids (Linear-style issue numbers) and epoch-millis
// integers for timestamps (so `created`/`modified` are filterable and sortable with lt/gt).
export const schema: Schema = {
  tables: {
    issues: {
      columns: {
        id: { type: 'int' },
        title: { type: 'text' },
        description: { type: 'text' },
        status: { type: 'text' }, // STATUSES below
        priority: { type: 'text' }, // PRIORITIES below
        username: { type: 'text' }, // assignee / creator
        project_id: { type: 'int' }, // the project this issue belongs to (visibility)
        created: { type: 'int' }, // epoch ms
        modified: { type: 'int' }, // epoch ms
        kanbanorder: { type: 'float' }, // fractional ordering within a status column
      },
      primaryKey: 'id',
    },
    // Visibility model: a user sees an issue only if they belong to its project. Membership is the
    // `project_members` join table, consulted via a subquery on the issue queries (see electric.ts).
    projects: {
      columns: {
        id: { type: 'int' },
        name: { type: 'text' },
        color: { type: 'text' }, // badge color
      },
      primaryKey: 'id',
    },
    users: {
      columns: {
        id: { type: 'int' },
        name: { type: 'text' }, // also the issue `username` (assignee)
      },
      primaryKey: 'id',
    },
    project_members: {
      columns: {
        id: { type: 'int' },
        project_id: { type: 'int' },
        user_id: { type: 'int' },
      },
      primaryKey: 'id',
    },
    comments: {
      columns: {
        id: { type: 'int' },
        issue_id: { type: 'int' },
        body: { type: 'text' },
        username: { type: 'text' },
        created: { type: 'int' }, // epoch ms
      },
      primaryKey: 'id',
    },
  },
}

// Fixed value sets, mirroring Linear/LinearLite.
export const STATUSES = ['backlog', 'todo', 'in_progress', 'done', 'canceled'] as const
export const PRIORITIES = ['none', 'low', 'medium', 'high', 'urgent'] as const
export type Status = (typeof STATUSES)[number]
export type Priority = (typeof PRIORITIES)[number]

export const STATUS_LABEL: Record<Status, string> = {
  backlog: 'Backlog',
  todo: 'To Do',
  in_progress: 'In Progress',
  done: 'Done',
  canceled: 'Canceled',
}
export const PRIORITY_LABEL: Record<Priority, string> = {
  none: 'No priority',
  low: 'Low',
  medium: 'Medium',
  high: 'High',
  urgent: 'Urgent',
}
// Sort weight for priority (urgent first).
export const PRIORITY_RANK: Record<Priority, number> = { urgent: 4, high: 3, medium: 2, low: 1, none: 0 }
