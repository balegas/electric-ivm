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
        created: { type: 'int' }, // epoch ms
        modified: { type: 'int' }, // epoch ms
        kanbanorder: { type: 'float' }, // fractional ordering within a status column
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
