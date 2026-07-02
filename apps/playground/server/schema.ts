// The playground's replicated schema (protocol form — used by the engine boot in dev/tests) and
// the seed constants. Data tables carry workspace_id on every row; the playground_* meta tables
// are NOT part of this schema, so they are never replicated into the engine.

import type { Schema } from '@electric-ivm/protocol'

export const PLAYGROUND_SCHEMA: Schema = {
  tables: {
    projects: {
      columns: {
        id: { type: 'int' },
        workspace_id: { type: 'text' },
        name: { type: 'text' },
        team: { type: 'text' },
      },
      primaryKey: 'id',
    },
    issues: {
      columns: {
        id: { type: 'int' },
        workspace_id: { type: 'text' },
        project_id: { type: 'int' },
        title: { type: 'text' },
        status: { type: 'text' },
        priority: { type: 'int' },
      },
      primaryKey: 'id',
    },
  },
}

export const SEED_PROJECTS: { name: string; team: string }[] = [
  { name: 'Web App', team: 'web' },
  { name: 'Mobile', team: 'mobile' },
]

export const SEED_ISSUES: { title: string; status: string; priority: number; pi: number }[] = [
  { title: 'Fix login redirect', status: 'todo', priority: 3, pi: 0 },
  { title: 'Dark mode', status: 'todo', priority: 1, pi: 0 },
  { title: 'Sync conflict banner', status: 'in_progress', priority: 4, pi: 1 },
  { title: 'Upgrade Postgres 17', status: 'done', priority: 2, pi: 0 },
]

export const ISSUE_TITLES = [
  'Flaky e2e test', 'Rate-limit uploads', 'Empty-state copy', 'Retry on 502', 'Offline banner',
  'Split vendor bundle', 'Audit log export', 'Keyboard shortcuts', 'Migrate icons', 'Cache avatars',
] as const

export const PROJECT_NAMES = ['Infra', 'Design System', 'Billing', 'Search', 'Notifications'] as const
