import { createClient } from '@electric-lite/client'
import type { ShapeDef } from '@electric-lite/protocol'
import { schema } from './schema'

// The browser talks to the API and reads shape streams through the Vite dev proxy
// (/api -> tRPC API, /ds -> durable-streams). long-poll is the most proxy-friendly live mode.
export const client = createClient({
  apiUrl: '/api',
  schema,
  dsBaseUrl: '/ds',
  liveMode: 'long-poll',
})

// The live shape rendered on the right: active, high-priority todos. The engine evaluates this
// predicate; rows enter/leave as you edit todos on the left.
export const LIVE_SHAPE: ShapeDef = {
  table: 'todos',
  where: { and: [{ col: 'done', op: 'eq', value: false }, { col: 'priority', op: 'gte', value: 3 }] },
}
