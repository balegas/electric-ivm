// Live trace subscription: an EventSource on the playground server's per-workspace SSE feed.
// Consumers register a callback; each TraceEvent fires it once. Reconnects automatically
// (EventSource semantics) and tears down with the component.

import { useEffect, useRef } from 'react'

import type { TraceEvent } from '../shared/types.ts'

export function useTrace(workspaceId: string | undefined, onEvent: (ev: TraceEvent) => void): void {
  const cb = useRef(onEvent)
  cb.current = onEvent

  useEffect(() => {
    if (!workspaceId) return
    const es = new EventSource(`/api/trace?workspace=${encodeURIComponent(workspaceId)}`)
    es.onmessage = (m) => {
      try {
        cb.current(JSON.parse(m.data as string) as TraceEvent)
      } catch {
        /* malformed event — ignore */
      }
    }
    return () => es.close()
  }, [workspaceId])
}
