// Live trace subscription: an EventSource on the engine's `GET /trace` SSE feed (via the
// `/engine` dev proxy). Consumers register a callback; each event — a data TraceEvent or a
// graph-lifecycle event — fires it once. Reconnects automatically (EventSource semantics) and
// tears down with the component.

import { useEffect, useRef } from 'react'

import type { TraceEvent, TraceLifecycle } from './types'

export function useTrace(enabled: boolean, onEvent: (ev: TraceEvent | TraceLifecycle) => void): void {
  const cb = useRef(onEvent)
  cb.current = onEvent

  useEffect(() => {
    if (!enabled) return
    const es = new EventSource('/engine/trace')
    es.onmessage = (m) => {
      try {
        cb.current(JSON.parse(m.data as string) as TraceEvent | TraceLifecycle)
      } catch {
        /* malformed event — ignore */
      }
    }
    return () => es.close()
  }, [enabled])
}
