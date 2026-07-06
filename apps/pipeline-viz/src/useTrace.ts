// Live trace subscription: an EventSource on the engine's `GET /trace` SSE feed (via the
// `/engine` dev proxy). Consumers register a callback; each message — a data TraceEvent, a
// graph-lifecycle event, or a per-node state update — fires it once. Reconnects automatically
// (EventSource semantics); `onOpen` also fires on each (re)connect so the consumer can re-seed
// state it may have missed while disconnected.

import { useEffect, useRef } from 'react'

import type { TraceMessage } from './types'

export function useTrace(enabled: boolean, onEvent: (ev: TraceMessage) => void, onOpen?: () => void): void {
  const cb = useRef(onEvent)
  cb.current = onEvent
  const openCb = useRef(onOpen)
  openCb.current = onOpen

  useEffect(() => {
    if (!enabled) return
    const es = new EventSource('/engine/trace')
    es.onopen = () => openCb.current?.()
    es.onmessage = (m) => {
      try {
        cb.current(JSON.parse(m.data as string) as TraceMessage)
      } catch {
        /* malformed event — ignore */
      }
    }
    return () => es.close()
  }, [enabled])
}
