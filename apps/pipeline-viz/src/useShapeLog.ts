import { useEffect, useState } from 'react'

// Live change log of an EXISTING shape by polling the engine's `GET /shapes/{id}/log`, which
// returns the tail of the shape's stream as-is (insert/update/delete envelopes, oldest → newest).
// Used for changes-only feed shapes, whose natural "contents" is the flow of changes, not a set.

const POLL_MS = 2000

export interface ShapeLogEntry {
  op: string
  key: string
  value?: Record<string, unknown>
  /** Prior row on update/delete — what a delete removed. */
  old?: Record<string, unknown>
  lsn?: string
}

export interface ShapeLog {
  entries: ShapeLogEntry[]
  total: number
  loading: boolean
  live: boolean
  error: string | null
}

interface LogResp {
  id: string
  table: string
  changesOnly: boolean
  total: number
  entries: ShapeLogEntry[]
}

const EMPTY: ShapeLog = { entries: [], total: 0, loading: false, live: false, error: null }

export function useShapeLog(enabled: boolean, shapeId: string | undefined, limit = 50): ShapeLog {
  const [state, setState] = useState<ShapeLog>(EMPTY)

  useEffect(() => {
    if (!enabled || !shapeId) {
      setState(EMPTY)
      return
    }
    setState({ ...EMPTY, loading: true })
    let stopped = false
    const ac = new AbortController()

    const poll = async () => {
      try {
        const r = await fetch(`/engine/shapes/${encodeURIComponent(shapeId)}/log?limit=${limit}`, {
          signal: ac.signal,
        })
        if (!r.ok) throw new Error(`log → ${r.status}`)
        const data = (await r.json()) as LogResp
        if (stopped) return
        setState({ entries: data.entries, total: data.total, loading: false, live: true, error: null })
      } catch (e) {
        if (!ac.signal.aborted && !stopped) setState((s) => ({ ...s, live: false, loading: false, error: String(e) }))
      }
    }

    void poll()
    const t = setInterval(() => void poll(), POLL_MS)
    return () => {
      stopped = true
      ac.abort()
      clearInterval(t)
    }
  }, [enabled, shapeId, limit])

  return state
}
