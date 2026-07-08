import { useEffect, useRef, useState } from 'react'

import { useShapeChangeTick } from './shape-change-store'

// Live-preview an EXISTING shape's contents from the engine's `GET /shapes/{id}/rows`, which
// materializes the shape's current rows by folding its own stream. This creates NO new shape (earlier
// we used `/v1/shape`, which spawned an ephemeral view-shape per open and leaked them into the graph
// under React StrictMode). Refresh is event-driven: an initial fetch on selection, then a refetch the
// instant a `/trace` event touches this shape (via the shared shape-change store — reuses the one SSE
// the app already runs, no second connection). A slow safety poll bounds staleness if an event is
// missed (the trace broadcast is lossy by design).

const POLL_MS = 5000

export interface ShapeRow {
  key: string
  value: Record<string, unknown>
}

export interface ShapeContents {
  rows: ShapeRow[]
  columns: string[]
  count: number
  truncated: boolean
  changesOnly: boolean
  live: boolean
  loading: boolean
  error: string | null
}

interface RowsResp {
  id: string
  table: string
  changesOnly: boolean
  count: number
  truncated: boolean
  rows: ShapeRow[]
}

const EMPTY: ShapeContents = {
  rows: [],
  columns: [],
  count: 0,
  truncated: false,
  changesOnly: false,
  live: false,
  loading: false,
  error: null,
}

export function useShapeContents(enabled: boolean, shapeId: string | undefined, limit = 200): ShapeContents {
  const [state, setState] = useState<ShapeContents>(EMPTY)
  // Bumps whenever a /trace event touches this shape — the trigger for an event-driven refetch.
  const tick = useShapeChangeTick(enabled ? shapeId : undefined)
  // The current fetch, published by the setup effect so the tick effect can fire it without owning
  // the abort/interval lifecycle.
  const pollRef = useRef<() => void>(() => {})

  useEffect(() => {
    if (!enabled || !shapeId) {
      setState(EMPTY)
      pollRef.current = () => {}
      return
    }
    setState({ ...EMPTY, loading: true })
    let stopped = false
    const ac = new AbortController()

    const poll = async () => {
      try {
        const r = await fetch(`/engine/shapes/${encodeURIComponent(shapeId)}/rows?limit=${limit}`, {
          signal: ac.signal,
        })
        if (!r.ok) throw new Error(`rows → ${r.status}`)
        const data = (await r.json()) as RowsResp
        if (stopped) return
        const columns = data.rows.length ? Object.keys(data.rows[0]!.value) : []
        setState({
          rows: data.rows,
          columns,
          count: data.count,
          truncated: data.truncated,
          changesOnly: data.changesOnly,
          live: true,
          loading: false,
          error: null,
        })
      } catch (e) {
        if (!ac.signal.aborted && !stopped) setState((s) => ({ ...s, live: false, loading: false, error: String(e) }))
      }
    }

    pollRef.current = () => void poll()
    void poll()
    const t = setInterval(() => void poll(), POLL_MS)
    return () => {
      stopped = true
      ac.abort()
      clearInterval(t)
      pollRef.current = () => {}
    }
  }, [enabled, shapeId, limit])

  // Event-driven refetch: re-run the current fetch when this shape is touched. No loading flash —
  // the visible rows stay put until the fresh set replaces them.
  useEffect(() => {
    pollRef.current()
  }, [tick])

  return state
}
