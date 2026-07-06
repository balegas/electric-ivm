import { useEffect, useState } from 'react'

// Live-preview an EXISTING shape's contents by polling the engine's `GET /shapes/{id}/rows`, which
// materializes the shape's current rows by folding its own stream. This creates NO new shape (earlier
// we used `/v1/shape`, which spawned an ephemeral view-shape per open and leaked them into the graph
// under React StrictMode). Polling matches the visualizer's existing /graph refresh cadence, so writes
// show up within one interval.

const POLL_MS = 2000

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
