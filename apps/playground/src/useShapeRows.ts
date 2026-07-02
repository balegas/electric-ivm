// Ground truth for a device card: poll the shape's materialized rows via the server proxy.
// Between polls we diff by key to produce the card's raw upsert/delete feed — the same enter/leave
// the wire protocol would deliver. `tick` forces an immediate re-poll (after user actions).

import { useEffect, useRef, useState } from 'react'

import { api } from './api.ts'

export interface FeedEntry {
  kind: 'upsert' | 'delete'
  key: string
  value: Record<string, unknown> | null
  at: number
}

export interface ShapeRowsState {
  rows: { key: string; value: Record<string, unknown> }[]
  feed: FeedEntry[]
  changedAt: number
  error: string | null
}

const POLL_MS = 2000
const FEED_CAP = 30

export function useShapeRows(workspaceId: string | undefined, shapeId: string, tick: number): ShapeRowsState {
  const [state, setState] = useState<ShapeRowsState>({ rows: [], feed: [], changedAt: 0, error: null })
  const prev = useRef<Map<string, string> | null>(null)
  const feed = useRef<FeedEntry[]>([])

  useEffect(() => {
    if (!workspaceId) return
    let stopped = false
    const poll = async () => {
      try {
        const data = await api.shapeRows(workspaceId, shapeId)
        if (stopped) return
        const cur = new Map(data.rows.map((r) => [r.key, JSON.stringify(r.value)]))
        let changed = false
        if (prev.current) {
          for (const r of data.rows) {
            if (prev.current.get(r.key) !== JSON.stringify(r.value)) {
              feed.current.unshift({ kind: 'upsert', key: r.key, value: r.value, at: Date.now() })
              changed = true
            }
          }
          for (const key of prev.current.keys()) {
            if (!cur.has(key)) {
              feed.current.unshift({ kind: 'delete', key, value: null, at: Date.now() })
              changed = true
            }
          }
          feed.current = feed.current.slice(0, FEED_CAP)
        }
        prev.current = cur
        setState((s) => ({
          rows: data.rows,
          feed: [...feed.current],
          changedAt: changed ? Date.now() : s.changedAt,
          error: null,
        }))
      } catch (e) {
        if (!stopped) setState((s) => ({ ...s, error: String((e as Error).message ?? e) }))
      }
    }
    void poll()
    const t = setInterval(() => void poll(), POLL_MS)
    return () => {
      stopped = true
      clearInterval(t)
    }
  }, [workspaceId, shapeId, tick])

  return state
}
