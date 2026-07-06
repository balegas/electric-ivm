import { useEffect, useState } from 'react'

import type { EngineGraph } from './types'

// Poll the LIVE SCALAR of every aggregation shape in the graph via `GET /shapes/{id}/rows`
// (the same endpoint the detail panel uses — it folds the shape's own stream; for an aggregation
// shape that is a single row keyed "agg" whose `value.value` is the running scalar).
// One effect for all aggregations, matching the visualizer's poll cadence; a no-op when the
// graph has none. Returns shapeId → formatted scalar (e.g. "42").

const POLL_MS = 2500

interface RowsResp {
  rows: { key: string; value: Record<string, unknown> }[]
}

export function fmtScalar(v: unknown): string {
  if (typeof v === 'number') {
    return Number.isInteger(v) ? v.toLocaleString() : v.toLocaleString(undefined, { maximumFractionDigits: 2 })
  }
  return String(v)
}

export function useAggValues(graph: EngineGraph | null): Map<string, string> {
  const [values, setValues] = useState<Map<string, string>>(new Map())
  // Key the effect on the SET of aggregation shape ids, not the graph object — /graph polls
  // every 2.5s and must not tear the interval down each time.
  const idsKey = (graph?.shapes ?? [])
    .filter((s) => s.aggregate)
    .map((s) => s.id)
    .join(',')

  useEffect(() => {
    const ids = idsKey ? idsKey.split(',') : []
    if (ids.length === 0) {
      setValues((prev) => (prev.size === 0 ? prev : new Map()))
      return
    }
    let stopped = false
    const ac = new AbortController()

    const poll = async () => {
      const entries = await Promise.all(
        ids.map(async (id): Promise<[string, string] | null> => {
          try {
            const r = await fetch(`/engine/shapes/${encodeURIComponent(id)}/rows?limit=1`, { signal: ac.signal })
            if (!r.ok) return null
            const data = (await r.json()) as RowsResp
            const v = data.rows[0]?.value?.['value']
            return v === undefined || v === null ? null : [id, fmtScalar(v)]
          } catch {
            return null
          }
        }),
      )
      if (stopped) return
      const next = new Map(entries.filter((e): e is [string, string] => e !== null))
      // Keep the previous Map identity when nothing changed so downstream memos don't re-run.
      setValues((prev) =>
        prev.size === next.size && [...next].every(([k, v]) => prev.get(k) === v) ? prev : next,
      )
    }

    void poll()
    const t = setInterval(() => void poll(), POLL_MS)
    return () => {
      stopped = true
      ac.abort()
      clearInterval(t)
    }
  }, [idsKey])

  return values
}
