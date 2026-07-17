// Latest Z-set delta per table, captured from the `/trace` SSE data events the app already
// handles for the flow animation. A delta event carries the change as weighted rows —
// insert = [{row,+1}], delete = [{row,−1}], update = [{old,−1},{new,+1}] — i.e. the DBSP Z-set
// the Δ change operator emits. This store keeps only the MOST RECENT delta per table so the Δ
// node (and its detail panel) can surface the reconstructed Z-set; rapid successive deltas simply
// replace the entry, and `clearDeltas` drops stale ones on a reset.

import { useSyncExternalStore } from 'react'

import { derivedVia } from './trace-anim'
import type { TraceEvent } from './types'

/** The reconstructed Z-set of one change: its weighted rows and when it was captured. */
export interface CapturedDelta {
  rows: { row: Record<string, unknown>; w: number }[]
  at: number
  /** For a subquery move-in/out, the inner/membership table the change entered through — this Δ
   *  arrived via a pooled Postgres query-back, not this table's own replication stream. null for a
   *  normal same-table change. Drives the "via query-back" tag on the Δ node's inline peek. */
  via: string | null
}

const latest = new Map<string, CapturedDelta>()
const listeners = new Set<() => void>()

function notify(): void {
  for (const l of listeners) l()
}

function subscribe(l: () => void): () => void {
  listeners.add(l)
  return () => listeners.delete(l)
}

/** Record a trace event's Z-set as the latest delta for its table. Empty deltas (no-ops) are
 *  ignored so they don't wipe a still-relevant prior delta. Replaces the stored object identity,
 *  so subscribed components re-render. */
export function recordDelta(ev: TraceEvent): void {
  if (!ev.delta || ev.delta.length === 0) return
  latest.set(ev.table, { rows: ev.delta, at: Date.now(), via: derivedVia(ev) })
  notify()
}

/** Drop every captured delta (e.g. a purge/reset — the prior Z-sets are stale). */
export function clearDeltas(): void {
  if (latest.size === 0) return
  latest.clear()
  notify()
}

/** The most recent Z-set delta for a table, or undefined if none seen yet. Re-renders the caller
 *  when that table's delta is replaced or cleared. */
export function useLatestDelta(table: string | null): CapturedDelta | undefined {
  return useSyncExternalStore(subscribe, () => (table ? latest.get(table) : undefined))
}
