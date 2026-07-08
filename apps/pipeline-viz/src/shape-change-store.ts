// Per-shape change ticks, captured from the `/trace` SSE data events the app already handles for
// the flow animation. Every data event carries `shapes: string[]` — the shape ids that change
// touched. This store bumps a counter per touched shape so a component showing one shape's live
// data (e.g. the SINK operator's row preview) can refetch the instant a relevant change arrives,
// reusing the single existing SSE stream rather than opening a second one.

import { useSyncExternalStore } from 'react'

const ticks = new Map<string, number>()
const listeners = new Set<() => void>()

function notify(): void {
  for (const l of listeners) l()
}

function subscribe(l: () => void): () => void {
  listeners.add(l)
  return () => listeners.delete(l)
}

/** Bump the change tick for every shape a trace event touched. Subscribed views re-render and
 *  refetch. No-op for empty arrays so unrelated events don't churn every subscriber. */
export function recordShapeChanges(shapes: string[] | undefined): void {
  if (!shapes || shapes.length === 0) return
  for (const id of shapes) ticks.set(id, (ticks.get(id) ?? 0) + 1)
  notify()
}

/** The change tick for one shape (starts at 0). Re-renders the caller each time that shape is
 *  touched by a change, so an effect keyed on it can refetch on demand. */
export function useShapeChangeTick(shapeId: string | undefined | null): number {
  return useSyncExternalStore(subscribe, () => (shapeId ? ticks.get(shapeId) ?? 0 : 0))
}
