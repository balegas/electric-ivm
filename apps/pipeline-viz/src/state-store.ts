// Reactive per-node state: a module-level store keyed by graph node id, seeded from `GET /state`
// and updated by the `{"type":"state"}` events the engine pushes on the `/trace` SSE feed. Node
// components subscribe per id via `useNodeState`, so a state tick re-renders only the touched
// chips — never the graph. This replaces the old per-concern polling hooks (agg values were
// polled per shape every 2.5s); the only standing connection is the one SSE stream.

import { useSyncExternalStore } from 'react'

import type { NodeStateSummary, StateSnapshot } from './types'

const nodes = new Map<string, NodeStateSummary>()
const listeners = new Set<() => void>()

function notify() {
  for (const l of listeners) l()
}

function subscribe(l: () => void): () => void {
  listeners.add(l)
  return () => listeners.delete(l)
}

/** Merge a batch of summaries, keeping object identity for unchanged entries so subscribed
 *  components (compared by `Object.is` in `useSyncExternalStore`) don't re-render for no-ops. */
export function applyState(update: Record<string, NodeStateSummary>): void {
  let changed = false
  for (const [id, next] of Object.entries(update)) {
    const prev = nodes.get(id)
    if (prev && JSON.stringify(prev) === JSON.stringify(next)) continue
    nodes.set(id, next)
    changed = true
  }
  if (changed) notify()
}

/** Seed (or re-seed after an SSE reconnect) from the engine's full snapshot. Entries the snapshot
 *  no longer carries are dropped — their nodes are gone from the graph too. */
export async function seedState(): Promise<void> {
  try {
    const r = await fetch('/engine/state')
    if (!r.ok) return
    const snap = (await r.json()) as StateSnapshot
    for (const id of [...nodes.keys()]) {
      if (!(id in snap.nodes)) nodes.delete(id)
    }
    applyState(snap.nodes)
    notify()
  } catch {
    /* engine unreachable — the next reconnect re-seeds */
  }
}

/** The live state summary of one node, or undefined while unknown. Re-renders the caller only
 *  when this node's entry is replaced. */
export function useNodeState(id: string): NodeStateSummary | undefined {
  return useSyncExternalStore(subscribe, () => nodes.get(id))
}
