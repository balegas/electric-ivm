// Reactive per-node state: a module-level store keyed by graph node id, seeded from `GET /state`
// and updated by the `{"type":"state"}` events the engine pushes on the `/trace` SSE feed. Node
// components subscribe per id via `useNodeState`, so a state tick re-renders only the touched
// chips — never the graph. This replaces the old per-concern polling hooks (agg values were
// polled per shape every 2.5s); the only standing connection is the one SSE stream.
//
// Displayed vs authoritative: a chip's number stays IN SYNC with the flow animation — it only
// advances once the travelling +1/−1 dot reaches that node. So the store keeps two layers: the
// `authoritative` truth from the engine, and the `displayed` value the chips read. A live change
// stages each node's reveal (`applyStateStaggered`) to fire when its dot arrives; the backfill /
// re-seed / no-animation paths reveal immediately (`applyState`). A reveal always snaps `displayed`
// to the CURRENT authoritative, so the chip converges to the truth and never drifts, even if
// several changes overlap.

import { useSyncExternalStore } from 'react'

import type { NodeStateSummary, StateSnapshot } from './types'

/** What chips render — lags `authoritative` until the flow animation reaches each node. */
const displayed = new Map<string, NodeStateSummary>()
/** The engine's latest truth per node. Reveals snap `displayed` to this. */
const authoritative = new Map<string, NodeStateSummary>()
/** Pending staggered reveals (node id → timer), so a newer update or re-seed can supersede them. */
const timers = new Map<string, ReturnType<typeof setTimeout>>()
const listeners = new Set<() => void>()

function notify() {
  for (const l of listeners) l()
}

function subscribe(l: () => void): () => void {
  listeners.add(l)
  return () => listeners.delete(l)
}

function sameSummary(a: NodeStateSummary | undefined, b: NodeStateSummary | undefined): boolean {
  return a !== undefined && b !== undefined && JSON.stringify(a) === JSON.stringify(b)
}

/** Snap `displayed[id]` to the current authoritative value. Returns whether it changed (keeps
 *  object identity for no-ops so subscribed components don't re-render). */
function reveal(id: string): boolean {
  const next = authoritative.get(id)
  if (next === undefined) return false
  if (sameSummary(displayed.get(id), next)) return false
  displayed.set(id, next)
  return true
}

function clearTimer(id: string): void {
  const t = timers.get(id)
  if (t) {
    clearTimeout(t)
    timers.delete(id)
  }
}

/** Apply a batch immediately: update authoritative truth AND reveal it now (cancelling any pending
 *  staggered reveal). Used for the initial backfill, the periodic/​reconnect re-seed, and as the
 *  no-animation fallback — chips must never get stuck behind an animation that isn't playing. */
export function applyState(update: Record<string, NodeStateSummary>): void {
  let changed = false
  for (const [id, next] of Object.entries(update)) {
    authoritative.set(id, next)
    clearTimer(id)
    if (reveal(id)) changed = true
  }
  if (changed) notify()
}

/** Apply a batch in sync with the flow animation: record the authoritative truth now, but defer
 *  each node's visible reveal by `delayMs(id)` — the moment the travelling dot arrives at it. A
 *  delay of 0 (or a node the animation doesn't touch, e.g. a change dropped at σ before it) reveals
 *  immediately. The reveal snaps to whatever authoritative is when it fires, so overlapping changes
 *  still converge to the latest truth. */
export function applyStateStaggered(
  update: Record<string, NodeStateSummary>,
  delayMs: (id: string) => number,
): void {
  let changed = false
  for (const [id, next] of Object.entries(update)) {
    authoritative.set(id, next)
    clearTimer(id)
    const d = delayMs(id)
    if (!(d > 0)) {
      if (reveal(id)) changed = true
      continue
    }
    timers.set(
      id,
      setTimeout(() => {
        timers.delete(id)
        if (reveal(id)) notify()
      }, d),
    )
  }
  if (changed) notify()
}

/** Seed (or re-seed after an SSE reconnect) from the engine's full snapshot. Entries the snapshot
 *  no longer carries are dropped — their nodes are gone from the graph too. Applied immediately
 *  (the authoritative fallback): it bounds staleness and unsticks any chip a lost stagger left behind. */
export async function seedState(): Promise<void> {
  try {
    const r = await fetch('/engine/state')
    if (!r.ok) return
    const snap = (await r.json()) as StateSnapshot
    for (const id of [...authoritative.keys()]) {
      if (!(id in snap.nodes)) {
        clearTimer(id)
        authoritative.delete(id)
        displayed.delete(id)
      }
    }
    applyState(snap.nodes)
    notify()
  } catch {
    /* engine unreachable — the next reconnect re-seeds */
  }
}

/** The live state summary of one node, or undefined while unknown. Re-renders the caller only
 *  when this node's displayed entry is replaced. */
export function useNodeState(id: string): NodeStateSummary | undefined {
  return useSyncExternalStore(subscribe, () => displayed.get(id))
}
