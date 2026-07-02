// Trace fan-out: one lazy upstream SSE connection to the engine's /trace, re-broadcast to every
// connected browser with per-workspace tagging. Your events arrive whole; other visitors' events
// are stripped to shared-node hops and rowless weights — enough to render an ambient pulse, never
// enough to leak another workspace's data.

import type { ServerResponse } from 'node:http'
import type { TraceEvent, TraceHop } from '../shared/types.ts'

/** Raw engine event (crate::trace::TraceEvent) — same wire shape minus `yours`. */
export type EngineTraceEvent = Omit<TraceEvent, 'yours'>

const SHARED_PREFIXES = ['table:', 'family:', 'node:']

/** Tag an engine event for one subscriber and strip it if it isn't theirs. */
export function tagAndStrip(ev: EngineTraceEvent, ws: string, ownerOf: Map<string, string>): TraceEvent {
  const yours =
    ev.shapes.some((sid) => ownerOf.get(sid) === ws) ||
    ev.delta.some((d) => (d.row as { workspace_id?: unknown }).workspace_id === ws)
  if (yours) {
    // Shapes belonging to OTHER workspaces still don't leak: drop foreign shape ids/hops.
    const foreign = (sid: string) => ownerOf.has(sid) && ownerOf.get(sid) !== ws
    return {
      ...ev,
      hops: ev.hops.filter((h) => !isForeignShapeHop(h, foreign)),
      shapes: ev.shapes.filter((sid) => !foreign(sid)),
      yours: true,
    }
  }
  return {
    lsn: ev.lsn,
    table: ev.table,
    delta: ev.delta.map((d) => ({ row: {}, w: d.w })),
    hops: ev.hops.filter((h) => SHARED_PREFIXES.some((p) => h.node.startsWith(p))).map((h) => ({ ...h, key: undefined })),
    shapes: [],
    yours: false,
  }
}

function isForeignShapeHop(h: TraceHop, foreign: (sid: string) => boolean): boolean {
  for (const p of ['shape:', 'filter:']) {
    if (h.node.startsWith(p)) return foreign(h.node.slice(p.length))
  }
  return false
}

interface TraceClient {
  ws: string
  res: ServerResponse
}

export class TraceFanout {
  private clients = new Set<TraceClient>()
  private upstreamAbort: AbortController | null = null
  private ownersFresh = 0
  private owners: Map<string, string> = new Map()

  constructor(
    private traceUrl: string,
    private loadOwners: () => Promise<Map<string, string>>,
  ) {}

  /** Attach a browser. Writes SSE headers and keeps the response open until the client goes away. */
  async attach(ws: string, res: ServerResponse): Promise<void> {
    res.writeHead(200, {
      'content-type': 'text/event-stream',
      'cache-control': 'no-cache',
      connection: 'keep-alive',
      'x-accel-buffering': 'no',
    })
    res.write(':connected\n\n')
    const client: TraceClient = { ws, res }
    this.clients.add(client)
    res.on('close', () => {
      this.clients.delete(client)
      if (this.clients.size === 0) this.stopUpstream()
    })
    await this.ensureUpstream()
  }

  /** The shape→workspace index, refreshed at most every 2s (shape churn is rare). */
  private async ownersIndex(): Promise<Map<string, string>> {
    if (Date.now() - this.ownersFresh > 2000) {
      this.owners = await this.loadOwners()
      this.ownersFresh = Date.now()
    }
    return this.owners
  }

  /** Force-refresh on shape create/delete so the very next event tags correctly. */
  invalidateOwners(): void {
    this.ownersFresh = 0
  }

  private stopUpstream(): void {
    this.upstreamAbort?.abort()
    this.upstreamAbort = null
  }

  private async ensureUpstream(): Promise<void> {
    if (this.upstreamAbort) return
    const ac = new AbortController()
    this.upstreamAbort = ac
    void this.pump(ac)
  }

  /** Read the engine's SSE stream and re-broadcast; reconnect with backoff while clients remain. */
  private async pump(ac: AbortController): Promise<void> {
    let backoff = 250
    while (!ac.signal.aborted && this.clients.size > 0) {
      try {
        const res = await fetch(this.traceUrl, { signal: ac.signal, headers: { accept: 'text/event-stream' } })
        if (!res.ok || !res.body) throw new Error(`trace upstream → ${res.status}`)
        backoff = 250
        const reader = res.body.getReader()
        const decoder = new TextDecoder()
        let buf = ''
        for (;;) {
          const { done, value } = await reader.read()
          if (done) break
          buf += decoder.decode(value, { stream: true })
          let idx: number
          // SSE events are separated by a blank line; each of ours is a single `data:` line.
          while ((idx = buf.indexOf('\n\n')) >= 0) {
            const chunk = buf.slice(0, idx)
            buf = buf.slice(idx + 2)
            const data = chunk
              .split('\n')
              .filter((l) => l.startsWith('data:'))
              .map((l) => l.slice(5).trim())
              .join('')
            if (!data) continue
            await this.broadcast(data)
          }
        }
      } catch {
        if (ac.signal.aborted) return
      }
      if (this.clients.size === 0) break
      await new Promise((r) => setTimeout(r, backoff))
      backoff = Math.min(backoff * 2, 5000)
    }
    if (this.upstreamAbort === ac) this.upstreamAbort = null
  }

  private async broadcast(json: string): Promise<void> {
    let ev: EngineTraceEvent
    try {
      ev = JSON.parse(json) as EngineTraceEvent
    } catch {
      return
    }
    const owners = await this.ownersIndex()
    for (const c of this.clients) {
      try {
        c.res.write(`data: ${JSON.stringify(tagAndStrip(ev, c.ws, owners))}\n\n`)
      } catch {
        this.clients.delete(c)
      }
    }
  }
}
