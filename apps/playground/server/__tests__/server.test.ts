// Integration: the playground server against a real Postgres + engine (via the conformance
// harness — ephemeral DB per file, engine subprocess in Postgres mode). Covers provisioning
// idempotency, the action lifecycle, workspace scoping, scene idempotency + self-heal, graph/rows
// proxying, subsets, reset recovery, rate limiting, and a live end-to-end trace event.

import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { bootHarness, type Harness } from '../../../../packages/conformance/src/harness.ts'
import type { Order, TraceEvent, WorkspaceState } from '../../shared/types.ts'
import { createPlaygroundServer, type PlaygroundServer } from '../main.ts'
import { PLAYGROUND_SCHEMA } from '../schema.ts'

let h: Harness
let s: PlaygroundServer

beforeAll(async () => {
  h = await bootHarness(PLAYGROUND_SCHEMA)
  s = await createPlaygroundServer({ pgUrl: h.pgUrl, engineUrl: h.engineUrl, epoch: 7, ttlHours: 0 })
}, 120_000)

afterAll(async () => {
  await s?.close()
  await h?.shutdown()
})

async function api<T>(path: string, init?: RequestInit): Promise<{ status: number; body: T }> {
  const res = await fetch(`${s.url}${path}`, {
    headers: { 'content-type': 'application/json' },
    ...init,
  })
  return { status: res.status, body: (await res.json()) as T }
}

async function newWorkspace(): Promise<WorkspaceState> {
  const { status, body } = await api<WorkspaceState>('/api/workspace', { method: 'POST', body: '{}' })
  expect(status).toBe(200)
  return body
}

describe('workspaces', () => {
  it('provisions 1 restaurant + 3 seed orders and is idempotent by id', async () => {
    const ws = await newWorkspace()
    expect(ws.restaurants).toHaveLength(1)
    expect(ws.orders).toHaveLength(3)
    expect(ws.workspace.epoch).toBe(7)
    const again = await api<WorkspaceState>('/api/workspace', {
      method: 'POST',
      body: JSON.stringify({ existingId: ws.workspace.id }),
    })
    expect(again.body.workspace.id).toBe(ws.workspace.id)
    expect(again.body.restaurants.map((r) => r.id)).toEqual(ws.restaurants.map((r) => r.id))
  })

  it('an unknown id mints a fresh workspace (reset recovery)', async () => {
    const { body } = await api<WorkspaceState>('/api/workspace', {
      method: 'POST',
      body: JSON.stringify({ existingId: 'w_gone' }),
    })
    expect(body.workspace.id).not.toBe('w_gone')
    expect(body.restaurants).toHaveLength(1)
  })

  it('add_restaurant grows the world from the seed pool, workspace-scoped', async () => {
    const ws = await newWorkspace()
    const r = await api<{ ok: true; restaurant: { id: number; name: string; workspace_id: string } }>('/api/action', {
      method: 'POST',
      body: JSON.stringify({ workspace: ws.workspace.id, verb: 'add_restaurant' }),
    })
    expect(r.status).toBe(200)
    expect(r.body.restaurant.workspace_id).toBe(ws.workspace.id)
    expect(r.body.restaurant.name).not.toBe(ws.restaurants[0]!.name)
    const state = await api<WorkspaceState>(`/api/workspace/${ws.workspace.id}`)
    expect(state.body.restaurants).toHaveLength(2)
  })

  it('GET of an unknown workspace is a 404 carrying the epoch', async () => {
    const r = await api<{ error: string; epoch: number }>('/api/workspace/w_nope')
    expect(r.status).toBe(404)
    expect(r.body.epoch).toBe(7)
  })
})

describe('actions', () => {
  it('runs the order lifecycle and rejects illegal transitions', async () => {
    const ws = await newWorkspace()
    const rid = ws.restaurants[0]!.id
    const placed = await api<{ ok: true; order: Order }>('/api/action', {
      method: 'POST',
      body: JSON.stringify({ workspace: ws.workspace.id, verb: 'place_order', restaurantId: rid }),
    })
    expect(placed.status).toBe(200)
    expect(placed.body.order.status).toBe('new')

    const oid = placed.body.order.id
    const act = (verb: string, orderId = oid) =>
      api<{ ok: true; order: Order }>('/api/action', {
        method: 'POST',
        body: JSON.stringify({ workspace: ws.workspace.id, verb, orderId }),
      })

    // deliver before riding -> 409
    expect((await act('deliver')).status).toBe(409)
    expect((await act('start_cooking')).body.order.status).toBe('cooking')
    expect((await act('pickup')).body.order.status).toBe('riding')
    expect((await act('deliver')).body.order.status).toBe('delivered')
    // terminal -> cancel is illegal
    expect((await act('cancel')).status).toBe(409)
  })

  it("cannot touch another workspace's orders (404)", async () => {
    const a = await newWorkspace()
    const b = await newWorkspace()
    const r = await api('/api/action', {
      method: 'POST',
      body: JSON.stringify({ workspace: b.workspace.id, verb: 'start_cooking', orderId: a.orders[0]!.id }),
    })
    expect(r.status).toBe(404)
  })

  it('rate limits rapid writes with 429', async () => {
    const ws = await newWorkspace()
    const rid = ws.restaurants[0]!.id
    const results = await Promise.all(
      Array.from({ length: 30 }, () =>
        api('/api/action', {
          method: 'POST',
          body: JSON.stringify({ workspace: ws.workspace.id, verb: 'place_order', restaurantId: rid }),
        }),
      ),
    )
    expect(results.some((r) => r.status === 429)).toBe(true)
    expect(results.some((r) => r.status === 200)).toBe(true)
  })
})

describe('scenes and shapes', () => {
  it('provisions scene shapes idempotently and self-heals a lost engine shape', async () => {
    const ws = await newWorkspace()
    const first = await api<{ scene: number; shapes: { id: string }[] }>('/api/scene', {
      method: 'POST',
      body: JSON.stringify({ workspace: ws.workspace.id, scene: 2 }),
    })
    expect(first.status).toBe(200)
    expect(first.body.shapes).toHaveLength(1)
    const sid = first.body.shapes[0]!.id

    const second = await api<{ shapes: { id: string }[] }>('/api/scene', {
      method: 'POST',
      body: JSON.stringify({ workspace: ws.workspace.id, scene: 2 }),
    })
    expect(second.body.shapes.map((x) => x.id)).toEqual([sid])

    // Simulate an engine that lost the shape (restart): drop it engine-side, re-provision.
    await fetch(`${h.engineUrl}/shapes/${sid}`, { method: 'DELETE' })
    const healed = await api<{ shapes: { id: string }[] }>('/api/scene', {
      method: 'POST',
      body: JSON.stringify({ workspace: ws.workspace.id, scene: 2 }),
    })
    expect(healed.status).toBe(200)
    expect(healed.body.shapes).toHaveLength(1)
    expect(healed.body.shapes[0]!.id).not.toBe(sid)
  })

  it('graph returns the engine graph plus only my shape ids; rows are ownership-guarded', async () => {
    const a = await newWorkspace()
    const b = await newWorkspace()
    const sceneA = await api<{ shapes: { id: string }[] }>('/api/scene', {
      method: 'POST',
      body: JSON.stringify({ workspace: a.workspace.id, scene: 2 }),
    })
    const shapeA = sceneA.body.shapes[0]!.id

    const graphB = await api<{ graph: { shapes: { id: string }[] }; mine: string[] }>(
      `/api/graph?workspace=${b.workspace.id}`,
    )
    expect(graphB.status).toBe(200)
    expect(graphB.body.mine).not.toContain(shapeA)
    // the full graph is public introspection — a's shape is visible there (honest shared engine)
    expect(graphB.body.graph.shapes.map((x) => x.id)).toContain(shapeA)

    const rowsDenied = await api(`/api/shapes/${shapeA}/rows?workspace=${b.workspace.id}`)
    expect(rowsDenied.status).toBe(404)
    const rowsOk = await api<{ count: number }>(`/api/shapes/${shapeA}/rows?workspace=${a.workspace.id}`)
    expect(rowsOk.status).toBe(200)
  })

  it('creates a custom shape whose predicate carries the workspace conjunct, then deletes it', async () => {
    const ws = await newWorkspace()
    const created = await api<{ id: string; where: unknown }>('/api/shape', {
      method: 'POST',
      body: JSON.stringify({
        workspace: ws.workspace.id,
        label: 'big orders',
        role: 'custom',
        spec: { table: 'orders', where: [{ col: 'total', op: 'gte', value: 20 }] },
      }),
    })
    expect(created.status).toBe(200)
    expect(JSON.stringify(created.body.where)).toContain('workspace_id')

    const del = await api(`/api/shape/${created.body.id}?workspace=${ws.workspace.id}`, { method: 'DELETE' })
    expect(del.status).toBe(200)
    const graph = await api<{ mine: string[] }>(`/api/graph?workspace=${ws.workspace.id}`)
    expect(graph.body.mine).not.toContain(created.body.id)
  })

  it('subset queries return ordered rows pinned at an LSN', async () => {
    const ws = await newWorkspace()
    const r = await api<{ rows: { total: number }[]; lsn: string }>('/api/subset', {
      method: 'POST',
      body: JSON.stringify({ workspace: ws.workspace.id, orderBy: { col: 'total', desc: true }, limit: 3 }),
    })
    expect(r.status).toBe(200)
    expect(r.body.rows.length).toBeGreaterThan(0)
    expect(r.body.rows.length).toBeLessThanOrEqual(3)
    expect(typeof r.body.lsn).toBe('string')
    const totals = r.body.rows.map((x) => x.total)
    expect([...totals].sort((x, y) => y - x)).toEqual(totals)
  })
})

describe('trace', () => {
  it('a write produces a yours-tagged trace event that reaches the SSE subscriber', async () => {
    const ws = await newWorkspace()
    await api('/api/scene', { method: 'POST', body: JSON.stringify({ workspace: ws.workspace.id, scene: 2 }) })

    const ac = new AbortController()
    const events: TraceEvent[] = []
    const streamDone = (async () => {
      const res = await fetch(`${s.url}/api/trace?workspace=${ws.workspace.id}`, { signal: ac.signal })
      const reader = res.body!.getReader()
      const dec = new TextDecoder()
      let buf = ''
      for (;;) {
        const { done, value } = await reader.read()
        if (done) break
        buf += dec.decode(value, { stream: true })
        let i: number
        while ((i = buf.indexOf('\n\n')) >= 0) {
          const chunk = buf.slice(0, i)
          buf = buf.slice(i + 2)
          const data = chunk.split('\n').filter((l) => l.startsWith('data:')).map((l) => l.slice(5).trim()).join('')
          if (data) events.push(JSON.parse(data) as TraceEvent)
        }
      }
    })().catch(() => {})

    // Give the fan-out a beat to attach upstream, then write.
    await new Promise((r) => setTimeout(r, 300))
    const placed = await api<{ order: Order }>('/api/action', {
      method: 'POST',
      body: JSON.stringify({ workspace: ws.workspace.id, verb: 'place_order', restaurantId: ws.restaurants[0]!.id }),
    })
    expect(placed.status).toBe(200)

    // Wait for the event to arrive through replication -> engine -> SSE -> fan-out.
    const deadline = Date.now() + 15_000
    while (Date.now() < deadline && !events.some((e) => e.yours && e.table === 'orders')) {
      await new Promise((r) => setTimeout(r, 100))
    }
    ac.abort()
    await streamDone

    const mine = events.find((e) => e.yours && e.table === 'orders')
    expect(mine, `no yours event among ${events.length} events`).toBeTruthy()
    expect(mine!.hops.some((hop) => hop.node === 'table:orders')).toBe(true)
    expect(mine!.delta.some((d) => (d.row as { workspace_id?: string }).workspace_id === ws.workspace.id)).toBe(true)
  }, 30_000)
})
