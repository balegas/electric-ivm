// Integration: the playground server against a real Postgres + engine (via the conformance
// harness — ephemeral DB per file, engine subprocess in Postgres mode). Covers provisioning
// idempotency, the grid-edit verbs, workspace scoping, scene idempotency + self-heal, graph/rows
// proxying, subsets, reset recovery, rate limiting, and a live end-to-end trace event.

import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { bootHarness, type Harness } from '../../../../packages/conformance/src/harness.ts'
import type { Issue, TraceEvent, WorkspaceState } from '../../shared/types.ts'
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
  it('provisions 2 projects + 4 seed issues and is idempotent by id', async () => {
    const ws = await newWorkspace()
    expect(ws.projects).toHaveLength(2)
    expect(ws.issues).toHaveLength(4)
    expect(ws.workspace.epoch).toBe(7)
    const again = await api<WorkspaceState>('/api/workspace', {
      method: 'POST',
      body: JSON.stringify({ existingId: ws.workspace.id }),
    })
    expect(again.body.workspace.id).toBe(ws.workspace.id)
    expect(again.body.projects.map((r) => r.id)).toEqual(ws.projects.map((r) => r.id))
  })

  it('an unknown id mints a fresh workspace (reset recovery)', async () => {
    const { body } = await api<WorkspaceState>('/api/workspace', {
      method: 'POST',
      body: JSON.stringify({ existingId: 'w_gone' }),
    })
    expect(body.workspace.id).not.toBe('w_gone')
    expect(body.projects).toHaveLength(2)
  })

  it('GET of an unknown workspace is a 404 carrying the epoch', async () => {
    const r = await api<{ error: string; epoch: number }>('/api/workspace/w_nope')
    expect(r.status).toBe(404)
    expect(r.body.epoch).toBe(7)
  })
})

describe('actions', () => {
  it('adds, edits, and deletes issues; validates inputs', async () => {
    const ws = await newWorkspace()
    const pid = ws.projects[0]!.id
    const act = (body: Record<string, unknown>) =>
      api<{ ok: true; issue?: Issue }>('/api/action', {
        method: 'POST',
        body: JSON.stringify({ workspace: ws.workspace.id, ...body }),
      })

    const added = await act({ verb: 'add_issue', projectId: pid })
    expect(added.status).toBe(200)
    expect(added.body.issue!.status).toBe('todo')

    const iid = added.body.issue!.id
    expect((await act({ verb: 'set_status', issueId: iid, status: 'in_progress' })).body.issue!.status).toBe('in_progress')
    expect((await act({ verb: 'set_priority', issueId: iid, priority: 4 })).body.issue!.priority).toBe(4)
    expect((await act({ verb: 'set_priority', issueId: iid, priority: 9 })).status).toBe(400)
    expect((await act({ verb: 'set_status', issueId: iid, status: 'bogus' })).status).toBe(400)
    expect((await act({ verb: 'delete_issue', issueId: iid })).status).toBe(200)
    expect((await act({ verb: 'delete_issue', issueId: iid })).status).toBe(404)
  })

  it("cannot touch another workspace's issues (404)", async () => {
    const a = await newWorkspace()
    const b = await newWorkspace()
    const r = await api('/api/action', {
      method: 'POST',
      body: JSON.stringify({ workspace: b.workspace.id, verb: 'set_status', issueId: a.issues[0]!.id, status: 'done' }),
    })
    expect(r.status).toBe(404)
  })

  it('add_project grows the world; set_team reassigns', async () => {
    const ws = await newWorkspace()
    const r = await api<{ ok: true; project: { id: number; team: string } }>('/api/action', {
      method: 'POST',
      body: JSON.stringify({ workspace: ws.workspace.id, verb: 'add_project' }),
    })
    expect(r.status).toBe(200)
    const moved = await api<{ ok: true; project: { team: string } }>('/api/action', {
      method: 'POST',
      body: JSON.stringify({ workspace: ws.workspace.id, verb: 'set_team', projectId: r.body.project.id, team: 'infra' }),
    })
    expect(moved.body.project.team).toBe('infra')
  })

  it('rate limits rapid writes with 429', async () => {
    const ws = await newWorkspace()
    const pid = ws.projects[0]!.id
    const results = await Promise.all(
      Array.from({ length: 30 }, () =>
        api('/api/action', {
          method: 'POST',
          body: JSON.stringify({ workspace: ws.workspace.id, verb: 'add_issue', projectId: pid }),
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
      body: JSON.stringify({ workspace: ws.workspace.id, scene: 1 }),
    })
    expect(first.status).toBe(200)
    expect(first.body.shapes).toHaveLength(1)
    const sid = first.body.shapes[0]!.id

    const second = await api<{ shapes: { id: string }[] }>('/api/scene', {
      method: 'POST',
      body: JSON.stringify({ workspace: ws.workspace.id, scene: 1 }),
    })
    expect(second.body.shapes.map((x) => x.id)).toEqual([sid])

    await fetch(`${h.engineUrl}/shapes/${sid}`, { method: 'DELETE' })
    const healed = await api<{ shapes: { id: string }[] }>('/api/scene', {
      method: 'POST',
      body: JSON.stringify({ workspace: ws.workspace.id, scene: 1 }),
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
      body: JSON.stringify({ workspace: a.workspace.id, scene: 1 }),
    })
    const shapeA = sceneA.body.shapes[0]!.id

    const graphB = await api<{ graph: { shapes: { id: string }[] }; mine: string[] }>(
      `/api/graph?workspace=${b.workspace.id}`,
    )
    expect(graphB.status).toBe(200)
    expect(graphB.body.mine).not.toContain(shapeA)

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
        label: 'urgent',
        spec: { table: 'issues', where: [{ col: 'priority', op: 'gte', value: 4 }] },
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
    const r = await api<{ rows: { priority: number }[]; lsn: string }>('/api/subset', {
      method: 'POST',
      body: JSON.stringify({ workspace: ws.workspace.id, orderBy: { col: 'priority', desc: true }, limit: 3 }),
    })
    expect(r.status).toBe(200)
    expect(r.body.rows.length).toBeGreaterThan(0)
    expect(r.body.rows.length).toBeLessThanOrEqual(3)
    const prios = r.body.rows.map((x) => x.priority)
    expect([...prios].sort((x, y) => y - x)).toEqual(prios)
  })
})

describe('trace', () => {
  it('a write produces a yours-tagged trace event that reaches the SSE subscriber', async () => {
    const ws = await newWorkspace()
    await api('/api/scene', { method: 'POST', body: JSON.stringify({ workspace: ws.workspace.id, scene: 1 }) })

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

    await new Promise((r) => setTimeout(r, 300))
    const added = await api<{ issue: Issue }>('/api/action', {
      method: 'POST',
      body: JSON.stringify({ workspace: ws.workspace.id, verb: 'add_issue', projectId: ws.projects[0]!.id }),
    })
    expect(added.status).toBe(200)

    const deadline = Date.now() + 15_000
    while (Date.now() < deadline && !events.some((e) => e.yours && e.table === 'issues')) {
      await new Promise((r) => setTimeout(r, 100))
    }
    ac.abort()
    await streamDone

    const mine = events.find((e) => e.yours && e.table === 'issues')
    expect(mine, `no yours event among ${events.length} events`).toBeTruthy()
    expect(mine!.hops.some((hop) => hop.node === 'table:issues')).toBe(true)
  }, 30_000)
})
