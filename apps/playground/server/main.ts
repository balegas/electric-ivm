// The playground server: the only surface browsers talk to. Owns workspaces/actions/shapes/scenes
// (Postgres + engine registration), proxies engine introspection (graph, rows, subset), and fans
// out the engine's /trace with per-workspace tagging. Defenses: per-workspace token-bucket rate
// limit on writes, shape/order caps, idle-workspace TTL sweep, epoch-based reset recovery.

import { createServer, type IncomingMessage, type ServerResponse } from 'node:http'
import { readFile, stat } from 'node:fs/promises'
import { extname, join, normalize } from 'node:path'

import type { Verb } from '../shared/types.ts'
import { applyAction, ActionError } from './actions.ts'
import { createDb, type Db, ensureTables } from './db.ts'
import { EngineClient } from './engine-client.ts'
import { provisionScene } from './scenes.ts'
import {
  allShapeOwners,
  createShape,
  deleteShape,
  deleteWorkspaceShapes,
  listShapes,
  ShapeError,
  shapeOwned,
} from './shapes.ts'
import { TraceFanout } from './trace.ts'
import {
  deleteWorkspaceRows,
  getWorkspaceState,
  idleWorkspaces,
  provisionWorkspace,
  workspaceExists,
  type WorkspaceDeps,
} from './workspace.ts'

export interface PlaygroundServerOptions {
  pgUrl: string
  engineUrl: string
  port?: number
  epoch?: number
  /** Serve a built SPA from this directory (production); dev uses the Vite proxy instead. */
  staticDir?: string
  /** Idle-workspace TTL in hours (default 24; 0 disables the sweep). */
  ttlHours?: number
  /** Rate limit: sustained requests/second per workspace on write endpoints (default 5, burst 15). */
  rps?: number
}

export interface PlaygroundServer {
  url: string
  port: number
  db: Db
  close(): Promise<void>
}

// ── tiny http helpers ─────────────────────────────────────────────────────────────────────────

function json(res: ServerResponse, status: number, body: unknown): void {
  const s = JSON.stringify(body)
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(s) })
  res.end(s)
}

async function readJson(req: IncomingMessage): Promise<Record<string, unknown>> {
  let body = ''
  for await (const chunk of req) {
    body += chunk
    if (body.length > 64 * 1024) throw new ActionError(413, 'body too large')
  }
  if (!body) return {}
  try {
    return JSON.parse(body) as Record<string, unknown>
  } catch {
    throw new ActionError(400, 'invalid JSON body')
  }
}

// ── rate limiting (token bucket per workspace) ────────────────────────────────────────────────

class Buckets {
  private buckets = new Map<string, { tokens: number; at: number }>()
  constructor(
    private rps: number,
    private burst: number,
  ) {}
  take(ws: string): boolean {
    const now = Date.now()
    const b = this.buckets.get(ws) ?? { tokens: this.burst, at: now }
    b.tokens = Math.min(this.burst, b.tokens + ((now - b.at) / 1000) * this.rps)
    b.at = now
    if (b.tokens < 1) {
      this.buckets.set(ws, b)
      return false
    }
    b.tokens -= 1
    this.buckets.set(ws, b)
    return true
  }
}

// ── static file serving (production SPA) ─────────────────────────────────────────────────────

const MIME: Record<string, string> = {
  '.html': 'text/html',
  '.js': 'text/javascript',
  '.css': 'text/css',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
  '.json': 'application/json',
  '.woff2': 'font/woff2',
}

async function serveStatic(dir: string, url: string, res: ServerResponse): Promise<void> {
  const path = normalize(url.split('?')[0]!).replace(/^(\.\.[/\\])+/, '')
  let file = join(dir, path === '/' ? 'index.html' : path)
  try {
    const st = await stat(file)
    if (st.isDirectory()) file = join(file, 'index.html')
  } catch {
    file = join(dir, 'index.html') // SPA fallback
  }
  try {
    const data = await readFile(file)
    res.writeHead(200, { 'content-type': MIME[extname(file)] ?? 'application/octet-stream' })
    res.end(data)
  } catch {
    res.writeHead(404)
    res.end('not found')
  }
}

// ── server ────────────────────────────────────────────────────────────────────────────────────

export async function createPlaygroundServer(opts: PlaygroundServerOptions): Promise<PlaygroundServer> {
  const db = createDb(opts.pgUrl)
  await ensureTables(db)
  const engine = new EngineClient(opts.engineUrl)
  const epoch = opts.epoch ?? Number(process.env.PLAYGROUND_EPOCH ?? 1)
  const shapeDeps = { db, engine }
  const wsDeps: WorkspaceDeps = { db, epoch, listShapes: (ws) => listShapes(db, ws) }
  const buckets = new Buckets(opts.rps ?? 5, (opts.rps ?? 5) * 3)
  const fanout = new TraceFanout(engine.traceUrl(), () => allShapeOwners(db))

  // Idle-workspace sweep: engine shapes torn down first, then rows/meta.
  const ttlMs = (opts.ttlHours ?? 24) * 3600_000
  const sweep = ttlMs
    ? setInterval(async () => {
        try {
          for (const ws of await idleWorkspaces(db, ttlMs)) {
            await deleteWorkspaceShapes(shapeDeps, ws)
            await deleteWorkspaceRows(db, ws)
          }
        } catch (e) {
          console.warn('playground sweep failed:', e)
        }
      }, 600_000)
    : null
  sweep?.unref()

  /** Extract + validate the workspace, or respond 404/429 and return null. `write` costs a token. */
  async function guard(res: ServerResponse, ws: unknown, write: boolean): Promise<string | null> {
    if (typeof ws !== 'string' || !ws) {
      json(res, 400, { error: 'workspace required' })
      return null
    }
    if (write && !buckets.take(ws)) {
      json(res, 429, { error: 'rate limited — slow down' })
      return null
    }
    if (!(await workspaceExists(wsDeps, ws))) {
      json(res, 404, { error: 'unknown or reset workspace', epoch })
      return null
    }
    return ws
  }

  const server = createServer((req, res) => {
    void route(req, res).catch((e) => {
      const status = e instanceof ActionError || e instanceof ShapeError ? e.status : ((e as { status?: number }).status ?? 500)
      if (status >= 500) console.error('playground:', e)
      if (!res.headersSent) json(res, status, { error: String((e as Error).message ?? e) })
    })
  })

  async function route(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const url = new URL(req.url ?? '/', 'http://x')
    const p = url.pathname
    const m = req.method ?? 'GET'

    if (p === '/api/health') return json(res, 200, { ok: true, epoch })

    if (p === '/api/workspace' && m === 'POST') {
      const body = await readJson(req)
      const state = await provisionWorkspace(wsDeps, typeof body.existingId === 'string' ? body.existingId : undefined)
      return json(res, 200, state)
    }
    let match = p.match(/^\/api\/workspace\/([^/]+)$/)
    if (match && m === 'GET') {
      const state = await getWorkspaceState(wsDeps, match[1]!)
      return state ? json(res, 200, state) : json(res, 404, { error: 'unknown or reset workspace', epoch })
    }

    if (p === '/api/action' && m === 'POST') {
      const body = await readJson(req)
      const ws = await guard(res, body.workspace, true)
      if (!ws) return
      return json(res, 200, await applyAction(db, ws, body as unknown as Verb))
    }

    if (p === '/api/scene' && m === 'POST') {
      const body = await readJson(req)
      const ws = await guard(res, body.workspace, true)
      if (!ws) return
      const result = await provisionScene(shapeDeps, ws, Number(body.scene))
      fanout.invalidateOwners()
      return json(res, 200, result)
    }

    if (p === '/api/shape' && m === 'POST') {
      const body = await readJson(req)
      const ws = await guard(res, body.workspace, true)
      if (!ws) return
      const spec = body.spec as Parameters<typeof createShape>[2]
      if (!spec || (spec.table !== 'orders' && spec.table !== 'restaurants')) {
        return json(res, 400, { error: 'invalid shape spec' })
      }
      const shape = await createShape(
        shapeDeps,
        ws,
        spec,
        typeof body.label === 'string' ? body.label : 'Custom shape',
        (body.role as Parameters<typeof createShape>[4]) ?? 'custom',
      )
      fanout.invalidateOwners()
      return json(res, 200, shape)
    }
    match = p.match(/^\/api\/shape\/([^/]+)$/)
    if (match && m === 'DELETE') {
      const ws = await guard(res, url.searchParams.get('workspace'), true)
      if (!ws) return
      await deleteShape(shapeDeps, ws, match[1]!)
      fanout.invalidateOwners()
      return json(res, 200, { ok: true })
    }

    if (p === '/api/graph' && m === 'GET') {
      const ws = await guard(res, url.searchParams.get('workspace'), false)
      if (!ws) return
      const [graph, mine] = await Promise.all([engine.graph(), listShapes(db, ws)])
      return json(res, 200, { graph, mine: mine.map((s) => s.id) })
    }

    match = p.match(/^\/api\/shapes\/([^/]+)\/rows$/)
    if (match && m === 'GET') {
      const ws = await guard(res, url.searchParams.get('workspace'), false)
      if (!ws) return
      if (!(await shapeOwned(db, ws, match[1]!))) return json(res, 404, { error: 'unknown shape' })
      const limit = Math.min(500, Number(url.searchParams.get('limit') ?? 200))
      return json(res, 200, await engine.shapeRows(match[1]!, limit))
    }

    if (p === '/api/subset' && m === 'POST') {
      const body = await readJson(req)
      const ws = await guard(res, body.workspace, false)
      if (!ws) return
      const orderBy = body.orderBy as { col: string; desc?: boolean } | undefined
      const limit = Math.min(50, Number(body.limit ?? 5))
      const result = await engine.query({
        table: 'orders',
        where: { col: 'workspace_id', op: 'eq', value: ws },
        orderBy,
        limit,
      })
      return json(res, 200, result)
    }

    if (p === '/api/trace' && m === 'GET') {
      const ws = await guard(res, url.searchParams.get('workspace'), false)
      if (!ws) return
      return fanout.attach(ws, res)
    }

    if (p.startsWith('/api/')) return json(res, 404, { error: 'not found' })
    if (opts.staticDir) return serveStatic(opts.staticDir, p, res)
    return json(res, 404, { error: 'not found (no static dir configured)' })
  }

  const port = await new Promise<number>((resolve, reject) => {
    server.on('error', reject)
    server.listen(opts.port ?? 0, '0.0.0.0', () => {
      const a = server.address()
      resolve(typeof a === 'object' && a ? a.port : 0)
    })
  })

  return {
    url: `http://127.0.0.1:${port}`,
    port,
    db,
    close: async () => {
      if (sweep) clearInterval(sweep)
      await new Promise<void>((r) => server.close(() => r()))
      await db.end()
    },
  }
}

// Run directly (docker / production): env-configured.
const isMain = process.argv[1] && import.meta.url.endsWith(process.argv[1].split('/').pop()!)
if (isMain) {
  const pgUrl = process.env.PLAYGROUND_PG_URL
  const engineUrl = process.env.PLAYGROUND_ENGINE_URL
  if (!pgUrl || !engineUrl) {
    console.error('PLAYGROUND_PG_URL and PLAYGROUND_ENGINE_URL must be set')
    process.exit(1)
  }
  const s = await createPlaygroundServer({
    pgUrl,
    engineUrl,
    port: Number(process.env.PLAYGROUND_PORT ?? 5199),
    staticDir: process.env.PLAYGROUND_STATIC,
    ttlHours: Number(process.env.PLAYGROUND_WS_TTL_H ?? 24),
  })
  console.log(`playground server → ${s.url}`)
}
