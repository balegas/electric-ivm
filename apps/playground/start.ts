// One-command dev boot for the playground (pattern-copy of examples/linearlite/start.ts):
// ephemeral Postgres (logical replication) → tables → durable-streams → engine (Postgres mode) →
// playground server → Vite. Same workspace model as the hosted deployment — localhost is just a
// one-visitor instance.
//   pnpm demo:playground     (or: pnpm --filter @electric-ivm/playground demo)
import { type ChildProcess, execFileSync, spawn } from 'node:child_process'
import { appendFileSync, existsSync, mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { DurableStreamTestServer } from '@durable-streams/server'
import { createServer as createViteServer, type ViteDevServer } from 'vite'

import { createDb, ensureTables } from './server/db.ts'
import { createPlaygroundServer, type PlaygroundServer } from './server/main.ts'
import { PLAYGROUND_SCHEMA } from './server/schema.ts'

const here = dirname(fileURLToPath(import.meta.url))
function repoRoot(): string {
  let d = here
  for (let i = 0; i < 8; i++) {
    if (existsSync(join(d, 'Cargo.toml'))) return d
    d = dirname(d)
  }
  throw new Error('repo root not found')
}

const SLOT = 'electric_ivm_playground'

let pgDir: string | undefined
let pgData: string | undefined
let ds: DurableStreamTestServer | undefined
let engineProc: ChildProcess | undefined
let server: PlaygroundServer | undefined
let vite: ViteDevServer | undefined
let caddyProc: ChildProcess | undefined
let shuttingDown = false

async function shutdown(code = 0): Promise<void> {
  if (shuttingDown) return
  shuttingDown = true
  caddyProc?.kill('SIGKILL')
  engineProc?.kill('SIGKILL')
  await vite?.close().catch(() => {})
  await server?.close().catch(() => {})
  await ds?.stop().catch(() => {})
  if (pgData) {
    try {
      execFileSync('pg_ctl', ['-D', pgData, '-m', 'immediate', '-w', 'stop'], { stdio: 'ignore' })
    } catch {
      /* already down */
    }
  }
  if (pgDir) {
    try {
      rmSync(pgDir, { recursive: true, force: true })
    } catch {
      /* ignore */
    }
  }
  process.exit(code)
}
process.on('SIGINT', () => void shutdown(0))
process.on('SIGTERM', () => void shutdown(0))

try {
  // --- 1. Ephemeral Postgres with logical replication ------------------------------------------
  pgDir = mkdtempSync(join(tmpdir(), 'el-playground-pg-'))
  pgData = join(pgDir, 'data')
  execFileSync('initdb', ['-D', pgData, '-U', 'postgres', '--auth=trust', '--no-sync'], { stdio: 'ignore' })
  let pgPort = 0
  let pgStarted = false
  for (let attempt = 0; attempt < 8 && !pgStarted; attempt++) {
    pgPort = 54800 + Math.floor(Math.random() * 4000)
    appendFileSync(
      join(pgData, 'postgresql.conf'),
      `\nwal_level = logical\nmax_replication_slots = 10\nmax_wal_senders = 10\n` +
        `listen_addresses = '127.0.0.1'\nunix_socket_directories = '/tmp'\nport = ${pgPort}\nfsync = off\n`,
    )
    try {
      execFileSync('pg_ctl', ['-D', pgData, '-l', join(pgDir, 'log'), '-w', 'start'], { stdio: 'ignore' })
      pgStarted = true
    } catch {
      /* port in use; retry */
    }
  }
  if (!pgStarted) throw new Error('failed to start ephemeral postgres')
  const pgUrl = `postgres://postgres@127.0.0.1:${pgPort}/postgres`
  console.log('postgres          →', pgUrl)

  // Tables must exist (REPLICA IDENTITY FULL) before the engine introspects.
  const bootstrapDb = createDb(pgUrl)
  await ensureTables(bootstrapDb)
  await bootstrapDb.end()

  // --- 2. durable-streams + engine (Postgres mode) ---------------------------------------------
  ds = new DurableStreamTestServer({ port: 0 })
  const dsUrl = await ds.start()
  console.log('durable-streams   →', dsUrl)

  execFileSync('cargo', ['build', '-p', 'electric-ivm-engine'], { cwd: repoRoot(), stdio: 'inherit' })
  engineProc = spawn(join(repoRoot(), 'target', 'debug', 'electric-ivm-engine'), [], {
    env: {
      ...process.env,
      ELECTRIC_IVM_DS_URL: dsUrl,
      ELECTRIC_IVM_BIND: '127.0.0.1:0',
      ELECTRIC_IVM_LOG: 'warn',
      ELECTRIC_IVM_PG_URL: pgUrl,
      ELECTRIC_IVM_PG_TABLES: Object.keys(PLAYGROUND_SCHEMA.tables).join(','),
      ELECTRIC_IVM_PG_SLOT: SLOT,
      ELECTRIC_IVM_PG_POLL_MS: '25',
    },
    stdio: ['ignore', 'pipe', 'inherit'],
  })
  const engineUrl = await new Promise<string>((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('engine did not start')), 20000)
    let buf = ''
    engineProc!.stdout!.on('data', (d: Buffer) => {
      buf += d.toString()
      const m = buf.match(/ENGINE_LISTENING (\S+)/)
      if (m) {
        clearTimeout(t)
        resolve(m[1]!)
      }
    })
    engineProc!.on('exit', (c) => reject(new Error(`engine exited ${c}`)))
  })
  console.log('engine            →', engineUrl, '(postgres mode)')

  // --- 3. playground server + Vite ---------------------------------------------------------------
  server = await createPlaygroundServer({ pgUrl, engineUrl, ttlHours: 0 })
  console.log('playground server →', server.url)

  vite = await createViteServer({
    root: here,
    configFile: join(here, 'vite.config.ts'),
    server: {
      proxy: { '/api': { target: server.url, changeOrigin: true } },
    },
  })
  await vite.listen()
  console.log('')
  vite.printUrls()

  // HTTPS / HTTP-2 front (Caddy), same as the linearlite demo: browsers cap HTTP/1.1 at ~6
  // connections per origin, and the playground holds several at once (the /trace SSE stream + a
  // rows poll per device card + graph/workspace polling + HMR). Fronting Vite with an HTTP/2 TLS
  // proxy multiplexes everything over one connection. Auto-enabled when `caddy` is on PATH; set
  // DEMO_HTTPS=0 to skip, DEMO_HTTPS_PORT to change the port (default 8444).
  if (process.env.DEMO_HTTPS !== '0') {
    let hasCaddy = true
    try {
      execFileSync('caddy', ['version'], { stdio: 'ignore' })
    } catch {
      hasCaddy = false
    }
    if (hasCaddy) {
      // Dial the exact address Vite bound (it listens on IPv6 ::1 by default; a 127.0.0.1
      // upstream would 502). Bracket IPv6 hosts for the host:port form.
      const a = vite.httpServer?.address()
      const vitePort = a && typeof a === 'object' ? a.port : 5190
      const host = a && typeof a === 'object' && a.family === 'IPv6' && a.address !== '::' ? `[${a.address}]` : '127.0.0.1'
      const httpsPort = process.env.DEMO_HTTPS_PORT ?? '8444'
      caddyProc = spawn(
        'caddy',
        ['reverse-proxy', '--from', `https://localhost:${httpsPort}`, '--to', `${host}:${vitePort}`],
        { stdio: 'ignore' },
      )
      caddyProc.on('exit', (c) => {
        if (!shuttingDown) console.warn(`caddy proxy exited (${c}); continuing over HTTP only`)
      })
      console.log(`\n🔒 HTTPS (HTTP/2) →  https://localhost:${httpsPort}/   ← use this one`)
      console.log('   Multiplexes the trace stream + device-card polls over one connection (past the ~6-per-origin HTTP/1.1 cap).')
      console.log("   The cert is from Caddy's local CA: run `caddy trust` once to remove the browser warning, or click through it.")
    } else {
      console.log('\n(Install `caddy` to serve over HTTPS/HTTP-2 — the many live streams can exhaust the browser’s ~6-connection HTTP/1.1 cap.)')
    }
  }

  console.log('\n🍕 dbsp playground — every write is a delta; watch it travel.\n')
} catch (e) {
  console.error('playground: startup failed:', e)
  await shutdown(1)
}
