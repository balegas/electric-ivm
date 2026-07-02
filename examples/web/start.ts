// Dev entrypoint for the web demo, wired to the Postgres backend. Boots an ephemeral Postgres with
// logical replication, the engine in Postgres mode (it ingests changes via the replication slot and
// reads rows back for backfill), durable-streams + the API for the read/shape path, and Vite. Browser
// writes go to Postgres through a tiny /pg/write middleware — Postgres is the system of record.
// durable-streams + API use ephemeral ports; Vite proxies to them dynamically (no fixed-port clashes).
//   pnpm demo:web
import { type ChildProcess, execFileSync, spawn } from 'node:child_process'
import { appendFileSync, existsSync, mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { DurableStreamTestServer } from '@durable-streams/server'
import { type ApiServer, createApiServer } from '@electric-ivm/api'
import { changeEventToDML, tableDDL } from '@electric-ivm/protocol'
import pgpkg from 'pg'
import { createServer as createViteServer, type Plugin, type ViteDevServer } from 'vite'

import { schema } from './src/schema.js'

const here = dirname(fileURLToPath(import.meta.url))
function repoRoot(): string {
  let d = here
  for (let i = 0; i < 8; i++) {
    if (existsSync(join(d, 'Cargo.toml'))) return d
    d = dirname(d)
  }
  throw new Error('repo root not found')
}

const SLOT = 'electric_ivm_web'

// Resources to tear down (in reverse order) on shutdown or partial-boot failure.
let pgDir: string | undefined
let pgData: string | undefined
let pg: pgpkg.Client | undefined
let ds: DurableStreamTestServer | undefined
let engineProc: ChildProcess | undefined
let api: ApiServer | undefined
let vite: ViteDevServer | undefined
let churnTimer: NodeJS.Timeout | undefined
let shuttingDown = false

async function shutdown(code = 0): Promise<void> {
  if (shuttingDown) return
  shuttingDown = true
  if (churnTimer) clearInterval(churnTimer)
  // Stop the engine + Postgres FIRST (so a slow vite/api close can't leave an orphan cluster).
  engineProc?.kill('SIGKILL')
  if (pg) {
    await pg.query('SELECT pg_drop_replication_slot($1)', [SLOT]).catch(() => {})
    await pg.end().catch(() => {})
  }
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
  await vite?.close().catch(() => {})
  await api?.close().catch(() => {})
  await ds?.stop().catch(() => {})
  process.exit(code)
}
process.on('SIGINT', () => void shutdown(0))
process.on('SIGTERM', () => void shutdown(0))

const TITLES = [
  'Ship electric-ivm',
  'Write the docs',
  'Buy milk',
  'Refactor the parser',
  'Review the PR',
  'Fix the flaky test',
  'Plan the sprint',
  'Answer support tickets',
  'Update dependencies',
  'Profile the hot path',
]
const makeTodo = (id: number) => ({
  id,
  title: `${TITLES[id % TITLES.length]} #${id}`,
  priority: (id % 5) + 1, // 1..5
  done: id % 3 === 0, // ~1/3 done
})
let maxId = 0

try {
  // --- 1. Ephemeral Postgres with logical replication ----------------------------------------------
  pgDir = mkdtempSync(join(tmpdir(), 'el-web-pg-'))
  pgData = join(pgDir, 'data')
  execFileSync('initdb', ['-D', pgData, '-U', 'postgres', '--auth=trust', '--no-sync'], { stdio: 'ignore' })
  let pgPort = 0
  let pgStarted = false
  for (let attempt = 0; attempt < 8 && !pgStarted; attempt++) {
    pgPort = 54330 + Math.floor(Math.random() * 4000)
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
  console.log('postgres        →', pgUrl)

  // Create the tables (REPLICA IDENTITY FULL so replication carries old+new) and seed a few rows.
  pg = new pgpkg.Client({ connectionString: pgUrl })
  await pg.connect()
  for (const [name, def] of Object.entries(schema.tables)) {
    await pg.query(`${tableDDL(name, def)};`)
    await pg.query(`ALTER TABLE "${name}" REPLICA IDENTITY FULL;`)
  }
  // --- Priming: parametrized initial workload ----------------------------------------------------
  // DEMO_SEED_COUNT  initial rows to load into Postgres (default 200).
  // DEMO_CHURN_MS    if set (>0), drive a continuous write workload: one random op every N ms.
  const SEED_COUNT = Math.max(0, Number(process.env.DEMO_SEED_COUNT ?? 200))
  const CHURN_MS = Math.max(0, Number(process.env.DEMO_CHURN_MS ?? 0))

  // Bulk-insert in chunked multi-row statements inside one transaction (fast even for large counts).
  if (SEED_COUNT > 0) {
    const t0 = Date.now()
    const CHUNK = 1000
    await pg.query('BEGIN')
    for (let start = 1; start <= SEED_COUNT; start += CHUNK) {
      const tuples: string[] = []
      const params: unknown[] = []
      let p = 1
      for (let id = start; id < start + CHUNK && id <= SEED_COUNT; id++) {
        const row = makeTodo(id)
        tuples.push(`($${p++},$${p++},$${p++},$${p++})`)
        params.push(row.id, row.title, row.priority, row.done)
        maxId = id
      }
      await pg.query(`INSERT INTO "todos" (id, title, priority, done) VALUES ${tuples.join(', ')}`, params)
    }
    await pg.query('COMMIT')
    console.log(`primed          → ${SEED_COUNT} todos into Postgres (${Date.now() - t0}ms)`)
  }

  // --- 2. durable-streams + engine (Postgres mode) + API (ephemeral ports) -----------------------
  ds = new DurableStreamTestServer({ port: 0 })
  const dsUrl = await ds.start()
  console.log('durable-streams →', dsUrl)

  execFileSync('cargo', ['build', '-p', 'electric-ivm-engine'], { cwd: repoRoot(), stdio: 'inherit' })
  engineProc = spawn(join(repoRoot(), 'target', 'debug', 'electric-ivm-engine'), [], {
    env: {
      ...process.env,
      ELECTRIC_IVM_DS_URL: dsUrl,
      ELECTRIC_IVM_BIND: '127.0.0.1:0',
      ELECTRIC_IVM_LOG: 'warn',
      ELECTRIC_IVM_PG_URL: pgUrl,
      ELECTRIC_IVM_PG_TABLES: Object.keys(schema.tables).join(','),
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
  console.log('engine          →', engineUrl, '(postgres mode)')

  // No defineSchema: in Postgres mode the engine self-configures from introspection.
  api = await createApiServer({ dsUrl, engineUrl })
  console.log('api             →', api.url)

  // --- 3. Vite + the /pg/write middleware (writes go to Postgres) --------------------------------
  // Postgres is the system of record: browser writes become real DML; the engine sees them via the
  // replication slot and updates the live shapes. No tRPC write path.
  const pgWritePlugin: Plugin = {
    name: 'electric-ivm-pg-write',
    configureServer(server) {
      server.middlewares.use('/pg/write', (req, res) => {
        if (req.method !== 'POST') {
          res.statusCode = 405
          return res.end()
        }
        let body = ''
        req.on('data', (c) => {
          body += c
        })
        req.on('end', async () => {
          try {
            const { table, op, pk, row } = JSON.parse(body)
            const def = schema.tables[table as keyof typeof schema.tables]
            if (!def) throw new Error(`unknown table ${table}`)
            const { text, params } = changeEventToDML(table, def, { op, pk, row })
            await pg!.query(text, params)
            res.statusCode = 200
            res.setHeader('content-type', 'application/json')
            res.end('{"ok":true}')
          } catch (e) {
            res.statusCode = 400
            res.setHeader('content-type', 'application/json')
            res.end(JSON.stringify({ error: String(e) }))
          }
        })
      })
    },
  }

  vite = await createViteServer({
    root: here,
    configFile: join(here, 'vite.config.ts'),
    plugins: [pgWritePlugin],
    // Proxy to the ephemeral durable-streams + API ports resolved above (no fixed-port collisions).
    server: {
      proxy: {
        '/api': { target: api.url, changeOrigin: true, rewrite: (p) => p.replace(/^\/api/, '') },
        '/ds': { target: dsUrl, changeOrigin: true, rewrite: (p) => p.replace(/^\/ds/, '') },
      },
    },
  })
  await vite.listen()
  console.log('')
  vite.printUrls()
  console.log('\n👉 Open the Local URL above. Edit todos on the left; they are written to Postgres,')
  console.log('   ingested via logical replication, and the live shape on the right updates.\n')

  // --- Optional continuous write workload --------------------------------------------------------
  // One random op per CHURN_MS: ~30% insert, ~50% update (toggle done / re-roll priority), ~20% delete.
  // All writes go to Postgres, so the live shape moves on its own — useful for load/visual demos.
  if (CHURN_MS > 0) {
    console.log(`churn           → 1 op / ${CHURN_MS}ms (writing to Postgres)\n`)
    churnTimer = setInterval(async () => {
      try {
        const r = Math.random()
        if (r < 0.3) {
          const row = makeTodo(++maxId)
          const { text, params } = changeEventToDML('todos', schema.tables.todos!, { op: 'insert', pk: row.id, row })
          await pg!.query(text, params)
        } else if (r < 0.8) {
          const id = 1 + Math.floor(Math.random() * maxId)
          await pg!.query('UPDATE "todos" SET done = NOT done, priority = 1 + floor(random() * 5)::int WHERE id = $1', [id])
        } else {
          const id = 1 + Math.floor(Math.random() * maxId)
          await pg!.query('DELETE FROM "todos" WHERE id = $1', [id])
        }
      } catch (e) {
        console.error('churn op failed:', e)
      }
    }, CHURN_MS)
  }
} catch (e) {
  console.error('web demo: startup failed:', e)
  await shutdown(1)
}
