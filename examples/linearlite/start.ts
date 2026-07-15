// Dev entrypoint for LinearLite on electric-ivm, wired to the Postgres backend. Boots an ephemeral
// Postgres with logical replication, the engine in Postgres mode (it ingests changes via the
// replication slot and reads rows back for backfill), durable-streams + the API for the read/shape
// path, and Vite. Browser writes go to Postgres through a /pg/write middleware — Postgres is the
// system of record. durable-streams + API use ephemeral ports; Vite proxies to them dynamically.
//   pnpm --filter @electric-ivm/linearlite start     (or: pnpm demo:linearlite)
import { type ChildProcess, execFileSync, spawn } from 'node:child_process'
import { appendFileSync, existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { createServer as createNetServer } from 'node:net'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { type ApiServer, createApiServer } from '@electric-ivm/api'
import { DurableStreamTestServer } from '@electric-ivm/ds-rust'
import { changeEventToDML } from '@electric-ivm/protocol'
import { faker } from '@faker-js/faker'
import pgpkg from 'pg'
import { createServer as createViteServer, type Plugin, type ViteDevServer } from 'vite'

import { PRIORITIES, schema, STATUSES } from './src/schema.js'

const here = dirname(fileURLToPath(import.meta.url))
function repoRoot(): string {
  let d = here
  for (let i = 0; i < 8; i++) {
    if (existsSync(join(d, 'Cargo.toml'))) return d
    d = dirname(d)
  }
  throw new Error('repo root not found')
}

// The next free TCP port at or after `start` (127.0.0.1 only, matching every bind in this file).
// Used so a printed banner is never a lie: we resolve the real port BEFORE spawning anything that
// might otherwise silently fall back to a different one (e.g. vite, with a busy DEMO_VIZ_PORT).
function findFreePort(start: number): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = createNetServer()
    srv.unref()
    srv.on('error', (err: NodeJS.ErrnoException) => {
      if (err.code === 'EADDRINUSE' && start < 65535) {
        resolve(findFreePort(start + 1))
      } else {
        reject(err)
      }
    })
    srv.listen(start, '127.0.0.1', () => {
      const port = start
      srv.close(() => resolve(port))
    })
  })
}

const SLOT = 'electric_ivm_linearlite'
// PIDs of the engine + durable-streams-server children, so `scripts/linearlite.sh stop` can kill
// exactly these two processes by PID as a backstop — never by matching on their generic binary
// name, which would risk hitting an unrelated engine/ds instance running elsewhere on the machine.
const CHILDREN_PIDFILE = process.env.EL_LINEARLITE_CHILDREN_PIDFILE ?? '/tmp/el-linearlite-children.pids'

// Resources to tear down (in reverse order) on shutdown or partial-boot failure.
let pgDir: string | undefined
let pgData: string | undefined
let pg: pgpkg.Client | undefined
let ds: DurableStreamTestServer | undefined
let engineProc: ChildProcess | undefined
let api: ApiServer | undefined
let vite: ViteDevServer | undefined
let caddyProc: ChildProcess | undefined
let vizCaddyProc: ChildProcess | undefined
let vizProc: ChildProcess | undefined
let shuttingDown = false

async function shutdown(code = 0): Promise<void> {
  if (shuttingDown) return
  shuttingDown = true
  vizProc?.kill('SIGKILL')
  vizCaddyProc?.kill('SIGKILL')
  caddyProc?.kill('SIGKILL')
  engineProc?.kill('SIGKILL')
  rmSync(CHILDREN_PIDFILE, { force: true })
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

try {
  // --- 1. Ephemeral Postgres with logical replication --------------------------------------------
  pgDir = mkdtempSync(join(tmpdir(), 'el-linearlite-pg-'))
  pgData = join(pgDir, 'data')
  execFileSync('initdb', ['-D', pgData, '-U', 'postgres', '--auth=trust', '--no-sync'], { stdio: 'ignore' })
  let pgPort = 0
  let pgStarted = false
  for (let attempt = 0; attempt < 8 && !pgStarted; attempt++) {
    pgPort = 54600 + Math.floor(Math.random() * 4000)
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

  pg = new pgpkg.Client({ connectionString: pgUrl })
  await pg.connect()
  // Explicit DDL (not tableDDL) so ids/timestamps are BIGINT — the client mints ids from Date.now(),
  // which overflows int4. id is GENERATED BY DEFAULT AS IDENTITY (not ALWAYS) so the explicit-id seed
  // below still works while an add-row that omits id gets an auto value (sequence bumped past the seed
  // after priming). REPLICA IDENTITY FULL so replication carries old+new; a cascading FK so
  // deleting an issue removes its comments in one shot (the engine sees both deletes via replication).
  await pg.query(`CREATE TABLE projects (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    color TEXT NOT NULL
  )`)
  await pg.query(`CREATE TABLE users (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL
  )`)
  await pg.query(`CREATE TABLE project_members (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    project_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL
  )`)
  await pg.query(`CREATE TABLE issues (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    status TEXT NOT NULL,
    priority TEXT NOT NULL,
    username TEXT NOT NULL,
    project_id BIGINT NOT NULL,
    created BIGINT NOT NULL,
    modified BIGINT NOT NULL,
    kanbanorder DOUBLE PRECISION NOT NULL
  )`)
  await pg.query(`CREATE TABLE comments (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    issue_id BIGINT NOT NULL REFERENCES issues(id) ON DELETE CASCADE,
    body TEXT NOT NULL,
    username TEXT NOT NULL,
    created BIGINT NOT NULL
  )`)
  for (const t of ['projects', 'users', 'project_members', 'issues', 'comments']) {
    await pg.query(`ALTER TABLE ${t} REPLICA IDENTITY FULL`)
  }

  // --- Parametrized seed (faker), mirroring the original LinearLite generator ---------------------
  // DEMO_SEED_COUNT  number of issues to generate (default 512, the upstream default).
  const SEED_COUNT = Math.max(0, Number(process.env.DEMO_SEED_COUNT ?? 512))
  const SEED_PRIORITIES = ['none', 'low', 'medium', 'high'] as const // upstream never seeds 'urgent'

  if (SEED_COUNT > 0) {
    const t0 = Date.now()
    faker.seed(42)
    const now = Date.now()

    // Roster scales with the workload (DEMO_USERS users over DEMO_PROJECTS projects; default 6/5). Each
    // user belongs to an overlapping random subset of projects so switching the current user changes which
    // issues are in scope. The first few keep the classic names/projects; the rest are faker-generated.
    // The app reads the roster back from the users/projects tables, so the "Viewing as" switcher adapts.
    const NUM_USERS = Math.max(1, Number(process.env.DEMO_USERS ?? 6))
    const NUM_PROJECTS = Math.max(1, Number(process.env.DEMO_PROJECTS ?? 5))
    const CLASSIC_USERS = ['alice', 'bob', 'carol', 'dave', 'erin', 'frank']
    const CLASSIC_PROJECTS = [
      { name: 'Web App', color: '#5e6ad2' },
      { name: 'Mobile', color: '#26a269' },
      { name: 'Infra', color: '#c64600' },
      { name: 'Design System', color: '#a347ba' },
      { name: 'Marketing', color: '#0a7ea4' },
    ]
    const PALETTE = ['#5e6ad2', '#26a269', '#c64600', '#a347ba', '#0a7ea4', '#b8860b', '#d1477a', '#2f855a']
    const USERS = Array.from({ length: NUM_USERS }, (_, i) =>
      i < CLASSIC_USERS.length ? CLASSIC_USERS[i]! : `${faker.person.firstName().toLowerCase()}_${i + 1}`,
    ) // user id = index + 1
    const PROJECTS = Array.from({ length: NUM_PROJECTS }, (_, i) =>
      i < CLASSIC_PROJECTS.length
        ? CLASSIC_PROJECTS[i]!
        : { name: `${faker.commerce.department()} ${i + 1}`, color: PALETTE[i % PALETTE.length]! },
    ) // project id = index + 1
    // Membership: the classic overlapping sets at the default size (6 users / ≥5 projects) so the stock
    // demo is unchanged; scaled-up extra users join 2..min(6, NUM_PROJECTS) random distinct projects.
    const CLASSIC_MEMBERSHIP = [[1, 2, 3], [2, 3, 4], [3, 4, 5], [1, 4, 5], [1, 2, 5], [1, 2, 3, 4, 5]]
    const MEMBERSHIP: number[][] = USERS.map((_, ui) => {
      if (ui < CLASSIC_MEMBERSHIP.length && NUM_PROJECTS >= 5) return CLASSIC_MEMBERSHIP[ui]!
      const k = Math.max(1, Math.min(NUM_PROJECTS, 2 + Math.floor(Math.random() * 5)))
      const ids = new Set<number>()
      while (ids.size < k) ids.add(1 + Math.floor(Math.random() * NUM_PROJECTS))
      return [...ids]
    })

    const projectRows: unknown[][] = PROJECTS.map((p, i) => [i + 1, p.name, p.color])
    const userRows: unknown[][] = USERS.map((n, i) => [i + 1, n])
    const memberRows: unknown[][] = []
    let memberId = 1
    MEMBERSHIP.forEach((projectIds, ui) => {
      for (const pid of projectIds) memberRows.push([memberId++, pid, ui + 1])
    })
    const issueRows: unknown[][] = []
    const commentRows: unknown[][] = []
    let commentId = 1
    let order = 1
    for (let id = 1; id <= SEED_COUNT; id++) {
      const created = faker.date.past({ years: 1 }).getTime()
      const modified = faker.date.between({ from: created, to: now }).getTime()
      const projectId = (id % PROJECTS.length) + 1
      issueRows.push([
        id,
        faker.lorem.sentence({ min: 3, max: 8 }).replace(/\.$/, ''),
        faker.lorem.paragraphs({ min: 1, max: 3 }),
        STATUSES[Math.floor(Math.random() * STATUSES.length)],
        SEED_PRIORITIES[Math.floor(Math.random() * SEED_PRIORITIES.length)],
        USERS[Math.floor(Math.random() * USERS.length)], // assignee from the roster (so My Tasks is real)
        projectId,
        created,
        modified,
        order++ + Math.random(),
      ])
      if (Math.random() < 0.5) {
        commentRows.push([
          commentId++,
          id,
          faker.lorem.sentences({ min: 1, max: 3 }),
          USERS[Math.floor(Math.random() * USERS.length)],
          faker.date.between({ from: created, to: now }).getTime(),
        ])
      }
    }

    const bulkInsert = async (table: string, cols: string[], rows: unknown[][]) => {
      const CHUNK = 1000
      for (let s = 0; s < rows.length; s += CHUNK) {
        const slice = rows.slice(s, s + CHUNK)
        const params: unknown[] = []
        let p = 1
        const tuples = slice.map((r) => `(${r.map(() => `$${p++}`).join(',')})`)
        for (const r of slice) params.push(...r)
        await pg!.query(`INSERT INTO "${table}" (${cols.map((c) => `"${c}"`).join(',')}) VALUES ${tuples.join(',')}`, params)
      }
    }
    await pg.query('BEGIN')
    await bulkInsert('projects', ['id', 'name', 'color'], projectRows)
    await bulkInsert('users', ['id', 'name'], userRows)
    await bulkInsert('project_members', ['id', 'project_id', 'user_id'], memberRows)
    await bulkInsert('issues', ['id', 'title', 'description', 'status', 'priority', 'username', 'project_id', 'created', 'modified', 'kanbanorder'], issueRows)
    await bulkInsert('comments', ['id', 'issue_id', 'body', 'username', 'created'], commentRows)
    await pg.query('COMMIT')
    // The seed inserts explicit ids (1..N); advance each table's IDENTITY sequence past max(id) so an
    // add-row that omits id gets an auto-assigned value that can't collide with a seeded row. setval
    // with is_called=false makes the next nextval() return exactly the given value.
    for (const t of ['projects', 'users', 'project_members', 'issues', 'comments']) {
      await pg.query(
        `SELECT setval(pg_get_serial_sequence('${t}', 'id'), (SELECT COALESCE(MAX(id), 0) + 1 FROM "${t}"), false)`,
      )
    }
    console.log(`primed          → ${issueRows.length} issues, ${commentRows.length} comments, ${projectRows.length} projects, ${userRows.length} users, ${memberRows.length} memberships (${Date.now() - t0}ms)`)
  }

  // --- 2. durable-streams + engine (Postgres mode) + API (ephemeral ports) ------------------------
  ds = new DurableStreamTestServer({ port: 0 })
  const dsUrl = await ds.start()
  console.log('durable-streams →', dsUrl)

  execFileSync('cargo', ['build', '-p', 'electric-ivm-engine'], { cwd: repoRoot(), stdio: 'inherit' })
  // dbsp pipeline: the demo runs the engine with the circuit serving the app's query graph —
  // cohort indexes for every lookup/membership column LinearLite uses, and a counts pipeline
  // for the browse header's live COUNT. Counts state is in-memory (reseeded each boot from a
  // group-aggregated Postgres snapshot); row data lives in Postgres — membership shapes are
  // registry-served with pooled query-backs. Pre-set ELECTRIC_IVM_DBSP* env vars win.
  engineProc = spawn(join(repoRoot(), 'target', 'debug', 'electric-ivm-engine'), [], {
    env: {
      ELECTRIC_IVM_DBSP_COUNTS: 'issues:project_id+status+priority+username',
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
  writeFileSync(CHILDREN_PIDFILE, `${engineProc.pid}\n${ds.pid}\n`)

  api = await createApiServer({ dsUrl, engineUrl })
  console.log('api             →', api.url)

  // --- 3. Vite + the /pg/write middleware (writes go to Postgres) --------------------------------
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

  // Optional HTTPS / HTTP-2 front (Caddy). Browsers cap HTTP/1.1 at ~6 connections per origin, which
  // the many concurrent live shape streams (5 board columns + list pages + HMR) can exhaust — the
  // durable-streams client even warns about it. Fronting Vite with an HTTP/2 TLS proxy multiplexes
  // every stream over a single connection, lifting the cap. Auto-enabled when `caddy` is on PATH; set
  // DEMO_HTTPS=0 to skip, DEMO_HTTPS_PORT to change the port (default 8443).
  if (process.env.DEMO_HTTPS !== '0') {
    let hasCaddy = true
    try {
      execFileSync('caddy', ['version'], { stdio: 'ignore' })
    } catch {
      hasCaddy = false
    }
    if (hasCaddy) {
      // Dial the exact address Vite bound (it listens on IPv6 ::1 by default; a 127.0.0.1 upstream
      // would 502). Bracket IPv6 hosts for the host:port form.
      const a = vite.httpServer?.address()
      const vitePort = a && typeof a === 'object' ? a.port : 5174
      const host = a && typeof a === 'object' && a.family === 'IPv6' && a.address !== '::' ? `[${a.address}]` : '127.0.0.1'
      const httpsPort = process.env.DEMO_HTTPS_PORT ?? '8443'
      caddyProc = spawn(
        'caddy',
        ['reverse-proxy', '--from', `https://localhost:${httpsPort}`, '--to', `${host}:${vitePort}`],
        { stdio: 'inherit' },
      )
      caddyProc.on('exit', (c) => {
        if (!shuttingDown) console.warn(`caddy proxy exited (${c}); continuing over HTTP only`)
      })
      console.log(`\n🔒 HTTPS (HTTP/2) →  https://localhost:${httpsPort}/`)
      console.log('   Multiplexes the live shape streams over one connection (past the ~6-per-origin HTTP/1.1 cap).')
      console.log("   The cert is from Caddy's local CA: run `caddy trust` once to remove the browser warning, or click through it.")
    } else {
      console.log('\n(Install `caddy` to also serve over HTTPS/HTTP-2 and avoid the browser ~6-connection HTTP/1.1 cap on live streams.)')
    }
  }

  // Optional pipeline visualizer (the learning tool): a small web GUI attached to THIS engine that
  // renders the maintained dbsp pipeline — shapes, shared equality families, and shared subquery nodes.
  // Auto-launched pointed at the engine; set DEMO_VIZ=0 to skip, DEMO_VIZ_PORT to change the port.
  if (process.env.DEMO_VIZ !== '0') {
    // Resolved up front (not left to vite's own silent fallback) so the banner below is always
    // the port the visualizer actually bound to, not just the one we asked for.
    const vizPort = String(await findFreePort(Number(process.env.DEMO_VIZ_PORT ?? '5180')))
    const vizHttpsOn = process.env.DEMO_HTTPS !== '0' && caddyProc != null
    const vizHttpsPort = process.env.DEMO_VIZ_HTTPS_PORT ?? '5443'
    // VIZ_HOST pins the bind so the Caddy front below has a deterministic upstream (vite's default
    // binding varies by platform — see the webui proxy above, which must dial the exact bound address).
    // VIZ_HMR_CLIENT_PORT makes vite's HMR websocket dial the caddy front (wss) instead of the
    // hardcoded plain-HTTP vite port, which a browser on the https URL cannot reach.
    vizProc = spawn('pnpm', ['--filter', '@electric-ivm/pipeline-viz', 'dev'], {
      cwd: repoRoot(),
      env: {
        ...process.env,
        ELECTRIC_IVM_ENGINE_URL: engineUrl,
        VIZ_PORT: vizPort,
        VIZ_HOST: '127.0.0.1',
        ...(vizHttpsOn ? { VIZ_HMR_CLIENT_PORT: vizHttpsPort } : {}),
      },
      stdio: 'ignore',
    })
    vizProc.on('exit', (c) => {
      if (!shuttingDown) console.warn(`pipeline visualizer exited (${c})`)
    })
    console.log(`\n🔬 Pipeline visualizer →  http://localhost:${vizPort}/`)
    console.log('   The live dbsp pipeline of this engine — click shapes to see how they are maintained.')

    // Same HTTP/2 front as the webui: the explorer holds a /trace SSE stream open plus per-card
    // polls, so it hits the same ~6-per-origin HTTP/1.1 cap (lifecycle events stall behind other
    // requests and the graph misses updates until a refresh). A second caddy instance needs its
    // own admin port; upstream is the pinned 127.0.0.1 host above.
    if (vizHttpsOn) {
      vizCaddyProc = spawn(
        'caddy',
        ['reverse-proxy', '--from', `https://localhost:${vizHttpsPort}`, '--to', `127.0.0.1:${vizPort}`],
        { stdio: 'inherit', env: { ...process.env, CADDY_ADMIN: 'localhost:2027' } },
      )
      vizCaddyProc.on('exit', (c) => {
        if (!shuttingDown) console.warn(`viz caddy proxy exited (${c}); explorer continues over HTTP only`)
      })
      console.log(`🔒 Visualizer HTTPS (HTTP/2) →  https://localhost:${vizHttpsPort}/`)
    }
  }

  console.log(`\n👉 Open a URL above. LinearLite (${PRIORITIES.length} priorities, ${STATUSES.length} statuses)`)
  console.log('   on electric-ivm: writes go to Postgres, replicate into the engine, and the')
  console.log('   board/list shapes update live.\n')
} catch (e) {
  console.error('linearlite: startup failed:', e)
  await shutdown(1)
}
