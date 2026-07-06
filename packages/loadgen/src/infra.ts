// Headless infrastructure lifecycle for the load generator: boots an ephemeral Postgres (logical
// replication), a file-backed durable-streams server, the Rust engine (Postgres mode), and the tRPC
// API; seeds the LinearLite dataset; and tears everything down. Mirrors examples/linearlite/start.ts
// minus Vite/Caddy/viz. Writes go to Postgres (the system of record); the engine ingests via
// replication. `dataDir` on durable-streams makes it file-backed so disk usage is measurable.

import { type ChildProcess, execFileSync, spawn } from 'node:child_process'
import { appendFileSync, mkdtempSync, mkdirSync, rmSync } from 'node:fs'
import { existsSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import type { DurableStreamTestServer } from '@durable-streams/server'
import { type ApiServer, createApiServer } from '@electric-ivm/api'
import type { Schema } from '@electric-ivm/protocol'
import { faker } from '@faker-js/faker'
import pgpkg from 'pg'

export const STATUSES = ['backlog', 'todo', 'in_progress', 'done', 'canceled'] as const
export const PRIORITIES = ['none', 'low', 'medium', 'high', 'urgent'] as const
export const SEED_PRIORITIES = ['none', 'low', 'medium', 'high'] as const
export const USERS = ['alice', 'bob', 'carol', 'dave', 'erin', 'frank']
export const PROJECTS = [
  { name: 'Web App', color: '#5e6ad2' },
  { name: 'Mobile', color: '#26a269' },
  { name: 'Infra', color: '#c64600' },
  { name: 'Design System', color: '#a347ba' },
  { name: 'Marketing', color: '#0a7ea4' },
]
/** Project ids each user (index+1) belongs to — drives issue visibility. */
export const MEMBERSHIP: number[][] = [[1, 2, 3], [2, 3, 4], [3, 4, 5], [1, 4, 5], [1, 2, 5], [1, 2, 3, 4, 5]]

export const schema: Schema = {
  tables: {
    issues: {
      columns: {
        id: { type: 'int' },
        title: { type: 'text' },
        description: { type: 'text' },
        status: { type: 'text' },
        priority: { type: 'text' },
        username: { type: 'text' },
        project_id: { type: 'int' },
        created: { type: 'int' },
        modified: { type: 'int' },
        kanbanorder: { type: 'float' },
      },
      primaryKey: 'id',
    },
    projects: { columns: { id: { type: 'int' }, name: { type: 'text' }, color: { type: 'text' } }, primaryKey: 'id' },
    users: { columns: { id: { type: 'int' }, name: { type: 'text' } }, primaryKey: 'id' },
    project_members: {
      columns: { id: { type: 'int' }, project_id: { type: 'int' }, user_id: { type: 'int' } },
      primaryKey: 'id',
    },
    comments: {
      columns: {
        id: { type: 'int' },
        issue_id: { type: 'int' },
        body: { type: 'text' },
        username: { type: 'text' },
        created: { type: 'int' },
      },
      primaryKey: 'id',
    },
  },
}

const SLOT = 'electric_ivm_loadgen'

function repoRoot(): string {
  let d = dirname(fileURLToPath(import.meta.url))
  for (let i = 0; i < 10; i++) {
    if (existsSync(join(d, 'Cargo.toml'))) return d
    d = dirname(d)
  }
  throw new Error('repo root (Cargo.toml) not found')
}

export interface Infra {
  pgUrl: string
  dsUrl: string
  engineUrl: string
  apiUrl: string
  enginePid: number
  dsDir: string
  pg: pgpkg.Client
  /** Bound ports (for building client-reachable URLs from an external host in Docker). */
  ports: { pg: number; ds: number; api: number }
  shutdown: () => Promise<void>
}

const DDL = `
CREATE TABLE projects ( id BIGINT PRIMARY KEY, name TEXT NOT NULL, color TEXT NOT NULL );
CREATE TABLE users ( id BIGINT PRIMARY KEY, name TEXT NOT NULL );
CREATE TABLE project_members ( id BIGINT PRIMARY KEY, project_id BIGINT NOT NULL, user_id BIGINT NOT NULL );
CREATE TABLE issues (
  id BIGINT PRIMARY KEY, title TEXT NOT NULL, description TEXT NOT NULL, status TEXT NOT NULL,
  priority TEXT NOT NULL, username TEXT NOT NULL, project_id BIGINT NOT NULL,
  created BIGINT NOT NULL, modified BIGINT NOT NULL, kanbanorder DOUBLE PRECISION NOT NULL );
CREATE TABLE comments (
  id BIGINT PRIMARY KEY, issue_id BIGINT NOT NULL REFERENCES issues(id) ON DELETE CASCADE,
  body TEXT NOT NULL, username TEXT NOT NULL, created BIGINT NOT NULL );
`

export interface InfraOpts {
  /** Bind host for the API + durable-streams + Postgres so client containers can connect. Default 127.0.0.1. */
  bindHost?: string
  /** Fixed ports (for Docker clients to have stable URLs). 0/undefined = ephemeral. */
  dsPort?: number
  apiPort?: number
  pgPort?: number
  /** In-memory durable-streams (no per-append fsync). Needed for high concurrency; ds disk = 0. */
  dsInMemory?: boolean
}

/** Boot the whole stack headless and seed `seedIssues` issues. Returns handles + a shutdown fn. */
export async function bootInfra(seedIssues: number, opts: InfraOpts = {}, log = console.log): Promise<Infra> {
  const bindHost = opts.bindHost ?? '127.0.0.1'
  const listen = bindHost === '0.0.0.0' ? '*' : bindHost
  const work = mkdtempSync(join(tmpdir(), 'el-loadgen-'))
  const pgData = join(work, 'pg')
  const dsDir = join(work, 'ds')
  mkdirSync(dsDir, { recursive: true })

  let pg: pgpkg.Client | undefined
  let ds: DurableStreamTestServer | undefined
  let engineProc: ChildProcess | undefined
  let api: ApiServer | undefined
  let shuttingDown = false

  const shutdown = async () => {
    if (shuttingDown) return
    shuttingDown = true
    engineProc?.kill('SIGKILL')
    if (pg) {
      await pg.query('SELECT pg_drop_replication_slot($1)', [SLOT]).catch(() => {})
      await pg.end().catch(() => {})
    }
    try {
      execFileSync('pg_ctl', ['-D', pgData, '-m', 'immediate', '-w', 'stop'], { stdio: 'ignore' })
    } catch {
      /* ignore */
    }
    await api?.close().catch(() => {})
    await ds?.stop().catch(() => {})
    try {
      rmSync(work, { recursive: true, force: true })
    } catch {
      /* ignore */
    }
  }

  try {
    // 1. Ephemeral Postgres with logical replication.
    execFileSync('initdb', ['-D', pgData, '-U', 'postgres', '--auth=trust', '--no-sync'], { stdio: 'ignore' })
    // Allow connections from other hosts/containers when bound externally (ephemeral demo PG, trust auth).
    if (bindHost !== '127.0.0.1') appendFileSync(join(pgData, 'pg_hba.conf'), `\nhost all all 0.0.0.0/0 trust\n`)
    const fixedPg = opts.pgPort && opts.pgPort > 0 ? opts.pgPort : undefined
    let pgPort = 0
    let started = false
    const attempts = fixedPg ? 1 : 8
    for (let attempt = 0; attempt < attempts && !started; attempt++) {
      pgPort = fixedPg ?? 54600 + Math.floor(Math.random() * 4000)
      appendFileSync(
        join(pgData, 'postgresql.conf'),
        `\nwal_level = logical\nmax_replication_slots = 10\nmax_wal_senders = 10\n` +
          `listen_addresses = '${listen}'\nunix_socket_directories = '/tmp'\nport = ${pgPort}\nfsync = off\n`,
      )
      try {
        execFileSync('pg_ctl', ['-D', pgData, '-l', join(work, 'pg.log'), '-w', 'start'], { stdio: 'ignore' })
        started = true
      } catch {
        /* port in use; retry */
      }
    }
    if (!started) throw new Error('failed to start ephemeral postgres')
    const pgUrl = `postgres://postgres@127.0.0.1:${pgPort}/postgres`
    pg = new pgpkg.Client({ connectionString: pgUrl })
    await pg.connect()

    await pg.query(DDL)
    for (const t of Object.keys(schema.tables)) await pg.query(`ALTER TABLE ${t} REPLICA IDENTITY FULL`)

    // 2. Seed.
    await seed(pg, seedIssues, log)

    // 3. durable-streams (file-backed → disk measurable), engine (Postgres mode), API.
    // Lazily loaded so `client` mode (Docker replicas) never pulls the native LMDB dependency.
    const { DurableStreamTestServer } = await import('@durable-streams/server')
    // File-backed (LMDB) makes disk measurable but fsyncs each append — a bottleneck at high concurrency.
    // In-memory removes that ceiling (ds disk then reads 0).
    ds = new DurableStreamTestServer(
      opts.dsInMemory ? { port: opts.dsPort ?? 0, host: bindHost } : { port: opts.dsPort ?? 0, host: bindHost, dataDir: dsDir },
    )
    const dsUrlRaw = await ds.start()
    const dsPort = new URL(dsUrlRaw).port
    // Server-side URLs use 127.0.0.1 (the engine/API/sampler run on this host); client containers use
    // the externally-reachable host + the same ports.
    const dsUrl = `http://127.0.0.1:${dsPort}`

    execFileSync('cargo', ['build', '-p', 'electric-ivm-engine'], { cwd: repoRoot(), stdio: 'ignore' })
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
      const t = setTimeout(() => reject(new Error('engine did not start within 30s')), 30000)
      let buf = ''
      engineProc!.stdout!.on('data', (d: Buffer) => {
        buf += d.toString()
        const m = buf.match(/ENGINE_LISTENING (\S+)/)
        if (m) {
          clearTimeout(t)
          resolve(m[1]!)
        }
      })
      engineProc!.on('exit', (c) => reject(new Error(`engine exited early (${c})`)))
    })

    api = await createApiServer({ dsUrl, engineUrl, host: bindHost, port: opts.apiPort })

    return {
      pgUrl,
      dsUrl,
      engineUrl,
      apiUrl: api.url,
      enginePid: engineProc.pid!,
      dsDir: opts.dsInMemory ? '' : dsDir,
      pg,
      ports: { pg: pgPort, ds: Number(dsPort), api: Number(new URL(api.url).port) },
      shutdown,
    }
  } catch (e) {
    await shutdown()
    throw e
  }
}

/** Seed projects/users/memberships/issues/comments (fixed roster; `seedIssues` issues). */
export async function seed(pg: pgpkg.Client, seedIssues: number, log = console.log): Promise<void> {
  faker.seed(42)
  const now = Date.now()
  const projectRows = PROJECTS.map((p, i) => [i + 1, p.name, p.color])
  const userRows = USERS.map((n, i) => [i + 1, n])
  const memberRows: unknown[][] = []
  let memberId = 1
  MEMBERSHIP.forEach((projectIds, ui) => {
    for (const pid of projectIds) memberRows.push([memberId++, pid, ui + 1])
  })

  const issueRows: unknown[][] = []
  const commentRows: unknown[][] = []
  let commentId = 1
  let order = 1
  for (let id = 1; id <= seedIssues; id++) {
    const created = faker.date.past({ years: 1 }).getTime()
    const modified = faker.date.between({ from: created, to: now }).getTime()
    const projectId = (id % PROJECTS.length) + 1
    issueRows.push([
      id,
      faker.lorem.sentence({ min: 3, max: 8 }).replace(/\.$/, ''),
      faker.lorem.paragraphs({ min: 1, max: 3 }),
      STATUSES[Math.floor(Math.random() * STATUSES.length)],
      SEED_PRIORITIES[Math.floor(Math.random() * SEED_PRIORITIES.length)],
      USERS[Math.floor(Math.random() * USERS.length)],
      projectId,
      created,
      modified,
      order++ + Math.random(),
    ])
    if (Math.random() < 0.5) {
      commentRows.push([commentId++, id, faker.lorem.sentences({ min: 1, max: 3 }), USERS[Math.floor(Math.random() * USERS.length)], faker.date.between({ from: created, to: now }).getTime()])
    }
  }

  const bulk = async (table: string, cols: string[], rows: unknown[][]) => {
    const CHUNK = 1000
    for (let s = 0; s < rows.length; s += CHUNK) {
      const slice = rows.slice(s, s + CHUNK)
      const params: unknown[] = []
      let p = 1
      const tuples = slice.map((r) => `(${r.map(() => `$${p++}`).join(',')})`)
      for (const r of slice) params.push(...r)
      await pg.query(`INSERT INTO "${table}" (${cols.map((c) => `"${c}"`).join(',')}) VALUES ${tuples.join(',')}`, params)
    }
  }

  const t0 = Date.now()
  await pg.query('BEGIN')
  await bulk('projects', ['id', 'name', 'color'], projectRows)
  await bulk('users', ['id', 'name'], userRows)
  await bulk('project_members', ['id', 'project_id', 'user_id'], memberRows)
  await bulk('issues', ['id', 'title', 'description', 'status', 'priority', 'username', 'project_id', 'created', 'modified', 'kanbanorder'], issueRows)
  await bulk('comments', ['id', 'issue_id', 'body', 'username', 'created'], commentRows)
  await pg.query('COMMIT')
  log(`  seeded ${issueRows.length} issues + ${commentRows.length} comments (${Date.now() - t0}ms)`)
}
