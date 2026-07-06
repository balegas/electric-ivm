// Conformance harness (Postgres mode): Postgres is the system of record. Changes are written to
// Postgres, captured by the engine's logical-replication ingestor, fanned out to shape streams, and
// materialized by the streamdb client. The SAME Postgres answers `SELECT … WHERE pred` as the oracle.
//
// Topology:
//   Vitest worker:  per-test Postgres database (in the shared ephemeral PG) + DurableStreamTestServer
//                   + tRPC API + streamdb client + pg-backed oracle
//   child process:  electric-ivm-engine (Rust) in Postgres mode (ingestor + query-back backfill)

import { type ChildProcess, execFileSync, spawn } from 'node:child_process'
import { existsSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { DurableStreamTestServer } from '@durable-streams/server'
import { type ApiServer, createApiServer } from '@electric-ivm/api'
import { createClient, type ElectricIvmClient, type ShapeMaterialization } from '@electric-ivm/client'
import { createPgOracle, createPgTables, type Oracle } from '@electric-ivm/oracle'
import type { ChangeEvent, Row, Schema, ShapeDef } from '@electric-ivm/protocol'
import pgpkg from 'pg'

import { compareShapeSets, type CompareResult } from './compare.js'

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

function repoRoot(): string {
  let d = dirname(fileURLToPath(import.meta.url))
  for (let i = 0; i < 8; i++) {
    if (existsSync(join(d, 'Cargo.toml'))) return d
    d = dirname(d)
  }
  throw new Error('repo root (Cargo.toml) not found')
}

let engineBuilt = false
/** Build the engine binary once per process. Skipped when the vitest globalSetup already built it. */
export function buildEngine(): void {
  if (engineBuilt || process.env.ELECTRIC_IVM_ENGINE_PREBUILT === '1') return
  execFileSync('cargo', ['build', '-p', 'electric-ivm-engine'], { cwd: repoRoot(), stdio: 'inherit' })
  engineBuilt = true
}

function engineBin(): string {
  return join(repoRoot(), 'target', 'debug', 'electric-ivm-engine')
}

async function spawnEngine(
  dsUrl: string,
  pgUrl: string,
  tables: string[],
  slot: string,
  fault?: string,
): Promise<{ url: string; proc: ChildProcess }> {
  const proc = spawn(engineBin(), [], {
    env: {
      ...process.env,
      ELECTRIC_IVM_DS_URL: dsUrl,
      ELECTRIC_IVM_BIND: '127.0.0.1:0',
      ELECTRIC_IVM_LOG: process.env.ELECTRIC_IVM_LOG ?? 'warn',
      ELECTRIC_IVM_PG_URL: pgUrl,
      ELECTRIC_IVM_PG_TABLES: tables.join(','),
      ELECTRIC_IVM_PG_SLOT: slot,
      ELECTRIC_IVM_PG_POLL_MS: '25',
      ...(fault ? { ELECTRIC_IVM_FAULT: fault } : {}),
    },
    stdio: ['ignore', 'pipe', 'inherit'],
  })
  const url = await new Promise<string>((resolve, reject) => {
    const timer = setTimeout(() => {
      proc.kill('SIGKILL') // don't leak the child if it never reports listening
      reject(new Error('engine did not report listening within 20s'))
    }, 20000)
    let buf = ''
    proc.stdout!.on('data', (d: Buffer) => {
      buf += d.toString()
      const m = buf.match(/ENGINE_LISTENING (\S+)/)
      if (m) {
        clearTimeout(timer)
        resolve(m[1]!)
      }
    })
    proc.on('exit', (code) => {
      clearTimeout(timer)
      reject(new Error(`engine exited early with code ${code}`))
    })
  }).catch((e) => {
    proc.kill('SIGKILL')
    throw e
  })
  return { url, proc }
}

export interface Harness {
  dsUrl: string
  engineUrl: string
  apiUrl: string
  api: ApiServer
  client: ElectricIvmClient
  oracle: Oracle
  schema: Schema
  /** Postgres connection string for this harness's database (the system of record). */
  pgUrl: string
  shutdown(): Promise<void>
}

export interface BootOptions {
  /** TEST-ONLY: inject an engine fault (e.g. 'drop_deletes', 'off_by_one_cmp') for negative controls. */
  fault?: string
}

function adminUrl(): string {
  const url = process.env.ELECTRIC_IVM_TEST_PG_URL
  if (!url) throw new Error('ELECTRIC_IVM_TEST_PG_URL not set (vitest globalSetup should boot Postgres)')
  return url
}

let dbCounter = 0
function uniqueDbName(): string {
  dbCounter += 1
  return `el_${process.pid}_${Date.now().toString(36)}_${dbCounter}`.toLowerCase()
}

export async function bootHarness(schema: Schema, opts: BootOptions = {}): Promise<Harness> {
  buildEngine()

  // 1. Create a dedicated database in the shared ephemeral Postgres (per-test isolation; slots are
  //    per-database). Create the tables (with REPLICA IDENTITY FULL) before the engine starts so its
  //    startup introspection + slot creation see them.
  const admin = new pgpkg.Client({ connectionString: adminUrl() })
  await admin.connect()
  const dbName = uniqueDbName()
  await admin.query(`CREATE DATABASE ${dbName}`)
  await admin.end()
  const pgUrl = adminUrl().replace(/\/[^/]+$/, `/${dbName}`)
  // Replication slot names are GLOBALLY unique in Postgres (not per-database), so derive a unique one.
  const slot = `slot_${dbName}`

  // Drop this harness's Postgres artifacts (slot then database). Used by both shutdown and the
  // partial-boot-failure cleanup, so a half-built harness never leaks a slot or database.
  const dropPgArtifacts = async () => {
    try {
      const c = new pgpkg.Client({ connectionString: pgUrl })
      await c.connect()
      for (let i = 0; i < 60; i++) {
        try {
          // Terminate any lingering walsender holding the slot, then drop it.
          await c.query('SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL', [slot]).catch(() => {})
          await c.query('SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)', [slot])
          break
        } catch {
          await sleep(100) // slot still marked active until PG notices the killed consumer
        }
      }
      await c.end()
    } catch {
      /* ignore */
    }
    try {
      const a = new pgpkg.Client({ connectionString: adminUrl() })
      await a.connect()
      await a.query(`DROP DATABASE IF EXISTS ${dbName} WITH (FORCE)`)
      await a.end()
    } catch {
      /* ignore */
    }
  }

  // Track resources so a failure at any step tears down everything created so far.
  let server: DurableStreamTestServer | undefined
  let proc: ChildProcess | undefined
  let api: ApiServer | undefined
  let oracle: Oracle | undefined
  let client: ElectricIvmClient | undefined
  const teardown = async () => {
    await client?.close().catch(() => {})
    await api?.close().catch(() => {})
    proc?.kill('SIGKILL')
    await oracle?.close().catch(() => {})
    await server?.stop().catch(() => {})
    await dropPgArtifacts()
  }

  try {
    // Create the tables (with REPLICA IDENTITY FULL) before the engine starts so its startup
    // introspection + slot creation see them.
    await createPgTables(pgUrl, schema)
    // Drain-barrier sentinel: a single-row counter table the replicator decodes (but does not treat
    // as a data table). drainEngine bumps it and waits for the engine to report it (see drainEngine).
    const c = new pgpkg.Client({ connectionString: pgUrl })
    await c.connect()
    await c.query('CREATE TABLE __el_sync (id int PRIMARY KEY, n bigint NOT NULL)')
    await c.query('INSERT INTO __el_sync (id, n) VALUES (1, 0)')
    await c.end()

    // 2. Boot durable-streams + the engine (Postgres mode) + API + client + oracle.
    server = new DurableStreamTestServer({ port: 0 })
    const dsUrl = await server.start()
    const tables = Object.keys(schema.tables)
    const spawned = await spawnEngine(dsUrl, pgUrl, tables, slot, opts.fault)
    proc = spawned.proc
    const engineUrl = spawned.url
    api = await createApiServer({ dsUrl, engineUrl })
    oracle = await createPgOracle(schema, pgUrl)
    client = createClient({ apiUrl: api.url, schema })
    // No client.defineSchema: in Postgres mode the engine self-configures from introspection.

    return {
      dsUrl,
      engineUrl,
      apiUrl: api.url,
      api,
      client,
      oracle,
      schema,
      pgUrl,
      shutdown: teardown,
    }
  } catch (e) {
    await teardown()
    throw e
  }
}

/** Apply one change to Postgres (the system of record). The engine receives it via replication. */
export async function applyOp(h: Harness, table: string, ev: ChangeEvent): Promise<void> {
  await h.oracle.applyChange(table, ev)
}

async function tableTail(dsUrl: string, table: string): Promise<string | null> {
  const res = await fetch(`${dsUrl}/table/${table}`, { method: 'HEAD' })
  if (!res.ok) return null
  return res.headers.get('stream-next-offset')
}

async function engineTableOffset(engineUrl: string, table: string): Promise<string | null> {
  const res = await fetch(`${engineUrl}/tables/${encodeURIComponent(table)}/offset`)
  if (res.status === 404) return null
  if (!res.ok) throw new Error(`engine offset ${table} -> ${res.status}`)
  return ((await res.json()) as { offset: string }).offset
}

async function engineReplicationSync(engineUrl: string): Promise<number> {
  const res = await fetch(`${engineUrl}/replication/lsn`)
  if (!res.ok) throw new Error(`engine replication status -> ${res.status}`)
  return Number(((await res.json()) as { sync: number }).sync)
}

/**
 * Convergence barrier (Postgres mode), in two stages:
 *  1. bump the per-database `__el_sync` sentinel counter, then wait until the engine reports having
 *     decoded-and-appended at least that value. The sentinel UPDATE commits AFTER every prior data
 *     write has committed (drainEngine runs once all applyOp() awaits have resolved), so its commit
 *     LSN is higher; the ingestor decodes in commit-LSN order, so seeing the sentinel implies every
 *     prior change is already on the stream. This is per-database, so it is robust under a shared
 *     multi-database Postgres (no dependence on server-global WAL LSNs).
 *  2. wait until the engine has processed each table stream up to its tail.
 * Without this a freshly-empty shape could read `[] == []` before the change has propagated.
 */
export async function drainEngine(h: Harness, timeoutMs = 20000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  // Stage 1: replication caught up to "now" (sentinel-based).
  const c = new pgpkg.Client({ connectionString: h.pgUrl })
  await c.connect()
  let target: number
  try {
    target = Number((await c.query('UPDATE __el_sync SET n = n + 1 WHERE id = 1 RETURNING n')).rows[0].n)
  } finally {
    await c.end().catch(() => {})
  }
  let synced = false
  while (Date.now() < deadline) {
    if ((await engineReplicationSync(h.engineUrl)) >= target) {
      synced = true
      break
    }
    await sleep(15)
  }
  // A missed barrier means propagation stalled — throw rather than let a stale/empty comparison
  // false-green. (Tests rely on drainEngine actually establishing the barrier.)
  if (!synced) {
    throw new Error(`drainEngine: replication did not reach sentinel ${target} within ${timeoutMs}ms`)
  }
  // Stage 2: engine processed each table stream up to its tail.
  for (const table of Object.keys(h.schema.tables)) {
    const tail = await tableTail(h.dsUrl, table)
    if (!tail) continue
    let reached = false
    while (Date.now() < deadline) {
      const off = await engineTableOffset(h.engineUrl, table)
      if (off === null || off >= tail) {
        reached = true
        break
      }
      await sleep(20)
    }
    if (!reached) {
      throw new Error(`drainEngine: engine did not reach tail ${tail} for table ${table} within ${timeoutMs}ms`)
    }
  }
}

export interface ConvergenceTarget {
  shape: ShapeMaterialization
  def: ShapeDef
  columns: string[]
  pk: string
}

/** One-shot comparison of the client-materialized set against the oracle (no polling). */
export async function snapshotCompare(h: Harness, target: ConvergenceTarget): Promise<CompareResult> {
  const oracleRows: Row[] = await h.oracle.queryShape(target.def)
  const clientRows = target.shape.currentRows()
  return compareShapeSets(target.columns, target.pk, oracleRows, clientRows)
}

/** Poll until the client-materialized set equals the oracle's, or the timeout elapses. */
export async function waitForConvergence(
  h: Harness,
  target: ConvergenceTarget,
  timeoutMs = 10000,
): Promise<CompareResult> {
  const start = Date.now()
  let last: CompareResult = { equal: false, missing: [], extra: [], mismatched: [] }
  while (Date.now() - start < timeoutMs) {
    const oracleRows: Row[] = await h.oracle.queryShape(target.def)
    const clientRows = target.shape.currentRows()
    last = compareShapeSets(target.columns, target.pk, oracleRows, clientRows)
    if (last.equal) return last
    await sleep(50)
  }
  return last
}
