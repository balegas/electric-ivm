// Conformance harness: boots the whole electric-lite stack and tears it down cleanly.
//
// Topology (one external process — the Rust engine):
//   Vitest process: DurableStreamTestServer (Node) + tRPC API + pglite oracle + streamdb client
//   child process:  electric-lite-engine (Rust), a durable-streams client running dbsp circuits

import { type ChildProcess, execFileSync, spawn } from 'node:child_process'
import { existsSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { DurableStreamTestServer } from '@durable-streams/server'
import { type ApiServer, createApiServer } from '@electric-lite/api'
import { createClient, type ElectricLiteClient, type ShapeMaterialization } from '@electric-lite/client'
import { createOracle, type Oracle } from '@electric-lite/oracle'
import type { ChangeEvent, Row, Schema, ShapeDef } from '@electric-lite/protocol'

import { compareShapeSets, type CompareResult } from './compare.js'

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
  if (engineBuilt || process.env.ELECTRIC_LITE_ENGINE_PREBUILT === '1') return
  execFileSync('cargo', ['build', '-p', 'electric-lite-engine'], { cwd: repoRoot(), stdio: 'inherit' })
  engineBuilt = true
}

function engineBin(): string {
  return join(repoRoot(), 'target', 'debug', 'electric-lite-engine')
}

async function spawnEngine(dsUrl: string): Promise<{ url: string; proc: ChildProcess }> {
  const proc = spawn(engineBin(), [], {
    env: {
      ...process.env,
      ELECTRIC_LITE_DS_URL: dsUrl,
      ELECTRIC_LITE_BIND: '127.0.0.1:0',
      ELECTRIC_LITE_LOG: process.env.ELECTRIC_LITE_LOG ?? 'warn',
    },
    stdio: ['ignore', 'pipe', 'inherit'],
  })
  const url = await new Promise<string>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('engine did not report listening within 20s')), 20000)
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
  })
  return { url, proc }
}

export interface Harness {
  dsUrl: string
  engineUrl: string
  apiUrl: string
  api: ApiServer
  client: ElectricLiteClient
  oracle: Oracle
  schema: Schema
  shutdown(): Promise<void>
}

export async function bootHarness(schema: Schema): Promise<Harness> {
  buildEngine()
  const server = new DurableStreamTestServer({ port: 0 })
  const dsUrl = await server.start()
  const { url: engineUrl, proc } = await spawnEngine(dsUrl)
  const api = await createApiServer({ dsUrl, engineUrl })
  const oracle = await createOracle(schema)
  const client = createClient({ apiUrl: api.url, schema })
  await client.defineSchema(schema) // defines on the engine (oracle DDL is built by createOracle)

  return {
    dsUrl,
    engineUrl,
    apiUrl: api.url,
    api,
    client,
    oracle,
    schema,
    async shutdown() {
      await client.close().catch(() => {})
      await api.close().catch(() => {})
      proc.kill('SIGKILL')
      await oracle.close().catch(() => {})
      await server.stop().catch(() => {})
    },
  }
}

/** Apply one change to both the oracle and electric-lite (via the API). Returns the write txid. */
export async function applyOp(h: Harness, table: string, ev: ChangeEvent): Promise<string> {
  await h.oracle.applyChange(table, ev)
  const { txid } = await h.client.write({ table, op: ev.op, pk: ev.pk, row: ev.row })
  return txid
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

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

/**
 * Wait until the engine has processed every table stream up to its current tail. This is the
 * convergence barrier: without it, a freshly-empty shape can read `[] == []` before the engine
 * has done any work and pass spuriously. Durable-streams offsets are zero-padded, lexicographically
 * comparable tokens, so a string `>=` is a valid "has reached the tail" check.
 */
export async function drainEngine(h: Harness, timeoutMs = 15000): Promise<void> {
  for (const table of Object.keys(h.schema.tables)) {
    const tail = await tableTail(h.dsUrl, table)
    if (!tail) continue
    const start = Date.now()
    while (Date.now() - start < timeoutMs) {
      const off = await engineTableOffset(h.engineUrl, table)
      if (off === null) break // no tailer -> no shape on this table -> nothing to drain
      if (off >= tail) break
      await sleep(25)
    }
  }
}

export interface ConvergenceTarget {
  shape: ShapeMaterialization
  def: ShapeDef
  columns: string[]
  pk: string
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
