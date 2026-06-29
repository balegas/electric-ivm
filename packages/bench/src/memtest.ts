// Large-scale MEMORY test for the dbsp disk-spill path.
//
// Goal: drive the engine's dbsp state (family join traces) well past RAM-friendly sizes and compare
// in-memory vs on-disk storage. We use MEM_FAMILIES distinct equality templates (one per key column)
// so the base table is held once per family in its join trace — the M× memory amplification from
// ARCHITECTURE.md §8. The firehose inserts ever-growing distinct rows with a wide payload, so every
// trace grows without bound. Shapes use a constant the firehose never writes, so there is ~no append
// fan-out — we isolate *trace memory growth*, not output. A single "hot" shape is subscribed for
// latency.
//
// Run it twice, identical except for the engine's storage env:
//   in-memory:  pnpm --filter @electric-lite/bench exec tsx src/memtest.ts
//   on-disk:    ELECTRIC_LITE_STORAGE_DIR=/tmp/s ELECTRIC_LITE_STORAGE_CACHE=feldera \
//               ELECTRIC_LITE_STORAGE_CACHE_MIB=128 ELECTRIC_LITE_STORAGE_MIN_BYTES=1048576 \
//               pnpm --filter @electric-lite/bench exec tsx src/memtest.ts
//
// Config: MEM_FAMILIES (8), MEM_PAYLOAD bytes (512), MEM_DURATION s (45), MEM_CONC (8).

import { execFile, spawn } from 'node:child_process'
import { appendFileSync, existsSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'

import { DurableStreamTestServer } from '@durable-streams/server'
import { createApiServer } from '@electric-lite/api'
import { createClient } from '@electric-lite/client'
import type { Schema } from '@electric-lite/protocol'

const execFileP = promisify(execFile)
const env = (k: string, d: number) => (process.env[k] ? Number(process.env[k]) : d)
const M = env('MEM_FAMILIES', 8)
const W = env('MEM_PAYLOAD', 512)
const DURATION = env('MEM_DURATION', 45)
const CONC = env('MEM_CONC', 8)
const STORAGE_DIR = process.env.ELECTRIC_LITE_STORAGE_DIR || ''

const OUTFILE = process.env.MEM_OUT ?? join(dirname(fileURLToPath(import.meta.url)), '..', 'memtest.txt')
const log = (line = '') => {
  process.stdout.write(`${line}\n`)
  try {
    appendFileSync(OUTFILE, `${line}\n`)
  } catch {
    /* ignore */
  }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))
const now = () => Number(process.hrtime.bigint() / 1000n) / 1000

function repoRoot(): string {
  let d = dirname(fileURLToPath(import.meta.url))
  for (let i = 0; i < 8; i++) {
    if (existsSync(join(d, 'Cargo.toml'))) return d
    d = dirname(d)
  }
  throw new Error('repo root not found')
}

// Schema: id pk, k0..k{M-1} int (one family each), wide payload text.
const cols: Record<string, { type: string }> = { id: { type: 'int' } }
for (let t = 0; t < M; t++) cols[`k${t}`] = { type: 'int' }
cols.payload = { type: 'text' }
const schema = { tables: { items: { columns: cols, primaryKey: 'id' } } } as unknown as Schema
const PAD = 'x'.repeat(W)
const HOT = 7 // hot-shape constant on k0; the firehose never writes k0=7

let enginePid = 0

async function spawnEngine(dsUrl: string) {
  const proc = spawn(join(repoRoot(), 'target', 'release', 'electric-lite-engine'), [], {
    env: { ...process.env, ELECTRIC_LITE_DS_URL: dsUrl, ELECTRIC_LITE_BIND: '127.0.0.1:0', ELECTRIC_LITE_LOG: 'warn' },
    stdio: ['ignore', 'pipe', 'inherit'],
  })
  const url = await new Promise<string>((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('engine did not start')), 30000)
    let buf = ''
    proc.stdout!.on('data', (d: Buffer) => {
      buf += d.toString()
      const m = buf.match(/ENGINE_LISTENING (\S+)/)
      if (m) {
        clearTimeout(t)
        resolve(m[1]!)
      }
    })
    proc.on('exit', (c) => reject(new Error(`engine exited ${c}`)))
  })
  return { url, proc }
}

async function rssMb(pid: number): Promise<number> {
  for (let i = 0; i < 3; i++) {
    try {
      const { stdout } = await execFileP('ps', ['-o', 'rss=', '-p', String(pid)])
      const rss = Number(stdout.trim())
      if (rss) return rss / 1024
    } catch {
      /* retry */
    }
    await sleep(40)
  }
  return 0
}

async function diskMb(): Promise<number> {
  if (!STORAGE_DIR) return 0
  try {
    const { stdout } = await execFileP('du', ['-sk', STORAGE_DIR])
    return Number(stdout.trim().split(/\s+/)[0]) / 1024
  } catch {
    return 0
  }
}

async function createShape(engineUrl: string, where: Record<string, unknown>) {
  const res = await fetch(`${engineUrl}/shapes`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ table: 'items', where }),
  })
  if (!res.ok) throw new Error(`create shape -> ${res.status}: ${await res.text()}`)
}

const pct = (s: number[], q: number) => (s.length ? s[Math.min(s.length - 1, Math.max(0, Math.ceil(q * s.length) - 1))]! : 0)

function randomRow(id: number): Record<string, unknown> {
  const row: Record<string, unknown> = { id, payload: PAD }
  for (let t = 0; t < M; t++) row[`k${t}`] = t === 0 ? 10 + ((Math.random() * 1000) | 0) : (Math.random() * 1000) | 0
  return row
}

async function main() {
  writeFileSync(OUTFILE, '')
  log(`\n=== memtest: ${M} families, ${W}B payload, ${DURATION}s, conc=${CONC}, storage=${STORAGE_DIR ? `on (${process.env.ELECTRIC_LITE_STORAGE_CACHE || 'page'})` : 'OFF (in-memory)'} ===`)
  if (!existsSync(join(repoRoot(), 'target', 'release', 'electric-lite-engine'))) {
    console.error('build first: cargo build --release -p electric-lite-engine')
    process.exit(1)
  }

  const ds = new DurableStreamTestServer({ port: 0 })
  const dsUrl = await ds.start()
  const engine = await spawnEngine(dsUrl)
  enginePid = engine.proc.pid!
  const api = await createApiServer({ dsUrl, engineUrl: engine.url })
  const client = createClient({ apiUrl: api.url, schema, liveMode: 'long-poll' })
  await client.defineSchema(schema)

  // One family per key column (constant the firehose never writes), plus a hot k0 shape we subscribe.
  for (let t = 0; t < M; t++) await createShape(engine.url, { col: `k${t}`, op: 'eq', value: -1 })
  const tables = client.tables as Record<string, { update: (r: Record<string, unknown>) => Promise<{ txid: string }> }>
  const hot = await client.shape({ table: 'items', where: { col: 'k0', op: 'eq', value: HOT } })
  const fams = await (await fetch(`${engine.url}/tables/items/families`)).json()
  log(`families=${fams.families.length}; baseline RSS=${(await rssMb(enginePid)).toFixed(0)}MB`)

  const deadline = now() + DURATION * 1000
  let nextId = 1
  let inserts = 0
  const latencies: number[] = []
  const rssTrace: number[] = []

  const sampler = (async () => {
    while (now() < deadline) {
      rssTrace.push(await rssMb(enginePid))
      await sleep(2000)
    }
  })()

  const prober = (async () => {
    let pid = 1_000_000_000
    while (now() < deadline) {
      const row = randomRow(pid++)
      row.k0 = HOT // force a match on the subscribed hot shape
      const t0 = now()
      try {
        const { txid } = await tables.items.update(row)
        await hot.awaitTxId(txid, 8000)
        latencies.push(now() - t0)
      } catch {
        /* miss */
      }
      await sleep(40)
    }
  })()

  const firehose = Array.from({ length: CONC }, () =>
    (async () => {
      while (now() < deadline) {
        await tables.items.update(randomRow(nextId++)).catch(() => {})
        inserts++
      }
    })(),
  )

  await Promise.all([sampler, prober, ...firehose])
  latencies.sort((a, b) => a - b)
  const peak = Math.max(...rssTrace)
  const start = rssTrace[0] ?? 0
  const end = rssTrace.at(-1) ?? 0
  const disk = await diskMb()

  // Drained measurement: wait until the engine has processed the whole table stream, then let memory
  // settle, and re-measure RSS. With the routing model the engine retains NO table rows (only per-shape
  // key metadata), so this should fall back toward baseline — the firehose-time peak is dominated by
  // the transient read-batch backlog, not retained data.
  const tail = (await fetch(`${dsUrl}/table/items`, { method: 'HEAD' })).headers.get('stream-next-offset')
  const drainDeadline = now() + 30000
  while (now() < drainDeadline) {
    const r = await fetch(`${engine.url}/tables/items/offset`).catch(() => null)
    if (r?.ok) {
      const { offset } = (await r.json()) as { offset: string }
      if (tail && offset >= tail) break
    }
    await sleep(200)
  }
  await sleep(3000)
  const drained = await rssMb(enginePid)

  log(`\nrows inserted:     ${inserts} (~${(nextId / 1000).toFixed(0)}k distinct rows written to the table stream)`)
  log(`engine RSS:        start=${start.toFixed(0)}MB  peak=${peak.toFixed(0)}MB  end=${end.toFixed(0)}MB`)
  log(`drained RSS:       ${drained.toFixed(0)}MB  (after the engine processed the whole stream + settled)`)
  log(`RSS trajectory:    ${rssTrace.map((r) => r.toFixed(0)).join(' ')}`)
  if (STORAGE_DIR) log(`disk spilled:      ${disk.toFixed(0)}MB`)
  log(`hot-shape latency: p50=${pct(latencies, 0.5).toFixed(1)}ms  p99=${pct(latencies, 0.99).toFixed(1)}ms  max=${pct(latencies, 1).toFixed(1)}ms  (${latencies.length} probes)`)

  await client.close().catch(() => {})
  await api.close().catch(() => {})
  engine.proc.kill('SIGKILL')
  await ds.stop().catch(() => {})
  process.exit(0)
}

main().catch((e) => {
  console.error(e)
  try {
    enginePid && process.kill(enginePid, 'SIGKILL')
  } catch {
    /* gone */
  }
  process.exit(1)
})
