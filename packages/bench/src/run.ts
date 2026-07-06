// Local stress benchmark for electric-ivm. Boots the stack (durable-streams + engine + API, no
// oracle), registers many equality shapes (which share one family circuit), then runs three things
// concurrently for a fixed duration:
//   1. a write firehose (bounded in-flight) to measure sustained throughput,
//   2. a latency prober that writes to subscribed shapes and times write -> shape-update (p50/p99),
//   3. a resource sampler (engine RSS/CPU via `ps`, plus the engine's /metrics histograms).
//
// Config via env (defaults are a quick smoke run):
//   BENCH_SHAPES   number of `tenant = k` shapes to register      (default 1000)
//   BENCH_SUBS     number of those shapes to subscribe for latency (default 100)
//   BENCH_DURATION load phase seconds                              (default 10)
//   BENCH_CONC     firehose in-flight writes                       (default 64)
//   BENCH_REGCONC  shape-registration concurrency                  (default 64)

import { execFile, spawn } from 'node:child_process'
import { appendFileSync, existsSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'

import { DurableStreamTestServer } from '@durable-streams/server'
import { createApiServer } from '@electric-ivm/api'
import { createClient } from '@electric-ivm/client'
import type { Schema } from '@electric-ivm/protocol'

const execFileP = promisify(execFile)
// Durable, line-buffered logging so results survive even if the run is backgrounded or killed.
const OUTFILE = process.env.BENCH_OUT ?? join(dirname(fileURLToPath(import.meta.url)), '..', 'results.txt')
const log = (line = '') => {
  process.stdout.write(`${line}\n`)
  try {
    appendFileSync(OUTFILE, `${line}\n`)
  } catch {
    /* ignore */
  }
}
let enginePidForCleanup = 0 // set after the engine spawns, used by the error handler
const env = (k: string, d: number) => (process.env[k] ? Number(process.env[k]) : d)
const SHAPES = env('BENCH_SHAPES', 1000)
const SUBS = Math.min(env('BENCH_SUBS', 100), SHAPES)
const STANDALONE = env('BENCH_STANDALONE', 0) // non-equality shapes (each its own circuit today)
const DURATION = env('BENCH_DURATION', 10)
const CONC = env('BENCH_CONC', 64)
const REGCONC = env('BENCH_REGCONC', 64)
// Firehose writes only to tenants in [0, HOTSET) so the load phase touches a bounded set of shape
// streams (connections stay pooled) instead of churning sockets across all SHAPES streams. All SHAPES
// shapes are still registered and active in the family join, so per-write cost reflects the full
// shape count. Defaults to the whole keyspace; set it to scale shapes past the local port limit.
const HOTSET = Math.min(env('BENCH_HOTSET', SHAPES), SHAPES)
// Optional chunked registration: register CHUNK shapes, pause CHUNK_PAUSE seconds to let TIME_WAIT
// drain, repeat. Lets registration exceed the ~16k ephemeral-port ceiling without sysctl changes.
const CHUNK = env('BENCH_CHUNK', 0)
const CHUNK_PAUSE = env('BENCH_CHUNK_PAUSE', 35)

const schema: Schema = {
  tables: {
    users: {
      columns: { id: { type: 'int' }, tenant: { type: 'int' }, seq: { type: 'int' }, active: { type: 'bool' } },
      primaryKey: 'id',
    },
  },
}

function repoRoot(): string {
  let d = dirname(fileURLToPath(import.meta.url))
  for (let i = 0; i < 8; i++) {
    if (existsSync(join(d, 'Cargo.toml'))) return d
    d = dirname(d)
  }
  throw new Error('repo root not found')
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))
const now = () => Number(process.hrtime.bigint() / 1000n) / 1000 // ms, sub-ms precision

async function spawnEngine(dsUrl: string) {
  const proc = spawn(join(repoRoot(), 'target', 'release', 'electric-ivm-engine'), [], {
    env: { ...process.env, ELECTRIC_IVM_DS_URL: dsUrl, ELECTRIC_IVM_BIND: '127.0.0.1:0', ELECTRIC_IVM_LOG: 'warn' },
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

// Create a shape directly on the engine (create-only: ensures the shape stream + registers it in
// the tailer, so every write fans out to it) WITHOUT opening a client-side live subscription. This
// is what lets us register 100k shapes — client.shape() would hold a long-poll connection per shape
// and exhaust ephemeral ports. Only the measured SUBS sample gets a real subscription.
async function createShape(engineUrl: string, where: Record<string, unknown>): Promise<void> {
  const res = await fetch(`${engineUrl}/shapes`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ table: 'users', where }),
  })
  if (!res.ok) throw new Error(`create shape -> ${res.status}: ${await res.text()}`)
}

function pct(sorted: number[], q: number): number {
  if (sorted.length === 0) return 0
  const i = Math.min(sorted.length - 1, Math.ceil(q * sorted.length) - 1)
  return sorted[Math.max(0, i)]!
}

async function sampleRss(pid: number): Promise<{ rssMb: number; cpu: number }> {
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const { stdout } = await execFileP('ps', ['-o', 'rss=,%cpu=', '-p', String(pid)])
      const [rss, cpu] = stdout.trim().split(/\s+/).map(Number)
      if (rss) return { rssMb: rss / 1024, cpu: cpu ?? 0 }
    } catch {
      /* retry */
    }
    await sleep(50)
  }
  return { rssMb: 0, cpu: 0 }
}

/** OS thread count of the engine process (macOS `ps -M` lists one line per thread). */
async function threadCount(pid: number): Promise<number> {
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const { stdout } = await execFileP('ps', ['-M', '-p', String(pid)])
      const n = stdout.trim().split('\n').length - 1 // minus the header row
      if (n > 0) return n
    } catch {
      /* retry */
    }
    await sleep(50)
  }
  return 0
}

async function main() {
  writeFileSync(OUTFILE, "")
  log(`\n=== electric-ivm bench: ${SHAPES} shapes, ${SUBS} subscribers, ${DURATION}s load, conc=${CONC} ===\n`)
  // Engine must be built in release for a meaningful benchmark.
  if (!existsSync(join(repoRoot(), 'target', 'release', 'electric-ivm-engine'))) {
    console.error('Build the release engine first: cargo build --release -p electric-ivm-engine')
    process.exit(1)
  }

  const ds = new DurableStreamTestServer({ port: 0 })
  const dsUrl = await ds.start()
  const engine = await spawnEngine(dsUrl)
  const api = await createApiServer({ dsUrl, engineUrl: engine.url })
  const client = createClient({ apiUrl: api.url, schema, liveMode: 'long-poll' })
  await client.defineSchema(schema)
  const pid = engine.proc.pid!
  enginePidForCleanup = pid

  // --- 1. Register SHAPES equality shapes (tenant = k); they share one family circuit. ---
  const regStart = now()
  let registered = 0
  const registerOne = async (k: number) => {
    await createShape(engine.url, { col: 'tenant', op: 'eq', value: k })
    registered++
  }
  if (CHUNK > 0) {
    for (let base = 0; base < SHAPES; base += CHUNK) {
      const n = Math.min(CHUNK, SHAPES - base)
      await runPool(n, REGCONC, async (i) => registerOne(base + i))
      log(`  registered ${registered}/${SHAPES}${base + n < SHAPES ? ` (pause ${CHUNK_PAUSE}s for TIME_WAIT drain)` : ''}`)
      if (base + n < SHAPES) await sleep(CHUNK_PAUSE * 1000)
    }
  } else {
    await runPool(SHAPES, REGCONC, registerOne)
  }
  const regMs = now() - regStart
  log(`registered ${registered} equality shapes in ${(regMs / 1000).toFixed(1)}s (${Math.round(registered / (regMs / 1000))}/s)`)

  // --- 1b. Register STANDALONE (non-equality) shapes: each is a distinct range filter. Thresholds
  // sit above the firehose's seq range so every write is *evaluated* against all K predicates
  // (the O(K)/write cost we want to measure) without exploding append fan-out. ---
  if (STANDALONE > 0) {
    const sStart = now()
    await runPool(STANDALONE, REGCONC, async (k) => {
      await createShape(engine.url, { col: 'seq', op: 'gt', value: 100_000_000 + k })
    })
    const sMs = now() - sStart
    log(`registered ${STANDALONE} standalone shapes in ${(sMs / 1000).toFixed(1)}s (${Math.round(STANDALONE / (sMs / 1000))}/s)`)
  }

  const afterReg = await sampleRss(pid)
  const threads = await threadCount(pid)
  const fams = await (await fetch(`${engine.url}/tables/users/families`)).json()
  log(
    `topology: families=${fams.families.length} (eq shapes), standalone=${fams.standalone}; ` +
      `engine threads=${threads}; RSS=${afterReg.rssMb.toFixed(0)}MB`,
  )

  // --- 2. Subscribe a sample for end-to-end latency (these are extra shapes on the same family). ---
  const subs = [] as { tenant: number; sub: Awaited<ReturnType<typeof client.shape>> }[]
  await runPool(SUBS, REGCONC, async (k) => {
    const sub = await client.shape({ table: 'users', where: { col: 'tenant', op: 'eq', value: k } })
    subs.push({ tenant: k, sub })
  })

  await fetch(`${engine.url}/metrics/reset`, { method: 'POST' })

  // --- 3. Load phase: firehose + latency prober + resource sampler, concurrently. ---
  const deadline = now() + DURATION * 1000
  const latencies: number[] = []
  const rssTrace: number[] = []
  const cpuTrace: number[] = []
  let firehoseWrites = 0
  let seq = 0

  const sampler = (async () => {
    while (now() < deadline) {
      const s = await sampleRss(pid)
      rssTrace.push(s.rssMb)
      cpuTrace.push(s.cpu)
      await sleep(1000)
    }
  })()

  const prober = (async () => {
    while (now() < deadline) {
      const { tenant, sub } = subs[Math.floor(Math.random() * subs.length)]!
      const pk = 2_000_000 + tenant
      const t0 = now()
      try {
        const { txid } = await client.tables.users.update({ id: pk, tenant, seq: seq++, active: true })
        await sub.awaitTxId(txid, 5000)
        latencies.push(now() - t0)
      } catch {
        /* timeout counts as a miss; ignore for percentile (reported separately if needed) */
      }
      await sleep(20) // ~50 probes/sec
    }
  })()

  // Firehose: CONC workers upserting rows within the bounded hot set of tenants (so load touches a
  // small set of streams). Each write still flows through the family join against all SHAPES params.
  const firehose = Array.from({ length: CONC }, () =>
    (async () => {
      while (now() < deadline) {
        const tenant = Math.floor(Math.random() * HOTSET)
        const pk = 1 + tenant + HOTSET * Math.floor(Math.random() * 40) // bounded pk space, tenant = pk's slot
        await client.tables.users.update({ id: pk, tenant, seq: seq++, active: pk % 2 === 0 }).catch(() => {})
        firehoseWrites++
      }
    })(),
  )

  await Promise.all([sampler, prober, ...firehose])
  const loadMs = now() - (deadline - DURATION * 1000)
  const metrics = await (await fetch(`${engine.url}/metrics`)).json()

  // --- Report ---
  latencies.sort((a, b) => a - b)
  const usMs = (us: number) => (us / 1000).toFixed(2)
  log(`\n--- throughput (firehose hot set = ${HOTSET} of ${SHAPES} shapes) ---`)
  log(`firehose writes:   ${firehoseWrites} in ${(loadMs / 1000).toFixed(1)}s = ${Math.round(firehoseWrites / (loadMs / 1000))}/s`)
  log(`engine envelopes:  ${metrics.counters.envelopes_processed} processed, ${metrics.counters.shape_appends} shape appends`)
  log(`\n--- end-to-end write->shape latency (subscribed shapes, ${latencies.length} probes) ---`)
  log(`p50=${pct(latencies, 0.5).toFixed(1)}ms  p99=${pct(latencies, 0.99).toFixed(1)}ms  max=${pct(latencies, 1).toFixed(1)}ms`)
  log(`\n--- engine stage latencies (from /metrics) ---`)
  log(`process_envelope: p50=${usMs(metrics.process_envelope_us.p50_us)}ms p99=${usMs(metrics.process_envelope_us.p99_us)}ms max=${usMs(metrics.process_envelope_us.max_us)}ms`)
  log(`family_step:      p50=${usMs(metrics.family_step_us.p50_us)}ms p99=${usMs(metrics.family_step_us.p99_us)}ms max=${usMs(metrics.family_step_us.max_us)}ms`)
  log(`append:           p50=${usMs(metrics.append_us.p50_us)}ms p99=${usMs(metrics.append_us.p99_us)}ms max=${usMs(metrics.append_us.max_us)}ms`)
  log(`\n--- resources (engine process) ---`)
  log(`RSS: reg=${afterReg.rssMb.toFixed(0)}MB  load start=${rssTrace[0]?.toFixed(0)}MB  peak=${Math.max(...rssTrace).toFixed(0)}MB  end=${rssTrace.at(-1)?.toFixed(0)}MB`)
  log(`CPU: avg=${(cpuTrace.reduce((a, b) => a + b, 0) / Math.max(1, cpuTrace.length)).toFixed(0)}%  peak=${Math.max(...cpuTrace).toFixed(0)}%`)

  await client.close().catch(() => {})
  await api.close().catch(() => {})
  engine.proc.kill('SIGKILL')
  await ds.stop().catch(() => {})
  process.exit(0)
}

/** Run `fn(i)` for i in [0,n) with at most `conc` in flight. */
async function runPool(n: number, conc: number, fn: (i: number) => Promise<void>): Promise<void> {
  let next = 0
  await Promise.all(
    Array.from({ length: Math.min(conc, n) }, async () => {
      while (true) {
        const i = next++
        if (i >= n) break
        await fn(i)
      }
    }),
  )
}

main().catch((e) => {
  console.error(e)
  // Best-effort: don't leave the spawned engine orphaned on error.
  try {
    enginePidForCleanup && process.kill(enginePidForCleanup, 'SIGKILL')
  } catch {
    /* already gone */
  }
  process.exit(1)
})
