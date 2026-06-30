// Electric benchmarking-fleet runner against electric-lite. For each benchmark it: boots our stack
// (durable-streams + engine + /v1/shape adapter on an ephemeral Postgres) via the launcher, seeds the
// benchmark's schema at scale, runs the (unmodified) ElectricSQL byo_electric benchmark `.exs` script
// pointed at our adapter, collects the statsd/UDP telemetry the script emits, and reports latency
// percentiles + throughput. Writes a markdown report.
//
//   FLEET_DIR=/path/to/benchmarking-fleet pnpm --filter @electric-lite/bench exec tsx src/electric-bench-runner.ts
//
// Env: BENCH_SCALE (workload multiplier, default 1), BENCH_ONLY (comma list of benchmark names),
//      FLEET_DIR (path to a clone of electric-sql/benchmarking-fleet), BENCH_OUT (report path).

import { type ChildProcess, spawn } from 'node:child_process'
import dgram from 'node:dgram'
import { existsSync, mkdirSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import pgpkg from 'pg'

const here = dirname(fileURLToPath(import.meta.url))
function repoRoot(): string {
  let d = here
  for (let i = 0; i < 8; i++) {
    if (existsSync(join(d, 'Cargo.toml'))) return d
    d = dirname(d)
  }
  throw new Error('repo root not found')
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))
const num = (k: string, d: number) => (process.env[k] ? Number(process.env[k]) : d)

const FLEET_DIR =
  process.env.FLEET_DIR ||
  join(repoRoot(), '..', 'benchmarking-fleet')
const BENCH_DIR = join(FLEET_DIR, 'predefined_benchmarks', 'benchmarks')
const SCALE = num('BENCH_SCALE', 1)
const STATSD_PORT = num('BENCH_STATSD_PORT', 8125)
const OUT = process.env.BENCH_OUT || join(repoRoot(), 'docs', 'bench', 'electric-fleet-results.md')

// ---- statsd UDP sink: aggregate every "name:value|type|#tags" packet by metric name --------------
interface Sink {
  metrics: Map<string, number[]>
  reset(): void
  close(): void
}
function startSink(port: number): Promise<Sink> {
  const metrics = new Map<string, number[]>()
  const sock = dgram.createSocket('udp4')
  sock.on('message', (msg) => {
    for (const line of msg.toString().split('\n')) {
      const head = line.split('|')[0]
      const ci = head?.lastIndexOf(':')
      if (!head || ci === undefined || ci < 0) continue
      const name = head.slice(0, ci)
      const val = Number(head.slice(ci + 1))
      if (!name || Number.isNaN(val)) continue
      const arr = metrics.get(name)
      if (arr) arr.push(val)
      else metrics.set(name, [val])
    }
  })
  return new Promise((resolve) => {
    sock.bind(port, '127.0.0.1', () =>
      resolve({ metrics, reset: () => metrics.clear(), close: () => sock.close() }),
    )
  })
}

function pct(sorted: number[], q: number): number {
  if (!sorted.length) return 0
  return sorted[Math.min(sorted.length - 1, Math.max(0, Math.ceil(q * sorted.length) - 1))]!
}
function stats(values: number[]) {
  const s = [...values].sort((a, b) => a - b)
  const sum = s.reduce((a, b) => a + b, 0)
  return {
    count: s.length,
    min: s[0] ?? 0,
    p50: pct(s, 0.5),
    p95: pct(s, 0.95),
    p99: pct(s, 0.99),
    max: s.at(-1) ?? 0,
    mean: s.length ? Math.round(sum / s.length) : 0,
  }
}

// ---- our stack (launcher in two-phase mode) -------------------------------------------------------
interface Stack {
  pgUrl: string
  proc: ChildProcess
  stop(): void
  waitAdapter(): Promise<string>
}
async function bootStack(waitTable: string, tables: string): Promise<Stack> {
  const proc = spawn(
    'bash',
    ['-lc', `cd ${repoRoot()} && exec env ADAPTER_WAIT_TABLE=${waitTable} ADAPTER_PG_TABLES=${tables} ADAPTER_LONGPOLL_MS=1000 pnpm --filter @electric-lite/bench exec tsx src/electric-adapter.ts`],
    { stdio: ['ignore', 'pipe', 'inherit'] },
  )
  // One persistent handler accumulates all output; waiters poll the parsed values (no lost lines).
  let buf = ''
  let pgUrl = ''
  let adapterUrl = ''
  let exited: number | null = null
  proc.stdout!.on('data', (d: Buffer) => {
    buf += d.toString()
    pgUrl = buf.match(/ADAPTER_PG (\S+)/)?.[1] ?? pgUrl
    adapterUrl = buf.match(/ADAPTER_LISTENING (\S+)/)?.[1] ?? adapterUrl
  })
  proc.on('exit', (c) => (exited = c ?? -1))
  const waitFor = (get: () => string, what: string) =>
    new Promise<string>((resolve, reject) => {
      const deadline = Date.now() + 120000
      const iv = setInterval(() => {
        const v = get()
        if (v) {
          clearInterval(iv)
          resolve(v)
        } else if (exited !== null) {
          clearInterval(iv)
          reject(new Error(`launcher exited (${exited}) before ${what}`))
        } else if (Date.now() > deadline) {
          clearInterval(iv)
          reject(new Error(`launcher: timed out waiting for ${what}`))
        }
      }, 50)
    })
  const pg = await waitFor(() => pgUrl, 'ADAPTER_PG')
  return {
    pgUrl: pg,
    proc,
    stop: () => proc.kill('SIGKILL'),
    waitAdapter: () => waitFor(() => adapterUrl, 'ADAPTER_LISTENING'),
  }
}

// ---- benchmark definitions ------------------------------------------------------------------------
interface Bench {
  name: string
  tables: string
  waitTable: string
  // seed the schema + data (scaled); returns a human label of the workload.
  seed(client: pgpkg.Client, scale: number): Promise<string>
  // TEST_SPEC for the .exs, scaled.
  spec(scale: number): object
  needsDb?: boolean // the script does Postgrex writes -> pass TEST_DATABASE_URL
}

const usersSeed = (n: number) => async (c: pgpkg.Client) => {
  await c.query(`CREATE TABLE public.users (id UUID PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL)`)
  await c.query(`INSERT INTO public.users SELECT gen_random_uuid(), 'u'||g, 'u'||g||'@x.com' FROM generate_series(1,$1) g`, [n])
  return `${n.toLocaleString()} users`
}
const parentChildSeed = (groups: number, perGroup: number, childrenPer: number) => async (c: pgpkg.Client) => {
  await c.query(`CREATE TABLE public.parent (id UUID PRIMARY KEY, group_id INT NOT NULL, name TEXT)`)
  await c.query(`CREATE TABLE public.child (id UUID PRIMARY KEY, parent_id UUID NOT NULL REFERENCES public.parent(id), name TEXT)`)
  const parents = groups * perGroup
  await c.query(`INSERT INTO public.parent SELECT gen_random_uuid(), (g/$2)+1, 'p'||g FROM generate_series(0,$1) g`, [parents - 1, perGroup])
  await c.query(`INSERT INTO public.child SELECT gen_random_uuid(), p.id, 'c' FROM public.parent p, generate_series(1,$1) s`, [childrenPer])
  return `${groups.toLocaleString()} groups, ${parents.toLocaleString()} parents, ${(parents * childrenPer).toLocaleString()} children`
}

const BENCHES: Bench[] = [
  {
    name: 'concurrent_shape_creation',
    tables: 'users',
    waitTable: 'users',
    seed: usersSeed(2_000),
    spec: (s) => ({ concurrent: 500 * s }),
  },
  {
    name: 'concurrent_shape_creation_with_subqueries',
    tables: 'parent,child',
    waitTable: 'child',
    seed: parentChildSeed(300 * SCALE, 2, 5),
    spec: (s) => ({ parent_group_count: 300 * s }),
  },
  {
    name: 'many_shapes_one_client_latency',
    tables: 'users',
    waitTable: 'users',
    seed: usersSeed(2_000),
    spec: (s) => ({ shape_count: 500 * s, tx_row_count: 1 }),
    needsDb: true,
  },
  {
    name: 'diverse_shape_fanout',
    tables: 'users',
    waitTable: 'users',
    seed: usersSeed(2_000),
    spec: (s) => ({ concurrent: 200 * s, tx_row_count: 5 }),
    needsDb: true,
  },
  {
    name: 'write_fanout',
    tables: 'users',
    waitTable: 'users',
    seed: usersSeed(2_000),
    spec: (s) => ({ concurrent: 200 * s, tx_row_count: 5 }),
    needsDb: true,
  },
]

// Benchmarks that use Postgrex want an auth-form URL; our trust PG accepts any password.
function dbUrl(pgUrl: string): string {
  return `postgresql://postgres:postgres@${pgUrl.replace(/^postgres(ql)?:\/\/postgres@/, '')}`
}

function specEnvFilter(only?: string) {
  if (!only) return BENCHES
  const set = new Set(only.split(',').map((x) => x.trim()))
  return BENCHES.filter((b) => set.has(b.name))
}

async function runBench(b: Bench, sink: Sink): Promise<{ label: string; specStr: string; durationMs: number; byMetric: Record<string, ReturnType<typeof stats>> }> {
  process.stdout.write(`\n=== ${b.name} (scale ${SCALE}) ===\n`)
  const stack = await bootStack(b.waitTable, b.tables)
  const client = new pgpkg.Client({ connectionString: stack.pgUrl })
  await client.connect()
  const label = await b.seed(client, SCALE)
  process.stdout.write(`  seeded: ${label}\n`)
  await client.end()
  const adapterUrl = await stack.waitAdapter()
  const hostPort = adapterUrl.replace(/^https?:\/\//, '')

  sink.reset()
  const spec = b.spec(SCALE)
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    TEST_NAME: b.name,
    TELEMETRY_SERVER: `127.0.0.1:${STATSD_PORT}`,
    ELECTRIC_SERVER: hostPort,
    TEST_SPEC: JSON.stringify(spec),
    TEST_DATABASE_URL: b.needsDb ? dbUrl(stack.pgUrl) : 'unused',
  }
  process.stdout.write(`  running elixir ${b.name}.exs  spec=${JSON.stringify(spec)}\n`)
  const t0 = Date.now()
  await new Promise<void>((resolve) => {
    const p = spawn('elixir', [join(BENCH_DIR, `${b.name}.exs`)], { env, stdio: ['ignore', 'pipe', 'pipe'] })
    let got = 0
    p.stdout!.on('data', (d) => {
      got += (d.toString().match(/got \d+ bytes/g) || []).length
    })
    p.stderr!.on('data', () => {})
    p.on('exit', () => {
      process.stdout.write(`  elixir done (${got} shape responses)\n`)
      resolve()
    })
  })
  const durationMs = Date.now() - t0
  await sleep(500) // let trailing UDP packets land

  const byMetric: Record<string, ReturnType<typeof stats>> = {}
  for (const [name, vals] of sink.metrics) byMetric[name] = stats(vals)
  stack.stop()
  await sleep(1500) // let the ephemeral PG/engine tear down before the next boot
  return { label, specStr: JSON.stringify(spec), durationMs, byMetric }
}

async function main() {
  if (!existsSync(BENCH_DIR)) {
    console.error(`benchmarking-fleet not found at ${FLEET_DIR}. Set FLEET_DIR=/path/to/benchmarking-fleet`)
    process.exit(1)
  }
  if (!existsSync(join(repoRoot(), 'target', 'release', 'electric-lite-engine'))) {
    console.error('build first: cargo build --release -p electric-lite-engine')
    process.exit(1)
  }
  const sink = await startSink(STATSD_PORT)
  const benches = specEnvFilter(process.env.BENCH_ONLY)
  const lines: string[] = []
  const log = (s = '') => {
    lines.push(s)
    process.stdout.write(`${s}\n`)
  }
  log(`# Electric benchmarking-fleet — results vs electric-lite`)
  log('')
  log(`Generated ${new Date().toISOString()} on ${process.platform}/${process.arch}. Scale ${SCALE}.`)
  log(`Each row runs the unmodified ElectricSQL \`byo_electric\` benchmark \`.exs\` against our \`/v1/shape\``)
  log(`adapter; latency is the per-shape fetch duration the benchmark emits over statsd (ms).`)
  log('')
  log(`| benchmark | workload | shapes | wall (s) | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) |`)
  log(`|-----------|----------|-------:|---------:|---------:|---------:|---------:|---------:|`)

  for (const b of benches) {
    try {
      const r = await runBench(b, sink)
      // pick the primary duration metric (the benchmarks emit *.duration counters)
      const durName = Object.keys(r.byMetric).find((n) => /duration/.test(n)) || Object.keys(r.byMetric)[0]
      const st = durName ? r.byMetric[durName]! : stats([])
      log(`| ${b.name} | ${r.label} | ${st.count} | ${(r.durationMs / 1000).toFixed(1)} | ${st.p50} | ${st.p95} | ${st.p99} | ${st.max} |`)
    } catch (e) {
      log(`| ${b.name} | ERROR | - | - | - | - | - | ${String((e as Error).message).slice(0, 40)} |`)
      process.stderr.write(`${b.name} failed: ${(e as Error).stack}\n`)
    }
  }
  log('')
  sink.close()
  mkdirSync(dirname(OUT), { recursive: true })
  writeFileSync(OUT, lines.join('\n'))
  process.stdout.write(`\nwrote ${OUT}\n`)
  process.exit(0)
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})
