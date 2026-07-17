// Electric benchmarking-fleet runner against electric-circuits. For each benchmark it: boots our stack
// (durable-streams + engine + /v1/shape adapter on an ephemeral Postgres) via the launcher, seeds the
// benchmark's schema at scale, runs the (unmodified) ElectricSQL byo_electric benchmark `.exs` script
// pointed at our adapter, collects the statsd/UDP telemetry the script emits, and reports latency
// percentiles + throughput. Writes a markdown report.
//
//   pnpm bench:fleet            # from the repo root — clones the fleet repo itself if needed
//
// Env: BENCH_SCALE (workload multiplier, default 1), BENCH_ONLY (comma list of benchmark names),
//      FLEET_DIR (path to a clone of electric-sql/benchmarking-fleet; auto-cloned from FLEET_REPO
//      when absent), FLEET_REPO (default https://github.com/electric-sql/benchmarking-fleet),
//      BENCH_OUT (report path).
//
// External-target mode — run the same fleet benchmarks against ANY Electric-compatible server
// (e.g. a stock `electricsql/electric` container) instead of booting our stack:
//
//   EXTERNAL_ELECTRIC_URL=http://localhost:3000 \
//   EXTERNAL_DATABASE_URL=postgresql://postgres:password@localhost:54321/electric \
//   pnpm bench:fleet
//
// The runner then seeds the benchmark schemas into EXTERNAL_DATABASE_URL (dropping/recreating the
// benchmark tables) and points the unmodified `.exs` scripts at EXTERNAL_ELECTRIC_URL. Both vars
// are required together. Compare reports by setting a distinct BENCH_OUT per target.

import { type ChildProcess, execFileSync, spawn } from 'node:child_process'
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

const FLEET_REPO = process.env.FLEET_REPO || 'https://github.com/electric-sql/benchmarking-fleet'
const FLEET_DIR =
  process.env.FLEET_DIR ||
  join(repoRoot(), '..', 'benchmarking-fleet')
const BENCH_DIR = join(FLEET_DIR, 'predefined_benchmarks', 'benchmarks')

/** Clone electric-sql/benchmarking-fleet on first run (shallow), so `pnpm bench:fleet` is one command. */
function ensureFleet(): void {
  if (existsSync(BENCH_DIR)) return
  if (existsSync(FLEET_DIR)) {
    throw new Error(`${FLEET_DIR} exists but has no predefined_benchmarks/benchmarks — not a benchmarking-fleet clone?`)
  }
  console.log(`cloning ${FLEET_REPO} -> ${FLEET_DIR}`)
  execFileSync('git', ['clone', '--depth', '1', FLEET_REPO, FLEET_DIR], { stdio: 'inherit' })
}
const EXTERNAL_URL = process.env.EXTERNAL_ELECTRIC_URL
const EXTERNAL_DB = process.env.EXTERNAL_DATABASE_URL
if ((EXTERNAL_URL ? 1 : 0) + (EXTERNAL_DB ? 1 : 0) === 1) {
  throw new Error('EXTERNAL_ELECTRIC_URL and EXTERNAL_DATABASE_URL must be set together')
}
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
  stop(): Promise<void>
  waitAdapter(): Promise<string>
}
async function bootStack(waitTable: string, tables: string): Promise<Stack> {
  // ADAPTER_LIVE_TIMEOUT_MS=20000: the fleet scripts expect Electric-like ~20s live long-polls (a
  // live request must outlast the gap before the benchmark's write, not 204 at the ds poll timeout).
  // detached: the child gets its own process group so stop() can signal bash+pnpm+tsx+engine together
  // (killing just pnpm orphans the tsx launcher, its Rust engine, and an ephemeral Postgres per bench).
  const proc = spawn(
    'bash',
    ['-lc', `cd ${repoRoot()} && exec env ADAPTER_WAIT_TABLE=${waitTable} ADAPTER_PG_TABLES=${tables} ADAPTER_LONGPOLL_MS=1000 ADAPTER_LIVE_TIMEOUT_MS=20000 pnpm --filter @electric-circuits/bench exec tsx src/electric-adapter.ts`],
    { stdio: ['ignore', 'pipe', 'inherit'], detached: true },
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
  // SIGTERM the whole process group (negative pid) so the tsx adapter runs its own shutdown handler
  // (kills its engine child and hands PG teardown — pg_ctl stop + tmpdir removal — to a detached
  // helper that survives it), wait for the group to drain, then SIGKILL the group as a backstop.
  const signalGroup = (sig: NodeJS.Signals | 0) => {
    try {
      process.kill(-proc.pid!, sig)
      return true
    } catch {
      return false // group already gone
    }
  }
  return {
    pgUrl: pg,
    proc,
    stop: async () => {
      signalGroup('SIGTERM')
      const deadline = Date.now() + 8000
      while (signalGroup(0) && Date.now() < deadline) await sleep(100)
      signalGroup('SIGKILL')
    },
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
  await c.query(`DROP TABLE IF EXISTS public.users CASCADE`) // external targets reuse a database
  await c.query(`CREATE TABLE public.users (id UUID PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL)`)
  await c.query(`INSERT INTO public.users SELECT gen_random_uuid(), 'u'||g, 'u'||g||'@x.com' FROM generate_series(1,$1) g`, [n])
  return `${n.toLocaleString()} users`
}
const parentChildSeed = (groups: number, perGroup: number, childrenPer: number) => async (c: pgpkg.Client) => {
  await c.query(`DROP TABLE IF EXISTS public.child CASCADE`)
  await c.query(`DROP TABLE IF EXISTS public.parent CASCADE`)
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
  if (EXTERNAL_URL && EXTERNAL_DB) {
    // External-target mode: nothing to boot or tear down; the target owns its own lifecycle.
    const stack: Stack = {
      pgUrl: EXTERNAL_DB,
      proc: undefined as unknown as ChildProcess,
      stop: async () => {},
      waitAdapter: async () => EXTERNAL_URL,
    }
    return await runBenchOnStack(b, sink, stack)
  }
  const stack = await bootStack(b.waitTable, b.tables)
  try {
    return await runBenchOnStack(b, sink, stack)
  } finally {
    await stack.stop() // SIGTERM group + 2s for the adapter's own PG/engine teardown + SIGKILL backstop
  }
}

async function runBenchOnStack(
  b: Bench,
  sink: Sink,
  stack: Stack,
): Promise<{ label: string; specStr: string; durationMs: number; byMetric: Record<string, ReturnType<typeof stats>> }> {
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
    TEST_DATABASE_URL: b.needsDb ? (EXTERNAL_DB ?? dbUrl(stack.pgUrl)) : 'unused',
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
  return { label, specStr: JSON.stringify(spec), durationMs, byMetric }
}

async function main() {
  ensureFleet()
  if (!existsSync(BENCH_DIR)) {
    console.error(`benchmarking-fleet not found at ${FLEET_DIR}. Set FLEET_DIR=/path/to/benchmarking-fleet`)
    process.exit(1)
  }
  if (!EXTERNAL_URL && !existsSync(join(repoRoot(), 'target', 'release', 'electric-circuits-engine'))) {
    console.log('building the release engine (cargo build --release -p electric-circuits-engine)…')
    execFileSync('cargo', ['build', '--release', '-p', 'electric-circuits-engine'], {
      cwd: repoRoot(),
      stdio: 'inherit',
    })
  }
  const sink = await startSink(STATSD_PORT)
  const benches = specEnvFilter(process.env.BENCH_ONLY)
  const lines: string[] = []
  const log = (s = '') => {
    lines.push(s)
    process.stdout.write(`${s}\n`)
  }
  log(`# Electric benchmarking-fleet — results vs ${EXTERNAL_URL ? `external Electric at ${EXTERNAL_URL}` : 'electric-circuits'}`)
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
