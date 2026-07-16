// Scale variant of shape-mem-matrix: memory at 100k shape subscriptions with held live
// long-polls, driven by MULTIPLE CLIENT PROCESSES (one Node process can't realistically model
// thousands of client sessions creating + live-tailing).
//
// Differences from shape-mem-matrix.ts:
//   - creation fan-out across SCALE_CLIENT_PROCS spawned worker processes (plain-JS `node -e`
//     workers fed shape bodies over stdin as JSON lines — the parent keeps the TS workload
//     logic, the workers are dumb POST pools);
//   - after the last milestone, SCALE_LIVE_SUBS live long-polls are HELD OPEN against the
//     durable-streams server (`?live=long-poll`), spread over SCALE_LIVE_PROCS workers, and
//     memory is sampled again with subscriptions active (engine RSS must not care — live
//     serving is the streams server's job);
//   - samples include the durable-streams server's RSS next to the engine's;
//   - ELECTRIC_IVM_FEED_TRACE passes through to the engine (set 0 to drop the feed-relation
//     enumeration copy — the "tracing" duplication — and halve the per-feed memory term).
//
//   pnpm --filter @electric-ivm/bench exec tsx src/shape-mem-scale.ts
//   SCALE_ISSUES=100000 SCALE_PROJECTS=2000 SCALE_USERS=1000,2500,5000,10000 \
//   SCALE_CLIENT_PROCS=4 SCALE_LIVE_SUBS=20000 SCALE_LIVE_PROCS=8 \
//   ELECTRIC_IVM_FEED_TRACE=0 tsx src/shape-mem-scale.ts

import { type ChildProcess, execFile, execFileSync, execSync, spawn } from 'node:child_process'
import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'

const execFileP = promisify(execFile)

import { DurableStreamTestServer } from '@electric-ivm/ds-rust'
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
const numEnv = (k: string, d: number) => (process.env[k] ? Number(process.env[k]) : d)
const listEnv = (k: string, d: number[]) => (process.env[k] ? process.env[k]!.split(',').map(Number) : d)

const ISSUES = numEnv('SCALE_ISSUES', 100000)
const PROJECTS = numEnv('SCALE_PROJECTS', 2000)
const MEMBERSHIPS_PER_USER = numEnv('SCALE_MEMBERSHIPS', 6)
const USER_MILESTONES = listEnv('SCALE_USERS', [1000, 2500, 5000, 10000])
const CLIENT_PROCS = numEnv('SCALE_CLIENT_PROCS', 4)
const CONC_PER_PROC = numEnv('SCALE_CONC_PER_PROC', 12)
const LIVE_SUBS = numEnv('SCALE_LIVE_SUBS', 20000)
const LIVE_PROCS = numEnv('SCALE_LIVE_PROCS', 8)
const MATERIALIZED = process.env.SCALE_MATERIALIZED !== '0'
const OUT = process.env.SCALE_OUT ?? join(repoRoot(), 'docs', 'bench', 'shape-memory-scale.md')
// Phase-0 attribution hook (Task 0.2): after the final milestone drains, snapshot /memory (full
// JSON incl. the bytes_* self-accounting fields), `vmmap --summary` (MALLOC region totals) and
// `footprint` (phys footprint breakdown) for the engine process, and dump all three into the
// bench output dir next to the existing raw tables. Zero behavior change when unset.
const ATTRIBUTION = process.env.SCALE_ATTRIBUTION === '1'
const ATTRIBUTION_LABEL =
  process.env.SCALE_ATTRIBUTION_LABEL ?? (process.env.ELECTRIC_IVM_SUBQ_STORAGE_DIR ? 'spill' : 'inmemory')
// Defaults to the `raw/` dir next to wherever SCALE_OUT's markdown table lands; override
// independently so a custom SCALE_OUT (e.g. to avoid clobbering the curated results doc) doesn't
// also relocate the attribution dump.
const ATTRIBUTION_DIR = process.env.SCALE_ATTRIBUTION_DIR ?? join(dirname(OUT), 'raw')
const MAX_USERS = Math.max(...USER_MILESTONES)
const SLOT = 'electric_ivm_shapescale'
const STATUSES = ['backlog', 'todo', 'in_progress', 'done', 'canceled']
const MIB = 1024 * 1024

// --- infra (same shapes as shape-mem-matrix) ---------------------------------------------------

function bootPgSimple(): { pgUrl: string; dir: string; port: number } {
  const dir = mkdtempSync(join(tmpdir(), 'el-scale-pg-'))
  const dataDir = join(dir, 'data')
  execFileSync('initdb', ['-D', dataDir, '-U', 'postgres', '--auth=trust', '--no-sync'], { stdio: 'ignore' })
  const port = 20000 + Math.floor(Math.random() * 20000)
  execFileSync('pg_ctl', ['-D', dataDir, '-o', `-p ${port} -c listen_addresses=127.0.0.1 -c wal_level=logical -c fsync=off -c synchronous_commit=off -c max_wal_senders=8 -c max_replication_slots=8`, '-w', 'start'], { stdio: 'ignore' })
  return { pgUrl: `postgres://postgres@127.0.0.1:${port}/postgres`, dir, port }
}

async function createSchemaAndSeed(client: pgpkg.Client, issues: number): Promise<void> {
  await client.query(`CREATE TABLE users (id BIGINT PRIMARY KEY, username TEXT NOT NULL)`)
  await client.query(`CREATE TABLE projects (id BIGINT PRIMARY KEY, name TEXT NOT NULL)`)
  await client.query(`CREATE TABLE project_members (id BIGINT PRIMARY KEY, project_id BIGINT NOT NULL, user_id BIGINT NOT NULL)`)
  await client.query(`CREATE TABLE issues (id BIGINT PRIMARY KEY, title TEXT NOT NULL, status TEXT NOT NULL, priority TEXT NOT NULL,
    username TEXT NOT NULL, project_id BIGINT NOT NULL, created BIGINT NOT NULL)`)
  await client.query(`CREATE TABLE comments (id BIGINT PRIMARY KEY, issue_id BIGINT NOT NULL, body TEXT NOT NULL, username TEXT NOT NULL, created BIGINT NOT NULL)`)

  const bulk = async (table: string, cols: string[], rows: unknown[][]) => {
    const B = 5000
    for (let i = 0; i < rows.length; i += B) {
      const chunk = rows.slice(i, i + B)
      const params: unknown[] = []
      const tuples = chunk.map((r, j) => `(${r.map((_, k) => `$${j * cols.length + k + 1}`).join(',')})`)
      for (const r of chunk) params.push(...r)
      await client.query(`INSERT INTO ${table} (${cols.join(',')}) VALUES ${tuples.join(',')}`, params)
    }
  }
  await bulk('users', ['id', 'username'], Array.from({ length: MAX_USERS }, (_, i) => [i + 1, `user-${i + 1}`]))
  await bulk('projects', ['id', 'name'], Array.from({ length: PROJECTS }, (_, i) => [i + 1, `project-${i + 1}`]))
  const members: unknown[][] = []
  let mid = 1
  for (let u = 1; u <= MAX_USERS; u++)
    for (let k = 0; k < MEMBERSHIPS_PER_USER; k++) members.push([mid++, ((u * 13 + k * 7) % PROJECTS) + 1, u])
  await bulk('project_members', ['id', 'project_id', 'user_id'], members)
  const issueRows = Array.from({ length: issues }, (_, i) => [
    i + 1, `Issue ${i + 1}`, STATUSES[i % STATUSES.length], 'medium', `user-${(i % MAX_USERS) + 1}`, (i % PROJECTS) + 1, i,
  ])
  await bulk('issues', ['id', 'title', 'status', 'priority', 'username', 'project_id', 'created'], issueRows)
  const nComments = Math.floor(issues / 2)
  await bulk('comments', ['id', 'issue_id', 'body', 'username', 'created'],
    Array.from({ length: nComments }, (_, i) => [i + 1, (i % issues) + 1, `comment ${i}`, `user-${(i % MAX_USERS) + 1}`, i]))
}

async function spawnEngine(dsUrl: string, pgUrl: string): Promise<{ url: string; proc: ChildProcess }> {
  const proc = spawn(join(repoRoot(), 'target', 'release', 'electric-ivm-engine'), [], {
    env: {
      ...process.env, // carries ELECTRIC_IVM_FEED_TRACE through
      ELECTRIC_IVM_DS_URL: dsUrl,
      ELECTRIC_IVM_BIND: '127.0.0.1:0',
      ELECTRIC_IVM_LOG: 'warn',
      ELECTRIC_IVM_PG_URL: pgUrl,
      ELECTRIC_IVM_PG_TABLES: 'issues,projects,users,project_members,comments',
      ELECTRIC_IVM_PG_SLOT: SLOT,
      ELECTRIC_IVM_PG_POLL_MS: '50',
      ELECTRIC_IVM_MAX_SHAPES: String(numEnv('SCALE_MAX_SHAPES', 200000)),
    },
    stdio: ['ignore', 'pipe', 'inherit'],
  })
  const url = await new Promise<string>((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('engine did not start')), 60000)
    let buf = ''
    proc.stdout!.on('data', (d: Buffer) => {
      buf += d.toString()
      const m = buf.match(/ENGINE_LISTENING (\S+)/)
      if (m) { clearTimeout(t); resolve(m[1]!) }
    })
    proc.on('exit', (c) => reject(new Error(`engine exited ${c}`)))
  })
  return { url, proc }
}

function procRssMib(pid: number | undefined): number {
  if (!pid) return 0
  try {
    return Number(execSync(`ps -o rss= -p ${pid}`).toString().trim()) / 1024
  } catch {
    return 0
  }
}

/// macOS "phys footprint" — includes compressed pages, which `ps rss` excludes (idle
/// processes get compressed and their RSS collapses, understating real state).
function footprintMib(pid: number | undefined): number {
  if (!pid) return 0
  try {
    const out = execSync(`/usr/bin/footprint ${pid} 2>/dev/null | head -3`).toString()
    const m = out.match(/Footprint:\s+([\d.]+)\s+(KB|MB|GB)/)
    if (!m) return 0
    const v = Number(m[1])
    return m[2] === 'KB' ? v / 1024 : m[2] === 'GB' ? v * 1024 : v
  } catch {
    return 0
  }
}

async function engineMemory(engineUrl: string): Promise<{ rss: number; card: Record<string, number> }> {
  const r = await fetch(`${engineUrl}/memory`)
  const j = (await r.json()) as { process: { rss_bytes: number }; cardinalities: Record<string, number> }
  return { rss: j.process.rss_bytes, card: j.cardinalities }
}

/// Phase-0 attribution snapshot (`SCALE_ATTRIBUTION=1`): the full `/memory` JSON (incl. the six
/// `bytes_*` self-accounting fields from Task 0.1), `vmmap --summary` (MALLOC region totals — the
/// allocator-owned virtual memory, as opposed to owned-heap bytes we self-account), and
/// `footprint` (macOS phys-footprint breakdown, compression-inclusive). Written next to the
/// existing raw tables so `docs/bench/mem-attribution-100k.md` can be built from them by hand.
async function writeAttributionSnapshot(engineUrl: string, pid: number | undefined, outDir: string, label: string): Promise<void> {
  mkdirSync(outDir, { recursive: true })
  const prefix = join(outDir, `attribution-${label}`)

  let memoryJson = '{}'
  try {
    const r = await fetch(`${engineUrl}/memory`)
    memoryJson = JSON.stringify(await r.json(), null, 2)
  } catch (e) {
    memoryJson = JSON.stringify({ error: String(e) })
  }
  writeFileSync(`${prefix}-memory.json`, memoryJson + '\n')

  const runTool = async (file: string, args: string[]): Promise<string> => {
    if (!pid) return 'no engine pid available\n'
    try {
      const { stdout, stderr } = await execFileP(file, args, { maxBuffer: 64 * MIB })
      return stdout + (stderr ? `\n--- stderr ---\n${stderr}` : '')
    } catch (e) {
      return `${file} ${args.join(' ')} failed: ${String(e)}\n`
    }
  }
  writeFileSync(`${prefix}-vmmap-summary.txt`, await runTool('/usr/bin/vmmap', ['--summary', String(pid)]))
  writeFileSync(`${prefix}-footprint.txt`, await runTool('/usr/bin/footprint', [String(pid)]))

  // Per-circuit dbsp profiler dump (membership + counts circuits, per-operator used bytes) —
  // on-demand debug endpoint, introspection-gated; tolerate absence on older engines.
  let profileJson = '{}'
  try {
    const r = await fetch(`${engineUrl}/debug/dbsp-profile`)
    profileJson = r.ok ? JSON.stringify(await r.json(), null, 2) : JSON.stringify({ error: `HTTP ${r.status}` })
  } catch (e) {
    profileJson = JSON.stringify({ error: String(e) })
  }
  writeFileSync(`${prefix}-dbsp-profile.json`, profileJson + '\n')

  console.log(`  attribution snapshot (${label}) -> ${prefix}-{memory.json,vmmap-summary.txt,footprint.txt,dbsp-profile.json}`)
}

// --- workload -----------------------------------------------------------------------------------

const visibilityWhere = (userId: number) => ({
  col: 'project_id',
  in: { table: 'project_members', project: 'project_id', where: { col: 'user_id', op: 'eq', value: userId } },
})

function shapesForUser(userId: number, issues: number): object[] {
  const out: object[] = []
  const co = !MATERIALIZED
  out.push({ table: 'issues', where: visibilityWhere(userId), changesOnly: co })
  for (const s of STATUSES) out.push({ table: 'issues', where: { col: 'status', op: 'eq', value: s }, changesOnly: co })
  out.push({ table: 'issues', where: { col: 'username', op: 'eq', value: `user-${userId}` }, changesOnly: co })
  for (let k = 0; k < 3; k++)
    out.push({ table: 'comments', where: { col: 'issue_id', op: 'eq', value: ((userId * 7 + k * 101) % issues) + 1 }, changesOnly: co })
  return out
}
const SHAPES_PER_USER = 10

// --- multi-process client drivers ----------------------------------------------------------------

// Creation worker: reads JSON lines (shape bodies) from stdin, POSTs them with a bounded pool,
// prints one stream_url per created shape, exits when stdin closes and the pool drains.
const CREATE_WORKER_JS = `
const CONC = Number(process.env.WORKER_CONC || 12);
const URL0 = process.env.WORKER_ENGINE_URL;
let buf = ''; const queue = []; let done = false; let active = 0; let failed = 0;
async function pump() {
  while (active < CONC && queue.length) {
    const body = queue.shift(); active++;
    fetch(URL0 + '/shapes', { method: 'POST', headers: { 'content-type': 'application/json' }, body })
      .then(async (r) => {
        if (!r.ok) { failed++; console.error('create -> ' + r.status); }
        else { const j = await r.json(); if (j.streamUrl) console.log(j.streamUrl); }
      })
      .catch((e) => { failed++; console.error(String(e)); })
      .finally(() => { active--; pump(); maybeExit(); });
  }
}
function maybeExit() { if (done && !queue.length && !active) process.exit(failed ? 1 : 0); }
process.stdin.on('data', (d) => {
  buf += d.toString();
  let i;
  while ((i = buf.indexOf('\\n')) >= 0) { const line = buf.slice(0, i); buf = buf.slice(i + 1); if (line) queue.push(line); }
  pump();
});
process.stdin.on('end', () => { done = true; pump(); maybeExit(); });
`

// Live worker: reads stream URLs from stdin; for each, holds a long-poll loop open forever.
const LIVE_WORKER_JS = `
let buf = ''; let count = 0;
async function hold(url) {
  let offset = '-1';
  for (;;) {
    try {
      const r = await fetch(url + '?offset=' + encodeURIComponent(offset) + '&live=long-poll');
      const next = r.headers.get('stream-next-offset');
      if (r.body) { for await (const _ of r.body) {} } // drain
      if (next) offset = next;
    } catch { await new Promise((res) => setTimeout(res, 1000)); }
  }
}
process.stdin.on('data', (d) => {
  buf += d.toString();
  let i;
  while ((i = buf.indexOf('\\n')) >= 0) {
    const line = buf.slice(0, i); buf = buf.slice(i + 1);
    if (line) { count++; hold(line); }
  }
});
process.stdin.on('end', () => console.error('live worker holding ' + count + ' subscriptions'));
`

async function runWorkers(
  js: string,
  env: Record<string, string>,
  feeds: string[][],
  waitExit: boolean,
): Promise<{ procs: ChildProcess[]; stdout: string[] }> {
  const procs: ChildProcess[] = []
  const exits: Promise<void>[] = []
  const outs: string[] = []
  for (const feed of feeds) {
    const p = spawn(process.execPath, ['-e', js], { env: { ...process.env, ...env }, stdio: ['pipe', 'pipe', 'inherit'] })
    procs.push(p)
    const idx = outs.push('') - 1
    // Consume stdout LIVE — a full pipe buffer would block the worker.
    p.stdout!.on('data', (d: Buffer) => { outs[idx] += d.toString() })
    for (const line of feed) p.stdin!.write(line + '\n')
    p.stdin!.end()
    if (waitExit) exits.push(new Promise((res, rej) => p.on('exit', (c) => (c ? rej(new Error(`worker exited ${c}`)) : res()))))
  }
  if (waitExit) await Promise.all(exits)
  return { procs, stdout: outs }
}

function chunks<T>(arr: T[], n: number): T[][] {
  const out: T[][] = Array.from({ length: n }, () => [])
  arr.forEach((x, i) => out[i % n]!.push(x))
  return out.filter((c) => c.length)
}

// --- main -----------------------------------------------------------------------------------------

async function main() {
  if (!existsSync(join(repoRoot(), 'target', 'release', 'electric-ivm-engine'))) {
    throw new Error('build first: cargo build --release -p electric-ivm-engine')
  }
  const ds = new DurableStreamTestServer({ port: 0, longPollTimeout: 25000 })
  const dsUrl = await ds.start()
  // The test server holds its child process privately; reach in for RSS sampling, falling
  // back to whoever LISTENs on the ds port (pgrep -f can match unrelated processes).
  const dsPort = new URL(dsUrl).port
  const dsPid = ((ds as unknown as { proc?: { pid?: number } }).proc?.pid) ||
    Number(execSync(`lsof -nP -iTCP:${dsPort} -sTCP:LISTEN -t | head -1`).toString().trim()) || undefined
  const pg = bootPgSimple()
  const client = new pgpkg.Client({ connectionString: pg.pgUrl })
  await client.connect()
  const t0 = Date.now()
  await createSchemaAndSeed(client, ISSUES)
  console.log(`seeded ${ISSUES} issues, ${MAX_USERS} users, ${PROJECTS} projects (${Date.now() - t0}ms)`)

  const engine = await spawnEngine(dsUrl, pg.pgUrl)
  await sleep(1500)

  type Row = { label: string; users: number; requested: number; engineShapes: number; engineRssMib: number; engineFpMib: number; dsRssMib: number; card: Record<string, number> }
  const rows: Row[] = []
  const sample = async (label: string, users: number, requested: number) => {
    const m = await engineMemory(engine.url)
    const row: Row = {
      label, users, requested,
      engineShapes: m.card.shapes ?? 0,
      engineRssMib: m.rss / MIB,
      engineFpMib: footprintMib(engine.proc.pid ?? undefined),
      dsRssMib: procRssMib(dsPid),
      card: m.card,
    }
    rows.push(row)
    console.log(`${label.padEnd(22)} users=${String(users).padStart(5)} requested=${String(requested).padStart(6)} liveShapes=${String(row.engineShapes).padStart(6)} engineRSS=${row.engineRssMib.toFixed(1)}MiB engineFP=${row.engineFpMib.toFixed(1)}MiB dsRSS=${row.dsRssMib.toFixed(1)}MiB sqNodes=${m.card.subquery_nodes} contributors=${m.card.subquery_contributors}`)
  }

  await sample('baseline', 0, 0)

  const streamUrls: string[] = []
  let createdUsers = 0
  let requested = 0
  for (const milestone of USER_MILESTONES) {
    const bodies: string[] = []
    for (; createdUsers < milestone; createdUsers++)
      for (const b of shapesForUser(createdUsers + 1, ISSUES)) bodies.push(JSON.stringify(b))
    requested += bodies.length
    const t = Date.now()
    const { stdout } = await runWorkers(
      CREATE_WORKER_JS,
      { WORKER_ENGINE_URL: engine.url, WORKER_CONC: String(CONC_PER_PROC) },
      chunks(bodies, CLIENT_PROCS),
      true,
    )
    for (const acc of stdout)
      for (const line of acc.split('\n')) if (line.startsWith('http')) streamUrls.push(line.trim())
    console.log(`  milestone ${milestone}: created ${bodies.length} in ${((Date.now() - t) / 1000).toFixed(1)}s across ${CLIENT_PROCS} client procs`)
    await sleep(1500)
    await sample('created', createdUsers, requested)
  }

  // Phase-0 attribution hook (Task 0.2): once the final milestone has drained (creation storm
  // settled), snapshot /memory + vmmap --summary + footprint for the engine process. Additive —
  // no effect on the existing tables/rows when SCALE_ATTRIBUTION is unset.
  if (ATTRIBUTION) {
    await sleep(2000)
    await writeAttributionSnapshot(engine.url, engine.proc.pid ?? undefined, ATTRIBUTION_DIR, ATTRIBUTION_LABEL)
  }

  // Hold live subscriptions — either one step (SCALE_LIVE_SUBS) or a ramp
  // (SCALE_LIVE_RAMP=5000,10000,20000: sample at each level to isolate the cost of LISTENERS
  // from the cost of shapes; unset = single-step behavior).
  const LIVE_RAMP = process.env.SCALE_LIVE_RAMP
    ? process.env.SCALE_LIVE_RAMP.split(',').map(Number)
    : LIVE_SUBS > 0 ? [LIVE_SUBS] : []
  let liveProcs: ChildProcess[] = []
  let held = 0
  for (const level of LIVE_RAMP) {
    if (!streamUrls.length) break
    const targets: string[] = []
    for (let i = held; i < level; i++) targets.push(streamUrls[i % streamUrls.length]!)
    if (targets.length) {
      liveProcs.push(...(await runWorkers(LIVE_WORKER_JS, {}, chunks(targets, LIVE_PROCS), false)).procs)
    }
    held = level
    console.log(`holding ${held} live long-polls…`)
    const psSample = (label: string) => {
      const engineRss = procRssMib(engine.proc.pid ?? undefined)
      const engineFp = footprintMib(engine.proc.pid ?? undefined)
      const dsRss = procRssMib(dsPid)
      const last = rows[rows.length - 1]!
      rows.push({ ...last, label, engineRssMib: engineRss, engineFpMib: engineFp, dsRssMib: dsRss })
      console.log(`${label.padEnd(22)} engineRSS=${engineRss.toFixed(1)}MiB engineFP=${engineFp.toFixed(1)}MiB dsRSS=${dsRss.toFixed(1)}MiB (ps+footprint)`)
    }
    await sleep(15000)
    psSample(`live subs ${held}`)
    if (held === LIVE_RAMP[LIVE_RAMP.length - 1]) {
      await sleep(15000)
      psSample(`live subs ${held} +15s`)
    }
  }
  // Report.
  mkdirSync(dirname(OUT), { recursive: true })
  const md: string[] = []
  md.push(`# Shape-memory at scale — ${ISSUES} issues, ${MAX_USERS} users, ${requested} subscriptions`)
  md.push('')
  md.push(`Config: projects=${PROJECTS}, memberships/user=${MEMBERSHIPS_PER_USER}, shapes/user=${SHAPES_PER_USER}, ` +
    `materialized=${MATERIALIZED}, clientProcs=${CLIENT_PROCS}, liveSubs=${LIVE_SUBS}/${LIVE_PROCS} procs, ` +
    `ELECTRIC_IVM_FEED_TRACE=${process.env.ELECTRIC_IVM_FEED_TRACE ?? '1'}`)
  md.push('')
  md.push('| phase | users | subscriptions | live shapes | engine RSS (MiB) | engine footprint (MiB) | ds RSS (MiB) | sq nodes | contributors |')
  md.push('|---|---:|---:|---:|---:|---:|---:|---:|---:|')
  for (const r of rows) {
    md.push(`| ${r.label} | ${r.users} | ${r.requested} | ${r.engineShapes} | ${r.engineRssMib.toFixed(1)} | ${r.engineFpMib.toFixed(1)} | ${r.dsRssMib.toFixed(1)} | ${r.card.subquery_nodes ?? 0} | ${r.card.subquery_contributors ?? 0} |`)
  }
  writeFileSync(OUT, md.join('\n') + '\n')
  console.log(`wrote ${OUT}`)

  for (const p of liveProcs) p.kill('SIGKILL')
  engine.proc.kill('SIGKILL')
  await client.end()
  try { execFileSync('pg_ctl', ['-D', join(pg.dir, 'data'), '-m', 'immediate', 'stop'], { stdio: 'ignore' }) } catch {}
  rmSync(pg.dir, { recursive: true, force: true })
  await ds.stop()
}

main().then(() => process.exit(0)).catch((e) => { console.error(e); process.exit(1) })
