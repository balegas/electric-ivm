// Workload-size sweep: runs the `all`-mode loadgen once per USERS size (each boots + tears down its own
// infra), then prints a comparison table of peak memory / CPU / disk vs. workload size. Each run writes
// results/metrics-u<N>.csv (time series) + results/summary-u<N>.json (peaks/finals).
//
//   SWEEP_USERS=5,25,100 SEED_ISSUES=5000 DURATION_S=30 pnpm --filter @electric-circuits/loadgen sweep

import { spawnSync } from 'node:child_process'
import { existsSync, readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const outDir = process.env.OUT_DIR ?? 'results'
const sizes = (process.env.SWEEP_USERS ?? '5,25,100').split(',').map((s) => Number(s.trim())).filter((n) => n > 0)
const durationS = process.env.DURATION_S ?? '30'
const seedIssues = process.env.SEED_ISSUES ?? '5000'

console.log(`sweep: USERS=[${sizes.join(', ')}]  seed=${seedIssues}  duration=${durationS}s each\n`)

for (const users of sizes) {
  const label = `u${users}`
  console.log(`\n———————————————————————— ${label} ————————————————————————`)
  // Bound each run so one stuck boot/teardown can't wedge the whole sweep.
  const budgetMs = (Number(durationS) + 180) * 1000
  const r = spawnSync('npx', ['tsx', join(here, 'run.ts')], {
    stdio: 'inherit',
    timeout: budgetMs,
    killSignal: 'SIGKILL',
    env: { ...process.env, LOADGEN_MODE: 'all', USERS: String(users), LABEL: label, DURATION_S: durationS, SEED_ISSUES: seedIssues, OUT_DIR: outDir },
  })
  if (r.status !== 0) console.log(`  (run ${label} exited ${r.status ?? r.signal}; continuing)`)
}

// --- comparison table ---
const cols = [
  ['users', 'users'],
  ['peak_rss_mb', 'RSS MB'],
  ['peak_cpu_cores', 'CPU cores'],
  ['final_pg_mb', 'PG MB'],
  ['peak_ds_mb', 'ds MB'],
  ['final_engine_shapes', 'shapes'],
  ['final_subquery_nodes', 'sq nodes'],
  ['peak_open_subs', 'open subs'],
  ['total_writes', 'writes'],
  ['peak_writes_per_s', 'w/s'],
  ['total_envelopes', 'envelopes'],
] as const

const rows: Record<string, number>[] = []
for (const users of sizes) {
  const p = join(outDir, `summary-u${users}.json`)
  if (existsSync(p)) rows.push(JSON.parse(readFileSync(p, 'utf8')))
}

if (rows.length) {
  console.log('\n\n=== workload-size sweep: engine memory / CPU / disk ===\n')
  const header = cols.map(([, h]) => h.padStart(10)).join(' ')
  console.log(header)
  console.log('-'.repeat(header.length))
  for (const r of rows) console.log(cols.map(([k]) => String(r[k] ?? '').padStart(10)).join(' '))
  console.log(`\nper-run time series: ${outDir}/metrics-u*.csv`)
}
