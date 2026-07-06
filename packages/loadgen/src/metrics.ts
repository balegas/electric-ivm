// Metrics sampling for the load generator: engine RSS + CPU, Postgres + durable-streams disk, engine
// counters/cardinalities, and the loadgen's own op rates. Sampled on an interval into a CSV; a summary
// is printed at teardown. No UI — just numbers for observing memory/CPU/disk vs workload size.

import { execFile } from 'node:child_process'
import { appendFileSync, existsSync, writeFileSync } from 'node:fs'
import { promisify } from 'node:util'

const execFileP = promisify(execFile)

export interface OpCounters {
  users: number
  reads: number // subscriptions opened (browse/board/aggregate) over the run
  writes: number // mutations applied to Postgres over the run
  openSubs: number // currently-open client subscriptions (≈ read connections)
}

export interface Sample {
  t: number // seconds since start
  users: number
  openSubs: number
  reads: number
  writes: number
  writesPerSec: number
  rssMb: number
  cpuCores: number // engine CPU as fraction of one core (delta CPU-time / interval)
  pgMb: number
  dsMb: number
  envelopes: number
  appends: number
  shapes: number
  familyCircuits: number
  subqueryNodes: number
  standalone: number
  appendP99Ms: number
}

/** Parse `ps -o time=` cumulative CPU time (`[DD-]HH:MM:SS[.ff]` / `MM:SS.ff`) into seconds. */
function parseCpuTime(s: string): number | null {
  s = s.trim()
  if (!s) return null
  let days = 0
  const dash = s.indexOf('-')
  if (dash >= 0) {
    days = Number(s.slice(0, dash))
    s = s.slice(dash + 1)
  }
  const parts = s.split(':').map(Number)
  if (parts.some((n) => Number.isNaN(n))) return null
  let sec = 0
  for (const p of parts) sec = sec * 60 + p
  return days * 86400 + sec
}

async function cpuTimeSeconds(pid: number): Promise<number | null> {
  try {
    const { stdout } = await execFileP('ps', ['-o', 'time=', '-p', String(pid)])
    return parseCpuTime(stdout)
  } catch {
    return null
  }
}

async function dirBytes(dir?: string): Promise<number> {
  if (!dir || !existsSync(dir)) return 0
  try {
    const { stdout } = await execFileP('du', ['-sk', dir])
    return parseInt(stdout.trim().split(/\s+/)[0]!, 10) * 1024
  } catch {
    return 0
  }
}

const MB = 1024 * 1024

export interface SamplerOptions {
  engineUrl: string
  enginePid: number
  /** Run a scalar SQL query (for `pg_database_size`). */
  pgScalar: (sql: string) => Promise<number>
  dsDir?: string
  csvPath: string
  counters: () => OpCounters
}

export class MetricsSampler {
  private timer?: ReturnType<typeof setInterval>
  private started = Date.now()
  private prevCpu: number | null = null
  private prevWall = Date.now()
  private prevWrites = 0
  private prevWritesWall = Date.now()
  readonly samples: Sample[] = []

  constructor(private readonly o: SamplerOptions) {
    writeFileSync(
      o.csvPath,
      't_s,users,open_subs,reads,writes,writes_per_s,rss_mb,cpu_cores,pg_mb,ds_mb,envelopes,appends,shapes,family_circuits,subquery_nodes,standalone,append_p99_ms\n',
    )
  }

  private async fetchJson(path: string): Promise<any> {
    try {
      const r = await fetch(`${this.o.engineUrl}${path}`)
      return r.ok ? await r.json() : {}
    } catch {
      return {}
    }
  }

  async sample(): Promise<Sample> {
    const now = Date.now()
    const [mem, met, cpuNow, dsMb, pgBytes] = await Promise.all([
      this.fetchJson('/memory'),
      this.fetchJson('/metrics'),
      cpuTimeSeconds(this.o.enginePid),
      dirBytes(this.o.dsDir).then((b) => b / MB),
      this.o.pgScalar('SELECT pg_database_size(current_database())').catch(() => 0),
    ])

    // Engine CPU as cores = Δ(cpu-time) / Δ(wall).
    let cpuCores = 0
    if (cpuNow != null && this.prevCpu != null) {
      const dt = (now - this.prevWall) / 1000
      if (dt > 0) cpuCores = Math.max(0, (cpuNow - this.prevCpu) / dt)
    }
    if (cpuNow != null) {
      this.prevCpu = cpuNow
      this.prevWall = now
    }

    const c = this.o.counters()
    const dw = (now - this.prevWritesWall) / 1000
    const writesPerSec = dw > 0 ? Math.max(0, (c.writes - this.prevWrites) / dw) : 0
    this.prevWrites = c.writes
    this.prevWritesWall = now

    const card = (mem.cardinalities ?? {}) as Record<string, number>
    const s: Sample = {
      t: Math.round((now - this.started) / 1000),
      users: c.users,
      openSubs: c.openSubs,
      reads: c.reads,
      writes: c.writes,
      writesPerSec: Math.round(writesPerSec),
      rssMb: Math.round((mem.process?.rss_bytes ?? 0) / MB),
      cpuCores: Number(cpuCores.toFixed(2)),
      pgMb: Math.round(pgBytes / MB),
      dsMb: Math.round(dsMb),
      envelopes: met.counters?.envelopes_processed ?? 0,
      appends: met.counters?.shape_appends ?? 0,
      shapes: card.shapes ?? 0,
      familyCircuits: card.families ?? 0,
      subqueryNodes: card.subquery_nodes ?? 0,
      standalone: card.standalone ?? 0,
      appendP99Ms: Number(((met.append_us?.p99_us ?? 0) / 1000).toFixed(2)),
    }
    this.samples.push(s)
    appendFileSync(
      this.o.csvPath,
      [
        s.t,
        s.users,
        s.openSubs,
        s.reads,
        s.writes,
        s.writesPerSec,
        s.rssMb,
        s.cpuCores,
        s.pgMb,
        s.dsMb,
        s.envelopes,
        s.appends,
        s.shapes,
        s.familyCircuits,
        s.subqueryNodes,
        s.standalone,
        s.appendP99Ms,
      ].join(',') + '\n',
    )
    return s
  }

  start(intervalMs: number): void {
    this.timer = setInterval(() => void this.sample(), intervalMs)
  }
  stop(): void {
    if (this.timer) clearInterval(this.timer)
  }

  /** A one-line-per-metric summary over the run (peaks + finals). */
  summary(): Record<string, number> {
    const peak = (k: keyof Sample) => this.samples.reduce((m, s) => Math.max(m, s[k] as number), 0)
    const last = this.samples.at(-1)
    return {
      samples: this.samples.length,
      users: last?.users ?? 0,
      peak_open_subs: peak('openSubs'),
      total_reads: last?.reads ?? 0,
      total_writes: last?.writes ?? 0,
      peak_writes_per_s: peak('writesPerSec'),
      peak_rss_mb: peak('rssMb'),
      peak_cpu_cores: peak('cpuCores'),
      final_pg_mb: last?.pgMb ?? 0,
      peak_ds_mb: peak('dsMb'),
      final_engine_shapes: last?.shapes ?? 0,
      final_subquery_nodes: last?.subqueryNodes ?? 0,
      total_envelopes: last?.envelopes ?? 0,
      total_appends: last?.appends ?? 0,
    }
  }
}
