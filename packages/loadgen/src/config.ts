// Load-generator configuration (env-driven) + the file-descriptor / connection guard. Every simulated
// user holds ~FEEDS_PER_USER long-poll connections to durable-streams, so total open connections ≈
// USERS × FEEDS_PER_USER. One machine can open ~16k outbound connections to a single server before
// ephemeral-port exhaustion — beyond that, scale out with more client nodes (Docker).

import { execFileSync } from 'node:child_process'

const num = (k: string, d: number) => (process.env[k] ? Number(process.env[k]) : d)
const str = (k: string, d: string) => process.env[k] ?? d

export interface Config {
  mode: 'all' | 'infra' | 'client'
  users: number
  durationS: number
  seedIssues: number
  /** Mutations per user per second (Poisson-ish think time between writes). */
  writeRate: number
  /** Bounded number of live client subscriptions per user (browse feeds + board + aggregate). */
  feedsPerUser: number
  thinkMinMs: number
  thinkMaxMs: number
  /** Stagger between starting users, so connections don't all open at once (thundering herd). */
  rampMs: number
  /** Postgres write-pool size shared across all users on this node. */
  writePool: number
  /** Use in-memory durable-streams (no per-append fsync) — needed for high concurrency; ds disk = 0. */
  dsInMemory: boolean
  sampleMs: number
  outDir: string
  label: string
  /** For `client` mode: connect to an already-running infra instead of booting one. */
  engineUrl?: string
  apiUrl?: string
  dsUrl?: string
  pgUrl?: string
  /** For `infra` mode: bind API/ds/Postgres so client containers can connect (0.0.0.0), + fixed ports. */
  bindHost: string
  dsPort: number
  apiPort: number
  pgPort: number
}

export function loadConfig(): Config {
  const mode = str('LOADGEN_MODE', 'all') as Config['mode']
  return {
    mode,
    users: num('USERS', 20),
    durationS: num('DURATION_S', 60),
    seedIssues: num('SEED_ISSUES', 2000),
    writeRate: num('WRITE_RATE', 0.25),
    feedsPerUser: num('FEEDS_PER_USER', 4),
    thinkMinMs: num('THINK_MIN_MS', 400),
    thinkMaxMs: num('THINK_MAX_MS', 2500),
    rampMs: num('RAMP_MS', 25),
    writePool: num('WRITE_POOL', 24),
    dsInMemory: process.env.DS_MEMORY === '1',
    sampleMs: num('SAMPLE_MS', 2000),
    outDir: str('OUT_DIR', 'results'),
    label: str('LABEL', `u${num('USERS', 20)}`),
    engineUrl: process.env.ENGINE_URL,
    apiUrl: process.env.API_URL,
    dsUrl: process.env.DS_URL,
    pgUrl: process.env.PG_URL,
    bindHost: str('BIND_HOST', '127.0.0.1'),
    dsPort: num('DS_PORT', 0),
    apiPort: num('API_PORT', 0),
    pgPort: num('PG_PORT', 0),
  }
}

/** Read the OS soft FD limit; warn (and print how to raise it) when it's below what the run needs. */
export function ensureFdLimit(neededConnections: number): number {
  let soft = Number.POSITIVE_INFINITY
  try {
    soft = Number(execFileSync('sh', ['-c', 'ulimit -n']).toString().trim())
  } catch {
    /* ignore */
  }
  // headroom for pg pool + engine/api sockets + logs
  const needed = neededConnections + 128
  if (Number.isFinite(soft) && soft < needed) {
    console.warn(
      `\n⚠  FD soft limit is ${soft}, but this run needs ~${needed} (USERS × FEEDS_PER_USER + overhead).\n` +
        `   Raise it before running:  ulimit -n ${Math.max(needed, 65536)}\n` +
        `   Or reduce USERS / FEEDS_PER_USER, or spread load over more client nodes (docker compose --scale).\n`,
    )
  }
  return soft
}
