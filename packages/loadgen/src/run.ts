// Load-generator entrypoint. Modes:
//   all    (default) boot infra + seed + run USERS simulated users + sample metrics + report + teardown
//   infra            boot infra + seed, print URLs, sample server metrics until SIGINT (for docker clients)
//   client           connect to an existing infra (ENGINE_URL/API_URL/DS_URL/PG_URL) + run users (no infra)
//
// Reads go through @electric-ivm/client (one shared client; each user opens its own subscriptions).
// Writes go through Postgres (a shared, bounded write pool). Observe engine memory/CPU + Postgres /
// durable-streams disk in the emitted CSV + the printed summary, across USERS / SEED_ISSUES.

import { mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'

import { createClient } from '@electric-ivm/client'
import { changeEventToDML, type ChangeEvent } from '@electric-ivm/protocol'
import pgpkg from 'pg'

import { type Config, ensureFdLimit, loadConfig } from './config'
import { bootInfra, type Infra, schema } from './infra'
import { MetricsSampler } from './metrics'
import { type Counters, type PgWrite, SimUser } from './user'

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

async function pgScalarOf(pool: pgpkg.Pool): Promise<(sql: string) => Promise<number>> {
  return async (sql) => {
    const r = await pool.query(sql)
    return Number(Object.values(r.rows[0] ?? { v: 0 })[0])
  }
}

function makeWrite(pool: pgpkg.Pool): PgWrite {
  return async (table, op, pk, row) => {
    const def = schema.tables[table]
    if (!def) throw new Error(`unknown table ${table}`)
    const { text, params } = changeEventToDML(table, def, { op, pk, row } as ChangeEvent)
    await pool.query(text, params as unknown[])
  }
}

async function main() {
  const cfg = loadConfig()
  ensureFdLimit(cfg.users * cfg.feedsPerUser)
  mkdirSync(cfg.outDir, { recursive: true })

  // --- resolve infrastructure: boot locally, or connect to a running one (client mode) ---
  let infra: Infra | undefined
  let engineUrl: string, apiUrl: string, dsUrl: string, pgUrl: string
  let enginePid: number | undefined
  let dsDir: string | undefined
  if (cfg.mode === 'client') {
    // Clients use the API (tRPC) + durable-streams (reads) + Postgres (writes); not the engine directly.
    apiUrl = req('API_URL')
    dsUrl = req('DS_URL')
    pgUrl = req('PG_URL')
    engineUrl = process.env.ENGINE_URL ?? ''
    console.log(`[client] api=${apiUrl} ds=${dsUrl} pg=${pgUrl}`)
  } else {
    console.log(`[${cfg.mode}] booting infra (seed ${cfg.seedIssues} issues)…`)
    infra = await bootInfra(cfg.seedIssues, {
      bindHost: cfg.bindHost,
      dsPort: cfg.dsPort,
      apiPort: cfg.apiPort,
      pgPort: cfg.pgPort,
      dsInMemory: cfg.dsInMemory,
    })
    ;({ engineUrl, apiUrl, dsUrl, pgUrl, enginePid, dsDir } = infra)
    console.log(`  engine=${engineUrl}\n  api=${apiUrl}\n  ds=${dsUrl}\n  pg=${pgUrl}`)
  }

  const pool = new pgpkg.Pool({ connectionString: pgUrl, max: cfg.writePool })

  // --- infra mode: just keep it up + sample server metrics; clients connect from elsewhere ---
  if (cfg.mode === 'infra') {
    // Client containers reach the infra at an external host (e.g. host.docker.internal) + these ports.
    writeFileSync(
      join(cfg.outDir, 'infra.json'),
      JSON.stringify({ engineUrl, apiUrl, dsUrl, pgUrl, ports: infra!.ports, bindHost: cfg.bindHost }, null, 2),
    )
    console.log(`  client URLs (from an external host H):  API=http://H:${infra!.ports.api}  DS=http://H:${infra!.ports.ds}  PG=postgres://postgres@H:${infra!.ports.pg}/postgres`)
    const state = { users: 0, reads: 0, writes: 0, openSubs: 0 }
    const sampler = new MetricsSampler({
      engineUrl,
      enginePid: enginePid!,
      pgScalar: await pgScalarOf(pool),
      dsDir,
      csvPath: join(cfg.outDir, `metrics-infra.csv`),
      counters: () => state,
    })
    sampler.start(cfg.sampleMs)
    console.log('infra up; sampling server metrics. Point clients at the URLs above. Ctrl-C to stop.')
    const stop = async () => {
      sampler.stop()
      await pool.end().catch(() => {})
      await infra!.shutdown()
      process.exit(0)
    }
    process.on('SIGINT', stop)
    process.on('SIGTERM', stop)
    await new Promise(() => {}) // run forever
    return
  }

  // --- all / client mode: run the simulated users ---
  const client = createClient({ apiUrl, schema, dsBaseUrl: dsUrl, liveMode: 'long-poll' })
  const write = makeWrite(pool)
  const counters: Counters = { reads: 0, writes: 0, openSubs: 0, errors: 0 }
  const state = { active: 0 }

  let sampler: MetricsSampler | undefined
  if (infra) {
    sampler = new MetricsSampler({
      engineUrl,
      enginePid: enginePid!,
      pgScalar: await pgScalarOf(pool),
      dsDir,
      csvPath: join(cfg.outDir, `metrics-${cfg.label}.csv`),
      counters: () => ({ users: state.active, reads: counters.reads, writes: counters.writes, openSubs: counters.openSubs }),
    })
    sampler.start(cfg.sampleMs)
  }

  const t0 = Date.now()
  const users: SimUser[] = []
  for (let i = 0; i < cfg.users; i++) {
    const u = new SimUser((i % 6) + 1, client, write, cfg, counters)
    users.push(u)
    await u.start()
    state.active++
    if (i % 10 === 0) process.stdout.write(`\r  ramping users: ${state.active}/${cfg.users}   `)
    await sleep(cfg.rampMs)
  }
  console.log(`\n[${cfg.mode}] ${cfg.users} users up (${((Date.now() - t0) / 1000).toFixed(1)}s ramp); running ${cfg.durationS}s…`)

  await sleep(cfg.durationS * 1000)

  // --- teardown ---
  console.log('draining…')
  // Force-exit if teardown hangs (a stuck pg/ds/engine close under heavy load must not wedge a sweep).
  const forceExit = setTimeout(() => {
    console.error('teardown timed out — forcing exit')
    process.exit(0)
  }, 25000)
  forceExit.unref()
  sampler?.stop()
  await Promise.all(users.map((u) => u.stop()))
  // Users already closed their own subscriptions; client.close() only tears down what's still open
  // (every close is one-shot + pruned, so this never double-decrements a shared shape's refcount).
  await client.close().catch(() => {})
  if (sampler) {
    await sampler.sample()
    const s = sampler.summary()
    console.log(`\n=== summary (${cfg.label}: ${cfg.users} users, ${cfg.seedIssues} seed issues, ${cfg.durationS}s) ===`)
    for (const [k, v] of Object.entries(s)) console.log(`  ${k.padEnd(22)} ${v}`)
    writeFileSync(
      join(cfg.outDir, `summary-${cfg.label}.json`),
      JSON.stringify({ label: cfg.label, users: cfg.users, seedIssues: cfg.seedIssues, durationS: cfg.durationS, ...s }, null, 2),
    )
    console.log(`  csv: ${join(cfg.outDir, `metrics-${cfg.label}.csv`)}`)
  } else {
    console.log(`[client] done: reads=${counters.reads} writes=${counters.writes} errors=${counters.errors}`)
  }
  await pool.end().catch(() => {})
  await infra?.shutdown()
  process.exit(0)
}

function req(k: string): string {
  const v = process.env[k]
  if (!v) throw new Error(`client mode requires env ${k}`)
  return v
}

main().catch((e) => {
  console.error('loadgen failed:', e)
  process.exit(1)
})
