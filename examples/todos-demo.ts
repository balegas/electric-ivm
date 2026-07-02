// electric-ivm demo: a live "active high-priority todos" shape that reacts to writes.
//
// Run:  pnpm demo            (from the repo root)
//   or:  pnpm --filter @electric-ivm/examples demo
//
// It boots the whole stack in one process for convenience:
//   - a durable-streams server (here the in-process test server; in production you'd run the
//     real `durable-streams-server` binary and just point at its URL),
//   - the Rust engine as a child process,
//   - the tRPC API server,
//   - a client that materializes a shape via stream-db + TanStack DB.

import { execFileSync, spawn } from 'node:child_process'
import { existsSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { DurableStreamTestServer } from '@durable-streams/server'
import { createApiServer } from '@electric-ivm/api'
import { createClient } from '@electric-ivm/client'
import type { Row, Schema } from '@electric-ivm/protocol'

// --- the schema the user defines ---------------------------------------------------------
const schema: Schema = {
  tables: {
    todos: {
      columns: {
        id: { type: 'int' },
        title: { type: 'text' },
        priority: { type: 'int' },
        done: { type: 'bool' },
      },
      primaryKey: 'id',
    },
  },
}

// --- boot the Rust engine (built once via `cargo build`) ---------------------------------
function repoRoot(): string {
  let d = dirname(fileURLToPath(import.meta.url))
  for (let i = 0; i < 8; i++) {
    if (existsSync(join(d, 'Cargo.toml'))) return d
    d = dirname(d)
  }
  throw new Error('repo root not found')
}

async function spawnEngine(dsUrl: string) {
  execFileSync('cargo', ['build', '-p', 'electric-ivm-engine'], { cwd: repoRoot(), stdio: 'inherit' })
  const proc = spawn(join(repoRoot(), 'target', 'debug', 'electric-ivm-engine'), [], {
    env: { ...process.env, ELECTRIC_IVM_DS_URL: dsUrl, ELECTRIC_IVM_BIND: '127.0.0.1:0', ELECTRIC_IVM_LOG: 'warn' },
    stdio: ['ignore', 'pipe', 'inherit'],
  })
  const url = await new Promise<string>((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('engine did not start')), 20000)
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

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))
const COLS = ['id', 'title', 'priority', 'done']
const summarize = (rows: Row[]) =>
  rows
    .map((r) => Object.fromEntries(COLS.map((c) => [c, r[c]])))
    .sort((a, b) => Number(a.id) - Number(b.id))

async function main() {
  console.log('Booting electric-ivm (durable-streams + engine + API + client)...\n')
  const ds = new DurableStreamTestServer({ port: 0 })
  const dsUrl = await ds.start()
  const engine = await spawnEngine(dsUrl)
  const api = await createApiServer({ dsUrl, engineUrl: engine.url })
  const client = createClient({ apiUrl: api.url, schema })
  await client.defineSchema(schema)

  // The shape: a query over one table. Receives every matching change, live.
  const shape = await client.shape({
    table: 'todos',
    where: { and: [{ col: 'done', op: 'eq', value: false }, { col: 'priority', op: 'gte', value: 3 }] },
  })
  console.log('Shape registered:  todos WHERE done = false AND priority >= 3\n')

  // Subscribe to live changes (the headless equivalent of a live query).
  shape.subscribe((changes) => {
    for (const c of changes) console.log(`     ↳ live event: ${c.type} id=${c.key}`)
  })

  async function step(label: string, p: Promise<{ txid: string }>, matches: boolean) {
    const { txid } = await p
    if (matches) await shape.awaitTxId(txid, 5000).catch(() => {})
    else await sleep(350) // a non-matching write produces no shape event
    console.log(`\n${label}`)
    console.log('  shape now:', JSON.stringify(summarize(shape.currentRows())))
  }

  // Write through the schema-derived ingestion API; watch the shape react live.
  await step('insert #1 "Ship electric-ivm" (priority 5)            — MATCHES',
    client.tables.todos.insert({ id: 1, title: 'Ship electric-ivm', priority: 5, done: false }), true)
  await step('insert #2 "Water the plants" (priority 1)              — no match',
    client.tables.todos.insert({ id: 2, title: 'Water the plants', priority: 1, done: false }), false)
  await step('insert #3 "Write the docs" (priority 4)                — MATCHES',
    client.tables.todos.insert({ id: 3, title: 'Write the docs', priority: 4, done: false }), true)
  await step('complete #1 (done = true)                             — LEAVES the shape',
    client.tables.todos.update({ id: 1, title: 'Ship electric-ivm', priority: 5, done: true }), true)
  await step('bump #2 to priority 5                                 — ENTERS the shape',
    client.tables.todos.update({ id: 2, title: 'Water the plants', priority: 5, done: false }), true)
  await step('delete #3                                             — LEAVES the shape',
    client.tables.todos.delete(3), true)

  console.log('\nFinal materialized shape (read live via stream-db + TanStack DB):')
  console.log(JSON.stringify(summarize(shape.currentRows()), null, 2))

  await client.close()
  await api.close()
  engine.proc.kill('SIGKILL')
  await ds.stop()
  process.exit(0)
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})
