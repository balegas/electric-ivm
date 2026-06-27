// Dev entrypoint for the web demo: boots durable-streams + engine + API on fixed ports, defines
// the schema, then starts the Vite dev server (which proxies /api and /ds to them). One command:
//   pnpm demo:web
import { execFileSync, spawn } from 'node:child_process'
import { existsSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { DurableStreamTestServer } from '@durable-streams/server'
import { createApiServer } from '@electric-lite/api'
import { createServer as createViteServer } from 'vite'

import { schema } from './src/schema.js'

const here = dirname(fileURLToPath(import.meta.url))
function repoRoot(): string {
  let d = here
  for (let i = 0; i < 8; i++) {
    if (existsSync(join(d, 'Cargo.toml'))) return d
    d = dirname(d)
  }
  throw new Error('repo root not found')
}

const DS_PORT = 4500
const API_PORT = 4501

const ds = new DurableStreamTestServer({ port: DS_PORT })
const dsUrl = await ds.start()
console.log('durable-streams →', dsUrl)

execFileSync('cargo', ['build', '-p', 'electric-lite-engine'], { cwd: repoRoot(), stdio: 'inherit' })
const engineProc = spawn(join(repoRoot(), 'target', 'debug', 'electric-lite-engine'), [], {
  env: { ...process.env, ELECTRIC_LITE_DS_URL: dsUrl, ELECTRIC_LITE_BIND: '127.0.0.1:0', ELECTRIC_LITE_LOG: 'warn' },
  stdio: ['ignore', 'pipe', 'inherit'],
})
const engineUrl = await new Promise<string>((resolve, reject) => {
  const t = setTimeout(() => reject(new Error('engine did not start')), 20000)
  let buf = ''
  engineProc.stdout!.on('data', (d: Buffer) => {
    buf += d.toString()
    const m = buf.match(/ENGINE_LISTENING (\S+)/)
    if (m) {
      clearTimeout(t)
      resolve(m[1]!)
    }
  })
  engineProc.on('exit', (c) => reject(new Error(`engine exited ${c}`)))
})
console.log('engine        →', engineUrl)

const api = await createApiServer({ dsUrl, engineUrl, port: API_PORT })
await api.core.defineSchema(schema)
console.log('api           →', api.url)

const vite = await createViteServer({ root: here, configFile: join(here, 'vite.config.ts') })
await vite.listen()
console.log('')
vite.printUrls()
console.log('\n👉 Open the Local URL above. Edit todos on the left; watch the live shape on the right.\n')

const shutdown = async () => {
  try {
    await vite.close()
  } catch {}
  try {
    await api.close()
  } catch {}
  engineProc.kill('SIGKILL')
  try {
    await ds.stop()
  } catch {}
  process.exit(0)
}
process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)
