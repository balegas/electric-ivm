// Standalone durable-streams server for the Docker stack: the log every layer meets at
// (`table/<name>` in from the engine's ingestor, `shape/<id>` out to clients). File-backed under
// DS_DATA_DIR so streams survive a container restart (compose mounts a volume there); set
// DS_MEMORY=1 for the in-memory mode (no fsync-per-append ceiling, no persistence).
import { mkdirSync } from 'node:fs'

const port = Number(process.env.DS_PORT ?? 8791)
const host = process.env.BIND_HOST ?? '0.0.0.0'
const dataDir = process.env.DS_DATA_DIR ?? '/data'
const inMemory = process.env.DS_MEMORY === '1'

const { DurableStreamTestServer } = await import('@durable-streams/server')

if (!inMemory) mkdirSync(dataDir, { recursive: true })
const server = new DurableStreamTestServer(inMemory ? { port, host } : { port, host, dataDir })
const url = await server.start()
console.log(`durable-streams listening on ${url}${inMemory ? ' (in-memory)' : ` (data: ${dataDir})`}`)

process.on('SIGTERM', async () => {
  await server.stop()
  process.exit(0)
})
