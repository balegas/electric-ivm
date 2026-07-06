// Standalone tRPC API server for the Docker stack: the extended electric-ivm surface
// (schema.define / ingest.write / shapes / subset queries / aggregations) over the engine +
// durable-streams. The Electric-compatible `GET /v1/shape` endpoint is served by the ENGINE
// container directly, not here.
import { createApiServer } from '@electric-ivm/api'

const dsUrl = process.env.DS_URL ?? 'http://ds:8791'
const engineUrl = process.env.ENGINE_URL ?? 'http://engine:7010'
const port = Number(process.env.API_PORT ?? 8790)

const server = await createApiServer({ dsUrl, engineUrl, port, host: process.env.BIND_HOST ?? '0.0.0.0' })
console.log(`electric-ivm API listening on ${server.url} (engine: ${engineUrl}, ds: ${dsUrl})`)

process.on('SIGTERM', async () => {
  await server.close()
  process.exit(0)
})
