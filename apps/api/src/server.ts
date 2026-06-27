import type { AddressInfo } from 'node:net'
import { createHTTPServer } from '@trpc/server/adapters/standalone'
import { createCore, type ElectricCore } from './core.js'
import { appRouter } from './router.js'

export interface ApiServer {
  url: string
  core: ElectricCore
  close(): Promise<void>
}

/**
 * Start a standalone tRPC HTTP server. `createHTTPServer` returns a Node `http.Server`, so we
 * listen (port 0 = a free port) and read the bound port from `server.address()`.
 */
export async function createApiServer(opts: {
  dsUrl: string
  engineUrl: string
  port?: number
}): Promise<ApiServer> {
  const core = createCore({ dsUrl: opts.dsUrl, engineUrl: opts.engineUrl })
  const server = createHTTPServer({ router: appRouter, createContext: () => ({ core }) })
  await new Promise<void>((resolve) => server.listen(opts.port ?? 0, '127.0.0.1', () => resolve()))
  const addr = server.address() as AddressInfo
  return {
    url: `http://127.0.0.1:${addr.port}`,
    core,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  }
}
