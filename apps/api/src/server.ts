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
  /** Bind host. Default `127.0.0.1`; pass `0.0.0.0` to accept connections from other hosts/containers. */
  host?: string
}): Promise<ApiServer> {
  const core = createCore({ dsUrl: opts.dsUrl, engineUrl: opts.engineUrl })
  const server = createHTTPServer({ router: appRouter, createContext: () => ({ core }) })
  const bind = opts.host ?? '127.0.0.1'
  await new Promise<void>((resolve) => server.listen(opts.port ?? 0, bind, () => resolve()))
  const addr = server.address() as AddressInfo
  const host = bind === '0.0.0.0' || bind === '::' ? '127.0.0.1' : bind
  return {
    url: `http://${host}:${addr.port}`,
    core,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  }
}
