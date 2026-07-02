import react from '@vitejs/plugin-react'
import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vite'

// The playground browser app talks ONLY to the playground server (workspaces, actions, shape
// builder, and proxied engine introspection/trace). In dev, vite proxies `/api/*` to it; in
// production the server itself serves the built assets, so there is no proxy hop.
const SERVER = process.env.PLAYGROUND_SERVER_URL ?? 'http://127.0.0.1:5199'

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      // Reuse pipeline-viz's graph-building modules (build-graph, build-dbsp, nodes, labels)
      // straight from source — they are pure functions over the engine's /graph JSON.
      '@viz': fileURLToPath(new URL('../pipeline-viz/src', import.meta.url)),
    },
  },
  server: {
    port: Number(process.env.PLAYGROUND_PORT ?? 5190),
    fs: { allow: ['..'] },
    proxy: {
      '/api': {
        target: SERVER,
        changeOrigin: true,
      },
    },
  },
})
