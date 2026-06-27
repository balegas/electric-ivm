import { fileURLToPath } from 'node:url'
import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

const repoRoot = fileURLToPath(new URL('../..', import.meta.url))

// The browser is same-origin with the Vite dev server; these proxies forward to the API and the
// durable-streams server (started by start.ts on fixed ports), so no CORS is needed.
export default defineConfig({
  plugins: [react()],
  // Workspace packages export raw .ts from src/; process them through the pipeline instead of
  // pre-bundling, and let Vite read files from the repo root (they're symlinked there).
  optimizeDeps: { exclude: ['@electric-lite/client', '@electric-lite/protocol', '@electric-lite/api'] },
  server: {
    port: 5173,
    fs: { allow: [repoRoot] },
    proxy: {
      '/api': { target: 'http://127.0.0.1:4501', changeOrigin: true, rewrite: (p) => p.replace(/^\/api/, '') },
      '/ds': { target: 'http://127.0.0.1:4500', changeOrigin: true, rewrite: (p) => p.replace(/^\/ds/, '') },
    },
  },
})
