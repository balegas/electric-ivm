import { fileURLToPath } from 'node:url'
import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

const repoRoot = fileURLToPath(new URL('../..', import.meta.url))

// The browser is same-origin with the Vite dev server. start.ts boots durable-streams + the API on
// EPHEMERAL ports and injects the `/api` and `/ds` proxies dynamically (so there are no fixed-port
// collisions), plus the `/pg/write` middleware. Vite auto-increments its own port if 5173 is taken.
export default defineConfig({
  plugins: [react()],
  // Workspace packages export raw .ts from src/; process them through the pipeline instead of
  // pre-bundling, and let Vite read files from the repo root (they're symlinked there).
  optimizeDeps: { exclude: ['@electric-circuits/client', '@electric-circuits/protocol', '@electric-circuits/api'] },
  server: {
    port: 5173,
    fs: { allow: [repoRoot] },
  },
})
