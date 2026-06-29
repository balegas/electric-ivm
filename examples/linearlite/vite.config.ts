import { fileURLToPath } from 'node:url'
import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

const repoRoot = fileURLToPath(new URL('../..', import.meta.url))

// The browser is same-origin with the Vite dev server. start.ts boots durable-streams + the API on
// EPHEMERAL ports and injects the `/api` and `/ds` proxies dynamically (so there are no fixed-port
// collisions), plus the `/pg/write` middleware. Vite auto-increments its own port if 5174 is taken.
export default defineConfig({
  plugins: [react()],
  optimizeDeps: { exclude: ['@electric-lite/client', '@electric-lite/protocol', '@electric-lite/api'] },
  server: {
    port: 5174,
    fs: { allow: [repoRoot] },
  },
})
