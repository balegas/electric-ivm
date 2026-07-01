import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// The visualizer is a thin web front attached to a running electric-lite engine. It proxies `/engine/*`
// to the engine's control-plane HTTP (default local port; override with ELECTRIC_LITE_ENGINE_URL), so
// the browser never needs CORS and you just point it at whichever engine you want to inspect.
const ENGINE = process.env.ELECTRIC_LITE_ENGINE_URL ?? 'http://127.0.0.1:3000'

export default defineConfig({
  plugins: [react()],
  server: {
    port: Number(process.env.VIZ_PORT ?? 5180),
    proxy: {
      '/engine': {
        target: ENGINE,
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/engine/, ''),
      },
    },
  },
})
