import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// The visualizer is a thin web front attached to a running electric-circuits engine. It proxies `/engine/*`
// to the engine's control-plane HTTP (default local port; override with ELECTRIC_CIRCUITS_ENGINE_URL), so
// the browser never needs CORS and you just point it at whichever engine you want to inspect.
const ENGINE = process.env.ELECTRIC_CIRCUITS_ENGINE_URL ?? 'http://127.0.0.1:3000'

export default defineConfig({
  plugins: [react()],
  server: {
    port: Number(process.env.VIZ_PORT ?? 5180),
    // Set by demo launchers that front the explorer with a TLS proxy and need a deterministic
    // upstream address (vite's default `localhost` binding resolves per-platform, e.g. ::1).
    ...(process.env.VIZ_HOST ? { host: process.env.VIZ_HOST } : {}),
    // Vite rejects requests with unrecognized Host headers; a tunnel (cloudflared/ngrok) fronting
    // the explorer needs its hostname allowed. "all" disables the check (dev/demo only), else a
    // comma-separated hostname list.
    ...(process.env.VIZ_ALLOWED_HOSTS
      ? { allowedHosts: process.env.VIZ_ALLOWED_HOSTS === 'all' ? true : process.env.VIZ_ALLOWED_HOSTS.split(',') }
      : {}),
    // Behind a TLS front (the demo's caddy on 5443), Vite's HMR client must dial the FRONT's port
    // over wss — by default it hardcodes this server's plain-HTTP port, so the browser attempts
    // wss://host:5180 and the websocket fails (the app still works; HMR doesn't). Set by the demo
    // launcher to the front's public port.
    ...(process.env.VIZ_HMR_CLIENT_PORT
      ? { hmr: { protocol: 'wss', clientPort: Number(process.env.VIZ_HMR_CLIENT_PORT) } }
      : {}),
    proxy: {
      '/engine': {
        target: ENGINE,
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/engine/, ''),
      },
    },
  },
})
