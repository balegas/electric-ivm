---
name: run-linearlite
description: Use when starting, stopping, or driving the LinearLite demo and the pipeline visualizer in a browser — covers the caddy HTTPS fronts (required in browsers because of the ~6-connection HTTP/1.1 cap), ports, teardown, and headless verification hooks.
---

# Running the LinearLite demo + pipeline visualizer

## Start / stop

```bash
scripts/linearlite.sh start <size>   # small|medium(default)|large|xlarge|<issue count>
scripts/linearlite.sh status
scripts/linearlite.sh stop           # ALWAYS stop before restarting (teardown is pattern-based)
```

Boots: ephemeral Postgres (logical replication) + durable-streams + Rust engine + tRPC API +
LinearLite web UI + the pipeline visualizer. Log: `/tmp/el-linearlite.log` (`EL_LOG` to change).

## Use the caddy HTTPS fronts in a browser — for BOTH apps

Browsers cap plain HTTP/1.1 at ~6 connections per host. Both apps hold many concurrent live
streams (shape long-polls, the visualizer's `/trace` SSE + engine polling), so over plain HTTP
they **freeze silently** once the cap is hit. Caddy fronts them with HTTPS/HTTP-2, which
multiplexes every stream over one connection:

- **LinearLite** → `https://localhost:8443/` (never the raw vite port; vite also binds IPv6
  `[::1]` only)
- **Pipeline visualizer** → `https://localhost:5443/` (the plain `http://localhost:5180/` is fine
  for `curl`, but in a browser session alongside the app it competes for the same connection
  budget — the visualizer also needs its caddy front)

Ports: `DEMO_HTTPS_PORT` (8443), `DEMO_VIZ_HTTPS_PORT` (5443), `DEMO_VIZ_PORT` (5180),
`DEMO_VIZ=0` skips the visualizer, `DEMO_HTTPS=0` skips caddy (dev/curl only). The cert comes
from Caddy's local CA: run `caddy trust` once, or click through the warning.

Running the visualizer standalone against any engine (front it with caddy yourself for browser use):

```bash
ELECTRIC_IVM_ENGINE_URL=http://127.0.0.1:<engine-port> VIZ_PORT=5180 \
  pnpm --filter @electric-ivm/pipeline-viz dev
caddy reverse-proxy --from https://localhost:5443 --to 127.0.0.1:5180
```

## Observing shape retention live

Shapes are retained through an active → dormant → evicted lifecycle (`apps/engine/src/retention.rs`);
switching users parks the old user's shapes (dashed + DORMANT badge in the visualizer) instead of
dropping them, and a rejoin reactivates them by change-log replay. The production timers are slow
(30 min idle); boot with second-scale knobs to watch it happen:

```bash
ELECTRIC_IVM_SHAPE_IDLE_SECS=12 ELECTRIC_IVM_RETENTION_SWEEP_SECS=3 \
ELECTRIC_IVM_SHAPE_DORMANT_TTL_SECS=3600 scripts/linearlite.sh start small
```

Then switch "Viewing as" users in LinearLite and watch the visualizer: the previous user's routed
shapes go dormant after ~15 s; switching back reactivates them. Subquery and aggregate shapes are
exempt from dormancy by design. Lifecycle is visible on `GET /graph` (`shapes[].state`) and
`GET /shapes/{id}` (`state`).

## Headless verification (no browser needed)

The engine's endpoints back everything the visualizer shows:

- `GET /graph`, `GET /state`, `GET /state/node?id=<node>` — topology + live per-node state +
  deep dumps (routing indexes, aggregate fold internals, subquery inner sets)
- `GET /trace` — SSE per-envelope pipeline traces
- `GET /shapes/{id}/rows`, `GET /shapes/{id}/log`, `POST /query` — shape contents vs. Postgres
  ground truth
- `GET /replication/lsn` — `{lsn, sync, pendingFlips}` (drain barrier)

Find the engine URL in the demo log: `grep ENGINE_LISTENING /tmp/el-linearlite.log`.

## Gotchas

- One demo instance at a time; a leftover `tsx start.ts`/`caddy`/engine keeps ports and serves
  stale code. `scripts/linearlite.sh stop`, else `pkill -f electric-ivm-engine`,
  `pkill -f "tsx start.ts"`, `pkill -f caddy`.
- The demo Postgres is ephemeral (`mkdtemp`) — data does not survive a restart. Leaked ephemeral
  Postgres instances exhaust macOS shared memory (SHMMNI≈32) and make `initdb` fail everywhere;
  clean with `ipcs -ma` + `ipcrm -m <id>` for 0-attach segments.
