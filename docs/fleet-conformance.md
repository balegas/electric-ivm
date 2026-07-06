# Benchmarking-Fleet Conformance Spec

Contract for the single `electric-ivm` Docker image that is a drop-in replacement for
`electricsql/electric` in the [benchmarking-fleet](https://github.com/electric-sql/benchmarking-fleet).
Ground truths: the fleet's executor (`apps/executor/lib/executor/{presets,docker,gcloud_provisioner,statsd_server}.ex`),
Electric's telemetry package (`electric/packages/electric-telemetry`), Electric's `config/runtime.exs`,
and electrustic's proven parity implementation (`electrustic/src/telemetry/`).

## 1. Deployment contract (what the fleet does to the image)

- Launches ONE container from `spec["electric_image"]`, name `electric`, bridge network,
  `use_init: true`, port 3000 published 1:1. No command override — the image entrypoint must
  start everything.
- Sets env: `DATABASE_URL=postgresql://postgres:password@<proxy>:5433/postgres?sslmode=disable`,
  `ELECTRIC_INSECURE=true`, `ELECTRIC_INSTANCE_ID=<uuid>`, `ELECTRIC_STATSD_HOST=host.docker.internal`,
  `TELEMETRY_POLLER_PERIOD=200`, plus any `electric_env_X=Y` spec params as `X=Y`.
- Health check (every 500 ms, must pass within ~10 s of start):
  `curl -s http://localhost:3000/v1/health` must return HTTP 200 with body **exactly**
  `{"status":"active"}` (no whitespace). **`curl` must be installed in the image.**
- Metrics: fleet runs a StatsD UDP server on `host.docker.internal:8125` and stores every
  metric that (a) parses as `name:value|type|#tags`, (b) has numeric value, and (c) carries an
  `instance_id` tag equal to `ELECTRIC_INSTANCE_ID`. Metrics without `instance_id` are silently
  dropped. Prometheus/OTLP are NOT used by the fleet.
- Benchmarks hit `GET /v1/shape` with `table`, `offset` (-1 or token), `handle`, `live=true`,
  `where`; read `electric-handle`/`electric-offset` response headers (legacy `x-electric-*`
  also accepted by clients); parse `{"headers":{"control":"up-to-date"}}`; treat 204 as retry.
  Up to ~200 concurrent live long-polls and ~500 concurrent snapshot fetches per run.
- Postgres runs with `wal_level=logical`; connection goes through toxiproxy (latency injection).

## 2. Environment variable surface

The image accepts the union of what the fleet sets and Electric's documented vars. Mapping to
engine internals happens in the engine itself (preferred) or the entrypoint.

| Env var | Behavior in electric-ivm image |
|---|---|
| `DATABASE_URL` | → engine Postgres URL (`ELECTRIC_IVM_PG_URL`). Must tolerate `?sslmode=disable`. Required. |
| `ELECTRIC_PORT` | HTTP port for `/v1/shape` + `/v1/health` (default **3000**, bind `0.0.0.0`). |
| `ELECTRIC_INSTANCE_ID` | Tag every StatsD metric `instance_id:<value>`. Default: generated UUID. |
| `ELECTRIC_STATSD_HOST` | StatsD destination, `host[:port]`, default port 8125. Absent → StatsD off. |
| `TELEMETRY_POLLER_PERIOD` | Poll interval (ms) for periodic metrics. (Electric itself ignores this, but the fleet sets it; we honor it.) |
| `ELECTRIC_SYSTEM_METRICS_POLL_INTERVAL` | Same knob, Electric's spelling (human-readable time, e.g. `5s`). Takes precedence. Default **5s** (Electric's default). |
| `ELECTRIC_INSECURE` | Accepted. `true` → no auth (our current behavior). |
| `ELECTRIC_SECRET` | Accepted; if set, require `secret`/`api_secret` query param on `/v1/shape` (401 otherwise). |
| `ELECTRIC_STORAGE_DIR` | Root dir for durable-streams file storage (default `./persistent`). |
| `ELECTRIC_STORAGE` | `MEMORY` → in-memory durable-streams; `FAST_FILE` (default) → file-backed under `$ELECTRIC_STORAGE_DIR/shapes`. |
| `ELECTRIC_LOG_LEVEL` | error/warning/info/debug → engine log filter (default info). |
| `ELECTRIC_REPLICATION_STREAM_ID` | Suffix for slot name: `electric_slot_<id>` (default `default`). |
| `ELECTRIC_DB_POOL_SIZE` | Accepted; sizes the engine's backfill connection pool if applicable. |
| `ELECTRIC_LIVE_TIMEOUT_MS` (ours) / long-poll default | 20000 ms, matches Electric. |
| Others (`ELECTRIC_CACHE_MAX_AGE`, `ELECTRIC_OTLP_ENDPOINT`, `ELECTRIC_USAGE_REPORTING`, ...) | Accept and log "accepted (no-op)" once at boot — never crash on unknown `ELECTRIC_*`. |

Boot must log the resolved config (redact `DATABASE_URL` credentials and `ELECTRIC_SECRET`).

## 3. HTTP API contract

Engine serves on `0.0.0.0:$ELECTRIC_PORT`:

- `GET /v1/shape` — already implemented in `apps/engine/src/electric.rs` (params `table`,
  `offset`, `handle`, `where`, `columns`, `live`, `cursor`, `replica`; headers
  `electric-handle`, `electric-offset`, `electric-schema`, `electric-cursor`,
  `electric-up-to-date`; 204 empty live poll; 400/409/500 semantics; up-to-date control
  message). Keep as is.
- `GET /v1/health` — **new**: JSON body exactly `{"status":"waiting"}` /
  `{"status":"starting"}` / `{"status":"active"}`; 202 for the first two, 200 for active.
  Active once Postgres is connected, tables introspected, replication slot created, and the
  ingest loop is running. Headers: `cache-control: no-cache, no-store, must-revalidate`.
- `GET /` — 200 empty body.
- `OPTIONS /v1/shape` — 204 with `access-control-allow-methods: GET, POST, HEAD, DELETE, OPTIONS`.
- `GET /metrics` on `$ELECTRIC_PROMETHEUS_PORT` (only if set) — Prometheus text with the
  `electric_*` names from `sync-service/lib/electric/stack_supervisor/telemetry.ex` where we
  have real equivalents (see §5).

## 4. StatsD telemetry (the fleet's only metrics channel)

Wire format = `TelemetryMetricsStatsd` datadog formatter:

```
<dot.separated.name>:<value>|<type>|#instance_id:<uuid>[,tag:value...]
```

- Types: counter → `1|c` (value literally 1 per event), sum → `<v>|c`, last_value/gauge →
  `<v>|g`, distribution → `<v>|d` (one packet per observation).
- Multiple metrics MAY share one UDP datagram, newline-separated.
- EVERY metric carries `instance_id`. Floats formatted plainly (no exponent).

### 4a. Periodic metrics (every poll interval; default 5 s, fleet sets 200 ms)

Application-level (from `ApplicationTelemetry.metrics/1`), with our honest Rust equivalents.
**Emit only genuinely measured values — omit any metric we cannot measure; never fake.**

| Electric StatsD name | Type | electric-ivm source (must be real) |
|---|---|---|
| `system.cpu.core_count` | g | logical cores (respect cgroup quota if detectable) |
| `system.cpu.utilization.total` | g | mean CPU busy % across cores (0–100), sysinfo |
| `system.cpu.utilization.core_<N>` | g | per-core busy % (0-indexed like Electric's `core_#{cpu_index}`) |
| `system.load_percent.avg1/.avg5/.avg15` | g | `100 * loadavg / cores` |
| `system.memory_percent.free_memory/.available_memory/.used_memory` | g | system-wide, sysinfo |
| `vm.memory.total` | g | **process RSS bytes** (BEAM-total analog; electrustic precedent) |
| `vm.memory.processes` | g | process RSS bytes (same basis, keeps dashboards populated) — optional |
| `vm.uptime.total` | g | seconds since process start |
| `vm.total_run_queue_lengths.total` / `.cpu` | g | tokio global injection queue depth |
| `vm.total_run_queue_lengths.io` | g | 0 (real: we have no separate IO scheduler queue) — optional, may omit |
| `vm.scheduler_utilization.total` | g | process CPU % ÷ core count, 0–100 |
| `vm.system_counts.process_count` | g | OS thread count of the process |
| `system.memory.*` absolute + swap | g | optional; emit if sysinfo provides |

Do NOT emit BEAM-only internals we can't honestly measure: `vm.memory.atom*`, `vm.reductions.*`,
`vm.persistent_term.*`, `vm.garbage_collection.*`, `process.memory.total{process_type}`,
`ets.memory.total`, scheduler per-id metrics.

### 4b. Event/stack metrics (from `StackTelemetry.metrics/1` + router dispatch)

| Electric StatsD name | Type | When we emit |
|---|---|---|
| `plug.router_dispatch.stop.duration` | d, **milliseconds** | every HTTP request; tags `route:/v1/shape,status:<code>` (fleet dashboards whitelist this name) |
| `electric.plug.serve_shape.duration` | d, ms | every non-live `/v1/shape` request |
| `electric.plug.serve_shape.count` | c | every non-live `/v1/shape` request |
| `electric.plug.serve_shape.bytes` | c (sum) | response bytes, non-live |
| `electric.plug.serve_shape.requests.count` | c | every `/v1/shape` request; tags `status`, `known_error:true/false`, `live:true/false` |
| `electric.shape.response_size.bytes` | d | per response; tags `root_table`, `is_live`, `stack_id` (stack_id = `single_stack` or replication stream id) |
| `electric.postgres.replication.transaction_received.count` | c | per replicated txn |
| `electric.postgres.replication.transaction_received.bytes` | c (sum) | txn payload bytes |
| `electric.postgres.replication.transaction_received.operations` | d | ops per txn |
| `electric.postgres.replication.transaction_received.receive_lag` | d, ms | now − txn commit timestamp |
| `electric.storage.transaction_stored.count/.bytes/.operations` | c | per txn appended to durable streams |
| `electric.storage.transaction_stored.replication_lag` | d, ms | now − commit ts at append time |
| `electric.storage.snapshot_stored.count/.bytes/.operations` | c | per shape backfill/snapshot completion |
| `electric.storage.make_new_snapshot.stop.duration` | d, ms | backfill duration |
| `electric.shape_snapshot.create_snapshot_task.stop.duration` | d, ms | shape creation incl. snapshot |
| `electric.storage.used.bytes` | g | du of `$ELECTRIC_STORAGE_DIR` (file mode) or ds memory estimate; poll ~60 s |
| `electric.shape_log_collector.transaction.affected_shape_count` | d | shapes touched per txn |
| `electric.admission_control.acquire.current` | g, tag `kind:initial/existing` | real in-flight request counts, if we track them (else omit) |
| `electric.connection.consumers_ready.duration/.total` | g | boot-to-ready duration/count once at startup |

Counters in `TelemetryMetricsStatsd` are per-event packets (`:1|c`), not pre-aggregated. At high
request rates batch multiple lines per datagram; keep datagrams < 1432 bytes.

## 5. Prometheus (secondary, not used by fleet)

If `ELECTRIC_PROMETHEUS_PORT` set, serve `GET /metrics` (text 0.0.4) with Electric's names where
real: `electric_shapes_total_shapes_count`, `electric_shapes_active_shapes_count`,
`electric_postgres_replication_slot_retained_wal_size` (query `pg_replication_slots`),
`electric_postgres_replication_pg_wal_offset`, `electric_storage_used_bytes`,
`electric_plug_serve_shape_requests_total{status}`, `electric_shape_response_size_bytes`.
Keep existing `/metrics` (JSON) and `/metrics/prometheus` (engine-internal names) on the engine
port for our own tooling.

## 6. Single image composition

- Base: `node:22-slim` runtime (+ `curl`, `ca-certificates`); multi-stage with `rust:1-bookworm`
  builder for the engine binary.
- Processes: (1) durable-streams server (`docker/ds-server.ts`, port internal-only, memory or
  file mode per `ELECTRIC_STORAGE`); (2) engine binding `0.0.0.0:$ELECTRIC_PORT`, serving
  `/v1/shape` + `/v1/health`.
- Entrypoint (shell or small node script): translate env vars → internal vars, start ds, wait
  for its TCP port, start engine, forward signals, exit non-zero if either child dies.
  `/v1/health` reports `waiting`/`starting` until the engine is fully ready.
- Must be healthy < 10 s after start (engine boot incl. introspection + slot creation is
  sub-second; ds boot ~1 s).
- Runs as non-root where possible; must run fine as PID≠1 under docker `--init`.
- Publish `ghcr.io/<owner>/electric-ivm/electric:<tag>` via existing docker workflow.

## 7. Verification gates (all must pass before calling it done)

1. `docker build` succeeds; image runs with only fleet-provided env vars against a
   `wal_level=logical` Postgres.
2. The fleet's exact health command (curl+awk one-liner) exits 0 within 10 s.
3. Protocol smoke: initial `offset=-1` fetch returns rows + handle/offset headers; live
   long-poll returns inserted row then `up-to-date`; 204 on quiet poll; `where` filtering works;
   500-way concurrent snapshot + 200-way live fanout complete.
4. StatsD validation: run a UDP listener, parse with the fleet's exact parsing rules; assert
   (a) every metric has `instance_id`, (b) names match the tables above, (c) values are real —
   CPU rises under load, `vm.memory.total` ≈ container RSS, replication counters advance with
   writes, zero when idle.
5. Side-by-side: run `electricsql/electric:latest` with the same env; capture its StatsD names;
   diff against ours; every name the fleet's dashboards use must be present in ours.
6. Run at least one real fleet benchmark (its local provisioner or `packages/bench` fleet
   runner) against the image end-to-end.
