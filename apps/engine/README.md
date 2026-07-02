# electric-ivm-engine

The Rust engine at the center of [electric-ivm](../../README.md): a durable-streams client that
turns Postgres logical-replication changes into incrementally-maintained **shapes**, **subquery
inner-sets**, and **scalar aggregations** — one maintained stream per *distinct* definition,
ref-counted and shared across subscribers. It serves two HTTP surfaces from one process:

- the **control plane** (`/schema`, `/shapes`, `/aggregate`, `/query`, introspection), used by
  `@electric-ivm/api`;
- the **Electric-compatible `GET /v1/shape`**, so an unmodified ElectricSQL client can sync from it.

Design and execution model: [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md) and
[docs/ivm-engine-internals.md](../../docs/ivm-engine-internals.md).

## Build & run

```bash
cargo build -p electric-ivm-engine          # or: pnpm engine:build (repo root)
cargo test  -p electric-ivm-engine          # or: pnpm engine:test

ELECTRIC_IVM_DS_URL=http://127.0.0.1:8791 \
ELECTRIC_IVM_PG_URL=postgres://postgres@127.0.0.1:5432/postgres \
ELECTRIC_IVM_PG_TABLES='*' \
target/debug/electric-ivm-engine
```

The engine prints `ENGINE_LISTENING <url>` to **stdout** (logs go to stderr) so a harness can
discover the bound port.

## Environment

| Var | Default | Meaning |
|---|---|---|
| `ELECTRIC_IVM_DS_URL` | *(required)* | Durable-streams server base URL (the change log) |
| `ELECTRIC_IVM_PG_URL` | *(unset)* | Enables **Postgres mode**: ingest via logical replication, backfill by query-back. Unset = library mode (writes arrive on table streams) |
| `ELECTRIC_IVM_PG_TABLES` | *(empty)* | Comma list of tables to replicate; `*` (or empty) introspects every `public` table with a primary key |
| `ELECTRIC_IVM_PG_SLOT` | `electric_ivm` | Logical replication slot name |
| `ELECTRIC_IVM_PG_POLL_MS` | `50` | Replication-slot poll interval |
| `ELECTRIC_IVM_BIND` | `127.0.0.1:0` | Bind address (`:0` = ephemeral port) |
| `ELECTRIC_IVM_LOG` | `info` | `tracing` EnvFilter (e.g. `warn`, `electric_ivm_engine=debug`) |
| `ELECTRIC_HANDLE_TTL` | `600` | Seconds a `/v1/shape` handle may sit idle before eviction (drops its shape + stream; a late request gets `409 must-refetch`) |
| `ELECTRIC_LIVE_TIMEOUT_MS` | `20000` | Overall deadline for a `live=true` `/v1/shape` long-poll, then `204` |

### Benchmarking-fleet surface (`ELECTRIC_*`)

The engine also accepts Electric's own env surface so the `electric-ivm` image is a drop-in for
`electricsql/electric` in the [benchmarking-fleet](../../docs/fleet-conformance.md). These are resolved
in `config.rs`; the `ELECTRIC_IVM_*` vars above always **win** (dev/test behavior is unchanged). Any
unknown `ELECTRIC_*` var is accepted and logged once as "accepted (no-op)" — it never crashes boot.

| Var | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | *(unset)* | Postgres URL (tolerates `?sslmode=disable`); `ELECTRIC_IVM_PG_URL` wins |
| `ELECTRIC_PORT` | `3000` when set / under `DATABASE_URL` | Binds `0.0.0.0:<port>`; `ELECTRIC_IVM_BIND` wins |
| `ELECTRIC_LOG_LEVEL` | `info` | `error`/`warning`/`info`/`debug` → log filter; `ELECTRIC_IVM_LOG` wins |
| `ELECTRIC_REPLICATION_STREAM_ID` | *(unset)* | Slot name `electric_slot_<id>`; also the `stack_id` metric tag |
| `ELECTRIC_INSTANCE_ID` | generated UUID | Tags every StatsD metric `instance_id:<id>` |
| `ELECTRIC_STATSD_HOST` | *(unset → StatsD off)* | `host[:port]` (default port 8125) StatsD destination |
| `TELEMETRY_POLLER_PERIOD` / `ELECTRIC_SYSTEM_METRICS_POLL_INTERVAL` | `5s` | Periodic-metrics interval (ms / human duration; the latter wins) |
| `ELECTRIC_SECRET` | *(unset)* | If set, `/v1/shape` requires `secret`/`api_secret` query param (else `401`) |
| `ELECTRIC_INSECURE` | *(unset)* | Accepted; no-op when no secret |
| `ELECTRIC_STORAGE_DIR` | *(unset)* | If set + exists, `du`'d every ~60s → `electric.storage.used.bytes` |

**`GET /v1/health`** reports the boot state machine as an exact, whitespace-free JSON body:
`{"status":"waiting"}` (202) until Postgres connects, `{"status":"starting"}` (202) through
introspection/slot/ingest spawn, then `{"status":"active"}` (200). Library mode is `active` at once.
`GET /` → 200 empty; `OPTIONS /v1/shape` → 204 with `access-control-allow-methods`.

**StatsD telemetry** (`statsd.rs`) is the fleet's only metrics channel — the datadog wire format
(`name:value|type|#instance_id:<id>,...`), non-blocking (bounded channel → batched ≤1432-byte UDP
datagrams), off unless `ELECTRIC_STATSD_HOST` is set. It emits a periodic system-metrics table
(`system.*`/`vm.*`, sampled with `sysinfo`) plus event metrics at the HTTP, replication, storage, and
snapshot paths. Only genuinely-measured values are emitted; anything unmeasurable on the host is
omitted, never faked. The existing `GET /metrics` (JSON) + `GET /metrics/prometheus` (OTel) are
unchanged.

## HTTP endpoints

| Route | Purpose |
|---|---|
| `GET /health` | liveness |
| `POST /schema` | define the schema (library mode; Postgres mode self-configures by introspection) |
| `POST /shapes` | create a shape (`table`, `where`, `columns`, `changesOnly`) — identical definitions share one stream |
| `POST /aggregate` | create a live scalar aggregation (`table`, `where`, `fn`, `col`) |
| `GET /shapes/{id}` / `DELETE /shapes/{id}` | look up / drop (decrement) a shape |
| `GET /shapes/{id}/rows` | current contents of an existing shape (folds its stream; visualizer preview) |
| `POST /query` | one-shot subset query: `SELECT … ORDER BY … LIMIT/OFFSET` + snapshot LSN |
| `GET /tables/{name}/offset` · `GET /tables/{name}/families` | tailer position / routing-family stats |
| `GET /subqueries` · `GET /graph` · `GET /graph/node?sig=…` | shared-node stats, pipeline graph, one node's live index |
| `GET /replication/lsn` | ingestor LSN + sync status |
| `GET /metrics` · `POST /metrics/reset` · `GET /memory` · `GET /metrics/prometheus` | counters/histograms, memory snapshot, OTel/Prometheus exposition |
| `GET /v1/shape` | Electric protocol: snapshot (`offset=-1`), live long-poll, handles/offsets/`must-refetch` |

The `/v1/shape` adapter parses Electric's SQL `where` grammar and is validated against Electric's own
oracle/property/integration tests ([electric-conformance/](../../electric-conformance/README.md)).
