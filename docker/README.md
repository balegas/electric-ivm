# Docker

The whole electric-ivm server stack, containerized. From the repo root:

```bash
pnpm docker:up            # = docker compose -f docker/compose.yaml up --build
```

Services (see `compose.yaml`):

| service | image | role | port |
|---|---|---|---|
| `postgres` | `postgres:16` (`wal_level=logical`) | system of record | 5432 |
| `ds` | `docker/Dockerfile.node` | durable-streams server (the log) | 8791 |
| `engine` | `docker/Dockerfile.engine` | Rust engine: replication ingest, shape/subquery/aggregation maintenance, control-plane HTTP **and Electric-compatible `GET /v1/shape`** | 7010 |
| `api` | `docker/Dockerfile.node` | extended tRPC API for `@electric-ivm/client` (shapes, subset queries, aggregations) | 8790 |

Once up:

- **ElectricSQL clients** sync from `http://localhost:7010/v1/shape` (the engine speaks the Electric
  protocol directly — same URL shape as an Electric sync-service).
- **`@electric-ivm/client`** points at `http://localhost:8790` (API) + `http://localhost:8791` (streams).
- Apps write to Postgres at `postgres://postgres:password@localhost:5432/electric`.

The engine introspects the table set at startup (`ELECTRIC_IVM_PG_TABLES=*` = every `public` table
with a primary key). Create your tables first, or `docker compose -f docker/compose.yaml restart engine`
after a migration.

## Fleet-conformance image (single `electric` container)

`Dockerfile.electric` builds **one** image that bundles the durable-streams server *and* the Rust
engine in a single container — a drop-in replacement for `electricsql/electric` in the
[benchmarking-fleet](https://github.com/electric-sql/benchmarking-fleet). Full contract:
`docs/fleet-conformance.md`.

The container's entrypoint (`electric-entrypoint.sh`) starts durable-streams on loopback, waits for
it, then starts the engine bound to `0.0.0.0:$ELECTRIC_PORT` serving `/v1/shape` + `/v1/health`. It
supervises both: if either exits, the other is killed and the container exits with that code;
`SIGTERM`/`SIGINT` are forwarded to both (clean `docker stop`, works under `docker run --init`).

**Fleet env contract** (what the fleet sets; the image also accepts the `ELECTRIC_IVM_*` knobs):

| Env var | Meaning |
|---|---|
| `DATABASE_URL` | Postgres URL (`wal_level=logical`); tolerates `?sslmode=disable`. Required. |
| `ELECTRIC_PORT` | HTTP port for `/v1/shape` + `/v1/health` (default `3000`, bind `0.0.0.0`). |
| `ELECTRIC_INSTANCE_ID` | Tags every StatsD metric `instance_id:<value>`. |
| `ELECTRIC_STATSD_HOST` | StatsD sink `host[:port]` (default port 8125); absent → StatsD off. |
| `ELECTRIC_STORAGE` | `MEMORY` → in-memory durable-streams; else (`FAST_FILE`, default) → file-backed under `$ELECTRIC_STORAGE_DIR/shapes`. |
| `ELECTRIC_STORAGE_DIR` | Root dir for file storage (default `./persistent`, anchored at `/app`). |
| `ELECTRIC_LOG_LEVEL`, `ELECTRIC_INSECURE`, `ELECTRIC_SECRET`, `ELECTRIC_REPLICATION_STREAM_ID`, … | Accepted (see the spec's env table). |

The entrypoint additionally exports the equivalent `ELECTRIC_IVM_*` vars (`ELECTRIC_IVM_PG_URL`,
`ELECTRIC_IVM_BIND`, `ELECTRIC_IVM_DS_URL`, …) so the image works with both the current engine and
newer builds that read `ELECTRIC_*` natively.

Run the local test harness (postgres with `wal_level=logical` + the image, wired with the exact env
the fleet sets):

```bash
docker compose -f docker/compose.electric.yaml up --build
# then create your tables in postgres and sync from http://localhost:3000/v1/shape
```

Point the benchmarking-fleet at the published image by setting the run spec's `electric_image` to
`ghcr.io/<owner>/electric-ivm/electric:main` (any extra `electric_env_X=Y` spec params arrive as
`X=Y` env vars, which the image passes through).

Build it standalone:

```bash
docker build -f docker/Dockerfile.electric -t electric-ivm-electric .
```

## Published images

CI publishes all three images to the GitHub Container Registry on every push to `main` and on `v*`
tags (`.github/workflows/docker.yml`):

```bash
docker pull ghcr.io/balegas/electric-ivm/engine:main
docker pull ghcr.io/balegas/electric-ivm/node:main
docker pull ghcr.io/balegas/electric-ivm/electric:main   # single fleet image
```

## Building images individually

```bash
docker build -f docker/Dockerfile.engine -t electric-ivm-engine .   # Rust engine (multi-stage)
docker build -f docker/Dockerfile.node   -t electric-ivm-node .     # ds server + API (CMD selects)
```

The engine image is a plain-HTTP binary (no TLS backend compiled in) on `debian:bookworm-slim`; the
node image runs the durable-streams server (`docker/ds-server.ts`) or the API (`docker/api-server.ts`)
via `tsx`.

## Env knobs

- `PG_PORT` / `DS_PORT` / `ENGINE_PORT` / `API_PORT` — host port mappings.
- `ELECTRIC_IVM_PG_TABLES` — comma list of tables instead of `*`.
- `DS_MEMORY=1` (on the `ds` service) — in-memory streams: no fsync-per-append ceiling, no persistence.
- Engine tuning: `ELECTRIC_IVM_PG_SLOT`, `ELECTRIC_IVM_PG_POLL_MS`, `ELECTRIC_IVM_LOG`,
  `ELECTRIC_HANDLE_TTL` (idle `/v1/shape` handle-state eviction; the shape is retained), and the
  shape-retention knobs `ELECTRIC_IVM_SHAPE_IDLE_SECS`, `ELECTRIC_IVM_SHAPE_DORMANT_TTL_SECS`,
  `ELECTRIC_IVM_MAX_SHAPES`, `ELECTRIC_IVM_SHAPE_DISK_BUDGET_MB` (see `apps/engine/README.md`).

Related: `packages/loadgen/docker/` scales headless load-generator *clients* against a host-run stack.
