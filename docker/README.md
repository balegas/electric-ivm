# Docker

The whole electric-lite server stack, containerized. From the repo root:

```bash
pnpm docker:up            # = docker compose -f docker/compose.yaml up --build
```

Services (see `compose.yaml`):

| service | image | role | port |
|---|---|---|---|
| `postgres` | `postgres:16` (`wal_level=logical`) | system of record | 5432 |
| `ds` | `docker/Dockerfile.node` | durable-streams server (the log) | 8791 |
| `engine` | `docker/Dockerfile.engine` | Rust engine: replication ingest, shape/subquery/aggregation maintenance, control-plane HTTP **and Electric-compatible `GET /v1/shape`** | 7010 |
| `api` | `docker/Dockerfile.node` | extended tRPC API for `@electric-lite/client` (shapes, subset queries, aggregations) | 8790 |

Once up:

- **ElectricSQL clients** sync from `http://localhost:7010/v1/shape` (the engine speaks the Electric
  protocol directly — same URL shape as an Electric sync-service).
- **`@electric-lite/client`** points at `http://localhost:8790` (API) + `http://localhost:8791` (streams).
- Apps write to Postgres at `postgres://postgres:password@localhost:5432/electric`.

The engine introspects the table set at startup (`ELECTRIC_LITE_PG_TABLES=*` = every `public` table
with a primary key). Create your tables first, or `docker compose -f docker/compose.yaml restart engine`
after a migration.

## Building images individually

```bash
docker build -f docker/Dockerfile.engine -t electric-lite-engine .   # Rust engine (multi-stage)
docker build -f docker/Dockerfile.node   -t electric-lite-node .     # ds server + API (CMD selects)
```

The engine image is a plain-HTTP binary (no TLS backend compiled in) on `debian:bookworm-slim`; the
node image runs the durable-streams server (`docker/ds-server.ts`) or the API (`docker/api-server.ts`)
via `tsx`.

## Env knobs

- `PG_PORT` / `DS_PORT` / `ENGINE_PORT` / `API_PORT` — host port mappings.
- `ELECTRIC_LITE_PG_TABLES` — comma list of tables instead of `*`.
- `DS_MEMORY=1` (on the `ds` service) — in-memory streams: no fsync-per-append ceiling, no persistence.
- Engine tuning: `ELECTRIC_LITE_PG_SLOT`, `ELECTRIC_LITE_PG_POLL_MS`, `ELECTRIC_LITE_LOG`,
  `ELECTRIC_HANDLE_TTL` (idle `/v1/shape` handle eviction).

Related: `packages/loadgen/docker/` scales headless load-generator *clients* against a host-run stack.
