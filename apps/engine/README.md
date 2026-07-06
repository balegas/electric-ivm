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

## HTTP endpoints

| Route | Purpose |
|---|---|
| `GET /health` | liveness |
| `POST /schema` | define the schema (library mode; Postgres mode self-configures by introspection) |
| `POST /shapes` | create a shape (`table`, `where`, `columns`, `changesOnly`) — identical definitions share one stream |
| `POST /aggregate` | create a live scalar aggregation (`table`, `where`, `fn`, `col`) |
| `GET /shapes/{id}` / `DELETE /shapes/{id}` | look up / drop (decrement) a shape |
| `GET /shapes/{id}/rows` | current contents of an existing shape (folds its stream; visualizer preview) |
| `GET /shapes/{id}/log` | tail of a shape's stream as-is (op/key/value/lsn) — the visualizer's feed change log |
| `POST /query` | one-shot subset query: `SELECT … ORDER BY … LIMIT/OFFSET` + snapshot LSN |
| `GET /trace` | SSE: per-envelope pipeline traces (hops + outcomes) and `shapeAdded`/`shapeDropped` lifecycle events; lossy by design, zero cost with no subscribers |
| `GET /tables/{name}/offset` · `GET /tables/{name}/families` | tailer position / routing-family stats |
| `GET /subqueries` · `GET /graph` · `GET /graph/node?sig=…` | shared-node stats, pipeline graph, one node's live index |
| `GET /replication/lsn` | ingestor LSN + sync status |
| `GET /metrics` · `POST /metrics/reset` · `GET /memory` · `GET /metrics/prometheus` | counters/histograms, memory snapshot, OTel/Prometheus exposition |
| `GET /v1/shape` | Electric protocol: snapshot (`offset=-1`), live long-poll, handles/offsets/`must-refetch` |

The `/v1/shape` adapter parses Electric's SQL `where` grammar and is validated against Electric's own
oracle/property/integration tests ([electric-conformance/](../../electric-conformance/README.md)).
