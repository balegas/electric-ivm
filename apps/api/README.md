# @electric-circuits/api

The extended tRPC API server — the surface `@electric-circuits/client` talks to. It sits beside the
Rust engine and durable-streams:

- **writes** (`ingest.write`) append State-Protocol envelopes directly to the durable-streams
  `table/<name>` stream (the engine tails it; used in library mode — in Postgres mode apps write
  SQL to Postgres instead);
- **schema and shape/subset/aggregate lifecycle** are forwarded to the engine's control-plane HTTP
  (`/schema`, `/shapes`, `/query`, `/aggregate`);
- **reads never pass through this server**: a create returns a `ShapeHandle` (`shapeId`,
  `streamPath`, `streamUrl`) and the client reads the durable stream directly.

The Electric-compatible `GET /v1/shape` endpoint is served by the **engine**, not here. Architecture:
[docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md).

## Procedures (`src/router.ts`)

| Procedure | Kind | Purpose |
|---|---|---|
| `schema.define` | mutation | define the schema (tables, columns, primary keys) |
| `ingest.write` | mutation | apply one change: `{ table, op, pk, row?, txid? }` |
| `shapes.create` | mutation | register a materialized, live shape (`table`, `where?`, `columns?`) — identical creates share one stream, ref-counted |
| `shapes.get` / `shapes.delete` | query / mutation | look up / drop (decrement) a shape or feed |
| `subset.query` | query | one-shot `SELECT … ORDER BY … LIMIT/OFFSET` page + snapshot LSN (ephemeral, nothing stored) |
| `subset.live` | mutation | open a changes-only live tail feed on a base predicate (no backfill) |
| `aggregate.create` | mutation | live scalar COUNT/SUM/AVG/MIN/MAX (`fn`, optional `col`) over a filter |

The predicate input is the shared AST from [`@electric-circuits/protocol`](../../packages/protocol/README.md):
leaf comparisons, `isNull`, `and`/`or`/`not`, and `IN (SELECT …)` subqueries.

## Starting a server

```ts
import { createApiServer } from '@electric-circuits/api'

const api = await createApiServer({
  dsUrl: 'http://127.0.0.1:8791',     // durable-streams server
  engineUrl: 'http://127.0.0.1:7010', // electric-circuits-engine control plane
  port: 8790,                         // omit for an ephemeral port
  host: '0.0.0.0',                    // default 127.0.0.1
})
console.log(api.url)
await api.close()
```

`docker/api-server.ts` is a complete standalone entrypoint (env: `DS_URL`, `ENGINE_URL`,
`API_PORT`, `BIND_HOST`) — it is what the `api` service in [docker/](../../docker/README.md) runs.
For embedding without HTTP, `createCore` (`src/core.ts`) exposes the same operations as plain
async methods.
