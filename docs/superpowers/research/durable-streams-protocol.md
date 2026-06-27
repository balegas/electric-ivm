# Durable Streams — Protocol & Implementation Brief

Research date: 2026-06-27. For "electric-lite" (Node/TS appender → Rust tailer → TS live reader).

Durable Streams is an open protocol (by ElectricSQL / electric.ax) that extends plain HTTP
with ordered, replayable, offset-resumable streams plus live tailing (long-poll + SSE) and
Kafka-style exactly-once producers. There is a Rust reference server, a Node.js reference
server, a CLI, and multi-language clients.

Primary sources:
- GitHub: https://github.com/durable-streams/durable-streams
- Protocol spec: https://github.com/durable-streams/durable-streams/blob/main/PROTOCOL.md
- Docs: https://durablestreams.com (mirror: https://durable-streams-durable-streams.mintlify.app)
- Rust crate: https://lib.rs/crates/durable-streams-server , https://crates.io/crates/durable-streams-server
- npm: `@durable-streams/server`, `@durable-streams/client`, `@durable-streams/cli`

## Version numbers (confirmed via registry APIs, 2026-06-27)

| Artifact | Latest | Notes |
|---|---|---|
| `durable-streams-server` (crates.io) | **0.3.0** (2026-04-15) | prior: 0.2.0 (04-13), 0.1.3 (04-06), 0.1.x (Mar 2026) |
| `@durable-streams/server` (npm) | **0.3.7** | Node.js reference / test server |
| `@durable-streams/client` (npm) | **0.2.6** | TS client |
| `@durable-streams/cli` (npm) | **0.2.6** | CLI |

Protocol milestone "Durable Streams 0.1.0 and State Protocol" announced 2025-12-23.
Default server port across all implementations: **4437**.

---

## 1. Install & run the server locally (for tests)

### Option A — Rust server (recommended for the Rust tailer side)

```bash
cargo install durable-streams-server        # installs binary `durable-streams-server` (v0.3.0)
```

Configuration is via **environment variables (prefix `DS_`) and/or TOML**, plus a `--profile`
flag — NOT classic `--port` style flags.

- Listen address: `DS_SERVER__BIND_ADDRESS` (or `server.bind_address` in TOML). Default `0.0.0.0:4437`.
- Storage mode: `DS_STORAGE__MODE` = `memory` (default) | `file-fast` (buffered) | `file-durable` (fsync) | `acid` (redb).
- Data dir: `DS_STORAGE__DATA_DIR`.
- Profile: `--profile dev|prod|prod-tls|prod-mtls`.
- Protocol mounted at `/v1/stream`; health at `/healthz`, `/readyz`.

Ephemeral in-memory dev server on a chosen port:
```bash
DS_SERVER__BIND_ADDRESS=127.0.0.1:4437 durable-streams-server --profile dev
```

File-backed on a temp dir (for the Rust tailer's per-shape streams):
```bash
DS_STORAGE__MODE=file-durable \
DS_STORAGE__DATA_DIR="$(mktemp -d)" \
DS_SERVER__BIND_ADDRESS=127.0.0.1:4437 \
durable-streams-server
```
(From a source checkout: `cargo run -p durable-streams-server -- --profile dev`.)

> Exact precedence of `--profile` vs `DS_*` env, and the full TOML schema, are not fully
> documented in what I could fetch — see Open Questions.

### Option B — Node.js test server (easiest to embed in a TS/Vitest test)

```bash
npm install @durable-streams/server   # v0.3.7
```
```typescript
import { DurableStreamTestServer } from "@durable-streams/server"

const server = new DurableStreamTestServer({
  port: 4437,            // default 4437
  host: "127.0.0.1",     // default 127.0.0.1
  // dataDir: "./data",  // omit => in-memory (ephemeral); set => file-backed (log files + LMDB)
  longPollTimeout: 30000,        // ms, default 30000
  cursorIntervalSeconds: 20,     // default 20
  compression: true,             // default true
  onStreamCreated: (e) => console.log("created", e.path, e.contentType),
  onStreamDeleted: (e) => console.log("deleted", e.path),
})
await server.start()
console.log(server.baseUrl)   // e.g. http://127.0.0.1:4437
// ...
await server.stop()
```
For an ephemeral port pass `port: 0` (then read `server.port` / `server.baseUrl`) — *assumed*,
not explicitly documented.

A conformance suite exists: `@durable-streams/server-conformance-tests` →
`runConformanceTests({ baseUrl })`.

### Option C — Caddy-based standalone binary

Prebuilt Caddy-plugin server binaries are published on GitHub Releases (macOS/Linux/Windows).
No documented bespoke port/data-dir flags (configured via Caddyfile / the Caddy plugin).
No official Docker image found.

### CLI (handy for manual testing)

```bash
npm install -g @durable-streams/cli           # or: npx @durable-streams/cli
export STREAM_URL=http://localhost:4437
durable-stream create test-stream
durable-stream write  test-stream "Hello, world!"
echo '{"message":"hello"}' | durable-stream write test-stream --json
durable-stream read   test-stream
durable-stream tail   test-stream             # live
```

---

## 2. HTTP API (from PROTOCOL.md)

Streams are identified by a URL path. With the Rust server the mount is `/v1/stream`, so a
stream lives at e.g. `http://127.0.0.1:4437/v1/stream/{path}`. The exact path layout is
implementation-defined; the protocol only defines the methods/headers on that URL. Below,
`{STREAM}` = the full stream URL.

### Create — `PUT {STREAM}`
Streams are **explicitly created** with PUT (not purely implicit-on-append). Headers:
- `Content-Type: <type>` — sets the stream's MIME type; default `application/octet-stream`.
- `Stream-TTL: <seconds>` — sliding TTL window.
- `Stream-Expires-At: <rfc3339>` — absolute expiry.
- `Stream-Closed: true` — create already-closed (optional body = final content).
- `Stream-Forked-From: <source-path>`, `Stream-Fork-Offset: <offset>`, `Stream-Fork-Sub-Offset: <int>` — forks.

Responses: `201 Created`; `200 OK` (idempotent re-create with same config); `409 Conflict`
(config mismatch). Response headers: `Location`, `Content-Type`, `Stream-Next-Offset`,
`Stream-Closed: true` (if applicable).

> Whether the Rust/Node servers also auto-create on first POST is not confirmed — treat PUT as required.

### Append — `POST {STREAM}`
- `Content-Type: <type>` must match the stream's type (omit only for empty close-only body).
- `Transfer-Encoding: chunked` allowed for streaming bodies.
- `Stream-Seq: <string>` — optional **lexicographic** monotonic-per-writer sequence (ordering hint; distinct from the idempotent-producer headers below).
- `Stream-Closed: true` — atomically close the stream after this append (empty body allowed for close-only).
- Body = bytes to append.

Responses: `204 No Content` (success); `400`; `404`; `409 Conflict` (appending to closed
stream); `410 Gone` (soft-deleted). Response headers: `Stream-Next-Offset: <offset>`,
`Stream-Closed: true` (if now closed).

**Body framing depends on content type — there is no length-prefix/newline framing for JSON:**
- **JSON streams** (`Content-Type: application/json`): POST body is parsed as JSON and **one
  array level is flattened**. POST `[{a},{b}]` stores **two** messages; POST `{a}` stores
  **one** message. POST `[]` → `400`. Reads return a JSON **array** of the stored messages.
- **Other types**: byte-level append, no special framing (you define your own framing).

### Read (catch-up) — `GET {STREAM}?offset=<offset>`
- `offset` — start position. Omit or `-1` = beginning of stream; `now` = current tail.
- Offsets are **opaque, case-sensitive, lexicographically sortable tokens** — NOT byte offsets
  and NOT guaranteed integer message indexes. Clients MUST NOT interpret their structure;
  just persist the latest and pass it back to resume.
- Response `200 OK`. Headers: `Cache-Control`, `ETag: {id}:{start}:{end}`,
  `Stream-Next-Offset` (use as the next `offset`), `Stream-Up-To-Date: true` (caught up),
  `Stream-Cursor` (optional, for CDN collapsing), `Stream-Closed: true` (at final offset of a closed stream).
- Body = data from `offset` onward (JSON array for JSON streams). At EOF of a closed stream: empty body + `Stream-Closed: true`.

### Read (long-poll live) — `GET {STREAM}?offset=<offset>&live=long-poll[&cursor=<cursor>]`
Server holds the request up to `longPollTimeout` (default 30s) for new data.
- `200 OK` when data arrives (same shape as catch-up).
- `204 No Content` on timeout, with `Stream-Next-Offset`, `Stream-Up-To-Date: true`, `Stream-Cursor`, `Stream-Closed` if applicable.
- Resume loop: read `Stream-Next-Offset` from the response, re-issue GET with that offset.
- Echo `Stream-Cursor` back as `cursor=<cursor>` on the next long-poll to enable CDN
  request-collapsing (cursor is interval-based, server enforces monotonic progression).

### Read (SSE live) — `GET {STREAM}?offset=<offset>&live=sse`
Response `200 OK`, `Content-Type: text/event-stream`. For binary streams the response adds
`stream-sse-data-encoding: base64`. Two event types:
- `event: data` — the stream bytes (base64 if binary; raw JSON array for JSON streams).
- `event: control` — JSON: `{ "streamNextOffset": "...", "streamCursor": "...", "upToDate": true, "streamClosed": true }` (fields present as applicable).

Exact SSE framing example from the spec:
```
event: data
data: [{"k":"v"}]

event: control
data: {"streamNextOffset":"123","streamCursor":"abc"}
```
Resume: persist `streamNextOffset` from the latest `control` event; reconnect with
`?offset=<that>&live=sse`. (The protocol uses its own `control` offset rather than relying on
SSE's native `Last-Event-ID` — *the use of `id:` lines is unconfirmed*.)

### Metadata — `HEAD {STREAM}`
`200 OK` with `Content-Type`, `Stream-Next-Offset` (current tail), `Stream-TTL`,
`Stream-Expires-At`, `Stream-Closed: true` (if closed). Servers SHOULD send `Cache-Control: no-store`.
Use HEAD to get the current tail offset for "read only new data" (`offset = Stream-Next-Offset`).

### Close / Delete
- Close: `POST {STREAM}` with `Stream-Closed: true` and empty body → `204`, `Stream-Closed: true`. Closed streams reject further appends with `409`.
- Delete: `DELETE {STREAM}`. Soft-deletion if forks exist (source → `410 Gone`, forks keep reading; cascades when last fork deleted).

---

## 3. Idempotent producer (exactly-once on replay)

Three request headers on `POST {STREAM}` give Kafka-style exactly-once. The server dedups on
the tuple **`(Producer-Id, Producer-Epoch, Producer-Seq)`**:

```
Producer-Id:    <string>    # stable writer identity across restarts
Producer-Epoch: <integer>   # bump on each (re)start / fencing generation
Producer-Seq:   <integer>   # monotonic per epoch, one increment per HTTP request
```

Server validation:
- **Epoch regression** (older epoch than last seen) → `403 Forbidden` (zombie fencing — stale writer rejected).
- **Seq regression / gap** → `409 Conflict`, with diagnostic headers `Producer-Expected-Seq` and `Producer-Received-Seq`.
- **Duplicate** (already-applied `(id,epoch,seq)`) → `204 No Content` (idempotent success — the replay is a no-op).

So a single writer gets exactly-once by: keep `Producer-Id` stable, increment `Producer-Epoch`
on restart, and assign a strictly increasing `Producer-Seq` per append. On a network error it
is safe to retry the same `(id,epoch,seq)`; the server either applies it once or returns `204`.

> Note: `Stream-Seq` (lexicographic ordering hint, §2 Append) is a **separate** mechanism from
> these `Producer-*` headers. The exactly-once guarantee comes from the `Producer-*` tuple.

### TS client helper (`IdempotentProducer`)
```typescript
import { DurableStream, IdempotentProducer } from "@durable-streams/client"

const s = await DurableStream.create({ url, contentType: "application/json" })
const producer = new IdempotentProducer(s, "event-processor-1", {
  autoClaim: true,                                  // claims/bumps epoch
  onError: (err) => console.error("batch failed", err),
})
for (const event of events) producer.append(event) // fire-and-forget, auto-batched + pipelined (≤5 in flight)
await producer.flush()                              // MUST flush before shutdown
await producer.close()
```
Lower-level: `handle.append(body, { seq: "wr-iter-1-000001" })`.

---

## 4. Content-type / binary vs JSON; forks, TTL, ETag

- **JSON** (`Content-Type: application/json`): POST flattens one array level; GET/read returns a
  JSON array; SSE `data:` carries the JSON array directly; empty-array POST `[]` → 400.
- **text/***: treated as text; SSE carries it raw.
- **Binary / `application/octet-stream`** (default when no Content-Type on create): pure byte
  append, no framing. In SSE mode binary is **base64-encoded** and the response sets
  `stream-sse-data-encoding: base64`; clients must request/decode base64.
- **TTL**: `Stream-TTL: <seconds>` (sliding) or `Stream-Expires-At: <rfc3339>` (absolute), set on PUT.
- **ETag**: catch-up/long-poll responses include `ETag: {id}:{start}:{end}` and
  `Cache-Control: public, max-age=60, stale-while-revalidate=300` (use `private` for user data).
  ETags vary with closed-state. HEAD uses `Cache-Control: no-store`.
- **Forks**: create with `Stream-Forked-From`/`Stream-Fork-Offset`; reads transparently stitch
  source+fork; independent after creation; deleting a forked source soft-deletes it.

---

## Concrete curl-equivalents (Rust server, mount `/v1/stream`, base `http://127.0.0.1:4437`)

Assume a per-shape JSON stream at path `shapes/orders-active`.

Create the stream (JSON, 1h TTL):
```bash
curl -i -X PUT "http://127.0.0.1:4437/v1/stream/shapes/orders-active" \
  -H "Content-Type: application/json" \
  -H "Stream-TTL: 3600"
# -> 201 Created ; Stream-Next-Offset: <offset>
```

Append two JSON change-events in one request (array flattened to 2 messages),
idempotently:
```bash
curl -i -X POST "http://127.0.0.1:4437/v1/stream/shapes/orders-active" \
  -H "Content-Type: application/json" \
  -H "Producer-Id: tailer-1" \
  -H "Producer-Epoch: 1" \
  -H "Producer-Seq: 42" \
  --data '[{"op":"insert","id":1},{"op":"update","id":2}]'
# -> 204 No Content ; Stream-Next-Offset: <newOffset>
# Re-issuing the identical request (same id/epoch/seq) -> 204 (dedup no-op)
```

Catch-up read from the beginning (offset 0 == `-1`/omitted):
```bash
curl -i "http://127.0.0.1:4437/v1/stream/shapes/orders-active?offset=-1"
# -> 200 ; body: [ {...}, {...} ] ; Stream-Next-Offset: <o> ; Stream-Up-To-Date: true
```

Live SSE read from offset 0 (catch up then tail):
```bash
curl -N "http://127.0.0.1:4437/v1/stream/shapes/orders-active?offset=-1&live=sse"
# event: data
# data: [{"op":"insert","id":1},{"op":"update","id":2}]
#
# event: control
# data: {"streamNextOffset":"<o>","upToDate":true}
```

Live long-poll loop (binary or JSON), resuming by offset:
```bash
OFFSET=-1
while true; do
  RESP=$(curl -is "http://127.0.0.1:4437/v1/stream/shapes/orders-active?offset=$OFFSET&live=long-poll")
  OFFSET=$(printf '%s' "$RESP" | grep -i '^Stream-Next-Offset:' | awk '{print $2}' | tr -d '\r')
  # process body if 200; on 204 timeout just loop again with same/next offset
done
```

Metadata / current tail:
```bash
curl -I "http://127.0.0.1:4437/v1/stream/shapes/orders-active"
# -> 200 ; Stream-Next-Offset: <tail> ; Content-Type: application/json ; Stream-Closed?: ...
```

---

## Mapping to electric-lite

- **Node/TS API → per-table streams**: PUT-create one JSON stream per table; use
  `IdempotentProducer` (or `DurableStream.append`) with a stable `Producer-Id` per API
  instance to APPEND change-events. JSON array body lets you batch multiple events per POST.
- **Rust tailer**: run `durable-streams-server`; READ per-table streams with
  `GET ?offset=<persisted>&live=long-poll|sse`, filter, then APPEND deltas to per-shape JSON
  streams using `Producer-*` headers (persist your read offset alongside your producer seq so
  reprocessing is exactly-once end-to-end).
- **TS client**: `stream({ url, offset: savedOffset, live: true })` then `subscribeJson(...)`,
  persisting `batch.offset` for resumption. `live: true` auto-selects SSE for JSON streams.

---

## Open questions / unverified

1. **Exact stream URL path layout** for the Rust server under `/v1/stream` (slashes in path vs
   percent-encoding — Node server exports `encodeStreamPath`/`decodeStreamPath`, implying paths
   are encoded). Confirm against a running server.
2. **Auto-create on POST?** Spec describes explicit PUT; unclear whether servers auto-create a
   stream on first append. Assume PUT required.
3. **Full Rust server config**: complete `DS_*` env list, TOML schema, and `--profile`
   semantics/precedence not fully documented in fetched material. Verify with
   `durable-streams-server --help` after install.
4. **Ephemeral port**: `port: 0` for the Node test server (then read `server.port`) is assumed,
   not documented.
5. **SSE native `id:`/`Last-Event-ID`**: spec uses `control`-event `streamNextOffset` for
   resume; whether servers also emit SSE `id:` lines / honor `Last-Event-ID` is unconfirmed.
6. **`Stream-Seq` vs `Producer-Seq` interaction** when both are sent — unconfirmed; rely on
   `Producer-*` for exactly-once.
7. **Max body / batch size, long-poll timeout config** on the Rust server (Node default 30s).
8. **No official Docker image** found as of 2026-06-27 (Caddy binary on GitHub Releases instead).
9. crates.io README (install/flag specifics) is JS-rendered and wasn't directly fetchable;
   flag details above come from lib.rs's rendering of the same crate — double-check via `--help`.
