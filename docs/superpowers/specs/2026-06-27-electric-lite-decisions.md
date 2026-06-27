# electric-lite — Research-driven decisions (concretizing the spec)

Date: 2026-06-27. Derived from the seven briefs in `docs/superpowers/research/`.
These pin the spec's abstractions to real, verified APIs.

## Pinned versions
- Rust: `dbsp = 0.299.0` (MSRV 1.93.1; we have 1.96.0). Companions: `rkyv`, `size-of`,
  `feldera-macros`, `ordered-float` (feature `rkyv_64`), `serde`. **Pin rkyv/size-of/
  feldera-macros to the versions dbsp itself uses** (mismatched rkyv majors won't compile).
- Engine HTTP: `axum` + `tokio`; durable-streams client: `reqwest`.
- TS: `@trpc/server`/`@trpc/client` 11.18, `zod` 4, `@durable-streams/state` 0.3.1 (+`/db`
  subpath), `@tanstack/db` 0.6.12, `@durable-streams/server` 0.3.7 (Node test server),
  `@durable-streams/client` 0.2.6, `@electric-sql/pglite` 0.5.3, `@faker-js/faker` 10.5.0.

## D1 — Engine row representation (resolves the #1 risk)
Z-set key = `Row(Vec<Value>)` newtype, NOT a map. `Value` enum:
`Null | Int(i64) | Text(String) | Bool(bool) | Float(OrderedFloat<f64>)`.
Column name→index mapping is stored out-of-band per shape (from the schema). Derive stack
on both types:
```rust
#[derive(Clone, Default, Debug, Eq, PartialEq, Ord, PartialOrd, Hash,
         SizeOf, Archive, Serialize, rkyv::Deserialize, IsNone)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd, Hash))]
```
Reason: `ArchivedBTreeMap` lacks reliable `Ord/Hash`; `Vec<Value>` + scalar leaves archive
cleanly and are totally ordered, satisfying `DBData` automatically.

## D2 — Circuit-per-shape, circuit-actor threading
`CircuitHandle` (from `RootCircuit::build`) is `!Send + !Sync`. Each shape gets its own
circuit confined to a dedicated OS thread ("circuit actor"); the async/HTTP side sends
`(batch, oneshot-reply)` over an `mpsc` channel. Circuit graph is fixed at build, so the
WHERE predicate is compiled to an `Arc<CompiledPredicate>` and captured in the `filter`
closure (Arc is Clone, so the closure is Clone as dbsp requires). filter is `filter(|row| pred.eval(row))`; output read via `output_handle.consolidate().iter()` → `(Row, (), weight)`.

## D3 — Z-set delta semantics & shape-output translation
Engine maintains the authoritative table as a `pk -> Row` map (replayed from the table
stream). Per change event:
- upsert (insert/update): if pk exists, feed `(-old, +1)` and `(+new, +1)`... i.e. append
  `Tup2(old,-1)` and `Tup2(new,+1)`; else just `Tup2(new,+1)`. Update the map.
- delete: if pk exists, append `Tup2(old,-1)`; remove from map.
Run `transaction()`, then read the output delta. Translate the output Z-set to shape
envelopes by grouping on pk: if any positive-weight row exists for a pk → emit `upsert`
(value = that row); else → emit `delete` (key = pk). This yields correct enter/leave/update
regardless of intra-step ordering. Assume weights ∈ {−1,+1} (table is a set keyed by pk).

## D4 — Wire envelope = State Protocol ChangeEvent (both stream kinds)
On-stream JSON item shape (what `createStreamDB` consumes):
```jsonc
{ "type": "<table>", "key": "<pk-as-string>", "value": { ...row }, // omit on delete
  "headers": { "operation": "insert"|"update"|"delete"|"upsert", "txid": "..." } }
```
- `type` = table name (the client registers one collection per shape with `type:<table>`,
  `primaryKey:<pk col>`).
- API (tRPC) converts its ergonomic `{table, op, pk, row}` input into this envelope and
  appends to `table/<name>`. Engine reads envelopes from table streams and emits envelopes
  to `shape/<id>`. Client reads `shape/<id>` via `createStreamDB`.
- JSON streams flatten one array level on POST: append `[envelope]` → one message.
- `txid` is echoed through so the live test can `db.utils.awaitTxId(txid)`.

## D5 — durable-streams usage
- Streams are explicitly created with `PUT` and **must be `Content-Type: application/json`**
  for the State Protocol. API PUT-creates `table/<name>` on `schema.define`; engine
  PUT-creates `shape/<id>` on shape registration (then backfills, then tails).
- Offsets are **opaque tokens**; persist `Stream-Next-Offset` to resume. Engine tails table
  streams with `?offset=<persisted>&live=sse` (or long-poll); appends with idempotent
  `Producer-Id/Epoch/Seq` headers (deferred hardening — M3).
- Tests embed `new DurableStreamTestServer({ dataDir? })` in-process (in-memory ephemeral);
  pass `server.baseUrl` to the engine subprocess + API + client. **Confirm the Node server's
  stream path prefix empirically** (Rust server uses `/v1/stream/<path>`; Node may differ).

## D6 — Oracle & client comparison
- pglite `PGlite.create('memory://')`; DDL/DML/SELECT from `@electric-lite/protocol`
  compilers; int/text/bool/float round-trip to JS primitives exactly.
- Client materialization via `createStreamDB(...).preload()` then `collection.toArray` /
  `collection.state`. **Strip virtual props** (`$synced/$origin/$key/$collectionId`, `_seq`)
  and compare only declared columns. Set-equality keyed by pk.
- TanStack collections keep the Node event loop alive → ensure `subscription.unsubscribe()`
  + `db.close()` teardown in every test.

## D7 — Test process topology (one external process)
Vitest process hosts: DurableStreamTestServer (Node), pglite oracle, tRPC API
(`createHTTPServer().listen(0)`), and the streamdb client. The Rust **engine** runs as a
child process (built once via `cargo build`, spawned per suite) pointed at the DS server URL
and given a control-plane port. Harness boots all, runs, tears down; prints faker seed on
failure.

## Open items to verify empirically during build
1. Node `DurableStreamTestServer` stream-path prefix + `port:0` ephemeral behavior.
2. dbsp companion crate versions that compile together (start from `cargo add dbsp`, then add
   macro crates the compiler asks for; copy versions from dbsp's lockfile if needed).
3. `IsNone` derive on a tuple-struct/enum (fallback: named-field wrapper).
4. createStreamDB delete-event shape (value omitted) against our engine's emitted envelopes.
