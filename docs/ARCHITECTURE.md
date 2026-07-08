# electric-ivm — architecture

The as-built system architecture. Companion documents:

- **[ivm-engine-internals.md](ivm-engine-internals.md)** — the engine's execution strategies and the
  analytical cost model (what grows with shapes/users/rows).
- **[shapes-and-subqueries-guide.md](shapes-and-subqueries-guide.md)** — the user/integrator guide.
- **[deployment-postgres.md](deployment-postgres.md)** — running against your Postgres.

---

## 0. System in one diagram

```
  app ──ordinary SQL writes──▶ POSTGRES (system of record; wal_level=logical)
                                  │ logical replication (streaming pgoutput slot, REPLICA IDENTITY FULL)
                                  ▼
                               INGESTOR (replication.rs)
                                  │ decode commits → envelopes stamped (commit LSN, xid, seq)
                                  │ append, then acknowledge (append-then-acknowledge)
                                  ▼
                               DURABLE STREAMS  changes            (ONE ordered change log, commit order)
                                  │ tail (single LSN-ordered sequencer; global (lsn,seq) de-dup)
                                  ▼
                               ENGINE (engine.rs)
                                  │ Z-set delta → key routing ⊕ stateless filters
                                  │              ⊕ subquery registry ⊕ aggregations
                                  │ reliable append (retry-until-landed)
                                  ▼
                               DURABLE STREAMS  shape/<id>         (one feed per DISTINCT shape)
                                  │ read / long-poll
                                  ▼
                               CLIENTS
                                  ├─ @electric-ivm/client  (shapes, subset queries, aggregations)
                                  └─ ElectricSQL client     (GET /v1/shape on the engine)
```

Three ideas carry the whole design:

1. **Postgres is the system of record; the engine holds no copy of any table.** State that scales
   with row count lives in Postgres. The engine keeps per-shape routing metadata and shared subquery
   inner-sets only; shape backfills read just the matching rows back from Postgres.
2. **Everything between layers is an append-only stream.** The write path (replication → table
   streams) and the read path (shape streams → clients) never talk directly; the engine is a
   restartable consumer in the middle.
3. **Every maintained result is de-duplicated.** Two equal shapes — same table, canonical predicate,
   projection, and kind — share one maintained stream, ref-counted. Identical subqueries share one
   inner-set node. Identical aggregations share one running fold. The engine maintains and appends
   once for all subscribers.

---

## 1. Components

- **durable-streams** — append-only, offset-addressed JSON streams with long-poll tailing. One
  `table/<name>` stream per table (the write log), one `shape/<id>` stream per distinct shape (the
  result feed). The decoupling boundary between write and read paths.
- **engine** (`apps/engine`, Rust) — the core: replication ingest, per-change Z-set deltas, fan-out to
  shapes/subqueries/aggregations, the control-plane HTTP API, and the Electric-compatible
  `GET /v1/shape` endpoint.
- **API** (`apps/api`, tRPC) — the extended surface used by `@electric-ivm/client`: `schema.define`,
  `ingest.write` (library mode), `shapes.create/get/delete`, `subset.query/live`, `aggregate`.
- **client** (`packages/client`) — `shape()` (a live TanStack DB collection), `subset()` (an ordered,
  windowed page + a shared live tail), `aggregate()` (a live scalar), typed writes, `awaitTxId`.
- **oracle + conformance** (`packages/oracle`, `packages/conformance`) — a Postgres/pglite reference
  implementation and the harness asserting engine ≡ oracle for the same op stream, through the real
  API + client, including live replication, fuzzing, NULLs, and concurrent writers.

---

## 2. Data model

- **`Value`** (`value.rs`) — `Int | Float | Text | Bool | Null`. NULL is first-class (three-valued
  logic). **`Row`** = positional `Vec<Value>`; the schema names the positions.
- **Z-set delta** — `Vec<Tup2<Row, ZWeight>>`, `ZWeight` a signed i64: insert = `(row,+1)`, delete =
  `(old,−1)`, update = `(old,−1),(new,+1)`. `old` comes from the replication envelope
  (`REPLICA IDENTITY FULL`), so no local table state is needed to retract a row. The delta algebra
  is [`dbsp`](https://crates.io/crates/dbsp)'s, and the crate is a dependency again — `Tup2` and
  `ZWeight` are dbsp's own, and `Value`/`Row` carry the `DBData` derive stack. The **hot path still
  runs no dbsp circuit** (key routing + stateless predicate evaluation; internals doc §1); dbsp
  powers the optional storage-backed arrangement layer (§6b) that replaces Postgres query-backs
  with local, disk-spillable table indexes.
- **Envelope** (`ds.rs`) — the unit on every stream:
  `{ type, key, value, old, headers{ operation, txid, offset, lsn, seq } }`. The ingestor stamps
  `lsn` (transaction **commit** LSN), `txid` (the Postgres **xid**), and `seq` (the change's position
  within its transaction).

---

## 3. Ingest: logical replication, exactly-once effect

`replication.rs` **streams** a `pgoutput` slot over the walsender protocol (push delivery — no
poll floor; the wire client is `pgwire-replication`, the message decoding is our `pgoutput.rs`).
Each transaction's changes are buffered between `Begin` and `Commit`, stamped with
`(commit LSN, xid, seq)`, appended to `table/<name>`, and only **then** acknowledged to Postgres
(`confirmed_flush_lsn`) — a failed append tears the connection down unacknowledged, and the server
resends from the confirmed position.

Delivery to the table streams is therefore **at-least-once** (a partial multi-table append failure,
or acknowledgements not yet flushed at a crash, re-deliver whole transactions). Deltas are *not*
idempotent for aggregates and subquery contributor weights, so the consumer side restores
exactly-once **effect**:

- **Sequencer de-duplication.** `(commit LSN, seq)` uniquely identifies a change and is strictly
  increasing on the single ordered log. The sequencer keeps a highwater mark and skips anything at
  or below it.
- The drain-barrier sentinel (`__el_sync`) is published only after its whole commit landed on the
  streams, so the barrier can never claim "drained" while a transaction is still due for re-append.

Degraded/unsupported forms are **loud, never silent**: an `UPDATE` without an old image or a `DELETE`
without tuple data (REPLICA IDENTITY no longer FULL) and `TRUNCATE` log errors describing the staleness
they cause; unparseable values (e.g. `NaN` floats) log errors when degraded to NULL.

---

## 4. Backfill and the consistency fence (SnapshotGate)

A shape's initial rows come from a single `REPEATABLE READ` snapshot with the predicate pushed into
the `SELECT`. Live and backfill must then be reconciled so every change counts exactly once.

**LSN comparison alone is not sound.** `pg_current_wal_lsn()` is a WAL *write* position, but snapshot
visibility is decided later, at `ProcArrayEndTransaction` (after the commit record is fsynced). A
transaction whose commit record is already in the WAL can be **invisible** to a snapshot taken during
that window — skipping its replicated change "because commit LSN < seed LSN" would drop the row from
both the backfill and the live stream, permanently. Conversely, a visible commit can sit exactly at
the boundary and be replayed as a duplicate.

The fence is therefore **transaction visibility** (`pg::SnapshotGate`): the backfill transaction
captures `pg_current_snapshot()` (xmin / xmax / in-progress xids) in the same statement that
establishes the snapshot, and the engine skips a replicated change **iff its xid was visible to that
snapshot** (every xid seen on the slot is committed, so visibility is `xid < xmin`, or
`xmin ≤ xid < xmax` and not in the in-progress list). Changes without a parseable xid (library mode)
fall back to the strict-`<` LSN comparison. Every seeded structure — routed shapes, standalone shapes,
aggregations, subquery nodes, subquery shapes — carries its own gate; `changes_only` feeds carry a
passthrough gate (no backfill ⇒ forward everything).

The backfill row representation is normalized to match the live path: text-mapped columns are read
with `::text` casts so a cell's value is Postgres's *text output* — the same form pgoutput's
text-mode tuples
prints — rather than `to_jsonb`'s (which would make the same timestamp compare unequal between a
backfilled row and its first live update).

*Known residual:* the **client-side subset seam** (§7) still positions by LSN watermarks; the same
visibility window theoretically applies to a subset page's snapshot vs its live tail and would need
the page query-back to also return the snapshot's xid list. Engine-maintained state is fully fenced.

---

## 5. The engine: fan-out, sharing, lifecycle

### 5.1 Sequencer model

ONE tokio task consumes the single ordered change log for all tables — Electric's
`ShapeLogCollector` pattern. Processing is serial in commit order (global ordering and state are
trivially correct), and each source transaction's shape appends are flushed **before the next
transaction is processed** — per-transaction atomic emission, across tables; the only intra-txn
parallelism is the append flush (bounded-concurrent, CAP=32). After a batch is fully fanned out
**and every append has landed**, the sequencer publishes its processed offset — the convergence
barrier used by the conformance harness (`GET /tables/<t>/offset` reports the global offset).

Shape creation is **two-phase** so a Postgres backfill never stalls the pipeline: `BeginShape`
registers a pending shape that buffers its table's deltas; the creator runs the backfill on a
pooled connection concurrently; `ActivateShape` replays the buffer through the shape's snapshot
gate and goes live. The buffer is registered before the snapshot is taken, so no change can fall
between them.

### 5.2 Three execution strategies

The shape of the predicate picks the strategy (full detail + cost model: internals doc §3):

- **Equality templates** (`a = 1 AND b = 2`) → **key routing**: one shared `KeyRouter` per key-column
  set; `key_tuple → {shapes}`. Routing is O(log N), independent of shape count; zero table rows held.
- **Standalone** (ranges, OR, NOT, …) → a stateless three-valued filter evaluated directly on the
  delta. No state. A necessary-conjunct index (`(column, op)` — equality hash buckets + ordered
  range bounds) selects only the candidate shapes per change; predicates with no indexable
  conjunct (OR/NOT/LIKE/`!=` at the top) fall back to a scan list.
- **Subqueries** (`col [NOT] IN (SELECT …)`) → the cross-table registry (§6).

**Aggregations** (electric-ivm extension, not part of the Electric-compatible API): a scalar
COUNT/SUM/AVG/MIN/MAX over a non-subquery predicate, maintained incrementally as a fold over the
delta — COUNT/SUM/AVG hold running scalars, MIN/MAX a `value → net-weight` multiset so retractions
restore the previous extreme. SQL NULL semantics are mirrored exactly: aggregates ignore NULL values,
`COUNT(col)` counts non-NULLs (`COUNT(*)` counts rows), AVG divides by the non-NULL count, and
SUM/AVG/MIN/MAX over zero non-NULL values are NULL. The feed carries the current value as a
single-row stream (`{ value, n }`).

### 5.3 Shape de-duplication (the sharing layer)

Any two **equal** shapes share one maintained stream, ref-counted:

- **Signature.** Row shapes: `(table, canonical predicate, sorted projection, changes_only)` —
  predicate canonicalization is order-insensitive (`a AND b` ≡ `b AND a`). Aggregations:
  `(table, canonical predicate, function, column)`, namespaced so the two kinds never collide.
- **Join.** A create whose signature already exists increments the refcount and returns the *same*
  shape id + stream. Joiners **wait for the creator's backfill to land** (a watch channel in the
  share entry) so no caller ever sees a stream whose snapshot isn't readable yet — and a *failed*
  creation propagates to every waiting joiner rather than handing them a dead stream.
- **Drop.** Deletes decrement; the shape, its routing/registry entries, **and its durable stream**
  are torn down when the last subscriber leaves (a dropped shape must not leave an orphaned stream
  on the storage server). N joiners hold the same id and must each delete exactly once; the client
  enforces one-shot `close()`.
- The Electric `/v1/shape` adapter opts out (`share=false`) — its protocol needs per-request handles.

### 5.4 Creation is atomic; failures never leave zombies

`create_shape` returns `Ok` only after registration + backfill actually succeeded. On any failure —
backfill error, subquery seeding error, append error — the shape record, share entries, sequencer
registration, and (for subqueries) every node refcount/edge/pending-seed added by the attempt are
rolled back, waiting joiners get the error, and the stream is deleted. This structurally excludes
the "zombie shape" failure mode: a shape that is registered, streams nothing, and pins its
signature so all future identical creates silently join a dead feed.

### 5.5 Reliability: appends never drop silently

A lost shape-stream append is a permanent divergence for every subscriber, so live-path appends use
`append_reliable`: transient failures retry with capped backoff (backpressuring the sequencer — the same
stance as the ingestor's read-then-commit), and the only non-retried case is 404 (the shape was
dropped mid-flush; discard is correct). Because shape envelopes are absolute per-pk
(`upsert`/`delete` by key), an ambiguous-failure double-append is idempotent for readers.

---

## 6. Subqueries: shared inner-set nodes

(Cost model: internals doc §3.3.)

A predicate leaf `col [NOT] IN (SELECT proj FROM inner WHERE …)` routes through a registry the
sequencer feeds every table's deltas into:

- **Node** — one per distinct inner query (canonical signature), ref-counted: a map
  `projected value → set of contributing inner-row pks`. A value is in the set iff its contributor
  set is non-empty; tracking pks (not counts) makes maintenance reconcile-by-identity — idempotent
  and order-independent.
- **Edges** — `node → dependent` (an outer shape, or a *parent node* for nested subqueries), labeled
  with the connecting column. When a node **flips** a value (∅→non-empty or back), the dependent rows
  with `connecting_col = value` are queried back and re-evaluated, recursing up the DAG. Flip
  propagation runs on a dedicated engine task, **off the sequencer hot path**: the sequencer only
  reconciles nodes in-memory and emits own-table deltas under the registry lock; the Postgres
  query-backs never hold it. Membership evaluation and the resulting append stay atomic under the lock (a stale move
  landing after a fresher emission would be permanent divergence), and the engine exposes the
  in-flight flip count (`GET /replication/lsn` → `pendingFlips`) as the extra convergence-barrier term.
- **Absolute emission** — the correctness rule that keeps deferred flips convergent: for each
  touched pk the registry emits the row's *current* membership (`upsert` if it matches now, else
  `delete` by pk), never a history-dependent delta. Flip propagation runs deferred (out of commit
  order relative to the sequencer's own emissions); a delta-based "delete only if the old row
  matched" would miss move-outs whenever the inner set runs ahead. Absolute emission converges
  regardless of that timing — which is why the Electric-style LSN-buffering/tag protocol isn't
  needed here.
- **NULL sensitivity** — SQL: a NULL in the inner set makes `x NOT IN S` UNKNOWN. A NULL flip
  re-derives exactly the dependents that can change: those whose `IN` leaf is negated **or sits under
  any `Not{…}`** (with no negation above the leaf, NULL only moves the leaf between FALSE and
  UNKNOWN, and AND/OR are monotone over FALSE < UNKNOWN < TRUE, so inclusion can't change).
- **Atomicity** — node creation/refcounts/edges roll back exactly on a failed shape create (§5.4).

---

## 6b. dbsp arrangements (optional): disk-spillable table state

`ELECTRIC_IVM_DBSP=1` (`arrangements.rs`) brings dbsp back — not as per-shape circuits (removed
for cause; see the git history around `75488b6`/`c1aa075`) but as a **table-state layer**: one
shared, storage-enabled circuit maintains per-table arrangements (primary key always, plus
columns declared via `ELECTRIC_IVM_DBSP_INDEXES=table.col,…`). Subquery flip re-derivations and
full re-derives read these local snapshots instead of querying Postgres back; a lookup against a
missing/unseeded index returns `None` and the caller falls back to Postgres, so correctness never
depends on the layer.

- **Why**: the workload is small today but may not stay small. The engine's zero-state invariant
  priced every flip as a Postgres round-trip; the arrangement layer keeps that state local with a
  bounded RAM footprint — dbsp spills batches to Snappy-compressed layer files as tables grow
  (`ELECTRIC_IVM_DBSP_MIN_STORAGE_KB`, default 1 MiB; block cache `ELECTRIC_IVM_DBSP_CACHE_MIB`;
  memory-pressure spilling via `ELECTRIC_IVM_DBSP_MAX_RSS_MB`).
- **Consistency**: the sequencer feeds each transaction into the circuit and steps it **before**
  fanning the transaction out, so flip re-derivations enqueued by a txn observe post-txn state —
  the same read-your-committed-writes guarantee the Postgres query-back gave. Fresh seeds read one
  `REPEATABLE READ` snapshot per table (chunked), and the live feed is fenced by the seed's
  `SnapshotGate` (xid visibility), exactly like shape backfills.
- **Restart**: periodic checkpoints (`ELECTRIC_IVM_DBSP_CHECKPOINT_SECS`, default 60; plus at
  shutdown) persist the circuit; `meta.json` records the change-log offset and the `(lsn, seq)`
  de-duplication highwater. On boot the circuit restores and the sequencer replays the gap;
  overlap is harmless because the arrangement layer re-checks the highwater (Z-set deltas are not
  idempotent). An index-layout change discards state and reseeds. The default state dir is
  slot-keyed (`<storage>/dbsp/<slot>`): dbsp state is only valid for the database identity it was
  built from.
- **The 0.299 lesson, addressed**: the earlier memtest (git `f5dab45`) found dbsp storage spilled
  ~2 MB of a ~570 MB trace — because batches spill at **merge and checkpoint boundaries**, and a
  static table seeded as one giant batch never merges. The layer therefore seeds in bounded chunks
  (many level-0 batches ⇒ real merges) and checkpoints (persists every in-memory batch);
  `spill_produces_layer_files` / `memtest_spill_large` in `arrangements.rs` pin this down.
- **Observability**: when the layer is on, `/graph` carries an `arrangements` section — the
  compiled circuit as stable-id nodes (`arr:input:<table>` per table, `arr:index:<table>:<cols>`
  per index pipeline, with seeded flags and the layer's served/fallback lookup counters) plus a
  `consumers` list connecting each index to the subquery shapes/nodes whose flip re-derivations it
  currently serves. The circuit is static, so the visualizer's circuit view renders it as a
  permanent dashed lane; the consumer edges appear and disappear with the shapes.
- **Limits (v1)**: indexes are fixed at boot (a dbsp circuit's structure is fixed at
  construction) — new lookup columns need a restart, until circuit-rebuild-on-demand lands;
  single worker; equality lookups and full scans only (no range seeks yet).

### The serving model this is one tier of

The arrangement layer is the degenerate first tier of a three-tier serving model
(`building-app-pipelines.md` is the full treatment):

- **Pipelines serve query families.** Deploy-time circuit structure, one delta stream per
  *cohort group*, never growing with shapes/users/parameter combinations.
- **Routing serves query instances.** A shape = a selection/union of cohort groups from one
  pipeline's keyed output, materialized at the delivery edge; correct when the cohort key
  partitions the table. Time-varying membership (subquery shapes) is routing *driven by a
  feed*: membership deltas subscribe/unsubscribe cohort groups, move-in = replay the group's
  log.
- **The fallback serves query strangers.** Predicates matching no template run on the
  always-on dynamic path (standalone eval + `AccessLeaf` index, `KeyRouter` families, the
  subquery registry). The pipeline tiers are optimizations in front of it, never a
  correctness dependency — a new query pattern works immediately at fallback cost and is
  promoted into the circuit at the next deploy if it matters.

---

## 7. Subset queries and client positioning

A **subset query** is the non-materialized counterpart to a shape: one
`SELECT … WHERE … ORDER BY … LIMIT/OFFSET` page against Postgres (subquery predicates evaluated
natively by Postgres) + a **shared** `changes_only` live feed for the base predicate. Ranges live
*only* here — they are never live-tailed, so a change is matched against one base predicate, never
split across ranges. `orderBy`/`limit` are subset knobs, not shape knobs.

The client (`packages/client/src/subset.ts`) merges the page(s) and the live tail by **per-pk LSN
watermarks**: the page's snapshot LSN, and each applied delta's commit LSN. Engine output envelopes
carry their commit LSN for exactly this. Key invariants (all regression-tested):

- The feed is created and its head offset captured **before** the page snapshot, so no delta can fall
  in the gap; overlap reconciles idempotently by pk (`delta lsn ≥ snapshot lsn` applies; the engine's
  backfill-visible side is strictly below).
- **Deletes leave tombstone watermarks** (including for pks never seen): a `loadMore` page whose
  snapshot predates a delete must not resurrect the row / insert a ghost. Tombstones prune when no
  page is in flight.
- Close is one-shot; the feed is deleted with retries; a failed page query-back deletes the
  just-created feed before rethrowing (no refcount pinning).

---

## 8. Electric protocol adapter

`GET /v1/shape` (`electric.rs`) serves the ElectricSQL client protocol directly from the engine:
`table` + SQL `where` (+ `columns`) are parsed (`where_sql.rs`) into the same predicate AST used
everywhere else, identical `/v1/shape` definitions share ONE engine shape (`share=true`, so the
handle is the shared shape id), the shape stream is folded into the Electric message shape
(insert/update/delete + `up-to-date` control messages), and live requests long-poll. Handle state
is evicted after an idle TTL (`ELECTRIC_HANDLE_TTL`); the backing shape + stream are **retained**
and follow the engine's three-tier retention lifecycle (active / dormant / evicted — idle shapes
drop their engine state but keep the stream, and any touch reactivates them by change-log replay
from the captured resume offset (through the sequencer's two-phase pending-buffer handshake);
see `apps/engine/src/retention.rs`). A request with an evicted handle gets `409 must-refetch`,
which the Electric client handles by re-syncing onto the retained shape. Conformance against Electric's own oracle + integration tests lives in
`electric-conformance/` (see its README for scope and known gaps — e.g. row `tags` are not emitted;
absolute membership emission makes them unnecessary for convergence).

---

## 9. Consistency & durability model (summary)

| seam | mechanism | guarantee |
|---|---|---|
| backfill ↔ live | `SnapshotGate` (xid visibility; LSN fallback) | each change counts exactly once per shape/aggregate/node |
| ingestor → change log | append → acknowledge + `(lsn,seq)` sequencer de-dup | at-least-once delivery, exactly-once effect |
| engine → shape streams | `append_reliable` + offset published only after landing | no silently-lost deltas; barrier implies subscriber streams reflect the batch |
| cross-table subquery order | absolute membership emission + flip query-backs | convergence independent of deferred-flip timing |
| shared shapes | signature + refcount + ready-watch + atomic rollback | joiners see a live, backfilled stream or an error; last drop tears everything down |
| subset page ↔ live tail | per-pk LSN watermarks + delete tombstones | no double-count, no resurrections/ghosts across the seam (LSN-based; see §4 residual) |
| client lifecycle | one-shot close, delete-with-retry | balanced create/drop; no refcount pinning or steal |
| engine restart | durable shape catalog (`meta/catalog`: create/join/leave/drop + change-log offset checkpoints) | plain/routed shapes + aggregates restore without client re-registration (plain resume via replay + passthrough gates; aggregates re-seed with a fresh gate); subquery shapes are dropped loudly (inner-node state is not persisted) and recreated by clients |

The invariant the conformance suite asserts end-to-end: *for any shape and any op stream, the
client-materialized set equals the oracle's `SELECT … WHERE <predicate>`* — through the real API,
stream, and client, including live replication, batched mutations, NULLs, and concurrent writers.

---

## 10. Threading model

| unit | threads | notes |
|------|---------|-------|
| engine main | tokio multi-thread | sequencer + flush run here |
| sequencer (all tables) | 1 task | commit-ordered change processing; per-txn atomic flush |
| shapes (any kind) | **0** | no per-shape thread or circuit |
| replication ingestor | 1 task | stream pgoutput/decode/append/acknowledge |
| subquery registry | 0 (a mutex) | the sequencer reconciles nodes + emits under it (in-memory + appends only) |
| flip propagator | 1 task | deferred subquery query-backs; PG round-trips never hold the registry lock |

Threads are flat in the number of shapes *and* in the number of equality templates.

---

## 11. Telemetry

- `GET /metrics` — atomic counters (`envelopes_processed`, `shape_appends`, `family_steps`) +
  log-bucket latency histograms (`process_envelope`, `family_step`, `append`) with p50/p99/p999/max.
- `GET /memory` + OTel gauges (`engine_shapes`, `engine_subquery_nodes`, `engine_subquery_contributors`,
  `engine_family_circuits`, …) — the cardinalities that drive RSS; `GET /metrics/prometheus` exports.
- `GET /graph`, `GET /graph/node?sig=…`, `GET /shapes/{id}/rows` — the live pipeline topology + node
  indexes + shape contents, consumed by the **pipeline explorer** (`apps/pipeline-viz`).
- `GET /state`, `GET /state/node?id=…` — per-node live state: summaries for every pipeline node
  (offsets, emit counters, routing-index/inner-set cardinalities, fold values) and on-demand deep
  dumps (a family's routing index contents, an aggregate's fold internals incl. the MIN/MAX
  multiset). Summaries are also pushed as `{"type":"state"}` events on `/trace` after each batch,
  which is what makes the explorer's node chips reactive without polling.
- `GET /tables/<t>/families`, `GET /subqueries` — sharing topology (proof that N shapes share one
  router/node).

---

## 12. Potential speedups

The engine's internal per-change cost is <1 ms p99 at 100k shapes; the end-to-end ceiling under load
is **storage throughput** (the single-process durable-streams test server), not engine compute.

**Storage / append path (current ceiling)**
1. Multi-stream append (one request, many streams) — fan-out to M streams is M HTTP requests today.
2. HTTP/2 multiplexing / persistent pipelined connections to storage.
3. Shard the sequencer's fan-out (partition a table's shapes/key-space across tasks).
4. A production durable-streams backend (the test server fsyncs per append when file-backed).

**Standalone evaluation (O(K) per change)**
5. ~~Predicate indexing by `(column, op)` — turn O(K) into output-sensitive.~~ Done: standalone
   shapes are indexed by a necessary conjunct (equality → hash bucket, range bound → ordered
   scan); only candidates are evaluated per change. Un-indexable predicates (OR/NOT/LIKE/`!=`)
   remain on a fallback scan list.
6. Widen the shared class beyond pure equality (e.g. single-column range templates).

**Engine compute / representation**
7. ~~Backfill connection pooling for burst shape creation (the fleet benchmark's p99 driver).~~
   Done: backfills/query-backs/subset queries share a per-URL pool (`ELECTRIC_DB_POOL_SIZE`, default 20).
8. Intern stream paths/txids; pack `Value` (smaller enum, interned strings).

---

## 13. Client query layer (two-level querying)

There are **two query layers** with different jobs:

1. **Server-side shape predicate** — *what crosses the network*. One table + a `WHERE` over its
   columns (+ subqueries), optionally narrowed by a `columns` projection (sync only what a view
   needs; the pk is always included). The engine maintains exactly this set on the shape stream.
2. **Client-side live query** (TanStack DB `useLiveQuery` over the materialized collection) — *how
   it's presented*: ordering, text search, finer filtering. Maintained incrementally on the client;
   a refinement (typing in a search box) never touches the engine or re-syncs.

**Windowed / infinite-scroll sync** uses **subset queries** (§7): each page is a bounded keyset range
query (`col < lastSeen OR (col = lastSeen AND id < lastId)` folded into the `WHERE`), no stateful
top-N anywhere. The render layer is virtualized, so a 100k-row deployment stays a few dozen DOM nodes.
For permissioned/faceted lists, prefer **per-facet feeds reused across filter changes** + a client
merge (identical predicates across users ⇒ shared engine families) over folding UI filters into the
predicate (which recreates the feed per click) — see AGENTS.md "gotchas".

## 14. File map

| path | role |
|------|------|
| `apps/engine/src/engine.rs` | the LSN-ordered sequencer, delta computation, routing + standalone + aggregation fan-out, shape sharing/lifecycle, reliable per-txn flush |
| `apps/engine/src/subquery.rs` | subquery registry: shared nodes, edges, flips, absolute emission, atomic create/rollback |
| `apps/engine/src/arrangements.rs` | optional dbsp arrangement layer: storage-backed table indexes, checkpoints, snapshot lookups (§6b) |
| `apps/engine/src/replication.rs` | ingestor: streaming pgoutput, per-txn buffering, (lsn, xid, seq) stamping, append-then-acknowledge |
| `apps/engine/src/pg.rs` | connect/introspect, slot + REPLICA IDENTITY, backfill (+ `SnapshotGate`), subset query-back, value normalization |
| `apps/engine/src/predicate.rs` | predicate compile, three-valued eval, equality templates, subquery signatures |
| `apps/engine/src/sql.rs` / `where_sql.rs` | predicate → SQL (pushdown) / SQL `WHERE` → predicate (Electric path) |
| `apps/engine/src/electric.rs` | Electric `/v1/shape` adapter (handles, offsets, TTL eviction) |
| `apps/engine/src/ds.rs` | durable-streams client: `append`, `append_reliable`, `delete_stream`, reads |
| `apps/engine/src/http.rs` | control-plane HTTP |
| `apps/api/src/core.ts` | extended API core (writes, shape/subset/aggregate forwarding) |
| `packages/client/src/index.ts` | client: shapes/aggregations, tracked lifecycles, `awaitTxId` |
| `packages/client/src/subset.ts` | subset queries: page merge, LSN watermarks, tombstones, feed lifecycle |
| `docker/` | containerized stack (engine, durable-streams, API, Postgres) |
| `apps/pipeline-viz` | live pipeline explorer over `GET /graph` + `/state` + `/trace` |
