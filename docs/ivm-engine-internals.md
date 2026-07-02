# IVM engine internals: shapes, subqueries, and cost

Audience: engineers working on `apps/engine`. This is the as-built model of the
incremental-view-maintenance (IVM) engine — how a shape becomes a live, incrementally
maintained result set, how subqueries extend that across tables, and what each construct
costs as you add shapes, users, and rows.

It is grounded in the code (`apps/engine/src/`) and the design records under
`docs/superpowers/specs/`. `docs/ARCHITECTURE.md` is the system-level companion (ingest,
consistency fences, reliability, adapters); this document goes deeper on execution and cost.

---

## 1. The as-built model in one page

Three layers, one idea — *maintain query results incrementally as the database changes*:

```
  app ──writes──▶ POSTGRES (system of record)
                     │  logical replication (test_decoding slot, REPLICA IDENTITY FULL)
                     ▼
                  INGESTOR (replication.rs) ── decode commits → envelopes (old+new, commit LSN)
                     │  append
                     ▼
                  DURABLE STREAMS   table/<name>   (the change log)
                     │  tail (one task per table)
                     ▼
                  ENGINE  ── route/filter each delta to matching shapes ──┐
                     │  append matched upsert/delete                      │
                     ▼                                                     │
                  DURABLE STREAMS   shape/<id>   (the per-shape feed)      │
                     │  read / long-poll                                  │
                     ▼                                                     │
                  CLIENT  stream-db + TanStack DB → live materialized set  │
```

**The engine holds no copy of any table.** This is the single most important fact for
reasoning about cost. State that scales with row count lives in Postgres; the engine keeps
only per-shape metadata and, for subqueries, a shared set of *inner-query result values*.
Baseline engine RSS is ~19 MiB whether the database has 1,000 or 100,000 rows
(`docs/bench/shape-memory-matrix.md`).

### What dbsp is, and isn't, here

The crate is named after [`dbsp`](https://crates.io/crates/dbsp) and the data model borrows
its vocabulary — rows carry signed weights and a change is a **Z-set delta**. But there is
**no running dbsp circuit and no per-shape thread**. dbsp survives only as the *value types*
for deltas: `Row` (a positional `Vec<Value>`), `Tup2<Row, ZWeight>`, and `ZWeight` (a signed
`i64` multiplicity) in `value.rs`. The live-maintenance path is plain Rust: key routing and
stateless tri-valued predicate evaluation. (An earlier design did run a shared dbsp join per
equality template; it was removed to drop the table-copy-per-template memory. See
`reduce-engine-memory-design.md`.)

### A change is a Z-set delta

Every table change is converted (`apply_envelope`, `engine.rs`) into weighted rows:

| operation | delta |
|---|---|
| insert | `[(new, +1)]` |
| update | `[(old, −1), (new, +1)]` |
| delete | `[(old, −1)]` |

`old` comes from the replication envelope (`REPLICA IDENTITY FULL` makes Postgres emit the
prior tuple), so no local table state is needed to retract a row.

---

## 2. The change pipeline, end to end

### 2.1 Ingest (`replication.rs`)

The engine creates a `test_decoding` logical-replication slot and **peeks** it
(`pg_logical_slot_peek_changes`, non-consuming). It buffers each transaction and stamps every
change with its transaction's **COMMIT LSN** (taken from the `COMMIT` record, not the
per-change record LSN). It appends the resulting `Envelope`s (with `old` + `new`) to the
`table/<name>` durable stream, and only **then** advances the slot. A failed append re-reads
rather than loses data (read-then-commit).

The `Envelope` (`ds.rs`) is the unit on every stream:

```
Envelope { type, key, value, old, headers { operation, txid, offset, lsn } }
```

### 2.2 Backfill and the snapshot gate (xid-visibility reconciliation)

When a shape is created, its initial rows are read from Postgres in a single `REPEATABLE READ`
snapshot (`pg.rs::backfill`), with the predicate **pushed into the `SELECT`**. Text-mapped
columns are read with `::text` casts (`row_json_expr`) so backfilled values are byte-identical
to what `test_decoding` prints on the live path (timestamps, uuids, jsonb — a representation
mismatch breaks retractions, key routing, and MIN/MAX multisets).

Live and backfill are reconciled by **transaction visibility** (`pg::SnapshotGate`), so every
row counts exactly once. The backfill statement captures `pg_current_snapshot()` +
`pg_current_wal_lsn()` atomically with the snapshot, and a replicated change is skipped **iff
its xid was visible to that snapshot** (xids on the slot are always committed, so: `xid < xmin`
→ skip; in the in-progress list or `≥ xmax` → process). Changes without a parseable xid
(library mode) fall back to the strict `commit_lsn < seed_lsn` comparison.

Why not LSN alone: a commit's WAL record is written (and `pg_current_wal_lsn()` moves past it)
*before* the transaction becomes visible to snapshots (`ProcArrayEndTransaction`, after the WAL
fsync). An LSN fence silently drops rows committed-but-invisible during the snapshot and
duplicates at the exact boundary; the xid gate decides both cases. Guarded by
`conformance-concurrency.test.ts` + `pg.rs` unit tests.

### 2.3 Tail and fan-out (`engine.rs`)

There is **one tailer task per table** (`tailer_loop`). It long-polls `table/<name>`, and for
each change:

1. **de-duplicate**: skip if the envelope's `(commit lsn, seq)` is at/below the tailer's
   highwater — the ingestor's delivery is at-least-once (re-appends after a partial failure or
   a crash before the slot advance), and deltas are not idempotent for aggregates/subquery
   weights;
2. build the delta (`apply_envelope`);
3. skip per shape via its `SnapshotGate` (§2.2);
4. evaluate **standalone** filters, **family** routers, **aggregations**, and the **subquery
   registry** (§3);
5. stage output envelopes in `pending: HashMap<stream_path, Vec<Envelope>>`;
6. `flush_pending` appends them, bounded-concurrently (CAP=32) across streams, with
   **`append_reliable`** — transient storage failures retry with capped backoff
   (backpressuring the tailer) rather than dropping; the only non-retried case is 404 (the
   shape was dropped mid-flush). A dropped shape append would be a permanent divergence for
   every subscriber, so the batch is not "processed" until every append lands.

Output is grouped per shape by pk (`translate_output`): any positive-weight row → an `upsert`
envelope (the row entered or updated the result), a purely negative pk → a `delete` (it left).
Each envelope keeps its originating `txid` (`awaitTxId`) and its commit `lsn` (subset
positioning).

Processing is **serial within a table** (one task), which makes ordering and state trivially
correct; the only intra-batch parallelism is the append flush. After a batch is fully fanned
out **and every append has landed**, the tailer publishes its processed offset as a sound
convergence barrier.

---

## 3. The three shape execution strategies

A shape is *one table + an optional `WHERE` predicate + an optional `columns` projection*. The
**shape of the predicate** decides which of three strategies runs it. This choice is the
backbone of the cost analysis: each strategy retains a different amount of state and pays a
different per-change cost.

### 3.1 Equality templates → key routing (shared)

If a predicate is a conjunction of non-null equality leaves on distinct columns
(`tenant = 7 AND region = 'eu'`), `equality_template()` returns the **key columns**
(`{tenant, region}`). All shapes sharing the same key-column *set* — regardless of the
constants — share **one `KeyRouter`** ("family"):

```
KeyRouter (per template key-column set):
  key_tuple ──▶ { shape_id → (stream_path, seed_lsn) }
```

- **Registration** inserts `(key_tuple, shape_id)` into the index and backfills the shape
  directly from Postgres (`SELECT … WHERE key = const`). No table trace is built.
- **Live routing:** compute `old_key`/`new_key` over the template columns from the envelope.
  Because an equality predicate matches a row iff its key equals the shape's constants,
  **key membership *is* shape membership**:
  - insert → upsert to shapes on `new_key`;
  - delete → delete from shapes on `old_key`;
  - update, key unchanged → upsert to shapes on that key;
  - update, key changed → delete from `old_key`, upsert to `new_key`.

Routing a change is `O(log N)` over the index (a hashmap/btree lookup), **independent of the
number of shapes** on other keys. Thousands of equality shapes collapse onto a handful of
routers — one per distinct *template*, not per shape. In the shape-memory matrix, 10,000
equality shapes use **3** family circuits (board-status on `status`, "my tasks" on `username`,
per-issue comments on `issue_id`).

State retained: `O(#shapes)` routing entries (a key tuple + a stream path + an LSN each).
**Zero table rows.**

### 3.2 Standalone → stateless tri-valued filter (per-shape compute, no state)

Anything that isn't an equality template — ranges, `OR`, `NOT`, inequalities, match-all — is a
`StandaloneShape`. A `WHERE` filter is stateless, so it needs no index and no state:

```rust
// eval_standalone: keep the delta tuples whose row matches the predicate
delta.iter().filter(|t| pred.matches(&t.0)).cloned().collect()
```

`matches` is SQL **three-valued logic** (`True`/`False`/`Unknown`): a NULL operand yields
UNKNOWN, AND/OR follow SQL truth tables, `NOT UNKNOWN = UNKNOWN`, and a row is included only
when the predicate is TRUE. Backfill pushes the same predicate into the `SELECT`.

State retained: **none**. Cost: `O(K)` predicate evaluations per change for `K` standalone
shapes on that table — every standalone shape is tested on every change (no pruning). This is
the one strategy whose *per-change compute* grows with shape count; it is CPU-bound and cheap
per eval, but it is the scaling watch-item (see §4.4 and the predicate-indexing idea in
`ARCHITECTURE.md` §9).

### 3.3 Subqueries → shared inner-set nodes (`subquery.rs`)

A subquery shape's `WHERE` contains `col [NOT] IN (SELECT proj FROM inner WHERE …)`, possibly
nested. Subqueries are inherently cross-table (a change to `inner` moves rows of the outer
table), so they route through one shared `Arc<Mutex<SubqueryRegistry>>` that every tailer calls.

**Node — the shared, maintained inner set.** A `SubqueryNode` materializes
`SELECT proj FROM inner WHERE pred` as a value set, keyed by a canonical signature
`sig = (inner_table, proj_col, canonical(pred))`. Two subqueries with equal `sig` share **one**
node (refcounted):

```
SubqueryNode {
  sig, inner_table, proj_col, pred,
  contributors: HashMap<Value, HashSet<pk>>,  // value present ⟺ its contributor set is non-empty
  has_null, seed_lsn, refcount,
}
```

Tracking contributor **pks** (not a bare count) makes maintenance *reconcile-by-identity*: set a
row's presence to equal `match(row)` regardless of history — idempotent and order-independent.

**Edges form a DAG.** Each `col IN node` leaf is an edge `(dependent, connecting_col, node,
negated)`. A dependent is either an outer `SubqueryShape` or a **parent node** (enabling nested
subqueries).

**Maintenance — one rule, applied recursively** (`on_table_delta`):

1. **`table` is a node's `inner_table`:** reconcile each changed inner row's pk into the node;
   record per-value **flips** (`∅→nonempty` = *enter*, `nonempty→∅` = *leave*).
2. **For each flipped value `v` of node `N`:** for every edge on `N`, the affected dependent
   rows are exactly those with `col = v`. Query them (`SELECT … WHERE col = v`) and either
   reconcile a parent node (recurse) or re-evaluate the outer shape predicate.
3. **`table` is a SubqueryShape's outer table:** evaluate the shape filter on the delta with
   `matches_ctx` (subquery leaves consult node sets) — the normal enter/leave/update path.

**The critical correctness rule:** outer membership is emitted **absolutely**, not as a delta
— `emit_shape_delta` emits each touched pk's *current* membership (`upsert` if it now matches,
else idempotent `delete` by pk). Because per-table tailers process out of global commit order,
a delta-based "delete only if the old row matched" would miss move-outs. Absolute emission
converges regardless of cross-table order, which is why we don't need Electric's LSN-buffering /
row-tag streaming protocol — our conformance asserts *convergence after drain*, not a control-
message stream.

State retained: `O(inner result size)` contributor pks per node, **shared** across all shapes
that reference the same inner query. The outer shape stores nothing extra; affected rows are
fetched by a keyed query-back on flip.

### 3.4 Strategy summary

| | equality (router) | standalone | subquery |
|---|---|---|---|
| Predicate | `a=1 AND b=2` | ranges, OR, NOT, ≠ | `col [NOT] IN (SELECT …)` |
| State retained | `O(#shapes)` routing entries | none | `O(inner-set)` pks, **shared** |
| Shared across shapes? | yes — 1 router / template | no (but no state to share) | yes — 1 node / `sig` |
| Per-change compute | `O(log N)` routed | `O(K)` evals (all standalone shapes) | node reconcile + keyed query-back per flipped value |
| Table copies | 0 | 0 | 0 |

### 3.5 Full shape de-duplication (the sharing layer above the strategies)

Independent of strategy, any two **equal** shapes share ONE shape id, ONE maintained
routing/registry entry, and ONE durable stream, ref-counted (`engine.rs::feed_by_sig` /
`feed_shares`). The signature is `(kind, table, canonical predicate, sorted projection,
changes_only)` — canonicalization is order-insensitive, so `a AND b` ≡ `b AND a`; aggregations
key on `(table, predicate, fn, column)` in their own namespace. Consequences:

- N clients opening the same shape cost one maintenance path and one append per change —
  per-subscriber cost collapses onto per-*distinct*-shape cost everywhere in §4.
- A joiner waits on the share's **ready-watch** until the creator's backfill has landed (and
  observes a creation *failure* as an error, never a dead stream).
- Deletes decrement; the **last** drop removes the routing/registry entry AND deletes the
  durable stream (otherwise every dropped shape leaks a stream on the storage server).
- Creation is **atomic**: on any failure the record, share entries, and (for subqueries) every
  node refcount/edge/pending-seed added by the attempt are rolled back
  (`subquery.rs::rollback_create`).
- The Electric `/v1/shape` adapter passes `share=false` (its protocol needs per-request
  handles); everything else shares by default.

### 3.6 Aggregations (extended API)

A scalar COUNT/SUM/AVG/MIN/MAX over a non-subquery predicate (`AggShape`), maintained as a fold
over the delta: COUNT/SUM/AVG are running scalars; MIN/MAX keep a `value → net-weight` multiset
so retracting the current extreme restores the previous one. SQL NULL semantics exactly:
aggregates ignore NULL values, `COUNT(col)` counts non-NULLs (`COUNT(*)` counts rows), AVG
divides by the non-NULL count, and SUM/AVG/MIN/MAX over zero non-NULL values are NULL. The feed
is a single-row stream (`{ value, n }`, key `"agg"`), emitted only when the value changes.
State: O(1) (+ O(distinct values) for MIN/MAX). Identical aggregations share one fold (§3.5).

---

## 4. Cost analysis — pipeline growth

The question this section answers: **as you add shapes, users, and rows, where does work and
state accumulate, what is shared vs per-shape vs per-user, and how does that show up in memory
and disk?**

The headline, measured (`docs/bench/shape-memory-matrix.md`): a steady fleet of *many* shapes
over a *large* table is cheap; the only deployment-size-sensitive cost is the **transient
backfill working set** of a *materialized* shape.

### 4.1 What is shared vs per-shape vs per-user

| construct | granularity | cost driver |
|---|---|---|
| Engine baseline (no shapes) | global | constant ~19 MiB, **independent of table size** (no table copy) |
| `KeyRouter` (family) | per **template** (key-column set) | a handful; *not* per shape and *not* per user |
| Routing entry | per **shape** | one `(key_tuple, stream_path, seed_lsn)` |
| `StandaloneShape` | per **shape** | one predicate + metadata; per-change eval cost |
| `SubqueryNode` | per distinct **inner query** (`sig`) | `O(inner result size)` contributor pks; shared by refcount |
| Subquery contributors | per inner **row** that contributes a value | one pk in a `HashSet` |
| Subquery edge | per dependent (shape or parent node) | one DAG edge |
| Per-shape stream | per **shape** | one `shape/<id>` durable stream (storage, not engine RAM) |

"Per user" is not a first-class concept in the engine — it shows up as *the shapes a user
opens*. The LinearLite model opens ~10 shapes/user. In the matrix run, **1,000 users → 10,000
shapes → 1,000 subquery nodes, 6,000 contributors, 1,000 edges, but still only 3 family
circuits.** The router count is flat in users; node/contributor/edge counts grow linearly in
users but with a tiny constant (a node holds the user's ~6 membership rows, not any issues).

### 4.2 Memory: the numbers

From `docs/bench/shape-memory-matrix.md` (Postgres mode, OTel RSS probe):

- **Baseline RSS is independent of deployment size:** ~18.7 MiB at 1k issues, ~19.0 at 10k,
  ~18.7 at 100k. The engine keeps no table copy.
- **Per-shape registration is ~0.7–0.9 KiB/shape and constant across deployment sizes.** Even
  10,000 changes-only shapes grow RSS by **< 10 MiB**.
- **Family circuits stay at a small constant** (3 here) no matter how many equality shapes
  share them.
- **Backfill is the deployment-size-sensitive cost:** a *materialized* shape's one-off backfill
  working set scales ~linearly with the number of *visible* rows, ≈ **2 KiB/visible-row** peak
  (e.g. a 12,000-visible-issue visibility shape over a 100k-issue DB peaks ~22 MiB above
  baseline, then settles). This is transient read-batch + JSON serialization memory, **not
  retained state**.

So engine memory is, to first order:

```
RSS ≈ baseline(~19 MiB)
    + ~0.8 KiB × (#shapes)
    + Σ over nodes (inner-set size × pk size)        // shared, tiny for visibility-style subqueries
    + peak concurrent backfill working set (~2 KiB × visible-rows-per-shape, transient)
```

### 4.3 Sizing rule

**Budget by concurrent backfill working set, not by shape count or table size.** A steady fleet
of many shapes over a large table is cheap; a *burst of large materialized backfills* is the
spike to provision for. Two levers reduce the spike:

- **`changes-only` / subset feeds skip the backfill entirely** — they pay only registration
  (~0.8 KiB) and live deltas.
- The **`columns` projection** narrows each backfilled/synced row to the columns a view needs
  (the pk is always kept), cutting both the backfill working set and the synced payload.

A caveat from the matrix: RSS is a coarse, non-monotonic signal (allocator slack; freed pages
return to the OS at the allocator's discretion). For steady-state sizing, rely on the OTel
*cardinality* gauges (`engine_shapes`, `engine_subquery_nodes`,
`engine_subquery_contributors`, `engine_family_circuits`) to read retained structural state
independent of allocator noise; measure RSS after warmup.

### 4.4 Per-change (hot-path) compute

For one table change:

- **Routed (equality):** `O(log N)` to find the shapes on the changed key, then one append per
  matched shape. Independent of shapes on other keys.
- **Standalone:** `O(K)` predicate evaluations, `K` = standalone shapes on that table. This is
  the term that grows with shape count on the live path. Each eval is cheap (a compiled
  predicate tree over a positional row), but there is no pruning. If a deployment leans on many
  distinct *range* shapes, this is the first thing to index (`ARCHITECTURE.md` §9: index
  standalone predicates by `(column, op)` to turn `O(K)` into output-sensitive).
- **Subquery:** an inner change reconciles the node (`O(1)` per changed inner row) and, per
  *flipped value*, runs one keyed `SELECT … WHERE col = v` per dependent edge plus a re-eval.
  An outer change is `matches_ctx` = `O(1)` node lookups per subquery leaf. The pathological
  case is a very low-selectivity inner value referenced by many outer rows (large fan-out on
  flip) — the inherent cost any correct incremental subquery maintenance pays.

The append/storage path — not engine compute — is the throughput ceiling under max load
(telemetry shows < 1 ms p99 internal per-change cost at 100k shapes). The flush coalesces per
stream and parallelizes across streams (CAP=32) precisely because HTTP round-trips to storage
dominate.

### 4.5 Disk

The engine does **not** currently page state to disk in a way you can rely on for memory
bounding. dbsp 0.299 has a storage subsystem (on-disk columnar batches), and it is wired in,
but `ARCHITECTURE.md` §10 records an honest negative result: on our ephemeral, hand-built
circuit it does **not** offload the steady-state working set, and the `FelderaCache` / forced-
spill knobs made RSS *worse*. Effective spill appears tied to dbsp's persistent-id/checkpoint
machinery we don't use. Do **not** ship `MIN_BYTES=0` or `FelderaCache`. The higher-confidence
"run from disk" path, if needed, is to back the engine's own per-shape/routing metadata with an
embedded KV (redb/lmdb/sled) — but note that, post routing model, there is little resident state
left to spill: the table data that used to dominate is gone.

In short: **disk is not a tuning lever today; the memory story is "keep nothing big resident,"
and the engine already does.**

---

## 5. Worked example: LinearLite per-user visibility

The flagship example (`examples/linearlite`, verified in-browser at 100k issues) makes a user
see only issues in projects they're a member of. That is a subquery shape:

```jsonc
// issues visible to user u
{ "table": "issues",
  "where": { "col": "project_id",
             "in": { "table": "project_members",
                     "project": "project_id",
                     "where": { "col": "user_id", "op": "eq", "value": "u" } } } }
```

**Setup cost (one user opens this shape):**

- One `SubqueryNode` for `sig = (project_members, project_id, user_id = u)`. Seeded from
  Postgres: the user's ~6 membership rows → up to 6 contributor pks across ≤6 distinct
  `project_id` values. **~6 pks, not any issues.**
- One edge `(this shape, project_id, node, negated=false)`.
- One routing/shape entry + one `shape/<id>` stream.
- If materialized: a backfill of the *visible* issues (~2 KiB/visible-row, transient). If
  `changes-only`, no backfill.

**Live cost (membership changes — user added to a project):**

1. Insert into `project_members` lands on that table's tailer → registry reconciles the node →
   value `project_id = P` flips **enter** (its contributor set went `∅→{pk}`).
2. For the edge, the engine queries the outer rows that could change:
   `SELECT … FROM issues WHERE project_id = P`, re-evaluates the shape predicate, and emits
   `upsert` for each now-visible issue → appended to `shape/<id>`.
3. The client's TanStack DB collection receives the upserts; the board updates live.

**Live cost (an issue moves projects):** lands on the `issues` tailer → `matches_ctx` checks
`new.project_id ∈ node` and `old.project_id ∈ node` → emits `upsert`/`delete` accordingly.
`O(1)` node lookups, no inner-table query.

**Scaling tally (the matrix's 1,000-user run):** 1,000 such shapes → 1,000 nodes, 6,000
contributors, 1,000 edges, ~8 MiB total RSS growth, **3** family circuits for the *other*
(equality) shapes those users open. The visibility subquery is per-user by nature (each user's
membership set differs, so each gets its own node), but each node is tiny and the per-change
cost is bounded by the fan-out of the specific project that changed.

---

## 6. Subset queries (the non-materialized counterpart)

Not every read needs a live shape. A **subset query** (`Engine::query_subset` →
`pg::query_subset_where`) is a one-shot `SELECT … WHERE … ORDER BY … LIMIT … OFFSET` returning
a page of rows + a snapshot LSN — **no shape, no stream, no retained state.** `orderBy` and
`limit` are knobs of *subset queries*, not of shapes (a common doc trap — shapes do not have a
top-N operator). Subquery predicates in subset queries are evaluated natively by Postgres via
`predicate_json_to_sql`. This is the basis for windowed / infinite-scroll sync: each page is a
bounded keyset range query folded into the `WHERE`, so the engine never holds a stateful top-N.

---

## 7. File map

| path | role |
|---|---|
| `apps/engine/src/engine.rs` | tailers (+ (lsn,seq) de-dup), delta computation, key routing + standalone + aggregation fan-out, shape sharing/lifecycle, reliable flush |
| `apps/engine/src/subquery.rs` | cross-table subquery registry: shared nodes, edges, flips, absolute emission, atomic create/rollback |
| `apps/engine/src/replication.rs` | Postgres logical-replication ingestor (capped peek, buffer per-txn, stamp commit LSN + xid + seq, append, advance) |
| `apps/engine/src/pg.rs` | connect/introspect, `REPLICA IDENTITY FULL`, slot create, predicate-pushdown backfill + `SnapshotGate`, subset query-back |
| `apps/engine/src/predicate.rs` | predicate compile, three-valued `matches`/`matches_ctx`, `equality_template`, subquery signatures |
| `apps/engine/src/sql.rs` / `where_sql.rs` | predicate → SQL (backfill pushdown); SQL `WHERE` → predicate (Electric path) |
| `apps/engine/src/schema.rs` | schema, composite PK (`pk_cols`, `\u{1f}` key join), JSON⇄Row |
| `apps/engine/src/value.rs` | `Value`, `Row` (the dbsp Z-set element) |
| `apps/engine/src/ds.rs` | durable-streams HTTP client + `Envelope` |
| `apps/engine/src/electric.rs` / `http.rs` | `/v1/shape` Electric adapter + control-plane HTTP |
| `apps/engine/src/metrics.rs` / `mem.rs` | counters, latency histograms, OTel memory/cardinality gauges |

## 8. Related records

- `docs/superpowers/specs/2026-06-29-reduce-engine-memory-design.md` — why the table-copy /
  per-template circuit model was removed (the routing model this doc describes).
- `docs/superpowers/specs/2026-06-29-subqueries-design.md` — the subquery node/edge/flip design
  and the absolute-emission correctness argument.
- `docs/superpowers/specs/2026-06-29-postgres-logical-replication.md` — ingest + commit-LSN
  reconciliation.
- `docs/bench/shape-memory-matrix.md` — the measured memory-vs-shapes data used above.
- `ARCHITECTURE.md` §9–§10 — speedup backlog and the dbsp-storage negative result.
- `docs/shapes-and-subqueries-guide.md` — the user-facing companion to this document.
