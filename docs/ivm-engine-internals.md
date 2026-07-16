# IVM engine internals: shapes, subqueries, and cost

Audience: engineers working on `apps/engine`. This is the as-built model of the
incremental-view-maintenance (IVM) engine — how a shape becomes a live, incrementally
maintained result set, how subqueries extend that across tables, and what each construct
costs as you add shapes, users, and rows.

It is grounded in the code (`apps/engine/src/`). `docs/ARCHITECTURE.md` is the system-level
companion (ingest, consistency fences, reliability, adapters); this document goes deeper on
execution and cost.

---

## 1. The as-built model in one page

Three layers, one idea — *maintain query results incrementally as the database changes*:

```
  app ──writes──▶ POSTGRES (system of record)
                     │  logical replication (streaming pgoutput slot, REPLICA IDENTITY FULL)
                     ▼
                  INGESTOR (replication.rs) ── decode commits → envelopes (old+new, commit LSN)
                     │  append
                     ▼
                  DURABLE STREAMS   changes   (the single ordered change log)
                     │  tail (one LSN-ordered sequencer, all tables)
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
reasoning about cost. State that scales with row count lives in Postgres, full stop; the
engine keeps only per-shape metadata, the counts pipelines' (group → count) relations, and,
for subqueries, a shared set of *inner-query result values*.
Baseline engine RSS is ~19 MiB whether the database has 1,000 or 100,000 rows
(measured by the shape-memory matrix benchmark in `packages/bench`).

### What dbsp is here

The data model is [`dbsp`](https://crates.io/crates/dbsp)'s: rows carry signed weights and a
change is a **Z-set delta** (`Row`, a positional `Vec<Value>`; `Tup2<Row, ZWeight>`; and
`ZWeight`, a signed `i64` multiplicity). The engine runs dbsp at exactly one place — **one
shared, in-memory circuit per engine** (`src/arrangements.rs`) holding **counts pipelines**
(a live COUNT per group projection, O(distinct groups) state, reseeded each boot from a
group-aggregated Postgres snapshot). Row data lives in Postgres: subquery flip re-derivations
and membership move-ins are pooled, parallel Postgres query-backs (`engine/membership.rs`).
See `ARCHITECTURE.md` §6b.

There are deliberately **no per-shape circuits and no per-shape threads**: the routing and
fallback tiers are plain Rust — key routing and stateless tri-valued predicate evaluation.
A shape whose predicate an index can route (equality templates, conjunct-indexed standalone
predicates) is *cheaper* outside the circuit: the index finds a change's shapes in
`O(log N)`, whereas a circuit shape pays a linear scan of every delta. Circuit structure
must never scale with shapes, users, or parameter combinations — only with the app's query
*templates*.

### A change is a Z-set delta

Every table change is converted (`apply_envelope`, `engine/output.rs`) into weighted rows:

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

The engine creates a `pgoutput` logical-replication slot (+ a `<slot>_pub` publication) and
**streams** it over the walsender protocol (push delivery — no poll interval). It buffers each
transaction between `Begin` and `Commit` and stamps every change with its transaction's
**COMMIT LSN** (not the per-change record LSN). It appends the resulting `Envelope`s (with
`old` + `new`) to the `changes` durable stream, and only **then** acknowledges the commit
to Postgres (`confirmed_flush_lsn`). A failed append tears the connection down unacknowledged;
the server resends from the confirmed position (append-then-acknowledge).

The `Envelope` (`ds.rs`) is the unit on every stream:

```
Envelope { type, key, value, old, headers { operation, txid, offset, lsn } }
```

### 2.2 Backfill and the snapshot gate (xid-visibility reconciliation)

When a shape is created, its initial rows are read from Postgres in a single `REPEATABLE READ`
snapshot (`pg.rs::backfill`), with the predicate **pushed into the `SELECT`**. Text-mapped
columns are read with `::text` casts (`row_json_expr`) so backfilled values are byte-identical
to what pgoutput's text-mode tuples carry on the live path (timestamps, uuids, jsonb — a representation
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

### 2.3 Tail and fan-out (`engine/sequencer.rs`)

There is **one sequencer task for all tables** (`sequencer_loop`) — the LSN-ordered executor.
It long-polls the single `changes` log (whole commits, in commit order) and for each change:

1. **de-duplicate**: skip if the envelope's `(commit lsn, seq)` is at/below the sequencer's
   global highwater — the ingestor's delivery is at-least-once (unacknowledged commits
   re-deliver after a reconnect), and deltas are not idempotent for aggregates/subquery
   weights;
2. build the delta (`apply_envelope`);
3. skip per shape via its `SnapshotGate` (§2.2);
4. evaluate **standalone** filters, **family** routers, **aggregations**, and the **subquery
   registry** (§3);
5. stage output envelopes in `pending: HashMap<stream_path, Vec<Envelope>>`;
6. `flush_pending` appends them, bounded-concurrently (CAP=32) across streams, with
   **`append_reliable`** — transient storage failures retry with capped backoff
   (backpressuring the sequencer) rather than dropping; the only non-retried case is 404 (the
   shape was dropped mid-flush). A dropped shape append would be a permanent divergence for
   every subscriber, so the batch is not "processed" until every append lands.

Output is grouped per shape by pk (`translate_output`): any positive-weight row → an `upsert`
envelope (the row entered or updated the result), a purely negative pk → a `delete` (it left).
Each envelope keeps its originating `txid` (`awaitTxId`) and its commit `lsn` (subset
positioning).

Processing is **serial in commit order across all tables** (one task), which makes global
ordering and state trivially correct, and each source transaction's appends are flushed before
the next transaction is processed — **per-transaction atomic emission**; the only intra-txn
parallelism is the append flush. After a batch is fully fanned out **and every append has
landed**, the sequencer publishes its processed offset as a sound convergence barrier. Shape
creation is two-phase (pending buffer → concurrent pooled backfill → gated activation) so a
backfill never stalls the pipeline.

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

State retained: **none**. Cost: output-sensitive — a **necessary-conjunct index** maps each
change to the candidate shapes whose indexed conjunct (an equality, or a range bound) it
satisfies; only candidates are evaluated. Predicates with no indexable conjunct (top-level
OR/NOT, `LIKE`, `!=`, `IS NULL`, match-all) stay on a fallback scan list and are evaluated on
every change, so a deployment leaning on many such shapes still degrades linearly (see §4.4).

### 3.3 Subqueries → shared inner-set nodes (`subquery.rs`)

A subquery shape's `WHERE` contains `col [NOT] IN (SELECT proj FROM inner WHERE …)`, possibly
nested. The registry serves every subquery form — it is the one membership implementation.
Subqueries are inherently cross-table (a change to `inner` moves rows of the outer table), so
they route through one shared `Arc<Mutex<SubqueryRegistry>>` the sequencer calls.

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
   reconcile a parent node (recurse) or re-evaluate the outer shape predicate. This propagation
   runs **deferred, on a semaphore-bounded worker pool** (`ELECTRIC_CIRCUITS_FLIP_WORKERS`) — the
   sequencer only collects the flips; the query-backs run concurrently against pooled Postgres
   and never hold the registry lock. Evaluation and the **enqueue** of the resulting envelopes
   happen atomically under the lock, and per-stream FIFO emission lanes make append order equal
   eval order per shape. The in-flight count (flips + enqueued-but-unlanded batches) is the
   extra convergence-barrier term (`GET /replication/lsn` → `pendingFlips`).
3. **`table` is a SubqueryShape's outer table:** evaluate the shape filter on the delta with
   `matches_ctx` (subquery leaves consult node sets) — the normal enter/leave/update path.

**The critical correctness rule:** outer membership is emitted **absolutely**, not as a delta
— `emit_shape_delta` emits each touched pk's *current* membership (`upsert` if it now matches,
else idempotent `delete` by pk). Because deferred flip propagation runs out of commit order,
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
| Per-change compute | `O(log N)` routed | output-sensitive via conjunct index (`O(K)` fallback for un-indexable predicates) | node reconcile + keyed query-back per flipped value |
| Table copies | 0 | 0 | 0 |

### 3.5 Full shape de-duplication (the sharing layer above the strategies)

Independent of strategy, any two **equal** shapes share ONE shape id, ONE maintained
routing/registry entry, and ONE durable stream, ref-counted (`engine/mod.rs::feed_by_sig` /
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
Per-change cost is output-sensitive like the standalone tier: aggregates are indexed by a
necessary conjunct (`engine/sequencer.rs::TableExec::agg_index`), so only candidate aggregates
are folded per change — match-all / un-indexable predicates stay on the always-candidate scan
list. Both aggregate tiers (this fold and circuit-served counts) emit through one shared wire
envelope (`engine/output.rs::agg_envelope`).

---

## 4. Cost analysis — pipeline growth

The question this section answers: **as you add shapes, users, and rows, where does work and
state accumulate, what is shared vs per-shape vs per-user, and how does that show up in memory
and disk?**

The headline, measured (shape-memory matrix benchmark, `packages/bench`): a steady fleet of *many* shapes
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

From the shape-memory matrix run (Postgres mode, OTel RSS probe):

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
- **Standalone:** output-sensitive. The necessary-conjunct index prunes to candidates by
  equality hash lookup / ordered range-bound scan; each candidate then pays one full predicate
  eval. Only shapes with no indexable conjunct (top-level OR/NOT, `LIKE`, `!=`) are evaluated
  on every change — that fallback list is the remaining `O(K)` term to watch.
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

The engine holds **no disk state of its own**: row data lives in Postgres, and the circuit's
counts pipelines are in-memory (O(distinct groups)), reseeded on boot from a group-aggregated
Postgres snapshot (`ARCHITECTURE.md` §6b). Per-shape routing metadata, subquery contributor
sets, and running aggregates are the only retained structures, and they are small (§4.2).

What IS durable is the **shape catalog** (`meta/catalog`, an append-only durable stream of
create/join/leave/drop events plus change-log offset checkpoints): at boot the engine replays
it and re-registers its shapes itself, so a restart is not a client re-registration storm.
Plain/routed shapes resume with passthrough gates and replay the change log from the persisted
offset (crash-window re-emission is idempotent absolute upserts); aggregates re-seed their fold
from a fresh Postgres snapshot whose gate then skips the replayed history (circuit-served
COUNTs re-seed from the reseeded counts pipelines). Subquery shapes are
deliberately NOT restored — their inner-node contributor state is not persisted, and a
fresh-seeded node cannot detect flips that happened while the engine was down (stale move-outs
would persist forever) — so they are dropped loudly and clients recreate them. The catalog
stream is never compacted (append-only); if event volume ever matters, snapshot+truncate or an
embedded KV (redb/lmdb/sled) is the natural next step.

In short: **the memory story is "keep nothing big resident"** — nothing the engine holds
scales with table size; everything row-scale lives in Postgres.

---

## 5. Worked example: LinearLite per-user visibility

The flagship example (`examples/linearlite`, verified in-browser at 100k issues) makes a user
see only issues in projects they're a member of. With the demo's default circuit config this
shape is circuit-served (`docs/linearlite-circuit-design.md`); the walkthrough below traces
the registry path — what the same shape costs with the circuit off. It is a subquery shape:

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

1. Insert into `project_members` reaches the sequencer → registry reconciles the node →
   value `project_id = P` flips **enter** (its contributor set went `∅→{pk}`).
2. For the edge, the engine queries the outer rows that could change:
   `SELECT … FROM issues WHERE project_id = P`, re-evaluates the shape predicate, and emits
   `upsert` for each now-visible issue → appended to `shape/<id>`.
3. The client's TanStack DB collection receives the upserts; the board updates live.

**Live cost (an issue moves projects):** reaches the sequencer → `matches_ctx` checks
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

## 6b. Design decision: no interpreted operator graph in the dynamic tier

*(July 2026, closing the structural-debt epic. Context: after the engine.rs split and the
membership/aggregate kernel unifications, the remaining critique of the dynamic tier was that
`process_envelope` is a fixed sequence of executor loops rather than a composable, interpreted
dataflow graph — and that the visualizer's operator graph is derived presentation, not the
execution model.)*

**Decision: the dynamic tier stays flat — indexed executors + shared kernels, no hand-rolled
interpreted operator graph.** Rationale:

- **dbsp is already the composition engine.** New template *kinds* (joins, reductions,
  derived-visibility pipelines) are new operators in `arrangements.rs`, where dbsp provides
  compositional correctness, arrangement sharing, and spilling. A dynamic-tier interpreter
  would be a second dataflow engine beside the real one, with none of its guarantees.
- **Flatness is the design, not debt.** The engine's central performance idea is that routing
  is *not* dataflow: an index finds a change's shapes in `O(log N)`, whereas graph-shaped
  evaluation passes deltas per node — the per-shape linear scan this design exists to avoid
  (§1, the same argument that keeps equality shapes out of the circuit).
- **No variation to abstract over.** The executor kinds (filter, router, fold, membership,
  subquery hook) have been stable; they grow in *count* (solved by the conjunct indexes), not
  in *kind*. The hard correctness — gates, absolute emission, the `(lsn,seq)` highwater,
  per-txn flush — is cross-cutting and would not be simplified by node interfaces.
- **The composability mechanism is the kernel pattern.** When two paths must agree on an
  invariant, extract ONE implementation plus a cross-path regression test —
  `engine/membership.rs` (flip detection, query-backs, absolute-emission fold) and
  `engine/output.rs::agg_envelope` (the aggregate wire format) are the pattern to follow.

**Revisit iff** runtime-defined per-shape pipelines enter the roadmap (user-supplied computed
projections / chained transforms that cannot compile to deploy-time circuit templates) — that
is the one workload where an interpreted, per-shape mini-graph earns its indirection.
Optional follow-up, independent of this decision: derive the visualizer's `OpNode`s from the
executors via a trait so presentation cannot drift from execution.

---

## 7. File map

| path | role |
|---|---|
| `apps/engine/src/engine/` | the engine module: `mod.rs` (the `Engine` handle + shared state), `sequencer.rs` (the LSN-ordered sequencer, (lsn,seq) de-dup, per-txn reliable flush), `lifecycle.rs` (shape creation/sharing/retention), `circuit_serving.rs` (circuit-tier serving), `executors.rs` (routers, filters, folds), `planning.rs` (circuit placement), `catalog.rs` (durable catalog + restore), `introspection.rs` (graph/state DTOs + builders), `membership.rs` (the shared membership kernel: flip detection, pooled Postgres query-backs), `emission.rs` (per-stream ordered emission lanes), `output.rs` (envelope ⇄ delta codec) |
| `apps/engine/src/subquery.rs` | cross-table subquery registry: shared nodes, edges, flips, absolute emission, atomic create/rollback |
| `apps/engine/src/arrangements.rs` | the circuit: in-memory dbsp counts pipelines, group-aggregated boot seeding |
| `apps/engine/src/replication.rs` | Postgres logical-replication ingestor (streaming pgoutput via the `pgoutput.rs` decoder, buffer per-txn, stamp commit LSN + xid + seq, append, acknowledge) |
| `apps/engine/src/pg.rs` | connect/introspect, `REPLICA IDENTITY FULL`, slot create, predicate-pushdown backfill + `SnapshotGate`, subset query-back |
| `apps/engine/src/predicate.rs` | predicate compile, three-valued `matches`/`matches_ctx`, `equality_template`, subquery signatures |
| `apps/engine/src/sql.rs` / `where_sql.rs` | predicate → SQL (backfill pushdown); SQL `WHERE` → predicate (Electric path) |
| `apps/engine/src/schema.rs` | schema, composite PK (`pk_cols`, `\u{1f}` key join), JSON⇄Row |
| `apps/engine/src/value.rs` | `Value`, `Row` (the dbsp Z-set element) |
| `apps/engine/src/ds.rs` | durable-streams HTTP client + `Envelope` |
| `apps/engine/src/electric.rs` / `http.rs` | `/v1/shape` Electric adapter + control-plane HTTP |
| `apps/engine/src/metrics.rs` / `mem.rs` | counters, latency histograms, OTel memory/cardinality gauges |
| `apps/engine/src/retention.rs` | shape retention: the active / dormant / evicted lifecycle + layered dormant-only eviction |
| `apps/engine/src/config.rs` | boot config: `ELECTRIC_CIRCUITS_*` env + Electric fleet-surface mapping |
| `apps/engine/src/params.rs` | Electric `params[N]` / `$N` substitution for `/v1/shape` |
| `apps/engine/src/statsd.rs` | StatsD (datadog wire) telemetry for the benchmarking fleet |
| `apps/engine/src/trace.rs` | per-envelope pipeline trace broadcast (`GET /trace` SSE, feeds the explorer) |

## 8. Related documents

- `docs/ARCHITECTURE.md` — the system-level architecture (consistency fences, reliability,
  adapters) and the speedup backlog.
- `packages/bench/README.md` — the benchmark runners, including the shape-memory matrix that
  produced the memory-vs-shapes data used above.
- `docs/shapes-and-subqueries-guide.md` — the user-facing companion to this document.
