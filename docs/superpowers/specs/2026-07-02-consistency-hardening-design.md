# Consistency hardening: design review findings + fixes

**Date:** 2026-07-02. **Scope:** an adversarial design review of the whole pipeline (shared-state
de-duplication + fan-out), the confirmed integrity/consistency bugs it found, and the fixes. All
fixed in this pass unless marked *residual*.

## Method

Four independent review passes (engine core + sharing; subquery registry; replication/backfill;
Electric adapter; client subset seam + lifecycles), each producing concrete failure scenarios, then
fixes with regression tests. Baselines: 36 Rust / 114 TS tests green before; 55+ Rust / 133+ TS
green after (plus Electric's own oracle property + integration tests).

## Findings and fixes

### 1. The backfill↔live fence was unsound (critical)

`seed_lsn = pg_current_wal_lsn()` is a WAL *write* position; snapshot visibility is decided at
`ProcArrayEndTransaction`, after the commit record is fsynced. A transaction committed-but-not-yet-
visible during the backfill snapshot had `commit_lsn < seed_lsn` and was skipped → **row dropped
from both backfill and live, permanently**. The mirror case: a visible commit exactly at the
boundary (`end_lsn == seed_lsn`) replayed as a duplicate.

**Fix: `pg::SnapshotGate`** — the backfill statement captures `pg_current_snapshot()` atomically
with the snapshot; a replicated change is skipped **iff its xid was visible to that snapshot**
(every xid on the slot is committed, so visibility = `xid < xmin`, or in `[xmin, xmax)` and not
in-progress). Every seeded structure (routed/standalone shapes, aggregations, subquery nodes and
shapes) carries a gate; `changes_only` feeds carry a passthrough. LSN comparison survives only as
the fallback for changes with no parseable xid (library mode). The ingestor now stamps each change
with its transaction xid.

*Residual:* the client-side **subset page ↔ live tail** seam still positions by LSN watermarks; the
same (much narrower, self-healing on next row touch) window applies there. Closing it needs the
subset query-back to return the snapshot's xip list and the client to fence by txid — deliberate
follow-up, not done in this pass.

### 2. At-least-once redelivery corrupted non-idempotent consumers (critical)

The ingestor appends then advances the slot; a crash between the two (or a partial multi-table
append failure) re-appends whole batches. Deltas are not idempotent for aggregates (double-count)
or subquery contributor weights (phantom membership).

**Fix:** each change is stamped `(commit lsn, xid, seq=position-in-txn)`; `(lsn, seq)` is strictly
increasing per table stream, and each tailer keeps a highwater mark, skipping anything at/below it.
The drain-barrier sentinel is published only after the slot actually advanced. The peek is now
capped (5000, escalating for oversized transactions) so a backlog doesn't materialize in memory.

### 3. Shape-stream appends could drop silently (critical)

`flush_pending` logged-and-dropped failed appends, then published the processed offset — permanent,
undetectable divergence for that shape's subscribers (the convergence barrier false-greened).

**Fix: `ds::append_reliable`** — retry with capped backoff (backpressure, matching the ingestor's
read-then-commit stance); 404 = shape dropped mid-flush = clean discard. Used by the tailer flush
and all subquery registry emission paths. Idempotent for readers (absolute per-pk envelopes).

### 4. Failed creations left zombie shapes that pinned their signature (high)

`create_shape` ignored backfill/registration failures: the shape stayed registered, streamed
nothing, and — worse under full de-duplication — every future identical create silently joined the
dead feed. Subquery creation could also fail half-way, leaving orphaned node refcounts/edges and
permanently-unseeded nodes.

**Fix:** creation is atomic. The tailer's `ready` channel carries a `Result`; on failure the record,
share entries, tailer registration, and stream are removed and the error propagates. The subquery
registry logs every refcount increment per create (`collect_log`) and rolls back exactly on failure.
Joiners of a shared shape now wait on a ready-watch — they see a live, backfilled stream or the
creation error, never an unbackfilled or dead stream.

### 5. The shape-count leak: clients never deleted; drops could also steal (high)

The observed "drops lag creates" leak was client-side: `shape().close()` never called
`shapes.delete` (every materialized shape a permanent refcount +1); `createSubset` leaked its
just-created feed if the page query-back failed; delete errors were swallowed with a comment about
a server-side reaper that does not exist. Separately, N joiners share one shape id, so any
double-close stole another subscriber's refcount and could tear down a live shared feed.

**Fix:** every client subscription goes through a `track()` wrapper (one-shot close, pruned from the
client's open list); deletes retry with backoff (not-found = success); `createSubset` deletes its
feed on any post-create failure; `client.close()` now also tears down subsets. Engine-side, the
final drop now **deletes the durable stream** (previously every dropped shape leaked its stream on
the storage server forever).

### 6. Subquery NULL-flip parity (high)

A NULL entering an inner set only re-derived dependents whose `IN` leaf was `negated`. An `IN`
under a `Not{…}` wrapper (`NOT (x IN (SELECT …))` — reachable from Electric SQL) is exactly as
NULL-sensitive, and went stale. **Fix:** edges carry `null_sensitive = negated ∨ under-any-Not`
(with no negation above a leaf, NULL only moves it FALSE↔UNKNOWN and AND/OR are monotone over
FALSE < UNKNOWN < TRUE, so inclusion can't change — proven, tested).

### 7. Replication decoding corruptions (high)

- Array-typed columns (`tags[integer[]]:…`) broke the type-name scan at the first `]`, silently
  NULL-ing **every later column** of the row. Fixed with bracket-depth parsing.
- Backfill (`to_jsonb`) and live (`test_decoding` text output) rendered the same cell differently
  (ISO-`T` vs Postgres text timestamps; raw jsonb vs text) → broken retractions/routing/MIN-MAX.
  Fixed: backfill reads text-mapped columns with `::text` casts (`row_json_expr`).
- Degraded forms are now loud: REPLICA IDENTITY reset (update without old image / delete without
  tuple), TRUNCATE, unparseable values — errors, never silent NULLs/no-ops.

### 8. Electric adapter (high, own record in `electric-conformance/README.md`)

Handle idle-TTL eviction (was: a permanent shape+stream leak per request); offset-replay resync
rebuilt as-of-the-requested-offset (was: dropped deletes / update-of-missing on ordinary client
retries); per-handle serialization + per-(handle, offset) coalescing of concurrent live long-polls;
error classification (400 validation with Electric-style JSON vs 500 transient); 204 live timeouts
with a configurable Electric-like deadline; snapshot folds no longer truncated by empty mid-stream
pages; number/quoted-ident lexing hardened; `IS [NOT] NULL` supported natively.

### 9. Aggregations: SQL NULL semantics (medium)

Aggregates ignored nothing: MIN/MAX could surface NULL, AVG divided by the row count, COUNT(col)
counted NULL rows, SUM over zero rows was 0. All now mirror Postgres (non-NULL counts; NULL for
empty SUM/AVG/MIN/MAX).

### 10. Native `IS NULL` (predicate algebra extension)

`PredicateJson::IsNull { col, isNull }` — the one leaf that is TRUE on a NULL cell, two-valued, so
it composes soundly under `not`. Wired through: Rust compile/eval/SQL emitters, TS types/evaluator/
validator/SQL compiler, API zod, the Electric SQL `where` parser, fuzz generation, and conformance
fixtures (engine ≡ pglite oracle).

### 11. Client subset seam: tombstones (medium)

An accepted delete erased its per-pk watermark, so an in-flight `loadMore` page whose snapshot
predated the delete could resurrect the row; a delete for a never-seen pk recorded nothing (ghost
rows). Deletes now leave LSN tombstones (pruned when no page is in flight).

## Invariant summary

See `docs/ARCHITECTURE.md` §9 (the consistency & durability table) — that table is the normative
statement of what each seam guarantees after this pass.
