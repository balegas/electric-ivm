# Subset queries: position the live tail at the snapshot LSN (no double-count)

Design record — 2026-07-01. Status: **proposed → implementing**.

## Problem

A subset query (`packages/client/src/subset.ts`) merges two row sources that never meet on the
server: a one-shot Postgres **page** (`subset.query`, read in a `REPEATABLE READ` snapshot at
`pg_current_wal_lsn() = S`) and a **changes-only live feed** (`subset.live`, a `changesOnly` shape
with `seed_lsn = 0` that forwards *every* future matching delta).

Today the client opens the feed first, then reads it **from offset `-1`** and dedups by primary key
(idempotent upsert/delete). This *converges*, but it has three real costs:

1. **Overlap replay.** Every delta the feed emitted in `[feed-open, S)` is already reflected in the
   page, yet the client re-applies it (idempotent, but it causes transient flicker — a row briefly
   showing a pre-snapshot value, or appearing then disappearing).
2. **Not reusable.** A late joiner can't share an existing feed: reading a long-lived shared stream
   from `-1` replays its entire backlog. Efficient reuse needs a way to start at "this client's
   snapshot point" — which doesn't exist today.
3. **No deterministic invariant.** "Eventually converges" is hard to assert; we want "each
   post-snapshot change is delivered exactly once."

The root cause: the page is positioned at a precise point in the engine's replication timeline
(`S`), but the live feed is **not positioned relative to it**. `query_subset` already returns `S`
end-to-end (`SubsetResult.lsn`) — and the client throws it away.

## What Electric does (the reference)

ElectricSQL (`../electric`, `lib/electric/`) positions the snapshot↔live cut by **MVCC
transaction-id visibility**, not a single LSN:

- The snapshot txn runs `SELECT pg_current_snapshot(), pg_current_wal_lsn()` in `REPEATABLE READ`,
  capturing `{xmin, xmax, xip_list}` + `lsn` (`postgres/snapshot_query.ex`).
- For every transaction that later arrives on logical replication, `visible_in_snapshot?/2`
  (`replication/changes.ex:166-184`) decides — by xid vs `xmin/xmax` and **membership in
  `xip_list`** — whether the snapshot already contains it; if so the live copy is dropped
  (`consider_flushed`), else it's written to the log.
- Snapshot rows live at "virtual" offsets (`tx_offset = 0`), live rows at "real" offsets
  (`tx_offset = LSN`), so every live row sorts after every snapshot row
  (`replication/log_offset.ex`).

**Electric's explicit caveat:** a naïve "snapshot up to LSN X, then stream from X" mishandles
transactions that were *in progress* at snapshot time (in `xip_list`) — they commit *after* X but
their effects are *not* in the snapshot. `xip_list` is what catches them.

## Our adaptation — and why a single LSN is sufficient here

We don't need `xip_list`, because our ingestor already gives us the in-flight-safe key: it stamps
each change with its transaction's **COMMIT-record LSN** (`replication.rs`, the `COMMIT` row), not
the per-change record LSN. Therefore:

> An in-progress transaction at snapshot time has **not yet written its COMMIT record**, so its
> commit LSN is necessarily `> S = pg_current_wal_lsn()` captured at the snapshot. Hence
> `commit_lsn < S ⟺ visible to the REPEATABLE READ snapshot`, with **no** `xip_list` needed.

This is exactly the compare materialized shapes already use on the live path
(`engine.rs:715,737`: `if lsn != 0 && lsn < seed_lsn { skip }`), proven under concurrent writers by
`packages/conformance/src/conformance-concurrency.test.ts`. Our change makes subset queries reuse
that proven contract — applied **at the client**, because the subset page is a direct PG read that
bypasses the stream (there is nowhere server-side to drop the overlap).

## Design

Two small pieces.

### 1. Engine — stamp the commit LSN onto feed/shape output envelopes

`translate_output` currently hard-codes `lsn: None` on every output envelope (`engine.rs:871,886`),
dropping the commit LSN. Thread the change's commit-LSN string through and stamp it:

- `translate_output(ts, out, txid, lsn: Option<String>, out_cols)` — stamp `lsn` on both `upsert`
  and `delete` headers.
- In `process_envelope`, keep the original commit-LSN **string** (currently shadowed into a `u64`
  at `engine.rs:703`); pass it to the two live `translate_output` call sites (standalone + router).
  Backfill emission in `add_shape_routed` passes `None` (those rows are the snapshot baseline, not a
  live change).

Harmless for materialized consumers (stream-db / `electric.rs` ignore `headers.lsn`); it only
becomes load-bearing for the subset client.

### 2. Client — apply feed deltas only at/after the snapshot, last-writer-wins by LSN

`subset.ts` reads `S = lsn_to_u64(first.lsn)` and maintains a per-present-pk **watermark**
`applied: Map<pk, lsnU64>`:

- Page rows seed `applied[pk] = S`. `loadMore` rows (taken at their own snapshot `L2`) set
  `applied[pk] = L2`.
- A feed delta with `deltaLsn = lsn_to_u64(env.headers.lsn)` is applied iff:
  - present row: `deltaLsn >= applied[pk]` (last-writer-wins — never regress to an older state);
  - absent row: `deltaLsn >= S` (global floor — never admit a pre-snapshot row; it belongs to a
    later page, fetched by `loadMore`).
- `env.headers.lsn` absent (library/no-Postgres mode) ⇒ apply (fall back to today's idempotent-pk
  behaviour). Subset queries are Postgres-only, so feed deltas always carry an LSN in practice.

`lsn_to_u64` in JS mirrors the Rust `pg::lsn_to_u64`: parse `"HI/LO"` hex → `(HI << 32) | LO` as a
`BigInt`.

Feed-before-snapshot ordering is **still required** (no-gap: the feed must be live before `S` so it
captures everything `>= S`). The watermark replaces best-effort idempotent overlap with a
deterministic cut, and additionally fixes the `loadMore` race (a feed delta older than a page we
already fetched at `L2` is dropped instead of regressing the row).

### Why this is correct (case analysis)

For a change with commit LSN `c` and a row `R`:

| situation | feed delivers | decision | result |
|---|---|---|---|
| `R` in page, `c < S` (overlap) | yes, `c` | `c < applied[R]=S` → **drop** | page already has it; **no double-count** |
| `R` in page, `c >= S` (live update) | yes | `c >= S` → apply | one update |
| `R` absent, `c < S` (pre-snapshot, below window or pre-deleted) | yes | `c < S` floor → **drop** | not admitted; comes via `loadMore` if in range |
| `R` absent, `c >= S`, in view (move-in) | yes | apply | one insert |
| `R` loadMore'd at `L2`, stale feed delta `S <= c < L2` | yes | `c < applied[R]=L2` → **drop** | no regress |
| in-flight txn at snapshot, commits after as `c > S` | yes | `c >= S` → apply | one insert (the `xip_list` case, handled by commit-LSN) |

No gap (feed live before `S`) + no overlap (drop `c < watermark`) ⇒ **exactly-once after the
snapshot**.

## Scope / non-goals

- **In scope:** the positioning primitive (commit-LSN on feed envelopes) + client watermark; tests;
  Playwright validation on LinearLite.
- **Out of scope (follow-up):** sharing one feed across late joiners. This design is its
  prerequisite — a shared feed becomes viable once a joiner can drop everything `< S` — but
  efficient reuse also needs a server-side LSN→offset fast-forward (offsets are opaque tokens
  today; see `ds.rs`), which is a separate change. Documented in `ivm-engine-internals.md`.
- **Subquery feeds** keep `lsn: None` on their bespoke `emit_shape_delta` envelopes; subset feeds
  are standalone/routed, so this is unaffected. Threading LSN there is a later, separate change.

## Testing

- **Rust unit:** `translate_output` stamps the commit LSN on upsert + delete envelopes.
- **Client unit (vitest):** `lsn_to_u64` parity with the Rust parser; the watermark decision table
  above (drop `< S`, drop `< L2`, admit `>= S`).
- **Conformance (live stack):** the overlap window — write a row matching the predicate *between*
  feed-open and the page snapshot, assert it appears **exactly once**; concurrent-writer variant
  (writers running across the snapshot); move-in/move-out across the boundary; stale-delete.
- **Playwright (LinearLite):** drive the infinite-scroll list, write rows during/after the snapshot,
  assert the rendered list has **no duplicate ids** and reflects live updates.
