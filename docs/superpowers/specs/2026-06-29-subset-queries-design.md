# Subset queries: non-materialized, query-back + live-tail (separate from shapes)

Design record — 2026-06-29. Status: **draft (research in progress)**. Supersedes the reverted
windowed page-shape approach (commit `8858977`, reverted in `2a04399`).

## Problem with page-shapes (what we reverted)

The cursor-paginated list made each page a **materialized shape**: its own `shape/<id>` durable stream,
its own backfill, its own predicate. Three faults:

1. **Change fanout to multiple ranges.** Page predicates were nested (`created < cursorₖ`), so a single
   live insert of an old row matched *every* page above it and was fanned out to all of them
   (deduped only on the client). One change → many ranges.
2. **Per-page materialization.** Each page stored its 200 rows server-side (durable stream) — redundant
   with Postgres, which already has them.
3. **Accumulation.** Shapes were never dropped on close, so scrolling/ filtering leaked shapes, each
   still evaluated on every write.

## Two distinct concepts

Make the API separate them explicitly (as Electric does — see Research):

- **Shape** *(materialized, unchanged)* — one table + `where` (+ `columns`). The engine backfills it,
  stores it as a durable stream, and incrementally maintains the **whole** matching set. Use for sets
  you want fully synced and live (board columns, a bounded working set). Cost: stores the set.

- **Subset query** *(new, non-materialized)* — a windowed/ad-hoc slice (`where` + `orderBy` + `limit` +
  cursor) the engine **never stores**. Two parts:
  1. **Query-back:** a one-shot Postgres `SELECT … WHERE <pred> ORDER BY … LIMIT n` returns the page
     rows directly (with the snapshot's LSN). No durable stream, no server-side copy.
  2. **Live tail:** the client follows the table's change tail from that LSN and checks each change
     against **the one predicate describing the currently-loaded view** — a single contiguous range
     `[lowerBound, ∞)` ∧ base filter. A change in range updates the view; out of range is ignored.

**One range per view ⇒ no fanout.** The view is a *single* growing range (newest down to the oldest
loaded row), not N overlapping pages. Each change is tested against one predicate. Scrolling moves the
single `lowerBound` down and issues another query-back for the newly revealed rows — it does **not**
create another range/shape.

## Mechanism

```
            ┌─ query-back (one-shot PG SELECT, returns rows + snapshot LSN) ──► initial page rows
view ──┤
            └─ live subset subscription (predicate = base ∧ created ≥ lowerBound) ─► matching deltas only
```

- **Where the live filter runs:** server-side in the engine (it already evaluates predicates against
  each replication change in `process_envelope`). A subset subscription is a predicate filter that
  **forwards matching deltas to an ephemeral stream** — like a standalone shape but with **no
  backfill** appended. The client reads that stream **from the tail offset captured at query time**
  (not from 0), so it sees only live changes; the initial rows come from the query-back. The
  seed-LSN/offset handoff (already built for shapes) closes the snapshot↔live gap with no dup/gap.
- **Move-in / move-out** is automatic: the subscription predicate includes the view range, so a row
  whose update brings it into `[lowerBound, ∞)` ∧ base emits an upsert (move-in); a row leaving emits a
  delete (move-out). No re-query needed for the common case. (Edge: a move-out from the *bottom* of a
  count-bounded window — TBD, see Open questions.)
- **Scrolling:** load-more = one query-back for `created < oldestLoaded LIMIT n`, append to the client
  list, and extend the live subscription's `lowerBound` to the new oldest. Still one range.

## API separation (proposed)

- `shapes.create` / `shapes.get` — unchanged (materialized).
- New `subset` (or `query`) router:
  - `subset.query({ table, where?, columns?, orderBy?, limit?, after? }) → { rows, lsn }` — one-shot
    query-back (no stream).
  - `subset.live({ table, where? }) → { streamPath, fromOffset }` — an ephemeral live-delta stream for
    the view predicate, read from `fromOffset`. (Or fold the live predicate update into one
    subscription handle the client mutates as it scrolls.)
- Client: `client.shape(def)` (materialize) vs `client.subset(def)` (query-back + live), clearly named.

## Correctness

- **Snapshot↔live handoff:** query-back records `pg_current_wal_lsn()`; the live stream is read from the
  offset/LSN at/after that snapshot; changes with commit LSN `< snapshot` are already in the page
  (reuse the existing strict-`<` reconciliation).
- **No fanout:** one predicate per view ⇒ each change matched once per view.
- **No server materialization of the page:** page rows live only in Postgres + the client's memory.

## Research: how Electric does it (../electric)

Electric has **two separate concepts**, which is the model to copy:

- **Shape** (`shapes/shape.ex`, struct at :31-49): one `root_table` + `where` (compiled expr, not raw
  SQL) + `selected_columns` + `replica`. **Materialized + live-tailed**: initial rows from a snapshot
  query, then logical-replication changes are matched against the shape's `where` and appended to that
  shape's persistent log. No range/limit/offset on a Shape.
- **Subset** (`shapes/shape/subset.ex`, struct `{order_by, limit, offset, where}`): an **ephemeral,
  one-shot, NON-live** SQL read, scoped to an existing shape. `query_subset` (`querying.ex:42-91`)
  AND-combines `base_where AND subset_where` and appends `ORDER BY/LIMIT/OFFSET`, run directly against
  Postgres in a `REPEATABLE READ READ ONLY` txn (`snapshot_query.ex`), tagged `electric-snapshot: true`,
  `no-cache`, `response_type: :subset`. Subqueries forbidden; `order_by` required with `limit/offset`.

**Range/limit/offset exist ONLY in Subset, never in Shape — Electric has no live range primitive.**
This is the key: *"Electric sidesteps range fanout entirely by making ranges non-live snapshot reads
rather than materialized live shapes."* A change is matched against shape `where`-clauses
(`Filter.affected_shapes`, ETS equality/inclusion indexes bound the cost), but **never against
ranges** — so one change never fans out across ranges.

- **Move-in/out** (simple shapes): computed from the WAL alone via `replica: :full` old+new rows —
  `{old_in,new_in}` → upsert / delete, no Postgres round-trip (`shape.ex:735-773`). We already produce
  `[(old,-1),(new,+1)]` deltas, so our standalone predicate filter does the same.
- **Snapshot↔live handoff:** snapshot captures `pg_current_snapshot()`+`pg_current_wal_lsn()`; live txns
  deduped by MVCC visibility (`visible_in_snapshot?`). Our strict-`<` commit-LSN compare is the
  equivalent.

**Where we extend Electric:** Electric Subsets are *static* (no live). We additionally want the loaded
view to stay live. We get that **without** re-introducing range fanout by following the live tail with
**one predicate for the whole loaded view** (§ below) — a single range, not N pages.

## Live tail: one predicate, client-side range check (locked)

The cleanest way to keep loaded subset rows live, matching "follow the live tail to check if the new
rows match the page in view" with **zero range fanout**:

- The engine exposes **one non-materialized live feed** per view, on the **base predicate only**
  (status/priority — no range): a shape variant with **no backfill** (`changesOnly`), forwarding only
  matching change deltas. One predicate, no ranges → a change is matched once, never across ranges.
- The **client follows that tail and checks view membership locally**: a delta whose row falls in the
  currently-loaded range `[lowestLoaded, ∞)` is applied to the list (move-in/update/move-out via the
  old+new rows already in the delta); out-of-range deltas are ignored. This is the literal "check if
  the new row matches the page in view," done in JS — no server range object.
- Pages are pure **`subset.query`** one-shot query-backs. Scrolling = another query-back for the next
  page + extend the client's `lowestLoaded`. No new server state per page, no range fanout.

So the only live server object is **one base feed** (a predicate filter, no stored set, no ranges).
Trade-off: the client receives base-matching changes even for rows outside the loaded window — fine for
LinearLite's user-driven write rate; a high-write table would instead narrow the feed predicate.

## Resolved questions

1. **Range view vs count window.** The live view is the **loaded range** `[lowestLoaded, ∞)`, not a
   count-bounded top-N. New inserts at the top belong there (correct); virtual scrolling caps the DOM.
   No live count edge ⇒ no top-N rebalancing ⇒ no fanout.
2. **Move-out from the bottom.** There is no live count edge — the bottom is the `lowestLoaded` cursor,
   moved only by explicit load-more. A row deleted/changed at the bottom just leaves; nothing rebalances.
3. **Ephemeral feed lifecycle.** The base live feed is dropped when the view unmounts (fixes the
   shape-leak we hit before). Tie its lifetime to the client subscription; the engine already has
   `DELETE /shapes/{id}`.

## Implementation plan (incremental, each verified)

1. **Protocol:** add `SubsetDef { table, where?, columns?, orderBy?, limit?, offset? }`, separate from
   `ShapeDef`.
2. **Engine `query` (subset):** `GET/POST /query` → one-shot `SELECT … WHERE <pred> ORDER BY … LIMIT …
   OFFSET …` in a REPEATABLE READ snapshot, returns `{ rows, lsn }`. Reuses `predicate_to_sql`; **no
   durable stream**.
3. **Engine live-only feed:** a shape created with `changesOnly` (skip backfill); client reads from the
   tail offset. (Or reuse standalone shape + a `no_backfill` flag.)
4. **API:** new `subset` router (`subset.query`) distinct from `shapes`; keep `shapes.*` as-is.
5. **Client:** `client.query(def)` (one-shot) and `client.subset(def)` (query-back + tail-follow live
   collection), clearly named vs `client.shape(def)` (materialized).
6. **Demo:** list = `client.subset` (pages + live tail); board/search unchanged.
