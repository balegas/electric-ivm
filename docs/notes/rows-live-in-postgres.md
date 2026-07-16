# Verified: row state lives in Postgres; the engine queries it on demand

Note for collaborating agents/writers. Every claim below was verified against the code at
main (post the host-side FeedSet move — feeds are no longer a circuit relation). File:line
references are the proof points.

## What the engine's state actually contains — keys, never row bodies

- The membership circuit stores the **contributor** relation only: keyed `(node_id, pk) →
  projected value`. The projected value is ONE column's value (e.g. a `project_id`), not a row.
  Before the first stateful operator, the pipeline projects away everything else —
  `subq_circuit.rs:134-140`: the contributor stream maps to `Row([node_id, value])`; row bodies
  never enter a spine.
- The **feed** set — which primary keys are currently in each subscribed feed, i.e. the delete
  gate — lives **host-side, not in the circuit**: a `HashMap<feed_id, RoaringBitmap>` of pk-ids
  (`subq_feed.rs`, `FeedSet`). Still keys, never rows.
- The routing tier holds no rows either: `KeyRouter` is `key_tuple → {shapes}` routing
  metadata only (`engine/executors.rs` — the struct doc says exactly this), and the counts
  circuit holds `(group → count)` pairs (`arrangements.rs` module doc: "Row data lives in
  Postgres, not here").
- Grep-level check: no struct field in `subquery.rs`/`executors.rs` retains `Row`s. The
  `Vec<Row>`/`HashMap<String, Row>` occurrences are function-local buffers
  (`subquery.rs:958, 1088, 1092`) or parameters (`:621`). The one bounded exception: during
  a shape's three-phase creation, raw deltas buffer in `SubqueryNode.seed_buffer` and
  `PendingSubqueryShape.buffer` until the backfill installs — a transient window, drained at
  `finish_create`.

## Where row bodies come from, per path (all Postgres or the replication delta)

1. **Backfills** (shape creation): one `REPEATABLE READ` snapshot query with the predicate
   pushed into the SELECT (`pg.rs::backfill/backfill_where`), streamed to the shape's
   durable stream. Nothing retained engine-side.
2. **Live changes on the shape's own table**: the row body comes from the replication
   envelope itself (`REPLICA IDENTITY FULL` carries old + new rows) —
   `engine/membership.rs::latest_rows_by_pk` folds the delta; used at `subquery.rs:928, 995`.
3. **Flip-driven moves** (a subquery's inner set changed, affected outer rows must be
   re-evaluated): a pooled Postgres query per flip —
   `engine/membership.rs:54-72` (`query_rows_by_col` = `SELECT … WHERE connecting_col = $v`,
   `query_rows_all` for NULL re-derives), both via `pg::backfill_where` on the shared
   connection pool (`ELECTRIC_DB_POOL_SIZE`). Call sites: `subquery.rs:1266, 1298-1299,
   1336`.

In every path the rows transit: evaluated, translated to stream envelopes, appended to the
durable stream — then dropped. The engine is a router between Postgres and the stream logs,
not a cache of either.

## The one-line versions (safe to publish)

- "The engine holds keys, not rows: which values are in each subquery's set, and which
  primary keys are currently in each feed. When it needs actual rows — a backfill or a
  membership flip — it queries Postgres through a bounded pool and forwards the results to
  the feed log."
- "Memory follows what users sync (a small, bounded cost per delivered-row pk) and the
  relationships being watched — never table sizes, because tables never enter the engine."

Corollary that surprises people: engine restart loses nothing durable — feeds' history is on
the streams server's disk, data is in Postgres, and the in-memory relations reseed/replay.
