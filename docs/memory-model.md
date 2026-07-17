# Engine memory model — what's in the circuit, what's off it, and why

Written to back the 2026-07-14 blog post's memory claims, after the subquery-templates work
(bead dbsp-ds-jq6) moved membership state into the DBSP circuit. The one-line version for the
post:

> **Engine memory is flat in the data being served.** It never scales with the outer tables
> the shapes deliver, with total database size, or with unsubscribed users/queries. It scales
> with two things: the *relationships being watched* (the matching rows of subquery inner
> tables, bind-gated to actual subscriptions) and one **per-feed key set** — the delete gate —
> that is linear in each feed's current row count. "Flat" without qualification overclaims;
> this is the honest shape.
>
> **Update (Task 2.2, dbsp-ds-dh6 re-litigated):** the per-feed key set is a **host-side
> Roaring bitmap** (`HashMap<feed_id, RoaringBitmap>` keyed by `u32` pk-id — see
> `apps/engine/src/subq_feed.rs`), not an in-circuit relation. Measurement showed it stays small
> even at large subscription counts — dramatically lighter than the dbsp relation dh6 briefly
> used — and needs no spilling. §3–§4 below are updated accordingly; the full decision is in
> `docs/notes/2026-07-16-feed-set-representation-spike.md`. (fresh benchmarks pending)

---

## 1. The memory map

### In the DBSP circuits (derived, reseedable, in-memory)

| state | circuit | cardinality | scales with |
|---|---|---|---|
| counts pipelines (`group → count`) | counts (`arrangements.rs`) | O(distinct groups) per configured table | data *shape*, not data size — flat in shapes/users |
| subquery membership sets (`(node_id, value) → contributor count`, held twice: published trace + the incremental distinct's own integral) | membership (`subq_circuit.rs`) | O(distinct projected values) per subscribed bind | subscriptions × inner-query selectivity |

Both circuits are derived state: reseeded from Postgres on boot (counts) or on shape
creation (membership), never the record of truth. Neither holds a row body or a primary key —
the membership circuit's `map` drops the pk *before* the first stateful operator.

### Host-side, per subquery node/template (the reconcile bookkeeping)

| structure | cardinality | why it exists |
|---|---|---|
| `pk_value` (per node): inner-row pk → projected value | O(contributing inner rows) | exact retractions. A row's circuit tuple is **not** a pure function of the row — a nested `IN` in the residual reads *other* nodes' sets at eval time — so reconcile-by-identity must remember what it previously asserted to retract it precisely. |
| `pk_nodes` (per template): inner-row pk → holding node | O(contributing inner rows) | O(1) move-out routing across binds (which user's node held this membership row?) without scanning every bind. |
| template registry (compiled residual, param cols, bind map) | O(distinct query structures) | the sharing layer itself; dozens of bytes per *template*, not per user. |

Together these are the same order as the old kernel's `contributors` + `pk_value` maps —
the circuit swap achieved memory **parity**, while collapsing per-delta evaluation from
O(nodes) to O(1) per template.

### Per subquery shape — the feed key set (host-side Roaring bitmap)

| structure | cardinality | scales with |
|---|---|---|
| `FeedSet`: `HashMap<feed_id, RoaringBitmap>` of `u32` pk-ids (`subq_feed.rs`) | **O(current feed size)** per shape | rows in each subscribed feed |

The delete gate. It exists to gate deletes: upserts flow for every current member, but a
delete is emitted for a pk **iff** `FeedSet::remove(feed, pk)` returns `true` (the pk was
actually in the feed) — the check-and-set IS the emission decision, a synchronous `&mut self`
op under the registry lock (no `.await`, so the borrow checker enforces atomicity). This is
the largest per-feed term and the reason "flat" is wrong as an absolute claim, but it is
*small*: a small per-entry footprint both resident and serialized, staying small in aggregate
even for large feed sets. Reported as `bytes_feed_sets` / `subquery_feed_entries` in
`GET /memory`.

History: it began host-side (a `known_members` set), briefly moved *into* the membership
circuit as an `add_input_set` upsert-SET (dbsp-ds-dh6, for spill + structural gating), and
Task 2.2 moved it back to the host as a bitmap — which re-provides the structural gate (a
delete exists iff `remove()` returns true, same lock scope) while being far lighter and needing
no spill. §3–§4 below.

(The Electric `/v1/shape` adapter additionally keeps a TTL-evicted per-handle key set in
`electric.rs` for protocol filtering — same order, handle-scoped, dropped on idle.)

### Deliberately NOT in the engine, ever

| data | where it lives | why |
|---|---|---|
| outer-table rows (the data shapes deliver) | Postgres only | the inner-side-only decision: a materialized semijoin needs the outer relation arranged; that is exactly the O(rows) state PR #27 removed. Flips pay a pooled Postgres query-back instead of RSS. |
| unsubscribed binds (`user_id = 99` with no feed) | nowhere | bind-gating: templates share *structure* eagerly but hold *state* only for subscribed parameter values, each seeded like a backfill. |
| shape results / feed history | durable streams | the engine is a restartable consumer between two logs. |

---

## 2. Worked example: LinearLite (`issues WHERE project_id IN (SELECT … WHERE user_id = ?)`)

Small demo data: 1 000 issues, 5 projects, 8 users, 29 memberships. One template
(`project_members|project_id|P(user_id)|A()`), one bind per viewing user.

The engine keeps, per active viewing user: the host `pk_value` + `pk_nodes` bookkeeping, the
membership circuit's contribution (held twice — published trace plus the incremental distinct's
integral), and a `FeedSet` bitmap of that user's currently-visible rows. The `issues` table itself
never enters engine RSS, at any user count.

Two readings matter for the post:

1. **The watched-relationship state is genuinely cheap and shared**: total size is bounded above
   by (memberships table size) × constant, regardless of user count — it converges to "the
   membership table, once", no matter how many identical query shapes exist.
2. **The per-feed key sets are the largest per-feed term** as feeds grow: the `FeedSet` bitmaps
   scale linearly with rows per feed × feeds, but are an order of magnitude lighter than the old
   string-keyed `known_members` representation — pk-ids in Roaring bitmaps, not pk strings.

Live numbers: `GET /memory` (`engine_subquery_contributors`,
`engine_subquery_distinct_values`, `engine_subquery_shapes`, `engine_subquery_feed_entries`,
`bytes_feed_sets`, …).

---

## 3. Why the feed key set is host-side (again)

**What it is.** Per subquery shape, the set of outer-row pks the shape's *stream* currently
asserts as members — the shape's own emission history. It exists to gate deletes: absolute
emission computes `upsert-if-matches-now / delete-by-pk` for every touched candidate row,
which makes deferred, out-of-order flip propagation convergent — but delivering a delete for
a row the stream never contained is a *spurious* append, and durable-streams wakes every
live long-poll on any non-empty append. Pre-fix, N idle feeds on a table woke on every write
to it (the PR #30 wake-storm). The feed key set drops those never-member deletes before they
reach the stream: a delete is emitted iff `FeedSet::remove(feed, pk)` returns `true`.

dh6 briefly moved this into the membership circuit (an `add_input_set` upsert-SET whose
retraction deltas were the deletes) to get spill + structural gating. Task 2.2 moved it back
host-side as a Roaring bitmap, because the three original reasons for keeping it off the
pipeline all hold — and the bitmap satisfies them *better* than the circuit did:

1. **It is output-side state, not source-derived state.** Everything in the circuits is a
   function of the replicated tables and reseeds from Postgres. The feed set is a function of
   *what this shape's stream has been told* — including emissions produced by flip-driven
   Postgres query-backs and NULL re-derives. Postgres cannot reseed it (it is seeded from the
   shape's own backfill and then tracks the stream, not the database). The host-side bitmap,
   seeded from the backfill in three-phase-create phase C under the lock, honours this directly.
2. **It must be read-modify-written atomically with the emission decision.** The check-and-set
   (`insert`/`remove`) IS the emission decision, a synchronous `&mut self` op inside the
   registry-lock critical section — no `.await`, so the borrow checker enforces the mutation
   and the decision are one indivisible step. There is no cross-thread circuit replica to go
   stale between steps (which the dh6 circuit form risked); this reason is *strengthened*, not
   merely satisfied.
3. **Maintaining it *in* the circuit is equivalent to materializing the semijoin.** A relation
   "pks currently in shape S" is precisely `outer ⋉ inner-membership` keyed per shape — the
   operator the design deliberately did not build (inner-side-only). A host check-and-set set
   does not make the circuit compute feed membership end-to-end; it is exactly the "RSS hash
   set" alternative this section always named.

Note the cardinality point: moving it does **not shrink it** in *rows*. One pk per feed row is
the irreducible cost of knowing what the feed contains. But the *representation* matters: a
Roaring bitmap of `u32` pk-ids is dramatically lighter per entry than both the old string-keyed
set and the dh6 in-circuit relation.

---

## 4. Does it need to spill to disk? No — measurement refutes the premise.

The earlier version of this section argued the feed set could grow large enough at scale to
warrant moving it into a **storage-enabled dbsp circuit** (spines spilling to layer files,
checkpoint/restore) — the same machinery the deleted row-arrangement layer used. The Task 2.2
spike measured the actual cost and refuted that premise:

- The feed set in Roaring bitmaps stays small resident (RSS Δ) even at large subscription
  counts, dramatically lighter than the equivalent dbsp in-circuit relation (profiler
  `total_used_bytes`, spill off). Even the deliberately-unflattering shape (mega-feeds dense,
  bulk feeds a sparse random sample — roaring's worst case) lands there. (fresh benchmarks
  pending)
- At that size, spilling and paging are pointless: the set stays resident, and a checkpoint is
  a small file (`RoaringBitmap::serialize_into` per feed).

So the feed set does **not** move to disk. It stays a host-side bitmap, and the spill machinery
(`ELECTRIC_CIRCUITS_SUBQ_STORAGE_*`) now covers only the **contributor** relation still in the
circuit — kept because a high-selectivity inner query could grow it, with the default a
candidate to flip off (a separate, gated follow-up; see the spike §4c). Checkpoint/restore of
the feed bitmaps and the contributor relation (beads **dbsp-ds-mrt** / **dbsp-ds-pg5**) is now
*simpler*: the bulk of what needed checkpointing was the feed set, which serialises trivially,
and a checkpoint taken under the registry lock is consistent by construction (same
SnapshotGate/xid-fencing discipline the counts tier uses). Full analysis:
`docs/notes/2026-07-16-feed-set-representation-spike.md`.

---

## 5. Suggested claims for the blog post

Safe and precise:

- "The circuit's state is **independent of table sizes**: counts pipelines are O(distinct
  groups); subquery membership is O(matching inner rows) for **subscribed** queries only,
  shared across every user asking the same query shape."
- "Row data never enters the engine — shapes are served out of Postgres and an append-only
  stream; the engine's job is deciding *what changed for whom*, and its memory scales with
  the watched relationships, not the watched data."
- "Per active feed the engine keeps one key set (a pk per delivered row) to keep idle
  clients from waking on irrelevant writes — linear in feed size, but a Roaring bitmap of
  integer pk-ids is small enough per row to stay resident and checkpoint as a small file, even
  at scale."

Avoid: "memory is flat" unqualified, and "the circuit maintains all state needed by the
queries" (the per-feed key sets and the reconcile reverse index are host-side by design).
