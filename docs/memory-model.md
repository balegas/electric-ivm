# Engine memory model — what's in the circuit, what's off it, and why

Written to back the 2026-07-14 blog post's memory claims, after the subquery-templates work
(bead dbsp-ds-jq6) moved membership state into the DBSP circuit. The one-line version for the
post:

> **Engine memory is flat in the data being served.** It never scales with the outer tables
> the shapes deliver, with total database size, or with unsubscribed users/queries. It scales
> with two things: the *relationships being watched* (the matching rows of subquery inner
> tables, bind-gated to actual subscriptions) and, today, one **per-feed key set**
> (`known_members`) that is linear in each feed's current row count. "Flat" without
> qualification overclaims; this is the honest shape.

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

### Per subquery shape — the feed relation (was `known_members`, now circuit state)

| structure | cardinality | scales with |
|---|---|---|
| feed relation slice `(feed_id, pk)` in the membership circuit | **O(current feed size)** per shape | rows in each subscribed feed |

Since the feed-relations change (dbsp-ds-dh6) this lives in the membership circuit as an
upsert map — same bytes, no host structure, and its output deltas ARE the emission decisions
(deletes only from retractions; §3's reasons-it-was-host-side are resolved by making the
relation's transition *be* the decision). Still the largest per-feed term and the reason
"flat" is wrong as an absolute claim; now spillable/checkpointable once the storage follow-up
lands (§4).

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

| term | 2 active users | all 8 users | 10 000 users × 5 memberships |
|---|---|---|---|
| host `pk_value` + `pk_nodes` | ~1.5 KB | ~7 KB | ~12 MB |
| membership circuit (values ×2) | ~1 KB | ~5 KB | ~8 MB |
| `known_members` (≈600-row feeds) | ~70 KB | ~290 KB | **~360 MB** (600-row feeds) |
| issues table in engine RSS | 0 | 0 | 0 |

Two readings of that table matter for the post:

1. **The watched-relationship state is genuinely cheap and shared**: at 10k users it is
   ~20 MB total and bounded above by (memberships table size) × constant — it converges to
   "the membership table, once", no matter how many identical query shapes exist.
2. **The per-feed key sets dominate** as feeds grow: `known_members` ≈ 60 bytes × rows per
   feed × feeds. Sensible (it's pk strings, not rows), linear, and honest to state.

Live numbers: `GET /memory` (`engine_subquery_contributors`,
`engine_subquery_distinct_values`, `engine_subquery_shapes`, …).

---

## 3. Why `known_members` is outside the pipeline

**What it is.** Per subquery shape, the set of outer-row pks the shape's *stream* currently
asserts as members — the shape's own emission history. It exists to gate deletes: absolute
emission computes `upsert-if-matches-now / delete-by-pk` for every touched candidate row,
which makes deferred, out-of-order flip propagation convergent — but delivering a delete for
a row the stream never contained is a *spurious* append, and durable-streams wakes every
live long-poll on any non-empty append. Pre-fix, N idle feeds on a table woke on every write
to it (the PR #30 wake-storm). `known_members` drops those never-member deletes before they
reach the stream.

**Why it isn't circuit state today — three reasons, in decreasing order of force:**

1. **It is output-side state, not source-derived state.** Everything in the circuits is a
   function of the replicated tables and reseeds from Postgres. `known_members` is a function
   of *what this shape's stream has been told* — including emissions produced by flip-driven
   Postgres query-backs and NULL re-derives that run on worker tasks between circuit steps.
   Postgres cannot reseed it (it is seeded from the shape's own backfill and then tracks the
   stream, not the database).
2. **It must be read-modify-written atomically with the emission decision.** The filter
   mutates the set under the registry lock at enqueue time (`filter_known_members`), in the
   same critical section that fixes per-stream emission order. A circuit-maintained replica
   is one step stale exactly when flip workers emit between steps; a stale filter either
   leaks a spurious delete (the bug returns) or drops a genuine one (divergence).
3. **Maintaining it *in* the circuit is equivalent to materializing the semijoin.** A
   relation "pks currently in shape S" is precisely the output of
   `outer ⋉ inner-membership` keyed per shape — the operator the design deliberately did not
   build (inner-side-only). You cannot have the circuit maintain the feed's key set without
   the circuit computing feed membership end-to-end.

Note the cardinality point: moving it would **not shrink it**. One pk per feed row is the
irreducible cost of knowing what the feed contains; the question is only *where* it lives
(RSS hash set vs. circuit trace) and whether it can page to disk and survive restarts.

---

## 4. Could it move to disk via DBSP? Yes — and it's the same project as finishing the semijoin

Feldera's dbsp supports **storage-enabled circuits**: integrated traces (spines) spill to
layer files with a bounded in-memory cache, plus checkpoint/restore of operator state. The
engine has already been there once — the deleted row-arrangement implementation used exactly
this (`ELECTRIC_IVM_DBSP_DIR` / `_CACHE_MIB` / `_CHECKPOINT_SECS`, now deprecated no-ops) —
so the machinery is known to work; it was removed because *row bodies* didn't belong in the
engine, not because spilling was broken.

The path, if per-feed RSS ever matters:

1. Add a per-shape membership relation to the membership circuit:
   `(shape_id, outer_pk)`, maintained as the distinct output of
   `outer-candidate tuples ⋉ membership` — with outer-candidate tuples fed the same way
   membership tuples are today (host evaluates, feeds exact deltas: outer-table deltas from
   the sequencer, query-back results from flip workers). The relation's **output delta is
   exactly what to emit**: inserts = upserts, deletes = genuine leaves. `known_members` and
   `filter_known_members` stop existing as separate concepts — the spurious-delete gate
   becomes structural (a delete only appears when the relation actually retracts).
2. Run that circuit storage-enabled: the `(shape_id, pk)` spine pages to disk; RSS becomes
   O(cache), tail on disk. 360 MB of key sets at 10k×600 becomes a disk file plus a
   configurable cache.
3. Checkpoint it. This is the same lever as bead **dbsp-ds-pg5** (subquery state
   persistence): today subquery shapes are dropped at boot precisely because node state and
   `known_members` aren't persisted; a checkpointed membership circuit restores both, and
   the SnapshotGate/xid-fencing story for replay-from-checkpoint already exists in the
   counts tier's design.

**Costs, honestly:** it reintroduces the storage layer (directory, cache tuning, checkpoint
cadence, fsync behavior) that PR #27 deleted for simplicity; emission ordering needs care —
the emit-decision moves from "under the registry lock" to "the circuit step's output delta",
so flip-driven emissions must route their candidates through a circuit step instead of
deciding inline (an extra step per flip batch, serialized on the circuit thread); and
per-shape keying means the relation grows with feeds × feed size — same bytes as today, now
with a disk story. None of these are research problems; they are the natural phase 2, and
the spec's out-of-scope note (§12) already points here: revisit when flip query-back latency
or per-feed RSS becomes the measured bottleneck, not before.

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
  clients from waking on irrelevant writes — linear in feed size, ~60 bytes/row, and the
  design has a clear DBSP path to spill it to disk when that matters."

Avoid: "memory is flat" unqualified, and "the circuit maintains all state needed by the
queries" (the per-feed key sets and the reconcile reverse index are host-side by design).
