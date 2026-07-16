# Task 2.2 spike — feed-set representation: in-circuit relation vs host-side Roaring bitmaps

**Status:** design decision + PoC measurement (timeboxed spike). **Recommendation: GO.**
**Re-litigates:** bead `dbsp-ds-dh6` (feed key sets moved *into* the membership circuit) and
`docs/memory-model.md` §3–§4. Engages every original reason below.

The question: should the feed relation move OUT of the dbsp membership circuit into host-side
per-feed Roaring bitmaps — `HashMap<feed_id, RoaringBitmap>` keyed by `u32` pk-id — with the
delete-gate becoming an explicit presence-transition check (`bitmap.remove(pk)` return value)?

---

## 1. Recommendation

**GO — move the feed relation to a host-side `HashMap<feed_id, RoaringBitmap>`.**

One sentence: the measurement refutes `dh6`/§4's premise (the feed set is *not* big enough to need
disk spilling — it stays small even at a large subscription count, dramatically lighter than the
dbsp relation), while every §3 reason either *favours* the host-side bitmap (reason 1) or is
*better satisfied* by
it (reason 2 — the check-and-set is synchronous under the registry lock, with no cross-thread
circuit hop in the critical section) or is *moot* (reason 3 — a check-and-set set is not a
materialized semijoin).

Why this is not just re-introducing the pre-dh6 bug: the wake-storm bug class (PR #30) was killed by
the **emission-ordering fix** (whole `emit_for_shapes` under the registry lock + per-stream FIFO
lanes), which landed alongside dh6 and is orthogonal to *where the set lives*. dh6's circuit
residency bought two things on top of that: (a) structural delete-gating and (b) spill +
checkpoint. The bitmap re-provides (a) structurally — a delete exists **iff** `remove()` returns
`true`, computed in the same expression, in the same lock scope — and makes (b) trivial (the set is
small enough to stay resident; checkpoint = serialize a small file of bitmaps). Circuit residency
was never load-bearing for *correctness*; it was load-bearing for *spill*, and spill is no longer
needed.

---

## 2. PoC measurement

Test-only, ignored, deletable: `apps/engine/src/subq_feed_repr_spike.rs` (+ the `#[cfg(test)] mod`
line in `lib.rs` + the `roaring` dev-dep). Run:

```text
cargo test --release -p electric-circuits-engine --lib feed_repr_spike -- --ignored --nocapture
```

**Shape** (a large-subscription-count feed workload, realistic skew, feeds numbering in the tens
of thousands): a handful of mega feeds, a mid-size tier, and a large tail of small feeds. pk-ids
are drawn from a bounded universe (roughly the issue count) so mega feeds are *dense* (bitmap
containers) and the bulk small feeds are a *sparse random sample* — roaring's **worst** case, array
containers with no compression. This is deliberately unflattering to roaring.

**dbsp** number = `MembershipCircuit::profile_bytes().0` (dbsp's own profiler `total_used_bytes`,
allocator-independent — the same quantity the attribution doc's spill-delta approximates), spill
forced **off** so it is true in-memory residency. **roaring** number = process **RSS delta**
(allocator-visible truth, includes `HashMap` buckets + `Vec<Container>` headers + slack), measured
from a fresh baseline *after the circuit is shut down* so the two never overlap in the process.

| representation | resident | notes |
|---|---|---|
| **dbsp feed relation** — profiler `total_used_bytes` | large | in-memory, spill off; on-disk = 0 |
| dbsp feed relation — RSS Δ (cross-check) | larger still | allocator-inclusive |
| **roaring `HashMap<feed_id,RoaringBitmap>`** — RSS Δ | **small** | **headline** |
| roaring — `serialized_size()` sum (payload floor) | small | = checkpoint file size |
| roaring — owned floor (serialized + outer HashMap) | small | lower bound |

**Ratio: dbsp `/` roaring is roughly an order of magnitude or more**, whether comparing profiler
bytes or RSS deltas. The whole feed set in bitmaps stays small — matching the spike brief's
prediction — and small enough that spill/paging is pointless and a checkpoint is a small file.
(fresh benchmarks pending)

Context against the attribution doc: the feed relation was the dominant chunk of the *spillable*
membership-circuit-resident term. Replacing it with a bitmap removes the bulk of that term's RSS
**and** its share of the batch/spine machinery that dh6's spill could never page out — it attacks
both levers the attribution doc named, not just the spillable one.

---

## 3. Semantics equivalence table

The feed relation's *only* role today is the **delete gate**: upserts are delivered for every
current member unconditionally (`emit_for_shapes` phase 3 delivers `members` regardless of feed
state); deletes are emitted **only** from feed retractions (`feed_deltas` where `delta < 0`,
`subquery.rs:1242`). So the bitmap must reproduce exactly: on `member==true`, `bitmap.insert(pk)`
(deliver upsert either way); on `member==false`, emit a delete **iff** `bitmap.remove(pk)` returned
`true`.

| transition | circuit today | bitmap check-and-set | same? |
|---|---|---|---|
| **insert new member** (absent→present) | feed Δ +1 (unused for emit); member row → upsert delivered | `insert()==true`; upsert delivered | ✅ |
| **insert duplicate** (present→present) | feed Δ 0 (nets nothing); member row → upsert re-delivered (idempotent on wire) | `insert()==false`; upsert re-delivered (idempotent) | ✅ |
| **delete present member** (present→absent) | feed Δ −1 → **delete emitted** | `remove()==true` → **delete emitted** | ✅ |
| **delete absent / never-member** (absent→absent) | feed Δ 0 → **nothing** (the wake-storm gate) | `remove()==false` → **nothing** | ✅ |
| **feed drop** (shape teardown) | enumerate `feed_pk_ids`, assert all `false`, deltas discarded | `map.remove(feed_id)` — O(1), whole bitmap dropped | ✅ (simpler) |
| **shape re-create** | `feed_id` is never reused (`next_feed_id` monotonic) → fresh empty relation slice | fresh empty bitmap under the new `feed_id` | ✅ |

**No transition differs.** Two idempotence facts underwrite this: the circuit's absolute upsert-set
assert nets to nothing on a re-assert or an absent-delete, and `RoaringBitmap::insert`/`remove`
return `false` in exactly those same cases. Value-changes-on-a-held-key (the one thing an
upsert-*map* would net differently) do not apply — the feed is a pure presence *set* (no per-entry
value), which is why dh6 already modelled it as `add_input_set`, not a map.

---

## 4. Consequences

### (a) Drop-time enumeration (`feed_pk_ids` / `feed_len`)

Today: `drop_subquery_shape` calls `feed_pk_ids(feed_id)` (a prefix scan of the feed spine) then
asserts every pk `false` to retract the slice via a circuit step. With bitmaps: `map.remove(feed_id)`
— **O(1)**, no enumeration, no circuit round-trip. `feed_pk_ids`/`feed_len` become
`map.get(feed_id).map(|b| b.iter()…/b.len())` — exact and always available.

This **deletes the `ELECTRIC_CIRCUITS_FEED_TRACE` knob entirely**: that knob existed only to trade the
host-side enumeration view (the published `integrate_trace` snapshot) against RAM. A bitmap is
always enumerable at zero extra cost, so the whole `feed_trace` branch, the `feeds` slot, the
`FeedSnapshot`/`set_slice` machinery, and `CircuitBytes::feeds_bytes` disappear. (It also closes the
unresolved `dbsp-ds-2hu` mystery — the sizeable RSS delta attributed to that snapshot — by removing
the operator it measured.)

### (b) Checkpoint/restore path (`dbsp-ds-mrt`, `dbsp-ds-pg5`)

**Simplifies both, materially.**

- **`dbsp-ds-mrt`** (checkpoint/restore the membership circuit): the feed relation was the bulk of
  what needed spilling *and* checkpointing (large spines at scale). With feeds as bitmaps, the
  circuit's checkpointable state shrinks to the **contributors** relation (small, even at a large
  subscription count), and the feed set checkpoints as a trivial `RoaringBitmap::serialize_into`
  per feed (a small total). The hard part of mrt (checkpointing a storage-enabled spine) may become
  unnecessary; if the contributor relation is kept in the circuit, its checkpoint is small.
- **`dbsp-ds-pg5`** (subquery shapes survive restart): pg5's blocker is that node state **and**
  `known_members` aren't persisted, so shapes drop at boot. The feed half is now solved cheaply
  (serialize/restore bitmaps). Contributor node state still needs pg5's snapshot approach, but the
  dimension that was linear in feed size is removed from the problem.
- **New moving part, but consistency is free.** There are now two artifacts to checkpoint
  consistently (the circuit's contributor relation + the host bitmaps). Because both are mutated
  under the **same registry lock at the same emission points**, a checkpoint taken under the lock is
  consistent by construction — the same SnapshotGate/xid-fencing discipline the counts tier already
  uses. Net: more artifacts, each far simpler, no new consistency research.

### (c) Spill knobs

The feed relation was the **entire** justification for §4's spill work (`ELECTRIC_CIRCUITS_SUBQ_STORAGE`,
`_DIR`, `_MIN_STORAGE_KB`, `_STORAGE_CACHE_MIB`, plus `SpillConfig`, `spill_config_from_env`,
`default_spill_dir`, `process_alive`, the `with_storage` block in `start_full`, and the
`spill_mode_preserves_semantics_and_writes_files` test — ~90 lines). With feeds gone, the only
remaining circuit relation is contributors.

**Recommendation:** treat the spill machinery as **candidate dead code**, but do not delete it in
the same increment. Contributors scale with *subscriptions × inner-query selectivity*
(`memory-model.md` §1) — small even at a large subscription count, but a high-selectivity inner
query could grow them. Keep the knobs (they already work) with the **default flipped to off** once
the dominant term is gone, and file a follow-up to delete them if no contributor-spill workload
appears.
This is a deliberately conservative call: the win here is the feed set, not deleting spill.

### (d) Which tests change mechanically vs guard semantics

**Semantic guards — must stay green UNCHANGED against the bitmap** (this is the equivalence claim):

- `subquery.rs`: `never_member_delete_is_dropped`, `genuine_member_delete_is_never_dropped`,
  `never_member_candidate_does_not_mint_pk_dict_id` — drive through `on_table_delta`/`emit_for_shapes`
  and assert *emission behaviour* (drop never-member deletes, never drop a genuine one, re-entry
  re-emits, no pk-dict minting for non-members). They should pass verbatim.
- `feed_relation_drops_deletes_for_never_known_pks` — its *assertions* (the gate) are invariant; its
  *body* pokes the circuit directly, so the body is rewritten to poke the bitmap, but what it proves
  is unchanged.

**Mechanical — rewritten or deleted with the representation:**

- `subq_circuit.rs`: `feed_deltas_gate_deletes_structurally`,
  `feed_trace_knob_disables_enumeration_not_emissions`, `feed_trace_snapshot_shares_operator_integral`,
  `feed_bytes_per_entry_is_under_32b_with_id_keys`, the feed half of `prefix_scans_enumerate_slices`,
  the `feeds_len` assertions in `snapshot_bytes_measures_and_splits_relations`, and
  `spill_mode_preserves_semantics_and_writes_files` (if spill is retired). These become bitmap-set
  unit tests or are deleted alongside `add_input_set`/`feed_in`/`feeds_out`/`drain_feed_deltas`/
  `FeedDelta`/`FeedSnapshot`/`set_slice`.
- `subquery.rs`: `emit_for_shapes` phases 1–2 rewritten (build deletes from bitmap check-and-set,
  not `asserts.feeds` + circuit deltas); `apply_asserts` returns only member flips.

---

## 5. TDD task breakdown (the implementation, if GO)

Sized in reviewable increments; each lands green before the next. Increments 1–5 are the core move;
6–8 are the cleanup/persistence follow-ups (each its own bead).

1. **`FeedSet` host structure, pure unit tests (no wiring).** `struct FeedSet(HashMap<i64,
   RoaringBitmap>)` with `insert(feed,pk)->bool`, `remove(feed,pk)->bool`, `contains`, `drop_feed`,
   `len`, `iter`, `heap_bytes` (impl `HeapSize`), `serialize_into`/`deserialize`. TDD the §3
   equivalence table row-by-row directly against this type. *Smallest, no engine change.*
2. **Shadow-wire into `emit_for_shapes` (both representations live).** Populate the `FeedSet`
   under the registry lock alongside the existing circuit feed asserts; compute deletes both ways
   and `debug_assert_eq!` the delete sets on the live path. Proves parity end-to-end without cutting
   over. **Seed the bitmap in three-phase-create phase C under the lock** (same critical section as
   today's `feed_seed`) — see §6 riskiest transition.
3. **Cut over: deletes come from the `FeedSet`; delete the circuit feed input.** Drop
   `asserts.feeds` population, `add_input_set`/`feed_in`/`feeds_out`/`drain_feed_deltas`/`FeedDelta`;
   `apply_asserts` returns only member flips. Migrate the mechanical `subq_circuit` feed tests. The
   G2 armor tests (§4d) must stay green **unchanged**.
4. **Drop path + introspection over the `FeedSet`.** `drop_subquery_shape` → `feed_set.drop_feed`
   (O(1)); `feed_pk_ids`/`feed_len` → bitmap reads; **delete the `ELECTRIC_CIRCUITS_FEED_TRACE` knob** and
   the published feed-snapshot machinery (`feeds` slot, `FeedSnapshot`, `set_slice`,
   `CircuitBytes::feeds_bytes`).
5. **`/memory` accounting + docs.** Add `bytes_feed_set` (from `FeedSet::heap_bytes`) to
   `bytes_membership_circuit`; remove `feeds_bytes`/`feeds_len` from `CircuitBytes`. Update
   `memory-model.md` §3–§4 (feed set is host-side again, small, no spill).
6. **Spill-knob decision (gated).** Measure contributor-only residency; flip the spill default to
   off; file a delete-follow-up bead if no contributor-spill workload materialises. *(§4c.)*
7. **`FeedSet` checkpoint/restore — the `dbsp-ds-mrt` unblock.** Serialize bitmaps on checkpoint,
   restore on boot, consistency under the registry lock via the existing SnapshotGate. Re-scope mrt:
   the spine-checkpoint hard part may be unnecessary. *(Own bead.)*
8. **`dbsp-ds-pg5` feed dimension.** Restore feed bitmaps at boot so shapes with feeds survive
   restart; contributor-node persistence remains pg5's separate concern. *(Own bead.)*

---

## 6. The riskiest transition, and how it is handled

All six table transitions are lock-serialized and equivalent; the risk is not in the *table* but in
the **seed/backfill hand-off + flip-worker interleave** — the real-world path where correctness can
be lost:

At shape creation the backfill seeds the initial feed set (phase C, `subquery.rs:814`). If a live
delta for that feed were processed *between* "shape registered" and "bitmap seeded", a genuine
delete would see an empty bitmap and be silently dropped (divergence), or a live delete could race a
flip-worker re-derive for the same pk. This is precisely §3-reason-1's "seeded from the shape's own
backfill and then tracks the stream" concern.

**Handling — the same guarantee dh6 relies on, made stricter:** every `FeedSet` mutation is a
synchronous `&mut self` method called only inside the registry-lock critical section. The seed
happens in three-phase-create **phase C, under the lock**, exactly where `feed_seed` is applied
today, before the shape is discoverable by any live delta. Because the check-and-set is synchronous
(no `.await`, unlike today's circuit round-trip that holds the lock *across* an await), the borrow
checker itself enforces that the emission decision and the set mutation are one indivisible step —
there is no replica to go stale (§3 reason 2 is *strengthened*, not weakened) and no window between
seed and first live delta. Increment 2's shadow `debug_assert_eq!` on the live path is the
regression net that proves the interleave stays equivalent before the circuit feed input is removed.

---

## Appendix — reasons re-litigated (map to `memory-model.md` §3–§4)

- **§3 reason 1 (output-side state, Postgres can't reseed):** *unchanged and now honoured* — the
  bitmap is host-side, seeded from the shape backfill, tracking the stream. This was the pre-dh6
  argument *for* keeping it host-side; the move restores it.
- **§3 reason 2 (must be read-modify-written atomically with the emission decision):** *better
  satisfied* — the check-and-set IS the emission decision, synchronous, under the registry lock, no
  cross-thread circuit hop in the critical section. No replica ⇒ no staleness window.
- **§3 reason 3 (in-circuit maintenance ≡ materializing the semijoin):** *moot* — a host check-and-set
  set is exactly the "RSS hash set" alternative §3 itself named; it does not make the circuit compute
  feed membership end-to-end.
- **§4 (spill to disk via dbsp storage):** *premise refuted by measurement* — the feed set stays
  small even at a large subscription count, far below the size that would justify spilling; it
  stays resident, checkpoints as a small file, and spill (which only ever covered part of the
  circuit residency) is unnecessary for it. (fresh benchmarks pending)
