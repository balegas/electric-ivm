# Shape-Memory Reduction — Phased Experiment Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce engine memory per shape/subscription (today ~8 KiB/subscription + ~85 B/synced-row at 100k subscriptions ≈ 789 MiB footprint) by testing the highest-impact approaches first, one iteration at a time, with a fixed correctness + benchmark loop after every iteration.

**Architecture:** A shared verification loop (Gate G: three test suites + two memory benchmarks against frozen baselines) wraps a sequence of independently shippable phases: (0) attribution — instrument where the bytes actually are; (1) host-metadata slimming + allocator; (2) pk dictionary + compact feed-key representation; (3) semijoin factorization (child plan, gated); (4) partial-state eviction (child plan, gated). After each phase, a decision gate compares the measured residual against targets and decides whether the next approach is still worth combining.

**Tech Stack:** Rust engine (`apps/engine`), Feldera dbsp membership circuit, vitest bench harness (`packages/bench`), Electric oracle conformance (elixir), macOS `footprint`/`vmmap` for measurement.

## Global Constraints

- **Task tracking:** create one bead per phase before starting it (`bd create --title="mem: <phase>" --type=task --priority=2`); close it at phase gate. Do NOT use TodoWrite/markdown TODOs. Existing related beads: `dbsp-ds-4d8` (feed double-copy), `dbsp-ds-mrt` (checkpoint), `dbsp-ds-pg5` (persistence).
- **Git:** conservative profile — branch per phase (`mem/phase-<n>-<slug>`), commit frequently, no push/PR without explicit approval.
- **Every engine-touching iteration must pass Gate G in full** (see below) before its numbers are recorded or the next iteration starts.
- **Semantics invariant (non-negotiable):** the delete-gate may only fail OPEN, never CLOSED — a spurious delete for a never-member pk is idempotent (one wasted wake); a *dropped genuine delete is divergence*. Any approximate/compact structure must preserve this asymmetry and encode it in a test.
- **Benchmarks:** `release` build only; macOS phys footprint (`/usr/bin/footprint`) is the state metric, `ps rss` is the hot-set metric; clean 0-attach shm segments between runs (`ipcs -ma` / `ipcrm -m`).
- **Numbers land in** `docs/bench/mem-reduction-log.md` (created in Phase 0) — one row per iteration, cumulative, so regressions across phases are visible.

---

## Gate G — the correctness + benchmark loop (run after EVERY iteration)

This is the loop the user asked for. It is identical for all phases; each task below ends with "Run Gate G".

### G1. Correctness suites (all three must be green)

```bash
# 1. Rust unit + integration (fast, run first)
pnpm engine:test

# 2. Full vitest suite incl. oracle conformance (boots its own Postgres)
cargo build --release -p electric-ivm-engine
ELECTRIC_IVM_ENGINE_PREBUILT=1 pnpm test

# 3. Electric's own oracle vs /v1/shape (needs elixir + ../electric)
ASDF_ELIXIR_VERSION=1.18.4-otp-28 ASDF_ERLANG_VERSION=28.1 \
  ./electric-conformance/run.sh oracle
```

Expected: suites 1–2 fully green; suite 3 at the **13/15 baseline** (2 known row-tags
protocol gaps). Any new failure = the iteration is rejected until fixed or reverted.

### G2. Targeted regression: the delete-gate asymmetry

The wake-storm bug class (pre-PR#30) is the failure mode most of these optimizations
could reintroduce. Every iteration that touches feed keys, emission gating, or the
membership circuit must keep these two properties green (added as engine tests in
Phase 0, Task 0.3):

- `never_member_delete_is_dropped` — a delete for a pk the feed never contained does
  not reach the stream (no spurious wake).
- `genuine_member_delete_is_never_dropped` — a pk that entered the feed and then
  leaves ALWAYS produces exactly one delete on the stream, across circuit steps,
  flip-driven query-backs, and shape drop/re-create.

### G3. Memory benchmarks (both, fixed configs, compare against baseline table)

```bash
# B1 — matrix (medium scale, materialized): the ~85 B/row and ~3 KiB/shape terms
cargo build --release -p electric-ivm-engine
MATRIX_SIZES=10000 MATRIX_USERS=100,500,1000 MATRIX_PROJECTS=20 MATRIX_MATERIALIZED=1 \
  pnpm --filter @electric-ivm/bench exec tsx src/shape-mem-matrix.ts

# B2 — scale (100k subscriptions, the blog-post scenario): footprint is the headline
# (FEED_TRACE removed in Phase 2)
SCALE_ISSUES=100000 SCALE_PROJECTS=2000 SCALE_USERS=100,1000,2500,5000,10000 \
SCALE_CLIENT_PROCS=4 SCALE_LIVE_RAMP=5000 SCALE_LIVE_PROCS=8 \
  pnpm --filter @electric-ivm/bench exec tsx src/shape-mem-scale.ts
```

Record per iteration in `docs/bench/mem-reduction-log.md`:

| iteration | branch/commit | B2 footprint @100k subs (peak/steady) | B2 KiB/subscription | B1 bytes/synced-row | B1 KiB/shape (registration) | G1 | G2 | verdict |

**Frozen baselines (from `docs/bench/shape-memory-scale.md` + `memory-matrix-blogpost.md`, engine at PR #37):**

| metric | baseline |
|---|---:|
| B2 footprint @100k subs, in-memory, FEED_TRACE=0 | 789 MiB peak / 698 steady |
| B2 footprint @100k subs, spill cache 64 MiB | 699 / 657 MiB |
| per-subscription state | ~8 KiB |
| B1 bytes per synced row (materialized − registration) | ~85 B |
| B1 KiB per shape (registration) | ~3 KiB |

### G4. Acceptance rule (per iteration)

- **Accept** if: G1 green, G2 green, the iteration's *target metric* improves ≥10%,
  and no other metric regresses >5% (footprint AND rss; creation-storm peak counts).
- **Reject/revert** otherwise. A rejected iteration still gets a log row — negative
  results steer the combination decisions at phase gates.
- **Live-path iterations additionally** drive the LinearLite demo once
  (`pnpm demo:linearlite`, per AGENTS.md checklist) before the phase closes.

---

## Phase 0 — Attribution: find out where the 789 MiB actually is

The scale report's §4 says the bottleneck *moved* to "per-shape host metadata +
allocator retention + possibly trace-snapshot pinning" — but that is an inference,
not a measurement. Everything downstream (which phase to run, whether to combine)
depends on this attribution, so it comes first. No optimization in this phase.

**Branch:** `mem/phase-0-attribution`

### Task 0.1: Byte-level self-accounting in `/memory`

**Files:**
- Create: `apps/engine/src/heap_size.rs`
- Modify: `apps/engine/src/mem.rs` (extend `Cardinalities`)
- Modify: `apps/engine/src/engine/introspection.rs` (compute the new fields in `mem_cardinalities`)
- Modify: `apps/engine/src/lib.rs` (register module)
- Test: `apps/engine/src/heap_size.rs` (inline `#[cfg(test)]`)

**Interfaces:**
- Produces: `trait HeapSize { fn heap_bytes(&self) -> usize; }` with impls for
  `String`, `Vec<T: HeapSize>`, `HashMap<K,V>`, `HashSet<K>`, `Option<T>`, and the
  engine structs below. Byte estimates are *lower bounds* (owned heap, not allocator
  slack) — that is the point: the gap between the sum and `footprint` IS the
  allocator/pinning term.
- Produces: new `Cardinalities` fields (all `usize` bytes):
  `bytes_shape_records`, `bytes_executors` (StandaloneShape/RoutedShape/KeyRouter/AggShape),
  `bytes_retention`, `bytes_subquery_registry` (nodes + pk_value + pk_nodes + templates),
  `bytes_membership_circuit` (via dbsp trace size hooks where available, else key-count × key-size),
  `bytes_electric_adapter` (the TTL handle sets in `electric.rs`).

- [ ] **Step 1: Write the failing test**

```rust
// apps/engine/src/heap_size.rs (bottom)
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn string_heap_bytes_is_capacity() {
        let s = String::from("hello");
        assert_eq!(s.heap_bytes(), s.capacity());
    }

    #[test]
    fn map_heap_bytes_counts_keys_values_and_buckets() {
        let mut m: HashMap<String, String> = HashMap::new();
        m.insert("k".repeat(100), "v".repeat(100));
        // at least the owned key+value heap; bucket overhead estimated at
        // 1.1 × capacity × entry size
        assert!(m.heap_bytes() >= 200);
    }
}
```

- [ ] **Step 2: Run to verify it fails** — `cargo test -p electric-ivm-engine heap_size` → FAIL (module missing).

- [ ] **Step 3: Implement `heap_size.rs`**

```rust
//! Lower-bound owned-heap accounting for the /memory breakdown. Estimates, not
//! allocator truth — the delta vs. phys footprint is the allocator/pinning term.
use std::collections::{HashMap, HashSet};

pub trait HeapSize {
    fn heap_bytes(&self) -> usize;
}

impl HeapSize for String {
    fn heap_bytes(&self) -> usize { self.capacity() }
}
impl<T: HeapSize> HeapSize for Vec<T> {
    fn heap_bytes(&self) -> usize {
        self.capacity() * std::mem::size_of::<T>()
            + self.iter().map(HeapSize::heap_bytes).sum::<usize>()
    }
}
impl<K: HeapSize, V: HeapSize> HeapSize for HashMap<K, V> {
    fn heap_bytes(&self) -> usize {
        let entry = std::mem::size_of::<(K, V)>() + 1; // +1 ctrl byte (swiss table)
        (self.capacity() * entry * 11) / 10
            + self.iter().map(|(k, v)| k.heap_bytes() + v.heap_bytes()).sum::<usize>()
    }
}
impl<K: HeapSize> HeapSize for HashSet<K> {
    fn heap_bytes(&self) -> usize {
        let entry = std::mem::size_of::<K>() + 1;
        (self.capacity() * entry * 11) / 10
            + self.iter().map(HeapSize::heap_bytes).sum::<usize>()
    }
}
impl<T: HeapSize> HeapSize for Option<T> {
    fn heap_bytes(&self) -> usize { self.as_ref().map_or(0, HeapSize::heap_bytes) }
}
// numeric/leaf impls: zero owned heap
macro_rules! leaf { ($($t:ty),*) => { $(impl HeapSize for $t { fn heap_bytes(&self) -> usize { 0 } })* } }
leaf!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize, bool, f32, f64);
```

Then implement `HeapSize` for `ShapeRecord`, `StandaloneShape`, `RoutedShape`,
`KeyRouter`, `AggShape` (`engine/executors.rs`), `SubqueryNode`, `SubqueryShape`,
`SubqueryRegistry` (`subquery.rs`), and the retention/electric maps — field-by-field
sums, following each struct's actual fields (read them at implementation time; do
not guess).

- [ ] **Step 4: Wire into `mem_cardinalities` + `Cardinalities` + the `/memory` JSON.** Keep gauges out of OTel for now (JSON only) to avoid metric churn.

- [ ] **Step 5: Run the tests** — `cargo test -p electric-ivm-engine heap_size` and `pnpm engine:test` → PASS.

- [ ] **Step 6: Commit** — `git commit -m "mem: byte-level self-accounting in /memory (heap_size trait)"`

### Task 0.2: Region-level attribution runbook (allocator vs. owned heap)

**Files:**
- Create: `docs/bench/mem-attribution-100k.md` (results doc)
- Modify: `packages/bench/src/shape-mem-scale.ts` (add `SCALE_ATTRIBUTION=1`: after the final milestone, curl `/memory`, run `vmmap --summary <pid>` and `footprint <pid>`, and dump all three into the bench output dir)

**Steps:**

- [ ] **Step 1:** Add the `SCALE_ATTRIBUTION` hook to the bench script (spawn `vmmap`/`footprint` via `child_process.execFile`, write alongside the existing raw tables).
- [ ] **Step 2:** Run B2 at `SCALE_USERS=10000` with `SCALE_ATTRIBUTION=1`, in-memory and with spill (`ELECTRIC_IVM_SUBQ_STORAGE_DIR`), one run each.
- [ ] **Step 3:** Write `docs/bench/mem-attribution-100k.md`: a table attributing footprint into — (a) self-accounted owned heap per subsystem (Task 0.1 numbers), (b) MALLOC region total minus (a) = allocator slack/retention, (c) dbsp trace/batch bytes, (d) unattributed. State explicitly which of Phase 1's three hypotheses (metadata, allocator, snapshot pinning) the numbers support, with magnitudes.
- [ ] **Step 4:** Commit — `git commit -m "bench: 100k-subscription memory attribution runbook + results"`

### Task 0.3: Encode the delete-gate asymmetry as permanent tests (G2)

**Files:**
- Modify: `apps/engine/src/engine/tests.rs` (or the existing integration-test home for emission tests — locate the current subquery emission tests and sit next to them)

**Steps:**

- [ ] **Step 1: Write both tests (failing is not expected here — they must pass on main; they exist to fail later).**

```rust
#[tokio::test]
async fn never_member_delete_is_dropped() {
    // subquery shape over issues WHERE project_id IN (...user 1...);
    // write+delete an issue in a NON-matching project; assert the shape's
    // stream received zero appends (no spurious wake).
}

#[tokio::test]
async fn genuine_member_delete_is_never_dropped() {
    // row enters the feed (matching insert), then leaves via (a) row delete,
    // (b) membership flip (user loses the project); assert exactly one
    // delete emission per exit path, and that a re-entering pk re-emits.
}
```

Flesh these out against the existing test harness patterns in the same file
(reuse its engine-boot + shape-create helpers verbatim).

- [ ] **Step 2:** `pnpm engine:test` → both PASS on the unmodified engine.
- [ ] **Step 3:** Commit — `git commit -m "test: delete-gate asymmetry invariants (G2 loop tests)"`

### Phase 0 gate (decision point)

Run Gate G once end-to-end on the instrumented engine (expect: no metric change >2% — instrumentation must be ~free) and record the **iteration-0 row** in `docs/bench/mem-reduction-log.md` (create the file with the baseline table from G3).

**Decide from the attribution doc:**
- If allocator slack ≥ 25% of footprint → Phase 1 starts with Task 1.1 (allocator).
- If owned per-shape metadata ≥ 25% → Task 1.2 (interning) is the priority.
- If dbsp batch/pinning dominates → Task 1.3 first.
- Feed-key owned bytes (~3.7 M keys) sets the ceiling on Phase 2's win — if it is < 15% of footprint at 100k subs, **skip Phase 2's circuit-key rework and do only the cheap fingerprint variant**, jumping the effort to Phase 3/4 evaluation.

---

## Phase 1 — Host metadata + allocator (the measured §4 bottleneck)

Three independent iterations, each individually accepted/rejected via Gate G.
Order within the phase comes from the Phase 0 gate. Expected combined win at 100k
subs: 200–400 MiB.

**Branch:** `mem/phase-1-host-slimming`

### Task 1.1: Allocator iteration — jemalloc with decay

**Files:**
- Modify: `apps/engine/Cargo.toml` (add `tikv-jemallocator = "0.6"` behind default-on feature `jemalloc`)
- Modify: `apps/engine/src/main.rs` (global allocator + `MALLOC_CONF` docs)

**Steps:**

- [ ] **Step 1:**

```toml
# Cargo.toml
[features]
default = ["jemalloc"]
jemalloc = ["dep:tikv-jemallocator"]

[dependencies]
tikv-jemallocator = { version = "0.6", optional = true }
```

```rust
// main.rs
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
```

- [ ] **Step 2:** `pnpm engine:test` → PASS (allocator swaps are behaviorally invisible; this catches build issues).
- [ ] **Step 3:** Run Gate G. The interesting comparison is B2 **peak vs steady** footprint: jemalloc's decay should collapse the creation-storm retention. Also try `MALLOC_CONF=dirty_decay_ms:1000,muzzy_decay_ms:1000` as a second data point (log both rows).
- [ ] **Step 4:** Accept/reject per G4. If macOS jemalloc behaves poorly (known platform roughness), test `mimalloc = "0.1"` as the alternative before rejecting the iteration.
- [ ] **Step 5:** Commit — `git commit -m "mem: jemalloc global allocator (feature-gated) + decay tuning"`

### Task 1.2: String interning for per-shape metadata

**Files:**
- Create: `apps/engine/src/intern.rs`
- Modify: the top-N string-holding structs from the Phase 0 attribution (expected: `ShapeRecord` + catalog maps in `engine/catalog.rs`, `StandaloneShape`/`RoutedShape` in `engine/executors.rs`, stream paths in `engine/emission.rs` / `retention.rs`)
- Test: inline in `intern.rs` + existing suites

**Interfaces:**
- Produces: `pub struct Interner` (global, `OnceLock`), `pub fn intern(s: &str) -> Istr` where `Istr = Arc<str>` newtype with `Deref<Target=str>`, `Hash`, `Eq`, `Serialize`. Interning is append-only (shape vocabulary — table names, columns, signatures, path segments — is small and stable; no eviction in v1).

**Steps:**

- [ ] **Step 1: Failing test**

```rust
#[test]
fn intern_dedupes_storage() {
    let a = intern("public.issues");
    let b = intern("public.issues");
    assert!(std::ptr::eq(a.as_ptr(), b.as_ptr()));
}
```

- [ ] **Step 2:** `cargo test -p electric-ivm-engine intern` → FAIL → implement (`RwLock<HashSet<Arc<str>>>` behind `OnceLock`) → PASS.
- [ ] **Step 3:** Convert the attribution's top-3 string fields to `Istr`, one struct per commit, `pnpm engine:test` between each. Do NOT convert pk strings here (that is Phase 2's job — pks are high-cardinality and don't intern).
- [ ] **Step 4:** Run Gate G; the target metric is B2 KiB/subscription and `bytes_shape_records`+`bytes_executors` from the self-accounting (which makes the win directly attributable).
- [ ] **Step 5:** Commit per struct; final commit `"mem: intern shape-metadata strings (Istr)"`.

### Task 1.3: Trace-snapshot pinning audit

**Files:**
- Modify: `apps/engine/src/subq_circuit.rs` (snapshot lifetime), possibly `apps/engine/src/trace.rs`

**Steps:**

- [ ] **Step 1:** Instrument: log/gauge the count + total bytes of batches reachable from published `integrate_trace` snapshots vs. the operator integrals (add to Task 0.1's `bytes_membership_circuit` split: `bytes_circuit_integral` vs `bytes_circuit_snapshots`).
- [ ] **Step 2:** Reproduce: run B2 to 10k users, then force a write burst and sample — if `bytes_circuit_snapshots` grows or holds pre-compaction batches, snapshots are pinning.
- [ ] **Step 3:** Fix shape depends on finding (bounded snapshot window / explicit re-snapshot after step / drop+reacquire around compaction) — write the failing test that asserts post-compaction snapshot bytes ≤ integral bytes × 1.5, then fix.
- [ ] **Step 4:** Run Gate G; commit.

### Phase 1 gate (decision point)

Sum the accepted iterations' wins in the log. **Decide:**
- If B2 footprint @100k ≤ ~450 MiB (≈4.5 KiB/subscription), per-shape metadata is no longer dominant → proceed to Phase 2 only if feed-key owned bytes (self-accounted) are now the top term; otherwise go straight to the Phase 3/4 evaluation.
- If < 15% total win, the attribution was wrong — STOP, re-run Task 0.2 on the new binary, and re-plan before spending on Phase 2.

---

## Phase 2 — Feed keys: pk dictionary + compact membership (85 B/row → ≤16 B/row)

Two variants, tried in order; the second only if the first's Gate G run leaves
feed-key bytes > 15% of footprint. Both must keep G2's asymmetry: **collisions/
approximation may only ADD spurious deletes, never drop genuine ones.**

**Branch:** `mem/phase-2-feed-keys`

### Task 2.1: Global pk dictionary (pk string ↔ u32) + integer-keyed feed relation

**Files:**
- Create: `apps/engine/src/pk_dict.rs`
- Modify: `apps/engine/src/subq_circuit.rs` (feed relation keys `(u32, u32)` instead of `Row([Int, Text])`; `FeedDelta` carries `pk_id`)
- Modify: `apps/engine/src/subquery.rs` (`pk_value`/`pk_nodes` keyed by `pk_id`; emission paths translate back via the dictionary's reverse vec)
- Test: `pk_dict.rs` inline + G2 tests + a property test

**Interfaces:**
- Produces: `pub struct PkDict` — `fn get_or_insert(&self, pk: &str) -> u32` (append-only, sharded `RwLock<HashMap<Arc<str>, u32>>` + `RwLock<Vec<Arc<str>>>` reverse), `fn resolve(&self, id: u32) -> Arc<str>`. One instance per engine, shared by registry + circuit + emission. The reverse vec is O(distinct pks ever synced) — SHARED across all feeds (that is the win: per-feed cost drops to 8 B/entry, amortized string storage once).
- Note: dictionary entries are never freed in v1 (matches feed-id non-reuse precedent from FEED_TRACE=0); log the dict's own bytes in `/memory` (`bytes_pk_dict`) so the trade is visible.

**Steps:**

- [ ] **Step 1: Failing tests** — `get_or_insert` idempotence, `resolve` round-trip, concurrent insert stress (spawn 8 threads × 10k pks, assert unique ids + consistent resolve).
- [ ] **Step 2:** Implement `pk_dict.rs` → tests PASS.
- [ ] **Step 3: Failing integration expectation** — temporarily assert in a new engine test that after materializing a 1k-row feed, `bytes_membership_circuit` < 1k × 32 B (impossible with string keys) → FAIL.
- [ ] **Step 4:** Convert the feed relation + registry maps to `pk_id` keys. Emission is the delicate seam: deltas leaving the circuit resolve `pk_id → Arc<str>` once per batch, before the registry lock is released (order-preserving).
- [ ] **Step 5:** Step-3 test PASSes; run G2 tests; run full Gate G. Target metric: B1 bytes/synced-row ≤ 20 B and B2 footprint delta ≈ −(3.7 M × ~65 B) ≈ −230 MiB at 100k subs *if* Phase 0 confirmed feed keys as owned bytes of that magnitude.
- [ ] **Step 6:** Commit — `"mem: pk dictionary — integer-keyed feed relation and registry"`.

### Task 2.2 (conditional): Roaring bitmaps for feed membership

Only if after 2.1 the feed term still > 15% of footprint. Replace the per-feed side
of the relation with `roaring::RoaringBitmap` over `pk_id` (crate `roaring = "0.10"`),
held OUTSIDE the dbsp spine as an operator-adjacent index, or — preferred if
tractable — as a custom batch implementation. **Spike first (timeboxed 1 day):**
whether dbsp's batch/trait interfaces admit a bitmap-backed batch without forking
dbsp; if not, fall back to bitmap-shadowing (bitmap for gating reads, spine remains
source of truth for retraction enumeration) and measure whether the shadow pays for
itself. Full TDD breakdown authored at spike exit as an addendum to this file.

### Phase 2 gate (decision point)

- If cumulative B2 footprint @100k ≤ **300 MiB** (≈3 KiB/subscription): the blog-post
  target zone — STOP structural work; file Phase 3/4 as backlog beads with the
  measured residuals attached, and prepare the write-up.
- Else: compute from the log which residual dominates —
  - per-feed keys still (heavy overlap across feeds) → **Phase 3** (factorization attacks exactly the duplication across feeds).
  - total synced-set size with mostly-idle feeds → **Phase 4** (eviction attacks cold state).
  - Both child plans below; write the detailed plan only for the chosen one.

---

## Phase 3 (child plan, gated) — Semijoin factorization: per-group shared key sets

**Trigger:** Phase 2 gate chose it. **Write as** `docs/superpowers/plans/YYYY-MM-DD-semijoin-factorization.md` when triggered.

**Design brief for that plan (so the decision at the gate is informed):** store
`(template, join-key value) → pk set` ONCE, shared by all binds (the shared-arrangements
move); a feed's key set becomes the union over its membership set (which the circuit
already maintains). Per-feed state drops from O(feed rows) to O(feed groups). The
delicate parts the child plan must cover: rows changing groups (the `(group, pk)`
relation must drive per-feed enter/leave transitions), delete-gating without a
per-feed set (gate = "pk's group ∈ feed's group set at last emission" — needs the
group-at-emission remembered per (group,pk), not per (feed,pk)), and drop-time
enumeration. Success bar: B1 bytes/synced-row becomes sublinear in feed overlap
(measure with the matrix workload where all users share projects).

## Phase 4 (child plan, gated) — Partial-state eviction with stream-fold reseed

**Trigger:** Phase 2/3 gates chose it, or product needs memory ∝ hot working set.
**Write as** `docs/superpowers/plans/YYYY-MM-DD-feed-key-eviction.md` when triggered.

**Design brief:** the feed key set is a cache of a fold over the feed's own stream
(the dbsp-ds-4d8 observation). Add a global LRU budget (`ELECTRIC_IVM_FEED_KEYS_CACHE_MIB`);
evicted feeds either (a) re-seed by folding the stream tail on first touch, or
(b) temporarily emit ungated (spurious deletes are idempotent; only cold feeds pay).
The child plan must cover: reseed atomicity vs. in-flight emissions (same critical
section discipline as `filter_known_members` had), eviction interaction with the
circuit-resident relation (evict = retract from circuit? or bypass circuit for
evicted feeds?), and a new benchmark scenario measuring steady-state memory with
90% idle feeds. This phase naturally merges with `dbsp-ds-mrt`/`dbsp-ds-pg5`
(checkpoint/persistence) — evaluate doing them together.

---

## Self-review notes

- Every phase ends in Gate G with exact commands and a frozen baseline table — the requested test/benchmark loop is closed.
- Highest-impact-first is enforced by Phase 0 measuring *before* optimizing, and each gate re-decides the ordering with data; combination decisions are explicit gate criteria, not implicit.
- Phases 3/4 are deliberately child plans (scope check: independent subsystems); their design briefs carry enough content to make the gate decision without re-research.
- Type/naming consistency: `Istr` (1.2), `PkDict`/`pk_id: u32` (2.1), `Cardinalities` byte fields (0.1) are each defined once and referenced by those names throughout.
