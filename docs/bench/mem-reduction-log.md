# Memory-reduction iteration log

One row per accepted/rejected iteration of the shape-memory reduction plan. Every
engine-touching iteration runs Gate G — three correctness suites (G1), the two delete-gate
asymmetry tests (G2), and the two fixed-config memory benchmarks (G3/B1/B2) — before its
numbers land here. Gate G and the acceptance rule are defined in
`docs/superpowers/plans/2026-07-15-shape-memory-reduction.md`. Negative results get a row
too — rejected iterations steer the combination decisions at phase gates.

## Frozen baselines (engine at PR #37, from `docs/bench/shape-memory-scale.md` + `memory-matrix-blogpost.md`)

| metric | baseline |
|---|---:|
| B2 footprint @100k subs, in-memory, FEED_TRACE=0 | 789 MiB peak / 698 steady |
| B2 spill (cache 64 MiB) | 699 / 657 MiB |
| per-subscription state | ~8 KiB |
| B1 bytes/synced-row | ~85 B |
| B1 KiB/shape (registration) | ~3 KiB |

## Iterations

| iteration | branch/commit | B2 footprint @100k subs (peak/steady) | B2 KiB/subscription | B1 bytes/synced-row | B1 KiB/shape (registration) | G1 | G2 | verdict |
|---|---|---|---|---|---|---|---|---|
| 0 — instrumented baseline (Phase 0) | `mem/phase-0-attribution` @ dd7a259 | **1116 / 1059 MiB** (run 1: 1130 / 1130; reproduced) | ~11.4 KiB | ~269 B | ~9.7 KiB | PASS (engine:test green 172 tests; vitest green 27 files / 162 tests; electric-conformance `oracle` 1/1 green — no new failures; note: the "13/15" figure describes oracle+subqueries combined, not this suite alone) | PASS (`never_member_delete_is_dropped`, `genuine_member_delete_is_never_dropped` — both green in engine:test) | **REJECT** — headline B2 peak +41% / steady +52% vs baseline; B1 terms ~3× baseline; far beyond the ≤2% rule |

### Iteration 0 notes — the instrumentation is NOT free

Expectation was ≤2% deviation; measured (two independent ramped B2 runs, same config as the
frozen baseline): peak 1130 / 1116 MiB vs 789 (+41–43%), live-phase steady 1059+ vs 698
(+52%). B1 (10k issues, materialized, 1000 users / 10,000 shapes): registration Δ 95.1 MiB
(9.7 KiB/shape vs ~3), materialized Δ 864.7 MiB → (materialized − registration) ≈ 770 MiB
over ~3.0 M synced rows ≈ 269 B/row vs ~85. The benchmark harness is unchanged vs baseline,
the LinearLite/demo processes present during the runs were idle, and both B2 runs agree to
~1% — this is a real regression on the branch, not noise.

Prime suspect (code-confirmed, not yet profiled): `main.rs` spawns a **500 ms background
sampler** calling `Engine::mem_cardinalities`, which on this branch performs the full
Task-0.1 byte-walk every tick — `st.shapes.heap_bytes()` under the engine state lock, the
executor walk serialized onto the sequencer task via `SequencerCmd::MemBytes`, the whole
subquery-registry walk under its lock, and the retention map walk. At 50k live shapes that
is a recursive walk over ~100 MB of host structures every 500 ms, contending with the
creation path's locks and stalling the sequencer between batches (at PR #37 the same
sampler computed O(tables) counters only). Consistent with this, the Task-0.2 single-burst
attribution run measured 1147 MB on this same branch — the ramp-vs-burst reconciliation in
`mem-attribution-100k.md` is refuted by this ramped run; the elevation follows the branch,
not the run shape.

Consequences: (1) fix before Phase 1 — make the byte-walk on-demand-only (GET /memory), or
sample it at a much longer interval / skip when unchanged, then re-run Gate G to establish
the true instrumented baseline; (2) until then, frozen baselines must be compared against
non-instrumented builds or a fixed sampler. Secondary observation: both B2 runs logged
ENOBUFS (os error 55) storms in the engine sequencer's reconnect loop during the 5000-hold
live phase; all configured phases still completed.

Attribution *ratios* from `mem-attribution-100k.md` (owned-heap vs slack vs circuit) were
measured with the same tax active in both of its runs and remain directionally valid; the
absolute footprints there carry the same inflation.

## Phase 0 gate decision (per `docs/bench/mem-attribution-100k.md`)

- **dbsp circuit residency dominates**: routing the membership circuit through dbsp's
  storage backend dropped live-allocated bytes by ~454 MB (~40% of the in-memory footprint)
  while only ~59 MB landed on disk — spines/traces/per-batch structures are the dominant
  term. **Phase 1 starts with Task 1.3** (trace/snapshot pinning + circuit representation).
- **Allocator slack (11–16%) and owned host metadata (8.8–15.6%)** are both below the
  plan's 25% thresholds → Tasks 1.1 (jemalloc) and 1.2 (interning) are secondary
  follow-ups, not the lead.
- **Feed keys ≈ 27% of footprint (~3.7 M keys × 88 B floor)** is well above the 15%
  threshold → **Phase 2 (compact feed-key representation) proceeds** (full variant, not
  just the fingerprint).
