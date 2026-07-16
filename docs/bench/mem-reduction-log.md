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
| 0b — instrumented baseline, sampler fixed (66117f2) | `mem/phase-0-attribution` @ 66117f2 | **1091 / 1107 MiB** | ~11.2 KiB | ~185 B | ~9.5 KiB | PASS (engine:test 173 green incl. new `sampler_cardinalities_never_populates_bytes_fields`; vitest 27 files / 162 tests green; conformance `oracle` 1/1 green) | PASS (both delete-gate tests green, verified at 66117f2) | **ACCEPT vs same-day control** (baseline commit 3213e41, same command: 1111 / 1053 → peak −1.8%, steady +5.1%, within the observed run-to-run spread of the steady cell); the frozen 789/698 row is NOT reproducible under the Gate-G config on today's machine — see the control note |
| 1 — circuit observability split, Task 1.3 (380a0f9) | `mem/phase-1-host-slimming` @ 380a0f9 | **1125 / 1062 MiB** | ~11.5 KiB | ~204 B | ~9.5 KiB | PASS (engine:test 157 lib + integration green incl. new `snapshots_do_not_pin_precompaction_batches` + `snapshot_bytes_measures_and_splits_relations`; vitest 27 files / 162 tests green; conformance `oracle` 1/1 green) | PASS (both delete-gate tests green at 380a0f9) | **ACCEPT vs 0b** (peak +3.1%, steady −4.1%, B1 reg unchanged — all inside the ~5–7% observed run spread; observability-only diff). FEED_TRACE A/B piggyback: on-vs-off delta ~1% → dbsp-ds-2hu resolved, see notes |
| 2 — pk dictionary, Task 2.1 (2e94ddc) | `mem/phase-2-feed-keys` @ 2e94ddc | **1102 / 1046 MiB** | ~11.3 KiB | ~86 B (legacy ΔRSS) / **~9 B owned** (circuit+dict, primary) | ~8.5 KiB | PASS (engine:test 164 green incl. PkDict suite + `never_member_candidate_does_not_mint_pk_dict_id`; vitest 162 green; conformance `oracle` 1/1 green) | PASS (both delete-gate tests green at 2e94ddc) | **ACCEPT vs it1** — target term (owned feed/circuit bytes) collapsed (B1 materialized ΔRSS halved 675.8→329.2 MiB; synced-row 204→86 B legacy; owned ~9 B/row); B2 footprint unchanged (−2.0% / −1.5%, noise) → the 100k footprint is dbsp residency overhead, not key payload — see notes + Phase 2 gate data |

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

### Iteration 2 notes — pk dictionary (Task 2.1); Phase 2 gate data

Change under test (`mem/phase-2-feed-keys` @ 2e94ddc): global `PkDict` (append-only
pk-string ↔ u32 interner, new `/memory` field `bytes_pk_dict`); circuit relations and
registry maps keyed by u32 pk ids; feed relation converted from upsert-map to key-only
upsert set. Test-scale measurement: 123 → 24 B per feed entry. Comparison point =
iteration 1.

**B1 — where the dictionary demonstrably saved.** Registration: Δ 82.9 MiB (**8.5
KiB/shape**, −10.5% vs it1). Materialized: Δ **329.2 MiB, half of it1's 675.8** (footprint
variant: Δ 296.3 MiB); legacy synced-row (matΔ−regΔ)/~3.0 M rows ≈ **86 B** vs 204.
Owned-bytes variant (new 05f4777 columns; Δ(`bytes_membership_circuit`+`bytes_pk_dict`)
main-loop milestone deltas — the materialized-probe delta reads 0 by design):
27,267 KiB (materialized) − 948 KiB (registration) ≈ 26.3 MiB / ~3.0 M rows ≈ **9 B per
synced row owned** — the logical feed-entry cost after u32 keying (no it1 owned reference;
the columns are new this iteration).

**B2 — where it didn't move the headline.** 1102 peak / 1046 steady vs it1's 1125 / 1062
(−2.0% / −1.5%, inside the run spread; 20 ENOBUFS warnings, run completed). Attribution
snapshot at the top milestone (100k subs): `bytes_membership_circuit` **1.8 MiB**
(integral 1.1 + snapshots 0.7) + `bytes_pk_dict` **6.3 MiB**; owned-heap sum 92.2 MiB.
Caveat: under the Gate-G `FEED_TRACE=0` config the FEEDS integral term reads **zero** by
construction (`snapshot_bytes` doc, subq_circuit.rs), so B2's owned circuit number
excludes the ~3.7 M feed keys even though they remain resident in the gating integral —
B1's ~9 B/row owned (trace on) is the valid logical-feed-cost evidence.

**The finding:** the dictionary collapsed the logical feed-key bytes (~40× vs the Phase-0
88 B/key owned floor; ~5× at test scale) and halved B1-scale RSS, but the 100k footprint
is unchanged — so the dominant 100k cost is **dbsp per-entry/spine residency overhead and
allocator retention, not key payload width** (consistent with Phase 0's 8× in-memory vs
serialized blow-up). Shrinking entry payloads further cannot move the headline; reducing
the **number of resident entries/batches** (bitmap/set-per-feed representations,
compaction, spill tuning) can.

**Post-dict spill attribution (one extra B2, spill cache 64 MiB, HEAD 2e94ddc — full
numbers in `mem-attribution-100k.md` §2b):** spillable circuit-resident term **~459 MB**
(pre-dict: 454 — unchanged by the 5× payload shrink; on-disk serialized form halved to
27 MB → ~17× resident/serialized blow-up), allocator slack ~92–96 MB (8–15%), remainder
~551 MB (96.8 owned + ~8 non-malloc + ~446 non-spillable circuit machinery) — the
footprint is entry/batch-count-driven, not payload-driven.

**Phase 2 gate data (controller decides):**
- Criterion "cumulative footprint @100k ≤ ~300 MiB → stop structural work":
  **NOT met** — 1102 peak / 1046 steady (same-day series: 0b 1091/1108 → it1 1125/1062 →
  it2 1102/1046).
- Criterion "feed/circuit term still >15% of footprint → Task 2.2 (roaring bitmaps) stays
  on the table": two readings. *Owned/logical*: ~8 MiB measured at B2 (feed-blind, see
  caveat) or ~38 MiB estimated incl. feed keys from B1's 9 B/row — **≈0.7–3.5%, well
  under 15%**. *Resident*: the Phase-0 spill-delta method put circuit residency at
  ~40% of footprint, and iteration 2's unchanged footprint says that overhead is still
  there — **>15% in residency terms**. Note Task 2.2's mechanism (one bitmap per feed
  instead of one dbsp tuple per key) attacks the per-entry overhead — the residency
  reading is the relevant one for it.

### Iteration 1 notes — circuit observability split (Task 1.3); FEED_TRACE A/B

Change under test (`mem/phase-1-host-slimming` @ 380a0f9): observability-only —
`bytes_membership_circuit` split into `bytes_circuit_integral` / `bytes_circuit_snapshots`
via real dbsp sizing, a snapshot-pinning regression test
(`snapshots_do_not_pin_precompaction_batches`), and doc corrections. Expected no benchmark
movement vs 0b; measured (comparison point = iteration 0b, NOT the stale frozen row):

- **B2 (FEED_TRACE=0, spill-by-default)**: peak 1125 / steady 1062 MiB vs 0b's 1091 / 1108
  → +3.1% / −4.1%, both inside the ~5–7% run-to-run spread this cell has shown on
  identical configs. No ENOBUFS.
- **B1**: registration Δ 93.1 MiB (**9.5 KiB/shape**, = 0b); materialized Δ 675.8 MiB →
  (mat − reg) ≈ 583 MiB / ~3.0 M rows ≈ **204 B/row** vs 0b's 185 (+10% — an RSS-based
  cell whose iteration-0→0b swing was −28% on identical structural state; treated as
  noise, flagged for watching).
- **Verdict: ACCEPT** — no regression signal attributable to an observability-only diff.

**FEED_TRACE A/B (resolves bead dbsp-ds-2hu).** One extra B2, identical config except
`ELECTRIC_IVM_FEED_TRACE=1`, same day, back-to-back, both spill-by-default:

| run | peak @100k | live-5000 | +15 s (steady) |
|---|---:|---:|---:|
| FEED_TRACE=0 | 1125 MiB | 1125 | 1062 |
| FEED_TRACE=1 | 1135 MiB | 1135 | 1075 |

Delta ~10–13 MiB (~1%) — the historical ~323 MiB saving did **not** reappear. This
matches the Task-1.3 audit mechanism (the published feed trace shares the upsert
operator's integral via dbsp's TraceId cache — no second copy): the old 731.8-vs-408.1
RSS measurement was an artifact of its era (creation-peak `ps rss` on different code and
machine state, likely allocator/paging effects), not a real steady saving of the knob
today. `docs/bench/shape-memory-scale.md` §3 updated; **bead dbsp-ds-2hu can be closed**.

### Iteration 0b notes — sampler fixed; the frozen baselines are not reproducible under the Gate-G configs

Commit 66117f2 makes `mem_cardinalities()` counts-only again (all `bytes_*` zero from the
500 ms sampler; the byte-walk moved to `Engine::mem_bytes`, called only from GET `/memory`,
enforced by `sampler_cardinalities_never_populates_bytes_fields`). Re-running Gate G:

- **B2 (0b)**: peak 1091 / steady 1107 MiB (live-5000 phase; zero ENOBUFS this run).
- **Control** — the exact Gate-G B2 command on the **frozen-baseline commit 3213e41**
  (binary swapped, same harness, same machine, same hour): **peak 1111 / steady 1053 MiB**
  (8 ENOBUFS warnings). The baseline engine itself lands at ~1.1 GiB, not 789/698 —
  **the frozen B2 row is not reproducible under the plan's fixed Gate-G config on today's
  machine state**, so the mechanical ≤2%-vs-frozen rule cannot attribute the gap to this
  branch. Likely contributors to frozen-vs-today: the frozen raw runs used a finer user
  ramp (100,250,500,1000,2500,5000,10000) and a 5k→20k live ramp; spilling flipped to
  default-ON in PR #37 itself (the Gate-G command sets no storage env, so every run here
  — including the control — is spill-by-default with the default cache, matching neither
  frozen row exactly); and macOS-side state differs from the day the baseline was frozen.
- **Instrumentation-cost verdict (0b vs control, like-for-like)**: peak −1.8%, steady
  +5.1% — the steady cell's observed run-to-run spread on identical configs is ~7%
  (1059 vs 1130 across iteration-0's two runs), so this is within noise:
  **instrumentation after the sampler fix is ~free; iteration 0b ACCEPTED against the
  same-day control.**
- **B1 (0b)**: registration Δ 92.5 MiB (~9.5 KiB/shape), materialized Δ 621.5 MiB →
  (mat − reg) ≈ 529 MiB / ~3.0 M synced rows ≈ **185 B/row**. The sampler fix recovered
  ~28% of the materialized run (864.7 → 621.5) but the registration term is unchanged
  (95.1 → 92.5) — its elevation vs the frozen ~3 KiB predates the sampler issue. No
  baseline-commit control was run for B1 (run budget); note the frozen B1 terms come from
  `memory-matrix-blogpost.md` measured at PR #34/#35 (pre spill-default flip) with a finer
  user ramp (50,100,250,500,1000) than the Gate-G command (100,500,1000) — the same
  reproducibility caveat likely applies, unconfirmed.
- Iteration-0's diagnosis is **partially corrected**: the 500 ms sampler byte-walk was real
  and is now fixed, but it accounted for only ~2–3% of the B2 elevation (and ~28% of B1
  materialized); the rest was the frozen baselines not being reproducible under the Gate-G
  configs, not a branch regression.

**Action required before Phase 1:** re-freeze the baseline table from Gate-G-config runs
(candidate numbers: B2 1111 peak / 1053 steady = the 3213e41 control; B1 9.5 KiB/shape,
185 B/row at 66117f2 pending a baseline-commit B1 control), or pin the Gate-G commands to
the configs that produced the original frozen numbers. Until then, iteration verdicts must
use same-day controls, not the frozen table.

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
