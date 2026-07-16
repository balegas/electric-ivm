# Memory attribution at 100k subscriptions — allocator vs. owned heap vs. circuit

Phase 0 (Task 0.2) of the shape-memory reduction plan
(`docs/superpowers/plans/2026-07-15-shape-memory-reduction.md`): attribute the engine's
footprint at 100,000 subscriptions (10,000 users × 10 shapes → 50,005 distinct live
shapes over 100k issues) into (a) self-accounted **owned heap** per subsystem (the six
`bytes_*` fields Task 0.1 added to `GET /memory`), (b) **allocator slack/retention**
(malloc-region dirty bytes minus live-allocated bytes), (c) **dbsp circuit bytes**, and
(d) **unattributed** — so Phase 1 starts on the right target.

Collected with the `SCALE_ATTRIBUTION=1` hook in `packages/bench/src/shape-mem-scale.ts`:
after the final milestone drains, the harness snapshots the full `/memory` JSON,
`vmmap --summary <pid>` and `footprint <pid>` into `docs/bench/raw/attribution-<label>-*`.
Two runs, one milestone each (`SCALE_USERS=10000`, no live phase): **in-memory** and
**spill** (`ELECTRIC_CIRCUITS_SUBQ_STORAGE_DIR`, cache 64 MiB). Engine at
`mem/phase-0-attribution` (Task 0.1 instrumentation included), `ELECTRIC_CIRCUITS_FEED_TRACE=0`.

**What each source measures.** `footprint` = phys footprint (dirty + compressed/swapped,
the honest state metric). `vmmap`'s MALLOC ZONE table splits the malloc heap into
**bytes allocated** (live allocations) and **frag/slack** (dirty pages the allocator
retains but nothing owns). The `bytes_*` fields are lower-bound *owned-heap* walks of the
engine's host-side structures. So: `footprint ≈ malloc regions + ~4 MB non-heap`;
`malloc regions = live allocated + allocator slack`;
`live allocated = self-accounted owned + everything nobody self-accounts` — and that last
term is dominated by dbsp's circuit state (spines/traces), which no `bytes_*` walk covers.

## 1. Owned heap per subsystem (`/memory` `bytes_*`)

Identical across both runs (host-side metadata does not care where the circuit keeps
its relations) — values from the in-memory run:

| subsystem | bytes | MiB |
|---|---:|---:|
| `bytes_subquery_registry` (nodes, templates, edges, shapes) | 35,594,096 | 33.9 |
| `bytes_executors` (standalone/routed shapes, routers, agg folds) | 23,306,709 | 22.2 |
| `bytes_shape_records` (shape registry) | 21,736,343 | 20.7 |
| `bytes_membership_circuit` (key-count × 88 B estimate; **see caveat**) | 10,560,000 | 10.1 |
| `bytes_retention` (per-shape lifecycle records) | 9,939,919 | 9.5 |
| `bytes_electric_adapter` (TTL handles; none in this workload) | 0 | 0 |
| **sum — self-accounted owned heap** | **101,137,067** | **96.5** |

≈ 2.0 KiB of host metadata per distinct live shape (50,005), ≈ 1.0 KiB per subscription.

**Caveat — `bytes_membership_circuit` badly undercounts here.** 10,560,000 =
(60,000 contributors + 60,000 distinct values + **0 feed keys**) × 88 B. With
`ELECTRIC_CIRCUITS_FEED_TRACE=0` the published feed trace is disabled, so `feed_len` reads an
empty snapshot and the ~3.7 M feed keys (prior-report count for this workload) contribute
nothing to the estimate — even though the gating integral inside the dbsp circuit still
holds them. The circuit's real resident cost is measured below by the spill delta, not by
this field.

## 2. Region-level attribution

Both snapshots taken ~3.5 s after the creation storm drained (single 100k-subscription
milestone) — this is **creation-peak state, not steady state** (see caveats).

| | in-memory | spill (cache 64 MiB) |
|---|---:|---:|
| phys footprint (`footprint`) | **1147 MB** (peak 1147) | **649 MB** (peak 715) |
| engine RSS (`/memory`) | 614 MiB | 329 MiB |
| MALLOC regions, dirty+swapped (incl. empty + metadata) | ~1143 MB (99.7%) | ~646 MB (99.5%) |
| — malloc live-allocated (vmmap zone "bytes allocated") | 997.9 MB (87%) | 543.7 MB (84%) |
| —— (a) self-accounted owned heap (`bytes_*` sum) | 101.1 MB (**8.8%**) | 101.1 MB (**15.6%**) |
| —— (d) live-allocated, unattributed (≈ dbsp circuits + buffers) | **896.8 MB (78%)** | **442.6 MB (68%)** |
| — (b) allocator slack (regions − live-allocated; zone frag 120.5 / 87.4 MB) | 145.4 MB (**12.7%**) | 102.4 MB (**15.8%**) |
| non-MALLOC (stacks, page tables, __DATA…) | ~4 MB | ~3 MB |
| spilled to disk | — | 59 MB |

**(c) dbsp circuit bytes, measured by difference.** The two runs have byte-identical
owned-heap sums and the same live-shape/contributor counts; the only change is routing
the membership circuit through dbsp's storage backend. Live-allocated bytes drop
**997.9 → 543.7 MB (−454 MB)** while only 59 MB lands on disk (read manually off the
storage directory during the run; the directory was not captured for artifact archiving)
— so at least **~454 MB (40% of the in-memory footprint) is membership-circuit-resident
state** (spines, traces, per-batch structures; the 8× blow-up vs. the on-disk serialized
form is dbsp in-memory representation overhead, consistent with §4 of `shape-memory-scale.md`).
The ~443 MB still unattributed under spill is the remaining circuit machinery that does
not spill: the family circuits (3 shared equality circuits each holding its base table),
the storage cache (64 MiB), dbsp step buffers, and tokio/runtime state.

## 2b. Post-dictionary attribution (HEAD 2e94ddc, 2026-07-16)

Same A/B method re-run after Task 2.1 (global pk dictionary: u32-keyed relations,
key-only feed upsert set — logical feed entries shrank ~5× at test scale). Workload:
the standard Gate-G **ramped** creation (100→10,000 users, 100k subscriptions,
FEED_TRACE=0), snapshot ~3.5 s after the final 50k-subscription burst drained
(creation-peak state); spill run: explicit storage dir, cache 64 MiB.

| | in-memory (it2) | spill (cache 64 MiB) |
|---|---:|---:|
| phys footprint (peak) | **1102 MB** | **645 MB** |
| footprint, live-5000 steady | 1046 MB | 613 MB |
| MALLOC regions, dirty+swapped (incl. empty + metadata) | ~1094 MB | ~639 MB |
| — zone live-allocated (vmmap "bytes allocated") | 1001.9 MB | 543.1 MB |
| —— self-accounted owned heap (`bytes_*` sum, incl. `bytes_pk_dict` 6.3 MiB) | 96.8 MB (8.8%) | 96.8 MB (15.0%) |
| — allocator slack (regions − live; zone frag 70.9 / 67.4 MB) | ~92 MB (8.4%) | ~96 MB (14.9%) |
| non-MALLOC | ~8 MB | ~6 MB |
| spilled to disk (du during run) | — | **27 MB** |

**(a) circuit-resident (spillable): 1001.9 − 543.1 ≈ 459 MB** — statistically identical
to the pre-dictionary 454 MB, even though each feed entry's payload shrank ~5× and the
serialized on-disk form halved (59 → 27 MB). The resident-to-serialized blow-up is now
~17× (was ~8×): the circuit's resident cost is **per-entry/spine machinery, not payload
bytes**. **(b) allocator slack: ~92–96 MB (8–15% of footprint)** — still below the 25%
jemalloc gate. **(c) neither: ~551 MB** of the in-memory footprint = 96.8 MB owned host
metadata + ~8 MB non-malloc + **~446 MB live-but-unattributed even under spill** (family
circuits' base tables, dbsp step buffers, storage cache, runtime) — unchanged from
Phase 0's ~443 MB residual.

**Conclusion:** payload narrowing is exhausted; the two remaining levers are the ~459 MB
spillable term and the ~446 MB non-spillable circuit machinery, both driven by **entry
and batch counts** — i.e. Task 2.2 (bitmap/set-per-feed representations) / Phase-3-class
work, not further key compression and not the allocator.

> **Addendum (Task 2.2 landed, dbsp-ds-dh6 re-litigated).** The feed relation — the dominant
> chunk of the ~459 MB spillable membership-circuit-resident term above — has been moved OUT
> of the circuit to host-side Roaring bitmaps (`apps/engine/src/subq_feed.rs`). The spike
> (`docs/notes/2026-07-16-feed-set-representation-spike.md`) measured the two representations
> over this same ~3.7M-entry feed shape: **169 MiB** in-circuit (profiler `total_used_bytes`,
> spill off) vs **~16 MiB** as bitmaps (RSS Δ) — a 10–19× reduction. This attacks *both* levers
> the conclusion names: it removes the feed set's share of the spillable term **and** its share
> of the batch/spine machinery the circuit could never page out. The residual circuit term is
> now dominated by the **contributor** relation (~60k entries at 100k subs). This doc's
> measured numbers predate the change; re-measure to quantify the landed RSS reduction.

## 2c. Per-circuit decomposition — the residual is the buffer cache, not circuit state (HEAD bc97f0a, 2026-07-16)

Final measurement of the plan: `GET /debug/dbsp-profile` (new on-demand endpoint; dbsp's
own profiler, per-operator) captured by the `SCALE_ATTRIBUTION=1` hook at the top
milestone of a standard Gate-G B2 run (ramped creation to 100k subscriptions, engine at
`mem/phase-2-feed-keys` bc97f0a = 3cd9956 + the endpoint). Footprint 1104 MB (peak 1109),
zone live-allocated 998.6 MB, frag 80.1 MB, owned `bytes_*` 98.7 MiB.

**Circuit inventory ground truth:** the engine runs exactly ONE dbsp circuit in this
workload — the subquery membership circuit. The counts/arrangements circuit was not
configured (`counts_circuit: null`), and "family"/"standalone" circuits are host-side
executor structures (no dbsp runtime; sized by `bytes_executors`, 22.2 MiB).

**The membership circuit's entire operator memory is 0.94 MB** (+5.3 MB in storage layer
files): root integral 0.47 MB / 180,000 records total — exactly 3 traces × 60,000
contributor entries (contributor upsert integral, members Z1 trace, distinct Z1 trace) —
plus two 0.4 KB accumulators. Everything else profiles at zero.

**Where the ~0.9 GB actually is (creation-peak footprint 1104 MB):**

| term | MB | evidence |
|---|---:|---|
| dbsp storage **buffer cache**, default limit **512 MiB** (`cache_mib` unset → 256 MiB × 1 worker × 2 thread-types, dbsp `runtime.rs`) | **~450** | the only A/B difference vs the 64 MiB-cache run (645/613 MB footprint): 512 − 64 = 448 ≈ measured 454–459 delta; cumulative background cache traffic 314 MB in the dump |
| live-allocated, other dbsp/runtime (merger + FBuf slab allocators, step/exchange buffers, tokio, ds client, backfill churn) | ~395 | live 998.6 − owned 103.5 − cache ~450 − operators ~1 |
| owned host metadata (`bytes_*` incl. `bytes_feed_sets` 6.4) | 103.5 | `/memory` |
| allocator slack (regions − live; zone frag 80.1) | ~90 | vmmap |
| dbsp operator state (all circuits) | **0.94** | profiler |
| non-MALLOC | ~10 | vmmap totals |

**Reinterpretation of §2 and §2b:** the "≥454 MB circuit-resident (spillable)" bucket was
never relation state — it is the **buffer-cache limit difference** between the default
(512 MiB) and the explicit 64 MiB configuration. Both the Phase-0 "in-memory" runs and
every Gate-G run since PR #37 actually ran spill-by-default with the 512 MiB default
cache; the "8× / 17× resident-vs-serialized blow-up" dissolves into cache pages. This is
why neither the pk dictionary (payload ×5 smaller) nor the FeedSet (relation removed
outright) moved the headline: **an LRU cache fills to its limit regardless of how small
its working set gets.**

**O(table-rows) verdict: design claim upheld.** No circuit holds row data: 180k records
= 3 × contributors (60k), independent of the 100k issue rows; the counts circuit (state
O(groups)) was not even running. The §2 speculation that "family circuits each hold their
base table" is refuted — family circuits are host-side (22.2 MiB owned, no dbsp).

**Actionable next step (one line, ~40% of footprint):** default
`ELECTRIC_CIRCUITS_SUBQ_STORAGE_CACHE_MIB` to a bounded value (e.g. 64) instead of inheriting
dbsp's 512 MiB — the measured 64 MiB-cache configuration held the same workload at
**645 peak / 613 steady** with 27 MB on disk. The remaining ~395 MB of runtime/buffer
churn (merger/FBuf slabs, step buffers) is the term after that.

**Done:** `apps/engine/src/subq_circuit.rs` now defaults `cache_mib` to 64 MiB (TOTAL — dbsp
uses the value verbatim, unmultiplied) when the env var is unset, instead of leaving it `None`
and inheriting dbsp's 256 MiB × 1 worker × 2 thread-types = 512 MiB unset-default; the env var
override still works exactly as before.

## 3. Verdict on the three Phase-1 hypotheses

| hypothesis | magnitude at 100k subs | verdict |
|---|---|---|
| (c) dbsp trace/snapshot/circuit bytes dominate | ≥ 454 MB membership circuit (measured, spill delta) + up to ~443 MB residual circuit machinery; together ~78% of the in-memory footprint | **Supported — dominant.** |
| (b) allocator slack/retention dominates | 145 MB in-memory / 102 MB spill (11–16% of footprint; zone frag 11–14%) | Real, but secondary. Below the plan's 25% gate. |
| (a) per-shape host metadata dominates | 101 MB self-accounted (8.8% in-memory, 15.6% spill) ≈ 2 KiB/live shape | Not dominant. Below the 25% gate. |

**Phase-0 gate decision (per the plan):** neither allocator slack nor owned metadata
reaches the 25% threshold; dbsp circuit bytes dominate → **Phase 1 starts with Task 1.3
(dbsp batch/snapshot pinning), not the allocator or interning tasks.** Interning (1.2)
and jemalloc (1.1) remain worthwhile follow-ups — together they bound ~250 MB — but they
cannot move the headline number the way the circuit term can.

**Feed-key fraction / Phase-2 call:** ~3.7 M feed keys × 88 B ≈ **310 MiB owned-bytes
floor ≈ 27% of the in-memory peak footprint** (~44% of the prior 698-MiB steady
baseline) — and the measured circuit-resident cost (454 MB for state whose serialized
form is 59 MB) says the true in-memory cost per key is several× the 88 B floor. This is
**well above the 15% threshold: Phase 2's compact feed-key representation stays on the
plan** (not just the cheap fingerprint variant).

## Caveats, honestly

- **Creation-peak, not steady state.** One 100k-subscription milestone with a snapshot
  ~3.5 s after drain. The prior report's ramped run (users added in stages with drains
  between milestones) peaked at 789 MiB and settled to 698 MiB steady (in-memory); our
  in-memory 1147 MB is the storm peak from creating all 100k subscriptions in a single
  burst, which explains the ~45% higher peak (1147 vs 789 MB) on top of the peak-vs-steady
  distinction (548 MB of the 1147 was swapped/compressed at sample time — the machine was
  under memory pressure). The *ratios* are the deliverable here; the headline footprint of
  this run shape is not comparable 1:1 with the ramped runs.
- **Spill run peak (715 MB) ≈ prior spill peak (699 MB)** — the spill numbers line up
  with the ramped baseline; the in-memory storm peak is the outlier, consistent with
  allocator retention after a single giant burst.
- `bytes_*` are lower-bound owned-heap walks (capacity-based, swiss-table overhead
  estimated at 1.1×); they cannot see dbsp-internal allocations by design.
- `bytes_membership_circuit` counts 0 feed keys under `FEED_TRACE=0` (see §1) — a
  follow-up could plumb a real key count (or byte count) out of the gating integral.
- The ~3.7 M feed-key count is carried over from the prior report's run of the same
  workload, not re-measured here (not enumerable with the trace off).
- vmmap/footprint numbers are the tools' own "MB" units; sub-percent unit mismatches
  (MB vs MiB) are ignored throughout.

## Reproduce

```bash
cargo build --release -p electric-circuits-engine

# in-memory
ELECTRIC_CIRCUITS_FEED_TRACE=0 \
SCALE_ISSUES=100000 SCALE_PROJECTS=2000 SCALE_USERS=10000 \
SCALE_CLIENT_PROCS=4 SCALE_LIVE_SUBS=0 \
SCALE_ATTRIBUTION=1 SCALE_ATTRIBUTION_LABEL=inmemory \
  pnpm --filter @electric-circuits/bench exec tsx src/shape-mem-scale.ts

# spill: add
#   ELECTRIC_CIRCUITS_SUBQ_STORAGE_DIR=$(mktemp -d) ELECTRIC_CIRCUITS_SUBQ_STORAGE_CACHE_MIB=64 \
#   SCALE_ATTRIBUTION_LABEL=spill

# artifacts land in docs/bench/raw/attribution-<label>-{memory.json,vmmap-summary.txt,footprint.txt}
# (override the directory with SCALE_ATTRIBUTION_DIR, the run table with SCALE_OUT)
```

macOS: clean leaked 0-attach PG shm segments between runs (`ipcs -ma`, `ipcrm -m <id>`)
or initdb starts failing. `vmmap`/`footprint` need no sudo on your own processes.
