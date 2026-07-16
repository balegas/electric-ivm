# Shape memory at scale — 100k subscriptions, 20k live listeners, in-memory vs spill

> **Superseded.** This is a PR-#37-era snapshot. The feed relation now lives host-side
> (Phase 2) and `ELECTRIC_IVM_FEED_TRACE` has been removed — §3 below is historical. For
> current numbers see `docs/bench/mem-reduction-log.md` and `docs/memory-model.md`.

Extends `memory-matrix-blogpost.md` to the scales the blog post claims should hold:
**100,000 shape subscriptions** (10,000 users × 10 shapes, → 50,005 distinct live shapes
after signature sharing) over a fixed **100k-issue** deployment, all shapes materialized,
driven by 4 client processes, with up to **20,000 live long-polls held** by 8 more.
Raw run tables: `raw/scale-*.md`. Engine at the feed-relations merge + the
`ELECTRIC_IVM_FEED_TRACE` / `ELECTRIC_IVM_SUBQ_STORAGE_*` knobs.

**Metric note.** At these scales macOS compresses idle pages, so `ps rss` collapses on
quiescent processes (to ~35 MiB!) and overstates during allocation storms. The honest
state metric is **phys footprint** (`/usr/bin/footprint`, compression-inclusive); RSS is
reported as the *hot working set*.

## 1. Fixed dataset, growing audience (in-memory, feed trace off)

| users | subscriptions | live shapes | engine footprint | engine RSS |
|---:|---:|---:|---:|---:|
| 0 | 0 | 0 | 10 MiB | 24 MiB |
| 100 | 1,000 | 505 | 93 MiB | 168 MiB |
| 1,000 | 10,000 | 5,005 | 142 MiB | 131 MiB |
| 2,500 | 25,000 | 12,505 | 253 MiB | 292 MiB |
| 5,000 | 50,000 | 25,005 | 350 MiB | 457 MiB |
| 10,000 | 100,000 | 50,005 | **789 MiB** | 424 MiB |

≈ **8 KiB of state per subscription** at the top end (≈ 16 KiB per distinct live shape),
on one fixed dataset — the growth is the audience, not the data. Feed keys are ~3.7 M
(each user's ~300-row visibility feed + shared status boards).

## 2. Live listeners are free (for the engine)

Holding long-polls at the final state (ramp 5k → 10k → 20k):

| held live subscriptions | engine footprint | engine RSS (hot set) |
|---:|---:|---:|
| 5,000 | 692–699 MiB | 38–378 MiB |
| 10,000 | 657–698 MiB | 38–52 MiB |
| 20,000 | 657–698 MiB | 34–150 MiB |

Engine footprint is **flat across the ramp** — live serving is the durable-streams
server's job; the engine only decides what changed for whom. Striking corollary: with 20k
listeners attached and no writes, the engine's hot working set is **~35 MiB** — the other
~660 MiB is cold, compressed state.

## 3. The feed-trace knob (`ELECTRIC_IVM_FEED_TRACE=0`)

The historical RSS delta at 100k subscriptions, creation peak: **731.8 MiB (trace on) vs
408.1 MiB (off)**. The "second full copy" explanation was disproven by the Task 1.3 audit:
the trace shares the feed upsert operator's own integral via dbsp's TraceId cache, not a
logical duplicate.

**Resolved (2026-07-16, closes bead dbsp-ds-2hu):** a same-day A/B under the Gate-G B2
config (100k subs, 100/1000/2500/5000/10000 ramp, 5000-hold live phase, spill-by-default,
engine at `mem/phase-1-host-slimming` 380a0f9) measured phys footprint **1135 peak /
1075 steady (trace on) vs 1125 / 1062 (off)** — a ~10–13 MiB (~1%) delta, within
run-to-run noise. The knob has **no material real-world saving on current code**,
consistent with the shared-integral mechanism; the historical ~323 MiB delta was an
artifact of its measurement era (creation-peak `ps rss` on pre-audit code and different
machine state), not a property of the trace. Consequently the "off for feed-heavy
deployments" recommendation is withdrawn: the trace is memory-free under the shared
integral, and leaving it on keeps drop-time enumeration + introspection. (Unreachable
integral entries from dropped shapes exist with the trace on or off — feed ids are never
reused, so correctness is unaffected; stream-fold drop enumeration remains dbsp-ds-4d8.)

## 4. Disk spilling (worktree `feat/subq-circuit-spill`, bead dbsp-ds-4gc)

`ELECTRIC_IVM_SUBQ_STORAGE_DIR` routes the membership circuit through dbsp's storage
backend. Same workload, footprint at 100k subscriptions:

| configuration | engine footprint (peak / steady) | spilled to disk |
|---|---:|---:|
| in-memory | 789 / 698 MiB | — |
| spill, cache 64 MiB, min 128 KB | 699 / 657 MiB | 53 MB |
| spill, cache 16 MiB, min 1 KB | 609 / 642 MiB | 52 MB |

**Findings, honestly:** spilling works end-to-end (identical semantics, full conformance,
the 100k-subscription workload ran unmodified) and the relations serialize compactly
(~53 MB on disk for ~3.7 M keys). But it recovers only **~150–180 MiB (19–23%)** of
footprint at this scale, and aggressive settings don't improve on defaults — the
circuit-relation bytes are a *minority* of engine memory at 100k subscriptions. The
dominant remainder is per-shape host metadata (50k live shapes × records, stream paths,
counters, retention entries ≈ several KiB each) plus allocator retention from the creation
storm, and possibly trace-snapshot batch pinning. **The bottleneck moved**: the next
memory lever is slimming per-shape host metadata (and investigating snapshot pinning),
not tuning the spill cache.

## 5. What to publish

- "One engine node held 100,000 materialized subscriptions (50k distinct live shapes) over
  a 100k-row dataset in under 800 MiB, flat across 20,000 concurrent live listeners."
- "State per subscription is ~8 KiB; per synced row ~85 B; per live listener ~0."
- Frame spill as shipped-and-measured groundwork ("relations page to disk; the residual is
  engine metadata we're now shrinking"), not as a solved memory story.

## Reproduce

```bash
cargo build --release -p electric-ivm-engine
# ELECTRIC_IVM_FEED_TRACE=0 removed
SCALE_ISSUES=100000 SCALE_PROJECTS=2000 SCALE_USERS=100,250,500,1000,2500,5000,10000 \
SCALE_CLIENT_PROCS=4 SCALE_LIVE_RAMP=5000,10000,20000 SCALE_LIVE_PROCS=8 \
  pnpm --filter @electric-ivm/bench exec tsx src/shape-mem-scale.ts
# spill variant: add ELECTRIC_IVM_SUBQ_STORAGE_DIR=… [ELECTRIC_IVM_SUBQ_STORAGE_CACHE_MIB=64]
```
(macOS: clean 0-attach shm segments between runs; expect ENOBUFS for NEW sockets while 20k
long-polls are held — the bench samples via ps/footprint during that phase.)
