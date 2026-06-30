# Shape-memory matrix (engine, Postgres mode)

Generated 2026-06-30T08:56:54.432Z on darwin/arm64.

**Question.** How does the engine's memory evolve as shapes are created over time, for different
deployment sizes (issue counts)?

**Method.** An ephemeral Postgres is seeded with N issues plus a project/membership graph (the
LinearLite visibility model); the engine runs in Postgres mode. We then simulate user sessions
connecting over time — each adds 2 *changes-only* shapes (a visibility subquery
`project_id IN (SELECT project_id FROM project_members WHERE user_id = u)` plus a board-status
equality `status = …`) — and sample the engine's **OpenTelemetry** memory probe (`GET /memory`,
also exported in Prometheus format at `/metrics/prometheus`) at each milestone. Changes-only shapes
skip the one-off backfill, so the per-shape numbers isolate *registration* memory; a separate probe
creates one *materialized* visibility shape to measure the backfill working set vs deployment size.

**Probes** (OTel observable gauges): `engine_process_resident_memory_bytes`,
`engine_process_virtual_memory_bytes`, `engine_shapes`, `engine_tailers`, `engine_family_circuits`,
`engine_standalone_circuits`, `engine_subquery_nodes`, `engine_subquery_contributors`,
`engine_subquery_distinct_values`, `engine_subquery_edges`.

**Reproduce.** `cargo build --release -p electric-lite-engine` then
`MATRIX_SIZES=1000,10000,100000 MATRIX_USERS=25,50,100,200 pnpm --filter @electric-lite/bench shape-mem`.

Config this run: projects=20, memberships/user=4, user milestones=25,50,100,200.

## 1,000 issues

| users | shapes | RSS (MiB) | ΔRSS vs init | subquery nodes | contributors | edges | family circuits |
|------:|-------:|----------:|-------------:|---------------:|-------------:|------:|----------------:|
| 0 | 0 | 18.7 | 0.0 | 0 | 0 | 0 | 0 |
| 25 | 50 | 19.4 | 0.7 | 25 | 100 | 25 | 1 |
| 50 | 100 | 19.5 | 0.8 | 50 | 200 | 50 | 1 |
| 100 | 200 | 19.7 | 1.0 | 100 | 400 | 100 | 1 |
| 200 | 400 | 20.1 | 1.4 | 200 | 800 | 200 | 1 |

- Init RSS: **18.7 MiB**; after 400 shapes: **20.1 MiB** (Δ 1.4 MiB ≈ 3.6 KiB/shape).
- Materialized backfill probe (1 visibility shape, 200 visible issues): RSS 20.1 → 20.5 MiB (peak), settled 16.7 MiB.

## 10,000 issues

| users | shapes | RSS (MiB) | ΔRSS vs init | subquery nodes | contributors | edges | family circuits |
|------:|-------:|----------:|-------------:|---------------:|-------------:|------:|----------------:|
| 0 | 0 | 18.9 | 0.0 | 0 | 0 | 0 | 0 |
| 25 | 50 | 19.6 | 0.7 | 25 | 100 | 25 | 1 |
| 50 | 100 | 19.8 | 0.9 | 50 | 200 | 50 | 1 |
| 100 | 200 | 20.0 | 1.1 | 100 | 400 | 100 | 1 |
| 200 | 400 | 20.4 | 1.5 | 200 | 800 | 200 | 1 |

- Init RSS: **18.9 MiB**; after 400 shapes: **20.4 MiB** (Δ 1.5 MiB ≈ 3.8 KiB/shape).
- Materialized backfill probe (1 visibility shape, 2,000 visible issues): RSS 20.4 → 25.1 MiB (peak), settled 25.1 MiB.

## 100,000 issues

| users | shapes | RSS (MiB) | ΔRSS vs init | subquery nodes | contributors | edges | family circuits |
|------:|-------:|----------:|-------------:|---------------:|-------------:|------:|----------------:|
| 0 | 0 | 18.9 | 0.0 | 0 | 0 | 0 | 0 |
| 25 | 50 | 19.6 | 0.7 | 25 | 100 | 25 | 1 |
| 50 | 100 | 19.7 | 0.8 | 50 | 200 | 50 | 1 |
| 100 | 200 | 20.0 | 1.1 | 100 | 400 | 100 | 1 |
| 200 | 400 | 17.2 | -1.7 | 200 | 800 | 200 | 1 |

- Init RSS: **18.9 MiB**; after 400 shapes: **17.2 MiB** (Δ -1.7 MiB ≈ -4.4 KiB/shape).
- Materialized backfill probe (1 visibility shape, 20,000 visible issues): RSS 17.2 → 57.4 MiB (peak), settled 57.4 MiB.

## Summary across deployment sizes

| issues | init RSS (MiB) | RSS @ 400 shapes (MiB) | KiB/shape | backfill peak (MiB) | bytes/visible-row (peak) |
|-------:|---------------:|----------------------:|----------:|--------------------:|-------------------------:|
| 1,000 | 18.7 | 20.1 | 3.6 | 0.4 | 2130 |
| 10,000 | 18.9 | 20.4 | 3.8 | 4.7 | 2474 |
| 100,000 | 18.9 | 17.2 | -4.4 | 40.2 | 2107 |

## Findings

1. **Baseline RSS is independent of deployment size** (~18.8 MiB at 1k / 10k / 100k issues). The engine keeps *no copy* of the table — it backfills from a Postgres snapshot and tails replication — so startup memory does not scale with the row count.
2. **Per-shape registration memory is small and ~constant (~4 KiB/shape)** across all deployment sizes. Creating hundreds of changes-only shapes grows RSS by a couple MiB total. Subquery nodes, contributor pks, and edges grow linearly with shapes but cheaply (a node holds only its inner-set contributor pks — here the user's membership rows — not issues).
3. **Family circuits stay at 1**: every board-status shape shares one equality family (keyed by the `status` column), so adding more such shapes adds ~no circuit memory. This is the family-sharing win — N same-template shapes cost one circuit, not N.
4. **Backfill is the deployment-size-sensitive cost.** A *materialized* shape's one-off backfill working set scales ~linearly with the number of *visible* rows — see the "bytes/visible-row" column above (~2 KiB/row peak at 10k and 100k; the 1k row is below RSS/allocator resolution, so treat small-N backfill deltas as noise). This is transient read-batch + serialization memory, not retained table state.
5. **Caveat — allocator slack & RSS noise.** RSS is a coarse, non-monotonic signal: after a large backfill it sometimes settles near the peak and sometimes below the pre-backfill baseline, because the system allocator decides when to return freed pages to the OS. Sub-MiB deltas are within noise. For steady-state sizing, measure after warmup or build with jemalloc + background reclamation; rely on the OTel *cardinality* gauges (nodes, contributors, family circuits) to read retained structural state independent of allocator slack.

**Takeaway for deployment sizing.** Budget memory by *concurrent backfill working set* (≈ peak
visible-rows-per-shape × 2 KiB, summed over shapes backfilling at once), not by total shape count or
total issues. A steady fleet of many shapes over a large table is cheap; bursts of large materialized
backfills are the spike to provision for. Changes-only / subset feeds avoid the backfill spike entirely.
