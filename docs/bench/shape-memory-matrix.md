# Shape-memory matrix (engine, Postgres mode)

Generated 2026-06-30T10:24:03.726Z on darwin/arm64.

**Question.** How does the engine's memory evolve as shapes are created over time, for different
deployment sizes (issue counts)?

**Method.** An ephemeral Postgres is seeded with N issues, ~0.5×N comments, 50 projects, 1000 users,
and a membership graph (6 projects/user — the LinearLite visibility model); the engine runs in
Postgres mode. We then simulate user sessions connecting over time — each opens 10 *changes-only*
shapes: a per-user visibility subquery `project_id IN (SELECT project_id FROM project_members WHERE
`user_id = u)`, 5 board-status columns, a "my tasks" filter, and 3 per-issue comment shapes — and we
sample the engine's **OpenTelemetry** memory probe (`GET /memory`, also exported in Prometheus format
at `/metrics/prometheus`) at each milestone, up to **10,000 shapes**. Changes-only shapes skip the one-off
backfill, so the per-shape numbers isolate *registration* memory; a separate probe creates one
*materialized* visibility shape to measure the backfill working set vs deployment size.

**Probes** (OTel observable gauges): `engine_process_resident_memory_bytes`,
`engine_process_virtual_memory_bytes`, `engine_shapes`, `engine_tailers`, `engine_family_circuits`,
`engine_standalone_circuits`, `engine_subquery_nodes`, `engine_subquery_contributors`,
`engine_subquery_distinct_values`, `engine_subquery_edges`.

**Reproduce.** `cargo build --release -p electric-ivm-engine` then
`MATRIX_SIZES=1000,10000,100000 MATRIX_USERS=100,250,500,1000 pnpm --filter @electric-ivm/bench shape-mem`.

Config this run: projects=50, users=1000, memberships/user=6, comments/issue=0.5, shapes/user=10, user milestones=100,250,500,1000.

## 1,000 issues

| users | shapes | RSS (MiB) | ΔRSS vs init | subquery nodes | contributors | edges | family circuits |
|------:|-------:|----------:|-------------:|---------------:|-------------:|------:|----------------:|
| 0 | 0 | 18.7 | 0.0 | 0 | 0 | 0 | 0 |
| 100 | 1000 | 21.9 | 3.2 | 100 | 600 | 100 | 3 |
| 250 | 2500 | 20.2 | 1.5 | 250 | 1500 | 250 | 3 |
| 500 | 5000 | 22.3 | 3.6 | 500 | 3000 | 500 | 3 |
| 1000 | 10000 | 27.4 | 8.7 | 1000 | 6000 | 1000 | 3 |

- Init RSS: **18.7 MiB**; after 10000 shapes: **27.4 MiB** (Δ 8.7 MiB ≈ 0.9 KiB/shape).
- Materialized backfill probe (1 visibility shape, 120 visible issues): RSS 27.4 → 26.8 MiB (peak), settled 23.1 MiB.

## 10,000 issues

| users | shapes | RSS (MiB) | ΔRSS vs init | subquery nodes | contributors | edges | family circuits |
|------:|-------:|----------:|-------------:|---------------:|-------------:|------:|----------------:|
| 0 | 0 | 19.0 | 0.0 | 0 | 0 | 0 | 0 |
| 100 | 1000 | 20.4 | 1.3 | 100 | 600 | 100 | 3 |
| 250 | 2500 | 20.7 | 1.6 | 250 | 1500 | 250 | 3 |
| 500 | 5000 | 22.9 | 3.9 | 500 | 3000 | 500 | 3 |
| 1000 | 10000 | 27.4 | 8.3 | 1000 | 6000 | 1000 | 3 |

- Init RSS: **19.0 MiB**; after 10000 shapes: **27.4 MiB** (Δ 8.3 MiB ≈ 0.9 KiB/shape).
- Materialized backfill probe (1 visibility shape, 1,200 visible issues): RSS 27.4 → 29.5 MiB (peak), settled 22.1 MiB.

## 100,000 issues

| users | shapes | RSS (MiB) | ΔRSS vs init | subquery nodes | contributors | edges | family circuits |
|------:|-------:|----------:|-------------:|---------------:|-------------:|------:|----------------:|
| 0 | 0 | 18.7 | 0.0 | 0 | 0 | 0 | 0 |
| 100 | 1000 | 19.6 | 0.9 | 100 | 600 | 100 | 3 |
| 250 | 2500 | 20.5 | 1.8 | 250 | 1500 | 250 | 3 |
| 500 | 5000 | 21.9 | 3.2 | 500 | 3000 | 500 | 3 |
| 1000 | 10000 | 25.5 | 6.8 | 1000 | 6000 | 1000 | 3 |

- Init RSS: **18.7 MiB**; after 10000 shapes: **25.5 MiB** (Δ 6.8 MiB ≈ 0.7 KiB/shape).
- Materialized backfill probe (1 visibility shape, 12,000 visible issues): RSS 25.5 → 47.4 MiB (peak), settled 26.9 MiB.

## Summary across deployment sizes

| issues | init RSS (MiB) | RSS @ 10000 shapes (MiB) | KiB/shape | backfill peak (MiB) | bytes/visible-row (peak) |
|-------:|---------------:|----------------------:|----------:|--------------------:|-------------------------:|
| 1,000 | 18.7 | 27.4 | 0.9 | -0.6 | -5188 |
| 10,000 | 19.0 | 27.4 | 0.9 | 2.2 | 1884 |
| 100,000 | 18.7 | 25.5 | 0.7 | 21.8 | 1909 |

## Findings

1. **Baseline RSS is independent of deployment size** (~18.8 MiB at 1k / 10k / 100k issues). The engine keeps *no copy* of the table — it backfills from a Postgres snapshot and tails replication — so startup memory does not scale with the row count.
2. **Per-shape registration memory is small (≈0.7–0.9 KiB/shape)** and ~constant across all deployment sizes — see the "KiB/shape" column. Even **10,000** changes-only shapes grow RSS by under 10 MiB. Subquery nodes, contributor pks, and edges grow linearly with shapes but cheaply (a node holds only its inner-set contributor pks — here the user's 6 membership rows — not issues).
3. **Family circuits stay at a small constant** (a handful — one per equality *template*, not per shape): all board-status shapes share one family (key column `status`), all "my tasks" shapes another (`username`), and all per-issue comment shapes one more (`issue_id` on the comments table). So thousands of equality shapes collapse onto ~3 circuits — the family-sharing win.
4. **Backfill is the deployment-size-sensitive cost.** A *materialized* shape's one-off backfill working set scales ~linearly with the number of *visible* rows — see the "bytes/visible-row" column above (~2 KiB/row peak at 10k and 100k; the 1k row is below RSS/allocator resolution, so treat small-N backfill deltas as noise). This is transient read-batch + serialization memory, not retained table state.
5. **Caveat — allocator slack & RSS noise.** RSS is a coarse, non-monotonic signal: after a large backfill it sometimes settles near the peak and sometimes below the pre-backfill baseline, because the system allocator decides when to return freed pages to the OS. Sub-MiB deltas are within noise. For steady-state sizing, measure after warmup or build with jemalloc + background reclamation; rely on the OTel *cardinality* gauges (nodes, contributors, family circuits) to read retained structural state independent of allocator slack.

**Takeaway for deployment sizing.** Budget memory by *concurrent backfill working set* (≈ peak
visible-rows-per-shape × 2 KiB, summed over shapes backfilling at once), not by total shape count or
total issues. A steady fleet of many shapes over a large table is cheap; bursts of large materialized
backfills are the spike to provision for. Changes-only / subset feeds avoid the backfill spike entirely.
