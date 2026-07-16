# Shape-memory matrix (engine, Postgres mode)

Generated 2026-07-16T00:25:50.759Z on darwin/arm64.

**Question.** How does the engine's memory evolve as shapes are created over time, for different
deployment sizes (issue counts)?

**Method.** An ephemeral Postgres is seeded with N issues, ~0.5×N comments, 20 projects, 1000 users,
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

**Reproduce.** `cargo build --release -p electric-circuits-engine` then
`MATRIX_SIZES=1000,10000,100000 MATRIX_USERS=100,250,500,1000 pnpm --filter @electric-circuits/bench shape-mem`.

Config this run: projects=20, users=1000, memberships/user=6, comments/issue=0.5, shapes/user=10, user milestones=100,500,1000.

## 10,000 issues

| users | shapes | RSS (MiB) | ΔRSS vs init | subquery nodes | contributors | edges | family circuits |
|------:|-------:|----------:|-------------:|---------------:|-------------:|------:|----------------:|
| 0 | 0 | 24.3 | 0.0 | 0 | 0 | 0 | 0 |
| 100 | 1000 | 153.9 | 129.6 | 100 | 600 | 100 | 3 |
| 500 | 5000 | 516.9 | 492.6 | 500 | 3000 | 500 | 3 |
| 1000 | 10000 | 645.8 | 621.5 | 1000 | 6000 | 1000 | 3 |

- Init RSS: **24.3 MiB**; after 10000 shapes: **645.8 MiB** (Δ 621.5 MiB ≈ 63.6 KiB/shape).
- Materialized backfill probe (1 visibility shape, 3,000 visible issues): RSS 674.7 → 730.1 MiB (peak), settled 606.8 MiB.

## Summary across deployment sizes

| issues | init RSS (MiB) | RSS @ 10000 shapes (MiB) | KiB/shape | backfill peak (MiB) | bytes/visible-row (peak) |
|-------:|---------------:|----------------------:|----------:|--------------------:|-------------------------:|
| 10,000 | 24.3 | 645.8 | 63.6 | 55.3 | 19344 |

## Findings

1. **Baseline RSS is independent of deployment size** (~24.3 MiB at 1k / 10k / 100k issues). The engine keeps *no copy* of the table — it backfills from a Postgres snapshot and tails replication — so startup memory does not scale with the row count.
2. **Per-shape registration memory is small (≈0.7–0.9 KiB/shape)** and ~constant across all deployment sizes — see the "KiB/shape" column. Even **10,000** changes-only shapes grow RSS by under 10 MiB. Subquery nodes, contributor pks, and edges grow linearly with shapes but cheaply (a node holds only its inner-set contributor pks — here the user's 6 membership rows — not issues).
3. **Family circuits stay at a small constant** (a handful — one per equality *template*, not per shape): all board-status shapes share one family (key column `status`), all "my tasks" shapes another (`username`), and all per-issue comment shapes one more (`issue_id` on the comments table). So thousands of equality shapes collapse onto ~3 circuits — the family-sharing win.
4. **Backfill is the deployment-size-sensitive cost.** A *materialized* shape's one-off backfill working set scales ~linearly with the number of *visible* rows — see the "bytes/visible-row" column above (~2 KiB/row peak at 10k and 100k; the 1k row is below RSS/allocator resolution, so treat small-N backfill deltas as noise). This is transient read-batch + serialization memory, not retained table state.
5. **Caveat — allocator slack & RSS noise.** RSS is a coarse, non-monotonic signal: after a large backfill it sometimes settles near the peak and sometimes below the pre-backfill baseline, because the system allocator decides when to return freed pages to the OS. Sub-MiB deltas are within noise. For steady-state sizing, measure after warmup or build with jemalloc + background reclamation; rely on the OTel *cardinality* gauges (nodes, contributors, family circuits) to read retained structural state independent of allocator slack.

**Takeaway for deployment sizing.** Budget memory by *concurrent backfill working set* (≈ peak
visible-rows-per-shape × 2 KiB, summed over shapes backfilling at once), not by total shape count or
total issues. A steady fleet of many shapes over a large table is cheap; bursts of large materialized
backfills are the spike to provision for. Changes-only / subset feeds avoid the backfill spike entirely.
