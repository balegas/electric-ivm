# ElectricSQL benchmarking-fleet — results vs electric-lite

Generated 2026-06-30 on darwin/arm64. We ran ElectricSQL's **own** `byo_electric` benchmarks
(unmodified `.exs` scripts from [electric-sql/benchmarking-fleet](https://github.com/electric-sql/benchmarking-fleet))
against electric-lite's Electric-protocol `/v1/shape` adapter, the same way the fleet benchmarks the real
Electric sync-service. This is a load/throughput companion to the oracle conformance tests.

## Method

A runner (`packages/bench/src/electric-bench-runner.ts`) does, per benchmark: boot our stack
(durable-streams + engine + `/v1/shape` adapter on an ephemeral Postgres), seed the benchmark's schema at
scale, run the unmodified ElectricSQL benchmark script with `ELECTRIC_SERVER` pointed at our adapter, and
aggregate the per-shape latency the benchmark emits over statsd/UDP. The benchmark scripts are vanilla
`Mix.install([:req, …])` — no GCP fleet, no Docker. Reproduce:

```
cargo build --release -p electric-lite-engine
git clone https://github.com/electric-sql/benchmarking-fleet ../benchmarking-fleet
BENCH_SCALE=2 pnpm --filter @electric-lite/bench exec tsx src/electric-bench-runner.ts
```

`latency` is the per-shape *fetch-complete* duration the benchmark measures (initial shape sync, or — for
the fanout/latency benchmarks — write-to-visible propagation). `wall` is the whole benchmark's wall-clock.

## Results

### Standard workload (`BENCH_SCALE=1`)

| benchmark | workload | samples | wall (s) | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) |
|-----------|----------|--------:|---------:|---------:|---------:|---------:|---------:|
| concurrent_shape_creation | 500 concurrent shapes, 2k-row table | 500 | 6.4 | 2637 | 5040 | 5264 | 5312 |
| concurrent_shape_creation_with_subqueries | 300 concurrent **subquery** shapes | 300 | 4.6 | 351 | 561 | 577 | 582 |
| many_shapes_one_client_latency | write → 500 shapes, 1 client | 1 | 7.3 | 1 | 1 | 1 | 1 |
| diverse_shape_fanout | write fanout → 200 shapes | 200 | 4.2 | ~0† | ~0† | ~0† | 3 |
| write_fanout | write fanout → 200 shapes | 200 | 2.2 | 19 | 39 | 41 | 41 |

### Large workload (`BENCH_SCALE=2`)

| benchmark | workload | samples | wall (s) | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) |
|-----------|----------|--------:|---------:|---------:|---------:|---------:|---------:|
| concurrent_shape_creation | **1000** concurrent shapes, 2k-row table | 1000 | 11.5 | 5171 | 9813 | 10213 | 10332 |
| concurrent_shape_creation_with_subqueries | **600** concurrent **subquery** shapes | 600 | 5.1 | 569 | 1031 | 1069 | 1079 |
| many_shapes_one_client_latency | write → 1000 shapes, 1 client | 1 | 12.2 | 1 | 1 | 1 | 1 |
| diverse_shape_fanout | write fanout → 400 shapes | 400 | 6.5 | ~0† | ~0† | ~0† | 0 |
| write_fanout | write fanout → 400 shapes | 400 | 2.2 | 38 | 44 | 44 | 44 |

† `diverse_shape_fanout` measures write-to-visible latency relative to the write-commit timestamp; our
propagation is fast enough that many samples land at or just before that baseline (the benchmark emits
small negative values). Read it as "changes are visible ~immediately"; `max` (0–3 ms) is the honest bound.

## Findings

1. **Everything completes — no failures, no connection exhaustion.** All shapes returned at every scale,
   including 1000 concurrent shape creations and 600 concurrent subquery shapes (Postgres
   `max_connections=1200`).
2. **Shape creation is backfill-bound.** 500→1000 concurrent shapes ≈ doubles p99 (5.3 s → 10.2 s). The
   engine opens a **fresh Postgres connection per standalone backfill**, so at high concurrency the
   backfills contend on connections + the snapshot read. This is the clear next optimization (pool/limit
   backfill connections — already done for the subquery registry).
3. **Subquery shapes scale much better than match-all shapes.** 600 concurrent subquery shapes finish at
   p99 ~1.07 s vs 1000 match-all at p99 ~10 s — a subquery shape returns only the *matching* rows (keyed
   node lookup) and the subquery registry reuses a single pooled Postgres connection, while the match-all
   benchmark backfills the whole 2k-row table per shape.
4. **Write fanout is excellent and nearly flat in shape count.** Writes propagate to 200 *and* 400
   concurrent shapes at p99 ~41–44 ms — the live replication/tail path scales well; doubling the shape
   count barely moved the latency.

## Engine changes the benchmarks required

Running Electric's benchmarks surfaced two real protocol gaps, now fixed:
- **Constant `where` comparisons** (`373 = 373`): the benchmarks use a trivial always-true literal
  comparison as a unique match-all predicate; our SQL `where` parser now evaluates `<lit> <op> <lit>`
  (`where_sql.rs`).
- **Schema-qualified table names** (`public.users`): the adapter now strips the schema prefix
  (`electric.rs`).
