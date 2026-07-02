# ElectricSQL benchmarking-fleet — results vs electric-ivm

Generated 2026-07-02 on darwin/arm64. We ran ElectricSQL's **own** `byo_electric` benchmarks
(unmodified `.exs` scripts from [electric-sql/benchmarking-fleet](https://github.com/electric-sql/benchmarking-fleet))
against electric-ivm's Electric-protocol `/v1/shape` adapter, the same way the fleet benchmarks the real
Electric sync-service. This is a load/throughput companion to the oracle conformance tests.

## Method

A runner (`packages/bench/src/electric-bench-runner.ts`) does, per benchmark: boot our stack
(durable-streams + engine + `/v1/shape` adapter on an ephemeral Postgres), seed the benchmark's schema at
scale, run the unmodified ElectricSQL benchmark script with `ELECTRIC_SERVER` pointed at our adapter, and
aggregate the per-shape latency the benchmark emits over statsd/UDP. The benchmark scripts are vanilla
`Mix.install([:req, …])` — no GCP fleet, no Docker. Reproduce with one command (it clones the fleet repo
and builds the release engine itself if needed):

```
pnpm bench:fleet                 # scale 1
BENCH_SCALE=2 pnpm bench:fleet   # double workload
```

`latency` is the per-shape *fetch-complete* duration the benchmark measures: initial shape sync for the
shape-creation benchmarks, and **write-to-visible propagation through a live long-poll** for the
fanout/latency benchmarks (each of N concurrent live requests on one handle must receive the fanned-out
write). `wall` is the whole benchmark's wall-clock.

## Results

### Standard workload (`BENCH_SCALE=1`)

| benchmark | workload | samples | wall (s) | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) |
|-----------|----------|--------:|---------:|---------:|---------:|---------:|---------:|
| concurrent_shape_creation | 500 concurrent shapes, 2k-row table | 500 | 6.2 | 2581 | 4828 | 5022 | 5072 |
| concurrent_shape_creation_with_subqueries | 300 concurrent **subquery** shapes | 300 | 4.5 | 213 | 339 | 350 | 353 |
| many_shapes_one_client_latency | write → visible, 1 live client | 1 | 7.1 | 86 | 86 | 86 | 86 |
| diverse_shape_fanout | write fanout → 200 live long-polls | 200 | 4.2 | 59 | 68 | 69 | 69 |
| write_fanout | write fanout → 200 live long-polls | 200 | 2.1 | 37 | 40 | 40 | 40 |

### Large workload (`BENCH_SCALE=2`)

| benchmark | workload | samples | wall (s) | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) |
|-----------|----------|--------:|---------:|---------:|---------:|---------:|---------:|
| concurrent_shape_creation | **1000** concurrent shapes, 2k-row table | 1000 | 12.0 | 5197 | 10336 | 10733 | 10834 |
| concurrent_shape_creation_with_subqueries | **600** concurrent **subquery** shapes | 600 | 4.9 | 473 | 822 | 852 | 860 |
| many_shapes_one_client_latency | write → visible, 1 live client | 1 | 12.6 | 94 | 94 | 94 | 94 |
| diverse_shape_fanout | write fanout → **400** live long-polls | 400 | 6.6 | 75 | 90 | 91 | 91 |
| write_fanout | write fanout → **400** live long-polls | 400 | 2.2 | 44 | 50 | 50 | 50 |

## Findings

1. **Everything completes — no failures, no connection exhaustion.** All shapes returned at every scale,
   including 1000 concurrent shape creations, 600 concurrent subquery shapes (Postgres
   `max_connections=1200`), and 400 concurrent live long-polls receiving a fanned-out write.
2. **Shape creation is backfill-bound.** 500→1000 concurrent shapes ≈ doubles p99 (5.0 s → 10.7 s). The
   engine opens a **fresh Postgres connection per standalone backfill**, so at high concurrency the
   backfills contend on connections + the snapshot read. This is the clear next optimization (pool/limit
   backfill connections — already done for the subquery registry).
3. **Subquery shapes scale much better than match-all shapes.** 600 concurrent subquery shapes finish at
   p99 ~0.85 s vs 1000 match-all at p99 ~10.7 s — a subquery shape returns only the *matching* rows
   (keyed node lookup) and the subquery registry reuses a single pooled Postgres connection, while the
   match-all benchmark backfills the whole 2k-row table per shape.
4. **Write fanout is fast and nearly flat in fan-out width.** A committed write is visible to 200
   concurrent live long-polls at p99 ~40–69 ms and to 400 at p99 ~50–91 ms — the full
   replication → engine → shape-stream → adapter live path. Concurrent live requests on one handle at
   the same offset are **coalesced** (one leader reads/applies; every waiter gets the identical
   response), so doubling the fan-out width barely moves the latency.

## Engine/adapter changes the benchmarks required or validated

- **Constant `where` comparisons** (`373 = 373`): the SQL `where` parser evaluates `<lit> <op> <lit>`
  at parse time (`where_sql.rs`).
- **Schema-qualified table names** (`public.users`): the adapter strips the schema prefix.
- **Live long-poll semantics**: live requests hold until data or an Electric-like deadline
  (`ELECTRIC_LIVE_TIMEOUT_MS`, default 20 s; 204 + up-to-date at the deadline), and concurrent live
  requests per (handle, offset) are coalesced — both were required for the fanout benchmarks, whose
  N clients long-poll one handle concurrently and treat premature timeouts as fatal.
