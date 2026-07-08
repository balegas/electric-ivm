# Electric benchmarking-fleet — results vs electric-ivm

Generated 2026-07-08T01:43:24.930Z on darwin/arm64. Scale 1.
Each row runs the unmodified ElectricSQL `byo_electric` benchmark `.exs` against our `/v1/shape`
adapter; latency is the per-shape fetch duration the benchmark emits over statsd (ms).

| benchmark | workload | shapes | wall (s) | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) |
|-----------|----------|-------:|---------:|---------:|---------:|---------:|---------:|
| concurrent_shape_creation | 2,000 users | 500 | 3.9 | 1469 | 2712 | 2806 | 2824 |
| concurrent_shape_creation_with_subqueries | 300 groups, 600 parents, 3,000 children | 300 | 4.4 | 207 | 334 | 346 | 349 |
| many_shapes_one_client_latency | 2,000 users | 1 | 4.9 | 44 | 44 | 44 | 44 |
| diverse_shape_fanout | 2,000 users | 200 | 3.3 | 36 | 43 | 43 | 44 |
| write_fanout | 2,000 users | 200 | 2.2 | 34 | 37 | 37 | 37 |
