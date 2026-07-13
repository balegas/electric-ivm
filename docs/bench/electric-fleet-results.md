# Electric benchmarking-fleet — results vs electric-ivm

Generated 2026-07-13T16:03:01.778Z on darwin/arm64. Scale 1.
Each row runs the unmodified ElectricSQL `byo_electric` benchmark `.exs` against our `/v1/shape`
adapter; latency is the per-shape fetch duration the benchmark emits over statsd (ms).

| benchmark | workload | shapes | wall (s) | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) |
|-----------|----------|-------:|---------:|---------:|---------:|---------:|---------:|
| concurrent_shape_creation | 2,000 users | 500 | 1.7 | 398 | 554 | 556 | 557 |
| concurrent_shape_creation_with_subqueries | 300 groups, 600 parents, 3,000 children | 300 | 6.5 | 1223 | 2228 | 2321 | 2370 |
| many_shapes_one_client_latency | 2,000 users | 1 | 2.6 | 25 | 25 | 25 | 25 |
| diverse_shape_fanout | 2,000 users | 200 | 2.5 | 23 | 27 | 27 | 27 |
| write_fanout | 2,000 users | 200 | 2.3 | 29 | 32 | 33 | 33 |
