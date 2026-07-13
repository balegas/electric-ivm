# Electric benchmarking-fleet — results vs electric-ivm

Generated 2026-07-13T14:23:54.120Z on darwin/arm64. Scale 1.
Each row runs the unmodified ElectricSQL `byo_electric` benchmark `.exs` against our `/v1/shape`
adapter; latency is the per-shape fetch duration the benchmark emits over statsd (ms).

| benchmark | workload | shapes | wall (s) | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) |
|-----------|----------|-------:|---------:|---------:|---------:|---------:|---------:|
| concurrent_shape_creation | 2,000 users | 500 | 2.6 | 766 | 1376 | 1429 | 1443 |
| concurrent_shape_creation_with_subqueries | 300 groups, 600 parents, 3,000 children | 300 | 4.4 | 206 | 342 | 355 | 360 |
| many_shapes_one_client_latency | 2,000 users | 1 | 3.5 | 64 | 64 | 64 | 64 |
| diverse_shape_fanout | 2,000 users | 200 | 2.7 | 38 | 42 | 42 | 43 |
| write_fanout | 2,000 users | 200 | 2.2 | 42 | 47 | 47 | 47 |
