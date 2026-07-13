# @electric-ivm/bench

Benchmark and measurement tooling for [electric-ivm](../../README.md). Every runner boots the full
stack itself (durable-streams + engine, plus Postgres/API where relevant); results are written to
files under `docs/bench/` or the package directory.

## ElectricSQL benchmarking-fleet (`src/electric-bench-runner.ts`)

Runs the **unmodified** `.exs` benchmarks from
[electric-sql/benchmarking-fleet](https://github.com/electric-sql/benchmarking-fleet)
(`byo_electric` mode) against our `/v1/shape` adapter: boots the stack on an ephemeral Postgres,
seeds each benchmark's schema at scale, points the script at our engine, collects its statsd/UDP
telemetry, and reports latency percentiles + throughput as markdown.

```bash
pnpm bench:fleet          # from the repo root — auto-clones the fleet repo on first run
```

| Env | Default | Meaning |
|---|---|---|
| `BENCH_SCALE` | `1` | workload multiplier |
| `BENCH_ONLY` | *(all)* | comma list of benchmark names to run |
| `FLEET_DIR` | `../benchmarking-fleet` | path to a benchmarking-fleet clone (auto-cloned when absent) |
| `FLEET_REPO` | electric-sql/benchmarking-fleet | clone source |
| `BENCH_OUT` | `docs/bench/electric-fleet-results.md` | report path |
| `EXTERNAL_ELECTRIC_URL` | *(unset)* | run against an external Electric-compatible server instead of booting our stack (requires `EXTERNAL_DATABASE_URL`) |
| `EXTERNAL_DATABASE_URL` | *(unset)* | the external target's Postgres; benchmark tables are dropped/recreated there |

**macOS latency note:** the stack boots the real `durable-streams-server` binary (`@electric-ivm/ds-rust`)
instead of an in-process store. That binary's `--durability memory` mode is Linux-only (a zero-copy
socket→file path); on macOS every append falls back to disk-durable `wal` mode, which inflates
latency on shape-creation-heavy benchmarks (e.g. `concurrent_shape_creation_with_subqueries` p50
206ms → 1700ms+ in local testing). This is a platform artifact, not an engine regression — verified
by running the same workload in a Linux container with `ELECTRIC_STORAGE=MEMORY`, which reproduced
the original numbers (p50 ~318ms). Don't read macOS-local numbers on these benchmarks as regressions;
compare against the hosted fleet (Linux) instead.

## Shape-memory matrix (`src/shape-mem-matrix.ts`)

How engine memory evolves as shapes are created, across deployment sizes: seeds N issues + a
project/membership graph (the LinearLite visibility model), creates subquery-visibility and
status-equality shapes in batches, and samples the engine's `GET /memory` probes (RSS +
cardinalities) at each milestone. Results are written to `docs/bench/shape-memory-matrix.md`.

```bash
pnpm --filter @electric-ivm/bench shape-mem
MATRIX_SIZES=1000,10000,100000 MATRIX_USERS=10,25,50,100 MATRIX_PROJECTS=20 \
  pnpm --filter @electric-ivm/bench shape-mem
```

## Other runners

- **`src/run.ts`** (`pnpm bench` from the root) — local stress benchmark: many equality shapes,
  a write firehose, a write→shape-update latency prober (p50/p99), and a resource sampler.
  Env: `BENCH_SHAPES` (1000), `BENCH_SUBS` (100), `BENCH_DURATION` (10 s), `BENCH_CONC` (64),
  `BENCH_REGCONC` (64), `BENCH_OUT`.
- **`src/electric-adapter.ts`** — boots the stack with the `/v1/shape` adapter for external
  drivers: standalone (seeds Electric's standard `level_1..4` schema, for curl smoke tests) or
  driven by Electric's Elixir harness (`ADAPTER_PG_URL`/`ADAPTER_PG_TABLES` provided by it; prints
  `ADAPTER_LISTENING <url>`). Used by [electric-conformance/](../../electric-conformance/README.md).

Companion: [packages/loadgen](../loadgen) (state-machine load-generator clients,
Docker-scalable). Benchmark reports are written under `docs/bench/`.
