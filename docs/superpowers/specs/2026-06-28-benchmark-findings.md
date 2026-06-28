# electric-lite stress benchmark — findings & improvements (2026-06-28)

Goal: stress the system (max shapes, write throughput, subscribers), use engine telemetry to find
bottlenecks, drive improvements, and keep memory/CPU bounded under sustained load — targeting 100k
shapes at <50ms p99. Plus: include non-shareable shapes, and find & cut memory amplifications.

## Harness

`packages/bench` boots the real stack (durable-streams + Rust engine + tRPC API, no oracle) and runs,
concurrently for a fixed window:

1. a **write firehose** (bounded in-flight) — sustained throughput;
2. a **latency prober** — writes to subscribed shapes, times write→shape-update (p50/p99);
3. a **resource sampler** — engine RSS/CPU + thread count via `ps`, plus the engine's `/metrics`.

Shapes are registered create-only (direct `POST /shapes`); only the measured sample is live-subscribed
(a `client.shape()` subscription holds a long-poll connection, so subscribing all N would itself
exhaust sockets — see the port limit below). Config via `BENCH_*` env; results are written durably to
`packages/bench/results.txt`.

## Telemetry added

`GET /metrics` (+ `POST /metrics/reset`): lock-free counters (`envelopes_processed`, `shape_appends`,
`family_steps`) and log-bucket latency histograms (`process_envelope`, `family_step`, `append`) with
p50/p99/p999/max. This is what attributed each bottleneck below.

## Bottlenecks found → improvements (telemetry-driven)

### 1. Serial, per-envelope HTTP appends (throughput + latency)
Telemetry: `append` dominated `process_envelope` (p99 8.2ms, vs `family_step` 0.06ms), done serially
one-per-envelope → backlog and head-of-line latency blowup under load.
**Fix:** process a whole read batch, stage appends per shape stream, flush bounded-concurrently
(CAP=32). Per-envelope txids preserved (no envelope merging), so `awaitTxId` still works.
**Result @1k shapes/8s:** processed 9k→21k envelopes; e2e p99 2879ms→188ms; `process_envelope` p99
8.2ms→0.26ms.

### 2. One OS thread per non-shareable shape (the standalone scaling wall + memory amplification)
A `WHERE` filter is *stateless*, yet each non-equality shape (range / OR / NOT / inequality / match-all)
ran in its own dbsp filter circuit on its **own OS thread**, with a **per-shape clone of every delta**
fed over a channel. That capped standalone shapes at ~a few thousand (thread limit) and amplified
memory/CPU under fan-out.
**Fix:** `eval_standalone()` filters the delta in place — no thread, no circuit, no clone. Only
families (which *join*) still use dbsp. `circuit.rs` removed entirely.
**Result:** engine threads stay flat at ~18 whether there are 0 or 5,000 standalone shapes. The new
ceiling is CPU — O(K) predicate evals per write — not threads:

| standalone shapes | threads | RSS    | throughput |
|-------------------|---------|--------|------------|
| 2,000             | 18      | 29MB   | 1,999/s    |
| 5,000             | 18      | 31MB   | 1,481/s    |

### 3. Connection leak: one leaked socket per stream op (resource amplification)
`ds.rs` checked the response status but never consumed the body on the success path, so reqwest could
not return the connection to its pool — a leaked socket per `ensure_stream`/`append`.
**Fix:** drain the body on success. Verified: ESTABLISHED connections now hold flat at ~197 during a
30k-shape registration instead of growing one-per-shape.

## Headline scaling results

**100,000 equality shapes, one shared family circuit (target scale, met):**

| metric | value |
|--------|-------|
| shapes registered & active | **100,000** (1 family) |
| **end-to-end p99 latency** | **10.8ms** (p50 3.4ms) — under the 50ms target |
| `process_envelope` p99 | 0.26ms |
| `family_step` p99 (join over 100k params) | **0.26ms — identical to the 10k run** |
| engine RSS | 64→67MB (bounded) |
| threads | 16 (flat) |
| CPU avg | 34% |

The decisive result: every write is joined against all 100k params, yet `family_step` p99 is **0.26ms**,
the same as at 10k — per-write cost is genuinely **O(log N)** and memory stays flat. The firehose was
confined to a 200-shape hot set so the load phase would not churn 100k streams (the local socket limit
below); all 100k shapes remained registered and active in the join. Registration used chunked batches
with TIME_WAIT-drain pauses (`BENCH_CHUNK`) to stay under the ephemeral-port ceiling without sysctl —
355s for 100k, a one-time cost.

**10,000 equality shapes, one shared family circuit:**

| load | writes/s | e2e p50 | e2e p99 | process_envelope p99 | append p99 | RSS | threads |
|------|----------|---------|---------|----------------------|------------|-----|---------|
| sustainable (1.6k/s) | 1,636 | **3.0ms** | **7.3ms** | 0.51ms | 0.51ms | 37→47MB | 18 |
| max firehose | 6,919 issued / ~6,800 processed | 345ms | 1478ms | 0.51ms | 8.2ms | 37→87MB | 18 |

- **<50ms p99 met with margin** (7.3ms) under sustainable load. The engine adds <1ms; `process_envelope`
  p99 is 0.51ms even at 10k shapes.
- Under the max firehose, e2e latency is bound by the **single-threaded Node test DS server** queuing
  (6.9k writes + 110k appends/s through one process), not the engine — the engine still processes 139k
  envelopes in 20s with internal p99 0.51ms.
- Memory and threads stay bounded; throughput is storage-bound, not engine-bound.

## Memory amplifications: cut vs. remaining

**Cut:**
- thread stack per standalone shape → 0 (direct eval);
- per-shape delta clone per standalone shape → 0 (eval by reference);
- leaked socket per stream op → 0 (drain response body).

**Remaining (acceptable, documented):**
- Each **family** holds the full table indexed by its key in its dbsp data trace. Memory is therefore
  `O(#templates × table) + O(#shapes × small-constant)`, **not** `O(#shapes × table)` — bounded by the
  number of distinct equality-column-sets (a handful), not by shape count. Equality shapes add only a
  `Params` entry each; standalone shapes hold no state.

## The 100k local ceiling (harness, not engine)

Registering 100k shapes locally fails with `EADDRNOTAVAIL` (`os error 49`) — **ephemeral-port
exhaustion**, not a file-descriptor limit (fds are already 1,048,576). macOS exposes only ports
49152–65535 (~16k) with a ~30s `TIME_WAIT`; creating/touching >~16k streams in a burst churns sockets
faster than they recycle. After the leak fix, connections pool (ESTABLISHED flat ~197) but `TIME_WAIT`
still climbs to ~15k during a fast registration and hits the wall.

This is a property of the local in-memory test server + macOS defaults, **not** the engine: per-write
cost is O(log N) in the family join and O(1) in streams touched, and memory/threads are flat.

**Worked around without sysctl** to hit the 100k target above: register in chunks with TIME_WAIT-drain
pauses (`BENCH_CHUNK`), and confine the firehose to a bounded hot set (`BENCH_HOTSET`) so the load phase
reuses a small pool of connections. All 100k shapes stay registered and active in the join. Other
options for a continuous high-rate 100k load: widen `net.inet.ip.portrange` / shorten `net.inet.tcp.msl`
via sysctl, or point the bench at a keep-alive production storage backend.

## Reproduce

```
cargo build --release -p electric-lite-engine
# 100k shapes end-to-end (chunked registration + bounded hot set), p99 latency:
BENCH_SHAPES=100000 BENCH_CHUNK=12000 BENCH_CHUNK_PAUSE=30 BENCH_HOTSET=200 \
  BENCH_SUBS=100 BENCH_CONC=4 BENCH_DURATION=15 pnpm bench
BENCH_SHAPES=10000 BENCH_SUBS=100 BENCH_DURATION=20 BENCH_CONC=64 pnpm bench  # max throughput
BENCH_SHAPES=100 BENCH_STANDALONE=5000 BENCH_DURATION=6 pnpm bench            # standalone scaling
```
