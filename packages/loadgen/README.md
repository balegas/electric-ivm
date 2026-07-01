# @electric-lite/loadgen

A headless load generator that simulates users of the LinearLite issue tracker to observe **engine
memory, CPU, and disk** across workload sizes. No rendering, no DOM — each user is a state machine that
drives the real `@electric-lite/client` for reads and writes to **Postgres** (the system of record) for
mutations. Designed to run many users from one node, and to scale out across client nodes with Docker
when a single machine's connection limits get in the way.

## What a simulated user does

Each user is a state machine (`src/user.ts`) with a bounded set of live subscriptions (≈ its open
connections) and a think-timed action loop:

- **Reads (via the client)** — the app's navigation:
  - browse **subset feeds** (`project_id = P`, paginated) for the user's member projects,
  - a live **COUNT aggregation** over its visible issues (the top-of-list counter),
  - the **board** view: 5 status shapes using the visibility **subquery** (`project_id IN (SELECT …)`).
  - navigation: scroll (`loadMore`), switch project (close/open feeds), toggle the board.
- **Writes (to Postgres)** — `create / update / delete` issues and `add comment`, compiled with the
  protocol's `changeEventToDML` and applied through a shared, bounded write pool. Postgres replicates
  them into the engine, which drives the subscriptions — so writes exercise the whole pipeline.

Visibility is faithful: a user only sees issues in projects it belongs to (the fixed roster/membership
from the demo). One shared client is used across users (identity lives in the predicates); each user
opens its own subscriptions.

## Run it (single node)

Boots its own ephemeral Postgres (logical replication) + durable-streams (file-backed, so disk is
measurable) + engine + API, seeds, runs, samples, reports, and tears everything down.

```bash
# defaults: 20 users, 2000 seed issues, 60s
pnpm --filter @electric-lite/loadgen loadgen

# a bigger workload
USERS=100 SEED_ISSUES=20000 DURATION_S=90 pnpm --filter @electric-lite/loadgen loadgen
```

Output: a per-second CSV (`results/metrics-<label>.csv`) and a printed + JSON summary
(`results/summary-<label>.json`) with peaks/finals:

```
t_s,users,open_subs,reads,writes,writes_per_s,rss_mb,cpu_cores,pg_mb,ds_mb,envelopes,appends,shapes,family_circuits,subquery_nodes,standalone,append_p99_ms
```

### Sweep workload sizes

Runs one `all`-mode job per USERS size and prints a comparison table (RSS / CPU / PG MB / ds MB /
shapes / subquery nodes / … vs. workload size):

```bash
SWEEP_USERS=10,50,150 SEED_ISSUES=10000 DURATION_S=45 pnpm --filter @electric-lite/loadgen sweep
```

## Config (env)

| var | default | meaning |
|---|---|---|
| `USERS` | 20 | concurrent simulated users |
| `SEED_ISSUES` | 2000 | issues seeded before the run |
| `DURATION_S` | 60 | run length |
| `WRITE_RATE` | 0.25 | probability a user's action is a mutation (else navigate) |
| `FEEDS_PER_USER` | 4 | bounded live subscriptions per user (≈ read connections) |
| `THINK_MIN_MS`/`THINK_MAX_MS` | 400 / 2500 | think time between actions |
| `RAMP_MS` | 25 | stagger between starting users (avoid connection thundering-herd) |
| `WRITE_POOL` | 24 | shared Postgres write-pool size on this node |
| `SAMPLE_MS` | 2000 | metrics sampling interval |
| `OUT_DIR` / `LABEL` | `results` / `u<USERS>` | output location / run label |

## Connections & file descriptors — the single-node ceiling

Each open subscription is a long-poll connection to durable-streams, so **open connections ≈ USERS ×
FEEDS_PER_USER**. Two limits bite from one machine:

- **File descriptors** — the loadgen checks the soft `ulimit -n` at startup and tells you to raise it
  (`ulimit -n 100000`) if it's below `USERS × FEEDS_PER_USER + overhead`.
- **Ephemeral ports** — one machine has ~16k outbound ports to a *single* destination (49152–65535 on
  macOS). At `FEEDS_PER_USER=4` that caps you near **~3–4k users per node** to one durable-streams
  server, regardless of FD limit. Beyond that, spread load over more nodes.

## Scale out across nodes (Docker)

Client containers each get their own network namespace → their own ephemeral-port range, so N client
nodes multiply the ceiling. The infra runs once (on the host); clients connect to it.

```bash
# 1. Host: boot the infra bound to all interfaces with fixed ports, and keep it up + sample metrics.
BIND_HOST=0.0.0.0 API_PORT=8790 DS_PORT=8791 PG_PORT=8792 SEED_ISSUES=20000 \
  LOADGEN_MODE=infra pnpm --filter @electric-lite/loadgen loadgen

# 2. Scale client nodes (each = one container/network namespace):
cd packages/loadgen/docker
USERS=50 DURATION_S=120 docker compose up --build --scale client=8
#   → 8 nodes × 50 users = 400 users, across 8 ephemeral-port ranges.
```

The host infra samples engine RSS/CPU + Postgres/durable-streams disk into `results/metrics-infra.csv`
while the client nodes generate load. Client containers set `nofile` ulimits high; tune `USERS`,
`FEEDS_PER_USER`, and the replica count to the connection budget above.

## Modes

- `all` (default) — self-contained: boot infra + run users + sample + report + teardown.
- `infra` — boot infra + seed, print client URLs, sample server metrics until Ctrl-C (for Docker).
- `client` — connect to an existing infra (`API_URL`, `DS_URL`, `PG_URL`) and run users (Docker replicas).
