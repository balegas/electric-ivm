# Electric Circuits — a hands-on tutorial series

Modern applications need to keep the client continuously in step with the backend. The usual ways to
do that are polling, or hand-rolled caches and queues that drift out of sync. **Electric Circuits**
replaces all of that: your app keeps writing to Postgres with ordinary SQL, and Postgres stays the
single source of truth — but every query your app needs becomes a **live query**, a result the engine
keeps up to date forever, delivered to you as a **Stream** of `insert`/`update`/`delete` messages. No
polling, no cache invalidation.

Behind every live query is a **circuit**: a small, fixed set of always-on dataflows — one per *kind*
of query (filtering, membership, aggregation) — that your queries register onto rather than build.
That's what makes registering a new query cheap, and it's what this series shows you, from the
terminal, one step at a time, with a live window into what the engine is actually doing.

## The three public nouns

Three words carry the whole series:

- **Streams** — the durable, replayable log every live query's changes travel over.
- **Circuits** — the fixed, shared dataflows (built on DBSP) that maintain live queries incrementally.
- **Live queries** — the result you actually get: current rows, then every change that affects them,
  forever.

In the API, live queries are created with `client.shape()` and served at `/v1/shape` (the Electric
protocol name this engine also speaks).

## What you'll learn

By the end of the series you'll understand how Electric Circuits turns a query into something live —
not as theory, but as something you've driven by hand: how a live query becomes a running circuit of
operators, why a change flows through it exactly once, why an update is secretly a retraction plus an
insertion, and how cross-table membership queries and live aggregations actually work today. You don't
need to know DBSP going in. You do need to be comfortable running commands in a shell and reading a
bit of SQL.

## How the series works

Three tools, and you'll use all three in most episodes:

- **the shell (`curl`)** — you create and read live queries over plain HTTP. No SDK, no client
  library; just requests you can read.
- **`psql`** — you make changes the ordinary way, from your host, with SQL `INSERT`/`UPDATE`/`DELETE`
  straight against the tutorial's Postgres, exactly as a real app would.
- **the pipeline explorer** — a browser view that draws the engine's *actual* running circuit. The
  nodes, counters, and edges you see aren't a diagram someone drew; they come from the engine's own
  introspection endpoints (`/graph`, `/state`, `/trace`). When you write a row, you watch the change
  travel the circuit live.

Each episode leads with `curl` and `psql`, because those teach the wire protocol and the real-app
write path; the explorer is there alongside them so you can see what the engine did, not just what it
returned.

## Before you start

You'll need **Docker** (with Compose), a **terminal**, a **browser**, and **`psql`** — the PostgreSQL
client (`brew install libpq` on macOS, or your distro's `postgresql-client`).

Don't want to install `psql`? Every `psql` command in the series also runs inside the stack — just
swap the host connection for a container exec:

```sh
# host (what the episodes show):
psql "postgres://postgres:password@localhost:5432/electric" -c '…'
# in-container fallback (no psql needed):
docker compose exec -T postgres psql -U postgres -d electric -c '…'
```

## Start the stack

From this directory:

```sh
docker compose up --build
```

Give it a minute the first time (it builds the engine). Five containers come up:

| service | role |
|---|---|
| `postgres` | the system of record, auto-seeded from `./seed` on first boot |
| `ds` | the durable-streams server — every live query's Stream lives here |
| `engine` | the Rust engine: replication ingest, circuits, `/v1/shape`, and the introspection endpoints |
| `api` | the extended API (tRPC) — not used until episode 4 |
| `viz` | the pipeline explorer |

| you want | address |
|---|---|
| engine control HTTP (`/health`, `/v1/shape`, `/graph`, …) | `http://localhost:7010` |
| pipeline explorer (browser) | `https://localhost:5543` |
| pipeline explorer (plain HTTP, for curl/scripts) | `http://localhost:5280` |
| extended API | `http://localhost:8790` |
| durable streams | `http://localhost:8791` |
| Postgres | `localhost:5432` |

(These ports are deliberately offset from the flagship LinearLite demo's, so both stacks can run at
once without colliding.)

Episode 1 walks through checking each of these is healthy — start there.

## Trusting the explorer's certificate

The explorer is served over HTTPS with a certificate signed by Caddy's own internal CA, which your
browser doesn't know about — so the first time you open `https://localhost:5543` it warns that the
connection isn't private. The connection is still encrypted; click through the warning and carry on.

To remove the warning for good, trust the CA once. It persists across container rebuilds (it lives in
the `viz-caddy-data` volume), so trusting it sticks:

```sh
# extract the root CA the explorer signs with
docker compose cp viz:/data/caddy/pki/authorities/local/root.crt ./viz-local-ca.crt
# trust it (macOS — prompts for your admin password)
sudo security add-trusted-cert -d -r trustRoot -k /Library/Keychains/System.keychain ./viz-local-ca.crt
```

Reopen `https://localhost:5543` and the warning is gone.

## The episodes

1. **[Your first live query](episodes/01-first-live-query/README.md)** — create one live query with a
   single HTTP request, watch the engine build its circuit, and watch one write flow through it end to
   end.
2. **[Inside the circuit: deltas and weights](episodes/02-inside-the-circuit/README.md)** — the same
   live query, exploded into the DBSP operators the engine actually runs: deltas, weights, and why an
   update is a retraction plus an insertion.
3. **[Cross-table live queries with subqueries](episodes/03-subqueries-are-dynamic/README.md)**
   — a membership live query across two tables, maintained by a shared inner-set node; equality live
   queries sharing a router automatically.
4. **[Aggregations: a live COUNT](episodes/04-aggregations/README.md)** — a live per-list open-todo
   count, maintained incrementally — the one place today's engine still has a piece you configure
   ahead of time.

Each episode picks up from the last.

## Resetting

To wipe all state and return to episode 1's starting point from anywhere in the series:

```sh
docker compose down -v && docker compose up
```
