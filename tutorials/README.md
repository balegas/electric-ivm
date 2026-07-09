# electric-ivm — a hands-on tutorial series

Modern applications need to continuously synchronize backend changes to the client in real time. The usual ways to do this are to poll the database repeatedly, or to assemble caches and queues and hope they stay consistent. A **sync engine** replaces all of that: it keeps subsets of your database (Postgres in our case) continuously up to date wherever they're needed, streaming every change out in real time over plain HTTP. Postgres stays the single source of truth; the sync engine is the pipe that keeps everything downstream in step with it.

The unit of sync is a **shape**. A shape is a subset of your Postgres data — the rows of a table that match a `where` clause (and, if you like, only some of their columns). You declare the shape once and the engine hands you its rows, then keeps them live: every insert, update, or delete that affects the shape is pushed to you as it happens. You sync only the data you actually need, and it never goes stale. (Shapes come from [ElectricSQL](https://electric.ax), whose shape protocol this engine speaks.)

**electric-ivm** is an experiment: a reimagining of Electric in Rust that swaps its hand-rolled shape-matching for a **general-purpose incremental view maintenance (IVM) engine**. This allows more expressive shapes, so a shape on the backend stays close to what the client actually queries. It's built on **DBSP** ([paper](https://arxiv.org/abs/2203.16684), VLDB 2023), which compiles a query into a circuit of operators over streams of changes and maintains even rich queries incrementally. That's what this series is about.

This series teaches that engine the way you'd actually poke at it — from a terminal, one small step at a time — and gives you a window into what it's doing while you do it.

## What you'll learn

By the end of the series you'll understand how a **DBSP-style incremental view maintenance** system works, not as theory but as something you've driven by hand: how a query becomes a running dataflow pipeline, why a change flows through that pipeline exactly once, and why an update is secretly a retraction plus an insertion. Early episodes cover a single live shape and the operators inside it; later ones reach into cross-table queries, aggregations, and pipelines shared between clients.

You don't need to know DBSP going in. You do need to be comfortable running commands in a shell and reading a bit of SQL.

## How the series works

Three tools, and you'll use all three in most episodes:

- **the shell** (`curl`) — you create and read shapes over plain HTTP. No SDK, no client library; just requests you can read.
- **`psql`** — you make changes the ordinary way, from your host, with SQL `INSERT`/`UPDATE`/`DELETE` straight against the tutorial's Postgres, exactly as a real app would.
- **the pipeline visualizer** — a browser view that draws the engine's *actual* running pipeline. The nodes, counters, and edges you see aren't a diagram someone drew; they come from the engine's own introspection endpoints. When you write a row, you watch the change travel the pipeline live. Open it over HTTPS (`https://localhost:5543`) — the page holds several live streams at once, and browsers throttle those over plain HTTP.

Everything runs locally in Docker — one Postgres, the engine, a durable-streams log, and the visualizer — so you can break things freely and reset to a clean slate whenever you like.

## Before you start

You'll need **Docker** (with Compose), a **terminal**, a **browser**, and **`psql`** — the PostgreSQL client, for making writes against the database (`brew install libpq` on macOS, or your distro's `postgresql-client`).

Don't want to install `psql`? Every `psql` command in the series also runs inside the stack — just swap the host connection for a container exec:

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

Give it a minute the first time (it builds the engine). Episode 1 walks through what each container is and how to check it's healthy — start there.

## Trusting the visualizer's certificate

The visualizer is served over HTTPS with a certificate signed by Caddy's own internal CA, which your browser doesn't know about — so the first time you open `https://localhost:5543` it warns that the connection isn't private. The connection is still encrypted; you can just click through the warning and carry on.

To remove the warning for good, trust the CA once. The CA persists across container rebuilds (it lives in the `viz-caddy-data` volume), so trusting it once sticks. Extract the root certificate from the running `viz` container, then add it to your system trust store:

```sh
# extract the root CA the visualizer signs with
docker compose cp viz:/data/caddy/pki/authorities/local/root.crt ./viz-local-ca.crt
# trust it (macOS — prompts for your admin password)
sudo security add-trusted-cert -d -r trustRoot -k /Library/Keychains/System.keychain ./viz-local-ca.crt
```

Reopen `https://localhost:5543` and the warning is gone.

## The episodes

1. **[Your first live shape](episodes/01-first-shape/README.md)** — create one shape with a single HTTP request, watch the engine build its pipeline, and watch one write flow through it end to end.
2. **[Inside the pipeline](episodes/02-inside-the-pipeline/README.md)** — the same shape, exploded into the DBSP circuit the engine really executes: deltas, weights, and why an update is a retraction plus an insertion.
3. **[Pipelines, shapes, and strangers](episodes/03-serving-model/README.md)** — step out of one shape into the app's whole query graph: deploy a static compiled pipeline for a todo model and watch the three-tier serving model at work — pipelines serve families, routing serves instances, the fallback serves strangers — with shapes latching onto the pipeline (and letting go) live on the canvas.

Each episode picks up from the last.

## Resetting

To wipe all state and return to Episode 1's starting point from anywhere in the series:

```sh
docker compose down -v && docker compose up
```
