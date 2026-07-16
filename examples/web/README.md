# examples/web — live todos

The smallest end-to-end electric-circuits demo: a React todos app where **writes go to Postgres and the
UI updates through the sync engine**, never by local mutation.

- Left pane: every todo (a match-all shape), editable — each edit `POST`s to a tiny `/pg/write`
  dev middleware that runs real DML against Postgres.
- Right pane: a **live shape** the engine evaluates —
  `done = false AND priority >= 3` (`src/electric.ts`). Rows enter/leave as you edit on the left:
  Postgres → logical replication → engine → shape stream → TanStack DB collection
  (`useLiveQuery`).

## Run

Requires Node ≥ 22, pnpm 10, Rust stable, and PostgreSQL 16 binaries on `PATH`
(`initdb`/`pg_ctl` — the demo boots its own ephemeral cluster). From the repo root:

```bash
pnpm install
pnpm demo:web     # then open the printed Vite URL
```

`start.ts` boots everything on ephemeral ports: an ephemeral Postgres with `wal_level=logical`,
the Rust engine in Postgres mode (built via cargo, ingesting through a replication slot),
durable-streams + the tRPC API for the read path, and Vite (proxying `/api` → API and `/ds` →
streams, so the browser only ever talks to the dev server).

## Env knobs

| Var | Default | Meaning |
|---|---|---|
| `DEMO_SEED_COUNT` | `200` | initial todos bulk-inserted into Postgres |
| `DEMO_CHURN_MS` | `0` (off) | when > 0, one random Postgres write every N ms (~30% insert / 50% update / 20% delete) — the live shape moves on its own |

```bash
DEMO_SEED_COUNT=10000 DEMO_CHURN_MS=50 pnpm demo:web
```

## Where things are

| File | Role |
|---|---|
| `start.ts` | dev entrypoint: Postgres + engine + streams + API + Vite (+ `/pg/write`, seeding, churn) |
| `src/schema.ts` | the one-table `todos` schema (`@electric-circuits/protocol` Schema) |
| `src/electric.ts` | `createClient` wiring (proxy-friendly `long-poll` live mode) + the live-shape definition |
| `src/shapes.ts` | shape registration, cached so HMR/re-renders don't register duplicates |
| `src/App.tsx` | the two panes; writes via `/pg/write` |

The flagship demo (subqueries, subsets, aggregations at scale) is
[examples/linearlite](../linearlite). Architecture: [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md).
