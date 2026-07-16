# LinearLite on electric-circuits

A port of [ElectricSQL's LinearLite](https://github.com/electric-sql/electric/tree/main/examples/linearlite)
(a Linear-style issue tracker) to the **electric-circuits** prototype. It demonstrates the prototype's
model end-to-end: **Postgres is the system of record**, the app writes to Postgres, and the engine
keeps each filtered view (a *shape*) live by ingesting Postgres **logical replication** and reading
rows back for backfill.

```
  browser ──writes──▶  Postgres  ──logical replication──▶  engine  ──append──▶  durable-streams
   (list/board)          ▲                                   │                    (shape/<id>)
        ▲                └──────────── backfill SELECT ──────┘                         │
        └──────────────────────── live shape rows (TanStack DB) ◀──────────────────────┘
```

## Run it

```sh
pnpm demo:linearlite
# or: DEMO_SEED_COUNT=2000 pnpm demo:linearlite
```

This boots everything with one command: an ephemeral Postgres (with `wal_level=logical`), the engine
in Postgres mode, durable-streams, the API, and Vite. Open the printed Local URL (default
`http://localhost:5174`). The Postgres cluster is throwaway — data resets each run.

| Env var | Default | Effect |
|---|---|---|
| `DEMO_SEED_COUNT` | `512` | Number of issues to generate (faker), matching the upstream default. ~50% get one comment. |

## What it shows

- **List view** — issues filtered by the engine. The status/priority filter builds a shape predicate
  (`status IN (…) AND priority IN (…)`, expressed with our `eq`/`and`/`or`) that the engine evaluates;
  changing filters swaps the shape. Search and sort are applied client-side.
- **Board view** — five Kanban columns, each backed by its own live shape
  (`issues WHERE status = '<status>'`). Drag a card to another column to change its status (a write to
  Postgres that flows back through replication).
- **Issue detail** — a live single-issue shape plus a per-issue comments shape
  (`comments WHERE issue_id = <id>`), created and closed with the view. Edit title/description/status/
  priority, add/delete comments, delete the issue (comments cascade in Postgres).
- **Create issue** — a modal that inserts into Postgres.

Every mutation is a real `INSERT`/`UPDATE`/`DELETE` against Postgres (via the dev server's `/pg/write`
middleware); the engine observes it over the replication slot and updates the relevant shapes live.

## How it maps onto electric-circuits

electric-circuits shapes are *one table + a `WHERE` over that table's own columns* (no joins), with value
types `int | float | text | bool` and a single-column primary key. The port adapts the original
accordingly:

- **Schema** (`src/schema.ts`): `issues` and `comments`. electric-circuits's value types are
  `int | float | text | bool`, so the original's UUID ids and `timestamptz` become electric-circuits **`int`**
  columns (Linear-style numeric issue ids; epoch-millis timestamps, so `created`/`modified` are
  sortable/filterable). Because the client mints ids from `Date.now()` (which overflows Postgres `int4`),
  `start.ts` creates the underlying Postgres columns as **`BIGINT`** explicitly rather than via the
  default `tableDDL` (which would emit `int4`) — the electric-circuits type is `int`, the physical Postgres
  type is `BIGINT`. `status` and `priority` are stored as their lowercase string constants, exactly as
  upstream.
- **No cross-table queries**: the issue↔comments relationship is expressed as a per-issue comments
  shape, not a join.
- **Two-level querying**: the *shape* (engine-side predicate) decides what syncs — status/priority/id.
  Everything else runs as a **TanStack DB live query** over the materialized collection: ordering
  (`orderBy` by date/kanban-order), text search (`ilike` on title/description), and projection. These
  refine the *already-synced* set incrementally (no re-sync when you type in the search box); only the
  priority sort is a small client-side step (a rank over a text enum, no integer column). See
  `src/lib/useShape.ts` and the [ARCHITECTURE client-query-layer section](../../docs/ARCHITECTURE.md#12-client-query-layer-two-level-querying).
- The Electric sync-engine columns (`deleted`, `new`, `synced`, …) and the offline write path are
  dropped — electric-circuits's ingestion *is* Postgres logical replication.

## Files

- `start.ts` — one-command boot: ephemeral Postgres + engine (Postgres mode) + API + Vite + the
  `/pg/write` middleware; faker seed.
- `src/schema.ts` — tables + the status/priority value sets and labels.
- `src/electric.ts` — the client, write helpers (all writes go to Postgres), and shape definitions.
- `src/lib/useShape.ts` — the client query layer: `useShapeCollection` creates a shape and returns its
  live TanStack DB collection; `useShapeRows(def, build?, deps?)` runs a `useLiveQuery` over it,
  pushing sort/filter/search into the query.
- `src/components/` — `Sidebar`, `TopFilter`, `IssueList`, `Board`, `IssueDetail`, `IssueModal`, and
  shared `ui` (status/priority icons, avatars, menus).
