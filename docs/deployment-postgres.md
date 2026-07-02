# Deploying electric-lite with Postgres

electric-lite can run **Postgres as the system of record**: your application writes to Postgres
normally, and electric-lite keeps every declared shape (a filtered view of a table) live by ingesting
changes from Postgres **logical replication** and reading rows back from Postgres for backfill. There
is no separate write API and no in-memory table copy to keep in sync — Postgres is the source of truth.

```
  app ──writes──▶  Postgres  ──logical replication──▶  engine  ──append──▶  durable-streams
                     ▲                                   │                    (shape/<id>)
                     └──────────── backfill SELECT ──────┘                         │
                                                                                   ▼
                                                                          client (live rows)
```

## What you need

- **Postgres 10+** with logical decoding (the built-in `test_decoding` plugin — no extensions to
  install). Managed Postgres works if it allows `wal_level = logical` and a logical replication slot
  (RDS, Cloud SQL, Supabase, Neon, etc. all do).
- **A durable-streams server** (the transport/persistence layer). Set its base URL in
  `ELECTRIC_LITE_DS_URL`.
- **The engine binary** (`apps/engine`, Rust): `cargo build -p electric-lite-engine --release` →
  `target/release/electric-lite-engine`.

## Step 1 — Configure Postgres

Logical replication must be on. In `postgresql.conf` (or your provider's parameter group):

```conf
wal_level = logical
max_replication_slots = 10     # ≥ number of engine instances
max_wal_senders = 10
```

Then restart Postgres (the `wal_level` change requires a restart).

The engine sets everything else up for you on startup, per configured table:

- `ALTER TABLE <t> REPLICA IDENTITY FULL` — so an UPDATE/DELETE carries the **full old row** (needed to
  compute the exact delta). The role you connect with must own the tables (or be superuser) for this.
- `pg_create_logical_replication_slot('<slot>', 'test_decoding')` — the replication slot, created once
  and reused.

> **Each table needs a single-column primary key.** The engine introspects columns, types, and the pk
> from the catalog; composite primary keys are not supported.

> **One slot per engine instance.** Replication-slot names are unique across the whole Postgres
> instance. If you run more than one engine against the same database, give each a distinct
> `ELECTRIC_LITE_PG_SLOT`.

## Step 2 — Run the engine

Point it at Postgres, list the tables to watch, and give it the durable-streams URL:

```sh
export ELECTRIC_LITE_DS_URL="https://streams.internal:8080"
export ELECTRIC_LITE_PG_URL="postgres://user:pass@db.internal:5432/appdb"
export ELECTRIC_LITE_PG_TABLES="users,projects,tasks"
export ELECTRIC_LITE_BIND="0.0.0.0:9000"

./electric-lite-engine
```

On startup the engine introspects each table, sets `REPLICA IDENTITY FULL`, ensures the slot, starts
the replication ingestor, and begins serving the control API on `ELECTRIC_LITE_BIND`. It prints
`ENGINE_LISTENING <addr>` once ready.

### Configuration reference

| Variable                  | Required | Default          | Meaning |
|---------------------------|:--------:|------------------|---------|
| `ELECTRIC_LITE_DS_URL`    | yes      | —                | durable-streams base URL. |
| `ELECTRIC_LITE_PG_URL`    | yes¹     | —                | Postgres connection string. Setting it enables Postgres mode. |
| `ELECTRIC_LITE_PG_TABLES` | yes¹     | (empty)          | Comma-separated tables to watch (in schema `public`). |
| `ELECTRIC_LITE_PG_SLOT`   | no       | `electric_lite`  | Logical replication slot name (unique per engine). |
| `ELECTRIC_LITE_PG_POLL_MS`| no       | `50`             | How often (ms) to poll the slot for new changes. |
| `ELECTRIC_LITE_BIND`      | no       | `127.0.0.1:0`    | Address for the control/HTTP API. |
| `ELECTRIC_LITE_LOG`       | no       | `info`           | Log filter (`error`, `warn`, `info`, `debug`). |

¹ Omit `ELECTRIC_LITE_PG_URL` to run in library/no-source mode (shapes start empty; used by tests).

## Step 3 — Connect the client

The client subscribes to shapes over the engine's API and materializes them with TanStack DB.
Writes go to **Postgres**, not the client:

```ts
import { createClient } from '@electric-lite/client'

const client = createClient({ apiUrl: 'http://engine.internal:9000', schema })

// Declare a shape; rows stay live as Postgres changes.
const activeUsers = await client.shape({
  table: 'users',
  where: { col: 'active', op: 'eq', value: true },
})

activeUsers.subscribe((rows) => render(rows))

// To change data, write to Postgres however you already do (psql, your ORM, etc.):
//   UPDATE users SET active = false WHERE id = 42;
// electric-lite picks it up via logical replication and updates the shape.
```

## Operating notes

- **Adding a table:** add it to `ELECTRIC_LITE_PG_TABLES` and restart the engine. It will introspect
  and set replica identity on the new table at startup.
- **Replication slot lag:** an engine that is stopped for a long time holds its slot, and Postgres
  retains WAL for it. If you decommission an engine, drop its slot:
  `SELECT pg_drop_replication_slot('<slot>');` Monitor `pg_replication_slots.confirmed_flush_lsn` vs
  `pg_current_wal_lsn()` to watch lag.
- **Consistency:** on shape registration the engine takes a `REPEATABLE READ` snapshot of the
  matching rows (the backfill) and, atomically with it, captures the snapshot's
  `pg_current_snapshot()` — the **snapshot gate**. Each replicated change is stamped with its
  transaction's **commit LSN, xid, and in-transaction position**, and the engine skips a change iff
  its xid was **visible to the backfill snapshot** (already in the seed); everything else is taken
  from the live stream. Visibility — not WAL position — is the fence because a commit's WAL record
  exists before the transaction becomes snapshot-visible; an LSN-only comparison would drop rows in
  that window. Ingest delivery is at-least-once (append, then advance the slot), and the engine
  de-duplicates by `(commit LSN, position)`, so each change takes effect exactly once. This assumes a
  single ingestor per database (the model above). Running multiple ingestors over the same tables is
  not supported.
- **Degraded forms are loud:** if a table's `REPLICA IDENTITY` is reset from `FULL` (e.g. a migration
  recreated it), updates lose their old image and deletes their tuple — the engine logs errors and
  tells you to restore identity + recreate shapes. `TRUNCATE` is not propagated (also logged);
  recreate shapes after one.
- **Permissions:** the engine's Postgres role needs `SELECT` on the watched tables, ownership (for
  `ALTER TABLE … REPLICA IDENTITY`), and the `REPLICATION` attribute (to create/read the slot).
