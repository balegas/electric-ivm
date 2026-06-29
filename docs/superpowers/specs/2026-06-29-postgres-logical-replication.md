# Postgres logical replication + Postgres-backed state (design)

Status: in progress (2026-06-29)

## Goal

Make Postgres the system of record. Replace the engine's in-memory `table_state` with:
1. a **logical-replication ingestor** that turns Postgres row changes into the engine's change stream, and
2. **query-back to Postgres** for shape backfill.

Keep table/replication configuration simple, document deployment, keep all oracle tests green, and
expand them to cover the new scenarios.

## Why this works

`table_state` exists for exactly two jobs (see ARCHITECTURE.md §4): computing deltas (needs the *old*
row on update/delete) and backfilling new shapes. Postgres logical decoding with
`REPLICA IDENTITY FULL` delivers **old + new** for every UPDATE/DELETE, and a `SELECT … WHERE pred`
serves backfill. So both jobs move to Postgres and `table_state` is removed.

Verified locally: an ephemeral PG 16 with `wal_level=logical`, `REPLICA IDENTITY FULL`, and the
built-in `test_decoding` plugin emits, via `pg_logical_slot_get_changes`:
```
UPDATE: old-key: id[integer]:1 tenant[integer]:7 name[text]:'a' new-tuple: id[integer]:1 ... name[text]:'b'
DELETE: id[integer]:1 tenant[integer]:7 name[text]:'b'
```

## Architecture

```
   app/SQL ──writes──▶ Postgres ──WAL──▶ logical slot (test_decoding)
                          │                     │  poll pg_logical_slot_get_changes
                          │ SELECT WHERE pred   ▼
                          │ (backfill)     replication ingestor (engine)
                          │                     │  old+new envelopes
                          ▼                     ▼
                     (query-back) ◀── engine ── durable-streams table/<t> ──▶ tailer ──▶ shapes
```

- **Ingestor** (`apps/engine/src/replication.rs`): connects to PG (`tokio-postgres`), ensures the slot,
  polls `pg_logical_slot_get_changes` on an interval, parses `test_decoding` output into change
  envelopes carrying **old + new**, and appends them to the durable-streams `table/<t>` stream. The
  existing tailer/fan-out is unchanged. It tracks the last consumed LSN and exposes it for a barrier.
- **PG access** (`apps/engine/src/pg.rs`): connection, schema introspection (columns/types/pk from
  `information_schema`), and backfill `SELECT` with the predicate compiled to SQL.
- **Engine changes**: the table envelope gains an optional `old` value; `apply_envelope` uses it to
  build the delta (no `table_state` lookup); `table_state` is removed; shape backfill calls PG instead
  of replaying `table_state`.
- **Durable-streams stays** in the path (minimal change, preserves the offset convergence barrier).

We use built-in `test_decoding` (no extension to install). One slot decodes the whole database; the
ingestor routes rows to per-table streams.

## Configuration (kept simple)

Engine env:
- `ELECTRIC_LITE_PG_URL` — `postgres://user:pass@host:port/db`. Presence switches the engine into
  Postgres mode (ingestor + query-back; `table_state` disabled).
- `ELECTRIC_LITE_PG_TABLES` — comma-separated table names to replicate. The engine introspects their
  columns/types/primary key from Postgres (no separate schema needed).
- `ELECTRIC_LITE_PG_SLOT` — replication slot name (default `electric_lite`).
- `ELECTRIC_LITE_PG_POLL_MS` — slot poll interval (default 50).

On startup in PG mode the engine: introspects the tables, runs `ALTER TABLE … REPLICA IDENTITY FULL`,
creates the slot if absent (`pg_create_logical_replication_slot(slot,'test_decoding')`), and starts the
ingestor. Shapes are created/queried as before; backfill reads from PG.

## Consistency

A new shape's backfill must line up with the stream. The ingestor advances a single slot monotonically;
a shape registered at wall-clock T backfills with `SELECT … WHERE pred` and then receives all changes
the ingestor appends after that point. Because every change is also reflected in the base table the
SELECT reads, a row present at backfill time and later changed produces a redundant (idempotent) upsert,
and a row inserted concurrently arrives via the stream — no loss. (A strict snapshot-LSN handshake using
`CREATE_REPLICATION_SLOT … USE_SNAPSHOT` is a later hardening; for the single-writer test/oracle model
the monotonic-slot + idempotent-upsert approach converges.)

## Testing

The oracle becomes a **real Postgres** (`@electric-lite/oracle` gains a `pg` backend reusing the existing
`tableDDL`/`changeEventToDML`/`shapeSelectSql` from `@electric-lite/protocol`). The harness boots one
ephemeral PG, points the oracle (writes + SELECT) and the engine (ingestor + backfill) at it. `applyOp`
writes to PG only; the change flows PG→slot→ingestor→stream→engine→shape. The drain barrier waits until
the ingestor's LSN ≥ PG's `pg_current_wal_lsn()` and then the engine offset ≥ stream tail. Comparison is
unchanged (engine/client shape set vs `oracle.queryShape`).

New scenarios to add: update moving a row across a shape boundary (enter/leave via old+new), delete by
replication, NULL transitions via replication, backfill of pre-existing rows, multi-statement
transactions, and a negative control (ingestor dropping `old` → leave-detection breaks).

## Deployment

Documented in `docs/deployment-postgres.md`: `wal_level=logical`, `max_replication_slots/wal_senders`, a
role with `REPLICATION`, `REPLICA IDENTITY FULL` on replicated tables (engine does this), and the engine
env above. Slot lifecycle caveats (unconsumed slot pins WAL) included.
