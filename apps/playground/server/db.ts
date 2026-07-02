// Postgres access for the playground server: table bootstrap (data + meta), id minting, and the
// tiny query helpers everything else shares. Data tables (restaurants, orders) are replicated into
// the engine and carry workspace_id on every row; playground_* meta tables are server bookkeeping
// only and are never part of the engine's table list.

import pgpkg from 'pg'

export type Db = pgpkg.Pool

export function createDb(connectionString: string): Db {
  return new pgpkg.Pool({ connectionString, max: 8 })
}

/** Create the playground's tables if missing. Data tables get REPLICA IDENTITY FULL (the engine
 *  needs old+new tuples); meta tables don't need it. Idempotent — safe on every boot. */
export async function ensureTables(db: Db): Promise<void> {
  await db.query(`CREATE TABLE IF NOT EXISTS restaurants (
    id BIGINT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    name TEXT NOT NULL,
    emoji TEXT NOT NULL,
    city TEXT NOT NULL
  )`)
  await db.query(`CREATE TABLE IF NOT EXISTS orders (
    id BIGINT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    restaurant_id BIGINT NOT NULL,
    status TEXT NOT NULL,
    dish TEXT NOT NULL,
    total DOUBLE PRECISION NOT NULL
  )`)
  for (const t of ['restaurants', 'orders']) {
    await db.query(`ALTER TABLE "${t}" REPLICA IDENTITY FULL`)
  }
  await db.query(`CREATE TABLE IF NOT EXISTS playground_workspaces (
    id TEXT PRIMARY KEY,
    epoch INT NOT NULL,
    created_at BIGINT NOT NULL,
    last_seen BIGINT NOT NULL
  )`)
  await db.query(`CREATE TABLE IF NOT EXISTS playground_shapes (
    shape_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    scene INT,
    skey TEXT,
    role TEXT NOT NULL,
    label TEXT NOT NULL,
    spec JSONB NOT NULL,
    where_json JSONB NOT NULL
  )`)
  await seedIds(db)
}

// Sequential id minting, seeded from MAX(id) at boot (single server instance). Small ids keep the
// UI readable and fit int4 columns (the conformance harness's DDL maps 'int' to INTEGER).
let nextId = 1
export function mintId(): number {
  return nextId++
}

/** Seed the id counter past anything already in the data tables. Called from ensureTables. */
export async function seedIds(db: Db): Promise<void> {
  const r = await db.query(
    'SELECT GREATEST((SELECT COALESCE(MAX(id),0) FROM restaurants), (SELECT COALESCE(MAX(id),0) FROM orders)) AS m',
  )
  nextId = Number(r.rows[0].m) + 1
}

/** BIGINT columns come back from pg as strings; normalize the rows we hand to clients. */
export function num(v: unknown): number {
  return typeof v === 'string' ? Number(v) : (v as number)
}
