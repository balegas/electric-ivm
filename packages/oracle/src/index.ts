// pglite-backed oracle: a real Postgres that receives the same change events as electric-ivm
// and answers `SELECT * WHERE <predicate>` for any shape. The conformance invariant is that
// electric-ivm's materialized shape set equals this oracle's result set for the same op stream.

import { PGlite } from '@electric-sql/pglite'
import {
  type ChangeEvent,
  changeEventToDML,
  type Row,
  type Schema,
  type ShapeDef,
  shapeSelectSql,
  tableDDL,
} from '@electric-ivm/protocol'
import pgpkg from 'pg'

export interface Oracle {
  /** Apply a single change to `table` (upsert on insert/update, delete by pk). */
  applyChange(table: string, ev: ChangeEvent): Promise<void>
  /** Current result set of a shape: `SELECT * FROM table WHERE <where>`. */
  queryShape(shape: ShapeDef): Promise<Row[]>
  /** Drop all rows from every table (keeps the schema). */
  reset(): Promise<void>
  close(): Promise<void>
}

export async function createOracle(schema: Schema): Promise<Oracle> {
  const db = await PGlite.create('memory://')
  for (const [name, def] of Object.entries(schema.tables)) {
    await db.exec(`${tableDDL(name, def)};`)
  }

  return {
    async applyChange(table, ev) {
      const def = schema.tables[table]
      if (!def) throw new Error(`oracle: unknown table "${table}"`)
      const { text, params } = changeEventToDML(table, def, ev)
      await db.query(text, params)
    },

    async queryShape(shape) {
      const def = schema.tables[shape.table]
      if (!def) throw new Error(`oracle: unknown table "${shape.table}"`)
      const { text, params } = shapeSelectSql(shape.table, shape.where)
      const res = await db.query<Row>(text, params)
      return res.rows
    },

    async reset() {
      for (const name of Object.keys(schema.tables)) {
        await db.exec(`TRUNCATE "${name.replace(/"/g, '""')}" RESTART IDENTITY CASCADE;`)
      }
    },

    async close() {
      await db.close()
    },
  }
}

// --- Real Postgres backend -------------------------------------------------------------------
// Used by the Postgres-mode conformance harness: the *same* Postgres is the write source (changes
// flow source -> logical replication -> engine) and the comparison oracle (SELECT ... WHERE pred).

/** Create the schema's tables in Postgres with `REPLICA IDENTITY FULL` (so logical decoding carries
 * the full old row). Run before starting the engine. */
export async function createPgTables(connectionString: string, schema: Schema): Promise<void> {
  const client = new pgpkg.Client({ connectionString })
  await client.connect()
  try {
    for (const [name, def] of Object.entries(schema.tables)) {
      await client.query(`${tableDDL(name, def)};`)
      await client.query(`ALTER TABLE "${name.replace(/"/g, '""')}" REPLICA IDENTITY FULL;`)
    }
  } finally {
    await client.end()
  }
}

/** A Postgres-backed oracle: applies changes as real DML (the replication source) and answers shape
 * queries with `SELECT … WHERE pred` (the comparison truth). */
export async function createPgOracle(schema: Schema, connectionString: string): Promise<Oracle> {
  const client = new pgpkg.Client({ connectionString })
  await client.connect()

  return {
    async applyChange(table, ev) {
      const def = schema.tables[table]
      if (!def) throw new Error(`oracle: unknown table "${table}"`)
      const { text, params } = changeEventToDML(table, def, ev)
      await client.query(text, params)
    },

    async queryShape(shape) {
      const def = schema.tables[shape.table]
      if (!def) throw new Error(`oracle: unknown table "${shape.table}"`)
      const { text, params } = shapeSelectSql(shape.table, shape.where)
      const res = await client.query(text, params)
      return res.rows as Row[]
    },

    async reset() {
      for (const name of Object.keys(schema.tables)) {
        await client.query(`TRUNCATE "${name.replace(/"/g, '""')}" RESTART IDENTITY CASCADE;`)
      }
    },

    async close() {
      await client.end()
    },
  }
}
