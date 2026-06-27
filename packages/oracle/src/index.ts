// pglite-backed oracle: a real Postgres that receives the same change events as electric-lite
// and answers `SELECT * WHERE <predicate>` for any shape. The conformance invariant is that
// electric-lite's materialized shape set equals this oracle's result set for the same op stream.

import { PGlite } from '@electric-sql/pglite'
import {
  type ChangeEvent,
  changeEventToDML,
  type Row,
  type Schema,
  type ShapeDef,
  shapeSelectSql,
  tableDDL,
} from '@electric-lite/protocol'

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
