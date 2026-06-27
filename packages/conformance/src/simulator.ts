// Deterministic, seeded op-stream simulator. A printed seed replays a run exactly, so a
// failing conformance run can be reproduced and shrunk. Ops are upsert/delete over a bounded
// pk space (so inserts/updates/deletes naturally overlap and exercise enter/leave/update).

import { en, Faker, generateMersenne53Randomizer } from '@faker-js/faker'
import type { ChangeEvent, Row, Schema, TableDef, Value } from '@electric-lite/protocol'

export interface SimOp {
  table: string
  ev: ChangeEvent
}

export interface SimulatorOptions {
  seed: number
  /** Size of the per-table primary-key space; smaller => more upserts/overlap. Default 24. */
  pkSpace?: number
  /** Relative weights for op selection. Defaults: insert 5, update 4, delete 2. */
  weights?: { insert: number; update: number; delete: number }
}

export interface Simulator {
  readonly seed: number
  next(): SimOp
  take(n: number): SimOp[]
}

export function createSimulator(schema: Schema, opts: SimulatorOptions): Simulator {
  const randomizer = generateMersenne53Randomizer()
  const f = new Faker({ locale: en, randomizer })
  f.seed(opts.seed)
  const pkSpace = opts.pkSpace ?? 24
  const weights = opts.weights ?? { insert: 5, update: 4, delete: 2 }
  const tableNames = Object.keys(schema.tables)

  function genValue(type: TableDef['columns'][string]['type']): Value {
    switch (type) {
      case 'int':
        return f.number.int({ min: 0, max: 1000 })
      case 'float':
        return f.number.float({ min: 0, max: 1000, fractionDigits: 4 })
      case 'text':
        return f.helpers.arrayElement(['alpha', 'bravo', 'charlie', 'delta', 'echo', 'foxtrot'])
      case 'bool':
        return f.datatype.boolean()
    }
  }

  function genRow(def: TableDef, pk: number): Row {
    const row: Row = {}
    for (const [col, c] of Object.entries(def.columns)) {
      row[col] = col === def.primaryKey ? pk : genValue(c.type)
    }
    return row
  }

  function next(): SimOp {
    const table = f.helpers.arrayElement(tableNames)
    const def = schema.tables[table]!
    const pk = f.number.int({ min: 1, max: pkSpace })
    const op = f.helpers.weightedArrayElement([
      { weight: weights.insert, value: 'insert' as const },
      { weight: weights.update, value: 'update' as const },
      { weight: weights.delete, value: 'delete' as const },
    ])
    if (op === 'delete') return { table, ev: { op: 'delete', pk } }
    return { table, ev: { op, pk, row: genRow(def, pk) } }
  }

  return {
    seed: opts.seed,
    next,
    take(n) {
      const out: SimOp[] = []
      for (let i = 0; i < n; i++) out.push(next())
      return out
    },
  }
}

/** Pick a fresh random seed (used when a test doesn't pin one). */
export function randomSeed(): number {
  const r = generateMersenne53Randomizer()
  const f = new Faker({ locale: en, randomizer: r })
  return f.seed()
}
