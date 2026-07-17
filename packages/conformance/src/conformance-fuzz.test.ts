// Oracle-driven property/fuzz test: the loop an agent iterates against. For each random seed
// it generates random-predicate shapes (eq/neq/lt/lte/gt/gte + and/or/not over the schema),
// applies a random op stream to electric-circuits AND pglite, and asserts every shape's
// client-materialized set equals the oracle. A failure prints the seed for exact replay.
//
// Tunables (env): FUZZ_SEEDS (scenarios per run), FUZZ_SHAPES, FUZZ_OPS, SEED (base seed).

import type { Schema } from '@electric-circuits/protocol'
import { describe, expect, it } from 'vitest'
import { formatCompare } from './compare.js'
import { applyOp, bootHarness, drainEngine, waitForConvergence } from './harness.js'
import { createSimulator, randomSeed, randomShapeDefs } from './simulator.js'

const schema: Schema = {
  tables: {
    users: {
      columns: {
        id: { type: 'int' },
        name: { type: 'text' },
        age: { type: 'int' },
        active: { type: 'bool' },
        score: { type: 'float' },
      },
      primaryKey: 'id',
    },
  },
}

const SEEDS = Number(process.env.FUZZ_SEEDS ?? 5)
const SHAPES = Number(process.env.FUZZ_SHAPES ?? 10)
const OPS = Number(process.env.FUZZ_OPS ?? 250)

describe('conformance fuzz: random predicates vs oracle', () => {
  it(
    `holds the oracle invariant across ${SEEDS} random scenarios`,
    async () => {
      const base = process.env.SEED ? Number(process.env.SEED) : randomSeed()
      for (let i = 0; i < SEEDS; i++) {
        const seed = base + i * 7919 // distinct, deterministic per scenario
        const h = await bootHarness(schema)
        try {
          const defs = randomShapeDefs(schema, seed, SHAPES)
          const shapes = await Promise.all(defs.map((d) => h.client.shape(d)))

          for (const { table, ev } of createSimulator(schema, { seed }).take(OPS)) {
            await applyOp(h, table, ev)
          }
          // Barrier: ensure the engine consumed the whole op stream before comparing, so an
          // empty-result shape can't pass by reading [] before the engine has done any work.
          await drainEngine(h)

          for (let s = 0; s < defs.length; s++) {
            const def = defs[s]!
            const tdef = schema.tables[def.table]!
            const res = await waitForConvergence(h, {
              shape: shapes[s]!,
              def,
              columns: Object.keys(tdef.columns),
              pk: tdef.primaryKey,
            })
            expect(
              res.equal,
              `FAILED seed=${seed} shape#${s}=${JSON.stringify(def.where ?? 'ALL')}\n${formatCompare(res)}`,
            ).toBe(true)
          }
        } finally {
          await h.shutdown()
        }
      }
    },
    300000,
  )
})
