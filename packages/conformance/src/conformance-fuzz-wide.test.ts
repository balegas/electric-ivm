// Heavier, wider fuzz: deeper predicate trees (depth up to 4), edge/boundary literals, and
// occasional empty combinators, over more shapes and a longer op stream than the default fuzz.
// Deterministic (fixed base seed) so it runs in CI and replays exactly; env tunables scale it.

import type { Schema } from '@electric-circuits/protocol'
import { describe, expect, it } from 'vitest'
import { formatCompare } from './compare.js'
import { applyOp, bootHarness, drainEngine, waitForConvergence } from './harness.js'
import { createSimulator, randomShapeDefs } from './simulator.js'

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

const SEEDS = Number(process.env.WIDE_FUZZ_SEEDS ?? 3)
const SHAPES = Number(process.env.WIDE_FUZZ_SHAPES ?? 24)
const OPS = Number(process.env.WIDE_FUZZ_OPS ?? 500)

describe('conformance fuzz (wide): deep predicates + edge literals vs oracle', () => {
  it(
    `holds the oracle invariant across ${SEEDS} wide scenarios (${SHAPES} shapes, ${OPS} ops)`,
    async () => {
      const base = process.env.SEED ? Number(process.env.SEED) : 90210
      for (let i = 0; i < SEEDS; i++) {
        const seed = base + i * 7919
        const h = await bootHarness(schema)
        try {
          const defs = randomShapeDefs(schema, seed, SHAPES, {
            maxDepth: 4,
            edgeLiterals: true,
            emptyCombinators: true,
          })
          const shapes = await Promise.all(defs.map((d) => h.client.shape(d)))

          // Smaller pk space than the default fuzz -> more churn/overlap per key.
          for (const { table, ev } of createSimulator(schema, { seed, pkSpace: 16 }).take(OPS)) {
            await applyOp(h, table, ev)
          }
          await drainEngine(h)

          for (let s = 0; s < defs.length; s++) {
            const def = defs[s]!
            const res = await waitForConvergence(h, { shape: shapes[s]!, def, columns: Object.keys(schema.tables.users!.columns), pk: 'id' })
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
