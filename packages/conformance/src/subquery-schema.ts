// A multi-level relational schema + deterministic, FK-respecting mutation generator for subquery
// conformance, mirroring Electric's `level_1..4` (+ tag side-tables) oracle schema. The generator
// tracks current rows so it can emit valid upserts/deletes/toggles/re-parents (keeping foreign keys in
// the id space, so subquery membership actually changes); the SAME op stream drives the engine and the
// pg oracle, so any divergence is a real bug.

import type { ChangeEvent, Row, Schema } from '@electric-lite/protocol'

export interface SimOp {
  table: string
  ev: ChangeEvent
}

export const subquerySchema: Schema = {
  tables: {
    level_1: { columns: { id: { type: 'int' }, active: { type: 'bool' } }, primaryKey: 'id' },
    level_2: {
      columns: { id: { type: 'int' }, level_1_id: { type: 'int' }, active: { type: 'bool' } },
      primaryKey: 'id',
    },
    level_3: {
      columns: { id: { type: 'int' }, level_2_id: { type: 'int' }, active: { type: 'bool' } },
      primaryKey: 'id',
    },
    level_4: {
      columns: { id: { type: 'int' }, level_3_id: { type: 'int' }, value: { type: 'text' } },
      primaryKey: 'id',
    },
    level_1_tags: {
      columns: { id: { type: 'int' }, level_1_id: { type: 'int' }, tag: { type: 'text' } },
      primaryKey: 'id',
    },
    level_2_tags: {
      columns: { id: { type: 'int' }, level_2_id: { type: 'int' }, tag: { type: 'text' } },
      primaryKey: 'id',
    },
    level_3_tags: {
      columns: { id: { type: 'int' }, level_3_id: { type: 'int' }, tag: { type: 'text' } },
      primaryKey: 'id',
    },
  },
}

const TAGS = ['alpha', 'beta', 'gamma', 'delta']
const L1 = 4
const L2 = 6
const L3 = 8
const L4 = 12

/** A small deterministic RNG (mulberry32) so a seed replays a run exactly. */
function rng(seed: number): () => number {
  let a = seed >>> 0
  return () => {
    a |= 0
    a = (a + 0x6d2b79f5) | 0
    let t = Math.imul(a ^ (a >>> 15), 1 | a)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

interface State {
  level_1: Map<number, Row>
  level_2: Map<number, Row>
  level_3: Map<number, Row>
  level_4: Map<number, Row>
  level_1_tags: Map<number, Row>
  level_2_tags: Map<number, Row>
  level_3_tags: Map<number, Row>
  nextTagId: number
}

/** Build the deterministic initial state (and the ops that create it). */
export function seedOps(): { ops: SimOp[]; state: State } {
  const state: State = {
    level_1: new Map(),
    level_2: new Map(),
    level_3: new Map(),
    level_4: new Map(),
    level_1_tags: new Map(),
    level_2_tags: new Map(),
    level_3_tags: new Map(),
    nextTagId: 1,
  }
  const ops: SimOp[] = []
  const ins = (table: keyof State, row: Row) => {
    ;(state[table] as Map<number, Row>).set(row.id as number, row)
    ops.push({ table, ev: { op: 'insert', pk: row.id as number, row } })
  }
  for (let id = 1; id <= L1; id++) ins('level_1', { id, active: id % 2 === 1 })
  for (let id = 1; id <= L2; id++) ins('level_2', { id, level_1_id: ((id - 1) % L1) + 1, active: id % 2 === 0 })
  for (let id = 1; id <= L3; id++) ins('level_3', { id, level_2_id: ((id - 1) % L2) + 1, active: id % 2 === 1 })
  for (let id = 1; id <= L4; id++) ins('level_4', { id, level_3_id: ((id - 1) % L3) + 1, value: `v${id % 5}` })
  // a few seed tags at each level
  for (let i = 0; i < 6; i++) {
    const tid = state.nextTagId++
    ins('level_3_tags', { id: tid, level_3_id: (i % L3) + 1, tag: TAGS[i % TAGS.length]! })
  }
  for (let i = 0; i < 4; i++) {
    const tid = state.nextTagId++
    ins('level_2_tags', { id: tid, level_2_id: (i % L2) + 1, tag: TAGS[i % TAGS.length]! })
  }
  for (let i = 0; i < 3; i++) {
    const tid = state.nextTagId++
    ins('level_1_tags', { id: tid, level_1_id: (i % L1) + 1, tag: TAGS[i % TAGS.length]! })
  }
  return { ops, state }
}

/**
 * A deterministic mutation generator over the seeded state: toggles `active`, re-parents children to
 * valid parents, updates `level_4.value`, and adds/removes tags — all FK-valid so subquery membership
 * genuinely moves. Returns `take(n)` of `SimOp`s.
 */
export function subqueryMutations(state: State, seed: number) {
  const r = rng(seed)
  const pick = (n: number) => Math.floor(r() * n) + 1 // 1..n
  const choice = <T>(xs: T[]): T => xs[Math.floor(r() * xs.length)]!

  function toggleActive(table: 'level_1' | 'level_2' | 'level_3'): SimOp | null {
    const m = state[table]
    const id = pick(table === 'level_1' ? L1 : table === 'level_2' ? L2 : L3)
    const row = m.get(id)
    if (!row) return null
    const updated = { ...row, active: !(row.active as boolean) }
    m.set(id, updated)
    return { table, ev: { op: 'update', pk: id, row: updated } }
  }

  function reparent(table: 'level_2' | 'level_3' | 'level_4'): SimOp | null {
    const m = state[table]
    const id = pick(table === 'level_2' ? L2 : table === 'level_3' ? L3 : L4)
    const row = m.get(id)
    if (!row) return null
    const parentCol = table === 'level_2' ? 'level_1_id' : table === 'level_3' ? 'level_2_id' : 'level_3_id'
    const parentMax = table === 'level_2' ? L1 : table === 'level_3' ? L2 : L3
    const updated = { ...row, [parentCol]: pick(parentMax) }
    m.set(id, updated)
    return { table, ev: { op: 'update', pk: id, row: updated } }
  }

  function updateValue(): SimOp | null {
    const id = pick(L4)
    const row = state.level_4.get(id)
    if (!row) return null
    const updated = { ...row, value: `v${Math.floor(r() * 6)}` }
    state.level_4.set(id, updated)
    return { table: 'level_4', ev: { op: 'update', pk: id, row: updated } }
  }

  function addTag(table: 'level_1_tags' | 'level_2_tags' | 'level_3_tags'): SimOp {
    const parentCol = table === 'level_1_tags' ? 'level_1_id' : table === 'level_2_tags' ? 'level_2_id' : 'level_3_id'
    const parentMax = table === 'level_1_tags' ? L1 : table === 'level_2_tags' ? L2 : L3
    const id = state.nextTagId++
    const row = { id, [parentCol]: pick(parentMax), tag: choice(TAGS) }
    state[table].set(id, row)
    return { table, ev: { op: 'insert', pk: id, row } }
  }

  function removeTag(table: 'level_1_tags' | 'level_2_tags' | 'level_3_tags'): SimOp | null {
    const ids = [...state[table].keys()]
    if (ids.length === 0) return null
    const id = choice(ids)
    state[table].delete(id)
    return { table, ev: { op: 'delete', pk: id } }
  }

  function next(): SimOp {
    for (let attempt = 0; attempt < 8; attempt++) {
      const kind = Math.floor(r() * 9)
      let op: SimOp | null = null
      switch (kind) {
        case 0: op = toggleActive('level_1'); break
        case 1: op = toggleActive('level_2'); break
        case 2: op = toggleActive('level_3'); break
        case 3: op = reparent('level_2'); break
        case 4: op = reparent('level_3'); break
        case 5: op = reparent('level_4'); break
        case 6: op = updateValue(); break
        case 7: op = addTag(choice(['level_1_tags', 'level_2_tags', 'level_3_tags'])); break
        default: op = removeTag(choice(['level_1_tags', 'level_2_tags', 'level_3_tags'])); break
      }
      if (op) return op
    }
    // fallback: always succeeds
    return updateValue() ?? { table: 'level_4', ev: { op: 'update', pk: 1, row: state.level_4.get(1)! } }
  }

  return {
    next,
    take(n: number): SimOp[] {
      const out: SimOp[] = []
      for (let i = 0; i < n; i++) out.push(next())
      return out
    },
  }
}
