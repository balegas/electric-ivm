import type { Row, Schema } from '@electric-circuits/protocol'
import { describe, expect, it } from 'vitest'
import { compareShapeSets } from './compare.js'
import { createSimulator } from './simulator.js'

const schema: Schema = {
  tables: {
    users: {
      columns: { id: { type: 'int' }, name: { type: 'text' }, active: { type: 'bool' } },
      primaryKey: 'id',
    },
  },
}

describe('simulator determinism', () => {
  it('replays an identical op stream for the same seed', () => {
    const a = createSimulator(schema, { seed: 12345 }).take(200)
    const b = createSimulator(schema, { seed: 12345 }).take(200)
    expect(b).toEqual(a)
  })
  it('produces a different stream for a different seed', () => {
    const a = createSimulator(schema, { seed: 1 }).take(50)
    const b = createSimulator(schema, { seed: 2 }).take(50)
    expect(b).not.toEqual(a)
  })
  it('only emits known tables and valid ops', () => {
    for (const { table, ev } of createSimulator(schema, { seed: 7 }).take(100)) {
      expect(table).toBe('users')
      expect(['insert', 'update', 'delete']).toContain(ev.op)
      if (ev.op !== 'delete') expect(ev.row).toBeDefined()
    }
  })
})

describe('compareShapeSets', () => {
  const cols = ['id', 'name', 'active']
  // oracle rows have numeric pk; client rows have stringified pk + virtual props.
  const oracle: Row[] = [
    { id: 1, name: 'alpha', active: true },
    { id: 2, name: 'bravo', active: false },
  ]
  const clientLike = (rows: Array<{ id: string; name: string; active: boolean }>): Row[] =>
    rows.map((r) => ({ ...r, $synced: true, $key: r.id, _seq: 0 }))

  it('treats stringified-pk + virtual-prop rows as equal', () => {
    const client = clientLike([
      { id: '1', name: 'alpha', active: true },
      { id: '2', name: 'bravo', active: false },
    ])
    expect(compareShapeSets(cols, 'id', oracle, client).equal).toBe(true)
  })

  it('detects a missing row', () => {
    const client = clientLike([{ id: '1', name: 'alpha', active: true }])
    const r = compareShapeSets(cols, 'id', oracle, client)
    expect(r.equal).toBe(false)
    expect(r.missing).toEqual(['2'])
  })

  it('detects an extra row', () => {
    const client = clientLike([
      { id: '1', name: 'alpha', active: true },
      { id: '2', name: 'bravo', active: false },
      { id: '3', name: 'charlie', active: true },
    ])
    expect(compareShapeSets(cols, 'id', oracle, client).extra).toEqual(['3'])
  })

  it('detects a value mismatch on a non-pk column', () => {
    const client = clientLike([
      { id: '1', name: 'alpha', active: false }, // active flipped
      { id: '2', name: 'bravo', active: false },
    ])
    const r = compareShapeSets(cols, 'id', oracle, client)
    expect(r.equal).toBe(false)
    expect(r.mismatched.map((m) => m.key)).toEqual(['1'])
  })
})
