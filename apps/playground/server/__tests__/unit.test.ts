// Pure unit tests: predicate composition (workspace scoping is ALWAYS present, including inside
// subquery inner wheres) and trace tagging/stripping (no cross-workspace leakage).

import { describe, expect, it } from 'vitest'

import { specToWhere } from '../shapes.ts'
import { tagAndStrip, type EngineTraceEvent } from '../trace.ts'

describe('specToWhere', () => {
  it('empty spec is just the workspace conjunct', () => {
    expect(specToWhere({ table: 'orders', where: [] }, 'w_a')).toEqual({
      col: 'workspace_id',
      op: 'eq',
      value: 'w_a',
    })
  })

  it('conjuncts AND the workspace conjunct last (honest display order: user predicate first)', () => {
    expect(specToWhere({ table: 'orders', where: [{ col: 'status', op: 'eq', value: 'cooking' }] }, 'w_a')).toEqual({
      and: [
        { col: 'status', op: 'eq', value: 'cooking' },
        { col: 'workspace_id', op: 'eq', value: 'w_a' },
      ],
    })
  })

  it('subquery inner where is workspace-scoped too', () => {
    const p = specToWhere(
      {
        table: 'orders',
        where: [],
        subquery: {
          col: 'restaurant_id',
          inner: { table: 'restaurants', project: 'id', where: [{ col: 'city', op: 'eq', value: 'Lisbon' }] },
        },
      },
      'w_a',
    )
    expect(p).toEqual({
      and: [
        {
          col: 'restaurant_id',
          in: {
            table: 'restaurants',
            project: 'id',
            where: {
              and: [
                { col: 'city', op: 'eq', value: 'Lisbon' },
                { col: 'workspace_id', op: 'eq', value: 'w_a' },
              ],
            },
          },
        },
        { col: 'workspace_id', op: 'eq', value: 'w_a' },
      ],
    })
  })
})

describe('tagAndStrip', () => {
  const owners = new Map([
    ['s1', 'w_me'],
    ['s2', 'w_other'],
  ])
  const ev: EngineTraceEvent = {
    lsn: '0/1',
    table: 'orders',
    delta: [{ row: { id: 1, workspace_id: 'w_me', total: 12 }, w: 1 }],
    hops: [
      { node: 'table:orders', outcome: 'passed' },
      { node: 'family:orders:status,workspace_id', outcome: 'routed', key: ['cooking', 'w_me'] },
      { node: 'shape:s1', outcome: 'passed' },
      { node: 'filter:s2', outcome: 'dropped' },
    ],
    shapes: ['s1'],
  }

  it('tags own events yours and keeps rows, stripping foreign shape hops', () => {
    const out = tagAndStrip(ev, 'w_me', owners)
    expect(out.yours).toBe(true)
    expect(out.delta[0]!.row).toHaveProperty('total', 12)
    expect(out.shapes).toEqual(['s1'])
    expect(out.hops.map((h) => h.node)).toEqual([
      'table:orders',
      'family:orders:status,workspace_id',
      'shape:s1',
    ])
  })

  it('strips foreign events to shared hops with rowless weights and no shapes', () => {
    // w_other owns s2, but s2 only appears as a dropped filter hop; the delta row belongs to
    // w_me, and no reached shape is w_other's -> the event is NOT theirs.
    const out = tagAndStrip(ev, 'w_other', owners)
    expect(out.yours).toBe(false)
    expect(out.delta).toEqual([{ row: {}, w: 1 }])
    expect(out.shapes).toEqual([])
    expect(out.hops.every((h) => !h.node.startsWith('shape:') && !h.node.startsWith('filter:'))).toBe(true)
    expect(out.hops.every((h) => h.key === undefined)).toBe(true)
  })
})
