import type { Schema } from '@electric-lite/protocol'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { createOracle, type Oracle } from './index.js'

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

describe('oracle', () => {
  let oracle: Oracle
  beforeAll(async () => {
    oracle = await createOracle(schema)
  })
  afterAll(async () => {
    await oracle.close()
  })

  it('upserts and filters rows', async () => {
    await oracle.applyChange('users', { op: 'insert', pk: 1, row: { id: 1, name: 'Alice', age: 30, active: true, score: 9.5 } })
    await oracle.applyChange('users', { op: 'insert', pk: 2, row: { id: 2, name: 'Bob', age: 17, active: false, score: 3.2 } })
    await oracle.applyChange('users', { op: 'insert', pk: 3, row: { id: 3, name: 'Carol', age: 40, active: true, score: 7.1 } })

    const active = await oracle.queryShape({ table: 'users', where: { col: 'active', op: 'eq', value: true } })
    expect(active.map((r) => r.id).sort()).toEqual([1, 3])
    // types round-trip exactly
    expect(active.find((r) => r.id === 1)).toMatchObject({ name: 'Alice', age: 30, active: true, score: 9.5 })
  })

  it('update (upsert) moves a row in and out of a shape', async () => {
    // Bob becomes active -> enters the shape.
    await oracle.applyChange('users', { op: 'update', pk: 2, row: { id: 2, name: 'Bob', age: 18, active: true, score: 3.2 } })
    let active = await oracle.queryShape({ table: 'users', where: { col: 'active', op: 'eq', value: true } })
    expect(active.map((r) => r.id).sort()).toEqual([1, 2, 3])

    // Alice becomes inactive -> leaves the shape.
    await oracle.applyChange('users', { op: 'update', pk: 1, row: { id: 1, name: 'Alice', age: 30, active: false, score: 9.5 } })
    active = await oracle.queryShape({ table: 'users', where: { col: 'active', op: 'eq', value: true } })
    expect(active.map((r) => r.id).sort()).toEqual([2, 3])
  })

  it('delete removes a row', async () => {
    await oracle.applyChange('users', { op: 'delete', pk: 3 })
    const active = await oracle.queryShape({ table: 'users', where: { col: 'active', op: 'eq', value: true } })
    expect(active.map((r) => r.id)).toEqual([2])
  })

  it('comparison + boolean predicates match Postgres semantics', async () => {
    await oracle.reset()
    for (let i = 1; i <= 5; i++) {
      await oracle.applyChange('users', { op: 'insert', pk: i, row: { id: i, name: `u${i}`, age: 10 * i, active: i % 2 === 0, score: i } })
    }
    const rows = await oracle.queryShape({
      table: 'users',
      where: { and: [{ col: 'age', op: 'gte', value: 20 }, { or: [{ col: 'active', op: 'eq', value: true }, { col: 'score', op: 'gt', value: 4 }] }] },
    })
    // age>=20 -> ids 2,3,4,5 ; AND (active(2,4) OR score>4(5)) -> ids 2,4,5
    expect(rows.map((r) => r.id).sort()).toEqual([2, 4, 5])
  })
})
