import { describe, expect, it } from 'vitest'
import {
  changeEventToDML,
  evaluate,
  type Predicate,
  PredicateError,
  predicateToSql,
  type Row,
  shapeSelectSql,
  tableDDL,
  type TableDef,
  validatePredicate,
} from './index.js'

const users: TableDef = {
  columns: {
    id: { type: 'int' },
    name: { type: 'text' },
    age: { type: 'int' },
    active: { type: 'bool' },
    score: { type: 'float' },
  },
  primaryKey: 'id',
}

const alice: Row = { id: 1, name: 'Alice', age: 30, active: true, score: 9.5 }
const bob: Row = { id: 2, name: 'Bob', age: 17, active: false, score: 3.2 }

describe('evaluate', () => {
  it('handles equality leaves', () => {
    expect(evaluate({ col: 'name', op: 'eq', value: 'Alice' }, alice)).toBe(true)
    expect(evaluate({ col: 'name', op: 'eq', value: 'Alice' }, bob)).toBe(false)
  })

  it('handles comparison ops', () => {
    expect(evaluate({ col: 'age', op: 'gte', value: 18 }, alice)).toBe(true)
    expect(evaluate({ col: 'age', op: 'gte', value: 18 }, bob)).toBe(false)
    expect(evaluate({ col: 'age', op: 'lt', value: 18 }, bob)).toBe(true)
    expect(evaluate({ col: 'name', op: 'neq', value: 'Alice' }, bob)).toBe(true)
  })

  it('handles and/or/not', () => {
    const p: Predicate = {
      and: [
        { col: 'active', op: 'eq', value: true },
        { or: [{ col: 'age', op: 'gt', value: 25 }, { col: 'score', op: 'gt', value: 100 }] },
      ],
    }
    expect(evaluate(p, alice)).toBe(true)
    expect(evaluate(p, bob)).toBe(false)
    expect(evaluate({ not: { col: 'active', op: 'eq', value: true } }, bob)).toBe(true)
  })

  it('treats missing/null cells as non-matching', () => {
    expect(evaluate({ col: 'missing', op: 'eq', value: 1 }, alice)).toBe(false)
    expect(evaluate({ col: 'name', op: 'eq', value: null }, { name: null })).toBe(false)
  })
})

describe('validatePredicate', () => {
  it('accepts valid predicates', () => {
    expect(() => validatePredicate({ col: 'age', op: 'gt', value: 18 }, users)).not.toThrow()
  })
  it('rejects unknown columns', () => {
    expect(() => validatePredicate({ col: 'nope', op: 'eq', value: 1 }, users)).toThrow(PredicateError)
  })
  it('rejects type mismatches', () => {
    expect(() => validatePredicate({ col: 'age', op: 'eq', value: 'x' }, users)).toThrow(PredicateError)
    expect(() => validatePredicate({ col: 'name', op: 'eq', value: 5 }, users)).toThrow(PredicateError)
  })
})

describe('predicateToSql', () => {
  it('compiles a leaf with a parameter', () => {
    const f = predicateToSql({ col: 'name', op: 'eq', value: 'Alice' })
    expect(f.text).toBe('"name" = $1')
    expect(f.params).toEqual(['Alice'])
  })
  it('compiles nested and/or/not with sequential params', () => {
    const p: Predicate = {
      and: [
        { col: 'active', op: 'eq', value: true },
        { or: [{ col: 'age', op: 'gt', value: 25 }, { not: { col: 'name', op: 'eq', value: 'Bob' } }] },
      ],
    }
    const f = predicateToSql(p)
    expect(f.text).toBe('("active" = $1 AND ("age" > $2 OR (NOT "name" = $3)))')
    expect(f.params).toEqual([true, 25, 'Bob'])
  })
})

describe('tableDDL', () => {
  it('emits CREATE TABLE with a primary key', () => {
    const ddl = tableDDL('users', users)
    expect(ddl).toContain('CREATE TABLE "users"')
    expect(ddl).toContain('"id" INTEGER')
    expect(ddl).toContain('"score" DOUBLE PRECISION')
    expect(ddl).toContain('"active" BOOLEAN')
    expect(ddl).toContain('PRIMARY KEY ("id")')
  })
})

describe('changeEventToDML', () => {
  it('upserts on insert/update', () => {
    const f = changeEventToDML('users', users, { op: 'insert', pk: 1, row: alice })
    expect(f.text).toContain('INSERT INTO "users"')
    expect(f.text).toContain('ON CONFLICT ("id") DO UPDATE SET')
    expect(f.text).toContain('"name" = EXCLUDED."name"')
    expect(f.params).toEqual([1, 'Alice', 30, true, 9.5])
  })
  it('deletes by pk', () => {
    const f = changeEventToDML('users', users, { op: 'delete', pk: 2 })
    expect(f.text).toBe('DELETE FROM "users" WHERE "id" = $1')
    expect(f.params).toEqual([2])
  })
})

describe('shapeSelectSql', () => {
  it('selects all without a predicate', () => {
    expect(shapeSelectSql('users').text).toBe('SELECT * FROM "users"')
  })
  it('selects with a where clause', () => {
    const f = shapeSelectSql('users', { col: 'active', op: 'eq', value: true })
    expect(f.text).toBe('SELECT * FROM "users" WHERE "active" = $1')
    expect(f.params).toEqual([true])
  })
})
