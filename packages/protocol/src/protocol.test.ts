import { describe, expect, it } from 'vitest'
import {
  changeEventToDML,
  evaluate,
  type InSubqueryPredicate,
  isInSubquery,
  type Predicate,
  PredicateError,
  predicateToSql,
  type Row,
  type Schema,
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

  it('uses SQL three-valued logic for nulls', () => {
    const r: Row = { id: 1, name: null, age: null, active: true, score: 1 }
    // leaf over null -> UNKNOWN -> excluded (eq and neq alike)
    expect(evaluate({ col: 'name', op: 'eq', value: 'Alice' }, r)).toBe(false)
    expect(evaluate({ col: 'name', op: 'neq', value: 'Alice' }, r)).toBe(false)
    // NOT(eq) over null -> NOT UNKNOWN = UNKNOWN -> excluded (the fix; was true under two-valued)
    expect(evaluate({ not: { col: 'name', op: 'eq', value: 'Alice' } }, r)).toBe(false)
    // AND: TRUE AND UNKNOWN = UNKNOWN -> excluded
    expect(evaluate({ and: [{ col: 'active', op: 'eq', value: true }, { col: 'age', op: 'gt', value: 18 }] }, r)).toBe(false)
    // OR: TRUE OR UNKNOWN = TRUE -> included
    expect(evaluate({ or: [{ col: 'active', op: 'eq', value: true }, { col: 'age', op: 'gt', value: 18 }] }, r)).toBe(true)
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

describe('subqueries', () => {
  const schema: Schema = {
    tables: {
      parent: { columns: { id: { type: 'int' }, active: { type: 'bool' } }, primaryKey: 'id' },
      child: { columns: { id: { type: 'int' }, parent_id: { type: 'int' } }, primaryKey: 'id' },
    },
  }
  const sub: InSubqueryPredicate = {
    col: 'parent_id',
    in: { table: 'parent', project: 'id', where: { col: 'active', op: 'eq', value: true } },
  }

  it('recognizes an in-subquery leaf (and not as a plain leaf)', () => {
    expect(isInSubquery(sub)).toBe(true)
    expect(isInSubquery({ col: 'parent_id', op: 'eq', value: 1 } as Predicate)).toBe(false)
  })

  it('validates the inner where against the inner table', () => {
    expect(() => validatePredicate(sub, schema.tables.child!, schema)).not.toThrow()
    const badInnerCol: InSubqueryPredicate = {
      col: 'parent_id',
      in: { table: 'parent', project: 'id', where: { col: 'nope', op: 'eq', value: true } },
    }
    expect(() => validatePredicate(badInnerCol, schema.tables.child!, schema)).toThrow(PredicateError)
    const badProject: InSubqueryPredicate = { col: 'parent_id', in: { table: 'parent', project: 'nope' } }
    expect(() => validatePredicate(badProject, schema.tables.child!, schema)).toThrow(PredicateError)
    const badOuterCol: InSubqueryPredicate = { col: 'nope', in: { table: 'parent', project: 'id' } }
    expect(() => validatePredicate(badOuterCol, schema.tables.child!, schema)).toThrow(PredicateError)
    expect(() => validatePredicate(sub, schema.tables.child!)).toThrow(/requires a schema/)
  })

  it('evaluate() throws on a subquery (resolved via SQL, not the row evaluator)', () => {
    expect(() => evaluate(sub, { parent_id: 1 })).toThrow(/subquery/)
  })

  it('emits IN (SELECT …) SQL with parameterized inner literals', () => {
    const f = predicateToSql(sub, 1)
    expect(f.text).toBe('"parent_id" IN (SELECT "id" FROM "parent" WHERE "active" = $1)')
    expect(f.params).toEqual([true])
  })

  it('emits NOT IN and supports an omitted inner where', () => {
    const f = predicateToSql({ col: 'parent_id', negated: true, in: { table: 'parent', project: 'id' } }, 1)
    expect(f.text).toBe('"parent_id" NOT IN (SELECT "id" FROM "parent")')
    expect(f.params).toEqual([])
  })

  it('composes inside shapeSelectSql with surrounding params', () => {
    const where: Predicate = { and: [{ col: 'id', op: 'gt', value: 5 }, sub] }
    const f = shapeSelectSql('child', where)
    expect(f.text).toBe(
      'SELECT * FROM "child" WHERE ("id" > $1 AND "parent_id" IN (SELECT "id" FROM "parent" WHERE "active" = $2))',
    )
    expect(f.params).toEqual([5, true])
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
  it('upserts the full row on insert', () => {
    const f = changeEventToDML('users', users, { op: 'insert', pk: 1, row: alice })
    expect(f.text).toContain('INSERT INTO "users"')
    expect(f.text).toContain('ON CONFLICT ("id") DO UPDATE SET')
    expect(f.text).toContain('"name" = EXCLUDED."name"')
    expect(f.params).toEqual([1, 'Alice', 30, true, 9.5])
  })
  it('updates only the columns present in the row (partial patch)', () => {
    const f = changeEventToDML('users', users, { op: 'update', pk: 1, row: { name: 'Alicia', age: 31 } })
    expect(f.text).toBe('UPDATE "users" SET "name" = $1, "age" = $2 WHERE "id" = $3')
    expect(f.params).toEqual(['Alicia', 31, 1])
  })
  it('upserts when a full row is given (an update with a new pk inserts it — Electric semantics)', () => {
    const f = changeEventToDML('users', users, { op: 'update', pk: 1, row: alice })
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
