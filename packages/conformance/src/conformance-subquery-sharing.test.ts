// Node-sharing topology: shapes whose subquery references the *same* inner shape (same table +
// projection + where) must share ONE maintained node (the memory win the design calls for), regardless
// of the outer column or surrounding predicate. Asserted via the engine's GET /subqueries introspection
// (refcount == number of dependents), analogous to conformance-sharing.test.ts for equality families.

import type { Predicate, Schema, ShapeDef } from '@electric-ivm/protocol'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { bootHarness, drainEngine, type Harness } from './harness.js'

interface NodeStat { sig: string; inner_table: string; distinct_values: number; refcount: number }

async function subqueryNodes(h: Harness): Promise<NodeStat[]> {
  const res = await fetch(`${h.engineUrl}/subqueries`)
  const body = (await res.json()) as { nodes: NodeStat[] }
  return body.nodes
}

const schema: Schema = {
  tables: {
    parent: { columns: { id: { type: 'int' }, active: { type: 'bool' } }, primaryKey: 'id' },
    child: { columns: { id: { type: 'int' }, parent_id: { type: 'int' }, owner_id: { type: 'int' } }, primaryKey: 'id' },
  },
}

const activeParents = { table: 'parent', project: 'id', where: { col: 'active', op: 'eq', value: true } } as const
const inactiveParents = { table: 'parent', project: 'id', where: { col: 'active', op: 'eq', value: false } } as const

describe('conformance: subquery node sharing', () => {
  let h: Harness
  beforeAll(async () => { h = await bootHarness(schema) }, 60000)
  afterAll(async () => await h?.shutdown())

  it('identical subqueries share one node; distinct subqueries get their own', async () => {
    // Three shapes referencing the SAME inner subquery (active parents) via different outer columns /
    // surrounding predicates — all must dedupe to ONE node with refcount 3.
    const shared: Predicate[] = [
      { col: 'parent_id', in: activeParents },
      { col: 'owner_id', in: activeParents }, // different outer column, same inner shape
      { and: [{ col: 'parent_id', in: activeParents }, { col: 'id', op: 'gt', value: 0 }] }, // wrapped
    ]
    // One more shape with a DIFFERENT inner subquery (inactive parents) -> its own node.
    const distinct: Predicate = { col: 'parent_id', in: inactiveParents }

    const defs: ShapeDef[] = [...shared, distinct].map((where) => ({ table: 'child', where }))
    await Promise.all(defs.map((d) => h.client.shape(d)))
    await drainEngine(h)

    const nodes = await subqueryNodes(h)
    // Exactly two nodes: the shared active-parents node (refcount 3) and the inactive-parents node (1).
    expect(nodes.length).toBe(2)
    const byRef = [...nodes].sort((a, b) => b.refcount - a.refcount)
    expect(byRef[0]!.refcount).toBe(3)
    expect(byRef[1]!.refcount).toBe(1)
    // Both nodes are over the parent table.
    expect(nodes.every((n) => n.inner_table === 'parent')).toBe(true)
    // The two nodes have distinct signatures (active vs inactive).
    expect(new Set(nodes.map((n) => n.sig)).size).toBe(2)
  }, 60000)
})
