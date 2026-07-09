// The off-path fade set (`fadedElements`) that drives the trace animation's "dim everything not on
// the delta's path" treatment. The decor's node/edge maps are keyed by exactly the INVOLVED (lit)
// ids, so the faded set is their complement over the rendered graph. These tests pin that
// complement, the empty-decor short-circuit, and — the load-bearing case — that a decoration MERGED
// from two concurrent deltas keeps every element touched by EITHER out of the faded set.

import { describe, expect, it } from 'vitest'

import { type Decor, type EdgePulse, type NodeFlash, fadedElements, mergeDecor } from './trace-anim'

const flash = (): NodeFlash => ({ kind: 'pass', delayMs: 0 })
const pulse = (id: number): EdgePulse => ({ id, color: '#000', label: '+1', delayMs: 0, durMs: 750 })

/** A decor lighting the given node/edge ids (flash/pulse detail is irrelevant to fading). */
function decorOf(nodeIds: string[], edgeIds: string[], id = 1): Decor {
  return {
    nodes: new Map(nodeIds.map((n) => [n, flash()])),
    edges: new Map(edgeIds.map((e) => [e, pulse(id)])),
    id,
    totalMs: 750,
  }
}

describe('fadedElements', () => {
  const allNodes = ['a', 'b', 'c', 'd']
  const allEdges = ['a~b', 'b~c', 'c~d']

  it('fades every element NOT in the decor (the complement of the lit path)', () => {
    const decor = decorOf(['a', 'b'], ['a~b'])
    const faded = fadedElements(decor, allNodes, allEdges)
    expect([...faded.nodes].sort()).toEqual(['c', 'd'])
    expect([...faded.edges].sort()).toEqual(['b~c', 'c~d'])
  })

  it('fades nothing when there is no active decoration', () => {
    const faded = fadedElements(null, allNodes, allEdges)
    expect(faded.nodes.size).toBe(0)
    expect(faded.edges.size).toBe(0)
  })

  it('fades nothing when the whole graph is lit', () => {
    const decor = decorOf(allNodes, allEdges)
    const faded = fadedElements(decor, allNodes, allEdges)
    expect(faded.nodes.size).toBe(0)
    expect(faded.edges.size).toBe(0)
  })

  it('ignores lit ids that are not in the rendered graph (other selections)', () => {
    // A decor may reference nodes that the current view does not render; those simply never appear
    // in the faded set, and every rendered id stays faded.
    const decor = decorOf(['x', 'y'], ['x~y'])
    const faded = fadedElements(decor, allNodes, allEdges)
    expect([...faded.nodes].sort()).toEqual(allNodes)
    expect([...faded.edges].sort()).toEqual([...allEdges].sort())
  })

  it('keeps elements lit by EITHER of two merged concurrent deltas out of the faded set', () => {
    // Two overlapping animations: one lights a→b, the other c→d. After merging, the lit path is the
    // union, so ONLY elements in neither (here: none of the nodes; the middle edge b~c) fade.
    const merged = mergeDecor(decorOf(['a', 'b'], ['a~b'], 1), decorOf(['c', 'd'], ['c~d'], 2))
    const faded = fadedElements(merged, allNodes, allEdges)
    expect(faded.nodes.size).toBe(0)
    expect([...faded.edges]).toEqual(['b~c'])
  })
})
