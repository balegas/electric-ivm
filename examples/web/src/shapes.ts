import type { ShapeMaterialization } from '@electric-ivm/client'
import { client, LIVE_SHAPE } from './electric'

export interface Shapes {
  all: ShapeMaterialization // match-all: every todo (the editable list)
  live: ShapeMaterialization // the filtered, engine-evaluated shape
}

// Created once and cached, so React re-renders / HMR don't register duplicate engine shapes.
let cache: Promise<Shapes> | null = null
export function getShapes(): Promise<Shapes> {
  if (!cache) {
    cache = (async () => {
      const all = await client.shape({ table: 'todos' })
      const live = await client.shape(LIVE_SHAPE)
      return { all, live }
    })()
  }
  return cache
}
