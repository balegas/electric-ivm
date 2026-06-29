import type { Row, ShapeDef } from '@electric-lite/protocol'
import { createCollection, type Collection } from '@tanstack/db'
import { useLiveQuery } from '@tanstack/react-db'
import { useEffect, useState } from 'react'
import { client } from '../electric'

// Always-ready, empty placeholder collection. While a shape's real collection is still being created
// (async), we query this instead, so `useLiveQuery` can be called unconditionally (rules of hooks)
// and simply returns no rows until the shape is ready. Referencing columns that don't exist on it is
// safe — order-by/where expressions are only evaluated against rows, and it has none.
const EMPTY = createCollection<Row>({
  id: '__el_empty__',
  getKey: (r) => r.id as string,
  sync: {
    sync: ({ markReady }) => {
      markReady()
    },
  },
})

/**
 * Create the engine-side shape for `def` and return its live TanStack DB collection (null while the
 * shape is being created, or when `def` is null). The shape is (re)created whenever `def` changes
 * (keyed by its JSON) and closed on unmount, so changing a filter swaps the engine-side predicate.
 */
export function useShapeCollection<T extends Row = Row>(def: ShapeDef | null): Collection<T, string> | null {
  const [collection, setCollection] = useState<Collection<T, string> | null>(null)
  const key = def ? JSON.stringify(def) : null

  useEffect(() => {
    if (!def) {
      setCollection(null)
      return
    }
    let closed = false
    let mat: Awaited<ReturnType<typeof client.shape>> | undefined
    setCollection(null)
    client.shape(def).then((m) => {
      if (closed) {
        void m.close()
        return
      }
      mat = m
      setCollection(m.collection as Collection<T, string>)
    })
    return () => {
      closed = true
      if (mat) void mat.close()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key])

  return collection
}

/** A live-query builder over the shape's collection, aliased as `t` (e.g. `b.orderBy(({t}) => t.x)`). */
// The query-builder generics are heavy; the callback uses `any` and the caller keeps `T` for the rows.
type ShapeQueryBuilder = any // eslint-disable-line @typescript-eslint/no-explicit-any

/**
 * Live rows of a shape, bound through TanStack DB's `useLiveQuery` (incrementally maintained — no
 * manual re-snapshot on every change). Sorting/filtering belong in the query, not in JS: pass `build`
 * to push `.where()/.orderBy()/.select()` into the live query. `build` may close over values; list
 * them in `deps` so the query re-runs when they change (the engine-side shape is unchanged — only the
 * client-side query is refined, which is how search/sort run without re-syncing).
 */
export function useShapeRows<T extends Row = Row>(
  def: ShapeDef | null,
  build?: (from: ShapeQueryBuilder) => ShapeQueryBuilder,
  deps: unknown[] = [],
): { rows: T[]; loading: boolean } {
  const collection = useShapeCollection<T>(def)
  const src = (collection ?? EMPTY) as unknown as Collection<T, string>
  const { data } = useLiveQuery(
    (q: ShapeQueryBuilder) => {
      const base = q.from({ t: src })
      return build ? build(base) : base.select(({ t }: { t: T }) => t)
    },
    // `src` identity gates readiness; `deps` carry whatever `build` closes over.
    [src, ...deps],
  )
  return { rows: (data ?? []) as T[], loading: def !== null && collection === null }
}
