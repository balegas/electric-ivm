import type { AggregateSubscription, SubsetSubscription } from '@electric-circuits/client'
import type { AggregateDef, Row, ShapeDef, SubsetDef } from '@electric-circuits/protocol'
import { createCollection, type Collection } from '@tanstack/db'
import { useLiveQuery } from '@tanstack/react-db'
import { useCallback, useEffect, useRef, useState } from 'react'
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

/**
 * Live rows of a **subset query**: the first page is query-backed from Postgres, then the loaded
 * window is kept current by following the table's tail (no materialized page-shape). `loadMore` pages
 * by query-backing the next chunk; `hasMore` is false once the set is exhausted. The subscription is
 * (re)created whenever `def` changes (keyed by JSON) and closed on unmount — which also drops the
 * server-side feed. `build` refines the client-side live query (sort/select) like {@link useShapeRows}.
 */
export function useSubset<T extends Row = Row>(
  def: SubsetDef | null,
  build?: (from: ShapeQueryBuilder) => ShapeQueryBuilder,
  deps: unknown[] = [],
): { rows: T[]; loading: boolean; loadMore: () => void; hasMore: boolean } {
  const [sub, setSub] = useState<SubsetSubscription<T> | null>(null)
  const key = def ? JSON.stringify(def) : null

  useEffect(() => {
    if (!def) {
      setSub(null)
      return
    }
    let closed = false
    let s: SubsetSubscription<T> | undefined
    setSub(null)
    void client.subset(def).then((sb) => {
      if (closed) {
        void sb.close()
        return
      }
      s = sb as unknown as SubsetSubscription<T>
      setSub(s)
    })
    return () => {
      closed = true
      if (s) void s.close()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key])

  const collection = (sub?.collection ?? null) as Collection<T, string> | null
  const src = (collection ?? EMPTY) as unknown as Collection<T, string>
  const { data } = useLiveQuery(
    (q: ShapeQueryBuilder) => {
      const base = q.from({ t: src })
      return build ? build(base) : base.select(({ t }: { t: T }) => t)
    },
    [src, ...deps],
  )

  // One page in flight at a time: `onEndReached` fires on every rows-length change while the tail
  // streams rows in, so an unguarded loadMore fans out into dozens of duplicate same-boundary page
  // fetches (tRPC batches them into one oversized request URL → HTTP 431). Dropped calls are safe —
  // the next length change re-triggers paging if the end is still in view.
  const loadingMoreRef = useRef(false)
  const loadMore = useCallback(() => {
    if (!sub || loadingMoreRef.current) return
    loadingMoreRef.current = true
    void sub.loadMore().finally(() => {
      loadingMoreRef.current = false
    })
  }, [sub])

  return {
    rows: (data ?? []) as T[],
    loading: def !== null && sub === null,
    loadMore,
    hasMore: sub?.hasMore() ?? false,
  }
}

/**
 * A live scalar **aggregation** (COUNT/SUM/…) over a filtered set, maintained incrementally by the
 * engine. Recreated whenever `def` changes (keyed by JSON) and closed on unmount. Returns the running
 * `value` and matching-row `count` — e.g. the top-of-list issue counter, which is a real COUNT over the
 * visible set rather than a client-side length of the loaded window.
 */
export function useAggregate(def: AggregateDef | null): { value: number | null; count: number } {
  const [state, setState] = useState<{ value: number | null; count: number }>({ value: null, count: 0 })
  const key = def ? JSON.stringify(def) : null

  useEffect(() => {
    if (!def) {
      setState({ value: null, count: 0 })
      return
    }
    let closed = false
    let sub: AggregateSubscription | undefined
    void client.aggregate(def).then((s) => {
      if (closed) {
        void s.close()
        return
      }
      sub = s
      setState({ value: s.value(), count: s.count() })
      // Guard against a stale closure: after cleanup this effect's subscription is closed, and a
      // late callback must not overwrite the replacement definition's state.
      s.subscribe((v) => {
        if (!closed) setState({ value: v, count: s.count() })
      })
    })
    return () => {
      closed = true
      if (sub) void sub.close()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key])

  return state
}
