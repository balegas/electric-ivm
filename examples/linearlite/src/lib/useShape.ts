import type { Row, ShapeDef } from '@electric-lite/protocol'
import { useEffect, useState } from 'react'
import { client } from '../electric'

/**
 * Subscribe to a shape and return its live rows. The shape is (re)created whenever `def` changes
 * (keyed by its JSON), and closed on unmount — so changing the list filter swaps the engine-side
 * predicate. Pass `null` to subscribe to nothing.
 */
export function useShapeRows<T extends Row = Row>(def: ShapeDef | null): { rows: T[]; loading: boolean } {
  const [rows, setRows] = useState<T[]>([])
  const [loading, setLoading] = useState(true)
  const key = def ? JSON.stringify(def) : null

  useEffect(() => {
    if (!def) {
      setRows([])
      setLoading(false)
      return
    }
    let closed = false
    let mat: Awaited<ReturnType<typeof client.shape>> | undefined
    let unsub = () => {}
    setLoading(true)
    client.shape(def).then((m) => {
      if (closed) {
        void m.close()
        return
      }
      mat = m
      setRows(m.currentRows() as T[])
      setLoading(false)
      unsub = m.subscribe(() => setRows(m.currentRows() as T[]))
    })
    return () => {
      closed = true
      unsub()
      if (mat) void mat.close()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key])

  return { rows, loading }
}
