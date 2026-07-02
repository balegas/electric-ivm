// electric-ivm "core": the logic behind the tRPC procedures. Writes append State-Protocol
// envelopes directly to the durable-streams table stream (decoupled from the engine, which
// tails it). Schema definition and shape lifecycle are forwarded to the Rust engine.

import {
  type AggregateDef,
  type Op,
  type Row,
  type Schema,
  type ShapeDef,
  type SubsetDef,
  type SubsetResult,
  toTableEnvelope,
  type Value,
} from '@electric-ivm/protocol'

export interface WriteInput {
  table: string
  op: Op
  pk: Value
  row?: Row
  txid?: string
}

export interface ShapeHandle {
  shapeId: string
  table: string
  streamPath: string
  streamUrl: string
}

export interface ElectricCore {
  readonly dsUrl: string
  defineSchema(schema: Schema): Promise<void>
  write(input: WriteInput): Promise<{ txid: string }>
  /** Register a **materialized, live** shape (backfilled + maintained as a durable stream). */
  createShape(def: ShapeDef): Promise<ShapeHandle>
  getShape(id: string): Promise<ShapeHandle | null>
  /** Drop a shape (or subset feed) and tear down its stream. Idempotent. */
  dropShape(id: string): Promise<void>
  /** Run a one-shot **subset query** (ephemeral, non-materialized query-back from Postgres). */
  querySubset(def: SubsetDef): Promise<SubsetResult>
  /**
   * Open the **live tail** for a subset: a non-materialized, changes-only feed on the base predicate
   * (no backfill, no stored set). The client seeds rows from {@link querySubset} and applies this
   * feed's deltas, re-checking view membership — so paging never becomes server-side range state.
   */
  createSubsetFeed(def: Pick<SubsetDef, 'table' | 'where' | 'columns'>): Promise<ShapeHandle>
  /** Register a scalar **aggregation** (COUNT/SUM/AVG/MIN/MAX) over a filter — an electric-ivm
   * extension (not in the Electric protocol). Streams a single value maintained incrementally. */
  createAggregate(def: AggregateDef): Promise<ShapeHandle>
}

export interface CoreOptions {
  dsUrl: string
  engineUrl: string
  /** Injectable for tests; defaults to global fetch. */
  fetch?: typeof fetch
}

export function createCore(opts: CoreOptions): ElectricCore {
  const dsUrl = opts.dsUrl.replace(/\/$/, '')
  const engineUrl = opts.engineUrl.replace(/\/$/, '')
  const doFetch = opts.fetch ?? fetch
  const genTxid = () => globalThis.crypto.randomUUID()

  async function engineJson<T>(path: string, init: RequestInit): Promise<T> {
    const res = await doFetch(`${engineUrl}${path}`, {
      ...init,
      headers: { 'content-type': 'application/json', ...(init.headers ?? {}) },
    })
    if (!res.ok) throw new Error(`engine ${path} -> ${res.status}: ${await res.text()}`)
    return (await res.json()) as T
  }

  return {
    dsUrl,

    async defineSchema(schema) {
      await engineJson('/schema', { method: 'POST', body: JSON.stringify({ schema }) })
    },

    async write(input) {
      const txid = input.txid ?? genTxid()
      const env = toTableEnvelope(input.table, input.op, input.pk, input.row, txid)
      const res = await doFetch(`${dsUrl}/table/${input.table}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify([env]),
      })
      if (!res.ok) throw new Error(`append table/${input.table} -> ${res.status}: ${await res.text()}`)
      return { txid }
    },

    async createShape(def) {
      return engineJson<ShapeHandle>('/shapes', {
        method: 'POST',
        body: JSON.stringify({ table: def.table, where: def.where ?? null, columns: def.columns ?? null }),
      })
    },

    async getShape(id) {
      const res = await doFetch(`${engineUrl}/shapes/${encodeURIComponent(id)}`)
      if (res.status === 404) return null
      if (!res.ok) throw new Error(`engine /shapes/${id} -> ${res.status}`)
      return (await res.json()) as ShapeHandle
    },

    async dropShape(id) {
      const res = await doFetch(`${engineUrl}/shapes/${encodeURIComponent(id)}`, { method: 'DELETE' })
      if (!res.ok && res.status !== 404) throw new Error(`engine DELETE /shapes/${id} -> ${res.status}`)
    },

    async createSubsetFeed(def) {
      return engineJson<ShapeHandle>('/shapes', {
        method: 'POST',
        body: JSON.stringify({
          table: def.table,
          where: def.where ?? null,
          columns: def.columns ?? null,
          changesOnly: true,
        }),
      })
    },

    async createAggregate(def) {
      return engineJson<ShapeHandle>('/aggregate', {
        method: 'POST',
        body: JSON.stringify({ table: def.table, where: def.where ?? null, fn: def.fn, col: def.col ?? null }),
      })
    },

    async querySubset(def) {
      return engineJson<SubsetResult>('/query', {
        method: 'POST',
        body: JSON.stringify({
          table: def.table,
          where: def.where ?? null,
          columns: def.columns ?? null,
          orderBy: def.orderBy ?? null,
          limit: def.limit ?? null,
          offset: def.offset ?? null,
        }),
      })
    },
  }
}
