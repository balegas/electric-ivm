// electric-lite client: a thin wrapper over a typed tRPC client plus stream-db
// (`@durable-streams/state/db`) for materializing a shape into a live TanStack DB collection.

import type { AppRouter } from '@electric-lite/api'
import type { Op, Row, Schema, ShapeDef, TableDef, Value } from '@electric-lite/protocol'
import { createStateSchema, createStreamDB } from '@durable-streams/state/db'
import { createTRPCClient, httpBatchLink } from '@trpc/client'
import { z } from 'zod'

export interface ShapeHandle {
  shapeId: string
  table: string
  streamPath: string
  streamUrl: string
}

export interface ShapeMaterialization {
  handle: ShapeHandle
  /** Current materialized rows (declared columns + virtual props). */
  currentRows(): Row[]
  /** Resolve once an event bearing `txid` has been consumed (append-then-read determinism). */
  awaitTxId(txid: string, timeoutMs?: number): Promise<void>
  /** Subscribe to live change batches; returns an unsubscribe fn. */
  subscribe(cb: (changes: Array<{ type: string; key: unknown; value?: unknown }>) => void): () => void
  close(): Promise<void>
}

export interface ElectricLiteClient {
  defineSchema(schema: Schema): Promise<unknown>
  write(input: { table: string; op: Op; pk: Value; row?: Row; txid?: string }): Promise<{ txid: string }>
  shape(def: ShapeDef): Promise<ShapeMaterialization>
  close(): Promise<void>
}

function zodRowSchema(def: TableDef): z.ZodType {
  const shape: Record<string, z.ZodTypeAny> = {}
  for (const [col, c] of Object.entries(def.columns)) {
    // pk is validated as its declared type here, then the dispatcher stringifies it on the row.
    shape[col] =
      c.type === 'bool'
        ? z.boolean()
        : c.type === 'text'
          ? z.string()
          : z.number()
  }
  // be permissive about extra/loose fields the stream layer may add
  return z.object(shape).loose()
}

export function createClient(opts: { apiUrl: string; schema: Schema }): ElectricLiteClient {
  const trpc = createTRPCClient<AppRouter>({ links: [httpBatchLink({ url: opts.apiUrl })] })
  const open: ShapeMaterialization[] = []

  return {
    defineSchema: (schema) => trpc.schema.define.mutate({ schema }),

    write: (input) => trpc.ingest.write.mutate(input),

    async shape(def) {
      const tableDef = opts.schema.tables[def.table]
      if (!tableDef) throw new Error(`client: unknown table "${def.table}"`)

      const handle = (await trpc.shapes.create.mutate({
        table: def.table,
        where: def.where as never,
      })) as ShapeHandle

      const state = createStateSchema({
        [def.table]: { schema: zodRowSchema(tableDef), type: def.table, primaryKey: tableDef.primaryKey },
      })
      const db = createStreamDB({
        streamOptions: { url: handle.streamUrl, contentType: 'application/json' },
        state,
        live: true,
      })
      await db.preload()
      const collection = db.collections[def.table]

      const mat: ShapeMaterialization = {
        handle,
        currentRows: () => collection.toArray as Row[],
        awaitTxId: (txid, timeoutMs) => db.utils.awaitTxId(txid, timeoutMs),
        subscribe: (cb) => {
          const sub = collection.subscribeChanges(cb as never, { includeInitialState: false })
          return () => sub.unsubscribe()
        },
        close: async () => {
          await db.close?.()
        },
      }
      open.push(mat)
      return mat
    },

    async close() {
      for (const m of open) await m.close()
    },
  }
}
