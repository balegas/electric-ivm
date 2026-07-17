// tRPC router: the public, schema-derived write/read API.

import { initTRPC, TRPCError } from '@trpc/server'
import { z } from 'zod'
import type { ElectricCore } from './core.js'

export interface Context {
  core: ElectricCore
}

const t = initTRPC.context<Context>().create()

const valueSchema = z.union([z.number(), z.string(), z.boolean(), z.null()])
const rowSchema = z.record(z.string(), valueSchema)
const columnType = z.enum(['int', 'text', 'bool', 'float'])
const leafOp = z.enum(['eq', 'neq', 'lt', 'lte', 'gt', 'gte'])

const schemaSchema = z.object({
  tables: z.record(
    z.string(),
    z.object({
      columns: z.record(z.string(), z.object({ type: columnType })),
      primaryKey: z.string(),
    }),
  ),
})

// Recursive predicate AST: leaf | is-null | and | or | not | in-subquery.
const predicateSchema: z.ZodType = z.lazy(() =>
  z.union([
    z.object({ col: z.string(), op: leafOp, value: valueSchema }),
    z.object({ col: z.string(), isNull: z.boolean() }),
    z.object({ and: z.array(predicateSchema) }),
    z.object({ or: z.array(predicateSchema) }),
    z.object({ not: predicateSchema }),
    z.object({
      col: z.string(),
      in: z.object({ table: z.string(), project: z.string(), where: predicateSchema.optional() }),
      negated: z.boolean().optional(),
    }),
  ]),
)

export const appRouter = t.router({
  schema: t.router({
    define: t.procedure
      .input(z.object({ schema: schemaSchema }))
      .mutation(async ({ input, ctx }) => {
        await ctx.core.defineSchema(input.schema as Parameters<ElectricCore['defineSchema']>[0])
        return { ok: true as const }
      }),
  }),

  ingest: t.router({
    write: t.procedure
      .input(
        z.object({
          table: z.string(),
          op: z.enum(['insert', 'update', 'delete']),
          pk: valueSchema,
          row: rowSchema.optional(),
          txid: z.string().optional(),
        }),
      )
      .mutation(async ({ input, ctx }) => ctx.core.write(input)),
  }),

  shapes: t.router({
    create: t.procedure
      .input(z.object({ table: z.string(), where: predicateSchema.optional(), columns: z.array(z.string()).optional() }))
      .mutation(async ({ input, ctx }) =>
        ctx.core.createShape({ table: input.table, where: input.where as never, columns: input.columns }),
      ),

    get: t.procedure.input(z.object({ id: z.string() })).query(async ({ input, ctx }) => {
      const handle = await ctx.core.getShape(input.id)
      if (!handle) throw new TRPCError({ code: 'NOT_FOUND', message: `shape ${input.id} not found` })
      return handle
    }),

    delete: t.procedure
      .input(z.object({ id: z.string() }))
      .mutation(async ({ input, ctx }) => {
        await ctx.core.dropShape(input.id)
        return { ok: true as const }
      }),
  }),

  // Subset queries — the non-materialized counterpart to shapes. `query` is a one-shot, cacheable
  // read (no stream, no live state); page by moving a keyset cursor in `where` or bumping `offset`.
  // `live` opens a changes-only tail feed on the base predicate that the client follows to keep a
  // loaded page live (re-checking view membership client-side) — a single predicate, no range fanout.
  subset: t.router({
    query: t.procedure
      .input(
        z.object({
          table: z.string(),
          where: predicateSchema.optional(),
          columns: z.array(z.string()).optional(),
          orderBy: z.object({ col: z.string(), desc: z.boolean().optional() }).optional(),
          limit: z.number().int().nonnegative().optional(),
          offset: z.number().int().nonnegative().optional(),
        }),
      )
      .query(async ({ input, ctx }) =>
        ctx.core.querySubset({
          table: input.table,
          where: input.where as never,
          columns: input.columns,
          orderBy: input.orderBy,
          limit: input.limit,
          offset: input.offset,
        }),
      ),

    live: t.procedure
      .input(z.object({ table: z.string(), where: predicateSchema.optional(), columns: z.array(z.string()).optional() }))
      .mutation(async ({ input, ctx }) =>
        ctx.core.createSubsetFeed({ table: input.table, where: input.where as never, columns: input.columns }),
      ),
  }),

  // Scalar aggregations (COUNT/SUM/AVG/MIN/MAX) over a filter — an electric-circuits extension, maintained
  // incrementally by the engine and streamed as a single live value.
  aggregate: t.router({
    create: t.procedure
      .input(
        z.object({
          table: z.string(),
          where: predicateSchema.optional(),
          fn: z.enum(['count', 'sum', 'avg', 'min', 'max']),
          col: z.string().optional(),
        }),
      )
      .mutation(async ({ input, ctx }) =>
        ctx.core.createAggregate({ table: input.table, where: input.where as never, fn: input.fn, col: input.col }),
      ),
  }),
})

export type AppRouter = typeof appRouter
