# tRPC v11 — Standalone Node HTTP Server + Vanilla Client

Research brief for `electric-ivm`: expose `schema.define`, `ingest.write`, `shapes.create`,
`shapes.get` over tRPC, run as a standalone Node HTTP server (no Next.js, no React),
and call it from Node test code with a fully typed vanilla client.

Verified against official docs (trpc.io) and npm registry on **2026-06-27**.

## Versions

| Package | Latest stable | Notes |
|---|---|---|
| `@trpc/server` | **11.18.0** | core: `initTRPC`, `TRPCError`, adapters |
| `@trpc/client` | **11.18.0** | `createTRPCClient`, `httpBatchLink`, `httpLink` |
| `zod` | **4.4.3** | input validation; works via Standard Schema (see below) |

`@trpc/server` and `@trpc/client` are versioned in lockstep — keep them on the same minor.

### Install

```bash
npm add @trpc/server@11 @trpc/client@11 zod
# standalone adapter cors middleware is optional:
npm add cors
npm add -D @types/cors
```

Requirements: Node 18+, TypeScript 5.7.2+ (tRPC v11 minimum), `"strict": true` in tsconfig
is strongly recommended (type inference depends on it).

### Zod v4 compatibility

tRPC v11 validates inputs through **Standard Schema** (`@standard-schema/spec`). Zod v4
(and v3.24+) implements Standard Schema, so passing a Zod schema to `.input()` / `.output()`
works directly with no adapter. Both `import { z } from 'zod'` (v4 default) and
`zod/v4` entrypoints are fine. (Unverified detail: if you pin Zod v3 < 3.24 you fall back to
tRPC's legacy resolver, which still works.)

## Defining the router + procedures

Create a single `initTRPC` instance per app, then derive `router` / `procedure` helpers.
The exported `AppRouter` **type** is what the client imports (type-only, no runtime code).

```typescript
// server/trpc.ts
import { initTRPC, TRPCError } from '@trpc/server';

// Context is whatever createContext returns (see server below).
export type Context = {
  // e.g. db handle, request-scoped state
  electric: ElectricIvm;
};

const t = initTRPC.context<Context>().create();

export const router = t.router;
export const publicProcedure = t.procedure;
export { TRPCError };
```

```typescript
// server/appRouter.ts
import { z } from 'zod';
import { router, publicProcedure, TRPCError } from './trpc';

export const appRouter = router({
  schema: router({
    define: publicProcedure
      .input(z.object({
        table: z.string(),
        columns: z.record(z.string(), z.string()), // example shape
      }))
      .mutation(({ input, ctx }) => {
        return ctx.electric.schema.define(input);
      }),
  }),

  ingest: router({
    write: publicProcedure
      .input(z.object({
        table: z.string(),
        op: z.enum(['insert', 'update', 'delete']),
        pk: z.string(),
        row: z.record(z.string(), z.unknown()),
      }))
      .mutation(({ input, ctx }) => {
        return ctx.electric.ingest.write(input);
      }),
  }),

  shapes: router({
    create: publicProcedure
      .input(z.object({
        table: z.string(),
        where: z.string().optional(),
      }))
      .mutation(({ input, ctx }) => {
        return ctx.electric.shapes.create(input); // returns { id, ... }
      }),

    get: publicProcedure
      .input(z.object({ id: z.string() }))
      .query(({ input, ctx }) => {
        const shape = ctx.electric.shapes.get(input.id);
        if (!shape) {
          throw new TRPCError({ code: 'NOT_FOUND', message: `shape ${input.id} not found` });
        }
        return shape;
      }),
});

// CRITICAL: export the TYPE for the client.
export type AppRouter = typeof appRouter;
```

Key rules:
- `.input(schema)` runs the Zod schema before the resolver; a parse failure throws a
  `BAD_REQUEST` `TRPCError` automatically (the parsed/typed value is what the resolver sees).
- `.query(...)` = read (idempotent, GET-able, batchable); `.mutation(...)` = write (POST).
  This is the only semantic difference you must get right — `shapes.get` is a query, the
  rest are mutations.
- Nested routers (`schema.define`, `ingest.write`) are created by nesting `router({...})`.
  The client mirrors the nesting: `client.ingest.write.mutate(...)`.
- Optional `.output(schema)` validates/clamps the return value (good for tests).

## Standalone HTTP server (no framework)

Use the **standalone adapter** `createHTTPServer` from `@trpc/server/adapters/standalone`.
It wraps Node's native `http.Server`, so `.listen(port)` returns/behaves like a normal server.

```typescript
// server/index.ts
import { createHTTPServer } from '@trpc/server/adapters/standalone';
import cors from 'cors'; // optional, only needed for browser cross-origin
import { appRouter } from './appRouter';
import type { Context } from './trpc';

const server = createHTTPServer({
  router: appRouter,
  middleware: cors(), // optional; handles OPTIONS preflight + CORS headers
  createContext(opts): Context {
    // opts has { req, res, info }. Build request-scoped context here.
    return { electric: getElectricIvm() };
  },
  // basePath: '/trpc/', // optional, default is '/'
  onError({ error, path }) {
    console.error(`tRPC error on ${path ?? '<no-path>'}:`, error);
  },
});

const { port } = server.listen(2022); // listen() returns { port, server }
console.log(`tRPC listening on http://localhost:${port}`);
```

Notes:
- `createContext` is sync or async; it receives `{ req, res, info }` (Node `IncomingMessage` /
  `ServerResponse`). For tests you can keep it trivial.
- `listen(port)` returns an object `{ port, server }` where `server` is the underlying
  `http.Server` — call `server.close()` in test teardown. (Pass `0` to get a random free port,
  then read the returned `port` — handy for parallel tests.)
- Endpoint URL is `http://host:port/<basePath><procedure.path>`. With default basePath the
  client base url is just `http://localhost:2022`.
- `middleware` accepts any `(req, res, next)` connect-style middleware; `cors()` is the common
  one. You can omit it entirely for Node-to-Node test usage.

## Typed vanilla client (Node)

`createTRPCClient<AppRouter>` gives a fully typed proxy. Use `httpBatchLink` (batches calls
made in the same tick into one HTTP request) or `httpLink` (one request per call — simpler to
reason about in tests / when reading network logs).

```typescript
// client.ts
import { createTRPCClient, httpBatchLink } from '@trpc/client';
import type { AppRouter } from './server/appRouter'; // TYPE ONLY

export const client = createTRPCClient<AppRouter>({
  links: [
    httpBatchLink({
      url: 'http://localhost:2022',
      // async headers() { return { authorization: '...' }; }, // optional
      // fetch,  // override fetch if needed; Node 18+ has global fetch
    }),
  ],
});

// queries use .query(), mutations use .mutate(); nesting mirrors the router:
const shape = await client.shapes.create.mutate({ table: 'users', where: 'active = true' });
const got   = await client.shapes.get.query({ id: shape.id });
await client.ingest.write.mutate({ table: 'users', op: 'insert', pk: '1', row: { name: 'a' } });
await client.schema.define.mutate({ table: 'users', columns: { id: 'text', name: 'text' } });
```

Notes:
- Input/return types are inferred end-to-end from `AppRouter` — no codegen.
- Node 18+ provides a global `fetch`, so no polyfill is needed. For older Node, pass
  `fetch` (e.g. from `undici`) into the link config.
- `httpBatchLink` requires the server to accept batching (the standalone adapter does by
  default). If you prefer one-request-per-call in tests, swap to `httpLink` (same import path
  `@trpc/client`, same `{ url }` config).

## Error handling shape

Throw `TRPCError` server-side. The client receives a `TRPCClientError<AppRouter>` whose shape
mirrors the JSON-RPC error envelope:

```jsonc
{
  "error": {
    "message": "shape s_42 not found",
    "code": -32004,                 // JSON-RPC numeric code
    "data": {
      "code": "NOT_FOUND",          // tRPC string code
      "httpStatus": 404,
      "path": "shapes.get",
      "stack": "...",               // only when NODE_ENV !== 'production'
      // for Zod input failures, data may include a parsed ZodError under data.zodError
      // if you add a custom errorFormatter; by default message contains the Zod message
    }
  }
}
```

Code → HTTP status (subset):

| tRPC code | HTTP |
|---|---|
| `BAD_REQUEST` | 400 |
| `UNAUTHORIZED` | 401 |
| `FORBIDDEN` | 403 |
| `NOT_FOUND` | 404 |
| `TIMEOUT` | 408 |
| `CONFLICT` | 409 |
| `PRECONDITION_FAILED` | 412 |
| `PAYLOAD_TOO_LARGE` | 413 |
| `UNPROCESSABLE_CONTENT` | 422 |
| `TOO_MANY_REQUESTS` | 429 |
| `CLIENT_CLOSED_REQUEST` | 499 |
| `INTERNAL_SERVER_ERROR` | 500 |
| `NOT_IMPLEMENTED` | 501 |
| `SERVICE_UNAVAILABLE` | 503 |

Client-side handling:

```typescript
import { TRPCClientError } from '@trpc/client';

try {
  await client.shapes.get.query({ id: 'nope' });
} catch (err) {
  if (err instanceof TRPCClientError) {
    err.message;            // "shape nope not found"
    err.data?.code;         // "NOT_FOUND"
    err.data?.httpStatus;   // 404
    err.shape?.data?.path;  // "shapes.get"
  }
}
```

Zod input failures surface automatically as `BAD_REQUEST` (HTTP 400). To get the structured
Zod issues on the client, add a custom `errorFormatter` in `initTRPC...create({ errorFormatter })`
that attaches `error.cause` (a `ZodError`) onto `shape.data.zodError`. Without it, the
human-readable Zod message is still in `error.message`.

Server-side, you can also map status codes with
`import { getHTTPStatusCodeFromError } from '@trpc/server/http'`.

## Minimal end-to-end snippet (server + client in one process, for a Node test)

```typescript
import { initTRPC, TRPCError } from '@trpc/server';
import { createHTTPServer } from '@trpc/server/adapters/standalone';
import { createTRPCClient, httpBatchLink } from '@trpc/client';
import { z } from 'zod';

// --- server ---
const t = initTRPC.create();
const appRouter = t.router({
  greet: t.procedure
    .input(z.object({ name: z.string() }))
    .query(({ input }) => `hello ${input.name}`),
  fail: t.procedure.query(() => {
    throw new TRPCError({ code: 'NOT_FOUND', message: 'nope' });
  }),
});
type AppRouter = typeof appRouter;

const { server, port } = createHTTPServer({ router: appRouter }).listen(0); // random port

// --- client ---
const client = createTRPCClient<AppRouter>({
  links: [httpBatchLink({ url: `http://localhost:${port}` })],
});

console.log(await client.greet.query({ name: 'world' })); // "hello world"
try { await client.fail.query(); } catch (e) { console.log((e as any).data?.code); } // NOT_FOUND

server.close();
```

## Open questions

- **electric-ivm return types**: the `.input()` schemas above are illustrative
  (`z.record`, `z.enum([...])`). Confirm the real column/row/op shapes and `shapes.create`
  return value so `.input()`/`.output()` match exactly.
- **Sync vs async electric-ivm methods**: snippets assume the methods may be sync; if any
  return Promises, the resolvers already `return` them so it's fine — just confirm.
- **Context lifetime**: whether `electric-ivm` is a singleton (build once, reuse in
  `createContext`) or per-request. Affects how `createContext` is written.
- **errorFormatter for Zod**: decide whether tests need structured Zod issues
  (`shape.data.zodError`) or whether the default message string suffices.
- **`listen(0)` random-port return shape**: docs show `listen(port)` returning `{ port, server }`;
  confirmed in source for v11 but worth a smoke test for the exact destructure used above.
- **Batching in tests**: `httpBatchLink` is fine, but if test assertions inspect raw network
  requests, prefer `httpLink` (one request per call). Not blocking.
