// Smoke test (not a vitest test): resolve open questions empirically —
//  1. the DurableStreamTestServer stream-path layout (prefix? slashes?),
//  2. that our State-Protocol envelope round-trips through createStreamDB into a
//     materialized TanStack collection.
// Run: pnpm --filter @electric-ivm/conformance exec tsx src/smoke-ds.ts

import { DurableStreamTestServer } from '@electric-ivm/ds-rust'
import { createStreamDB, createStateSchema } from '@durable-streams/state/db'
import { z } from 'zod'

type Envelope = {
  type: string
  key: string
  value?: Record<string, unknown>
  headers: { operation: 'insert' | 'update' | 'delete' | 'upsert'; txid?: string }
}

async function tryCreate(base: string, path: string): Promise<{ ok: boolean; status: number; url: string }> {
  const url = `${base}/${path}`
  const res = await fetch(url, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
  })
  return { ok: res.status === 201 || res.status === 200, status: res.status, url }
}

async function main() {
  const server = new DurableStreamTestServer({ port: 0 })
  const base = await server.start()
  console.log('[server] url =', base, '| getter url =', server.url)

  // 1. Discover the working stream-path layout.
  const candidates = ['table/users', 'v1/stream/table/users', 'streams/table-users', 'table-users']
  let streamUrl = ''
  for (const path of candidates) {
    const r = await tryCreate(base, path)
    console.log(`[PUT] ${r.url} -> ${r.status} ${r.ok ? '(created)' : ''}`)
    if (r.ok && !streamUrl) streamUrl = r.url
  }
  if (!streamUrl) {
    console.error('No PUT path worked; aborting.')
    await server.stop()
    process.exit(1)
  }
  console.log('[chosen stream url]', streamUrl)

  // 2. Append two State-Protocol envelopes (JSON array flattens to 2 messages).
  const envelopes: Envelope[] = [
    { type: 'users', key: '1', value: { id: 1, name: 'Alice', active: true }, headers: { operation: 'insert' } },
    { type: 'users', key: '2', value: { id: 2, name: 'Bob', active: false }, headers: { operation: 'insert' } },
  ]
  const appendRes = await fetch(streamUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(envelopes),
  })
  console.log('[POST append] status =', appendRes.status, '| Stream-Next-Offset =', appendRes.headers.get('Stream-Next-Offset'))

  // 3. Raw catch-up read from the beginning.
  const readRes = await fetch(`${streamUrl}?offset=-1`)
  const body = await readRes.text()
  console.log('[GET ?offset=-1] status =', readRes.status, '| Up-To-Date =', readRes.headers.get('Stream-Up-To-Date'))
  console.log('[GET body]', body)

  // 4. Materialize via createStreamDB (the real client path).
  const rowSchema = z.object({ id: z.number(), name: z.string(), active: z.boolean() })
  const schema = createStateSchema({
    users: { schema: rowSchema, type: 'users', primaryKey: 'id' },
  })
  const db = createStreamDB({
    streamOptions: { url: streamUrl, contentType: 'application/json' },
    state: schema,
    live: true,
  })
  await db.preload()
  const coll = db.collections.users
  console.log('[streamdb] toArray =', JSON.stringify(coll.toArray))
  console.log('[streamdb] size =', coll.size)

  // 5. Live: append a third envelope and observe it propagate.
  let liveSeen: string[] = []
  const sub = coll.subscribeChanges(
    (changes: Array<{ type: string; key: unknown }>) => {
      for (const c of changes) liveSeen.push(`${c.type}:${String(c.key)}`)
    },
    { includeInitialState: false },
  )
  await fetch(streamUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify([
      { type: 'users', key: '3', value: { id: 3, name: 'Carol', active: true }, headers: { operation: 'insert' } },
    ] satisfies Envelope[]),
  })
  // give the live consumer a moment to receive the SSE/long-poll update
  await new Promise((r) => setTimeout(r, 800))
  console.log('[streamdb live] after append, toArray =', JSON.stringify(coll.toArray))
  console.log('[streamdb live] change events seen =', JSON.stringify(liveSeen))

  sub.unsubscribe()
  await db.close?.()
  await server.stop()
  console.log('[done]')
  process.exit(0)
}

main().catch((e) => {
  console.error('SMOKE FAILED:', e)
  process.exit(1)
})
