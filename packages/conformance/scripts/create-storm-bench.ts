// Micro-bench: 300 concurrent subquery-shape creations straight against the engine HTTP API
// (no Electric adapter), replicating the fleet benchmark's shape template. Prints latency
// percentiles + wall time so the serializer can be located.
import { bootHarness, drainEngine } from '../src/harness.js'
import pgpkg from 'pg'

const schema = {
  tables: {
    parent: { columns: { id: { type: 'text' }, group_id: { type: 'int' }, name: { type: 'text' } }, primaryKey: 'id' },
    child: { columns: { id: { type: 'text' }, parent_id: { type: 'text' }, name: { type: 'text' } }, primaryKey: 'id' },
  },
} as any

const GROUPS = Number(process.env.GROUPS ?? 300)

const h = await bootHarness(schema)
try {
  // Seed: 2 parents per group, 5 children per parent (matches the fleet bench shape/size).
  const c = new pgpkg.Client({ connectionString: h.pgUrl })
  await c.connect()
  await c.query(`INSERT INTO parent SELECT 'p'||g, (g/2)+1, 'p'||g FROM generate_series(0,${GROUPS * 2 - 1}) g`)
  await c.query(`INSERT INTO child SELECT 'c'||g||'-'||s, 'p'||g, 'c' FROM generate_series(0,${GROUPS * 2 - 1}) g, generate_series(1,5) s`)
  await c.end()
  await drainEngine(h)

  const t0 = performance.now()
  const lat: number[] = []
  await Promise.all(
    Array.from({ length: GROUPS }, (_, i) =>
      (async () => {
        const s = performance.now()
        const res = await fetch(`${h.engineUrl}/shapes`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            table: 'child',
            where: { col: 'parent_id', in: { table: 'parent', project: 'id', where: { col: 'group_id', op: 'eq', value: i + 1 } } },
          }),
        })
        if (!res.ok) throw new Error(`create ${i}: ${res.status} ${await res.text()}`)
        await res.json()
        lat.push(performance.now() - s)
      })(),
    ),
  )
  const wall = performance.now() - t0
  lat.sort((a, b) => a - b)
  const q = (p: number) => lat[Math.min(lat.length - 1, Math.floor((p / 100) * lat.length))].toFixed(0)
  console.log(`engine-direct: ${GROUPS} creates wall=${(wall / 1000).toFixed(1)}s p50=${q(50)}ms p95=${q(95)}ms p99=${q(99)}ms max=${lat[lat.length - 1].toFixed(0)}ms`)
} finally {
  await h.shutdown()
}
