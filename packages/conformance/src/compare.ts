// Set-equality comparison between an oracle result set and a client-materialized set.
//
// Materialized rows differ from oracle rows in two known ways (see decisions D5b/D6):
//  - the client stringifies the pk (value[pk] = event.key), so we key by String(pk);
//  - both layers add virtual props ($synced/$origin/_seq/...), so we compare only the
//    declared columns, and only the NON-pk columns by value (the pk is compared as a string
//    key). Non-pk columns keep their JS types on both sides (text/bool/number).

import type { Row } from '@electric-circuits/protocol'

export interface CompareResult {
  equal: boolean
  /** Keys present only in the oracle (missing from the client). */
  missing: string[]
  /** Keys present only in the client (extra rows). */
  extra: string[]
  /** Keys whose non-pk columns differ. */
  mismatched: Array<{ key: string; oracle: Row; client: Row }>
}

function project(row: Row, columns: string[]): Row {
  const out: Row = {}
  for (const c of columns) out[c] = (row[c] ?? null) as Row[string]
  return out
}

function valuesEqual(a: unknown, b: unknown): boolean {
  if (typeof a === 'number' && typeof b === 'number') return Object.is(a, b)
  return a === b
}

export function compareShapeSets(
  declaredColumns: string[],
  pk: string,
  oracle: Row[],
  client: Row[],
): CompareResult {
  const nonPk = declaredColumns.filter((c) => c !== pk)
  const oByKey = new Map(oracle.map((r) => [String(r[pk]), r]))
  const cByKey = new Map(client.map((r) => [String(r[pk]), r]))

  const missing: string[] = []
  const extra: string[] = []
  const mismatched: Array<{ key: string; oracle: Row; client: Row }> = []

  for (const [key, orow] of oByKey) {
    const crow = cByKey.get(key)
    if (!crow) {
      missing.push(key)
      continue
    }
    const ok = nonPk.every((c) => valuesEqual(orow[c] ?? null, crow[c] ?? null))
    if (!ok) mismatched.push({ key, oracle: project(orow, declaredColumns), client: project(crow, declaredColumns) })
  }
  for (const key of cByKey.keys()) {
    if (!oByKey.has(key)) extra.push(key)
  }

  return { equal: missing.length === 0 && extra.length === 0 && mismatched.length === 0, missing, extra, mismatched }
}

/** Human-readable diff for test failure messages. */
export function formatCompare(result: CompareResult): string {
  if (result.equal) return 'sets are equal'
  const parts: string[] = []
  if (result.missing.length) parts.push(`missing from client: [${result.missing.join(', ')}]`)
  if (result.extra.length) parts.push(`extra in client: [${result.extra.join(', ')}]`)
  for (const m of result.mismatched) {
    parts.push(`mismatch key=${m.key}: oracle=${JSON.stringify(m.oracle)} client=${JSON.stringify(m.client)}`)
  }
  return parts.join('\n')
}
