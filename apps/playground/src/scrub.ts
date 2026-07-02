// Silent workspace scoping: the server ALWAYS scopes every shape to the caller's workspace, but
// the UI hides that plumbing — predicates, router keys, and labels render without the
// workspace_id conjunct unless the "under the hood" toggle is on. Scrubbing is display-only:
// graph/node ids keep the real (unscrubbed) identities so trace animation still matches.

import type { Predicate } from '@viz/types'

/** Remove `workspace_id = …` conjuncts from a predicate AST (recursively, incl. subquery inner). */
export function scrubPredicate(p: Predicate | null | undefined): Predicate | null {
  if (!p) return null
  if ('and' in p) {
    const kept = p.and.map(scrubPredicate).filter((x): x is Predicate => x !== null)
    if (kept.length === 0) return null
    return kept.length === 1 ? kept[0]! : { and: kept }
  }
  if ('or' in p) return { or: p.or.map(scrubPredicate).filter((x): x is Predicate => x !== null) }
  if ('not' in p) {
    const inner = scrubPredicate(p.not)
    return inner ? { not: inner } : null
  }
  if ('in' in p) {
    return { ...p, in: { ...p.in, where: scrubPredicate(p.in.where) } }
  }
  return p.col === 'workspace_id' ? null : p
}

/** Strip workspace_id from rendered label/sub strings (router keys, predicate labels). */
export function scrubText(s: string): string {
  return s
    .replace(/ AND workspace_id = '[^']*'/g, '')
    .replace(/workspace_id = '[^']*' AND /g, '')
    .replace(/workspace_id = '[^']*'/g, 'all rows')
    .replace(/\(workspace_id, /g, '(')
    .replace(/, workspace_id\)/g, ')')
    .replace(/\(workspace_id\)/g, '(—)')
    .replace(/workspace_id,\s*/g, '')
    .replace(/,\s*workspace_id/g, '')
}

/** Row object for display: drop the workspace_id column. */
export function scrubRow<T extends Record<string, unknown>>(row: T): Omit<T, 'workspace_id'> {
  const { workspace_id: _ws, ...rest } = row
  return rest
}
