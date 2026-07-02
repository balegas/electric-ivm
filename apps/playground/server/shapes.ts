// Shape building: compose the guided-builder ShapeSpec into the engine's predicate AST — ALWAYS
// appending the workspace conjunct (top level AND inside any subquery inner where, since inner
// tables are also per-workspace rows) — register it with the engine, and persist the meta row.
// Honest display: the stored where_json is exactly what the engine maintains, workspace_id and all.

import type { Predicate } from '@electric-ivm/protocol'
import type { PlaygroundShape, ShapeSpec } from '../shared/types.ts'
import { type Db } from './db.ts'
import { EngineClient } from './engine-client.ts'

export const MAX_SHAPES_PER_WS = 12

export class ShapeError extends Error {
  constructor(
    public status: number,
    msg: string,
  ) {
    super(msg)
  }
}

/** Compose the full predicate the engine will maintain (spec + workspace scoping). */
export function specToWhere(spec: ShapeSpec, ws: string): Predicate {
  const conjuncts: Predicate[] = []
  for (const c of spec.where) conjuncts.push({ col: c.col, op: c.op, value: c.value } as Predicate)
  if (spec.subquery) {
    const innerConjuncts: Predicate[] = spec.subquery.inner.where.map(
      (c) => ({ col: c.col, op: c.op, value: c.value }) as Predicate,
    )
    innerConjuncts.push({ col: 'workspace_id', op: 'eq', value: ws } as Predicate)
    conjuncts.push({
      col: spec.subquery.col,
      in: {
        table: spec.subquery.inner.table,
        project: spec.subquery.inner.project,
        where: innerConjuncts.length === 1 ? innerConjuncts[0] : { and: innerConjuncts },
      },
      ...(spec.subquery.negated ? { negated: true } : {}),
    } as Predicate)
  }
  conjuncts.push({ col: 'workspace_id', op: 'eq', value: ws } as Predicate)
  return conjuncts.length === 1 ? conjuncts[0]! : { and: conjuncts }
}

export interface ShapeDeps {
  db: Db
  engine: EngineClient
}

/** PlaygroundShape plus the scene-shape key (server-internal, used for scene idempotency). */
export type StoredShape = PlaygroundShape & { skey: string | null }

function rowToShape(r: Record<string, unknown>): StoredShape {
  return {
    id: r.shape_id as string,
    workspaceId: r.workspace_id as string,
    scene: (r.scene as number | null) ?? null,
    skey: (r.skey as string | null) ?? null,
    label: r.label as string,
    spec: r.spec as ShapeSpec,
    where: r.where_json as PlaygroundShape['where'],
  }
}

export async function listShapes(db: Db, ws: string): Promise<StoredShape[]> {
  const r = await db.query('SELECT * FROM playground_shapes WHERE workspace_id = $1 ORDER BY shape_id', [ws])
  return r.rows.map(rowToShape)
}

export async function shapeOwned(db: Db, ws: string, shapeId: string): Promise<boolean> {
  const r = await db.query('SELECT 1 FROM playground_shapes WHERE shape_id = $1 AND workspace_id = $2', [shapeId, ws])
  return r.rowCount === 1
}

/** All (workspace, shape id) pairs — the trace fan-out's tagging index. */
export async function allShapeOwners(db: Db): Promise<Map<string, string>> {
  const r = await db.query('SELECT shape_id, workspace_id FROM playground_shapes')
  return new Map(r.rows.map((x) => [x.shape_id as string, x.workspace_id as string]))
}

export async function createShape(
  deps: ShapeDeps,
  ws: string,
  spec: ShapeSpec,
  label: string,
  scene: number | null = null,
  skey: string | null = null,
): Promise<PlaygroundShape> {
  const existing = await deps.db.query('SELECT COUNT(*)::int AS n FROM playground_shapes WHERE workspace_id = $1', [ws])
  if ((existing.rows[0].n as number) >= MAX_SHAPES_PER_WS) {
    throw new ShapeError(413, `workspace shape cap reached (${MAX_SHAPES_PER_WS}) — delete one first`)
  }
  const where = specToWhere(spec, ws)
  const resp = spec.aggregate
    ? await deps.engine.createAggregate(spec.table, where, spec.aggregate.func, spec.aggregate.col)
    : await deps.engine.createShape(spec.table, where)
  // The engine SHARES identical feeds: an equal spec (same workspace, same predicate) returns an
  // EXISTING shape id. In that case the meta row already describes this stream — keep it (a DO
  // UPDATE here would hijack a scene shape's provenance) and hand the caller the existing card.
  const ins = await deps.db.query(
    `INSERT INTO playground_shapes (shape_id, workspace_id, scene, skey, role, label, spec, where_json)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
     ON CONFLICT (shape_id) DO NOTHING`,
    [resp.shapeId, ws, scene, skey, 'custom', label, JSON.stringify(spec), JSON.stringify(where)],
  )
  if (ins.rowCount === 0) {
    // Shared with an existing shape: the engine bumped its refcount — undo that so meta stays 1:1
    // with engine registrations, then return the existing card.
    await deps.engine.deleteShape(resp.shapeId)
    const existing = await deps.db.query('SELECT * FROM playground_shapes WHERE shape_id = $1', [resp.shapeId])
    return rowToShape(existing.rows[0])
  }
  return { id: resp.shapeId, workspaceId: ws, scene, label, spec, where: where as PlaygroundShape['where'] }
}

export async function deleteShape(deps: ShapeDeps, ws: string, shapeId: string): Promise<void> {
  if (!(await shapeOwned(deps.db, ws, shapeId))) throw new ShapeError(404, 'unknown shape')
  await deps.engine.deleteShape(shapeId)
  await deps.db.query('DELETE FROM playground_shapes WHERE shape_id = $1', [shapeId])
}

/** Tear down every shape of a workspace (engine + meta). Used by the TTL sweep and resets. */
export async function deleteWorkspaceShapes(deps: ShapeDeps, ws: string): Promise<void> {
  const shapes = await listShapes(deps.db, ws)
  for (const s of shapes) {
    await deps.engine.deleteShape(s.id)
  }
  await deps.db.query('DELETE FROM playground_shapes WHERE workspace_id = $1', [ws])
}
