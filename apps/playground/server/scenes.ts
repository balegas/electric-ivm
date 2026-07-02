// Scene provisioning: create the scene's shapes for a workspace, idempotently. Idempotency is by
// (workspace, scene-shape key) in the meta table; if the engine lost a shape (restart) the stale
// meta row is detected and the shape re-created, so re-entering a scene self-heals.

import { SCENES } from '../shared/scenes.ts'
import type { SceneShapeResult } from '../shared/types.ts'
import { createShape, listShapes, type ShapeDeps } from './shapes.ts'

export async function provisionScene(deps: ShapeDeps, ws: string, n: number): Promise<SceneShapeResult> {
  const scene = SCENES.find((s) => s.n === n)
  if (!scene) throw Object.assign(new Error(`unknown scene ${n}`), { status: 404 })
  const existing = await listShapes(deps.db, ws)
  const bySkey = new Map(existing.filter((s) => s.scene !== null && s.skey).map((s) => [`${s.scene}:${s.skey}`, s]))

  const out = []
  for (const def of scene.shapes) {
    const found = bySkey.get(`${n}:${def.key}`)
    if (found && (await deps.engine.shapeExists(found.id))) {
      out.push(found)
      continue
    }
    if (found) {
      // Engine lost it (restart/wipe) — drop the stale meta row and re-create.
      await deps.db.query('DELETE FROM playground_shapes WHERE shape_id = $1', [found.id])
    }
    out.push(await createShape(deps, ws, def.spec, def.label, def.role, n, def.key))
  }
  return { scene: n, shapes: out }
}
