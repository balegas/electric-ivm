// The domain verbs — the ONLY way playground visitors write data. Each verb is fixed,
// parameterized SQL scoped to the caller's workspace; there is no raw SQL surface.

import type { Order, Verb } from '../shared/types.ts'
import { type Db, mintId, num } from './db.ts'
import { DISHES, TRANSITIONS } from './schema.ts'

export class ActionError extends Error {
  constructor(
    public status: number,
    msg: string,
  ) {
    super(msg)
  }
}

const MAX_OPEN_ORDERS = 30

export async function applyAction(db: Db, ws: string, verb: Verb): Promise<{ ok: true; order?: Order }> {
  switch (verb.verb) {
    case 'place_order': {
      const r = await db.query('SELECT id FROM restaurants WHERE id = $1 AND workspace_id = $2', [
        verb.restaurantId,
        ws,
      ])
      if (r.rowCount !== 1) throw new ActionError(404, 'unknown restaurant')
      // Cap open orders per workspace: past the cap, the oldest non-terminal order is auto-cancelled
      // (visible as an ordinary delta — the cap itself becomes a little demo of retractions).
      const open = await db.query(
        `SELECT id FROM orders WHERE workspace_id = $1 AND status NOT IN ('delivered','cancelled') ORDER BY id`,
        [ws],
      )
      if ((open.rowCount ?? 0) >= MAX_OPEN_ORDERS) {
        await db.query(`UPDATE orders SET status = 'cancelled' WHERE id = $1`, [open.rows[0].id])
      }
      const id = mintId()
      const dish = DISHES[Math.floor(Math.random() * DISHES.length)]
      const total = Math.round((6 + Math.random() * 34) * 100) / 100
      const ins = await db.query(
        `INSERT INTO orders (id, workspace_id, restaurant_id, status, dish, total)
         VALUES ($1,$2,$3,'new',$4,$5) RETURNING *`,
        [id, ws, verb.restaurantId, dish, total],
      )
      const o = ins.rows[0]
      return { ok: true, order: { ...o, id: num(o.id), restaurant_id: num(o.restaurant_id) } }
    }
    case 'move_restaurant': {
      const r = await db.query('UPDATE restaurants SET city = $3 WHERE id = $1 AND workspace_id = $2', [
        verb.restaurantId,
        ws,
        verb.city,
      ])
      if (r.rowCount !== 1) throw new ActionError(404, 'unknown restaurant')
      return { ok: true }
    }
    default: {
      const t = TRANSITIONS[verb.verb]
      if (!t) throw new ActionError(400, `unknown verb ${(verb as { verb: string }).verb}`)
      const cur = await db.query('SELECT * FROM orders WHERE id = $1 AND workspace_id = $2', [verb.orderId, ws])
      if (cur.rowCount !== 1) throw new ActionError(404, 'unknown order')
      const status = cur.rows[0].status as string
      if (!t.from.includes(status)) {
        throw new ActionError(409, `cannot ${verb.verb} an order in status '${status}'`)
      }
      const upd = await db.query(
        'UPDATE orders SET status = $3 WHERE id = $1 AND workspace_id = $2 RETURNING *',
        [verb.orderId, ws, t.to],
      )
      const o = upd.rows[0]
      return { ok: true, order: { ...o, id: num(o.id), restaurant_id: num(o.restaurant_id) } }
    }
  }
}
