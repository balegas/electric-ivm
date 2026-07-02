// The left pane — "the world": restaurants with their open orders and the verb buttons. Every
// button is one write to Postgres; the delta's journey is what the rest of the screen shows.

import type { Order, Restaurant, Verb } from '../shared/types.ts'

const NEXT_VERBS: Record<string, { verb: 'start_cooking' | 'pickup' | 'deliver' | 'cancel'; label: string }[]> = {
  new: [
    { verb: 'start_cooking', label: '🔥 Start cooking' },
    { verb: 'cancel', label: '✕' },
  ],
  cooking: [
    { verb: 'pickup', label: '🛵 Rider picks up' },
    { verb: 'cancel', label: '✕' },
  ],
  riding: [
    { verb: 'deliver', label: '✅ Delivered' },
    { verb: 'cancel', label: '✕' },
  ],
}

const CITIES = ['Lisbon', 'Porto', 'Faro']

const STATUS_DOT: Record<string, string> = {
  new: '🆕',
  cooking: '🔥',
  riding: '🛵',
  delivered: '✅',
  cancelled: '✖️',
}

export function WorldPanel({
  restaurants,
  orders,
  showMoveCity,
  act,
}: {
  restaurants: Restaurant[]
  orders: Order[]
  showMoveCity: boolean
  act: (verb: Verb) => void
}) {
  const byRestaurant = new Map<number, Order[]>()
  for (const o of orders) {
    if (!byRestaurant.has(o.restaurant_id)) byRestaurant.set(o.restaurant_id, [])
    byRestaurant.get(o.restaurant_id)!.push(o)
  }

  return (
    <div className="world">
      <div className="world-h">Food delivery</div>
      {restaurants.map((r) => {
        const ros = (byRestaurant.get(r.id) ?? []).filter((o) => o.status !== 'cancelled')
        return (
          <div key={r.id} className="rest">
            <div className="rest-h">
              <span className="rest-name">
                {r.emoji} {r.name}
              </span>
              {showMoveCity ? (
                <select
                  className="rest-city"
                  value={r.city}
                  onChange={(e) => act({ verb: 'move_restaurant', restaurantId: r.id, city: e.target.value })}
                  title="Move the restaurant to another city (watch the subquery cascade)"
                >
                  {CITIES.map((c) => (
                    <option key={c}>{c}</option>
                  ))}
                </select>
              ) : (
                <span className="rest-city-label">{r.city}</span>
              )}
            </div>
            <button className="order-btn" onClick={() => act({ verb: 'place_order', restaurantId: r.id })}>
              ＋ Place order
            </button>
            {ros.map((o) => (
              <div key={o.id} className={`order order-${o.status}`}>
                <span className="order-desc" title={`order #${o.id}`}>
                  {STATUS_DOT[o.status]} {o.dish} <span className="order-total">€{o.total.toFixed(2)}</span>
                </span>
                <span className="order-verbs">
                  {(NEXT_VERBS[o.status] ?? []).map((v) => (
                    <button key={v.verb} className="verb" onClick={() => act({ verb: v.verb, orderId: o.id })}>
                      {v.label}
                    </button>
                  ))}
                </span>
              </div>
            ))}
          </div>
        )
      })}
      <button className="add-rest" onClick={() => act({ verb: 'add_restaurant' })}>
        ＋ Add restaurant
      </button>
    </div>
  )
}
