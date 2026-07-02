// The playground's replicated schema (protocol form — used by the engine boot in dev/tests) and
// the seed constants. Data tables carry workspace_id on every row; the playground_* meta tables
// are NOT part of this schema, so they are never replicated into the engine.

import type { Schema } from '@electric-ivm/protocol'

export const PLAYGROUND_SCHEMA: Schema = {
  tables: {
    restaurants: {
      columns: {
        id: { type: 'int' },
        workspace_id: { type: 'text' },
        name: { type: 'text' },
        emoji: { type: 'text' },
        city: { type: 'text' },
      },
      primaryKey: 'id',
    },
    orders: {
      columns: {
        id: { type: 'int' },
        workspace_id: { type: 'text' },
        restaurant_id: { type: 'int' },
        status: { type: 'text' },
        dish: { type: 'text' },
        total: { type: 'float' },
      },
      primaryKey: 'id',
    },
  },
}

export const SEED_RESTAURANTS: { name: string; emoji: string; city: string }[] = [
  { name: "Nono's Pizza", emoji: '🍕', city: 'Lisbon' },
  { name: 'Bifana Bros', emoji: '🥪', city: 'Lisbon' },
  { name: 'Peixe & Co', emoji: '🐟', city: 'Lisbon' },
  { name: 'Casa do Caril', emoji: '🍛', city: 'Lisbon' },
  { name: 'Francesinha 24', emoji: '🥩', city: 'Porto' },
  { name: 'Tripas Douro', emoji: '🍲', city: 'Porto' },
]

export const DISHES = [
  'Margherita', 'Bifana', 'Bacalhau', 'Tikka Masala', 'Francesinha', 'Caldo Verde',
  'Piri-piri Chicken', 'Pastel de Nata x6', 'Arroz de Marisco', 'Prego no Pão',
] as const

/** Legal status transitions for the action verbs. */
export const TRANSITIONS: Record<string, { from: string[]; to: string }> = {
  start_cooking: { from: ['new'], to: 'cooking' },
  pickup: { from: ['cooking'], to: 'riding' },
  deliver: { from: ['riding'], to: 'delivered' },
  cancel: { from: ['new', 'cooking', 'riding'], to: 'cancelled' },
}
