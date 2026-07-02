// The walkthrough scenes: explainer copy (client) + the shapes each scene provisions (server).
// One module so the story and the provisioning can never drift apart. Scenes only ADD shapes —
// data and earlier shapes persist; free play is available inside every scene.

import type { DeviceRole, ShapeSpec } from './types'

export interface SceneShapeDef {
  /** Stable per-workspace key so provisioning is idempotent (scene re-entry creates nothing). */
  key: string
  label: string
  role: DeviceRole
  spec: ShapeSpec
}

export interface SceneDef {
  n: number
  title: string
  /** Short explainer shown in the scene card. Markdown-ish plain text, 2-4 sentences. */
  body: string
  /** What to try — rendered as hint chips under the explainer. */
  try: string[]
  shapes: SceneShapeDef[]
}

export const SCENES: SceneDef[] = [
  {
    n: 0,
    title: 'Start here',
    body:
      'electric-ivm is a sync engine for Postgres. Apps subscribe to shapes — subsets of your ' +
      'data, defined by a query — and the engine streams every relevant change to them as it ' +
      'happens. This playground shows the machinery doing it: the left panel writes to Postgres, ' +
      'the middle shows the pipeline inside the engine, and the right shows the subscribed ' +
      'screens. Nothing is syncing yet — open scene 1 to create your first live query.',
    try: ['Open scene 1 →'],
    shapes: [],
  },
  {
    n: 1,
    title: 'Your workspace',
    body:
      'This playground runs a real electric-ivm engine on a real Postgres. Everyone shares the same ' +
      'two tables — restaurants and orders — and every row carries a workspace_id. Yours was just ' +
      'minted and seeded. Every shape you see includes `workspace_id = <yours>` in its predicate: ' +
      'that is honest multi-tenancy, and you will see it in every pipeline. If the server gets ' +
      'wiped, the app notices and offers you a fresh workspace — nothing here is precious.',
    try: ['Place an order and watch the delta reach the shape', 'Open the device card to see raw upserts'],
    shapes: [
      {
        key: 'all-orders',
        label: 'All my orders',
        role: 'orders',
        spec: { table: 'orders', where: [] },
      },
    ],
  },
  {
    n: 2,
    title: 'A shape is a filter',
    body:
      'A shape is a query whose result set is maintained for you. Under DBSP every write becomes a ' +
      'weighted delta — an insert is (row, +1), a delete is (row, −1), an update is both — and the ' +
      'shape is a filter operator over that delta stream. Watch the kitchen screen: it subscribes to ' +
      "status = 'cooking'. Deltas that match flow through; deltas that don't die at the filter.",
    try: [
      'Start cooking an order → +1 flows to the kitchen screen',
      'Deliver it → a −1 retraction removes it',
      'Place a new order → watch it get DROPPED at the filter',
    ],
    shapes: [
      {
        key: 'kitchen',
        label: 'Kitchen screen — cooking now',
        role: 'kitchen',
        spec: { table: 'orders', where: [{ col: 'status', op: 'eq', value: 'cooking' }] },
      },
    ],
  },
  {
    n: 3,
    title: 'Shapes share machinery',
    body:
      'Equality queries on the same columns collapse into one shared router: a single ' +
      'index-and-route join keyed by (workspace_id, status) dispatches each delta to exactly the ' +
      'queries registered on its key — one lookup no matter how many exist. The router is ' +
      "genuinely shared with other visitors' queries too: that's the shared ×N badge.",
    try: [
      'Move one order through cooking → riding → delivered and watch it hop between screens',
      'Note the shared ×N badge — one route lookup per write, however many shapes listen',
    ],
    shapes: [
      {
        key: 'rider',
        label: 'Rider phone — out for delivery',
        role: 'rider',
        spec: { table: 'orders', where: [{ col: 'status', op: 'eq', value: 'riding' }] },
      },
      {
        key: 'delivered',
        label: 'Customer history — delivered',
        role: 'customer',
        spec: { table: 'orders', where: [{ col: 'status', op: 'eq', value: 'delivered' }] },
      },
    ],
  },
  {
    n: 4,
    title: 'Subqueries',
    body:
      'Shapes can reach across tables: orders whose restaurant is in Lisbon. The engine maintains ' +
      'the inner SELECT as a shared distinct-set node fed by the restaurants table, and the shape ' +
      'becomes a semijoin against it. The cascade is the point: change a restaurant, and its orders ' +
      'enter or leave the shape without any order row being touched.',
    try: [
      'Move a restaurant to Porto → watch its orders leave the shape',
      'Move it back → they return',
      'Add a restaurant (left panel) to grow the inner set',
    ],
    shapes: [
      {
        key: 'lisbon',
        label: 'Lisbon orders',
        role: 'orders',
        spec: {
          table: 'orders',
          where: [],
          subquery: {
            col: 'restaurant_id',
            inner: { table: 'restaurants', project: 'id', where: [{ col: 'city', op: 'eq', value: 'Lisbon' }] },
          },
        },
      },
    ],
  },
  {
    n: 5,
    title: 'Live aggregations',
    body:
      'An aggregation shape is a running fold: each delta adds weight·value to the maintained ' +
      'scalar (COUNT is just Σ weights), so the dashboard number moves the instant a delta lands — ' +
      'no re-query, no scan. A retraction subtracts precisely what the original contributed.',
    try: ['Deliver an order → revenue ticks up', 'Cancel one → the count ticks down'],
    shapes: [
      {
        key: 'active-count',
        label: 'Ops board — orders cooking',
        role: 'dashboard',
        spec: {
          table: 'orders',
          where: [{ col: 'status', op: 'eq', value: 'cooking' }],
          aggregate: { func: 'count', col: null },
        },
      },
      {
        key: 'revenue',
        label: 'Ops board — delivered revenue',
        role: 'dashboard',
        spec: {
          table: 'orders',
          where: [{ col: 'status', op: 'eq', value: 'delivered' }],
          aggregate: { func: 'sum', col: 'total' },
        },
      },
    ],
  },
  {
    n: 6,
    title: 'Subset queries',
    body:
      'Ordering and windowing are deliberately not shape features — a shape never keeps range ' +
      'state. Instead you ask for a subset: an ordered page over a shape, positioned at an exact ' +
      'LSN. The top-5 board below is a one-shot subset query pinned at the moment you fetched it, ' +
      'while the shape underneath keeps flowing. Refresh it to re-pin at the current LSN.',
    try: ['Place a few big orders, then refresh the top-5 board', 'Note the pinned LSN vs the live feed'],
    shapes: [],
  },
]

export const sceneByN = (n: number): SceneDef | undefined => SCENES.find((s) => s.n === n)
