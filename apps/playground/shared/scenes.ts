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
  /** Explainer shown in the scene card (logical view). `\n\n` splits paragraphs. */
  body: string
  /** Alternative explainer shown while the dbsp-circuit view is active: the same scene, told in
   *  operator terms. Falls back to `body` when absent. */
  dbsp?: string
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
      'screens. Nothing is syncing yet — open scene 1 to create your first live query.' +
      '\n\n' +
      'The engine is built on DBSP, a theory of incremental computation: data is a collection ' +
      'where every row carries a signed weight, a change is a tiny delta (+1 insert, −1 delete), ' +
      'and a query is a circuit of operators that processes deltas instead of re-running — so the ' +
      'cost of keeping a query live scales with the change, not the data. The canvas has two ' +
      'views: Logical shows what is connected to what; "dbsp circuit" shows the same pipeline as ' +
      'its actual operators. Each scene explains both — toggle the view to switch explanations.',
    try: ['Open scene 1 →'],
    shapes: [],
  },
  {
    n: 1,
    title: 'Your workspace',
    body:
      'This playground runs a real electric-ivm engine on a real Postgres. Your workspace was ' +
      'just minted and seeded with a restaurant and a few orders. If the server gets wiped, the ' +
      'app notices and offers you a fresh one — nothing here is precious.' +
      '\n\n' +
      'Everyone here shares the same database — the same two tables, restaurants and orders. ' +
      'Multi-tenancy is workspace isolation, and workspace isolation is itself just a shape ' +
      'filter: every query you see carries `workspace_id = <yours>` in its predicate. Data that ' +
      "isn't yours never reaches you for the same reason a delivered order never reaches the " +
      'kitchen screen — the filter drops it.' +
      '\n\n' +
      'On the canvas: the grey TABLE node is the replication source — every committed write to ' +
      'orders becomes a change event. The blue ROUTER dispatches each change by key. The green ' +
      'LIVE QUERY is the output your device card (right) subscribes to. Click any node for its ' +
      'details and live contents.',
    dbsp:
      'This is the same pipeline as its actual operators. The Z-SET SOURCE is your orders table ' +
      'as a weighted collection. Δ turns each write into a delta: insert (row, +1), delete ' +
      '(row, −1), update both. ↦ INDEX arranges the delta by its key columns; ⋈ JOIN dispatches ' +
      'it against the params ARRANGEMENT (the stateful key → queries map); π MAP groups the ' +
      'output by primary key into upsert/delete messages; the SINK is the stream your device ' +
      'card reads.' +
      '\n\n' +
      'Note what is missing: no operator stores table rows. The only state is the small ' +
      'arrangements — that is why engine memory stays flat as data grows.',
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
      "status = 'cooking'. Deltas that match flow through; deltas that don't die at the filter." +
      '\n\n' +
      'On the canvas: a green +1 dot is a row entering a query, a red −1 dot is a retraction, and ' +
      'a red ✕ marks a change that matched nothing and was dropped — the engine doing no work is ' +
      'half the point. Toggle "dbsp circuit" (top) to see the same pipeline as its raw operators: ' +
      'Δ change, ↦ index, ⋈ join, π map.',
    dbsp:
      'In operator terms a filter is the simplest case: σ keeps the weighted rows whose predicate ' +
      'is true and discards the rest. It holds no state at all — one predicate check per delta ' +
      'row. Enter/leave falls out for free: an update is (old, −1), (new, +1); if only the new ' +
      'row matches, the query gains a row; if only the old one did, the −1 flows through and ' +
      'becomes a delete downstream. Nothing is ever re-queried.',
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
      "genuinely shared with other visitors' queries too: that's the shared ×N badge." +
      '\n\n' +
      'On the canvas: your three queries now hang off ONE router node. Click it to see the ' +
      'routing index — key → query, other visitors included (their rows stay private; only the ' +
      'machinery is shared). Faint grey pulses through shared nodes are other visitors changing ' +
      'their own data.',
    dbsp:
      'Sharing is an arrangement. The params state (key → queries) is maintained once, and one ' +
      'incremental ⋈ JOIN dispatches every delta against it — the dashed edge is that stateful ' +
      'arrangement feeding the join. Registering another query just adds a key to the ' +
      'arrangement; the join does not get slower. Cost scales with change volume, not with how ' +
      'many queries subscribe — that is the whole economics of shape sharing.',
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
      'enter or leave the shape without any order row being touched.' +
      '\n\n' +
      'On the canvas: a second table (restaurants) now feeds a purple SUBQUERY NODE — the live ' +
      'set of Lisbon restaurant ids. The dashed edge is its dependency into the orders query. ' +
      'Click the node to watch the inner set itself change as you move restaurants.',
    dbsp:
      'The subquery is two circuits joined. Restaurants flow through σ (the inner WHERE) and ' +
      '↦ INDEX into DISTINCT — a stateful arrangement of the inner value set. The orders side ' +
      '⋈ semijoins against it: a row is kept iff its restaurant_id is in the set. When a value ' +
      'enters or leaves DISTINCT, the join re-derives exactly the affected outer rows — that is ' +
      'the cascade, and it costs one flip, not a re-scan.',
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
      'no re-query, no scan. A retraction subtracts precisely what the original contributed.' +
      '\n\n' +
      'On the canvas: the teal Σ AGGREGATION node stores the running scalar — never the rows. ' +
      'Its device card shows a single number instead of a list; an empty SUM shows — (SQL: the ' +
      'sum of nothing is NULL).',
    dbsp:
      'Σ FOLD is the stateful operator here, and its state is one number. It consumes weighted ' +
      'rows and adds weight·value to the running scalar — COUNT is just the sum of the weights, ' +
      'and a retraction is a negative weight, subtracting exactly what the original contributed. ' +
      'MIN/MAX keep an ordered multiset so retracting the current extreme restores the previous ' +
      'one. No rows are stored, and the number is never recomputed from scratch.',
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
      'LSN. The top-5 board keeps its answer pinned at the moment you fetched it, while the live ' +
      'queries around it keep flowing. Refresh it to re-pin at the current LSN.' +
      '\n\n' +
      'On the right: the 🏆 top-5 board is the new component — note it is a card, not a canvas ' +
      'node. It shows the LSN its answer is pinned at; compare it with the live cards below, ' +
      'which move on their own.',
    dbsp:
      'Look at the circuit: the subset query is not in it. That is the point — ordering and ' +
      'windowing keep no incremental state, so they get no operator. The page is computed once ' +
      'against Postgres, stamped with the LSN of that instant, and handed to the client; the ' +
      'circuits around it keep processing deltas. A client that wants both uses the pinned page ' +
      'for layout and the live query for changes past that LSN.',
    try: ['Place a few big orders, then refresh the top-5 board', 'Note the pinned LSN vs the live feed'],
    shapes: [],
  },
]

export const sceneByN = (n: number): SceneDef | undefined => SCENES.find((s) => s.n === n)
