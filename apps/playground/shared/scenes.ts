// The walkthrough scenes: explainer copy (client) + the shapes each scene provisions (server).
// One module so the story and the provisioning can never drift apart. Scenes only ADD shapes —
// data and earlier shapes persist; the composer and grids work in every scene. Each scene has a
// logical-view explainer (`body`) and an operator-level one (`dbsp`) shown in the circuit view.

import type { ShapeSpec } from './types.ts'

export interface SceneShapeDef {
  /** Stable per-workspace key so provisioning is idempotent (scene re-entry creates nothing). */
  key: string
  label: string
  spec: ShapeSpec
}

export interface SceneDef {
  n: number
  title: string
  /** Explainer shown in the scene card (logical view). `\n\n` splits paragraphs. */
  body: string
  /** Alternative explainer while the dbsp-circuit view is active. Falls back to `body`. */
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
      'happens. This playground drives the Shape API directly against a tiny issue tracker: the ' +
      'left panel edits real rows in Postgres, the middle shows the pipeline inside the engine, ' +
      'and the right shows each shape’s live results. Nothing is syncing yet — open scene 1 ' +
      'to create your first shape.' +
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
    title: 'Your first shape',
    body:
      'A shape is defined by a query. This scene proposes one — todo issues. Press "Create the ' +
      'shape" and it is registered through the Shape API; the engine answers with a pipeline: it ' +
      'backfills the current matches, then maintains the result incrementally. Expand "API ' +
      'request" on the result card to see the exact call.' +
      '\n\n' +
      'On the canvas: the grey TABLE node is the replication source — every committed write to ' +
      'issues becomes a change event. The blue ROUTER dispatches each change by key. The green ' +
      'LIVE QUERY is the output the result card (right) subscribes to. Click any node for its ' +
      'details and live contents.',
    dbsp:
      'This is the same pipeline as its actual operators. The Z-SET SOURCE is your issues table ' +
      'as a weighted collection. Δ turns each write into a delta: insert (row, +1), delete ' +
      '(row, −1), update both. ↦ INDEX arranges the delta by its key columns; ⋈ JOIN dispatches ' +
      'it against the params ARRANGEMENT (the stateful key → queries map); π MAP groups the ' +
      'output by primary key into upsert/delete messages; the SINK is the stream the result card ' +
      'reads.' +
      '\n\n' +
      'Note what is missing: no operator stores table rows. The only state is the small ' +
      'arrangements — that is why engine memory stays flat as data grows.',
    try: ['Add an issue (left) and watch the delta reach the shape', 'Open the result card’s API request'],
    shapes: [
      {
        key: 'todo',
        label: 'Todo issues',
        spec: { table: 'issues', where: [{ col: 'status', op: 'eq', value: 'todo' }] },
      },
    ],
  },
  {
    n: 2,
    title: 'Deltas and drops',
    body:
      'Under DBSP every write becomes a weighted delta — an insert is (row, +1), a delete is ' +
      '(row, −1), an update is both — and the shape is a filter over that delta stream. Move an ' +
      'issue out of todo and the shape emits a retraction; changes that match nothing die on the ' +
      'way and reach no one.' +
      '\n\n' +
      'On the canvas: a green +1 dot is a row entering a query, a red −1 dot is a retraction, ' +
      'and a red ✕ marks a change that matched nothing and was dropped — the engine doing no ' +
      'work is half the point.',
    dbsp:
      'In operator terms a filter is the simplest case: σ keeps the weighted rows whose ' +
      'predicate is true and discards the rest — no state, one predicate check per delta row. ' +
      'Enter/leave falls out for free: an update is (old, −1), (new, +1); if only the new row ' +
      'matches, the query gains a row; if only the old one did, the −1 flows through and becomes ' +
      'a delete downstream. Nothing is ever re-queried.',
    try: [
      'Set a todo issue to in_progress → a −1 retraction leaves the shape',
      'Edit a done issue’s priority → the change is DROPPED (✕): no shape wants it',
    ],
    shapes: [],
  },
  {
    n: 3,
    title: 'Shapes share machinery',
    body:
      'Equality queries on the same columns collapse into one shared router: a single ' +
      'index-and-route join keyed by (status, …) dispatches each delta to exactly the queries ' +
      'registered on its key — one lookup no matter how many exist.' +
      '\n\n' +
      'On the canvas: your three status queries hang off ONE router node. Click it to see the ' +
      'routing index — key → query. Create your own shape (＋ shape, top right) on status and it ' +
      'joins the same router.',
    dbsp:
      'Sharing is an arrangement. The params state (key → queries) is maintained once, and one ' +
      'incremental ⋈ JOIN dispatches every delta against it — the dashed edge is that stateful ' +
      'arrangement feeding the join. Registering another query just adds a key to the ' +
      'arrangement; the join does not get slower. Cost scales with change volume, not with how ' +
      'many queries subscribe — that is the whole economics of shape sharing.',
    try: [
      'Walk an issue todo → in_progress → done and watch it hop between result cards',
      'One write, one route lookup — however many shapes listen',
    ],
    shapes: [
      {
        key: 'in-progress',
        label: 'In progress',
        spec: { table: 'issues', where: [{ col: 'status', op: 'eq', value: 'in_progress' }] },
      },
      {
        key: 'done',
        label: 'Done',
        spec: { table: 'issues', where: [{ col: 'status', op: 'eq', value: 'done' }] },
      },
    ],
  },
  {
    n: 4,
    title: 'Subqueries',
    body:
      'Shapes can reach across tables: issues whose project belongs to the web team. The engine ' +
      'maintains the inner SELECT as a shared distinct-set node fed by the projects table, and ' +
      'the shape becomes a semijoin against it. The cascade is the point: move a project to ' +
      'another team and its issues enter or leave the shape without any issue row being touched.' +
      '\n\n' +
      'On the canvas: a second table (projects) now feeds a purple SUBQUERY NODE — the live set ' +
      'of web-team project ids. The dashed edge is its dependency into the issues query. Click ' +
      'the node to watch the inner set itself change as you reassign teams.',
    dbsp:
      'The subquery is two circuits joined. Projects flow through σ (the inner WHERE) and ' +
      '↦ INDEX into DISTINCT — a stateful arrangement of the inner value set. The issues side ' +
      '⋈ semijoins against it: a row is kept iff its project_id is in the set. When a value ' +
      'enters or leaves DISTINCT, the join re-derives exactly the affected outer rows — that is ' +
      'the cascade, and it costs one flip, not a re-scan.',
    try: [
      'Change a project’s team (left) → its issues leave the shape',
      'Change it back → they return',
      'Add a project to grow the inner set',
    ],
    shapes: [
      {
        key: 'web-issues',
        label: 'Web team issues',
        spec: {
          table: 'issues',
          where: [],
          subquery: {
            col: 'project_id',
            inner: { table: 'projects', project: 'id', where: [{ col: 'team', op: 'eq', value: 'web' }] },
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
      'scalar (COUNT is just Σ weights), so the number moves the instant a delta lands — no ' +
      're-query, no scan. A retraction subtracts precisely what the original contributed.' +
      '\n\n' +
      'On the canvas: the teal Σ AGGREGATION node stores the running scalar — never the rows. ' +
      'Its result card shows a single number instead of a list.',
    dbsp:
      'Σ FOLD is the stateful operator here, and its state is one number. It consumes weighted ' +
      'rows and adds weight·value to the running scalar — COUNT is just the sum of the weights, ' +
      'and a retraction is a negative weight, subtracting exactly what the original contributed. ' +
      'MIN/MAX keep an ordered multiset so retracting the current extreme restores the previous ' +
      'one. No rows are stored, and the number is never recomputed from scratch.',
    try: ['Add an issue → the todo count ticks up', 'Finish one → it moves to the done count'],
    shapes: [
      {
        key: 'todo-count',
        label: 'Todo count',
        spec: {
          table: 'issues',
          where: [{ col: 'status', op: 'eq', value: 'todo' }],
          aggregate: { func: 'count', col: null },
        },
      },
      {
        key: 'max-priority',
        label: 'Highest open priority',
        spec: {
          table: 'issues',
          where: [{ col: 'status', op: 'neq', value: 'done' }],
          aggregate: { func: 'max', col: 'priority' },
        },
      },
    ],
  },
  {
    n: 6,
    title: 'Subset queries',
    body:
      'Ordering and windowing are deliberately not shape features — a shape never keeps range ' +
      'state. Instead you ask for a subset: an ordered page, positioned at an exact LSN. The ' +
      'top-5 board keeps its answer pinned at the moment you fetched it, while the live queries ' +
      'around it keep flowing. Refresh it to re-pin at the current LSN.' +
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
    try: ['Add a few high-priority issues, then refresh the top-5 board', 'Note the pinned LSN vs the live cards'],
    shapes: [],
  },
]

export const sceneByN = (n: number): SceneDef | undefined => SCENES.find((s) => s.n === n)
