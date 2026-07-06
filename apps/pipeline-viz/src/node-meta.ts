// Per-kind identity of the pipeline's nodes: the visual treatment, the dbsp reading of what the
// node computes (formula), and the "inside this operator" explainer shown in the detail panel.
// The copy describes what the engine ACTUALLY executes (see docs/ivm-engine-internals.md §3) —
// this engine hand-rolls its operators rather than running a compiled dbsp circuit, and the graph
// shows those real structures, one node per maintained thing.

import type { NodeKind } from './build-graph'

export interface KindMeta {
  color: string
  bg: string
  /** Header tag on the node card (operator glyph + role; STATE marks a stateful arrangement). */
  tag: string
  /** The dbsp reading of what this node computes, shown as the card's formula line. */
  formula: string
  /** Whether the node holds incremental state (an arrangement / fold) or is stateless. */
  stateful: boolean
  /** Detail-panel prose: what happens inside this operator, per change. */
  inside: string
}

export const KIND_META: Record<NodeKind, KindMeta> = {
  table: {
    color: '#334155',
    bg: '#e2e8f0',
    tag: 'TABLE · Δ SOURCE',
    formula: 'change → {(old,−1), (new,+1)}',
    stateful: false,
    inside:
      'The replication source. Each committed change arrives as an envelope on the table stream; ' +
      'the tailer turns it into a Z-set delta — insert (new,+1), delete (old,−1), update ' +
      '(old,−1)+(new,+1), with REPLICA IDENTITY FULL supplying the old row. That one delta is ' +
      'shared by every operator downstream. Nothing here stores table rows; the state chip counts ' +
      'envelopes processed and the convergence offset.',
  },
  filter: {
    color: '#b45309',
    bg: '#fef3c7',
    tag: 'σ FILTER · stateless',
    formula: 'σ(where) · π(columns)',
    stateful: false,
    inside:
      'A standalone predicate (range / OR / NOT / inequality — anything that is not a pure ' +
      'equality) evaluated directly on each delta tuple under SQL three-valued logic (a NULL ' +
      'operand excludes the row). Stateless: no index, no arrangement, no table copy — O(1) ' +
      'predicate evaluations per change. Matching (row, weight) pairs continue to the shape output.',
  },
  family: {
    color: '#0369a1',
    bg: '#e0f2fe',
    tag: '↦⋈ ROUTE JOIN · STATE',
    formula: 'key(Δrow) ⋈ index{key → shapes}',
    stateful: true,
    inside:
      'One shared router for every equality shape on the same key columns — WHERE key = const ' +
      'compiles to an entry in this routing index, not a per-shape circuit. The index maps ' +
      'predicate-key tuples to the shapes registered on them and holds no table rows. A delta is ' +
      'routed by its key to exactly the matching shapes in O(log N), independent of shape count: ' +
      'the dbsp equivalent of one semijoin against a params arrangement for the whole family.',
  },
  sqnode: {
    color: '#7e22ce',
    bg: '#f3e8ff',
    tag: 'IN-SET ARRANGE · STATE',
    formula: 'distinct(π(proj) · σ(where) · inner)',
    stateful: true,
    inside:
      'A maintained inner set — SELECT proj FROM inner WHERE … — arranged as value → contributing ' +
      'row pks. Identical subqueries share one node (the refcount is its dependents). When a value ' +
      'enters or leaves the set, the flip propagates: dependent shapes re-derive the outer rows ' +
      'that move in or out, and nested IN nodes reconcile recursively — all within the same batch, ' +
      'so the processed-offset barrier still implies convergence.',
  },
  shape: {
    color: '#166534',
    bg: '#dcfce7',
    tag: 'SHAPE OUT · π',
    formula: 'π(pk → upsert | delete)',
    stateful: false,
    inside:
      'The shape’s output stream. The output Z-set is grouped by primary key into envelopes — ' +
      'any positive weight becomes an upsert, a purely negative one a delete — stamped with the ' +
      'originating txid and commit LSN so subscribers can position their live tails. Equal shape ' +
      'definitions share this one stream, ref-counted; the state chip counts envelopes emitted ' +
      '(backfill + live).',
  },
  agg: {
    color: '#0d9488',
    bg: '#ccfbf1',
    tag: 'Σ FOLD · STATE',
    formula: 'fold(Σ value·w over σ(where))',
    stateful: true,
    inside:
      'A scalar aggregation maintained as an incremental fold over the matching Z-set — it stores ' +
      'the running aggregate, never the rows. COUNT sums the weights; SUM and AVG add value·weight ' +
      '(NULLs excluded, SQL semantics); MIN and MAX keep a value → net-weight multiset so a ' +
      'retraction can restore the previous extreme. O(1) per change plus a log-factor for MIN/MAX; ' +
      'a new value is emitted only when the fold’s result actually changes.',
  },

  // --- circuit-view operators (the engine-emitted exploded decomposition) --------------------
  'op-source': {
    color: '#334155',
    bg: '#e2e8f0',
    tag: 'SOURCE',
    formula: 'tail(table/<t>)',
    stateful: false,
    inside:
      'The per-table tailer task: long-polls the table’s durable stream, de-duplicates redelivered ' +
      'changes by (commit LSN, seq), and publishes the processed offset after each batch is fully ' +
      'fanned out — the convergence barrier its state chip shows.',
  },
  'op-delta': {
    color: '#c2410c',
    bg: '#ffedd5',
    tag: 'Δ CHANGE',
    formula: 'env → {(old,−1), (new,+1)}',
    stateful: false,
    inside:
      'Turns one replicated envelope into a weighted Z-set delta: insert (new,+1), delete (old,−1), ' +
      'update (old,−1)+(new,+1) — REPLICA IDENTITY FULL supplies the old row, so no table copy is ' +
      'needed to retract. This one delta is shared by every operator downstream.',
  },
  'op-filter': {
    color: '#b45309',
    bg: '#fef3c7',
    tag: 'σ FILTER',
    formula: 'σ(where)',
    stateful: false,
    inside:
      'A stateless predicate applied directly to each delta tuple under SQL three-valued logic ' +
      '(a NULL operand excludes the row). No state, no arrangement — O(1) evaluations per change.',
  },
  'op-key': {
    color: '#0f766e',
    bg: '#ccfbf1',
    tag: '↦ KEY',
    formula: 'row ↦ key(cols)',
    stateful: false,
    inside:
      'Extracts the family’s key tuple from each delta row (positional projection of the key ' +
      'columns) — the join key the route join looks up in the params arrangement.',
  },
  'op-arrange': {
    color: '#7e22ce',
    bg: '#f3e8ff',
    tag: 'ARRANGE · STATE',
    formula: 'index{key → shapes}',
    stateful: true,
    inside:
      'The family’s params arrangement: predicate-key tuples → the shapes registered on them. ' +
      'Adding an equality shape is an index insert, not a new circuit. Holds no table rows — its ' +
      'chip shows the live key/shape cardinality.',
  },
  'op-join': {
    color: '#1d4ed8',
    bg: '#dbeafe',
    tag: '⋈ JOIN',
    formula: 'Δ ⋈ arrangement',
    stateful: false,
    inside:
      'A join of the incoming delta against a maintained arrangement. For a family it is the route ' +
      'join — key lookup dispatches the delta to exactly the matching shapes, O(log N) independent ' +
      'of shape count. For a subquery shape it is the membership semijoin/antijoin: the outer ' +
      'predicate’s IN leaves resolve against the node arrangements it is wired to (dashed edges).',
  },
  'op-distinct': {
    color: '#7e22ce',
    bg: '#f3e8ff',
    tag: 'DISTINCT · STATE',
    formula: 'distinct(values)',
    stateful: true,
    inside:
      'The subquery node’s maintained arrangement: projected value → contributing inner-row pks. ' +
      'A value enters when its first contributor appears and leaves when the last one goes — each ' +
      'flip propagates to every dependent join. Shared by all dependents (the refcount).',
  },
  'op-fold': {
    color: '#0d9488',
    bg: '#ccfbf1',
    tag: 'Σ FOLD · STATE',
    formula: 'fold(Σ value·w)',
    stateful: true,
    inside:
      'The incremental aggregation fold: running count/sum plus, for MIN/MAX, the value → ' +
      'net-weight retraction multiset. Emits a new value only when the result actually changes.',
  },
  'op-project': {
    color: '#475569',
    bg: '#e2e8f0',
    tag: 'π PROJECT',
    formula: 'group by pk → envelope',
    stateful: false,
    inside:
      'Groups the output Z-set by primary key into State-Protocol envelopes — any positive weight ' +
      'becomes an upsert, a purely negative one a delete — projecting the shape’s SELECT-list and ' +
      'stamping the originating txid + commit LSN.',
  },
  'op-sink': {
    color: '#166534',
    bg: '#dcfce7',
    tag: 'SINK',
    formula: 'append(shape/<id>)',
    stateful: false,
    inside:
      'The shape’s output stream. Appends are reliable (retry-until-landed) so the processed-offset ' +
      'barrier really means every subscriber stream reflects the batch. Equal shape definitions ' +
      'share this one stream, ref-counted; the chip counts envelopes appended (backfill + live).',
  },
}

/** Compact scalar for the live agg chip / stat card (rounds long floats, e.g. AVG). */
export function fmtScalar(v: unknown): string {
  if (v === null || v === undefined) return 'NULL'
  if (typeof v === 'number') {
    if (Number.isInteger(v)) return String(v)
    return v.toFixed(Math.abs(v) >= 100 ? 1 : 2)
  }
  return String(v)
}
