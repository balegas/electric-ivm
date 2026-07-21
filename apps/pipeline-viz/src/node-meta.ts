// Per-kind identity of the pipeline's nodes: the visual treatment, the dbsp reading of what the
// node computes (formula), and the "inside this operator" explainer shown in the detail panel.
// The copy describes what the engine ACTUALLY executes (see docs/ivm-engine-internals.md §3) —
// the engine hand-rolls most operators AND compiles a real dbsp circuit (the `arr:*` lane) that
// serves template-matching membership shapes and COUNT aggregates outright; the graph shows those
// real structures, one node per maintained thing.

import type { NodeKind } from './build-graph'
import type { GraphShape } from './types'

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
      'The replication source. Each committed change arrives as an envelope on the single ordered ' +
      '`changes` log (streaming pgoutput, whole commits in commit order); the sequencer dispatches ' +
      'it here and turns it into a Z-set delta — insert (new,+1), delete (old,−1), update ' +
      '(old,−1)+(new,+1), with REPLICA IDENTITY FULL supplying the old row. That one delta is ' +
      'shared by every operator downstream. Nothing here stores table rows; the state chip counts ' +
      'envelopes processed and the global change-log convergence offset.',
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
    tag: 'IN-SET DISTINCT · STATE',
    formula: 'distinct(π(proj) · σ(where) · inner)',
    stateful: true,
    inside:
      'A maintained inner set — SELECT proj FROM inner WHERE … — arranged as value → contributing ' +
      'row pks. Identical subqueries share one node (the refcount is its dependents). When a value ' +
      'enters or leaves the set, the flip propagates: dependent shapes re-derive the outer rows ' +
      'that move in or out (reading the compiled dbsp arrangements when present; a circuit-served ' +
      'dependent applies the flip inside the circuit itself), and nested IN nodes reconcile ' +
      'recursively — all within the same batch, so the processed-offset barrier still implies ' +
      'convergence.',
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
    tag: 'γ FOLD · STATE',
    formula: 'fold(Σ value·w over σ(where))',
    stateful: true,
    inside:
      'A scalar aggregation over the matching Z-set — it stores the running aggregate, never the ' +
      'rows. Two serving tiers: a COUNT whose predicate the table’s compiled counts pipeline ' +
      'covers is CIRCUIT-SERVED — seeded by summing the pipeline’s weighted_count groups and ' +
      'updated from group deltas as the circuit steps, with no fold executor at all. Every other ' +
      'aggregate is maintained as an incremental fold: COUNT sums the weights; SUM and AVG add ' +
      'value·weight (NULLs excluded, SQL semantics); MIN and MAX keep a value → net-weight ' +
      'multiset so a retraction can restore the previous extreme. Either way, a new value is ' +
      'emitted only when the result actually changes.',
  },

  // --- circuit-view operators (the engine-emitted exploded decomposition) --------------------
  'op-source': {
    color: '#334155',
    bg: '#e2e8f0',
    tag: 'SOURCE',
    formula: 'sequencer · tail(changes) ⋉ <t>',
    stateful: false,
    inside:
      'The table’s slice of the engine’s single LSN-ordered sequencer: ONE task long-polls the ' +
      'global `changes` log (whole commits, in commit order), de-duplicates redelivered changes by ' +
      '(commit LSN, seq), dispatches each envelope to its table’s executor, and flushes every ' +
      'transaction’s shape appends before the next transaction — atomic per-transaction emission, ' +
      'cross-table. The processed offset its state chip shows is the global change-log position ' +
      '(the convergence barrier).',
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
    formula: 'index{key → …}',
    stateful: true,
    inside:
      'A maintained keyed relation. For a family it is the params arrangement: predicate-key ' +
      'tuples → the shapes registered on them (adding an equality shape is an index insert, not a ' +
      'new circuit). For a subquery shape it is the FEED SET — a host-side per-feed key set ' +
      '(Roaring bitmap) holding the stream’s current pks: candidates are asserted absolutely, and ' +
      'a delete is emitted iff the pk was actually present (check-and-set), so a never-member ' +
      'delete is structurally impossible. Holds no table rows.',
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
    tag: 'γ FOLD · STATE',
    formula: 'fold(Σ value·w)',
    stateful: true,
    inside:
      'The incremental aggregation fold: running count/sum plus, for MIN/MAX, the value → ' +
      'net-weight retraction multiset. Emits a new value only when the result actually changes. ' +
      'A COUNT served from the compiled counts pipeline (solid serving edge from arr:counts) is ' +
      'the exception: its value lives in the circuit’s weighted_count, not in a fold here.',
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

  // --- the compiled dbsp arrangement pipeline (static; always-on infrastructure) --------------
  'arr-input': {
    color: '#4338ca',
    bg: '#e0e7ff',
    tag: 'DBSP INPUT · static',
    formula: 'add_input_zset<Row>',
    stateful: false,
    inside:
      'A table input of the compiled dbsp arrangement circuit — one shared, storage-backed circuit ' +
      'built once at boot (its structure is fixed at construction). The sequencer feeds each ' +
      'replicated transaction here and steps the circuit before fanning the transaction out, so ' +
      'lookups observe post-transaction state. “Seeding” means the initial Postgres snapshot is ' +
      'still loading; until it completes, lookups fall back to Postgres.',
  },
  'arr-index': {
    color: '#4338ca',
    bg: '#e0e7ff',
    tag: 'DBSP ARRANGEMENT · STATE',
    formula: 'map_index(cols) · integrate_trace',
    stateful: true,
    inside:
      'A storage-backed dbsp arrangement: the table’s rows indexed by the named columns, ' +
      'maintained by integrate_trace and spilled to layer files as it grows. It REMEMBERS rows; ' +
      'two edge kinds hang off it. Dashed lookup edges: subquery flip re-derivations do point ' +
      'lookups against its published read-only snapshot instead of querying Postgres back (a ' +
      'missing or unseeded index makes those consumers fall back to Postgres). Solid serving ' +
      'edges: a circuit-served shape is seeded from this index’s snapshot and maintained inside ' +
      'the circuit — the index is its data source, not an occasional read.',
  },
  'arr-counts': {
    color: '#6d28d9',
    bg: '#ede9fe',
    tag: 'DBSP COUNTS · γ STATE',
    formula: 'map_index(group) · weighted_count',
    stateful: true,
    inside:
      'A counts pipeline of the compiled dbsp circuit: the table’s rows grouped by the pipeline’s ' +
      'group columns and reduced by weighted_count — it COMPUTES a maintained count per group, ' +
      'where an index remembers rows. A bare COUNT aggregate whose predicate is an equality ' +
      'cohort over the group columns is served from it: seeded by summing the matching groups’ ' +
      'counts and updated from group deltas as the circuit steps — no fold executor, no Postgres. ' +
      'The solid serving edges point at the aggregates it currently feeds.',
  },
}

/** The serving tier of a shape/aggregate — where its data ACTUALLY comes from — with the detail
 *  prose the panels show. Derived from the engine's own `/graph` fields (`circuit`, `familyKey`,
 *  `isSubquery`), never guessed. */
export function servingTier(s: GraphShape): { label: string; note: string } {
  if (s.circuit) {
    return {
      label: `circuit-served · ${s.circuit.label}`,
      note: s.circuit.counts
        ? 'Circuit-served COUNT: the value is seeded by summing the counts pipeline’s ' +
          'weighted_count groups and updated from group deltas as the circuit steps — no fold ' +
          'executor runs and nothing queries Postgres.'
        : `Circuit-served: seeded from the dbsp circuit’s arrangement snapshots and maintained ` +
          `inside the circuit under the ${s.circuit.label} cohort constraint — deltas are routed ` +
          `by the constraint and membership flips move rows in/out without querying anything back.`,
    }
  }
  if (s.isSubquery) {
    return {
      label: 'subquery registry',
      note:
        'Membership is driven by the shared subquery node(s) upstream plus the outer-row filter — ' +
        'when an inner value flips, the affected rows move in/out of this stream (flip ' +
        're-derivations read the compiled dbsp arrangements when present, else Postgres).',
    }
  }
  if (s.familyKey) {
    return {
      label: `key-routed · family(${s.familyKey.join(', ')})`,
      note:
        'Fed by the shared route join upstream — the engine keeps only this shape’s routing entry ' +
        'and snapshot gate, no table rows.',
    }
  }
  if (s.aggregate) {
    return {
      label: 'standalone fold',
      note:
        'Maintained by the incremental fold executor: σ(where) over each delta feeds the running ' +
        'aggregate — the rows themselves are never stored.',
    }
  }
  return {
    label: 'standalone (stateless eval)',
    note: 'Fed by its standalone filter — enter/leave falls out of evaluating each delta; no state is kept.',
  }
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
