# Subqueries in the DBSP circuit as shared, parameterized templates

**Bead:** dbsp-ds-jq6 · **Date:** 2026-07-14 · **Status:** approved design, pre-implementation

## 1. Problem

Subquery (`[NOT] IN`) membership is served entirely outside the DBSP circuit by a hand-rolled
kernel: `subquery.rs`'s `SubqueryRegistry` keeps `contributors: HashMap<Value, HashSet<pk>>`
maps per inner-query node, detects flips by reconcile-by-identity, and shares a node only when
two shapes have the **byte-identical** canonical signature — the literal comparison value is
baked into `subquery_sig`, so `owner = 1` and `owner = 2` get separate nodes even though they
are the same query shape. The circuit (`arrangements.rs`) runs only counts pipelines.

Two costs follow:

1. **Per-delta eval scales with node count.** A delta on an inner table is evaluated once per
   node on that table (each node's predicate embeds its literal). 500 subscribed users on
   `project_members WHERE user_id = <me>` ⇒ 500 predicate evals per change.
2. **No structural sharing.** Each literal value duplicates the pipeline bookkeeping, unlike
   the equality tier, where one `KeyRouter` family (keyed by column set) serves every literal
   as a routing key.

This design moves flip detection into the circuit as **parameterized templates** — one compiled
subplan per distinct subquery *shape*, literals treated as parameters — and answers the bead's
three open questions:

- **How a new subplan enters a running circuit:** it doesn't need to. The circuit structure is
  fixed at boot (one generic subquery pipeline per replicated table); templates and binds are
  runtime *data* inside the operators. No rebuilds, no recompilation windows.
- **How registry semantics map onto compiled state:** flip detection (contributor sets,
  reconcile) is replaced by a linear operator + `distinct`; everything above flip detection
  (three-phase creation, absolute emission, `known_members`, flip workers, emission lanes)
  is kept as-is and fed by circuit output deltas.
- **NULL-sensitive negation:** keeps the query-back side-channel by design. There is no clean
  DBSP-operator equivalent without arranging the outer table, which this design rules out.

## 2. Decisions taken (with rationale)

| decision | choice | rationale |
|---|---|---|
| Deliverable | design + full implementation | user decision on bead scope |
| Outer-side state | **inner side only** — circuit maintains inner membership sets; outer rows stay in Postgres, flips still query-back | preserves the PR #27 architecture ("Postgres is the only row store"); a real semijoin would reintroduce O(rows) engine state |
| Rollout | **direct replacement** — one membership implementation, no flag | avoids dual-path drift; conformance suites gate the branch |
| Circuit dynamism | **templates as data** in a fixed per-table pipeline (approach A) | no rebuild cost, no per-template threads; rejected: rebuild windows (creation storms get worse — see dbsp-ds-ht9) and per-template mini-circuits (threads/step latency grow with templates) |
| Arrangement coverage | **bind-gated** — the flat_map emits only for registered binds; each new bind seeds its slice from Postgres | memory parity with today (proportional to subscriptions); rejected eager whole-template coverage (near-free bind creation but O(rows passing residual) memory per template) |

## 3. Template identity & parameterization

Replace the literal-baked `subquery_sig` with a two-level identity, mirroring how
`equality_template` (`predicate.rs:231`) factors literals out of the KeyRouter family key:

- **Template** = `(inner_table, proj_col, param_cols, residual_pred)`. Top-level AND conjuncts
  of the inner WHERE that are non-NULL equality leaves over distinct columns are lifted out as
  **parameter columns**; everything else (ranges, OR, NOT, IS NULL, nested IN) stays in the
  canonical residual with its literals baked in. A predicate with no liftable conjunct is a
  template with `param_cols = []` and a single unit bind.
- **Bind** = the tuple of parameter literal values, refcounted per dependent (occupancy model,
  like `router.index`). `user_id = 1` and `user_id = 2` ⇒ one template, two binds.

Sharing is strictly ≥ today's: identical-literal dedup becomes bind-level dedup, and distinct
literals now share the template's compiled structure and its single per-delta evaluation.

Per-delta cost on the inner table drops from O(nodes on table) predicate evals to **one
residual eval + one hash lookup** on the projected param values, fanning out only to the
affected bind — the route-join win, applied to subqueries.

## 4. Circuit pipeline (one per table, built at boot)

Table schemas are known at boot, so **every replicated table** gets one generic subquery
pipeline at circuit construction; tables with no templates carry empty state and cost nothing.

```
subq input (Row zset per table, own input handle — counts inputs untouched)
  → flat_map over shared template/bind set:        [linear operator]
      per template on this table:
        gate-check (bind's SnapshotGate vs the txn stamp side-channel)
        eval residual (three-valued), project param values
        if bind registered & live: emit key=(template_id, bind, proj_value), val=inner_pk
  → project away pk → distinct                     [stateful: the membership sets]
  → output deltas = flips (∅→non-empty = Enter, →∅ = Leave)
  → integrate_trace snapshot published per step    [serves contains()/has_null() reads]
```

Mechanics:

- **Linearity replaces `reconcile_row`.** value-of-row is a pure function of the row, and
  deltas carry old images (REPLICA IDENTITY FULL), so `(row,−1),(row′,+1)` nets contributor
  weights correctly. The circuit thread's `(lsn,seq)` highwater already prevents double-apply
  under at-least-once delivery. The `pk_value` reverse index is no longer needed.
- **Distinct's output deltas are exactly today's flips**, drained after each step (which
  already runs before fan-out) and handed to the unchanged downstream machinery.
- **Membership reads** (`contains`/`has_null` inside `matches_ctx`) move from registry HashMaps
  to seeking the published spine snapshot — the same read-your-writes pattern `count_groups`
  uses (`arrangements.rs:211`).
- **Gate fencing inside the circuit.** A transaction has one xid, so the sequencer sets a
  per-transaction stamp cell before appending the batch; the flat_map closure reads it and
  applies each bind's `SnapshotGate`. Sound under the two invariants this design asserts:
  single-worker circuit, one dbsp transaction per source transaction. Subquery pipelines get
  their own input handles so the counts pipelines' existing table-level pre-gating
  (`stamped_delta_for_arrangements`) is untouched.
- **Adjust input** (per table): a secondary input of pre-evaluated
  `(template_id, bind, proj_value, pk, ±1)` tuples unioned ahead of the distinct. It serves
  bind seeding, gated replay, retraction on bind drop, and nested-subquery reconciliation.

State size: the distinct's integrated trace over `(template, bind, value, pk)` has the same
cardinality as today's contributor sets (bind-gated), plus the projected `(template, bind,
value)` trace — O(distinct values), small.

## 5. Creation lifecycle (three-phase, mapped onto the circuit)

The orchestration in `lifecycle.rs:895` keeps its shape; what each phase touches changes:

- **Phase A (`begin_create`, under registry lock, in-memory):** compile the outer predicate;
  per IN leaf, resolve/insert the template and refcount the bind. A fresh bind registers as
  **pending**: the flat_map drops its live deltas (no gate yet) and the registry buffers the
  raw table deltas for it outside the circuit — today's `seed_buffer`, relocated. The conflict
  pre-check carries over: joining a *shared* bind still mid-seed bails and the caller retries
  (same 100×20 ms loop).
- **Phase B (no lock, all PG I/O):** one pooled query per fresh bind —
  `SELECT pk, proj FROM inner WHERE residual AND <one equality per param col>` — returning
  rows + a `SnapshotGate`. Outer backfill unchanged.
- **Phase C (`finish_create`):** serialized through the circuit command channel so it cannot
  interleave with a transaction step: (1) feed the snapshot tuples through the adjust input;
  (2) replay the buffered deltas, evaluated against just this bind and gate-filtered, as
  adjust tuples; (3) install the gate and flip the bind live in the flat_map set; (4) step.
  The drained output deltas are the replay flips, enqueued to the propagator barrier-covered —
  the same contract as today's `finish_create` return value.
- **Abort:** roll back bind refcounts and pending entries; nothing touched distinct state yet,
  so the circuit needs no rollback.
- **Bind drop (last dependent leaves):** read the bind's slice from the published snapshot,
  feed the negated tuples through the adjust input, unregister the bind. A template is removed
  when its last bind goes (occupancy-refcounted, like KeyRouter families). This retraction is
  what keeps dropped subscriptions from leaking arrangement state.

## 6. Live path & ordering

Per-transaction sequencer flow: set the txn stamp → `apply_batch` (subquery inputs fed for any
table with live templates, not just counts tables) → step → drain count deltas **and** flip
deltas → enqueue flips to the propagator → fan out to routed/standalone shapes and do
outer-side subquery-shape emission, which now reads the fresh post-step membership snapshot.

Invariants preserved structurally (ARCHITECTURE §5–6b, AGENTS.md invariants):

- circuit steps before fan-out (aggregates and now membership state are fresh within the txn);
- absolute per-pk emission, `known_members` gating of never-member deletes (PR #30), and
  per-shape emission lanes are untouched — flips just arrive from a different producer;
- `pendingFlips` still covers in-flight propagation + unlanded lane batches;
- sequencer `(lsn,seq)` de-dup and per-txn atomic flush unchanged.

Named honestly: every table with a live template now pays the circuit round-trip (bounded
channel + blocking step) on the hot path, where today only counts tables do. Mitigation is
already in the design (feed only tables with live templates); fleet benchmarks are the gate.

## 7. Nested subqueries & NULL handling

- **Nested IN.** A parent template whose residual contains `col IN (child)` evaluates against
  the child's *published* (pre-step) snapshot — one step stale, which is exactly today's
  semantics (nodes evaluate before child flips propagate). Correction is the existing
  machinery: child flips travel node→node edges (now keyed `(template_id, bind)`), the flip
  worker query-backs the parent's inner rows for the flipped value, and the reconciliation
  lands as adjust-input tuples for the parent. Convergent by identity, as today.
- **NULL bucket.** `proj_value = NULL` is an ordinary key in the arrangement; `has_null` is a
  snapshot seek. A NULL flip on a NULL-sensitive edge (leaf negated or under any `Not{…}`)
  triggers the existing `rederive_dependent` full re-derive, unchanged. **The bead's NULL
  question is answered: NULL-sensitive negation keeps the query-back side-channel by design** —
  a pure-operator equivalent needs the outer relation arranged in the circuit, which the
  inner-side-only decision excludes.

## 8. Code map

**Deleted from `subquery.rs`:** `SubqueryNode.contributors`/`pk_value`, `reconcile_row`,
`contains`/`has_null` on nodes, `node_present_values`, `apply_node_flips`, per-node
`seed_buffer` internals, the literal-keyed node map. `SubqueryEval` re-points at circuit
snapshots.

**Kept:** `SubqueryShape` + `known_members`, edges + `Dependent`, pending-shape buffering,
`filter_known_members`, `emit_shape_delta` / `move_shape_for_value` / `rederive_dependent`,
`propagate_flips`, three-phase orchestration (`lifecycle.rs`), `membership.rs` query-backs,
`emission.rs` lanes. Catalog behavior unchanged: subquery shapes are still dropped at boot
(circuit state is in-memory only). Bead dbsp-ds-pg5 stays open; its eventual answer becomes
"whatever checkpoint story the circuit grows", as that bead anticipated.

**New:** `predicate.rs` — template extraction (param-lifting canonicalization, bind
extraction, template sig). `arrangements.rs` — per-table subquery pipelines, template/bind
registration commands, adjust input, membership snapshot publication, flip drain.
`circuit_serving.rs` — txn stamp side-channel, flip-delta → `Flip` conversion.

## 9. Observability & config

- `/graph`: `subq:template:<table>:<sig>` nodes with bind counts replace per-node `sq:` nodes;
  `GET /subqueries` reports template → binds → dependents; `/state` exposes per-template
  arrangement cardinalities from the snapshot.
- No new env vars. Subquery pipelines are always-on per table (empty until a template
  registers). `ELECTRIC_CIRCUITS_FLIP_WORKERS` / `ELECTRIC_CIRCUITS_EMIT_LANES` unchanged.
- ARCHITECTURE.md §6b's "circuit structure is fixed at boot" limitation is rewritten to apply
  only to counts specs; subquery templates are runtime data. The blog-review claim that
  prompted this bead ("the circuit maintains the shared indexes… needed by the queries")
  becomes true for membership state.

## 10. Testing

Per AGENTS.md's engine-task checklist, all of:

1. `pnpm engine:test` — new units: template extraction (param lifting, residual
   canonicalization, sig stability), bind-gated flat_map eval, adjust-input
   seed/replay/retract round-trips, flip-delta equivalence vs. the old kernel's semantics
   (port the `reconcile_row` and known_members regression tests, incl. PR #30's).
2. `ELECTRIC_CIRCUITS_ENGINE_PREBUILT=1 pnpm test` — the oracle conformance suite is the real
   referee: NULLs, nested subqueries, concurrent writers, batched mutations (the historical
   symptom of emission-order bugs: op-by-op converges, batches diverge).
3. `./electric-conformance/run.sh oracle` — Electric's own oracle vs `/v1/shape`.
4. Drive the LinearLite demo + pipeline visualizer (live-path + `/graph` schema change).
5. Benchmarks before/after: fleet suite + `packages/conformance/scripts/create-storm-bench.ts`.
   Watch subquery-creation-storm p50 (dbsp-ds-ht9 baseline) and re-verify the PR #30
   wake-storm fix (per-request p50s that look *faster* can mean spurious wakes returned).

## 11. Risks

1. **Stamp side-channel** — operator closures reading a per-txn cell is the least idiomatic
   piece; sound only under single-worker + one-dbsp-transaction-per-source-transaction.
   Assert both at construction; document in `arrangements.rs`'s module header.
2. **Snapshot read-your-writes** — outer emission now depends on the post-step snapshot being
   published before fan-out. The counts tier already relies on this ordering; subqueries widen
   the blast radius. Cover with a dedicated unit test (step → snapshot seek in same command).
3. **Hot-path widening** — circuit round-trip per txn for every table with a template. Gate on
   fleet benchmarks; the fallback lever is finer feeding granularity, not a redesign.
4. **Behavioral parity under direct replacement** — absolute emission + known_members must
   survive byte-for-byte on the stream; any conformance gap blocks the branch (accepted
   trade-off of the no-flag decision).
5. **Adjust-input/live-input interleaving** — seeding and retraction tuples must be serialized
   with transaction steps through the circuit command channel; a race here corrupts weights
   silently. The Phase-C-as-circuit-command design exists precisely for this; test with
   concurrent create + write storms (conformance fuzzers already generate these).

## 11b. As-built amendments (discovered during implementation planning)

Three mechanism-level corrections, found while mapping the design onto the real code. All five
table-of-decisions choices in §2 are unchanged; these amend *how* §4–§5 realize them:

1. **Host-side tuple evaluation, single circuit input.** Template evaluation (residual eval,
   param projection, bind lookup, gate check) runs in the registry — under its existing lock,
   per envelope, where gates/stamps/schemas already live — and feeds exact weighted
   `(node_id, proj_value, inner_pk)` tuples into ONE circuit input. The circuit pipeline is
   `input → map(drop pk) → [integrate_trace snapshot] + [distinct → accumulate_output]`: the
   integrated trace serves `contains`/`has_null`/introspection (weight = contributor count),
   the distinct's output deltas ARE the flips. This eliminates the §4 stamp side-channel
   (risk 1) and the §4 adjust-input/live-input interleaving (risk 5): seeding, gated replay,
   bind retraction, and nested reconciles are all ordinary tuples on the same input, serialized
   by the circuit command channel.
2. **The reverse index stays host-side.** §4's claim that "value-of-row is a pure function of
   the row" fails when a template's residual contains a nested IN — evaluation then depends on
   child membership at eval time, so retraction-by-re-evaluation is not symmetric. Reconcile-
   by-identity therefore keeps a host `pk → value` map per node (exactly today's `pk_value`,
   same memory) and emits precise retract/insert tuple pairs. What the circuit replaces is the
   `contributors` map (the larger structure) and all flip-detection logic.
3. **A dedicated always-on membership circuit.** The counts circuit is built only when
   `ELECTRIC_CIRCUITS_DBSP_COUNTS` is configured, and only at Postgres boot — but subqueries must
   work in library mode too. The membership pipeline is its own small `dbsp` circuit (one
   global tuple input, structure fixed at construction, one worker thread), owned by the
   `SubqueryRegistry` and started with it. Circuits per engine stay O(1): counts + membership.
   Because tuples enter per envelope inside `on_table_delta`, intra-transaction ordering is
   *identical* to today's registry (inner envelope → step → outer envelope evaluates fresh
   membership), and the sequencer's flow needs no restructuring.

Node identity (canonical `SubquerySig`, literal-level) is retained for edges, dependents, and
sharing-by-signature; the **template** layer (literals lifted from top-level equality
conjuncts) is the new grouping above it, giving the O(1)-per-delta eval win (one residual eval
+ one bind hash-lookup instead of one full predicate eval per node) and template-level
structural sharing. Node state in the circuit is keyed `(node_id, value)`, so template sharing
is a host-level concern and the circuit stays oblivious to bind structure.

## 12. Out of scope

- Arranging outer tables in the circuit (semijoin/antijoin producing pk deltas) — revisit only
  if flip query-back latency becomes the bottleneck after this lands.
- Circuit checkpoint/persistence (dbsp-ds-pg5) and multi-worker circuits.
- Widening param lifting beyond top-level equality conjuncts (e.g. range parameters).
