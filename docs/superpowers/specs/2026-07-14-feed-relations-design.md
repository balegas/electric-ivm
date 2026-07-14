# Feed relations: the full semijoin — emissions as circuit output deltas

**Bead:** dbsp-ds-dh6 · **Date:** 2026-07-14 · **Status:** approved design, pre-implementation
**Builds on:** `2026-07-14-subquery-circuit-templates-design.md` (merged as PR #32) and the
simplification review recorded in that bead. Blocks dbsp-ds-pg5.

## 1. Problem

After PR #32, two host-side structures remain that exist only to remember prior state so the
engine can compute exact transitions:

1. **`known_members`** (per subquery shape, `O(feed rows)`): which pks this shape's stream
   currently asserts — needed to gate spurious deletes (the PR #30 wake-storm fix). It is the
   dominant per-feed RSS term (`docs/memory-model.md` §2) and is applied as a *filter*
   (`filter_known_members`) inside three near-identical emission functions.
2. **`pk_value`** (per node, `O(contributing rows)`): each inner row's previously-asserted
   projected value — needed because evaluation is impure (nested `IN` reads other nodes'
   sets), so retraction cannot re-derive the old tuple from the row.

Both are "remember-to-retract" state. dbsp has a primitive for exactly this —
**`add_input_map` (upsert handle)**: the caller asserts `key → value` or `key → delete`
*absolutely*, and the operator, which "internally maintains the contents of the map"
(dbsp 0.318 `operator/input.rs:563-575`), emits the exact `(old,-1),(new,+1)` deltas itself.
This design moves both structures into upsert maps and makes the feed relation's output
delta **be** the emission decision — the full semijoin, completed.

## 2. Decisions taken

| decision | choice | rationale |
|---|---|---|
| Scope | **full semijoin-completion** — feed-relation deltas ARE the emissions; `known_members`/`filter_known_members` deleted | user decision; delete-gating becomes structural (the PR #30 bug class becomes unwritable) |
| Storage | **in-memory first**; dbsp storage/spill is a separate follow-up gated on RSS measurement | keeps the emission-semantics change and the storage-layer reintroduction separately verifiable |
| Restarts | **drop-at-boot unchanged**; pg5 stays blocked on the storage follow-up (checkpointing rides on storage) | this bead changes emission semantics only |
| Contributor tracking | **also an upsert map** — `(node_id, pk) → value`; `pk_value` + `reconcile_row_tuples` deleted | same primitive, one layer down; circuit state can no longer drift from host bookkeeping |
| `pk_nodes` | **kept** (host-side, per template) | all three elimination routes fail: bind fan-out is O(binds)/pk; `(template, pk)` keying breaks bind-drop enumeration; a second routing map saves nothing and reintroduces read-then-decide ordering |
| Circuits | counts and membership stay **separate** | structural: counts config exists only at PG boot; membership must exist in library mode; a circuit is fixed at construction |

## 3. Circuit changes (`subq_circuit.rs`)

The membership circuit's structure changes from one z-set input to two upsert-map inputs —
still fixed at construction, still instance-keyed, still oblivious to templates/binds:

```
INPUT A (contributors, upsert map): Row([node_id, pk]) → Value(projected)
  → map_index to (node_id, value) weighted            [the map's own diff makes this exact]
  ├─ integrate_trace → membership snapshot            [contains()/has_null()/introspection]
  └─ distinct → accumulate_output                     [flips, exactly as today]

INPUT B (feeds, upsert map): Row([shape_num_id, pk]) → () (presence-only)
  ├─ integrate_trace → feed snapshot                  [drop-time enumeration, introspection]
  └─ accumulate_output                                [per-step deltas = THE EMISSIONS:
                                                       +1 = deliver upsert, −1 = deliver delete]
```

- **API:** `apply(contributor_upserts, feed_upserts) -> (Vec<MemberDelta>, Vec<FeedDelta>)`
  — one command, one `dbsp.transaction()`, both handles fed per step.
  `FeedDelta { shape_num_id: i64, pk: String, delta: i64 }`.
- **Upsert semantics do the reconcile:** asserting `Insert(v)` over a key holding `v` nets to
  nothing; over a key holding `w ≠ v` emits the retract/insert pair; `Delete` over an absent
  key nets to nothing. Re-assertion is idempotent — the property today's reconcile-by-identity
  provides by hand.
- **Prefix enumeration** (existing seek pattern) serves both drop paths: node drop scans
  `(node_id, *)` of input A's integral; shape drop scans `(shape_num_id, *)` of input B's.
  Both feed `Delete` assertions back through the same handles.
- `contributor_count`/`mem_totals` read a derived per-node key-count view (a stateless map off
  input A into its own trace) instead of host `pk_value.len()`.
- No highwater, unchanged: idempotent assertion + the sequencer's `(lsn, seq)` de-dup.

## 4. Registry changes (`subquery.rs`)

**Deleted:** `SubqueryNode.pk_value`, `reconcile_row_tuples`, `SubqueryShape.known_members`,
`filter_known_members`, and the value-diffing half of `reconcile_node_row`.

**Kept:** `pk_nodes` (template inverted index — now maintained directly where assertions are
computed), gates, `seed_buffer`, edges (sig-keyed map from PR #33), three-phase create,
`SubqueryEval` reads, NULL-sensitivity logic, emission lanes, `pendingFlips`.

**Assertion computation** replaces tuple computation. For an inner-table delta,
`template_present` is unchanged (residual eval + bind lookup per touched pk); what follows
becomes: build `Update::Insert(value)` / `Update::Delete` per `(node_id, pk)`, using
`pk_nodes` only to find the *old* bind on cross-bind moves and to keep itself current
(present-bind add / absent remove). Gate-skipped and mid-seed nodes simply don't assert —
the map keeps its prior state, which the seed covers (same soundness argument as today).

## 5. Emission unification — one tail, three sources

`emit_shape_delta`, `move_shape_for_value`, and `rederive_dependent`'s shape arm collapse to
thin candidate-sourcing wrappers (delta rows / query-by-connecting-value / query-all) around
ONE shared tail:

```
assert_candidates(shape, ts, candidates: Vec<(Row, bool /*exists*/)>, txid):
  under the registry lock:
    per candidate pk: member = exists && pred.matches_ctx(row, registry)   // fresh snapshot
    build feed upserts: member → Insert(()), else → Delete
    (feed_flips, feed_deltas) = circuit.apply([], feed_upserts).await      // awaited under lock
    envelopes: +1 → upsert with the candidate row's body (projection applied)
               −1 → delete by pk
    enqueue on the shape's emission lane (still under the lock)
```

- **Upserts for continuing members**: a candidate that matches and was already a member nets
  no feed delta — but its row may have *changed*. Continuing-member updates MUST still emit.
  Rule: emit an upsert for **every matching candidate** (as today — upserts are always safe
  and idempotent for readers); the feed relation's deltas add the **deletes** and are also
  used to suppress nothing — deletes come *only* from retractions. This preserves today's
  update-delivery semantics exactly while making spurious deletes impossible.
- **Stepping never double-books a transaction:** an envelope's delta belongs to one table, so
  membership assertions (inner tables) and feed assertions (outer emission) never co-occur in
  one step from the sequencer path; the flip path asserts feeds after its query-back, in its
  own step. Assertions **batch across shapes**: feed keys carry the shape's numeric id, so
  `on_table_delta`'s outer-emission phase collects every affected shape's assertions into ONE
  awaited apply per envelope (deltas fan back out per shape by key), and a FlipWork batch can
  do the same across its edges — steps per envelope stay O(1), not O(shapes).
- **Load-bearing ordering invariant (flag in the PR):** the circuit-apply await and the lane
  enqueue happen **while holding the registry lock**, exactly like membership applies today.
  Per-stream append order = eval order survives only because of this; dropping the lock
  before enqueueing (a tempting "optimization") silently reintroduces out-of-order emission.
- `emitted` counters and trace hops move into the shared tail (one place instead of three).

## 6. Lifecycle mapping

- **Shape seed (phase B/C):** backfill pks assert `Insert(())` through the feed handle in
  `finish_create` (deltas discarded — the stream already has the snapshot), replacing the
  `seeded_pks → known_members` hand-off. Buffered outer deltas then replay through
  `assert_candidates` as today.
- **Node seed:** snapshot rows assert `Insert(value)` through the contributor handle (flips
  discarded), then gated buffer replay via the same assertion path (flips propagated).
- **Shape drop:** prefix-scan the feed snapshot, assert `Delete` for the slice, discard
  deltas (the stream is being deleted). Node drop: same via the contributor handle (already
  the decref path's shape, minus host pk lists).
- **Restore:** unchanged — subquery shapes still dropped loudly at boot.

## 7. Invariants preserved (the review's adversarial checklist)

- **Absolute emission** — assertions are absolute per-pk current membership; unchanged.
- **Per-stream order = eval order** — lock + FIFO lanes, with the §5 flag.
- **Gate fencing** — host-side, per bind, before asserting; unchanged.
- **Exactly-once** — improved: upsert assertion is idempotent by construction, where ±1
  tuples relied on reconcile discipline.
- **Three-phase atomicity** — begin/abort touch no circuit state (assertions happen at
  finish); conflict-retry unchanged.
- **Convergence under deferred flips** — the feed relation is per-shape *output* state; its
  transitions derive from absolute assertions evaluated against then-current membership +
  Postgres, so late propagation converges exactly as today.

## 8. Testing

Same gates as PR #32: `pnpm engine:test` (new units: upsert-map flip/feed-delta semantics
incl. idempotent re-assertion, cross-bind moves, prefix-scan drops, continuing-member update
delivery); vitest oracle conformance (the PR #30 regression test must pass with
`known_members` deleted — it now pins the feed relation's behavior); Electric oracle +
subqueries suites (13/15 baseline); LinearLite drive.

**Plus the named benchmark gate:** with emissions decided by circuit steps, the emission tail
serializes on the single circuit thread — flip-worker parallelism remains only for PG
query-backs. Fleet suite + `create-storm-bench.ts` before/after must show no regression
beyond noise; if it regresses, the mitigation lever is batching assertions per FlipWork (many
shapes' assertions in one step), not abandoning the design.

## 9. Risks

1. **Emission-tail serialization** (§8) — measured, with a named mitigation.
2. **Continuing-member updates** — the one behavior that must NOT change while deletes move
   to relation-retractions; §5's rule keeps upsert delivery identical, and the oracle fuzzers
   (batched mutations, concurrent writers) are the referee.
3. **Drop-path scans** — O(slice) prefix scans replace O(1) host map drains at drop time;
   fine at drop frequency, noted for the record.
4. **Two upsert handles, one command** — both fed in one `transaction()`; a partial-feed bug
   (one handle fed, the other dropped) would corrupt silently. The apply API takes both
   vectors in one struct so the call site cannot split them.

## 10. Out of scope

- dbsp storage/spill + checkpointing (the follow-up that unblocks pg5).
- Aggregate-fold migration into the circuit (same pattern, separate bead).
- `TemplateGroup` absorbing `SubqueryNode` / derived-not-stored `node.pred` (optional
  follow-ups from the review; do only if convenient mid-implementation).
