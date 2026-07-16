# Feed Relations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans. Spec:
> docs/superpowers/specs/2026-07-14-feed-relations-design.md (§3-§6 are the blueprint).

**Goal:** Emissions as circuit output deltas — two upsert-map inputs replace the zset input;
known_members/filter_known_members/pk_value/reconcile_row_tuples deleted.

Branch: feat/feed-relations (off chore/subquery-registry-cleanups / PR #33).

### Task 1: subq_circuit.rs — two upsert maps
- [ ] Contributors: add_input_map K=Row([Int(node),Text(pk)]) V=Value U=Value (patch=assign);
      downstream map to Row([node,value]) -> integrate_trace snapshot + distinct->accumulate_output
      (flips, unchanged shape). Feeds: add_input_map K=Row([Int(feed),Text(pk)]) V=Value(Null);
      accumulate_output on the input stream (deltas = FeedDelta) + integrate_trace (prefix scans).
- [ ] apply(contribs: Vec<Tup2<Row,Update>>, feeds: Vec<Tup2<Row,Update>>) -> (Vec<MemberDelta>,
      Vec<FeedDelta>) — one Cmd, one transaction. feed_pks_for(feed_id) prefix scan;
      node_entries_for(node_id) prefix scan (drop paths).
- [ ] Unit tests: upsert idempotence (re-Insert same value = no delta), value-change =
      retract+insert, Delete-absent = no-op, feed enter/leave deltas, prefix scans, flips parity
      with fold_refcount_flips. Tests red->green->commit.

### Task 2: subquery.rs — assertions + one emission tail
- [ ] Node: delete pk_value/reconcile_row_tuples/contributor_count; registry: delete
      known_members plumbing, filter_known_members; SubqueryShape gains feed_id: i64
      (registry counter) + registry feed_by_id map.
- [ ] template_reconcile -> template_assertions: per pk, target from template_present; pk_nodes
      consulted for old bind (Delete on old node) and kept current; gate/mid-seed nodes don't
      assert. apply_node_evals -> assertions per (node,pk).
- [ ] Shared tail emit_for_shapes(groups: Vec<(shape_id, Vec<(Row,bool)>)>, ts, txid): under
      lock — eval matches_ctx per candidate; upsert envelopes for matching candidates; feed
      assertions for ALL candidates (Insert/Delete); ONE circuit apply; deletes built from
      FeedDelta -1 (key-only envelopes); enqueue per shape lane. Callers: on_table_delta step 2
      (batched across shapes), move_shape_for_value, rederive shape arm.
- [ ] finish_create: node seeds + shape seeded_pks as assertions (deltas discarded); replay via
      assertion paths. decref_nodes/drop shape: prefix-scan slices -> Delete assertions;
      pk_nodes cleanup from scan.
- [ ] Port unit tests (filter_known_members tests -> feed-relation delete-gating tests);
      engine/tests.rs regression test to assertion path. cargo suites green -> commit.

### Task 3: gates + demo
- [ ] pnpm engine:test; ELECTRIC_CIRCUITS_ENGINE_PREBUILT=1 pnpm test (PR30 regression must pass
      with known_members gone); electric-conformance oracle + subqueries (13/15 baseline);
      LinearLite via Playwright (join/leave churn, new issue, visualizer). create-storm bench
      before/after noted for the serialization risk.
- [ ] Docs: ARCHITECTURE §6/§6b, memory-model update. Commit; PR.
