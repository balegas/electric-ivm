# electric-lite — shape pipeline sharing (equality-first)

Status: **design approved, implementation gated** on the conformance suite (incl. NULL/three-valued
work) being green. No engine code starts before then.

## Problem

Today every shape is one dbsp `RootCircuit` on its own OS thread running `stream.filter(pred)`.
Consequences:

- **CPU per change is `O(Δ × total_shapes)`** — each delta row is tested against every shape.
- **One OS thread per shape** (`CircuitHandle` is `!Send`) — thousands of shapes ⇒ thousands of
  threads.
- **Add-shape is `O(table_rows)`** — spawn a thread, build a circuit, backfill by scanning the
  whole `table_state`.

Many real workloads register lots of shapes that differ only by a constant (`tenant_id = acme`,
`tenant_id = globex`, …). We want those to **share one pipeline**.

## Goal & scope

Share a single circuit across shapes whose predicate is the **same equality template modulo
constants**. A shape *qualifies* iff its predicate is:

- a single equality leaf `col = ?`, or
- an `AND` of equality leaves `a = ? AND b = ?` (composite key).

Everything else — ranges (`<,<=,>,>=`), `OR`, `NOT`, mixed predicates — is **out of scope** and
keeps using the current per-shape `filter` circuit unchanged. This avoids the inequality/overlap
cross-product blow-up (see Tradeoffs) and keeps the first cut small.

## Design

### 1. Predicate templating

Canonicalize a `CompiledPredicate` into `(template, params)`:

- **template signature** = the sorted tuple of equality columns + the structural shape. Two shapes
  share a family iff their template signatures are equal.
- **params** = the equality literal values, in canonical column order (the join key tuple).

Non-qualifying predicates return no template → standalone path.

### 2. Family circuit — one `RootCircuit` per template

```
table Δ ─► index_by(key cols) ─┐
                               ├─► equi-join on key ─► (shape_id, row, ±w) ─► demux by shape_id ─► shape streams
Params Δ {(key_tuple, shape_id)}┘
```

- The data arrangement (`index_by(key)`) and the join are built once and maintained incrementally.
- Per-shape results **fan out from the single join output**, demultiplexed by `shape_id` in the
  tailer (group the consolidated batch by `shape_id`, append each group to its shape stream).
- A single `RootCircuit` is required: dbsp only shares an arrangement *within* one circuit, and
  circuits are statically constructed — so a dynamic shape set must be expressed as **data**
  (the `Params` collection), not as new operators.

### 3. Dynamic shapes = `Params` deltas (always-share, N=1)

- **Add shape**: the *first* qualifying shape on a template creates the family circuit; every
  qualifying shape (including the first) is an insert `(key_tuple, shape_id)` into `Params`. The
  incremental join emits exactly that shape's backfill — no thread spawn, no full re-scan.
- **Drop shape**: delete `(key_tuple, shape_id)` from `Params`.
- N=1 means we never migrate a running shape from a standalone filter into a family; a single-shape
  template simply pays minor join overhead vs a filter (accepted for operational simplicity).

### 4. Coexistence & registry

`EngineState` gains a `families: HashMap<TemplateSignature, FamilyHandle>` alongside the existing
per-table tailers. `create_shape`:

1. compute the template; if it qualifies → route to the family (create-or-join), else
2. fall back to the current per-shape circuit.

Both paths feed the same per-table change stream. The per-table tailer still owns `table_state`
and the authoritative read offset.

### 5. Thread model

One thread per *template* family + the existing per-shape threads for non-qualifying shapes. A
table with 10k `tenant_id = ?` shapes goes from 10k threads to 1 family thread + 10k tiny `Params`
rows.

### 6. Drain barrier / processed-offset interaction

The soundness barrier (`drainEngine` polling `GET /tables/:name/offset`) must still hold:

- The family tailer publishes the processed offset **after** a table-delta batch is fully fanned
  out (unchanged invariant).
- Shape-add backfill goes through `Params` (a command), processed biased before the next table
  read, and its append must complete before the offset advances past subsequent deltas — same
  ordering guarantee the current `AddShape` command path provides.

## Performance / memory tradeoff

| | Per-shape filter (today) | Shared family join (equality) |
|---|---|---|
| CPU / change | `O(Δ × total_shapes)` | `O(Δ × matching_shapes)` |
| Threads | one per shape | one per template |
| Add / drop shape | thread spawn + `O(table_rows)` backfill | incremental `Params` delta, `O(matching rows)` |
| Memory | ~stateless filters + shared `table_state` `O(rows)` | data arrangement `O(rows)×idx` + join trace `O(result pairs)` |

- The win is **CPU + threads + cheap add/remove**; the cost is **resident index/join memory**.
- For selective, disjoint equality (partition keys) the join output is ≈ `O(rows)` total ⇒ memory
  cost is modest and the trade is strongly favorable.
- The data arrangement can **subsume `table_state`** (one col-indexed copy instead of a HashMap),
  softening the memory cost.
- Pathological only for ranges / heavily-overlapping shapes (a row matching many shapes ⇒
  `O(rows × shapes)` pairs) — explicitly excluded from scope.

## Correctness & verification

Sharing is a **pure engine optimization**: a family circuit must produce byte-identical shape
streams to the per-shape path. No oracle / protocol / SQL changes. The (null-aware) conformance
suite validates both paths against pglite with zero new oracle code; add fixtures that register the
same shapes via both the shared and standalone paths and assert identical materialization.

## Risks & edge cases

- **Same-value fan-out**: many shapes with the *same* key value ⇒ a row emits one pair per such
  shape. That equals the real combined result size; acceptable.
- **Composite keys**: `AND` of equalities ⇒ key is the value tuple in canonical column order.
  Different column sets ⇒ different templates (e.g. `a=?` and `a=? AND b=?` don't share).
- **Backfill ordering** vs the drain barrier — see §6.
- **Demux cost**: grouping the join output by `shape_id` per batch is `O(output)`; fine.

## Implementation sequencing (after conformance is green)

1. Predicate templating + qualification (pure function, unit-tested) — no behavior change.
2. Family circuit (`index_by` + `Params` equi-join + demux) behind the existing `create_shape`
   routing; standalone path untouched.
3. Wire add/drop shape to `Params` deltas; preserve the offset/barrier invariant.
4. Conformance fixtures asserting shared == standalone == oracle; extend fuzz to register many
   same-template shapes.
5. (Optional) subsume `table_state` into the data arrangement.

## Out of scope / future

Ranges, `OR`, `NOT`, residual-filter splitting (extract the equality part as the shared key and
apply the remainder as a per-shape residual filter on the join output) — revisit once the
equality-only family path is proven.
