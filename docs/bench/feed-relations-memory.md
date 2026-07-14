# Feed-relations memory A/B — main (jq6) vs feat/feed-relations (dh6)

`packages/bench/src/shape-mem-matrix.ts`, 10k-issue LinearLite deployment, 50 projects,
6 memberships/user, ~10 shapes/user (1 visibility subquery + equality/comment shapes).
Same host, same run, binaries swapped. Raw tables in the bench output; summary:

## Registration memory (changes-only feeds — membership state only)

| binary | Δ RSS @ 5000 shapes (500 users) | KiB/shape |
|---|---:|---:|
| main (host contributor maps) | 15.2 MiB | 3.1 |
| dh6 (circuit upsert maps) | 17.5 MiB | 3.6 |

**≈ +15% (+2.3 MiB @ 5k shapes).** Near-parity: the membership/bookkeeping term moved into
circuit spines with modest constant overhead. Cardinalities identical (500 nodes, 3000
contributor pks, 500 edges, 3 family circuits).

## Feed-heavy memory (materialized visibility shapes — the per-feed key-set term)

200 users × ~1,200 visible rows each ≈ 240k feed keys:

| binary | Δ RSS @ 2000 shapes | KiB/shape | ≈ bytes/feed-row |
|---|---:|---:|---:|
| main (`known_members` HashSets) | 41.3 MiB | 21.1 | ~150 |
| dh6 (circuit feed relation) | 60.0 MiB | 30.7 | ~220 |

**≈ +45% (+18.7 MiB @ 240k feed keys).** Not parity. The explanation is structural: the
circuit currently holds the feed relation **twice** — the upsert-map operator's internal
integral (needed for diffing) plus the published `integrate_trace` snapshot (drop-time
enumeration + introspection) — and each entry is a `Row([Int, Text])` key in a spine batch,
heavier than a bare `HashSet<String>` entry. The contributor relation similarly carries the
map integral + the `(node,value)` membership trace + the `(node,pk)` enumeration trace.

## Read

- The **watched-relationship state is cheap and parity-ish** — the redesign's structural wins
  (no host bookkeeping to drift, structural delete-gating, one emission tail) cost ~15%.
- The **per-feed key term costs ~1.5× in RSS today**, in exchange for: the wake-storm bug
  class being unwritable, and the relation being spillable/checkpointable the moment the
  storage follow-up lands (which flips this comparison entirely — main's HashSets can never
  page; the circuit trace can).
- **Optimization headroom before reaching for storage:** drop the published feed
  `integrate_trace` and enumerate drop-time retractions differently (or accept lazy garbage
  with monotonic feed ids + periodic compaction), and/or intern pk strings. Either would
  close most of the 2×-copy gap. Filed as follow-up work on dbsp-ds-dh6.

Absolute context: at LinearLite scale these numbers are tens of MiB; the term matters at
fleet scale (100k feeds × 600 rows ⇒ ~9 GiB on dh6 vs ~6 GiB on main, both unacceptable
without spill — which only dh6's design can add).
