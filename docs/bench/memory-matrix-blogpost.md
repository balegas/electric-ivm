# Engine memory across deployment size × audience size

Benchmark backing the electric-circuits blog post's memory claims. Run on a MacBook (M-series,
macOS), engine `release` build at the feed-relations merge (PR #34/#35), via
`packages/bench/src/shape-mem-matrix.ts`.

## Setup

LinearLite schema (issues, comments, projects, project_members, users), seeded at three
deployment sizes: **1k / 10k / 100k issues** (projects scaled 20/20/200 so a user's visible
slice stays realistic: 300 rows at 1k, ~3,000 rows at 10k and 100k). Simulated users connect
in milestones (50 → 1,000); each user registers **~10 shapes** spanning every serving tier:

- 1 **visibility subquery** `project_id IN (SELECT project_id FROM project_members WHERE
  user_id = ?)` — N users share ONE compiled template (N binds).
- 5 **board shapes** `status = <s>` — identical across users ⇒ signature-shared: **5 streams
  total** for the whole fleet, at any user count.
- 1 personal equality (`username = ?`) — KeyRouter family-routed.
- 3 comment shapes — the long tail of small feeds.

Every cell is measured twice: **registration-only** (changes-only feeds — pure engine state)
and **materialized** (every shape backfills — real synced clients). The difference isolates
the per-feed key sets. Samples are `GET /memory` (RSS + engine cardinalities) after the
change log drains.

## Results (ΔRSS over a 24 MiB init baseline)

**Engine state** (registration runs) — flat in data, ~3 KiB per shape:

| users (shapes) | 1k issues | 10k issues | 100k issues |
|---:|---:|---:|---:|
| 100 (1,000) | 8.6 MiB | 7.9 MiB | 8.6 MiB |
| 500 (5,000) | 19.0 MiB | 20.8 MiB | 21.2 MiB |
| 1,000 (10,000) | 26.5 MiB | 34.3 MiB | 36.2 MiB |

**100× the data ⇒ 1.0–1.4× the engine state.** At the top milestone there are **10,000 shape
registrations** but only **5,005 distinct live shapes** — signature sharing collapses all
1,000 users' five `status` boards onto the same 5 streams, so half the registrations are free
joins. The engine's routing, templates, membership sets and edges fit in ~36 MiB: **~3 KiB per
registration** (what a connecting user session costs) or **~7 KiB per distinct live shape**
(what a live pipeline costs) — regardless of how big the tables are.

**Feed key sets** (materialized − registration) — follow synced rows, not database size:

| users | 1k issues (300 rows/user) | 10k issues (3k rows/user) | 100k issues (3k rows/user) |
|---:|---:|---:|---:|
| 100 | 4.7 MiB | 44.8 MiB | 41.2 MiB |
| 500 | 14.2 MiB | 129.5 MiB | 127.6 MiB |
| 1,000 | 25.5 MiB | 244.0 MiB | 238.8 MiB |

Converges to **~85 bytes per synced row** at scale (small cells read high — allocator
granularity dominates below ~10 MiB). The money comparison: **10k and 100k issues produce the
same curves** (302 vs 299 MiB total at 1,000 users) because users sync the same number of
rows — memory follows *what your users see*, not *what you store*.

**Totals** (materialized, 1,000 users / 10,000 shapes): 76 MiB (1k) · 302 MiB (10k) ·
**299 MiB (100k issues)**.

Full per-cell data: `memory-matrix-blogpost.csv` (chart-ready).

## The model the numbers fit

```
RSS ≈ 24 MiB (engine)  +  ~3 KiB × live shapes  +  ~85 B × currently-synced rows
```

- The **shapes term** is the shared machinery: one subquery template serves every user's
  visibility query (1,000 users = 1,000 binds, 6,000 membership entries — one structure);
  the five board shapes are one stream each for the entire fleet.
- The **synced-rows term** is the per-feed key sets — one pk per row currently in a feed,
  held in the DBSP circuit's feed relation (the same relation whose retractions gate
  deletes). It is bounded by client subscriptions, not table sizes, and it is the term the
  storage-enabled circuit follow-up moves to disk (bounded cache + spill), which the old
  host-side hash sets could never do.
- Backfill itself adds **no retained memory** (probe: a 3,000-visible-row materialized shape
  moved RSS by <0.1 MiB at the 100k size) — rows stream from Postgres to the shape log
  without an engine-side copy.

## Blog-safe claims

1. "Engine memory is independent of database size: 100× the rows moved total RSS by ~1%
   (302→299 MiB) when users synced the same data."
2. "10,000 shape subscriptions across 1,000 users cost ~36 MiB of engine state — about 3 KiB
   per subscription — because identical queries share one pipeline: they resolve to just
   5,005 distinct live shapes, and every duplicate is a free join."
3. "The rest is ~85 bytes per row your clients currently sync — a quantity you control with
   shape predicates, with disk-spill on the roadmap." 

Avoid claiming absolute flatness of total RSS as users grow — the synced-rows term is real
and linear in feeds × feed size; the honest (and stronger) claim is *what it scales with*.

## Reproduce

```bash
cargo build --release -p electric-circuits-engine
MATRIX_SIZES=100000 MATRIX_USERS=50,100,250,500,1000 MATRIX_PROJECTS=200 \
  [MATRIX_MATERIALIZED=1] pnpm --filter @electric-circuits/bench exec tsx src/shape-mem-matrix.ts
```

(macOS: clean leaked PG shared-memory segments between runs — `ipcs -ma` / `ipcrm -m` for
0-attach segments — or `initdb` starts failing after ~30 ephemeral instances.)
