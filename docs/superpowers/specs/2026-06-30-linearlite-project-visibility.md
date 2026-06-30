# LinearLite project visibility via subqueries

Design record — 2026-06-30. Status: **implemented + verified in-browser (incl. 100k issues)**. Verified
via Playwright against the live demo: visibility filters the list to the current user's projects,
switching users rebinds the visible set, joining a project makes its issues appear live, "My Tasks" shows
the user's issues across multiple projects, the board's five status shapes share one inner node
(`/subqueries` refcount 5), and at `DEMO_SEED_COUNT=100000` the per-project-subset browse list pages each
feed (200→1000), merges 3 feeds in global created-desc order (1400 rows interleaving all 3 projects),
and project switching is instant client-side (no new shape) — all with zero console errors and without
materializing the ~60k visible rows. Two non-obvious client gotchas: (1) shapes carry the primary key as
a *string* (TanStack DB collection keys are strings) while non-pk int columns and the subset path are
numbers — reference-data ids are normalized to numbers in `CurrentUser` so visibility joins compare
cleanly; (2) a single subset whose predicate folds in the active filters re-creates the engine feed on
every filter click (the delay) — the list instead mounts a reused per-project feed and filters on the
client (see "Browse list" below).

Goal: exercise the new subquery engine feature in the
LinearLite demo by (a) replacing the demo's flat issue queries with subquery-based visibility queries,
keeping infinite scroll working, and (b) adding a real project/membership visibility model — users only
see issues from projects they belong to, projects are reflected in the UI, and a "My Tasks" view spans
projects.

## Model

New tables (added to both `examples/linearlite/src/schema.ts` and the DDL + seed in `start.ts`):

- `projects (id BIGINT pk, name TEXT, color TEXT)`
- `users (id BIGINT pk, name TEXT)`
- `project_members (id BIGINT pk, project_id BIGINT, user_id BIGINT)` — membership join table.
- `issues` gains `project_id BIGINT` (every issue belongs to a project).

Seed: ~6 users, ~5 projects, memberships so each user sees a *different, overlapping* subset of projects
(so switching users visibly changes the issue set). Each issue is assigned a `project_id` (round-robin /
faker) and an assignee `username` (kept as-is, drawn from the seeded user roster so "My Tasks" is real).

## Visibility = a subquery

The core predicate, reused everywhere issues are listed:

```
visibleIssues(userId) =
  { col: 'project_id', in: { table: 'project_members', project: 'project_id',
                             where: { col: 'user_id', op: 'eq', value: userId } } }
```

- **Board columns** (5 materialized shapes) — each `AND(visibility, status=col)`. All five reference the
  *same* inner subquery ⇒ **share one registry node** (demonstrates node sharing in a real app).
- **Search** (materialized shape) — `AND(visibility, status/priority)`, refined client-side.

Issue detail / comments stay keyed by id (no visibility needed once you hold the id).

## Browse list: per-project subset feeds (scale + shape reuse)

The browse list (the primary, scalable view) does **not** use the visibility subquery as one subset.
Folding the filters into a single subset predicate means every project/status click changes the predicate
→ a new engine feed (teardown + query-back) → a visible delay; and a single per-user subset can't bound
memory cheaply. Instead the list mounts **one paginated subset per project the user belongs to**
(`project_id = P`) and merges them on the client:

- Each per-project subset is a query-back (never materialized) → a member of a **100k-issue** workspace
  holds only the loaded pages, not the ~60k visible rows. Demonstrated at `DEMO_SEED_COUNT=100000`.
- The `project_id = P` predicate is identical across users, so the engine reuses **one feed family per
  project** (equality family circuits) instead of a per-user-per-filter subquery subset.
- Project / status / priority / my-tasks selection and ordering are **client-side** over the merged
  loaded window (a k-way merge by `created`), so switching any filter is instant — no new engine request.
  Scrolling pages every active feed forward together.

This is the reuse/scale counterpart to the subquery: the subquery is the elegant declarative form (used by
the bounded Board + Search views, where node-sharing shines); per-project subsets are the
reuse-maximizing, memory-bounded form for the large primary list. The engine's subquery *subset* support
(below) remains available but the scalable list deliberately uses per-project equality subsets.

## Engine support (the only non-UI work)

The subquery AST already flowed end-to-end for *materialized shapes*. Two gaps blocked the *subset* path;
both are fixed:

1. **Subset query-back** now builds its `WHERE` from the JSON predicate (`predicate_json_to_sql`, Postgres
   evaluates the subquery natively) instead of the compiled form that `unreachable!()`s on subqueries —
   `engine.rs::query_subset` → `pg.rs::query_subset_where`.
2. **Changes-only feeds** (the subset live tail) now accept subqueries: `create_subquery_shape` takes a
   `changes_only` flag — it still seeds the inner nodes (so live `matches_ctx` works) but skips the outer
   backfill and forwards only future membership deltas (`seed_lsn = 0`).

Absolute-membership emission (already in place) makes the live feed correct regardless of cross-table
order: a membership insert/delete flips the node and query-backs the affected issues; a new issue in a
visible project enters via the outer-delta path.

## UI

- **Current user**: a `currentUserId` in `App` state (default first user) + a user switcher in the sidebar.
  Threaded through `Filters`/hooks so all issue queries rebind when it changes.
- **Sidebar**: a "Projects" section listing the current user's member projects (each a filter link) + a
  **Join/Leave** toggle per project (writes `project_members` via `pgWrite`) so visibility can be changed
  live in the demo. A "My Tasks" entry.
- **Issue row / detail**: a project badge (name + color).
- **New-issue modal**: a project picker (defaults to the user's first project).

## Verification

Boot the demo; use the Chrome MCP to confirm: (1) infinite scroll still pages through the visible set;
(2) switching users changes the visible issues; (3) joining a project makes its issues appear live;
(4) project badges render; (5) "My Tasks" shows the current user's issues across projects.

## Out of scope

Auth (user is a demo switcher, not authenticated); per-issue ACLs beyond project membership; editing
project/user rosters from the UI (fixed seed); ordering by project.
