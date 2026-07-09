# How LinearLite's query graph is served

The flagship demo (`examples/linearlite`) exercises every tier of the serving model
(`building-app-pipelines.md`): the circuit serves its visibility shapes and its live header
count, the routing tier serves its equality and reference shapes, and its ordered pages go to
Postgres by design. This documents the query inventory, the circuit the demo launches with,
and which tier serves what.

## 1. Query inventory (every call site in `examples/linearlite`)

| # | Call site | Query | Character |
|---|-----------|-------|-----------|
| 1 | `usersShapeDef` (CurrentUser) | `users` (whole table) | global reference |
| 2 | `projectsShapeDef` (CurrentUser) | `projects` (whole table) | global reference |
| 3 | `myMembershipsShapeDef(u)` (CurrentUser) | `project_members WHERE user_id = $me` | per-user equality |
| 4 | `statusShapeDef` (Board) | `issues WHERE project_id IN (my projects) ∧ status = S [∧ project_id = P]`, board columns | visibility subquery × status |
| 5 | `commentsShapeDef(i)` (IssueDetail) | `comments WHERE issue_id = X` | per-issue equality |
| 6 | `issuesShapeDef(q)` (IssueList search) | `issues WHERE visibility ∧ status∈… ∧ priority∈… [∧ project] [∧ mine]`, full row | visibility subquery × filters |
| 7 | `projectIssuesSubsetDef(P)` (IssueList browse) | `issues WHERE project_id = P ORDER BY created/modified LIMIT 200` | subset: Postgres keyset page + changes-only tail |
| 8 | `issuesSubsetDef(q)` (IssueList, no project) | visibility predicate, ordered + paged | subset: Postgres native |
| 9 | `useAggregate` (IssueList header) | `COUNT(issues WHERE project_id IN [expanded member ids] ∧ filters)` | live count |

Two observations do all the work:

- **Visibility is cohort-shaped.** Every member of a project sees the same issues. The app
  exploits this everywhere: the browse view mounts one `ProjectSubsetFeed` **per member
  project** and merges client-side, and the count expands visibility to an explicit
  project-id list (`aggProjects = memberIds`). The per-user subquery (`visibleIssues($me)`)
  is per-user only in its *routing*, not in its *data*.
- **Ordering/pagination is deliberately not IVM.** Subsets (#7, #8) are keyset pages
  evaluated by Postgres with a changes-only live tail. That division of labor is right; the
  circuit does not absorb it.

## 2. The circuit the demo launches

The circuit is always on; `examples/linearlite/start.ts` boots the engine with the full circuit
configuration by default (pre-set `ELECTRIC_IVM_DBSP_*` vars win over these):

```sh
ELECTRIC_IVM_DBSP_INDEXES=issues.project_id,project_members.user_id,\
project_members.project_id,comments.issue_id
ELECTRIC_IVM_DBSP_COUNTS=issues:project_id+status+priority+username
```

That compiles: one Z-set input per table, fed per transaction by the sequencer; a pk
arrangement per table plus the four declared lookup arrangements; and one counts pipeline —
a live COUNT of `issues` per `(project_id, status, priority, username)` group that actually
occurs (sparse). No per-user, per-shape, or per-subscription structure anywhere.

## 3. Which query goes to which tier

| Queries | Tier | How |
|---|---|---|
| #4 board columns, #6 search list | **circuit** | the visibility subquery is the cohort constraint (`project_id IN (SELECT project_id FROM project_members WHERE user_id = $me)`, both columns arrangement-indexed); status/priority/project/mine filters ride as the residual. Seeded from arrangement snapshots — no Postgres backfill, no snapshot gate. Adding/removing the user from a project is a `project_members` delta that moves whole project cohorts in/out, emitted from the post-transaction snapshots. |
| #9 header count | **circuit (counts)** | the predicate — member-project IN-list × selected statuses × priorities [× me] — decomposes over the counts pipeline's group columns, so the aggregate is seeded by summing matching groups and updated from each step's group deltas. The visibility-over-aggregates problem disappears because visibility is, again, a set of project cohorts. |
| #3 memberships, #5 comments | **routing** | single-column equality templates on `KeyRouter` families (`user_id`, `issue_id`); one router per template, shared by every instance. Deliberately not circuit-served — the router finds a change's shapes by index instead of scanning deltas. |
| #1 users, #2 projects | **routing** | whole-table (match-all) fan-out; nothing to compute. |
| #7, #8 ordered pages | **Postgres, by design** | keyset pages evaluated natively (subquery predicates included), merged client-side with a changes-only live tail. |
| anything ad hoc | **fallback** | stateless three-valued eval / the subquery registry; works immediately at fallback cost. |

**State** (all in disk-spillable dbsp arrangements): issues ×2 (pk + by project) + the sparse
count groups; comments ×2 (pk + by issue); members ×3 (pk + by user + by project);
users/projects ×1 (pk) — a small constant factor over one table copy, independent of user
count. A thousand users cost a thousand *subscriptions*, zero circuit growth.

**What the circuit replaces for these shapes**: the subquery registry's nodes, contributor
sets, and flip query-backs; per-shape Postgres backfills and snapshot gates for the
visibility shapes; and the per-shape aggregate fold for the header count. Move-in/move-out
(user added to / removed from a project) is not computation at all: a membership delta
re-derives the shape's cohort groups from local snapshots.

## 4. Future directions

Two upgrades are designed but not implemented:

- **Cohort-feed delivery with a client-side union**: materialize one durable feed per project
  cohort (`issues_by_project`) and serve a user's shape as the union of their cohorts' feeds,
  resolved at the delivery edge — making backfill a log replay instead of a snapshot read.
  Today each circuit-served shape has its own stream, seeded from snapshots.
- **Comments inherit their issue's project** (`comments ⋈ issue→project`, keyed
  `(project, issue)`): one bilinear join that would re-home comments automatically if an
  issue moved projects. LinearLite doesn't move issues across projects, so `comments` stays
  on the `issue_id` router.
