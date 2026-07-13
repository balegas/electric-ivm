# How LinearLite's query graph is served

The flagship demo (`examples/linearlite`) exercises every tier of the serving model
(`building-app-pipelines.md`): the circuit serves its live header count, the subquery
registry serves its visibility shapes, the routing tier serves its equality and reference
shapes, and its ordered pages go to Postgres by design. This documents the query inventory,
the circuit the demo launches with, and which tier serves what.

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

The circuit is always on; `examples/linearlite/start.ts` boots the engine with the counts
configuration by default (pre-set `ELECTRIC_IVM_DBSP_*` vars win over it):

```sh
ELECTRIC_IVM_DBSP_COUNTS=issues:project_id+status+priority+username
```

That compiles one counts pipeline — a live COUNT of `issues` per
`(project_id, status, priority, username)` group that actually occurs (sparse) — fed per
transaction by the sequencer. Its state is O(distinct groups), in memory, reseeded on every
boot from one group-aggregated Postgres snapshot; row data lives in Postgres.
(`ELECTRIC_IVM_DBSP_INDEXES` is deprecated and ignored: there are no engine-side row
arrangements.) No per-user, per-shape, or per-subscription structure anywhere.

## 3. Which query goes to which tier

| Queries | Tier | How |
|---|---|---|
| #4 board columns, #6 search list | **registry** | the visibility subquery (`project_id IN (SELECT project_id FROM project_members WHERE user_id = $me)`) is served by the subquery registry, like every membership subquery: two-phase creation with a Postgres backfill fenced by a `SnapshotGate`, and one shared inner-set node per distinct subquery — a user's board-column shapes (#4 × status) and search shape (#6) all share their `visibleIssues($me)` node. Status/priority/project/mine filters ride as the residual. Adding/removing the user from a project flips a value in the inner set; the flip queries that project's issues back from Postgres on the parallel flip-worker pool (`ELECTRIC_IVM_FLIP_WORKERS`) and emits them absolutely, per pk, through ordered per-stream emission lanes. |
| #9 header count | **circuit (counts)** | the predicate — member-project IN-list × selected statuses × priorities [× me] — decomposes over the counts pipeline's group columns, so the aggregate is seeded by summing matching groups and updated from each step's group deltas. The visibility-over-aggregates problem disappears because visibility is, again, a set of project cohorts. |
| #3 memberships, #5 comments | **routing** | single-column equality templates on `KeyRouter` families (`user_id`, `issue_id`); one router per template, shared by every instance. Deliberately not circuit-served — the router finds a change's shapes by index instead of scanning deltas. |
| #1 users, #2 projects | **routing** | whole-table (match-all) fan-out; nothing to compute. |
| #7, #8 ordered pages | **Postgres, by design** | keyset pages evaluated natively (subquery predicates included), merged client-side with a changes-only live tail. |
| anything ad hoc | **fallback** | stateless three-valued eval / the subquery registry; works immediately at fallback cost. |

**State**, engine-side: the sparse count groups of the counts pipeline (one integer per
occurring `(project, status, priority, username)` group, in memory) and the visibility
subquery nodes (each user's project ids + contributing membership pks) — **no copy of any
table anywhere**; row data stays in Postgres. A thousand users cost a thousand
*subscriptions* and a thousand small inner sets, zero circuit growth.

**What the circuit replaces for the header count**: the per-shape aggregate fold and its
per-shape Postgres seeding — the aggregate seeds by summing the matching count groups and
updates from each step's group deltas. The visibility shapes are the registry's job by
design: shared nodes, contributor sets, and flip query-backs against Postgres, with
move-in/move-out landing as a bounded query-back (one pooled query per flipped project)
rather than any engine-side snapshot read.

## 4. Future directions

Two upgrades are designed but not implemented:

- **Cohort-feed delivery with a client-side union**: materialize one durable feed per project
  cohort (`issues_by_project`) and serve a user's shape as the union of their cohorts' feeds,
  resolved at the delivery edge — making backfill a log replay instead of a snapshot read.
  Today each visibility shape has its own stream, seeded by its own Postgres backfill.
- **Comments inherit their issue's project** (`comments ⋈ issue→project`, keyed
  `(project, issue)`): one bilinear join that would re-home comments automatically if an
  issue moved projects — a new circuit template kind, and one that would hold row state
  engine-side, so it must be priced against the in-memory counts-only circuit. LinearLite
  doesn't move issues across projects, so `comments` stays on the `issue_id` router.
