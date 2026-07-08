# A single dbsp circuit for all of LinearLite's queries

Design study: can one deploy-time dbsp circuit serve every query the LinearLite app makes?
Answer: yes — the app's whole query surface collapses to seven templates, none of which needs
per-user state. This documents the inventory, the circuit, and what it would replace.
(Design only; the engine's dynamic-shape path is unchanged. See `ARCHITECTURE.md` §6b for the
arrangement layer that exists today — this is the "Level 3" end state of that ladder.)

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
  already exploits this: the browse view mounts one `ProjectSubsetFeed` **per member project**
  and merges client-side, and the count expands visibility to an explicit project-id list
  (`aggProjects = memberIds`). The per-user subquery (`visibleIssues($me)`) is per-user only in
  its *routing*, not in its *data*.
- **Ordering/pagination is deliberately not IVM.** Subsets (#7, #8) are keyset pages evaluated
  by Postgres with a changes-only live tail. That division of labor is good; the circuit
  should not absorb it.

## 2. The circuit

Inputs: one Z-set input per table (`issues`, `comments`, `projects`, `users`,
`project_members`), fed per transaction by the sequencer exactly as the arrangement layer is
today. Seven output pipelines, all fixed at deploy time — **no per-user, per-shape, or
per-subscription structure anywhere**:

```text
users ───────────────────────────────────────────▶ (A) users_all         one global feed
projects ────────────────────────────────────────▶ (B) projects_all      one global feed
project_members ── map_index(user_id) ───────────▶ (C) memberships_by_user   per-user feed
                                                       = query #3 AND the delivery router:
                                                       C's deltas drive subscribe/unsubscribe
                                                       to the project-cohort feeds below

issues ─┬─ map_index(project_id, LIST_COLUMNS) ──▶ (D) issues_by_project      per-project feed
        ├─ map_index((project_id, status),
        │            BOARD_COLUMNS) ─────────────▶ (E) board_columns          per-(project,status) feed
        └─ map_index((project_id, status,
        │             priority, username))
        │  .aggregate_linear(count) ─────────────▶ (G) issue_counts           per-group live counts
comments ── map_index(issue_id) ─────────────────▶ (F) comments_by_issue      per-issue feed
```

How each query is served:

- **#1/#2** → feeds A/B verbatim.
- **#3** → feed C keyed by the requesting user.
- **#4 (board)** → feed E: a user's board column for status S = the union of `(P, S)` feeds
  over their member projects (delivery resolves the union via C); with a project filter it is
  exactly one feed. No subquery machinery at all.
- **#5 (comments)** → feed F per issue. (Optional upgrade: `comments ⋈ issue→project`, keyed
  `(project, issue)`, makes an issue moving projects re-home its comments automatically — one
  bilinear join. LinearLite doesn't move issues across projects, so v1 skips it.)
- **#6 (search list)** → feed D per member project, merged client-side — which is already the
  app's browse architecture; status/priority/mine narrowing stays a client filter over the
  synced window.
- **#7/#8 (ordered pages)** → unchanged: Postgres keyset pages, with the *live tail* now being
  feed D (changes-only per project) instead of a per-shape stream.
- **#9 (header count)** → feed G: `COUNT` is linear, so `aggregate_linear` maintains one
  integer per `(project, status, priority, username)` group that actually occurs (sparse).
  The header count = sum over the groups matching (member projects × selected statuses ×
  selected priorities [× me]). The client (or delivery) sums a few dozen integers; the
  "aggregations don't take subquery predicates" limitation disappears because visibility is,
  again, a set of project cohorts.

## 3. What this replaces, and what it costs

**Replaced for this app**: the subquery registry (nodes, contributor sets, flip query-backs),
per-shape snapshot gates for issue/comment shapes, and the per-shape aggregate folds. Move-in/
move-out (user added to / removed from a project) stops being computation entirely: it is a
delta on feed C, which delivery turns into subscribe + replay-cohort-log / unsubscribe. Backfill
of a newly visible project = reading feed D's durable log from offset 0 — no Postgres snapshot,
no xmin fencing.

**State** (all in disk-spillable dbsp arrangements): issues ×3 (by project; by project+status;
count groups), comments ×1, members ×1, users/projects ×1 — a small constant factor over one
table copy, independent of user count. A thousand users cost a thousand *subscriptions*, zero
engine state.

**What stays outside the circuit**: ordering/pagination (Postgres, by design), text search
(client-side), and the generic dynamic-shape path for ad-hoc predicates — the standalone
evaluator and KeyRouter remain for shapes that don't match a template.

## 4. The generalization

The recipe that produced this circuit, applicable to any app on the engine. The load-bearing
separation: **pipelines are few and fixed; shapes are many and dynamic.** Shape cardinality —
one shape per parameter combination clients ask for, e.g. an issues filter per combination of
projects — can vastly exceed pipeline cardinality, because a shape is just a selection/union
of cohort groups from one pipeline's keyed output, materialized at the delivery edge. The
circuit never grows with shape count; only the routing table does.

1. **Enumerate call sites** and collapse them to templates — parameters become data.
2. **Find the access cohort** (here: project). Key every feed by cohort, never by user.
   Per-user predicates that are genuinely per-user (`username = $me`) get their own small
   keyed feed, same pattern.
3. **Visibility relations become the delivery router** (feed C), not a join input — until an
   app needs server-side per-user materialization, no membership join is needed at all.
4. **Linear operators are free** (filter/project/index); **joins and aggregates knowingly**
   (each join stores both inputs — acceptable now that spilling works; `aggregate_linear` is
   cheap).
5. **Structure ships with deploys.** New templates = circuit rebuild + reseed (or
   `Mode::Persistent` bootstrap); ad-hoc queries fall back to the dynamic-shape path.
