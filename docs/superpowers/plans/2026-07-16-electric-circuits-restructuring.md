# Electric Circuits Restructuring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the `electric-ivm` repo into the public **Electric Circuits** project — a full mechanical `electric-ivm → electric-circuits` rename, deletion of tutorials and pipeline-authoring docs, and a dynamic-first rewrite of the public documentation — without changing engine logic.

**Architecture:** Four phases, rename first so doc rewrites land on final names: (1) atomic mechanical rename via three literal substitutions; (2) delete tutorial/pipeline docs and repair cross-refs; (3) rewrite the public docs dynamic-first + create one new conceptual doc; (4) align code-reference docs with a bridge note and run a final consistency sweep. Spec: `docs/superpowers/specs/2026-07-16-electric-circuits-repo-restructuring-design.md`.

**Tech Stack:** pnpm 10 workspace (13 TS packages), Rust/cargo (`apps/engine`), Docker, Markdown docs. Branch: `restructure/electric-circuits` (already created).

## Global Constraints

- **Rename target is exactly three literal tokens:** `electric-ivm` → `electric-circuits`, `electric_ivm` → `electric_circuits`, `ELECTRIC_IVM_` → `ELECTRIC_CIRCUITS_`. No legacy aliases.
- **Never touch guarded upstream tokens** (none contain the rename target, so they are safe by construction, but verify): `electric-sql`, `electricsql`, `electric-sql.com`, `electricsql/electric`, `@electric-sql/*`, the `electric-conformance/` directory. "Electric" as the brand stays.
- **Public vocabulary = three nouns:** Streams, Circuits, Live queries. "Shapes" appears ONLY in code-reference contexts (the API method `client.shape()`, the endpoint `/v1/shape`), each with a one-line bridge sentence.
- **Dynamic-first message:** verb is *write / run*, not *declare*; the circuit is generic always-on infrastructure queries register onto; aggregation's up-front `COUNT` config is a one-line detail, never a headline; persistence and CDN caching are direction, not launch topics.
- **Canonical prose sources** (reuse verbatim where possible): `docs/notes/2026-07-16-electric-circuits-story-dynamic-first.md` (L0, unlock, vocabulary) and `docs/notes/2026-07-16-electric-circuits-messaging-hierarchy.md` (v2).
- **Engine logic does not change.** No `.rs` edits beyond the mechanical token substitution.
- **Post-rename invariant:** `git grep -nE 'electric-ivm|electric_ivm|ELECTRIC_IVM_'` returns nothing (0 matches) for the rest of the project's life.
- **Commit trailer** on every commit:
  ```
  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01RtY9SSsR7vndAxDqtbhSM4
  ```

## File Structure

| Path | Disposition |
|---|---|
| all tracked files (149 touch the token) | Task 1 rename |
| `tutorials/` (whole tree) | Task 2 delete |
| `docs/building-app-pipelines.md` | Task 2 delete (tier substance → `ivm-engine-internals.md`) |
| `docs/linearlite-circuit-design.md` | Task 2 delete |
| `docs/superpowers/plans/2026-07-08-tutorial-01-first-shape.md`, `docs/superpowers/specs/2026-07-08-tutorial-01-first-shape-design.md`, `docs/superpowers/specs/2026-07-08-tutorial-03-expressive-shapes-design.md` | Task 2 delete |
| `README.md` | Task 3 rewrite |
| `docs/getting-started.md` | Task 4 rewrite |
| `docs/shapes-and-subqueries-guide.md` → `docs/live-queries-guide.md` | Task 5 rename+rewrite |
| `docs/how-queries-become-live.md` | Task 6 create |
| `apps/*/README.md`, `packages/*/README.md`, `examples/*/README.md`, `docs/memory-model.md`, `docs/notes/rows-live-in-postgres.md` | Task 7 align |

---

### Task 1: Atomic mechanical rename `electric-ivm → electric-circuits`

**Files:** every tracked file containing the token (≈149), including `package.json` (root + all packages), `apps/engine/Cargo.toml`, `apps/engine/src/**/*.rs`, `scripts/*.sh`, `docker/*`, `.github/**` (CI), and all `docs/**` / `*.md`. Excludes `.git/`, `node_modules/`, `target/`.

**Interfaces:**
- Produces: package scope `@electric-circuits/*`; crate/binary `electric-circuits-engine` + lib `electric_circuits_engine`; env vars `ELECTRIC_CIRCUITS_*`; image path `ghcr.io/balegas/electric-circuits/*`. Every later task and all scripts use these names.

- [ ] **Step 1: Snapshot guarded-token counts (to prove they're untouched later)**

```bash
cd /Users/vbalegas/workspace/dbsp-ds
git grep -cE 'electric-sql|electricsql|electric-conformance' -- ':!node_modules' | awk -F: '{s+=$2} END {print "guarded before:", s}'
```
Record the number.

- [ ] **Step 2: Apply the three literal substitutions across tracked files**

```bash
cd /Users/vbalegas/workspace/dbsp-ds
git ls-files -z -- ':!:*.png' ':!:*.jpg' ':!:*.ico' ':!:pnpm-lock.yaml' \
  | xargs -0 perl -pi -e 's/electric-ivm/electric-circuits/g; s/electric_ivm/electric_circuits/g; s/ELECTRIC_IVM_/ELECTRIC_CIRCUITS_/g;'
```
(perl `-pi` edits in place; the three subs are independent and order-free — the guarded tokens contain none of the three, so they are untouched.)

- [ ] **Step 3: Regenerate the pnpm lockfile for the renamed packages**

```bash
pnpm install
```
Expected: resolves the `@electric-circuits/*` workspace links, rewrites `pnpm-lock.yaml`, exits 0.

- [ ] **Step 4: Verify the rename is complete (the invariant)**

```bash
git grep -nE 'electric-ivm|electric_ivm|ELECTRIC_IVM_'
```
Expected: **no output** (exit 1 = no matches).

- [ ] **Step 5: Verify guarded tokens are unchanged**

```bash
git grep -cE 'electric-sql|electricsql|electric-conformance' -- ':!node_modules' | awk -F: '{s+=$2} END {print "guarded after:", s}'
```
Expected: same number as Step 1.

- [ ] **Step 6: Verify the Rust build + tests**

```bash
cargo build -p electric-circuits-engine
pnpm engine:test
```
Expected: build succeeds; tests all pass (the crate/binary/env-var renames resolve).

- [ ] **Step 7: Verify the TS workspace typechecks/builds**

```bash
pnpm -r --if-present run build 2>&1 | tail -20 || pnpm -r exec tsc --noEmit 2>&1 | tail -20
```
Expected: no unresolved `@electric-circuits/*` import errors.

- [ ] **Step 8: Smoke-boot the demo (exercises the renamed env vars end-to-end)**

```bash
scripts/linearlite.sh stop; scripts/linearlite.sh start medium
# wait for "primed" in /tmp/el-linearlite.log, then:
curl -sk https://localhost:8443/ -o /dev/null -w "%{http_code}\n"   # expect 200
scripts/linearlite.sh stop
```
Expected: demo boots, LinearLite returns 200. (If boot fails on a missing `ELECTRIC_CIRCUITS_*` var, a reference was missed — re-run Step 4's grep pattern against the failing var's old name.)

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "rename: electric-ivm -> electric-circuits (packages, crate, env vars, Docker, refs)"
```

---

### Task 2: Delete tutorials + pipeline-authoring docs; preserve tier substance; repair cross-refs

**Files:**
- Delete: `tutorials/` (whole tree), `docs/building-app-pipelines.md`, `docs/linearlite-circuit-design.md`, `docs/superpowers/plans/2026-07-08-tutorial-01-first-shape.md`, `docs/superpowers/specs/2026-07-08-tutorial-01-first-shape-design.md`, `docs/superpowers/specs/2026-07-08-tutorial-03-expressive-shapes-design.md`
- Modify: `docs/ivm-engine-internals.md` (add the tier section), plus every file that links to a deleted doc

**Interfaces:**
- Consumes: renamed tree from Task 1.
- Produces: no dangling references to the deleted paths anywhere in tracked files.

- [ ] **Step 1: Extract the three-tier serving-model section into `ivm-engine-internals.md`**

Read `docs/building-app-pipelines.md`'s "The serving model: three tiers" section (the compiled/routed/fallback tier table and its cost model). Append it as a new section `## Serving tiers: compiled, routed, fallback` at the end of `docs/ivm-engine-internals.md`, adjusting the intro sentence to drop references to the (being-deleted) companion docs. Keep it code-accurate ("shape" is fine here — engineering doc).

- [ ] **Step 2: Delete the files**

```bash
cd /Users/vbalegas/workspace/dbsp-ds
git rm -r tutorials
git rm docs/building-app-pipelines.md docs/linearlite-circuit-design.md \
       docs/superpowers/plans/2026-07-08-tutorial-01-first-shape.md \
       docs/superpowers/specs/2026-07-08-tutorial-01-first-shape-design.md \
       docs/superpowers/specs/2026-07-08-tutorial-03-expressive-shapes-design.md
```

- [ ] **Step 3: Find every dangling reference to a deleted path**

```bash
git grep -nE 'building-app-pipelines|linearlite-circuit-design|tutorials/|tutorial-01-first-shape|tutorial-03-expressive'
```
Expected referrers to fix: `README.md`, `docs/getting-started.md`, `AGENTS.md`, and any `docs/*` "Companion docs:" header. (README and getting-started are rewritten in Tasks 3–4, so fixing them there is acceptable — but remove the specific dead links now if the surrounding text survives.)

- [ ] **Step 4: Repair each referrer**

For each hit from Step 3: delete the dead link and any sentence whose sole purpose was to point at the removed doc. In `AGENTS.md`, replace the "recipe summary / building-app-pipelines" pointer with a pointer to `docs/ivm-engine-internals.md#serving-tiers-compiled-routed-fallback`. Do not leave a link to a nonexistent file.

- [ ] **Step 5: Verify no dangling references remain**

```bash
git grep -nE 'building-app-pipelines|linearlite-circuit-design|tutorials/|tutorial-01-first-shape|tutorial-03-expressive'
```
Expected: **no output**.

- [ ] **Step 6: Verify the demo still boots (scripts referenced no deleted composes)**

```bash
scripts/linearlite.sh stop; scripts/linearlite.sh start medium
curl -sk https://localhost:8443/ -o /dev/null -w "%{http_code}\n"   # expect 200
scripts/linearlite.sh stop
```

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "docs: delete tutorials + pipeline-authoring docs; move serving-tier model into internals"
```

---

### Task 3: Rewrite `README.md` (dynamic-first front door)

**Files:** Modify `README.md`

**Interfaces:**
- Consumes: renamed names (Task 1), canonical prose in `docs/notes/2026-07-16-electric-circuits-story-dynamic-first.md`.

- [ ] **Step 1: Replace the top matter and opening with the dynamic-first framing**

Title → `# Electric Circuits`. Opening: adapt the L0 headline from the story doc — "Electric Circuits make your app's queries live. Write the queries your app already runs — joins, aggregates, subqueries — …". Product name in prose is **Electric Circuits** (capitalized); the package/repo token is `electric-circuits`.

- [ ] **Step 2: Replace the "What is a shape?" section with "What is a live query?"**

Use the three-nouns vocabulary. A live query is a query whose result set is maintained live; the client receives the snapshot then `upsert`/`delete`/nothing. Add the bridge sentence once: "In the API these are created with `client.shape()` and served at `/v1/shape` (the Electric protocol name); conceptually we call them **live queries**." Keep the three-message-kinds explanation.

- [ ] **Step 3: Replace the "Designing the pipeline for your app" section entirely**

Delete the static-compile / "compiled at deploy time / access cohort" framing and the todo-app five-pipelines diagram. Replace with a short "How your queries become live" paragraph (dynamic-first): you don't build a pipeline; you write queries and they register onto a small fixed set of shared, generic dataflows — one per *kind* of query — that never grow with query or user count. Link to `docs/how-queries-become-live.md` (created in Task 6).

- [ ] **Step 4: Fix the memory paragraph with current figures**

Replace the stale "engine RSS is ~19 MiB … +~0.8 KiB per shape" claim (in the DBSP-tiers paragraph) with the current model: a fixed set of shared dataflows sized by query *kind* not instances; ~13 KiB per live query at 50k distinct live queries; flat with database size (100× rows → ~1% RSS). Source: `docs/bench/mem-reduction-log.md`, `docs/memory-model.md §5`.

- [ ] **Step 5: Keep and vocab-align the rest**

Keep the Z-sets explainer, the system diagram, "Two client surfaces", "Try it", the apps list, Docker, conformance, benchmarks, tests, and the layout table — updating any "shape" in public-prose sentences to "live query" (leave code identifiers, `/v1/shape`, `client.shape()` as-is). The layout table already shows renamed packages after Task 1.

- [ ] **Step 6: Verify vocabulary + links**

```bash
git grep -iwn shape -- README.md
```
Expected: only code-bridge lines (`client.shape()`, `/v1/shape`, package/path identifiers) — no bare-prose "shape".
```bash
grep -oE '\]\(([^)]+\.md[^)]*)\)' README.md | sed -E 's/.*\(([^)#]+).*/\1/' | while read l; do [ -e "$l" ] || echo "BROKEN: $l"; done
```
Expected: no `BROKEN` lines.

- [ ] **Step 7: Commit**

```bash
git add README.md
git commit -m "docs: rewrite README as the Electric Circuits dynamic-first front door"
```

---

### Task 4: Rewrite `docs/getting-started.md` → "your first live queries"

**Files:** Modify `docs/getting-started.md`

- [ ] **Step 1: Retitle and rewrite the intro**

Title → `# Getting started: your first live queries`. Keep the promise: point Electric Circuits at a fresh Postgres, then create and consume **live queries** with nothing but HTTP (`curl`). Update the companion-docs line to `live-queries-guide.md` (Task 5), `deployment-postgres.md`, `how-queries-become-live.md`. **Remove** the "Hands-on learners should start with `tutorials/…`" sentence.

- [ ] **Step 2: Vocab-align the body**

Replace public-prose "shape(s)" with "live query/queries" throughout; keep the actual HTTP requests and any `/v1/shape` endpoint paths unchanged (add the bridge sentence once near the first `curl`). The regular/subquery/aggregation walkthrough content stays.

- [ ] **Step 3: Verify**

```bash
git grep -iwn shape -- docs/getting-started.md      # only endpoint/code-bridge lines
grep -oE '\]\(([^)]+\.md[^)]*)\)' docs/getting-started.md | sed -E 's/.*\(([^)#]+).*/\1/' | while read l; do [ -e "docs/$l" ] || [ -e "$l" ] || echo "BROKEN: $l"; done
```
Expected: no bare-prose "shape"; no `BROKEN` links.

- [ ] **Step 4: Commit**

```bash
git add docs/getting-started.md
git commit -m "docs: getting-started -> your first live queries (vocab + drop tutorial pointer)"
```

---

### Task 5: Rename + rewrite `shapes-and-subqueries-guide.md` → `live-queries-guide.md`

**Files:** Rename `docs/shapes-and-subqueries-guide.md` → `docs/live-queries-guide.md`; Modify content

- [ ] **Step 1: Rename the file**

```bash
git mv docs/shapes-and-subqueries-guide.md docs/live-queries-guide.md
```

- [ ] **Step 2: Rewrite with the three nouns**

Title → `# Guide: live queries and subqueries`. Audience unchanged (people integrating). Replace "What a shape is" → "What a live query is"; vocab-align throughout; add the bridge sentence once. Keep the integration/deployment-sizing substance and the pointer to `ivm-engine-internals.md`.

- [ ] **Step 3: Update inbound references to the old filename**

```bash
git grep -nE 'shapes-and-subqueries-guide'
```
Fix each hit (e.g. `docs/getting-started.md`, `README.md`, other `docs/*`) to `live-queries-guide.md`. Expected after fixing: the grep returns no output.

- [ ] **Step 4: Verify**

```bash
git grep -iwn shape -- docs/live-queries-guide.md   # only code-bridge lines
```
Expected: no bare-prose "shape".

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "docs: shapes-and-subqueries-guide -> live-queries-guide (rename + vocab rewrite)"
```

---

### Task 6: Create `docs/how-queries-become-live.md` (the conceptual circuit doc)

**Files:** Create `docs/how-queries-become-live.md`

- [ ] **Step 1: Write the doc from the spec outline**

Follow the outline in the spec (`§ New doc`), dynamic-first and honest to the code. Six sections: (1) you don't build a pipeline — you write queries; (2) what a circuit is — a small fixed set of generic always-on dataflows, one per *kind* (membership, aggregation, filtering/routing); (3) why nothing multiplies (new user = value in an existing set; identical queries share; sized by kinds not instances); (4) keys and counts, never rows — rows stay in Postgres, one pooled query-back on membership flips (the inner-side-only design → memory flat in db size); (5) what you get back — live queries as Durable Streams, continued in TanStack DB; (6) one honest detail — aggregation groupings are configured per deployment today, membership/filtering are fully dynamic (link `ivm-engine-internals.md`). Reuse the "unlock" prose from `docs/notes/2026-07-16-electric-circuits-story-dynamic-first.md`. Three-nouns vocabulary only.

- [ ] **Step 2: Verify vocabulary + links**

```bash
git grep -iwn shape -- docs/how-queries-become-live.md    # only the §6 code-link, if any
grep -oE '\]\(([^)]+\.md[^)]*)\)' docs/how-queries-become-live.md | sed -E 's/.*\(([^)#]+).*/\1/' | while read l; do [ -e "docs/$l" ] || [ -e "$l" ] || echo "BROKEN: $l"; done
```
Expected: no bare-prose "shape"; no `BROKEN` links.

- [ ] **Step 3: Commit**

```bash
git add docs/how-queries-become-live.md
git commit -m "docs: add how-queries-become-live (dynamic-first circuit concept)"
```

---

### Task 7: Align code-reference docs + final consistency sweep

**Files:** Modify `apps/*/README.md`, `packages/*/README.md`, `examples/*/README.md`, `docs/memory-model.md`, `docs/notes/rows-live-in-postgres.md`

**Interfaces:**
- Consumes: all prior tasks (final state).

- [ ] **Step 1: Add the bridge sentence to user-facing code-reference READMEs**

In `examples/linearlite/README.md` (most user-facing) do a fuller vocab pass to "live queries" while keeping code identifiers. In each `packages/*/README.md` / `apps/*/README.md` that uses "shape" as a user-facing concept, add once: "This package's API calls these `shapes` (the Electric protocol term); conceptually they are **live queries**." Do not rename code symbols.

- [ ] **Step 2: Update `memory-model.md` and `rows-live-in-postgres.md` to the FeedSet reality**

Both predate the Task 2.2 FeedSet move. Update any claim that the per-feed key set / delete gate lives "in the DBSP circuit" to: it lives host-side in per-feed Roaring bitmaps (`subq_feed.rs`, `FeedSet`), reported as `bytes_feed_sets`. Keep the blog-safe claims section accurate to the current numbers (`docs/bench/mem-reduction-log.md`).

- [ ] **Step 3: Final consistency sweep**

```bash
cd /Users/vbalegas/workspace/dbsp-ds
# 3a. The rename invariant still holds:
git grep -nE 'electric-ivm|electric_ivm|ELECTRIC_IVM_'                       # expect no output
# 3b. No dangling links anywhere in docs + README:
for f in README.md $(git ls-files 'docs/**/*.md'); do
  grep -oE '\]\(([^)]+\.md[^)]*)\)' "$f" | sed -E 's/.*\(([^)#]+).*/\1/' | while read l; do
    d=$(dirname "$f"); [ -e "$d/$l" ] || [ -e "$l" ] || echo "BROKEN in $f: $l"
  done
done                                                                         # expect no BROKEN
# 3c. No bare-prose "shape" in the public docs:
git grep -iwn shape -- README.md docs/getting-started.md docs/live-queries-guide.md docs/how-queries-become-live.md
#     expect only client.shape() / /v1/shape code-bridge lines
```

- [ ] **Step 4: Verify the demo boots on the fully restructured tree**

```bash
scripts/linearlite.sh stop; scripts/linearlite.sh start medium
curl -sk https://localhost:8443/ -o /dev/null -w "linearlite %{http_code}\n"    # expect 200
curl -sk https://localhost:5443/ -o /dev/null -w "viz %{http_code}\n"           # expect 200
scripts/linearlite.sh stop
```

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "docs: code-reference bridge notes, FeedSet memory update, final consistency sweep"
```

---

## Out of scope (handoff notes, not tasks)

- **Blog post** (`../electric/website/…`) — separate track.
- **GitHub remote repo rename** (`balegas/electric-ivm` → `balegas/electric-circuits`) and **publishing images** under `ghcr.io/balegas/electric-circuits/*` — manual, the maintainer does these; all in-repo references already point at the new names after Task 1.
- **Making the counts pipeline generic/dynamic** ("unify-down") — future design item, not part of this restructuring.

## Self-Review

- **Spec coverage:** rename (T1) ✓; deletions + tier-substance preservation + cross-ref repair (T2) ✓; README/getting-started/guide rewrites (T3–T5) ✓; new conceptual doc (T6) ✓; code-reference bridge + memory-model FeedSet update + final sweep (T7) ✓; guardrails (T1 steps 1/5) ✓; out-of-scope handoff ✓.
- **Placeholder scan:** every step has an exact command or exact content directive; doc-rewrite tasks point at canonical prose sources rather than restating 300 lines — appropriate for doc tasks.
- **Name consistency:** `electric-circuits` / `electric_circuits` / `ELECTRIC_CIRCUITS_` / `@electric-circuits/*` / `electric-circuits-engine` used identically across all tasks; `docs/live-queries-guide.md` and `docs/how-queries-become-live.md` referenced by those exact paths throughout.
