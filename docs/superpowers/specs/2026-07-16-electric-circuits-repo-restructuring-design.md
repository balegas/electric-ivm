# Electric Circuits — repo restructuring design

Status: design for implementation, 2026-07-16. Turns the `electric-circuits` repo into the public
**Electric Circuits** project: a full mechanical rename, deletion of tutorials and
pipeline-authoring docs, and a dynamic-first rewrite of the public documentation. Engine logic
does not change; the rename is mechanical.

Message basis: `docs/notes/2026-07-16-electric-circuits-messaging-hierarchy.md` (v2, dynamic-first)
and `docs/notes/2026-07-16-electric-circuits-story-dynamic-first.md`.

## Goal

Make the repository presentable and coherent for a public "Introducing Electric Circuits" launch:
one product identity (`electric-circuits`), one public vocabulary (three nouns, no "shapes"), and a
documentation set that tells the dynamic-first story — while every engineering/proof doc stays
code-accurate.

## Locked decisions

1. **Delete for real** (git preserves history — done in Task 2): `tutorials⁄` (whole tree), `docs/building‑app‑pipelines.md`, `docs/linearlite‑circuit‑design.md`, and the tutorial process artifacts under `docs/superpowers/{plans,specs}/*tutorial*`.
2. **Full rename `electric-circuits` → `electric-circuits`**, extended to code: npm packages, the Rust crate/binary, Docker image paths, env vars, and all imports/string references. No legacy aliases.
3. **Blog post out of scope** — `../electric/website/blog/posts/2026-07-14-electric-circuits.md` is a separate track (blog-planner flow, sibling repo).
4. **Dynamic-first message** — the verb is *write / run*, not *declare*; the circuit is generic always-on infrastructure queries register onto; aggregation's up-front `COUNT` configuration is a one-line detail, never a headline; persistence and CDN caching are not launch doc topics.

## Vocabulary mapping

Three public nouns: **Streams**, **Circuits**, **Live queries**. "Shapes" does not appear in
public/conceptual docs.

| Old (shapes-era) | New (public) | Where "shape" may still appear |
|---|---|---|
| shape | live query | Code-reference only: the API method is `client.shape()`, the endpoint is `/v1/shape` (Electric protocol) — kept accurate, with a one-line bridge ("the API calls these `shapes`; we call the concept a live query"). |
| shape pipeline / "build the pipeline" | the circuit (generic, shared) | — |
| declared query set / redeploy | queries register at runtime | — |

**Bridge rule:** in code-reference docs (package READMEs, API docs), keep the real symbol names
(`shape()`, `/v1/shape`) and add the bridge sentence once. In public/conceptual docs (README,
getting-started, live-queries-guide, how-queries-become-live), use only the three nouns.

## The rename (mechanical)

Blast radius measured at `d65daee`: **149 files, 468 `electric-circuits` occurrences, 381 `ELECTRIC_CIRCUITS_`
occurrences across 38 distinct env vars, 13 npm packages, the crate in 3 spots.**

Concrete substitutions:

- `@electric-circuits/<pkg>` → `@electric-circuits/<pkg>` (all 13: api, bench, client, conformance, docker, ds-rust, examples, linearlite, loadgen, oracle, pipeline-viz, protocol, web) — in every `package.json` `name`/`dependencies`/`devDependencies` and every import specifier.
- `electric-circuits-engine` → `electric-circuits-engine` (crate name, binary name); `electric_circuits_engine` → `electric_circuits_engine` (Rust lib name / underscore form); `ELECTRIC_CIRCUITS_ENGINE_PREBUILT` and friends follow the env-var rule below.
- `ELECTRIC_CIRCUITS_<X>` → `ELECTRIC_CIRCUITS_<X>` (all 38 vars, in engine `config.rs`, scripts, demos, docker, docs, CI).
- `ghcr.io/balegas/electric-circuits/{engine,node}` → `ghcr.io/balegas/electric-circuits/{engine,node}`.
- Prose/product name → "Electric Circuits"; the `electric-circuits` project token → `electric-circuits`.

**Guardrails (must NOT be renamed — these refer to upstream ElectricSQL, not to us):**

- `electric-sql`, `electricsql`, `electric-sql.com`, `electricsql/electric`, `@electric-sql/*`.
- The `electric-conformance/` **directory name** stays (it means "conformance against Electric"), as do
  genuine upstream references inside it (`ElectricSQL`, `Electric.Client`, `ELECTRIC_DIR`). But that
  directory's contents also reference **our own** product/symbols — the renamed npm packages
  (`@electric-circuits/bench`), the renamed crate (`cargo build -p electric-circuits-engine`), our own
  env var (`ELECTRIC_CIRCUITS_DIR`), and our oracle test filenames — and those MUST be renamed like
  everywhere else, or the harness (`pnpm --filter`, `cargo -p`, `run.sh`'s `cp`) breaks. Renaming a
  content string that names a test file (`run.sh`) also requires renaming that file on disk to match —
  `perl -pi` edits contents, not filenames. (Correction applied post-Task-1: the two oracle `.exs`
  files were `git mv`'d to their new names after the content pass.)
- The word "Electric" as the brand/umbrella.

The rename target is specifically the old product compound, which is unambiguous — none of the
guarded upstream tokens contain it. Post-rename, `git grep` for the old compound returns **0**.

**Not automatable by the agent (manual, note in handoff):** renaming the GitHub remote repository
(`balegas/electric-circuits` → `balegas/electric-circuits`) and publishing images under the new ghcr path.
The local checkout directory (`dbsp-ds`) is irrelevant and unchanged.

## Documentation dispositions

**Delete:** `tutorials⁄` (README + 4 episodes + compose + seed), `docs/building‑app‑pipelines.md`,
`docs/linearlite‑circuit‑design.md`, `docs/superpowers/plans/2026-07-08-tutorial‑01‑first‑shape.md`,
`docs/superpowers/specs/2026-07-08-tutorial‑01‑first‑shape-design.md`,
`docs/superpowers/specs/2026-07-08-tutorial‑03‑expressive-shapes-design.md`. (Done — Task 2.)

**Rewrite (public, dynamic-first + new vocabulary):**

- `README.md` — new front door. Lede is the app-dev pain hook → the circuit unlock (dynamic-first) → proof. Drop the "Designing the pipeline for your app" section and the static-compile framing entirely; replace with the "write queries, they register" model. Keep the Z-sets-in-60-seconds explainer, the system diagram, the conformance/benchmarks/tests sections, the layout table (with renamed packages), and the "Try it" demo. Replace the memory paragraph's stale `~19 MiB / ~0.8 KiB per shape` figures with the current model (a fixed set of shared dataflows per kind; ~13 KiB per live query at 50k; flat with data).
- `docs/getting-started.md` → "Getting started: your first live queries". Keep the bare-`curl` walkthrough (it is the honest, SDK-free path), retitle and re-vocabulary, and **remove the pointer to `tutorials⁄`** (done in Task 2).
- `docs/shapes-and-subqueries-guide.md` → rename to `docs/live-queries-guide.md`, rewrite with the three nouns; it stays the integration/deployment-sizing guide.

**Create:** `docs/how-queries-become-live.md` — the conceptual doc that *is* the "define your app's
circuit" experience under the dynamic-first reading (outline below). It occupies the role the
now-deleted pipeline-authoring doc held, from the correct angle.

**Keep, with light vocabulary alignment only where a passage is public-facing (these are
code-accurate engineering/proof docs; "shape" stays where it names code):**
`docs/ARCHITECTURE.md`, `docs/ivm-engine-internals.md`, `docs/memory-model.md`,
`docs/deployment-postgres.md`, `docs/fleet-conformance.md`, `docs/bench/*`,
`docs/notes/rows-live-in-postgres.md`, and all `apps/*/README.md` / `packages/*/README.md`
(code-reference + one bridge sentence where user-facing; `examples/linearlite/README.md` is the most
user-facing and gets the fuller vocab pass).

**Cross-reference repair (done in Task 2):** every doc that links to a deleted file was fixed. Known
referrers: `README.md` (linked `building‑app‑pipelines.md`), `docs/getting-started.md` (linked
`tutorials⁄episodes/01-first-shape`), `AGENTS.md` (referenced the "recipe summary" / pipeline docs),
`docs/linearlite‑circuit‑design.md` referrers, and any `docs/*` "Companion docs:" headers pointing at
the removed files. **Decided:** the three-tier serving-model substance from the now-deleted
pipeline-authoring doc (the compiled/routed/fallback tier table and its cost model) is preserved
by moving it into `docs/ivm-engine-internals.md` (engineering doc) as a new section — not re-exposed
publicly. Nothing valuable is lost to git history alone.

## New doc: `docs/how-queries-become-live.md` (outline)

Audience: an app developer asking "how do I make my app's queries live / what's the circuit?"
Dynamic-first, honest to the code.

1. **You don't build a pipeline — you write queries.** The circuit is already there.
2. **What a circuit is:** a small, fixed set of generic always-on dataflows, one per *kind* of query — membership (visibility), aggregation, filtering/routing. Your query registers onto the dataflow for its kind and runs as data through it.
3. **Why nothing multiplies:** a new user is a value in a set the membership dataflow already maintains; a new parameter is a key on an existing path; identical queries share one maintained result and one output stream. Size is fixed by *kinds*, not instances.
4. **Keys and counts, never rows:** the circuit holds the distinct values that decide membership and per-group counts; rows stay in Postgres. On a membership flip the engine does one pooled query-back to Postgres for the rows entering scope — the deliberate inner-side-only design (this is why engine memory is flat in database size).
5. **What you get back:** live queries delivered as Durable Streams, continued client-side in TanStack DB.
6. **One honest detail (not a headline):** aggregation groupings are configured per deployment today; membership and filtering are fully dynamic. (Link to `ivm-engine-internals.md` for the mechanism.)

## Phasing (for the implementation plan)

Rename first, so the doc rewrites land on final names.

1. **Mechanical rename** — all 149 files. Verify: `cargo build` + `pnpm engine:test` green; TS typecheck/build green; `git grep electric-circuits` returns 0; guarded upstream tokens (`electric-sql`, `electric-conformance/`) untouched (spot-check counts unchanged).
2. **Deletions + cross-ref repair** — remove the tutorial/pipeline docs; fix every dangling link. Verify: the deleted paths are gone; `git grep -F` for each removed filename returns 0 in remaining tracked files; `scripts/linearlite.sh` and demo entry points don't reference deleted composes; the LinearLite demo still boots.
3. **Public-doc rewrite** — README, getting-started, live-queries-guide (renamed), and the new how-queries-become-live.md. Verify: these four/five files use only the three nouns; `git grep -iwn shape <those files>` yields only code-bridge contexts (`client.shape()`, `/v1/shape`); the memory figures match `docs/bench/mem-reduction-log.md` / `docs/memory-model.md §5`.
4. **Internal-doc vocab alignment + final sweep** — bridge sentences in code-reference READMEs; consistency pass. Verify: no broken intra-repo links (a link-extraction grep over `docs/**` + `README.md` resolves every relative target); the demo boots; product name is "Electric Circuits" everywhere in prose.

## Out of scope

- The blog post (separate track).
- Renaming the GitHub remote repo and publishing new-path images (manual handoff steps).
- Any engine logic change, including making the counts pipeline generic/dynamic ("unify-down") — filed as a future design item, not a launch dependency.
- CDN caching and persistence as documentation topics (explicitly excluded from the launch message).

## Risks

1. **Rename over-reach.** A careless global replace could hit guarded upstream tokens. Mitigation: target the exact `electric-circuits` compound (and its `electric_circuits` / `ELECTRIC_CIRCUITS_` variants) only; verify guarded-token counts are unchanged after the pass.
2. **Env-var churn breaking demos/CI.** 381 references across scripts, docker, CI. Mitigation: the rename is a single coordinated pass; Phase 1 verification includes booting the demo and running the test suites, which exercise the env vars end-to-end.
3. **Public/code vocabulary gap confusing readers.** The API still says `shape()`. Mitigation: the bridge rule — one explicit mapping sentence in code-reference docs, three-nouns-only in public docs.
4. **Message drift into overclaim.** Mitigation: the story doc's honesty guardrails (aggregation configured-not-dynamic; persistence/CDN as direction) are carried into the new/rewritten docs verbatim in intent.
