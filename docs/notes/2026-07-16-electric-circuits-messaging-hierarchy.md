# Electric Circuits — messaging hierarchy (working doc)

Status: v2 draft for discussion (James + Valter), 2026-07-16. **v2 folds in the dynamic-first
message change:** the static pre-declared pipeline is de-emphasized, `COUNT`/aggregation is demoted
to a detail, the verb shifts from *declare* to *write / run*, and the core idea is now **one shared
circuit per *kind* of query, sized by kinds not instances**. Persistence is out of the post.
Scope: reframing of the electric-circuits post (`website/blog/posts/2026-07-14-electric-circuits.md`) as a
new-project announcement.

## Decisions so far

1. **Name: Electric Circuits.** "Durable stream processing" is the category descriptor under it, not the name. Resolves the DBSP/DS wordplay: *Electric Circuits — durable stream processing: live queries over Durable Streams.*
2. **Hook: app-dev pain.** Static queries / the realtime-collapses-at-scale trap. Architecture story is the spine; agents/AI is the closing vision.
3. **Positioning: fourth Electric primitive** alongside Postgres Sync, Durable Streams and Agents — but the post is written streams-first and assumes zero Electric knowledge. Electric is the brand, not a prerequisite.
4. **Maturity: launch the concept, label the code.** "Introducing Electric Circuits" + explicit research-preview labelling of the prototype. No hosted-product implication.
5. **L0 leads with the programming model, not infrastructure.** CDN fan-out and KiB-per-query numbers are supporting detail (L1/proof), not headline. The memory story is: **a small fixed set of shared dataflows — one per *kind* of query, not per query and not per user** — holding keys and counts, not rows, behind a bounded disk-backed cache, so memory doesn't explode with users or query variations.
6. **Dynamic-first (resolves old open question 3).** The verb is *write / run*, not *declare*. Queries register onto generic always-on dataflows at runtime; nothing compiles per-query or per-user. Aggregation's up-front `COUNT` configuration is demoted to an operational detail, not a headline mechanism, and the migrations/redeploy framing is dropped. Rationale: it is the honest reading of the code today, the stronger DX story ("just write the query — sharing is automatic"), and it needs no config layer to ship. *Unify-down (making the counts pipeline generic/dynamic like membership) is the one engine change this implies; it is filed as a future design item, not a launch dependency.*

## Verification vs code (2026-07-16)

All claims verified against `electric-circuits @ fc7e233` — full report in `2026-07-16-electric-circuits-claims-verification.md` (same directory): **10 verified, 4 partial, 1 contradicted**. Corrections are folded in throughout this doc. Headlines:

- **Stronger than we claimed:** the input change log literally *is* a Durable Stream today, and a non-Postgres producer already ships (library mode appends change envelopes with no Postgres in the loop). "Write to streams anyhow, query it live" is demonstrated, not roadmap.
- **Stale:** the post's memory table is a superseded benchmark snapshot; fresh benchmarks pending. Current: the dominant cost is the DBSP buffer cache, the hash structures are already Roaring bitmaps, and "flat as data grows" still holds.
- **Phrase as direction, not shipped:** CDN caching (`/v1/shape` sets `no-store` today) and the oracle suite's guarantees (restarts/concurrency come from the sibling TS suite, not the oracle runs). *(The migrations/redeploy workflow is no longer a message surface under the dynamic-first framing — see decision 6 — so it drops off this list.)*

## Vocabulary

Exactly three public nouns. "Shapes" does not appear.

| Noun             | What it is                                                   | Cost model                                                   |
| ---------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| **Streams**      | Durable Streams: the input (changes from any producer — Postgres replication or direct appends) and the output (every result is an addressable, offset-resumable stream) | Designed for CDN fan-out (not wired in the prototype)        |
| **Circuits**     | A small, fixed set of shared dataflows (DBSP) — one per *kind* of query. Your queries register onto them and run as data; the set never grows with query count | One shared dataflow per kind, holding keys and counts — not rows, not per-user state — behind a bounded disk-backed cache |
| **Live queries** | What a client holds: a parameterized registration on a circuit, delivered as a stream, continued client-side in TanStack DB | a small, bounded per-live-query cost; routing + delivery metadata only               |

Note: "live queries" deliberately matches TanStack DB's term — it is the same concept, extended server-side. That continuity is a feature of the story.

## The pyramid

### Level 0 — headline claim

> **Electric Circuits make your app's queries live.** Write the queries your app already runs — joins, aggregates, subqueries — and every result becomes a live primitive your code programs against: bind it to a component, sync it into a local collection, feed it to an agent. No fetch, poll, refetch, invalidate. Behind every result is a **circuit**: a small, fixed set of shared dataflows — one per *kind* of query, not one per query and never one per user — that maintains every result incrementally and never holds a copy of your data. Add a user, a parameter, a whole new query, and nothing new gets built: it's data flowing through a dataflow that's already there. Expressive live queries stay cheap no matter how many you run.

Compressed thesis / tagline candidates:

- "Just write the query. It's live."
- "Your queries, maintained — not per user, not per query."
- "One circuit. Every query, live."
- "The missing layer between your database and your app."

### Level 1 — three supporting messages (MECE)

**A. The trap (problem / why now).**
Apps run on static queries: fetch a snapshot, it's stale on arrival; teams bolt on polling, caches, invalidation. Every attempt to make queries live hits the same wall — *reactive demos great and collapses at scale*. The moment queries get expressive (a join, an aggregate) and audiences get real, per-user query state multiplies and memory explodes. That's why realtime demos well — and causes headaches at scale.

**B. The unlock (what it is) — three moves.**

1. *Queries run on a circuit — you don't build one.* A circuit is a small, fixed set of *generic* dataflows, always on: one for membership (which rows each user can see), one for aggregation, one for filtering and routing — a dataflow per **kind** of computation, not per query. Your queries don't compile into new operators; they *register* onto the dataflow that already handles their kind and run as data flowing through it. That's why nothing multiplies: a new user is a new value in a set the membership dataflow already maintains, a new parameter is a new key on an existing path, and a thousand clients on the same query share one maintained result and one output stream. The circuit's size is fixed by the *kinds* of query your app runs — a handful — not by the number of queries, parameters, or users, which can be unbounded. It holds only what's shared and small: the distinct values that decide membership, a live count per group — **keys and counts, never your rows.** The rows stay in Postgres; the circuit decides *what changed for whom* and streams the difference. *(A live count per group is one example of what the aggregation dataflow maintains — not a declared tier in its own right; the `COUNT` configuration knob is an operational detail for the docs, not the post.)*
2. *Logs as the substrate.* Input is — literally, today — a Durable Stream of changes; the engine is a restartable stream consumer sitting between two logs. Postgres logical replication is the **first producer**, not the only one: a producer just appends conforming change events to the stream, and library mode already does exactly that with no Postgres in the loop. Output: every live result is a Durable Stream — addressable, offset-resumable, plain HTTP (readable via the native streams protocol too). CDN caching is architecturally natural (append-only) but not wired up in the prototype — direction, not shipped.
3. *Client continuation.* Results land in TanStack DB collections; last-mile filters and further live queries run client-side on the same differential-dataflow model. The server maintains what's shared; the client personalizes.

**C. The proof (why believe).**

- Runs production Electric's own conformance oracle — harness, generators and the official client — against its `/v1/shape` endpoint; a sibling suite covers concurrency, engine restarts and resume. (Don't attach all three guarantees to the oracle runs — see verification C12.)
- Memory footprint is sublinear at large distinct-live-query and subscription counts on a fixed dataset — a small, bounded per-live-query cost — and flat with database size regardless of row count. Flat with data; small and bounded with audience. (fresh benchmarks pending; see `docs/memory-model.md §5` when drafting.)
- Server-side aggregations working: `COUNT(*)` per group is circuit-maintained; SUM/AVG/MIN/MAX are incremental engine folds — all server-side, no rescans.
- Run it yourself: interactive circuit visualizer + LinearLite demo (a real TanStack DB client with last-mile filtering — verified), clone-and-break.

### Level 2 — spine and kicker (in post order)

- **Architecture spine (mid-post):** this completes the inside-out database. Prior posts put the log on the outside (Durable Streams) and observed agents are logs. Circuits are the other half of Kleppmann's picture: views maintained incrementally from the log, delivered as subscriptions. Streams = the log, Circuits = the views, Sync = the delivery.
- **Differentiation beat (one paragraph, no comparison table):** Materialize/Feldera/Readyset point IVM at the warehouse and the data team. Circuits points it at the app: generic dataflows your queries register onto at runtime, KiB-per-subscriber, CDN fan-out, offline resume, client-side continuation.
- **Vision kicker (close):** agents don't sit on connection pools; they read and write logs. Circuits make accumulated agent state — and your database — queryable, live, everywhere. The data layer for AI-era software. ("Future of AI databases" lives here, earned, not in the headline.)

## Benefits ladder (app + agent developers)

1. Program against live primitives: bind a query to a component, sync it into a collection, feed it to an agent — no fetch/poll/refetch/invalidation code.
2. Expressive from day one: joins, aggregates, subqueries — not just filtered tables.
3. Scale without fear: a fixed set of shared dataflows sized by query *kind*, not by users or query count; state is keys and counts, not rows; disk-backed; subscribers cost KiB; designed for CDN fan-out.
4. One mental model from database to UI: the same live-query concept client- and server-side.
5. Beyond Postgres: services and agents write streams — library mode ships this today — and circuits make them queryable.

## Title candidates

- "Introducing Electric Circuits — live queries over Durable Streams"
- "Introducing Electric Circuits: just write the query, get it live"
- "Electric Circuits — the missing layer between your database and your app"
- (Amended from James's line: "…the key missing layer for reactive **apps**" — avoid "reactive programming", which scans as RxJS/FRP jargon.)

## What changes vs the current draft

- **Lede:** app-dev pain hook (static queries → realtime trap → circuits crack it). Electric-the-sync-engine no longer the frame.
- **Postgres demoted** from premise to "first producer". WAL tailer section becomes "producers".
- **"Capture, maintain, deliver"** triad maps onto producers → circuits → streams.
- **Shapes vocabulary removed**; conformance suite cited as proof, not frame.
- **Dynamic-first framing (decision 6):** the circuit is generic always-on infrastructure your queries *register onto at runtime* — not a per-app pipeline you declare and redeploy. The verb is *write / run*. `COUNT`'s up-front configuration is a one-line detail, not a message surface; the migrations/redeploy workflow is dropped.
- **Keep:** Z-sets-in-60-seconds box, memory table, subquery pipeline diagram, visualizer/"go break it" CTA.
- **Add:** research-preview label; one-paragraph differentiation beat; inside-out spine referencing the prior two posts; agents kicker.

## Risks and guardrails

1. **Overclaim vs prototype reality.** Launch the concept; label the code research preview. HN punishes the gap; the numbers are impressive unvarnished.
2. **Honest about what's dynamic vs configured.** Membership, filtering and routing are genuinely dynamic — queries register at runtime with no per-query build. Aggregation is presented as a dataflow-per-kind (true) without claiming it too is dynamic (today its groupings are configured up front). Under the dynamic-first framing (decision 6) this is a one-line detail, not a load-bearing claim, so there is no concept-ahead-of-code gap to explain away — but never state or imply that aggregate groupings register dynamically today.
3. **CDN caching is not shipped.** The prototype sets `cache-control: no-store` and the cache knob is a no-op. Keep CDN delivery as a protocol-level property and roadmap item, never a demo claim.
4. **SEO dead zone:** "electric circuits" collides with physics homework. Discovery is social/HN; docs need distinctive compounds (`/circuits`, "Electric Circuits engine").
5. **Adjacent giants:** Feldera *is* DBSP-the-company; Materialize/Readyset own "IVM for your database". Differentiation beat is mandatory; never read as "we discovered DBSP".
6. **Brand-name coupling:** "Electric Circuits" only pays off under the Electric umbrella — accepted (decision 3), the discipline is zero-prerequisite writing.

## Open questions

Resolved by verification (details in the claims report §2–3):

- ~~Producer contract~~ — **answered.** Append JSON change envelopes (`type`=table, `key`=pk, `value?`/`old?`, `headers.operation`, optional `txid/lsn/seq`) to the `changes` Durable Stream; transactions are contiguous equal-`(txid,lsn)` runs in commit order. pgoutput is not hardwired; library mode is a live second producer.
- ~~Input log a Durable Stream?~~ — **yes, literally**: ingested via `ds.append("changes")`, consumed by the sequencer via `ds.read` over HTTP.
- ~~Memory numbers~~ — **superseded in our favour**: the sublinear, small-per-live-query profile still holds at scale; Roaring bitmaps already landed; dominant cost is the bounded DBSP buffer cache. L0's "no memory explosion" claim is supported (rows stay in Postgres; state is keys/counts behind a bounded disk-backed cache). (fresh benchmarks pending)
- ~~Which declaration story leads?~~ — **resolved: dynamic-first (decision 6).** "Just write the query — sharing is automatic." *Declare* is out as the headline verb; aggregation's up-front configuration is a detail. Unify-down (generic/dynamic counts) is filed as a future design item, not a launch dependency.

Still open (for James + Valter):

1. Rename artifacts to match: repo `electric-circuits` → `circuits`? Demo URLs? The name won't stick if the artifacts contradict it.
2. Does "live queries" overloading TanStack DB's term help (same concept!) or confuse? Current call: it helps; confirm.
3. Will the cost-per-shape reduction work land before publish, and do we re-run `packages/bench/src/shape-mem-scale.ts` for final numbers? *(Landed: PR #39 merged the pk-dictionary + Roaring feed sets + bounded storage cache; the current qualitative claims are from that build. A final re-run for headline numbers is still worth doing.)*

## Next steps

- Redline this doc (James + Valter); feed in the next round of ideas.
- When the hierarchy settles: restructure the post against it (blog-planner flow), starting from title + lede.
