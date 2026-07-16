# Electric Circuits — the dynamic-first story

Status: rewrite for the blog post, 2026-07-16. Supersedes the "declare your queries as a
circuit" framing in the messaging working doc: static pre-declared pipeline de-emphasized,
`COUNT`/aggregation demoted to a detail, persistence out of scope. Keeps the core idea — **one
shared circuit per *kind* of query, sized by kinds not instances**.

---

## L0 — headline

> **Electric Circuits make your app's queries live.** Write the queries your app already runs —
> joins, aggregates, subqueries — and every result becomes a live primitive your code programs
> against: bind it to a component, sync it into a local collection, feed it to an agent. No fetch,
> poll, refetch, invalidate. Behind every result is a **circuit**: a small, fixed set of shared
> dataflows — one per *kind* of query, not one per query and never one per user — that maintains
> every result incrementally and never holds a copy of your data. Add a user, a parameter, a whole
> new query, and nothing new gets built: it's data flowing through a dataflow that's already there.
> Expressive live queries stay cheap no matter how many you run.

**Tagline candidates (dynamic-first):**

- "Just write the query. It's live."
- "Your queries, maintained — not per user, not per query."
- "One circuit. Every query, live."

---

## The unlock — queries run on a circuit, you don't build one

> A circuit is a small, fixed set of *generic* dataflows, always on. There's one for membership
> (which rows each user can see), one for aggregation, one for filtering and routing — a dataflow
> per **kind** of computation, not per query. Your queries don't compile into new operators; they
> *register* onto the dataflow that already handles their kind, and run as data flowing through it.
>
> That's why nothing multiplies. A new user is a new value in a set the membership dataflow already
> maintains. A new parameter is a new key on an existing path. A thousand clients opening the same
> query share one maintained result and one output stream. The circuit's size is fixed by the
> *kinds* of query your app runs — a handful — not by the number of queries, parameters, or users,
> which can be unbounded. And it holds only what's shared and small: the distinct values that decide
> membership, a live count per group — **keys and counts, never your rows.** The rows stay in
> Postgres; the circuit decides *what changed for whom* and streams the difference.
>
> This is the whole trick behind "reactive that doesn't collapse at scale." The moment queries get
> expressive — a join, a subquery — and audiences get real, the naïve approach multiplies per-user
> query state and memory explodes. A circuit refuses to multiply: expressiveness lives in the
> *kinds* of dataflow, which are fixed; scale lives in the *data* flowing through them, which is
> cheap.

A *live count per group* is one example of what the aggregation dataflow maintains — not a declared
tier in its own right. (The `COUNT` configuration knob is an operational detail for the docs, not
the blog.)

---

## Vocabulary — the Circuits row

| Noun | What it is | Cost model |
|---|---|---|
| **Circuits** | A small, fixed set of shared dataflows (built on DBSP) — one per *kind* of query. Your queries register onto them and run as data; the set never grows with query count | One shared dataflow per kind, holding keys and counts — not rows, not per-user state — behind a bounded disk-backed cache |

---

## What changed vs. the messaging working doc

- **Removed:** "Declare the queries… as a circuit", "declared like schema", "changing the set is a
  redeploy", the migrations analogy, and the code-vs-concept note about the declared counts tier.
  The verb is no longer *declare* — it's *write / run*.
- **Kept and strengthened:** the core — **one circuit, shared, sized by the *kinds* of query, not by
  instances**; keys-and-counts-not-rows; automatic sharing across identical queries.
- **Count:** demoted to a one-line example — no env var, not "declared".
- **Persistence:** out (headline, unlock, and vocabulary).
- **Honest to the code:** everything asserted is true today — membership, filtering and routing are
  genuinely dynamic; aggregation is presented as a dataflow-per-kind (true) without claiming it is
  dynamic (it isn't, but we don't deny it). No overclaim.
