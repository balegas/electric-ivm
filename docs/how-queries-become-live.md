# How your queries become live

Audience: people building an app on Electric Circuits who want the mental model behind "it's just
live" — not the engine's internal routing and fallback machinery (that's
`docs/ivm-engine-internals.md`, for people working on the engine itself). This doc explains what a
**circuit** is, why registering a query onto one is cheap, and what actually stays in memory versus
in Postgres.

---

## 1. You don't build a pipeline — you write queries

You don't design a dataflow, declare a pipeline, or provision anything for a new query. You write
the query your app already needs — a filter, a per-user visibility check, a live count — and it
becomes a **live query**: a result that Electric Circuits keeps in sync as Postgres changes,
delivered to your app as a **Stream**.

The thing that makes that possible — a **circuit** — is already there before you write a single
query. It doesn't get built per query, per user, or per deploy of your app's data. It's a small,
fixed, always-on piece of infrastructure that your queries register onto and run through. Writing a
new query is an act of registration, not construction.

## 2. What a circuit is

A **circuit** is a small, fixed set of generic, always-on dataflows — one per *kind* of query, not
one per query:

- one for **membership** — which rows a user, tenant, or filter currently makes visible;
- one for **aggregation** — a live count, sum, or similar fold per group;
- one for **filtering and routing** — matching a stream of changes against the predicates your
  live queries define, and getting each matching change to the right place.

Your query doesn't compile into a new operator or spin up new dataflow stages. It **registers**
onto the circuit that already handles its kind, and from then on it runs as data flowing through
that circuit — a key added to a routing structure, a subscriber added to a maintained result. The
circuit was going to be running with or without your query; your query just gives it one more thing
to carry.

This is the core structural idea worth carrying through the rest of this doc: **expressiveness
lives in the kinds of dataflow, which are fixed; scale lives in the data flowing through them,
which is cheap.**

## 3. Why nothing multiplies

Because a circuit is generic per kind and not compiled per query, the things that normally make
live systems expensive — more users, more parameters, more identical queries — don't add new
circuit structure:

- **A new user is a new value in a set the membership circuit already maintains**, not a new
  membership pipeline. Adding user 4,001 to a project is one value entering one dataflow that
  already exists.
- **A new parameter is a new key on an existing path.** A tenant filter for `tenant = 8` isn't a
  new filtering circuit; it's a new key routed through the filtering circuit every other tenant
  filter already uses.
- **A thousand clients opening the identical live query share one maintained result and one output
  Stream**, ref-counted. The circuit does the work once and every subscriber rides the same
  Stream.

So the circuit's size is fixed by the *kinds* of query your app runs — a handful — not by the
number of live queries, parameters, or users running through it, which can be unbounded. Add a
whole new user, a whole new parameter, a whole new live query of a kind the circuit already knows,
and nothing new gets built: it's data flowing through a dataflow that's already there.

## 4. Keys and counts, never rows

A circuit holds only what's shared and small: the distinct values that decide membership, and a
live count per group. It does not hold your rows. Rows stay in Postgres — the circuit decides
*what changed for whom* and streams the difference.

Take the per-user visibility case: the membership circuit maintains, per user, the small set of
keys (project ids, say) that decide what that user can see — not the issues themselves. When that
set changes for a user — a membership row is added or removed and a key flips into or out of the
set — the engine does **one pooled query-back to Postgres** to fetch the rows now entering or
leaving that user's scope, and emits exactly those as upserts or deletes on the user's Stream.

That's a deliberate design choice: the circuit only ever reaches into Postgres for the *inner* side
of a membership check (the small set of keys), never to materialize the *outer* side (the
potentially large set of rows those keys select). Rows are fetched on demand, in a pooled batch,
only for what just changed — never held speculatively, never duplicated per subscriber.

This inner-side-only design is why engine memory is **flat with database size**: growing the
tables underneath your app doesn't grow what the circuit holds, because the circuit was never
holding your data in the first place. What it holds — keys and counts — scales with your app's
membership and grouping structure (how many users, how many groups), not with how many rows sit
behind them.

## 5. What you get back

A live query doesn't hand you a one-time result — it hands you a Stream: your app receives the
current matching rows, then a live feed of exactly what changed, forever, as **upserts** and
**deletes**. That Stream is a **Durable Stream** — Electric Circuits' durable, replayable log
primitive — so it survives reconnects and can be resumed from any point, not just followed live.

What you do with that Stream client-side is up to your app: bind it directly to UI, feed it to an
agent, or sync it into a local collection and layer a **client-side live query** (TanStack DB's
`useLiveQuery`) on top for presentation concerns the circuit deliberately doesn't do —
ordering, text search, finer-grained filtering of an already-synced set. The circuit's job ends at
"here is what changed"; everything downstream of the Stream is the client library's job, not the
circuit's.

## 6. One honest detail

Membership and filtering/routing, as described above, are fully dynamic: register a new user, a
new parameter, a brand-new predicate the circuit has never seen, and it's served immediately, no
redeploy required.

Aggregation is the one place where that isn't the whole story yet. The aggregation circuit is
real and generic in the same sense as the others — one dataflow per aggregate *family*, shared
across every query of that kind — but today, which groupings it maintains is **configured per
deployment**, not discovered on the fly from whatever queries your app happens to run. A live
count you didn't configure ahead of time still works, just outside the circuit's fast path, and
gets promoted into the circuit at the next deploy if it turns out to matter. The mechanism behind
that — how a query lands in the circuit versus a fallback path, and what that costs — is documented
for engine developers in `docs/ivm-engine-internals.md`.
