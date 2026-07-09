# Getting started: a new database and your first shapes

This is the from-zero walkthrough: point electric-ivm at a fresh Postgres database, then create
and consume shapes with **nothing but HTTP** — regular shapes, subqueries, and aggregations.
Everything here is bare `curl`; the client SDK (`@electric-ivm/client`) wraps exactly these
requests. Companion docs: `shapes-and-subqueries-guide.md` (concepts + the SDK),
`deployment-postgres.md` (production Postgres notes), `ivm-engine-internals.md` (how it works).
Hands-on learners should start with `tutorials/episodes/01-first-shape/README.md` for a guided walkthrough.

---

## 1. The example schema

A tiny issue tracker. Four tables, chosen so we can show every shape form: plain filters
(`issues`), a cross-table subquery (`project_members` → `issues`), and aggregations (`points`).

```sql
CREATE TABLE users (
  id      bigint  PRIMARY KEY,
  name    text    NOT NULL,
  active  boolean NOT NULL DEFAULT true
);

CREATE TABLE projects (
  id    bigint PRIMARY KEY,
  name  text   NOT NULL
);

CREATE TABLE project_members (
  id          bigint PRIMARY KEY,
  project_id  bigint NOT NULL REFERENCES projects(id),
  user_id     bigint NOT NULL REFERENCES users(id)
);

CREATE TABLE issues (
  id          bigint PRIMARY KEY,
  project_id  bigint NOT NULL REFERENCES projects(id),
  title       text   NOT NULL,
  status      text   NOT NULL DEFAULT 'todo',   -- 'todo' | 'doing' | 'done'
  priority    bigint NOT NULL DEFAULT 0,
  points      bigint                            -- nullable, for the aggregation examples
);
```

Requirements the engine puts on your tables:

- **Every replicated table needs a primary key** (single or composite — composite keys are
  supported; the key is introspected from the catalog).
- Column types map to the engine's four scalar types: `int` (bigint/int), `float`
  (double precision), `text`, `bool`.
- The engine will run `ALTER TABLE … REPLICA IDENTITY FULL` on each table at startup, so the
  connecting role must own the tables (or be superuser).

Seed a few rows so the shapes below have something to return:

```sql
INSERT INTO users   VALUES (1, 'alice', true), (2, 'bob', true);
INSERT INTO projects VALUES (10, 'engine'), (20, 'client');
INSERT INTO project_members VALUES (100, 10, 1), (101, 20, 2);
INSERT INTO issues VALUES
  (1000, 10, 'fix the tailer',    'todo',  4, 3),
  (1001, 10, 'write the docs',    'doing', 2, 1),
  (1002, 20, 'ship the client',   'todo',  5, NULL),
  (1003, 20, 'triage the inbox',  'done',  1, 2);
```

---

## 2. Point the stack at the database

Postgres is the system of record — your app keeps writing to it with ordinary SQL. The one
setting it needs is logical replication:

```conf
wal_level = logical        # requires a Postgres restart
max_replication_slots = 10
max_wal_senders = 10
```

### Option A — Docker

`pnpm docker:up` (or `docker compose -f docker/compose.yaml up`) boots the whole stack:

| service | port | role |
|---|---|---|
| `postgres` (16, `wal_level=logical`) | 5432 | system of record — run the DDL above here |
| `ds` (durable-streams server) | 8791 | the change log; shape feeds are read from here |
| `engine` (Rust) | 7010 | replication ingest + shape maintenance + `/v1/shape` |
| `api` (extended API) | 8790 | shapes / subset queries / aggregations |

### Option B — three processes by hand

```sh
# 1. durable-streams server (the log)
DS_PORT=8791 node docker/ds-server.ts

# 2. the engine, pointed at your database
export ELECTRIC_IVM_DS_URL="http://127.0.0.1:8791"
export ELECTRIC_IVM_PG_URL="postgres://user:pass@127.0.0.1:5432/appdb"
export ELECTRIC_IVM_PG_TABLES="*"        # or "users,projects,project_members,issues"
export ELECTRIC_IVM_BIND="0.0.0.0:7010"
target/release/electric-ivm-engine       # prints ENGINE_LISTENING <addr> when ready

# 3. the extended API server
DS_URL=http://127.0.0.1:8791 ENGINE_URL=http://127.0.0.1:7010 API_PORT=8790 \
  node docker/api-server.ts
```

`ELECTRIC_IVM_PG_TABLES="*"` (or empty) means *introspect every public table that has a primary
key*. On boot, per table, the engine: introspects columns/types/pk, sets
`REPLICA IDENTITY FULL`, ensures the `changes` durable stream, creates the logical
replication slot (`pgoutput` + a `<slot>_pub` publication, name from `ELECTRIC_IVM_PG_SLOT`,
default `electric_ivm`),
and starts the ingestor. Nothing else to migrate or install in the database.

Optionally, enable the engine's circuit tier — disk-spillable table arrangements, counts
pipelines, and circuit serving — with the `ELECTRIC_IVM_DBSP*` variables; the full reference
table is in `ARCHITECTURE.md` §6b. The LinearLite demo (`pnpm demo:linearlite`) runs the
engine with the full circuit configuration by default.

Check it's up:

```sh
curl http://localhost:7010/health            # → ok
curl http://localhost:7010/replication/lsn   # → {"lsn":"0/0","sync":0} until the first change is ingested
```

---

## 3. Your first shape — `GET /v1/shape`

The engine speaks the Electric wire protocol, so this is the zero-dependency way to consume a
shape: one endpoint, two request forms (snapshot, then long-poll). Here the `where` is a **SQL
string**.

### Snapshot

`offset=-1` (or omitting `offset`) asks for the shape's current rows:

```sh
curl -i -G 'http://localhost:7010/v1/shape' \
  --data-urlencode "table=issues" \
  --data-urlencode "offset=-1" \
  --data-urlencode "where=status = 'todo' AND priority >= 3"
```

```http
HTTP/1.1 200 OK
electric-handle: <shape id>
electric-offset: <tail offset>
electric-schema: {"id":{"type":"int8","pk_index":0},"project_id":{"type":"int8"},...}
electric-up-to-date:
cache-control: no-store

[
  {"headers":{"operation":"insert"},"key":"1000","value":{"id":"1000","project_id":"10","title":"fix the tailer","status":"todo","priority":"4","points":"3"}},
  {"headers":{"operation":"insert"},"key":"1002","value":{"id":"1002","project_id":"20","title":"ship the client","status":"todo","priority":"5","points":null}},
  {"headers":{"control":"up-to-date","global_last_seen_lsn":"0"}}
]
```

Notes on the format:

- Change messages are `{"headers":{"operation":…},"key":…,"value":…}`; control messages are
  `{"headers":{"control":…}}`. Values are Postgres **text-encoded** (numbers and booleans arrive
  as strings), exactly as Electric clients expect.
- Save `electric-handle` and `electric-offset` — they drive the live phase.

### Live tail

Long-poll from where the snapshot left off:

```sh
curl -i -G 'http://localhost:7010/v1/shape' \
  --data-urlencode "table=issues" \
  --data-urlencode "handle=$HANDLE" \
  --data-urlencode "offset=$OFFSET" \
  --data-urlencode "live=true"
```

Now write to Postgres from anywhere (`psql`, your app, an ORM):

```sql
UPDATE issues SET status = 'doing' WHERE id = 1000;
```

The pending long-poll returns with the delta — here the row *leaves* the shape, so it's a
`delete`:

```json
[
  {"headers":{"operation":"delete"},"key":"1000","value":{"id":"1000"}},
  {"headers":{"control":"up-to-date","global_last_seen_lsn":"0"}}
]
```

(Deletes always carry a `value` — the old row when the engine holds it, otherwise just the
primary key, as here — because Electric's client parser requires one on every change message.)

Each response carries fresh `electric-handle` / `electric-offset` / `electric-cursor` headers —
loop with the new offset. If nothing happens within the long-poll window (default 20 s,
`ELECTRIC_LIVE_TIMEOUT_MS`) you get `204 No Content`: just re-issue the request.

If the handle has been evicted (idle for `ELECTRIC_HANDLE_TTL`, default 600 s) you get
`409 Conflict` with `[{"headers":{"control":"must-refetch"}}]` — restart from `offset=-1` (the
re-snapshot rejoins the retained shape; the engine keeps idle shapes dormant for days before
evicting them — see the retention section of `apps/engine/README.md`).
Malformed requests (unknown table/column, bad `where`) are `400` with `{"message":"…"}`.

### What the SQL `where` accepts

Comparisons `= <> != < <= > >=`, `[NOT] LIKE`, `[NOT] BETWEEN a AND b`, `[NOT] IN (list)`,
`[NOT] IN (SELECT …)` (next section), `IS [NOT] NULL`, `AND` / `OR` / `NOT`, parentheses,
`true` / `false`. NULL follows SQL three-valued logic — the engine's results match a Postgres
oracle exactly, and the conformance suite asserts it.

### Projecting columns

`columns` limits *what is synced* (not what is matched); the primary key is always included:

```sh
  --data-urlencode "columns=id,title,status"
```

---

## 4. Subqueries

The one cross-table form: a column is `[NOT] IN` a single-column subquery, nestable. The
canonical use is per-user visibility — *issues in projects alice belongs to*:

```sh
curl -i -G 'http://localhost:7010/v1/shape' \
  --data-urlencode "table=issues" \
  --data-urlencode "offset=-1" \
  --data-urlencode "where=project_id IN (SELECT project_id FROM project_members WHERE user_id = 1)"
```

This shape is *live across both tables*: add alice to project 20
(`INSERT INTO project_members VALUES (102, 20, 1)`) and every issue of project 20 upserts into
her shape on the next poll; remove her and they delete. No re-query — the membership table's
own delta drives it.

Subqueries nest recursively:

```sql
project_id IN (SELECT project_id FROM project_members
               WHERE user_id IN (SELECT id FROM users WHERE active = true))
```

`NOT IN` works too, with SQL semantics: if the inner set contains a NULL, `NOT IN` is UNKNOWN
for every row — same as Postgres.

The subquery grammar is deliberately narrow: `SELECT <one column> FROM <table> [WHERE …]` —
no joins, no `EXISTS`, no correlated subqueries (see the guide §4 for what to do instead).
Identical inner subqueries across shapes share **one** maintained inner-set node on the engine,
automatically — a thousand per-user shapes cost a thousand tiny membership sets, not a thousand
copies of `issues`.

---

## 5. The extended API — shapes as resources, feeds from the log

The extended API (port 8790) is where the API is headed: it adds subset queries and
aggregations, takes predicates as a **JSON AST** instead of SQL, and separates *creating* a
shape from *reading* it — you create a shape once and read its feed directly from the
durable-streams server (port 8791), which is what makes de-duplication end-to-end: every client
of an identical shape tails the same stream.

It's tRPC (v11, no transformer) served at the URL root, so bare HTTP is simple: **mutations are
`POST /<procedure>` with the raw input as the JSON body; queries are `GET /<procedure>?input=<url-encoded JSON>`**.
Responses are wrapped in the standard tRPC envelope `{"result":{"data":…}}`.

### Create a shape

```sh
curl -s -X POST http://localhost:8790/shapes.create \
  -H 'content-type: application/json' \
  -d '{
    "table": "issues",
    "where": { "and": [
      { "col": "status",   "op": "eq",  "value": "todo" },
      { "col": "priority", "op": "gte", "value": 3 }
    ] },
    "columns": ["id", "title", "status", "priority"]
  }'
```

```json
{"result":{"data":{
  "shapeId": "<id>",
  "table": "issues",
  "streamPath": "shape/<id>",
  "streamUrl": "http://localhost:8791/shape/<id>"
}}}
```

Creating the same shape twice (predicate order doesn't matter) returns the **same** stream —
shapes are ref-counted; delete what you create
(`curl -X POST http://localhost:8790/shapes.delete -H 'content-type: application/json' -d '{"id":"<id>"}'`).

### The predicate JSON AST

| node | wire form |
|---|---|
| comparison | `{"col":"priority","op":"gte","value":3}` — `op` ∈ `eq neq lt lte gt gte` |
| null test | `{"col":"points","isNull":true}` (`false` = `IS NOT NULL`) |
| boolean | `{"and":[…]}` · `{"or":[…]}` · `{"not":…}` |
| subquery | `{"col":"project_id","in":{"table":"project_members","project":"project_id","where":…},"negated":false}` |

The subquery's inner `where` is itself a predicate and may nest further `in` leaves. The
visibility shape from §4, as AST:

```sh
curl -s -X POST http://localhost:8790/shapes.create \
  -H 'content-type: application/json' \
  -d '{
    "table": "issues",
    "where": {
      "col": "project_id",
      "in": { "table": "project_members", "project": "project_id",
              "where": { "col": "user_id", "op": "eq", "value": 1 } }
    }
  }'
```

### Read the feed (durable streams)

The feed is an ordered log of envelopes. Catch up from the beginning, then long-poll the tail:

```sh
# backfill: everything from the start
curl -i "http://localhost:8791/shape/$SHAPE_ID?offset=-1"

# live tail: long-poll from the last stream-next-offset
curl -i "http://localhost:8791/shape/$SHAPE_ID?offset=$NEXT&live=long-poll"
```

Each response carries `stream-next-offset` (opaque resume token — always read it back) and
`stream-up-to-date` (present when you're caught up); a long-poll that times out with no data is
`204`. The body is a JSON array of State-Protocol envelopes — shape feeds carry **absolute**
membership, just two operations:

```json
[
  {"type":"issues","key":"1000","value":{"id":1000,"title":"fix the tailer","status":"todo","priority":4},
   "headers":{"operation":"upsert"}},
  {"type":"issues","key":"1000","headers":{"operation":"delete","lsn":"0/1A2B3C4"}}
]
```

`upsert` = the row is in the shape (entered, or changed while inside); `delete` = it left.
Applying envelopes in order to a `key → value` map always yields exactly the shape's current
result set.

### Subset queries — ordered pages

Shapes have no `ORDER BY`/`LIMIT`; pagination is a one-shot **subset query** (pair it with
`subset.live` for a shared changes-only tail — see the guide §5):

```sh
curl -s -G 'http://localhost:8790/subset.query' \
  --data-urlencode 'input={"table":"issues","orderBy":{"col":"priority","desc":true},"limit":2,
                          "where":{"col":"status","op":"eq","value":"todo"}}'
```

```json
{"result":{"data":{"rows":[{"id":1002,"priority":5,"...":"..."},{"id":1000,"priority":4,"...":"..."}],
           "lsn":"0/1A2B3C4"}}}
```

The returned `lsn` positions the page against the live tail: drop tail deltas with a smaller
`lsn` and the merge is exact.

---

## 6. Aggregations

A live scalar over a predicate — `count`, `sum`, `avg`, `min`, `max` — maintained as an
incremental fold and delivered on a feed like any shape. Create one:

```sh
curl -s -X POST http://localhost:8790/aggregate.create \
  -H 'content-type: application/json' \
  -d '{
    "table": "issues",
    "fn": "sum",
    "col": "points",
    "where": { "col": "status", "op": "neq", "value": "done" }
  }'
```

Same `ShapeHandle` response; `col` is required for `sum`/`avg`/`min`/`max` and ignored for
`count`. The aggregate `where` takes leaf/boolean predicates (no subqueries). Read the feed the
same way (`GET http://localhost:8791/shape/$SHAPE_ID?offset=-1…`); the running value is a single
envelope keyed `"agg"`, re-emitted on every change that moves it:

```json
{"type":"issues","key":"agg","value":{"value":4,"n":3},"headers":{"operation":"upsert"}}
```

`value` is the aggregate (`null` when `sum`/`avg`/`min`/`max` has no non-null input rows);
`n` is the matching row count. SQL NULL semantics throughout: `count(col)` counts non-null
values only, and `min`/`max` retract correctly (delete the current minimum and the next one is
emitted — the fold is retraction-aware, not append-only).

With the seed data above: `sum(points) WHERE status <> 'done'` is `4` over `n=3` matching rows —
issue 1002's NULL `points` contributes to `n` but not to the sum. Now
`UPDATE issues SET points = 8 WHERE id = 1002;` and the feed emits
`{"value":{"value":12,"n":3}}` — one envelope, no re-scan.

Identical aggregations are de-duplicated like shapes: any number of dashboards subscribing to
the same live count share one fold and one feed.

---

## 7. Endpoint quick reference

| you want | request |
|---|---|
| health / replication position | `GET :7010/health` · `GET :7010/replication/lsn` |
| shape, Electric protocol (SQL where) | `GET :7010/v1/shape?table=…&offset=-1[&where=…&columns=…]`, then `…&handle=…&offset=…&live=true` |
| shape, extended API (JSON AST) | `POST :8790/shapes.create` → handle; `POST :8790/shapes.delete` |
| read a shape/aggregate feed | `GET :8791/shape/<id>?offset=-1`, then `…?offset=<next>&live=long-poll` |
| ordered page | `GET :8790/subset.query?input=<json>` (+ `POST :8790/subset.live` for the tail) |
| live scalar | `POST :8790/aggregate.create` → feed keyed `"agg"`, value `{value,n}` |

Ports are the Docker defaults: engine `7010`, extended API `8790`, durable streams `8791`.
