# PGlite as a Postgres Oracle for Node Tests

Research brief on using `@electric-sql/pglite` as an in-memory, ephemeral Postgres
"oracle" inside Node test suites: build a real Postgres from a schema, apply
INSERT/UPDATE/DELETE (upsert by PK), run `SELECT * FROM t WHERE <clause>`, and
compare the typed rows against a separately-materialized client state.

All API claims below were verified empirically against installed version **0.5.3**
on **Node v24.5.0** (macOS), unless explicitly marked *unverified*.

---

## TL;DR

- PGlite is a full Postgres compiled to WASM that runs in-process. No server, no
  Docker, no network. `new PGlite()` with no arguments gives a fresh in-memory DB.
- It is an honest Postgres (the real planner/executor), so it is an excellent
  differential oracle for query semantics.
- `query(sql, params)` uses the extended protocol with `$1` placeholders and returns
  `{ rows, fields, affectedRows }`. `exec(sql)` runs multi-statement scripts (no params).
- JS↔PG type mapping is sane for your column set: `INTEGER`→number, `TEXT`→string,
  `BOOLEAN`→boolean, `DOUBLE PRECISION`/`REAL`→number, NULL→`null`. Watch `BIGINT`
  (number-or-BigInt) and `NUMERIC` (string) — see Type Mapping.
- One exclusive connection per instance; isolate tests by creating a fresh instance
  (cheapest correctness) or by `DROP`/`TRUNCATE` between cases.

---

## Version & Install

- Package: `@electric-sql/pglite`
- Latest stable: **0.5.3** (npm `latest`; published 2026-06-16). A `next` tag exists
  (`0.3.0-next.1`) — ignore it for production use.
- ESM-only package (`"type": "module"`). No `engines` field is declared; works on
  modern Node (tested on 24.x). Node 18+ recommended.

```bash
npm install @electric-sql/pglite
# pnpm add @electric-sql/pglite   /   yarn add @electric-sql/pglite   /   bun add @electric-sql/pglite
```

```js
import { PGlite } from '@electric-sql/pglite'
```

Because the package is ESM-only, use `.mjs`, `"type": "module"`, or a test runner
configured for ESM (Vitest works out of the box; Jest needs ESM/transform config).

---

## Constructing an in-memory instance

Three equivalent ways to get an ephemeral in-memory DB:

```js
const db = new PGlite()                 // default dataDir is in-memory
const db = new PGlite('memory://')      // explicit in-memory scheme
const db = await PGlite.create('memory://')  // awaits readiness + better TS typing
```

DataDir prefixes:
- *(none / `memory://`)* — in-memory, ephemeral (what you want for tests)
- `file://` or a bare path (e.g. `'./pgdata'`) — filesystem persistence (Node/Bun/Deno)
- `idb://` — IndexedDB (browser only)

Readiness: the constructor returns synchronously but init is async. Either
`await PGlite.create(...)`, `await db.waitReady`, or simply `await` your first
`query`/`exec` (they wait internally). `db.ready` (bool) and `db.waitReady`
(Promise) are exposed.

Useful options (`new PGlite(dataDir, options)`):
- `username`, `database` — connection identity
- `relaxedDurability: true` — skip awaiting storage flushes (irrelevant for memory)
- `debug: 1..5` — Postgres debug verbosity
- `parsers` / `serializers` — per-OID custom type (de)serialization (see Type Mapping)
- `extensions` — load PGlite extensions (e.g. `pgvector`, `live`); not needed here

---

## `query()` and `exec()` — signatures and return shapes

### `query<T>(sql, params?, options?): Promise<Results<T>>`

Single statement, extended protocol, `$1`-style parameters.

```ts
interface Results<T> {
  rows: T[]                                         // [] for write statements
  fields: { name: string; dataTypeID: number }[]    // column metadata (OIDs); [] for writes
  affectedRows?: number                             // INSERT/UPDATE/DELETE/COPY/MERGE row count
  blob?: Blob                                        // only for COPY TO /dev/blob
}
```

`options`:
- `rowMode: 'object' | 'array'` (default `'object'`). `'array'` returns each row as a
  positional array (pairs with `fields` for column names).
- `parsers` / `serializers` — override instance type handlers for this call.
- `blob` — attach a Blob/File for `COPY ... FROM '/dev/blob'`.

### `exec(sql, options?): Promise<Array<Results>>`

Runs a multi-statement SQL string (semicolon-separated). **No parameters.** Returns
one `Results` per statement. Use it for DDL scripts / schema setup.

### Verified return shapes (v0.5.3)

Table: `CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN, score DOUBLE PRECISION)`

| Operation | `rows` | `fields` | `affectedRows` |
|---|---|---|---|
| `INSERT ... ON CONFLICT DO UPDATE` (1 row) | `[]` | `[]` | `1` |
| `UPDATE ... WHERE id=1` (matched) | `[]` | `[]` | `1` |
| `DELETE ... WHERE id=999` (no match) | `[]` | `[]` | `0` |
| `SELECT * FROM t WHERE active=$1` | `[{id:1,name:'alice',active:true,score:9.9}]` | 4 entries with OIDs | (present, equals selected count) |

`fields` for the SELECT (note the OIDs / `dataTypeID`):

```json
[{"name":"id","dataTypeID":23},
 {"name":"name","dataTypeID":25},
 {"name":"active","dataTypeID":16},
 {"name":"score","dataTypeID":701}]
```

OIDs: 23=int4, 25=text, 16=bool, 701=float8, 700=float4, 20=int8/bigint, 1700=numeric.

> Note: `affectedRows` is *also* populated for SELECT (it reflects the command tag row
> count). Treat it as authoritative only for write statements; use `rows.length` for reads.

`rowMode: 'array'` for the same row: `[[1,"alice",true,9.9]]`.

---

## Parameterized queries & JS → PG type mapping

Use `$1, $2, ...` placeholders with a positional `params` array. PGlite serializes JS
values to text for the wire protocol based on **runtime JS type**, and parses results
back per the column OID. There is no per-parameter type declaration in the basic
`query` path — the value's JS type drives serialization.

Verified mapping for your column set (parse direction = what you get back in `rows`):

| Postgres type (OID) | JS value in (param) | JS value out (row) |
|---|---|---|
| `INTEGER` / int4 (23), `SMALLINT` (21) | `number` | `number` |
| `TEXT` (25), `VARCHAR` (1043) | `string` | `string` |
| `BOOLEAN` (16) | `boolean` (`true`/`false`) | `boolean` |
| `DOUBLE PRECISION`/float8 (701), `REAL`/float4 (700) | `number` | `number` |
| `BIGINT` / int8 (20) | `number` or `bigint` | `number` if within ±2^53 safe range, else `bigint` |
| `NUMERIC` (1700) | `number`/`string` | **`string`** (preserves precision) |
| any column, SQL NULL | `null` | `null` |
| `DATE`/`TIMESTAMP`/`TIMESTAMPTZ` | `Date`/string | `Date` |
| `JSON`/`JSONB` | object (auto-`JSON.stringify`) | parsed object |
| `BYTEA` | `Uint8Array` | `Uint8Array` |

Mechanism (from the bundled source, for confidence): the serializer/parser registry
maps JS types and PG OIDs. Boolean serializes to `'t'`/`'f'` and parses `'t'`→`true`.
`bigint` parses via `BigInt(...)` then downgrades to `number` when inside
`Number.MIN/MAX_SAFE_INTEGER`. `numeric` has no number parser, so it returns the raw
**string**. `null` short-circuits to `null` in both directions.

Implications for an oracle over int/text/bool/float + PK:
- Your four declared types (INTEGER, TEXT, BOOLEAN, DOUBLE PRECISION) round-trip to the
  exact JS primitives you'd expect — direct `===`/deep-equal comparison works.
- If your schema ever emits `BIGINT` or `NUMERIC`, normalize before comparing
  (BigInt vs number; numeric-as-string). Prefer `DOUBLE PRECISION` for "float" and
  `INTEGER` for "int" to keep comparisons primitive.
- Float equality: `DOUBLE PRECISION` is IEEE-754 double, matching JS `number` exactly,
  so a value written as `9.9` reads back as `9.9` (verified). Still beware that
  client-side float *arithmetic* may diverge from Postgres arithmetic — compare with a
  tolerance if you compute derived floats.

---

## Upsert by primary key

Standard Postgres `ON CONFLICT` works (verified, `affectedRows: 1` on both insert and
update path):

```sql
INSERT INTO t (id, name, active, score)
VALUES ($1, $2, $3, $4)
ON CONFLICT (id) DO UPDATE
  SET name = EXCLUDED.name, active = EXCLUDED.active, score = EXCLUDED.score
```

DELETE-by-PK returns `affectedRows: 1` when a row matched, `0` otherwise — use this to
assert presence/absence.

---

## Resetting / dropping between tests

Options, cheapest-to-strongest isolation:

1. **Fresh instance per test (recommended).** `const db = new PGlite()` in `beforeEach`,
   `await db.close()` in `afterEach`. Fully isolated; startup is fast (tens of ms,
   in-memory). This is the simplest correct default for an oracle.
2. **`TRUNCATE` between cases.** Keep one instance, `await db.exec('TRUNCATE t RESTART IDENTITY CASCADE')`.
   Faster than re-creating if startup ever dominates; preserves schema.
3. **`DROP TABLE` + re-DDL.** `await db.exec('DROP TABLE IF EXISTS t')` then re-create.
   Use when each test has a different generated schema.
4. **Transaction rollback.** Wrap a test body in `db.transaction(tx => ...)` and call
   `tx.rollback()` (or throw) to discard. Good for read-mostly cases; note nested
   transactions/savepoints have normal Postgres limits.

`close()` returns a Promise and shuts the instance down (`db.closed` becomes `true`);
always `await` it so resources are released between suites.

---

## Minimal end-to-end snippet (create, DDL, upsert, select-where)

```js
import { PGlite } from '@electric-sql/pglite'

// 1. create ephemeral in-memory Postgres
const db = await PGlite.create('memory://')

// 2. DDL generated from schema (int / text / bool / float + PK)
await db.exec(`
  CREATE TABLE t (
    id     INTEGER PRIMARY KEY,
    name   TEXT,
    active BOOLEAN,
    score  DOUBLE PRECISION
  );
`)

// 3. upsert by primary key
await db.query(
  `INSERT INTO t (id, name, active, score)
   VALUES ($1, $2, $3, $4)
   ON CONFLICT (id) DO UPDATE
     SET name = EXCLUDED.name, active = EXCLUDED.active, score = EXCLUDED.score`,
  [1, 'alice', true, 3.5],
)

// update + delete
await db.query(`UPDATE t SET score = $1 WHERE id = $2`, [9.9, 1])
await db.query(`DELETE FROM t WHERE id = $1`, [2])

// 4. SELECT * FROM t WHERE <clause>, typed rows back
const res = await db.query(`SELECT * FROM t WHERE active = $1`, [true])
// res.rows  -> [{ id: 1, name: 'alice', active: true, score: 9.9 }]
// res.fields -> [{name:'id',dataTypeID:23}, ...]
console.log(res.rows)

await db.close()
```

Tagged-template alternative (auto-parameterized, safe):

```js
const res = await db.sql`SELECT * FROM t WHERE id = ${1}`
```

---

## Gotchas

- **ESM-only.** `"type":"module"` package; no CommonJS `require`. Configure your test
  runner for ESM (Vitest native; Jest needs config).
- **Single exclusive connection.** One PGlite instance = one connection. Concurrent
  `query` calls are queued/serialized; don't share an instance across parallel test
  workers. Give each worker its own instance.
- **Async init.** The constructor doesn't block. Await `PGlite.create`, `waitReady`, or
  your first query before asserting on `ready`.
- **Top-level await** is convenient for setup in ESM modules but not required — normal
  `async` test hooks work.
- **Memory growth.** Each instance carries a WASM Postgres + its in-memory data dir.
  Many simultaneous instances cost real RAM; close instances you're done with. WASM
  initial heap is tunable via `initialMemory` (bytes) if you hit limits with large data.
- **`BIGINT` ambiguity / `NUMERIC` as string.** Covered above — normalize before
  comparing, or avoid these types in generated schemas.
- **`affectedRows` on SELECT** is populated (command-tag count); use `rows.length` for
  read assertions.
- **First-run cost.** Initial WASM compile/instantiate adds a one-time cost per process;
  reuse the module across instances in a process if you create many (PGlite caches the
  compiled module internally; `pgliteWasmModule` lets you pre-supply one).
- **Worker build** (`@electric-sql/pglite/worker`) exists mainly for browser multi-tab
  sharing; not needed for Node test oracles.

---

## Open questions

- **NaN / Infinity floats**: behavior of `'NaN'::float8` round-tripping to JS
  `NaN`/`Infinity` was not verified. *(unverified)* — test if your generator can emit
  these.
- **`numeric`/`bigint` final decision**: if the schema generator must support
  arbitrary-precision int/decimal, decide on a normalization strategy (compare as
  strings, or coerce both sides to BigInt) — not yet specified.
- **Instance creation throughput**: exact per-instance startup time (and whether
  fresh-instance-per-test is fast enough for large suites vs. TRUNCATE reuse) not
  benchmarked here. *(unverified — measure on target CI.)*
- **Determinism vs. real Postgres**: PGlite tracks a specific upstream Postgres major
  version; if the production target is a different major, rare semantic/edge-case
  differences are possible. Confirm the PGlite-bundled Postgres major matches your
  reference. *(unverified — check release notes for 0.5.3.)*
- **Collation / `ORDER BY` text**: default collation for `WHERE`/`ORDER BY` on `TEXT`
  may differ from a production cluster's locale. If row *ordering* is part of the
  oracle comparison, pin an explicit `ORDER BY` and/or `COLLATE`. *(unverified.)*
- **Float arithmetic parity**: equality of *computed* floats between client state and
  Postgres expression evaluation — recommend tolerance-based comparison; exact parity
  not guaranteed.

---

## Sources

- Official API reference: https://pglite.dev/docs/api
- Official getting-started / install: https://pglite.dev/docs/
- npm registry metadata (`npm view @electric-sql/pglite`) — version 0.5.3, dist-tags.
- Empirical verification + bundled source inspection of installed `@electric-sql/pglite@0.5.3`
  (`dist/chunk-*.js`, type serializer/parser registry).
