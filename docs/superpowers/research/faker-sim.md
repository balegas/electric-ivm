# @faker-js/faker for deterministic, seeded simulator data

Research brief for electric-ivm's simulator: schema-conforming row generation
(int/text/bool/float columns) plus a reproducible random stream of insert/update/
delete ops where a printed seed exactly replays a failing run.

- **Package:** `@faker-js/faker`
- **Latest version:** `10.5.0` (verified via `npm view @faker-js/faker` on 2026-06-27;
  dist-tags: `latest=10.5.0`, `stable=10.4.0`).
- **License:** MIT. ESM-first, ships TypeScript types. Requires a modern Node (v18+
  for the v9/v10 line).
- **Docs:** https://fakerjs.dev

## Install

```bash
npm install --save-dev @faker-js/faker
# or: pnpm add -D @faker-js/faker
```

## Determinism / seeding (the core requirement)

`faker.seed(n)` resets the underlying PRNG so the subsequent call sequence is fully
reproducible. Same seed + same sequence of calls => identical output.

```typescript
import { faker } from '@faker-js/faker';

faker.seed(5);
const a = faker.person.firstName();

faker.seed(5);
const b = faker.person.firstName();
// a === b
```

`faker.seed()` (no arg) generates and **returns** a fresh random seed number — useful
for "pick a random seed, print it, run". `faker.seed()` also returns the seed it set,
so you can capture it:

```typescript
const seed = faker.seed();        // random seed, also applied
console.log(`seed=${seed}`);      // print so a failing run can be replayed
// ...later: faker.seed(seed) reproduces the exact stream
```

Reproducibility holds **only if the order and number of faker calls is identical**.
If the simulator's op-generation path makes a different number of PRNG draws between
runs, the streams diverge. Keep the draw sequence deterministic (see Open questions).

### Randomizer / PRNG

- Underlying RNG is a Mersenne Twister. Since **v9** the default is the 53-bit variant
  (`generateMersenne53Randomizer`) — better distribution, far fewer duplicates than the
  old 32-bit one.
- The generated stream is stable for a given faker major version, but **not guaranteed
  identical across major versions**. Pin the version in the simulator if printed seeds
  must replay across upgrades (treat the faker version as part of the reproducibility
  contract).

## Instance isolation (recommended for the simulator)

Do **not** rely on the shared global `faker` singleton if other code (tests, libraries)
also draws from it — that perturbs the call sequence and breaks replay. Create a
dedicated instance whose randomizer you own:

```typescript
import { Faker, en } from '@faker-js/faker';
import { generateMersenne53Randomizer } from '@faker-js/faker';

const randomizer = generateMersenne53Randomizer();
const f = new Faker({ locale: en, randomizer });

f.seed(printedSeed);   // isolated, deterministic stream for this simulator run
```

A fresh `new Faker(...)` instance gives full isolation. Each run, construct a new
instance (or call `f.seed(seed)` to reset) so nothing external can shift the sequence.
Multiple instances can share one `randomizer` if you want their seeds synchronized; for
the simulator you generally want a single dedicated instance per run.

> Note: `generateMersenne53Randomizer` is exported from the package root. If your build
> complains, it is also reachable via the internal randomizer entry; verify the import
> path against the installed version's `package.json` exports (Open questions).

## Column generators by type

### int — `faker.number.int`

```typescript
function int(options?: number | {
  min?: number;            // default 0, inclusive
  max?: number;            // default Number.MAX_SAFE_INTEGER, inclusive
  multipleOf?: number;     // default 1
}): number

faker.number.int()                              // huge int
faker.number.int(100)                           // 0..100
faker.number.int({ min: 10, max: 100 })         // inclusive both ends
faker.number.int({ min: 0, max: 100, multipleOf: 10 })
```

### float — `faker.number.float`

```typescript
function float(options?: number | {
  min?: number;            // default 0.0, inclusive
  max?: number;            // default 1.0, exclusive
  fractionDigits?: number; // max decimal places (rounds)
  multipleOf?: number;
}): number

faker.number.float()                                  // [0,1)
faker.number.float({ min: 20, max: 30 })
faker.number.float({ min: 0, max: 1, fractionDigits: 4 })
faker.number.float({ min: 0, max: 10, multipleOf: 0.25 })
```

### text — `faker.string.*` (deterministic length control) or `faker.lorem.*`

For schema-conforming text columns prefer `faker.string.alphanumeric` / `alpha` with
explicit length bounds:

```typescript
faker.string.alpha(10)                                   // 10 letters
faker.string.alphanumeric({ length: { min: 5, max: 20 } })
faker.string.alpha({ length: 8, casing: 'lower' })
faker.string.sample({ min: 5, max: 10 })                 // printable UTF-16 33..125
```

Word-ish / human-readable text:

```typescript
faker.lorem.word()                       // single word
faker.lorem.words(3)                      // 'lorem ipsum dolor'
faker.word.noun()                         // semantic word
faker.lorem.sentence()
```

All of the above are seeded and deterministic.

### bool — `faker.datatype.boolean`

```typescript
function boolean(options?: number | { probability?: number }): boolean
// probability default 0.5; value limited to 2 decimals; <=0 => false, >=1 => true

faker.datatype.boolean()                 // 50/50
faker.datatype.boolean(0.9)              // true ~90%
faker.datatype.boolean({ probability: 0.1 })
```

## Random element / weighted choice (op selection)

### Uniform pick — `faker.helpers.arrayElement`

```typescript
faker.helpers.arrayElement(['insert', 'update', 'delete'])  // throws if array empty
faker.helpers.arrayElements(arr, { min: 2, max: 4 })        // random subset, random order
```

### Weighted pick — `faker.helpers.weightedArrayElement` (use this for op mix)

```typescript
function weightedArrayElement<T>(
  array: ReadonlyArray<{ weight: number; value: T }>
): T

const op = faker.helpers.weightedArrayElement([
  { weight: 5, value: 'insert' },   // ~50%
  { weight: 4, value: 'update' },   // ~40%
  { weight: 1, value: 'delete' },   // ~10%
]);
// weights are relative to their sum; need not total 1
```

### Other useful helpers

- `faker.helpers.enumValue(SomeEnum)` — random value from a TS enum.
- `faker.helpers.maybe(() => x, { probability })` — value or `undefined`.
- `faker.helpers.fromRegExp(/[A-Z0-9]{4}/)` — pattern-shaped strings (no groups/anchors).

## Minimal end-to-end snippet

```typescript
import { Faker, en, generateMersenne53Randomizer } from '@faker-js/faker';

// --- per-run setup: pick or accept a seed, print it for replay ---
const randomizer = generateMersenne53Randomizer();
const f = new Faker({ locale: en, randomizer });
const seed = process.env.SEED ? Number(process.env.SEED) : f.seed();
f.seed(seed);
console.log(`replay with SEED=${seed}`);

// --- schema-conforming row ---
type Row = { id: number; name: string; score: number; active: boolean };
function genRow(): Row {
  return {
    id: f.number.int({ min: 1, max: 1_000_000 }),
    name: f.string.alphanumeric({ length: { min: 4, max: 16 } }),
    score: f.number.float({ min: 0, max: 100, fractionDigits: 2 }),
    active: f.datatype.boolean(0.7),
  };
}

// --- weighted op selection ---
type Op = 'insert' | 'update' | 'delete';
function genOp(): Op {
  return f.helpers.weightedArrayElement<Op>([
    { weight: 5, value: 'insert' },
    { weight: 4, value: 'update' },
    { weight: 1, value: 'delete' },
  ]);
}

// --- reproducible op stream ---
for (let i = 0; i < 1000; i++) {
  const op = genOp();
  if (op !== 'delete') genRow();
}
```

Running with the same `SEED` reproduces the identical row + op stream, provided the
call sequence is unchanged.

## Open questions / unverified

- **Cross-major-version stability:** faker does not guarantee identical streams across
  major versions. Confirm whether electric-ivm needs replay across faker upgrades; if
  so, pin the exact version and record it alongside the seed. (Documented behavior;
  not separately tested here.)
- **Exact import path of `generateMersenne53Randomizer`** for v10.5.0 — docs show it
  imported from the package root, but verify against the installed
  `node_modules/@faker-js/faker/package.json` `exports` map.
- **Draw-count determinism:** any conditional code that changes how many PRNG draws
  occur (e.g. `weightedArrayElement` internally drawing once vs. a retry path, or
  skipping `genRow()` on delete as in the snippet) is fine *as long as it is itself
  deterministic given the seed*. If the simulator branches on external/async state, the
  stream will diverge. Keep all randomness sourced from the single seeded instance.
- **Thread/async safety:** faker is synchronous and single-threaded per instance.
  Concurrent consumers of one instance interleave draws nondeterministically — give each
  worker its own seeded instance if parallelizing.
- **Unicode/length edge cases for text columns:** `string.sample` emits printable ASCII
  range 33..125 only; if columns must hold arbitrary UTF-8, choose a generator
  accordingly (unverified against electric-ivm's column constraints).
