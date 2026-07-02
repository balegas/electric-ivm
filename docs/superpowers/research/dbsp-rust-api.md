# DBSP Rust crate — implementation brief for an incremental FILTER pipeline

Research target: the `dbsp` crate (crates.io/crates/dbsp, docs.rs/dbsp, and the
`feldera/feldera` repo, `crates/dbsp`). Goal: build a circuit that ingests Z-set
**deltas** of dynamically-typed table rows, applies a `filter` predicate, and emits
output deltas per shape.

All code below was verified against the `feldera/feldera` `main` branch
(`crates/dbsp`, repo SHA `98228b0…`) unless explicitly marked unverified. The
published crate at the time of writing is **0.299.0**, which already exposes the
`transaction()` / `consolidate()` API used here (confirmed on docs.rs 0.299.0).

---

## 1. Version, Cargo.toml, build caveats

- **Latest crates.io version: `0.299.0`** (DBSP uses a fast-moving, high-numbered
  versioning scheme; ~239 published versions). License MIT OR Apache-2.0.
- **MSRV: Rust `1.93.1`**, and the crate is **edition 2024**. Use a recent stable
  toolchain.
- Cargo dependency line:

```toml
[dependencies]
dbsp = "0.299.0"

# You also need these directly because the derive macros and helper types
# referenced in your row type live in sibling crates / re-exports:
rkyv = { version = "*", features = ["std", "size_64", "validation"] } # match dbsp's rkyv major
size-of = "*"            # provides #[derive(SizeOf)]  (crate name `size-of`, import as `size_of`)
feldera-macros = "*"     # provides #[derive(IsNone)]
serde = { version = "*", features = ["derive"] }   # only if you also deserialize rows
ordered-float = { version = "*", features = ["rkyv_64"] } # for any f64 column (see §3)
```

> IMPORTANT: pin `rkyv` / `size-of` / `feldera-macros` to versions compatible with
> the exact `dbsp` you select. The cleanest path is to copy the versions dbsp
> itself uses (dbsp's `Cargo.toml` takes them from the workspace). Mismatched
> `rkyv` majors will not compile because the derived `Archive` impls must match
> dbsp's expected `rkyv`. If publishing friction appears, prefer `cargo add dbsp`
> then add the macro crates that the compiler errors ask for.

**Heavy build caveat:** dbsp is a large dependency. It transitively pulls in
`tokio` (rt-multi-thread), `rkyv` w/ validation, `mimalloc-rust-sys` (builds a C
allocator), `zip`, `roaring`, `metrics`, `time`, `csv`, `clap`, etc. Expect a slow
cold build and a non-trivial binary. There is no "lite" feature; default feature is
`backend-mode`.

---

## 2. Minimal API surface

Entry points (single-threaded vs multi-threaded):

```rust
// Single-threaded: runs on the calling thread. Handle is NOT Send/Sync (see §5).
RootCircuit::build(|circuit: &mut RootCircuit| -> Result<T> { ... })
    -> Result<(CircuitHandle, T)>

// Multi-threaded: spawns N worker threads. Handle drives them via channels.
Runtime::init_circuit(config: impl Into<CircuitConfig>, constructor)
    -> Result<(DBSPHandle, T)>
// `config` can be a `usize` (number of worker threads) -> Into<CircuitConfig>.
```

Inside the constructor:

```rust
// Add an input Z-set of a custom key type K (K: DBData). Returns the stream and
// a handle you keep to feed data later.
let (stream, input_handle): (Stream<RootCircuit, OrdZSet<K>>, ZSetHandle<K>)
    = circuit.add_input_zset::<K>();

// Verified signature (operator/input.rs):
//   pub fn add_input_zset<K>(&self) -> (Stream<RootCircuit, OrdZSet<K>>, ZSetHandle<K>)
//   where K: DBData

// FILTER: predicate over &K; keeps rows where it returns true. Weights pass through,
// so a delta of (+1 insert / -1 delete) is filtered element-wise.
let out: Stream<RootCircuit, OrdZSet<K>> = stream.filter(|row: &K| /* WHERE clause */ true);

// Get an output handle on any stream.
let output_handle: OutputHandle<OrdZSet<K>> = out.output();
```

Feed + step + read loop:

```rust
// 1. Build a batch of (record, weight) tuples. ZWeight is the integer weight type
//    (i64). insert = +1, delete = -1, update = two rows (-1 old, +1 new).
let mut batch: Vec<Tup2<K, ZWeight>> = vec![
    Tup2(row_a, 1),    // insert
    Tup2(row_b, -1),   // delete
];

// 2. Stage the batch on the input handle.
input_handle.append(&mut batch);   // takes &mut Vec, drains it
// (single-record alternative: input_handle.push(key, weight);)

// 3. Advance the circuit one logical step / transaction.
circuit.transaction()?;            // CircuitHandle::transaction(&self)
                                   // DBSPHandle::transaction(&mut self) for Runtime

// 4. Read the OUTPUT delta produced by this step.
//    consolidate() merges per-worker output into one OrdZSet and dedups weights.
for (row, (), weight) in output_handle.consolidate().iter() {
    // weight > 0 => row added to result set this step; < 0 => removed.
}
```

> NOTE on iteration shape (verified in `examples/orgchart.rs`): an `OrdZSet<K>` is
> internally an indexed Z-set with unit values, so `.consolidate().iter()` yields a
> **3-tuple `(key, (), weight)`** — the middle element is `()`. For an
> `OrdIndexedZSet<K, V>` it yields `(key, value, weight)`.

`transaction()` vs `step()`: `transaction()` = start a transaction, run, and commit,
blocking until outputs are ready (what the tutorials use — simplest). `step()` is the
lower-level single evaluation; `DBSPHandle::step(&mut self) -> Result<bool>` returns
whether a commit completed. Use `transaction()` unless you need pipelined
start/commit control.

---

## 3. Trait bounds for the element type (THE #1 RISK)

The Z-set key type `K` in `add_input_zset::<K>()` must implement **`DBData`**.
Verified definition (`crates/dbsp/src/trace.rs`):

```rust
pub trait DBData:
    Default
    + Clone
    + Eq
    + Ord
    + Hash
    + SizeOf
    + Send
    + Sync
    + Debug
    + ArchivedDBData
    + IsNone<Inner: ArchivedDBData>
    + SupportsRoaring
    + 'static
{}
// Blanket impl: any T meeting all those bounds is DBData automatically.
```

So you never `impl DBData` directly — you satisfy the supertraits and get it for
free. Concretely your row type needs all of:

- `Default, Clone, Eq, Ord, Hash, Debug` — standard derives.
- `Send + Sync + 'static` — no borrowed data, no `Rc`.
- `SizeOf` — `#[derive(SizeOf)]` from the `size-of` crate.
- `ArchivedDBData` — provided by deriving rkyv `Archive`, `Serialize`,
  `rkyv::Deserialize`, **and** making the *archived* form `Ord + Eq + PartialEq +
  PartialOrd` via `#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]`.
  DBSP stores data in its archived (zero-copy) form and compares/orders it there,
  which is why the archived type must also be totally ordered.
- `IsNone` — `#[derive(IsNone)]` from `feldera_macros`. (SQL-NULL tracking; for a
  plain struct/enum it reports "never null".)
- `SupportsRoaring` — satisfied automatically via blanket impls for the standard
  derivable types; you do not derive it.

### The exact derive block the tutorials use (verified, tutorial2/3/9)

```rust
use feldera_macros::IsNone;
use rkyv::{Archive, Serialize};
use size_of::SizeOf;

#[derive(
    Clone, Default, Debug, Eq, PartialEq, Ord, PartialOrd, Hash,
    SizeOf, Archive, Serialize, rkyv::Deserialize, serde::Deserialize, IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
struct Record { /* fields ... */ }
```

(`serde::Deserialize` is optional — only needed if you also parse rows from
CSV/JSON. Everything else is mandatory.)

### Dynamic rows — does a `BTreeMap<String, Value>` newtype work?

**This is the real risk.** `DBData` is built around rkyv zero-copy archival and
ordering of the *archived* representation. The blocker is not "dynamic" per se but
whether the chosen container's **archived form derives `Ord/Eq/Hash` and has a
`SizeOf` impl**:

- `String` → rkyv `ArchivedString` is `Ord/Eq/Hash`. ✅ `SizeOf` impl exists. ✅
- `Vec<T>` → rkyv `ArchivedVec<T::Archived>` is `Ord/Eq/Hash` when `T::Archived`
  is. ✅ `SizeOf` impl exists. ✅
- `BTreeMap<String, V>` → rkyv supports it (with the `alloc`/`std` feature), **but
  `ArchivedBTreeMap` does NOT reliably implement `Ord`/`Hash`**, and a
  `#[archive_attr(derive(Ord, ...))]` on a struct containing a map will fail to
  compile because the derived `Ord` needs the field's archived type to be `Ord`.
  `size-of` support for `BTreeMap` is also not guaranteed. → **Treat the
  `BTreeMap`-based key as unsupported / high-friction. Do not use it as the Z-set
  key.** (Unverified in the exact map case, but this is the predictable failure;
  see Open Questions.)

### Recommended pattern for dynamically-typed rows

Use a **fixed newtype over `Vec<Value>`** plus an out-of-band schema (column-name →
index), where `Value` is an enum of the supported scalar types. `Vec<Value>` archives
cleanly and is totally ordered, satisfying every `DBData` bound:

```rust
use feldera_macros::IsNone;
use rkyv::{Archive, Serialize};
use size_of::SizeOf;
use ordered_float::OrderedFloat;   // f64 is NOT Ord/Eq/Hash; wrap it

#[derive(
    Clone, Default, Debug, Eq, PartialEq, Ord, PartialOrd, Hash,
    SizeOf, Archive, Serialize, rkyv::Deserialize, IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd, Hash))]
enum Value {
    #[default]
    Null,
    Int(i64),
    Text(String),
    Bool(bool),
    Float(OrderedFloat<f64>),   // ordered-float impls rkyv (feature "rkyv_64") + Ord + Hash
}

#[derive(
    Clone, Default, Debug, Eq, PartialEq, Ord, PartialOrd, Hash,
    SizeOf, Archive, Serialize, rkyv::Deserialize, IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd, Hash))]
struct Row(Vec<Value>);   // schema (name->index) is stored separately, per shape
```

Why `Vec<Value>` over a map:
- `Vec` + the scalar leaf types all have the required archived `Ord/Eq/Hash` and
  `SizeOf` impls, so `DBData` is satisfied automatically.
- Positional access is fine because each shape has a fixed schema; the WHERE clause
  closure captures the column index(es) it needs.
- A `Vec<(String, Value)>` (sorted by name) is also viable and self-describing, at
  the cost of bigger keys / slower compares; prefer `Vec<Value>` + schema.
- **Avoid `serde_json::Value`** as the key: it contains an `f64` (not Ord/Eq/Hash)
  and a `Map`, and has no `SizeOf`/rkyv-archived-Ord story — it will not satisfy
  `DBData`.

`f64` columns: never put a bare `f64` in the key (breaks `Eq/Ord/Hash`). Use
`ordered_float::OrderedFloat<f64>` (dbsp already depends on `ordered-float` with
`rkyv_64`).

---

## 4. Input / output batch typing

- Input batch element type: **`Tup2<K, ZWeight>`** (`dbsp::utils::Tup2`), a 2-tuple
  newtype. `ZWeight` is dbsp's integer weight alias (i64). `ZSetHandle::append`
  takes `&mut Vec<Tup2<K, ZWeight>>` and drains it.
- The input stream type is `Stream<RootCircuit, OrdZSet<K>>`; `filter` returns the
  same `OrdZSet<K>` stream.
- Output handle type: **`OutputHandle<OrdZSet<K>>`** from `out.output()`.
- Reading output: `output_handle.consolidate() -> OrdZSet<K>`, then `.iter()` yields
  `(K, (), ZWeight)` tuples (key, unit-value, weight). `.weighted_count()` gives the
  net number of rows. The output of each `transaction()` is the **delta** for that
  step (rows whose membership changed), with signed weights.

---

## 5. One circuit, many shapes? Threading model

**Multiple input/output pairs in one circuit: YES.** The constructor can call
`add_input_zset` any number of times and return a tuple/`Vec` of handles
(`feldera`'s own `replay_tests.rs` returns
`(input_handles1, input_handles2, output_handles1, output_handles2)`; tutorial8 has
multiple inputs). The constructor's return value `T` is whatever bundle of handles
you want.

**But the circuit graph is FIXED at build time.** You cannot add operators/inputs
after `RootCircuit::build` returns. For electric-ivm, where shapes register at
runtime with *different* WHERE predicates (captured as closures at build time), the
two viable models are:

1. **One circuit per shape** (recommended default). Each registered shape builds its
   own small `RootCircuit` with one `add_input_zset` + one `filter` + one `output`.
   Simple, isolated, predicate captured directly. Cost: one set of worker
   thread(s)/tokio runtime per circuit (for `RootCircuit::build` it's just the
   current thread, cheap; for `Runtime::init_circuit` each circuit owns worker
   threads).
2. **One shared circuit keyed by shape_id**: input is `(shape_id, Row)`, and the
   filter is data-driven (`filter(|(sid, row)| dispatch_predicate(*sid, row))`) with
   a predicate table looked up by `shape_id`. Only works if you can express all
   predicates as data rather than distinct closures, and you must `map_index` /
   route outputs by `shape_id`. More complex; only worth it if circuit count would
   be very large.

Given dynamic per-shape WHERE clauses, **one circuit per shape is the norm and the
simplest correct choice.**

**Threading / async:**
- `RootCircuit::build` returns a **`CircuitHandle`** that holds a `RootCircuit`
  (internally `Rc`-based) plus a current-thread tokio runtime. It is **`!Send +
  !Sync`** — it must live on and be driven from a single thread, and cannot be held
  across an `.await` that may move threads. Its `transaction(&self)` / `step(&self)`
  block the calling thread.
- `Runtime::init_circuit(workers, ctor)` returns a **`DBSPHandle`** that owns N
  worker threads and communicates with them over channels; you drive it with
  `transaction(&mut self)` / `step(&mut self)`. This handle is designed to be moved/
  owned by a controller and is the right choice when you want parallelism.
  (`DBSPHandle` being `Send` is *very likely* but not line-verified here — see Open
  Questions.)
- Recommended integration in an async server: **confine each circuit to a dedicated
  OS thread** (a "circuit actor"). The async side sends `(batch, oneshot reply)`
  commands over an `mpsc` channel; the circuit thread loops: `append` → `transaction`
  → read `consolidate().iter()` → reply. This sidesteps the `!Send` `CircuitHandle`
  entirely and serializes access (the circuit is inherently single-writer per step).
  Use `tokio::task::spawn_blocking` or a plain `std::thread` for that loop; never call
  `transaction()` directly inside an async task on the runtime's worker pool.

---

## Complete minimal compiling example (filter over a custom row type)

Mirrors the verified tutorial3 structure, generalized to a dynamic `Row(Vec<Value>)`
and showing feed → step → read of output deltas.

```rust
use anyhow::Result;
use dbsp::utils::Tup2;
use dbsp::{OrdZSet, OutputHandle, RootCircuit, ZSet, ZSetHandle, ZWeight};
use feldera_macros::IsNone;
use ordered_float::OrderedFloat;
use rkyv::{Archive, Serialize};
use size_of::SizeOf;

#[derive(
    Clone, Default, Debug, Eq, PartialEq, Ord, PartialOrd, Hash,
    SizeOf, Archive, Serialize, rkyv::Deserialize, IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd, Hash))]
enum Value {
    #[default]
    Null,
    Int(i64),
    Text(String),
    Bool(bool),
    Float(OrderedFloat<f64>),
}

#[derive(
    Clone, Default, Debug, Eq, PartialEq, Ord, PartialOrd, Hash,
    SizeOf, Archive, Serialize, rkyv::Deserialize, IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd, Hash))]
struct Row(Vec<Value>);

// WHERE row[0] == Int(x) AND x > 10  (column index captured by the closure)
fn build_circuit(
    circuit: &mut RootCircuit,
) -> Result<(ZSetHandle<Row>, OutputHandle<OrdZSet<Row>>)> {
    let (input_stream, input_handle) = circuit.add_input_zset::<Row>();

    let filtered = input_stream.filter(|Row(cols)| {
        matches!(cols.first(), Some(Value::Int(n)) if *n > 10)
    });

    Ok((input_handle, filtered.output()))
}

fn main() -> Result<()> {
    // Single-threaded handle; lives on this thread.
    let (circuit, (input_handle, output_handle)) = RootCircuit::build(build_circuit)?;

    // --- step 1: two inserts (+1) ---
    let mut batch: Vec<Tup2<Row, ZWeight>> = vec![
        Tup2(Row(vec![Value::Int(42), Value::Text("keep".into())]), 1),
        Tup2(Row(vec![Value::Int(7),  Value::Text("drop".into())]), 1),
    ];
    input_handle.append(&mut batch);
    circuit.transaction()?;

    println!("after insert:");
    for (row, (), w) in output_handle.consolidate().iter() {
        println!("  {w:+} {row:?}");          // expect: +1 Row([Int(42), Text("keep")])
    }

    // --- step 2: delete the kept row (-1) ---
    let mut batch: Vec<Tup2<Row, ZWeight>> = vec![
        Tup2(Row(vec![Value::Int(42), Value::Text("keep".into())]), -1),
    ];
    input_handle.append(&mut batch);
    circuit.transaction()?;

    println!("after delete:");
    for (row, (), w) in output_handle.consolidate().iter() {
        println!("  {w:+} {row:?}");          // expect: -1 Row([Int(42), Text("keep")])
    }

    Ok(())
}
```

Multi-threaded variant (swap the build + driver):

```rust
use dbsp::{Runtime};
// 4 worker threads:
let (mut circuit, (input_handle, output_handle)) =
    Runtime::init_circuit(4, build_circuit)?;
// ... input_handle.append(&mut batch); circuit.transaction()?; read output ...
```

---

## Open questions / unverified

1. **`ArchivedBTreeMap` ordering.** Not line-verified that
   `#[archive_attr(derive(Ord))]` fails for a `BTreeMap`-containing struct, but it is
   the expected outcome and the reason to prefer `Vec<Value>`. If a map key is truly
   required, verify whether rkyv's `ArchivedBTreeMap` implements `Ord`/`PartialOrd`
   and whether `size-of` implements `SizeOf` for `BTreeMap` in the pinned versions.
2. **`DBSPHandle: Send`.** Strongly implied (it manages worker threads via channels
   and the tutorials hold it as `mut circuit`), but not line-verified. Confirm before
   moving it across threads / into a `tokio::task`. (The single-threaded
   `CircuitHandle` is confidently `!Send` due to its internal `Rc` + current-thread
   runtime.)
3. **Exact `rkyv` / `size-of` / `feldera-macros` versions** compatible with the
   published `dbsp 0.299.0`. The derive macros must match dbsp's expected `rkyv`
   major; pull the versions from dbsp's own lockfile/workspace rather than `"*"`.
4. **`#[derive(IsNone)]` on a `Vec`-newtype / enum.** Tutorials derive it on plain
   structs with scalar/Option fields. Deriving on `struct Row(Vec<Value>)` and on the
   `Value` enum is expected to work (it reports non-null), but verify the macro
   accepts a tuple-struct / enum shape; if not, a named-field wrapper
   (`struct Row { cols: Vec<Value> }`) is the fallback.
5. **`SupportsRoaring`** for arbitrary custom enums — assumed covered by blanket
   impls; verify no extra bound surfaces for the `Value` enum at compile time.
6. **Predicate updates.** Changing a shape's WHERE clause means rebuilding the
   circuit (graph is fixed at build). Confirm the intended lifecycle (drop old
   circuit, build new, replay state) for electric-ivm's re-subscription path.
