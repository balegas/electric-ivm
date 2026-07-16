//! electric-ivm query engine.
//!
//! Takes change events from the ordered change log and fans each change out to registered shapes,
//! across a three-tier serving model. The **circuit** ([`arrangements`]) is always-on infrastructure
//! (Postgres mode): one shared storage-enabled dbsp circuit maintains disk-spillable table
//! arrangements and counts pipelines, serves point lookups (subquery re-derivations) from local
//! snapshots, and serves membership shapes and decomposable COUNT aggregates end to end — seeding
//! and move-in/out come from arrangement snapshots instead of Postgres backfills. The circuit only
//! serves a shape whose connecting column has a configured index (`ELECTRIC_IVM_DBSP_INDEXES` /
//! `_COUNTS`); everything else falls through to the tiers below. The **routing tier**: equality
//! shapes are routed by key (a `key -> shapes` index per template), and indexed standalone
//! predicates by necessary conjunct — the hot path holds **no table data**, only per-shape metadata.
//! The **fallback** serves everything else: stateless three-valued filters and the cross-table
//! subquery registry. Matching deltas are appended (as State-Protocol envelopes) to per-shape
//! durable streams. The Z-set element is a dynamically-typed [`value::Row`] (positional
//! `Vec<Value>`); the schema gives names to the positions. See `docs/ARCHITECTURE.md` and
//! `docs/ivm-engine-internals.md` for the system design.

pub mod arrangements;
pub mod config;
pub mod ds;
pub mod electric;
pub mod engine;
pub mod fault;
pub mod heap_size;
pub mod http;
pub mod mem;
pub mod metrics;
pub mod params;
pub mod pg;
pub mod pk_dict;
pub mod pgoutput;
pub mod statsd;
pub mod predicate;
pub mod replication;
pub mod retention;
pub mod schema;
pub mod sql;
pub mod subq_circuit;
pub mod subquery;
pub mod trace;
pub mod value;
pub mod where_sql;

pub use value::{Row, Value};

/// The single ordered change log: the ingestor appends whole commits here (in commit order), and
/// the engine's sequencer consumes it — the envelope's `type` field carries the table name.
pub const CHANGES_STREAM: &str = "changes";
