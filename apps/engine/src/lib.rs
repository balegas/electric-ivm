//! electric-lite query engine.
//!
//! Takes change events from per-table durable streams and fans each change out to registered shapes.
//! The engine holds **no table data** — only per-shape metadata: equality shapes are routed by key
//! (a `key -> shapes` index per template, each shape backfilled directly from Postgres), while
//! non-shareable shapes (ranges / OR / NOT / inequality) are stateless filters evaluated directly on
//! each delta. Matching deltas are appended (as State-Protocol envelopes) to per-shape durable
//! streams. The Z-set element is a dynamically-typed [`value::Row`] (positional `Vec<Value>`); the
//! schema gives names to the positions. See `docs/superpowers/specs/2026-06-27-electric-lite-decisions.md`
//! and `docs/superpowers/specs/2026-06-29-reduce-engine-memory-design.md`.

pub mod ds;
pub mod electric;
pub mod engine;
pub mod fault;
pub mod http;
pub mod mem;
pub mod metrics;
pub mod pg;
pub mod predicate;
pub mod replication;
pub mod schema;
pub mod sql;
pub mod subquery;
pub mod value;
pub mod where_sql;

pub use value::{Row, Value};
