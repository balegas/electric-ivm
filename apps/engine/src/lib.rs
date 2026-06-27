//! electric-lite query engine.
//!
//! Takes change events from per-table durable streams and fans each change out to registered
//! shapes: equality shapes share one dbsp join circuit per template (`family`), while non-shareable
//! shapes (ranges / OR / NOT / inequality) are stateless filters evaluated directly on each delta.
//! Matching deltas are appended (as State-Protocol envelopes) to per-shape durable streams. The
//! Z-set element is a dynamically-typed [`value::Row`] (positional `Vec<Value>`); the schema gives
//! names to the positions. See `docs/superpowers/specs/2026-06-27-electric-lite-decisions.md`.

pub mod ds;
pub mod engine;
pub mod family;
pub mod fault;
pub mod http;
pub mod metrics;
pub mod predicate;
pub mod schema;
pub mod value;

pub use value::{Row, Value};
