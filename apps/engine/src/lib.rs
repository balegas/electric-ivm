//! electric-lite query engine.
//!
//! The Z-set element is a dynamically-typed [`Row`] (a positional `Vec<Value>`); the
//! column name -> index mapping lives out-of-band per shape (from the schema). This avoids
//! a map-based key, whose rkyv archived form lacks the `Ord`/`Hash` impls `dbsp::DBData`
//! requires. See `docs/superpowers/specs/2026-06-27-electric-lite-decisions.md` (D1).

use feldera_macros::IsNone;
use ordered_float::OrderedFloat;
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;

/// A scalar cell value. `Float` wraps `OrderedFloat` because a bare `f64` is not
/// `Eq`/`Ord`/`Hash` and so cannot be part of a `dbsp::DBData` key.
#[derive(
    Clone, Default, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, SizeOf, Archive, Serialize,
    Deserialize, IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd, Hash))]
pub enum Value {
    #[default]
    Null,
    Int(i64),
    Text(String),
    Bool(bool),
    Float(OrderedFloat<f64>),
}

/// A row is a positional vector of cell values; the schema gives names to the positions.
#[derive(
    Clone, Default, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, SizeOf, Archive, Serialize,
    Deserialize, IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd, Hash))]
pub struct Row(pub Vec<Value>);

#[cfg(test)]
mod spike_tests {
    //! Spike: confirm dbsp 0.299.0 + the rkyv/size-of/feldera-macros/ordered-float derive
    //! stack compiles and that a filter circuit emits correct +/- deltas. This de-risks the
    //! #1 project risk (dynamic-row trait bounds) before building the full engine.

    use super::*;
    use dbsp::utils::Tup2;
    use dbsp::{IndexedZSetReader, OrdZSet, OutputHandle, RootCircuit, ZSetHandle, ZWeight};

    /// WHERE row[0] is an Int > 10. Column index captured by the closure.
    fn build(
        circuit: &mut RootCircuit,
    ) -> anyhow::Result<(ZSetHandle<Row>, OutputHandle<OrdZSet<Row>>)> {
        let (stream, handle) = circuit.add_input_zset::<Row>();
        let filtered = stream.filter(|Row(cols)| matches!(cols.first(), Some(Value::Int(n)) if *n > 10));
        Ok((handle, filtered.output()))
    }

    #[test]
    fn filter_circuit_emits_signed_deltas() -> anyhow::Result<()> {
        let (circuit, (input, output)) = RootCircuit::build(build)?;

        // Step 1: two inserts (+1). Only the Int(42) row passes the filter.
        let mut batch: Vec<Tup2<Row, ZWeight>> = vec![
            Tup2(Row(vec![Value::Int(42), Value::Text("keep".into())]), 1),
            Tup2(Row(vec![Value::Int(7), Value::Text("drop".into())]), 1),
        ];
        input.append(&mut batch);
        circuit.transaction()?;

        let out: Vec<(Row, ZWeight)> =
            output.consolidate().iter().map(|(r, (), w)| (r, w)).collect();
        assert_eq!(out.len(), 1, "only the matching row should appear");
        assert_eq!(out[0].1, 1, "insert is a +1 delta");
        assert_eq!(out[0].0, Row(vec![Value::Int(42), Value::Text("keep".into())]));

        // Step 2: delete the kept row (-1) -> a -1 delta leaves the shape.
        let mut batch: Vec<Tup2<Row, ZWeight>> =
            vec![Tup2(Row(vec![Value::Int(42), Value::Text("keep".into())]), -1)];
        input.append(&mut batch);
        circuit.transaction()?;

        let out: Vec<(Row, ZWeight)> =
            output.consolidate().iter().map(|(r, (), w)| (r, w)).collect();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1, -1, "delete is a -1 delta");
        Ok(())
    }
}
