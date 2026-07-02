//! The dynamically-typed Z-set element used by dbsp circuits.

use anyhow::{Context, Result, bail};
use feldera_macros::IsNone;
use ordered_float::OrderedFloat;
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;

use crate::schema::ColumnType;

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

impl Value {
    /// Parse a JSON scalar into a `Value` of the given column type. `null` -> `Null`.
    pub fn from_json(j: &serde_json::Value, ty: ColumnType) -> Result<Value> {
        if j.is_null() {
            return Ok(Value::Null);
        }
        Ok(match ty {
            ColumnType::Int => Value::Int(j.as_i64().context("expected an integer")?),
            ColumnType::Float => Value::Float(OrderedFloat(j.as_f64().context("expected a float")?)),
            ColumnType::Text => Value::Text(j.as_str().context("expected a string")?.to_string()),
            ColumnType::Bool => Value::Bool(j.as_bool().context("expected a bool")?),
        })
    }

    /// Parse a stringified primary-key (the durable-stream event `key`) into a typed `Value`.
    pub fn from_key_string(s: &str, ty: ColumnType) -> Result<Value> {
        Ok(match ty {
            ColumnType::Int => Value::Int(s.parse().context("pk is not an integer")?),
            ColumnType::Float => Value::Float(OrderedFloat(s.parse().context("pk is not a float")?)),
            ColumnType::Text => Value::Text(s.to_string()),
            ColumnType::Bool => Value::Bool(s.parse().context("pk is not a bool")?),
        })
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Null => serde_json::Value::Null,
            Value::Int(i) => (*i).into(),
            Value::Float(f) => serde_json::json!(f.0),
            Value::Text(s) => s.clone().into(),
            Value::Bool(b) => (*b).into(),
        }
    }

    /// String form used as the durable-stream event `key` (the primary key).
    pub fn to_key_string(&self) -> String {
        match self {
            Value::Null => "null".to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.0.to_string(),
            Value::Text(s) => s.clone(),
            Value::Bool(b) => b.to_string(),
        }
    }
}

/// A row is a positional vector of cell values; the schema gives names to the positions.
#[derive(
    Clone, Default, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, SizeOf, Archive, Serialize,
    Deserialize, IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd, Hash))]
pub struct Row(pub Vec<Value>);

impl Row {
    pub fn get(&self, idx: usize) -> Result<&Value> {
        self.0.get(idx).with_context(|| format!("column index {idx} out of range"))
    }
}

/// Best-effort sanity check used by JSON parsing paths.
pub fn ensure_object(j: &serde_json::Value) -> Result<&serde_json::Map<String, serde_json::Value>> {
    match j.as_object() {
        Some(m) => Ok(m),
        None => bail!("expected a JSON object, got {j}"),
    }
}
