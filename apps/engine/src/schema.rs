//! Schema types (deserialized from the control-plane JSON, mirroring `@electric-ivm/protocol`)
//! and their compiled, positional runtime form.

use std::collections::BTreeMap;
use std::collections::HashMap;

use anyhow::{Result, bail};
use serde::Deserialize;

use crate::value::{Row, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ColumnType {
    Int,
    Text,
    Bool,
    Float,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ColumnDef {
    #[serde(rename = "type")]
    pub ty: ColumnType,
    /// The raw Postgres type name (`pg_type.typname` / `udt_name`, e.g. `uuid`, `timestamptz`) captured
    /// at introspection. `None` in library mode (schema defined via JSON, which only carries the coarse
    /// [`ColumnType`]). Used to cast bound text params to the native type in backfill SQL so uuid/int
    /// comparisons stay index-eligible (see `sql.rs`).
    #[serde(default, skip)]
    pub pg_type: Option<String>,
    /// Whether Postgres auto-supplies this column's value when omitted — i.e. it's an IDENTITY column
    /// or carries a `DEFAULT`. Captured at introspection; `false` in library mode. Lets the visualizer
    /// mark the column optional in its add-row form.
    #[serde(default, skip)]
    pub has_default: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TableDef {
    // BTreeMap gives a deterministic (sorted) column order regardless of JSON key order, which
    // we use as the positional Row order. Only internal consistency matters.
    pub columns: BTreeMap<String, ColumnDef>,
    // Accepts a single column name (`"id"`) or an ordered list (`["a","b"]`) for composite keys.
    #[serde(rename = "primaryKey", deserialize_with = "de_primary_key")]
    pub primary_key: Vec<String>,
}

fn de_primary_key<'de, D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Vec<String>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum One {
        Many(Vec<String>),
        Single(String),
    }
    Ok(match One::deserialize(d)? {
        One::Single(s) => vec![s],
        One::Many(v) => v,
    })
}

#[derive(Debug, Clone, Deserialize)]
pub struct Schema {
    pub tables: BTreeMap<String, TableDef>,
}

/// Compiled, positional view of one table: sorted columns, a name->index map, and the pk index.
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<(String, ColumnType)>,
    pub index: HashMap<String, usize>,
    /// First primary-key column index/name/type. For single-PK tables this IS the pk; for composite-PK
    /// tables (only used as subquery inner tables) it's the first key column — prefer `pk_cols`/`key_string`.
    pub pk_index: usize,
    pub pk_name: String,
    pub pk_type: ColumnType,
    /// All primary-key column indices, in order. Length 1 for the common single-PK case.
    pub pk_cols: Vec<usize>,
    /// Raw Postgres type name per column (parallel to [`Self::columns`]); `None` in library mode. Used
    /// to cast bound text params to the native type in backfill SQL (index-eligible comparisons).
    pub pg_types: Vec<Option<String>>,
    /// Whether each column is auto-defaulted (IDENTITY or `DEFAULT`), parallel to [`Self::columns`];
    /// all `false` in library mode. Surfaced by `GET /table/{name}/schema` so the add-row form can
    /// treat these columns as optional.
    pub has_defaults: Vec<bool>,
}

/// Separator joining composite-key column values into the durable-stream `key` string. Chosen to not
/// collide with real id text (the standard schema's ids are `l1-1` etc.).
const PK_SEP: char = '\u{1f}';

impl TableSchema {
    pub fn from_def(name: &str, def: &TableDef) -> Result<Self> {
        let columns: Vec<(String, ColumnType)> =
            def.columns.iter().map(|(c, d)| (c.clone(), d.ty)).collect();
        let pg_types: Vec<Option<String>> = def.columns.values().map(|d| d.pg_type.clone()).collect();
        let has_defaults: Vec<bool> = def.columns.values().map(|d| d.has_default).collect();
        let index: HashMap<String, usize> =
            columns.iter().enumerate().map(|(i, (c, _))| (c.clone(), i)).collect();
        if def.primary_key.is_empty() {
            anyhow::bail!("table '{name}' has no primary key");
        }
        let pk_cols: Vec<usize> = def
            .primary_key
            .iter()
            .map(|c| index.get(c).copied().ok_or_else(|| anyhow::anyhow!("primaryKey '{c}' is not a column")))
            .collect::<Result<_>>()?;
        let pk_index = pk_cols[0];
        let pk_type = columns[pk_index].1;
        Ok(TableSchema {
            name: name.to_string(),
            columns,
            index,
            pk_index,
            pk_name: def.primary_key[0].clone(),
            pk_type,
            pk_cols,
            pg_types,
            has_defaults,
        })
    }

    /// The raw Postgres type name of a column by name (for casting bound params to the native type).
    /// `None` in library mode or for an unknown column.
    pub fn pg_type_of(&self, col: &str) -> Option<&str> {
        self.index.get(col).and_then(|&i| self.pg_types.get(i)).and_then(|o| o.as_deref())
    }

    /// The durable-stream event `key` for a row: the single PK value's key-string, or composite PK column
    /// values joined by [`PK_SEP`]. This is the row identity used for routing, dedup, and subquery
    /// contributor ref-counting.
    pub fn key_string(&self, row: &Row) -> Result<String> {
        if self.pk_cols.len() == 1 {
            return Ok(row.get(self.pk_cols[0])?.to_key_string());
        }
        let parts: Vec<String> = self
            .pk_cols
            .iter()
            .map(|&i| row.get(i).map(Value::to_key_string))
            .collect::<Result<_>>()?;
        Ok(parts.join(&PK_SEP.to_string()))
    }

    pub fn column_index(&self, col: &str) -> Result<usize> {
        self.index.get(col).copied().ok_or_else(|| anyhow::anyhow!("unknown column '{col}'"))
    }

    pub fn column_type(&self, idx: usize) -> ColumnType {
        self.columns[idx].1
    }

    /// Build a positional `Row` from a JSON object (the envelope `value`).
    pub fn row_from_json(&self, obj: &serde_json::Map<String, serde_json::Value>) -> Result<Row> {
        let mut cols = Vec::with_capacity(self.columns.len());
        let null = serde_json::Value::Null;
        for (cname, cty) in &self.columns {
            let j = obj.get(cname).unwrap_or(&null);
            cols.push(Value::from_json(j, *cty)?);
        }
        Ok(Row(cols))
    }

    /// Serialize a `Row` back to a JSON object keyed by column name.
    pub fn row_to_json(&self, row: &Row) -> serde_json::Value {
        self.row_to_json_cols(row, None)
    }

    /// Serialize a row to a JSON object. With `cols = Some(indices)` only those columns are emitted
    /// (a shape's output projection); `None` emits the full row. Used to keep large unused columns out
    /// of a shape's stream (e.g. the list view never reads `description`).
    pub fn row_to_json_cols(&self, row: &Row, cols: Option<&[usize]>) -> serde_json::Value {
        let mut m = serde_json::Map::with_capacity(cols.map_or(self.columns.len(), <[usize]>::len));
        let mut emit = |i: usize| {
            if let Some((cname, _)) = self.columns.get(i) {
                let v = row.0.get(i).cloned().unwrap_or(Value::Null);
                m.insert(cname.clone(), v.to_json());
            }
        };
        match cols {
            Some(cols) => cols.iter().for_each(|&i| emit(i)),
            None => (0..self.columns.len()).for_each(emit),
        }
        serde_json::Value::Object(m)
    }

    pub fn pk_of<'a>(&self, row: &'a Row) -> Result<&'a Value> {
        row.get(self.pk_index)
    }
}

/// Compile a full schema into per-table `TableSchema`s.
pub fn compile_schema(schema: &Schema) -> Result<HashMap<String, TableSchema>> {
    let mut out = HashMap::new();
    for (name, def) in &schema.tables {
        if def.columns.is_empty() {
            bail!("table '{name}' has no columns");
        }
        out.insert(name.clone(), TableSchema::from_def(name, def)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn users() -> TableSchema {
        let json = serde_json::json!({
            "columns": { "id": {"type":"int"}, "name": {"type":"text"}, "active": {"type":"bool"}, "score": {"type":"float"} },
            "primaryKey": "id"
        });
        let def: TableDef = serde_json::from_value(json).unwrap();
        TableSchema::from_def("users", &def).unwrap()
    }

    #[test]
    fn sorted_columns_and_pk_index() {
        let ts = users();
        // sorted: active, id, name, score
        assert_eq!(ts.columns.iter().map(|(c, _)| c.as_str()).collect::<Vec<_>>(), ["active", "id", "name", "score"]);
        assert_eq!(ts.pk_index, 1);
        assert_eq!(ts.pk_type, ColumnType::Int);
    }

    #[test]
    fn row_json_roundtrip() {
        let ts = users();
        let obj = serde_json::json!({"id": 7, "name": "Alice", "active": true, "score": 9.5});
        let row = ts.row_from_json(obj.as_object().unwrap()).unwrap();
        assert_eq!(ts.pk_of(&row).unwrap(), &Value::Int(7));
        let back = ts.row_to_json(&row);
        assert_eq!(back, obj);
    }
}
