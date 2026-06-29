//! Schema types (deserialized from the control-plane JSON, mirroring `@electric-lite/protocol`)
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
}

#[derive(Debug, Clone, Deserialize)]
pub struct TableDef {
    // BTreeMap gives a deterministic (sorted) column order regardless of JSON key order, which
    // we use as the positional Row order. Only internal consistency matters.
    pub columns: BTreeMap<String, ColumnDef>,
    #[serde(rename = "primaryKey")]
    pub primary_key: String,
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
    pub pk_index: usize,
    pub pk_name: String,
    pub pk_type: ColumnType,
}

impl TableSchema {
    pub fn from_def(name: &str, def: &TableDef) -> Result<Self> {
        let columns: Vec<(String, ColumnType)> =
            def.columns.iter().map(|(c, d)| (c.clone(), d.ty)).collect();
        let index: HashMap<String, usize> =
            columns.iter().enumerate().map(|(i, (c, _))| (c.clone(), i)).collect();
        let pk_index = *index
            .get(&def.primary_key)
            .ok_or_else(|| anyhow::anyhow!("primaryKey '{}' is not a column", def.primary_key))?;
        let pk_type = columns[pk_index].1;
        Ok(TableSchema {
            name: name.to_string(),
            columns,
            index,
            pk_index,
            pk_name: def.primary_key.clone(),
            pk_type,
        })
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
