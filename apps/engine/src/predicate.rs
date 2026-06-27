//! Predicate AST (deserialized from the control-plane JSON, mirroring `@electric-lite/protocol`)
//! compiled to a positional evaluator captured by a dbsp `filter` closure.

use anyhow::Result;
use serde::Deserialize;

use crate::schema::TableSchema;
use crate::value::{Row, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LeafOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
}

/// JSON predicate shape: a leaf `{col,op,value}` or a combinator `{and|or:[...]}` / `{not:{}}`.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum PredicateJson {
    Leaf { col: String, op: LeafOp, value: serde_json::Value },
    And { and: Vec<PredicateJson> },
    Or { or: Vec<PredicateJson> },
    Not { not: Box<PredicateJson> },
}

/// Compiled predicate over positional columns. `MatchAll` is used when a shape has no `where`.
#[derive(Debug, Clone)]
pub enum CompiledPredicate {
    MatchAll,
    Cmp { col: usize, op: LeafOp, value: Value },
    And(Vec<CompiledPredicate>),
    Or(Vec<CompiledPredicate>),
    Not(Box<CompiledPredicate>),
}

impl CompiledPredicate {
    pub fn compile(p: &PredicateJson, ts: &TableSchema) -> Result<Self> {
        Ok(match p {
            PredicateJson::Leaf { col, op, value } => {
                let idx = ts.column_index(col)?;
                let v = Value::from_json(value, ts.column_type(idx))?;
                CompiledPredicate::Cmp { col: idx, op: *op, value: v }
            }
            PredicateJson::And { and } => {
                CompiledPredicate::And(and.iter().map(|p| Self::compile(p, ts)).collect::<Result<_>>()?)
            }
            PredicateJson::Or { or } => {
                CompiledPredicate::Or(or.iter().map(|p| Self::compile(p, ts)).collect::<Result<_>>()?)
            }
            PredicateJson::Not { not } => {
                CompiledPredicate::Not(Box::new(Self::compile(not, ts)?))
            }
        })
    }

    /// Compile an optional predicate; `None` -> match all rows.
    pub fn compile_opt(p: Option<&PredicateJson>, ts: &TableSchema) -> Result<Self> {
        match p {
            Some(p) => Self::compile(p, ts),
            None => Ok(CompiledPredicate::MatchAll),
        }
    }

    pub fn eval(&self, row: &Row) -> bool {
        match self {
            CompiledPredicate::MatchAll => true,
            CompiledPredicate::Cmp { col, op, value } => {
                let cell = row.0.get(*col).unwrap_or(&Value::Null);
                cmp(cell, *op, value)
            }
            CompiledPredicate::And(ps) => ps.iter().all(|p| p.eval(row)),
            CompiledPredicate::Or(ps) => ps.iter().any(|p| p.eval(row)),
            // KNOWN LIMITATION (safe under the current no-null contract): this is two-valued, so
            // `NOT (col = x)` on a NULL cell yields true here but NULL (excluded) in Postgres. The
            // simulator never generates nulls, so engine and oracle agree today. Introducing nulls
            // requires three-valued logic (a null-derived leaf must keep the row out under NOT).
            CompiledPredicate::Not(p) => !p.eval(row),
        }
    }
}

/// Compare a cell against a literal under SQL-ish two-valued semantics: a `null` cell never
/// matches (mirrors `@electric-lite/protocol`'s evaluator and Postgres for non-null literals).
fn cmp(cell: &Value, op: LeafOp, value: &Value) -> bool {
    if matches!(cell, Value::Null) {
        return false;
    }
    match op {
        LeafOp::Eq => cell == value,
        LeafOp::Neq => cell != value,
        LeafOp::Lt | LeafOp::Lte | LeafOp::Gt | LeafOp::Gte => {
            let Some(ord) = ordering(cell, value) else { return false };
            match op {
                LeafOp::Lt => ord.is_lt(),
                LeafOp::Lte => ord.is_le(),
                LeafOp::Gt => ord.is_gt(),
                LeafOp::Gte => ord.is_ge(),
                _ => unreachable!(),
            }
        }
    }
}

fn ordering(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => x.partial_cmp(y),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y),
        (Value::Text(x), Value::Text(y)) => x.partial_cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.partial_cmp(y),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{TableDef, TableSchema};

    fn users() -> TableSchema {
        let json = serde_json::json!({
            "columns": { "id": {"type":"int"}, "name": {"type":"text"}, "age": {"type":"int"}, "active": {"type":"bool"} },
            "primaryKey": "id"
        });
        let def: TableDef = serde_json::from_value(json).unwrap();
        TableSchema::from_def("users", &def).unwrap()
    }

    fn row(ts: &TableSchema, j: serde_json::Value) -> Row {
        ts.row_from_json(j.as_object().unwrap()).unwrap()
    }

    #[test]
    fn equality_and_comparison() {
        let ts = users();
        let p: PredicateJson = serde_json::from_value(serde_json::json!({"col":"active","op":"eq","value":true})).unwrap();
        let cp = CompiledPredicate::compile(&p, &ts).unwrap();
        assert!(cp.eval(&row(&ts, serde_json::json!({"id":1,"name":"a","age":20,"active":true}))));
        assert!(!cp.eval(&row(&ts, serde_json::json!({"id":2,"name":"b","age":20,"active":false}))));

        let p2: PredicateJson = serde_json::from_value(serde_json::json!({"col":"age","op":"gte","value":18})).unwrap();
        let cp2 = CompiledPredicate::compile(&p2, &ts).unwrap();
        assert!(cp2.eval(&row(&ts, serde_json::json!({"id":1,"name":"a","age":18,"active":true}))));
        assert!(!cp2.eval(&row(&ts, serde_json::json!({"id":2,"name":"b","age":17,"active":true}))));
    }

    #[test]
    fn boolean_combinators() {
        let ts = users();
        let p: PredicateJson = serde_json::from_value(serde_json::json!({
            "and": [
                {"col":"active","op":"eq","value":true},
                {"or": [ {"col":"age","op":"gt","value":30}, {"not": {"col":"name","op":"eq","value":"bob"}} ]}
            ]
        })).unwrap();
        let cp = CompiledPredicate::compile(&p, &ts).unwrap();
        assert!(cp.eval(&row(&ts, serde_json::json!({"id":1,"name":"alice","age":20,"active":true}))));
        assert!(!cp.eval(&row(&ts, serde_json::json!({"id":2,"name":"bob","age":20,"active":true}))));
        assert!(!cp.eval(&row(&ts, serde_json::json!({"id":3,"name":"alice","age":20,"active":false}))));
    }

    #[test]
    fn match_all_when_no_predicate() {
        let ts = users();
        let cp = CompiledPredicate::compile_opt(None, &ts).unwrap();
        assert!(cp.eval(&row(&ts, serde_json::json!({"id":1,"name":"a","age":1,"active":false}))));
    }
}
