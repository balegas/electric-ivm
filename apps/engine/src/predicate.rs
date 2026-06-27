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

    /// Filter membership under SQL `WHERE` semantics: a row is included iff the predicate is TRUE.
    /// UNKNOWN (from a NULL operand) and FALSE both exclude the row.
    pub fn matches(&self, row: &Row) -> bool {
        self.eval(row) == Tri::True
    }

    /// Three-valued evaluation (TRUE / FALSE / UNKNOWN), mirroring Postgres so the engine and the
    /// pglite oracle agree even in the presence of NULLs.
    fn eval(&self, row: &Row) -> Tri {
        match self {
            CompiledPredicate::MatchAll => Tri::True,
            CompiledPredicate::Cmp { col, op, value } => {
                let cell = row.0.get(*col).unwrap_or(&Value::Null);
                cmp(cell, *op, value)
            }
            // AND: FALSE dominates; else UNKNOWN if any UNKNOWN; else TRUE (empty AND => TRUE).
            CompiledPredicate::And(ps) => {
                let mut acc = Tri::True;
                for p in ps {
                    match p.eval(row) {
                        Tri::False => return Tri::False,
                        Tri::Unknown => acc = Tri::Unknown,
                        Tri::True => {}
                    }
                }
                acc
            }
            // OR: TRUE dominates; else UNKNOWN if any UNKNOWN; else FALSE (empty OR => FALSE).
            CompiledPredicate::Or(ps) => {
                let mut acc = Tri::False;
                for p in ps {
                    match p.eval(row) {
                        Tri::True => return Tri::True,
                        Tri::Unknown => acc = Tri::Unknown,
                        Tri::False => {}
                    }
                }
                acc
            }
            // NOT TRUE = FALSE, NOT FALSE = TRUE, NOT UNKNOWN = UNKNOWN. The UNKNOWN case is the fix
            // that makes `NOT (col = x)` over a NULL cell keep the row out, exactly as Postgres does.
            CompiledPredicate::Not(p) => p.eval(row).not(),
        }
    }
}

/// SQL three-valued truth value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tri {
    True,
    False,
    Unknown,
}

impl Tri {
    fn from_bool(b: bool) -> Tri {
        if b { Tri::True } else { Tri::False }
    }
    fn not(self) -> Tri {
        match self {
            Tri::True => Tri::False,
            Tri::False => Tri::True,
            Tri::Unknown => Tri::Unknown,
        }
    }
}

/// Compare a cell against a literal under SQL three-valued semantics: any NULL operand yields
/// UNKNOWN (mirrors Postgres and `@electric-lite/protocol`'s evaluator).
fn cmp(cell: &Value, op: LeafOp, value: &Value) -> Tri {
    if matches!(cell, Value::Null) || matches!(value, Value::Null) {
        return Tri::Unknown;
    }
    let truth = match op {
        LeafOp::Eq => cell == value,
        LeafOp::Neq => cell != value,
        LeafOp::Lt | LeafOp::Lte | LeafOp::Gt | LeafOp::Gte => {
            // A type mismatch has no ordering; treat as UNKNOWN (literals are column-typed, so this
            // does not arise in practice).
            let Some(ord) = ordering(cell, value) else { return Tri::Unknown };
            // TEST-ONLY: the `off_by_one_cmp` fault makes `<=`/`>=` strict, so rows exactly on a
            // boundary literal are mishandled. No-op unless ELECTRIC_LITE_FAULT=off_by_one_cmp.
            let off_by_one = matches!(crate::fault::active(), crate::fault::Fault::OffByOneCmp);
            match op {
                LeafOp::Lt => ord.is_lt(),
                LeafOp::Lte => if off_by_one { ord.is_lt() } else { ord.is_le() },
                LeafOp::Gt => ord.is_gt(),
                LeafOp::Gte => if off_by_one { ord.is_gt() } else { ord.is_ge() },
                _ => unreachable!(),
            }
        }
    };
    Tri::from_bool(truth)
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
        assert!(cp.matches(&row(&ts, serde_json::json!({"id":1,"name":"a","age":20,"active":true}))));
        assert!(!cp.matches(&row(&ts, serde_json::json!({"id":2,"name":"b","age":20,"active":false}))));

        let p2: PredicateJson = serde_json::from_value(serde_json::json!({"col":"age","op":"gte","value":18})).unwrap();
        let cp2 = CompiledPredicate::compile(&p2, &ts).unwrap();
        assert!(cp2.matches(&row(&ts, serde_json::json!({"id":1,"name":"a","age":18,"active":true}))));
        assert!(!cp2.matches(&row(&ts, serde_json::json!({"id":2,"name":"b","age":17,"active":true}))));
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
        assert!(cp.matches(&row(&ts, serde_json::json!({"id":1,"name":"alice","age":20,"active":true}))));
        assert!(!cp.matches(&row(&ts, serde_json::json!({"id":2,"name":"bob","age":20,"active":true}))));
        assert!(!cp.matches(&row(&ts, serde_json::json!({"id":3,"name":"alice","age":20,"active":false}))));
    }

    #[test]
    fn match_all_when_no_predicate() {
        let ts = users();
        let cp = CompiledPredicate::compile_opt(None, &ts).unwrap();
        assert!(cp.matches(&row(&ts, serde_json::json!({"id":1,"name":"a","age":1,"active":false}))));
    }

    #[test]
    fn three_valued_null_logic() {
        let ts = users();
        let compile = |j: serde_json::Value| {
            CompiledPredicate::compile(&serde_json::from_value::<PredicateJson>(j).unwrap(), &ts).unwrap()
        };
        // Rows with a NULL `name` / NULL `age`.
        let null_name = row(&ts, serde_json::json!({"id":1,"name":null,"age":20,"active":true}));
        let null_age_active = row(&ts, serde_json::json!({"id":2,"name":"a","age":null,"active":true}));
        let null_age_inactive = row(&ts, serde_json::json!({"id":3,"name":"a","age":null,"active":false}));

        // Leaf over NULL -> UNKNOWN -> excluded (eq and neq alike).
        assert!(!compile(serde_json::json!({"col":"name","op":"eq","value":"alpha"})).matches(&null_name));
        assert!(!compile(serde_json::json!({"col":"name","op":"neq","value":"alpha"})).matches(&null_name));

        // THE FIX: NOT(name = 'alpha') over a NULL name is NOT UNKNOWN = UNKNOWN -> excluded.
        // The old two-valued evaluator wrongly returned true here and would have leaked the row.
        let not_eq = compile(serde_json::json!({"not":{"col":"name","op":"eq","value":"alpha"}}));
        assert!(!not_eq.matches(&null_name));
        // ...but NOT(name = 'alpha') over a concrete non-match is TRUE.
        assert!(not_eq.matches(&row(&ts, serde_json::json!({"id":9,"name":"bob","age":20,"active":true}))));

        // AND: TRUE AND UNKNOWN = UNKNOWN -> excluded; FALSE AND UNKNOWN = FALSE -> excluded.
        let and = compile(serde_json::json!({"and":[{"col":"active","op":"eq","value":true},{"col":"age","op":"gt","value":18}]}));
        assert!(!and.matches(&null_age_active));
        assert!(!and.matches(&null_age_inactive));

        // OR: TRUE OR UNKNOWN = TRUE -> included; FALSE OR UNKNOWN = UNKNOWN -> excluded.
        let or = compile(serde_json::json!({"or":[{"col":"active","op":"eq","value":true},{"col":"age","op":"gt","value":100}]}));
        assert!(or.matches(&null_age_active));
        assert!(!or.matches(&null_age_inactive));
    }
}
