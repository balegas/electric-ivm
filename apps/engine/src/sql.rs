//! Compile a shape's predicate to a parameterized SQL `WHERE` fragment, so backfill can read only the
//! rows a shape needs (`SELECT … WHERE <predicate>`) instead of the whole table. Mirrors the
//! TypeScript `predicateToSql` in `@electric-lite/protocol` (and therefore the oracle's `WHERE`), so
//! the SQL filter agrees with the engine's `CompiledPredicate::matches` (three-valued NULL included).
//!
//! Numeric / boolean / null literals are inlined directly (they are typed Rust scalars — only digits,
//! `.`, `-`, `e`, `true`/`false`, `NULL` — so there is no injection surface); only **text** literals
//! are sent as bound parameters (`$1`, `$2`, …), which both escapes them and matches text columns
//! cleanly. The caller still applies `matches()` as the final authority, so the SQL only needs to be a
//! sound *superset* filter; mirroring the proven compiler keeps it exact.

use crate::predicate::{CompiledPredicate, LeafOp, PredicateJson};
use crate::schema::TableSchema;
use crate::value::Value;

/// Build a `WHERE` fragment + ordered text parameters for `pred`. Returns `None` for `MatchAll`
/// (no `WHERE` at all). Placeholders are numbered `$1..$n` in the order text literals appear.
pub fn predicate_to_sql(pred: &CompiledPredicate, ts: &TableSchema) -> Option<(String, Vec<String>)> {
    if matches!(pred, CompiledPredicate::MatchAll) {
        return None;
    }
    let mut params: Vec<String> = Vec::new();
    let text = build(pred, ts, &mut params);
    Some((text, params))
}

fn op_sql(op: LeafOp) -> &'static str {
    match op {
        LeafOp::Eq => "=",
        LeafOp::Neq => "<>",
        LeafOp::Lt => "<",
        LeafOp::Lte => "<=",
        LeafOp::Gt => ">",
        LeafOp::Gte => ">=",
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn build(p: &CompiledPredicate, ts: &TableSchema, params: &mut Vec<String>) -> String {
    match p {
        // Only reachable at the top level (compile never nests MatchAll); treat as the identity.
        CompiledPredicate::MatchAll => "TRUE".to_string(),
        CompiledPredicate::Cmp { col, op, value } => {
            let name = quote_ident(&ts.columns[*col].0);
            let o = op_sql(*op);
            match value {
                Value::Null => format!("{name} {o} NULL"),
                Value::Int(i) => format!("{name} {o} {i}"),
                Value::Float(f) => format!("{name} {o} {}", f.0),
                Value::Bool(b) => format!("{name} {o} {}", if *b { "true" } else { "false" }),
                Value::Text(s) => {
                    params.push(s.clone());
                    format!("{name} {o} ${}", params.len())
                }
            }
        }
        CompiledPredicate::And(v) => {
            if v.is_empty() {
                "TRUE".to_string()
            } else {
                let parts: Vec<String> = v.iter().map(|p| build(p, ts, params)).collect();
                format!("({})", parts.join(" AND "))
            }
        }
        CompiledPredicate::Or(v) => {
            if v.is_empty() {
                "FALSE".to_string()
            } else {
                let parts: Vec<String> = v.iter().map(|p| build(p, ts, params)).collect();
                format!("({})", parts.join(" OR "))
            }
        }
        CompiledPredicate::Not(b) => format!("(NOT {})", build(b, ts, params)),
        // Subquery SQL is emitted from the raw `PredicateJson` (which carries the inner table /
        // projection / where) via `predicate_json_to_sql`; the compiled form drops those details.
        CompiledPredicate::InSubquery { .. } => {
            unreachable!("subquery SQL must be built from PredicateJson, not the compiled predicate")
        }
    }
}

/// Build a `WHERE` fragment + text parameters directly from a **JSON** predicate, supporting
/// `IN (SELECT …)` subqueries (which the compiled form can't reconstruct). Mirrors the TS
/// `predicateToSql` shape; literal handling matches `predicate_to_sql` above (numbers/bools/null
/// inlined, text bound as `$n`). `start_param` is the next placeholder index (1-based). Returns
/// `None` for an empty/match-all predicate is *not* applicable here — pass a real predicate.
pub fn predicate_json_to_sql(pred: &PredicateJson, start_param: usize) -> (String, Vec<String>) {
    let mut params: Vec<String> = Vec::new();
    let text = build_json(pred, start_param, &mut params);
    (text, params)
}

fn build_json(p: &PredicateJson, start: usize, params: &mut Vec<String>) -> String {
    match p {
        PredicateJson::Leaf { col, op, value } => {
            let name = quote_ident(col);
            let o = op_sql(*op);
            match value {
                serde_json::Value::Null => format!("{name} {o} NULL"),
                serde_json::Value::Bool(b) => format!("{name} {o} {}", if *b { "true" } else { "false" }),
                serde_json::Value::Number(n) => format!("{name} {o} {n}"),
                serde_json::Value::String(s) => {
                    params.push(s.clone());
                    format!("{name} {o} ${}", start + params.len() - 1)
                }
                other => {
                    // Arrays/objects are not valid leaf literals; stringify defensively as a param.
                    params.push(other.to_string());
                    format!("{name} {o} ${}", start + params.len() - 1)
                }
            }
        }
        PredicateJson::And { and } => {
            if and.is_empty() {
                "TRUE".to_string()
            } else {
                let parts: Vec<String> =
                    and.iter().map(|p| build_json(p, start, params)).collect();
                format!("({})", parts.join(" AND "))
            }
        }
        PredicateJson::Or { or } => {
            if or.is_empty() {
                "FALSE".to_string()
            } else {
                let parts: Vec<String> = or.iter().map(|p| build_json(p, start, params)).collect();
                format!("({})", parts.join(" OR "))
            }
        }
        PredicateJson::Not { not } => format!("(NOT {})", build_json(not, start, params)),
        PredicateJson::In { col, subquery, negated } => {
            let op = if *negated { "NOT IN" } else { "IN" };
            let inner = match &subquery.where_ {
                Some(w) => format!(" WHERE {}", build_json(w, start, params)),
                None => String::new(),
            };
            format!(
                "{} {op} (SELECT {} FROM {}{inner})",
                quote_ident(col),
                quote_ident(&subquery.project),
                quote_ident(&subquery.table),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::predicate::PredicateJson;
    use crate::schema::{ColumnDef, ColumnType, TableDef};
    use std::collections::BTreeMap;

    fn schema() -> TableSchema {
        let mut columns = BTreeMap::new();
        columns.insert("id".to_string(), ColumnDef { ty: ColumnType::Int });
        columns.insert("name".to_string(), ColumnDef { ty: ColumnType::Text });
        columns.insert("score".to_string(), ColumnDef { ty: ColumnType::Float });
        columns.insert("active".to_string(), ColumnDef { ty: ColumnType::Bool });
        let def = TableDef { columns, primary_key: "id".to_string() };
        TableSchema::from_def("users", &def).unwrap()
    }

    fn sql(json: serde_json::Value) -> (String, Vec<String>) {
        let ts = schema();
        let pj: PredicateJson = serde_json::from_value(json).unwrap();
        let cp = CompiledPredicate::compile(&pj, &ts).unwrap();
        predicate_to_sql(&cp, &ts).unwrap()
    }

    #[test]
    fn leaf_text_is_parameterized() {
        let (w, p) = sql(serde_json::json!({"col": "name", "op": "eq", "value": "Alice"}));
        assert_eq!(w, r#""name" = $1"#);
        assert_eq!(p, vec!["Alice".to_string()]);
    }

    #[test]
    fn numeric_bool_null_are_inlined() {
        assert_eq!(sql(serde_json::json!({"col": "id", "op": "gte", "value": 3})).0, r#""id" >= 3"#);
        assert_eq!(sql(serde_json::json!({"col": "score", "op": "lt", "value": 1.5})).0, r#""score" < 1.5"#);
        assert_eq!(sql(serde_json::json!({"col": "active", "op": "eq", "value": true})).0, r#""active" = true"#);
        // a null literal in a leaf compares as NULL (SQL three-valued -> never TRUE), matching matches()
        assert_eq!(sql(serde_json::json!({"col": "name", "op": "neq", "value": null})).0, r#""name" <> NULL"#);
    }

    #[test]
    fn and_or_not_compose_with_ordered_placeholders() {
        let (w, p) = sql(serde_json::json!({
            "and": [
                { "col": "name", "op": "eq", "value": "a" },
                { "or": [ { "col": "id", "op": "gt", "value": 5 }, { "col": "name", "op": "eq", "value": "b" } ] },
                { "not": { "col": "active", "op": "eq", "value": false } }
            ]
        }));
        assert_eq!(w, r#"("name" = $1 AND ("id" > 5 OR "name" = $2) AND (NOT "active" = false))"#);
        assert_eq!(p, vec!["a".to_string(), "b".to_string()]);
    }

    fn jsql(json: serde_json::Value) -> (String, Vec<String>) {
        let pj: PredicateJson = serde_json::from_value(json).unwrap();
        predicate_json_to_sql(&pj, 1)
    }

    #[test]
    fn json_emitter_in_subquery() {
        let (w, p) = jsql(serde_json::json!({
            "col": "parent_id",
            "in": { "table": "parent", "project": "id", "where": { "col": "active", "op": "eq", "value": true } }
        }));
        assert_eq!(w, r#""parent_id" IN (SELECT "id" FROM "parent" WHERE "active" = true)"#);
        assert!(p.is_empty());
    }

    #[test]
    fn json_emitter_not_in_and_text_params() {
        let (w, p) = jsql(serde_json::json!({
            "col": "pid", "negated": true,
            "in": { "table": "p", "project": "id", "where": { "col": "name", "op": "eq", "value": "x" } }
        }));
        assert_eq!(w, r#""pid" NOT IN (SELECT "id" FROM "p" WHERE "name" = $1)"#);
        assert_eq!(p, vec!["x".to_string()]);
    }

    #[test]
    fn json_emitter_nested_subquery_and_param_numbering() {
        let (w, p) = jsql(serde_json::json!({
            "and": [
                { "col": "tag", "op": "eq", "value": "a" },
                { "col": "l3", "in": { "table": "level_3", "project": "id", "where": {
                    "col": "l2", "in": { "table": "level_2", "project": "id", "where": { "col": "name", "op": "eq", "value": "b" } } } } }
            ]
        }));
        assert_eq!(
            w,
            r#"("tag" = $1 AND "l3" IN (SELECT "id" FROM "level_3" WHERE "l2" IN (SELECT "id" FROM "level_2" WHERE "name" = $2)))"#
        );
        assert_eq!(p, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn text_value_with_quote_is_a_param_not_inlined() {
        // Injection-style input stays a bound parameter (no inlining), so it can't break the SQL.
        let (w, p) = sql(serde_json::json!({"col": "name", "op": "eq", "value": "x'); DROP TABLE users;--"}));
        assert_eq!(w, r#""name" = $1"#);
        assert_eq!(p, vec!["x'); DROP TABLE users;--".to_string()]);
    }
}
