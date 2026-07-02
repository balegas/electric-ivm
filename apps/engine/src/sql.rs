//! Compile a shape's predicate to a parameterized SQL `WHERE` fragment, so backfill can read only the
//! rows a shape needs (`SELECT … WHERE <predicate>`) instead of the whole table. Mirrors the
//! TypeScript `predicateToSql` in `@electric-ivm/protocol` (and therefore the oracle's `WHERE`), so
//! the SQL filter agrees with the engine's `CompiledPredicate::matches` (three-valued NULL included).
//!
//! Numeric / boolean / null literals are inlined directly (they are typed Rust scalars — only digits,
//! `.`, `-`, `e`, `true`/`false`, `NULL` — so there is no injection surface); only **text** literals
//! are sent as bound parameters (`$1`, `$2`, …), which both escapes them and matches text columns
//! cleanly. The caller still applies `matches()` as the final authority, so the SQL only needs to be a
//! sound *superset* filter; mirroring the proven compiler keeps it exact.

use std::collections::HashMap;

use crate::predicate::{CompiledPredicate, LeafOp, PredicateJson};
use crate::schema::TableSchema;
use crate::value::Value;

/// Compare a text-bound param `$n` to column `name` (already quoted). When the column's native Postgres
/// type is known, cast the (text) param to it — `name = $n::text::uuid` — so the comparison stays
/// index-eligible (a plain `name::text = $n` defeats the btree index). `None` (library mode / unknown
/// column) falls back to the text-vs-text form, which is always correct for our coarse-Text columns.
fn text_param_cmp(name: &str, op: &str, n: usize, pg_type: Option<&str>) -> String {
    match pg_type {
        Some(ty) => format!("{name} {op} ${n}::text::{}", quote_ident(ty)),
        None => format!("{name}::text {op} ${n}"),
    }
}

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
        LeafOp::Like => "LIKE",
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
                    // Bind text as a `$n` param, cast to the column's native type when known. Our schema
                    // coarsens uuid/timestamptz/… to `Text`, so the real Postgres column may be uuid;
                    // binding a Rust `String` against a uuid param is refused by tokio-postgres
                    // (`cannot convert String -> uuid`). `$n::text::uuid` sends the param as text and
                    // parses it to the native type (index-eligible); unknown type → `col::text` fallback.
                    params.push(s.clone());
                    text_param_cmp(&name, o, params.len(), ts.pg_types.get(*col).and_then(|o| o.as_deref()))
                }
            }
        }
        CompiledPredicate::IsNull { col, is_null } => {
            let name = quote_ident(&ts.columns[*col].0);
            format!("{name} IS {}NULL", if *is_null { "" } else { "NOT " })
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
pub fn predicate_json_to_sql(
    pred: &PredicateJson,
    start_param: usize,
    schemas: &HashMap<String, TableSchema>,
    table: &str,
) -> (String, Vec<String>) {
    let mut params: Vec<String> = Vec::new();
    let text = build_json(pred, start_param, &mut params, schemas, table);
    (text, params)
}

/// `table` is the table the current predicate's columns belong to; it switches to the inner table when
/// descending into a subquery, so each leaf's param is cast to the right column's native type.
fn build_json(
    p: &PredicateJson,
    start: usize,
    params: &mut Vec<String>,
    schemas: &HashMap<String, TableSchema>,
    table: &str,
) -> String {
    let pg_type = |col: &str| schemas.get(table).and_then(|ts| ts.pg_type_of(col)).map(str::to_string);
    match p {
        PredicateJson::Leaf { col, op, value } => {
            let name = quote_ident(col);
            let o = op_sql(*op);
            match value {
                serde_json::Value::Null => format!("{name} {o} NULL"),
                serde_json::Value::Bool(b) => format!("{name} {o} {}", if *b { "true" } else { "false" }),
                serde_json::Value::Number(n) => format!("{name} {o} {n}"),
                serde_json::Value::String(s) => {
                    // Bind as text, cast to the column's native type when known (see `text_param_cmp`).
                    params.push(s.clone());
                    text_param_cmp(&name, o, start + params.len() - 1, pg_type(col).as_deref())
                }
                other => {
                    // Arrays/objects are not valid leaf literals; stringify defensively as a param.
                    params.push(other.to_string());
                    text_param_cmp(&name, o, start + params.len() - 1, pg_type(col).as_deref())
                }
            }
        }
        PredicateJson::IsNull { col, is_null } => {
            let name = quote_ident(col);
            format!("{name} IS {}NULL", if *is_null { "" } else { "NOT " })
        }
        PredicateJson::And { and } => {
            if and.is_empty() {
                "TRUE".to_string()
            } else {
                let parts: Vec<String> =
                    and.iter().map(|p| build_json(p, start, params, schemas, table)).collect();
                format!("({})", parts.join(" AND "))
            }
        }
        PredicateJson::Or { or } => {
            if or.is_empty() {
                "FALSE".to_string()
            } else {
                let parts: Vec<String> =
                    or.iter().map(|p| build_json(p, start, params, schemas, table)).collect();
                format!("({})", parts.join(" OR "))
            }
        }
        PredicateJson::Not { not } => format!("(NOT {})", build_json(not, start, params, schemas, table)),
        PredicateJson::In { col, subquery, negated } => {
            let op = if *negated { "NOT IN" } else { "IN" };
            // The inner where's columns belong to the subquery's table — switch context so their params
            // are cast to the inner table's native types.
            let inner = match &subquery.where_ {
                Some(w) => format!(" WHERE {}", build_json(w, start, params, schemas, &subquery.table)),
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
        columns.insert("id".to_string(), ColumnDef { ty: ColumnType::Int, pg_type: None });
        columns.insert("name".to_string(), ColumnDef { ty: ColumnType::Text, pg_type: None });
        columns.insert("score".to_string(), ColumnDef { ty: ColumnType::Float, pg_type: None });
        columns.insert("active".to_string(), ColumnDef { ty: ColumnType::Bool, pg_type: None });
        let def = TableDef { columns, primary_key: vec!["id".to_string()] };
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
        assert_eq!(w, r#""name"::text = $1"#);
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
        assert_eq!(w, r#"("name"::text = $1 AND ("id" > 5 OR "name"::text = $2) AND (NOT "active" = false))"#);
        assert_eq!(p, vec!["a".to_string(), "b".to_string()]);
    }

    // No schemas -> pg_type unknown -> the `col::text` fallback form (exercised by these tests).
    fn jsql(json: serde_json::Value) -> (String, Vec<String>) {
        let pj: PredicateJson = serde_json::from_value(json).unwrap();
        predicate_json_to_sql(&pj, 1, &HashMap::new(), "t")
    }

    /// A table with a real Postgres type per column (as introspection would produce).
    fn typed_schema(name: &str, cols: &[(&str, ColumnType, &str)]) -> TableSchema {
        let mut columns = BTreeMap::new();
        for (c, ty, pg) in cols {
            columns.insert(c.to_string(), ColumnDef { ty: *ty, pg_type: Some(pg.to_string()) });
        }
        let def = TableDef { columns, primary_key: vec![cols[0].0.to_string()] };
        TableSchema::from_def(name, &def).unwrap()
    }

    #[test]
    fn text_param_cast_to_native_type_when_known() {
        // uuid column -> `$n::text::uuid` (index-eligible), not `col::text = $n`.
        let ts = typed_schema("projects", &[("id", ColumnType::Text, "uuid"), ("owner_id", ColumnType::Text, "uuid")]);
        let pj: PredicateJson = serde_json::from_value(serde_json::json!({"col":"owner_id","op":"eq","value":"u1"})).unwrap();
        let cp = CompiledPredicate::compile(&pj, &ts).unwrap();
        let (w, p) = predicate_to_sql(&cp, &ts).unwrap();
        assert_eq!(w, r#""owner_id" = $1::text::"uuid""#);
        assert_eq!(p, vec!["u1".to_string()]);

        // Same via the JSON/subquery emitter, casting the INNER table's column to its native type.
        let mut schemas = HashMap::new();
        schemas.insert("projects".to_string(), ts);
        schemas.insert(
            "issues".to_string(),
            typed_schema("issues", &[("id", ColumnType::Text, "uuid"), ("project_id", ColumnType::Text, "uuid")]),
        );
        let outer: PredicateJson = serde_json::from_value(serde_json::json!({
            "col": "project_id",
            "in": { "table": "projects", "project": "id", "where": { "col": "owner_id", "op": "eq", "value": "u1" } }
        }))
        .unwrap();
        let (w, p) = predicate_json_to_sql(&outer, 1, &schemas, "issues");
        assert_eq!(w, r#""project_id" IN (SELECT "id" FROM "projects" WHERE "owner_id" = $1::text::"uuid")"#);
        assert_eq!(p, vec!["u1".to_string()]);
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
        assert_eq!(w, r#""pid" NOT IN (SELECT "id" FROM "p" WHERE "name"::text = $1)"#);
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
            r#"("tag"::text = $1 AND "l3" IN (SELECT "id" FROM "level_3" WHERE "l2" IN (SELECT "id" FROM "level_2" WHERE "name"::text = $2)))"#
        );
        assert_eq!(p, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn text_value_with_quote_is_a_param_not_inlined() {
        // Injection-style input stays a bound parameter (no inlining), so it can't break the SQL.
        let (w, p) = sql(serde_json::json!({"col": "name", "op": "eq", "value": "x'); DROP TABLE users;--"}));
        assert_eq!(w, r#""name"::text = $1"#);
        assert_eq!(p, vec!["x'); DROP TABLE users;--".to_string()]);
    }
}
