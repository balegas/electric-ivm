//! Parse Electric's SQL `where` clause (sent as a query param on `GET /v1/shape`) into our
//! [`PredicateJson`] AST. Covers the grammar Electric's oracle generator emits (and the hand-written
//! integration where-clauses): `col <op> lit` (`= <> != < <= > >=`), `col [NOT] LIKE 'pat'`,
//! `col [NOT] BETWEEN a AND b`, `col [NOT] IN ('a', …)`, `col [NOT] IN (SELECT proj FROM t [WHERE …])`
//! (recursive), `col IS [NOT] NULL`, `AND`/`OR`/`NOT`, parentheses, and `bool`/`true`/`false` literals.
//!
//! Desugaring keeps the engine's op set small: `BETWEEN a b` → `AND(>=a, <=b)`, `IN (list)` →
//! `OR(=v…)`, and every `NOT <x>` → `Not(<x>)` except `NOT IN (SELECT …)` which sets the subquery's
//! `negated` flag (so it shares the `In` node machinery). `col IS [NOT] NULL` maps to the native
//! [`PredicateJson::IsNull`] null-test leaf (the one predicate that is TRUE on a NULL cell — it
//! composes correctly under `NOT`, unlike any comparison-based desugaring).

use anyhow::{Result, bail};

use crate::predicate::{LeafOp, PredicateJson, SubqueryJson};
use crate::schema::TableSchema;

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    Str(String),
    Num(String),
    Op(String), // = <> != < <= > >=
    LParen,
    RParen,
    Comma,
    // keywords
    And,
    Or,
    Not,
    Like,
    Between,
    In,
    Select,
    From,
    Where,
    True,
    False,
    Is,
    Null,
}

fn tokenize(s: &str) -> Result<Vec<Tok>> {
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    let mut out = Vec::new();
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '\'' {
            // string literal; '' is an escaped quote
            i += 1;
            let mut buf = String::new();
            loop {
                if i >= chars.len() {
                    bail!("unterminated string literal");
                }
                if chars[i] == '\'' {
                    if i + 1 < chars.len() && chars[i + 1] == '\'' {
                        buf.push('\'');
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    buf.push(chars[i]);
                    i += 1;
                }
            }
            out.push(Tok::Str(buf));
        } else if c == '(' {
            out.push(Tok::LParen);
            i += 1;
        } else if c == ')' {
            out.push(Tok::RParen);
            i += 1;
        } else if c == ',' {
            out.push(Tok::Comma);
            i += 1;
        } else if "=<>!".contains(c) {
            let mut op = String::new();
            while i < chars.len() && "=<>!".contains(chars[i]) {
                op.push(chars[i]);
                i += 1;
            }
            out.push(Tok::Op(op));
        } else if c == '"' {
            // double-quoted identifier; "" is an escaped quote
            i += 1;
            let mut buf = String::new();
            loop {
                if i >= chars.len() {
                    bail!("unterminated quoted identifier");
                }
                if chars[i] == '"' {
                    if i + 1 < chars.len() && chars[i + 1] == '"' {
                        buf.push('"');
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    buf.push(chars[i]);
                    i += 1;
                }
            }
            out.push(Tok::Ident(buf));
        } else if starts_number(&chars, i) {
            // number: '-'? ( digits ('.' digits*)? | '.' digits ) ( [eE] [+-]? digits )?
            let start = i;
            if chars[i] == '-' {
                i += 1;
            }
            let mut seen_dot = false;
            while i < chars.len() && (chars[i].is_ascii_digit() || (chars[i] == '.' && !seen_dot)) {
                seen_dot |= chars[i] == '.';
                i += 1;
            }
            if i < chars.len() && (chars[i] == 'e' || chars[i] == 'E') {
                let mut j = i + 1;
                if j < chars.len() && (chars[j] == '+' || chars[j] == '-') {
                    j += 1;
                }
                if j < chars.len() && chars[j].is_ascii_digit() {
                    i = j;
                    while i < chars.len() && chars[i].is_ascii_digit() {
                        i += 1;
                    }
                }
            }
            let buf: String = chars[start..i].iter().collect();
            // A digit/dot/letter running straight on (`1.2.3`, `1e`, `10x`) is a malformed literal, not
            // a string — erroring beats the old silent fall-back to a STRING literal.
            if i < chars.len() && (chars[i] == '.' || chars[i].is_alphanumeric() || chars[i] == '_') {
                bail!("malformed numeric literal starting with '{buf}'");
            }
            out.push(Tok::Num(buf));
        } else if c.is_alphabetic() || c == '_' {
            let mut buf = String::new();
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                buf.push(chars[i]);
                i += 1;
            }
            let kw = buf.to_ascii_uppercase();
            out.push(match kw.as_str() {
                "AND" => Tok::And,
                "OR" => Tok::Or,
                "NOT" => Tok::Not,
                "LIKE" => Tok::Like,
                "BETWEEN" => Tok::Between,
                "IN" => Tok::In,
                "SELECT" => Tok::Select,
                "FROM" => Tok::From,
                "WHERE" => Tok::Where,
                "TRUE" => Tok::True,
                "FALSE" => Tok::False,
                "IS" => Tok::Is,
                "NULL" => Tok::Null,
                _ => Tok::Ident(buf),
            });
        } else {
            bail!("unexpected character '{c}' in where clause");
        }
    }
    Ok(out)
}

/// Does a numeric literal start at `chars[i]`? Digits, `.5` / `-.5` leading-dot floats, and negative
/// numbers all qualify (a bare `-`/`.` does not — those stay lexer errors).
fn starts_number(chars: &[char], i: usize) -> bool {
    let digit_at = |j: usize| chars.get(j).is_some_and(|c| c.is_ascii_digit());
    match chars[i] {
        c if c.is_ascii_digit() => true,
        '.' => digit_at(i + 1),
        '-' => digit_at(i + 1) || (chars.get(i + 1) == Some(&'.') && digit_at(i + 2)),
        _ => false,
    }
}

struct Parser<'a> {
    toks: Vec<Tok>,
    pos: usize,
    /// Table schema, when the caller has it ([`parse_where_typed`]): enables column-existence
    /// errors at parse time for `IS [NOT] NULL` leaves.
    schema: Option<&'a TableSchema>,
}

impl Parser<'_> {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }
    fn next(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.pos).cloned();
        self.pos += 1;
        t
    }
    fn eat(&mut self, t: &Tok) -> Result<()> {
        match self.next() {
            Some(ref got) if got == t => Ok(()),
            other => bail!("expected {t:?}, got {other:?}"),
        }
    }

    // or := and ( OR and )*
    fn parse_or(&mut self) -> Result<PredicateJson> {
        let mut parts = vec![self.parse_and()?];
        while matches!(self.peek(), Some(Tok::Or)) {
            self.next();
            parts.push(self.parse_and()?);
        }
        Ok(if parts.len() == 1 { parts.pop().unwrap() } else { PredicateJson::Or { or: parts } })
    }

    // and := not ( AND not )*
    fn parse_and(&mut self) -> Result<PredicateJson> {
        let mut parts = vec![self.parse_not()?];
        while matches!(self.peek(), Some(Tok::And)) {
            self.next();
            parts.push(self.parse_not()?);
        }
        Ok(if parts.len() == 1 { parts.pop().unwrap() } else { PredicateJson::And { and: parts } })
    }

    // not := NOT not | primary
    fn parse_not(&mut self) -> Result<PredicateJson> {
        if matches!(self.peek(), Some(Tok::Not)) {
            self.next();
            let inner = self.parse_not()?;
            return Ok(PredicateJson::Not { not: Box::new(inner) });
        }
        self.parse_primary()
    }

    // primary := '(' or ')' | predicate
    fn parse_primary(&mut self) -> Result<PredicateJson> {
        if matches!(self.peek(), Some(Tok::LParen)) {
            self.next();
            let e = self.parse_or()?;
            self.eat(&Tok::RParen)?;
            return Ok(e);
        }
        self.parse_predicate()
    }

    fn parse_ident(&mut self) -> Result<String> {
        match self.next() {
            Some(Tok::Ident(s)) => Ok(s),
            other => bail!("expected identifier, got {other:?}"),
        }
    }

    fn parse_literal(&mut self) -> Result<serde_json::Value> {
        match self.next() {
            Some(Tok::Str(s)) => Ok(serde_json::Value::String(s)),
            Some(Tok::True) => Ok(serde_json::Value::Bool(true)),
            Some(Tok::False) => Ok(serde_json::Value::Bool(false)),
            Some(Tok::Num(n)) => num_literal(&n),
            other => bail!("expected literal, got {other:?}"),
        }
    }

    // predicate := col [NOT] (op lit | LIKE str | BETWEEN a AND b | IN (...))  |  lit op lit (constant)
    fn parse_predicate(&mut self) -> Result<PredicateJson> {
        // Constant comparison `<lit> <op> <lit>` (e.g. `373 = 373`, used as a trivial always-true
        // predicate). Evaluate it at parse time → an empty `And` (TRUE) or empty `Or` (FALSE).
        if matches!(self.peek(), Some(Tok::Str(_) | Tok::Num(_) | Tok::True | Tok::False)) {
            let left = self.parse_literal()?;
            let Some(Tok::Op(op)) = self.next() else {
                bail!("expected comparison operator after literal in constant predicate");
            };
            let right = self.parse_literal()?;
            let truth = eval_const(&left, &op, &right);
            return Ok(if truth {
                PredicateJson::And { and: vec![] }
            } else {
                PredicateJson::Or { or: vec![] }
            });
        }
        let col = self.parse_ident()?;
        let negated = if matches!(self.peek(), Some(Tok::Not)) {
            self.next();
            true
        } else {
            false
        };
        let pred = match self.peek() {
            Some(Tok::Is) => {
                // col IS [NOT] NULL
                self.next();
                let is_not = if matches!(self.peek(), Some(Tok::Not)) {
                    self.next();
                    true
                } else {
                    false
                };
                self.eat(&Tok::Null)?;
                if negated {
                    bail!("unexpected NOT before IS");
                }
                // Validate the column when we have the schema (same behavior as other leaves).
                if let Some(ts) = self.schema {
                    ts.column_index(&col)?;
                }
                return Ok(PredicateJson::IsNull { col, is_null: !is_not });
            }
            Some(Tok::Op(_)) => {
                let Some(Tok::Op(op)) = self.next() else { unreachable!() };
                let value = self.parse_literal()?;
                let op = match op.as_str() {
                    "=" => LeafOp::Eq,
                    "<>" | "!=" => LeafOp::Neq,
                    "<" => LeafOp::Lt,
                    "<=" => LeafOp::Lte,
                    ">" => LeafOp::Gt,
                    ">=" => LeafOp::Gte,
                    other => bail!("unsupported operator '{other}'"),
                };
                PredicateJson::Leaf { col, op, value }
            }
            Some(Tok::Like) => {
                self.next();
                let value = self.parse_literal()?;
                PredicateJson::Leaf { col, op: LeafOp::Like, value }
            }
            Some(Tok::Between) => {
                self.next();
                let lo = self.parse_literal()?;
                self.eat(&Tok::And)?;
                let hi = self.parse_literal()?;
                PredicateJson::And {
                    and: vec![
                        PredicateJson::Leaf { col: col.clone(), op: LeafOp::Gte, value: lo },
                        PredicateJson::Leaf { col, op: LeafOp::Lte, value: hi },
                    ],
                }
            }
            Some(Tok::In) => {
                self.next();
                self.eat(&Tok::LParen)?;
                if matches!(self.peek(), Some(Tok::Select)) {
                    // subquery: IN (SELECT proj FROM table [WHERE expr])
                    self.next(); // SELECT
                    let project = self.parse_ident()?;
                    self.eat(&Tok::From)?;
                    let table = self.parse_ident()?;
                    let where_ = if matches!(self.peek(), Some(Tok::Where)) {
                        self.next();
                        Some(Box::new(self.parse_or()?))
                    } else {
                        None
                    };
                    self.eat(&Tok::RParen)?;
                    // `negated` is carried by the subquery leaf itself; return early (skip the outer Not wrap).
                    return Ok(PredicateJson::In {
                        col,
                        subquery: SubqueryJson { table, project, where_ },
                        negated,
                    });
                } else {
                    // literal list: IN ('a','b',...) -> OR(=a,=b,...)
                    let mut vals = vec![self.parse_literal()?];
                    while matches!(self.peek(), Some(Tok::Comma)) {
                        self.next();
                        vals.push(self.parse_literal()?);
                    }
                    self.eat(&Tok::RParen)?;
                    PredicateJson::Or {
                        or: vals
                            .into_iter()
                            .map(|v| PredicateJson::Leaf { col: col.clone(), op: LeafOp::Eq, value: v })
                            .collect(),
                    }
                }
            }
            other => bail!("expected operator/LIKE/BETWEEN/IN/IS after column '{col}', got {other:?}"),
        };
        Ok(if negated { PredicateJson::Not { not: Box::new(pred) } } else { pred })
    }

}

/// Parse a lexed numeric literal into a JSON number. The lexer guarantees shape, but range/finiteness
/// can still fail (`1e999`) — that's an error, never a silent fall-back to a string literal.
fn num_literal(n: &str) -> Result<serde_json::Value> {
    if !n.contains(['.', 'e', 'E']) {
        if let Ok(i) = n.parse::<i64>() {
            return Ok(serde_json::Value::Number(i.into()));
        }
    }
    let f: f64 = n.parse().map_err(|_| anyhow::anyhow!("invalid numeric literal '{n}'"))?;
    match serde_json::Number::from_f64(f) {
        Some(num) => Ok(serde_json::Value::Number(num)),
        None => bail!("numeric literal '{n}' is out of range"),
    }
}

/// Evaluate a constant `<lit> <op> <lit>` comparison (numbers compared numerically, strings/bools by
/// value/order). Used for trivial predicates like `373 = 373`.
fn eval_const(left: &serde_json::Value, op: &str, right: &serde_json::Value) -> bool {
    use serde_json::Value as V;
    let ord = match (left, right) {
        (V::Number(a), V::Number(b)) => a.as_f64().partial_cmp(&b.as_f64()),
        (V::String(a), V::String(b)) => Some(a.cmp(b)),
        (V::Bool(a), V::Bool(b)) => Some(a.cmp(b)),
        _ => None,
    };
    match op {
        "=" => left == right,
        "<>" | "!=" => left != right,
        "<" => ord.map(|o| o.is_lt()).unwrap_or(false),
        "<=" => ord.map(|o| o.is_le()).unwrap_or(false),
        ">" => ord.map(|o| o.is_gt()).unwrap_or(false),
        ">=" => ord.map(|o| o.is_ge()).unwrap_or(false),
        _ => false,
    }
}

/// Parse a SQL `where` clause string into a [`PredicateJson`]. `TRUE`/empty → no predicate (`None`).
pub fn parse_where(sql: &str) -> Result<Option<PredicateJson>> {
    parse_where_typed(sql, None)
}

/// [`parse_where`] with the table schema available: additionally validates column existence for
/// `IS [NOT] NULL` leaves at parse time. The Electric adapter uses this.
pub fn parse_where_typed(sql: &str, schema: Option<&TableSchema>) -> Result<Option<PredicateJson>> {
    let trimmed = sql.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("true") {
        return Ok(None);
    }
    let toks = tokenize(trimmed)?;
    let mut p = Parser { toks, pos: 0, schema };
    let pred = p.parse_or()?;
    if p.pos != p.toks.len() {
        bail!("trailing tokens after where clause at position {}: {:?}", p.pos, p.toks.get(p.pos));
    }
    Ok(Some(pred))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sig(sql: &str) -> String {
        let p = parse_where(sql).unwrap().unwrap();
        crate::predicate::canonical_pred(&p)
    }

    #[test]
    fn parses_comparisons_and_bools() {
        assert!(parse_where("TRUE").unwrap().is_none());
        let _ = sig("active = true");
        let _ = sig("value <> 'x'");
        let _ = sig("value >= 'a' AND value <= 'z'");
    }

    #[test]
    fn parses_like_between_in_and_subqueries() {
        let _ = sig("value LIKE 'a%'");
        let _ = sig("value NOT LIKE '_b'");
        let _ = sig("value BETWEEN 'a' AND 'm'");
        let _ = sig("value NOT BETWEEN 'a' AND 'm'");
        let _ = sig("level_3_id IN ('l3-1', 'l3-2')");
        let _ = sig("id NOT IN ('l4-1')");
        let _ = sig("level_3_id IN (SELECT id FROM level_3 WHERE active = true)");
        let _ = sig("level_3_id NOT IN (SELECT id FROM level_3 WHERE active = true)");
        let _ = sig(
            "level_3_id IN (SELECT id FROM level_3 WHERE level_2_id IN (SELECT id FROM level_2 WHERE active = true))",
        );
        let _ = sig("(active = true OR value = 'x') AND NOT active = false");
    }

    fn users() -> TableSchema {
        let json = serde_json::json!({
            "columns": { "id": {"type":"int"}, "name": {"type":"text"}, "score": {"type":"float"}, "active": {"type":"bool"} },
            "primaryKey": "id"
        });
        let def: crate::schema::TableDef = serde_json::from_value(json).unwrap();
        TableSchema::from_def("users", &def).unwrap()
    }

    fn lit(sql: &str) -> serde_json::Value {
        match parse_where(sql).unwrap().unwrap() {
            PredicateJson::Leaf { value, .. } => value,
            other => panic!("expected a leaf, got {other:?}"),
        }
    }

    #[test]
    fn number_lexing_accepts_floats_and_scientific_notation() {
        assert_eq!(lit("id = 42"), serde_json::json!(42));
        assert_eq!(lit("id = -7"), serde_json::json!(-7));
        assert_eq!(lit("score = 1.5"), serde_json::json!(1.5));
        assert_eq!(lit("score = .5"), serde_json::json!(0.5));
        assert_eq!(lit("score = -.5"), serde_json::json!(-0.5));
        assert_eq!(lit("score = 1e5"), serde_json::json!(100000.0));
        assert_eq!(lit("score = 1.5e-3"), serde_json::json!(0.0015));
        assert_eq!(lit("score = 2E2"), serde_json::json!(200.0));
    }

    #[test]
    fn malformed_numbers_are_parse_errors_not_strings() {
        // Every one of these used to silently become a STRING literal.
        assert!(parse_where("score = 1.2.3").is_err());
        assert!(parse_where("score = 1e").is_err());
        assert!(parse_where("score = 1.5ex").is_err());
        assert!(parse_where("id = 10x").is_err());
        assert!(parse_where("score = 1e999").is_err()); // non-finite
    }

    #[test]
    fn quoted_identifiers_escape_and_terminate() {
        // "" escapes a quote inside a quoted identifier
        let p = parse_where(r#""my ""col""" = 'x'"#).unwrap().unwrap();
        match p {
            PredicateJson::Leaf { col, .. } => assert_eq!(col, "my \"col\""),
            other => panic!("expected a leaf, got {other:?}"),
        }
        // unterminated quoted identifier is a parse error, not a silent consume-to-EOF
        assert!(parse_where(r#""oops = 'x'"#).is_err());
    }

    #[test]
    fn is_null_and_is_not_null_parse_to_native_leaves() {
        let ts = users();
        let leaf = |sql: &str| parse_where_typed(sql, Some(&ts)).unwrap().unwrap();
        match leaf("name IS NULL") {
            PredicateJson::IsNull { col, is_null } => {
                assert_eq!(col, "name");
                assert!(is_null);
            }
            other => panic!("expected IsNull, got {other:?}"),
        }
        match leaf("name IS NOT NULL") {
            PredicateJson::IsNull { col, is_null } => {
                assert_eq!(col, "name");
                assert!(!is_null);
            }
            other => panic!("expected IsNull, got {other:?}"),
        }
        // composes under NOT (the native leaf makes negation sound)
        match leaf("NOT (name IS NOT NULL)") {
            PredicateJson::Not { not } => assert!(matches!(*not, PredicateJson::IsNull { is_null: false, .. })),
            other => panic!("expected Not(IsNull), got {other:?}"),
        }
        assert!(parse_where_typed("name IS NOT NULL AND active = true", Some(&ts)).is_ok());
        // works without a schema too (validation happens at compile time instead)
        assert!(parse_where("name IS NULL").is_ok());
        // unknown column is a parse error when the schema is available
        assert!(parse_where_typed("nope IS NOT NULL", Some(&ts)).is_err());
    }

    #[test]
    fn like_match_basics() {
        assert!(crate::predicate::like_match("abc", "a%"));
        assert!(crate::predicate::like_match("abc", "_bc"));
        assert!(!crate::predicate::like_match("abc", "a_d"));
        assert!(crate::predicate::like_match("abc", "%"));
        assert!(crate::predicate::like_match("abc", "abc"));
        assert!(!crate::predicate::like_match("abcd", "abc"));
    }
}
