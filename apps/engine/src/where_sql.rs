//! Parse Electric's SQL `where` clause (sent as a query param on `GET /v1/shape`) into our
//! [`PredicateJson`] AST. Covers the grammar Electric's oracle generator emits (and the hand-written
//! integration where-clauses): `col <op> lit` (`= <> != < <= > >=`), `col [NOT] LIKE 'pat'`,
//! `col [NOT] BETWEEN a AND b`, `col [NOT] IN ('a', …)`, `col [NOT] IN (SELECT proj FROM t [WHERE …])`
//! (recursive), `AND`/`OR`/`NOT`, parentheses, and `bool`/`true`/`false` literals.
//!
//! Desugaring keeps the engine's op set small: `BETWEEN a b` → `AND(>=a, <=b)`, `IN (list)` →
//! `OR(=v…)`, and every `NOT <x>` → `Not(<x>)` except `NOT IN (SELECT …)` which sets the subquery's
//! `negated` flag (so it shares the `In` node machinery).

use anyhow::{Result, bail};

use crate::predicate::{LeafOp, PredicateJson, SubqueryJson};

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
            // double-quoted identifier
            i += 1;
            let mut buf = String::new();
            while i < chars.len() && chars[i] != '"' {
                buf.push(chars[i]);
                i += 1;
            }
            i += 1;
            out.push(Tok::Ident(buf));
        } else if c.is_ascii_digit() || (c == '-' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit()) {
            let mut buf = String::new();
            buf.push(c);
            i += 1;
            while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                buf.push(chars[i]);
                i += 1;
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
                _ => Tok::Ident(buf),
            });
        } else {
            bail!("unexpected character '{c}' in where clause");
        }
    }
    Ok(out)
}

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
}

impl Parser {
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
            return Ok(PredicateJson::Not { not: Box::new(self.parse_not()?) });
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
            Some(Tok::Num(n)) => Ok(serde_json::from_str(&n).unwrap_or(serde_json::Value::String(n))),
            other => bail!("expected literal, got {other:?}"),
        }
    }

    // predicate := col [NOT] (op lit | LIKE str | BETWEEN a AND b | IN (...))
    fn parse_predicate(&mut self) -> Result<PredicateJson> {
        let col = self.parse_ident()?;
        let negated = if matches!(self.peek(), Some(Tok::Not)) {
            self.next();
            true
        } else {
            false
        };
        let pred = match self.peek() {
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
            other => bail!("expected operator/LIKE/BETWEEN/IN after column '{col}', got {other:?}"),
        };
        Ok(if negated { PredicateJson::Not { not: Box::new(pred) } } else { pred })
    }
}

/// Parse a SQL `where` clause string into a [`PredicateJson`]. `TRUE`/empty → no predicate (`None`).
pub fn parse_where(sql: &str) -> Result<Option<PredicateJson>> {
    let trimmed = sql.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("true") {
        return Ok(None);
    }
    let toks = tokenize(trimmed)?;
    let mut p = Parser { toks, pos: 0 };
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
