//! `params` support for `GET /v1/shape`: Electric-compatible `$N` placeholder substitution.
//!
//! The benchmarking-fleet's subquery load generators send shapes like
//! `where=project_id IN (SELECT id FROM projects WHERE owner_id = $1)` together with `params` binding
//! `$1`. We resolve params into `{"N" => value}` and substitute each `$N` in the where clause with the
//! value as a single-quoted SQL string literal **before** the normal where-clause parse — so the
//! existing predicate / subquery machinery sees an ordinary where clause, and the shape's identity
//! (its signature) is derived from the *substituted* text (two different param values → two different
//! shapes, never a collision).
//!
//! Wire forms accepted (matching Electric's `Api.Params` cast):
//!  - bracket: `params[1]=a&params[2]=b` — the Elixir client's `:query` format (what the fleet sends)
//!  - JSON:    `params={"1":"a","2":"b"}`
//!
//! Validation mirrors Electric's `Validators.validate_parameters`: keys must be integers, numbered
//! sequentially from 1. A referenced `$N` with no bound value is a 400 (as Electric's parser reports).

use std::collections::HashMap;

/// Resolve the `params` map from the raw (decoded) query pairs. `Err(msg)` → the caller returns
/// `400 {"message": msg}`.
pub fn parse_params(pairs: &[(String, String)]) -> Result<HashMap<String, String>, String> {
    let mut map: HashMap<String, String> = HashMap::new();

    // Bracket form (Elixir `:query`) takes precedence; keys look like `params[1]`.
    for (k, v) in pairs {
        if let Some(inner) = k.strip_prefix("params[").and_then(|s| s.strip_suffix(']')) {
            map.insert(inner.to_string(), v.clone());
        }
    }

    // Otherwise a single `params=<json object>` value.
    if map.is_empty() {
        if let Some((_, json)) = pairs.iter().find(|(k, _)| k == "params") {
            let decoded: serde_json::Value =
                serde_json::from_str(json).map_err(|_| "params must be valid JSON".to_string())?;
            let obj = decoded.as_object().ok_or_else(|| "params must be a JSON object".to_string())?;
            for (k, v) in obj {
                map.insert(k.clone(), json_to_param_string(v));
            }
        }
    }

    validate_param_keys(&map)?;
    Ok(map)
}

/// Electric stringifies every param value (`to_string(v)`); a JSON number/bool becomes its text form.
fn json_to_param_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Electric's `validate_parameters`: all keys integers, numbered sequentially from 1.
fn validate_param_keys(map: &HashMap<String, String>) -> Result<(), String> {
    if map.is_empty() {
        return Ok(());
    }
    let mut nums: Vec<i64> = Vec::with_capacity(map.len());
    for k in map.keys() {
        match k.parse::<i64>() {
            Ok(n) => nums.push(n),
            Err(_) => return Err("Parameters can only use numbers as keys".to_string()),
        }
    }
    nums.sort_unstable();
    for (i, n) in nums.iter().enumerate() {
        if *n != (i as i64) + 1 {
            return Err("Parameters must be numbered sequentially, starting from 1".to_string());
        }
    }
    Ok(())
}

/// Substitute every `$N` in `where_` with `params["N"]` as a single-quoted SQL string literal
/// (`'` doubled). Quote-aware: a `$N` inside an existing `'…'` literal is left untouched. `$` binds the
/// **longest** following digit run, so `$10` is param 10, not `$1` then `0`. A referenced `$N` with no
/// bound value → `Err("parameter $N was not provided")` (400). Values are emitted as bound-safe string
/// literals, so a quote in a value cannot break out (no SQL injection; the value later reaches the
/// backfill query as a bound parameter, never as raw SQL).
pub fn substitute(where_: &str, params: &HashMap<String, String>) -> Result<String, String> {
    let chars: Vec<char> = where_.chars().collect();
    let mut out = String::with_capacity(where_.len() + 16);
    let mut i = 0;
    let mut in_quote = false;
    while i < chars.len() {
        let c = chars[i];
        if in_quote {
            out.push(c);
            if c == '\'' {
                // A doubled '' is an escaped quote and stays inside the literal.
                if i + 1 < chars.len() && chars[i + 1] == '\'' {
                    out.push('\'');
                    i += 2;
                    continue;
                }
                in_quote = false;
            }
            i += 1;
        } else if c == '\'' {
            in_quote = true;
            out.push(c);
            i += 1;
        } else if c == '$' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit() {
            let mut j = i + 1;
            while j < chars.len() && chars[j].is_ascii_digit() {
                j += 1;
            }
            let num: String = chars[i + 1..j].iter().collect();
            match params.get(&num) {
                Some(v) => {
                    out.push('\'');
                    for ch in v.chars() {
                        if ch == '\'' {
                            out.push('\'');
                        }
                        out.push(ch);
                    }
                    out.push('\'');
                }
                None => return Err(format!("parameter ${num} was not provided")),
            }
            i = j;
        } else {
            out.push(c);
            i += 1;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(kvs: &[(&str, &str)]) -> HashMap<String, String> {
        kvs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }
    fn pairs(kvs: &[(&str, &str)]) -> Vec<(String, String)> {
        kvs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn substitutes_exact() {
        assert_eq!(substitute("owner_id = $1", &m(&[("1", "u1")])).unwrap(), "owner_id = 'u1'");
        // a realistic uuid value
        assert_eq!(
            substitute("owner_id = $1", &m(&[("1", "3d5e0f2a-1b2c-4d5e-8f90-abcdef012345")])).unwrap(),
            "owner_id = '3d5e0f2a-1b2c-4d5e-8f90-abcdef012345'"
        );
    }

    #[test]
    fn one_param_used_multiple_times() {
        // The fleet's real clause: $1 appears twice.
        let w = "author_id = $1 OR project_id IN (SELECT id FROM projects WHERE owner_id = $1)";
        let got = substitute(w, &m(&[("1", "abc")])).unwrap();
        assert_eq!(got, "author_id = 'abc' OR project_id IN (SELECT id FROM projects WHERE owner_id = 'abc')");
    }

    #[test]
    fn multiple_distinct_params() {
        let w = "issue_id IN (SELECT id FROM issues WHERE project_id = $1) OR author_id = $2";
        let got = substitute(w, &m(&[("1", "p"), ("2", "a")])).unwrap();
        assert_eq!(got, "issue_id IN (SELECT id FROM issues WHERE project_id = 'p') OR author_id = 'a'");
    }

    #[test]
    fn ten_vs_one_disambiguation() {
        // $10 must bind param 10, not param 1 followed by a literal 0.
        let got = substitute("a = $1 AND b = $10", &m(&[("1", "x"), ("10", "y")])).unwrap();
        assert_eq!(got, "a = 'x' AND b = 'y'");
        // and a params map with only 1..10 present resolves $10 correctly
        assert!(substitute("b = $10", &m(&[("10", "z")])).unwrap().ends_with("'z'"));
    }

    #[test]
    fn escapes_single_quotes_in_value() {
        // Injection attempt: a quote in the value is doubled, staying a single string literal.
        let got = substitute("name = $1", &m(&[("1", "x' OR '1'='1")])).unwrap();
        assert_eq!(got, "name = 'x'' OR ''1''=''1'");
    }

    #[test]
    fn dollar_inside_string_literal_is_left_alone() {
        // $1 inside an existing quoted literal is literal text, not a placeholder.
        let got = substitute("note = 'cost is $1 total' AND owner_id = $1", &m(&[("1", "u")])).unwrap();
        assert_eq!(got, "note = 'cost is $1 total' AND owner_id = 'u'");
    }

    #[test]
    fn missing_param_is_error() {
        let err = substitute("owner_id = $1 AND x = $2", &m(&[("1", "u")])).unwrap_err();
        assert_eq!(err, "parameter $2 was not provided");
    }

    #[test]
    fn no_placeholders_returns_unchanged() {
        assert_eq!(substitute("active = true", &m(&[])).unwrap(), "active = true");
    }

    #[test]
    fn parse_bracket_form() {
        let p = parse_params(&pairs(&[("table", "issues"), ("params[1]", "u1"), ("params[2]", "u2")])).unwrap();
        assert_eq!(p.get("1").map(String::as_str), Some("u1"));
        assert_eq!(p.get("2").map(String::as_str), Some("u2"));
        assert_eq!(p.len(), 2);
    }

    #[test]
    fn parse_json_form() {
        let p = parse_params(&pairs(&[("params", r#"{"1":"u1","2":"u2"}"#)])).unwrap();
        assert_eq!(p.get("1").map(String::as_str), Some("u1"));
        assert_eq!(p.get("2").map(String::as_str), Some("u2"));
    }

    #[test]
    fn parse_json_stringifies_non_string_values() {
        let p = parse_params(&pairs(&[("params", r#"{"1":5,"2":true}"#)])).unwrap();
        assert_eq!(p.get("1").map(String::as_str), Some("5"));
        assert_eq!(p.get("2").map(String::as_str), Some("true"));
    }

    #[test]
    fn parse_empty_is_ok() {
        assert!(parse_params(&pairs(&[("table", "issues")])).unwrap().is_empty());
    }

    #[test]
    fn rejects_non_numeric_keys() {
        let err = parse_params(&pairs(&[("params[a]", "x")])).unwrap_err();
        assert_eq!(err, "Parameters can only use numbers as keys");
        let err = parse_params(&pairs(&[("params", r#"{"a":"x"}"#)])).unwrap_err();
        assert_eq!(err, "Parameters can only use numbers as keys");
    }

    #[test]
    fn rejects_non_sequential_keys() {
        let err = parse_params(&pairs(&[("params[1]", "x"), ("params[3]", "y")])).unwrap_err();
        assert_eq!(err, "Parameters must be numbered sequentially, starting from 1");
        // must start from 1
        let err = parse_params(&pairs(&[("params[2]", "y")])).unwrap_err();
        assert_eq!(err, "Parameters must be numbered sequentially, starting from 1");
    }

    #[test]
    fn rejects_invalid_json() {
        assert_eq!(parse_params(&pairs(&[("params", "{not json")])).unwrap_err(), "params must be valid JSON");
        assert_eq!(parse_params(&pairs(&[("params", "[1,2]")])).unwrap_err(), "params must be a JSON object");
    }

    #[test]
    fn extra_sequential_params_are_allowed_and_ignored_by_substitution() {
        // params {1,2} but where only uses $1: keys valid (sequential), $2 simply unused.
        let params = parse_params(&pairs(&[("params[1]", "a"), ("params[2]", "b")])).unwrap();
        assert_eq!(substitute("owner_id = $1", &params).unwrap(), "owner_id = 'a'");
    }
}
