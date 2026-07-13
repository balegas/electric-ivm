//! The shared membership kernel — the ONE implementation of the mechanics that both
//! membership-serving paths must agree on. Two subsystems maintain "rows enter/leave a shape
//! because a *related* table changed": the subquery registry (`crate::subquery` — nested,
//! negated, multi-subquery predicates) and circuit cohort serving (`circuit_serving` —
//! single-level non-negated membership on arrangement-indexed columns). They deliberately keep
//! different membership *state* (identity-reconciled contributor pk-sets vs. exactly-once
//! refcounted groups), but the surrounding mechanics are identical and live here so they cannot
//! drift:
//!
//!  * **flip detection** over a refcounted group map ([`fold_refcount_flips`]);
//!  * **candidate-row resolution** with arrangement-snapshot → pooled-Postgres fallback
//!    ([`query_rows_by_col`], [`query_rows_all`]) — the fallback is what makes a missed or
//!    unseeded arrangement lookup a slow path instead of a silent move-miss;
//!  * the **latest-row-per-pk fold** used before an absolute membership evaluation
//!    ([`latest_rows_by_pk`]).
//!
//! Emission itself is already shared: both paths hand their decided `(Row, ±1)` sets to
//! [`super::output::translate_output`], whose per-pk grouping (net positive → `upsert`, else
//! `delete`) *is* the absolute-emission invariant (see `docs/ARCHITECTURE.md` §6).

use super::*;

use crate::subquery::{Flip, FlipDir};

/// Fold a batch of weighted group contributions into a refcounted group map and report the
/// values whose membership **flipped** (refcount crossed zero). `Enter` = the value's refcount
/// went `≤0 → >0`, `Leave` = `>0 → ≤0`; values whose refcount changes without crossing zero
/// produce no flip. Callers must feed exactly-once deltas (the sequencer's `(lsn, seq)`
/// highwater guarantees this) — refcounts, unlike the registry's pk-sets, are not idempotent.
pub(crate) fn fold_refcount_flips(
    groups: &mut HashMap<Value, i64>,
    contributions: impl IntoIterator<Item = (Value, ZWeight)>,
) -> Vec<Flip> {
    let mut flips = Vec::new();
    for (v, w) in contributions {
        let e = groups.entry(v.clone()).or_insert(0);
        let was = *e;
        *e += w;
        let now = *e;
        if now <= 0 {
            groups.remove(&v);
        }
        if was <= 0 && now > 0 {
            flips.push(Flip { value: v, dir: FlipDir::Enter });
        } else if was > 0 && now <= 0 {
            flips.push(Flip { value: v, dir: FlipDir::Leave });
        }
    }
    flips
}

/// Query candidate rows where `col = value`: from the dbsp arrangement snapshot when a
/// `(table, [col])` index exists and is seeded (no I/O beyond dbsp's own storage), else from
/// Postgres on a pooled connection. Never silently empty: if neither source is available this
/// is an error, not a skipped move.
pub(crate) async fn query_rows_by_col(
    arr: &Option<crate::arrangements::Arrangements>,
    pg_url: &Option<String>,
    ts: &TableSchema,
    col: usize,
    value: &Value,
) -> Result<Vec<Row>> {
    if let Some(a) = arr {
        if let Some(rows) = a.lookup(&ts.name, &[col], &Row(vec![value.clone()])) {
            return Ok(rows);
        }
    }
    let url = pg_url.as_deref().context("membership query-back requires postgres")?;
    let client = crate::pg::pool_for(url).get().await?;
    let where_sql =
        value_eq_sql(&ts.columns[col].0, value, ts.pg_types.get(col).and_then(|o| o.as_deref()));
    Ok(crate::pg::backfill_where(&client, ts, Some(where_sql)).await?.rows)
}

/// Query all rows of `ts` (full re-derive): from the dbsp arrangement scan when available, else
/// from Postgres on a pooled connection.
pub(crate) async fn query_rows_all(
    arr: &Option<crate::arrangements::Arrangements>,
    pg_url: &Option<String>,
    ts: &TableSchema,
) -> Result<Vec<Row>> {
    if let Some(a) = arr {
        if let Some(rows) = a.scan(&ts.name) {
            return Ok(rows);
        }
    }
    let url = pg_url.as_deref().context("membership query-back requires postgres")?;
    let client = crate::pg::pool_for(url).get().await?;
    Ok(crate::pg::backfill_where(&client, ts, None).await?.rows)
}

/// Build a `WHERE col = value` fragment + params for a move query-back (the LIVE re-derive path).
/// Text is parameterized; other scalars are inlined (mirrors the SQL emitter). A text param is cast to
/// the column's native Postgres type (`$1::text::uuid`) when known — same as the backfill emitters, so
/// a uuid/timestamptz column doesn't hit tokio-postgres's `String -> uuid` refusal (which used to fail
/// this path SILENTLY, dropping live subquery move-ins) — else the `col::text = $1` fallback. NULL
/// never reaches here (handled by full re-derive).
pub(crate) fn value_eq_sql(col: &str, value: &Value, pg_type: Option<&str>) -> (String, Vec<String>) {
    let name = crate::pg::quote_ident(col);
    match value {
        Value::Null => (format!("{name} IS NULL"), Vec::new()),
        Value::Int(i) => (format!("{name} = {i}"), Vec::new()),
        Value::Float(f) => (format!("{name} = {}", f.0), Vec::new()),
        Value::Bool(b) => (format!("{name} = {}", if *b { "true" } else { "false" }), Vec::new()),
        Value::Text(s) => (crate::sql::text_param_cmp(&name, "=", 1, pg_type), vec![s.clone()]),
    }
}

/// Fold a Z-set delta down to each touched pk's **latest** state: the `+1` row if present
/// (insert/update — the row still exists), else the `-1` row (delete). The `bool` is
/// "row still exists". This is the front half of every absolute membership evaluation: decide
/// per touched pk from its latest row, never from history.
pub(crate) fn latest_rows_by_pk(
    ts: &TableSchema,
    delta: &[Tup2<Row, ZWeight>],
) -> Vec<(Row, bool)> {
    let mut by_pk: HashMap<String, (Row, bool)> = HashMap::new();
    for Tup2(row, w) in delta {
        let pk = ts.key_string(row).unwrap_or_default();
        if *w > 0 {
            by_pk.insert(pk, (row.clone(), true));
        } else {
            by_pk.entry(pk).or_insert_with(|| (row.clone(), false));
        }
    }
    by_pk.into_values().collect()
}
