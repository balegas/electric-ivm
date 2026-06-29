//! Postgres access for the Postgres-backed mode: connection, schema introspection, replication-slot
//! setup, and consistent backfill snapshots. This replaces the engine's in-memory `table_state` —
//! current data lives in Postgres and is read back on demand (shape backfill), while ongoing changes
//! arrive via logical replication (see `replication.rs`).

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use tokio_postgres::{Client, NoTls};

use crate::schema::{ColumnDef, ColumnType, TableDef, TableSchema};
use crate::value::Row;

/// Connect and drive the connection on a background task. Returns the query `Client`.
pub async fn connect(url: &str) -> Result<Client> {
    let (client, conn) = tokio_postgres::connect(url, NoTls).await.context("connect postgres")?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::error!("postgres connection error: {e}");
        }
    });
    Ok(client)
}

/// Double-quote a Postgres identifier.
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Map a Postgres `data_type` (from information_schema) to our column type.
fn map_pg_type(data_type: &str) -> ColumnType {
    match data_type {
        "integer" | "bigint" | "smallint" => ColumnType::Int,
        "boolean" => ColumnType::Bool,
        "real" | "double precision" | "numeric" => ColumnType::Float,
        _ => ColumnType::Text, // text, varchar, char, uuid, timestamptz, ... -> treated as text
    }
}

/// Introspect a table's columns (+ types) and single-column primary key from the catalog.
pub async fn introspect(client: &Client, table: &str) -> Result<TableDef> {
    let col_rows = client
        .query(
            "select column_name, data_type from information_schema.columns \
             where table_schema = 'public' and table_name = $1 order by ordinal_position",
            &[&table],
        )
        .await
        .context("introspect columns")?;
    if col_rows.is_empty() {
        bail!("table '{table}' not found in postgres (schema public)");
    }
    let mut columns = BTreeMap::new();
    for r in &col_rows {
        let name: String = r.get(0);
        let dt: String = r.get(1);
        columns.insert(name, ColumnDef { ty: map_pg_type(&dt) });
    }

    let pk_rows = client
        .query(
            "select a.attname from pg_index i \
             join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey) \
             where i.indrelid = to_regclass($1) and i.indisprimary",
            &[&table],
        )
        .await
        .context("introspect primary key")?;
    if pk_rows.len() != 1 {
        bail!("table '{table}' must have a single-column primary key (found {})", pk_rows.len());
    }
    let primary_key: String = pk_rows[0].get(0);
    Ok(TableDef { columns, primary_key })
}

/// `ALTER TABLE … REPLICA IDENTITY FULL` so logical decoding carries the full old row.
pub async fn ensure_replica_identity_full(client: &Client, table: &str) -> Result<()> {
    client
        .batch_execute(&format!("ALTER TABLE {} REPLICA IDENTITY FULL", quote_ident(table)))
        .await
        .with_context(|| format!("set REPLICA IDENTITY FULL on {table}"))
}

/// Create the logical replication slot (test_decoding) if it does not exist.
pub async fn ensure_slot(client: &Client, slot: &str) -> Result<()> {
    let exists = client
        .query("select 1 from pg_replication_slots where slot_name = $1", &[&slot])
        .await
        .context("check slot")?;
    if exists.is_empty() {
        client
            .execute("select pg_create_logical_replication_slot($1, 'test_decoding')", &[&slot])
            .await
            .context("create slot")?;
    }
    Ok(())
}

pub struct Backfill {
    pub rows: Vec<Row>,
    /// `pg_current_wal_lsn()` of the snapshot. A transaction visible to this REPEATABLE READ snapshot
    /// committed strictly before it, so its commit LSN is `< seed_lsn` and its changes are already in
    /// `rows`; a transaction committing at/after the snapshot has commit LSN `>= seed_lsn`.
    pub seed_lsn: String,
}

/// Read the table's current rows in a single repeatable-read snapshot, plus the snapshot LSN. The
/// engine seeds a shape/family from `rows` and skips replication changes whose COMMIT LSN is strictly
/// `< seed_lsn` (see `engine::process_envelope`; the comparison is against the transaction commit LSN
/// stamped by the ingestor, not the per-change record LSN).
/// Uses an explicit transaction over `&Client` (so it needs a dedicated connection, not a shared one).
pub async fn backfill(client: &Client, ts: &TableSchema) -> Result<Backfill> {
    client
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
        .context("begin backfill snapshot")?;
    let result = backfill_in_txn(client, ts).await;
    client.batch_execute("COMMIT").await.ok();
    result
}

async fn backfill_in_txn(client: &Client, ts: &TableSchema) -> Result<Backfill> {
    let seed_lsn: String = client.query_one("select pg_current_wal_lsn()::text", &[]).await?.get(0);
    // to_jsonb gives one JSON object per row, so we reuse the schema's JSON->Row mapping verbatim.
    let q = format!("select to_jsonb(t) from {} t", quote_ident(&ts.name));
    let rows = client.query(&q, &[]).await.with_context(|| format!("backfill select {}", ts.name))?;
    let mut out = Vec::with_capacity(rows.len());
    for r in &rows {
        let j: serde_json::Value = r.get(0);
        let obj = j.as_object().context("to_jsonb did not return an object")?;
        out.push(ts.row_from_json(obj)?);
    }
    Ok(Backfill { rows: out, seed_lsn })
}

/// Parse a Postgres LSN ("X/Y", hex) into a comparable u64. Returns 0 on parse failure.
pub fn lsn_to_u64(lsn: &str) -> u64 {
    match lsn.split_once('/') {
        Some((hi, lo)) => {
            let hi = u64::from_str_radix(hi.trim(), 16).unwrap_or(0);
            let lo = u64::from_str_radix(lo.trim(), 16).unwrap_or(0);
            (hi << 32) | lo
        }
        None => 0,
    }
}
