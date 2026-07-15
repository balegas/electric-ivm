//! Postgres access for the Postgres-backed mode: connection, schema introspection, replication-slot
//! setup, and consistent backfill snapshots. This replaces the engine's in-memory `table_state` —
//! current data lives in Postgres and is read back on demand (shape backfill), while ongoing changes
//! arrive via logical replication (see `replication.rs`).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};

use anyhow::{Context, Result, bail};
use tokio_postgres::{Client, NoTls};

use crate::heap_size::HeapSize;
use crate::predicate::CompiledPredicate;
use crate::schema::{ColumnDef, ColumnType, TableDef, TableSchema};
use crate::value::Row;

/// Connect and drive the connection on a background task. Returns the query `Client`.
/// For per-request work (backfills, query-backs, subset queries) prefer [`pool_for`] — a fresh
/// TCP+auth handshake per shape creation is the fleet benchmark's p99 driver, and thousands of
/// concurrent creations exhaust ephemeral ports.
pub async fn connect(url: &str) -> Result<Client> {
    let (client, conn) = tokio_postgres::connect(url, NoTls).await.context("connect postgres")?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::error!("postgres connection error: {e}");
        }
    });
    Ok(client)
}

/// Maximum connections per [`Pool`], set once at boot from `ELECTRIC_DB_POOL_SIZE` (default 20).
static POOL_SIZE: OnceLock<usize> = OnceLock::new();

/// One shared pool per distinct URL for the process lifetime.
static POOLS: OnceLock<std::sync::Mutex<HashMap<String, Pool>>> = OnceLock::new();

/// Set the per-URL pool capacity. Call once at boot, before the first [`pool_for`].
pub fn set_pool_size(size: usize) {
    let _ = POOL_SIZE.set(size.max(1));
}

/// The shared connection pool for `url` (created on first use).
pub fn pool_for(url: &str) -> Pool {
    let pools = POOLS.get_or_init(Default::default);
    let mut pools = pools.lock().unwrap();
    pools
        .entry(url.to_string())
        .or_insert_with(|| Pool::new(url.to_string(), *POOL_SIZE.get_or_init(|| 20)))
        .clone()
}

/// A small connection pool: at most `size` concurrent checkouts, idle connections reused.
/// Backfills/query-backs are self-contained `BEGIN … COMMIT` units with no session state, so
/// checkin only has to clear a possibly-aborted transaction (`ROLLBACK`, a no-op warning on a
/// clean session) before the connection is reusable.
#[derive(Clone)]
pub struct Pool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    url: String,
    idle: std::sync::Mutex<Vec<Client>>,
    sem: Arc<tokio::sync::Semaphore>,
}

impl Pool {
    pub fn new(url: String, size: usize) -> Pool {
        Pool {
            inner: Arc::new(PoolInner {
                url,
                idle: std::sync::Mutex::new(Vec::new()),
                sem: Arc::new(tokio::sync::Semaphore::new(size.max(1))),
            }),
        }
    }

    /// Check out a connection; waits if all `size` are in use. The checkout is returned to the
    /// pool (or discarded, if broken) when the guard drops.
    pub async fn get(&self) -> Result<PooledClient> {
        let permit =
            self.inner.sem.clone().acquire_owned().await.context("pg pool closed")?;
        // Reuse an idle connection if it is still healthy; otherwise dial a new one.
        let reused = self.inner.idle.lock().unwrap().pop().filter(|c| !c.is_closed());
        let client = match reused {
            Some(c) => c,
            None => connect(&self.inner.url).await?,
        };
        Ok(PooledClient { client: Some(client), inner: self.inner.clone(), permit: Some(permit) })
    }
}

/// A pooled connection checkout. Derefs to `tokio_postgres::Client`.
pub struct PooledClient {
    client: Option<Client>,
    inner: Arc<PoolInner>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl std::ops::Deref for PooledClient {
    type Target = Client;
    fn deref(&self) -> &Client {
        self.client.as_ref().expect("client present until drop")
    }
}

impl Drop for PooledClient {
    fn drop(&mut self) {
        let Some(client) = self.client.take() else { return };
        let permit = self.permit.take();
        if client.is_closed() {
            return; // permit drops here, freeing the slot
        }
        let inner = self.inner.clone();
        // Clear any transaction the caller left open/aborted, then check the connection back in.
        // The permit is held until the connection is actually idle again, so live connections
        // never exceed the pool size.
        tokio::spawn(async move {
            if client.batch_execute("ROLLBACK").await.is_ok() {
                inner.idle.lock().unwrap().push(client);
            }
            drop(permit);
        });
    }
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

/// List all base tables in the `public` schema that have a primary key (skipping the engine's own
/// `__el_sync` bookkeeping table). Used by "introspect all" mode (`ELECTRIC_IVM_PG_TABLES=*`), where the
/// set of tables isn't known up front (e.g. driving Electric's integration tests over varied schemas).
pub async fn list_tables(client: &Client) -> Result<Vec<String>> {
    let rows = client
        .query(
            "select t.table_name from information_schema.tables t \
             where t.table_schema = 'public' and t.table_type = 'BASE TABLE' \
               and t.table_name <> '__el_sync' \
               and exists (select 1 from pg_index i where i.indrelid = to_regclass('public.'||t.table_name) and i.indisprimary) \
             order by t.table_name",
            &[],
        )
        .await
        .context("list public tables")?;
    Ok(rows.iter().map(|r| r.get(0)).collect())
}

/// Introspect a table's columns (+ types) and single-column primary key from the catalog.
pub async fn introspect(client: &Client, table: &str) -> Result<TableDef> {
    let col_rows = client
        .query(
            "select column_name, data_type, udt_name, \
                    (is_identity = 'YES' or column_default is not null) as has_default \
             from information_schema.columns \
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
        // udt_name (pg_type.typname, e.g. `uuid`, `int4`, `timestamptz`) is the canonical, always-castable
        // type name — used to cast bound text params to the native type in backfill SQL.
        let udt: String = r.get(2);
        // Auto-defaulted (IDENTITY or DEFAULT) → the add-row form can treat it as optional.
        let has_default: bool = r.get(3);
        columns.insert(name, ColumnDef { ty: map_pg_type(&dt), pg_type: Some(udt), has_default });
    }

    // Composite primary keys are supported (e.g. Electric's `*_tags` tables); columns are ordered by
    // their position in the index key so the synthesized row key is deterministic.
    let pk_rows = client
        .query(
            "select a.attname from pg_index i \
             join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey) \
             where i.indrelid = to_regclass($1) and i.indisprimary \
             order by array_position(i.indkey, a.attnum)",
            &[&table],
        )
        .await
        .context("introspect primary key")?;
    if pk_rows.is_empty() {
        bail!("table '{table}' must have a primary key");
    }
    let primary_key: Vec<String> = pk_rows.iter().map(|r| r.get(0)).collect();
    Ok(TableDef { columns, primary_key })
}

/// `ALTER TABLE … REPLICA IDENTITY FULL` so logical decoding carries the full old row.
pub async fn ensure_replica_identity_full(client: &Client, table: &str) -> Result<()> {
    client
        .batch_execute(&format!("ALTER TABLE {} REPLICA IDENTITY FULL", quote_ident(table)))
        .await
        .with_context(|| format!("set REPLICA IDENTITY FULL on {table}"))
}

/// Create the logical replication slot (`pgoutput`) if it does not exist. A leftover slot with a
/// different output plugin (e.g. `test_decoding` from an earlier engine version) is dropped and
/// recreated — the plugin cannot be changed in place.
pub async fn ensure_slot(client: &Client, slot: &str) -> Result<()> {
    let existing = client
        .query("select plugin from pg_replication_slots where slot_name = $1", &[&slot])
        .await
        .context("check slot")?;
    if let Some(row) = existing.first() {
        let plugin: String = row.get(0);
        if plugin == "pgoutput" {
            return Ok(());
        }
        tracing::warn!("slot '{slot}' uses plugin '{plugin}'; dropping and recreating with pgoutput");
        client
            .execute("select pg_drop_replication_slot($1)", &[&slot])
            .await
            .context("drop stale slot")?;
    }
    client
        .execute("select pg_create_logical_replication_slot($1, 'pgoutput')", &[&slot])
        .await
        .context("create slot")?;
    Ok(())
}

/// Create the publication the pgoutput stream filters on, if it does not exist. `FOR ALL TABLES`
/// (requires superuser): the ingestor drops changes for untracked relations itself, and the set of
/// tracked tables can grow via introspect-all restarts without publication surgery.
pub async fn ensure_publication(client: &Client, publication: &str) -> Result<()> {
    let exists = client
        .query("select 1 from pg_publication where pubname = $1", &[&publication])
        .await
        .context("check publication")?;
    if exists.is_empty() {
        client
            .execute(&format!("create publication {} for all tables", quote_ident(publication)), &[])
            .await
            .context("create publication")?;
    }
    Ok(())
}

pub struct Backfill {
    pub rows: Vec<Row>,
    /// `pg_current_wal_lsn()` of the snapshot. A transaction visible to this REPEATABLE READ snapshot
    /// committed strictly before it, so its commit LSN is `< seed_lsn` and its changes are already in
    /// `rows`; a transaction committing at/after the snapshot has commit LSN `>= seed_lsn`.
    pub seed_lsn: String,
    /// The snapshot's transaction-visibility gate — the *sound* backfill↔replication fence. See
    /// [`SnapshotGate`] for why LSN comparison alone is not.
    pub gate: SnapshotGate,
}

/// The backfill snapshot's visibility fence for replicated changes.
///
/// **Why not LSN alone:** `pg_current_wal_lsn()` is a WAL *write* position, but snapshot visibility is
/// decided at `ProcArrayEndTransaction` — which happens *after* the commit record is written and
/// fsynced. A transaction whose commit record is already in the WAL (commit LSN `< seed_lsn`) can
/// still be **invisible** to a snapshot taken during that window; skipping its replicated change by
/// LSN would drop the row from both the backfill and the live stream, permanently. Conversely a
/// visible commit can sit exactly *at* the boundary (`end_lsn == seed_lsn`) and be replayed as a
/// duplicate. Transaction-id visibility (`pg_current_snapshot()`) decides both cases exactly:
/// **skip a replicated change iff its xid was visible to the backfill snapshot.** (Every xid seen on
/// the slot is committed, so visibility reduces to: `xid < xmin`, or `xmin <= xid < xmax` and not
/// in-progress at snapshot time.)
///
/// The stored xids are `pg_current_snapshot()`'s xid8 values masked to 32 bits so they compare
/// against test_decoding's 32-bit xids; the fence spans seconds around a backfill, so epoch
/// wraparound (a ~4-billion-transaction horizon) cannot straddle it in practice.
///
/// When a change carries no parseable xid (library mode / non-PG sources), the gate falls back to
/// the LSN comparison, and with neither it never skips.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct SnapshotGate {
    /// `pg_current_wal_lsn()` at the snapshot (numeric) — the fallback fence + subset positioning.
    pub lsn: u64,
    xmin: u64,
    xmax: u64,
    xip: std::collections::HashSet<u64>,
}

impl HeapSize for SnapshotGate {
    /// Only `xip` (the in-progress xid set) owns heap; `lsn`/`xmin`/`xmax` are inline `u64`s.
    fn heap_bytes(&self) -> usize {
        self.xip.heap_bytes()
    }
}

impl SnapshotGate {
    /// A gate that never skips — for `changes_only` feeds (no backfill ⇒ forward everything) and
    /// library mode (no Postgres snapshot exists).
    pub fn passthrough() -> Self {
        SnapshotGate::default()
    }

    /// Build from `pg_current_snapshot()::text` ("xmin:xmax:xip1,xip2,…") + the snapshot LSN.
    pub fn parse(snapshot: &str, lsn: &str) -> Self {
        let mask = |v: u64| v & 0xFFFF_FFFF;
        let mut parts = snapshot.split(':');
        let xmin = parts.next().and_then(|s| s.trim().parse::<u64>().ok()).map(mask).unwrap_or(0);
        let xmax = parts.next().and_then(|s| s.trim().parse::<u64>().ok()).map(mask).unwrap_or(0);
        let xip = parts
            .next()
            .map(|s| s.split(',').filter_map(|x| x.trim().parse::<u64>().ok()).map(mask).collect())
            .unwrap_or_default();
        SnapshotGate { lsn: lsn_to_u64(lsn), xmin, xmax, xip }
    }

    /// Was committed transaction `xid` visible to this snapshot (i.e. already reflected in the
    /// backfill rows)?
    fn visible(&self, xid: u64) -> bool {
        if self.xmax == 0 {
            return false; // passthrough gate: nothing is "already seeded"
        }
        if xid < self.xmin {
            return true;
        }
        if xid >= self.xmax {
            return false;
        }
        !self.xip.contains(&xid)
    }

    /// Should a replicated change (commit LSN + optional xid) be skipped because the backfill
    /// snapshot already reflects it?
    pub fn should_skip(&self, commit_lsn: u64, xid: Option<u64>) -> bool {
        match xid {
            Some(x) => self.visible(x),
            None => commit_lsn != 0 && self.lsn != 0 && commit_lsn < self.lsn,
        }
    }
}

/// Read the table's current rows in a single repeatable-read snapshot, plus the snapshot LSN. The
/// engine seeds a shape/family from `rows` and skips replication changes whose COMMIT LSN is strictly
/// `< seed_lsn` (see `engine::process_envelope`; the comparison is against the transaction commit LSN
/// stamped by the ingestor, not the per-change record LSN).
/// Uses an explicit transaction over `&Client` (so it needs a dedicated connection, not a shared one).
///
/// `filter`, when given, is the shape's predicate: backfill reads only the matching rows
/// (`… WHERE <predicate>`) instead of the whole table, so a selective shape never scans/transfers the
/// rest. `None` reads the whole table (used while a family still seeds a full-table trace).
pub async fn backfill(client: &Client, ts: &TableSchema, filter: Option<&CompiledPredicate>) -> Result<Backfill> {
    client
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
        .context("begin backfill snapshot")?;
    let result = backfill_in_txn(client, ts, filter).await;
    client.batch_execute("COMMIT").await.ok();
    result
}

async fn backfill_in_txn(client: &Client, ts: &TableSchema, filter: Option<&CompiledPredicate>) -> Result<Backfill> {
    // Push the shape's predicate into the SELECT so only matching rows are read. Text literals are
    // bound parameters; numeric/bool/null are inlined (see `crate::sql`). The engine still applies
    // `matches()` afterward, so the SQL only needs to be a sound superset filter.
    let where_sql = filter.and_then(|p| crate::sql::predicate_to_sql(p, ts));
    backfill_where_in_txn(client, ts, where_sql).await
}

/// Like [`backfill`], but with a **prebuilt** `WHERE` fragment + params (from the JSON SQL emitter) —
/// used for subquery shapes/nodes, whose `IN (SELECT …)` SQL the compiled-predicate emitter can't
/// reconstruct. `where_sql = None` reads the whole table.
pub async fn backfill_where(
    client: &Client,
    ts: &TableSchema,
    where_sql: Option<(String, Vec<String>)>,
) -> Result<Backfill> {
    client
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
        .context("begin backfill snapshot")?;
    let result = backfill_where_in_txn(client, ts, where_sql).await;
    client.batch_execute("COMMIT").await.ok();
    result
}

async fn backfill_where_in_txn(
    client: &Client,
    ts: &TableSchema,
    where_sql: Option<(String, Vec<String>)>,
) -> Result<Backfill> {
    // One statement establishes the snapshot AND captures both fences (LSN + xid snapshot)
    // atomically with it.
    let fence = client
        .query_one("select pg_current_wal_lsn()::text, pg_current_snapshot()::text", &[])
        .await?;
    let seed_lsn: String = fence.get(0);
    let snap: String = fence.get(1);
    let gate = SnapshotGate::parse(&snap, &seed_lsn);
    let (where_clause, params) = match where_sql {
        Some((w, ps)) => (format!(" where {w}"), ps),
        None => (String::new(), Vec::new()),
    };
    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        params.iter().map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
    let q = format!("select {} from {} t{}", row_json_expr(ts), quote_ident(&ts.name), where_clause);
    let rows = client.query(&q, &param_refs).await.with_context(|| format!("backfill select {}", ts.name))?;
    let mut out = Vec::with_capacity(rows.len());
    for r in &rows {
        let j: serde_json::Value = r.get(0);
        let obj = j.as_object().context("backfill row expr did not return an object")?;
        out.push(ts.row_from_json(obj)?);
    }
    Ok(Backfill { rows: out, seed_lsn, gate })
}

/// Group-count seed for a counts pipeline: `SELECT <group cols>, count(*) … GROUP BY` under a
/// `REPEATABLE READ` snapshot — O(distinct groups) rather than O(rows) — with the same
/// visibility fences as a row backfill. Returned rows are full-width with only the group
/// columns populated (the counts pipeline projects exactly those positions); text-mapped
/// columns are cast `::text` for live-path byte identity.
pub async fn backfill_group_counts(
    client: &Client,
    ts: &TableSchema,
    group_cols: &[usize],
) -> Result<(Vec<(Row, i64)>, SnapshotGate)> {
    client
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
        .context("begin counts seed snapshot")?;
    let result = group_counts_in_txn(client, ts, group_cols).await;
    client.batch_execute("COMMIT").await.ok();
    result
}

async fn group_counts_in_txn(
    client: &Client,
    ts: &TableSchema,
    group_cols: &[usize],
) -> Result<(Vec<(Row, i64)>, SnapshotGate)> {
    let fence = client
        .query_one("select pg_current_wal_lsn()::text, pg_current_snapshot()::text", &[])
        .await?;
    let seed_lsn: String = fence.get(0);
    let snap: String = fence.get(1);
    let gate = SnapshotGate::parse(&snap, &seed_lsn);
    let mut args = Vec::new();
    let mut by = Vec::new();
    for &i in group_cols {
        let (name, ty) = ts.columns.get(i).with_context(|| format!("group col {i} out of range"))?;
        let lit = format!("'{}'", name.replace('\'', "''"));
        let qi = quote_ident(name);
        match ty {
            ColumnType::Text => args.push(format!("{lit}, t.{qi}::text")),
            _ => args.push(format!("{lit}, to_jsonb(t.{qi})")),
        }
        by.push(format!("t.{qi}"));
    }
    let q = format!(
        "select jsonb_build_object({}), count(*)::bigint from {} t group by {}",
        args.join(", "),
        quote_ident(&ts.name),
        by.join(", ")
    );
    let rows = client.query(&q, &[]).await.with_context(|| format!("counts seed select {}", ts.name))?;
    let mut out = Vec::with_capacity(rows.len());
    for r in &rows {
        let j: serde_json::Value = r.get(0);
        let obj = j.as_object().context("counts seed expr did not return an object")?;
        // Missing (non-group) columns default to Null — the pipeline projects only group cols.
        out.push((ts.row_from_json(obj)?, r.get::<_, i64>(1)));
    }
    Ok((out, gate))
}

/// The per-row JSON projection used by backfill and subset query-backs. Text-mapped columns are cast
/// with `::text` so the value is Postgres's *text output* — the same representation `test_decoding`
/// prints on the live path. `to_jsonb(t)` would instead give e.g. ISO-8601 `T`-form timestamps and
/// raw jsonb objects, so the same cell would compare unequal between a backfilled row and its first
/// replicated update (breaking retractions, equality routing, and MIN/MAX multisets). Int/Float/Bool
/// columns keep `to_jsonb` (native JSON scalars, matching the live parser). Chunked into multiple
/// `jsonb_build_object` calls `||`-concatenated to stay under the 100-argument limit.
fn row_json_expr(ts: &TableSchema) -> String {
    let mut objs: Vec<String> = Vec::new();
    for chunk in ts.columns.chunks(40) {
        let args: Vec<String> = chunk
            .iter()
            .map(|(name, ty)| {
                let lit = format!("'{}'", name.replace('\'', "''"));
                let qi = quote_ident(name);
                match ty {
                    ColumnType::Text => format!("{lit}, t.{qi}::text"),
                    _ => format!("{lit}, to_jsonb(t.{qi})"),
                }
            })
            .collect();
        objs.push(format!("jsonb_build_object({})", args.join(", ")));
    }
    objs.join(" || ")
}

/// Result of a one-shot subset query: the page rows + the snapshot LSN they were read at.
pub struct SubsetQuery {
    pub rows: Vec<Row>,
    pub lsn: String,
}

/// Run a **non-materialized** subset query: a single `SELECT … WHERE … ORDER BY … LIMIT … OFFSET …`
/// against Postgres in a `REPEATABLE READ` snapshot, returning the page rows and the snapshot LSN.
/// Unlike [`backfill`], this creates no shape and no durable stream — it is the ephemeral query-back a
/// subset/pagination view uses (the live tail is followed separately). `order` is `(column index,
/// descending?)`; the pk is appended as a tiebreaker so the window is total/stable.
pub async fn query_subset(
    client: &Client,
    ts: &TableSchema,
    filter: Option<&CompiledPredicate>,
    order: Option<(usize, bool)>,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<SubsetQuery> {
    query_subset_where(client, ts, filter.and_then(|p| crate::sql::predicate_to_sql(p, ts)), order, limit, offset).await
}

/// Like [`query_subset`], but with a **prebuilt** `WHERE` fragment + params — used when the predicate
/// contains an `IN (SELECT …)` subquery (the JSON SQL emitter builds it; Postgres evaluates it natively,
/// so paginated subquery lists work without engine-side subquery state).
pub async fn query_subset_where(
    client: &Client,
    ts: &TableSchema,
    where_sql: Option<(String, Vec<String>)>,
    order: Option<(usize, bool)>,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<SubsetQuery> {
    client
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
        .context("begin subset snapshot")?;
    let result = query_subset_in_txn(client, ts, where_sql, order, limit, offset).await;
    client.batch_execute("COMMIT").await.ok();
    result
}

async fn query_subset_in_txn(
    client: &Client,
    ts: &TableSchema,
    where_sql: Option<(String, Vec<String>)>,
    order: Option<(usize, bool)>,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<SubsetQuery> {
    let lsn: String = client.query_one("select pg_current_wal_lsn()::text", &[]).await?.get(0);
    let (where_clause, params) = match where_sql {
        Some((w, ps)) => (format!(" where {w}"), ps),
        None => (String::new(), Vec::new()),
    };
    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        params.iter().map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
    // ORDER BY <col> <dir>, <pk> <dir> for a total order; a limit/offset without an explicit order
    // falls back to pk order so the page is deterministic. Idents are quoted; limit/offset are
    // non-negative integer literals — no injection surface.
    let order_sql = match order {
        Some((col, desc)) => {
            let d = if desc { "desc" } else { "asc" };
            format!(" order by {} {d}, {} {d}", quote_ident(&ts.columns[col].0), quote_ident(&ts.pk_name))
        }
        None if limit.is_some() || offset.is_some() => format!(" order by {} asc", quote_ident(&ts.pk_name)),
        None => String::new(),
    };
    let limit_sql = limit.map(|n| format!(" limit {}", n.max(0))).unwrap_or_default();
    let offset_sql = offset.map(|n| format!(" offset {}", n.max(0))).unwrap_or_default();
    let q = format!(
        "select {} from {} t{}{}{}{}",
        row_json_expr(ts),
        quote_ident(&ts.name),
        where_clause,
        order_sql,
        limit_sql,
        offset_sql
    );
    let rows = client.query(&q, &param_refs).await.with_context(|| format!("subset select {}", ts.name))?;
    let mut out = Vec::with_capacity(rows.len());
    for r in &rows {
        let j: serde_json::Value = r.get(0);
        let obj = j.as_object().context("subset row expr did not return an object")?;
        out.push(ts.row_from_json(obj)?);
    }
    Ok(SubsetQuery { rows: out, lsn })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The snapshot gate must skip exactly the transactions the backfill snapshot could see:
    /// committed-before (`xid < xmin`) → skip; in-progress at snapshot (`xip`) → process; started
    /// after (`xid >= xmax`) → process — regardless of WAL position. This is the fence that closes
    /// the "commit record written but not yet visible" window an LSN comparison cannot express.
    #[test]
    fn snapshot_gate_visibility() {
        // snapshot: xmin 100, xmax 110, in-progress {103, 107}; snapshot LSN 0/100.
        let g = SnapshotGate::parse("100:110:103,107", "0/100");
        // committed before the snapshot -> already in the backfill -> skip
        assert!(g.should_skip(0, Some(99)));
        // between xmin and xmax, not in-progress -> visible -> skip (even if its commit LSN were AT
        // or ABOVE the snapshot LSN, the boundary-duplicate case)
        assert!(g.should_skip(0x200, Some(105)));
        // in-progress at snapshot time -> INVISIBLE to the backfill -> must be processed, even though
        // its commit LSN may be below the snapshot LSN (the dropped-row race the LSN rule had)
        assert!(!g.should_skip(0x50, Some(103)));
        assert!(!g.should_skip(0x50, Some(107)));
        // started after the snapshot -> process
        assert!(!g.should_skip(0x200, Some(110)));
        assert!(!g.should_skip(0x200, Some(200)));
        // no xid -> LSN fallback (strict <)
        assert!(g.should_skip(0x50, None));
        assert!(!g.should_skip(0x100, None));
        // passthrough gate never skips
        let p = SnapshotGate::passthrough();
        assert!(!p.should_skip(0x50, Some(99)));
        assert!(!p.should_skip(0x50, None));
    }

    #[test]
    fn lsn_parse_roundtrip() {
        assert_eq!(lsn_to_u64("0/1A2B3C"), 0x1A2B3C);
        assert_eq!(lsn_to_u64("2/10"), (2u64 << 32) | 0x10);
        assert_eq!(lsn_to_u64("garbage"), 0);
    }
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
