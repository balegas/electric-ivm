//! Control-plane HTTP API (the swappable interface in front of the engine).

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::engine::{Engine, ShapeRecord, TableSchemaInfo, TableStats};
use crate::predicate::PredicateJson;
use crate::schema::Schema;

pub fn router(engine: Engine) -> Router {
    router_with_introspection(engine, true)
}

/// `introspection = false` (`ELECTRIC_CIRCUITS_TRACE=0`) leaves the visualizer/introspection surface
/// unregistered — `/trace` (SSE), `/graph`(`/node`), `/state`(`/node`) all 404. With no route there
/// can be no `/trace` subscriber, so the per-envelope trace instrumentation stays on its
/// zero-subscriber fast path (one atomic load). The surface is unauthenticated when enabled.
pub fn router_with_introspection(engine: Engine, introspection: bool) -> Router {
    let mut r = Router::new()
        // Fleet surface: root probe + health state machine (CORS preflight is on the /v1/shape route).
        .route("/", get(|| async { StatusCode::OK }))
        .route("/v1/health", get(health_v1))
        .route("/health", get(|| async { "ok" }))
        .route("/schema", post(define_schema))
        .route("/shapes", post(create_shape))
        .route("/aggregate", post(create_aggregate))
        .route("/shapes/{id}", get(get_shape).delete(release_shape))
        .route("/shapes/{id}/rows", get(get_shape_rows))
        .route("/shapes/{id}/log", get(get_shape_log))
        .route("/query", post(query_subset))
        .route("/tables/{name}/offset", get(table_offset))
        .route("/tables/{name}/families", get(table_families))
        // Table schema (columns + pk), a parameterized single-row INSERT (the visualizer's add-row
        // action), and a by-primary-key DELETE (its delete-rows action). Both writes go to Postgres
        // so the changes are captured by logical replication and flow through the pipeline like any
        // other write.
        .route("/table/{table}/schema", get(get_table_schema))
        .route("/table/{table}/rows", post(insert_table_row).delete(delete_table_rows))
        .route("/subqueries", get(subquery_stats))
        .route("/replication/lsn", get(replication_lsn))
        .route("/metrics", get(get_metrics))
        .route("/metrics/reset", post(reset_metrics))
        .route("/memory", get(get_memory))
        .route("/metrics/prometheus", get(get_prometheus))
        // Electric-protocol adapter: lets Electric's official client + oracle harness read our shapes.
        // OPTIONS is the CORS preflight the fleet's browser-style clients send.
        .route("/v1/shape", get(crate::electric::shape).options(shape_options));
    if introspection {
        r = r
            .route("/graph", get(get_graph))
            .route("/graph/node", get(get_node_index))
            .route("/state", get(get_state))
            .route("/state/node", get(get_state_node))
            // Per-envelope pipeline trace (SSE) — best-effort, for visualization/debugging.
            .route("/trace", get(get_trace))
            // On-demand dbsp profiler dump for every dbsp circuit (membership + counts).
            // Heavy — diagnostic/attribution use only; never sampled in the background.
            .route("/debug/dbsp-profile", get(get_dbsp_profile));
    }
    r.with_state(engine)
}

/// SSE stream of per-envelope [`crate::trace::TraceEvent`]s (one JSON object per `data:` line).
/// Lossy by design: a lagging subscriber silently skips the events it missed rather than slowing
/// envelope processing.
async fn get_trace(State(engine): State<Engine>) -> impl IntoResponse {
    use tokio_stream::StreamExt;
    let rx = engine.trace_sender().subscribe();
    let stream = tokio_stream::wrappers::BroadcastStream::new(rx).filter_map(|item| match item {
        Ok(json) => Some(Ok::<_, std::convert::Infallible>(
            axum::response::sse::Event::default().data(json.as_str()),
        )),
        // Lagged: drop the gap marker; the consumer treats trace as best-effort animation.
        Err(_) => None,
    });
    axum::response::sse::Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::default())
}

/// Exact `/v1/health` JSON body for a status — no whitespace (the fleet's healthcheck string-compares
/// the body against `{"status":"active"}`).
fn health_json(status: &str) -> String {
    format!("{{\"status\":\"{status}\"}}")
}

/// `GET /v1/health` — `waiting`/`starting` → 202, `active` → 200. Caches are disabled so the fleet's
/// 500ms poll always sees the live phase.
async fn health_v1(State(engine): State<Engine>) -> Response {
    let status = engine.health_status();
    let code = if status == "active" { StatusCode::OK } else { StatusCode::ACCEPTED };
    let mut headers = HeaderMap::new();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache, no-store, must-revalidate"));
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    (code, headers, health_json(status)).into_response()
}

/// `OPTIONS /v1/shape` — CORS preflight: 204 advertising the methods the adapter serves.
async fn shape_options() -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, POST, HEAD, DELETE, OPTIONS"),
    );
    (StatusCode::NO_CONTENT, headers).into_response()
}

#[derive(Deserialize)]
struct DefineSchemaReq {
    schema: Schema,
}

#[derive(Deserialize)]
struct SubsetOrderByReq {
    col: String,
    #[serde(default)]
    desc: bool,
}

/// A one-shot subset query (the non-materialized counterpart to `/shapes`).
#[derive(Deserialize)]
struct QueryReq {
    table: String,
    #[serde(default, rename = "where")]
    where_: Option<PredicateJson>,
    #[serde(default)]
    columns: Option<Vec<String>>,
    #[serde(default, rename = "orderBy")]
    order_by: Option<SubsetOrderByReq>,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    offset: Option<i64>,
}

#[derive(Serialize)]
struct QueryResp {
    rows: Vec<serde_json::Value>,
    lsn: String,
}

async fn query_subset(
    State(engine): State<Engine>,
    Json(req): Json<QueryReq>,
) -> Result<Json<QueryResp>, AppError> {
    let order_by = req.order_by.map(|o| (o.col, o.desc));
    let (rows, lsn) =
        engine.query_subset(&req.table, req.where_, req.columns, order_by, req.limit, req.offset).await?;
    Ok(Json(QueryResp { rows, lsn }))
}

async fn define_schema(
    State(engine): State<Engine>,
    Json(req): Json<DefineSchemaReq>,
) -> Result<Json<serde_json::Value>, AppError> {
    engine.define_schema(&req.schema).await?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

#[derive(Deserialize)]
struct CreateShapeReq {
    table: String,
    #[serde(default, rename = "where")]
    where_: Option<PredicateJson>,
    /// Optional output projection: column names to sync. Omitted = the full row.
    #[serde(default)]
    columns: Option<Vec<String>>,
    /// When true, skip the backfill and stream only future matching changes (a non-materialized live
    /// tail feed). Used by subset queries; a normal materialized shape leaves this false.
    #[serde(default, rename = "changesOnly")]
    changes_only: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ShapeResp {
    shape_id: String,
    table: String,
    stream_path: String,
    stream_url: String,
    /// Retention lifecycle: `active` | `deactivating` | `dormant` | `reactivating` (see
    /// `crate::retention`). Shapes handed out by create are always active.
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<&'static str>,
}

impl ShapeResp {
    fn of(engine: &Engine, rec: ShapeRecord) -> Self {
        let stream_url = engine.stream_url(&rec.stream_path);
        ShapeResp { shape_id: rec.id, table: rec.table, stream_path: rec.stream_path, stream_url, state: None }
    }
}

async fn create_shape(
    State(engine): State<Engine>,
    Json(req): Json<CreateShapeReq>,
) -> Result<Json<ShapeResp>, AppError> {
    // share = true: identical reference shapes from multiple clients collapse to one maintained stream.
    let rec = engine.create_shape(&req.table, req.where_, req.columns, req.changes_only, true).await?;
    Ok(Json(ShapeResp::of(&engine, rec)))
}

#[derive(Deserialize)]
struct AggregateReq {
    table: String,
    #[serde(default, rename = "where")]
    where_: Option<PredicateJson>,
    #[serde(rename = "fn")]
    func: crate::engine::AggFn,
    #[serde(default)]
    col: Option<String>,
}

/// Create a scalar aggregation shape (electric-circuits extension; not in the Electric protocol).
async fn create_aggregate(
    State(engine): State<Engine>,
    Json(req): Json<AggregateReq>,
) -> Result<Json<ShapeResp>, AppError> {
    let rec = engine.create_aggregate(&req.table, req.where_, req.func, req.col).await?;
    Ok(Json(ShapeResp::of(&engine, rec)))
}

async fn get_shape(
    State(engine): State<Engine>,
    Path(id): Path<String>,
) -> Result<Json<ShapeResp>, AppError> {
    match engine.get_shape(&id).await {
        Some(rec) => {
            let state = engine.shape_lifecycle(&rec.id).await;
            Ok(Json(ShapeResp { state, ..ShapeResp::of(&engine, rec) }))
        }
        None => Err(AppError { status: StatusCode::NOT_FOUND, msg: format!("shape {id} not found") }),
    }
}

#[derive(Deserialize)]
struct ShapeRowsQuery {
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Serialize)]
struct ShapeRowEntry {
    key: String,
    value: serde_json::Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ShapeRowsResp {
    id: String,
    table: String,
    changes_only: bool,
    /// Total materialized rows (before the display cap).
    count: usize,
    truncated: bool,
    rows: Vec<ShapeRowEntry>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ShapeLogEntry {
    op: String,
    key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<serde_json::Value>,
    /// Prior row on update/delete (REPLICA IDENTITY FULL) — lets a UI show what a delete removed.
    #[serde(skip_serializing_if = "Option::is_none")]
    old: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lsn: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ShapeLogResp {
    id: String,
    table: String,
    changes_only: bool,
    /// Total envelopes on the stream (before the tail cap).
    total: usize,
    /// Oldest → newest; capped to the tail (`limit`).
    entries: Vec<ShapeLogEntry>,
}

/// The change log of an **existing** shape: the tail of its stream as-is (insert/update/delete
/// envelopes, oldest → newest). Drives the visualizer's feed-shape "live log" view, which polls
/// this. Read-only — creates no shape.
async fn get_shape_log(
    State(engine): State<Engine>,
    Path(id): Path<String>,
    Query(q): Query<ShapeRowsQuery>,
) -> Result<Json<ShapeLogResp>, AppError> {
    let Some(rec) = engine.get_shape(&id).await else {
        return Err(AppError { status: StatusCode::NOT_FOUND, msg: format!("shape {id} not found") });
    };
    // A touch reactivates: if the shape is dormant, replay it live first so the log is current.
    engine.ensure_active(&id).await?;
    let limit = q.limit.unwrap_or(50).min(500);
    let mut entries: std::collections::VecDeque<ShapeLogEntry> = std::collections::VecDeque::new();
    let mut total = 0usize;
    // Walked over the WHOLE stream (not just the returned tail): which keys are live and their
    // last value. Lets the wire ops (upsert/delete) be reported as insert vs update exactly, and
    // gives a delete entry the row it removed.
    let mut live: std::collections::HashMap<String, Option<serde_json::Value>> = std::collections::HashMap::new();
    let mut offset = "-1".to_string();
    loop {
        let r = engine.read_shape_stream(&rec.stream_path, &offset, false).await?;
        let empty = r.envelopes.is_empty();
        for env in r.envelopes {
            total += 1;
            let (op, old) = if env.headers.operation == "delete" {
                let last = live.remove(&env.key).flatten();
                ("delete".to_string(), env.old.or(last))
            } else {
                let existed = live.insert(env.key.clone(), env.value.clone()).is_some();
                (if existed { "update".to_string() } else { "insert".to_string() }, env.old)
            };
            entries.push_back(ShapeLogEntry { op, key: env.key, value: env.value, old, lsn: env.headers.lsn });
            if entries.len() > limit {
                entries.pop_front();
            }
        }
        // Break when caught up, the page was empty, or the offset failed to advance (a defensive
        // guard against a non-empty page with a missing/unchanged next offset looping forever).
        let advanced = r.next_offset.as_deref().is_some_and(|n| n != offset);
        if let Some(n) = r.next_offset {
            offset = n;
        }
        if r.up_to_date || empty || !advanced {
            break;
        }
    }
    Ok(Json(ShapeLogResp {
        id: rec.id,
        table: rec.table,
        changes_only: rec.changes_only,
        total,
        entries: entries.into_iter().collect(),
    }))
}

/// The current contents of an **existing** shape, materialized by folding its stream — creates no new
/// shape (unlike `/v1/shape`). Drives the visualizer's live "contents" preview, which polls this.
async fn get_shape_rows(
    State(engine): State<Engine>,
    Path(id): Path<String>,
    Query(q): Query<ShapeRowsQuery>,
) -> Result<Json<ShapeRowsResp>, AppError> {
    let Some(rec) = engine.get_shape(&id).await else {
        return Err(AppError { status: StatusCode::NOT_FOUND, msg: format!("shape {id} not found") });
    };
    // A touch reactivates: if the shape is dormant, replay it live first so the fold is current.
    engine.ensure_active(&id).await?;
    // Fold the shape's whole stream (catch-up reads from -1) into the current key→row map.
    let mut rows: std::collections::HashMap<String, serde_json::Value> = std::collections::HashMap::new();
    let mut offset = "-1".to_string();
    loop {
        let r = engine.read_shape_stream(&rec.stream_path, &offset, false).await?;
        let empty = r.envelopes.is_empty();
        for env in r.envelopes {
            if env.headers.operation == "delete" {
                rows.remove(&env.key);
            } else if let Some(v) = env.value {
                rows.insert(env.key, v);
            }
        }
        // Same defensive break as get_shape_log: never spin on a non-advancing offset.
        let advanced = r.next_offset.as_deref().is_some_and(|n| n != offset);
        if let Some(n) = r.next_offset {
            offset = n;
        }
        if r.up_to_date || empty || !advanced {
            break;
        }
    }
    let count = rows.len();
    let limit = q.limit.unwrap_or(200).min(2000);
    let mut entries: Vec<ShapeRowEntry> =
        rows.into_iter().map(|(key, value)| ShapeRowEntry { key, value }).collect();
    // Deterministic order for a stable preview: by numeric key when possible, else lexicographic.
    entries.sort_by(|a, b| match (a.key.parse::<i64>(), b.key.parse::<i64>()) {
        (Ok(x), Ok(y)) => x.cmp(&y),
        _ => a.key.cmp(&b.key),
    });
    let truncated = entries.len() > limit;
    entries.truncate(limit);
    Ok(Json(ShapeRowsResp { id: rec.id, table: rec.table, changes_only: rec.changes_only, count, truncated, rows: entries }))
}

#[derive(Deserialize)]
struct ReleaseShapeQuery {
    /// `?purge=true` force-drops the shape NOW (full teardown, stream deleted), bypassing the
    /// retention lifecycle — an admin/debug operation (the visualizer's trash button).
    #[serde(default)]
    purge: bool,
}

/// `DELETE /shapes/{id}` = unsubscribe. Releases one subscription (refcount); the shape itself is
/// retained and follows the retention lifecycle (idle → dormant → evicted) — see `crate::retention`.
/// With `?purge=true` it instead force-drops the shape immediately (subscribed clients recreate via
/// the normal 404 / must-refetch path).
async fn release_shape(
    State(engine): State<Engine>,
    Path(id): Path<String>,
    Query(q): Query<ReleaseShapeQuery>,
) -> Result<Json<serde_json::Value>, AppError> {
    if q.purge {
        engine.purge_shape(&id).await?;
    } else {
        engine.release_shape(&id).await;
    }
    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn table_offset(
    State(engine): State<Engine>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    match engine.table_offset(&name).await {
        Some(offset) => Ok(Json(serde_json::json!({ "offset": offset }))),
        None => Err(AppError { status: StatusCode::NOT_FOUND, msg: format!("no tailer for table {name}") }),
    }
}

async fn table_families(
    State(engine): State<Engine>,
    Path(name): Path<String>,
) -> Result<Json<TableStats>, AppError> {
    match engine.table_stats(&name).await {
        Some(stats) => Ok(Json(stats)),
        None => Err(AppError { status: StatusCode::NOT_FOUND, msg: format!("no tailer for table {name}") }),
    }
}

/// `GET /table/{table}/schema` — the table's columns (+ coarse/native types, pk flag) and primary key,
/// so the visualizer can render one input per column in its add-row form.
async fn get_table_schema(
    State(engine): State<Engine>,
    Path(table): Path<String>,
) -> Result<Json<TableSchemaInfo>, AppError> {
    match engine.table_schema_info(&table).await {
        Ok(info) => Ok(Json(info)),
        Err(e) => Err(AppError { status: StatusCode::NOT_FOUND, msg: format!("{e:#}") }),
    }
}

/// Body of `POST /table/{table}/rows`: the new row as `column → value`, under either `columns` or
/// `values`. Omitted columns take their Postgres default / NULL.
#[derive(Deserialize)]
struct InsertRowReq {
    #[serde(default)]
    columns: Option<serde_json::Map<String, serde_json::Value>>,
    #[serde(default)]
    values: Option<serde_json::Map<String, serde_json::Value>>,
}

/// `POST /table/{table}/rows` — insert one row into the table's Postgres relation (parameterized,
/// identifier-quoted). The write is captured by logical replication and flows through the pipeline, so
/// the visualizer sees the change animate. Bad input (unknown column, type mismatch) is a 400.
async fn insert_table_row(
    State(engine): State<Engine>,
    Path(table): Path<String>,
    Json(req): Json<InsertRowReq>,
) -> Result<Json<serde_json::Value>, AppError> {
    let values = req.columns.or(req.values).unwrap_or_default();
    match engine.insert_row(&table, &values).await {
        Ok(v) => Ok(Json(v)),
        Err(e) => Err(AppError { status: StatusCode::BAD_REQUEST, msg: format!("{e:#}") }),
    }
}

/// Body of `DELETE /table/{table}/rows`: the primary keys of the rows to delete — one
/// `pk column → value` object per row.
#[derive(Deserialize)]
struct DeleteRowsReq {
    keys: Vec<serde_json::Map<String, serde_json::Value>>,
}

/// `DELETE /table/{table}/rows` — delete rows from the table's Postgres relation by primary key
/// (parameterized, identifier-quoted; all keys in one statement). Like the insert, the deletes are
/// captured by logical replication and flow through the pipeline, so the visualizer sees them
/// animate. Bad input (unknown table, non-pk column, missing/NULL key part) is a 400.
async fn delete_table_rows(
    State(engine): State<Engine>,
    Path(table): Path<String>,
    Json(req): Json<DeleteRowsReq>,
) -> Result<Json<serde_json::Value>, AppError> {
    match engine.delete_rows(&table, &req.keys).await {
        Ok(v) => Ok(Json(v)),
        Err(e) => Err(AppError { status: StatusCode::BAD_REQUEST, msg: format!("{e:#}") }),
    }
}

/// Full pipeline graph for the visualizer (`GET /graph`): tables, shapes with routing placement, and
/// the shared subquery node/edge DAG. Adds no cost to the hot path — reads in-memory topology only.
async fn get_graph(State(engine): State<Engine>) -> Json<crate::engine::EngineGraph> {
    Json(engine.graph().await)
}

#[derive(Deserialize)]
struct NodeIndexQuery {
    sig: String,
    #[serde(default)]
    cap: Option<usize>,
}

/// The live inner-set index of one subquery node (`GET /graph/node?sig=…`) — values + contributor
/// counts, for the visualizer's node-detail "index" view.
async fn get_node_index(
    State(engine): State<Engine>,
    Query(q): Query<NodeIndexQuery>,
) -> Result<Json<crate::engine::NodeIndex>, AppError> {
    match engine.node_index(&q.sig, q.cap.unwrap_or(500)).await {
        Some(idx) => Ok(Json(idx)),
        None => Err(AppError { status: StatusCode::NOT_FOUND, msg: format!("node {} not found", q.sig) }),
    }
}

/// Full per-node state snapshot (`GET /state`): the live summary of every pipeline node, keyed by
/// graph node id. The visualizer seeds from this, then applies the incremental `{"type":"state"}`
/// events pushed on `/trace`.
async fn get_state(State(engine): State<Engine>) -> Json<crate::engine::StateSnapshot> {
    Json(engine.state_snapshot().await)
}

#[derive(Deserialize)]
struct StateNodeQuery {
    id: String,
}

/// Deep state dump of one node (`GET /state/node?id=<node-id>`): a family router's routing-index
/// contents, an aggregate's fold internals, or a subquery node's inner-set index.
async fn get_state_node(
    State(engine): State<Engine>,
    Query(q): Query<StateNodeQuery>,
) -> Result<Json<serde_json::Value>, AppError> {
    match engine.dump_node(&q.id).await {
        Some(v) => Ok(Json(v)),
        None => Err(AppError { status: StatusCode::NOT_FOUND, msg: format!("node {} not found", q.id) }),
    }
}

async fn subquery_stats(State(engine): State<Engine>) -> Json<serde_json::Value> {
    let nodes = engine.subquery_stats().await;
    Json(serde_json::json!({ "nodes": nodes }))
}

async fn replication_lsn(State(engine): State<Engine>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "lsn": engine.replication_lsn(),
        "sync": engine.replication_sync(),
        // Deferred subquery flip batches not yet propagated. Convergence barrier = sync caught up
        // + per-table offsets at tail + pendingFlips == 0.
        "pendingFlips": engine.pending_flips(),
    }))
}

async fn get_metrics() -> Json<serde_json::Value> {
    Json(crate::metrics::metrics().snapshot())
}

async fn reset_metrics() -> Json<serde_json::Value> {
    crate::metrics::metrics().reset();
    Json(serde_json::json!({ "ok": true }))
}

/// JSON memory snapshot — process RSS/virtual + engine cardinalities. Recomputes cardinalities fresh so
/// the harness reads the exact state right after creating a batch of shapes (and republishes the OTel
/// gauges in the same pass).
///
/// This is the ONLY call site for `Engine::mem_bytes` — the expensive `heap_bytes`/`MemBytes` byte
/// walk (Phase 0 self-accounting) runs here, on demand, never on the 500ms background sampler (see
/// `mem::spawn_sampler` / `Engine::mem_cardinalities`'s doc comments).
async fn get_memory(State(engine): State<Engine>) -> Json<serde_json::Value> {
    let card = engine.mem_cardinalities().await.with_bytes(engine.mem_bytes().await);
    crate::mem::publish(&card);
    Json(crate::mem::snapshot_json(&card))
}

/// Diagnostic: dbsp profiler dump for every dbsp circuit the engine runs (see
/// `Engine::dbsp_profile_dump`). Heavy — on-demand only, introspection-gated.
async fn get_dbsp_profile(State(engine): State<Engine>) -> Json<serde_json::Value> {
    Json(engine.dbsp_profile_dump().await)
}

/// OpenTelemetry metrics in Prometheus exposition format (what an OTel collector's prometheus receiver
/// scrapes). Reflects the last published sample (refreshed by the background sampler + every `/memory`).
async fn get_prometheus() -> Response {
    (
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        crate::mem::prometheus_text(),
    )
        .into_response()
}

struct AppError {
    status: StatusCode,
    msg: String,
}

impl From<anyhow::Error> for AppError {
    fn from(e: anyhow::Error) -> Self {
        AppError { status: StatusCode::INTERNAL_SERVER_ERROR, msg: format!("{e:#}") }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (self.status, Json(serde_json::json!({ "error": self.msg }))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::health_json;

    // The fleet's healthcheck does an awk string-compare against the exact body, so byte-for-byte
    // exactness (no whitespace) matters more than JSON equivalence.
    #[test]
    fn health_body_is_exact() {
        assert_eq!(health_json("waiting"), r#"{"status":"waiting"}"#);
        assert_eq!(health_json("starting"), r#"{"status":"starting"}"#);
        assert_eq!(health_json("active"), r#"{"status":"active"}"#);
    }
}
