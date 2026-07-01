//! Control-plane HTTP API (the swappable interface in front of the engine).

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::engine::{Engine, ShapeRecord, TableStats};
use crate::predicate::PredicateJson;
use crate::schema::Schema;

pub fn router(engine: Engine) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/schema", post(define_schema))
        .route("/shapes", post(create_shape))
        .route("/aggregate", post(create_aggregate))
        .route("/shapes/{id}", get(get_shape).delete(drop_shape))
        .route("/shapes/{id}/rows", get(get_shape_rows))
        .route("/query", post(query_subset))
        .route("/tables/{name}/offset", get(table_offset))
        .route("/tables/{name}/families", get(table_families))
        .route("/subqueries", get(subquery_stats))
        .route("/graph", get(get_graph))
        .route("/graph/node", get(get_node_index))
        .route("/replication/lsn", get(replication_lsn))
        .route("/metrics", get(get_metrics))
        .route("/metrics/reset", post(reset_metrics))
        .route("/memory", get(get_memory))
        .route("/metrics/prometheus", get(get_prometheus))
        // Electric-protocol adapter: lets Electric's official client + oracle harness read our shapes.
        .route("/v1/shape", get(crate::electric::shape))
        .with_state(engine)
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
}

impl ShapeResp {
    fn of(engine: &Engine, rec: ShapeRecord) -> Self {
        let stream_url = engine.stream_url(&rec.stream_path);
        ShapeResp { shape_id: rec.id, table: rec.table, stream_path: rec.stream_path, stream_url }
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

/// Create a scalar aggregation shape (electric-lite extension; not in the Electric protocol).
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
        Some(rec) => Ok(Json(ShapeResp::of(&engine, rec))),
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
        if let Some(n) = r.next_offset {
            offset = n;
        }
        if r.up_to_date || empty {
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

async fn drop_shape(
    State(engine): State<Engine>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    engine.drop_shape(&id).await?;
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

async fn subquery_stats(State(engine): State<Engine>) -> Json<serde_json::Value> {
    let nodes = engine.subquery_stats().await;
    Json(serde_json::json!({ "nodes": nodes }))
}

async fn replication_lsn(State(engine): State<Engine>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "lsn": engine.replication_lsn(), "sync": engine.replication_sync() }))
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
async fn get_memory(State(engine): State<Engine>) -> Json<serde_json::Value> {
    let card = engine.mem_cardinalities().await;
    crate::mem::publish(&card);
    Json(crate::mem::snapshot_json())
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
