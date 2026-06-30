//! Control-plane HTTP API (the swappable interface in front of the engine).

use axum::extract::{Path, State};
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
        .route("/shapes/{id}", get(get_shape).delete(drop_shape))
        .route("/query", post(query_subset))
        .route("/tables/{name}/offset", get(table_offset))
        .route("/tables/{name}/families", get(table_families))
        .route("/subqueries", get(subquery_stats))
        .route("/replication/lsn", get(replication_lsn))
        .route("/metrics", get(get_metrics))
        .route("/metrics/reset", post(reset_metrics))
        .route("/memory", get(get_memory))
        .route("/metrics/prometheus", get(get_prometheus))
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
    let rec = engine.create_shape(&req.table, req.where_, req.columns, req.changes_only).await?;
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
