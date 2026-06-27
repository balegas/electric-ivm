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
        .route("/tables/{name}/offset", get(table_offset))
        .route("/tables/{name}/families", get(table_families))
        .route("/metrics", get(get_metrics))
        .route("/metrics/reset", post(reset_metrics))
        .with_state(engine)
}

#[derive(Deserialize)]
struct DefineSchemaReq {
    schema: Schema,
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
    let rec = engine.create_shape(&req.table, req.where_).await?;
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

async fn get_metrics() -> Json<serde_json::Value> {
    Json(crate::metrics::metrics().snapshot())
}

async fn reset_metrics() -> Json<serde_json::Value> {
    crate::metrics::metrics().reset();
    Json(serde_json::json!({ "ok": true }))
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
