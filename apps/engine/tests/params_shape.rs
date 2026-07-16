//! HTTP-level coverage of `params` on `GET /v1/shape`: the query→parse→validate→substitute→create
//! path, and the Electric-style 400s. Runs against the real axum router + a fake ds server in library
//! mode (no Postgres needed) — correct-rows-with-a-real-subquery is covered by the TS conformance
//! suite (packages/conformance/src/conformance-params.test.ts), which has a real PG.

use axum::extract::Request;
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Router;
use electric_circuits_engine::ds::DsClient;
use electric_circuits_engine::engine::Engine;
use electric_circuits_engine::http::router;
use electric_circuits_engine::schema::Schema;
use tower::ServiceExt;

/// Minimal fake ds: writes succeed; reads are empty + up-to-date (so the snapshot's materialize
/// completes and a handle is registered — library mode has no rows).
async fn ds_handler(req: Request) -> Response {
    match *req.method() {
        Method::PUT | Method::POST | Method::DELETE => StatusCode::OK.into_response(),
        Method::GET => ([("stream-next-offset", "tip"), ("stream-up-to-date", "1")], "[]").into_response(),
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

/// Percent-encode an ASCII query token (test values are ASCII).
fn enc(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
            _ => format!("%{:02X}", c as u32),
        })
        .collect()
}

fn uri(pairs: &[(&str, &str)]) -> String {
    let q: Vec<String> = pairs.iter().map(|(k, v)| format!("{}={}", enc(k), enc(v))).collect();
    format!("/v1/shape?{}", q.join("&"))
}

async fn boot() -> Router {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ds_url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let _ = axum::serve(listener, Router::new().fallback(ds_handler)).await;
    });
    let engine = Engine::new(DsClient::new(&ds_url));
    let schema: Schema = serde_json::from_value(serde_json::json!({
        "tables": { "t": { "columns": { "id": {"type":"text"}, "owner_id": {"type":"text"} }, "primaryKey": "id" } }
    }))
    .unwrap();
    engine.define_schema(&schema).await.unwrap();
    router(engine)
}

async fn get(app: &Router, uri: &str) -> (StatusCode, String) {
    let res = app
        .clone()
        .oneshot(axum::http::Request::builder().uri(uri).body(axum::body::Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let handle = res.headers().get("electric-handle").and_then(|h| h.to_str().ok()).map(str::to_string);
    let body = axum::body::to_bytes(res.into_body(), 64 * 1024).await.unwrap();
    // Return the handle (if any) prefixed so callers can assert both status and handle from one value.
    (status, handle.unwrap_or_else(|| String::from_utf8_lossy(&body).to_string()))
}

#[tokio::test]
async fn valid_params_create_a_shape_bracket_form() {
    let app = boot().await;
    let (status, handle) = get(&app, &uri(&[("table", "t"), ("offset", "-1"), ("where", "owner_id = $1"), ("params[1]", "u-abc")])).await;
    assert_eq!(status, StatusCode::OK);
    assert!(handle.starts_with('s'), "expected a shape handle, got {handle}");
}

#[tokio::test]
async fn valid_params_json_form() {
    let app = boot().await;
    let (status, handle) =
        get(&app, &uri(&[("table", "t"), ("offset", "-1"), ("where", "owner_id = $1"), ("params", r#"{"1":"u-abc"}"#)])).await;
    assert_eq!(status, StatusCode::OK, "json params: {handle}");
    assert!(handle.starts_with('s'));
}

#[tokio::test]
async fn missing_param_is_400() {
    let app = boot().await;
    let (status, body) = get(&app, &uri(&[("table", "t"), ("offset", "-1"), ("where", "owner_id = $1")])).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("parameter $1 was not provided"), "body: {body}");
}

#[tokio::test]
async fn non_sequential_keys_is_400() {
    let app = boot().await;
    let (status, body) = get(&app, &uri(&[("table", "t"), ("offset", "-1"), ("where", "owner_id = $1"), ("params[2]", "x")])).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("Parameters must be numbered sequentially"), "body: {body}");
}

#[tokio::test]
async fn non_numeric_keys_is_400() {
    let app = boot().await;
    let (status, body) = get(&app, &uri(&[("table", "t"), ("offset", "-1"), ("where", "owner_id = $1"), ("params[a]", "x")])).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("Parameters can only use numbers as keys"), "body: {body}");
}
