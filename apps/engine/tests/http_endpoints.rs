//! Integration tests for the fleet HTTP surface added to the engine router: `/v1/health` (state
//! machine + exact body + status codes + cache headers), `GET /` (200 empty), and the
//! `OPTIONS /v1/shape` CORS preflight. The router is driven in-process via `Service::oneshot`; no
//! Postgres or durable-streams server is needed (the health phase is set at Engine construction).

use axum::body::Body;
use axum::http::{Request, StatusCode};
use electric_ivm_engine::ds::DsClient;
use electric_ivm_engine::engine::Engine;
use electric_ivm_engine::http::router;
use tower::ServiceExt; // for `oneshot`

fn library_engine() -> Engine {
    Engine::new(DsClient::new("http://127.0.0.1:1"))
}

async fn body_string(res: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(res.into_body(), 64 * 1024).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn health_active_in_library_mode() {
    let res = router(library_engine())
        .oneshot(Request::builder().uri("/v1/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers().get("cache-control").unwrap(), "no-cache, no-store, must-revalidate");
    assert_eq!(res.headers().get("content-type").unwrap(), "application/json");
    assert_eq!(body_string(res).await, r#"{"status":"active"}"#);
}

#[tokio::test]
async fn health_waiting_returns_202_in_pg_mode_before_setup() {
    // new_pg starts `waiting`; without setup_postgres it stays there.
    let engine = Engine::new_pg(DsClient::new("http://127.0.0.1:1"), "postgres://x/y".into());
    assert_eq!(engine.health_status(), "waiting");
    let res = router(engine)
        .oneshot(Request::builder().uri("/v1/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::ACCEPTED);
    assert_eq!(body_string(res).await, r#"{"status":"waiting"}"#);
}

#[tokio::test]
async fn root_returns_200_empty() {
    let res = router(library_engine())
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert!(body_string(res).await.is_empty());
}

#[tokio::test]
async fn options_shape_is_cors_preflight() {
    let res = router(library_engine())
        .oneshot(Request::builder().method("OPTIONS").uri("/v1/shape").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        res.headers().get("access-control-allow-methods").unwrap(),
        "GET, POST, HEAD, DELETE, OPTIONS"
    );
}

#[tokio::test]
async fn legacy_health_still_ok() {
    let res = router(library_engine())
        .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_string(res).await, "ok");
}
