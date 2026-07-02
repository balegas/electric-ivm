//! End-to-end proof of the idle live long-poll deadline (the >60s-hang bug the docker agent found in
//! the container): an idle `GET /v1/shape?live=true` must return **204** with the electric headers at
//! `ELECTRIC_LIVE_TIMEOUT_MS`, even though the durable-streams server holds an idle long-poll far
//! longer. Driven through the real axum router against a fake ds server whose live reads park for 10s
//! (mode 0) or return data immediately (mode 1).
//!
//! Own test binary so the process-global `live_timeout()` `OnceLock` initializes to this test's short
//! value (set before the first live request, which is the first call that reads it).

use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::Request;
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Router;
use electric_ivm_engine::ds::DsClient;
use electric_ivm_engine::engine::Engine;
use electric_ivm_engine::http::router;
use electric_ivm_engine::schema::Schema;
use tower::ServiceExt; // oneshot

/// Fake ds server live-read behavior: 0 = idle (park 10s then 204), 1 = data available immediately.
static MODE: AtomicU8 = AtomicU8::new(0);

/// A minimal durable-streams server. PUT/POST/DELETE succeed; GET catch-up returns an empty
/// up-to-date page; GET long-poll parks (idle) or returns one change (data) per MODE.
async fn ds_handler(req: Request) -> Response {
    let is_long_poll = req.uri().query().unwrap_or("").split('&').any(|kv| kv == "live=long-poll");
    match *req.method() {
        Method::PUT | Method::POST | Method::DELETE => StatusCode::OK.into_response(),
        Method::GET if !is_long_poll => {
            // Catch-up read (materialize / key-set rebuild): empty and up-to-date.
            ([("stream-next-offset", "tip"), ("stream-up-to-date", "1")], "[]").into_response()
        }
        Method::GET => {
            if MODE.load(Ordering::SeqCst) == 0 {
                // Idle: hold the long-poll far past our client-side deadline, then 204 (as the real ds
                // server does). Our deadline must fire first.
                tokio::time::sleep(Duration::from_secs(10)).await;
                (StatusCode::NO_CONTENT, [("stream-next-offset", "tip")]).into_response()
            } else {
                // Data available promptly.
                (
                    [("stream-next-offset", "05"), ("stream-up-to-date", "1")],
                    r#"[{"type":"t","key":"k1","value":{"id":"k1"},"headers":{"operation":"upsert","offset":"05"}}]"#,
                )
                    .into_response()
            }
        }
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

async fn spawn_fake_ds() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, Router::new().fallback(ds_handler)).await;
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn idle_live_poll_returns_204_at_deadline_then_data_promptly() {
    // Short deadline; must be set before the first live_timeout() read (cached). This is the only test
    // in this binary and only live requests read it, so the OnceLock initializes to 300ms.
    // SAFETY: single-threaded setup before any other thread reads the env.
    unsafe {
        std::env::set_var("ELECTRIC_LIVE_TIMEOUT_MS", "300");
    }

    let ds_url = spawn_fake_ds().await;
    let engine = Engine::new(DsClient::new(&ds_url));
    let schema: Schema = serde_json::from_value(serde_json::json!({
        "tables": { "t": { "columns": { "id": { "type": "text" } }, "primaryKey": "id" } }
    }))
    .unwrap();
    engine.define_schema(&schema).await.unwrap();
    let app = router(engine);

    // 1) Snapshot (offset=-1) registers a handle at the stream tail.
    let snap = app
        .clone()
        .oneshot(Request::builder().uri("/v1/shape?table=t&offset=-1").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(snap.status(), StatusCode::OK);
    let handle = snap.headers().get("electric-handle").unwrap().to_str().unwrap().to_string();
    let offset = snap.headers().get("electric-offset").unwrap().to_str().unwrap().to_string();
    let live_uri = format!("/v1/shape?table=t&handle={handle}&offset={offset}&live=true");

    // 2) Idle live poll: the fake ds parks for 10s, but we must 204 at ~300ms with the electric headers.
    MODE.store(0, Ordering::SeqCst);
    let start = Instant::now();
    let res = app.clone().oneshot(Request::builder().uri(&live_uri).body(Body::empty()).unwrap()).await.unwrap();
    let elapsed = start.elapsed();
    assert_eq!(res.status(), StatusCode::NO_CONTENT, "idle live poll must return 204 at the deadline");
    assert!(res.headers().contains_key("electric-handle"), "204 must carry electric-handle");
    assert!(res.headers().contains_key("electric-offset"), "204 must carry electric-offset");
    assert!(elapsed >= Duration::from_millis(250), "should wait ~the deadline, waited {elapsed:?}");
    assert!(elapsed < Duration::from_secs(3), "must NOT hang for the ds server's 10s park, took {elapsed:?}");

    // 3) Inverse: data available before the deadline returns 200 promptly (not at the deadline).
    MODE.store(1, Ordering::SeqCst);
    let start = Instant::now();
    let res = app.clone().oneshot(Request::builder().uri(&live_uri).body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK, "data before the deadline must return 200");
    assert!(start.elapsed() < Duration::from_millis(250), "data must return promptly, took {:?}", start.elapsed());
}
