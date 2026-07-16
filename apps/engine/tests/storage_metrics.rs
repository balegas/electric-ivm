//! Verifies that a **live** append to an active shape emits the storage StatsD metrics
//! (`electric.storage.transaction_stored.*` + `electric.shape_log_collector.transaction.affected_shape_count`)
//! — the docker agent observed replication metrics advancing during an insert burst but did not see
//! these in its capture window. Drives a real engine (library mode) + tailer against a fake ds server
//! that delivers one insert on the change log, with StatsD pointed at a local UDP listener.

use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use axum::extract::Request;
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Router;
use electric_circuits_engine::config::{self, StatsdTarget};
use electric_circuits_engine::ds::DsClient;
use electric_circuits_engine::engine::Engine;
use electric_circuits_engine::schema::Schema;
use electric_circuits_engine::statsd;

/// The sequencer starts consuming at `define_schema` — hold delivery until the shape is live so
/// the insert actually fans out to it. Delivery is keyed on the read offset (serve the insert to
/// every read at `-1`, park reads past it) so a select-cancelled long-poll can't swallow it; the
/// sequencer's (lsn, seq) highwater de-duplicates any double delivery.
static READY: AtomicBool = AtomicBool::new(false);

async fn ds_handler(req: Request) -> Response {
    let path = req.uri().path().to_string();
    let is_long_poll = req.uri().query().unwrap_or("").split('&').any(|kv| kv == "live=long-poll");
    match *req.method() {
        Method::PUT | Method::POST | Method::DELETE => StatusCode::OK.into_response(),
        Method::GET if path.contains("/changes") && is_long_poll => {
            let at_start = req.uri().query().unwrap_or("").contains("offset=-1");
            if READY.load(Ordering::SeqCst) && at_start {
                // One committed insert (txid 100) matching the match-all shape.
                (
                    [("stream-next-offset", "01")],
                    r#"[{"type":"t","key":"1","value":{"id":"1"},"headers":{"operation":"insert","txid":"100","lsn":"0/10","seq":0}}]"#,
                )
                    .into_response()
            } else {
                // Not ready yet, or already past the insert: park briefly like a real long-poll.
                tokio::time::sleep(Duration::from_millis(50)).await;
                (StatusCode::NO_CONTENT, [("stream-next-offset", if at_start { "-1" } else { "01" })])
                    .into_response()
            }
        }
        Method::GET => {
            // Any other read (e.g. shape stream): empty + up-to-date.
            ([("stream-next-offset", "tip"), ("stream-up-to-date", "1")], "[]").into_response()
        }
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

fn collect(sock: UdpSocket, window: Duration) -> Vec<String> {
    sock.set_read_timeout(Some(Duration::from_millis(50))).unwrap();
    let deadline = Instant::now() + window;
    let mut lines = Vec::new();
    let mut buf = [0u8; 16384];
    while Instant::now() < deadline {
        if let Ok(n) = sock.recv(&mut buf) {
            for l in String::from_utf8_lossy(&buf[..n]).split('\n') {
                if !l.is_empty() {
                    lines.push(l.to_string());
                }
            }
        }
    }
    lines
}

#[tokio::test]
async fn live_append_emits_storage_txn_metrics() {
    // StatsD listener + global init (own test binary, so the global is fresh).
    let sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let port = sock.local_addr().unwrap().port();
    let id = "storage-metrics-test";
    config::set_globals(id, "single_stack", None);
    statsd::init(&StatsdTarget { host: "127.0.0.1".into(), port }, id);
    assert!(statsd::enabled());

    // Fake ds server.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ds_url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let _ = axum::serve(listener, Router::new().fallback(ds_handler)).await;
    });

    // Engine (library mode) + schema + a match-all shape (spawns the tailer on table t).
    let engine = Engine::new(DsClient::new(&ds_url));
    let schema: Schema = serde_json::from_value(serde_json::json!({
        "tables": { "t": { "columns": { "id": { "type": "text" } }, "primaryKey": "id" } }
    }))
    .unwrap();
    engine.define_schema(&schema).await.unwrap();
    engine.create_shape("t", None, None, false, false).await.unwrap();
    READY.store(true, Ordering::SeqCst);

    // Let the sequencer read the change log, process the insert, append to the shape, and emit
    // storage_txn.
    let lines = tokio::task::spawn_blocking(move || collect(sock, Duration::from_secs(2))).await.unwrap();
    let names: Vec<&str> = lines.iter().filter_map(|l| l.split(':').next()).collect();

    for expected in [
        "electric.storage.transaction_stored.count",
        "electric.storage.transaction_stored.bytes",
        "electric.storage.transaction_stored.operations",
        "electric.shape_log_collector.transaction.affected_shape_count",
    ] {
        assert!(names.contains(&expected), "missing {expected}; captured names: {names:?}");
    }
    // affected_shape_count should be 1 (the single active shape) and every line carries instance_id.
    assert!(
        lines.iter().any(|l| l.starts_with("electric.shape_log_collector.transaction.affected_shape_count:1|d")),
        "affected_shape_count should be 1: {lines:?}"
    );
    assert!(lines.iter().all(|l| l.contains(&format!("instance_id:{id}"))));
}
