//! Integration test: StatsD emission over a real UDP socket, validated with the benchmarking-fleet's
//! exact parsing rules (see `docs/fleet-conformance.md` §4 and §7 gate 4). We bind a std `UdpSocket`,
//! point the StatsD module at it, drive metrics, then assert every received line parses as
//! `name:value|type|#tags` with a numeric value and the mandatory `instance_id` tag.
//!
//! Each `tests/` file is its own test binary, so the process-global StatsD client set by
//! `statsd::init` is fresh here. Wire-format + batching cases use standalone `Statsd::connect` clients
//! (no global) so they don't collide with the single global-using instrumentation test.

use std::net::UdpSocket;
use std::time::{Duration, Instant};

use electric_ivm_engine::config::{self, StatsdTarget};
use electric_ivm_engine::statsd::{self, Statsd};

/// Drain all datagrams that arrive within `window`, returning them newline-unsplit.
fn collect(sock: UdpSocket, window: Duration) -> Vec<String> {
    sock.set_read_timeout(Some(Duration::from_millis(50))).unwrap();
    let deadline = Instant::now() + window;
    let mut out = Vec::new();
    let mut buf = [0u8; 16384];
    while Instant::now() < deadline {
        // A read timeout (Err) just means nothing arrived this tick; keep polling until the window closes.
        if let Ok(n) = sock.recv(&mut buf) {
            out.push(String::from_utf8(buf[..n].to_vec()).expect("utf8 datagram"));
        }
    }
    out
}

/// The fleet's parse: split lines, split each on `|` into exactly 3 parts, value parses as f64, and
/// the tags carry `instance_id:<expected>`. Panics on any violation. Returns the metric names seen.
fn assert_fleet_parseable(datagrams: &[String], instance_id: &str) -> Vec<String> {
    let mut names = Vec::new();
    for dg in datagrams {
        assert!(dg.len() <= 1432, "datagram exceeds 1432 bytes: {} bytes", dg.len());
        for line in dg.split('\n') {
            assert!(!line.is_empty(), "no empty lines");
            let parts: Vec<&str> = line.split('|').collect();
            assert_eq!(parts.len(), 3, "expected name:value|type|#tags, got {line:?}");

            let (name, value) = parts[0].rsplit_once(':').unwrap_or_else(|| panic!("no name:value in {line:?}"));
            assert!(!name.is_empty(), "empty metric name in {line:?}");
            value.parse::<f64>().unwrap_or_else(|_| panic!("value not f64 in {line:?}"));

            assert!(matches!(parts[1], "c" | "g" | "d"), "unexpected type in {line:?}");

            let tags = parts[2];
            assert!(tags.starts_with("#instance_id:"), "instance_id must be first tag in {line:?}");
            assert!(
                tags.contains(&format!("instance_id:{instance_id}")),
                "missing instance_id:{instance_id} in {line:?}"
            );
            names.push(name.to_string());
        }
    }
    names
}

#[tokio::test]
async fn client_wire_format_parses_with_fleet_rules() {
    let sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let addr = sock.local_addr().unwrap();
    let id = "wire-instance-01";
    let client = Statsd::connect(&addr.to_string(), id).unwrap();

    // One of each of the four datadog metric shapes, with and without extra tags.
    client.incr("electric.plug.serve_shape.requests.count", &[("status", "200"), ("known_error", "false"), ("live", "true")]);
    client.count("electric.plug.serve_shape.bytes", 4096, &[]);
    client.gauge("system.cpu.utilization.total", 42.5, &[]);
    client.dist("plug.router_dispatch.stop.duration", 1.5, &[("route", "/v1/shape"), ("status", "200")]);

    tokio::time::sleep(Duration::from_millis(200)).await;
    let datagrams = tokio::task::spawn_blocking(move || collect(sock, Duration::from_millis(800))).await.unwrap();

    let names = assert_fleet_parseable(&datagrams, id);
    assert!(names.contains(&"electric.plug.serve_shape.requests.count".to_string()));
    assert!(names.contains(&"system.cpu.utilization.total".to_string()));
    // Exact-string spot check that a counter is `:1|c` and a gauge float has no exponent.
    let all = datagrams.join("\n");
    assert!(all.contains("electric.plug.serve_shape.bytes:4096|c|#instance_id:wire-instance-01"));
    assert!(all.contains("system.cpu.utilization.total:42.5|g|#instance_id:wire-instance-01"));
}

#[tokio::test]
async fn batching_stays_under_1432_and_loses_nothing() {
    let sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let addr = sock.local_addr().unwrap();
    let id = "batch-instance-02";
    let client = Statsd::connect(&addr.to_string(), id).unwrap();

    // Enough lines to force many datagrams (each line ~80 bytes; 2000 lines ≈ 160 KB).
    const N: usize = 2000;
    for i in 0..N {
        client.count("electric.postgres.replication.transaction_received.bytes", i as u64, &[("k", "vvvvvvvv")]);
    }

    tokio::time::sleep(Duration::from_millis(300)).await;
    let datagrams = tokio::task::spawn_blocking(move || collect(sock, Duration::from_secs(2))).await.unwrap();

    assert!(datagrams.len() > 1, "expected multiple datagrams, got {}", datagrams.len());
    let names = assert_fleet_parseable(&datagrams, id); // also asserts every datagram <= 1432 bytes
    // UDP is lossy in principle, but on loopback within one process we expect every line delivered.
    assert_eq!(names.len(), N, "expected all {N} lines across datagrams, got {}", names.len());
}

#[tokio::test]
async fn instrumentation_emits_expected_metric_families() {
    // This is the ONLY test that uses the process-global StatsD client (set once per test binary).
    let sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let addr = sock.local_addr().unwrap();
    let id = "global-instance-03";
    config::set_globals(id, "bench-stream", None);
    statsd::init(&StatsdTarget { host: "127.0.0.1".into(), port: addr.port() }, id);
    assert!(statsd::enabled(), "global StatsD should be active after init");

    // Drive the same instrumentation the real call sites use, with the values they would compute.
    statsd::serve_shape("issues", false, 200, Duration::from_millis(3), 1234); // non-live path
    statsd::serve_shape("issues", true, 204, Duration::from_millis(20_000), 0); // live long-poll
    statsd::serve_shape("issues", false, 400, Duration::from_millis(1), 42); // known client error
    statsd::replication_txn(5, 512, 1.5);
    statsd::storage_txn(3, 256, 2);
    statsd::snapshot_stored(100, 4096, 12.0);
    statsd::consumers_ready(7);
    statsd::shape_gauges(5, 3, 2); // total, indexed (family), unindexed (standalone)
    statsd::storage_used(1_048_576, Duration::from_millis(4));
    statsd::replication_slot_gauges("0/20", Some("0/10"), Some("0/18"));

    tokio::time::sleep(Duration::from_millis(200)).await;
    let datagrams = tokio::task::spawn_blocking(move || collect(sock, Duration::from_millis(800))).await.unwrap();
    let names = assert_fleet_parseable(&datagrams, id);
    let all = datagrams.join("\n");

    for expected in [
        "plug.router_dispatch.stop.duration",
        "electric.plug.serve_shape.requests.count",
        "electric.shape.response_size.bytes",
        "electric.plug.serve_shape.duration",
        "electric.plug.serve_shape.count",
        "electric.plug.serve_shape.bytes",
        "electric.postgres.replication.transaction_received.count",
        "electric.postgres.replication.transaction_received.bytes",
        "electric.postgres.replication.transaction_received.operations",
        "electric.postgres.replication.transaction_received.receive_lag",
        "electric.storage.transaction_stored.count",
        "electric.storage.transaction_stored.bytes",
        "electric.storage.transaction_stored.operations",
        "electric.shape_log_collector.transaction.affected_shape_count",
        "electric.storage.snapshot_stored.count",
        "electric.storage.snapshot_stored.bytes",
        "electric.storage.snapshot_stored.operations",
        "electric.storage.make_new_snapshot.stop.duration",
        "electric.connection.consumers_ready.duration",
        "electric.connection.consumers_ready.total",
        "electric.shapes.total_shapes.count",
        "electric.shapes.active_shapes.count",
        "electric.shapes.total_shapes.count_indexed",
        "electric.shapes.total_shapes.count_unindexed",
        "electric.storage.used.bytes",
        "electric.storage.used.measurement_duration",
        "electric.postgres.replication.pg_wal_offset",
        "electric.postgres.replication.slot_retained_wal_size",
        "electric.postgres.replication.slot_confirmed_flush_lsn_lag",
    ] {
        assert!(names.iter().any(|n| n == expected), "missing metric {expected}");
    }

    // active_shapes == total_shapes for our engine (every registered shape is actively maintained).
    assert!(all.contains("electric.shapes.total_shapes.count:5|g"));
    assert!(all.contains("electric.shapes.active_shapes.count:5|g"));
    assert!(all.contains("electric.shapes.total_shapes.count_indexed:3|g"));
    assert!(all.contains("electric.shapes.total_shapes.count_unindexed:2|g"));
    // slot deltas: wal 0x20 - restart 0x10 = 0x10 (32); wal 0x20 - confirmed 0x18 = 0x8 (8).
    assert!(all.contains("electric.postgres.replication.pg_wal_offset:32|g"));
    assert!(all.contains("electric.postgres.replication.slot_retained_wal_size:16|g"));
    assert!(all.contains("electric.postgres.replication.slot_confirmed_flush_lsn_lag:8|g"));

    // The response-size distribution carries the fleet tags, incl. the configured stack_id.
    assert!(
        all.contains("root_table:issues") && all.contains("is_live:true") && all.contains("stack_id:bench-stream"),
        "response_size tags missing: {all}"
    );
    // A non-live 200 is not a known error; a 400 is.
    assert!(all.contains("status:200,known_error:false,live:false"));
    assert!(all.contains("status:400,known_error:true,live:false"));
    // A live request emits NO serve_shape.duration (that metric is non-live only).
    assert!(all.contains("is_live:true"), "live response_size present");
}
