//! electric-lite engine binary: a durable-streams client that runs dbsp filter circuits per
//! shape. Reads the durable-streams base URL from `ELECTRIC_LITE_DS_URL`, binds the control
//! plane (default `127.0.0.1:0`), and prints `ENGINE_LISTENING <url>` to stdout so a harness
//! can discover the chosen port.

use std::io::Write;

use anyhow::{Context, Result};
use electric_lite_engine::ds::DsClient;
use electric_lite_engine::engine::Engine;
use electric_lite_engine::http::router;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let ds_url = std::env::var("ELECTRIC_LITE_DS_URL")
        .context("ELECTRIC_LITE_DS_URL must be set to the durable-streams server base URL")?;
    let bind = std::env::var("ELECTRIC_LITE_BIND").unwrap_or_else(|_| "127.0.0.1:0".to_string());

    // TEST-ONLY: surface an injected fault so a faulted run is never silent (no-op when unset).
    if electric_lite_engine::fault::active() != electric_lite_engine::fault::Fault::None {
        tracing::warn!("ELECTRIC_LITE_FAULT active: {:?}", electric_lite_engine::fault::active());
    }

    // Postgres mode: data lives in Postgres, ingested via logical replication and read back for
    // backfill (no in-memory table_state). Enabled by ELECTRIC_LITE_PG_URL.
    let engine = match std::env::var("ELECTRIC_LITE_PG_URL") {
        Ok(url) if !url.is_empty() => {
            let engine = Engine::new_pg(DsClient::new(ds_url.clone()), url);
            let tables: Vec<String> = std::env::var("ELECTRIC_LITE_PG_TABLES")
                .unwrap_or_default()
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            let slot = std::env::var("ELECTRIC_LITE_PG_SLOT").unwrap_or_else(|_| "electric_lite".to_string());
            let poll_ms: u64 =
                std::env::var("ELECTRIC_LITE_PG_POLL_MS").ok().and_then(|s| s.parse().ok()).unwrap_or(50);
            engine
                .setup_postgres(&tables, &slot, poll_ms)
                .await
                .context("postgres setup (introspect, REPLICA IDENTITY FULL, create slot)")?;
            tracing::info!("postgres mode: {} table(s), slot '{slot}', poll {poll_ms}ms", tables.len());
            engine
        }
        _ => Engine::new(DsClient::new(ds_url.clone())),
    };

    // Memory probes via OpenTelemetry: register the meter provider + Prometheus exporter, publish an
    // initial sample, and start the background sampler. `_otel` is held for the process lifetime so the
    // provider (and its exporter) stays alive. Exposed at GET /metrics/prometheus and GET /memory.
    let _otel = electric_lite_engine::mem::init_otel();
    electric_lite_engine::mem::publish(&engine.mem_cardinalities().await);
    electric_lite_engine::mem::spawn_sampler(engine.clone(), std::time::Duration::from_millis(500));

    let app = router(engine);

    let listener = tokio::net::TcpListener::bind(&bind).await.with_context(|| format!("binding {bind}"))?;
    let addr = listener.local_addr()?;

    // stdout is the discovery channel (logs go to stderr).
    println!("ENGINE_LISTENING http://{addr}");
    std::io::stdout().flush().ok();
    tracing::info!("electric-lite engine listening on http://{addr}, ds={ds_url}");

    axum::serve(listener, app).await?;
    Ok(())
}

fn init_tracing() {
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_env("ELECTRIC_LITE_LOG").unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).with_writer(std::io::stderr).init();
}
