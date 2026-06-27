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

    let engine = Engine::new(DsClient::new(ds_url.clone()));
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
