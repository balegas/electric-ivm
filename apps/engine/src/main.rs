//! electric-ivm engine binary: a durable-streams client that incrementally maintains shapes
//! (key routing + stateless predicate evaluation over Z-set deltas).
//!
//! Boot configuration is resolved from the environment by [`electric_ivm_engine::config`], which maps
//! the benchmarking-fleet's `ELECTRIC_*` / `DATABASE_URL` surface onto the engine's `ELECTRIC_IVM_*`
//! internals (the latter still win, preserving the dev/test workflow). The durable-streams base URL
//! comes from `ELECTRIC_IVM_DS_URL`; the engine binds `0.0.0.0:$ELECTRIC_PORT` (default 3000 under the
//! fleet, `127.0.0.1:0` in dev) and prints `ENGINE_LISTENING <url>` to stdout for harness discovery.

use std::io::Write;
use std::time::Duration;

use anyhow::{Context, Result};
use electric_ivm_engine::config::{self, Config};
use electric_ivm_engine::ds::DsClient;
use electric_ivm_engine::engine::Engine;
use electric_ivm_engine::statsd;

#[tokio::main]
async fn main() -> Result<()> {
    // Anchor process-uptime / boot-to-ready timing before anything else runs.
    statsd::mark_start();

    let config = Config::from_env();
    init_tracing(&config.log_filter);

    // Unknown ELECTRIC_* vars are accepted, never fatal — surface them once so operators can see the
    // image tolerated (and ignored) them.
    if !config.noop_vars.is_empty() {
        tracing::info!("accepted (no-op) ELECTRIC_* vars: {}", config.noop_vars.join(", "));
    }
    tracing::info!("resolved config: {}", config.redacted());

    // Publish request-path globals (instance id, stack id, /v1/shape secret) and wire up StatsD.
    config::set_globals(&config.instance_id, &config.stack_id, config.secret.as_deref());
    if let Some(target) = &config.statsd {
        statsd::init(target, &config.instance_id);
    }
    if config.prometheus_port.is_some() {
        tracing::info!(
            "ELECTRIC_PROMETHEUS_PORT is set, but the dedicated Prometheus listener is not implemented; \
             /metrics/prometheus stays on the main port"
        );
    }

    let ds_url = config
        .ds_url
        .clone()
        .context("ELECTRIC_IVM_DS_URL must be set to the durable-streams server base URL")?;

    // TEST-ONLY: surface an injected fault so a faulted run is never silent (no-op when unset).
    if electric_ivm_engine::fault::active() != electric_ivm_engine::fault::Fault::None {
        tracing::warn!("ELECTRIC_IVM_FAULT active: {:?}", electric_ivm_engine::fault::active());
    }

    // Size the shared Postgres pool (backfills, query-backs, subset queries) before first use.
    electric_ivm_engine::pg::set_pool_size(config.db_pool_size);

    // Postgres mode: data lives in Postgres, ingested via logical replication and read back for
    // backfill. Enabled by a resolved pg_url (ELECTRIC_IVM_PG_URL or DATABASE_URL).
    let engine = match &config.pg_url {
        Some(url) if !url.is_empty() => {
            let engine = Engine::new_pg(DsClient::new(ds_url.clone()), url.clone());
            // The dbsp arrangement circuit is mandatory infrastructure — always configured.
            tracing::info!("dbsp arrangements: dir {}", config.dbsp.dir.display());
            engine.set_dbsp_config(config.dbsp.clone());
            engine
                .setup_postgres(&config.tables, &config.slot)
                .await
                .context("postgres setup (introspect, REPLICA IDENTITY FULL, create slot)")?;
            let tables = engine.table_count().await;
            tracing::info!("postgres mode: {tables} table(s), slot '{}', streaming pgoutput", config.slot);
            statsd::consumers_ready(tables as u64);
            // Replication-slot WAL gauges (own ~10s cadence, single pooled PG connection).
            statsd::spawn_replication_slot_sampler(url.clone(), config.slot.clone());
            engine
        }
        _ => {
            // Library mode: no Postgres source; the engine is `active` from construction.
            let engine = Engine::new(DsClient::new(ds_url.clone()));
            statsd::consumers_ready(engine.table_count().await as u64);
            engine
        }
    };

    // Memory probes via OpenTelemetry: register the meter provider + Prometheus exporter, publish an
    // initial sample, and start the background sampler. `_otel` is held for the process lifetime so the
    // provider (and its exporter) stays alive. Exposed at GET /metrics/prometheus and GET /memory.
    let _otel = electric_ivm_engine::mem::init_otel();
    electric_ivm_engine::mem::publish(&engine.mem_cardinalities().await);
    electric_ivm_engine::mem::spawn_sampler(engine.clone(), Duration::from_millis(500));

    // StatsD periodic samplers (no-ops when StatsD is off): system metrics + storage size.
    statsd::spawn_system_sampler(config.metrics_period);
    statsd::spawn_storage_sampler(config.storage_dir.clone());

    let app = electric_ivm_engine::http::router_with_introspection(engine, config.trace);

    let listener =
        tokio::net::TcpListener::bind(&config.bind).await.with_context(|| format!("binding {}", config.bind))?;
    let addr = listener.local_addr()?;

    // stdout is the discovery channel (logs go to stderr).
    println!("ENGINE_LISTENING http://{addr}");
    std::io::stdout().flush().ok();
    tracing::info!("electric-ivm engine listening on http://{addr}, ds={ds_url}");

    axum::serve(listener, app).await?;
    Ok(())
}

fn init_tracing(filter: &str) {
    use tracing_subscriber::{EnvFilter, fmt};
    // `filter` already reflects ELECTRIC_IVM_LOG / ELECTRIC_LOG_LEVEL precedence (see config.rs).
    let env_filter = EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(env_filter).with_writer(std::io::stderr).init();
}
