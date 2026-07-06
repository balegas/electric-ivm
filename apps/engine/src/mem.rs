//! Memory probes, exported via OpenTelemetry.
//!
//! Answers "how does engine memory evolve as shapes are created, for different deployment sizes?".
//! We track two layers:
//!   1. **Process memory** — resident (RSS) and virtual bytes (`memory-stats`, cross-platform).
//!   2. **Engine cardinalities** — the in-memory structures whose growth drives RSS: shapes, per-table
//!      tailers, shared family circuits (the M× join-trace amplifier), standalone circuits, and the
//!      subquery registry's nodes + contributor-pk sets.
//!
//! Both are published as OpenTelemetry observable gauges through a Prometheus exporter
//! (`GET /metrics/prometheus`, the format an OTel collector scrapes) and as JSON (`GET /memory`) for the
//! benchmark harness. OTel observable-gauge callbacks are synchronous, so a background sampler refreshes
//! a lock-free [`Gauges`] snapshot that the callbacks (and the JSON endpoint) read.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use prometheus::{Registry, TextEncoder};

/// Engine-internal cardinalities, computed from in-memory state by [`crate::engine::Engine::mem_cardinalities`].
#[derive(Clone, Default, serde::Serialize)]
pub struct Cardinalities {
    pub shapes: usize,
    pub tailers: usize,
    pub tables: usize,
    pub families: usize,
    pub family_shapes: usize,
    pub standalone: usize,
    pub subquery_nodes: usize,
    pub subquery_contributors: usize,
    pub subquery_distinct_values: usize,
    pub subquery_shapes: usize,
    pub subquery_edges: usize,
}

/// Lock-free snapshot the OTel gauge callbacks and `/memory` read. Updated by the sampler and on demand.
#[derive(Default)]
struct Gauges {
    rss_bytes: AtomicU64,
    virtual_bytes: AtomicU64,
    shapes: AtomicU64,
    tailers: AtomicU64,
    tables: AtomicU64,
    families: AtomicU64,
    family_shapes: AtomicU64,
    standalone: AtomicU64,
    subquery_nodes: AtomicU64,
    subquery_contributors: AtomicU64,
    subquery_distinct_values: AtomicU64,
    subquery_shapes: AtomicU64,
    subquery_edges: AtomicU64,
    samples: AtomicU64,
}

static GAUGES: OnceLock<Gauges> = OnceLock::new();
static PROM_REGISTRY: OnceLock<Registry> = OnceLock::new();

fn gauges() -> &'static Gauges {
    GAUGES.get_or_init(Gauges::default)
}

/// Current process resident + virtual memory in bytes (0 if unavailable on this platform).
pub fn process_memory() -> (u64, u64) {
    match memory_stats::memory_stats() {
        Some(s) => (s.physical_mem as u64, s.virtual_mem as u64),
        None => (0, 0),
    }
}

/// Refresh the published gauges from a freshly-measured process memory + engine cardinalities. Called by
/// the background sampler and by `/memory` so the JSON read and the OTel scrape agree.
pub fn publish(card: &Cardinalities) {
    let g = gauges();
    let (rss, virt) = process_memory();
    g.rss_bytes.store(rss, Ordering::Relaxed);
    g.virtual_bytes.store(virt, Ordering::Relaxed);
    g.shapes.store(card.shapes as u64, Ordering::Relaxed);
    g.tailers.store(card.tailers as u64, Ordering::Relaxed);
    g.tables.store(card.tables as u64, Ordering::Relaxed);
    g.families.store(card.families as u64, Ordering::Relaxed);
    g.family_shapes.store(card.family_shapes as u64, Ordering::Relaxed);
    g.standalone.store(card.standalone as u64, Ordering::Relaxed);
    g.subquery_nodes.store(card.subquery_nodes as u64, Ordering::Relaxed);
    g.subquery_contributors.store(card.subquery_contributors as u64, Ordering::Relaxed);
    g.subquery_distinct_values.store(card.subquery_distinct_values as u64, Ordering::Relaxed);
    g.subquery_shapes.store(card.subquery_shapes as u64, Ordering::Relaxed);
    g.subquery_edges.store(card.subquery_edges as u64, Ordering::Relaxed);
    g.samples.fetch_add(1, Ordering::Relaxed);
}

/// JSON snapshot for `GET /memory` (RSS measured fresh; cardinalities from the last publish).
pub fn snapshot_json() -> serde_json::Value {
    let g = gauges();
    let (rss, virt) = process_memory();
    g.rss_bytes.store(rss, Ordering::Relaxed);
    g.virtual_bytes.store(virt, Ordering::Relaxed);
    serde_json::json!({
        "process": {
            "rss_bytes": rss,
            "rss_mib": rss / (1024 * 1024),
            "virtual_bytes": virt,
        },
        "cardinalities": {
            "shapes": g.shapes.load(Ordering::Relaxed),
            "tailers": g.tailers.load(Ordering::Relaxed),
            "tables": g.tables.load(Ordering::Relaxed),
            "families": g.families.load(Ordering::Relaxed),
            "family_shapes": g.family_shapes.load(Ordering::Relaxed),
            "standalone": g.standalone.load(Ordering::Relaxed),
            "subquery_nodes": g.subquery_nodes.load(Ordering::Relaxed),
            "subquery_contributors": g.subquery_contributors.load(Ordering::Relaxed),
            "subquery_distinct_values": g.subquery_distinct_values.load(Ordering::Relaxed),
            "subquery_shapes": g.subquery_shapes.load(Ordering::Relaxed),
            "subquery_edges": g.subquery_edges.load(Ordering::Relaxed),
        },
        "samples": g.samples.load(Ordering::Relaxed),
    })
}

/// Render the OTel/Prometheus exposition text for `GET /metrics/prometheus`.
pub fn prometheus_text() -> String {
    let Some(reg) = PROM_REGISTRY.get() else { return String::new() };
    let mut buf = String::new();
    let _ = TextEncoder::new().encode_utf8(&reg.gather(), &mut buf);
    buf
}

/// Initialize the OpenTelemetry meter provider with a Prometheus exporter and register the memory +
/// cardinality observable gauges. Idempotent; returns the provider so the caller keeps it alive.
pub fn init_otel() -> SdkMeterProvider {
    let registry = Registry::new();
    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()
        .expect("build prometheus exporter");
    let _ = PROM_REGISTRY.set(registry);

    let provider = SdkMeterProvider::builder().with_reader(exporter).build();
    let meter = provider.meter("electric_ivm_engine");

    // One observable gauge per metric; each callback reads the lock-free published snapshot.
    macro_rules! gauge {
        ($name:expr, $desc:expr, $field:ident, $unit:expr) => {{
            let b = meter.u64_observable_gauge($name).with_description($desc);
            let b = if $unit.is_empty() { b } else { b.with_unit($unit) };
            b.with_callback(|obs| obs.observe(gauges().$field.load(Ordering::Relaxed), &[])).build();
        }};
    }
    gauge!("engine_process_resident_memory", "Resident set size of the engine process", rss_bytes, "By");
    gauge!("engine_process_virtual_memory", "Virtual memory of the engine process", virtual_bytes, "By");
    gauge!("engine_shapes", "Registered shapes (all kinds)", shapes, "");
    gauge!("engine_tailers", "Per-table replication tailers", tailers, "");
    gauge!("engine_tables", "Tables with a known schema", tables, "");
    gauge!("engine_family_circuits", "Shared equality family circuits (each holds the base table once)", families, "");
    gauge!("engine_family_shapes", "Shapes attached to family circuits", family_shapes, "");
    gauge!("engine_standalone_circuits", "Standalone per-shape circuits", standalone, "");
    gauge!("engine_subquery_nodes", "Maintained subquery inner-set nodes (shared)", subquery_nodes, "");
    gauge!("engine_subquery_contributors", "Total contributor pks across subquery nodes", subquery_contributors, "");
    gauge!("engine_subquery_distinct_values", "Distinct values across subquery nodes", subquery_distinct_values, "");
    gauge!("engine_subquery_shapes", "Subquery (cross-table) shapes", subquery_shapes, "");
    gauge!("engine_subquery_edges", "Subquery dependency edges", subquery_edges, "");

    // Touch a KeyValue so the import is used even if labels are added later.
    let _ = KeyValue::new("service.name", "electric-ivm-engine");
    provider
}

/// Spawn the background sampler: every `interval`, recompute engine cardinalities and republish the
/// gauges so the OTel scrape reflects current state without a `/memory` poll.
pub fn spawn_sampler(engine: crate::engine::Engine, interval: Duration) {
    tokio::spawn(async move {
        loop {
            let card = engine.mem_cardinalities().await;
            publish(&card);
            tokio::time::sleep(interval).await;
        }
    });
}
