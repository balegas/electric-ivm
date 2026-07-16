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
//!
//! A third layer, JSON-only (Phase 0 of the memory-reduction effort, no OTel gauges to avoid metric
//! churn): byte-level self-accounting, a lower-bound owned-heap estimate (see
//! [`crate::heap_size::HeapSize`]) per major structure. The gap between the sum of these and
//! `process.rss_bytes` is the allocator/pinning term this instrumentation exists to isolate.
//!
//! The byte-level walk (`Engine::mem_bytes`) is expensive — it locks engine state, round-trips a
//! `SequencerCmd::MemBytes` command to the sequencer task, locks the subquery registry, and walks
//! roughly the engine's entire owned heap. It must run ONLY when `GET /memory` is actually served,
//! never on the 500ms background sampler (`spawn_sampler` below): that sampler calls
//! `Engine::mem_cardinalities` exclusively, which computes cheap in-memory counts and never touches
//! `HeapSize::heap_bytes` or `SequencerCmd::MemBytes`. See `engine::Engine::mem_cardinalities` /
//! `mem_bytes` for the split and `http::get_memory` for the one place both are combined.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use prometheus::{Registry, TextEncoder};

/// The six byte-level self-accounting terms (Phase 0 of the memory-reduction effort), computed by
/// [`crate::engine::Engine::mem_bytes`] — the on-demand, `GET /memory`-only counterpart to
/// [`Cardinalities`]/[`crate::engine::Engine::mem_cardinalities`]. Deliberately its own type (not
/// folded into `Cardinalities` at the source) so the cheap-count path has no fields to leave zeroed
/// by convention — `mem_cardinalities` simply never constructs one of these.
#[derive(Clone, Default)]
pub struct HeapBytes {
    pub bytes_shape_records: usize,
    pub bytes_executors: usize,
    pub bytes_retention: usize,
    pub bytes_subquery_registry: usize,
    pub bytes_membership_circuit: usize,
    /// Split of `bytes_membership_circuit` (Task 1.3): the raw upsert-map integrals
    /// (CONTRIBUTORS + FEEDS `(id,pk)→value` maps — the operators' own input integrals).
    pub bytes_circuit_integral: usize,
    /// Split of `bytes_membership_circuit` (Task 1.3): the derived MEMBERS relation snapshot
    /// (`(node,value)`), published for `contains`/introspection reads.
    pub bytes_circuit_snapshots: usize,
    /// The global pk dictionary (Task 2.1): once-per-distinct-pk string storage + forward/reverse
    /// index. Append-only (no eviction in v1); reported so the string-interning trade is visible.
    pub bytes_pk_dict: usize,
    pub bytes_electric_adapter: usize,
}

/// Engine-internal cardinalities, computed from in-memory state by [`crate::engine::Engine::mem_cardinalities`].
#[derive(Clone, Default, serde::Serialize)]
pub struct Cardinalities {
    pub shapes: usize,
    /// Shapes currently dormant (retention lifecycle: stream retained, engine state dropped).
    pub shapes_dormant: usize,
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
    /// `ShapeRecord`s in the shape registry (`Engine.state.shapes`).
    pub bytes_shape_records: usize,
    /// Per-table executor structures: standalone shapes + their conjunct index, family routers
    /// (`RoutedShape`s), aggregate folds + their conjunct index.
    pub bytes_executors: usize,
    /// Per-shape retention lifecycle records (`Engine.lives`) — dominated by dormant shapes'
    /// resume offsets + snapshot gates.
    pub bytes_retention: usize,
    /// The cross-table subquery registry's own structures: nodes, templates (incl. their
    /// `pk_nodes` inverted index), shapes, and edges — excludes the membership circuit itself
    /// (see `bytes_membership_circuit`).
    pub bytes_subquery_registry: usize,
    /// Measured owned/on-disk bytes of the dbsp membership circuit's published snapshots
    /// (subquery inner sets + per-feed key sets), via dbsp's `BatchReader::approximate_byte_size`
    /// (exact in-memory columnar bytes when resident, on-disk file size when spilled). Equals
    /// `bytes_circuit_integral + bytes_circuit_snapshots`. See `SubqueryRegistry::circuit_bytes`.
    /// NOTE: this covers only the host-published snapshots (which share the operators' own
    /// integrals via dbsp's trace cache); dbsp's non-published incremental state (z1 delayed
    /// traces, `distinct` integrals) roughly doubles it and is measurable only via the profiler.
    pub bytes_membership_circuit: usize,
    /// Raw upsert-map integrals term of `bytes_membership_circuit` (CONTRIBUTORS + FEEDS maps).
    pub bytes_circuit_integral: usize,
    /// Derived MEMBERS relation snapshot term of `bytes_membership_circuit`.
    pub bytes_circuit_snapshots: usize,
    /// The global pk dictionary (Task 2.1): amortized once-per-distinct-pk string storage plus its
    /// forward/reverse index — the append-only cost of keying the circuit by `u32` pk ids instead
    /// of heap strings. See `SubqueryRegistry::pk_dict_bytes`.
    pub bytes_pk_dict: usize,
    /// The `/v1/shape` (Electric-protocol) adapter's TTL handle registry: per-handle cursor
    /// state (known-keys sets, in-flight live-poll map).
    pub bytes_electric_adapter: usize,
}

impl Cardinalities {
    /// Fold in the on-demand byte-level terms computed by `Engine::mem_bytes`. The only caller is
    /// the `/memory` HTTP handler — the counts alone (as returned by `mem_cardinalities`, all
    /// `bytes_*` left at their `Default` zero) are what the 500ms background sampler publishes.
    pub fn with_bytes(mut self, bytes: HeapBytes) -> Self {
        self.bytes_shape_records = bytes.bytes_shape_records;
        self.bytes_executors = bytes.bytes_executors;
        self.bytes_retention = bytes.bytes_retention;
        self.bytes_subquery_registry = bytes.bytes_subquery_registry;
        self.bytes_membership_circuit = bytes.bytes_membership_circuit;
        self.bytes_circuit_integral = bytes.bytes_circuit_integral;
        self.bytes_circuit_snapshots = bytes.bytes_circuit_snapshots;
        self.bytes_pk_dict = bytes.bytes_pk_dict;
        self.bytes_electric_adapter = bytes.bytes_electric_adapter;
        self
    }
}

/// Lock-free snapshot the OTel gauge callbacks and `/memory` read. Updated by the sampler and on demand.
#[derive(Default)]
struct Gauges {
    rss_bytes: AtomicU64,
    virtual_bytes: AtomicU64,
    shapes: AtomicU64,
    shapes_dormant: AtomicU64,
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
    g.shapes_dormant.store(card.shapes_dormant as u64, Ordering::Relaxed);
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
///
/// The `bytes_*` fields are read directly from `card` (the just-computed snapshot), not from
/// [`Gauges`] — they are JSON-only (no OTel gauge), so there is nothing published to read back;
/// every other field mirrors the last [`publish`] call, same as before.
pub fn snapshot_json(card: &Cardinalities) -> serde_json::Value {
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
            "shapes_dormant": g.shapes_dormant.load(Ordering::Relaxed),
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
            "bytes_shape_records": card.bytes_shape_records,
            "bytes_executors": card.bytes_executors,
            "bytes_retention": card.bytes_retention,
            "bytes_subquery_registry": card.bytes_subquery_registry,
            "bytes_membership_circuit": card.bytes_membership_circuit,
            "bytes_circuit_integral": card.bytes_circuit_integral,
            "bytes_circuit_snapshots": card.bytes_circuit_snapshots,
            "bytes_pk_dict": card.bytes_pk_dict,
            "bytes_electric_adapter": card.bytes_electric_adapter,
        },
        "samples": g.samples.load(Ordering::Relaxed),
    })
}

/// Shape counts from the last published cardinality snapshot (refreshed by the background sampler):
/// `(total, family_shapes, standalone)`. Used by the StatsD periodic sampler for the
/// `electric.shapes.*` gauges without re-locking engine state on the poll path.
pub fn published_shape_counts() -> (u64, u64, u64) {
    let g = gauges();
    (
        g.shapes.load(Ordering::Relaxed),
        g.family_shapes.load(Ordering::Relaxed),
        g.standalone.load(Ordering::Relaxed),
    )
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
    gauge!("engine_shapes_dormant", "Dormant shapes (retention: stream retained, engine state dropped)", shapes_dormant, "");
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
///
/// Deliberately calls `mem_cardinalities` only — cheap counts, no `heap_bytes` walk, no
/// `SequencerCmd::MemBytes` round-trip. Do not change this to call `mem_bytes` (or any function
/// that does): that byte-level walk is on-demand-only, reserved for `GET /memory` (see the module
/// doc comment above and `Engine::mem_bytes`'s doc comment for why).
pub fn spawn_sampler(engine: crate::engine::Engine, interval: Duration) {
    tokio::spawn(async move {
        loop {
            let card = engine.mem_cardinalities().await;
            publish(&card);
            tokio::time::sleep(interval).await;
        }
    });
}
