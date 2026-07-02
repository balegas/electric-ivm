//! StatsD telemetry — the benchmarking-fleet's only metrics channel (see `docs/fleet-conformance.md`
//! §4). Emits the `TelemetryMetricsStatsd` **datadog** wire format so the fleet's UDP StatsD server
//! stores our metrics:
//!
//! ```text
//! <dot.separated.name>:<value>|<type>|#instance_id:<id>[,tag:val...]
//! ```
//!
//! Types: counter → `1|c` (one packet per event), sum → `<v>|c`, gauge/last_value → `<v>|g`,
//! distribution → `<v>|d` (one packet per observation). **Every** metric carries the `instance_id`
//! tag — the fleet silently drops metrics without it. Lines are batched newline-separated into UDP
//! datagrams ≤ 1432 bytes.
//!
//! Emission is non-blocking on the hot path: [`Statsd::send_line`] does a `try_send` onto a bounded
//! channel drained by a background sender task; on overflow it drops the metric and counts the drop
//! (logging occasionally) rather than blocking a request or the ingest loop.
//!
//! Only genuinely-measured values are emitted — a metric we cannot honestly measure on this platform
//! is omitted, never faked (conformance §4a).

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use crate::config::StatsdTarget;

/// Max UDP datagram payload we pack lines into (fleet contract: keep datagrams < 1432 bytes).
const MAX_DATAGRAM: usize = 1432;
/// Bounded emission channel depth; overflow drops (counted) so hot paths never block on the network.
const CHANNEL_CAP: usize = 65_536;

// ---- pure formatting (unit-tested) ------------------------------------------------------------

/// Format an f64 as a plain decimal with no exponent (the fleet parses values as f64; a `1e-7`-style
/// string would still parse, but we keep it plain and integral-when-whole to match Elixir's output).
pub fn fmt_num(v: f64) -> String {
    if !v.is_finite() {
        return "0".to_string(); // never emit NaN/Infinity
    }
    if v == v.trunc() && v.abs() < 1e15 {
        return format!("{}", v as i64);
    }
    // Rust's f64 Display never uses scientific notation; guard anyway.
    let s = format!("{v}");
    if s.contains('e') || s.contains('E') { format!("{v:.6}") } else { s }
}

/// Build one datadog-format metric line. `tags` are appended after the mandatory `instance_id` tag.
pub fn format_metric(instance_id: &str, name: &str, value: &str, ty: &str, tags: &[(&str, &str)]) -> String {
    let mut s = String::with_capacity(96);
    s.push_str(name);
    s.push(':');
    s.push_str(value);
    s.push('|');
    s.push_str(ty);
    s.push_str("|#instance_id:");
    s.push_str(instance_id);
    for (k, v) in tags {
        s.push(',');
        s.push_str(k);
        s.push(':');
        s.push_str(v);
    }
    s
}

/// Pack newline-joined lines into datagrams each ≤ `max` bytes. A single line longer than `max` is
/// emitted alone (oversized) rather than dropped — our lines are ~100 bytes, so this never happens in
/// practice, but the boundary is exercised by tests.
pub fn batch_lines(lines: &[String], max: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for line in lines {
        if !cur.is_empty() && cur.len() + 1 + line.len() > max {
            out.push(std::mem::take(&mut cur));
        }
        if cur.is_empty() {
            cur.push_str(line);
        } else {
            cur.push('\n');
            cur.push_str(line);
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

// ---- client -----------------------------------------------------------------------------------

/// A connected StatsD client: formats + enqueues metric lines; a background task batches them into
/// UDP datagrams and sends them.
pub struct Statsd {
    tx: mpsc::Sender<String>,
    instance_id: String,
    drops: AtomicU64,
}

impl Statsd {
    /// Bind an ephemeral UDP socket connected to `addr` and spawn the sender task. Must be called from
    /// within a Tokio runtime.
    pub fn connect(addr: &str, instance_id: impl Into<String>) -> std::io::Result<Statsd> {
        let sock = std::net::UdpSocket::bind("0.0.0.0:0")?;
        sock.connect(addr)?; // resolves the host once; subsequent sends go here
        sock.set_nonblocking(true)?;
        let sock = tokio::net::UdpSocket::from_std(sock)?;
        let (tx, rx) = mpsc::channel::<String>(CHANNEL_CAP);
        tokio::spawn(sender_task(rx, sock));
        Ok(Statsd { tx, instance_id: instance_id.into(), drops: AtomicU64::new(0) })
    }

    fn send_line(&self, line: String) {
        if self.tx.try_send(line).is_err() {
            let n = self.drops.fetch_add(1, Ordering::Relaxed) + 1;
            if n.is_multiple_of(10_000) {
                tracing::warn!("statsd: dropped {n} metrics (emission channel full)");
            }
        }
    }

    fn emit(&self, name: &str, value: &str, ty: &str, tags: &[(&str, &str)]) {
        self.send_line(format_metric(&self.instance_id, name, value, ty, tags));
    }

    /// A counter event: `name:1|c`.
    pub fn incr(&self, name: &str, tags: &[(&str, &str)]) {
        self.emit(name, "1", "c", tags);
    }
    /// A summed counter: `name:<v>|c`.
    pub fn count(&self, name: &str, v: u64, tags: &[(&str, &str)]) {
        self.emit(name, &v.to_string(), "c", tags);
    }
    /// A gauge / last_value: `name:<v>|g`.
    pub fn gauge(&self, name: &str, v: f64, tags: &[(&str, &str)]) {
        self.emit(name, &fmt_num(v), "g", tags);
    }
    /// A distribution observation: `name:<v>|d`.
    pub fn dist(&self, name: &str, v: f64, tags: &[(&str, &str)]) {
        self.emit(name, &fmt_num(v), "d", tags);
    }
}

/// Drain the emission channel greedily, batch into datagrams, and send. Blocking on `recv` when idle;
/// under load each wakeup drains everything queued so far and packs it into ≤1432-byte datagrams.
async fn sender_task(mut rx: mpsc::Receiver<String>, sock: tokio::net::UdpSocket) {
    while let Some(first) = rx.recv().await {
        let mut lines = vec![first];
        while let Ok(l) = rx.try_recv() {
            lines.push(l);
            if lines.len() >= 4096 {
                break; // bound the batch so one wakeup can't hoard unbounded memory
            }
        }
        for dg in batch_lines(&lines, MAX_DATAGRAM) {
            let _ = sock.send(dg.as_bytes()).await;
        }
    }
}

// ---- process globals --------------------------------------------------------------------------

static STATSD: OnceLock<Option<Statsd>> = OnceLock::new();
static START: OnceLock<Instant> = OnceLock::new();

/// Record the process start instant (for `vm.uptime.total` and `consumers_ready.duration`). Idempotent.
pub fn mark_start() {
    let _ = START.set(Instant::now());
}

fn since_start() -> Duration {
    START.get().map(|t| t.elapsed()).unwrap_or_default()
}

/// Initialize the global StatsD client from a resolved target. A connect failure disables StatsD
/// (logged) rather than crashing the boot. Idempotent.
pub fn init(target: &StatsdTarget, instance_id: &str) {
    let client = match Statsd::connect(&target.addr(), instance_id) {
        Ok(c) => {
            tracing::info!("statsd: emitting to {} (instance_id={instance_id})", target.addr());
            Some(c)
        }
        Err(e) => {
            tracing::error!("statsd: cannot connect to {}: {e}; telemetry disabled", target.addr());
            None
        }
    };
    let _ = STATSD.set(client);
}

/// The global client, or `None` if StatsD is off (no `ELECTRIC_STATSD_HOST`).
pub fn statsd() -> Option<&'static Statsd> {
    STATSD.get().and_then(|o| o.as_ref())
}

/// Is StatsD emission active? Used to skip expensive per-metric accounting on hot paths when off.
pub fn enabled() -> bool {
    statsd().is_some()
}

// ---- high-level instrumentation (call sites stay terse; no-op when StatsD is off) --------------

/// Every `/v1/shape` response (see conformance §4b). `elapsed` is the whole handler time; `body_bytes`
/// is the response body size.
pub fn serve_shape(root_table: &str, live: bool, status: u16, elapsed: Duration, body_bytes: u64) {
    let Some(s) = statsd() else { return };
    let ms = elapsed.as_secs_f64() * 1000.0;
    let status_s = status.to_string();
    let live_s = if live { "true" } else { "false" };
    // 4xx (incl. 409) are client/known errors; 5xx are unknown/internal.
    let known_s = if (400..500).contains(&status) { "true" } else { "false" };

    s.dist("plug.router_dispatch.stop.duration", ms, &[("route", "/v1/shape"), ("status", status_s.as_str())]);
    s.incr(
        "electric.plug.serve_shape.requests.count",
        &[("status", status_s.as_str()), ("known_error", known_s), ("live", live_s)],
    );
    s.dist(
        "electric.shape.response_size.bytes",
        body_bytes as f64,
        &[("root_table", root_table), ("is_live", live_s), ("stack_id", crate::config::stack_id())],
    );
    if !live {
        s.dist("electric.plug.serve_shape.duration", ms, &[]);
        s.incr("electric.plug.serve_shape.count", &[]);
        s.count("electric.plug.serve_shape.bytes", body_bytes, &[]);
    }
}

/// Per committed replicated transaction (see conformance §4b). `receive_lag_ms` is ingest-side latency
/// (see the call site in `replication.rs` for exactly what it measures).
pub fn replication_txn(ops: u64, bytes: u64, receive_lag_ms: f64) {
    let Some(s) = statsd() else { return };
    s.incr("electric.postgres.replication.transaction_received.count", &[]);
    s.count("electric.postgres.replication.transaction_received.bytes", bytes, &[]);
    s.dist("electric.postgres.replication.transaction_received.operations", ops as f64, &[]);
    s.dist("electric.postgres.replication.transaction_received.receive_lag", receive_lag_ms, &[]);
}

/// Per source transaction whose changes were appended to shape streams (see conformance §4b).
pub fn storage_txn(ops: u64, bytes: u64, affected_shapes: u64) {
    let Some(s) = statsd() else { return };
    s.incr("electric.storage.transaction_stored.count", &[]);
    s.count("electric.storage.transaction_stored.bytes", bytes, &[]);
    s.count("electric.storage.transaction_stored.operations", ops, &[]);
    s.dist("electric.shape_log_collector.transaction.affected_shape_count", affected_shapes as f64, &[]);
}

/// Per completed shape backfill/snapshot (see conformance §4b). `make_new_ms` is the backfill query time.
pub fn snapshot_stored(rows: u64, bytes: u64, make_new_ms: f64) {
    let Some(s) = statsd() else { return };
    s.incr("electric.storage.snapshot_stored.count", &[]);
    s.count("electric.storage.snapshot_stored.bytes", bytes, &[]);
    s.count("electric.storage.snapshot_stored.operations", rows, &[]);
    s.dist("electric.storage.make_new_snapshot.stop.duration", make_new_ms, &[]);
}

/// The whole shape-creation task duration (backfill + registration), emitted by the creator only.
pub fn create_snapshot_task(elapsed: Duration) {
    let Some(s) = statsd() else { return };
    s.dist("electric.shape_snapshot.create_snapshot_task.stop.duration", elapsed.as_secs_f64() * 1000.0, &[]);
}

/// Boot-to-ready, emitted once when the engine becomes active.
pub fn consumers_ready(tables: u64) {
    let Some(s) = statsd() else { return };
    s.gauge("electric.connection.consumers_ready.duration", since_start().as_secs_f64() * 1000.0, &[]);
    s.gauge("electric.connection.consumers_ready.total", tables as f64, &[]);
}

/// Current durable-streams storage size (file mode; `du` of `ELECTRIC_STORAGE_DIR`) plus how long the
/// `du` took (matches Electric emitting `used.bytes` + `used.measurement_duration` together).
pub fn storage_used(bytes: u64, measurement: Duration) {
    let Some(s) = statsd() else { return };
    s.gauge("electric.storage.used.bytes", bytes as f64, &[]);
    s.dist("electric.storage.used.measurement_duration", measurement.as_secs_f64() * 1000.0, &[]);
}

/// Shape-count gauges (conformance §5 / baseline dashboard headline metrics), emitted every poll tick.
/// `indexed`/`unindexed` map to our shared-family vs standalone evaluation split — the honest analog of
/// Electric's indexed-where-clause distinction. Every registered shape is actively maintained in our
/// engine, so `active_shapes == total_shapes` (a true statement about this engine, not a copy of total).
pub fn shape_gauges(total: u64, indexed: u64, unindexed: u64) {
    let Some(s) = statsd() else { return };
    s.gauge("electric.shapes.total_shapes.count", total as f64, &[]);
    s.gauge("electric.shapes.active_shapes.count", total as f64, &[]);
    s.gauge("electric.shapes.total_shapes.count_indexed", indexed as f64, &[]);
    s.gauge("electric.shapes.total_shapes.count_unindexed", unindexed as f64, &[]);
}

/// Compute the replication-slot WAL gauges from raw LSN strings (pure, unit-tested). `restart` and
/// `confirmed` are `Option` because a freshly-created slot can report NULL for them — a missing value
/// omits its delta metric rather than emitting a fake/stale one. `pg_wal_offset` is always present.
pub fn slot_gauge_values(wal: &str, restart: Option<&str>, confirmed: Option<&str>) -> Vec<(&'static str, f64)> {
    let wal_u = crate::pg::lsn_to_u64(wal);
    let mut out = vec![("electric.postgres.replication.pg_wal_offset", wal_u as f64)];
    if let Some(r) = restart {
        let r = crate::pg::lsn_to_u64(r);
        out.push(("electric.postgres.replication.slot_retained_wal_size", wal_u.saturating_sub(r) as f64));
    }
    if let Some(c) = confirmed {
        let c = crate::pg::lsn_to_u64(c);
        out.push(("electric.postgres.replication.slot_confirmed_flush_lsn_lag", wal_u.saturating_sub(c) as f64));
    }
    out
}

/// Emit the replication-slot WAL gauges for one sample (`wal` = pg_current_wal_lsn, `restart`/`confirmed`
/// from `pg_replication_slots` for our slot). No-op when StatsD is off.
pub fn replication_slot_gauges(wal: &str, restart: Option<&str>, confirmed: Option<&str>) {
    let Some(s) = statsd() else { return };
    for (name, v) in slot_gauge_values(wal, restart, confirmed) {
        s.gauge(name, v, &[]);
    }
}

// ---- periodic samplers ------------------------------------------------------------------------

/// Spawn the periodic system-metrics sampler (conformance §4a). No-op when StatsD is off.
pub fn spawn_system_sampler(period: Duration) {
    if !enabled() {
        return;
    }
    tokio::spawn(system_sampler(period));
}

async fn system_sampler(period: Duration) {
    use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System};

    let Some(s) = statsd() else { return };
    // available_parallelism honors the process's CPU affinity / cgroup quota on Linux where set.
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1) as f64;
    let pid = sysinfo::get_current_pid().ok();
    // Refresh only our own process, and only its CPU. `new()` + targeted process refresh does NOT
    // populate process CPU (verified: it reads 0 forever); `new_all()` seeds the per-process CPU-time
    // baseline that the delta is computed against. remove_dead=false keeps our entry across refreshes.
    let proc_cpu = ProcessRefreshKind::nothing().with_cpu();

    let mut sys = System::new_all();
    // CRITICAL (proven in electrustic): refresh CPU before the process, and take TWO samples spaced at
    // least sysinfo's minimum interval before CPU deltas are trustworthy. Prime here; the first loop
    // iteration is the second sample.
    sys.refresh_cpu_all();
    if let Some(pid) = pid {
        sys.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), false, proc_cpu);
    }
    tokio::time::sleep(period.max(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL)).await;

    loop {
        sys.refresh_cpu_all();
        sys.refresh_memory();
        if let Some(pid) = pid {
            sys.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), false, proc_cpu);
        }

        s.gauge("system.cpu.core_count", cores, &[]);
        s.gauge("system.cpu.utilization.total", sys.global_cpu_usage() as f64, &[]);
        for (i, cpu) in sys.cpus().iter().enumerate() {
            s.gauge(&format!("system.cpu.utilization.core_{i}"), cpu.cpu_usage() as f64, &[]);
        }

        let la = System::load_average();
        if cores > 0.0 {
            s.gauge("system.load_percent.avg1", 100.0 * la.one / cores, &[]);
            s.gauge("system.load_percent.avg5", 100.0 * la.five / cores, &[]);
            s.gauge("system.load_percent.avg15", 100.0 * la.fifteen / cores, &[]);
        }

        let total = sys.total_memory();
        if total > 0 {
            let pct = |x: u64| 100.0 * x as f64 / total as f64;
            s.gauge("system.memory_percent.free_memory", pct(sys.free_memory()), &[]);
            s.gauge("system.memory_percent.available_memory", pct(sys.available_memory()), &[]);
            s.gauge("system.memory_percent.used_memory", pct(sys.used_memory()), &[]);
        }

        // vm.memory.total = process RSS bytes (memory-stats, matching mem.rs).
        let (rss, _virt) = crate::mem::process_memory();
        if rss > 0 {
            s.gauge("vm.memory.total", rss as f64, &[]);
        }

        s.gauge("vm.uptime.total", since_start().as_secs_f64(), &[]);

        // vm.scheduler_utilization.total = process CPU% ÷ cores, clamped 0-100. sysinfo reports
        // process CPU where 100 == one fully-used core.
        if let Some(pid) = pid
            && let Some(proc_) = sys.process(pid)
            && cores > 0.0
        {
            let util = (proc_.cpu_usage() as f64 / cores).clamp(0.0, 100.0);
            s.gauge("vm.scheduler_utilization.total", util, &[]);
        }

        // vm.total_run_queue_lengths.total = tokio global injection-queue depth (stable metric).
        s.gauge(
            "vm.total_run_queue_lengths.total",
            tokio::runtime::Handle::current().metrics().global_queue_depth() as f64,
            &[],
        );

        // vm.system_counts.process_count = OS thread count of the process (Linux only; omitted else).
        if let Some(threads) = os_thread_count() {
            s.gauge("vm.system_counts.process_count", threads as f64, &[]);
        }

        // Shape-count gauges from the engine's cardinality snapshot (published by the mem sampler).
        let (total, indexed, unindexed) = crate::mem::published_shape_counts();
        shape_gauges(total, indexed, unindexed);

        tokio::time::sleep(period).await;
    }
}

/// OS thread count of this process. Linux: entries in `/proc/self/task`. Elsewhere unmeasured → `None`
/// (the metric is then omitted rather than faked).
fn os_thread_count() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_dir("/proc/self/task").ok().map(|d| d.count() as u64)
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Spawn the ~60s storage-size sampler. No-op when StatsD is off or `ELECTRIC_STORAGE_DIR` is unset.
/// (In durable-streams mode we don't own the storage dir; if the entrypoint points us at it we `du` it.)
pub fn spawn_storage_sampler(dir: Option<String>) {
    let Some(dir) = dir else { return };
    if !enabled() {
        return;
    }
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(60));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let t0 = Instant::now();
            let bytes = du_bytes(std::path::Path::new(&dir));
            let measurement = t0.elapsed();
            if let Some(bytes) = bytes {
                storage_used(bytes, measurement);
            }
        }
    });
}

/// Spawn the replication-slot WAL sampler on its own ~10s cadence (Electric's cadence for these), pg
/// mode only. Holds a single PG connection (reconnecting on failure). No-op when StatsD is off.
pub fn spawn_replication_slot_sampler(pg_url: String, slot: String) {
    if !enabled() {
        return;
    }
    tokio::spawn(replication_slot_sampler(pg_url, slot));
}

async fn replication_slot_sampler(pg_url: String, slot: String) {
    let mut tick = tokio::time::interval(Duration::from_secs(10));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut client: Option<tokio_postgres::Client> = None;
    let mut logged_err = false;
    loop {
        tick.tick().await;
        if client.is_none() {
            match crate::pg::connect(&pg_url).await {
                Ok(c) => {
                    client = Some(c);
                    logged_err = false;
                }
                Err(e) => {
                    if !logged_err {
                        tracing::warn!("statsd slot sampler: connect failed: {e:#}; will retry");
                        logged_err = true;
                    }
                    continue;
                }
            }
        }
        // pg_current_wal_lsn() (primary) + our slot's restart/confirmed LSNs. NULLs (fresh slot) are
        // reported as None -> the corresponding delta metric is omitted, never faked.
        let q = "select pg_current_wal_lsn()::text, restart_lsn::text, confirmed_flush_lsn::text \
                 from pg_replication_slots where slot_name = $1";
        match client.as_ref().unwrap().query_opt(q, &[&slot]).await {
            Ok(Some(row)) => {
                let wal: String = row.get(0);
                let restart: Option<String> = row.get(1);
                let confirmed: Option<String> = row.get(2);
                replication_slot_gauges(&wal, restart.as_deref(), confirmed.as_deref());
                logged_err = false;
            }
            Ok(None) => { /* slot not present yet — skip this tick */ }
            Err(e) => {
                if !logged_err {
                    tracing::warn!("statsd slot sampler: query failed: {e:#}; reconnecting");
                    logged_err = true;
                }
                client = None; // drop the (possibly dead) connection and reconnect next tick
            }
        }
    }
}

/// Recursive `du` (sum of regular-file sizes). Skips symlinks. `None` if the path does not exist.
fn du_bytes(root: &std::path::Path) -> Option<u64> {
    if !root.exists() {
        return None;
    }
    fn walk(p: &std::path::Path) -> u64 {
        let mut total = 0u64;
        let Ok(entries) = std::fs::read_dir(p) else { return 0 };
        for entry in entries.flatten() {
            let Ok(ft) = entry.file_type() else { continue };
            if ft.is_symlink() {
                continue;
            } else if ft.is_dir() {
                total += walk(&entry.path());
            } else if let Ok(md) = entry.metadata() {
                total += md.len();
            }
        }
        total
    }
    Some(walk(root))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fmt_num_plain_no_exponent() {
        assert_eq!(fmt_num(8.0), "8");
        assert_eq!(fmt_num(0.0), "0");
        assert_eq!(fmt_num(45.7), "45.7");
        assert_eq!(fmt_num(100.0), "100");
        assert_eq!(fmt_num(1234567.0), "1234567");
        // a value that a naive formatter might render with an exponent
        assert!(!fmt_num(0.0000001).contains('e'));
        assert!(!fmt_num(1e20).contains('e'));
        // never emit NaN/Inf
        assert_eq!(fmt_num(f64::NAN), "0");
        assert_eq!(fmt_num(f64::INFINITY), "0");
    }

    #[test]
    fn format_metric_all_types() {
        let id = "abc-123";
        assert_eq!(format_metric(id, "electric.x.count", "1", "c", &[]), "electric.x.count:1|c|#instance_id:abc-123");
        assert_eq!(format_metric(id, "electric.x.bytes", "512", "c", &[]), "electric.x.bytes:512|c|#instance_id:abc-123");
        assert_eq!(format_metric(id, "system.cpu.core_count", "8", "g", &[]), "system.cpu.core_count:8|g|#instance_id:abc-123");
        assert_eq!(
            format_metric(id, "plug.router_dispatch.stop.duration", "1.5", "d", &[("route", "/v1/shape"), ("status", "200")]),
            "plug.router_dispatch.stop.duration:1.5|d|#instance_id:abc-123,route:/v1/shape,status:200"
        );
    }

    #[test]
    fn every_line_carries_instance_id_first() {
        let l = format_metric("id7", "n", "1", "c", &[("a", "b")]);
        assert!(l.contains("|#instance_id:id7"));
        // instance_id must be the first tag
        let tags = l.split("|#").nth(1).unwrap();
        assert!(tags.starts_with("instance_id:id7"));
    }

    #[test]
    fn batch_respects_1432_boundary() {
        // Build enough lines to overflow several datagrams.
        let lines: Vec<String> = (0..500)
            .map(|i| format_metric("instance-abcdefgh", &format!("electric.metric.number_{i}"), "1", "c", &[("k", "v")]))
            .collect();
        let dgs = batch_lines(&lines, MAX_DATAGRAM);
        assert!(dgs.len() > 1, "should split into multiple datagrams");
        for dg in &dgs {
            assert!(dg.len() <= MAX_DATAGRAM, "datagram {} bytes exceeds {MAX_DATAGRAM}", dg.len());
        }
        // No line is lost or split: re-joining reproduces every line in order.
        let rejoined: Vec<&str> = dgs.iter().flat_map(|d| d.split('\n')).collect();
        assert_eq!(rejoined.len(), lines.len());
        for (a, b) in rejoined.iter().zip(lines.iter()) {
            assert_eq!(a, b);
        }
    }

    #[test]
    fn slot_gauge_values_compute_deltas() {
        // wal=0x20, restart=0x10, confirmed=0x18 -> retained=0x10, confirmed_lag=0x8.
        let v = slot_gauge_values("0/20", Some("0/10"), Some("0/18"));
        assert_eq!(v[0], ("electric.postgres.replication.pg_wal_offset", 0x20 as f64));
        assert_eq!(v[1], ("electric.postgres.replication.slot_retained_wal_size", 0x10 as f64));
        assert_eq!(v[2], ("electric.postgres.replication.slot_confirmed_flush_lsn_lag", 0x8 as f64));
    }

    #[test]
    fn slot_gauge_values_omit_missing_lsns() {
        // A fresh slot with NULL restart/confirmed emits only pg_wal_offset (no faked deltas).
        let v = slot_gauge_values("0/20", None, None);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].0, "electric.postgres.replication.pg_wal_offset");
        // confirmed ahead of wal (shouldn't happen, but must not underflow/panic)
        let v = slot_gauge_values("0/10", Some("0/40"), None);
        assert_eq!(v[1], ("electric.postgres.replication.slot_retained_wal_size", 0.0));
    }

    #[test]
    fn batch_packs_tightly() {
        // Two short lines that fit together share one datagram.
        let a = "a:1|c|#instance_id:x".to_string();
        let b = "b:1|c|#instance_id:x".to_string();
        let dgs = batch_lines(&[a.clone(), b.clone()], MAX_DATAGRAM);
        assert_eq!(dgs, vec![format!("{a}\n{b}")]);
    }
}
