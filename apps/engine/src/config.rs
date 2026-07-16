//! Boot configuration resolved from the environment.
//!
//! The engine grew up on `ELECTRIC_CIRCUITS_*` vars (see `README.md`); the benchmarking-fleet drives the
//! image with Electric's own `ELECTRIC_*` / `DATABASE_URL` surface (see `docs/fleet-conformance.md`).
//! This module maps the fleet surface onto the engine, keeping the `ELECTRIC_CIRCUITS_*` vars as the
//! higher-precedence override so the existing dev/test workflow is unchanged. Resolution is a pure
//! function of an env getter ([`Config::resolve`]) so precedence is unit-testable without touching the
//! process environment.
//!
//! Unknown `ELECTRIC_*` vars are collected into [`Config::noop_vars`] and logged once as
//! "accepted (no-op)" — they must never crash the boot.

use std::time::Duration;

/// A StatsD destination (`host[:port]`, default port 8125).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatsdTarget {
    pub host: String,
    pub port: u16,
}

impl StatsdTarget {
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Fully-resolved boot configuration.
#[derive(Clone, Debug)]
pub struct Config {
    /// Postgres connection string (enables Postgres mode). `ELECTRIC_CIRCUITS_PG_URL` wins over `DATABASE_URL`.
    pub pg_url: Option<String>,
    /// Durable-streams base URL (`ELECTRIC_CIRCUITS_DS_URL`; required for a real run, set by the entrypoint).
    pub ds_url: Option<String>,
    /// HTTP bind address for the control plane + `/v1/shape` + `/v1/health`.
    pub bind: String,
    /// `tracing` EnvFilter string.
    pub log_filter: String,
    /// Logical-replication slot name.
    pub slot: String,
    /// Tables to replicate (`ELECTRIC_CIRCUITS_PG_TABLES`); empty / `["*"]` = introspect all.
    pub tables: Vec<String>,
    /// Legacy replication poll interval (ms). Unused since the ingestor streams pgoutput (push
    /// delivery); still parsed so existing `ELECTRIC_CIRCUITS_PG_POLL_MS` settings are accepted.
    pub poll_ms: u64,
    /// This instance's id — tags every StatsD metric.
    pub instance_id: String,
    /// The `stack_id` tag value on shape metrics: the replication stream id, or `single_stack`.
    pub stack_id: String,
    /// StatsD destination (absent → StatsD off).
    pub statsd: Option<StatsdTarget>,
    /// Period for the periodic system-metrics sampler.
    pub metrics_period: Duration,
    /// If set, `/v1/shape` requires a matching `secret`/`api_secret` query param.
    pub secret: Option<String>,
    /// Root dir of durable-streams file storage, for `electric.storage.used.bytes` (`du`).
    pub storage_dir: Option<String>,
    /// Optional second listener serving Prometheus text (`ELECTRIC_PROMETHEUS_PORT`).
    pub prometheus_port: Option<u16>,
    /// Max pooled Postgres connections for backfills/query-backs (`ELECTRIC_DB_POOL_SIZE`, default 20).
    pub db_pool_size: usize,
    /// Register the introspection surface (`/trace` SSE + `/graph`(`/node`) + `/state`(`/node`) —
    /// the pipeline-visualizer backend). `ELECTRIC_CIRCUITS_TRACE=0|false|off` disables it: the routes
    /// are never registered, so nothing can subscribe and the hot-path trace gating stays on its
    /// zero-subscriber fast path. Default on. Note: the surface is unauthenticated either way.
    pub trace: bool,
    /// dbsp-backed table arrangements (always built; see `arrangements.rs`). The circuit is
    /// mandatory infrastructure — the sub-knobs below tune it, but it can no longer be turned off.
    pub dbsp: DbspConfig,
    /// Unknown/unimplemented `ELECTRIC_*` vars, accepted as no-ops and logged once at boot.
    pub noop_vars: Vec<String>,
}

/// Settings for the dbsp arrangement layer (all under `ELECTRIC_CIRCUITS_DBSP*`).
#[derive(Clone, Debug)]
pub struct DbspConfig {
    /// State directory (`ELECTRIC_CIRCUITS_DBSP_DIR`; default
    /// `<ELECTRIC_STORAGE_DIR|./data>/dbsp/<slot>` — slot-keyed so parallel engines and
    /// different source databases never share dbsp state).
    pub dir: std::path::PathBuf,
    /// Storage-cache budget in MiB (`ELECTRIC_CIRCUITS_DBSP_CACHE_MIB`).
    pub cache_mib: Option<usize>,
    /// Spill threshold in KiB (`ELECTRIC_CIRCUITS_DBSP_MIN_STORAGE_KB`; default 1024 = 1 MiB;
    /// 0 spills everything eligible).
    pub min_storage_bytes: Option<usize>,
    /// Memory ceiling in MiB driving dbsp's pressure-based spilling (`ELECTRIC_CIRCUITS_DBSP_MAX_RSS_MB`).
    pub max_rss_bytes: Option<u64>,
    /// Checkpoint cadence in seconds (`ELECTRIC_CIRCUITS_DBSP_CHECKPOINT_SECS`; default 60; 0 = only
    /// at shutdown).
    pub checkpoint_every: Option<Duration>,
    /// Extra lookup indexes beyond the per-table primary key: `table.column[,table.column…]`
    /// (`ELECTRIC_CIRCUITS_DBSP_INDEXES`). Lookups against undeclared indexes fall back to Postgres.
    pub indexes: Vec<(String, String)>,
    /// Counts pipelines: `table:col+col[,table:col…]` (`ELECTRIC_CIRCUITS_DBSP_COUNTS`). The circuit
    /// maintains a live COUNT per distinct group projection; COUNT aggregates whose predicate
    /// decomposes over these columns are served from the groups.
    pub counts: Vec<(String, Vec<String>)>,
}

/// `ELECTRIC_*` vars the engine actually reads and acts on. Anything else matching `^ELECTRIC_`
/// (and not the internal `ELECTRIC_CIRCUITS_*` namespace) is an accepted no-op.
const HANDLED: &[&str] = &[
    "ELECTRIC_PORT",
    "ELECTRIC_INSTANCE_ID",
    "ELECTRIC_STATSD_HOST",
    "ELECTRIC_SYSTEM_METRICS_POLL_INTERVAL",
    "ELECTRIC_INSECURE",
    "ELECTRIC_SECRET",
    "ELECTRIC_STORAGE_DIR",
    "ELECTRIC_LOG_LEVEL",
    "ELECTRIC_REPLICATION_STREAM_ID",
    "ELECTRIC_LIVE_TIMEOUT_MS",
    "ELECTRIC_HANDLE_TTL",
    "ELECTRIC_PROMETHEUS_PORT",
    "ELECTRIC_DB_POOL_SIZE",
];

fn nonempty(s: Option<String>) -> Option<String> {
    s.filter(|v| !v.trim().is_empty())
}

/// Parse a human-readable duration (`5s`, `200ms`, `1m`, `2h`) or a bare integer (milliseconds).
/// Returns `None` on any parse failure so the caller can fall through to the next source.
pub fn parse_human_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (num, unit): (&str, &str) = if let Some(p) = s.strip_suffix("ms") {
        (p, "ms")
    } else if let Some(p) = s.strip_suffix('s') {
        (p, "s")
    } else if let Some(p) = s.strip_suffix('m') {
        (p, "m")
    } else if let Some(p) = s.strip_suffix('h') {
        (p, "h")
    } else {
        (s, "ms") // bare integer == milliseconds
    };
    let n: f64 = num.trim().parse().ok()?;
    if !n.is_finite() || n < 0.0 {
        return None;
    }
    let ms = match unit {
        "ms" => n,
        "s" => n * 1_000.0,
        "m" => n * 60_000.0,
        "h" => n * 3_600_000.0,
        _ => return None,
    };
    Some(Duration::from_millis(ms as u64))
}

impl Config {
    /// Resolve configuration from an env getter. Pure (no process-env access) so precedence is testable.
    pub fn resolve(get: impl Fn(&str) -> Option<String>) -> Config {
        let g = |k: &str| nonempty(get(k));

        // Postgres URL: our internal var wins, then the fleet's DATABASE_URL.
        let pg_url = g("ELECTRIC_CIRCUITS_PG_URL").or_else(|| g("DATABASE_URL"));
        let ds_url = g("ELECTRIC_CIRCUITS_DS_URL");

        // Bind address. ELECTRIC_CIRCUITS_BIND always wins (preserves 127.0.0.1:0 dev behavior). Otherwise,
        // if the fleet surface is present (ELECTRIC_PORT or DATABASE_URL) bind 0.0.0.0:<port|3000>.
        let bind = if let Some(b) = g("ELECTRIC_CIRCUITS_BIND") {
            b
        } else if let Some(port) = g("ELECTRIC_PORT") {
            format!("0.0.0.0:{}", port.trim())
        } else if pg_url.is_some() {
            "0.0.0.0:3000".to_string()
        } else {
            "127.0.0.1:0".to_string()
        };

        // Log filter: ELECTRIC_CIRCUITS_LOG (a raw EnvFilter) wins; else map ELECTRIC_LOG_LEVEL; else info.
        let log_filter = g("ELECTRIC_CIRCUITS_LOG").unwrap_or_else(|| match g("ELECTRIC_LOG_LEVEL").as_deref() {
            Some("error") => "error".into(),
            Some("warning") | Some("warn") => "warn".into(),
            Some("debug") => "debug".into(),
            Some("info") => "info".into(),
            _ => "info".into(),
        });

        // Slot name: ELECTRIC_CIRCUITS_PG_SLOT wins; else electric_slot_<stream id>; else the legacy default.
        let stream_id = g("ELECTRIC_REPLICATION_STREAM_ID");
        let slot = g("ELECTRIC_CIRCUITS_PG_SLOT").unwrap_or_else(|| match &stream_id {
            Some(id) => format!("electric_slot_{id}"),
            None => "electric_circuits".to_string(),
        });

        let tables: Vec<String> = g("ELECTRIC_CIRCUITS_PG_TABLES")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let poll_ms = g("ELECTRIC_CIRCUITS_PG_POLL_MS").and_then(|s| s.trim().parse().ok()).unwrap_or(50);

        let instance_id = g("ELECTRIC_INSTANCE_ID").unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let stack_id = stream_id.clone().unwrap_or_else(|| "single_stack".to_string());

        let statsd = g("ELECTRIC_STATSD_HOST").map(|h| {
            let h = h.trim();
            match h.rsplit_once(':') {
                Some((host, port)) if port.parse::<u16>().is_ok() => {
                    StatsdTarget { host: host.to_string(), port: port.parse().unwrap() }
                }
                _ => StatsdTarget { host: h.to_string(), port: 8125 },
            }
        });

        // ELECTRIC_SYSTEM_METRICS_POLL_INTERVAL (Electric's spelling, human duration) wins over the
        // fleet's TELEMETRY_POLLER_PERIOD (bare ms); default 5s (Electric's default).
        let metrics_period = g("ELECTRIC_SYSTEM_METRICS_POLL_INTERVAL")
            .and_then(|s| parse_human_duration(&s))
            .or_else(|| g("TELEMETRY_POLLER_PERIOD").and_then(|s| parse_human_duration(&s)))
            .unwrap_or_else(|| Duration::from_secs(5));

        // ELECTRIC_INSECURE is accepted; it is a no-op unless a secret is also set (then it does not
        // override the secret — an explicit secret always takes effect).
        let secret = g("ELECTRIC_SECRET");

        let storage_dir = g("ELECTRIC_STORAGE_DIR");
        let prometheus_port = g("ELECTRIC_PROMETHEUS_PORT").and_then(|s| s.trim().parse().ok());
        let db_pool_size = g("ELECTRIC_DB_POOL_SIZE")
            .and_then(|s| s.trim().parse::<usize>().ok())
            .filter(|n| *n >= 1)
            .unwrap_or(20);

        let trace = g("ELECTRIC_CIRCUITS_TRACE")
            .map(|s| !matches!(s.trim().to_ascii_lowercase().as_str(), "0" | "false" | "off"))
            .unwrap_or(true);

        // The dbsp arrangement circuit is always built — it is mandatory infrastructure, no longer
        // gated by an on/off flag. The knobs below only tune it (state dir, cache/spill budgets,
        // extra indexes, counts pipelines); empty `_INDEXES`/`_COUNTS` are valid (the circuit still
        // builds per-table primary-key arrangements).
        let dbsp = DbspConfig {
            // Default dir is keyed by the replication slot: dbsp state is only valid for the
            // database identity it was built from, and parallel engines (conformance harnesses)
            // get disjoint state dirs for free.
            dir: g("ELECTRIC_CIRCUITS_DBSP_DIR").map(std::path::PathBuf::from).unwrap_or_else(|| {
                std::path::Path::new(storage_dir.as_deref().unwrap_or("./data")).join("dbsp").join(&slot)
            }),
            cache_mib: g("ELECTRIC_CIRCUITS_DBSP_CACHE_MIB").and_then(|s| s.trim().parse().ok()),
            min_storage_bytes: Some(
                g("ELECTRIC_CIRCUITS_DBSP_MIN_STORAGE_KB")
                    .and_then(|s| s.trim().parse::<usize>().ok())
                    .unwrap_or(1024)
                    * 1024,
            ),
            max_rss_bytes: g("ELECTRIC_CIRCUITS_DBSP_MAX_RSS_MB")
                .and_then(|s| s.trim().parse::<u64>().ok())
                .map(|mb| mb * 1024 * 1024),
            checkpoint_every: match g("ELECTRIC_CIRCUITS_DBSP_CHECKPOINT_SECS").and_then(|s| s.trim().parse::<u64>().ok())
            {
                Some(0) => None,
                Some(s) => Some(Duration::from_secs(s)),
                None => Some(Duration::from_secs(60)),
            },
            indexes: g("ELECTRIC_CIRCUITS_DBSP_INDEXES")
                .unwrap_or_default()
                .split(',')
                .filter_map(|s| {
                    let (t, c) = s.trim().split_once('.')?;
                    Some((t.trim().to_string(), c.trim().to_string()))
                })
                .collect(),
            counts: g("ELECTRIC_CIRCUITS_DBSP_COUNTS")
                .unwrap_or_default()
                .split(',')
                .filter_map(|s| {
                    let (t, cols) = s.trim().split_once(':')?;
                    let cols: Vec<String> = cols
                        .split('+')
                        .map(|c| c.trim().to_string())
                        .filter(|c| !c.is_empty())
                        .collect();
                    if cols.is_empty() { None } else { Some((t.trim().to_string(), cols)) }
                })
                .collect(),
        };

        Config {
            pg_url,
            ds_url,
            bind,
            log_filter,
            slot,
            tables,
            poll_ms,
            instance_id,
            stack_id,
            statsd,
            metrics_period,
            secret,
            storage_dir,
            prometheus_port,
            db_pool_size,
            trace,
            dbsp,
            noop_vars: Vec::new(),
        }
    }

    /// Resolve from the real process environment, then scan it for accepted-no-op `ELECTRIC_*` vars.
    pub fn from_env() -> Config {
        let mut cfg = Config::resolve(|k| std::env::var(k).ok());
        cfg.noop_vars = std::env::vars()
            .map(|(k, _)| k)
            .filter(|k| is_noop_var(k))
            .collect();
        cfg.noop_vars.sort();
        cfg
    }

    /// The bind host:port with the `DATABASE_URL`/`ELECTRIC_SECRET` credentials redacted — safe to log.
    pub fn redacted(&self) -> String {
        format!(
            "bind={} pg_url={} ds_url={} slot={} instance_id={} stack_id={} statsd={} metrics_period={:?} \
             secret={} storage_dir={} prometheus_port={:?} trace={} log={}",
            self.bind,
            self.pg_url.as_deref().map(redact_url).unwrap_or_else(|| "<none>".into()),
            self.ds_url.as_deref().unwrap_or("<none>"),
            self.slot,
            self.instance_id,
            self.stack_id,
            self.statsd.as_ref().map(|s| s.addr()).unwrap_or_else(|| "<off>".into()),
            self.metrics_period,
            if self.secret.is_some() { "<redacted>" } else { "<none>" },
            self.storage_dir.as_deref().unwrap_or("<none>"),
            self.prometheus_port,
            self.trace,
            self.log_filter,
        )
    }
}

/// Is `k` an `ELECTRIC_*` var the engine does not act on (so it should be accepted as a no-op)?
/// Internal `ELECTRIC_CIRCUITS_*` vars are ours (handled) and never counted here.
pub fn is_noop_var(k: &str) -> bool {
    k.starts_with("ELECTRIC_") && !k.starts_with("ELECTRIC_CIRCUITS_") && !HANDLED.contains(&k)
}

/// Redact `user:pass@` credentials from a Postgres/URL connection string for logging.
fn redact_url(url: &str) -> String {
    // scheme://user:pass@host/... -> scheme://***@host/...
    match url.split_once("://") {
        Some((scheme, rest)) => match rest.split_once('@') {
            Some((_creds, host)) => format!("{scheme}://***@{host}"),
            None => url.to_string(),
        },
        None => url.to_string(),
    }
}

// ---- process-global accessors set once at boot (read from request handlers) --------------------

use std::sync::OnceLock;

static INSTANCE_ID: OnceLock<String> = OnceLock::new();
static STACK_ID: OnceLock<String> = OnceLock::new();
static SECRET: OnceLock<Option<String>> = OnceLock::new();

/// Publish the request-path globals (instance id, stack id, `/v1/shape` secret) once at boot.
pub fn set_globals(instance_id: &str, stack_id: &str, secret: Option<&str>) {
    let _ = INSTANCE_ID.set(instance_id.to_string());
    let _ = STACK_ID.set(stack_id.to_string());
    let _ = SECRET.set(secret.map(str::to_string));
}

pub fn instance_id() -> &'static str {
    INSTANCE_ID.get().map(String::as_str).unwrap_or("unknown")
}

pub fn stack_id() -> &'static str {
    STACK_ID.get().map(String::as_str).unwrap_or("single_stack")
}

pub fn secret() -> Option<&'static str> {
    SECRET.get().and_then(|s| s.as_deref())
}

/// Does the configured secret authorize a request carrying these `secret`/`api_secret` params?
/// `None` configured → always authorized (no auth). A constant-time-ish compare is unnecessary here
/// (the secret is a deployment-wide token, not a per-user password), but we still require an exact match.
pub fn secret_ok(configured: Option<&str>, secret_param: Option<&str>, api_secret_param: Option<&str>) -> bool {
    match configured {
        None => true,
        Some(want) => secret_param == Some(want) || api_secret_param == Some(want),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn cfg(pairs: &[(&str, &str)]) -> Config {
        let map: HashMap<String, String> = pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        Config::resolve(move |k| map.get(k).cloned())
    }

    #[test]
    fn pg_url_precedence_ivm_wins_over_database_url() {
        let c = cfg(&[("ELECTRIC_CIRCUITS_PG_URL", "postgres://ivm"), ("DATABASE_URL", "postgres://fleet")]);
        assert_eq!(c.pg_url.as_deref(), Some("postgres://ivm"));
        let c = cfg(&[("DATABASE_URL", "postgres://fleet")]);
        assert_eq!(c.pg_url.as_deref(), Some("postgres://fleet"));
        let c = cfg(&[]);
        assert_eq!(c.pg_url, None);
    }

    #[test]
    fn database_url_tolerates_sslmode_disable() {
        // We don't strip it — tokio-postgres accepts sslmode in the conn string. Just confirm it
        // passes through verbatim so the connect string is unchanged.
        let url = "postgresql://postgres:password@proxy:5433/postgres?sslmode=disable";
        let c = cfg(&[("DATABASE_URL", url)]);
        assert_eq!(c.pg_url.as_deref(), Some(url));
    }

    #[test]
    fn bind_precedence() {
        // nothing set -> dev default
        assert_eq!(cfg(&[]).bind, "127.0.0.1:0");
        // ELECTRIC_PORT -> 0.0.0.0:<port>
        assert_eq!(cfg(&[("ELECTRIC_PORT", "3000")]).bind, "0.0.0.0:3000");
        // DATABASE_URL present, no port -> 0.0.0.0:3000
        assert_eq!(cfg(&[("DATABASE_URL", "postgres://x")]).bind, "0.0.0.0:3000");
        // ELECTRIC_CIRCUITS_BIND always wins
        assert_eq!(
            cfg(&[("ELECTRIC_CIRCUITS_BIND", "127.0.0.1:9"), ("ELECTRIC_PORT", "3000")]).bind,
            "127.0.0.1:9"
        );
    }

    #[test]
    fn log_level_mapping() {
        assert_eq!(cfg(&[]).log_filter, "info");
        assert_eq!(cfg(&[("ELECTRIC_LOG_LEVEL", "warning")]).log_filter, "warn");
        assert_eq!(cfg(&[("ELECTRIC_LOG_LEVEL", "error")]).log_filter, "error");
        assert_eq!(cfg(&[("ELECTRIC_LOG_LEVEL", "debug")]).log_filter, "debug");
        // ELECTRIC_CIRCUITS_LOG wins and passes through verbatim
        assert_eq!(
            cfg(&[("ELECTRIC_CIRCUITS_LOG", "electric_circuits_engine=debug"), ("ELECTRIC_LOG_LEVEL", "error")]).log_filter,
            "electric_circuits_engine=debug"
        );
    }

    #[test]
    fn slot_name_from_stream_id() {
        assert_eq!(cfg(&[]).slot, "electric_circuits");
        assert_eq!(cfg(&[("ELECTRIC_REPLICATION_STREAM_ID", "bench")]).slot, "electric_slot_bench");
        assert_eq!(
            cfg(&[("ELECTRIC_CIRCUITS_PG_SLOT", "custom"), ("ELECTRIC_REPLICATION_STREAM_ID", "bench")]).slot,
            "custom"
        );
    }

    #[test]
    fn stack_id_from_stream_id() {
        assert_eq!(cfg(&[]).stack_id, "single_stack");
        assert_eq!(cfg(&[("ELECTRIC_REPLICATION_STREAM_ID", "bench")]).stack_id, "bench");
    }

    #[test]
    fn instance_id_default_is_a_uuid() {
        let c = cfg(&[]);
        assert_eq!(c.instance_id.len(), 36, "generated instance id should be a UUID");
        assert_eq!(cfg(&[("ELECTRIC_INSTANCE_ID", "fixed-id")]).instance_id, "fixed-id");
    }

    #[test]
    fn statsd_host_and_port() {
        assert_eq!(cfg(&[]).statsd, None);
        assert_eq!(
            cfg(&[("ELECTRIC_STATSD_HOST", "host.docker.internal")]).statsd,
            Some(StatsdTarget { host: "host.docker.internal".into(), port: 8125 })
        );
        assert_eq!(
            cfg(&[("ELECTRIC_STATSD_HOST", "10.0.0.5:9999")]).statsd,
            Some(StatsdTarget { host: "10.0.0.5".into(), port: 9999 })
        );
    }

    #[test]
    fn metrics_period_precedence() {
        assert_eq!(cfg(&[]).metrics_period, Duration::from_secs(5));
        assert_eq!(cfg(&[("TELEMETRY_POLLER_PERIOD", "200")]).metrics_period, Duration::from_millis(200));
        // Electric's spelling wins even when both are set.
        assert_eq!(
            cfg(&[("ELECTRIC_SYSTEM_METRICS_POLL_INTERVAL", "2s"), ("TELEMETRY_POLLER_PERIOD", "200")])
                .metrics_period,
            Duration::from_secs(2)
        );
    }

    #[test]
    fn duration_parsing() {
        assert_eq!(parse_human_duration("5s"), Some(Duration::from_secs(5)));
        assert_eq!(parse_human_duration("200ms"), Some(Duration::from_millis(200)));
        assert_eq!(parse_human_duration("1m"), Some(Duration::from_secs(60)));
        assert_eq!(parse_human_duration("500"), Some(Duration::from_millis(500)));
        assert_eq!(parse_human_duration("garbage"), None);
    }

    #[test]
    fn secret_and_noop() {
        assert_eq!(cfg(&[("ELECTRIC_SECRET", "sekret")]).secret.as_deref(), Some("sekret"));
        assert!(secret_ok(None, None, None));
        assert!(secret_ok(Some("s"), Some("s"), None));
        assert!(secret_ok(Some("s"), None, Some("s")));
        assert!(!secret_ok(Some("s"), Some("nope"), None));
        assert!(!secret_ok(Some("s"), None, None));
    }

    #[test]
    fn trace_flag() {
        assert!(cfg(&[]).trace, "introspection defaults on");
        assert!(!cfg(&[("ELECTRIC_CIRCUITS_TRACE", "0")]).trace);
        assert!(!cfg(&[("ELECTRIC_CIRCUITS_TRACE", "false")]).trace);
        assert!(!cfg(&[("ELECTRIC_CIRCUITS_TRACE", "off")]).trace);
        assert!(cfg(&[("ELECTRIC_CIRCUITS_TRACE", "1")]).trace);
        assert!(cfg(&[("ELECTRIC_CIRCUITS_TRACE", "true")]).trace);
    }

    #[test]
    fn dbsp_circuit_is_always_built() {
        // The circuit is mandatory infrastructure: no on/off flag. With nothing configured it
        // still resolves, with an empty index/counts config and a slot-keyed default state dir.
        let c = cfg(&[]);
        assert!(c.dbsp.indexes.is_empty(), "empty _INDEXES is valid");
        assert!(c.dbsp.counts.is_empty(), "empty _COUNTS is valid");
        assert!(c.dbsp.dir.ends_with("dbsp/electric_circuits"), "default dir is slot-keyed: {:?}", c.dbsp.dir);
        assert_eq!(c.dbsp.checkpoint_every, Some(Duration::from_secs(60)));
        assert_eq!(c.dbsp.min_storage_bytes, Some(1024 * 1024));
    }

    #[test]
    fn dbsp_tunables_parse() {
        let c = cfg(&[
            ("ELECTRIC_CIRCUITS_DBSP_DIR", "/tmp/dbsp"),
            ("ELECTRIC_CIRCUITS_DBSP_INDEXES", "todos.list_id, list_members.user_id"),
            ("ELECTRIC_CIRCUITS_DBSP_COUNTS", "todos:list_id+done"),
            ("ELECTRIC_CIRCUITS_DBSP_CHECKPOINT_SECS", "0"),
            ("ELECTRIC_CIRCUITS_DBSP_MIN_STORAGE_KB", "2048"),
        ]);
        assert_eq!(c.dbsp.dir, std::path::PathBuf::from("/tmp/dbsp"));
        assert_eq!(
            c.dbsp.indexes,
            vec![("todos".to_string(), "list_id".to_string()), ("list_members".to_string(), "user_id".to_string())]
        );
        assert_eq!(c.dbsp.counts, vec![("todos".to_string(), vec!["list_id".to_string(), "done".to_string()])]);
        assert_eq!(c.dbsp.checkpoint_every, None, "0 means checkpoint only at shutdown");
        assert_eq!(c.dbsp.min_storage_bytes, Some(2048 * 1024));
    }

    #[test]
    fn noop_var_detection() {
        assert!(is_noop_var("ELECTRIC_CACHE_MAX_AGE"));
        assert!(is_noop_var("ELECTRIC_OTLP_ENDPOINT"));
        assert!(!is_noop_var("ELECTRIC_DB_POOL_SIZE")); // handled: sizes the backfill pool
        assert!(!is_noop_var("ELECTRIC_PORT")); // handled
        assert!(!is_noop_var("ELECTRIC_CIRCUITS_PG_URL")); // internal
        assert!(!is_noop_var("DATABASE_URL")); // not an ELECTRIC_ var
    }
}
