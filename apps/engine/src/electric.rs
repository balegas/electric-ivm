//! Electric-protocol adapter: serves `GET /v1/shape` so Electric's official `Electric.Client` (and its
//! oracle test harness) can read shapes from our engine. We translate the engine's materialized-shape
//! durable stream into Electric's change-message log with the headers/control-messages the client
//! requires (`electric-handle`, `electric-offset`, `electric-schema`, `electric-cursor`,
//! `electric-up-to-date`; `up-to-date` / `must-refetch` control messages).
//!
//! Mapping notes:
//! - A `GET` with `offset=-1` is the **snapshot**: parse `where` → predicate, create a materialized
//!   shape, fold its stream to the current row set, emit every row as an `insert`, then an
//!   `up-to-date` control. The handle is the shape id; the offset is the stream's tail. An
//!   `offset > -1` without a handle is a 400 (Electric: "handle required").
//! - A `GET` with `live=true` long-polls the stream from `offset` and emits `insert`/`update`/`delete`.
//!   Our engine emits absolute `upsert`/`delete`, so we reconstruct insert-vs-update from a per-handle
//!   key set (Electric's client rejects insert-of-existing / update-of-missing). Requests on one handle
//!   are serialized by a per-handle mutex; a retry at an older offset rebuilds the key set **as of that
//!   offset** before replaying. Concurrent **live** requests at the *same* (handle, offset) are
//!   identical, so they are **coalesced**: one leader does the read+apply and every waiter receives the
//!   same response (write-fanout: N clients long-poll one handle at one offset — serializing them behind
//!   the mutex would hand each a full long-poll timeout in turn). A live request keeps re-polling the
//!   ds stream until data arrives or `ELECTRIC_LIVE_TIMEOUT_MS` (default 20000, Electric-like ~20s)
//!   elapses, then returns `204`. Every individual ds poll is bounded by the *remaining* deadline
//!   (`poll_live_until`): the ds server's own long-poll window can exceed ours, and an idle stream
//!   must still produce the 204 on time.
//! - Identical shape definitions (same table + canonical `where` + columns) share ONE engine shape
//!   (`create_shape` with `share = true`): concurrent clients and returning clients rejoin the same
//!   retained stream instead of re-backfilling from Postgres. Handles stay **per client** (each
//!   snapshot mints a unique handle id over the shared stream), so cursor state is never contended
//!   across clients; each live handle holds one subscription on the shared shape.
//! - Handles are evicted after sitting idle for `ELECTRIC_HANDLE_TTL` seconds (default 600). This is
//!   **handle-state cleanup only**: the per-handle cursor state is dropped and the shape subscription
//!   released — the underlying engine shape and its durable stream are retained and follow the
//!   retention lifecycle (idle → dormant → evicted; see [`crate::retention`]). A late request on the
//!   evicted handle gets the standard `409 must-refetch`; the client's re-snapshot rejoins the
//!   retained shape (reactivating it if dormant) rather than rebuilding from Postgres.
//! - Errors split by who must act: validation failures (bad `where`, unknown table/column, missing
//!   handle, table/handle mismatch) are 400 with an Electric-style `{"message": …}` body — the client
//!   treats 400 as fatal. Everything else (durable-streams hiccups, engine failures) is 500 so the
//!   client retries.
//! - Values are encoded as Postgres **text** (`bool`→`"true"`/`"false"`, text as-is); Electric's default
//!   value-mapper only coerces int/float, leaving these as strings — matching the oracle's stringified
//!   comparison.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use tokio::sync::watch;

use crate::ds::{Envelope, ReadResult};
use crate::engine::Engine;
use crate::heap_size::HeapSize;
use crate::schema::{ColumnType, TableSchema};

#[derive(Debug, Deserialize)]
pub struct ShapeParams {
    table: String,
    #[serde(default)]
    offset: Option<String>,
    #[serde(default)]
    handle: Option<String>,
    #[serde(default, rename = "where")]
    where_: Option<String>,
    #[serde(default)]
    columns: Option<String>,
    #[serde(default)]
    live: Option<String>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    replica: Option<String>,
    /// `ELECTRIC_SECRET` auth: either query param, when the secret is configured, must equal it.
    #[serde(default)]
    secret: Option<String>,
    #[serde(default)]
    api_secret: Option<String>,
}

/// A registered handle. The registry maps handle → `Arc<HandleEntry>`; the mutable cursor state
/// (`keys`/`offset`) sits behind a per-handle async mutex held across read+apply so two concurrent
/// requests on one handle (client abort + retry) cannot interleave and corrupt the key set. The
/// registry lock itself is only ever held briefly (get/insert/remove), never across I/O.
struct HandleEntry {
    stream_path: String,
    /// The underlying (shared) engine shape this handle subscribes to. Handles are per-client —
    /// identical definitions share ONE engine shape/stream but each snapshot mints its own handle,
    /// so per-handle cursor state is never contended across clients — and each live handle holds
    /// exactly one shape subscription, released when the handle is evicted.
    shape_id: String,
    table: String,
    pk_name: String,
    /// When this handle was last touched by a request — drives idle-TTL eviction.
    last_access: std::sync::Mutex<Instant>,
    state: tokio::sync::Mutex<HandleState>,
    /// In-flight **live** long-polls keyed by request offset. Concurrent live requests at the same
    /// (handle, offset) are identical, so the first arrival (the leader) does the read+apply while
    /// every other one awaits the published [`ReadOutcome`] on the watch channel instead of queueing
    /// behind the state mutex (which would hand each waiter a full long-poll timeout in turn). Only
    /// the leader touches [`HandleState`]. Lock order: this lock is only ever held briefly and never
    /// across I/O or the state mutex.
    live_inflight: std::sync::Mutex<HashMap<String, watch::Receiver<LiveSlot>>>,
}

/// `None` until the leader publishes; then the shared result every coalesced waiter clones.
type LiveSlot = Option<Result<ReadOutcome, ApiError>>;

impl HandleEntry {
    fn touch(&self) {
        *self.last_access.lock().unwrap() = Instant::now();
    }
}

/// Per-handle live state: which keys the client currently holds (to pick insert vs update) and the last
/// offset we served it.
struct HandleState {
    keys: HashSet<String>,
    offset: String,
}

fn handles() -> &'static std::sync::Mutex<HashMap<String, Arc<HandleEntry>>> {
    static H: OnceLock<std::sync::Mutex<HashMap<String, Arc<HandleEntry>>>> = OnceLock::new();
    H.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Owned-heap estimate of the `/v1/shape` handle registry — the memory probe's
/// `bytes_electric_adapter` term. `HandleEntry`/`HandleState` hold sync/async primitives
/// (`Mutex`, `watch::Receiver`), so this is a hand-rolled walk rather than a `HeapSize` impl:
/// the registry map's own key strings + each entry's owned strings (`stream_path`, `shape_id`,
/// `table`, `pk_name`) + its cursor state (`keys`/`offset`) + the in-flight live-poll map's key
/// strings (the `watch::Receiver` values are shared channel handles, not uniquely owned, so
/// only their keys are counted).
///
/// Single lock, allocation-free: holds the registry's std `Mutex` for the whole walk (no cloning
/// entries out into a `Vec` first, no second lock for the map's own bucket-overhead estimate) and
/// reads each handle's cursor state with `try_lock` instead of awaiting the async `Mutex` — this
/// is a best-effort byte estimate, not a request path, so a handle mid-request (lock momentarily
/// held) is simply skipped for its `keys`/`offset` term rather than blocked on.
pub(crate) async fn ttl_registry_heap_bytes() -> usize {
    let map = handles().lock().unwrap();
    let cap = map.capacity();
    let mut total = (cap * (std::mem::size_of::<(String, Arc<HandleEntry>)>() + 1) * 11) / 10;
    for (id, entry) in map.iter() {
        total += id.heap_bytes()
            + entry.stream_path.heap_bytes()
            + entry.shape_id.heap_bytes()
            + entry.table.heap_bytes()
            + entry.pk_name.heap_bytes();
        if let Ok(st) = entry.state.try_lock() {
            total += st.keys.heap_bytes() + st.offset.heap_bytes();
        }
        total += entry.live_inflight.lock().unwrap().keys().map(HeapSize::heap_bytes).sum::<usize>();
    }
    total
}

/// Overall deadline for a `live=true` long-poll, decoupled from the ds server's own long-poll timeout:
/// the adapter keeps re-polling the ds stream until data arrives or this elapses, then returns `204`.
/// `ELECTRIC_LIVE_TIMEOUT_MS` env var; default 20000 (Electric's ~20s live long-poll).
fn live_timeout() -> Duration {
    static T: OnceLock<Duration> = OnceLock::new();
    *T.get_or_init(|| {
        let ms = std::env::var("ELECTRIC_LIVE_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(20_000);
        Duration::from_millis(ms)
    })
}

/// Idle TTL for `/v1/shape` handles: a handle not touched for this long has its per-handle cursor
/// state evicted and its shape subscription released. The underlying shape + stream are retained
/// (retention lifecycle, see [`crate::retention`]) — this is handle-state cleanup, not shape
/// teardown. `ELECTRIC_HANDLE_TTL` env var, in seconds; default 600 (10 min).
fn handle_ttl() -> Duration {
    static TTL: OnceLock<Duration> = OnceLock::new();
    *TTL.get_or_init(|| {
        let secs = std::env::var("ELECTRIC_HANDLE_TTL")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(600);
        Duration::from_secs(secs)
    })
}

/// Spawn (once) the background evictor that cleans up handles idle longer than [`handle_ttl`]:
/// the per-handle cursor state is dropped and the shape's subscription released (`release_shape`)
/// — the shape itself is retained and ages through the retention lifecycle. A request arriving
/// with an evicted handle gets the standard `409 must-refetch` and re-snapshots (rejoining the
/// retained shape).
fn ensure_evictor(engine: &Engine) {
    static STARTED: OnceLock<()> = OnceLock::new();
    STARTED.get_or_init(|| {
        let engine = engine.clone();
        tokio::spawn(async move {
            let ttl = handle_ttl();
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let idle: Vec<(String, Arc<HandleEntry>)> = handles()
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(_, e)| e.last_access.lock().unwrap().elapsed() > ttl)
                    .map(|(id, e)| (id.clone(), e.clone()))
                    .collect();
                for (id, entry) in idle {
                    // Skip a handle mid-request (its state mutex is held). Holding the guard while we
                    // unregister keeps a racing request from mutating state we are tearing down; a
                    // request that snatched the Arc just before removal still reads the retained
                    // stream fine, and its next request gets 409 must-refetch (registry miss).
                    let Ok(guard) = entry.state.try_lock() else { continue };
                    if entry.last_access.lock().unwrap().elapsed() <= ttl {
                        continue; // touched between the scan and now
                    }
                    handles().lock().unwrap().remove(&id);
                    drop(guard);
                    engine.release_shape(&entry.shape_id).await;
                    tracing::debug!("evicted idle electric handle {id} (shape retained)");
                }
            }
        });
    });
}

fn next_cursor() -> u64 {
    static C: AtomicU64 = AtomicU64::new(1);
    C.fetch_add(1, Ordering::Relaxed)
}

fn col_csv(c: &Option<String>) -> Option<Vec<String>> {
    c.as_ref().map(|s| s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect())
}

/// A Postgres type name for the `electric-schema` header. Only `int*`/`float*` trigger the client's
/// value coercion; text/bool stay strings (which is what we want for the stringified oracle compare).
fn pg_type(ty: ColumnType) -> &'static str {
    match ty {
        ColumnType::Int => "int8",
        ColumnType::Float => "float8",
        ColumnType::Text => "text",
        ColumnType::Bool => "bool",
    }
}

/// Build the `electric-schema` JSON: `{col: {type, pk_index?}}`.
fn schema_json(ts: &TableSchema, columns: &Option<Vec<String>>) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (name, ty) in &ts.columns {
        if let Some(cols) = columns {
            if !cols.iter().any(|c| c == name) && name != &ts.pk_name {
                continue;
            }
        }
        let mut entry = serde_json::Map::new();
        entry.insert("type".into(), serde_json::Value::String(pg_type(*ty).into()));
        if name == &ts.pk_name {
            entry.insert("pk_index".into(), serde_json::Value::from(0));
        }
        map.insert(name.clone(), serde_json::Value::Object(entry));
    }
    serde_json::Value::Object(map)
}

/// Re-encode a row JSON value (typed: string/bool/number/null) as Electric text values (`{col: "text"}`).
fn encode_value(v: &serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Object(m) => {
            serde_json::Value::Object(m.iter().map(|(k, val)| (k.clone(), pg_text(val))).collect())
        }
        other => other.clone(),
    }
}

fn pg_text(v: &serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Null => serde_json::Value::Null,
        serde_json::Value::Bool(b) => serde_json::Value::String(if *b { "true".into() } else { "false".into() }),
        serde_json::Value::String(s) => serde_json::Value::String(s.clone()),
        serde_json::Value::Number(n) => serde_json::Value::String(n.to_string()),
        other => serde_json::Value::String(other.to_string()),
    }
}

fn change_msg(op: &str, key: &str, value: Option<serde_json::Value>) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    let mut headers = serde_json::Map::new();
    headers.insert("operation".into(), serde_json::Value::String(op.into()));
    m.insert("headers".into(), serde_json::Value::Object(headers));
    m.insert("key".into(), serde_json::Value::String(key.into()));
    if let Some(v) = value {
        m.insert("value".into(), v);
    }
    serde_json::Value::Object(m)
}

fn control_msg(control: &str) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    let mut headers = serde_json::Map::new();
    headers.insert("control".into(), serde_json::Value::String(control.into()));
    if control == "up-to-date" {
        headers.insert("global_last_seen_lsn".into(), serde_json::Value::String("0".into()));
    }
    m.insert("headers".into(), serde_json::Value::Object(headers));
    serde_json::Value::Object(m)
}

fn hv(s: &str) -> HeaderValue {
    HeaderValue::from_str(s).unwrap_or_else(|_| HeaderValue::from_static(""))
}

fn respond(messages: Vec<serde_json::Value>, mut headers: HeaderMap, status: StatusCode) -> Response {
    headers.insert(axum::http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    let body = serde_json::to_string(&messages).unwrap_or_else(|_| "[]".into());
    let len = body.len() as u64;
    let mut resp = (status, headers, body).into_response();
    resp.extensions_mut().insert(BodyLen(len));
    resp
}

fn must_refetch() -> Response {
    // No `electric-handle` header: the evicted/unknown handle has no replacement shape yet — the
    // client re-requests with offset=-1 and the snapshot response assigns the fresh handle.
    let mut headers = HeaderMap::new();
    headers.insert(HeaderName::from_static("electric-offset"), hv("-1"));
    respond(vec![control_msg("must-refetch")], headers, StatusCode::CONFLICT)
}

/// Adapter error split by who must act: Electric's client treats **400 as fatal** (it kills the sync
/// loop), so only request-validation failures may use it; anything transient/internal must be a 500 so
/// the client retries. The body mirrors Electric's error shape: `{"message": …}`.
#[derive(Clone, Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        ApiError { status: StatusCode::BAD_REQUEST, message: message.into() }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        ApiError { status: StatusCode::INTERNAL_SERVER_ERROR, message: format!("{e:#}") }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = serde_json::json!({ "message": self.message }).to_string();
        let len = body.len() as u64;
        let mut headers = HeaderMap::new();
        headers.insert(axum::http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let mut resp = (self.status, headers, body).into_response();
        resp.extensions_mut().insert(BodyLen(len));
        resp
    }
}

/// `true` iff stream offset `a` sorts after `b`. `"-1"` is the beginning; every other offset is the
/// durable-streams server's fixed-width zero-padded token, whose lexicographic order equals stream
/// order (`{seq:016}_{byte:016}`).
fn offset_after(a: &str, b: &str) -> bool {
    if a == "-1" {
        false
    } else if b == "-1" {
        true
    } else {
        a > b
    }
}

/// Page-driven fold of a shape stream into its current key→row map (pure, so unit-testable).
///
/// `until = Some(off)` rebuilds the state **as of** that offset: envelopes stamped after it are
/// ignored (used when a client retries an older offset — folding to the tail instead would drop
/// deletes / mis-classify inserts in the replayed window). `done` is set on `up-to-date`, when the
/// target offset is passed, or when the next offset stops advancing; an empty page mid-stream does
/// **not** end the fold (that truncated snapshots).
struct StreamFold {
    rows: HashMap<String, serde_json::Value>,
    /// Keys in first-appearance (stream) order. `rows` alone would randomize the snapshot's
    /// row order per request (HashMap iteration), which readers observably depend on — the
    /// engine appends the backfill in a deterministic order and the adapter must not shuffle
    /// it. Deleted keys keep their slot here and are filtered at emission by map membership.
    order: Vec<String>,
    offset: String,
    until: Option<String>,
    done: bool,
}

impl StreamFold {
    fn to_tail() -> Self {
        StreamFold { rows: HashMap::new(), order: Vec::new(), offset: "-1".into(), until: None, done: false }
    }

    fn up_to(offset: &str) -> Self {
        StreamFold {
            rows: HashMap::new(),
            order: Vec::new(),
            offset: "-1".into(),
            until: Some(offset.to_string()),
            done: false,
        }
    }

    /// The folded rows in stream (first-appearance) order.
    fn ordered_rows(&self) -> impl Iterator<Item = (&String, &serde_json::Value)> {
        self.order.iter().filter_map(|k| self.rows.get_key_value(k))
    }

    fn apply_page(&mut self, r: ReadResult) {
        for env in r.envelopes {
            if let (Some(target), Some(stamp)) = (self.until.as_deref(), env.headers.offset.as_deref()) {
                if offset_after(stamp, target) {
                    self.done = true;
                    return;
                }
            }
            match env.headers.operation.as_str() {
                "delete" => {
                    self.rows.remove(&env.key);
                }
                _ => {
                    if let Some(v) = env.value {
                        if self.rows.insert(env.key.clone(), v).is_none() {
                            self.order.push(env.key);
                        }
                    }
                }
            }
        }
        let advanced = match r.next_offset {
            Some(n) if n != self.offset => {
                self.offset = n;
                true
            }
            _ => false,
        };
        if r.up_to_date || !advanced {
            self.done = true;
        }
    }
}

/// Drive a [`StreamFold`] with catch-up reads until it completes.
async fn drive_fold(engine: &Engine, path: &str, mut fold: StreamFold) -> anyhow::Result<StreamFold> {
    while !fold.done {
        let offset = fold.offset.clone();
        let r = engine.read_shape_stream(path, &offset, false).await?;
        fold.apply_page(r);
    }
    Ok(fold)
}

/// Fold a shape's whole stream (catch-up reads from `-1`) into the current key→row-value map and return
/// it with the stream's tail offset.
async fn materialize(engine: &Engine, path: &str) -> anyhow::Result<(Vec<(String, serde_json::Value)>, String)> {
    let fold = drive_fold(engine, path, StreamFold::to_tail()).await?;
    let rows = fold.ordered_rows().map(|(k, v)| (k.clone(), v.clone())).collect();
    Ok((rows, fold.offset))
}

/// The key set a client holds when positioned at `offset` (fold `-1..=offset`).
async fn keys_as_of(engine: &Engine, path: &str, offset: &str) -> anyhow::Result<HashSet<String>> {
    let fold = drive_fold(engine, path, StreamFold::up_to(offset)).await?;
    Ok(fold.rows.into_keys().collect())
}

/// Classify the engine's absolute `upsert`/`delete` envelopes into Electric `insert`/`update`/`delete`
/// change messages against the client's key set (mutating it as it goes).
fn apply_changes(keys: &mut HashSet<String>, pk_name: &str, envelopes: Vec<Envelope>) -> Vec<serde_json::Value> {
    let mut messages = Vec::new();
    for env in envelopes {
        match env.headers.operation.as_str() {
            "delete" => {
                if keys.remove(&env.key) {
                    // Electric's client requires a `value` on every change message (its parser matches
                    // on `"value"`). For a delete we carry the row's old value if present, else the key.
                    let value = env
                        .value
                        .as_ref()
                        .map(encode_value)
                        .unwrap_or_else(|| serde_json::json!({ pk_name: env.key }));
                    messages.push(change_msg("delete", &env.key, Some(value)));
                }
            }
            _ => {
                let value = env.value.as_ref().map(encode_value);
                if keys.contains(&env.key) {
                    messages.push(change_msg("update", &env.key, value));
                } else {
                    keys.insert(env.key.clone());
                    messages.push(change_msg("insert", &env.key, value));
                }
            }
        }
    }
    messages
}

/// Schema-validation collector: lets [`CompiledPredicate::compile_with`] walk `IN (SELECT …)` leaves
/// (returning their signature) without registering nodes — used to pre-validate a snapshot request's
/// predicate against the table schema so unknown columns / mistyped literals are a 400, not a 500.
struct ValidateOnly;
impl crate::predicate::SubqueryCollector for ValidateOnly {
    fn collect(
        &mut self,
        table: &str,
        project: &str,
        where_: Option<&crate::predicate::PredicateJson>,
    ) -> anyhow::Result<crate::predicate::SubquerySig> {
        Ok(crate::predicate::subquery_sig(table, project, where_))
    }
}

/// The result of one positioned read on a handle, cloneable to every coalesced waiter. `body: None`
/// is the `204` long-poll-deadline response; `Some` carries the serialized JSON message array.
#[derive(Clone)]
struct ReadOutcome {
    offset: String,
    up_to_date: bool,
    cursor: u64,
    body: Option<String>,
}

/// Coalesce concurrent live requests at one (handle, offset): the first arrival becomes the leader,
/// runs `work` (the serialized resync+read+apply path) and publishes its result; every concurrent
/// caller at the same offset awaits that result instead of re-running `work`. If the leader vanishes
/// without publishing (client disconnect drops its future mid-poll), the channel closes and waiters
/// re-elect a leader (`work` is re-buildable via `Fn`).
async fn coalesce_live<F, Fut>(entry: &HandleEntry, offset: &str, work: F) -> Result<ReadOutcome, ApiError>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<ReadOutcome, ApiError>>,
{
    enum Role {
        Leader(watch::Sender<LiveSlot>),
        Waiter(watch::Receiver<LiveSlot>),
    }
    loop {
        let role = {
            let mut map = entry.live_inflight.lock().unwrap();
            match map.get(offset) {
                Some(rx) => Role::Waiter(rx.clone()),
                None => {
                    let (tx, rx) = watch::channel(None);
                    map.insert(offset.to_string(), rx);
                    Role::Leader(tx)
                }
            }
        };
        match role {
            Role::Leader(tx) => {
                // Unregister on scope exit *and* on cancellation (the future being dropped), so an
                // aborted leader never leaves a dead in-flight slot that waiters would hang on.
                struct Unregister<'a>(&'a HandleEntry, &'a str);
                impl Drop for Unregister<'_> {
                    fn drop(&mut self) {
                        self.0.live_inflight.lock().unwrap().remove(self.1);
                    }
                }
                let _unregister = Unregister(entry, offset);
                let result = work().await;
                // The map still holds a receiver until `_unregister` drops, so send cannot fail; late
                // arrivals that grabbed a receiver before removal see the value immediately.
                let _ = tx.send(Some(result.clone()));
                return result;
            }
            Role::Waiter(mut rx) => {
                loop {
                    if let Some(result) = rx.borrow_and_update().clone() {
                        return result;
                    }
                    if rx.changed().await.is_err() {
                        break; // leader cancelled without publishing — re-elect
                    }
                }
            }
        }
    }
}

/// The live polling loop: repeatedly `read(from)` until a page carries envelopes or `deadline`
/// elapses. Each poll is bounded by the **remaining** deadline — the ds server holds an idle
/// long-poll far longer than our window, so an unbounded read would blow straight through the
/// deadline (observed: idle `live=true` requests hanging >60s instead of 204 at ~20s). On expiry
/// the last empty page is returned so a `next_offset` advanced past empty pages is preserved;
/// expiry mid-poll (no page yet) yields an offset-less empty result, leaving the handle offset
/// unchanged.
async fn poll_live_until<F, Fut, E>(
    mut from: String,
    deadline: Instant,
    mut read: F,
) -> Result<ReadResult, E>
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = Result<ReadResult, E>>,
{
    let mut last = ReadResult { envelopes: Vec::new(), next_offset: None, up_to_date: false };
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(last);
        }
        match tokio::time::timeout(remaining, read(from.clone())).await {
            Err(_elapsed) => return Ok(last),
            Ok(page) => {
                let page = page?;
                if !page.envelopes.is_empty() {
                    return Ok(page);
                }
                if let Some(n) = &page.next_offset {
                    from = n.clone();
                }
                last = page;
            }
        }
    }
}

/// The serialized (per-handle state mutex) resync+read+apply path shared by non-live requests and the
/// live leader. Holds the state mutex across the whole read loop so only one request mutates
/// [`HandleState`]; for `live`, keeps re-polling the ds stream via [`poll_live_until`] until data
/// arrives or [`live_timeout`] elapses, every poll bounded by the remaining deadline.
async fn positioned_read(
    engine: &Engine,
    entry: &HandleEntry,
    offset: &str,
    live: bool,
) -> Result<ReadOutcome, ApiError> {
    let mut st = entry.state.lock().await;
    if st.offset != offset {
        // The client resumed at a different (older) offset than we last served: rebuild the key set
        // **as of that offset** (fold -1..=offset), then replay from there. Folding to the tail instead
        // would silently drop a delete of a key absent at tail and emit updates for reinserted keys.
        st.keys = keys_as_of(engine, &entry.stream_path, offset).await?;
        st.offset = offset.to_string();
    }
    let from = offset.to_string();
    let r = if live {
        let deadline = Instant::now() + live_timeout();
        let read_engine = engine.clone();
        let path = entry.stream_path.clone();
        poll_live_until(from, deadline, move |f| {
            let engine = read_engine.clone();
            let path = path.clone();
            async move { engine.read_shape_stream(&path, &f, true).await }
        })
        .await?
    } else {
        engine.read_shape_stream(&entry.stream_path, &from, false).await?
    };

    if let Some(n) = &r.next_offset {
        st.offset = n.clone();
    }

    // Live deadline reached with no new data: a 204 with the electric headers (handle, offset
    // unchanged) like Electric does — a 200 `[]` without `up-to-date` would make the client busy-loop.
    if live && r.envelopes.is_empty() {
        let served_offset = st.offset.clone();
        drop(st);
        entry.touch();
        return Ok(ReadOutcome { offset: served_offset, up_to_date: true, cursor: next_cursor(), body: None });
    }

    let mut messages = apply_changes(&mut st.keys, &entry.pk_name, r.envelopes);
    if r.up_to_date {
        messages.push(control_msg("up-to-date"));
    }
    let served_offset = st.offset.clone();
    drop(st);
    entry.touch();
    Ok(ReadOutcome {
        offset: served_offset,
        up_to_date: r.up_to_date,
        cursor: next_cursor(),
        body: Some(serde_json::to_string(&messages).unwrap_or_else(|_| "[]".into())),
    })
}

/// Response body size in bytes, stamped on every `/v1/shape` response so [`shape`] can emit the byte
/// metrics without re-reading (and consuming) the response body.
#[derive(Clone, Copy)]
struct BodyLen(u64);

/// `401 {"message":"Unauthorized"}` for a request that fails the `ELECTRIC_SECRET` check.
fn unauthorized() -> Response {
    let body = r#"{"message":"Unauthorized"}"#;
    let mut headers = HeaderMap::new();
    headers.insert(axum::http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    let mut resp = (StatusCode::UNAUTHORIZED, headers, body).into_response();
    resp.extensions_mut().insert(BodyLen(body.len() as u64));
    resp
}

pub async fn shape(
    State(engine): State<Engine>,
    Query(p): Query<ShapeParams>,
    // Raw query pairs (decoded) for `params` — bracket form `params[1]=…` isn't a single serde field.
    Query(raw_pairs): Query<Vec<(String, String)>>,
) -> Response {
    ensure_evictor(&engine);
    let start = Instant::now();
    let live = p.live.as_deref() == Some("true");
    // root_table tag: the bare table name (strip any schema prefix), computed before shape_inner moves p.
    let root_table = p.table.rsplit_once('.').map(|(_, b)| b.to_string()).unwrap_or_else(|| p.table.clone());

    let resp = if !crate::config::secret_ok(
        crate::config::secret(),
        p.secret.as_deref(),
        p.api_secret.as_deref(),
    ) {
        unauthorized()
    } else {
        match shape_inner(engine, p, &raw_pairs).await {
            Ok(resp) => resp,
            Err(e) => {
                tracing::warn!("/v1/shape error ({}): {}", e.status, e.message);
                e.into_response()
            }
        }
    };

    let status = resp.status().as_u16();
    let bytes = resp.extensions().get::<BodyLen>().map(|b| b.0).unwrap_or(0);
    crate::statsd::serve_shape(&root_table, live, status, start.elapsed(), bytes);
    resp
}

async fn shape_inner(
    engine: Engine,
    mut p: ShapeParams,
    raw_pairs: &[(String, String)],
) -> Result<Response, ApiError> {
    // Electric clients send schema-qualified table names (`public.users`); our engine keys by the bare
    // table name. Strip any schema prefix.
    if let Some((_schema, bare)) = p.table.rsplit_once('.') {
        p.table = bare.to_string();
    }
    let offset = p.offset.clone().unwrap_or_else(|| "-1".into());
    let live = p.live.as_deref() == Some("true");
    let columns = col_csv(&p.columns);
    let _ = &p.cursor; // accepted (cache-busting hint); we mint our own electric-cursor
    let _ = &p.replica; // accepted; we always send full rows (replica=full semantics)

    let Some(ts) = engine.table_schema(&p.table).await else {
        return Err(ApiError::bad_request(format!("unknown table '{}'", p.table)));
    };

    // A positioned read without a handle is invalid (Electric: "handle required") — silently serving a
    // fresh snapshot here would hand the client a log that doesn't match its offset.
    if offset != "-1" && p.handle.is_none() {
        return Err(ApiError::bad_request("offset was provided without a shape handle"));
    }

    // ---- Snapshot: offset=-1 -> create the shape and emit the current rows as inserts.
    if offset == "-1" {
        // Resolve + validate `params` (Electric-compatible), then substitute `$N` into the where clause
        // BEFORE parsing, so the predicate/subquery machinery — and the shape's identity/signature —
        // are derived from the substituted text (distinct param values never collide onto one shape).
        let params = crate::params::parse_params(raw_pairs).map_err(ApiError::bad_request)?;
        let where_raw = p.where_.as_deref().unwrap_or("");
        let where_sub = crate::params::substitute(where_raw, &params).map_err(ApiError::bad_request)?;
        let pred = crate::where_sql::parse_where_typed(&where_sub, Some(&ts))
            .map_err(|e| ApiError::bad_request(format!("invalid where clause: {e:#}")))?;
        // Validate the request shape against the schema up front: these are client errors (400). A
        // failure past this point (stream creation, backfill, reads) is internal (500).
        if let Some(cols) = &columns {
            for c in cols {
                if ts.column_index(c).is_err() {
                    return Err(ApiError::bad_request(format!("unknown column '{c}'")));
                }
            }
        }
        if let Some(pr) = &pred {
            crate::predicate::CompiledPredicate::compile_with(pr, &ts, &mut ValidateOnly)
                .map_err(|e| ApiError::bad_request(format!("invalid where clause: {e:#}")))?;
        }
        // share = true: identical /v1/shape definitions collapse to ONE engine shape/stream. A
        // returning client's re-snapshot rejoins the retained shape — reactivated inside
        // `create_shape` if it went dormant — instead of re-backfilling from Postgres. The handle
        // minted below stays per-client (see the module docs).
        let rec = engine.create_shape(&p.table, pred, columns.clone(), false, true).await?;
        let (rows, tail) = match materialize(&engine, &rec.stream_path).await {
            Ok(v) => v,
            Err(e) => {
                // Failed after taking the create/join subscription: give it back, or the dead
                // subscription pins the shape active forever.
                engine.release_shape(&rec.id).await;
                return Err(e.into());
            }
        };

        let mut messages = Vec::with_capacity(rows.len() + 1);
        let mut keys = HashSet::with_capacity(rows.len());
        for (key, value) in &rows {
            messages.push(change_msg("insert", key, Some(encode_value(value))));
            keys.insert(key.clone());
        }
        messages.push(control_msg("up-to-date"));

        let schema_str = serde_json::to_string(&schema_json(&ts, &columns)).unwrap_or_default();
        // Handles are per-client even though the shape is shared: a unique handle id per snapshot
        // keeps each client's cursor state (key set / offset) private, so one client's live
        // long-poll never head-of-line blocks another's positioned read and offsets never thrash.
        // The suffix keeps handle ids disjoint from shape ids. Each handle holds the one shape
        // subscription its create/join took; the idle evictor releases it with the handle.
        let handle_id = format!("{}h{}", rec.id, next_cursor());
        handles().lock().unwrap().insert(
            handle_id.clone(),
            Arc::new(HandleEntry {
                stream_path: rec.stream_path.clone(),
                shape_id: rec.id.clone(),
                table: p.table.clone(),
                pk_name: ts.pk_name.clone(),
                last_access: std::sync::Mutex::new(Instant::now()),
                state: tokio::sync::Mutex::new(HandleState { keys, offset: tail.clone() }),
                live_inflight: std::sync::Mutex::new(HashMap::new()),
            }),
        );

        let mut headers = HeaderMap::new();
        headers.insert(HeaderName::from_static("electric-handle"), hv(&handle_id));
        headers.insert(HeaderName::from_static("electric-offset"), hv(&tail));
        headers.insert(HeaderName::from_static("electric-schema"), hv(&schema_str));
        headers.insert(HeaderName::from_static("electric-up-to-date"), hv(""));
        headers.insert(axum::http::header::CACHE_CONTROL, hv("no-store"));
        return Ok(respond(messages, headers, StatusCode::OK));
    }

    // ---- Live: long-poll from `offset`, emit insert/update/delete reconstructed against the key set.
    let handle = p.handle.clone().unwrap();
    let entry = {
        // Registry lock held only for the lookup; never across I/O or the per-handle lock.
        let map = handles().lock().unwrap();
        match map.get(&handle) {
            Some(e) => e.clone(),
            None => return Ok(must_refetch()),
        }
    };
    entry.touch();
    if entry.table != p.table {
        return Err(ApiError::bad_request(format!(
            "table '{}' does not match the shape of handle '{handle}' (table '{}')",
            p.table, entry.table
        )));
    }

    // Live requests at one (handle, offset) coalesce onto a single leader; everything else takes the
    // serialized (state-mutex) path directly. Only the leader / the serialized caller mutates state.
    let outcome = if live {
        let work_engine = engine.clone();
        let work_entry = entry.clone();
        let work_offset = offset.clone();
        coalesce_live(&entry, &offset, move || {
            let engine = work_engine.clone();
            let entry = work_entry.clone();
            let offset = work_offset.clone();
            async move { positioned_read(&engine, &entry, &offset, true).await }
        })
        .await?
    } else {
        positioned_read(&engine, &entry, &offset, false).await?
    };

    let mut headers = HeaderMap::new();
    headers.insert(HeaderName::from_static("electric-handle"), hv(&handle));
    headers.insert(HeaderName::from_static("electric-offset"), hv(&outcome.offset));
    headers.insert(HeaderName::from_static("electric-cursor"), hv(&outcome.cursor.to_string()));
    if outcome.up_to_date {
        headers.insert(HeaderName::from_static("electric-up-to-date"), hv(""));
    }
    headers.insert(axum::http::header::CACHE_CONTROL, hv("no-store"));
    match outcome.body {
        None => {
            let mut resp = (StatusCode::NO_CONTENT, headers).into_response();
            resp.extensions_mut().insert(BodyLen(0));
            Ok(resp)
        }
        Some(body) => {
            headers.insert(axum::http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
            let len = body.len() as u64;
            let mut resp = (StatusCode::OK, headers, body).into_response();
            resp.extensions_mut().insert(BodyLen(len));
            Ok(resp)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ds::EnvelopeHeaders;

    fn env(op: &str, key: &str, offset: &str) -> Envelope {
        Envelope {
            type_: "change".into(),
            key: key.into(),
            value: if op == "delete" { None } else { Some(serde_json::json!({ "id": key })) },
            old: None,
            headers: EnvelopeHeaders {
                operation: op.into(),
                txid: None,
                offset: Some(offset.into()),
                lsn: None,
                seq: None,
            },
        }
    }

    fn page(envs: Vec<Envelope>, next: &str, up_to_date: bool) -> ReadResult {
        ReadResult { envelopes: envs, next_offset: Some(next.into()), up_to_date }
    }

    fn op_and_key(msg: &serde_json::Value) -> (String, String) {
        (
            msg["headers"]["operation"].as_str().unwrap().to_string(),
            msg["key"].as_str().unwrap().to_string(),
        )
    }

    // M9: an empty non-up-to-date page mid-stream must not end the fold (that truncated snapshots);
    // the fold only stops on up-to-date or when the next offset stops advancing.
    #[test]
    fn fold_survives_empty_mid_stream_page() {
        let mut fold = StreamFold::to_tail();
        fold.apply_page(page(vec![env("upsert", "k1", "01")], "01", false));
        assert!(!fold.done, "non-empty non-up-to-date page must not end the fold");
        fold.apply_page(page(vec![], "02", false)); // empty page, but offset advanced
        assert!(!fold.done, "empty page with an advancing offset must not end the fold");
        fold.apply_page(page(vec![env("upsert", "k2", "03")], "03", true));
        assert!(fold.done);
        assert_eq!(fold.offset, "03");
        let mut keys: Vec<_> = fold.rows.keys().cloned().collect();
        keys.sort();
        assert_eq!(keys, vec!["k1", "k2"], "snapshot missed rows past the empty page");
    }

    #[test]
    fn fold_stops_when_offset_stalls() {
        let mut fold = StreamFold::to_tail();
        fold.apply_page(page(vec![env("upsert", "k1", "01")], "01", false));
        // Same next offset, no up-to-date: the stream is not advancing — stop (no infinite loop).
        fold.apply_page(page(vec![], "01", false));
        assert!(fold.done);
        assert_eq!(fold.offset, "01");
    }

    // The idle-live-poll bug: the ds server holds an idle long-poll far longer than our live
    // deadline, so each poll must be bounded by the *remaining* deadline or the 204 never fires
    // (observed in-container: idle live requests hung >60s instead of 204 at ~20s).
    #[tokio::test(start_paused = true)]
    async fn live_poll_deadline_fires_through_hanging_read() {
        let deadline = Instant::now() + Duration::from_millis(200);
        let r = poll_live_until("00".into(), deadline, |_from| async {
            // Simulates the ds server parking an idle long-poll indefinitely.
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Ok::<ReadResult, ()>(page(vec![], "01", false))
        })
        .await
        .unwrap();
        assert!(r.envelopes.is_empty());
        assert_eq!(r.next_offset, None, "mid-poll expiry must not advance the offset");
    }

    #[tokio::test(start_paused = true)]
    async fn live_poll_returns_data_before_deadline() {
        let deadline = Instant::now() + Duration::from_secs(20);
        let r = poll_live_until("00".into(), deadline, |_from| async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok::<ReadResult, ()>(page(vec![env("upsert", "k1", "01")], "01", false))
        })
        .await
        .unwrap();
        assert_eq!(r.envelopes.len(), 1, "data arriving before the deadline must be served");
        assert_eq!(r.next_offset.as_deref(), Some("01"));
    }

    #[tokio::test(start_paused = true)]
    async fn live_poll_deadline_after_empty_pages_keeps_advanced_offset() {
        let calls = Arc::new(AtomicU64::new(0));
        let deadline = Instant::now() + Duration::from_millis(500);
        let c = calls.clone();
        let r = poll_live_until("00".into(), deadline, move |from| {
            let c = c.clone();
            async move {
                if c.fetch_add(1, Ordering::SeqCst) == 0 {
                    assert_eq!(from, "00");
                    // Empty page but the offset advanced (e.g. skipped envelopes) — not data.
                    Ok::<ReadResult, ()>(page(vec![], "05", false))
                } else {
                    assert_eq!(from, "05", "later polls must resume from the advanced offset");
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    Ok(page(vec![], "06", false))
                }
            }
        })
        .await
        .unwrap();
        assert!(r.envelopes.is_empty());
        assert_eq!(
            r.next_offset.as_deref(),
            Some("05"),
            "the last empty page's offset must survive to the 204 so the client resumes past it"
        );
    }

    // C2: rebuilding the key set as of the client's *requested* offset (not the tail).
    #[test]
    fn fold_up_to_stops_at_the_requested_offset() {
        // Stream: insert k1 @01, insert k2 @02, delete k2 @03. A client at offset 02 holds {k1, k2}.
        let mut fold = StreamFold::up_to("02");
        fold.apply_page(page(
            vec![env("upsert", "k1", "01"), env("upsert", "k2", "02"), env("delete", "k2", "03")],
            "03",
            true,
        ));
        assert!(fold.done);
        let mut keys: Vec<_> = fold.rows.keys().cloned().collect();
        keys.sort();
        assert_eq!(keys, vec!["k1", "k2"], "the delete past the requested offset must not be folded in");
    }

    // C2: a delete in the replayed window must reach the client. Folding to the TAIL loses k2 (absent
    // at tail), so the replayed delete was silently dropped and the client kept a deleted row forever.
    #[test]
    fn replay_emits_delete_for_row_deleted_in_the_replayed_window() {
        let all = vec![env("upsert", "k1", "01"), env("upsert", "k2", "02"), env("delete", "k2", "03")];

        // Rebuild as of the client's offset (02)...
        let mut fold = StreamFold::up_to("02");
        fold.apply_page(page(all.clone(), "03", true));
        let mut keys: HashSet<String> = fold.rows.into_keys().collect();

        // ...then replay everything after it.
        let replay: Vec<Envelope> = all.into_iter().filter(|e| offset_after(e.headers.offset.as_deref().unwrap(), "02")).collect();
        let msgs = apply_changes(&mut keys, "id", replay);
        assert_eq!(msgs.len(), 1);
        assert_eq!(op_and_key(&msgs[0]), ("delete".into(), "k2".into()));
        assert!(!keys.contains("k2"));
    }

    // C2: an insert in the replayed window whose key exists at TAIL must be emitted as an insert (the
    // tail-state rebuild classified it as `update`, which Electric's client rejects as update-of-missing).
    #[test]
    fn replay_emits_insert_for_row_reinserted_after_the_replayed_offset() {
        let all = vec![env("upsert", "k1", "01"), env("delete", "k1", "02"), env("upsert", "k1", "03")];

        let mut fold = StreamFold::up_to("02");
        fold.apply_page(page(all.clone(), "03", true));
        let mut keys: HashSet<String> = fold.rows.into_keys().collect();
        assert!(keys.is_empty(), "as of offset 02 the client holds no rows");

        let replay: Vec<Envelope> = all.into_iter().filter(|e| offset_after(e.headers.offset.as_deref().unwrap(), "02")).collect();
        let msgs = apply_changes(&mut keys, "id", replay);
        assert_eq!(msgs.len(), 1);
        assert_eq!(op_and_key(&msgs[0]), ("insert".into(), "k1".into()));
    }

    #[test]
    fn apply_changes_classifies_against_the_key_set() {
        let mut keys: HashSet<String> = ["k1".to_string()].into_iter().collect();
        let msgs = apply_changes(
            &mut keys,
            "id",
            vec![env("upsert", "k1", "01"), env("upsert", "k2", "02"), env("delete", "k9", "03")],
        );
        assert_eq!(op_and_key(&msgs[0]), ("update".into(), "k1".into()));
        assert_eq!(op_and_key(&msgs[1]), ("insert".into(), "k2".into()));
        // delete of a key the client never had is suppressed
        assert_eq!(msgs.len(), 2);
    }

    #[test]
    fn offset_ordering() {
        assert!(offset_after("02", "01"));
        assert!(!offset_after("01", "02"));
        assert!(!offset_after("-1", "01"));
        assert!(offset_after("01", "-1"));
        assert!(!offset_after("01", "01"));
    }

    // ---- live-request coalescing --------------------------------------------------------------

    use std::sync::atomic::AtomicUsize;

    fn test_entry() -> Arc<HandleEntry> {
        Arc::new(HandleEntry {
            stream_path: "s".into(),
            shape_id: "s1".into(),
            table: "t".into(),
            pk_name: "id".into(),
            last_access: std::sync::Mutex::new(Instant::now()),
            state: tokio::sync::Mutex::new(HandleState { keys: HashSet::new(), offset: "-1".into() }),
            live_inflight: std::sync::Mutex::new(HashMap::new()),
        })
    }

    /// Spawn one coalesced live request whose leader work bumps `calls`, sleeps `work_ms` of (paused)
    /// time, and publishes a canned outcome tagged with `offset`.
    fn spawn_live(
        entry: &Arc<HandleEntry>,
        offset: &'static str,
        calls: &Arc<AtomicUsize>,
        work_ms: u64,
    ) -> tokio::task::JoinHandle<Result<ReadOutcome, ApiError>> {
        let entry = entry.clone();
        let calls = calls.clone();
        tokio::spawn(async move {
            coalesce_live(&entry, offset, move || {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    // Stands in for the ds long-poll: long enough that every spawned request has
                    // registered (leader or waiter) before the leader publishes.
                    tokio::time::sleep(Duration::from_millis(work_ms)).await;
                    Ok(ReadOutcome {
                        offset: format!("{offset}-next"),
                        up_to_date: true,
                        cursor: 1,
                        body: Some("[]".into()),
                    })
                }
            })
            .await
        })
    }

    // The write-fanout shape: N concurrent live requests on one handle at one offset must produce ONE
    // leader read, with every request receiving the same response (not N serialized long-polls).
    #[tokio::test]
    async fn concurrent_live_requests_coalesce_to_one_leader() {
        let entry = test_entry();
        let calls = Arc::new(AtomicUsize::new(0));
        let tasks: Vec<_> = (0..200).map(|_| spawn_live(&entry, "07", &calls, 500)).collect();
        for t in tasks {
            let out = t.await.unwrap().expect("every coalesced request gets the leader's response");
            assert_eq!(out.offset, "07-next");
            assert_eq!(out.body.as_deref(), Some("[]"));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1, "exactly one leader must run the read");
        assert!(entry.live_inflight.lock().unwrap().is_empty(), "in-flight slot must be cleared");
    }

    // Requests at *different* offsets are not identical — each gets its own leader.
    #[tokio::test]
    async fn live_requests_at_different_offsets_do_not_coalesce() {
        let entry = test_entry();
        let calls = Arc::new(AtomicUsize::new(0));
        let a = spawn_live(&entry, "07", &calls, 100);
        let b = spawn_live(&entry, "08", &calls, 100);
        assert_eq!(a.await.unwrap().unwrap().offset, "07-next");
        assert_eq!(b.await.unwrap().unwrap().offset, "08-next");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(entry.live_inflight.lock().unwrap().is_empty());
    }

    // A leader whose request future is dropped (client disconnect mid-poll) must not strand waiters:
    // the watch channel closes and a waiter re-elects itself leader.
    #[tokio::test]
    async fn waiters_reelect_when_the_leader_is_cancelled() {
        let entry = test_entry();
        let calls = Arc::new(AtomicUsize::new(0));
        let leader = spawn_live(&entry, "07", &calls, 500);
        tokio::time::sleep(Duration::from_millis(50)).await; // let the leader register
        let waiter = spawn_live(&entry, "07", &calls, 500);
        tokio::time::sleep(Duration::from_millis(50)).await; // let the waiter subscribe
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        leader.abort();
        let out = waiter.await.unwrap().expect("waiter must recover from a cancelled leader");
        assert_eq!(out.offset, "07-next");
        assert_eq!(calls.load(Ordering::SeqCst), 2, "the waiter re-ran the read as the new leader");
        assert!(entry.live_inflight.lock().unwrap().is_empty());
    }
}
