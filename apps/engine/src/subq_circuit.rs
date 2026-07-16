//! The membership circuit: subquery inner-set state AND per-feed key sets, powered by dbsp —
//! the circuit tier's second pipeline family (alongside `arrangements`' counts pipelines).
//!
//! One always-on circuit per engine holds two **upsert maps** (dbsp `add_input_map`): the
//! caller asserts a key's current value *absolutely* (`Insert(v)` / `Delete`), and the
//! operator — which internally maintains the map's contents — derives the exact
//! retract/insert deltas itself. No host-side "remember the old value to retract it"
//! bookkeeping exists anywhere:
//!
//! ```text
//! CONTRIBUTORS (node_id, pk) → projected value      [assert: row's current contribution]
//!   → map to (node_id, value)                       // weight = contributor count
//!   ├─ integrate_trace → membership snapshot        // contains()/has_null()/introspection
//!   └─ distinct → accumulate_output                 // per-step deltas = membership FLIPS
//!
//! FEEDS (feed_id, pk) → ()                          [assert: pk's current feed membership]
//!   ├─ integrate_trace → feed snapshot              // drop-time enumeration, introspection
//!   └─ accumulate_output                            // per-step deltas = THE EMISSIONS:
//!                                                   //   +1 enter, −1 leave (deletes come
//!                                                   //   ONLY from here — structural gating)
//! ```
//!
//! Assertions are idempotent by construction (re-asserting the held value nets to nothing;
//! deleting an absent key nets to nothing), which is what makes deferred, out-of-order flip
//! propagation convergent without any highwater here — the sequencer's `(lsn, seq)` de-dup
//! plus absolute assertion give exactly-once effect.
//!
//! Structure is fixed at construction: two generic inputs serve every node and every feed,
//! so registering templates/nodes/binds/shapes is pure runtime data — no rebuild, ever.
//! Threading mirrors `arrangements`: a dedicated OS thread owns the `DBSPHandle`, fed by a
//! bounded channel; `apply` awaits the step, giving callers read-your-writes over both
//! snapshots.

use std::sync::{Arc, RwLock};

use anyhow::Result;
use dbsp::circuit::CircuitConfig;
use dbsp::dynamic::DowncastTrait;
use dbsp::trace::{BatchReader as DynBatchReaderTrait, Cursor};
use dbsp::typed_batch::{BatchReader, OrdIndexedZSet, OrdZSet, SpineSnapshot};
use dbsp::{MapHandle, OutputHandle, Runtime};
use tokio::sync::{mpsc, oneshot};

use crate::value::{Row, Tup2, Value};

/// An absolute assertion into one of the upsert maps: the key's current value, or absence.
pub type Assert = dbsp::operator::Update<Value, Value>;

/// The membership relation snapshot: `(node_id, value)` keys, weight = contributor count.
type MemberSnapshot = SpineSnapshot<OrdZSet<Row>>;
/// An upsert map's own integral: `(id, pk) → value`, weight 1 per present key.
type MapSnapshot = SpineSnapshot<OrdIndexedZSet<Row, Value>>;
type Slot<T> = Arc<RwLock<Option<T>>>;

/// One membership flip from a circuit step: `(node, value)` entered (`delta > 0`) or left
/// (`delta < 0`) the node's set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberDelta {
    pub node_id: i64,
    pub value: Value,
    pub delta: i64,
}

/// One feed transition from a circuit step: `pk` entered (`delta > 0`) or left (`delta < 0`)
/// feed `feed_id`. A leave is, by construction, a genuine delete — the pk was a member.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeedDelta {
    pub feed_id: i64,
    pub pk: String,
    pub delta: i64,
}

/// One batch of assertions for [`MembershipCircuit::apply`]. Both maps are fed in ONE
/// transaction — a single struct so a call site cannot feed one handle and drop the other.
#[derive(Default)]
pub struct Assertions {
    /// `(Row([Int(node_id), Text(pk)]), Insert(projected value) | Delete)`
    pub contributors: Vec<Tup2<Row, Assert>>,
    /// `(Row([Int(feed_id), Text(pk)]), Insert(Value::Null) | Delete)`
    pub feeds: Vec<Tup2<Row, Assert>>,
}

impl Assertions {
    pub fn is_empty(&self) -> bool {
        self.contributors.is_empty() && self.feeds.is_empty()
    }
}

/// Disk spilling for the membership circuit's relations (ON by default).
struct SpillConfig {
    dir: String,
    /// Spine batches above this size go to layer files (smaller ones stay in memory).
    min_storage_bytes: usize,
    /// Storage buffer-cache budget (MiB); `None` = dbsp's default.
    cache_mib: Option<usize>,
    /// Engine-owned temp dir (removed at circuit shutdown — without checkpointing the
    /// on-disk state is a cache, worthless across boots). `false` = user-specified dir,
    /// never deleted.
    auto: bool,
}

/// Spilling is ON by default: without checkpointing the layer files are a disposable cache,
/// so the default location is a per-circuit temp dir (unique per process + circuit, removed
/// on shutdown; stale dirs from dead processes are swept best-effort at start).
///
/// - `ELECTRIC_IVM_SUBQ_STORAGE=0` — disable (fully in-memory relations).
/// - `ELECTRIC_IVM_SUBQ_STORAGE_DIR=<path>` — explicit location (kept on shutdown).
/// - `ELECTRIC_IVM_SUBQ_MIN_STORAGE_KB` (default 128), `ELECTRIC_IVM_SUBQ_STORAGE_CACHE_MIB`.
fn spill_config_from_env() -> Result<Option<SpillConfig>> {
    if std::env::var("ELECTRIC_IVM_SUBQ_STORAGE").is_ok_and(|v| v == "0") {
        return Ok(None);
    }
    let min_kb: usize = std::env::var("ELECTRIC_IVM_SUBQ_MIN_STORAGE_KB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(128);
    let cache_mib: Option<usize> =
        std::env::var("ELECTRIC_IVM_SUBQ_STORAGE_CACHE_MIB").ok().and_then(|v| v.parse().ok());
    let (dir, auto) = match std::env::var("ELECTRIC_IVM_SUBQ_STORAGE_DIR") {
        Ok(d) if !d.is_empty() => (d, false),
        _ => (default_spill_dir(), true),
    };
    Ok(Some(SpillConfig { dir, min_storage_bytes: min_kb * 1024, cache_mib, auto }))
}

/// A unique engine-owned spill dir: `<tmp>/electric-ivm-subq/<pid>-<seq>`. Sweeps sibling
/// dirs whose owning process is gone (best-effort — a crash leaves the dir behind, and the
/// next boot on the machine reclaims it).
fn default_spill_dir() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let base = std::env::temp_dir().join("electric-ivm-subq");
    if let Ok(entries) = std::fs::read_dir(&base) {
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            let Some(pid) = name.split('-').next().and_then(|p| p.parse::<u32>().ok()) else {
                continue;
            };
            if pid != std::process::id() && !process_alive(pid) {
                let _ = std::fs::remove_dir_all(e.path());
            }
        }
    }
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    base.join(format!("{}-{}", std::process::id(), seq)).to_string_lossy().into_owned()
}

/// Is `pid` a live process? (`kill -0` semantics.)
fn process_alive(pid: u32) -> bool {
    // SAFETY: kill with signal 0 performs only the existence/permission check.
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

enum Cmd {
    Batch { asserts: Assertions, resp: oneshot::Sender<(Vec<MemberDelta>, Vec<FeedDelta>)> },
    /// Diagnostic: the whole circuit's operator memory via dbsp's profiler
    /// (`(total_used_bytes, total_storage_size)`). Heavy (round-trips every worker); test-only.
    Profile { resp: oneshot::Sender<(usize, usize)> },
    Shutdown { resp: oneshot::Sender<()> },
}

/// Measured byte sizes of the circuit's **published** `integrate_trace` snapshots — the only
/// circuit state the host can size without dbsp's (heavy) profiler. Each byte field is dbsp's
/// [`dbsp::trace::BatchReader::approximate_byte_size`]: exact in-memory columnar bytes (keys +
/// weights) when the batch is resident, the on-disk file size when the batch is spilled (so
/// under `ELECTRIC_IVM_SUBQ_STORAGE` these undercount RAM — spilling's whole point). `len` is the
/// total tuple count across all batches the snapshot pins, **including superseded, not-yet-
/// compacted `(key,+1)/(key,-1)` pairs** — the direct signal for compaction/pinning growth.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct CircuitBytes {
    /// The MEMBERS relation snapshot: `(node,value)` keys, weight = contributor count — the
    /// derived, deduplicated membership state published for `contains`/introspection.
    pub members_bytes: usize,
    pub members_len: usize,
    /// The CONTRIBUTORS upsert-map integral: `(node,pk)→value`, one tuple per contributor.
    /// dbsp shares this exact spine with the upsert operator's own integral (registered under
    /// `TraceId(stream)` in the circuit cache), so this is the operator's own integral, not a copy.
    pub contributors_bytes: usize,
    pub contributors_len: usize,
    /// The FEEDS upsert-map integral: `(feed,pk)→()` — likewise the feed upsert operator's own
    /// integral. Zero when `ELECTRIC_IVM_FEED_TRACE=0` (the published feed snapshot is disabled).
    pub feeds_bytes: usize,
    pub feeds_len: usize,
}

impl CircuitBytes {
    /// The raw upsert-map integrals (contributors + feeds): the operators' own input integrals,
    /// one tuple per asserted key. `bytes_circuit_integral` in `GET /memory`.
    pub fn integral_bytes(&self) -> usize {
        self.contributors_bytes + self.feeds_bytes
    }
    /// The derived membership relation snapshot, published purely for read APIs.
    /// `bytes_circuit_snapshots` in `GET /memory`.
    pub fn snapshot_bytes(&self) -> usize {
        self.members_bytes
    }
    /// Total measured membership-circuit owned/on-disk bytes (replaces the old key-count × 88 B
    /// estimate for `bytes_membership_circuit`).
    pub fn total_bytes(&self) -> usize {
        self.integral_bytes() + self.snapshot_bytes()
    }
}

/// Handle to the membership circuit. Cheap to clone; the registry and readers share it.
#[derive(Clone)]
pub struct MembershipCircuit {
    tx: mpsc::Sender<Cmd>,
    members: Slot<MemberSnapshot>,
    contributors: Slot<MapSnapshot>,
    feeds: Slot<MapSnapshot>,
}

impl MembershipCircuit {
    /// Build the circuit and start its thread. State is in-memory only — nodes reseed from
    /// Postgres on registration, feeds from shape backfills.
    ///
    /// `ELECTRIC_IVM_FEED_TRACE=0` disables the published feed-relation trace — the host-side
    /// `ro_snapshot` view used only for drop-time retraction and introspection.
    ///
    /// NOTE (measured, Task 1.3): `feed_stream.integrate_trace()` shares the feed upsert
    /// operator's own integral via dbsp's per-stream `TraceId(stream)` cache, so the published
    /// snapshot is NOT a second logical copy. However, a ~320 MiB RSS delta was measured at
    /// 100k subscriptions with the knob off (docs/bench/shape-memory-scale.md §3) and remains
    /// unexplained — the knob's real-world effect is unresolved, tracked in bead dbsp-ds-2hu.
    /// Behavioral effect: dropped shapes leave their (unreachable — feed ids are never reused)
    /// entries in the operator integral instead of retracting them, a documented trade until
    /// stream-fold drop enumeration lands (bead dbsp-ds-4d8).
    pub fn start() -> Result<MembershipCircuit> {
        let feed_trace = std::env::var("ELECTRIC_IVM_FEED_TRACE").map(|v| v != "0").unwrap_or(true);
        Self::start_with(feed_trace)
    }

    /// [`start`], with the feed-trace choice explicit (tests).
    pub fn start_with(feed_trace: bool) -> Result<MembershipCircuit> {
        Self::start_full(feed_trace, spill_config_from_env()?)
    }

    /// [`start`], with everything explicit.
    fn start_full(feed_trace: bool, spill: Option<SpillConfig>) -> Result<MembershipCircuit> {
        let members: Slot<MemberSnapshot> = Slot::default();
        let contributors: Slot<MapSnapshot> = Slot::default();
        let feeds: Slot<MapSnapshot> = Slot::default();
        let (m_slot, c_slot, f_slot) = (members.clone(), contributors.clone(), feeds.clone());
        // Spill: with a storage dir configured, the circuit's spines (the upsert maps'
        // integrals, the membership trace) page batches above `min_storage_bytes` to layer
        // files under `dir`, keeping a bounded in-memory cache — RAM becomes O(cache)
        // instead of O(relation). In-memory (no dir) remains the default.
        let mut config = CircuitConfig::with_workers(1);
        let cleanup_dir = spill.as_ref().filter(|sp| sp.auto).map(|sp| sp.dir.clone());
        if let Some(sp) = spill {
            use dbsp::circuit::{CircuitStorageConfig, StorageCacheConfig, StorageConfig, StorageOptions};
            std::fs::create_dir_all(&sp.dir)
                .map_err(|e| anyhow::anyhow!("membership circuit storage dir {}: {e}", sp.dir))?;
            let storage = CircuitStorageConfig::for_config(
                StorageConfig { path: sp.dir.clone(), cache: StorageCacheConfig::default() },
                StorageOptions {
                    min_storage_bytes: Some(sp.min_storage_bytes),
                    cache_mib: sp.cache_mib,
                    ..StorageOptions::default()
                },
            )
            .map_err(|e| anyhow::anyhow!("membership circuit storage config: {e}"))?;
            config = config.with_storage(Some(storage));
            tracing::info!(
                "membership circuit: spilling to {} (min_storage_bytes={}, cache_mib={:?})",
                sp.dir, sp.min_storage_bytes, sp.cache_mib
            );
        }
        let (dbsp, (contrib_in, feed_in, flips_out, feeds_out)) =
            Runtime::init_circuit(config, move |circuit| {
                // The upsert patch function is unused (we only Insert/Delete, never Update),
                // but the API requires one; assignment is the natural no-surprise choice.
                let (contrib_stream, contrib_in) =
                    circuit.add_input_map::<Row, Value, Value, _>(|v, u| *v = u.clone());
                let (feed_stream, feed_in) =
                    circuit.add_input_map::<Row, Value, Value, _>(|v, u| *v = u.clone());

                // Contributors: (node,pk)→value ⇒ (node,value) weighted by contributor count.
                let member_counts = contrib_stream
                    .map(|(k, v)| Row(vec![k.0.first().cloned().unwrap_or(Value::Null), v.clone()]));
                member_counts.integrate_trace().apply(move |spine| {
                    *m_slot.write().expect("members slot") = Some(spine.ro_snapshot());
                });
                let flips_out = member_counts.distinct().accumulate_output();

                // Both maps publish their own integrals for prefix enumeration (drop paths).
                contrib_stream.integrate_trace().apply(move |spine| {
                    *c_slot.write().expect("contributors slot") = Some(spine.ro_snapshot());
                });
                if feed_trace {
                    feed_stream.integrate_trace().apply(move |spine| {
                        *f_slot.write().expect("feeds slot") = Some(spine.ro_snapshot());
                    });
                }
                // The feed map's own deltas ARE the emissions.
                let feeds_out = feed_stream.accumulate_output();
                Ok((contrib_in, feed_in, flips_out, feeds_out))
            })
            .map_err(|e| anyhow::anyhow!("membership circuit: init_circuit: {e}"))?;

        let (tx, rx) = mpsc::channel::<Cmd>(256);
        std::thread::Builder::new()
            .name("dbsp-subq".into())
            .spawn(move || {
                circuit_thread(dbsp, contrib_in, feed_in, flips_out, feeds_out, rx);
                // The default spill dir is a per-boot cache (no checkpointing yet): remove it
                // once the circuit is gone. Explicit dirs are the user's to manage.
                if let Some(dir) = cleanup_dir {
                    let _ = std::fs::remove_dir_all(dir);
                }
            })
            .map_err(|e| anyhow::anyhow!("spawning dbsp-subq thread: {e}"))?;

        Ok(MembershipCircuit { tx, members, contributors, feeds })
    }

    /// Assert, step, and return the step's (membership flips, feed transitions). After this
    /// returns, snapshot reads reflect the batch (read-your-writes).
    pub async fn apply(&self, asserts: Assertions) -> (Vec<MemberDelta>, Vec<FeedDelta>) {
        if asserts.is_empty() {
            return (Vec::new(), Vec::new());
        }
        let (resp_tx, resp_rx) = oneshot::channel();
        if self.tx.send(Cmd::Batch { asserts, resp: resp_tx }).await.is_err() {
            tracing::error!("membership circuit: thread gone; dropping assertions");
            return (Vec::new(), Vec::new());
        }
        resp_rx.await.unwrap_or_default()
    }

    /// Is `value` currently a member of node `node_id`'s set? (Contributor count > 0.)
    pub fn contains(&self, node_id: i64, value: &Value) -> bool {
        let target = Row(vec![Value::Int(node_id), value.clone()]);
        let guard = self.members.read().expect("members slot");
        let Some(snap) = guard.as_ref() else { return false };
        let mut cursor = snap.inner().cursor();
        seek(&mut cursor, &target);
        cursor.key_valid()
            && unsafe { cursor.key().downcast::<Row>() } == &target
            && key_weight(&mut cursor) > 0
    }

    /// The node's current distinct-value count and up to `cap` `(value, contributor count)`
    /// pairs, most-shared first (introspection: the visualizer's inner-set index view).
    pub fn values_for_node(&self, node_id: i64, cap: usize) -> (usize, Vec<(Value, usize)>) {
        let guard = self.members.read().expect("members slot");
        let Some(snap) = guard.as_ref() else { return (0, Vec::new()) };
        let mut vals: Vec<(Value, usize)> = Vec::new();
        let mut cursor = snap.inner().cursor();
        seek(&mut cursor, &Row(vec![Value::Int(node_id)]));
        while cursor.key_valid() {
            let key = unsafe { cursor.key().downcast::<Row>() }.clone();
            if key.0.first() != Some(&Value::Int(node_id)) {
                break;
            }
            let w = key_weight(&mut cursor);
            if w > 0 {
                if let Some(v) = key.0.get(1) {
                    vals.push((v.clone(), w as usize));
                }
            }
            cursor.step_key();
        }
        let distinct = vals.len();
        vals.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        vals.truncate(cap);
        (distinct, vals)
    }

    /// Every `(pk, value)` currently contributed to node `node_id` (drop-path enumeration —
    /// O(that node's own contributor count) via prefix seek).
    pub fn contributor_entries(&self, node_id: i64) -> Vec<(String, Value)> {
        map_slice(&self.contributors, node_id)
    }

    /// Every pk currently in feed `feed_id` (shape-drop enumeration + introspection).
    pub fn feed_pks(&self, feed_id: i64) -> Vec<String> {
        map_slice(&self.feeds, feed_id).into_iter().map(|(pk, _)| pk).collect()
    }

    /// Number of pks currently in feed `feed_id` (introspection).
    pub fn feed_len(&self, feed_id: i64) -> usize {
        map_slice(&self.feeds, feed_id).len()
    }

    /// Measured byte sizes of the published snapshots (see [`CircuitBytes`]). Cheap: reads the
    /// three slot snapshots this circuit already holds and sums dbsp's per-batch
    /// `approximate_byte_size`/`len` — no circuit round-trip, no profiler. Safe on the on-demand
    /// `GET /memory` path; never call it from the 500 ms sampler.
    pub fn snapshot_bytes(&self) -> CircuitBytes {
        let (members_bytes, members_len) = snap_size(&self.members);
        let (contributors_bytes, contributors_len) = snap_size(&self.contributors);
        let (feeds_bytes, feeds_len) = snap_size(&self.feeds);
        CircuitBytes {
            members_bytes,
            members_len,
            contributors_bytes,
            contributors_len,
            feeds_bytes,
            feeds_len,
        }
    }

    /// Diagnostic only: the whole circuit's operator memory via dbsp's profiler —
    /// `(total_used_bytes, total_storage_size)` summed over every stateful operator (all
    /// integrals, `distinct` state, `z1` traces, upsert feedback). Heavy — round-trips every
    /// worker through the circuit thread — so this is for tests/attribution, NOT `GET /memory`.
    pub async fn profile_bytes(&self) -> (usize, usize) {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Cmd::Profile { resp: tx }).await.is_err() {
            return (0, 0);
        }
        rx.await.unwrap_or((0, 0))
    }

    /// Stop the circuit thread. State is in-memory only; nothing to persist.
    pub async fn shutdown(&self) {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Cmd::Shutdown { resp: tx }).await.is_ok() {
            let _ = rx.await;
        }
    }
}

/// `(approximate_byte_size, len)` of the snapshot a slot currently holds (0 if unpublished).
/// `approximate_byte_size` is dbsp's per-batch columnar size summed over the batches the
/// `ro_snapshot` pins; `len` is the total tuple count across those batches (superseded,
/// uncompacted `(k,+w)`/`(k,-w)` pairs included). Cheap — no circuit round-trip.
fn snap_size<T: BatchReader>(slot: &Slot<T>) -> (usize, usize) {
    let guard = slot.read().expect("snapshot slot");
    match guard.as_ref() {
        Some(s) => (s.inner().approximate_byte_size(), s.inner().len()),
        None => (0, 0),
    }
}

/// Enumerate one id's slice of an upsert-map integral: `(pk, value)` pairs with positive
/// weight under keys `[Int(id), Text(pk)]`, via prefix seek.
fn map_slice(slot: &Slot<MapSnapshot>, id: i64) -> Vec<(String, Value)> {
    let guard = slot.read().expect("map slot");
    let Some(snap) = guard.as_ref() else { return Vec::new() };
    let mut out = Vec::new();
    let mut cursor = snap.inner().cursor();
    {
        use dbsp::dynamic::Erase;
        let target = Row(vec![Value::Int(id)]);
        cursor.seek_key(target.erase());
    }
    while cursor.key_valid() {
        let key = unsafe { cursor.key().downcast::<Row>() }.clone();
        if key.0.first() != Some(&Value::Int(id)) {
            break;
        }
        let pk = match key.0.get(1) {
            Some(Value::Text(s)) => s.clone(),
            _ => {
                cursor.step_key();
                continue;
            }
        };
        // Net the (value, weight) entries: the present value has weight > 0.
        while cursor.val_valid() {
            if **cursor.weight() > 0 {
                let v = unsafe { cursor.val().downcast::<Value>() }.clone();
                out.push((pk.clone(), v));
                break;
            }
            cursor.step_val();
        }
        cursor.step_key();
    }
    out
}

/// Position a dynamic trace cursor at the first key ≥ `target` (a shorter `Row` is a strict
/// prefix and orders before every same-prefix longer row).
fn seek(
    cursor: &mut impl Cursor<dbsp::dynamic::DynData, dbsp::dynamic::DynUnit, (), dbsp::DynZWeight>,
    target: &Row,
) {
    use dbsp::dynamic::Erase;
    cursor.seek_key(target.erase());
}

/// Sum the current key's net weight (unit-valued zset cursor).
fn key_weight(
    cursor: &mut impl Cursor<dbsp::dynamic::DynData, dbsp::dynamic::DynUnit, (), dbsp::DynZWeight>,
) -> i64 {
    let mut w: i64 = 0;
    while cursor.val_valid() {
        w += **cursor.weight();
        cursor.step_val();
    }
    w
}

/// Drain the distinct's accumulated output: the step's membership flips, net per key.
fn drain_flips(handle: &OutputHandle<SpineSnapshot<OrdZSet<Row>>>, out: &mut Vec<MemberDelta>) {
    let batch = handle.concat();
    let mut cursor = batch.inner().cursor();
    while cursor.key_valid() {
        let key = unsafe { cursor.key().downcast::<Row>() }.clone();
        let mut delta: i64 = 0;
        while cursor.val_valid() {
            delta += **cursor.weight();
            cursor.step_val();
        }
        if delta != 0 {
            if let Some(Value::Int(node_id)) = key.0.first() {
                let value = key.0.get(1).cloned().unwrap_or(Value::Null);
                out.push(MemberDelta { node_id: *node_id, value, delta });
            }
        }
        cursor.step_key();
    }
}

/// Drain the feed map's accumulated output: the step's feed transitions, net per (feed, pk).
/// A value-change on a held key (never happens — the value is constant `Null`) would net to
/// zero here; only presence transitions survive.
fn drain_feed_deltas(
    handle: &OutputHandle<SpineSnapshot<OrdIndexedZSet<Row, Value>>>,
    out: &mut Vec<FeedDelta>,
) {
    let batch = handle.concat();
    let mut cursor = batch.inner().cursor();
    while cursor.key_valid() {
        let key = unsafe { cursor.key().downcast::<Row>() }.clone();
        let mut delta: i64 = 0;
        while cursor.val_valid() {
            delta += **cursor.weight();
            cursor.step_val();
        }
        if delta != 0 {
            if let (Some(Value::Int(feed_id)), Some(Value::Text(pk))) =
                (key.0.first(), key.0.get(1))
            {
                out.push(FeedDelta { feed_id: *feed_id, pk: pk.clone(), delta });
            }
        }
        cursor.step_key();
    }
}

/// The circuit thread: owns the `DBSPHandle`, applies assertion batches, steps, drains.
fn circuit_thread(
    mut dbsp: dbsp::DBSPHandle,
    contrib_in: MapHandle<Row, Value, Value>,
    feed_in: MapHandle<Row, Value, Value>,
    flips_out: OutputHandle<SpineSnapshot<OrdZSet<Row>>>,
    feeds_out: OutputHandle<SpineSnapshot<OrdIndexedZSet<Row, Value>>>,
    mut rx: mpsc::Receiver<Cmd>,
) {
    while let Some(cmd) = rx.blocking_recv() {
        match cmd {
            Cmd::Batch { asserts, resp } => {
                for Tup2(k, upd) in asserts.contributors {
                    contrib_in.push(k, upd);
                }
                for Tup2(k, upd) in asserts.feeds {
                    feed_in.push(k, upd);
                }
                let mut flips = Vec::new();
                let mut feed_deltas = Vec::new();
                match dbsp.transaction() {
                    Ok(()) => {
                        drain_flips(&flips_out, &mut flips);
                        drain_feed_deltas(&feeds_out, &mut feed_deltas);
                    }
                    Err(e) => tracing::error!("membership circuit: transaction failed: {e}"),
                }
                let _ = resp.send((flips, feed_deltas));
            }
            Cmd::Profile { resp } => {
                let bytes = match dbsp.retrieve_profile() {
                    Ok(p) => {
                        let used = p.total_used_bytes().map(|b| b.into_inner() as usize).unwrap_or(0);
                        let stored =
                            p.total_storage_size().map(|b| b.into_inner() as usize).unwrap_or(0);
                        (used, stored)
                    }
                    Err(e) => {
                        tracing::error!("membership circuit: retrieve_profile failed: {e}");
                        (0, 0)
                    }
                };
                let _ = resp.send(bytes);
            }
            Cmd::Shutdown { resp } => {
                let _ = dbsp.kill();
                let _ = resp.send(());
                return;
            }
        }
    }
    // Registry dropped without Shutdown (engine teardown): stop.
    let _ = dbsp.kill();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ckey(node: i64, pk: &str) -> Row {
        Row(vec![Value::Int(node), Value::Text(pk.into())])
    }

    fn contrib(node: i64, pk: &str, v: Option<Value>) -> Assertions {
        Assertions {
            contributors: vec![Tup2(
                ckey(node, pk),
                match v {
                    Some(v) => Assert::Insert(v),
                    None => Assert::Delete,
                },
            )],
            feeds: Vec::new(),
        }
    }

    fn feed(feed_id: i64, pk: &str, present: bool) -> Assertions {
        Assertions {
            contributors: Vec::new(),
            feeds: vec![Tup2(
                ckey(feed_id, pk),
                if present { Assert::Insert(Value::Null) } else { Assert::Delete },
            )],
        }
    }

    /// Flip semantics pin: assertion-driven contributor tracking agrees with the reference
    /// refcount fold — Enter on 0→positive, Leave on positive→0, nothing between; the upsert
    /// map generates the retract/insert pair on value changes itself.
    #[tokio::test(flavor = "multi_thread")]
    async fn flips_on_zero_crossings_and_value_moves() {
        let c = MembershipCircuit::start().unwrap();
        let mut groups = std::collections::HashMap::new();
        let refold = |g: &mut std::collections::HashMap<Value, i64>, contribs: Vec<(Value, i64)>| {
            crate::engine::membership::fold_refcount_flips(g, contribs)
        };

        // a → 7: Enter.
        let (flips, _) = c.apply(contrib(1, "a", Some(Value::Int(7)))).await;
        assert_eq!(flips, vec![MemberDelta { node_id: 1, value: Value::Int(7), delta: 1 }]);
        assert_eq!(refold(&mut groups, vec![(Value::Int(7), 1)]).len(), 1);
        // b → 7: second contributor, no flip.
        let (flips, _) = c.apply(contrib(1, "b", Some(Value::Int(7)))).await;
        assert!(flips.is_empty());
        assert!(refold(&mut groups, vec![(Value::Int(7), 1)]).is_empty());
        // a moves 7→8 in ONE assertion: the map derives retract(7)+insert(8); 7 stays (b).
        let (flips, _) = c.apply(contrib(1, "a", Some(Value::Int(8)))).await;
        assert_eq!(flips, vec![MemberDelta { node_id: 1, value: Value::Int(8), delta: 1 }]);
        assert_eq!(refold(&mut groups, vec![(Value::Int(7), -1), (Value::Int(8), 1)]).len(), 1);
        assert!(c.contains(1, &Value::Int(7)) && c.contains(1, &Value::Int(8)));
        // b leaves: Leave(7).
        let (flips, _) = c.apply(contrib(1, "b", None)).await;
        assert_eq!(flips, vec![MemberDelta { node_id: 1, value: Value::Int(7), delta: -1 }]);
        assert_eq!(refold(&mut groups, vec![(Value::Int(7), -1)]).len(), 1);
        // Idempotence: re-asserting a's current value nets nothing; deleting absent nets nothing.
        let (flips, _) = c.apply(contrib(1, "a", Some(Value::Int(8)))).await;
        assert!(flips.is_empty(), "re-asserting the held value must be a no-op");
        let (flips, _) = c.apply(contrib(1, "z", None)).await;
        assert!(flips.is_empty(), "deleting an absent key must be a no-op");
        c.shutdown().await;
    }

    /// The feed map's deltas are the emissions: enter on first Insert, nothing on repeat,
    /// leave on Delete of a member, nothing on Delete of a never-member (the wake-storm gate,
    /// now structural).
    #[tokio::test(flavor = "multi_thread")]
    async fn feed_deltas_gate_deletes_structurally() {
        let c = MembershipCircuit::start().unwrap();
        // Delete for a never-member pk: NO delta (this was filter_known_members' whole job).
        let (_, fd) = c.apply(feed(9, "ghost", false)).await;
        assert!(fd.is_empty(), "never-member delete must produce nothing");
        // Enter.
        let (_, fd) = c.apply(feed(9, "row1", true)).await;
        assert_eq!(fd, vec![FeedDelta { feed_id: 9, pk: "row1".into(), delta: 1 }]);
        // Repeat assert: nothing.
        let (_, fd) = c.apply(feed(9, "row1", true)).await;
        assert!(fd.is_empty());
        // Genuine leave.
        let (_, fd) = c.apply(feed(9, "row1", false)).await;
        assert_eq!(fd, vec![FeedDelta { feed_id: 9, pk: "row1".into(), delta: -1 }]);
        // And gone again: repeat delete nets nothing.
        let (_, fd) = c.apply(feed(9, "row1", false)).await;
        assert!(fd.is_empty());
        c.shutdown().await;
    }

    /// Prefix enumeration serves the drop paths: a node's contributor slice and a feed's pk
    /// set, each scoped to its own id.
    #[tokio::test(flavor = "multi_thread")]
    async fn prefix_scans_enumerate_slices() {
        let c = MembershipCircuit::start().unwrap();
        c.apply(Assertions {
            contributors: vec![
                Tup2(ckey(1, "a"), Assert::Insert(Value::Int(5))),
                Tup2(ckey(1, "b"), Assert::Insert(Value::Int(6))),
                Tup2(ckey(2, "z"), Assert::Insert(Value::Int(5))),
            ],
            feeds: vec![
                Tup2(ckey(7, "p"), Assert::Insert(Value::Null)),
                Tup2(ckey(7, "q"), Assert::Insert(Value::Null)),
                Tup2(ckey(8, "r"), Assert::Insert(Value::Null)),
            ],
        })
        .await;
        let mut entries = c.contributor_entries(1);
        entries.sort();
        assert_eq!(entries, vec![("a".into(), Value::Int(5)), ("b".into(), Value::Int(6))]);
        assert_eq!(c.contributor_entries(3), vec![]);
        let mut pks = c.feed_pks(7);
        pks.sort();
        assert_eq!(pks, vec!["p".to_string(), "q".to_string()]);
        assert_eq!(c.feed_len(8), 1);
        // Deleting via enumeration empties the slice without touching neighbours.
        let feeds = c.feed_pks(7).into_iter().map(|pk| Tup2(ckey(7, &pk), Assert::Delete)).collect();
        c.apply(Assertions { contributors: Vec::new(), feeds }).await;
        assert_eq!(c.feed_len(7), 0);
        assert_eq!(c.feed_len(8), 1);
        c.shutdown().await;
    }

    /// With the feed trace disabled, emissions (feed deltas) still flow — only the host-side
    /// enumeration view is gone: feed_pks/feed_len return empty. (Real memory saved is
    /// negligible — see `feed_trace_snapshot_shares_operator_integral`.)
    #[tokio::test(flavor = "multi_thread")]
    async fn feed_trace_knob_disables_enumeration_not_emissions() {
        let c = MembershipCircuit::start_with(false).unwrap();
        let (_, fd) = c.apply(feed(9, "row1", true)).await;
        assert_eq!(fd, vec![FeedDelta { feed_id: 9, pk: "row1".into(), delta: 1 }]);
        let (_, fd) = c.apply(feed(9, "row1", false)).await;
        assert_eq!(fd, vec![FeedDelta { feed_id: 9, pk: "row1".into(), delta: -1 }], "deletes still gate structurally");
        let (_, fd) = c.apply(feed(9, "ghost", false)).await;
        assert!(fd.is_empty(), "never-member gate intact without the trace");
        c.apply(feed(9, "row2", true)).await;
        assert_eq!(c.feed_pks(9), Vec::<String>::new(), "no enumeration copy");
        assert_eq!(c.feed_len(9), 0);
        c.shutdown().await;
    }

    /// Spill mode: same semantics, state pages to layer files under the storage dir.
    #[tokio::test(flavor = "multi_thread")]
    async fn spill_mode_preserves_semantics_and_writes_files() {
        let dir = std::env::temp_dir().join(format!("subq-spill-test-{}", std::process::id()));
        let c = MembershipCircuit::start_full(
            true,
            Some(SpillConfig {
                dir: dir.to_string_lossy().into_owned(),
                min_storage_bytes: 1, // spill aggressively so even tiny batches hit disk
                cache_mib: Some(8),
                auto: false, // the test owns and removes this dir itself
            }),
        )
        .unwrap();
        // Enough contributor entries to force at least one on-disk batch.
        let contributors = (0..2000)
            .map(|i| Tup2(ckey(1, &format!("pk{i}")), Assert::Insert(Value::Int(i % 50))))
            .collect();
        let (flips, _) = c.apply(Assertions { contributors, feeds: Vec::new() }).await;
        assert_eq!(flips.len(), 50, "50 distinct values entered");
        assert!(c.contains(1, &Value::Int(7)));
        let (distinct, _) = c.values_for_node(1, 5);
        assert_eq!(distinct, 50);
        // Storage dir gained content (layer files / runtime metadata).
        let entries = std::fs::read_dir(&dir).map(|d| d.count()).unwrap_or(0);
        assert!(entries > 0, "storage dir must contain spilled state, found {entries} entries");
        c.shutdown().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `snapshot_bytes` measures real dbsp columnar bytes: zero when empty, non-zero and
    /// split into the raw upsert-map integrals (contributors + feeds) vs the derived members
    /// snapshot once populated. This is the measured basis for `GET /memory`'s
    /// `bytes_circuit_integral` / `bytes_circuit_snapshots` split.
    #[tokio::test(flavor = "multi_thread")]
    async fn snapshot_bytes_measures_and_splits_relations() {
        let c = MembershipCircuit::start_full(true, None).unwrap();
        assert_eq!(c.snapshot_bytes(), CircuitBytes::default(), "empty circuit owns no batch bytes");

        let mut a = Assertions::default();
        for i in 0..1000i64 {
            // 1000 contributors over 100 distinct values on one node.
            a.contributors.push(Tup2(ckey(1, &format!("c{i}")), Assert::Insert(Value::Int(i % 100))));
            a.feeds.push(Tup2(ckey(9, &format!("f{i}")), Assert::Insert(Value::Null)));
        }
        c.apply(a).await;

        let cb = c.snapshot_bytes();
        // Contributors integral: 1000 live tuples; feeds integral: 1000; members: 100 distinct.
        assert_eq!(cb.contributors_len, 1000);
        assert_eq!(cb.feeds_len, 1000);
        assert_eq!(cb.members_len, 100, "members is the deduplicated (node,value) relation");
        assert!(cb.contributors_bytes > 0 && cb.feeds_bytes > 0 && cb.members_bytes > 0);
        // The integral (raw maps) dwarfs the derived membership snapshot.
        assert_eq!(cb.integral_bytes(), cb.contributors_bytes + cb.feeds_bytes);
        assert_eq!(cb.snapshot_bytes(), cb.members_bytes);
        assert!(cb.integral_bytes() > cb.snapshot_bytes());
        assert_eq!(cb.total_bytes(), cb.integral_bytes() + cb.snapshot_bytes());
        c.shutdown().await;
    }

    /// FEED_TRACE=0 does NOT free operator memory: dbsp shares the feed upsert operator's own
    /// integral with our published `integrate_trace` snapshot (per-stream `TraceId` cache), so
    /// disabling the trace only drops the host-side `ro_snapshot` view (feeds_bytes → 0) while
    /// the circuit's real resident bytes (profiler `total_used_bytes`) are unchanged. Guards the
    /// corrected doc claim on `start_with`'s `feed_trace` knob.
    #[tokio::test(flavor = "multi_thread")]
    async fn feed_trace_snapshot_shares_operator_integral() {
        let load = || {
            let mut a = Assertions::default();
            for i in 0..3000i64 {
                a.feeds.push(Tup2(ckey(9, &format!("f{i}")), Assert::Insert(Value::Null)));
            }
            a
        };
        let on = MembershipCircuit::start_full(true, None).unwrap();
        on.apply(load()).await;
        let (used_on, _) = on.profile_bytes().await;
        let feeds_view = on.snapshot_bytes().feeds_bytes;
        on.shutdown().await;

        let off = MembershipCircuit::start_full(false, None).unwrap();
        off.apply(load()).await;
        let (used_off, _) = off.profile_bytes().await;
        assert_eq!(off.snapshot_bytes().feeds_bytes, 0, "no published feed view when disabled");
        off.shutdown().await;

        // The host-side feed view is non-trivial, yet the operator memory is within 5% either way.
        assert!(feeds_view > 100_000, "feed view should be a meaningful number of bytes");
        let (hi, lo) = (used_on.max(used_off), used_on.min(used_off));
        assert!(
            hi - lo < hi / 20,
            "disabling the feed trace changed circuit memory by >5% ({used_on} vs {used_off}); \
             the integrate_trace snapshot is NOT sharing the upsert integral as documented"
        );
    }

    /// Pinning/compaction invariant: churning a FIXED live-key set across many steps must keep
    /// the published snapshots bounded — dbsp's background merger reclaims the superseded
    /// `(k,+w)/(k,-w)` tuples despite our per-step `ro_snapshot` (which pins at most one step,
    /// since each step replaces the slot). If a snapshot pinned pre-compaction batches, the
    /// spine's `len` would grow ~linearly with the step count instead of staying O(live keys).
    ///
    /// We assert on `len` (total tuples across pinned batches, superseded ones included) rather
    /// than the brief's "snapshot bytes ≤ integral bytes × 1.5": members is a deduplicated
    /// projection that is *structurally* ≤ the contributor integral, so that ratio holds
    /// trivially and cannot detect intra-spine pinning. The real risk is superseded-batch
    /// accumulation, which a growth-vs-steps bound tests directly.
    #[tokio::test(flavor = "multi_thread")]
    async fn snapshots_do_not_pin_precompaction_batches() {
        const LIVE: i64 = 400;
        const STEPS: i64 = 400;
        let c = MembershipCircuit::start_full(true, None).unwrap();
        let seed = (0..LIVE)
            .map(|i| Tup2(ckey(7, &format!("k{i}")), Assert::Insert(Value::Int(0))))
            .collect();
        c.apply(Assertions { contributors: seed, feeds: Vec::new() }).await;

        let mut max_contrib_len = 0usize;
        for step in 1..=STEPS {
            let contributors = (0..LIVE)
                .map(|i| Tup2(ckey(7, &format!("k{i}")), Assert::Insert(Value::Int(step))))
                .collect();
            c.apply(Assertions { contributors, feeds: Vec::new() }).await;
            max_contrib_len = max_contrib_len.max(c.snapshot_bytes().contributors_len);
        }

        // Live set is unchanged: still exactly LIVE keys collapsing to 1 distinct value (the
        // netted-weight view, which ignores superseded uncompacted tuples).
        assert!(c.contains(7, &Value::Int(STEPS)));
        assert_eq!(c.values_for_node(7, 0).0, 1, "one live value across all {LIVE} keys");
        // Compaction bound: the spine never holds more than a small multiple of the live-key
        // count. A pin/leak would make this ~STEPS × 2 × LIVE (≈320,000). Observed peak ≈ 15×
        // LIVE; the ×50 threshold is a deliberate flakiness/power tradeoff (generous headroom
        // for merge-cadence timing variance while still an order of magnitude below linear pin).
        let bound = (LIVE as usize) * 50;
        assert!(
            max_contrib_len <= bound,
            "contributor spine grew to {max_contrib_len} tuples over {STEPS} steps (bound {bound}); \
             superseded batches are being pinned — compaction is blocked"
        );
        c.shutdown().await;
    }

    /// Same value on two nodes stays isolated per node_id (unchanged from the zset design).
    #[tokio::test(flavor = "multi_thread")]
    async fn nodes_are_isolated_and_null_bucket_works() {
        let c = MembershipCircuit::start().unwrap();
        c.apply(Assertions {
            contributors: vec![
                Tup2(ckey(1, "a"), Assert::Insert(Value::Int(7))),
                Tup2(ckey(2, "a"), Assert::Insert(Value::Int(7))),
                Tup2(ckey(1, "n"), Assert::Insert(Value::Null)),
            ],
            feeds: Vec::new(),
        })
        .await;
        let (flips, _) = c.apply(contrib(1, "a", None)).await;
        assert_eq!(flips, vec![MemberDelta { node_id: 1, value: Value::Int(7), delta: -1 }]);
        assert!(!c.contains(1, &Value::Int(7)));
        assert!(c.contains(2, &Value::Int(7)), "node 2 unaffected");
        assert!(c.contains(1, &Value::Null), "the NULL bucket is an ordinary key");
        let (distinct, vals) = c.values_for_node(1, 10);
        assert_eq!(distinct, 1);
        assert_eq!(vals, vec![(Value::Null, 1)]);
        c.shutdown().await;
    }
}
