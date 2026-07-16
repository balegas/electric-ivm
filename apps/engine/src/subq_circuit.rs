//! The membership circuit: subquery inner-set state, powered by dbsp — the circuit tier's second
//! pipeline family (alongside `arrangements`' counts pipelines).
//!
//! One always-on circuit per engine holds the CONTRIBUTORS **upsert map** (dbsp `add_input_map`,
//! key→value). The caller asserts a key's current value *absolutely* (`Insert(v)`/`Delete`), and
//! the operator — which internally maintains the relation's contents — derives the exact
//! retract/insert deltas itself. No host-side "remember the old value to retract it" bookkeeping
//! exists anywhere. Keys are `PkKey { id, pk_id }` — the pk is a `u32` dictionary id (see
//! [`crate::pk_dict`]), never a heap string:
//!
//! ```text
//! CONTRIBUTORS (node_id, pk_id) → projected value   [assert: row's current contribution]
//!   → map to (node_id, value)                       // weight = contributor count
//!   ├─ integrate_trace → membership snapshot        // contains()/has_null()/introspection
//!   └─ distinct → accumulate_output                 // per-step deltas = membership FLIPS
//! ```
//!
//! The per-feed key sets (the delete gate) used to be a second in-circuit upsert-SET relation;
//! Task 2.2 (bead dbsp-ds-dh6) moved them OUT to host-side Roaring bitmaps
//! ([`crate::subq_feed::FeedSet`]) — ~10–19× lighter and needing no spill. This circuit now holds
//! only contributors.
//!
//! Assertions are idempotent by construction (re-asserting the held value nets to nothing;
//! deleting an absent key nets to nothing), which is what makes deferred, out-of-order flip
//! propagation convergent without any highwater here — the sequencer's `(lsn, seq)` de-dup
//! plus absolute assertion give exactly-once effect.
//!
//! Structure is fixed at construction: one generic input serves every node, so registering
//! templates/nodes/binds is pure runtime data — no rebuild, ever. Threading mirrors
//! `arrangements`: a dedicated OS thread owns the `DBSPHandle`, fed by a bounded channel; `apply`
//! awaits the step, giving callers read-your-writes over the snapshot.

use std::sync::{Arc, RwLock};

use anyhow::Result;
use dbsp::circuit::CircuitConfig;
use dbsp::dynamic::DowncastTrait;
use dbsp::trace::{BatchReader as DynBatchReaderTrait, Cursor};
use dbsp::typed_batch::{BatchReader, OrdIndexedZSet, OrdZSet, SpineSnapshot};
use dbsp::{MapHandle, OutputHandle, Runtime};
use tokio::sync::{mpsc, oneshot};

use feldera_macros::IsNone;
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;

use crate::value::{Row, Tup2, Value};

/// An absolute assertion into one of the upsert maps: the key's current value, or absence.
pub type Assert = dbsp::operator::Update<Value, Value>;

/// A circuit relation key: a relation-scoped id — `node_id` for contributors, `feed_id` for feeds
/// — plus the primary key's global dictionary id (see [`crate::pk_dict::PkDict`]). This replaces
/// the old `Row([Int(id), Text(pk)])` key: 12 inline bytes with NO per-entry heap string (the
/// string lives once in the dictionary), which is the memory win driving Task 2.1.
#[derive(
    Clone, Copy, Default, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, SizeOf, Archive, Serialize,
    Deserialize, IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd, Hash))]
pub struct PkKey {
    /// `node_id` (contributors) or `feed_id` (feeds). Ordered FIRST, so one id's entries are
    /// contiguous in the relation — prefix scans seek `PkKey { id, pk: 0 }` and iterate while the
    /// id matches (`pk: 0` is the least key for any id, `pk` being `u32`).
    pub id: i64,
    /// The primary key's dictionary id (`PkDict::get_or_insert`).
    pub pk: u32,
}

/// The membership relation snapshot: `(node_id, value)` keys, weight = contributor count.
type MemberSnapshot = SpineSnapshot<OrdZSet<Row>>;
/// The CONTRIBUTORS upsert-map's own integral: `(node_id, pk_id) → value`, weight 1 per present
/// key (the projected value is carried, so this stays an indexed map).
type MapSnapshot = SpineSnapshot<OrdIndexedZSet<PkKey, Value>>;
type Slot<T> = Arc<RwLock<Option<T>>>;

/// One membership flip from a circuit step: `(node, value)` entered (`delta > 0`) or left
/// (`delta < 0`) the node's set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberDelta {
    pub node_id: i64,
    pub value: Value,
    pub delta: i64,
}

/// One batch of contributor assertions for [`MembershipCircuit::apply`], fed in ONE transaction.
#[derive(Default)]
pub struct Assertions {
    /// `(PkKey { node_id, pk_id }, Insert(projected value) | Delete)`
    pub contributors: Vec<Tup2<PkKey, Assert>>,
}

impl Assertions {
    pub fn is_empty(&self) -> bool {
        self.contributors.is_empty()
    }
}

/// Disk spilling for the membership circuit's relations (ON by default).
struct SpillConfig {
    dir: String,
    /// Spine batches above this size go to layer files (smaller ones stay in memory).
    min_storage_bytes: usize,
    /// Storage buffer-cache budget, in MiB, passed to dbsp as `StorageOptions::cache_mib`.
    ///
    /// dbsp treats a `Some` value as the **grand TOTAL** cache size and uses it verbatim — no
    /// further multiplication by worker count or thread-type (see `RuntimeInner::new` in
    /// dbsp 0.318: `Some(cache_mib) => cache_mib * 1 MiB`, full stop). The ×nworkers×thread-types
    /// scaling dbsp describes in its own docs only fires on ITS unset-default (`None` ⇒
    /// `256 MiB × nworkers × ThreadType::LENGTH(=2)`); for our 1-worker circuit that default is
    /// 512 MiB, which is why we always pass an explicit value here (see
    /// [`spill_config_from_env`]/[`storage_cache_mib`]) instead of ever leaving this `None`.
    cache_mib: usize,
    /// Engine-owned temp dir (removed at circuit shutdown — without checkpointing the
    /// on-disk state is a cache, worthless across boots). `false` = user-specified dir,
    /// never deleted.
    auto: bool,
}

/// The engine's own default for [`SpillConfig::cache_mib`] when
/// `ELECTRIC_IVM_SUBQ_STORAGE_CACHE_MIB` is unset — a hard bound on dbsp's unset-default, which
/// for this circuit's 1-worker layout resolves to 256 MiB × 1 worker × 2 thread-types = 512 MiB
/// (see docs/bench/mem-attribution-100k.md §2c). Measured operator state across all circuits on
/// the 100k-subscription benchmark is well under 1 MiB, so the LRU was filling toward an
/// oversized ceiling regardless of working set; 64 MiB cut ~40% off process RSS with identical
/// semantics on that workload.
const DEFAULT_STORAGE_CACHE_MIB: usize = 64;

/// Parses `ELECTRIC_IVM_SUBQ_STORAGE_CACHE_MIB`'s raw value (`None` if the var is unset), giving
/// the storage buffer-cache budget in MiB — TOTAL across every worker and thread-type (see
/// [`SpillConfig::cache_mib`]). Unset or unparseable ⇒ [`DEFAULT_STORAGE_CACHE_MIB`]; a valid
/// value overrides it exactly (dbsp uses it verbatim as the total, so `=64` here means the same
/// 64 MiB TOTAL as the default, not per-thread-type).
fn storage_cache_mib(raw: Option<&str>) -> usize {
    raw.and_then(|v| v.parse().ok()).unwrap_or(DEFAULT_STORAGE_CACHE_MIB)
}

/// Spilling is ON by default: without checkpointing the layer files are a disposable cache,
/// so the default location is a per-circuit temp dir (unique per process + circuit, removed
/// on shutdown; stale dirs from dead processes are swept best-effort at start).
///
/// - `ELECTRIC_IVM_SUBQ_STORAGE=0` — disable (fully in-memory relations).
/// - `ELECTRIC_IVM_SUBQ_STORAGE_DIR=<path>` — explicit location (kept on shutdown).
/// - `ELECTRIC_IVM_SUBQ_MIN_STORAGE_KB` (default 128).
/// - `ELECTRIC_IVM_SUBQ_STORAGE_CACHE_MIB` (default 64, TOTAL across all workers/thread-types —
///   see [`storage_cache_mib`]; dbsp's own unset-default would be 512 MiB for this circuit).
fn spill_config_from_env() -> Result<Option<SpillConfig>> {
    if std::env::var("ELECTRIC_IVM_SUBQ_STORAGE").is_ok_and(|v| v == "0") {
        return Ok(None);
    }
    let min_kb: usize = std::env::var("ELECTRIC_IVM_SUBQ_MIN_STORAGE_KB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(128);
    let cache_mib =
        storage_cache_mib(std::env::var("ELECTRIC_IVM_SUBQ_STORAGE_CACHE_MIB").ok().as_deref());
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
    Batch { asserts: Assertions, resp: oneshot::Sender<Vec<MemberDelta>> },
    /// Diagnostic: totals plus the full per-operator profile as dbsp's own JSON
    /// (`DbspProfile::as_json` — worker profiles with per-node `used_memory_bytes` etc., plus
    /// the named operator graph). Heavy; on-demand only (`GET /debug/dbsp-profile`).
    ProfileDump { resp: oneshot::Sender<(usize, usize, String)> },
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
}

impl CircuitBytes {
    /// The raw contributor upsert-map integral: the operator's own input integral, one tuple per
    /// asserted key. `bytes_circuit_integral` in `GET /memory`. (Per-feed key sets left the
    /// circuit in Task 2.2 — see [`crate::subq_feed::FeedSet`], reported as `bytes_feed_sets`.)
    pub fn integral_bytes(&self) -> usize {
        self.contributors_bytes
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
}

impl MembershipCircuit {
    /// Build the circuit and start its thread. State is in-memory only — nodes reseed from
    /// Postgres on registration.
    pub fn start() -> Result<MembershipCircuit> {
        Self::start_full(spill_config_from_env()?)
    }

    /// [`start`], with everything explicit (tests).
    fn start_full(spill: Option<SpillConfig>) -> Result<MembershipCircuit> {
        let members: Slot<MemberSnapshot> = Slot::default();
        let contributors: Slot<MapSnapshot> = Slot::default();
        let (m_slot, c_slot) = (members.clone(), contributors.clone());
        // Spill: with a storage dir configured, the circuit's spines (the contributor upsert
        // map's integral, the membership trace) page batches above `min_storage_bytes` to layer
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
                    cache_mib: Some(sp.cache_mib),
                    ..StorageOptions::default()
                },
            )
            .map_err(|e| anyhow::anyhow!("membership circuit storage config: {e}"))?;
            config = config.with_storage(Some(storage));
            tracing::info!(
                "membership circuit: spilling to {} (min_storage_bytes={}, cache_mib={})",
                sp.dir, sp.min_storage_bytes, sp.cache_mib
            );
        }
        let (dbsp, (contrib_in, flips_out)) =
            Runtime::init_circuit(config, move |circuit| {
                // Contributors are a key→value upsert MAP (the projected value is carried). The
                // upsert patch function is unused (we only Insert/Delete, never Update), but the
                // API requires one; assignment is the natural no-surprise choice.
                let (contrib_stream, contrib_in) =
                    circuit.add_input_map::<PkKey, Value, Value, _>(|v, u| *v = u.clone());

                // Contributors: (node,pk_id)→value ⇒ (node,value) weighted by contributor count.
                let member_counts =
                    contrib_stream.map(|(k, v)| Row(vec![Value::Int(k.id), v.clone()]));
                member_counts.integrate_trace().apply(move |spine| {
                    *m_slot.write().expect("members slot") = Some(spine.ro_snapshot());
                });
                let flips_out = member_counts.distinct().accumulate_output();

                // The contributor relation publishes its own integral for prefix enumeration (the
                // node drop path).
                contrib_stream.integrate_trace().apply(move |spine| {
                    *c_slot.write().expect("contributors slot") = Some(spine.ro_snapshot());
                });
                Ok((contrib_in, flips_out))
            })
            .map_err(|e| anyhow::anyhow!("membership circuit: init_circuit: {e}"))?;

        let (tx, rx) = mpsc::channel::<Cmd>(256);
        std::thread::Builder::new()
            .name("dbsp-subq".into())
            .spawn(move || {
                circuit_thread(dbsp, contrib_in, flips_out, rx);
                // The default spill dir is a per-boot cache (no checkpointing yet): remove it
                // once the circuit is gone. Explicit dirs are the user's to manage.
                if let Some(dir) = cleanup_dir {
                    let _ = std::fs::remove_dir_all(dir);
                }
            })
            .map_err(|e| anyhow::anyhow!("spawning dbsp-subq thread: {e}"))?;

        Ok(MembershipCircuit { tx, members, contributors })
    }

    /// Assert, step, and return the step's membership flips. After this returns, snapshot reads
    /// reflect the batch (read-your-writes).
    pub async fn apply(&self, asserts: Assertions) -> Vec<MemberDelta> {
        if asserts.is_empty() {
            return Vec::new();
        }
        let (resp_tx, resp_rx) = oneshot::channel();
        if self.tx.send(Cmd::Batch { asserts, resp: resp_tx }).await.is_err() {
            tracing::error!("membership circuit: thread gone; dropping assertions");
            return Vec::new();
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

    /// Every `(pk_id, value)` currently contributed to node `node_id` (drop-path enumeration —
    /// O(that node's own contributor count) via prefix seek). pk ids resolve to strings via the
    /// registry's dictionary if a string is needed (the drop paths key on the id directly).
    pub fn contributor_entries(&self, node_id: i64) -> Vec<(u32, Value)> {
        map_slice(&self.contributors, node_id)
    }

    /// Measured byte sizes of the published snapshots (see [`CircuitBytes`]). Cheap: reads the
    /// two slot snapshots this circuit holds and sums dbsp's per-batch
    /// `approximate_byte_size`/`len` — no circuit round-trip, no profiler. Safe on the on-demand
    /// `GET /memory` path; never call it from the 500 ms sampler.
    pub fn snapshot_bytes(&self) -> CircuitBytes {
        let (members_bytes, members_len) = snap_size(&self.members);
        let (contributors_bytes, contributors_len) = snap_size(&self.contributors);
        CircuitBytes { members_bytes, members_len, contributors_bytes, contributors_len }
    }

    /// Diagnostic only: `(total_used_bytes, total_storage_size, per-operator profile JSON)` —
    /// the JSON is dbsp's own `DbspProfile::as_json` (per-node `used_memory_bytes` + the named
    /// operator graph). Heavy — round-trips every worker through the circuit thread — so this
    /// is on-demand only, never from the 500 ms sampler.
    pub async fn profile_dump(&self) -> (usize, usize, String) {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Cmd::ProfileDump { resp: tx }).await.is_err() {
            return (0, 0, String::new());
        }
        rx.await.unwrap_or((0, 0, String::new()))
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

/// Enumerate one id's slice of an upsert-map integral: `(pk_id, value)` pairs with positive
/// weight under keys `PkKey { id, pk_id }`, via prefix seek (`PkKey { id, pk: 0 }` is the least
/// key for `id`; iterate while `id` matches).
fn map_slice(slot: &Slot<MapSnapshot>, id: i64) -> Vec<(u32, Value)> {
    let guard = slot.read().expect("map slot");
    let Some(snap) = guard.as_ref() else { return Vec::new() };
    let mut out = Vec::new();
    let mut cursor = snap.inner().cursor();
    {
        use dbsp::dynamic::Erase;
        let target = PkKey { id, pk: 0 };
        cursor.seek_key(target.erase());
    }
    while cursor.key_valid() {
        let key = *unsafe { cursor.key().downcast::<PkKey>() };
        if key.id != id {
            break;
        }
        // Net the (value, weight) entries: the present value has weight > 0.
        while cursor.val_valid() {
            if **cursor.weight() > 0 {
                let v = unsafe { cursor.val().downcast::<Value>() }.clone();
                out.push((key.pk, v));
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

/// The circuit thread: owns the `DBSPHandle`, applies assertion batches, steps, drains.
fn circuit_thread(
    mut dbsp: dbsp::DBSPHandle,
    contrib_in: MapHandle<PkKey, Value, Value>,
    flips_out: OutputHandle<SpineSnapshot<OrdZSet<Row>>>,
    mut rx: mpsc::Receiver<Cmd>,
) {
    while let Some(cmd) = rx.blocking_recv() {
        match cmd {
            Cmd::Batch { asserts, resp } => {
                for Tup2(k, upd) in asserts.contributors {
                    contrib_in.push(k, upd);
                }
                let mut flips = Vec::new();
                match dbsp.transaction() {
                    Ok(()) => drain_flips(&flips_out, &mut flips),
                    Err(e) => tracing::error!("membership circuit: transaction failed: {e}"),
                }
                let _ = resp.send(flips);
            }
            Cmd::ProfileDump { resp } => {
                let dump = match dbsp.retrieve_profile() {
                    Ok(p) => {
                        let used = p.total_used_bytes().map(|b| b.into_inner() as usize).unwrap_or(0);
                        let stored =
                            p.total_storage_size().map(|b| b.into_inner() as usize).unwrap_or(0);
                        (used, stored, p.as_json())
                    }
                    Err(e) => {
                        tracing::error!("membership circuit: retrieve_profile failed: {e}");
                        (0, 0, format!("{{\"error\":\"retrieve_profile: {e}\"}}"))
                    }
                };
                let _ = resp.send(dump);
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

    /// Pins the default: `ELECTRIC_IVM_SUBQ_STORAGE_CACHE_MIB` unset (or unparseable) bounds the
    /// cache at [`DEFAULT_STORAGE_CACHE_MIB`] (64 MiB TOTAL) rather than falling through to
    /// dbsp's own unset-default (512 MiB for this circuit's 1-worker layout — see
    /// docs/bench/mem-attribution-100k.md §2c). A pure function (not `spill_config_from_env`
    /// itself) so the test doesn't mutate process-global env vars across parallel test threads.
    #[test]
    fn storage_cache_mib_defaults_to_64_when_unset() {
        assert_eq!(storage_cache_mib(None), 64);
        assert_eq!(storage_cache_mib(Some("")), 64);
        assert_eq!(storage_cache_mib(Some("not-a-number")), 64);
    }

    /// An explicit `ELECTRIC_IVM_SUBQ_STORAGE_CACHE_MIB` value overrides the default exactly —
    /// dbsp uses it verbatim as the TOTAL cache size, so `"64"` here means the same 64 MiB total
    /// as the default (not per-thread-type), and other values scale linearly from there.
    #[test]
    fn storage_cache_mib_respects_explicit_override() {
        assert_eq!(storage_cache_mib(Some("64")), 64);
        assert_eq!(storage_cache_mib(Some("256")), 256);
        assert_eq!(storage_cache_mib(Some("8")), 8);
    }

    fn ckey(id: i64, pk: u32) -> PkKey {
        PkKey { id, pk }
    }

    fn contrib(node: i64, pk: u32, v: Option<Value>) -> Assertions {
        Assertions {
            contributors: vec![Tup2(
                ckey(node, pk),
                match v {
                    Some(v) => Assert::Insert(v),
                    None => Assert::Delete,
                },
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

        // a → 7: Enter. (pk ids: a=1, b=2, z=3.)
        let flips = c.apply(contrib(1, 1, Some(Value::Int(7)))).await;
        assert_eq!(flips, vec![MemberDelta { node_id: 1, value: Value::Int(7), delta: 1 }]);
        assert_eq!(refold(&mut groups, vec![(Value::Int(7), 1)]).len(), 1);
        // b → 7: second contributor, no flip.
        let flips = c.apply(contrib(1, 2, Some(Value::Int(7)))).await;
        assert!(flips.is_empty());
        assert!(refold(&mut groups, vec![(Value::Int(7), 1)]).is_empty());
        // a moves 7→8 in ONE assertion: the map derives retract(7)+insert(8); 7 stays (b).
        let flips = c.apply(contrib(1, 1, Some(Value::Int(8)))).await;
        assert_eq!(flips, vec![MemberDelta { node_id: 1, value: Value::Int(8), delta: 1 }]);
        assert_eq!(refold(&mut groups, vec![(Value::Int(7), -1), (Value::Int(8), 1)]).len(), 1);
        assert!(c.contains(1, &Value::Int(7)) && c.contains(1, &Value::Int(8)));
        // b leaves: Leave(7).
        let flips = c.apply(contrib(1, 2, None)).await;
        assert_eq!(flips, vec![MemberDelta { node_id: 1, value: Value::Int(7), delta: -1 }]);
        assert_eq!(refold(&mut groups, vec![(Value::Int(7), -1)]).len(), 1);
        // Idempotence: re-asserting a's current value nets nothing; deleting absent nets nothing.
        let flips = c.apply(contrib(1, 1, Some(Value::Int(8)))).await;
        assert!(flips.is_empty(), "re-asserting the held value must be a no-op");
        let flips = c.apply(contrib(1, 3, None)).await;
        assert!(flips.is_empty(), "deleting an absent key must be a no-op");
        c.shutdown().await;
    }

    // The feed relation's delete-gate semantics (never-member gate, enter, repeat, genuine leave)
    // moved out of the circuit to `crate::subq_feed::FeedSet` in Task 2.2; its equivalence table is
    // pinned by `subq_feed::tests::transition_table_matches_circuit_gate`, and the end-to-end gate
    // by the G2 armor tests in `subquery` (`never_member_delete_is_dropped`,
    // `genuine_member_delete_is_never_dropped`).

    /// Prefix enumeration serves the node drop path: a node's contributor slice, scoped to its
    /// own id. (The per-feed key set is no longer a circuit relation — see `subq_feed`.)
    #[tokio::test(flavor = "multi_thread")]
    async fn prefix_scans_enumerate_slices() {
        let c = MembershipCircuit::start().unwrap();
        // pk ids: a=1, b=2, z=3.
        c.apply(Assertions {
            contributors: vec![
                Tup2(ckey(1, 1), Assert::Insert(Value::Int(5))),
                Tup2(ckey(1, 2), Assert::Insert(Value::Int(6))),
                Tup2(ckey(2, 3), Assert::Insert(Value::Int(5))),
            ],
        })
        .await;
        let mut entries = c.contributor_entries(1);
        entries.sort();
        assert_eq!(entries, vec![(1, Value::Int(5)), (2, Value::Int(6))]);
        assert_eq!(c.contributor_entries(3), vec![]);
        c.shutdown().await;
    }

    /// The on-demand profiler dump returns parseable dbsp JSON with per-operator metadata and
    /// non-zero totals (once the circuit holds state).
    #[tokio::test(flavor = "multi_thread")]
    async fn profile_dump_returns_parseable_per_operator_json() {
        let c = MembershipCircuit::start().unwrap();
        c.apply(Assertions {
            contributors: vec![
                Tup2(ckey(1, 1), Assert::Insert(Value::Int(5))),
                Tup2(ckey(1, 2), Assert::Insert(Value::Int(6))),
            ],
        })
        .await;
        let (used, _stored, json) = c.profile_dump().await;
        assert!(used > 0, "circuit with state must report used bytes");
        let v: serde_json::Value = serde_json::from_str(&json).expect("profiler JSON parses");
        assert!(v.get("worker_profiles").is_some(), "dump carries per-worker/per-node metadata");
        c.shutdown().await;
    }

    /// Spill mode: same semantics, state pages to layer files under the storage dir.
    #[tokio::test(flavor = "multi_thread")]
    async fn spill_mode_preserves_semantics_and_writes_files() {
        let dir = std::env::temp_dir().join(format!("subq-spill-test-{}", std::process::id()));
        let c = MembershipCircuit::start_full(Some(SpillConfig {
            dir: dir.to_string_lossy().into_owned(),
            min_storage_bytes: 1, // spill aggressively so even tiny batches hit disk
            cache_mib: 8,
            auto: false, // the test owns and removes this dir itself
        }))
        .unwrap();
        // Enough contributor entries to force at least one on-disk batch.
        let contributors = (0..2000)
            .map(|i| Tup2(ckey(1, i as u32), Assert::Insert(Value::Int(i % 50))))
            .collect();
        let flips = c.apply(Assertions { contributors }).await;
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

    /// `snapshot_bytes` measures real dbsp columnar bytes: zero when empty, non-zero and split
    /// into the raw contributor upsert-map integral vs the derived members snapshot once
    /// populated. This is the measured basis for `GET /memory`'s `bytes_circuit_integral` /
    /// `bytes_circuit_snapshots` split.
    #[tokio::test(flavor = "multi_thread")]
    async fn snapshot_bytes_measures_and_splits_relations() {
        let c = MembershipCircuit::start_full(None).unwrap();
        assert_eq!(c.snapshot_bytes(), CircuitBytes::default(), "empty circuit owns no batch bytes");

        let mut a = Assertions::default();
        for i in 0..1000i64 {
            // 1000 contributors over 100 distinct values on one node.
            a.contributors.push(Tup2(ckey(1, i as u32), Assert::Insert(Value::Int(i % 100))));
        }
        c.apply(a).await;

        let cb = c.snapshot_bytes();
        // Contributors integral: 1000 live tuples; members: 100 distinct.
        assert_eq!(cb.contributors_len, 1000);
        assert_eq!(cb.members_len, 100, "members is the deduplicated (node,value) relation");
        assert!(cb.contributors_bytes > 0 && cb.members_bytes > 0);
        // The integral (raw contributor map) dwarfs the derived membership snapshot.
        assert_eq!(cb.integral_bytes(), cb.contributors_bytes);
        assert_eq!(cb.snapshot_bytes(), cb.members_bytes);
        assert!(cb.integral_bytes() > cb.snapshot_bytes());
        assert_eq!(cb.total_bytes(), cb.integral_bytes() + cb.snapshot_bytes());
        c.shutdown().await;
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
        let c = MembershipCircuit::start_full(None).unwrap();
        let seed = (0..LIVE)
            .map(|i| Tup2(ckey(7, i as u32), Assert::Insert(Value::Int(0))))
            .collect();
        c.apply(Assertions { contributors: seed }).await;

        let mut max_contrib_len = 0usize;
        for step in 1..=STEPS {
            let contributors = (0..LIVE)
                .map(|i| Tup2(ckey(7, i as u32), Assert::Insert(Value::Int(step))))
                .collect();
            c.apply(Assertions { contributors }).await;
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
                Tup2(ckey(1, 1), Assert::Insert(Value::Int(7))),
                Tup2(ckey(2, 1), Assert::Insert(Value::Int(7))),
                Tup2(ckey(1, 2), Assert::Insert(Value::Null)),
            ],
        })
        .await;
        let flips = c.apply(contrib(1, 1, None)).await;
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
