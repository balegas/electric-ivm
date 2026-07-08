//! Storage-backed table arrangements, powered by dbsp.
//!
//! This module reintroduces dbsp — not as per-shape circuits (removed for cause; see
//! `docs/ARCHITECTURE.md` §"routing model") but as the engine's **table-state layer**: one
//! shared circuit whose arrangements hold replicated tables indexed by primary key and by
//! registered lookup columns, with dbsp's storage layer spilling batches to disk as tables
//! grow. Point lookups that previously required a Postgres query-back (subquery flip
//! re-derivation, `query_all` re-derives) are served from consistent local snapshots instead.
//!
//! Design constraints, and how they are honored:
//!
//! - **A dbsp circuit is fixed at construction.** The circuit is built once, when table
//!   schemas are known, from the index specs registered up front. A lookup against an index
//!   that does not exist returns `None` and the caller falls back to Postgres — correctness
//!   never depends on an index being present.
//! - **Spilling engages at merge and checkpoint boundaries** (the 0.299 memtest lesson:
//!   a static trace seeded as one giant batch never merges, so it never spills). Seeding
//!   therefore feeds tables in bounded chunks — many level-0 batches force real merges —
//!   and periodic checkpoints persist every in-memory batch as layer files.
//! - **Restart**: each checkpoint records the change-log position and the `(lsn, seq)`
//!   de-duplication highwater in `meta.json` next to dbsp's own state. On boot the circuit
//!   resumes from the checkpoint and the engine replays the change log from the recorded
//!   position; the highwater makes replay overlap harmless (deltas are not idempotent).
//! - **The circuit thread owns the `DBSPHandle`.** Steps are blocking; they run on a
//!   dedicated OS thread fed by a bounded channel (backpressure to the sequencer). Readers
//!   never touch the circuit: `inspect` operators publish a read-only spine snapshot per
//!   index after every step, and lookups seek those snapshots directly.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use dbsp::circuit::{CircuitConfig, CircuitStorageConfig, StorageCacheConfig, StorageConfig, StorageOptions};
use dbsp::dynamic::{DowncastTrait, Erase};
use dbsp::trace::{BatchReader as DynBatchReaderTrait, Cursor};
use dbsp::typed_batch::{BatchReader, SpineSnapshot};
use dbsp::{OrdIndexedZSet, Runtime, ZSetHandle};
use tokio::sync::{mpsc, oneshot};

use crate::value::{Row, Tup2, Value, ZWeight};

/// One arrangement: `table` indexed by the values of `cols` (column positions, in order).
/// The primary-key arrangement is just `cols == [pk_idx]`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IndexSpec {
    pub table: String,
    pub cols: Vec<usize>,
}

/// Tuning knobs, resolved from `ELECTRIC_IVM_DBSP_*` env vars (see `config.rs`).
#[derive(Clone, Debug)]
pub struct ArrangementsConfig {
    /// Root directory for dbsp state (layer files, checkpoints, `meta.json`).
    pub dir: PathBuf,
    /// Storage-cache budget in MiB (`None` = dbsp default: 256 MiB per thread).
    pub cache_mib: Option<usize>,
    /// Spill threshold: batches at least this large go to disk when merged
    /// (`None` = 1 MiB; dbsp's own default of 10 MiB keeps small tables entirely in memory).
    pub min_storage_bytes: Option<usize>,
    /// Memory ceiling driving dbsp's pressure-based eager spilling (`None` = no ceiling).
    pub max_rss_bytes: Option<u64>,
    /// Checkpoint cadence (`None` = only on shutdown).
    pub checkpoint_every: Option<Duration>,
    /// Rows per seeding chunk (one circuit transaction each). Bounded chunks create
    /// multiple level-0 batches, which is what makes merges — and therefore spill — happen.
    pub seed_chunk_rows: usize,
}

impl Default for ArrangementsConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("./data/dbsp"),
            cache_mib: None,
            min_storage_bytes: Some(1024 * 1024),
            max_rss_bytes: None,
            checkpoint_every: Some(Duration::from_secs(60)),
            seed_chunk_rows: 50_000,
        }
    }
}

/// The snapshot type published per index: full rows, keyed by the projected index columns.
type Snapshot = SpineSnapshot<OrdIndexedZSet<Row, Row>>;

/// A published read slot. `None` until the first step after circuit construction.
type Slot = Arc<RwLock<Option<Snapshot>>>;

/// One envelope's worth of change, stamped for de-duplication. `lsn`/`seq` are `None` for
/// library-mode envelopes (no replication stamps), which bypass the highwater (they are only
/// produced by tests that never redeliver).
pub struct StampedDelta {
    pub table: String,
    pub delta: Vec<Tup2<Row, ZWeight>>,
    pub lsn: Option<u64>,
    pub seq: Option<u64>,
}

enum Cmd {
    /// Apply one change-log batch (any number of transactions) and step the circuit once.
    /// `next_offset` is the change-log position after this batch; recorded for checkpoints.
    Batch { deltas: Vec<StampedDelta>, next_offset: Option<String> },
    /// Seed one chunk of a table's initial snapshot (bypasses the highwater: seeding is
    /// fenced by the snapshot gate at the feed site, not by replication stamps).
    SeedChunk { table: String, rows: Vec<Row>, done: Option<oneshot::Sender<Result<(), String>>> },
    Checkpoint { resp: Option<oneshot::Sender<Result<(), String>>> },
    Shutdown { resp: oneshot::Sender<()> },
}

/// Checkpoint sidecar: what the circuit state corresponds to in the change log.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
struct Meta {
    checkpoint: Option<uuid::Uuid>,
    /// Change-log offset the checkpointed state is complete up to (replay starts here).
    offset: Option<String>,
    /// De-duplication highwater at checkpoint time.
    lsn: Option<u64>,
    seq: Option<u64>,
    /// Fingerprint of the index layout; a mismatch discards the checkpoint (different circuit).
    layout: String,
}

/// Handle to the arrangement layer. Cheap to clone; readers and the feeder share it.
#[derive(Clone)]
pub struct Arrangements {
    tx: mpsc::Sender<Cmd>,
    slots: Arc<HashMap<IndexSpec, Slot>>,
    /// Tables whose initial seed completed (lookups against unseeded tables return `None`).
    seeded: Arc<HashMap<String, AtomicBool>>,
    /// Change-log offset to resume replay from (from the restored checkpoint), if any.
    restored_offset: Option<String>,
    /// Lookup counters, exposed for tests/observability: (served, fallback).
    served: Arc<AtomicU64>,
    fallback: Arc<AtomicU64>,
}

impl Arrangements {
    /// Build the circuit (restoring from the latest compatible checkpoint if one exists)
    /// and start the circuit thread. `specs` must include every index that lookups will
    /// need; it is deduplicated and ordered for a stable circuit layout.
    pub fn start(cfg: ArrangementsConfig, mut specs: Vec<IndexSpec>) -> Result<Arrangements> {
        specs.sort();
        specs.dedup();
        anyhow::ensure!(!specs.is_empty(), "arrangements: no indexes registered");

        std::fs::create_dir_all(&cfg.dir)
            .with_context(|| format!("creating dbsp dir {}", cfg.dir.display()))?;
        let layout = layout_fingerprint(&specs);
        let meta = read_meta(&cfg.dir);
        // A layout change means a different circuit: dbsp would refuse the checkpoint via its
        // own fingerprint check; discard state proactively so we reseed instead of erroring.
        let meta = match meta {
            Some(m) if m.layout == layout => Some(m),
            Some(_) => {
                tracing::warn!("arrangements: index layout changed; discarding dbsp state in {}", cfg.dir.display());
                std::fs::remove_dir_all(&cfg.dir).ok();
                std::fs::create_dir_all(&cfg.dir)?;
                None
            }
            None => None,
        };

        let mut storage = CircuitStorageConfig::for_config(
            StorageConfig {
                path: cfg.dir.to_string_lossy().into_owned(),
                cache: StorageCacheConfig::default(),
            },
            StorageOptions {
                min_storage_bytes: cfg.min_storage_bytes,
                cache_mib: cfg.cache_mib,
                ..StorageOptions::default()
            },
        )
        .map_err(|e| anyhow::anyhow!("arrangements: storage config: {e}"))?;
        if let Some(m) = &meta {
            storage.init_checkpoint = m.checkpoint;
        }

        let mut circuit_config = CircuitConfig::with_workers(1).with_storage(Some(storage));
        circuit_config.max_rss_bytes = cfg.max_rss_bytes;

        // Read slots, created up front and shared with the constructor closure.
        let slots: Arc<HashMap<IndexSpec, Slot>> =
            Arc::new(specs.iter().map(|s| (s.clone(), Slot::default())).collect());
        let seeded: Arc<HashMap<String, AtomicBool>> = Arc::new(
            specs.iter().map(|s| (s.table.clone(), AtomicBool::new(meta.is_some()))).collect(),
        );

        // Group specs per table: one input handle per table, N arrangements over it.
        let mut per_table: Vec<(String, Vec<IndexSpec>)> = Vec::new();
        for spec in &specs {
            match per_table.iter_mut().find(|(t, _)| t == &spec.table) {
                Some((_, v)) => v.push(spec.clone()),
                None => per_table.push((spec.table.clone(), vec![spec.clone()])),
            }
        }

        let ctor_slots = slots.clone();
        let ctor_tables = per_table.clone();
        let (mut dbsp, inputs) = Runtime::init_circuit(circuit_config, move |circuit| {
            let mut handles: HashMap<String, ZSetHandle<Row>> = HashMap::new();
            for (table, table_specs) in &ctor_tables {
                let (stream, handle) = circuit.add_input_zset::<Row>();
                for spec in table_specs {
                    let cols = spec.cols.clone();
                    let slot = ctor_slots.get(spec).expect("slot for spec").clone();
                    // `apply`, not `inspect`: `inspect` re-emits the `Spine` downstream, which
                    // clones it (unimplemented for spines). `apply` produces the snapshot only.
                    stream
                        .map_index(move |row| (project(row, &cols), row.clone()))
                        .integrate_trace()
                        .apply(move |spine| {
                            *slot.write().expect("slot lock") = Some(spine.ro_snapshot());
                        });
                }
                handles.insert(table.clone(), handle);
            }
            Ok(handles)
        })
        .map_err(|e| anyhow::anyhow!("arrangements: init_circuit: {e}"))?;

        let restored_offset = meta.as_ref().and_then(|m| m.offset.clone());
        let restored_hw = meta.as_ref().and_then(|m| m.lsn.map(|l| (l, m.seq.unwrap_or(0))));

        // Publish initial snapshots before returning: the `apply` operators only run inside a
        // step, so a restored circuit would otherwise serve `None` until the first change
        // arrives. An empty transaction evaluates every operator once (harmless when fresh),
        // and running it here (not on the circuit thread) makes `start()` deterministic:
        // restored state is servable the moment this returns.
        dbsp.transaction()
            .map_err(|e| anyhow::anyhow!("arrangements: initial transaction: {e}"))?;

        let (tx, rx) = mpsc::channel::<Cmd>(256);
        let thread_cfg = cfg.clone();
        std::thread::Builder::new()
            .name("dbsp-arrangements".into())
            .spawn(move || circuit_thread(dbsp, inputs, rx, thread_cfg, layout, restored_hw))
            .context("spawning dbsp-arrangements thread")?;

        Ok(Arrangements {
            tx,
            slots,
            seeded,
            restored_offset,
            served: Arc::new(AtomicU64::new(0)),
            fallback: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Change-log offset to replay from after a checkpoint restore (`None` = fresh state,
    /// seed from Postgres instead).
    pub fn restored_offset(&self) -> Option<&str> {
        self.restored_offset.as_deref()
    }

    /// Feed one change-log batch. Await-safe (bounded channel = backpressure).
    pub async fn apply_batch(&self, deltas: Vec<StampedDelta>, next_offset: Option<String>) {
        if deltas.is_empty() && next_offset.is_none() {
            return;
        }
        if self.tx.send(Cmd::Batch { deltas, next_offset }).await.is_err() {
            tracing::error!("arrangements: circuit thread gone; dropping batch");
        }
    }

    /// Feed one seeding chunk. The last chunk of a table should be sent with `finish_seed`.
    pub async fn seed_chunk(&self, table: &str, rows: Vec<Row>) -> Result<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.tx
            .send(Cmd::SeedChunk { table: table.to_string(), rows, done: Some(done_tx) })
            .await
            .map_err(|_| anyhow::anyhow!("arrangements: circuit thread gone"))?;
        done_rx.await.context("arrangements: seed ack")?.map_err(|e| anyhow::anyhow!(e))
    }

    /// Mark a table's initial seed complete; lookups start serving.
    pub fn finish_seed(&self, table: &str) {
        if let Some(flag) = self.seeded.get(table) {
            flag.store(true, Ordering::Release);
        }
    }

    /// Point lookup: full rows of `table` whose projected `cols` equal `key`.
    /// `None` = this layer cannot answer (no such index, or table not seeded yet);
    /// the caller must fall back to Postgres. `Some(vec![])` is an authoritative empty result.
    pub fn lookup(&self, table: &str, cols: &[usize], key: &Row) -> Option<Vec<Row>> {
        if !self.seeded.get(table)?.load(Ordering::Acquire) {
            self.fallback.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let spec = IndexSpec { table: table.to_string(), cols: cols.to_vec() };
        let slot = match self.slots.get(&spec) {
            Some(s) => s,
            None => {
                self.fallback.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        let guard = slot.read().expect("slot lock");
        let snap = guard.as_ref()?;
        // The typed layer exposes no seekable cursor; use the dynamic cursor with the
        // downcast idiom dbsp's own operators use. Safety: the snapshot's key/val erase `Row`.
        let mut out = Vec::new();
        let mut cursor = snap.inner().cursor();
        cursor.seek_key(key.erase());
        if cursor.key_valid() && unsafe { cursor.key().downcast::<Row>() } == key {
            while cursor.val_valid() {
                if **cursor.weight() > 0 {
                    out.push(unsafe { cursor.val().downcast::<Row>() }.clone());
                }
                cursor.step_val();
            }
        }
        self.served.fetch_add(1, Ordering::Relaxed);
        Some(out)
    }

    /// Full scan of a table via its first registered index. Same `None` contract as `lookup`.
    pub fn scan(&self, table: &str) -> Option<Vec<Row>> {
        if !self.seeded.get(table)?.load(Ordering::Acquire) {
            self.fallback.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let slot = self
            .slots
            .iter()
            .filter(|(spec, _)| spec.table == table)
            .min_by_key(|(spec, _)| spec.cols.clone())
            .map(|(_, slot)| slot)?;
        let guard = slot.read().expect("slot lock");
        let snap = guard.as_ref()?;
        let mut out = Vec::new();
        let mut cursor = snap.inner().cursor();
        while cursor.key_valid() {
            while cursor.val_valid() {
                if **cursor.weight() > 0 {
                    out.push(unsafe { cursor.val().downcast::<Row>() }.clone());
                }
                cursor.step_val();
            }
            cursor.step_key();
        }
        self.served.fetch_add(1, Ordering::Relaxed);
        Some(out)
    }

    /// (served, fallback) lookup counters.
    pub fn counters(&self) -> (u64, u64) {
        (self.served.load(Ordering::Relaxed), self.fallback.load(Ordering::Relaxed))
    }

    /// Checkpoint now (also runs on the periodic cadence and at shutdown).
    pub async fn checkpoint(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Cmd::Checkpoint { resp: Some(tx) })
            .await
            .map_err(|_| anyhow::anyhow!("arrangements: circuit thread gone"))?;
        rx.await.context("arrangements: checkpoint ack")?.map_err(|e| anyhow::anyhow!(e))
    }

    /// Checkpoint and stop the circuit thread.
    pub async fn shutdown(&self) {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Cmd::Shutdown { resp: tx }).await.is_ok() {
            let _ = rx.await;
        }
    }
}

/// Project `cols` of `row` into an index key. Out-of-range positions become `Null`
/// (schema drift is handled upstream; a lookup with the same projection still matches).
fn project(row: &Row, cols: &[usize]) -> Row {
    Row(cols.iter().map(|&i| row.0.get(i).cloned().unwrap_or(Value::Null)).collect())
}

fn layout_fingerprint(specs: &[IndexSpec]) -> String {
    let mut s = String::new();
    for spec in specs {
        s.push_str(&spec.table);
        s.push(':');
        for c in &spec.cols {
            s.push_str(&c.to_string());
            s.push(',');
        }
        s.push(';');
    }
    s
}

fn meta_path(dir: &std::path::Path) -> PathBuf {
    dir.join("meta.json")
}

fn read_meta(dir: &std::path::Path) -> Option<Meta> {
    let bytes = std::fs::read(meta_path(dir)).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn write_meta(dir: &std::path::Path, meta: &Meta) {
    let tmp = dir.join("meta.json.tmp");
    if std::fs::write(&tmp, serde_json::to_vec(meta).expect("meta json")).is_ok() {
        let _ = std::fs::rename(&tmp, meta_path(dir));
    }
}

/// The circuit thread: owns the `DBSPHandle`, applies batches, steps, checkpoints.
fn circuit_thread(
    mut dbsp: dbsp::DBSPHandle,
    inputs: HashMap<String, ZSetHandle<Row>>,
    mut rx: mpsc::Receiver<Cmd>,
    cfg: ArrangementsConfig,
    layout: String,
    restored_hw: Option<(u64, u64)>,
) {
    let mut offset: Option<String> = None;
    let mut highwater: Option<(u64, u64)> = restored_hw;
    let mut last_ckpt = Instant::now();

    let checkpoint = |dbsp: &mut dbsp::DBSPHandle,
                      offset: &Option<String>,
                      highwater: &Option<(u64, u64)>|
     -> Result<(), String> {
        let meta_ckpt = dbsp
            .checkpoint()
            .with_name("arrangements")
            .run()
            .map_err(|e| format!("checkpoint: {e}"))?;
        write_meta(
            &cfg.dir,
            &Meta {
                checkpoint: Some(meta_ckpt.uuid),
                offset: offset.clone(),
                lsn: highwater.map(|(l, _)| l),
                seq: highwater.map(|(_, s)| s),
                layout: layout.clone(),
            },
        );
        Ok(())
    };

    while let Some(cmd) = rx.blocking_recv() {
        match cmd {
            Cmd::Batch { deltas, next_offset } => {
                let mut touched = false;
                for d in deltas {
                    // De-duplication: replay overlap after a restore, or redelivery upstream.
                    if let (Some(l), Some(s)) = (d.lsn, d.seq) {
                        if let Some(hw) = highwater {
                            if (l, s) <= hw {
                                continue;
                            }
                        }
                        highwater = Some((l, s));
                    }
                    if d.delta.is_empty() {
                        continue;
                    }
                    let Some(handle) = inputs.get(&d.table) else { continue };
                    let mut buf = d.delta;
                    handle.append(&mut buf);
                    touched = true;
                }
                if touched {
                    if let Err(e) = dbsp.transaction() {
                        tracing::error!("arrangements: transaction failed: {e}");
                    }
                }
                if next_offset.is_some() {
                    offset = next_offset;
                }
                if let Some(every) = cfg.checkpoint_every {
                    if touched && last_ckpt.elapsed() >= every {
                        if let Err(e) = checkpoint(&mut dbsp, &offset, &highwater) {
                            tracing::error!("arrangements: periodic {e}");
                        }
                        last_ckpt = Instant::now();
                    }
                }
            }
            Cmd::SeedChunk { table, rows, done } => {
                let res = match inputs.get(&table) {
                    Some(handle) => {
                        let mut buf: Vec<Tup2<Row, ZWeight>> =
                            rows.into_iter().map(|r| Tup2(r, 1)).collect();
                        handle.append(&mut buf);
                        dbsp.transaction().map_err(|e| format!("seed transaction: {e}"))
                    }
                    None => Err(format!("seed: unknown table '{table}'")),
                };
                if let Some(done) = done {
                    let _ = done.send(res);
                }
            }
            Cmd::Checkpoint { resp } => {
                let res = checkpoint(&mut dbsp, &offset, &highwater);
                last_ckpt = Instant::now();
                if let Some(resp) = resp {
                    let _ = resp.send(res);
                }
            }
            Cmd::Shutdown { resp } => {
                if let Err(e) = checkpoint(&mut dbsp, &offset, &highwater) {
                    tracing::error!("arrangements: shutdown {e}");
                }
                let _ = dbsp.kill();
                let _ = resp.send(());
                return;
            }
        }
    }
    // Feeder dropped without Shutdown (engine teardown): checkpoint best-effort and stop.
    if let Err(e) = checkpoint(&mut dbsp, &offset, &highwater) {
        tracing::error!("arrangements: final {e}");
    }
    let _ = dbsp.kill();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(vals: &[i64]) -> Row {
        Row(vals.iter().map(|&v| Value::Int(v)).collect())
    }

    fn test_cfg(dir: &std::path::Path) -> ArrangementsConfig {
        ArrangementsConfig {
            dir: dir.to_path_buf(),
            // Spill everything storage-eligible: exercises the layer-file path in tests.
            min_storage_bytes: Some(0),
            checkpoint_every: None,
            seed_chunk_rows: 100,
            ..ArrangementsConfig::default()
        }
    }

    fn specs() -> Vec<IndexSpec> {
        vec![
            IndexSpec { table: "t".into(), cols: vec![0] },
            IndexSpec { table: "t".into(), cols: vec![1] },
        ]
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn seed_lookup_delta_lookup() {
        let dir = tempdir();
        let arr = Arrangements::start(test_cfg(&dir), specs()).unwrap();
        assert_eq!(arr.restored_offset(), None);

        // Unseeded: lookups refuse (fallback contract).
        assert_eq!(arr.lookup("t", &[0], &row(&[1])), None);

        arr.seed_chunk("t", vec![row(&[1, 10]), row(&[2, 10]), row(&[3, 30])]).await.unwrap();
        arr.finish_seed("t");

        // pk lookup
        assert_eq!(arr.lookup("t", &[0], &row(&[2])), Some(vec![row(&[2, 10])]));
        // secondary-index lookup: two rows share col1 == 10
        let hits = arr.lookup("t", &[1], &row(&[10])).unwrap();
        assert_eq!(hits.len(), 2);
        // authoritative empty
        assert_eq!(arr.lookup("t", &[0], &row(&[99])), Some(vec![]));
        // unknown index -> fallback
        assert_eq!(arr.lookup("t", &[0, 1], &row(&[1, 10])), None);

        // Live delta: update row 2 (retract old, insert new), delete row 3.
        arr.apply_batch(
            vec![StampedDelta {
                table: "t".into(),
                delta: vec![Tup2(row(&[2, 10]), -1), Tup2(row(&[2, 20]), 1), Tup2(row(&[3, 30]), -1)],
                lsn: Some(100),
                seq: Some(0),
            }],
            Some("off-1".into()),
        )
        .await;
        // A redelivery of the same stamp must be ignored (deltas are not idempotent).
        arr.apply_batch(
            vec![StampedDelta {
                table: "t".into(),
                delta: vec![Tup2(row(&[2, 10]), -1), Tup2(row(&[2, 20]), 1), Tup2(row(&[3, 30]), -1)],
                lsn: Some(100),
                seq: Some(0),
            }],
            None,
        )
        .await;
        // Drain: send an empty batch and checkpoint to synchronize.
        arr.checkpoint().await.unwrap();

        assert_eq!(arr.lookup("t", &[0], &row(&[2])), Some(vec![row(&[2, 20])]));
        assert_eq!(arr.lookup("t", &[0], &row(&[3])), Some(vec![]));
        assert_eq!(arr.lookup("t", &[1], &row(&[10])), Some(vec![row(&[1, 10])]));
        assert_eq!(arr.scan("t").map(|v| v.len()), Some(2));

        arr.shutdown().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn checkpoint_restore_resumes_offset_and_dedup() {
        let dir = tempdir();
        {
            let arr = Arrangements::start(test_cfg(&dir), specs()).unwrap();
            arr.seed_chunk("t", vec![row(&[1, 10])]).await.unwrap();
            arr.finish_seed("t");
            arr.apply_batch(
                vec![StampedDelta {
                    table: "t".into(),
                    delta: vec![Tup2(row(&[2, 20]), 1)],
                    lsn: Some(7),
                    seq: Some(1),
                }],
                Some("off-42".into()),
            )
            .await;
            arr.shutdown().await; // checkpoints
        }
        {
            let arr = Arrangements::start(test_cfg(&dir), specs()).unwrap();
            assert_eq!(arr.restored_offset(), Some("off-42"));
            // Restored tables serve immediately (state came from the checkpoint).
            assert_eq!(arr.lookup("t", &[0], &row(&[2])), Some(vec![row(&[2, 20])]));
            // Replay overlap: the same stamped delta must be skipped by the restored highwater.
            arr.apply_batch(
                vec![StampedDelta {
                    table: "t".into(),
                    delta: vec![Tup2(row(&[2, 20]), 1)],
                    lsn: Some(7),
                    seq: Some(1),
                }],
                None,
            )
            .await;
            arr.checkpoint().await.unwrap();
            assert_eq!(arr.lookup("t", &[0], &row(&[2])), Some(vec![row(&[2, 20])]));
            arr.shutdown().await;
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn layout_change_discards_state() {
        let dir = tempdir();
        {
            let arr = Arrangements::start(test_cfg(&dir), specs()).unwrap();
            arr.seed_chunk("t", vec![row(&[1, 10])]).await.unwrap();
            arr.finish_seed("t");
            arr.shutdown().await;
        }
        {
            // Different index layout: state must be discarded, not restored.
            let arr = Arrangements::start(
                test_cfg(&dir),
                vec![IndexSpec { table: "t".into(), cols: vec![0] }],
            )
            .unwrap();
            assert_eq!(arr.restored_offset(), None);
            assert_eq!(arr.lookup("t", &[0], &row(&[1])), None); // unseeded again
            arr.shutdown().await;
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    fn tempdir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("arr-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Total bytes under `dir`, recursively.
    fn dir_bytes(dir: &std::path::Path) -> u64 {
        let mut total = 0;
        if let Ok(entries) = std::fs::read_dir(dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    total += dir_bytes(&p);
                } else {
                    total += e.metadata().map(|m| m.len()).unwrap_or(0);
                }
            }
        }
        total
    }

    /// The 0.299 lesson, inverted: with chunked seeding + a zero spill threshold + a
    /// checkpoint, the table's data must actually land in layer files on disk — not stay
    /// resident. (The old memtest observed ~2 MB spilled of a ~570 MB resident trace.)
    #[tokio::test(flavor = "multi_thread")]
    async fn spill_produces_layer_files() {
        let dir = tempdir();
        let arr = Arrangements::start(
            ArrangementsConfig {
                dir: dir.clone(),
                min_storage_bytes: Some(0),
                checkpoint_every: None,
                seed_chunk_rows: 10_000,
                ..ArrangementsConfig::default()
            },
            vec![IndexSpec { table: "t".into(), cols: vec![0] }],
        )
        .unwrap();

        // ~100k rows x ~80B of payload ≈ 8 MB of logical data, seeded in 10k-row chunks so the
        // spine takes many level-0 batches and merges (the spill point) actually run.
        let payload = "x".repeat(64);
        let mut expect_bytes = 0u64;
        for chunk_start in (0..100_000).step_by(10_000) {
            let rows: Vec<Row> = (chunk_start..chunk_start + 10_000)
                .map(|i| Row(vec![Value::Int(i as i64), Value::Text(payload.clone())]))
                .collect();
            expect_bytes += rows.len() as u64 * 72;
            arr.seed_chunk("t", rows).await.unwrap();
        }
        arr.finish_seed("t");
        arr.checkpoint().await.unwrap(); // persists any still-in-memory batches

        let on_disk = dir_bytes(&dir);
        assert!(
            on_disk > expect_bytes / 2,
            "expected the seeded table (~{expect_bytes}B logical) on disk, found only {on_disk}B — spill did not engage"
        );

        // And the data is still fully readable through the file-backed snapshot.
        assert_eq!(
            arr.lookup("t", &[0], &Row(vec![Value::Int(54_321)])).map(|v| v.len()),
            Some(1)
        );
        assert_eq!(arr.scan("t").map(|v| v.len()), Some(100_000));

        arr.shutdown().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Large-scale spill check (~1M rows, ~300+ MB logical): run manually with
    /// `cargo test memtest_spill -- --ignored --nocapture`. Prints on-disk bytes and process
    /// RSS so the memory-bounding claim can be eyeballed against table size.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn memtest_spill_large() {
        let dir = tempdir();
        let arr = Arrangements::start(
            ArrangementsConfig {
                dir: dir.clone(),
                min_storage_bytes: Some(0),
                cache_mib: Some(128),
                checkpoint_every: None,
                seed_chunk_rows: 50_000,
                ..ArrangementsConfig::default()
            },
            vec![IndexSpec { table: "t".into(), cols: vec![0] }],
        )
        .unwrap();

        let payload = "x".repeat(256);
        for chunk_start in (0..1_000_000u64).step_by(50_000) {
            let rows: Vec<Row> = (chunk_start..chunk_start + 50_000)
                .map(|i| Row(vec![Value::Int(i as i64), Value::Text(payload.clone())]))
                .collect();
            arr.seed_chunk("t", rows).await.unwrap();
        }
        arr.finish_seed("t");
        arr.checkpoint().await.unwrap();

        let on_disk = dir_bytes(&dir);
        let rss = memory_stats::memory_stats().map(|m| m.physical_mem).unwrap_or(0);
        println!("memtest: 1M rows (~300MB logical): on_disk={} MiB rss={} MiB", on_disk / (1 << 20), rss / (1 << 20));
        assert!(on_disk > 100 * (1 << 20), "expected >100 MiB on disk, got {on_disk}");
        assert_eq!(
            arr.lookup("t", &[0], &Row(vec![Value::Int(999_999)])).map(|v| v.len()),
            Some(1)
        );
        arr.shutdown().await;
        std::fs::remove_dir_all(&dir).ok();
    }
}
