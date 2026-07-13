//! Counts pipelines, powered by dbsp — the circuit tier.
//!
//! One shared circuit per engine (never per-shape circuits: structure must not scale with
//! subscriptions — see `docs/ARCHITECTURE.md` §6b). The circuit maintains a live COUNT per
//! group projection for each configured table (`ELECTRIC_IVM_DBSP_COUNTS`); circuit-served
//! COUNT aggregates are seeded by summing groups and updated from each step's group deltas.
//!
//! **Row data lives in Postgres, not here.** The circuit's state is O(distinct groups) — in
//! memory, no storage layer, no spill, no checkpoints. Row lookups (subquery flip
//! re-derivations, membership move-ins, shape seeding) go to pooled Postgres queries
//! (`engine::membership`); correctness comes from snapshot-gate fencing and absolute per-pk
//! emission, exactly as for ordinary shape backfills. On boot the counts reseed from one
//! `SELECT <group_cols>, count(*) … GROUP BY` per table under a `REPEATABLE READ` snapshot —
//! O(groups), not O(rows) — and the seed's `SnapshotGate` fences change-log replay.
//!
//! Design constraints, and how they are honored:
//!
//! - **A dbsp circuit is fixed at construction.** The circuit is built once, when table
//!   schemas are known, from the count specs registered up front. Tables without a counts
//!   pipeline never enter the circuit.
//! - **The circuit thread owns the `DBSPHandle`.** Steps are blocking; they run on a
//!   dedicated OS thread fed by a bounded channel (backpressure to the sequencer). Readers
//!   never touch the circuit: `apply` operators publish a read-only spine snapshot per
//!   counts pipeline after every step, and reads seek those snapshots directly.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use anyhow::Result;
use dbsp::circuit::CircuitConfig;
use dbsp::dynamic::DowncastTrait;
use dbsp::trace::{BatchReader as DynBatchReaderTrait, Cursor};
use dbsp::typed_batch::{BatchReader, SpineSnapshot};
use dbsp::{OrdIndexedZSet, OutputHandle, Runtime, ZSetHandle};
use tokio::sync::{mpsc, oneshot};

use crate::value::{Row, Tup2, Value, ZWeight};

/// One counts pipeline: the live COUNT of `table`'s rows per distinct projection of
/// `group_cols` (`map_index(group) → weighted_count`). At most one per table. Aggregate
/// shapes whose predicate decomposes over these columns are served by summing groups.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct CountSpec {
    pub table: String,
    pub group_cols: Vec<usize>,
}

/// The net change of one count group in one circuit step: the group's count moved by `delta`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CountDelta {
    pub table: String,
    pub group: Row,
    pub delta: i64,
}

/// The snapshot type published per counts pipeline: current count, keyed by the group.
type CountSnapshot = SpineSnapshot<OrdIndexedZSet<Row, ZWeight>>;
type CountSlot = Arc<RwLock<Option<CountSnapshot>>>;

/// The transaction-accumulated output handle of a counts pipeline.
type CountOutput = OutputHandle<SpineSnapshot<OrdIndexedZSet<Row, ZWeight>>>;

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
    /// `resp` acknowledges completion with the step's count-group deltas — awaiting it is
    /// what gives the feeder read-your-writes over the snapshots.
    Batch { deltas: Vec<StampedDelta>, resp: Option<oneshot::Sender<Vec<CountDelta>>> },
    /// Seed a table's counts from pre-aggregated `(group, count)` pairs (one synthetic
    /// weighted row per group — O(groups), not O(rows)). Bypasses the highwater: seeding is
    /// fenced by the snapshot gate at the feed site, not by replication stamps.
    SeedGroups {
        table: String,
        groups: Vec<(Row, i64)>,
        done: Option<oneshot::Sender<Result<(), String>>>,
    },
    Shutdown { resp: oneshot::Sender<()> },
}

/// Handle to the counts layer. Cheap to clone; readers and the feeder share it.
#[derive(Clone)]
pub struct Arrangements {
    tx: mpsc::Sender<Cmd>,
    /// Counts pipelines: per table, the group columns and the published count snapshot.
    counts: Arc<HashMap<String, (Vec<usize>, CountSlot)>>,
    /// Tables whose initial seed completed (reads against unseeded tables return `None`).
    seeded: Arc<HashMap<String, AtomicBool>>,
}

impl Arrangements {
    /// Build the circuit and start the circuit thread. `counts` must include every
    /// count-group pipeline (at most one per table); it is deduplicated and ordered for a
    /// stable circuit layout. State is in-memory only — callers reseed on boot.
    pub fn start(mut counts: Vec<CountSpec>) -> Result<Arrangements> {
        counts.sort();
        counts.dedup();
        anyhow::ensure!(!counts.is_empty(), "arrangements: no counts pipelines registered");
        for pair in counts.windows(2) {
            anyhow::ensure!(pair[0].table != pair[1].table, "arrangements: one counts spec per table");
        }

        let count_slots: Arc<HashMap<String, (Vec<usize>, CountSlot)>> = Arc::new(
            counts.iter().map(|c| (c.table.clone(), (c.group_cols.clone(), CountSlot::default()))).collect(),
        );
        let seeded: Arc<HashMap<String, AtomicBool>> =
            Arc::new(counts.iter().map(|c| (c.table.clone(), AtomicBool::new(false))).collect());

        let ctor_counts = count_slots.clone();
        let ctor_specs = counts.clone();
        let (dbsp, (inputs, count_outputs)) =
            Runtime::init_circuit(CircuitConfig::with_workers(1), move |circuit| {
                let mut handles: HashMap<String, ZSetHandle<Row>> = HashMap::new();
                let mut count_handles: HashMap<String, CountOutput> = HashMap::new();
                for spec in &ctor_specs {
                    let (stream, handle) = circuit.add_input_zset::<Row>();
                    let (gcols, cslot) = ctor_counts.get(&spec.table).expect("count slot").clone();
                    let counted = stream
                        .map_index(move |row| (project(row, &gcols), ()))
                        .weighted_count();
                    // `apply`, not `inspect`: `inspect` re-emits the `Spine` downstream, which
                    // clones it (unimplemented for spines). `apply` produces the snapshot only.
                    counted.integrate_trace().apply(move |spine| {
                        *cslot.write().expect("count slot lock") = Some(spine.ro_snapshot());
                    });
                    // `accumulate_output`, not `output`: a transaction can span several
                    // microsteps, and the plain mailbox only holds the last one's delta.
                    count_handles.insert(spec.table.clone(), counted.accumulate_output());
                    handles.insert(spec.table.clone(), handle);
                }
                Ok((handles, count_handles))
            })
            .map_err(|e| anyhow::anyhow!("arrangements: init_circuit: {e}"))?;

        let (tx, rx) = mpsc::channel::<Cmd>(256);
        std::thread::Builder::new()
            .name("dbsp-arrangements".into())
            .spawn(move || circuit_thread(dbsp, inputs, count_outputs, rx))
            .map_err(|e| anyhow::anyhow!("spawning dbsp-arrangements thread: {e}"))?;

        Ok(Arrangements { tx, counts: count_slots, seeded })
    }

    /// Feed one change-log batch and wait for the circuit to step. Returns the step's
    /// count-group deltas (empty when nothing new applied). Awaiting completion is what
    /// gives the caller read-your-writes over the snapshots: after `apply_batch` returns,
    /// count reads reflect this batch.
    pub async fn apply_batch(&self, deltas: Vec<StampedDelta>) -> Vec<CountDelta> {
        if deltas.is_empty() {
            return Vec::new();
        }
        let (resp_tx, resp_rx) = oneshot::channel();
        if self.tx.send(Cmd::Batch { deltas, resp: Some(resp_tx) }).await.is_err() {
            tracing::error!("arrangements: circuit thread gone; dropping batch");
            return Vec::new();
        }
        resp_rx.await.unwrap_or_default()
    }

    /// Seed a table's counts from `(group, count)` pairs (from
    /// `SELECT <group_cols>, count(*) … GROUP BY` under the seeding snapshot).
    pub async fn seed_groups(&self, table: &str, groups: Vec<(Row, i64)>) -> Result<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.tx
            .send(Cmd::SeedGroups { table: table.to_string(), groups, done: Some(done_tx) })
            .await
            .map_err(|_| anyhow::anyhow!("arrangements: circuit thread gone"))?;
        done_rx.await.map_err(|_| anyhow::anyhow!("arrangements: seed ack"))?.map_err(|e| anyhow::anyhow!(e))
    }

    /// Mark a table's initial seed complete; count reads start serving.
    pub fn finish_seed(&self, table: &str) {
        if let Some(flag) = self.seeded.get(table) {
            flag.store(true, Ordering::Release);
        }
    }

    /// Whether `table`'s initial seed has completed. `false` for unknown tables.
    pub fn is_seeded(&self, table: &str) -> bool {
        self.seeded.get(table).is_some_and(|f| f.load(Ordering::Acquire))
    }

    /// The group columns of `table`'s counts pipeline, if one is compiled in.
    pub fn counts_group_cols(&self, table: &str) -> Option<&[usize]> {
        self.counts.get(table).map(|(cols, _)| cols.as_slice())
    }

    /// The registered counts specs, in stable (sorted) order.
    pub fn count_specs(&self) -> Vec<CountSpec> {
        let mut specs: Vec<CountSpec> = self
            .counts
            .iter()
            .map(|(t, (cols, _))| CountSpec { table: t.clone(), group_cols: cols.clone() })
            .collect();
        specs.sort();
        specs
    }

    /// Every current count group of `table` with its count. `None` = no counts pipeline
    /// or table not seeded (caller falls back); `Some(vec![])` = authoritative empty.
    /// A group's current count is the value with positive net weight in the trace.
    pub fn count_groups(&self, table: &str) -> Option<Vec<(Row, i64)>> {
        if !self.seeded.get(table)?.load(Ordering::Acquire) {
            return None;
        }
        let (_, slot) = self.counts.get(table)?;
        let guard = slot.read().expect("count slot lock");
        let snap = guard.as_ref()?;
        let mut out = Vec::new();
        let mut cursor = snap.inner().cursor();
        while cursor.key_valid() {
            let group = unsafe { cursor.key().downcast::<Row>() }.clone();
            while cursor.val_valid() {
                if **cursor.weight() > 0 {
                    let count = *unsafe { cursor.val().downcast::<ZWeight>() };
                    if count != 0 {
                        out.push((group.clone(), count));
                    }
                }
                cursor.step_val();
            }
            cursor.step_key();
        }
        Some(out)
    }

    /// Stop the circuit thread. State is in-memory only; there is nothing to persist.
    pub async fn shutdown(&self) {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Cmd::Shutdown { resp: tx }).await.is_ok() {
            let _ = rx.await;
        }
    }
}

/// Project `cols` of `row` into a group key. Out-of-range positions become `Null`
/// (schema drift is handled upstream; a read with the same projection still matches).
fn project(row: &Row, cols: &[usize]) -> Row {
    Row(cols.iter().map(|&i| row.0.get(i).cloned().unwrap_or(Value::Null)).collect())
}

/// Drain a counts output handle: the transaction's per-group net deltas. The output stream
/// is the delta of the (group → count) relation — a changed group appears as retract(old
/// count) + insert(new count), so `Σ value×weight` per group is exactly the count's change.
fn drain_count_deltas(table: &str, handle: &CountOutput, out: &mut Vec<CountDelta>) {
    let batch = handle.concat();
    let mut cursor = batch.inner().cursor();
    while cursor.key_valid() {
        let group = unsafe { cursor.key().downcast::<Row>() }.clone();
        let mut delta: i64 = 0;
        while cursor.val_valid() {
            let count = *unsafe { cursor.val().downcast::<ZWeight>() };
            delta += count * **cursor.weight();
            cursor.step_val();
        }
        if delta != 0 {
            out.push(CountDelta { table: table.to_string(), group, delta });
        }
        cursor.step_key();
    }
}

/// The circuit thread: owns the `DBSPHandle`, applies batches, steps. De-duplicates by the
/// `(lsn, seq)` highwater (Z-set deltas are not idempotent under redelivery).
fn circuit_thread(
    mut dbsp: dbsp::DBSPHandle,
    inputs: HashMap<String, ZSetHandle<Row>>,
    count_outputs: HashMap<String, CountOutput>,
    mut rx: mpsc::Receiver<Cmd>,
) {
    let mut highwater: Option<(u64, u64)> = None;

    while let Some(cmd) = rx.blocking_recv() {
        match cmd {
            Cmd::Batch { deltas, resp } => {
                let mut touched = false;
                for d in deltas {
                    // De-duplication: redelivery upstream (the ingestor is at-least-once).
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
                let mut count_deltas = Vec::new();
                if touched {
                    match dbsp.transaction() {
                        Ok(()) => {
                            for (table, handle) in &count_outputs {
                                drain_count_deltas(table, handle, &mut count_deltas);
                            }
                        }
                        Err(e) => tracing::error!("arrangements: transaction failed: {e}"),
                    }
                }
                if let Some(resp) = resp {
                    let _ = resp.send(count_deltas);
                }
            }
            Cmd::SeedGroups { table, groups, done } => {
                let res = match inputs.get(&table) {
                    Some(handle) => {
                        // One synthetic row per group, weighted by the group's count: the
                        // pipeline's `map_index(group) → weighted_count` state ends up
                        // identical to having fed every row individually.
                        let gcols_len = groups.first().map(|(g, _)| g.0.len()).unwrap_or(0);
                        let _ = gcols_len;
                        let mut buf: Vec<Tup2<Row, ZWeight>> =
                            groups.into_iter().map(|(g, n)| Tup2(g, n)).collect();
                        handle.append(&mut buf);
                        dbsp.transaction().map_err(|e| format!("seed transaction: {e}"))
                    }
                    None => Err(format!("seed: unknown table '{table}'")),
                };
                if let Some(done) = done {
                    let _ = done.send(res);
                }
            }
            Cmd::Shutdown { resp } => {
                let _ = dbsp.kill();
                let _ = resp.send(());
                return;
            }
        }
    }
    // Feeder dropped without Shutdown (engine teardown): stop.
    let _ = dbsp.kill();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(vals: &[i64]) -> Row {
        Row(vals.iter().map(|&v| Value::Int(v)).collect())
    }

    fn counts() -> Vec<CountSpec> {
        vec![CountSpec { table: "t".into(), group_cols: vec![1] }]
    }

    /// Group-aggregated seeding is equivalent to row-by-row feeding: seeding `(group, n)`
    /// pairs produces the same counts state as inserting n rows per group, and live deltas
    /// fold on top of it correctly.
    #[tokio::test(flavor = "multi_thread")]
    async fn seed_groups_equivalent_to_rows_then_deltas() {
        let arr = Arrangements::start(counts()).unwrap();
        assert_eq!(arr.counts_group_cols("t"), Some(&[1][..]));
        // Unseeded: reads refuse (fallback contract).
        assert_eq!(arr.count_groups("t"), None);

        // Seed from pre-aggregated groups: group 10 → 2 rows, group 30 → 1 row. The synthetic
        // group rows only need the group columns populated (position 1 here).
        arr.seed_groups(
            "t",
            vec![(Row(vec![Value::Null, Value::Int(10)]), 2), (Row(vec![Value::Null, Value::Int(30)]), 1)],
        )
        .await
        .unwrap();
        arr.finish_seed("t");
        let mut groups = arr.count_groups("t").unwrap();
        groups.sort();
        assert_eq!(groups, vec![(row(&[10]), 2), (row(&[30]), 1)]);

        // A live delta: one row moves group 10 → 20, one row deleted from 30. The step's
        // count deltas are returned by apply_batch, netted per group.
        let mut deltas = arr
            .apply_batch(vec![StampedDelta {
                table: "t".into(),
                delta: vec![
                    Tup2(row(&[2, 10]), -1),
                    Tup2(row(&[2, 20]), 1),
                    Tup2(row(&[3, 30]), -1),
                ],
                lsn: Some(50),
                seq: Some(0),
            }])
            .await;
        deltas.sort_by(|a, b| a.group.cmp(&b.group));
        assert_eq!(
            deltas,
            vec![
                CountDelta { table: "t".into(), group: row(&[10]), delta: -1 },
                CountDelta { table: "t".into(), group: row(&[20]), delta: 1 },
                CountDelta { table: "t".into(), group: row(&[30]), delta: -1 },
            ]
        );
        // A redelivered stamp is a no-op (no count deltas; Z-set deltas are not idempotent).
        let dup = arr
            .apply_batch(vec![StampedDelta {
                table: "t".into(),
                delta: vec![Tup2(row(&[2, 10]), -1)],
                lsn: Some(50),
                seq: Some(0),
            }])
            .await;
        assert_eq!(dup, vec![]);
        let mut groups = arr.count_groups("t").unwrap();
        groups.sort();
        assert_eq!(groups, vec![(row(&[10]), 1), (row(&[20]), 1)]);

        arr.shutdown().await;
    }

    /// The circuit only admits configured tables; unknown tables are refused loudly at seed
    /// time and ignored in batches.
    #[tokio::test(flavor = "multi_thread")]
    async fn unknown_tables_are_refused() {
        let arr = Arrangements::start(counts()).unwrap();
        assert!(arr.seed_groups("nope", vec![]).await.is_err());
        assert_eq!(arr.count_groups("nope"), None);
        // A batch for an unknown table is skipped without disturbing known state.
        arr.seed_groups("t", vec![(Row(vec![Value::Null, Value::Int(1)]), 1)]).await.unwrap();
        arr.finish_seed("t");
        arr.apply_batch(vec![StampedDelta {
            table: "nope".into(),
            delta: vec![Tup2(row(&[1, 1]), 1)],
            lsn: Some(1),
            seq: Some(0),
        }])
        .await;
        assert_eq!(arr.count_groups("t"), Some(vec![(row(&[1]), 1)]));
        arr.shutdown().await;
    }
}
