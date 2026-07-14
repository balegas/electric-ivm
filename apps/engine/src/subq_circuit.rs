//! The membership circuit: subquery inner-set state, powered by dbsp — the circuit tier's
//! second pipeline family (alongside `arrangements`' counts pipelines).
//!
//! One always-on circuit per engine maintains EVERY subquery node's value-membership set as
//! one relation, keyed `(node_id, projected value)` and weighted by contributor count. The
//! registry (`subquery.rs`) evaluates templates host-side and feeds exact weighted
//! **contributor tuples** `Row([Int(node_id), value, Text(pk)])` (retract old / insert new,
//! reconcile-by-identity via the host `pk → value` reverse index); the circuit does the
//! stateful part:
//!
//! ```text
//! input (contributor tuples, ±1)
//!   → map(drop pk)                          // (node_id, value) weighted by #contributors
//!   ├─ integrate_trace → published snapshot // contains()/has_null()/introspection reads
//!   └─ distinct → accumulate_output         // per-step deltas = membership FLIPS
//! ```
//!
//! The distinct's output delta is exactly a flip: `+1` when a `(node, value)` key's
//! contributor count crosses 0 → positive (the value entered the set), `-1` on the way back
//! (it left). Because the host feeds *exact* deltas (a pk's contribution is retracted before
//! its new value is inserted, duplicates are impossible), there is no highwater here — the
//! sequencer's `(lsn, seq)` de-dup plus reconcile-by-identity already guarantee
//! exactly-once effect.
//!
//! Structure is fixed at construction (a dbsp circuit is fixed at construction): one global
//! tuple input serves every node, so registering a new subquery template/node/bind is pure
//! runtime *data* — no rebuild, ever. Node identity is a registry-assigned `node_id`;
//! template/bind structure is a host-level concern the circuit never sees.
//!
//! Threading mirrors `arrangements`: a dedicated OS thread owns the `DBSPHandle`, fed by a
//! bounded channel; `apply` awaits the step's completion, which gives callers
//! read-your-writes over the published snapshot.

use std::sync::{Arc, RwLock};

use anyhow::Result;
use dbsp::circuit::CircuitConfig;
use dbsp::dynamic::DowncastTrait;
use dbsp::trace::{BatchReader as DynBatchReaderTrait, Cursor};
use dbsp::typed_batch::{BatchReader, OrdZSet, SpineSnapshot};
use dbsp::{OutputHandle, Runtime, ZSetHandle};
use tokio::sync::{mpsc, oneshot};

use crate::value::{Row, Tup2, Value, ZWeight};

/// The membership relation snapshot: `(node_id, value)` keys, weight = contributor count.
type MemberSnapshot = SpineSnapshot<OrdZSet<Row>>;
type MemberSlot = Arc<RwLock<Option<MemberSnapshot>>>;

/// One membership flip from a circuit step: `(node, value)` entered (`delta > 0`) or left
/// (`delta < 0`) the node's set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberDelta {
    pub node_id: i64,
    pub value: Value,
    pub delta: i64,
}

enum Cmd {
    /// Feed one batch of contributor tuples and step. `resp` returns the step's membership
    /// flips — awaiting it gives the feeder read-your-writes over the snapshot.
    Batch { tuples: Vec<Tup2<Row, ZWeight>>, resp: oneshot::Sender<Vec<MemberDelta>> },
    Shutdown { resp: oneshot::Sender<()> },
}

/// Handle to the membership circuit. Cheap to clone; the registry and readers share it.
#[derive(Clone)]
pub struct MembershipCircuit {
    tx: mpsc::Sender<Cmd>,
    snapshot: MemberSlot,
}

impl MembershipCircuit {
    /// Build the circuit and start its thread. State is in-memory only — nodes reseed from
    /// Postgres on registration, exactly like every other engine structure.
    pub fn start() -> Result<MembershipCircuit> {
        let slot: MemberSlot = MemberSlot::default();
        let ctor_slot = slot.clone();
        let (dbsp, (input, flips)) =
            Runtime::init_circuit(CircuitConfig::with_workers(1), move |circuit| {
                let (stream, handle) = circuit.add_input_zset::<Row>();
                // Drop the pk (last position): (node_id, value) weighted by contributor count.
                let members = stream.map(|t| Row(t.0[..t.0.len().saturating_sub(1)].to_vec()));
                // `apply`, not `inspect` (see arrangements.rs): publish a read-only snapshot.
                members.integrate_trace().apply(move |spine| {
                    *ctor_slot.write().expect("member slot lock") = Some(spine.ro_snapshot());
                });
                // The incremental distinct's output delta IS the flip stream.
                // `accumulate_output`: a transaction can span microsteps.
                let flips = members.distinct().accumulate_output();
                Ok((handle, flips))
            })
            .map_err(|e| anyhow::anyhow!("membership circuit: init_circuit: {e}"))?;

        let (tx, rx) = mpsc::channel::<Cmd>(256);
        std::thread::Builder::new()
            .name("dbsp-subq".into())
            .spawn(move || circuit_thread(dbsp, input, flips, rx))
            .map_err(|e| anyhow::anyhow!("spawning dbsp-subq thread: {e}"))?;

        Ok(MembershipCircuit { tx, snapshot: slot })
    }

    /// Feed contributor tuples (`Row([Int(node_id), value, Text(pk)])` weighted ±1), step,
    /// and return the step's membership flips. After this returns, snapshot reads reflect
    /// the batch (read-your-writes).
    pub async fn apply(&self, tuples: Vec<Tup2<Row, ZWeight>>) -> Vec<MemberDelta> {
        if tuples.is_empty() {
            return Vec::new();
        }
        let (resp_tx, resp_rx) = oneshot::channel();
        if self.tx.send(Cmd::Batch { tuples, resp: resp_tx }).await.is_err() {
            tracing::error!("membership circuit: thread gone; dropping tuples");
            return Vec::new();
        }
        resp_rx.await.unwrap_or_default()
    }

    /// Is `value` currently a member of node `node_id`'s set? (Contributor count > 0 in the
    /// published snapshot.) `false` before any step has published.
    pub fn contains(&self, node_id: i64, value: &Value) -> bool {
        let target = Row(vec![Value::Int(node_id), value.clone()]);
        self.read_key_weight(&target) > 0
    }

    /// The node's current distinct-value count and up to `cap` `(value, contributor count)`
    /// pairs, most-shared first (introspection: the visualizer's inner-set index view).
    pub fn values_for_node(&self, node_id: i64, cap: usize) -> (usize, Vec<(Value, usize)>) {
        let guard = self.snapshot.read().expect("member slot lock");
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

    /// Net weight of one exact key in the published snapshot (0 when absent/unpublished).
    fn read_key_weight(&self, target: &Row) -> i64 {
        let guard = self.snapshot.read().expect("member slot lock");
        let Some(snap) = guard.as_ref() else { return 0 };
        let mut cursor = snap.inner().cursor();
        seek(&mut cursor, target);
        if !cursor.key_valid() {
            return 0;
        }
        if unsafe { cursor.key().downcast::<Row>() } != target {
            return 0;
        }
        key_weight(&mut cursor)
    }

    /// Stop the circuit thread. State is in-memory only; nothing to persist.
    pub async fn shutdown(&self) {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Cmd::Shutdown { resp: tx }).await.is_ok() {
            let _ = rx.await;
        }
    }
}

/// Position a dynamic trace cursor at the first key ≥ `target` (a shorter `Row` is a strict
/// prefix and orders before every same-prefix longer row, so `[id]` seeks to node `id`'s
/// first entry).
fn seek(
    cursor: &mut impl Cursor<dbsp::dynamic::DynData, dbsp::dynamic::DynUnit, (), dbsp::DynZWeight>,
    target: &Row,
) {
    use dbsp::dynamic::Erase;
    cursor.seek_key(target.erase());
}

/// Sum the current key's net weight (an OrdZSet has one unit value per key; a spine cursor
/// still exposes it through the val loop).
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
            let node_id = match key.0.first() {
                Some(Value::Int(id)) => *id,
                _ => {
                    cursor.step_key();
                    continue;
                }
            };
            let value = key.0.get(1).cloned().unwrap_or(Value::Null);
            out.push(MemberDelta { node_id, value, delta });
        }
        cursor.step_key();
    }
}

/// The circuit thread: owns the `DBSPHandle`, applies tuple batches, steps, drains flips.
fn circuit_thread(
    mut dbsp: dbsp::DBSPHandle,
    input: ZSetHandle<Row>,
    flips: OutputHandle<SpineSnapshot<OrdZSet<Row>>>,
    mut rx: mpsc::Receiver<Cmd>,
) {
    while let Some(cmd) = rx.blocking_recv() {
        match cmd {
            Cmd::Batch { tuples, resp } => {
                let mut buf = tuples;
                input.append(&mut buf);
                let mut out = Vec::new();
                match dbsp.transaction() {
                    Ok(()) => drain_flips(&flips, &mut out),
                    Err(e) => tracing::error!("membership circuit: transaction failed: {e}"),
                }
                let _ = resp.send(out);
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

    fn tuple(node: i64, value: Value, pk: &str, w: ZWeight) -> Tup2<Row, ZWeight> {
        Tup2(Row(vec![Value::Int(node), value, Value::Text(pk.into())]), w)
    }

    /// Flip semantics pin: the circuit's distinct deltas agree with the reference refcount
    /// fold (`engine::membership::fold_refcount_flips`) — Enter on 0→positive, Leave on
    /// positive→0, nothing in between.
    #[tokio::test(flavor = "multi_thread")]
    async fn flips_on_zero_crossings() {
        let c = MembershipCircuit::start().unwrap();
        // Reference fold over the same contribution stream.
        let mut groups = std::collections::HashMap::new();
        let contributions =
            |w: ZWeight| vec![(Value::Int(5), w)];

        // First contributor: Enter.
        let flips = c.apply(vec![tuple(1, Value::Int(5), "a", 1)]).await;
        let reference = crate::engine::membership::fold_refcount_flips(&mut groups, contributions(1));
        assert_eq!(flips, vec![MemberDelta { node_id: 1, value: Value::Int(5), delta: 1 }]);
        assert_eq!(reference.len(), 1, "reference agrees: one Enter");
        assert!(c.contains(1, &Value::Int(5)));

        // Second contributor to the same value: no flip.
        let flips = c.apply(vec![tuple(1, Value::Int(5), "b", 1)]).await;
        let reference = crate::engine::membership::fold_refcount_flips(&mut groups, contributions(1));
        assert!(flips.is_empty(), "second contributor must not flip: {flips:?}");
        assert!(reference.is_empty());

        // Remove one of two: still present, no flip.
        let flips = c.apply(vec![tuple(1, Value::Int(5), "a", -1)]).await;
        let reference = crate::engine::membership::fold_refcount_flips(&mut groups, contributions(-1));
        assert!(flips.is_empty());
        assert!(reference.is_empty());
        assert!(c.contains(1, &Value::Int(5)));

        // Remove the last: Leave.
        let flips = c.apply(vec![tuple(1, Value::Int(5), "b", -1)]).await;
        let reference = crate::engine::membership::fold_refcount_flips(&mut groups, contributions(-1));
        assert_eq!(flips, vec![MemberDelta { node_id: 1, value: Value::Int(5), delta: -1 }]);
        assert_eq!(reference.len(), 1, "reference agrees: one Leave");
        assert!(!c.contains(1, &Value::Int(5)));

        c.shutdown().await;
    }

    /// Same value on two nodes: state and flips are fully isolated per node_id.
    #[tokio::test(flavor = "multi_thread")]
    async fn nodes_are_isolated() {
        let c = MembershipCircuit::start().unwrap();
        let flips = c
            .apply(vec![tuple(1, Value::Int(7), "a", 1), tuple(2, Value::Int(7), "a", 1)])
            .await;
        assert_eq!(flips.len(), 2);
        assert!(c.contains(1, &Value::Int(7)));
        assert!(c.contains(2, &Value::Int(7)));

        let flips = c.apply(vec![tuple(1, Value::Int(7), "a", -1)]).await;
        assert_eq!(flips, vec![MemberDelta { node_id: 1, value: Value::Int(7), delta: -1 }]);
        assert!(!c.contains(1, &Value::Int(7)), "node 1 lost the value");
        assert!(c.contains(2, &Value::Int(7)), "node 2 keeps it");
        c.shutdown().await;
    }

    /// The NULL bucket is an ordinary key: `contains(node, Null)` is `has_null`.
    #[tokio::test(flavor = "multi_thread")]
    async fn contains_and_null_bucket() {
        let c = MembershipCircuit::start().unwrap();
        assert!(!c.contains(1, &Value::Null), "empty circuit: nothing is a member");
        c.apply(vec![tuple(1, Value::Null, "a", 1)]).await;
        assert!(c.contains(1, &Value::Null));
        c.apply(vec![tuple(1, Value::Null, "a", -1)]).await;
        assert!(!c.contains(1, &Value::Null));
        c.shutdown().await;
    }

    /// Introspection: per-node distinct count + contributor counts, node-scoped.
    #[tokio::test(flavor = "multi_thread")]
    async fn values_for_node_reports_contributor_counts() {
        let c = MembershipCircuit::start().unwrap();
        c.apply(vec![
            tuple(1, Value::Int(5), "a", 1),
            tuple(1, Value::Int(5), "b", 1),
            tuple(1, Value::Int(9), "c", 1),
            tuple(2, Value::Int(5), "z", 1),
        ])
        .await;
        let (distinct, vals) = c.values_for_node(1, 10);
        assert_eq!(distinct, 2);
        assert_eq!(vals, vec![(Value::Int(5), 2), (Value::Int(9), 1)]);
        // cap truncates but distinct stays true
        let (distinct, vals) = c.values_for_node(1, 1);
        assert_eq!(distinct, 2);
        assert_eq!(vals.len(), 1);
        // unknown node: empty
        assert_eq!(c.values_for_node(3, 10), (0, Vec::new()));
        c.shutdown().await;
    }

    /// A pk moving values inside one step nets to Leave(old) + Enter(new).
    #[tokio::test(flavor = "multi_thread")]
    async fn retract_insert_same_step_nets() {
        let c = MembershipCircuit::start().unwrap();
        c.apply(vec![tuple(1, Value::Int(5), "a", 1)]).await;
        let mut flips =
            c.apply(vec![tuple(1, Value::Int(5), "a", -1), tuple(1, Value::Int(7), "a", 1)]).await;
        flips.sort_by(|a, b| a.value.cmp(&b.value));
        assert_eq!(
            flips,
            vec![
                MemberDelta { node_id: 1, value: Value::Int(5), delta: -1 },
                MemberDelta { node_id: 1, value: Value::Int(7), delta: 1 },
            ]
        );
        c.shutdown().await;
    }
}
