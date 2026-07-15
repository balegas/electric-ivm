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

enum Cmd {
    Batch { asserts: Assertions, resp: oneshot::Sender<(Vec<MemberDelta>, Vec<FeedDelta>)> },
    Shutdown { resp: oneshot::Sender<()> },
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
    /// `ELECTRIC_IVM_FEED_TRACE=0` disables the published feed-relation trace — the SECOND
    /// copy of every feed's key set, used only for drop-time retraction and introspection
    /// (the upsert operator's internal integral, which decides the emissions, is unaffected).
    /// Disabling roughly halves the per-feed memory term; dropped shapes then leave their
    /// (unreachable — feed ids are never reused) entries in the operator integral instead of
    /// retracting them, a documented trade until stream-fold drop enumeration lands
    /// (bead dbsp-ds-4d8).
    pub fn start() -> Result<MembershipCircuit> {
        let feed_trace = std::env::var("ELECTRIC_IVM_FEED_TRACE").map(|v| v != "0").unwrap_or(true);
        Self::start_with(feed_trace)
    }

    /// [`start`], with the feed-trace choice explicit (tests).
    pub fn start_with(feed_trace: bool) -> Result<MembershipCircuit> {
        let members: Slot<MemberSnapshot> = Slot::default();
        let contributors: Slot<MapSnapshot> = Slot::default();
        let feeds: Slot<MapSnapshot> = Slot::default();
        let (m_slot, c_slot, f_slot) = (members.clone(), contributors.clone(), feeds.clone());
        let (dbsp, (contrib_in, feed_in, flips_out, feeds_out)) =
            Runtime::init_circuit(CircuitConfig::with_workers(1), move |circuit| {
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
            .spawn(move || circuit_thread(dbsp, contrib_in, feed_in, flips_out, feeds_out, rx))
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

    /// Stop the circuit thread. State is in-memory only; nothing to persist.
    pub async fn shutdown(&self) {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Cmd::Shutdown { resp: tx }).await.is_ok() {
            let _ = rx.await;
        }
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

    /// With the feed trace disabled, emissions (feed deltas) still flow — only the
    /// enumeration copy is gone: feed_pks/feed_len return empty, halving feed memory.
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
