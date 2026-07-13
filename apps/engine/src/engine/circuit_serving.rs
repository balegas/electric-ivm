//! Circuit-tier serving: feeding transactions into the dbsp counts pipelines and maintaining
//! circuit-served COUNT aggregates. Row data lives in Postgres (see `membership`); the
//! circuit's only state is the in-memory (group → count) relations.

use super::*;

/// Convert one change-log envelope into a gate-fenced, stamped counts delta.
/// `None` = not applicable (table without a counts pipeline, empty delta, or fenced out by
/// the seed gate).
pub(crate) fn stamped_delta_for_arrangements(
    tables: &SharedTables,
    arr: &crate::arrangements::Arrangements,
    arr_gates: &HashMap<String, crate::pg::SnapshotGate>,
    env: &Envelope,
) -> Option<crate::arrangements::StampedDelta> {
    // Only counted tables enter the circuit — everything else has no input handle.
    arr.counts_group_cols(&env.type_)?;
    let ts = tables.read().unwrap().get(&env.type_).cloned()?;
    let (delta, txid, lsn) = apply_envelope(&ts, env).ok()?;
    if delta.is_empty() {
        return None;
    }
    let lsn_u = lsn.as_deref().map(crate::pg::lsn_to_u64);
    let xid_u = txid.as_deref().and_then(|t| t.parse::<u64>().ok());
    // Fresh-seed fence: skip changes the seed snapshot already contains (Z-set deltas are not
    // idempotent, so a double-apply would corrupt counts).
    if let Some(gate) = arr_gates.get(&env.type_) {
        if gate.should_skip(lsn_u.unwrap_or(0), xid_u) {
            return None;
        }
    }
    Some(crate::arrangements::StampedDelta {
        table: env.type_.clone(),
        delta,
        lsn: lsn_u,
        seq: env.headers.seq,
    })
}

/// Sequencer-side creation of a circuit-served COUNT aggregate: seed = Σ matching count groups
/// from the counts snapshot (consistent with the processed offset), emitted immediately.
pub(crate) async fn create_circuit_agg(
    ds: &DsClient,
    arr: Option<&crate::arrangements::Arrangements>,
    execs: &mut HashMap<String, TableExec>,
    tables: &SharedTables,
    table: &str,
    shape_id: &str,
    stream_path: &str,
    constraints: Vec<Option<std::collections::HashSet<Value>>>,
) -> Result<()> {
    let arr = arr.context("circuit aggregates require the counts layer")?;
    let exec = exec_for(execs, tables, table)
        .with_context(|| format!("circuit aggregate: unknown table '{table}'"))?;
    let mut agg = CircuitAgg { stream_path: stream_path.to_string(), constraints, value: 0 };
    let groups = arr.count_groups(table).context("counts pipeline not ready")?;
    agg.value = groups.iter().filter(|(g, _)| agg.group_matches(g)).map(|(_, c)| c).sum();
    let env = agg.envelope(&exec.ts.name, None, None);
    ds.append(stream_path, &[env])
        .await
        .map_err(|e| anyhow::anyhow!("append initial aggregate: {e:#}"))?;
    tracing::info!("circuit aggregate {shape_id}: serving COUNT('{table}') from the counts pipeline (initial {})", agg.value);
    exec.circuit_aggs.insert(shape_id.to_string(), agg);
    Ok(())
}

/// Fold one transaction's count-group deltas into the circuit-served aggregates and emit one
/// updated `{value, n}` envelope per changed aggregate. A circuit-served count is maintained here
/// (over the table's counts pipeline), NOT by the in-engine fold in [`process_envelope`], so this
/// is also where its fold trace hop is emitted — otherwise a count-affecting change would flash the
/// source table but never the fold node, and the source→fold serving edge would never pulse.
pub(crate) fn apply_count_deltas(
    execs: &mut HashMap<String, TableExec>,
    deltas: Vec<crate::arrangements::CountDelta>,
    txid: Option<String>,
    lsn: Option<String>,
    txn_pending: &mut HashMap<String, Vec<Envelope>>,
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
) {
    let mut changed: Vec<(String, String)> = Vec::new(); // (table, shape id), in first-touch order
    let mut net: HashMap<String, i64> = HashMap::new(); // shape id -> net count change this txn
    for d in deltas {
        let Some(exec) = execs.get_mut(&d.table) else { continue };
        for (sid, agg) in exec.circuit_aggs.iter_mut() {
            if agg.group_matches(&d.group) {
                agg.value += d.delta;
                *net.entry(sid.clone()).or_insert(0) += d.delta;
                if !changed.iter().any(|(_, s)| s == sid) {
                    changed.push((d.table.clone(), sid.clone()));
                }
            }
        }
    }
    for (table, sid) in changed {
        let Some(exec) = execs.get(&table) else { continue };
        let Some(agg) = exec.circuit_aggs.get(&sid) else { continue };
        let env = agg.envelope(&exec.ts.name, txid.clone(), lsn.clone());
        txn_pending.entry(agg.stream_path.clone()).or_default().push(env);
        // Animate the fold absorbing the source delta (see the fn doc): emit a `folded` hop on the
        // aggregate's `shape:<sid>` node, alongside the source `table:<t>` node the change entered
        // through — but only when the running count actually moved (a net-zero regrouping shows
        // nothing).
        let delta = net.get(&sid).copied().unwrap_or(0);
        if delta != 0 {
            emit_count_fold_trace(trace_tx, &table, &sid, delta, txid.clone(), lsn.clone());
        }
    }
}

/// Broadcast a trace event for a circuit-served COUNT fold that absorbed a source delta this
/// transaction: the source `table:<t>` passed the change into the aggregate's `shape:<sid>` fold.
/// This lets the pipeline visualizer flash the fold node and pulse the source→fold serving edge,
/// exactly as the in-engine fold does from [`process_envelope`]. Best-effort and zero-cost when no
/// one is subscribed (see [`crate::trace`]).
pub(crate) fn emit_count_fold_trace(
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
    table: &str,
    sid: &str,
    delta: i64,
    txid: Option<String>,
    lsn: Option<String>,
) {
    if trace_tx.receiver_count() == 0 {
        return;
    }
    let ev = crate::trace::TraceEvent {
        lsn,
        txid,
        table: table.to_string(),
        // One synthetic weighted row carrying the net count change, so the visualizer labels the
        // travelling dot +1 / −1 and colours it. The count's grouping is not itself a table row, so
        // the row payload is left empty.
        delta: vec![crate::trace::TraceDelta { row: serde_json::json!({}), w: delta }],
        hops: vec![
            crate::trace::TraceHop::new(format!("table:{table}"), "passed"),
            crate::trace::TraceHop::new(format!("shape:{sid}"), "folded"),
        ],
        shapes: vec![sid.to_string()],
    };
    if let Ok(json) = serde_json::to_string(&ev) {
        let _ = trace_tx.send(Arc::new(json));
    }
}
