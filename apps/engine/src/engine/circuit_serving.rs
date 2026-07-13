//! Circuit-tier serving: feeding transactions into the dbsp arrangements, creating
//! circuit-served shapes/aggregates, and routing their live deltas.

use super::*;

/// Convert one change-log envelope into a gate-fenced, stamped arrangement delta.
/// `None` = not applicable (unknown table, empty delta, or fenced out by the seed gate).
pub(crate) fn stamped_delta_for_arrangements(
    tables: &SharedTables,
    arr_gates: &HashMap<String, crate::pg::SnapshotGate>,
    env: &Envelope,
) -> Option<crate::arrangements::StampedDelta> {
    let ts = tables.read().unwrap().get(&env.type_).cloned()?;
    let (delta, txid, lsn) = apply_envelope(&ts, env).ok()?;
    if delta.is_empty() {
        return None;
    }
    let lsn_u = lsn.as_deref().map(crate::pg::lsn_to_u64);
    let xid_u = txid.as_deref().and_then(|t| t.parse::<u64>().ok());
    // Fresh-seed fence: skip changes the seed snapshot already contains (Z-set deltas are not
    // idempotent, so a double-apply would corrupt weights).
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

/// Catch the restored arrangement state up to the change-log tail before live processing:
/// read from the checkpoint's recorded offset and feed only the arrangements (shapes replay
/// through their own path). Overlap with the live loop is harmless — the arrangement layer
/// de-duplicates by `(lsn, seq)` highwater.
pub(crate) async fn arrangements_catch_up(
    ds: &DsClient,
    tables: &SharedTables,
    arr: &crate::arrangements::Arrangements,
    arr_gates: &HashMap<String, crate::pg::SnapshotGate>,
) {
    let Some(mut off) = arr.restored_offset().map(str::to_string) else { return };
    tracing::info!("arrangements: catch-up replay from {off}");
    loop {
        match ds.read(crate::CHANGES_STREAM, &off, false).await {
            Ok(rr) => {
                if rr.envelopes.is_empty() {
                    break;
                }
                let deltas: Vec<_> = rr
                    .envelopes
                    .iter()
                    .filter_map(|env| stamped_delta_for_arrangements(tables, arr_gates, env))
                    .collect();
                arr.apply_batch(deltas, rr.next_offset.clone()).await;
                match rr.next_offset {
                    Some(n) => off = n,
                    None => break,
                }
            }
            Err(e) => {
                tracing::warn!("arrangements catch-up read error: {e:#}; backing off");
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    }
}

/// Sequencer-side creation of a circuit-served shape (see [`SequencerCmd::CreateCircuitShape`]).
/// Runs between transactions, so the arrangement snapshots it reads are exactly at the
/// sequencer's processed position — the seed needs no gate and the live routing no buffer.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_circuit_shape(
    ds: &DsClient,
    arr: Option<&crate::arrangements::Arrangements>,
    execs: &mut HashMap<String, TableExec>,
    router_watch: &mut HashMap<String, Vec<(String, String)>>,
    tables: &SharedTables,
    table: &str,
    shape_id: &str,
    num_id: u64,
    stream_path: &str,
    constraint: PlannedConstraint,
    residual: Option<Arc<CompiledPredicate>>,
    out_cols: Option<Arc<Vec<usize>>>,
    seed: bool,
) -> Result<u64> {
    let arr = arr.context("circuit shapes require the arrangement layer")?;
    let exec = exec_for(execs, tables, table)
        .with_context(|| format!("circuit shape: unknown table '{table}'"))?;
    let groups = match constraint {
        PlannedConstraint::All => CohortGroups::All,
        PlannedConstraint::Static { col, keys } => CohortGroups::Static { col, keys },
        PlannedConstraint::Dynamic { col, inner_table, inner_proj, inner_col, inner_key } => {
            let rows = arr
                .lookup(&inner_table, &[inner_col], &Row(vec![inner_key.clone()]))
                .with_context(|| format!("arrangement not ready: {inner_table} router lookup"))?;
            let mut groups: HashMap<Value, i64> = HashMap::new();
            for r in &rows {
                match r.0.get(inner_proj) {
                    Some(Value::Null) | None => {}
                    Some(v) => *groups.entry(v.clone()).or_insert(0) += 1,
                }
            }
            router_watch
                .entry(inner_table.clone())
                .or_default()
                .push((table.to_string(), shape_id.to_string()));
            CohortGroups::Dynamic { col, inner_table, inner_proj, inner_col, inner_key, groups }
        }
    };
    let shape = CircuitShape { num_id, stream_path: stream_path.to_string(), groups, residual, out_cols };
    let mut seeded = 0u64;
    if seed {
        let rows = circuit_seed_rows(arr, &exec.ts.name, &shape)?;
        let out: Vec<(Row, ZWeight)> =
            rows.into_iter().filter(|r| shape.matches(r)).map(|r| (r, 1)).collect();
        if !out.is_empty() {
            let envs =
                translate_output(&exec.ts, out, None, None, shape.out_cols.as_deref().map(Vec::as_slice));
            ds.append(stream_path, &envs)
                .await
                .map_err(|e| anyhow::anyhow!("append snapshot: {e:#}"))?;
            seeded = envs.len() as u64;
        }
    }
    tracing::info!(
        "circuit shape {shape_id}: serving '{table}' from the pipeline (seeded {seeded} envelopes)"
    );
    exec.circuit_shapes.insert(shape_id.to_string(), shape);
    Ok(seeded)
}

/// The seed rows of a circuit-served shape: its cohort groups, read from the arrangements.
pub(crate) fn circuit_seed_rows(
    arr: &crate::arrangements::Arrangements,
    table: &str,
    shape: &CircuitShape,
) -> Result<Vec<Row>> {
    match &shape.groups {
        CohortGroups::All => arr.scan(table).context("arrangement not ready: scan"),
        CohortGroups::Static { col, keys } => {
            let mut rows = Vec::new();
            for k in keys {
                rows.extend(
                    arr.lookup(table, &[*col], &Row(vec![k.clone()]))
                        .context("arrangement not ready: lookup")?,
                );
            }
            Ok(rows)
        }
        CohortGroups::Dynamic { col, groups, .. } => {
            let mut rows = Vec::new();
            for k in groups.keys() {
                rows.extend(
                    arr.lookup(table, &[*col], &Row(vec![k.clone()]))
                        .context("arrangement not ready: lookup")?,
                );
            }
            Ok(rows)
        }
    }
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
    let arr = arr.context("circuit aggregates require the arrangement layer")?;
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

/// Apply one transaction's membership deltas to the dynamic circuit shapes watching them, and
/// emit the resulting move-ins (absolute upserts) / move-outs (deletes) from the
/// post-transaction arrangement snapshots. Absolute emission makes any overlap with the row
/// loop idempotent for subscribers. Flip detection and move-row resolution are the shared
/// membership kernel's ([`membership::fold_refcount_flips`], [`membership::query_rows_by_col`])
/// — resolution falls back to a pooled Postgres query when the arrangement lookup is
/// unavailable, so a missing/unseeded index is a slow path, never a silent move-miss.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_router_deltas(
    arr: &Option<crate::arrangements::Arrangements>,
    pg_url: &Option<String>,
    execs: &mut HashMap<String, TableExec>,
    router_watch: &HashMap<String, Vec<(String, String)>>,
    member_deltas: Vec<(String, Vec<Tup2<Row, ZWeight>>)>,
    txid: Option<String>,
    lsn: Option<String>,
    txn_pending: &mut HashMap<String, Vec<Envelope>>,
) {
    use crate::subquery::FlipDir;
    for (inner_table, delta) in member_deltas {
        let Some(watchers) = router_watch.get(&inner_table) else { continue };
        for (outer_table, sid) in watchers {
            let Some(exec) = execs.get_mut(outer_table) else { continue };
            let ts = exec.ts.clone();
            let Some(cs) = exec.circuit_shapes.get_mut(sid) else { continue };
            let CohortGroups::Dynamic { col, inner_proj, inner_col, inner_key, groups, .. } =
                &mut cs.groups
            else {
                continue;
            };
            let col = *col;
            // Refcount the projected values contributed by this shape's router key; a flip is a
            // group whose refcount crossed zero. NULL projections are skipped — circuit-served
            // membership is non-negated, so a NULL in the inner set cannot change membership.
            let contributions = delta.iter().filter_map(|Tup2(r, w)| {
                if r.0.get(*inner_col) != Some(&*inner_key) {
                    return None;
                }
                match r.0.get(*inner_proj) {
                    Some(Value::Null) | None => None,
                    Some(v) => Some((v.clone(), *w)),
                }
            });
            let flips = membership::fold_refcount_flips(groups, contributions);
            if flips.is_empty() {
                continue;
            }
            let (residual, out_cols, stream_path) =
                (cs.residual.clone(), cs.out_cols.clone(), cs.stream_path.clone());
            for f in flips {
                let dir: ZWeight = match f.dir {
                    FlipDir::Enter => 1,
                    FlipDir::Leave => -1,
                };
                let rows = match membership::query_rows_by_col(arr, pg_url, &ts, col, &f.value).await
                {
                    Ok(rows) => rows,
                    Err(e) => {
                        tracing::error!(
                            "circuit shape {sid}: move query-back failed for group {:?}: {e:#}",
                            f.value
                        );
                        continue;
                    }
                };
                let out: Vec<(Row, ZWeight)> = rows
                    .into_iter()
                    .filter(|r| residual.as_ref().is_none_or(|p| p.matches(r)))
                    .map(|r| (r, dir))
                    .collect();
                if out.is_empty() {
                    continue;
                }
                let envs = translate_output(
                    &ts, out, txid.clone(), lsn.clone(), out_cols.as_deref().map(Vec::as_slice),
                );
                txn_pending.entry(stream_path.clone()).or_default().extend(envs);
            }
        }
    }
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
