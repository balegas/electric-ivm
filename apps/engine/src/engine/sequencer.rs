//! The LSN-ordered sequencer: command protocol, per-table executors, the main loop,
//! envelope processing, activation/backfill, and the reliable flush.

use super::*;

/// Handle to the engine's single **sequencer** task — the LSN-ordered executor consuming the
/// global `changes` stream (Electric's `ShapeLogCollector` pattern): one task processes every
/// table's changes in commit order and flushes each transaction's shape appends before the next
/// transaction, restoring per-transaction atomic emission across tables.
pub(crate) struct SequencerHandle {
    pub(crate) cmd_tx: mpsc::UnboundedSender<SequencerCmd>,
    /// Change-log offset up to which every envelope has been processed AND fanned to every shape
    /// (appends landed). A harness polls this against the change log's tail as the convergence
    /// barrier.
    pub(crate) processed: Arc<std::sync::Mutex<String>>,
    /// Per-table circuit topology (shared families + standalone count), for tests/observability.
    pub(crate) stats: Arc<std::sync::Mutex<HashMap<String, TableStats>>>,
    /// Live per-node state summaries, merged across all tables, keyed by graph node id.
    /// Republished after every processed batch and on shape add/remove; read by `GET /state`.
    pub(crate) node_states: Arc<std::sync::Mutex<HashMap<String, NodeStateSummary>>>,
}

/// The tables the sequencer can decode, shared with the `Engine` (which updates it on
/// `setup_postgres` / `define_schema`). A std lock: reads are brief and never held across awaits.
pub(crate) type SharedTables = Arc<std::sync::RwLock<HashMap<String, TableSchema>>>;

pub(crate) enum SequencerCmd {
    /// Phase 1 of shape creation: register a PENDING shape that buffers this table's deltas while
    /// the creator runs the Postgres backfill concurrently — the sequencer itself never blocks on
    /// Postgres, so one slow backfill cannot stall the whole change pipeline. Buffer registration
    /// is acknowledged BEFORE the creator takes its snapshot, so no change can fall between the
    /// snapshot and activation.
    BeginShape {
        table: String,
        shape_id: String,
        num_id: u64,
        stream_path: String,
        pred: Arc<CompiledPredicate>,
        /// Output projection (column indices to emit), or `None` for the full row.
        out_cols: Option<Arc<Vec<usize>>>,
        kind: CreateKind,
        ack: tokio::sync::oneshot::Sender<()>,
    },
    /// Phase 2: the creator's backfill snapshot is appended (plain) or carried as `agg_seed`
    /// (aggregates); drain the buffered deltas through the shape's snapshot gate and go live.
    /// `ready` mirrors the old add-shape handshake: `Ok(())` once the shape is live and its
    /// snapshot + gated buffer are on the stream, `Err(reason)` otherwise.
    ActivateShape {
        table: String,
        shape_id: String,
        gate: crate::pg::SnapshotGate,
        /// Backfill rows for seeding an aggregate's fold (empty for plain shapes — the creator
        /// already appended their snapshot envelopes).
        agg_seed: Vec<Row>,
        /// Snapshot envelopes the creator appended (seeds the shape's emit counter).
        emitted_seed: u64,
        ready: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    },
    /// Creation failed after `BeginShape`: drop the pending buffer.
    AbortShape { table: String, shape_id: String },
    /// Retention: unregister a plain row shape's routing and hand back its resume state — the
    /// sequencer's fully-processed change-log offset (the batch preceding this command was fully
    /// fanned out + flushed, so the shape's stream is complete up to here) and the shape's
    /// backfill-snapshot gate. `None` if the shape is unknown (or an aggregate — not parkable).
    DeactivateShape {
        table: String,
        shape_id: String,
        resp: tokio::sync::oneshot::Sender<Option<(String, crate::pg::SnapshotGate)>>,
    },
    RemoveShape { table: String, shape_id: String },
    /// Create a **circuit-served** COUNT aggregate over the table's counts pipeline: seeded by
    /// summing matching groups, then updated from the pipeline's per-transaction group deltas.
    CreateCircuitAgg {
        table: String,
        shape_id: String,
        stream_path: String,
        constraints: Vec<Option<std::collections::HashSet<Value>>>,
        ready: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    },
    /// Dump the full internal state of one node (`family:<t>:<cols>` → the routing index
    /// contents; an aggregate `shape:<sid>` → the fold internals incl. the MIN/MAX multiset).
    /// `None` if the node id is unknown. Serves `GET /state/node`.
    DumpNode { table: String, node_id: String, resp: tokio::sync::oneshot::Sender<Option<serde_json::Value>> },
}

/// What kind of shape a pending creation becomes at activation.
#[derive(Clone)]
pub(crate) enum CreateKind {
    Plain,
    Aggregate { func: AggFn, col: Option<usize> },
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_sequencer(
    ds: DsClient,
    tables: SharedTables,
    start_offset: String,
    catalog_tx: mpsc::UnboundedSender<CatalogEvent>,
    subq: SubqueryHandle,
    trace_tx: tokio::sync::broadcast::Sender<Arc<String>>,
    arr: Option<crate::arrangements::Arrangements>,
    arr_gates: HashMap<String, crate::pg::SnapshotGate>,
) -> SequencerHandle {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let processed = Arc::new(std::sync::Mutex::new(start_offset.clone()));
    let stats = Arc::new(std::sync::Mutex::new(HashMap::new()));
    let node_states = Arc::new(std::sync::Mutex::new(HashMap::new()));
    tokio::spawn(sequencer_loop(
        ds,
        tables,
        start_offset,
        catalog_tx,
        cmd_rx,
        processed.clone(),
        stats.clone(),
        node_states.clone(),
        subq,
        trace_tx,
        arr,
        arr_gates,
    ));
    SequencerHandle { cmd_tx, processed, stats, node_states }
}

/// Rebuild + publish the merged node-state map and per-table stats to the sequencer's shared
/// handles and, when anyone is subscribed to `/trace`, broadcast the merged map (plus the
/// subquery registry's summaries) as a `{"type":"state"}` event.
pub(crate) async fn publish_all(
    execs: &HashMap<String, TableExec>,
    offset: &str,
    emitted: &HashMap<String, u64>,
    stats: &std::sync::Mutex<HashMap<String, TableStats>>,
    node_states: &std::sync::Mutex<HashMap<String, NodeStateSummary>>,
    subqueries: &Arc<Mutex<SubqueryRegistry>>,
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
) {
    let mut stats_map = HashMap::new();
    let mut merged: HashMap<String, NodeStateSummary> = HashMap::new();
    for (t, exec) in execs {
        stats_map.insert(t.clone(), stats_of(exec));
        merged.extend(build_node_states(
            &exec.ts,
            offset,
            exec.envelopes_total,
            &exec.shapes,
            &exec.families,
            &exec.family_of,
            &exec.aggregates,
            &exec.circuit_aggs,
            emitted,
        ));
    }
    *stats.lock().unwrap() = stats_map;
    *node_states.lock().unwrap() = merged.clone();
    if trace_tx.receiver_count() == 0 {
        return;
    }
    let mut ev_nodes = merged;
    for (id, s) in subqueries.lock().await.state_summaries() {
        ev_nodes.insert(id, s);
    }
    if let Ok(json) = serde_json::to_string(&crate::trace::StateEvent::new(ev_nodes)) {
        let _ = trace_tx.send(Arc::new(json));
    }
}

/// Per-table executor state owned by the sequencer: the routing structures a table's changes fan
/// out through, plus any in-flight (pending) shape creations buffering deltas.
pub(crate) struct TableExec {
    pub(crate) ts: TableSchema,
    pub(crate) shapes: HashMap<String, StandaloneShape>,
    pub(crate) shape_index: StandaloneIndex,
    pub(crate) families: HashMap<Vec<usize>, KeyRouter>,
    pub(crate) family_of: HashMap<String, (Vec<usize>, u64, Row)>,
    pub(crate) aggregates: HashMap<String, AggShape>,
    /// Necessary-conjunct index over the aggregates' predicates (same structure as
    /// `shape_index`): per change only candidate aggregates are folded, so aggregate count
    /// stops being a linear per-change term. Match-all / un-indexable predicates land on the
    /// index's scan list and stay always-candidates.
    pub(crate) agg_index: StandaloneIndex,
    /// Circuit-served COUNT aggregates on this table (see [`CircuitAgg`]).
    pub(crate) circuit_aggs: HashMap<String, CircuitAgg>,
    pub(crate) pending: HashMap<String, PendingShape>,
    pub(crate) envelopes_total: u64,
}

impl TableExec {
    pub(crate) fn new(ts: TableSchema) -> TableExec {
        TableExec {
            ts,
            shapes: HashMap::new(),
            shape_index: StandaloneIndex::default(),
            families: HashMap::new(),
            family_of: HashMap::new(),
            aggregates: HashMap::new(),
            agg_index: StandaloneIndex::default(),
            circuit_aggs: HashMap::new(),
            pending: HashMap::new(),
            envelopes_total: 0,
        }
    }
}

/// A shape between `BeginShape` and `ActivateShape`: buffers every processed delta of its table so
/// activation can replay exactly what the backfill snapshot did not see (through the gate).
pub(crate) struct PendingShape {
    pub(crate) num_id: u64,
    pub(crate) stream_path: String,
    pub(crate) pred: Arc<CompiledPredicate>,
    pub(crate) out_cols: Option<Arc<Vec<usize>>>,
    pub(crate) kind: CreateKind,
    pub(crate) buffered: Vec<Envelope>,
}

/// Get (or lazily create) the executor for `table`; `None` if the table has no known schema.
pub(crate) fn exec_for<'a>(
    execs: &'a mut HashMap<String, TableExec>,
    tables: &SharedTables,
    table: &str,
) -> Option<&'a mut TableExec> {
    if !execs.contains_key(table) {
        let ts = tables.read().unwrap().get(table).cloned()?;
        execs.insert(table.to_string(), TableExec::new(ts));
    }
    execs.get_mut(table)
}

/// The engine's single LSN-ordered executor: consumes the global `changes` stream in commit order
/// and dispatches each envelope to its table's executor. Each transaction's shape appends are
/// flushed **before the next transaction is processed**, so every shape stream reflects source
/// transactions atomically and in commit order — cross-table included (Electric's
/// `ShapeLogCollector` pattern; the property the old per-table tailers lost).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn sequencer_loop(
    ds: DsClient,
    tables: SharedTables,
    start_offset: String,
    catalog_tx: mpsc::UnboundedSender<CatalogEvent>,
    mut cmd_rx: mpsc::UnboundedReceiver<SequencerCmd>,
    processed: Arc<std::sync::Mutex<String>>,
    stats: Arc<std::sync::Mutex<HashMap<String, TableStats>>>,
    node_states: Arc<std::sync::Mutex<HashMap<String, NodeStateSummary>>>,
    subq: SubqueryHandle,
    trace_tx: tokio::sync::broadcast::Sender<Arc<String>>,
    arr: Option<crate::arrangements::Arrangements>,
    arr_gates: HashMap<String, crate::pg::SnapshotGate>,
) {
    let mut execs: HashMap<String, TableExec> = HashMap::new();
    let mut offset = start_offset;
    // Offset checkpointing: persist the processed position (the restart replay start) at most
    // every ~2s of change.
    let mut last_ckpt = std::time::Instant::now();
    let mut ckpt_offset = offset.clone();
    // Envelopes appended per shape id — the counters behind the per-node state summaries.
    let mut emitted: HashMap<String, u64> = HashMap::new();
    // De-duplication highwater: the ingestor's delivery is at-least-once (unacknowledged commits
    // re-deliver after a reconnect), and deltas are NOT idempotent for aggregates/subquery
    // weights. Every ingestor envelope carries (commit lsn, seq = position in txn), strictly
    // increasing on the single ordered log, so anything at/below the highwater has already been
    // applied and is skipped. Envelopes without both stamps (library mode) bypass this.
    let mut highwater: Option<(u64, u64)> = None;

    loop {
        let off = offset.clone();
        tokio::select! {
            biased;
            cmd = cmd_rx.recv() => match cmd {
                Some(SequencerCmd::BeginShape { table, shape_id, num_id, stream_path, pred, out_cols, kind, ack }) => {
                    match exec_for(&mut execs, &tables, &table) {
                        Some(exec) => {
                            exec.pending.insert(
                                shape_id,
                                PendingShape { num_id, stream_path, pred, out_cols, kind, buffered: Vec::new() },
                            );
                        }
                        None => tracing::error!("begin_shape: unknown table '{table}'"),
                    }
                    let _ = ack.send(());
                }
                Some(SequencerCmd::ActivateShape { table, shape_id, gate, agg_seed, emitted_seed, ready }) => {
                    let res = activate_shape(
                        &ds, &mut execs, &table, &shape_id, gate, agg_seed, emitted_seed, &mut emitted,
                    ).await;
                    if let Err(e) = &res {
                        tracing::error!("activate_shape failed: {e:#}");
                    }
                    let _ = ready.send(res.map_err(|e| format!("{e:#}")));
                    publish_all(&execs, &offset, &emitted, &stats, &node_states, &subq.registry, &trace_tx).await;
                }
                Some(SequencerCmd::AbortShape { table, shape_id }) => {
                    if let Some(exec) = execs.get_mut(&table) {
                        exec.pending.remove(&shape_id);
                    }
                }
                Some(SequencerCmd::DeactivateShape { table, shape_id, resp }) => {
                    // Capture-and-unregister is atomic w.r.t. envelope processing (commands run
                    // between fully-flushed transactions), so `offset` is exactly "the shape's
                    // stream is complete up to here".
                    let gate = execs.get_mut(&table).and_then(|exec| {
                        if let Some(shape) = exec.shapes.remove(&shape_id) {
                            exec.shape_index.remove(&shape_id);
                            Some(shape.gate)
                        } else if let Some((key_cols, num_id, key_tuple)) = exec.family_of.remove(&shape_id) {
                            let mut gate = None;
                            if let Some(router) = exec.families.get_mut(&key_cols) {
                                if let Some(routed) = router.index.get_mut(&key_tuple) {
                                    if let Some(pos) = routed.iter().position(|rs| rs.num_id == num_id) {
                                        gate = Some(routed.remove(pos).gate);
                                    }
                                    if routed.is_empty() {
                                        router.index.remove(&key_tuple);
                                    }
                                }
                                if router.index.is_empty() {
                                    exec.families.remove(&key_cols);
                                }
                            }
                            gate
                        } else {
                            None // unknown, pending, or an aggregate — not parkable from here
                        }
                    });
                    if gate.is_some() {
                        emitted.remove(&shape_id);
                    }
                    let _ = resp.send(gate.map(|g| (offset.clone(), g)));
                    publish_all(&execs, &offset, &emitted, &stats, &node_states, &subq.registry, &trace_tx).await;
                }
                Some(SequencerCmd::RemoveShape { table, shape_id }) => {
                    if let Some(exec) = execs.get_mut(&table) {
                        exec.pending.remove(&shape_id);
                        if exec.circuit_aggs.remove(&shape_id).is_some() {
                            // a circuit-served COUNT — nothing else to unwind
                        } else if exec.aggregates.remove(&shape_id).is_some() {
                            // an aggregation shape — drop its conjunct-index entry too
                            exec.agg_index.remove(&shape_id);
                        } else if exec.shapes.remove(&shape_id).map(|_| exec.shape_index.remove(&shape_id)).is_none()
                            && let Some((key_cols, num_id, key_tuple)) = exec.family_of.remove(&shape_id)
                            && let Some(router) = exec.families.get_mut(&key_cols)
                        {
                            // Drop the shape from its key's routing list (the shape stream is torn
                            // down elsewhere); discard the router once it routes to no shapes.
                            if let Some(routed) = router.index.get_mut(&key_tuple) {
                                routed.retain(|rs| rs.num_id != num_id);
                                if routed.is_empty() {
                                    router.index.remove(&key_tuple);
                                }
                            }
                            if router.index.is_empty() {
                                exec.families.remove(&key_cols);
                            }
                        }
                    }
                    emitted.remove(&shape_id);
                    publish_all(&execs, &offset, &emitted, &stats, &node_states, &subq.registry, &trace_tx).await;
                }
                Some(SequencerCmd::CreateCircuitAgg { table, shape_id, stream_path, constraints, ready }) => {
                    let res = create_circuit_agg(
                        &ds, arr.as_ref(), &mut execs, &tables, &table, &shape_id, &stream_path, constraints,
                    )
                    .await;
                    if res.is_ok() {
                        emitted.insert(shape_id.clone(), 1);
                    }
                    let _ = ready.send(res.map_err(|e| format!("{e:#}")));
                    publish_all(&execs, &offset, &emitted, &stats, &node_states, &subq.registry, &trace_tx).await;
                }
                Some(SequencerCmd::DumpNode { table, node_id, resp }) => {
                    let val = execs.get(&table).and_then(|exec| dump_node_json(exec, &offset, &emitted, &node_id));
                    let _ = resp.send(val);
                }
                None => break,
            },
            res = ds.read(crate::CHANGES_STREAM, &off, true) => match res {
                Ok(rr) => {
                    let next = rr.next_offset.clone();
                    if let Some(n) = rr.next_offset { offset = n; }
                    // Split the read batch into transactions (runs of equal (txid, lsn) — the
                    // ingestor appends whole commits contiguously, in commit order) and flush each
                    // transaction's appends before processing the next: atomic per-transaction
                    // emission, across tables.
                    let envs = rr.envelopes;
                    let mut touched = false;
                    let mut i = 0;
                    while i < envs.len() {
                        let txid = envs[i].headers.txid.clone();
                        let lsn = envs[i].headers.lsn.clone();
                        let mut j = i + 1;
                        while j < envs.len() && envs[j].headers.txid == txid && envs[j].headers.lsn == lsn {
                            j += 1;
                        }
                        // Feed this transaction into the dbsp counts pipelines and step the
                        // circuit BEFORE fanning it out, so circuit-served aggregates emit
                        // within the transaction that changed them. The counts layer re-checks
                        // its own (lsn, seq) highwater, so feeding pre-dedup envelopes is safe.
                        let txn_count_deltas = if let Some(arr) = &arr {
                            let deltas: Vec<_> = envs[i..j]
                                .iter()
                                .filter_map(|env| stamped_delta_for_arrangements(&tables, arr, &arr_gates, env))
                                .collect();
                            arr.apply_batch(deltas).await
                        } else {
                            Vec::new()
                        };
                        let mut txn_pending: HashMap<String, Vec<Envelope>> = HashMap::new();
                        for env in envs[i..j].iter() {
                            // Skip redelivered changes (see `highwater` above).
                            let pos = match (env.headers.lsn.as_deref(), env.headers.seq) {
                                (Some(l), Some(seq)) => Some((crate::pg::lsn_to_u64(l), seq)),
                                _ => None,
                            };
                            if let (Some(p), Some(hw)) = (pos, highwater) {
                                if p <= hw {
                                    tracing::debug!("sequencer: skipping duplicate change at {p:?}");
                                    continue;
                                }
                            }
                            let Some(exec) = exec_for(&mut execs, &tables, &env.type_) else {
                                tracing::error!("sequencer: change for unknown table '{}'", env.type_);
                                if let Some(p) = pos { highwater = Some(p); }
                                continue;
                            };
                            // Buffer for in-flight creations on this table: their `BeginShape` was
                            // acknowledged before the creator's snapshot, so everything the
                            // snapshot cannot contain lands in the buffer.
                            for pending in exec.pending.values_mut() {
                                pending.buffered.push(env.clone());
                            }
                            if let Err(e) = process_envelope(
                                &exec.ts, &exec.shapes, &exec.shape_index, &exec.families,
                                &mut exec.aggregates, &exec.agg_index, env.clone(), &mut txn_pending,
                                &subq, &trace_tx,
                            )
                            .await
                            {
                                tracing::error!("process_envelope failed: {e:#}");
                            }
                            exec.envelopes_total += 1;
                            touched = true;
                            if let Some(p) = pos {
                                highwater = Some(p);
                            }
                        }
                        // Counts pipeline → circuit-served aggregates.
                        if !txn_count_deltas.is_empty() {
                            apply_count_deltas(
                                &mut execs, txn_count_deltas, txid.clone(), lsn.clone(), &mut txn_pending,
                                &trace_tx,
                            );
                        }
                        emit_storage_txn_metrics(&txn_pending);
                        for (path, envs) in &txn_pending {
                            *emitted.entry(sid_of_path(path).to_string()).or_insert(0) += envs.len() as u64;
                        }
                        // Transaction boundary: every append of this commit lands before the next
                        // commit is processed.
                        flush_pending(&ds, txn_pending).await;
                        i = j;
                    }
                    // Publish the processed offset only after the whole batch is fanned out + flushed.
                    if let Some(n) = next {
                        *processed.lock().unwrap() = n.clone();
                        if n != ckpt_offset && last_ckpt.elapsed() >= std::time::Duration::from_secs(2) {
                            ckpt_offset = n.clone();
                            last_ckpt = std::time::Instant::now();
                            let _ = catalog_tx.send(CatalogEvent::Offset { offset: n });
                        }
                    }
                    if touched {
                        publish_all(&execs, &offset, &emitted, &stats, &node_states, &subq.registry, &trace_tx).await;
                    }
                }
                Err(e) => {
                    tracing::warn!("sequencer read error on {}: {e:#}; backing off", crate::CHANGES_STREAM);
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
            },
        }
    }
}

/// Make a pending shape live: register its routing, then replay its buffered deltas through the
/// snapshot gate — emitting exactly the changes the backfill snapshot did not see. The buffered
/// replay is appended before the sequencer processes any further change, so the shape stream stays
/// in commit order.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn activate_shape(
    ds: &DsClient,
    execs: &mut HashMap<String, TableExec>,
    table: &str,
    shape_id: &str,
    gate: crate::pg::SnapshotGate,
    agg_seed: Vec<Row>,
    emitted_seed: u64,
    emitted: &mut HashMap<String, u64>,
) -> Result<()> {
    let exec = execs.get_mut(table).with_context(|| format!("no executor for table '{table}'"))?;
    let p = exec
        .pending
        .remove(shape_id)
        .with_context(|| format!("no pending shape '{shape_id}' (aborted?)"))?;
    if emitted_seed > 0 {
        emitted.insert(shape_id.to_string(), emitted_seed);
    }
    match p.kind {
        CreateKind::Plain => {
            // Register routing first (an equality template joins/creates its family's KeyRouter;
            // everything else is a standalone indexed filter)...
            match p.pred.equality_template() {
                Some(pairs) => {
                    let key_cols: Vec<usize> = pairs.iter().map(|(c, _)| *c).collect();
                    let key_tuple = Row(pairs.into_iter().map(|(_, v)| v).collect());
                    let router = exec
                        .families
                        .entry(key_cols.clone())
                        .or_insert_with(|| KeyRouter { key_cols: key_cols.clone(), index: HashMap::new() });
                    router.index.entry(key_tuple.clone()).or_default().push(RoutedShape {
                        num_id: p.num_id,
                        stream_path: p.stream_path.clone(),
                        gate: gate.clone(),
                        out_cols: p.out_cols.clone(),
                    });
                    exec.family_of.insert(shape_id.to_string(), (key_cols, p.num_id, key_tuple));
                }
                None => {
                    exec.shape_index.insert(shape_id, &p.pred);
                    exec.shapes.insert(
                        shape_id.to_string(),
                        StandaloneShape {
                            pred: p.pred.clone(),
                            stream_path: p.stream_path.clone(),
                            gate: gate.clone(),
                            out_cols: p.out_cols.clone(),
                        },
                    );
                }
            }
            // ...then drain the buffer through the gate. `matches()` evaluates equality templates
            // and standalone predicates alike, so one replay path covers both placements.
            let mut outs: Vec<Envelope> = Vec::new();
            for env in &p.buffered {
                let Ok((delta, txid, lsn)) = apply_envelope(&exec.ts, env) else { continue };
                if delta.is_empty() {
                    continue;
                }
                let lsn_u64 = lsn.as_deref().map(crate::pg::lsn_to_u64).unwrap_or(0);
                let xid = txid.as_deref().and_then(|s| s.parse::<u64>().ok());
                if gate.should_skip(lsn_u64, xid) {
                    continue;
                }
                let matched = eval_standalone(&p.pred, &delta);
                if matched.is_empty() {
                    continue;
                }
                outs.extend(translate_output(
                    &exec.ts,
                    matched,
                    txid,
                    lsn,
                    p.out_cols.as_deref().map(Vec::as_slice),
                ));
            }
            if !outs.is_empty() {
                *emitted.entry(shape_id.to_string()).or_insert(0) += outs.len() as u64;
                ds.append_reliable(&p.stream_path, &outs).await;
            }
        }
        CreateKind::Aggregate { func, col } => {
            // Seed the fold from the backfill rows, emit the initial value, then fold the gated
            // buffer (emitting a value envelope whenever the aggregate moves).
            let mut agg = AggShape {
                pred: p.pred.clone(),
                func,
                col,
                stream_path: p.stream_path.clone(),
                gate: gate.clone(),
                count: 0,
                nn_count: 0,
                sum: 0.0,
                multiset: std::collections::BTreeMap::new(),
                last: None,
            };
            let seed: Vec<Tup2<Row, ZWeight>> = agg_seed.iter().map(|r| Tup2(r.clone(), 1)).collect();
            agg.apply(&seed);
            let mut outs = vec![agg.envelope(&exec.ts, None, None)];
            agg.last = Some(agg.value());
            for env in &p.buffered {
                let Ok((delta, txid, lsn)) = apply_envelope(&exec.ts, env) else { continue };
                if delta.is_empty() {
                    continue;
                }
                let lsn_u64 = lsn.as_deref().map(crate::pg::lsn_to_u64).unwrap_or(0);
                let xid = txid.as_deref().and_then(|s| s.parse::<u64>().ok());
                if gate.should_skip(lsn_u64, xid) {
                    continue;
                }
                if agg.apply(&delta) {
                    let val = agg.value();
                    if agg.last.as_ref() != Some(&val) {
                        agg.last = Some(val.clone());
                        outs.push(agg.envelope(&exec.ts, txid, lsn));
                    }
                }
            }
            *emitted.entry(shape_id.to_string()).or_insert(0) += outs.len() as u64;
            ds.append(&p.stream_path, &outs).await?;
            exec.agg_index.insert(shape_id, &agg.pred);
            exec.aggregates.insert(shape_id.to_string(), agg);
        }
    }
    Ok(())
}

/// Replay the global change log from `from` for one dormant shape: apply each of its table's
/// envelopes through the shape's snapshot gate + predicate + projection and append the matches to
/// the retained stream. Pages until the log reports up-to-date. Appends are direct (`ds.append`):
/// a 404 means the retained stream vanished (evicted/purged mid-replay) and must fail the resume.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn replay_changes_for_shape(
    ds: &DsClient,
    ts: &TableSchema,
    table: &str,
    pred: &CompiledPredicate,
    out_cols: Option<&Arc<Vec<usize>>>,
    gate: &crate::pg::SnapshotGate,
    stream_path: &str,
    from: &str,
) -> Result<u64> {
    let mut off = from.to_string();
    let mut emitted = 0u64;
    loop {
        let rr = ds.read(crate::CHANGES_STREAM, &off, false).await?;
        let mut outs: Vec<Envelope> = Vec::new();
        for env in &rr.envelopes {
            if env.type_ != table {
                continue;
            }
            let Ok((delta, txid, lsn)) = apply_envelope(ts, env) else { continue };
            if delta.is_empty() {
                continue;
            }
            let lsn_u64 = lsn.as_deref().map(crate::pg::lsn_to_u64).unwrap_or(0);
            let xid = txid.as_deref().and_then(|s| s.parse::<u64>().ok());
            if gate.should_skip(lsn_u64, xid) {
                continue;
            }
            let matched = eval_standalone(pred, &delta);
            if matched.is_empty() {
                continue;
            }
            outs.extend(translate_output(ts, matched, txid, lsn, out_cols.map(|c| c.as_slice())));
        }
        if !outs.is_empty() {
            emitted += outs.len() as u64;
            ds.append(stream_path, &outs).await.context("append replay to retained stream")?;
        }
        match rr.next_offset {
            Some(n) if n != off => {
                off = n;
                if rr.up_to_date {
                    break;
                }
            }
            _ => break,
        }
    }
    Ok(emitted)
}

/// Creator-side half of the two-phase shape creation: await the pending-buffer ack, run the
/// Postgres backfill on a pooled connection (appending the snapshot for plain shapes), then
/// activate. The sequencer keeps processing other work the whole time — a slow backfill only
/// delays THIS shape. Returns the creation outcome (`Err(reason)` mirrors the old handshake).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn backfill_and_activate(
    ds: &DsClient,
    pg_url: &Option<String>,
    cmd_tx: &mpsc::UnboundedSender<SequencerCmd>,
    ts: &TableSchema,
    table: &str,
    shape_id: &str,
    stream_path: &str,
    pred: &Arc<CompiledPredicate>,
    out_cols: Option<&Arc<Vec<usize>>>,
    changes_only: bool,
    is_aggregate: bool,
    ack_rx: tokio::sync::oneshot::Receiver<()>,
) -> std::result::Result<(), String> {
    let abort = || {
        let _ = cmd_tx.send(SequencerCmd::AbortShape {
            table: table.to_string(),
            shape_id: shape_id.to_string(),
        });
    };
    if ack_rx.await.is_err() {
        return Err("sequencer dropped the begin-shape ack".to_string());
    }
    // Backfill: current matching rows from a REPEATABLE READ snapshot, predicate pushed into the
    // SELECT; `matches()` is the final authority (a safety net if the SQL is ever a looser
    // superset). A `changes_only` feed skips the backfill and forwards only future matches
    // (passthrough gate) — the non-materialized live tail a subset query follows.
    let (gate, agg_seed, emitted_seed) = if changes_only {
        (crate::pg::SnapshotGate::passthrough(), Vec::new(), 0u64)
    } else {
        let t0 = std::time::Instant::now();
        let bf = match pg_backfill(pg_url, ts, Some(pred.as_ref())).await {
            Ok(bf) => bf,
            Err(e) => {
                abort();
                return Err(format!("{e:#}"));
            }
        };
        let make_new_ms = t0.elapsed().as_secs_f64() * 1000.0;
        if is_aggregate {
            // The sequencer seeds the fold and emits the initial value itself.
            (bf.gate, bf.rows, 0)
        } else {
            let out: Vec<(Row, ZWeight)> =
                bf.rows.iter().filter(|r| pred.matches(r)).map(|r| (r.clone(), 1)).collect();
            let rows = out.len() as u64;
            let mut snapshot_bytes = 0u64;
            let mut emitted_seed = 0u64;
            if !out.is_empty() {
                let envs = translate_output(ts, out, None, None, out_cols.map(|c| c.as_slice()));
                if crate::statsd::enabled() {
                    snapshot_bytes = envs_bytes(&envs);
                }
                if let Err(e) = ds.append(stream_path, &envs).await {
                    abort();
                    return Err(format!("append snapshot: {e:#}"));
                }
                emitted_seed = envs.len() as u64;
            }
            crate::statsd::snapshot_stored(rows, snapshot_bytes, make_new_ms);
            (bf.gate, Vec::new(), emitted_seed)
        }
    };
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    if cmd_tx
        .send(SequencerCmd::ActivateShape {
            table: table.to_string(),
            shape_id: shape_id.to_string(),
            gate,
            agg_seed,
            emitted_seed,
            ready: ready_tx,
        })
        .is_err()
    {
        return Err("sequencer is gone".to_string());
    }
    ready_rx.await.unwrap_or_else(|_| Err("sequencer dropped the ready channel".to_string()))
}

/// Read a backfill snapshot from Postgres (current rows + snapshot LSN). `filter`, when given, is the
/// shape's predicate — backfill reads only matching rows instead of the whole table. Without a
/// `pg_url` (library/no-source mode) the shape simply starts empty.
pub(crate) async fn pg_backfill(
    pg_url: &Option<String>,
    ts: &TableSchema,
    filter: Option<&CompiledPredicate>,
) -> Result<crate::pg::Backfill> {
    match pg_url {
        Some(url) => {
            let client = crate::pg::pool_for(url).get().await?;
            crate::pg::backfill(&client, ts, filter).await
        }
        None => Ok(crate::pg::Backfill {
            rows: Vec::new(),
            seed_lsn: "0/0".to_string(),
            gate: crate::pg::SnapshotGate::passthrough(),
        }),
    }
}


#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_envelope(
    ts: &TableSchema,
    shapes: &HashMap<String, StandaloneShape>,
    shape_index: &StandaloneIndex,
    families: &HashMap<Vec<usize>, KeyRouter>,
    aggregates: &mut HashMap<String, AggShape>,
    agg_index: &StandaloneIndex,
    env: Envelope,
    pending: &mut HashMap<String, Vec<Envelope>>,
    subq: &SubqueryHandle,
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
) -> Result<()> {
    let (delta, txid, lsn) = apply_envelope(ts, &env)?;
    if delta.is_empty() {
        return Ok(());
    }
    // Per-envelope trace collection (hops, reached shape ids). `None` when nobody is subscribed,
    // so the untraced hot path pays only this one atomic load — see `crate::trace`.
    let mut tr: Option<(Vec<crate::trace::TraceHop>, Vec<String>)> = if trace_tx.receiver_count() > 0 {
        Some((vec![crate::trace::TraceHop::new(format!("table:{}", ts.name), "passed")], Vec::new()))
    } else {
        None
    };
    // `lsn` (the commit-LSN string) is stamped onto output envelopes so a subset client can position
    // its live tail at the page snapshot (drop deltas with `lsn < snapshot_lsn`); `lsn_u64` is the
    // numeric fallback for the per-shape backfill-skip compare, and `xid` (the transaction id the
    // ingestor stamps as `txid`) is the primary fence — see `pg::SnapshotGate` for why xid visibility,
    // not LSN order, is the sound backfill↔replication reconciliation.
    let lsn_u64 = lsn.as_deref().map(crate::pg::lsn_to_u64).unwrap_or(0);
    let xid = txid.as_deref().and_then(|s| s.parse::<u64>().ok());
    metrics().envelopes.fetch_add(1, Ordering::Relaxed);
    let _t = Timer::new(&metrics().process_envelope);
    // Standalone shapes: evaluate each stateless filter directly on the delta (no thread, no clone).
    // Skip changes already visible to the shape's backfill snapshot (xid-visibility gate, LSN
    // fallback for changes without a parseable xid). On the untraced hot path only the index's
    // candidates are visited (a non-candidate's necessary conjunct fails, so it cannot match);
    // with a trace subscriber the full scan is kept so every filter node still reports a hop.
    let candidate_ids;
    let candidates: Box<dyn Iterator<Item = (&String, &StandaloneShape)>> = if tr.is_some() {
        Box::new(shapes.iter())
    } else {
        candidate_ids = shape_index.candidates(&delta);
        Box::new(candidate_ids.iter().filter_map(|sid| shapes.get_key_value(sid)))
    };
    for (sid, shape) in candidates {
        if shape.gate.should_skip(lsn_u64, xid) {
            if let Some((hops, _)) = tr.as_mut() {
                hops.push(crate::trace::TraceHop::new(format!("filter:{sid}"), "dropped"));
            }
            continue;
        }
        let out = eval_standalone(&shape.pred, &delta);
        if out.is_empty() {
            if let Some((hops, _)) = tr.as_mut() {
                hops.push(crate::trace::TraceHop::new(format!("filter:{sid}"), "dropped"));
            }
            continue;
        }
        if let Some((hops, ids)) = tr.as_mut() {
            hops.push(crate::trace::TraceHop::new(format!("filter:{sid}"), "passed"));
            hops.push(crate::trace::TraceHop::new(format!("shape:{sid}"), "passed"));
            ids.push(sid.clone());
        }
        let envs =
            translate_output(ts, out, txid.clone(), lsn.clone(), shape.out_cols.as_deref().map(Vec::as_slice));
        pending.entry(shape.stream_path.clone()).or_default().extend(envs);
    }
    // Equality routers: route each delta row by its key to exactly the shapes registered on that key.
    // No table copy, no join state — membership is the key match (an equality-template predicate matches a
    // row iff its key equals the shape's constants). Each shape's own snapshot gate is applied, so
    // changes already in that shape's backfill are skipped.
    let _s = Timer::new(&metrics().family_step);
    for router in families.values() {
        type ShapeOut<'a> = (&'a str, Option<&'a [usize]>, Vec<(Row, ZWeight)>);
        let mut by_shape: HashMap<u64, ShapeOut> = HashMap::new();
        let mut routed_keys: Vec<Row> = Vec::new();
        for Tup2(row, w) in &delta {
            let key = key_of(row, &router.key_cols);
            let Some(routed) = router.index.get(&key) else { continue };
            if tr.is_some() && !routed_keys.contains(&key) {
                routed_keys.push(key);
            }
            for rs in routed {
                if rs.gate.should_skip(lsn_u64, xid) {
                    continue;
                }
                by_shape
                    .entry(rs.num_id)
                    .or_insert_with(|| (rs.stream_path.as_str(), rs.out_cols.as_deref().map(Vec::as_slice), Vec::new()))
                    .2
                    .push((row.clone(), *w));
            }
        }
        if let Some((hops, ids)) = tr.as_mut() {
            // Node id matches the visualizer's logical graph: family:<table>:<key cols by name>.
            let cols = router
                .key_cols
                .iter()
                .map(|i| ts.columns.get(*i).map(|(n, _)| n.clone()).unwrap_or_else(|| format!("col{i}")))
                .collect::<Vec<_>>()
                .join(",");
            let node = format!("family:{}:{cols}", ts.name);
            if by_shape.is_empty() {
                hops.push(crate::trace::TraceHop::new(node, "dropped"));
            } else {
                for key in &routed_keys {
                    let key_json = serde_json::Value::Array(key.0.iter().map(crate::value::Value::to_json).collect());
                    hops.push(crate::trace::TraceHop::routed(node.clone(), key_json));
                }
                for num_id in by_shape.keys() {
                    let sid = format!("s{num_id}");
                    hops.push(crate::trace::TraceHop::new(format!("shape:{sid}"), "passed"));
                    ids.push(sid);
                }
            }
        }
        if by_shape.is_empty() {
            continue;
        }
        metrics().family_steps.fetch_add(1, Ordering::Relaxed);
        for (_sid, (stream_path, out_cols, rows)) in by_shape {
            let envs = translate_output(ts, rows, txid.clone(), lsn.clone(), out_cols);
            if !envs.is_empty() {
                pending.entry(stream_path.to_string()).or_default().extend(envs);
            }
        }
    }
    // Subquery shapes/nodes: route this delta through the cross-table registry. Under the lock it
    // updates the shared inner-set nodes (in-memory) and emits outer-shape deltas; the flip-driven
    // Postgres query-backs are handed to the engine's flip-propagator task so they never block
    // this tailer. The convergence barrier is processed offsets + a drained flip queue
    // (`pending_flips == 0`).
    {
        let mut work = std::collections::VecDeque::new();
        {
            let mut reg = subq.registry.lock().await;
            if reg.touches(&ts.name) {
                let mut sq_hops: Option<Vec<crate::trace::TraceHop>> = tr.as_ref().map(|_| Vec::new());
                work = reg.on_table_delta(ts, &delta, lsn_u64, xid, txid.clone(), sq_hops.as_mut()).await?;
                if let (Some((hops, ids)), Some(sq)) = (tr.as_mut(), sq_hops) {
                    for h in &sq {
                        if h.outcome == "passed"
                            && let Some(sid) = h.node.strip_prefix("shape:")
                            && !ids.iter().any(|i| i == sid)
                        {
                            ids.push(sid.to_string());
                        }
                    }
                    hops.extend(sq);
                }
            }
        }
        if !work.is_empty() {
            subq.pending_flips.fetch_add(1, Ordering::SeqCst);
            if subq.flip_tx.send(FlipWork { work, txid: txid.clone() }).is_err() {
                // Propagator gone (shutdown) — don't leave the barrier stuck.
                subq.pending_flips.fetch_sub(1, Ordering::SeqCst);
            }
        }
    }
    // Scalar aggregations: fold this delta into each *candidate* aggregate (necessary-conjunct
    // index — a non-candidate's predicate provably matches no delta row, so skipping it leaves
    // the fold unchanged); emit the new value when it changes. Skips changes already counted in
    // the seed (the aggregate's snapshot gate). Under an attached trace subscriber the index is
    // bypassed (like the standalone tier) so every aggregate node reports a folded/dropped hop.
    let agg_candidates: Option<HashSet<String>> =
        if tr.is_none() { Some(agg_index.candidates(&delta).into_iter().collect()) } else { None };
    for (sid, agg) in aggregates.iter_mut() {
        if let Some(c) = &agg_candidates {
            if !c.contains(sid) {
                continue;
            }
        }
        if agg.gate.should_skip(lsn_u64, xid) {
            if let Some((hops, _)) = tr.as_mut() {
                hops.push(crate::trace::TraceHop::new(format!("shape:{sid}"), "dropped"));
            }
            continue;
        }
        let mut folded = false;
        if agg.apply(&delta) {
            let val = agg.value();
            if agg.last.as_ref() != Some(&val) {
                agg.last = Some(val.clone());
                let env = agg.envelope(ts, txid.clone(), lsn.clone());
                pending.entry(agg.stream_path.clone()).or_default().push(env);
                folded = true;
            }
        }
        if let Some((hops, ids)) = tr.as_mut() {
            hops.push(crate::trace::TraceHop::new(format!("shape:{sid}"), if folded { "folded" } else { "dropped" }));
            if folded {
                ids.push(sid.clone());
            }
        }
    }
    // Publish the trace event (serialize once; lossy send — see `crate::trace`).
    if let Some((hops, shape_ids)) = tr {
        let ev = crate::trace::TraceEvent {
            lsn: lsn.clone(),
            txid: txid.clone(),
            table: ts.name.clone(),
            delta: delta
                .iter()
                .take(crate::trace::DELTA_CAP)
                .map(|Tup2(row, w)| crate::trace::TraceDelta { row: ts.row_to_json(row), w: *w })
                .collect(),
            hops,
            shapes: shape_ids,
        };
        if let Ok(json) = serde_json::to_string(&ev) {
            let _ = trace_tx.send(Arc::new(json));
        }
    }
    Ok(())
}

/// Total serialized byte size of a set of output envelopes (for storage/snapshot byte metrics).
pub(crate) fn envs_bytes(envs: &[Envelope]) -> u64 {
    envs.iter().map(|e| serde_json::to_string(e).map(|s| s.len() as u64).unwrap_or(0)).sum()
}

/// Emit the per-source-transaction storage StatsD metrics from one txn's staged appends.
/// `affected_shape_count` = distinct shape streams the txn touched; `operations`/`bytes` = output
/// envelopes appended + their serialized size. (Subquery-registry appends go out synchronously inside
/// `process_envelope` and are not reflected here.) No-op when the txn produced no appends.
pub(crate) fn emit_storage_txn_metrics(txn_pending: &HashMap<String, Vec<Envelope>>) {
    let ops: u64 = txn_pending.values().map(|v| v.len() as u64).sum();
    if ops == 0 {
        return;
    }
    let bytes: u64 = txn_pending
        .values()
        .flatten()
        .map(|e| serde_json::to_string(e).map(|s| s.len() as u64).unwrap_or(0))
        .sum();
    crate::statsd::storage_txn(ops, bytes, txn_pending.len() as u64);
}

/// Flush the batch's staged appends, bounded-concurrently. Each envelope keeps its own txid, so
/// `awaitTxId` semantics are preserved; only the HTTP round-trips are coalesced + parallelized.
///
/// Appends are **reliable**: transient failures retry with backoff (`append_reliable`) rather than
/// being dropped — a lost shape append is a permanent divergence for that shape's subscribers, and
/// the tailer's processed-offset barrier (published after this returns) must mean "every subscriber
/// stream reflects the batch". The only non-retried case is a 404 (the shape was dropped mid-flush),
/// which discards cleanly.
pub(crate) async fn flush_pending(ds: &DsClient, pending: HashMap<String, Vec<Envelope>>) {
    const CAP: usize = 32; // bound in-flight appends so we don't swamp the storage server
    let mut items: Vec<(String, Vec<Envelope>)> = pending.into_iter().collect();
    while !items.is_empty() {
        let take = items.len().min(CAP);
        let batch = items.split_off(items.len() - take);
        let mut set = tokio::task::JoinSet::new();
        for (path, envs) in batch {
            let ds = ds.clone();
            set.spawn(async move {
                let _t = Timer::new(&metrics().append);
                ds.append_reliable(&path, &envs).await;
                metrics().shape_appends.fetch_add(1, Ordering::Relaxed);
            });
        }
        while set.join_next().await.is_some() {}
    }
}
