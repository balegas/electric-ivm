//! Durable shape catalog: the append-only `meta/catalog` event stream, its writer
//! task, and boot-time restore/resume of shapes.

use super::*;

/// The engine's durable **shape catalog**: an append-only event stream replayed at boot so a
/// restart re-registers every shape itself instead of requiring a client re-registration storm.
/// Plain/routed shapes resume with passthrough gates (the change log replays everything after the
/// persisted offset; re-emission across the crash window is idempotent absolute upserts);
/// aggregates re-seed their fold from a fresh Postgres snapshot (their fresh gate then skips the
/// replayed history). Subquery shapes are NOT restorable without persisted inner-node state (a
/// fresh-seeded node cannot detect downtime flips, which would leave stale move-outs forever) —
/// they are dropped loudly at restore for clients to recreate.
pub(crate) const CATALOG_STREAM: &str = "meta/catalog";

/// One catalog event. `Offset` checkpoints the sequencer's processed change-log position (the
/// replay start after a restart), appended at most every ~2s.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "t", rename_all = "camelCase")]
pub(crate) enum CatalogEvent {
    Created { rec: ShapeRecord, sig: Option<String> },
    /// A subscriber joined a shared feed (refcount +1).
    Joined { id: String },
    /// A subscriber left a shared feed (refcount −1). With retention, reaching refcount 0 keeps
    /// the shape (it goes dormant later), so `Left` never implies teardown.
    Left { id: String },
    /// The shape went dormant: routing state dropped, stream + record retained. `resume_offset`
    /// is the change-log position its stream is complete up to; `gate` is its original
    /// backfill-snapshot fence. Restores as dormant (an improvement over the in-memory-only
    /// lifecycle: a restart no longer forgets dormant shapes).
    Dormant { id: String, resume_offset: String, gate: crate::pg::SnapshotGate },
    /// A dormant shape was reactivated (replayed + re-registered).
    Reactivated { id: String },
    Dropped { id: String },
    Offset { offset: String },
}

/// Spawn the single catalog writer: events are appended strictly in send order (senders enqueue
/// while holding the engine-state lock, so the log order matches the state-mutation order).
pub(crate) fn spawn_catalog_writer(ds: DsClient, mut rx: mpsc::UnboundedReceiver<CatalogEvent>) {
    tokio::spawn(async move {
        let mut ensured = false;
        while let Some(ev) = rx.recv().await {
            if !ensured {
                ensured = self::ensure_catalog(&ds).await;
            }
            let Ok(json) = serde_json::to_value(&ev) else { continue };
            if let Err(e) = ds.append_json(CATALOG_STREAM, &[json]) .await {
                tracing::error!("catalog append failed (event lost; restart may under-restore): {e:#}");
            }
        }
    });
}

pub(crate) async fn ensure_catalog(ds: &DsClient) -> bool {
    match ds.ensure_stream(CATALOG_STREAM).await {
        Ok(()) => true,
        Err(e) => {
            tracing::error!("catalog stream create failed: {e:#}");
            false
        }
    }
}


impl Engine {
    /// Replay the durable shape catalog and re-register every restorable shape with the (not yet
    /// spawned) sequencer — see [`CATALOG_STREAM`] for the restore semantics per shape kind.
    pub(crate) async fn restore_catalog(&self, compiled: &HashMap<String, TableSchema>) -> Result<()> {
        // 1. Fold the event log.
        // (rec, sig, refcount, dormant resume state). The last Dormant/Reactivated event wins.
        type Restored = (ShapeRecord, Option<String>, usize, Option<(String, crate::pg::SnapshotGate)>);
        let mut recs: HashMap<String, Restored> = HashMap::new();
        let mut start_offset = "-1".to_string();
        let mut off = "-1".to_string();
        loop {
            let (events, next, up_to_date) = self.ds.read_json(CATALOG_STREAM, &off).await?;
            for ev in events {
                let Ok(ev) = serde_json::from_value::<CatalogEvent>(ev) else { continue };
                match ev {
                    CatalogEvent::Created { rec, sig } => {
                        recs.insert(rec.id.clone(), (rec, sig, 1, None));
                    }
                    CatalogEvent::Joined { id } => {
                        if let Some(e) = recs.get_mut(&id) {
                            e.2 += 1;
                        }
                    }
                    CatalogEvent::Left { id } => {
                        if let Some(e) = recs.get_mut(&id) {
                            e.2 = e.2.saturating_sub(1);
                        }
                    }
                    CatalogEvent::Dormant { id, resume_offset, gate } => {
                        if let Some(e) = recs.get_mut(&id) {
                            e.3 = Some((resume_offset, gate));
                        }
                    }
                    CatalogEvent::Reactivated { id } => {
                        if let Some(e) = recs.get_mut(&id) {
                            e.3 = None;
                        }
                    }
                    CatalogEvent::Dropped { id } => {
                        recs.remove(&id);
                    }
                    CatalogEvent::Offset { offset } => start_offset = offset,
                }
            }
            match next {
                Some(n) if !up_to_date && n != off => off = n,
                _ => break,
            }
        }
        if recs.is_empty() && start_offset == "-1" {
            return Ok(());
        }
        tracing::info!("catalog restore: {} shape(s), change-log replay from {start_offset}", recs.len());
        *self.seq_start.lock().unwrap() = start_offset;

        // 2. Restore records + shares; subquery shapes are dropped (see CATALOG_STREAM docs).
        let mut resume: Vec<ShapeRecord> = Vec::new();
        let mut dead_streams: Vec<String> = Vec::new();
        {
            let mut st = self.state.lock().await;
            for (id, (rec, sig, refcount, dormant)) in recs {
                if let Ok(num) = id.trim_start_matches('s').parse::<u64>() {
                    st.next_shape_id = st.next_shape_id.max(num + 1);
                }
                if rec.is_subquery {
                    // Circuit-served subquery shapes need no registry state: they re-register
                    // against the arrangements (seed=false) like any other circuit shape.
                    let circuit_ok = self.arrangements.lock().unwrap().clone().is_some_and(|arr| {
                        compiled.get(&rec.table).is_some_and(|ts| {
                            plan_circuit_shape(rec.where_json.as_ref(), ts, compiled, &arr).is_some()
                        })
                    });
                    if !circuit_ok {
                        tracing::warn!(
                            "restore: dropping subquery shape {id} (inner-node state is not persisted);                          subscribers observe the deleted stream and recreate"
                        );
                        let _ = self.catalog_tx.send(CatalogEvent::Dropped { id: id.clone() });
                        dead_streams.push(rec.stream_path.clone());
                        continue;
                    }
                }
                st.shapes.insert(id.clone(), rec.clone());
                if let Some(sig) = sig {
                    // Restored feeds are live immediately (their streams already hold data).
                    let (ready_tx, ready_rx) = tokio::sync::watch::channel(Some(true));
                    drop(ready_tx); // receivers keep observing Some(true)
                    st.feed_by_sig.insert(sig.clone(), id.clone());
                    st.feed_shares.insert(id.clone(), FeedShare { sig, refcount, ready: ready_rx });
                }
                match dormant {
                    // A dormant shape restores AS dormant: record + stream retained, no routing,
                    // no replay at boot — the first touch reactivates it from its own resume
                    // offset. (Dormancy age restarts at boot; the TTL clock is conservative.)
                    Some((resume_offset, gate)) => {
                        self.lives.lock().unwrap().insert(
                            id.clone(),
                            ShapeLife {
                                last_read: std::time::Instant::now(),
                                state: LifeState::Dormant {
                                    since: std::time::Instant::now(),
                                    resume_offset,
                                    gate,
                                },
                            },
                        );
                    }
                    None => {
                        self.lives.lock().unwrap().insert(id.clone(), ShapeLife::active());
                        resume.push(rec);
                    }
                }
            }
            self.ensure_sequencer(&mut st);
        }
        // Restored dormant shapes still need the TTL/eviction layers running.
        self.ensure_retention_sweeper();
        for path in dead_streams {
            let _ = self.ds.delete_stream(&path).await;
        }

        // 3. Re-register with the sequencer. Plain/routed shapes resume without a backfill and
        // with a passthrough gate (`changes_only = true` path): everything after the restored
        // offset replays, and re-emission across the crash window is idempotent. Aggregates
        // re-seed their fold from a fresh snapshot (fresh gate skips the replayed history).
        let cmd_tx = {
            let st = self.state.lock().await;
            st.sequencer.as_ref().expect("sequencer spawned above").cmd_tx.clone()
        };
        for rec in resume {
            let outcome = self.resume_shape(&cmd_tx, &rec, compiled).await;
            if let Err(e) = outcome {
                tracing::error!("restore: shape {} failed to resume ({e:#}); dropping it", rec.id);
                let mut st = self.state.lock().await;
                st.shapes.remove(&rec.id);
                let _ = self.catalog_tx.send(CatalogEvent::Dropped { id: rec.id.clone() });
                if let Some(share) = st.feed_shares.remove(&rec.id) {
                    st.feed_by_sig.remove(&share.sig);
                }
            }
        }
        Ok(())
    }

    /// Re-register one restored shape with the sequencer (the resume half of `restore_catalog`).
    pub(crate) async fn resume_shape(
        &self,
        cmd_tx: &mpsc::UnboundedSender<SequencerCmd>,
        rec: &ShapeRecord,
        compiled: &HashMap<String, TableSchema>,
    ) -> Result<()> {
        let ts = compiled
            .get(&rec.table)
            .with_context(|| format!("table '{}' no longer exists", rec.table))?;
        let out_cols: Option<Arc<Vec<usize>>> = match &rec.columns {
            Some(names) => {
                let idx: Result<Vec<usize>> = names.iter().map(|n| ts.column_index(n)).collect();
                Some(Arc::new(idx?))
            }
            None => None,
        };
        let num_id: u64 = rec.id.trim_start_matches('s').parse().unwrap_or(0);
        // Circuit-served restore: re-register with the sequencer, seed=false for plain shapes
        // (the stream is already complete up to the resume offset; dynamic groups re-derive
        // from the router snapshot, which the catch-up replay has brought to the same point).
        // Aggregates re-seed from the counts snapshot (their fold is not persisted) — same
        // fresh-value semantics as the legacy aggregate resume.
        if let Some(arr) = self.arrangements.lock().unwrap().clone() {
            match &rec.aggregate {
                Some(a) if matches!(a.func, AggFn::Count) && a.col.is_none() => {
                    if let Some(gcols) = arr.counts_group_cols(&rec.table).map(|g| g.to_vec()) {
                        if let Some(constraints) = plan_circuit_agg(rec.where_json.as_ref(), ts, &gcols) {
                            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
                            cmd_tx
                                .send(SequencerCmd::CreateCircuitAgg {
                                    table: rec.table.clone(),
                                    shape_id: rec.id.clone(),
                                    stream_path: rec.stream_path.clone(),
                                    constraints,
                                    ready: ready_tx,
                                })
                                .map_err(|_| anyhow::anyhow!("sequencer is gone"))?;
                            ready_rx
                                .await
                                .unwrap_or_else(|_| Err("sequencer dropped".to_string()))
                                .map_err(|e| anyhow::anyhow!(e))?;
                            self.state.lock().await.circuit_placement.insert(
                                rec.id.clone(),
                                CircuitPlacement { label: "counts".into(), col: None, counts: true },
                            );
                            return Ok(());
                        }
                    }
                }
                None => {
                    if let Some(plan) = {
                        let st = self.state.lock().await;
                        plan_circuit_shape(rec.where_json.as_ref(), ts, &st.tables, &arr)
                    } {
                        let residual = match plan.residual.as_ref() {
                            Some(r) => Some(Arc::new(CompiledPredicate::compile(r, ts)?)),
                            None => None,
                        };
                        let placement = match &plan.constraint {
                            PlannedConstraint::All => {
                                CircuitPlacement { label: "all".into(), col: None, counts: false }
                            }
                            PlannedConstraint::Static { col, .. } => CircuitPlacement {
                                label: format!("static:{}", ts.columns[*col].0),
                                col: Some(*col),
                                counts: false,
                            },
                            PlannedConstraint::Dynamic { col, .. } => CircuitPlacement {
                                label: format!("dynamic:{}", ts.columns[*col].0),
                                col: Some(*col),
                                counts: false,
                            },
                        };
                        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
                        cmd_tx
                            .send(SequencerCmd::CreateCircuitShape {
                                table: rec.table.clone(),
                                shape_id: rec.id.clone(),
                                num_id,
                                stream_path: rec.stream_path.clone(),
                                constraint: plan.constraint,
                                residual,
                                out_cols: out_cols.clone(),
                                seed: false,
                                ready: ready_tx,
                            })
                            .map_err(|_| anyhow::anyhow!("sequencer is gone"))?;
                        ready_rx
                            .await
                            .unwrap_or_else(|_| Err("sequencer dropped".to_string()))
                            .map_err(|e| anyhow::anyhow!(e))?;
                        self.state.lock().await.circuit_placement.insert(rec.id.clone(), placement);
                        return Ok(());
                    }
                }
                _ => {}
            }
        }
        // Compiled lazily, after the circuit branch: a circuit-served subquery record never
        // needs (and could not build) a registry-free compiled predicate.
        let pred = Arc::new(CompiledPredicate::compile_opt(rec.where_json.as_ref(), ts)?);
        let (kind, changes_only, is_aggregate) = match &rec.aggregate {
            Some(a) => {
                let col = a.col.as_deref().map(|c| ts.column_index(c)).transpose()?;
                (CreateKind::Aggregate { func: a.func, col }, false, true)
            }
            None => (CreateKind::Plain, true, false),
        };
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        cmd_tx
            .send(SequencerCmd::BeginShape {
                table: rec.table.clone(),
                shape_id: rec.id.clone(),
                num_id,
                stream_path: rec.stream_path.clone(),
                pred: pred.clone(),
                out_cols: out_cols.clone(),
                kind,
                ack: ack_tx,
            })
            .map_err(|_| anyhow::anyhow!("sequencer is gone"))?;
        backfill_and_activate(
            &self.ds,
            &self.pg_url,
            cmd_tx,
            ts,
            &rec.table,
            &rec.id,
            &rec.stream_path,
            &pred,
            out_cols.as_ref(),
            changes_only,
            is_aggregate,
            ack_rx,
        )
        .await
        .map_err(|e| anyhow::anyhow!(e))
    }

}
