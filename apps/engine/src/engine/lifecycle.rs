//! Shape lifecycle: creation (all strategies), sharing, and the retention state machine
//! (release/touch/dormancy/eviction/sweep).

use super::*;


impl Engine {
    /// `share`: when true, an identical existing shape (same table, canonical predicate, and columns) is
    /// joined by ref-count instead of creating a second stream — so N app clients subscribing to the same
    /// reference shape (e.g. `project_members WHERE user_id = me`) share one maintained output. The
    /// Electric `/v1/shape` path passes `false`: it keys per-request live state by shape id, so each
    /// request needs its own handle.
    pub async fn create_shape(
        &self,
        table: &str,
        where_: Option<PredicateJson>,
        columns: Option<Vec<String>>,
        changes_only: bool,
        share: bool,
    ) -> Result<ShapeRecord> {
        // Whole shape-creation timer (backfill + registration); emitted by the creator on success only
        // (joiners return early before this fires) as `create_snapshot_task.stop.duration`.
        let created_at = std::time::Instant::now();
        let mut st = self.state.lock().await;
        let ts = match st.tables.get(table) {
            Some(ts) => ts.clone(),
            None => bail!("unknown table '{table}'"),
        };
        let col_names = columns.clone();
        let out_cols = resolve_columns(&ts, columns)?;

        // Shape sharing: an identical shape (subset feed, materialized, OR subquery) that already exists
        // is joined (ref-count++), returning the same stream — no second stream, no per-subscriber append
        // fan-out. Subquery shapes share their inner-set nodes in the registry regardless; sharing the
        // *outer* shape here collapses identical subquery shapes fully.
        let feed_sig = if share { Some(shape_signature(table, &where_, &out_cols, changes_only)) } else { None };
        if let Some(sig) = &feed_sig {
            if let Some(existing_id) = st.feed_by_sig.get(sig).cloned() {
                if let Some(rec) = st.shapes.get(&existing_id).cloned() {
                    let share = st.feed_shares.get_mut(&existing_id).expect("share entry for live feed");
                    share.refcount += 1;
                let _ = self.catalog_tx.send(CatalogEvent::Joined { id: existing_id.clone() });
                    let ready = share.ready.clone();
                    // Release the lock, then wait for the creator's backfill to land: a joiner must not
                    // see a stream whose snapshot isn't readable yet, and must surface (not mask) a
                    // failed creation.
                    drop(st);
                    if let Err(e) = await_share_ready(ready, &existing_id).await {
                        // The failed creator already removed the share entries; undo nothing.
                        return Err(e);
                    }
                    // A rejoin is a touch: if the shape went dormant since the last subscriber
                    // left, reactivate it (change-log replay) before handing out the stream.
                    if let Err(e) = self.ensure_active(&existing_id).await {
                        // Roll the failed join back so the dead subscription doesn't pin the shape.
                        self.release_shape(&existing_id).await;
                        return Err(e);
                    }
                    return Ok(rec);
                }
            }
        }

        let num_id = st.next_shape_id;
        let id = format!("s{num_id}");
        st.next_shape_id += 1;
        let stream_path = format!("shape/{id}");
        // NOTE: the stream itself is created AFTER the state lock is released (per path below):
        // the PUT is a storage round-trip with a durability fsync, and holding the global lock
        // across it serializes concurrent shape creations.


        // Subquery shapes (`col IN (SELECT …)`) are maintained by the cross-table registry, not by a
        // tailer's local routing. Ensure a tailer exists for the outer table AND every referenced inner
        // table (so their deltas reach the registry), then register + backfill via the registry.
        if where_.as_ref().is_some_and(predicate_has_subquery) {
            let where_json = where_.expect("subquery predicate present");
            let mut tables = referenced_tables(&where_json);
            tables.push(table.to_string());
            for t in &tables {
                if !st.tables.contains_key(t) {
                    bail!("unknown table '{t}' referenced by subquery");
                }
            }
            // The sequencer feeds every table's deltas to the registry; just make sure it runs.
            self.ensure_sequencer(&mut st);
            let rec = ShapeRecord {
                id: id.clone(),
                table: table.to_string(),
                stream_path: stream_path.clone(),
                changes_only,
                where_json: Some(where_json.clone()),
                columns: col_names.clone(),
                family_key: None,
                is_subquery: true,
                aggregate: None,
            };
            st.shapes.insert(id.clone(), rec.clone());
            let _ = self.catalog_tx.send(CatalogEvent::Created { rec: rec.clone(), sig: feed_sig.clone() });
            self.lives.lock().unwrap().insert(id.clone(), ShapeLife::active());
            self.ensure_retention_sweeper();
            // Register this (first) subquery shape so later identical ones join it by ref-count.
            // Joiners wait on `ready_tx` — the shape isn't live until the registry has seeded its
            // nodes and backfilled the stream.
            let (ready_tx, ready_rx) = tokio::sync::watch::channel(None);
            if let Some(sig) = feed_sig {
                st.feed_by_sig.insert(sig.clone(), id.clone());
                st.feed_shares.insert(id.clone(), FeedShare { sig, refcount: 1, ready: ready_rx });
            }
            // Release the engine-state lock before the registry work. Creation is three-phase:
            // begin (brief registry lock: nodes/edges/pending buffer registered) → Postgres
            // seeding + backfill with NO lock held (concurrent creates parallelize on the
            // shared pool) → finish (brief lock: install seeds, gated replay of buffered
            // deltas, register the shape). Replay flips propagate through the worker pool.
            drop(st);
            let res = async {
                self.ds.ensure_stream(&stream_path).await?;
                self.create_subquery_three_phase(&id, table, &stream_path, &where_json, out_cols, changes_only)
                    .await
            }
            .await;
            match res {
                Ok(()) => {
                    let _ = ready_tx.send(Some(true));
                    trace_lifecycle(
                        &self.trace_tx,
                        crate::trace::GraphLifecycle::ShapeAdded { shape: id, table: table.to_string() },
                    );
                    crate::statsd::create_snapshot_task(created_at.elapsed());
                    return Ok(rec);
                }
                Err(e) => {
                    // Registration failed (the registry rolled its own state back). Remove the shape
                    // record + share entries so later identical creates don't join a dead stream, and
                    // wake any joiners with the failure.
                    let mut st = self.state.lock().await;
                    st.shapes.remove(&id);
                    let _ = self.catalog_tx.send(CatalogEvent::Dropped { id: id.clone() });
                    if let Some(share) = st.feed_shares.remove(&id) {
                        st.feed_by_sig.remove(&share.sig);
                    }
                    drop(st);
                    let _ = ready_tx.send(Some(false));
                    let _ = self.ds.delete_stream(&stream_path).await;
                    return Err(e);
                }
            }
        }

        let pred = Arc::new(CompiledPredicate::compile_opt(where_.as_ref(), &ts)?);
        // Family placement (for graph introspection): an equality template routes by these key columns
        // via a shared family; otherwise it's a standalone filter.
        let family_key = pred
            .equality_template()
            .map(|pairs| pairs.iter().map(|(i, _)| ts.columns[*i].0.clone()).collect::<Vec<_>>());

        let cmd_tx = self.ensure_sequencer(&mut st).cmd_tx.clone();
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        cmd_tx
            .send(SequencerCmd::BeginShape {
                table: table.to_string(),
                shape_id: id.clone(),
                num_id,
                stream_path: stream_path.clone(),
                pred: pred.clone(),
                out_cols: out_cols.clone(),
                kind: CreateKind::Plain,
                ack: ack_tx,
            })
            .map_err(|_| anyhow::anyhow!("sequencer is gone"))?;

        let rec = ShapeRecord {
            id: id.clone(),
            table: table.to_string(),
            stream_path,
            changes_only,
            where_json: where_.clone(),
            columns: col_names,
            family_key,
            is_subquery: false,
            aggregate: None,
        };
        st.shapes.insert(id.clone(), rec.clone());
        let _ = self.catalog_tx.send(CatalogEvent::Created { rec: rec.clone(), sig: feed_sig.clone() });
        self.lives.lock().unwrap().insert(id.clone(), ShapeLife::active());
        self.ensure_retention_sweeper();
        // Register the (first) shared feed so later identical subset feeds join it. Joiners wait on
        // `share_tx` for the backfill outcome.
        let (share_tx, share_rx) = tokio::sync::watch::channel(None);
        if let Some(sig) = feed_sig {
            st.feed_by_sig.insert(sig.clone(), id.clone());
            st.feed_shares.insert(id.clone(), FeedShare { sig, refcount: 1, ready: share_rx });
        }
        // Release the engine-state lock, then run the two-phase backfill+activate so the shape's
        // snapshot is readable when we return (the Electric adapter folds the stream immediately).
        // The sequencer keeps processing all tables meanwhile, buffering this shape's deltas.
        drop(st);
        let outcome = match self.ds.ensure_stream(&rec.stream_path).await {
            Err(e) => Err(format!("creating shape stream: {e:#}")),
            Ok(()) => backfill_and_activate(
            &self.ds, &self.pg_url, &cmd_tx, &ts, table, &id, &rec.stream_path, &pred,
            out_cols.as_ref(), changes_only, false, ack_rx,
        )
        .await,
        };
        match outcome {
            Ok(()) => {
                let _ = share_tx.send(Some(true));
                trace_lifecycle(
                    &self.trace_tx,
                    crate::trace::GraphLifecycle::ShapeAdded { shape: rec.id.clone(), table: rec.table.clone() },
                );
                crate::statsd::create_snapshot_task(created_at.elapsed());
                Ok(rec)
            }
            Err(e) => {
                // Backfill/registration failed: remove the record + share entries (no zombie shape a
                // later identical create would join) and surface the error to the caller.
                let mut st = self.state.lock().await;
                st.shapes.remove(&id);
                let _ = self.catalog_tx.send(CatalogEvent::Dropped { id: id.clone() });
                if let Some(share) = st.feed_shares.remove(&id) {
                    st.feed_by_sig.remove(&share.sig);
                }
                if let Some(seq) = st.sequencer.as_ref() {
                    let _ = seq
                        .cmd_tx
                        .send(SequencerCmd::RemoveShape { table: rec.table.clone(), shape_id: id.clone() });
                }
                drop(st);
                let _ = share_tx.send(Some(false));
                let _ = self.ds.delete_stream(&rec.stream_path).await;
                bail!("shape '{id}' creation failed: {e}")
            }
        }
    }

    /// Create a scalar **aggregation** shape (COUNT/SUM/AVG/MIN/MAX over `where`), maintained
    /// incrementally. An electric-ivm extension — not part of the Electric-compatible API. Rejects
    /// subquery predicates (use a plain filter); SUM/AVG/MIN/MAX require a column.
    pub async fn create_aggregate(
        &self,
        table: &str,
        where_: Option<PredicateJson>,
        func: AggFn,
        col: Option<String>,
    ) -> Result<ShapeRecord> {
        let mut st = self.state.lock().await;
        let ts = st.tables.get(table).cloned().ok_or_else(|| anyhow::anyhow!("unknown table '{table}'"))?;
        if where_.as_ref().is_some_and(predicate_has_subquery) {
            bail!("aggregations over subquery predicates are not supported");
        }
        let col_idx = match &col {
            Some(c) => Some(ts.column_index(c)?),
            None => None,
        };
        if matches!(func, AggFn::Sum | AggFn::Avg | AggFn::Min | AggFn::Max) && col_idx.is_none() {
            bail!("aggregation {func:?} requires a column");
        }

        // Aggregate sharing: an identical aggregation (same table, predicate, function, column) is joined
        // by ref-count — one maintained fold feeds every subscriber (e.g. the same live COUNT opened by
        // many clients).
        let agg_sig = agg_signature(table, &where_, &func, col_idx);
        if let Some(existing_id) = st.feed_by_sig.get(&agg_sig).cloned() {
            if let Some(rec) = st.shapes.get(&existing_id).cloned() {
                let share = st.feed_shares.get_mut(&existing_id).expect("share entry for aggregate");
                share.refcount += 1;
                let _ = self.catalog_tx.send(CatalogEvent::Joined { id: existing_id.clone() });
                let ready = share.ready.clone();
                drop(st);
                await_share_ready(ready, &existing_id).await?;
                self.touch_shape(&existing_id); // aggregates never park, but the read is a touch
                return Ok(rec);
            }
        }

        let pred = Arc::new(CompiledPredicate::compile_opt(where_.as_ref(), &ts)?);

        let num_id = st.next_shape_id;
        let id = format!("s{num_id}");
        st.next_shape_id += 1;
        let stream_path = format!("shape/{id}");
        self.ds.ensure_stream(&stream_path).await?;

        // Circuit-served path: a bare COUNT whose predicate decomposes over the table's counts
        // pipeline is seeded by summing groups and updated from group deltas — no Postgres.
        if matches!(func, AggFn::Count) && col_idx.is_none() {
            let arr = self.arrangements.lock().unwrap().clone();
            if let Some(arr) = arr {
                if let Some(gcols) = arr.counts_group_cols(table).map(|g| g.to_vec()) {
                    if let Some(constraints) = plan_circuit_agg(where_.as_ref(), &ts, &gcols) {
                        let cmd_tx = self.ensure_sequencer(&mut st).cmd_tx.clone();
                        let (ready_tx2, ready_rx2) = tokio::sync::oneshot::channel();
                        cmd_tx
                            .send(SequencerCmd::CreateCircuitAgg {
                                table: table.to_string(),
                                shape_id: id.clone(),
                                stream_path: stream_path.clone(),
                                constraints,
                                ready: ready_tx2,
                            })
                            .map_err(|_| anyhow::anyhow!("sequencer is gone"))?;
                        let rec = ShapeRecord {
                            id: id.clone(),
                            table: table.to_string(),
                            stream_path: stream_path.clone(),
                            changes_only: false,
                            where_json: where_,
                            columns: None,
                            family_key: None,
                            is_subquery: false,
                            aggregate: Some(AggInfo { func, col }),
                        };
                        st.shapes.insert(id.clone(), rec.clone());
                        st.circuit_placement.insert(
                            id.clone(),
                            CircuitPlacement { label: "counts".into(), col: None, counts: true },
                        );
                        let _ = self
                            .catalog_tx
                            .send(CatalogEvent::Created { rec: rec.clone(), sig: Some(agg_sig.clone()) });
                        self.lives.lock().unwrap().insert(id.clone(), ShapeLife::active());
                        self.ensure_retention_sweeper();
                        let (share_tx, share_rx) = tokio::sync::watch::channel(None);
                        st.feed_by_sig.insert(agg_sig.clone(), id.clone());
                        st.feed_shares.insert(id.clone(), FeedShare { sig: agg_sig, refcount: 1, ready: share_rx });
                        drop(st);
                        return match ready_rx2
                            .await
                            .unwrap_or_else(|_| Err("sequencer dropped the ready channel".to_string()))
                        {
                            Ok(()) => {
                                let _ = share_tx.send(Some(true));
                                trace_lifecycle(
                                    &self.trace_tx,
                                    crate::trace::GraphLifecycle::ShapeAdded {
                                        shape: rec.id.clone(),
                                        table: rec.table.clone(),
                                    },
                                );
                                Ok(rec)
                            }
                            Err(e) => {
                                let mut st = self.state.lock().await;
                                st.shapes.remove(&id);
                                st.circuit_placement.remove(&id);
                                let _ = self.catalog_tx.send(CatalogEvent::Dropped { id: id.clone() });
                                if let Some(share) = st.feed_shares.remove(&id) {
                                    st.feed_by_sig.remove(&share.sig);
                                }
                                if let Some(seq) = st.sequencer.as_ref() {
                                    let _ = seq.cmd_tx.send(SequencerCmd::RemoveShape {
                                        table: rec.table.clone(),
                                        shape_id: id.clone(),
                                    });
                                }
                                drop(st);
                                let _ = share_tx.send(Some(false));
                                let _ = self.ds.delete_stream(&rec.stream_path).await;
                                bail!("aggregate '{id}' creation failed: {e}")
                            }
                        };
                    }
                }
            }
        }

        let cmd_tx = self.ensure_sequencer(&mut st).cmd_tx.clone();
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        cmd_tx
            .send(SequencerCmd::BeginShape {
                table: table.to_string(),
                shape_id: id.clone(),
                num_id,
                stream_path: stream_path.clone(),
                pred: pred.clone(),
                out_cols: None,
                kind: CreateKind::Aggregate { func, col: col_idx },
                ack: ack_tx,
            })
            .map_err(|_| anyhow::anyhow!("sequencer is gone"))?;

        let stream_path_c = stream_path.clone();
        let rec = ShapeRecord {
            id: id.clone(),
            table: table.to_string(),
            stream_path,
            changes_only: false,
            where_json: where_,
            columns: None,
            family_key: None,
            is_subquery: false,
            aggregate: Some(AggInfo { func, col }),
        };
        st.shapes.insert(id.clone(), rec.clone());
        let _ = self.catalog_tx.send(CatalogEvent::Created { rec: rec.clone(), sig: Some(agg_sig.clone()) });
        self.lives.lock().unwrap().insert(id.clone(), ShapeLife::active());
        self.ensure_retention_sweeper();
        // Register this (first) aggregate so later identical ones join it by ref-count.
        let (share_tx, share_rx) = tokio::sync::watch::channel(None);
        st.feed_by_sig.insert(agg_sig.clone(), id.clone());
        st.feed_shares.insert(id.clone(), FeedShare { sig: agg_sig, refcount: 1, ready: share_rx });
        drop(st);
        let outcome = backfill_and_activate(
            &self.ds, &self.pg_url, &cmd_tx, &ts, table, &id, &stream_path_c, &pred,
            None, false, true, ack_rx,
        )
        .await;
        match outcome {
            Ok(()) => {
                let _ = share_tx.send(Some(true));
                trace_lifecycle(
                    &self.trace_tx,
                    crate::trace::GraphLifecycle::ShapeAdded { shape: rec.id.clone(), table: rec.table.clone() },
                );
                Ok(rec)
            }
            Err(e) => {
                let mut st = self.state.lock().await;
                st.shapes.remove(&id);
                let _ = self.catalog_tx.send(CatalogEvent::Dropped { id: id.clone() });
                if let Some(share) = st.feed_shares.remove(&id) {
                    st.feed_by_sig.remove(&share.sig);
                }
                if let Some(seq) = st.sequencer.as_ref() {
                    let _ = seq
                        .cmd_tx
                        .send(SequencerCmd::RemoveShape { table: rec.table.clone(), shape_id: id.clone() });
                }
                drop(st);
                let _ = share_tx.send(Some(false));
                let _ = self.ds.delete_stream(&rec.stream_path).await;
                bail!("aggregate '{id}' creation failed: {e}")
            }
        }
    }

    /// Release one subscription on a shape (extended-API `DELETE /shapes/{id}`, `/v1/shape` handle
    /// eviction). Refcount-0 does **not** tear the shape down: it stays active (a brief reconnect
    /// rejoins it warm), goes dormant after the retention idle timeout, and is eventually evicted
    /// by the layered policy (see `crate::retention`). Releasing is also a touch, so the idle
    /// countdown starts at the disconnect. Infallible: it only adjusts in-memory counters.
    pub async fn release_shape(&self, id: &str) {
        let mut st = self.state.lock().await;
        if let Some(share) = st.feed_shares.get_mut(id) {
            share.refcount = share.refcount.saturating_sub(1);
            let _ = self.catalog_tx.send(CatalogEvent::Left { id: id.to_string() });
        }
        drop(st);
        self.touch_shape(id);
    }

    /// Force-drop a shape NOW, bypassing the retention lifecycle: full teardown (record, share
    /// entries, lifecycle entry, sequencer routing, subquery-registry entry, durable stream)
    /// regardless of refcount or lifecycle state. An admin/debug operation (`DELETE
    /// /shapes/{id}?purge=true`, the visualizer's trash button) — subscribed clients see their
    /// stream vanish and recreate via the normal 404 / must-refetch path. The sequencer command
    /// queue is FIFO, so a purge ordered after an in-flight resume removes whatever the resume
    /// registered.
    pub async fn purge_shape(&self, id: &str) -> Result<()> {
        let mut st = self.state.lock().await;
        self.lives.lock().unwrap().remove(id);
        if let Some(share) = st.feed_shares.remove(id) {
            st.feed_by_sig.remove(&share.sig);
        }
        let removed = st.shapes.remove(id);
        st.circuit_placement.remove(id);
        if removed.is_some() {
            let _ = self.catalog_tx.send(CatalogEvent::Dropped { id: id.to_string() });
        }
        if let Some(rec) = &removed {
            if let Some(seq) = st.sequencer.as_ref() {
                let _ = seq
                    .cmd_tx
                    .send(SequencerCmd::RemoveShape { table: rec.table.clone(), shape_id: id.to_string() });
            }
        }
        drop(st);
        // Subquery shapes live in the registry (a no-op for plain shapes).
        self.subqueries.lock().await.drop_subquery_shape(id);
        if let Some(rec) = removed {
            if let Err(e) = self.ds.delete_stream(&rec.stream_path).await {
                tracing::warn!("failed to delete stream {} for purged shape {id}: {e:#}", rec.stream_path);
            }
            trace_lifecycle(&self.trace_tx, crate::trace::GraphLifecycle::ShapeDropped { shape: id.to_string() });
            tracing::info!("purged shape {id} (forced)");
        }
        Ok(())
    }

    /// Record an engine-visible read of a shape (drives the retention idle timer + LRU order).
    pub(crate) fn touch_shape(&self, id: &str) {
        if let Some(life) = self.lives.lock().unwrap().get_mut(id) {
            life.last_read = std::time::Instant::now();
        }
    }

    /// The shape's retention lifecycle, for introspection (`GET /shapes/{id}`).
    pub async fn shape_lifecycle(&self, id: &str) -> Option<&'static str> {
        self.lives.lock().unwrap().get(id).map(|l| match l.state {
            LifeState::Active => "active",
            LifeState::Deactivating { .. } => "deactivating",
            LifeState::Dormant { .. } => "dormant",
            LifeState::Reactivating { .. } => "reactivating",
        })
    }

    /// Make sure a shape is active, reactivating it from dormancy if needed ("any touch
    /// reactivates"): replay the change log from the shape's resume offset through its predicate
    /// onto the retained stream — no Postgres backfill — then re-register it for live routing.
    /// Concurrent touches coalesce onto one replay; a touch during deactivation waits for the
    /// transition to settle first. Also refreshes `last_read`.
    pub async fn ensure_active(&self, id: &str) -> Result<()> {
        loop {
            enum Step {
                Done,
                WaitDeactivate(tokio::sync::watch::Receiver<bool>),
                WaitReactivate(tokio::sync::watch::Receiver<Option<bool>>),
            }
            let step = {
                let mut lives = self.lives.lock().unwrap();
                match lives.get_mut(id) {
                    // Unknown to retention (already evicted, or never tracked): nothing to do here —
                    // the caller's own record lookup decides between 404 and normal service.
                    None => Step::Done,
                    Some(life) => {
                        life.last_read = std::time::Instant::now();
                        match &life.state {
                            LifeState::Active => Step::Done,
                            LifeState::Deactivating { done } => Step::WaitDeactivate(done.clone()),
                            LifeState::Reactivating { done } => Step::WaitReactivate(done.clone()),
                            LifeState::Dormant { resume_offset, gate, .. } => {
                                // Kick off the replay in a DETACHED task: `ensure_active` futures
                                // are dropped when an HTTP client disconnects, and a cancelled
                                // in-place replay would strand the shape in `Reactivating`. The
                                // task always settles the lifecycle state and publishes the
                                // outcome; this caller then awaits THIS attempt's channel like any
                                // concurrent toucher.
                                let resume_offset = resume_offset.clone();
                                let gate = gate.clone();
                                let (tx, rx) = tokio::sync::watch::channel(None);
                                life.state = LifeState::Reactivating { done: rx.clone() };
                                let engine = self.clone();
                                let id = id.to_string();
                                tokio::spawn(async move {
                                    let res = engine.resume_dormant(&id, resume_offset.clone(), gate.clone()).await;
                                    let mut lives = engine.lives.lock().unwrap();
                                    match res {
                                        Ok(()) => {
                                            if let Some(life) = lives.get_mut(&id) {
                                                life.state = LifeState::Active;
                                                life.last_read = std::time::Instant::now();
                                            }
                                            let _ = tx.send(Some(true));
                                        }
                                        Err(e) => {
                                            tracing::warn!("reactivating shape {id} failed: {e:#}");
                                            // Restore the dormant resume state so a later touch retries.
                                            if let Some(life) = lives.get_mut(&id) {
                                                life.state = LifeState::Dormant {
                                                    since: std::time::Instant::now(),
                                                    resume_offset,
                                                    gate,
                                                };
                                            }
                                            let _ = tx.send(Some(false));
                                        }
                                    }
                                });
                                Step::WaitReactivate(rx)
                            }
                        }
                    }
                }
            };
            match step {
                Step::Done => return Ok(()),
                Step::WaitDeactivate(mut rx) => {
                    // Deactivation in flight: wait for it to settle, then loop (we'll see Dormant).
                    while !*rx.borrow_and_update() {
                        if rx.changed().await.is_err() {
                            break; // deactivator vanished; re-inspect the state
                        }
                    }
                }
                Step::WaitReactivate(mut rx) => loop {
                    let outcome = *rx.borrow_and_update();
                    match outcome {
                        Some(true) => return Ok(()),
                        Some(false) => bail!("shape '{id}' reactivation failed; retry the read"),
                        None => {
                            if rx.changed().await.is_err() {
                                bail!("shape '{id}' reactivator died; retry the read");
                            }
                        }
                    }
                },
            }
        }
    }

    /// The replay half of a reactivation: re-register the shape through the sequencer's two-phase
    /// pending-buffer handshake, but replay the change log from the dormant resume offset instead
    /// of taking a Postgres snapshot. Live deltas arriving during the replay buffer in the pending
    /// shape and drain through the same gate at activation; any overlap between the replay and the
    /// buffer double-applies only absolute per-pk upserts/deletes — idempotent for stream readers.
    /// Split from [`ensure_active`] so the lifecycle bookkeeping stays in one place.
    pub(crate) async fn resume_dormant(&self, id: &str, resume_offset: String, gate: crate::pg::SnapshotGate) -> Result<()> {
        let (rec, ts, pred, out_cols, num_id, cmd_tx) = {
            let mut st = self.state.lock().await;
            let rec =
                st.shapes.get(id).cloned().with_context(|| format!("shape '{id}' vanished during reactivation"))?;
            let ts =
                st.tables.get(&rec.table).cloned().with_context(|| format!("unknown table '{}'", rec.table))?;
            let pred = Arc::new(CompiledPredicate::compile_opt(rec.where_json.as_ref(), &ts)?);
            let out_cols = resolve_columns(&ts, rec.columns.clone())?;
            let num_id: u64 =
                id.strip_prefix('s').and_then(|n| n.parse().ok()).context("unparseable shape id")?;
            let cmd_tx = self.ensure_sequencer(&mut st).cmd_tx.clone();
            (rec, ts, pred, out_cols, num_id, cmd_tx)
        };
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        cmd_tx
            .send(SequencerCmd::BeginShape {
                table: rec.table.clone(),
                shape_id: id.to_string(),
                num_id,
                stream_path: rec.stream_path.clone(),
                pred: pred.clone(),
                out_cols: out_cols.clone(),
                kind: CreateKind::Plain,
                ack: ack_tx,
            })
            .map_err(|_| anyhow::anyhow!("sequencer is gone"))?;
        ack_rx.await.map_err(|_| anyhow::anyhow!("sequencer dropped the begin-shape ack"))?;
        // Replay everything the retained stream is missing (buffering live deltas meanwhile).
        let emitted = match replay_changes_for_shape(
            &self.ds,
            &ts,
            &rec.table,
            &pred,
            out_cols.as_ref(),
            &gate,
            &rec.stream_path,
            &resume_offset,
        )
        .await
        {
            Ok(n) => n,
            Err(e) => {
                let _ = cmd_tx
                    .send(SequencerCmd::AbortShape { table: rec.table.clone(), shape_id: id.to_string() });
                return Err(e.context(format!("shape '{id}' reactivation replay failed")));
            }
        };
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        cmd_tx
            .send(SequencerCmd::ActivateShape {
                table: rec.table.clone(),
                shape_id: id.to_string(),
                gate,
                agg_seed: Vec::new(),
                emitted_seed: emitted,
                ready: ready_tx,
            })
            .map_err(|_| anyhow::anyhow!("sequencer is gone"))?;
        ready_rx
            .await
            .unwrap_or_else(|_| Err("sequencer dropped the ready channel".to_string()))
            .map_err(|e| anyhow::anyhow!("shape '{id}' reactivation failed: {e}"))?;
        let _ = self.catalog_tx.send(CatalogEvent::Reactivated { id: id.to_string() });
        metrics().shapes_reactivated.fetch_add(1, Ordering::Relaxed);
        trace_lifecycle(
            &self.trace_tx,
            crate::trace::GraphLifecycle::ShapeReactivated { shape: id.to_string(), table: rec.table.clone() },
        );
        tracing::info!("reactivated dormant shape {id} (table {})", rec.table);
        Ok(())
    }

    /// Move an idle refcount-0 shape from active to dormant: the sequencer unregisters its
    /// routing and hands back the resume state (fully-processed change-log offset + the shape's
    /// snapshot gate); the stream and record are retained. Rechecks eligibility under the locks —
    /// a touch or rejoin racing the sweep wins.
    pub(crate) async fn deactivate_shape(&self, id: &str) -> Result<()> {
        let st = self.state.lock().await;
        let Some(rec) = st.shapes.get(id).cloned() else { return Ok(()) }; // already gone
        if rec.is_subquery || rec.aggregate.is_some() {
            return Ok(()); // never dormant (state not rebuildable from a bounded replay)
        }
        if st.feed_shares.get(id).is_some_and(|s| s.refcount > 0) {
            return Ok(()); // resubscribed since the sweep snapshot
        }
        let Some(cmd_tx) = st.sequencer.as_ref().map(|s| s.cmd_tx.clone()) else { return Ok(()) };
        let (done_tx, done_rx) = tokio::sync::watch::channel(false);
        {
            let mut lives = self.lives.lock().unwrap();
            let Some(life) = lives.get_mut(id) else { return Ok(()) };
            if !matches!(life.state, LifeState::Active)
                || life.last_read.elapsed() < self.retention.idle_timeout
            {
                return Ok(()); // touched or already transitioning since the sweep snapshot
            }
            life.state = LifeState::Deactivating { done: done_rx };
        }
        drop(st);

        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        let sent = cmd_tx
            .send(SequencerCmd::DeactivateShape { table: rec.table.clone(), shape_id: id.to_string(), resp: resp_tx })
            .is_ok();
        let resume = if sent { resp_rx.await.ok().flatten() } else { None };
        let mut lives = self.lives.lock().unwrap();
        let Some(life) = lives.get_mut(id) else { return Ok(()) };
        match resume {
            Some((resume_offset, gate)) => {
                life.state = LifeState::Dormant {
                    since: std::time::Instant::now(),
                    resume_offset: resume_offset.clone(),
                    gate: gate.clone(),
                };
                drop(lives);
                let _ = self.catalog_tx.send(CatalogEvent::Dormant { id: id.to_string(), resume_offset, gate });
                metrics().shapes_dormanted.fetch_add(1, Ordering::Relaxed);
                trace_lifecycle(&self.trace_tx, crate::trace::GraphLifecycle::ShapeDormant { shape: id.to_string() });
                tracing::debug!("shape {id} went dormant (idle)");
            }
            None => {
                // The sequencer didn't know the shape (or is gone): leave it active. Reset the
                // idle clock so the sweep backs off a full idle window instead of re-attempting
                // (and re-warning) every sweep.
                life.state = LifeState::Active;
                life.last_read = std::time::Instant::now();
                drop(lives);
                tracing::warn!("deactivating shape {id}: sequencer returned no resume state; left active");
            }
        }
        let _ = done_tx.send(true);
        Ok(())
    }

    /// Evict a shape: delete its record, share entries, lifecycle entry, and durable stream. A
    /// returning `/v1/shape` client gets `409 must-refetch`; an extended-API client gets `404` and
    /// recreates. Normally only **dormant** shapes are evicted; the exception is non-parkable
    /// shapes (subquery / aggregate — see [`crate::retention`]), which the TTL layer evicts
    /// straight from active with a full teardown. Rechecks eligibility under the locks — a
    /// reactivation or rejoin racing the sweep wins.
    pub(crate) async fn evict_shape(&self, id: &str, reason: EvictReason) -> Result<()> {
        let mut st = self.state.lock().await;
        let Some(rec) = st.shapes.get(id).cloned() else { return Ok(()) };
        let parkable = !rec.is_subquery && rec.aggregate.is_none();
        {
            let mut lives = self.lives.lock().unwrap();
            let evictable = match lives.get(id) {
                Some(life) if matches!(life.state, LifeState::Dormant { .. }) => true,
                // A non-parkable shape is evicted from active only if it is still idle past the
                // full grace window (a touch since the sweep snapshot wins).
                Some(life) if !parkable && matches!(life.state, LifeState::Active) => {
                    life.last_read.elapsed() >= self.retention.idle_timeout + self.retention.dormant_ttl
                }
                _ => false, // transitioning (or already evicted) since the sweep snapshot
            };
            if !evictable {
                return Ok(());
            }
            if st.feed_shares.get(id).is_some_and(|s| s.refcount > 0) {
                return Ok(());
            }
            lives.remove(id);
        }
        if let Some(share) = st.feed_shares.remove(id) {
            st.feed_by_sig.remove(&share.sig);
        }
        let removed = st.shapes.remove(id);
        st.circuit_placement.remove(id);
        if removed.is_some() {
            let _ = self.catalog_tx.send(CatalogEvent::Dropped { id: id.to_string() });
        }
        // A dormant shape is already unregistered from the sequencer; a non-parkable one is still
        // live and needs the full teardown (sequencer routing for aggregates, registry for subqueries).
        if !parkable {
            if let Some(seq) = st.sequencer.as_ref() {
                let _ = seq
                    .cmd_tx
                    .send(SequencerCmd::RemoveShape { table: rec.table.clone(), shape_id: id.to_string() });
            }
        }
        drop(st);
        if !parkable {
            self.subqueries.lock().await.drop_subquery_shape(id);
        }
        if let Some(rec) = removed {
            if let Err(e) = self.ds.delete_stream(&rec.stream_path).await {
                tracing::warn!("failed to delete stream {} for evicted shape {id}: {e:#}", rec.stream_path);
            }
            metrics().shapes_evicted.fetch_add(1, Ordering::Relaxed);
            trace_lifecycle(&self.trace_tx, crate::trace::GraphLifecycle::ShapeDropped { shape: id.to_string() });
            tracing::info!("evicted shape {id} ({})", reason.as_str());
        }
        Ok(())
    }

    /// One retention sweep: snapshot every shape's status, run the pure layered policy
    /// ([`crate::retention::plan_sweep`]), then execute the plan. Public so a harness can force a
    /// sweep instead of waiting for the background interval.
    pub async fn retention_sweep(&self) {
        let cfg = self.retention.clone();
        let snapshot: Vec<SweepShape> = {
            let st = self.state.lock().await;
            let bytes = self.ds.appended_bytes_with_prefix("shape/");
            let lives = self.lives.lock().unwrap();
            st.shapes
                .values()
                .map(|rec| {
                    let life = lives.get(&rec.id);
                    let (idle, dormant_for, in_transition) = match life {
                        None => (std::time::Duration::ZERO, None, true), // mid-create; leave alone
                        Some(l) => match &l.state {
                            LifeState::Active => (l.last_read.elapsed(), None, false),
                            LifeState::Dormant { since, .. } => (l.last_read.elapsed(), Some(since.elapsed()), false),
                            LifeState::Deactivating { .. } | LifeState::Reactivating { .. } => {
                                (l.last_read.elapsed(), None, true)
                            }
                        },
                    };
                    SweepShape {
                        id: rec.id.clone(),
                        refcount: st.feed_shares.get(&rec.id).map(|s| s.refcount).unwrap_or(0),
                        idle,
                        dormant_for,
                        in_transition,
                        dormancy_eligible: !rec.is_subquery && rec.aggregate.is_none(),
                        stream_bytes: bytes.get(&rec.stream_path).copied().unwrap_or(0),
                    }
                })
                .collect()
        };
        let plan = crate::retention::plan_sweep(&cfg, &snapshot);
        if plan.over_capacity {
            metrics().retention_pressure.fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                "retention: {} shapes exceed max_shapes={} but nothing dormant is left to evict — \
                 every shape is actively subscribed or recently read; raise ELECTRIC_IVM_MAX_SHAPES or lower the idle timeout",
                snapshot.len(),
                cfg.max_shapes
            );
        }
        if plan.over_budget {
            metrics().retention_pressure.fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                "retention: shape streams exceed the disk budget ({} bytes) but nothing dormant is left to evict — \
                 raise ELECTRIC_IVM_SHAPE_DISK_BUDGET_MB or lower the idle timeout",
                cfg.disk_budget_bytes
            );
        }
        for id in &plan.deactivate {
            if let Err(e) = self.deactivate_shape(id).await {
                tracing::warn!("retention: deactivating shape {id} failed: {e:#}");
            }
        }
        for (id, reason) in &plan.evict {
            if let Err(e) = self.evict_shape(id, *reason).await {
                tracing::warn!("retention: evicting shape {id} failed: {e:#}");
            }
        }
    }

    /// Spawn (once) the background retention sweeper. Started lazily from the shape-create paths
    /// (and after a catalog restore) so library users that never create shapes never run it.
    pub(crate) fn ensure_retention_sweeper(&self) {
        if self.retention_started.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        let engine = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(engine.retention.sweep_interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await; // the first tick fires immediately; skip it
            loop {
                tick.tick().await;
                engine.retention_sweep().await;
            }
        });
    }

}

impl Engine {
    /// Orchestrate the registry's three-phase subquery-shape creation (see
    /// `SubqueryRegistry::begin_create`): the Postgres seeding queries and the outer backfill
    /// run WITHOUT the registry lock, so concurrent creates parallelize on the shared pool
    /// (`ELECTRIC_DB_POOL_SIZE`) instead of serializing behind one create's round-trips.
    /// A begin-conflict (sharing a node another create is still seeding) retries briefly.
    async fn create_subquery_three_phase(
        &self,
        id: &str,
        table: &str,
        stream_path: &str,
        where_json: &PredicateJson,
        out_cols: Option<Arc<Vec<usize>>>,
        changes_only: bool,
    ) -> Result<()> {
        // Phase A (brief lock), with conflict retry.
        let begin = {
            let mut attempt = 0u32;
            loop {
                let res = self.subqueries.lock().await.begin_create(
                    id, table, stream_path, where_json, out_cols.clone(), changes_only,
                );
                match res {
                    Ok(b) => break b,
                    Err(e) if e.to_string().contains("subquery create conflict") && attempt < 100 => {
                        attempt += 1;
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    }
                    Err(e) => return Err(e),
                }
            }
        };
        // Phase B (no registry lock): seed fresh nodes + backfill the shape, all from pooled PG.
        let phase_b = async {
            let mut node_seeds = Vec::with_capacity(begin.seeds.len());
            for (sig, inner_table, inner_where) in &begin.seeds {
                let ts = begin
                    .schemas
                    .get(inner_table)
                    .cloned()
                    .with_context(|| format!("seed: unknown inner table '{inner_table}'"))?;
                let wsql = inner_where
                    .as_ref()
                    .map(|w| crate::sql::predicate_json_to_sql(w, 1, &begin.schemas, inner_table));
                let client = crate::pg::pool_for(
                    self.pg_url.as_deref().context("subquery work requires postgres")?,
                )
                .get()
                .await?;
                let bf = crate::pg::backfill_where(&client, &ts, wsql).await?;
                node_seeds.push((sig.clone(), bf.rows, bf.gate));
            }
            let outer_ts = begin
                .schemas
                .get(table)
                .cloned()
                .with_context(|| format!("unknown outer table '{table}'"))?;
            let (outer_gate, seeded, seeded_pks) = if changes_only {
                (crate::pg::SnapshotGate::passthrough(), 0u64, HashSet::new())
            } else {
                let (wsql, params) =
                    crate::sql::predicate_json_to_sql(where_json, 1, &begin.schemas, table);
                let client = crate::pg::pool_for(
                    self.pg_url.as_deref().context("subquery work requires postgres")?,
                )
                .get()
                .await?;
                let bf = crate::pg::backfill_where(&client, &outer_ts, Some((wsql, params))).await?;
                let seeded_pks: HashSet<String> =
                    bf.rows.iter().map(|r| outer_ts.key_string(r).unwrap_or_default()).collect();
                let out: Vec<(Row, ZWeight)> = bf.rows.iter().map(|r| (r.clone(), 1)).collect();
                let mut seeded = 0u64;
                if !out.is_empty() {
                    let envs = translate_output(
                        &outer_ts,
                        out,
                        None,
                        None,
                        out_cols.as_deref().map(Vec::as_slice),
                    );
                    self.ds.append(stream_path, &envs).await?;
                    seeded = envs.len() as u64;
                }
                (bf.gate, seeded, seeded_pks)
            };
            Ok::<_, anyhow::Error>((node_seeds, outer_gate, seeded, seeded_pks))
        }
        .await;
        // Phase C (brief lock): install + gated replay, or exact rollback on a phase-B failure.
        match phase_b {
            Ok((node_seeds, outer_gate, seeded, seeded_pks)) => {
                let work = self
                    .subqueries
                    .lock()
                    .await
                    .finish_create(id, node_seeds, outer_gate, seeded, seeded_pks)
                    .await?;
                if !work.is_empty() {
                    // Replay flips propagate exactly like live ones (barrier-covered).
                    self.pending_flips.fetch_add(1, Ordering::SeqCst);
                    if self.flip_tx.send(FlipWork { work, txid: None, lsn: None }).is_err() {
                        self.pending_flips.fetch_sub(1, Ordering::SeqCst);
                    }
                }
                Ok(())
            }
            Err(e) => {
                self.subqueries.lock().await.abort_create(id);
                Err(e)
            }
        }
    }
}
