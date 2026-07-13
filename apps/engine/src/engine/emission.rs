//! Ordered emission lanes for subquery-shape appends.
//!
//! The registry's correctness rule is that, per shape stream, **append order equals
//! membership-evaluation order** — a move evaluated at t₁ must never land after an emission
//! for the same pk evaluated at t₂ > t₁, or the stream's last word is stale (permanent
//! divergence). The old implementation guaranteed this by holding the registry lock across
//! the `append_reliable` network call; these lanes give the same guarantee without network
//! under the lock:
//!
//!   * emitters **enqueue under the registry lock** — so per-stream enqueue order is exactly
//!     eval order — and return immediately;
//!   * each stream path hashes to ONE lane (a FIFO drained by one writer task), so batches
//!     for a stream land in enqueue order;
//!   * appends use `append_reliable` (retry-until-landed; the only non-retried case is 404 —
//!     the shape was dropped, discard is correct).
//!
//! The convergence barrier: every enqueued batch increments the shared `pending` counter
//! (the engine's `pendingFlips` term) and decrements it only after the append **landed**, so
//! "`pendingFlips == 0`" still means every subquery effect is on its stream — queued batches
//! included. Increment-before/at-enqueue happens while the emitter still holds its own
//! accounting (a `FlipWork` token stays >0 until its batches are enqueued), so the counter
//! never dips to zero with effects in flight.

use super::*;

use std::sync::atomic::AtomicI64;

struct Batch {
    stream_path: String,
    envs: Vec<Envelope>,
}

/// Hash-sharded, per-stream-ordered append lanes. Cheap to clone.
#[derive(Clone)]
pub(crate) struct EmissionLanes {
    lanes: Arc<Vec<mpsc::UnboundedSender<Batch>>>,
    pending: Arc<AtomicI64>,
}

impl EmissionLanes {
    /// Spawn `n` writer tasks. `pending` is the engine's convergence-barrier counter
    /// (`pendingFlips`): incremented per enqueued batch, decremented after its append lands.
    pub(crate) fn spawn(ds: DsClient, n: usize, pending: Arc<AtomicI64>) -> EmissionLanes {
        let n = n.max(1);
        let mut lanes = Vec::with_capacity(n);
        for _ in 0..n {
            let (tx, mut rx) = mpsc::unbounded_channel::<Batch>();
            let ds = ds.clone();
            let pending = pending.clone();
            tokio::spawn(async move {
                while let Some(b) = rx.recv().await {
                    // Reliable: a dropped subquery envelope is permanent divergence for the
                    // shape's subscribers. 404 (shape dropped mid-flight) discards, correctly.
                    ds.append_reliable(&b.stream_path, &b.envs).await;
                    pending.fetch_sub(1, Ordering::SeqCst);
                }
            });
            lanes.push(tx);
        }
        EmissionLanes { lanes: Arc::new(lanes), pending }
    }

    /// Which lane serves `path` (stable: one stream always drains through one FIFO).
    pub(crate) fn lane_for(&self, path: &str) -> usize {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        path.hash(&mut h);
        (h.finish() % self.lanes.len() as u64) as usize
    }

    /// Enqueue one batch for ordered delivery. Call while holding the registry lock so that
    /// per-stream enqueue order is evaluation order. Never blocks (unbounded lane queues —
    /// backpressure is the PG pool bounding how fast evals can produce batches).
    pub(crate) fn enqueue(&self, stream_path: &str, envs: Vec<Envelope>) {
        if envs.is_empty() {
            return;
        }
        self.pending.fetch_add(1, Ordering::SeqCst);
        let lane = self.lane_for(stream_path);
        if self.lanes[lane].send(Batch { stream_path: stream_path.to_string(), envs }).is_err() {
            // Writer gone (engine teardown): don't leave the barrier stuck.
            self.pending.fetch_sub(1, Ordering::SeqCst);
        }
    }
}
