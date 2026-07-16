//! The global primary-key dictionary: an append-only, bidirectional `pk string ↔ u32 id`
//! interner, one instance per engine.
//!
//! The membership circuit (`crate::subq_circuit`) and its registry (`crate::subquery`) key every
//! relation by primary key. Storing the pk as a heap `String` in each circuit entry AND each
//! registry index entry — once per relation, per feed/node — was the dominant per-entry cost
//! (measured ~118 B resident/entry, ~27% of the membership footprint at 100k subscriptions). This
//! dictionary stores each distinct pk string exactly ONCE globally and hands out a `u32` id used
//! as the in-circuit / in-index key, so a per-feed entry drops to 8 B (its `(feed_id, pk_id)` key)
//! and the string is amortized across every feed/node that references it.
//!
//! **Append-only (no eviction in v1):** an id, once minted, is never freed or reused — matching
//! the feed-id / node-id non-reuse precedent (a stale snapshot read can never alias a new
//! meaning). The reverse table is therefore `O(distinct pks ever synced)`; its own footprint is
//! reported as `bytes_pk_dict` in `GET /memory` so the trade is visible.
//!
//! **Locking — single `RwLock` per table, not sharded:** the reverse table is a `Vec` indexed by
//! id, so a new id's slot MUST equal the current `Vec` length at mint time. A single write-locked
//! section makes "id = reverse.len(); reverse.push(pk); forward.insert(pk, id)" atomic with no
//! cross-shard id-allocation coordination and no holes in the reverse `Vec`. The common case —
//! a pk already interned — is a lock-free-of-writers *read* on the forward map (`get_or_insert`'s
//! fast path) and `resolve` is a pure reverse read, so the write lock is contended only on the
//! genuinely-first sync of each distinct pk. Sharding the forward map would buy nothing here (the
//! bottleneck is the shared, ordered reverse table, not forward-map hashing) while complicating
//! the append-only reverse invariant — so a single lock per table is the deliberate choice.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// An `Arc<str>`'s heap control block: two `usize` reference counts (strong + weak) preceding the
/// inline string bytes in the single `ArcInner<str>` allocation.
const ARC_CONTROL_BYTES: usize = 2 * std::mem::size_of::<usize>();

/// A global, append-only `pk string ↔ u32` interner shared by the circuit, its registry, and the
/// emission seam. See the module docs for the append-only / locking rationale.
pub struct PkDict {
    /// pk string → id. The `Arc<str>` key shares its allocation with the reverse table's entry.
    forward: RwLock<HashMap<Arc<str>, u32>>,
    /// id → pk string, indexed by id (`reverse[id]`). Append-only, so `reverse.len()` is the next
    /// id to mint.
    reverse: RwLock<Vec<Arc<str>>>,
}

impl Default for PkDict {
    fn default() -> Self {
        Self::new()
    }
}

impl PkDict {
    pub fn new() -> Self {
        PkDict { forward: RwLock::new(HashMap::new()), reverse: RwLock::new(Vec::new()) }
    }

    /// The id for `pk`, minting a fresh one (and interning the string once) if unseen. Idempotent:
    /// the same pk always maps to the same id for the life of the dictionary.
    pub fn get_or_insert(&self, pk: &str) -> u32 {
        // Fast path: already interned — a reader-only lock on the forward map.
        if let Some(&id) = self.forward.read().expect("pk_dict forward").get(pk) {
            return id;
        }
        // Slow path: mint under the write lock. Re-check inside the lock (another writer may have
        // interned `pk` between the read above and acquiring the write lock).
        let mut fwd = self.forward.write().expect("pk_dict forward");
        if let Some(&id) = fwd.get(pk) {
            return id;
        }
        let mut rev = self.reverse.write().expect("pk_dict reverse");
        let arc: Arc<str> = Arc::from(pk);
        let id = rev.len() as u32;
        rev.push(arc.clone());
        fwd.insert(arc, id);
        id
    }

    /// The id for `pk` if it has already been interned, WITHOUT minting a fresh one. Lets callers
    /// probe an inverted index keyed by pk id (a pk never interned can have no index entry) without
    /// polluting the dictionary with ids for pks that are only ever looked up (e.g. a delete for a
    /// never-member pk).
    pub fn get(&self, pk: &str) -> Option<u32> {
        self.forward.read().expect("pk_dict forward").get(pk).copied()
    }

    /// The pk string for a previously-minted `id`. Panics on an id this dictionary never minted —
    /// callers only ever resolve ids that came out of the same dictionary (append-only ⇒ any id it
    /// once returned stays resolvable forever).
    pub fn resolve(&self, id: u32) -> Arc<str> {
        self.reverse.read().expect("pk_dict reverse")[id as usize].clone()
    }

    /// Number of distinct pks interned (also the next id to be minted).
    pub fn len(&self) -> usize {
        self.reverse.read().expect("pk_dict reverse").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Estimated owned heap bytes — the amortized string storage plus the forward/reverse index
    /// buffers. Reported as `bytes_pk_dict` in `GET /memory` so the append-only trade is visible.
    /// On-demand only (walks every interned string); never call it from the 500 ms sampler.
    pub fn heap_bytes(&self) -> usize {
        let rev = self.reverse.read().expect("pk_dict reverse");
        let fwd = self.forward.read().expect("pk_dict forward");
        // Each distinct pk's `Arc<str>` allocation (control block + inline bytes) is counted ONCE
        // here even though it is pointed at by both the forward key and the reverse entry.
        let strings: usize = rev.iter().map(|s| ARC_CONTROL_BYTES + s.len()).sum();
        let reverse_buf = rev.capacity() * std::mem::size_of::<Arc<str>>();
        // hashbrown: one control byte + the (key, value) slot per bucket.
        let forward_buf = fwd.capacity()
            * (std::mem::size_of::<Arc<str>>() + std::mem::size_of::<u32>() + 1);
        strings + reverse_buf + forward_buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `get_or_insert` is idempotent: the same pk always returns the same id, distinct pks get
    /// distinct (densely-packed, mint-ordered) ids.
    #[test]
    fn get_or_insert_is_idempotent_and_dense() {
        let d = PkDict::new();
        let a1 = d.get_or_insert("alpha");
        let b = d.get_or_insert("beta");
        let a2 = d.get_or_insert("alpha");
        assert_eq!(a1, a2, "the same pk must always map to the same id");
        assert_ne!(a1, b, "distinct pks must map to distinct ids");
        assert_eq!((a1, b), (0, 1), "ids are minted densely in first-seen order");
        assert_eq!(d.len(), 2, "re-interning does not grow the dictionary");
    }

    /// `resolve` round-trips every minted id back to its exact pk string.
    #[test]
    fn resolve_round_trips() {
        let d = PkDict::new();
        let pks = ["", "1", "a-long-uuid-like-primary-key-0000", "unicode-π-θ", "42"];
        let ids: Vec<u32> = pks.iter().map(|p| d.get_or_insert(p)).collect();
        for (pk, id) in pks.iter().zip(&ids) {
            assert_eq!(&*d.resolve(*id), *pk, "resolve(get_or_insert(pk)) must be pk");
        }
        // And re-resolving via a re-insert is stable.
        for (pk, id) in pks.iter().zip(&ids) {
            assert_eq!(d.get_or_insert(pk), *id);
        }
    }

    /// Concurrent stress: 8 threads each interning the SAME 10k-pk keyspace must agree on one id
    /// per pk (no torn/duplicate mint), and every id must resolve back to its pk. The final
    /// dictionary holds exactly the distinct-pk count, and its id space is a dense `0..N`.
    #[test]
    fn concurrent_inserts_are_consistent() {
        use std::sync::Arc as StdArc;
        use std::thread;

        const THREADS: usize = 8;
        const KEYS: usize = 10_000;
        let dict = StdArc::new(PkDict::new());

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let dict = dict.clone();
                thread::spawn(move || {
                    let mut seen = Vec::with_capacity(KEYS);
                    for k in 0..KEYS {
                        seen.push((k, dict.get_or_insert(&format!("pk-{k}"))));
                    }
                    seen
                })
            })
            .collect();

        let results: Vec<Vec<(usize, u32)>> =
            handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Every thread must have observed the SAME id for a given pk (canonical mapping).
        let canonical = &results[0];
        for r in &results[1..] {
            assert_eq!(r, canonical, "all threads must agree on each pk's id");
        }
        // Exactly KEYS distinct ids, densely packed, each resolving back to its pk.
        assert_eq!(dict.len(), KEYS, "exactly the distinct-pk count interned");
        let mut ids: Vec<u32> = canonical.iter().map(|(_, id)| *id).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), KEYS, "no two distinct pks share an id");
        assert_eq!(*ids.last().unwrap(), (KEYS - 1) as u32, "id space is dense 0..KEYS");
        for (k, id) in canonical {
            assert_eq!(&*dict.resolve(*id), &format!("pk-{k}"), "id resolves to its pk");
        }
    }

    /// The reported footprint is zero when empty and grows with interned content.
    #[test]
    fn heap_bytes_reflects_content() {
        let d = PkDict::new();
        assert_eq!(d.heap_bytes(), 0, "an empty dictionary owns no heap");
        for i in 0..1000 {
            d.get_or_insert(&format!("some-primary-key-{i}"));
        }
        assert!(d.heap_bytes() > 0, "a populated dictionary reports non-zero bytes");
    }
}
