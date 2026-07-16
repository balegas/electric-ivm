//! Host-side per-feed key sets — the delete gate, moved OUT of the dbsp membership circuit
//! (Task 2.2, re-litigates bead dbsp-ds-dh6; see
//! `docs/notes/2026-07-16-feed-set-representation-spike.md`).
//!
//! Each subquery shape owns one feed: the set of pk ids (`u32` dictionary ids — see
//! [`crate::pk_dict`]) currently delivered to its stream. The feed's ONLY role is the **delete
//! gate**: upserts are delivered for every current member unconditionally, but a delete is emitted
//! for a pk **iff** that pk was actually in the feed. Previously this lived in the circuit as an
//! `add_input_set` upsert-SET whose per-step retraction deltas were the deletes; measurement (the
//! spike) showed the feed set is ~16 MiB at 100k subscriptions (10–19× lighter than the dbsp
//! relation) and needs no spilling — so it moves here, to a `HashMap<feed_id, RoaringBitmap>`.
//!
//! **The gate is now a synchronous check-and-set.** On a `member == true` verdict, `insert(pk)`
//! (the upsert is delivered regardless); on `member == false`, a delete is emitted **iff**
//! `remove(pk)` returned `true`. Both are `&mut self` methods with NO `.await`, called only inside
//! the registry-lock critical section, so the emission decision and the bitmap transition are one
//! indivisible step — the borrow checker itself enforces there is no window between them (the
//! wake-storm gate, PR #30, made structural without a cross-thread circuit hop). The §3
//! equivalence table of the spike is the contract; [`tests`] pins it row-by-row.

use std::collections::HashMap;

use roaring::RoaringBitmap;

use crate::heap_size::HeapSize;

/// Per-feed key sets: `feed_id -> { pk_id }`. `feed_id`s are minted monotonically by the registry
/// and never reused, so a dropped feed's id can never alias a live one (a fresh `feed_id` always
/// starts from an absent — hence empty — bitmap).
// Increment 1 introduces the type with its unit tests but no engine wiring yet; increment 2
// wires it into `SubqueryRegistry`, at which point this allow is removed.
#[allow(dead_code)]
#[derive(Default)]
pub(crate) struct FeedSet {
    feeds: HashMap<i64, RoaringBitmap>,
}

#[allow(dead_code)]
impl FeedSet {
    pub(crate) fn new() -> Self {
        FeedSet { feeds: HashMap::new() }
    }

    /// Assert `pk` present in `feed_id`. Returns `true` iff this was a genuinely new insertion
    /// (absent → present); `false` on a duplicate (present → present). Mirrors the circuit's
    /// feed Δ +1 (new) vs Δ 0 (duplicate): the upsert is delivered either way, so the return
    /// value is informational, not a gate.
    pub(crate) fn insert(&mut self, feed_id: i64, pk: u32) -> bool {
        self.feeds.entry(feed_id).or_default().insert(pk)
    }

    /// Retract `pk` from `feed_id`. Returns `true` iff `pk` was actually present (present →
    /// absent) — **this is the delete gate**: a delete is emitted iff this returns `true`. A
    /// never-member (absent feed, or absent pk within a live feed) returns `false` and emits
    /// nothing, exactly as the circuit's feed Δ 0 netted to nothing (the wake-storm gate).
    pub(crate) fn remove(&mut self, feed_id: i64, pk: u32) -> bool {
        match self.feeds.get_mut(&feed_id) {
            Some(bm) => bm.remove(pk),
            None => false,
        }
    }

    /// Is `pk` currently a member of `feed_id`?
    #[cfg(test)]
    pub(crate) fn contains(&self, feed_id: i64, pk: u32) -> bool {
        self.feeds.get(&feed_id).is_some_and(|bm| bm.contains(pk))
    }

    /// Drop a whole feed (shape teardown) — O(1), no per-pk enumeration or circuit round-trip.
    /// The whole bitmap is freed; the `feed_id` is never reused.
    pub(crate) fn drop_feed(&mut self, feed_id: i64) {
        self.feeds.remove(&feed_id);
    }

    /// Number of pks currently in `feed_id` (introspection). Zero for an unknown feed.
    pub(crate) fn feed_len(&self, feed_id: i64) -> usize {
        self.feeds.get(&feed_id).map_or(0, |bm| bm.len() as usize)
    }

    /// Every pk id currently in `feed_id`, ascending (introspection + tests). Empty for an
    /// unknown feed.
    pub(crate) fn feed_pk_ids(&self, feed_id: i64) -> Vec<u32> {
        self.feeds.get(&feed_id).map_or_else(Vec::new, |bm| bm.iter().collect())
    }
}

impl HeapSize for FeedSet {
    /// Lower-bound owned heap: the outer `HashMap`'s swiss-table backing store (same estimate as
    /// [`HeapSize for HashMap`](crate::heap_size)) plus each bitmap's serialized payload floor
    /// (`RoaringBitmap::serialized_size` — container headers + payload, the checkpoint-file size).
    /// This is the spike's documented "owned floor" (serialized + outer HashMap); the allocator's
    /// per-`Vec<Container>` slack beyond the payload is the RSS-vs-owned gap, not counted here
    /// (keeps this a lower bound, consistent with `heap_size.rs`).
    fn heap_bytes(&self) -> usize {
        let entry = std::mem::size_of::<(i64, RoaringBitmap)>() + 1; // +1 ctrl byte (swiss table)
        let outer = (self.feeds.capacity() * entry * 11) / 10;
        let payload: usize = self.feeds.values().map(|bm| bm.serialized_size()).sum();
        outer + payload
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The §3 semantics-equivalence table, row by row — the contract this move must preserve.
    /// Each assertion names the circuit behaviour it reproduces.
    #[test]
    fn transition_table_matches_circuit_gate() {
        let mut fs = FeedSet::new();
        let feed = 9i64;

        // Row "delete absent / never-member" (absent → absent): remove() == false → gated.
        // (The wake-storm gate: the circuit's feed Δ 0 nets to nothing.)
        assert!(!fs.remove(feed, 100), "never-member delete must gate (remove == false)");
        assert!(!fs.contains(feed, 100));

        // Row "insert new member" (absent → present): insert() == true → upsert delivered.
        assert!(fs.insert(feed, 1), "first insert of a pk is genuinely new (feed Δ +1)");
        assert!(fs.contains(feed, 1));

        // Row "insert duplicate" (present → present): insert() == false → no new side effect
        // (upsert re-delivered idempotently; circuit feed Δ 0).
        assert!(!fs.insert(feed, 1), "re-insert of a held pk nets nothing (feed Δ 0)");
        assert!(fs.contains(feed, 1));

        // Row "delete present member" (present → absent): remove() == true → delete emitted
        // (circuit feed Δ −1).
        assert!(fs.remove(feed, 1), "a genuine member's delete gates open (feed Δ −1)");
        assert!(!fs.contains(feed, 1));

        // And gone again: repeat delete nets nothing (absent → absent).
        assert!(!fs.remove(feed, 1), "repeat delete of an already-removed pk gates (remove == false)");
    }

    /// Feeds are isolated by id; drop is O(1) and total, and never touches a neighbour.
    #[test]
    fn feeds_are_isolated_and_drop_is_total() {
        let mut fs = FeedSet::new();
        fs.insert(7, 4);
        fs.insert(7, 5);
        fs.insert(8, 6);

        let mut pks = fs.feed_pk_ids(7);
        pks.sort_unstable();
        assert_eq!(pks, vec![4, 5]);
        assert_eq!(fs.feed_len(8), 1);
        assert_eq!(fs.feed_pk_ids(999), Vec::<u32>::new(), "unknown feed enumerates empty");
        assert_eq!(fs.feed_len(999), 0);

        // Drop feed 7: gone entirely, feed 8 untouched.
        fs.drop_feed(7);
        assert_eq!(fs.feed_len(7), 0);
        assert_eq!(fs.feed_pk_ids(7), Vec::<u32>::new());
        assert!(!fs.contains(7, 4));
        assert_eq!(fs.feed_len(8), 1, "dropping a feed must not touch a neighbour");
        assert!(fs.contains(8, 6));
    }

    /// A remove against a live feed that never held the pk gates (no delete), distinct from a
    /// remove against a feed that never existed — both `false`, neither mints an entry.
    #[test]
    fn remove_miss_does_not_create_a_feed() {
        let mut fs = FeedSet::new();
        fs.insert(7, 1);
        assert!(!fs.remove(7, 2), "pk never in this live feed gates (remove == false)");
        assert!(!fs.remove(42, 1), "remove against a nonexistent feed gates (remove == false)");
        assert_eq!(fs.feed_len(42), 0, "a remove miss must not create the feed");
        assert!(!fs.contains(42, 1));
    }

    /// `heap_bytes` is zero when empty and grows with populated feeds (the `bytes_feed_sets`
    /// term of `GET /memory`).
    #[test]
    fn heap_bytes_zero_when_empty_and_grows() {
        let mut fs = FeedSet::new();
        assert_eq!(fs.heap_bytes(), 0, "an empty feed set owns no heap");
        for pk in 0..1000u32 {
            fs.insert(9, pk);
        }
        assert!(fs.heap_bytes() > 0, "a populated feed set owns measurable heap");
    }
}
