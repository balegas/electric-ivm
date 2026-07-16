//! SPIKE (Task 2.2, re-litigates bead dbsp-ds-dh6) — measurement-only.
//!
//! Question: should the per-feed key set move OUT of the dbsp membership circuit and into a
//! host-side `HashMap<feed_id, RoaringBitmap>` keyed by `u32` pk-id? This module measures the
//! resident-byte cost of the two representations over the same ~3.7M-entry feed shape so the
//! decision doc can quote a real ratio, not a hand-wave.
//!
//! It is NOT wired into the engine: it is a `#[cfg(test)]` module, its one measurement test is
//! `#[ignore]` (it builds a multi-hundred-MB circuit and is slow in debug), and the whole thing
//! deletes cleanly (this file + the `mod` line in lib.rs + the `roaring` dev-dep).
//!
//! Run it (release, or it takes minutes):
//! ```text
//! cargo test --release -p electric-ivm-engine feed_repr_spike -- --ignored --nocapture
//! ```
//!
//! What is measured, and how honestly:
//! - **dbsp**: `MembershipCircuit::profile_bytes().0` — dbsp's own profiler `total_used_bytes`,
//!   summed over every stateful operator (the feed upsert integral, its `z1`, the published
//!   `integrate_trace` snapshot). Allocator-independent, the same quantity the attribution doc's
//!   spill-delta approximates. Spill is forced OFF (`ELECTRIC_IVM_SUBQ_STORAGE=0`) so this is the
//!   true in-memory residency, not an undercount that pages to disk.
//! - **roaring**: process **RSS delta** across building the map (the allocator-visible truth,
//!   includes `HashMap` buckets, `Vec<Container>` headers, and slack) plus the roaring
//!   `serialized_size()` sum (the container-payload floor). RSS is measured from a fresh baseline
//!   taken *after* the circuit is shut down, so the two structures do not overlap in the process.
//!
//! Both are "resident bytes owned by the feed representation"; the ratio is the deliverable.

use std::collections::HashMap;

use roaring::RoaringBitmap;

use crate::subq_circuit::{Assertions, MembershipCircuit, PkKey};
use crate::value::Tup2;

/// The ~3.7M-entry feed shape from the 100k-subscription workload, with realistic skew.
///
/// pk-ids are dictionary ids drawn from a bounded universe (`PK_UNIVERSE` ≈ the distinct issue
/// count) — this is what makes the roaring comparison honest rather than flattering: mega-feeds
/// approach the whole universe (dense → bitmap containers), while the many small feeds hold a
/// sparse random sample (array containers → 2 B/entry, roaring's *worst* case, no compression).
struct FeedShape;

/// Distinct pk-id universe (≈ issues in the standard 100k-subscription bench).
const PK_UNIVERSE: u32 = 100_000;

impl FeedShape {
    /// `(feed_id, entry_count)` per feed. Sums to ~3.7M over 50_005 feeds:
    /// - 5 mega feeds × 100_000 = 500_000 (dense, ~whole universe)
    /// - 10_000 medium feeds × 300 = 3_000_000 (sparse sample — the bulk of the entries)
    /// - 40_000 small feeds × 5 = 200_000 (tiny sparse)
    fn feeds() -> Vec<(i64, u32)> {
        let mut v = Vec::with_capacity(50_005);
        let mut fid: i64 = 1;
        for _ in 0..5 {
            v.push((fid, 100_000));
            fid += 1;
        }
        for _ in 0..10_000 {
            v.push((fid, 300));
            fid += 1;
        }
        for _ in 0..40_000 {
            v.push((fid, 5));
            fid += 1;
        }
        v
    }

    /// The pk-ids for one feed. Mega feeds (≥ universe size) take the whole universe densely;
    /// smaller feeds take a deterministic pseudo-random sample (a per-feed LCG walk over the
    /// universe — reproducible, no `rand` dep, and genuinely scattered so roaring cannot cheat).
    fn pk_ids(feed_id: i64, count: u32, out: &mut Vec<u32>) {
        out.clear();
        if count >= PK_UNIVERSE {
            out.extend(0..PK_UNIVERSE);
            return;
        }
        // LCG seeded by feed id; Knuth's MMIX constants, taken mod universe. Dedup via a small
        // membership guard is unnecessary at these low densities (<0.3%); collisions just make the
        // realized count marginally under `count`, which is fine for a shape measurement.
        let mut state = (feed_id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
        for _ in 0..count {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            out.push((state >> 33) as u32 % PK_UNIVERSE);
        }
    }
}

fn rss_bytes() -> u64 {
    crate::mem::process_memory().0
}

/// Load every feed's pk-ids into the dbsp membership circuit's feed relation, batched so no single
/// dbsp transaction is unreasonably large. Returns the total entries asserted.
async fn load_dbsp(circuit: &MembershipCircuit, feeds: &[(i64, u32)]) -> u64 {
    const BATCH: usize = 200_000;
    let mut asserts = Assertions::default();
    let mut total: u64 = 0;
    let mut buf = Vec::new();
    for &(feed_id, count) in feeds {
        FeedShape::pk_ids(feed_id, count, &mut buf);
        for &pk in &buf {
            asserts.feeds.push(Tup2(PkKey { id: feed_id, pk }, true));
            total += 1;
            if asserts.feeds.len() >= BATCH {
                circuit.apply(std::mem::take(&mut asserts)).await;
            }
        }
    }
    if !asserts.is_empty() {
        circuit.apply(asserts).await;
    }
    total
}

/// Build the host-side `HashMap<feed_id, RoaringBitmap>` for the same shape. Returns
/// `(map, entries, serialized_bytes)`.
fn build_roaring(feeds: &[(i64, u32)]) -> (HashMap<i64, RoaringBitmap>, u64, u64) {
    let mut map: HashMap<i64, RoaringBitmap> = HashMap::with_capacity(feeds.len());
    let mut entries: u64 = 0;
    let mut buf = Vec::new();
    for &(feed_id, count) in feeds {
        FeedShape::pk_ids(feed_id, count, &mut buf);
        let mut bm = RoaringBitmap::new();
        for &pk in &buf {
            bm.insert(pk);
        }
        entries += bm.len();
        map.insert(feed_id, bm);
    }
    let serialized: u64 = map.values().map(|b| b.serialized_size() as u64).sum();
    (map, entries, serialized)
}

/// Estimated host-heap the roaring map *owns* beyond the container payloads: the outer `HashMap`
/// backing store (swiss-table, ~1.1× load) + the inline `RoaringBitmap` struct per feed. The
/// per-bitmap `Vec<Container>` heap is already captured by RSS; this is only the floor cross-check.
fn roaring_owned_floor(map: &HashMap<i64, RoaringBitmap>) -> u64 {
    let entry = std::mem::size_of::<(i64, RoaringBitmap)>() + 1;
    (map.capacity() * entry * 11 / 10) as u64
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "spike measurement: multi-hundred-MB, minutes in debug — run with --release --ignored"]
async fn feed_repr_spike_dbsp_vs_roaring_resident_bytes() {
    let feeds = FeedShape::feeds();
    let feed_count = feeds.len();

    // ---- dbsp feed relation (in-memory, spill forced off) ------------------------------------
    // SAFETY: single-threaded at this point (no other thread reads the environment); set before
    // the circuit thread is spawned so its `spill_config_from_env` observes the disabled value.
    unsafe { std::env::set_var("ELECTRIC_IVM_SUBQ_STORAGE", "0") };
    let rss_before_dbsp = rss_bytes();
    let circuit = MembershipCircuit::start_with(true).expect("start membership circuit");
    let dbsp_entries = load_dbsp(&circuit, &feeds).await;
    let (dbsp_used, dbsp_stored) = circuit.profile_bytes().await;
    let rss_after_dbsp = rss_bytes();
    let dbsp_rss_delta = rss_after_dbsp.saturating_sub(rss_before_dbsp);
    circuit.shutdown().await;
    // Let the circuit thread's allocations settle back before the roaring baseline.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // ---- host-side HashMap<feed_id, RoaringBitmap> -------------------------------------------
    let rss_before_roar = rss_bytes();
    let (map, roar_entries, roar_serialized) = build_roaring(&feeds);
    let rss_after_roar = rss_bytes();
    let roar_rss_delta = rss_after_roar.saturating_sub(rss_before_roar);
    let roar_owned_floor = roar_serialized + roaring_owned_floor(&map);
    // Keep the map alive across the measurement.
    assert_eq!(map.len(), feed_count);

    let mib = |b: u64| b as f64 / (1024.0 * 1024.0);
    let per = |b: u64, n: u64| if n == 0 { 0.0 } else { b as f64 / n as f64 };

    println!("\n================ Task 2.2 feed-set representation spike ================");
    println!("feeds: {feed_count}  (5×100k mega, 10k×300, 40k×5)  pk universe: {PK_UNIVERSE}");
    println!("dbsp feed entries asserted: {dbsp_entries}   roaring entries: {roar_entries}");
    println!("-----------------------------------------------------------------------");
    println!("DBSP feed relation (profiler total_used_bytes, in-memory):");
    println!("    used   = {:>12} B  ({:>7.1} MiB)   {:.1} B/entry", dbsp_used, mib(dbsp_used as u64), per(dbsp_used as u64, dbsp_entries));
    println!("    stored = {:>12} B  (must be ~0 with spill off)", dbsp_stored);
    println!("    RSS Δ  = {:>12} B  ({:>7.1} MiB)  [cross-check, allocator-inclusive]", dbsp_rss_delta, mib(dbsp_rss_delta));
    println!("Roaring HashMap<feed_id, RoaringBitmap>:");
    println!("    RSS Δ      = {:>12} B  ({:>7.1} MiB)   {:.2} B/entry  [headline]", roar_rss_delta, mib(roar_rss_delta), per(roar_rss_delta, roar_entries));
    println!("    serialized = {:>12} B  ({:>7.1} MiB)   {:.2} B/entry  [container payload floor]", roar_serialized, mib(roar_serialized), per(roar_serialized, roar_entries));
    println!("    owned floor= {:>12} B  ({:>7.1} MiB)  [serialized + outer HashMap]", roar_owned_floor, mib(roar_owned_floor));
    println!("-----------------------------------------------------------------------");
    let ratio_profiler_rss = dbsp_used as f64 / roar_rss_delta.max(1) as f64;
    let ratio_rss_rss = dbsp_rss_delta as f64 / roar_rss_delta.max(1) as f64;
    println!("RATIO dbsp(profiler used) / roaring(RSS Δ) = {ratio_profiler_rss:.1}×");
    println!("RATIO dbsp(RSS Δ)         / roaring(RSS Δ) = {ratio_rss_rss:.1}×");
    println!("=======================================================================\n");

    // Guardrails so a regression/misconfig fails loudly rather than printing garbage.
    assert!(dbsp_entries > 3_000_000, "expected ~3.7M dbsp entries, got {dbsp_entries}");
    assert_eq!(dbsp_stored, 0, "spill should be off; got {dbsp_stored} on-disk bytes");
    assert!(dbsp_used > 0 && roar_rss_delta > 0, "both representations must measure > 0");
    assert!(
        ratio_profiler_rss > 3.0,
        "spike premise is that dbsp is materially heavier; got only {ratio_profiler_rss:.1}×"
    );
}
