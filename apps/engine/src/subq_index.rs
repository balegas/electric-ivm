//! The necessary-conjunct index over **subquery outer shapes** — the routing structure that makes
//! a change to a subquery shape's outer table cost `O(candidates)` instead of `O(#subquery shapes)`.
//!
//! Before this, `SubqueryRegistry::on_table_delta` step 2 scanned the whole global `shapes` map,
//! filtered by `outer_table`, and handed every survivor to `emit_for_shapes`, which re-evaluates the
//! FULL predicate per shape per touched pk. That was the one hot path left in the engine whose
//! per-change cost scaled with subscription count — every other tier is index-routed
//! (`engine::executors::KeyRouter`, `engine::executors::StandaloneIndex`).
//!
//! **Why a separate structure rather than reusing [`StandaloneIndex`](crate::engine::executors::StandaloneIndex).**
//! The two indexes answer different questions and would not collapse cleanly:
//!
//!  * `StandaloneIndex` is *per table already* — the sequencer holds one per table executor — so it
//!    has no table bucket. Subquery shapes all live in ONE global registry map keyed by shape id,
//!    across every outer table, so bucketing by outer table is the first thing this index must do
//!    and the first thing `StandaloneIndex` must not grow.
//!  * `StandaloneIndex`'s posting lists are `Vec<String>` and its probe returns `Vec<String>` built
//!    from a `HashSet<&str>`. Here the posting lists are [`RoaringBitmap`]s over **interned dense
//!    `u32` shape ids** (the [`crate::pk_dict`] interning pattern, applied to shape ids): the
//!    range-bound prefix scan unions whole posting lists in one pass instead of concatenating
//!    `Vec<String>`s, the eq ∪ lower ∪ upper ∪ scan candidate set dedups for free, and per-shape
//!    membership costs a couple of bits instead of a heap `String` clone per probe. Retrofitting
//!    that onto `StandaloneIndex` would mean changing the standalone and aggregate probe paths
//!    (`engine::sequencer` `shape_index`/`agg_index`) in the same change — destabilising two
//!    working tiers for a refactor. So: shared *extraction* logic
//!    ([`crate::predicate::CompiledPredicate::access_leaf`], the one definition of "necessary
//!    conjunct"), separate *storage*.
//!
//! **The correctness rule this index must not break.** Subquery outer membership is emitted
//! **absolutely**, not as a delta (`docs/ivm-engine-internals.md` §3.3, "The critical correctness
//! rule"): `emit_for_shapes` emits the *current* membership of every touched pk — an upsert if it
//! matches now, else a delete that the per-feed Roaring gate ([`crate::subq_feed`]) drops if the pk
//! was never a member. Skipping a shape because the **new** row image fails its indexed conjunct
//! would therefore silently drop the move-out delete for a row that was a member a moment ago.
//!
//! So [`SubqueryShapeIndex::candidates`] probes over the **raw Z-set delta**, whose `-1` tuples are
//! the row's old image and whose `+1` tuple is the new one (see `engine::output::apply_envelope`,
//! which builds updates as `[(old, -1), (new, +1)]` under `REPLICA IDENTITY FULL`). The candidate
//! set is thus the union over old ∪ new images, and the safety argument closes inductively: a pk is
//! in a shape's feed only because it matched at its last evaluation, which means the image that made
//! it a member is exactly the `-1` old image of the *next* delta touching that pk — so that delta
//! always makes the shape a candidate and the move-out is always emitted. (A table whose
//! `REPLICA IDENTITY` is not `FULL` carries no old image; `replication.rs` already logs that loudly
//! and the delta-based standalone/router tiers are equally degraded there — it is a pre-existing
//! whole-engine assumption, not something this index newly relies on.)
//!
//! Two more things `access_leaf` guarantees, which this index depends on and
//! [`tests::in_leaves_are_never_a_necessary_conjunct`] pins:
//!
//!  * a **subquery `IN` leaf is never returned** — its truth depends on node state, not on the row
//!    alone, so it is not a valid index key. `access_leaf` only ever returns
//!    `CompiledPredicate::Cmp` leaves, and `InSubquery` is a different variant.
//!  * a leaf under a **negation** (`NOT IN`, or any `Not{…}` wrapper) is never returned — the walk
//!    descends only through `And` chains, so a `Not` node yields nothing and its shape falls to the
//!    unconditional `scan` list.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use roaring::RoaringBitmap;

use crate::heap_size::HeapSize;
use crate::predicate::{AccessLeaf, CompiledPredicate};
use crate::value::{Row, Tup2, Value, ZWeight};

/// The two posting lists of one range-bound literal, split by strictness so the prefix scan can
/// union whole bitmaps without a per-shape predicate on `strict`. At the bound value itself only
/// `inclusive` qualifies (`x > x` is false); strictly past it, both do.
#[derive(Default)]
struct Bound {
    /// Shapes whose conjunct is `col > v` / `col < v`.
    strict: RoaringBitmap,
    /// Shapes whose conjunct is `col >= v` / `col <= v`.
    inclusive: RoaringBitmap,
}

impl Bound {
    fn is_empty(&self) -> bool {
        self.strict.is_empty() && self.inclusive.is_empty()
    }
}

/// One outer table's posting lists. Mirrors [`StandaloneIndex`](crate::engine::executors::StandaloneIndex)'s
/// eq/lower/upper/scan shape, with `RoaringBitmap` posting lists over interned shape ids.
#[derive(Default)]
struct TableIndex {
    /// `col = v` conjuncts: column -> literal -> shape ids.
    eq: HashMap<usize, HashMap<Value, RoaringBitmap>>,
    /// `col >/>= v` conjuncts: column -> bound literal -> posting lists. A row value `x` satisfies
    /// bounds `< x` (either strictness) and `== x` (inclusive only) — an ordered prefix scan.
    lower: HashMap<usize, BTreeMap<Value, Bound>>,
    /// `col </<= v` conjuncts, mirrored (suffix scan).
    upper: HashMap<usize, BTreeMap<Value, Bound>>,
    /// Shapes with no indexable conjunct — always candidates.
    scan: RoaringBitmap,
}

impl TableIndex {
    fn is_empty(&self) -> bool {
        self.eq.is_empty() && self.lower.is_empty() && self.upper.is_empty() && self.scan.is_empty()
    }
}

/// Where one shape was placed, for exact removal: its outer table plus the conjunct it was filed
/// under (`None` = the table's `scan` list).
struct Placement {
    table: Arc<str>,
    leaf: Option<AccessLeaf>,
}

/// Necessary-conjunct index over subquery outer shapes, bucketed by outer table.
///
/// **Shape-id interning.** Posting lists hold dense `u32`s, minted by the same
/// forward-map/reverse-`Vec` scheme as [`crate::pk_dict`]. Unlike the pk dictionary this one *does*
/// reuse ids of removed shapes (a free list): shape churn is unbounded over an engine's lifetime,
/// and — critically — reuse is safe here in a way it is not there. `placed` records exactly where
/// each shape sits, so [`remove`](Self::remove) erases every bit a shape ever set before its id
/// returns to the free list, and no id ever escapes this struct (probes resolve to shape-id strings
/// before returning). There is no snapshot a stale id could be read against.
#[derive(Default)]
pub(crate) struct SubqueryShapeIndex {
    /// shape id -> interned id. The `Arc<str>` key shares its allocation with `reverse`'s entry.
    forward: HashMap<Arc<str>, u32>,
    /// interned id -> shape id; `None` = a freed slot awaiting reuse.
    reverse: Vec<Option<Arc<str>>>,
    /// Interned ids of removed shapes, available for re-minting.
    free: Vec<u32>,
    /// Posting lists bucketed by outer table — this is what replaces the global
    /// `shapes.iter().filter(outer_table == table)` scan.
    tables: HashMap<Arc<str>, TableIndex>,
    /// Where each live shape was placed, for removal.
    placed: HashMap<u32, Placement>,
}

impl SubqueryShapeIndex {
    /// File `shape_id` (outer table `table`, outer predicate `pred`) under its necessary conjunct,
    /// or on the table's unconditional `scan` list if it has none. Idempotent-safe: re-inserting a
    /// live shape id removes the previous placement first, so an id can never be filed twice.
    pub(crate) fn insert(&mut self, shape_id: &str, table: &str, pred: &CompiledPredicate) {
        self.remove(shape_id);
        let sid = self.intern(shape_id);
        // `Arc<str>` hashes/compares by content, so this finds an existing bucket; the extra
        // allocation is per-shape-registration, never on the probe path.
        let table: Arc<str> = Arc::from(table);
        let leaf = pred.access_leaf();
        let bucket = self.tables.entry(table.clone()).or_default();
        match &leaf {
            Some(AccessLeaf::Eq { col, value }) => {
                bucket
                    .eq
                    .entry(*col)
                    .or_default()
                    .entry(value.clone())
                    .or_default()
                    .insert(sid);
            }
            Some(AccessLeaf::Lower { col, value, strict }) => {
                let e = bucket
                    .lower
                    .entry(*col)
                    .or_default()
                    .entry(value.clone())
                    .or_default();
                if *strict {
                    e.strict.insert(sid)
                } else {
                    e.inclusive.insert(sid)
                };
            }
            Some(AccessLeaf::Upper { col, value, strict }) => {
                let e = bucket
                    .upper
                    .entry(*col)
                    .or_default()
                    .entry(value.clone())
                    .or_default();
                if *strict {
                    e.strict.insert(sid)
                } else {
                    e.inclusive.insert(sid)
                };
            }
            None => {
                bucket.scan.insert(sid);
            }
        }
        self.placed.insert(sid, Placement { table, leaf });
    }

    /// Un-file a shape completely (drop / failed create), returning its interned id to the free
    /// list. A shape id this index never saw is a no-op.
    pub(crate) fn remove(&mut self, shape_id: &str) {
        let Some(sid) = self.forward.get(shape_id).copied() else {
            return;
        };
        let Some(Placement { table, leaf }) = self.placed.remove(&sid) else {
            return;
        };
        if let Some(bucket) = self.tables.get_mut(&table) {
            match leaf {
                Some(AccessLeaf::Eq { col, value }) => {
                    if let Some(by_val) = bucket.eq.get_mut(&col) {
                        if by_val.get_mut(&value).is_some_and(|bm| {
                            bm.remove(sid);
                            bm.is_empty()
                        }) {
                            by_val.remove(&value);
                        }
                        if by_val.is_empty() {
                            bucket.eq.remove(&col);
                        }
                    }
                }
                Some(AccessLeaf::Lower { col, value, .. }) => {
                    Self::remove_bound(&mut bucket.lower, col, &value, sid);
                }
                Some(AccessLeaf::Upper { col, value, .. }) => {
                    Self::remove_bound(&mut bucket.upper, col, &value, sid);
                }
                None => {
                    bucket.scan.remove(sid);
                }
            }
            // Reclaim the table bucket once its last shape leaves — the probe's first step is a
            // lookup here, so an engine that churns through outer tables must not accumulate
            // empty buckets.
            if bucket.is_empty() {
                self.tables.remove(&table);
            }
        }
        // Free the interned id LAST: everything above is keyed by it.
        self.forward.remove(shape_id);
        self.reverse[sid as usize] = None;
        self.free.push(sid);
    }

    /// Mint (or reuse) a dense id for `shape_id`. Only ever called for a shape not currently filed
    /// — `insert` un-files first — so this always allocates.
    fn intern(&mut self, shape_id: &str) -> u32 {
        let name: Arc<str> = Arc::from(shape_id);
        let sid = match self.free.pop() {
            Some(sid) => {
                self.reverse[sid as usize] = Some(name.clone());
                sid
            }
            None => {
                let sid = self.reverse.len() as u32;
                self.reverse.push(Some(name.clone()));
                sid
            }
        };
        self.forward.insert(name, sid);
        sid
    }

    /// Drop `sid` from one bound's posting lists, reclaiming the literal's entry (and the column's
    /// map) when they empty. Mirrors `StandaloneIndex::remove_bound`, over bitmaps.
    fn remove_bound(
        m: &mut HashMap<usize, BTreeMap<Value, Bound>>,
        col: usize,
        value: &Value,
        sid: u32,
    ) {
        let Some(by_val) = m.get_mut(&col) else {
            return;
        };
        if let Some(b) = by_val.get_mut(value) {
            b.strict.remove(sid);
            b.inclusive.remove(sid);
            if b.is_empty() {
                by_val.remove(value);
            }
        }
        if by_val.is_empty() {
            m.remove(&col);
        }
    }

    /// Shape ids on `table` whose necessary conjunct is satisfied by at least one row of `delta`,
    /// plus the table's unconditional `scan` shapes.
    ///
    /// `delta` is the **raw** Z-set delta — both the `-1` old images and the `+1` new image — so
    /// the result is the union over old ∪ new images. That is what keeps move-out deletes alive
    /// under absolute emission; see the module docs.
    ///
    /// A superset of the shapes that can change membership for any delta row (each candidate is
    /// still fully evaluated by `emit_for_shapes`); every non-candidate provably cannot.
    pub(crate) fn candidates(&self, table: &str, delta: &[Tup2<Row, ZWeight>]) -> Vec<String> {
        // Step 1 of the fix: bucket by outer table. A change to a table with no subquery shapes
        // costs one hash lookup, not a scan over every shape in the engine.
        let Some(bucket) = self.tables.get(table) else {
            return Vec::new();
        };
        // Unions accumulate into ONE bitmap: eq ∪ lower ∪ upper ∪ scan dedups for free, and each
        // ordered bound scan folds whole posting lists in with a single `|=` per literal.
        let mut out = bucket.scan.clone();
        for Tup2(row, _) in delta {
            for (col, by_val) in &bucket.eq {
                if let Some(cell) = row.0.get(*col)
                    && let Some(bm) = by_val.get(cell)
                {
                    out |= bm;
                }
            }
            for (col, bounds) in &bucket.lower {
                let Some(cell) = row.0.get(*col) else {
                    continue;
                };
                if matches!(cell, Value::Null) {
                    continue; // cmp with a NULL cell is UNKNOWN, never TRUE
                }
                for (bound, b) in bounds.range(..=cell) {
                    if bound != cell {
                        out |= &b.strict; // strictly past the bound: `col > bound` holds
                    }
                    out |= &b.inclusive;
                }
            }
            for (col, bounds) in &bucket.upper {
                let Some(cell) = row.0.get(*col) else {
                    continue;
                };
                if matches!(cell, Value::Null) {
                    continue;
                }
                for (bound, b) in bounds.range(cell..) {
                    if bound != cell {
                        out |= &b.strict;
                    }
                    out |= &b.inclusive;
                }
            }
        }
        // Resolve back to shape ids only for the survivors — the one place a `String` is minted
        // per probe, and only `O(candidates)` of them.
        out.iter()
            .filter_map(|sid| self.reverse.get(sid as usize).and_then(|n| n.as_deref()))
            .map(str::to_string)
            .collect()
    }

    /// Is any shape registered on `table`? One hash lookup — the O(1) replacement for
    /// `shapes.values().any(|s| s.outer_table == table)` in the registry's per-envelope
    /// `touches` fast-skip.
    pub(crate) fn has_table(&self, table: &str) -> bool {
        self.tables.contains_key(table)
    }

    /// Number of shapes currently filed (introspection / test assertions).
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.placed.len()
    }
}

impl HeapSize for SubqueryShapeIndex {
    /// Lower-bound owned heap. The interned shape-id strings are counted once (via `reverse`; the
    /// `forward` key shares the same `Arc<str>` allocation), and each posting list contributes its
    /// `RoaringBitmap::serialized_size` payload floor — the same "owned floor" convention
    /// [`crate::subq_feed::FeedSet`] uses for `bytes_feed_sets`.
    fn heap_bytes(&self) -> usize {
        if self.placed.is_empty() && self.reverse.is_empty() {
            return 0;
        }
        // Each distinct shape id's `Arc<str>` allocation (control block + inline bytes), counted
        // ONCE even though both `forward`'s key and `reverse`'s entry point at it.
        let arc_control = 2 * std::mem::size_of::<usize>();
        let names: usize = self
            .reverse
            .iter()
            .flatten()
            .map(|s| arc_control + s.len())
            .sum();
        let reverse_buf = self.reverse.capacity() * std::mem::size_of::<Option<Arc<str>>>();
        let forward_buf = self.forward.capacity()
            * (std::mem::size_of::<Arc<str>>() + std::mem::size_of::<u32>() + 1);
        let free_buf = self.free.capacity() * std::mem::size_of::<u32>();
        let placed: usize = self.placed.capacity() * (std::mem::size_of::<(u32, Placement)>() + 1)
            + self
                .placed
                .values()
                .map(|p| p.leaf.heap_bytes())
                .sum::<usize>();
        let tables: usize = self
            .tables
            .values()
            .map(|t| {
                let eq: usize =
                    t.eq.values()
                        .map(|by_val| {
                            by_val
                                .iter()
                                .map(|(v, bm)| v.heap_bytes() + bm.serialized_size())
                                .sum::<usize>()
                        })
                        .sum();
                let bounds = |m: &HashMap<usize, BTreeMap<Value, Bound>>| -> usize {
                    m.values()
                        .map(|by_val| {
                            by_val
                                .iter()
                                .map(|(v, b)| {
                                    v.heap_bytes()
                                        + b.strict.serialized_size()
                                        + b.inclusive.serialized_size()
                                })
                                .sum::<usize>()
                        })
                        .sum()
                };
                eq + bounds(&t.lower) + bounds(&t.upper) + t.scan.serialized_size()
            })
            .sum();
        names + reverse_buf + forward_buf + free_buf + placed + tables
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::predicate::LeafOp;

    fn eq_pred(col: usize, v: i64) -> CompiledPredicate {
        CompiledPredicate::Cmp {
            col,
            op: LeafOp::Eq,
            value: Value::Int(v),
        }
    }

    fn cmp_pred(col: usize, op: LeafOp, v: i64) -> CompiledPredicate {
        CompiledPredicate::Cmp {
            col,
            op,
            value: Value::Int(v),
        }
    }

    /// A stand-in subquery leaf: an `IN` whose truth depends on registry node state, never on the
    /// row alone.
    fn in_leaf(col: usize) -> CompiledPredicate {
        CompiledPredicate::InSubquery {
            col,
            sig: "inner_t|gid|MatchAll".to_string(),
            negated: false,
        }
    }

    /// A realistic outer subquery predicate: `col0 = k AND col1 IN (SELECT …)`.
    fn outer_pred(k: i64) -> CompiledPredicate {
        CompiledPredicate::And(vec![eq_pred(0, k), in_leaf(1)])
    }

    fn row2(a: i64, b: i64) -> Row {
        Row(vec![Value::Int(a), Value::Int(b)])
    }

    fn sorted(mut v: Vec<String>) -> Vec<String> {
        v.sort();
        v
    }

    /// **The point of the whole index**: N shapes on N distinct equality conjuncts, and a change
    /// visits O(1) of them — not N. This is the `O(#subquery shapes)` scan the bead is about.
    #[test]
    fn equality_conjunct_visits_one_shape_of_many() {
        let mut idx = SubqueryShapeIndex::default();
        const N: i64 = 500;
        for k in 0..N {
            idx.insert(&format!("s{k}"), "issue", &outer_pred(k));
        }
        assert_eq!(idx.len(), N as usize);

        let cands = idx.candidates("issue", &[Tup2(row2(7, 0), 1)]);
        assert_eq!(
            cands,
            vec!["s7".to_string()],
            "only the shape whose conjunct the row satisfies"
        );
    }

    /// **The correctness trap.** An update moves a row OUT of shape `s5`'s conjunct: the new image
    /// fails `col0 = 5`, the old image satisfied it. The old image is in the delta as the `-1`
    /// tuple, so `s5` MUST still be a candidate — otherwise `emit_for_shapes` never runs for it and
    /// the move-out delete (absolute emission) is silently dropped.
    #[test]
    fn candidate_set_is_the_union_of_old_and_new_images() {
        let mut idx = SubqueryShapeIndex::default();
        idx.insert("s5", "issue", &outer_pred(5));
        idx.insert("s6", "issue", &outer_pred(6));
        idx.insert("s9", "issue", &outer_pred(9));

        // UPDATE col0: 5 -> 6, as `apply_envelope` builds it.
        let delta = vec![Tup2(row2(5, 0), -1), Tup2(row2(6, 0), 1)];
        assert_eq!(
            sorted(idx.candidates("issue", &delta)),
            vec!["s5".to_string(), "s6".to_string()],
            "the shape the row LEFT must stay a candidate — its delete is emitted absolutely"
        );

        // A plain DELETE carries only the old image; the shape it leaves must still be visited.
        assert_eq!(
            idx.candidates("issue", &[Tup2(row2(9, 0), -1)]),
            vec!["s9".to_string()],
            "a delete's old image must still route to the shape holding that pk"
        );
    }

    /// Shapes are bucketed by outer table first: a change to `issue` never even looks at shapes
    /// whose outer table is `comment` (the global `shapes.iter().filter(...)` scan this replaces).
    #[test]
    fn shapes_are_bucketed_by_outer_table() {
        let mut idx = SubqueryShapeIndex::default();
        idx.insert("issue_shape", "issue", &outer_pred(1));
        idx.insert("comment_shape", "comment", &outer_pred(1));
        // A `scan` shape on the other table must not leak either — it is unconditional only
        // WITHIN its own bucket.
        idx.insert("comment_scan", "comment", &CompiledPredicate::MatchAll);

        assert_eq!(
            idx.candidates("issue", &[Tup2(row2(1, 0), 1)]),
            vec!["issue_shape".to_string()]
        );
        assert_eq!(
            sorted(idx.candidates("comment", &[Tup2(row2(1, 0), 1)])),
            vec!["comment_scan".to_string(), "comment_shape".to_string()]
        );
        assert!(
            idx.candidates("unrelated", &[Tup2(row2(1, 0), 1)])
                .is_empty()
        );
    }

    /// Predicates with no indexable conjunct stay unconditional candidates: a bare subquery leaf,
    /// a top-level OR/NOT, `!=`, and match-all.
    #[test]
    fn unindexable_predicates_stay_on_the_scan_list() {
        let mut idx = SubqueryShapeIndex::default();
        idx.insert("bare_in", "issue", &in_leaf(1));
        idx.insert("match_all", "issue", &CompiledPredicate::MatchAll);
        idx.insert(
            "or",
            "issue",
            &CompiledPredicate::Or(vec![eq_pred(0, 1), eq_pred(0, 2)]),
        );
        idx.insert("neq", "issue", &cmp_pred(0, LeafOp::Neq, 1));
        idx.insert(
            "negated",
            "issue",
            &CompiledPredicate::Not(Box::new(outer_pred(1))),
        );
        // …and one shape that IS indexed, to prove the scan list is unioned in, not exclusive.
        idx.insert("indexed", "issue", &outer_pred(1));

        // A row matching nothing indexed still visits every scan shape.
        assert_eq!(
            sorted(idx.candidates("issue", &[Tup2(row2(4242, 0), 1)])),
            vec![
                "bare_in".to_string(),
                "match_all".to_string(),
                "negated".to_string(),
                "neq".to_string(),
                "or".to_string()
            ]
        );
        // A row matching the indexed conjunct visits the scan shapes AND it.
        assert!(
            idx.candidates("issue", &[Tup2(row2(1, 0), 1)])
                .contains(&"indexed".to_string())
        );
    }

    /// `access_leaf` must never hand this index a key whose truth is not a function of the row
    /// alone. Two cases, both of which would be unsound as index keys:
    ///  * a subquery `IN` leaf (truth depends on the registry's node set), and
    ///  * any leaf under a negation — a `NOT IN` shape's membership is *inverted*, so the leaf is
    ///    not a necessary conjunct at all.
    #[test]
    fn in_leaves_are_never_a_necessary_conjunct() {
        assert_eq!(
            in_leaf(1).access_leaf(),
            None,
            "a subquery IN leaf is not row-local"
        );
        let negated_in = CompiledPredicate::InSubquery {
            col: 1,
            sig: "inner_t|gid|MatchAll".to_string(),
            negated: true,
        };
        assert_eq!(
            negated_in.access_leaf(),
            None,
            "a NOT IN leaf is not row-local either"
        );
        assert_eq!(
            CompiledPredicate::Not(Box::new(eq_pred(0, 5))).access_leaf(),
            None,
            "a Cmp under Not is NOT necessary — the predicate implies its negation"
        );
        // But a conjunct sitting BESIDE a negation is still necessary.
        let mixed = CompiledPredicate::And(vec![
            eq_pred(0, 5),
            CompiledPredicate::Not(Box::new(in_leaf(1))),
        ]);
        assert_eq!(
            mixed.access_leaf(),
            Some(AccessLeaf::Eq {
                col: 0,
                value: Value::Int(5)
            }),
            "`col0 = 5 AND col1 NOT IN (…)` still implies col0 = 5"
        );
    }

    /// Range bounds: the ordered prefix/suffix scans union whole posting lists, and strictness is
    /// honoured exactly at the bound value.
    #[test]
    fn range_bounds_union_the_ordered_prefix() {
        let mut idx = SubqueryShapeIndex::default();
        idx.insert(
            "gt10",
            "issue",
            &CompiledPredicate::And(vec![cmp_pred(0, LeafOp::Gt, 10), in_leaf(1)]),
        );
        idx.insert(
            "gte10",
            "issue",
            &CompiledPredicate::And(vec![cmp_pred(0, LeafOp::Gte, 10), in_leaf(1)]),
        );
        idx.insert(
            "gt20",
            "issue",
            &CompiledPredicate::And(vec![cmp_pred(0, LeafOp::Gt, 20), in_leaf(1)]),
        );
        idx.insert(
            "lt10",
            "issue",
            &CompiledPredicate::And(vec![cmp_pred(0, LeafOp::Lt, 10), in_leaf(1)]),
        );
        idx.insert(
            "lte10",
            "issue",
            &CompiledPredicate::And(vec![cmp_pred(0, LeafOp::Lte, 10), in_leaf(1)]),
        );

        // Exactly at 10: `> 10` is false, `>= 10` true; `< 10` false, `<= 10` true.
        assert_eq!(
            sorted(idx.candidates("issue", &[Tup2(row2(10, 0), 1)])),
            vec!["gte10".to_string(), "lte10".to_string()],
            "strict bounds must not fire at the bound value itself"
        );
        // At 15: both lower bounds at 10 fire, the one at 20 does not; upper bounds do not.
        assert_eq!(
            sorted(idx.candidates("issue", &[Tup2(row2(15, 0), 1)])),
            vec!["gt10".to_string(), "gte10".to_string()]
        );
        // At 5: only the upper bounds.
        assert_eq!(
            sorted(idx.candidates("issue", &[Tup2(row2(5, 0), 1)])),
            vec!["lt10".to_string(), "lte10".to_string()]
        );
        // A NULL cell never satisfies a comparison — no bound may fire.
        let null_row = Row(vec![Value::Null, Value::Int(0)]);
        assert!(
            idx.candidates("issue", &[Tup2(null_row, 1)]).is_empty(),
            "cmp against a NULL cell is UNKNOWN, never TRUE"
        );
    }

    /// Removal is exact: every bit the shape set is erased (eq, bounds and scan alike), the table
    /// bucket is reclaimed when it empties, and the freed interned id is safe to re-mint — a
    /// re-used id must never resurrect the removed shape's postings.
    #[test]
    fn remove_erases_every_posting_and_frees_the_id() {
        let mut idx = SubqueryShapeIndex::default();
        idx.insert("eq", "issue", &outer_pred(1));
        idx.insert(
            "bound",
            "issue",
            &CompiledPredicate::And(vec![cmp_pred(0, LeafOp::Gt, 0), in_leaf(1)]),
        );
        idx.insert("scan", "issue", &CompiledPredicate::MatchAll);
        assert_eq!(idx.len(), 3);

        idx.remove("eq");
        idx.remove("bound");
        idx.remove("scan");
        assert_eq!(idx.len(), 0);
        assert!(
            idx.candidates("issue", &[Tup2(row2(1, 0), 1)]).is_empty(),
            "removal must be total"
        );
        assert!(
            idx.tables.is_empty(),
            "an emptied table bucket is reclaimed"
        );
        idx.remove("never_seen"); // no-op, must not panic

        // Re-mint into the freed ids: the new shapes must answer only for their own conjuncts.
        idx.insert("fresh", "issue", &outer_pred(2));
        assert!(
            idx.candidates("issue", &[Tup2(row2(1, 0), 1)]).is_empty(),
            "a re-used id must not resurrect old postings"
        );
        assert_eq!(
            idx.candidates("issue", &[Tup2(row2(2, 0), 1)]),
            vec!["fresh".to_string()]
        );
    }

    /// Re-inserting a live shape id (defensive: a create that somehow re-registers) re-files it
    /// rather than leaving a stale posting behind.
    #[test]
    fn reinsert_replaces_the_previous_placement() {
        let mut idx = SubqueryShapeIndex::default();
        idx.insert("s", "issue", &outer_pred(1));
        idx.insert("s", "issue", &outer_pred(2));
        assert_eq!(idx.len(), 1);
        assert!(
            idx.candidates("issue", &[Tup2(row2(1, 0), 1)]).is_empty(),
            "the stale posting is gone"
        );
        assert_eq!(
            idx.candidates("issue", &[Tup2(row2(2, 0), 1)]),
            vec!["s".to_string()]
        );
    }

    /// The `bytes_subquery_registry` term: zero when empty, non-zero once populated.
    #[test]
    fn heap_bytes_zero_when_empty_and_grows() {
        let mut idx = SubqueryShapeIndex::default();
        assert_eq!(idx.heap_bytes(), 0, "an empty index owns no heap");
        for k in 0..200 {
            idx.insert(&format!("s{k}"), "issue", &outer_pred(k));
        }
        assert!(
            idx.heap_bytes() > 0,
            "a populated index owns measurable heap"
        );
    }
}
