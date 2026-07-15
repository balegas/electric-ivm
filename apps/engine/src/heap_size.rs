//! Lower-bound owned-heap accounting for the `/memory` breakdown. Estimates, not
//! allocator truth — the delta vs. phys footprint is the allocator/pinning term.
//!
//! `HeapSize::heap_bytes` sums only heap memory a value *owns* (`String`/`Vec` capacity,
//! hash-table backing storage), never `size_of::<Self>()` (that's already counted at the
//! container level: `Vec<T>::heap_bytes` multiplies capacity by `size_of::<T>()`, so a `T`'s
//! own `heap_bytes` must add only what's *beyond* its inline representation). Shared,
//! non-uniquely-owned data (`Arc<T>` payloads, `DsClient` handles, channel senders) is
//! deliberately not counted — undercounting shared refs keeps this a lower bound, never an
//! over-count from double-attribution.
use std::collections::{BTreeMap, HashMap, HashSet};

pub trait HeapSize {
    fn heap_bytes(&self) -> usize;
}

impl HeapSize for String {
    fn heap_bytes(&self) -> usize {
        self.capacity()
    }
}
impl<T: HeapSize> HeapSize for Vec<T> {
    fn heap_bytes(&self) -> usize {
        self.capacity() * std::mem::size_of::<T>() + self.iter().map(HeapSize::heap_bytes).sum::<usize>()
    }
}
impl<K: HeapSize, V: HeapSize> HeapSize for HashMap<K, V> {
    fn heap_bytes(&self) -> usize {
        let entry = std::mem::size_of::<(K, V)>() + 1; // +1 ctrl byte (swiss table)
        (self.capacity() * entry * 11) / 10 + self.iter().map(|(k, v)| k.heap_bytes() + v.heap_bytes()).sum::<usize>()
    }
}
impl<K: HeapSize> HeapSize for HashSet<K> {
    fn heap_bytes(&self) -> usize {
        let entry = std::mem::size_of::<K>() + 1;
        (self.capacity() * entry * 11) / 10 + self.iter().map(HeapSize::heap_bytes).sum::<usize>()
    }
}
impl<T: HeapSize> HeapSize for Option<T> {
    fn heap_bytes(&self) -> usize {
        self.as_ref().map_or(0, HeapSize::heap_bytes)
    }
}
impl<A: HeapSize, B: HeapSize> HeapSize for (A, B) {
    fn heap_bytes(&self) -> usize {
        self.0.heap_bytes() + self.1.heap_bytes()
    }
}
/// `BTreeMap` exposes no `capacity()` (no amortized bucket overhead to estimate, unlike the
/// swiss-table `HashMap` above); this counts each entry's own `size_of` plus its owned heap,
/// undercounting the tree's internal node/pointer overhead — an accepted lower-bound gap.
impl<K: HeapSize, V: HeapSize> HeapSize for BTreeMap<K, V> {
    fn heap_bytes(&self) -> usize {
        self.len() * std::mem::size_of::<(K, V)>()
            + self.iter().map(|(k, v)| k.heap_bytes() + v.heap_bytes()).sum::<usize>()
    }
}
/// A `Box<T>` owns exactly one heap allocation of `T`, plus whatever `T` itself owns.
impl<T: HeapSize> HeapSize for Box<T> {
    fn heap_bytes(&self) -> usize {
        std::mem::size_of::<T>() + (**self).heap_bytes()
    }
}
/// `serde_json::Value` shows up in predicate literals (`PredicateJson::Leaf.value`) and
/// aggregate output; walked field-by-field like any other owned tree. `Number`/`Bool`/`Null`
/// are inline (no heap); `Array`/`Object` recurse. Bucket overhead for `Object` is not
/// estimated (its map implementation is a serde_json internal, not part of the public API),
/// so this undercounts slightly relative to the `HashMap` impl above — an accepted, documented
/// lower-bound gap for what is normally small predicate-literal data anyway.
impl HeapSize for serde_json::Value {
    fn heap_bytes(&self) -> usize {
        match self {
            serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => 0,
            serde_json::Value::String(s) => s.heap_bytes(),
            serde_json::Value::Array(a) => {
                a.capacity() * std::mem::size_of::<serde_json::Value>()
                    + a.iter().map(HeapSize::heap_bytes).sum::<usize>()
            }
            serde_json::Value::Object(m) => {
                m.iter().map(|(k, v)| k.heap_bytes() + v.heap_bytes()).sum()
            }
        }
    }
}
// numeric/leaf impls: zero owned heap
macro_rules! leaf {
    ($($t:ty),*) => { $(impl HeapSize for $t { fn heap_bytes(&self) -> usize { 0 } })* }
}
leaf!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize, bool, f32, f64);

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn string_heap_bytes_is_capacity() {
        let s = String::from("hello");
        assert_eq!(s.heap_bytes(), s.capacity());
    }

    #[test]
    fn map_heap_bytes_counts_keys_values_and_buckets() {
        let mut m: HashMap<String, String> = HashMap::new();
        m.insert("k".repeat(100), "v".repeat(100));
        // at least the owned key+value heap; bucket overhead estimated at
        // 1.1 × capacity × entry size
        assert!(m.heap_bytes() >= 200);
    }
}
