//! In-process caches: immutable metadata files (manifest lists, manifests, last-value indexes,
//! Parquet footers) by path, and recently loaded table versions by name.

use std::borrow::Borrow;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Instant;

use arrow::array::RecordBatch;
use bytes::Bytes;
use iceberg::spec::ManifestFile;

use crate::name::TableName;
use crate::table::{LiveEntry, Table};

/// The caches of one [`Lake`](crate::Lake).
#[derive(Debug)]
pub(crate) struct Caches {
    pub(crate) manifest_lists: BoundedCache<String, Arc<Vec<ManifestFile>>>,
    pub(crate) manifests: BoundedCache<String, Arc<Vec<LiveEntry>>>,
    pub(crate) last_value_indexes: BoundedCache<String, RecordBatch>,
    /// Footers of data files by file URI, encoded (see `footer.rs`).
    pub(crate) footers: BoundedCache<String, Bytes>,
    /// Table versions this process loaded or committed, with when (see `Lake::table_recent`).
    pub(crate) tables: BoundedCache<TableName, (Instant, Table)>,
    /// Heads (by URI) this process deleted and hasn't written since: not deleted again.
    pub(crate) deleted_heads: BoundedCache<String, ()>,
}

impl Default for Caches {
    fn default() -> Self {
        Self {
            manifest_lists: BoundedCache::new(1024),
            manifests: BoundedCache::new(8192),
            last_value_indexes: BoundedCache::new(64),
            footers: BoundedCache::new(8192),
            tables: BoundedCache::new(1024),
            deleted_heads: BoundedCache::new(1024),
        }
    }
}

/// A map of at most `capacity` entries; inserting a new key into a full map evicts the oldest one.
#[derive(Debug)]
pub(crate) struct BoundedCache<K, V> {
    capacity: usize,
    state: Mutex<State<K, V>>,
}

#[derive(Debug)]
struct State<K, V> {
    map: HashMap<K, V>,
    /// Keys in insertion order.
    order: VecDeque<K>,
}

impl<K: Eq + Hash + Clone, V: Clone> BoundedCache<K, V> {
    pub(crate) fn new(capacity: usize) -> Self {
        let state = State { map: HashMap::new(), order: VecDeque::new() };
        Self { capacity: capacity.max(1), state: Mutex::new(state) }
    }

    pub(crate) fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.lock().map.get(key).cloned()
    }

    pub(crate) fn insert(&self, key: K, value: V) {
        self.insert_unless(key, value, |_| false);
    }

    /// Inserts unless the cached value for `key` satisfies `keep`.
    pub(crate) fn insert_unless(&self, key: K, value: V, keep: impl FnOnce(&V) -> bool) {
        let mut state = self.lock();
        let State { map, order } = &mut *state;
        if let Some(old) = map.get_mut(&key) {
            if !keep(old) {
                *old = value;
            }
            return;
        }
        while map.len() >= self.capacity
            && let Some(oldest) = order.pop_front()
        {
            map.remove(&oldest);
        }
        order.push_back(key.clone());
        map.insert(key, value);
    }

    pub(crate) fn remove<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut state = self.lock();
        if state.map.remove(key).is_some() {
            state.order.retain(|k| k.borrow() != key);
        }
    }

    fn lock(&self) -> MutexGuard<'_, State<K, V>> {
        // The state is consistent between statements, so a panic elsewhere doesn't poison it.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evicts_the_oldest_entry() {
        let c = BoundedCache::new(2);
        c.insert("a", 1);
        c.insert("b", 2);
        c.insert("a", 10); // an update doesn't change the order
        c.insert("c", 3);
        assert_eq!((c.get("a"), c.get("b"), c.get("c")), (None, Some(2), Some(3)));
    }

    #[test]
    fn insert_unless_keeps_matching_values() {
        let c = BoundedCache::new(4);
        c.insert("t", 5);
        c.insert_unless("t", 3, |old| *old > 3);
        assert_eq!(c.get("t"), Some(5));
        c.insert_unless("t", 7, |old| *old > 7);
        assert_eq!(c.get("t"), Some(7));
    }

    #[test]
    fn remove_frees_a_slot() {
        let c = BoundedCache::new(2);
        c.insert("a", 1);
        c.insert("b", 2);
        c.remove("a");
        c.insert("c", 3);
        assert_eq!((c.get("a"), c.get("b"), c.get("c")), (None, Some(2), Some(3)));
    }
}
