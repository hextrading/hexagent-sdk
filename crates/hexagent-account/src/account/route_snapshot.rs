//! One RCU-published route shard. A single-key mutation copies 64 leaf pointers
//! and only the affected leaf, rather than every retained route in the shard.
//! The outer account route index still provides the existing RCU writer fence;
//! readers hold immutable snapshots and GC retains its bounded cold retirement.
use super::*;

const LEAVES: usize = 64;
type Leaf = HashMap<Arc<str>, Arc<str>>;

#[derive(Debug, Clone)]
pub(super) struct RouteSnapshot {
    leaves: [Arc<Leaf>; LEAVES],
}

impl Default for RouteSnapshot {
    fn default() -> Self {
        let empty = Arc::new(HashMap::new());
        Self {
            leaves: std::array::from_fn(|_| Arc::clone(&empty)),
        }
    }
}

impl RouteSnapshot {
    fn leaf_index(key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        // Low bits select the outer shard. Use different bits for its leaf.
        (hasher.finish() as usize / ROUTE_SHARD_COUNT) % LEAVES
    }

    pub(super) fn get(&self, key: &str) -> Option<&Arc<str>> {
        self.leaves[Self::leaf_index(key)].get(key)
    }

    pub(super) fn get_hashed(&self, key: &str, hash: usize) -> Option<&Arc<str>> {
        self.leaves[(hash / ROUTE_SHARD_COUNT) % LEAVES].get(key)
    }

    #[cfg(test)]
    pub(super) fn get_key_value(&self, key: &str) -> Option<(&Arc<str>, &Arc<str>)> {
        self.leaves[Self::leaf_index(key)].get_key_value(key)
    }

    pub(super) fn contains_key(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    pub(super) fn insert(&mut self, key: Arc<str>, owner: Arc<str>) {
        let leaf = &mut self.leaves[Self::leaf_index(&key)];
        Arc::make_mut(leaf).insert(key, owner);
    }

    pub(super) fn remove(&mut self, key: &str) {
        let leaf = &mut self.leaves[Self::leaf_index(key)];
        if leaf.contains_key(key) {
            Arc::make_mut(leaf).remove(key);
        }
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&Arc<str>, &Arc<str>)> {
        self.leaves.iter().flat_map(|leaf| leaf.iter())
    }

    pub(super) fn keys(&self) -> impl Iterator<Item = &Arc<str>> {
        self.leaves.iter().flat_map(|leaf| leaf.keys())
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.leaves.iter().map(|leaf| leaf.len()).sum()
    }

    pub(super) fn retain(&mut self, mut keep: impl FnMut(&Arc<str>, &Arc<str>) -> bool) {
        for leaf in &mut self.leaves {
            if leaf.iter().any(|(key, owner)| !keep(key, owner)) {
                Arc::make_mut(leaf).retain(|key, owner| keep(key, owner));
            }
        }
    }
}

impl FromIterator<(Arc<str>, Arc<str>)> for RouteSnapshot {
    fn from_iter<T: IntoIterator<Item = (Arc<str>, Arc<str>)>>(rows: T) -> Self {
        let mut leaves: [Leaf; LEAVES] = std::array::from_fn(|_| HashMap::new());
        for (key, owner) in rows {
            leaves[Self::leaf_index(&key)].insert(key, owner);
        }
        Self {
            leaves: leaves.map(Arc::new),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutation_preserves_held_generations_and_shares_unrelated_leaves() {
        let first: RouteSnapshot = (0..20_000)
            .map(|i| (Arc::from(format!("key-{i}")), Arc::from("owner")))
            .collect();
        let mut second = first.clone();
        second.insert(Arc::from("key-0"), Arc::from("rebound"));
        let changed = RouteSnapshot::leaf_index("key-0");
        for i in 0..LEAVES {
            assert_eq!(
                Arc::ptr_eq(&first.leaves[i], &second.leaves[i]),
                i != changed
            );
        }
        assert_eq!(first.get("key-0").unwrap().as_ref(), "owner");
        second.remove("key-0");
        assert!(second.get("key-0").is_none());
        assert_eq!(first.get("key-0").unwrap().as_ref(), "owner");
        assert_eq!(second.iter().count(), 19_999);
    }
}
