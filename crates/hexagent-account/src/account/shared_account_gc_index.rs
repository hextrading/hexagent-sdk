//! Derived, instance-owned lookup state. Never persisted or authoritative.
//! Insertion/replacement/removal update both indexes; startup aggregate restore
//! rebuilds them via FromIterator. Mutable row access may update status/economics
//! only: token and client-order identity change by replacing the whole row.
use super::{AppliedTrade, OrderOwnership};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::{Bound, Deref};
use std::sync::Arc;

pub(super) trait IndexedRow {
    fn token(&self) -> &str;
    fn order_key(&self) -> Option<&str> {
        None
    }
}
impl IndexedRow for OrderOwnership {
    fn token(&self) -> &str {
        &self.token_id
    }
}
impl IndexedRow for AppliedTrade {
    fn token(&self) -> &str {
        &self.ownership.token_id
    }
    fn order_key(&self) -> Option<&str> {
        Some(&self.ownership.client_order_id)
    }
}

#[derive(Debug)]
pub(super) struct TokenIndexedRows<V> {
    rows: HashMap<String, V>,
    by_token: HashMap<String, BTreeSet<Arc<str>>>,
    by_order: HashMap<String, HashSet<Arc<str>>>,
    /// GC-only cursors rotate past protected entries, so an ineligible prefix
    /// cannot starve terminal rows later in the same token. No live iterators
    /// cross a private insertion, replay, or cold-owner turn.
    cursors: HashMap<String, Arc<str>>,
    scan_remaining: HashMap<String, usize>,
}
impl<V> Default for TokenIndexedRows<V> {
    fn default() -> Self {
        Self {
            rows: HashMap::new(),
            by_token: HashMap::new(),
            by_order: HashMap::new(),
            cursors: HashMap::new(),
            scan_remaining: HashMap::new(),
        }
    }
}
impl<V> Deref for TokenIndexedRows<V> {
    type Target = HashMap<String, V>;
    fn deref(&self) -> &Self::Target {
        &self.rows
    }
}
impl<V: IndexedRow> TokenIndexedRows<V> {
    pub(super) fn insert(&mut self, key: String, value: V) -> Option<V> {
        if self
            .rows
            .get(&key)
            .is_some_and(|old| old.token() == value.token() && old.order_key() == value.order_key())
        {
            return self.rows.insert(key, value);
        }
        let previous = self.remove(&key);
        let indexed_key: Arc<str> = Arc::from(key.as_str());
        self.by_token
            .entry(value.token().to_owned())
            .or_default()
            .insert(indexed_key.clone());
        if let Some(order) = value.order_key() {
            self.by_order
                .entry(order.to_owned())
                .or_default()
                .insert(indexed_key);
        }
        self.rows.insert(key, value);
        previous
    }
    pub(super) fn remove(&mut self, key: &str) -> Option<V> {
        let row = self.rows.remove(key)?;
        if let Some(keys) = self.by_token.get_mut(row.token()) {
            keys.remove(key);
            if keys.is_empty() {
                self.by_token.remove(row.token());
                self.cursors.remove(row.token());
                self.scan_remaining.remove(row.token());
            }
        }
        if let Some(order) = row.order_key() {
            if let Some(keys) = self.by_order.get_mut(order) {
                keys.remove(key);
                if keys.is_empty() {
                    self.by_order.remove(order);
                }
            }
        }
        Some(row)
    }
    pub(super) fn get_mut(&mut self, key: &str) -> Option<&mut V> {
        self.rows.get_mut(key)
    }
    pub(super) fn replace_rows(&mut self, rows: impl Iterator<Item = (String, V)>) {
        let mut next: Self = rows.collect();
        next.cursors = std::mem::take(&mut self.cursors);
        next.cursors
            .retain(|token, _| next.by_token.contains_key(token));
        next.scan_remaining = std::mem::take(&mut self.scan_remaining);
        next.scan_remaining
            .retain(|token, _| next.by_token.contains_key(token));
        *self = next;
    }
    pub(super) fn rows_for_order<'a>(
        &'a self,
        coid: &'a str,
    ) -> impl Iterator<Item = (&'a str, &'a V)> {
        self.by_order
            .get(coid)
            .into_iter()
            .flat_map(|keys| keys.iter())
            .filter_map(|key| self.rows.get(key.as_ref()).map(|row| (key.as_ref(), row)))
    }
    pub(super) fn has_tokens(&self, tokens: &HashSet<String>) -> bool {
        tokens.iter().any(|token| self.by_token.contains_key(token))
    }
    pub(super) fn scan_incomplete(&self, tokens: &HashSet<String>) -> bool {
        tokens.iter().any(|token| {
            self.scan_remaining
                .get(token)
                .is_some_and(|remaining| *remaining > 0)
        })
    }
    /// Scan at most `budget` candidates over the requested tokens. Each token
    /// receives a share, with a rotating per-token cursor. Returned Arc keys
    /// remain valid after row removal and avoid copying coids in the scan.
    pub(super) fn scan_keys(&mut self, tokens: &HashSet<String>, budget: usize) -> Vec<Arc<str>> {
        let mut out = Vec::with_capacity(budget);
        if !self.scan_incomplete(tokens) {
            for token in tokens {
                if let Some(keys) = self.by_token.get(token) {
                    self.scan_remaining.insert(token.clone(), keys.len());
                }
            }
        }
        let per_token = (budget / tokens.len().max(1)).max(1);
        for token in tokens {
            if out.len() == budget {
                break;
            }
            let Some(keys) = self.by_token.get(token) else {
                continue;
            };
            let pending = self
                .scan_remaining
                .get(token)
                .copied()
                .unwrap_or(keys.len());
            if pending == 0 {
                continue;
            }
            let limit = per_token.min(budget - out.len()).min(pending);
            let start = out.len();
            if let Some(cursor) = self.cursors.get(token) {
                out.extend(
                    keys.range::<str, _>((Bound::Excluded(cursor.as_ref()), Bound::Unbounded))
                        .take(limit)
                        .cloned(),
                );
                let remaining = limit - (out.len() - start);
                if remaining > 0 {
                    out.extend(
                        keys.range::<str, _>((Bound::Unbounded, Bound::Included(cursor.as_ref())))
                            .take(remaining)
                            .cloned(),
                    );
                }
            } else {
                out.extend(keys.iter().take(limit).cloned());
            }
            let remaining = self.scan_remaining.entry(token.clone()).or_insert(0);
            *remaining = remaining.saturating_sub(out.len() - start);
            if out.len() > start {
                self.cursors
                    .insert(token.clone(), out.last().unwrap().clone());
            }
        }
        out
    }
}
impl<V: IndexedRow> FromIterator<(String, V)> for TokenIndexedRows<V> {
    fn from_iter<T: IntoIterator<Item = (String, V)>>(iter: T) -> Self {
        let mut rows = Self::default();
        for (key, value) in iter {
            rows.insert(key, value);
        }
        rows
    }
}
