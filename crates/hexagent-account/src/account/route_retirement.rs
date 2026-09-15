//! Bounded ownership transfer of obsolete GC route snapshots to the existing
//! cold account owner. Credits include its pending reader-held batch, so a
//! stalled reader/consumer delays GC before mutation instead of growing memory
//! or moving a large destructor back onto a private lifecycle/quote thread.
use super::*;

const BATCH_CAPACITY: usize = 8;
const SNAPSHOTS_PER_BATCH: usize =
    2 * SETTLED_GC_ORDERS_PER_OWNER_TURN + 2 * SETTLED_GC_TRADES_PER_OWNER_TURN;
type Snapshot = Arc<HashMap<Arc<str>, Arc<str>>>;

#[derive(Debug)]
pub(super) struct RetiredRouteBatch {
    snapshots: [Option<Snapshot>; SNAPSHOTS_PER_BATCH],
    len: usize,
    enqueued_at: crate::latency::Instant,
}

impl RetiredRouteBatch {
    fn new() -> Self {
        Self {
            snapshots: std::array::from_fn(|_| None),
            len: 0,
            enqueued_at: crate::latency::Instant::now(),
        }
    }
}

#[derive(Debug)]
pub(super) struct RouteRetirementQueue {
    tx: crossbeam_channel::Sender<RetiredRouteBatch>,
    // Kept connected for the account's lifetime. Only its non-cloneable cold
    // owner capability polls it; producers cannot lose the receiver mid-GC.
    rx: crossbeam_channel::Receiver<RetiredRouteBatch>,
    outstanding: AtomicUsize,
    high_water: AtomicUsize,
    backpressure: AtomicU64,
    reclaimed: AtomicU64,
}

pub(super) struct RouteRetirementPermit<'a> {
    queue: &'a RouteRetirementQueue,
    batch: Option<RetiredRouteBatch>,
}

impl RouteRetirementQueue {
    pub(super) fn new() -> Self {
        let (tx, rx) = crossbeam_channel::bounded(BATCH_CAPACITY);
        Self {
            tx,
            rx,
            outstanding: AtomicUsize::new(0),
            high_water: AtomicUsize::new(0),
            backpressure: AtomicU64::new(0),
            reclaimed: AtomicU64::new(0),
        }
    }

    /// Called under the existing GC control gate, before deleting any state.
    /// A credit reserves channel capacity through publication and reclamation.
    pub(super) fn try_reserve(&self) -> Option<RouteRetirementPermit<'_>> {
        let previous = self
            .outstanding
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < BATCH_CAPACITY).then_some(n + 1)
            });
        let Ok(previous) = previous else {
            self.backpressure.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        self.high_water.fetch_max(previous + 1, Ordering::Relaxed);
        Some(RouteRetirementPermit {
            queue: self,
            batch: Some(RetiredRouteBatch::new()),
        })
    }

    /// At most one batch per cold-owner turn. Old readers retain an immutable
    /// snapshot; wait for them without blocking so their final Arc drop cannot
    /// become a full HashMap destructor on the reader's critical lane.
    pub(super) fn reclaim(&self, pending: &mut Option<RetiredRouteBatch>) {
        if pending.is_none() {
            *pending = self.rx.try_recv().ok();
        }
        let Some(batch) = pending.as_mut() else {
            return;
        };
        let started = crate::latency::Instant::now();
        for snapshot in &mut batch.snapshots[..batch.len] {
            if snapshot
                .as_ref()
                .is_some_and(|snapshot| Arc::strong_count(snapshot) == 1)
            {
                drop(snapshot.take());
            }
        }
        let complete = batch.snapshots[..batch.len].iter().all(Option::is_none);
        crate::latency::record("polymarket.account.route_reclaim.cold_drop", started);
        if complete {
            crate::latency::record(
                "polymarket.account.route_reclaim.retirement_age",
                batch.enqueued_at,
            );
            *pending = None;
            self.reclaimed.fetch_add(1, Ordering::Relaxed);
            self.outstanding.fetch_sub(1, Ordering::Release);
        }
    }

    pub(super) fn metrics(&self) -> (usize, usize, u64, u64) {
        (
            self.outstanding.load(Ordering::Acquire),
            self.high_water.load(Ordering::Relaxed),
            self.backpressure.load(Ordering::Relaxed),
            self.reclaimed.load(Ordering::Relaxed),
        )
    }
}

impl RouteRetirementPermit<'_> {
    pub(super) fn push(&mut self, snapshot: Snapshot) {
        let batch = self.batch.as_mut().expect("live GC retirement permit");
        // Four GC route maps can touch at most two shards per retired order
        // and two per retired trade. No retry extends the returned old table set.
        assert!(
            batch.len < SNAPSHOTS_PER_BATCH,
            "GC route batch exceeded deletion budget"
        );
        batch.snapshots[batch.len] = Some(snapshot);
        batch.len += 1;
    }
}

impl Drop for RouteRetirementPermit<'_> {
    fn drop(&mut self) {
        let Some(batch) = self.batch.take() else {
            return;
        };
        if batch.len == 0 {
            self.queue.outstanding.fetch_sub(1, Ordering::Release);
        } else {
            // Credit is counted before mutation; pending consumer batches also
            // keep their credit. The private receiver is never disconnected.
            // Thus every issued permit has one vacant channel slot here.
            assert!(
                self.queue.tx.try_send(batch).is_ok(),
                "reserved route retirement slot unavailable"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retirement_is_bounded_including_reader_held_batch() {
        let queue = RouteRetirementQueue::new();
        let mut held = Vec::new();
        for _ in 0..BATCH_CAPACITY {
            let snapshot = Arc::new(HashMap::new());
            held.push(Arc::clone(&snapshot));
            queue.try_reserve().unwrap().push(snapshot);
        }
        assert!(queue.try_reserve().is_none());
        let mut pending = None;
        queue.reclaim(&mut pending);
        assert!(pending.is_some());
        assert!(
            queue.try_reserve().is_none(),
            "reader-held batch must retain its credit"
        );
        drop(held);
        for _ in 0..BATCH_CAPACITY {
            queue.reclaim(&mut pending);
        }
        assert!(pending.is_none());
        assert_eq!(
            queue.metrics(),
            (0, BATCH_CAPACITY, 2, BATCH_CAPACITY as u64)
        );
        assert!(queue.try_reserve().is_some());
    }

    #[test]
    fn retirement_preserves_old_readers_and_rebound_ownership() {
        let routes = ShardedRouteMap::new();
        let queue = RouteRetirementQueue::new();
        routes.insert("key".into(), "old-owner".into());
        let old = routes.shards[ShardedRouteMap::shard_index("key")]
            .published
            .load_full();
        {
            let mut permit = queue.try_reserve().unwrap();
            routes.apply_batch_retiring(
                "old-owner",
                &[],
                &[("key".into(), "new-owner".into())],
                Some(&mut permit),
            );
        }
        assert_eq!(routes.get("key").as_deref(), Some("new-owner"));
        assert_eq!(old.get("key").unwrap().as_ref(), "old-owner");
        let mut pending = None;
        queue.reclaim(&mut pending);
        assert!(pending.is_some());
        drop(old);
        queue.reclaim(&mut pending);
        assert!(pending.is_none());
        assert_eq!(queue.metrics().0, 0);
        routes.apply_batch("old-owner", &["key".into()], &[]);
        assert_eq!(routes.get("key").as_deref(), Some("new-owner"));
    }

    #[test]
    fn unused_credit_and_separate_accounts_are_independent() {
        let first = RouteRetirementQueue::new();
        let second = RouteRetirementQueue::new();
        let credits: Vec<_> = (0..BATCH_CAPACITY)
            .map(|_| first.try_reserve().unwrap())
            .collect();
        assert!(first.try_reserve().is_none());
        assert!(second.try_reserve().is_some());
        drop(credits);
        assert_eq!(first.metrics().0, 0);
        assert_eq!(second.metrics().0, 0);
    }

    #[test]
    fn retirement_pays_arcswap_load_guard_before_cold_reclamation() {
        let routes = ShardedRouteMap::new();
        let queue = RouteRetirementQueue::new();
        routes.insert("key".into(), "owner".into());
        let shard = &routes.shards[ShardedRouteMap::shard_index("key")];
        // This is the production read path: an ArcSwap debt-backed Guard,
        // not a load_full() Arc whose strong count is already paid.
        let reader = shard.published.load();
        {
            let mut permit = queue.try_reserve().unwrap();
            routes.apply_batch_retiring("owner", &["key".into()], &[], Some(&mut permit));
        }
        assert!(routes.get("key").is_none());
        let mut pending = None;
        queue.reclaim(&mut pending);
        assert!(
            pending.is_some(),
            "an outstanding load() guard must retain the old table"
        );
        assert_eq!(reader.get("key").unwrap().as_ref(), "owner");
        assert_eq!(queue.metrics().0, 1);
        drop(reader);
        queue.reclaim(&mut pending);
        assert!(pending.is_none());
        assert_eq!(queue.metrics().0, 0);
    }

    #[test]
    fn noop_does_not_retire_a_still_published_snapshot_or_hold_credit() {
        let routes = ShardedRouteMap::new();
        let queue = RouteRetirementQueue::new();
        routes.insert("key".into(), "owner".into());
        let shard = &routes.shards[ShardedRouteMap::shard_index("key")];
        let before = shard.published.load_full();
        for _ in 0..BATCH_CAPACITY * 2 {
            let mut permit = queue.try_reserve().expect("no-op must return its credit");
            routes.apply_batch_retiring(
                "different-owner",
                &["key".into()],
                &[("key".into(), "owner".into())],
                Some(&mut permit),
            );
            assert_eq!(permit.batch.as_ref().unwrap().len, 0);
        }
        assert!(Arc::ptr_eq(&before, &shard.published.load_full()));
        assert_eq!(queue.rx.len(), 0);
        assert_eq!(queue.metrics(), (0, 1, 0, 0));
    }

    #[test]
    fn saturated_retirement_returns_busy_before_ledger_or_wal_mutation() {
        use super::super::tests::{
            install_test_settled_gc_candidate, settled_gc_benchmark_account,
        };

        let directory = std::env::temp_dir().join(format!(
            "hexagent-retirement-busy-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = directory.join("account.json");
        let mut account = settled_gc_benchmark_account();
        let tokens = HashSet::from(["SETTLED".into()]);
        install_test_settled_gc_candidate(&account, "settled", &tokens);
        let initial_state = account.state.lock().unwrap().clone();
        account.persistence = Some(
            AccountPersistence::start(
                path.clone(),
                account.account_id.clone(),
                initial_state,
                0,
                32,
            )
            .unwrap(),
        );
        let account = Arc::new(account);
        let (_, cold_owner) = account.bind_account_owner().unwrap();
        cold_owner.mark_current_thread().unwrap();
        let mailbox = account.register_settled_gc_owner("owner").unwrap();
        let request = SettledGcDeleteRequest {
            request_id: 1,
            registration_id: mailbox.registration_id,
            instance_id: "owner".into(),
            condition_id: "settled".into(),
            tokens: Arc::new(tokens),
            enqueued_at: crate::latency::Instant::now(),
        };
        let mut readers = Vec::new();
        for _ in 0..BATCH_CAPACITY {
            let snapshot = Arc::new(HashMap::new());
            readers.push(snapshot.clone());
            account
                .route_retirement
                .try_reserve()
                .unwrap()
                .push(snapshot);
        }
        cold_owner.reclaim_retired_routes();
        assert!(cold_owner.route_retirement_pending.borrow().is_some());
        let state_before = serde_json::to_value(&*account.state.lock().unwrap()).unwrap();
        let owner = account.virtual_account("owner").unwrap();
        let orders_before = serde_json::to_value(&*account.lifecycle(&owner).orders).unwrap();
        let trades_before = serde_json::to_value(&*account.lifecycle(&owner).trades).unwrap();
        let persistence = account.persistence.as_ref().unwrap();
        let generation_before = persistence.scheduled_generation();
        let wal_before = std::fs::read(persistence_wal_path(&path)).unwrap();
        let attempt = account.process_settled_gc_delete_request(request).unwrap();
        let SettledGcDeleteAttempt::Busy(request) = attempt else {
            panic!("full retirement credits must defer an eligible deletion");
        };
        assert_eq!(request.request_id, 1);
        assert_eq!(
            serde_json::to_value(&*account.state.lock().unwrap()).unwrap(),
            state_before
        );
        assert_eq!(
            serde_json::to_value(&*account.lifecycle(&owner).orders).unwrap(),
            orders_before
        );
        assert_eq!(
            serde_json::to_value(&*account.lifecycle(&owner).trades).unwrap(),
            trades_before
        );
        assert_eq!(persistence.scheduled_generation(), generation_before);
        assert_eq!(
            std::fs::read(persistence_wal_path(&path)).unwrap(),
            wal_before
        );
        assert_eq!(account.route_retirement.metrics().2, 1);

        drop(readers);
        for _ in 0..BATCH_CAPACITY {
            cold_owner.reclaim_retired_routes();
        }
        let result = account.process_settled_gc_delete_request(request).unwrap();
        let SettledGcDeleteAttempt::Completed(certificate) = result else {
            panic!("the retained request must progress when cold credits recover");
        };
        assert!(certificate.retired_orders > 0 && certificate.retired_trades > 0);
        assert!(persistence.scheduled_generation() > generation_before);
        cold_owner.reclaim_retired_routes();
        drop(cold_owner);
        drop(account);
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[cfg(test)]
#[path = "route_retirement_benchmark.rs"]
mod benchmark;
