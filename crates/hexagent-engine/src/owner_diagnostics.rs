//! One slow-record producer per physical owner, one existing background reader.
//! No MPSC cursor contention with other owners or router health publication.
//! 64 full records cover >=32 seconds at the >=500ms slow-command threshold.
//! Full/disconnected remain explicit loss: bounded storage cannot retain an
//! indefinitely stopped consumer, and diagnostics must never block trading.
use hexagent_runtime::{http1_pool::{PooledConnectionSnapshot, Role}, poll_channel};
use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering}};

pub(super) const CAPACITY: usize = 64;

#[derive(Debug)]
struct SlowRecord {
    elapsed_ns: u64,
    connection: PooledConnectionSnapshot,
}

// Not Clone. Engine ownership permits only the physical lane owner to publish.
pub(super) struct Publisher {
    tx: poll_channel::Sender<SlowRecord>,
    ready: Arc<AtomicBool>,
    full: AtomicU64,
    disconnected: AtomicU64,
    high_water: AtomicUsize,
}

pub(super) struct Registry {
    pub tx: crossbeam_channel::Sender<Lane>,
    pub ready: Arc<AtomicBool>,
}

pub(super) struct Lane {
    account: Arc<str>,
    role: Role,
    slot: usize,
    rx: poll_channel::Receiver<SlowRecord>,
}

#[derive(Default)]
pub(super) struct Stats {
    pub depth: usize,
    pub high_water: usize,
    pub full: u64,
    pub disconnected: u64,
}

pub(super) fn channel_with_ready(account: Arc<str>, role: Role, slot: usize,
    ready: Arc<AtomicBool>) -> (Publisher, Lane) {
    let (tx, rx) = poll_channel::bounded(CAPACITY);
    (Publisher { tx, ready, full: AtomicU64::new(0), disconnected: AtomicU64::new(0), high_water: AtomicUsize::new(0) },
     Lane { account, role, slot, rx })
}

#[cfg(test)]
fn channel(account: Arc<str>, role: Role, slot: usize) -> (Publisher, Lane) {
    channel_with_ready(account, role, slot, Arc::new(AtomicBool::new(false)))
}

impl Publisher {
    pub fn publish(&self, elapsed_ns: u64, connection: PooledConnectionSnapshot) -> bool {
        match self.tx.try_send(SlowRecord { elapsed_ns, connection }) {
            Ok(()) => {
                // Durable coalesced hint; no queue CAS contention and no
                // notification syscall. The existing background worker polls
                // this once, rather than probing every empty owner queue.
                self.ready.store(true, Ordering::Release);
                self.high_water.fetch_max(self.tx.len(), Ordering::Relaxed);
                true
            }
            Err(crossbeam_channel::TrySendError::Full(_)) => {
                // This channel has exactly one producer: no producer CAS race.
                // Includes a full ring whose consumer has not released its slot.
                self.full.fetch_add(1, Ordering::Relaxed);
                false
            }
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                self.disconnected.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    pub fn stats(&self) -> Stats {
        Stats { depth: self.tx.len(), high_water: self.high_water.load(Ordering::Relaxed),
                full: self.full.load(Ordering::Relaxed), disconnected: self.disconnected.load(Ordering::Relaxed) }
    }
}

impl Lane {
    // Bounded one-record turn per lane; no producer can starve another lane.
    pub fn log_one(&self) -> Result<bool, crossbeam_channel::RecvError> {
        match self.rx.try_recv() {
            Ok(r) => {
                log::warn!("[poly_connection_owner_slow] account={} role={:?} slot={} elapsed_ns={} pool_generation={} connect_generation={} peer={:?} selection_authority=router_generation_health",
                    self.account, self.role, self.slot, r.elapsed_ns, r.connection.pool_generation,
                    r.connection.connect_generation, r.connection.peer);
                Ok(true)
            }
            Err(crossbeam_channel::TryRecvError::Empty) => Ok(false),
            Err(crossbeam_channel::TryRecvError::Disconnected) => Err(crossbeam_channel::RecvError),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_lane_cannot_drop_another_owners_records_and_failure_is_observable() {
        let (a, ar) = channel("a".into(), Role::Fast, 1);
        let (b, br) = channel("b".into(), Role::Cancel, 2);
        for n in 0..CAPACITY as u64 {
            let connection = PooledConnectionSnapshot { pool_generation: n, ..Default::default() };
            assert!(a.publish(n, connection));
        }
        assert!(!a.publish(99, Default::default()));
        assert_eq!((a.stats().full, a.stats().high_water), (1, CAPACITY));
        assert!(b.publish(500_000_000, Default::default()));
        assert_eq!(br.rx.try_recv().unwrap().elapsed_ns, 500_000_000);
        assert_eq!((br.account.as_ref(), br.role, br.slot), ("b", Role::Cancel, 2));
        for n in 0..CAPACITY as u64 {
            let r = ar.rx.try_recv().unwrap();
            assert_eq!((r.elapsed_ns, r.connection.pool_generation), (n, n));
        }
        assert!(ar.rx.try_recv().is_err());
        assert!(a.publish(100, Default::default()), "draining restores capacity");
        drop(ar);
        assert!(!a.publish(101, Default::default()));
        assert_eq!((a.stats().full, a.stats().disconnected), (1, 1));
    }

    #[test]
    fn simultaneous_owner_burst_has_no_shared_cas_losses_or_duplicates() {
        const OWNERS: usize = 32;
        let ready = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(std::sync::Barrier::new(OWNERS));
        let mut readers = Vec::new();
        let mut handles = Vec::new();
        for slot in 0..OWNERS {
            let (tx, rx) = channel_with_ready(format!("account-{slot}").into(), Role::Fast, slot, ready.clone());
            readers.push(rx);
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for n in 0..CAPACITY as u64 { assert!(tx.publish(n, Default::default())); }
                assert_eq!(tx.stats().full + tx.stats().disconnected, 0);
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert!(ready.swap(false, Ordering::AcqRel));
        for (slot, rx) in readers.into_iter().enumerate() {
            assert_eq!(rx.slot, slot);
            for n in 0..CAPACITY as u64 { assert_eq!(rx.rx.try_recv().unwrap().elapsed_ns, n); }
            assert!(matches!(rx.rx.try_recv(), Err(crossbeam_channel::TryRecvError::Disconnected)));
        }
    }

    #[test]
    #[ignore = "offline contended diagnostic publication, no HTTP or credentials"]
    fn diagnostic_publication_burst_benchmark() {
        use super::super::ExecutionDiagnostic;
        const OWNERS: usize = 32;
        const BURST: usize = 16; // 512 < shared capacity 1024: failures cannot be full.
        const ROUNDS: usize = 100;
        for dedicated in [false, true] {
            let ready = Arc::new(AtomicBool::new(false));
            let (shared, reader) = poll_channel::bounded(1024);
            let barrier = Arc::new(std::sync::Barrier::new(OWNERS + 1));
            let mut readers = Vec::new();
            let mut handles = Vec::new();
            for slot in 0..OWNERS {
                let account: Arc<str> = format!("owner-{slot}").into();
                let (publisher, lane) = channel_with_ready(account.clone(), Role::Fast, slot, ready.clone());
                readers.push(lane);
                let (shared, barrier) = (shared.clone(), barrier.clone());
                handles.push(std::thread::spawn(move || {
                    let mut rows = Vec::with_capacity(ROUNDS * BURST);
                    for _ in 0..ROUNDS {
                        barrier.wait();
                        for _ in 0..BURST {
                            let start = std::time::Instant::now();
                            let ok = if dedicated {
                                publisher.publish(500_000_000, Default::default())
                            } else {
                                shared.try_send(ExecutionDiagnostic::OwnerSlow {
                                    account: account.clone(), role: Role::Fast, slot,
                                    elapsed_ns: 500_000_000, connection: Default::default(),
                                }).is_ok()
                            };
                            rows.push((start.elapsed().as_nanos() as u64, ok));
                        }
                        barrier.wait();
                        barrier.wait();
                    }
                    rows
                }));
            }
            let mut delivered = 0;
            let mut high_water = 0;
            for _ in 0..ROUNDS {
                barrier.wait();
                barrier.wait();
                if dedicated {
                    for lane in &readers {
                        high_water = high_water.max(lane.rx.len());
                        while lane.rx.try_recv().is_ok() { delivered += 1; }
                    }
                } else {
                    high_water = high_water.max(reader.len());
                    while reader.try_recv().is_ok() { delivered += 1; }
                }
                barrier.wait();
            }
            let rows: Vec<_> = handles.into_iter().flat_map(|h| h.join().unwrap()).collect();
            let dropped = rows.iter().filter(|(_, ok)| !ok).count();
            assert_eq!(rows.len(), delivered + dropped);
            if dedicated { assert_eq!(dropped, 0); }
            let mut ns: Vec<_> = rows.iter().map(|(ns, _)| *ns).collect();
            ns.sort_unstable();
            let n = ns.len();
            println!("diagnostic_publication dedicated={dedicated} n={n} delivered={delivered} dropped={dropped} high_water={high_water} capacity={} median_ns={} p99_ns={} p999_ns={} max_ns={}",
                if dedicated { CAPACITY } else { 1024 }, ns[n/2], ns[(n*99).div_ceil(100)-1],
                ns[(n*999).div_ceil(1000)-1], ns[n-1]);
            if let Ok(prefix) = std::env::var("HEX_DIAGNOSTIC_BENCH_OUT") {
                use std::io::Write;
                let mut f = std::io::BufWriter::new(std::fs::File::create(format!("{prefix}-{dedicated}.csv")).unwrap());
                writeln!(f, "ns,ok").unwrap();
                for (ns, ok) in rows { writeln!(f, "{ns},{ok}").unwrap(); }
            }
        }
    }
}
