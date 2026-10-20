//! Startup-allocated completion slots. Pool is owned by one connection worker;
//! endpoint ownership is transferred to its I/O task and completion consumer.
//! A slot is reusable only after BOTH endpoints AND metadata readers are gone.
//! Channels retain one value; generations cannot overlap and abandoned producer
//! tasks publish a typed failure. No lock, allocation, or wait on checkout.
use crossbeam_channel::{RecvError, RecvTimeoutError};
use std::sync::Arc;
struct Slot<T, M> {
    tx: crate::poll_channel::Sender<T>,
    rx: crate::poll_channel::Receiver<T>,
    metadata: Arc<M>,
}
pub struct Pool<T, M> {
    slots: Vec<Arc<Slot<T, M>>>,
    disconnected: fn() -> T,
}
pub struct Sender<T, M> {
    slot: Arc<Slot<T, M>>,
    sent: bool,
    disconnected: fn() -> T,
}
pub struct Receiver<T, M> {
    slot: Arc<Slot<T, M>>,
}
impl<T, M> Pool<T, M> {
    pub fn new(capacity: usize, metadata: impl Fn() -> M, disconnected: fn() -> T) -> Self {
        Self::with_wake(capacity, metadata, disconnected, None)
    }
    /// Startup-only: all slots on one reactor may share its single-consumer
    /// wake. Endpoint/metadata lifetime still prevents generation reuse.
    pub fn with_wake(capacity: usize, metadata: impl Fn() -> M, disconnected: fn() -> T,
        wake: Option<Arc<crate::wake::Wake>>) -> Self {
        Self {
            slots: (0..capacity)
                .map(|_| {
                    let (tx, rx) = crate::poll_channel::bounded_with_wake(1,
                        Some(wake.clone().unwrap_or_else(|| Arc::new(crate::wake::Wake::default()))));
                    Arc::new(Slot {
                        tx,
                        rx,
                        metadata: Arc::new(metadata()),
                    })
                })
                .collect(),
            disconnected,
        }
    }
    pub fn checkout(
        &mut self,
        reset: impl FnOnce(&M),
    ) -> Option<(Sender<T, M>, Receiver<T, M>, Arc<M>)> {
        let slot = self
            .slots
            .iter()
            .find(|slot| Arc::strong_count(slot) == 1 && Arc::strong_count(&slot.metadata) == 1)?;
        // Pair with the last endpoint/metadata Arc drop before resetting the
        // slot. Only this owner can create a new reference once count is one.
        std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
        // An abandoned receiver may leave a reply. No old endpoint remains.
        while slot.rx.try_recv().is_ok() {}
        reset(&slot.metadata);
        Some((
            Sender {
                slot: slot.clone(),
                sent: false,
                disconnected: self.disconnected,
            },
            Receiver { slot: slot.clone() },
            slot.metadata.clone(),
        ))
    }
}
impl<T, M> Sender<T, M> {
    pub fn try_send(mut self, value: T) -> Result<(), T> {
        self.sent = true;
        self.slot
            .tx
            .try_send(value)
            .map_err(|error| error.into_inner())
    }
}
impl<T, M> Drop for Sender<T, M> {
    fn drop(&mut self) {
        if !self.sent {
            let _ = self.slot.tx.try_send((self.disconnected)());
        }
    }
}
impl<T, M> Receiver<T, M> {
    pub fn try_recv(&self) -> Result<T, crossbeam_channel::TryRecvError> {
        self.slot.rx.try_recv()
    }
    pub fn recv(&self) -> Result<T, RecvError> {
        self.slot.rx.recv()
    }
    pub fn recv_with_control(&self, control: impl FnMut()) -> Result<T, RecvError> {
        self.slot.rx.recv_with_control(control)
    }
    pub fn recv_timeout(&self, timeout: std::time::Duration) -> Result<T, RecvTimeoutError> {
        self.slot.rx.recv_timeout(timeout)
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_notification_is_observed_before_old_reply_without_releasing_ownership() {
        let wake = Arc::new(crate::wake::Wake::event());
        let health = Arc::new(crate::wake::StateNotification::default());
        health.bind(wake.clone());
        let mut pool = Pool::with_wake(1, || (), || -1, Some(wake));
        let (tx, rx, metadata) = pool.checkout(|_| {}).unwrap();
        let (seen_tx, seen_rx) = crossbeam_channel::bounded(1);
        let h = health.clone();
        let owner = std::thread::spawn(move || {
            rx.recv_with_control(|| {
                if h.take_pending() { seen_tx.try_send(()).unwrap(); }
            }).unwrap()
        });
        health.notify();
        seen_rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
        assert!(pool.checkout(|_| {}).is_none(), "health never releases old business slot");
        tx.try_send(42).unwrap();
        assert_eq!(owner.join().unwrap(), 42);
        drop(metadata);
        let (abandoned, rx, _) = pool.checkout(|_| {}).unwrap();
        drop(abandoned);
        assert_eq!(rx.recv_with_control(|| panic!("immediate reply needs no control work")).unwrap(), -1);
    }
    #[test]
    fn shared_wake_control_does_not_lose_reply_or_duplicate_callback() {
        for _ in 0..128 {
            let wake = Arc::new(crate::wake::Wake::event());
            let health = Arc::new(crate::wake::StateNotification::default());
            health.bind(wake.clone());
            let mut pool = Pool::with_wake(1, || (), || -1, Some(wake));
            let (tx, rx, _) = pool.checkout(|_| {}).unwrap();
            let h = health.clone();
            let producer = std::thread::spawn(move || { h.notify(); tx.try_send(7).unwrap(); });
            let mut observed = 0;
            assert_eq!(rx.recv_with_control(|| { if health.take_pending() { observed += 1; } }).unwrap(), 7);
            producer.join().unwrap();
            observed += u32::from(health.take_pending());
            assert_eq!(observed, 1);
            assert!(rx.try_recv().is_err());
        }
    }
    #[test]
    fn reactor_poll_keeps_slow_and_abandoned_slots_independent() {
        let wake = Arc::new(crate::wake::Wake::default());
        let mut pool = Pool::with_wake(2, || (), || -1, Some(wake.clone()));
        let (slow, slow_rx, sm) = pool.checkout(|_| {}).unwrap();
        let (fast, fast_rx, fm) = pool.checkout(|_| {}).unwrap();
        assert!(matches!(slow_rx.try_recv(), Err(crossbeam_channel::TryRecvError::Empty)));
        wake.arm();
        fast.try_send(7).unwrap();
        wake.wait(std::time::Duration::from_millis(1));
        assert_eq!(fast_rx.try_recv().unwrap(), 7);
        assert!(fast_rx.try_recv().is_err());
        assert!(pool.checkout(|_| {}).is_none());
        drop(slow);
        assert_eq!(slow_rx.try_recv().unwrap(), -1);
        drop((slow_rx, fast_rx, sm, fm));
        let (tx, rx, _) = pool.checkout(|_| {}).unwrap();
        tx.try_send(8).unwrap();
        assert_eq!(rx.try_recv().unwrap(), 8);
    }
    #[test]
    fn lifecycle_is_lossless_and_no_reuse_while_any_generation_reader_remains() {
        let mut pool = Pool::new(1, || 7, || -1);
        let (tx, rx, meta) = pool.checkout(|_| {}).unwrap();
        assert!(pool.checkout(|_| {}).is_none());
        tx.try_send(42).unwrap();
        assert_eq!(rx.recv().unwrap(), 42);
        drop(rx);
        assert!(pool.checkout(|_| {}).is_none());
        drop(meta);
        let (tx, rx, _) = pool.checkout(|_| {}).unwrap();
        drop(tx);
        assert_eq!(rx.recv().unwrap(), -1);
    }
    #[test]
    fn dropped_receiver_and_late_completion_cannot_replay_into_reused_slot() {
        let mut pool = Pool::new(1, || (), || -1);
        let (tx, rx, meta) = pool.checkout(|_| {}).unwrap();
        drop(rx);
        drop(meta);
        assert!(pool.checkout(|_| {}).is_none());
        tx.try_send(9).unwrap();
        let (tx, rx, _) = pool.checkout(|_| {}).unwrap();
        tx.try_send(10).unwrap();
        assert_eq!(rx.recv().unwrap(), 10);
    }
    #[test]
    fn independent_owners_cannot_consume_each_others_replies() {
        let mut a = Pool::new(1, || (), || -1);
        let mut b = Pool::new(1, || (), || -1);
        let (at, ar, _) = a.checkout(|_| {}).unwrap();
        let (bt, br, _) = b.checkout(|_| {}).unwrap();
        std::thread::spawn(move || {
            at.try_send(1).unwrap();
        })
        .join()
        .unwrap();
        bt.try_send(2).unwrap();
        assert_eq!(br.recv().unwrap(), 2);
        assert_eq!(ar.recv().unwrap(), 1);
    }
}

#[cfg(test)]
mod control_benchmark {
    use super::*;
    use std::{sync::atomic::{AtomicU64, Ordering}, time::Duration};
    #[test]
    #[ignore = "focused release: controlled old reply delay, shared completion/health wake"]
    fn pending_reply_health_latency() {
        const N: usize = 1200;
        fn now() -> u64 { hexagent_types::types::monotonic_now_ns() }
        fn report(mode: &str, stage: &str, samples: &mut [u64]) {
            samples.sort_unstable(); let n = samples.len();
            eprintln!("control_wait mode={mode} stage={stage} n={n} median_ns={} p99_ns={} p999_ns={} max_ns={} capacity=1 high_water=1 overflow=0",
                samples[n/2], samples[(n*99).div_ceil(100)-1], samples[(n*999).div_ceil(1000)-1], samples[n-1]);
        }
        for turn in 0..4 {
            for enabled in if turn % 2 == 0 { [false, true] } else { [true, false] } {
                let wake = Arc::new(crate::wake::Wake::event());
                let health = Arc::new(crate::wake::StateNotification::default());
                health.bind(wake.clone());
                let ready = Arc::new(AtomicU64::new(0));
                let (dispatch_tx, dispatch_rx) = crossbeam_channel::bounded::<Sender<u64, ()>>(1);
                let h = health.clone(); let r = ready.clone();
                let producer = std::thread::spawn(move || {
                    while let Ok(tx) = dispatch_rx.recv() {
                        r.store(now(), Ordering::Release); h.notify();
                        std::thread::sleep(Duration::from_millis(1));
                        tx.try_send(now()).unwrap();
                    }
                });
                let mut pool = Pool::with_wake(1, || (), || 0, Some(wake));
                let mut health_samples = Vec::with_capacity(N);
                let mut reply_samples = Vec::with_capacity(N);
                for i in 0..N+100 {
                    let (tx, rx, _meta) = pool.checkout(|_| {}).unwrap();
                    dispatch_tx.send(tx).unwrap();
                    let mut observed = 0;
                    let mut control = || {
                        if health.take_pending() { observed = now(); }
                    };
                    let reply_ready = if enabled { rx.recv_with_control(&mut control).unwrap() }
                        else { rx.recv().unwrap() };
                    let completed = now();
                    control();
                    assert_ne!(observed, 0); assert_ne!(reply_ready, 0);
                    if i >= 100 {
                        health_samples.push(observed-ready.load(Ordering::Acquire));
                        reply_samples.push(completed-reply_ready);
                    }
                }
                drop(dispatch_tx); producer.join().unwrap();
                let mode = if enabled { "interruptible" } else { "completion_only" };
                report(mode, "ready_to_owner_observe", &mut health_samples);
                report(mode, "reply_publish_to_consume", &mut reply_samples);
            }
        }
    }
}
