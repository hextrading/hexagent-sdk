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
        Self {
            slots: (0..capacity)
                .map(|_| {
                    let (tx, rx) = crate::poll_channel::bounded_with_wake(1, Some(Arc::new(crate::wake::Wake::default())));
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
    pub fn recv(&self) -> Result<T, RecvError> {
        self.slot.rx.recv()
    }
    pub fn recv_timeout(&self, timeout: std::time::Duration) -> Result<T, RecvTimeoutError> {
        self.slot.rx.recv_timeout(timeout)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
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
