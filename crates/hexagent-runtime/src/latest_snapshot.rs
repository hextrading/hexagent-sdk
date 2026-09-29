//! Single-producer/single-consumer replaceable snapshots with no peer wait.
//!
//! Three startup-allocated slots: the producer owns `back`, the consumer owns
//! `front`, and one atomic exchange transfers the middle slot. A preempted peer
//! cannot hold a lock or an unfinished FIFO head. Only complete Copy snapshots
//! may use this lane; private trades and order lifecycle must remain lossless.
use crossbeam_channel::TryRecvError;
use std::cell::{Cell, UnsafeCell};
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

const DIRTY: usize = 4;
const INDEX: usize = 3;

struct Shared<T: Copy> {
    slots: [UnsafeCell<MaybeUninit<T>>; 3],
    middle: AtomicUsize,
    producer_alive: AtomicBool,
}

// Neither endpoint is Clone; the producer needs &mut self and the consumer's
// Cell makes it !Sync. Every slot has exactly one owner. AcqRel exchanges fence
// both initialized publication and the hand-back of a no-longer-read slot.
unsafe impl<T: Copy + Send> Sync for Shared<T> {}

pub struct Publisher<T: Copy> {
    shared: Arc<Shared<T>>,
    back: usize,
}

pub struct Receiver<T: Copy> {
    shared: Arc<Shared<T>>,
    front: Cell<usize>,
}

pub fn channel<T: Copy>() -> (Publisher<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        slots: std::array::from_fn(|_| UnsafeCell::new(MaybeUninit::uninit())),
        middle: AtomicUsize::new(1),
        producer_alive: AtomicBool::new(true),
    });
    (
        Publisher {
            shared: shared.clone(),
            back: 2,
        },
        Receiver {
            shared,
            front: Cell::new(0),
        },
    )
}

impl<T: Copy> Publisher<T> {
    /// Returns whether an unread snapshot was replaced. Exactly one exchange;
    /// no allocation, wakeup, retry, or access to the consumer-owned slot.
    pub fn publish(&mut self, value: T) -> bool {
        // SAFETY: back belongs exclusively to this non-cloneable publisher.
        unsafe {
            (*self.shared.slots[self.back].get()).write(value);
        }
        let previous = self.shared.middle.swap(self.back | DIRTY, Ordering::AcqRel);
        self.back = previous & INDEX;
        previous & DIRTY != 0
    }
}

impl<T: Copy> Drop for Publisher<T> {
    fn drop(&mut self) {
        self.shared.producer_alive.store(false, Ordering::Release);
    }
}

impl<T: Copy> Receiver<T> {
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        // Read liveness first so final publication is visible after disconnect.
        let alive = self.shared.producer_alive.load(Ordering::Acquire);
        if self.shared.middle.load(Ordering::Acquire) & DIRTY == 0 {
            return Err(if alive {
                TryRecvError::Empty
            } else {
                TryRecvError::Disconnected
            });
        }
        // Only this consumer clears DIRTY. A racing producer can replace the
        // middle with a newer complete value, but cannot remove its dirty bit.
        let previous = self.shared.middle.swap(self.front.get(), Ordering::AcqRel);
        let front = previous & INDEX;
        self.front.set(front);
        // SAFETY: the exchange acquired a complete snapshot and retains this
        // slot exclusively until the next receive. T is Copy (no drop traffic).
        Ok(unsafe { (*self.shared.slots[front].get()).assume_init_read() })
    }

    pub fn is_empty(&self) -> bool {
        self.shared.middle.load(Ordering::Acquire) & DIRTY == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latest_complete_snapshot_isolated_and_drained_before_disconnect() {
        let (mut a, arx) = channel();
        let (mut b, brx) = channel();
        assert_eq!(arx.try_recv(), Err(TryRecvError::Empty));
        assert!(!a.publish(1));
        assert!(a.publish(2));
        assert!(!b.publish(9));
        drop(a);
        assert_eq!(arx.try_recv(), Ok(2));
        assert_eq!(arx.try_recv(), Err(TryRecvError::Disconnected));
        assert_eq!(brx.try_recv(), Ok(9));
        assert_eq!(brx.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn concurrent_replacement_never_tears_or_replays_a_snapshot() {
        let (mut tx, rx) = channel();
        let producer = std::thread::spawn(move || {
            for n in 1_u64..=100_000 {
                tx.publish([n; 16]);
            }
        });
        let mut last = 0;
        loop {
            match rx.try_recv() {
                Ok(value) => {
                    assert!(value[0] > last);
                    assert!(value.iter().all(|n| *n == value[0]));
                    last = value[0];
                }
                Err(TryRecvError::Empty) => std::thread::yield_now(),
                Err(TryRecvError::Disconnected) => break,
            }
        }
        producer.join().unwrap();
        assert_eq!(last, 100_000);
    }

    #[test]
    fn preempted_writer_does_not_block_reader_or_expose_incomplete_value() {
        let (mut tx, rx) = channel();
        tx.publish([1; 8]);
        // Producer paused after writing its own slot but before publication.
        unsafe {
            (*tx.shared.slots[tx.back].get()).write([2; 8]);
        }
        assert_eq!(rx.try_recv(), Ok([1; 8]));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        tx.publish([3; 8]);
        assert_eq!(rx.try_recv(), Ok([3; 8]));
    }
}
