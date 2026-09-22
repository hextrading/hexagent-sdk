//! Bounded MPSC transfer to one polling owner, without a crossbeam wake lock
//! or a wait for a preempted peer to publish/release its reserved queue slot.
//!
//! `try_send` retains the value on contention/full. Lossless cold producers may
//! use `send`, which retains that exact value and sleeps between retries; this
//! method is not for quote callbacks. The consumer polls, and must yield when
//! `has_pending` is true but the head has not been committed. No notification
//! channel is used. FIFO follows successful reservations, not producer clocks.
use crate::try_queue::TryQueue;
use crossbeam_channel::{SendError, TryRecvError, TrySendError};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub const IDLE_POLL: Duration = Duration::from_micros(10);

struct Shared<T> {
    queue: TryQueue<T>,
    senders: AtomicUsize,
    receiver_alive: AtomicBool,
}

pub struct Sender<T>(Arc<Shared<T>>);
/// Intentionally not Clone: one owner consumes and mutates downstream state.
pub struct Receiver<T>(Arc<Shared<T>>);

pub fn bounded<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        queue: TryQueue::new(capacity),
        senders: AtomicUsize::new(1),
        receiver_alive: AtomicBool::new(true),
    });
    (Sender(shared.clone()), Receiver(shared))
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.0.senders.fetch_add(1, Ordering::Relaxed);
        Self(self.0.clone())
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.0.senders.fetch_sub(1, Ordering::Release);
    }
}

impl<T> Sender<T> {
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        if !self.0.receiver_alive.load(Ordering::Acquire) {
            return Err(TrySendError::Disconnected(value));
        }
        self.0.queue.try_push(value).map_err(TrySendError::Full)
    }

    pub fn send(&self, mut value: T) -> Result<(), SendError<T>> {
        loop {
            match self.try_send(value) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Disconnected(value)) => return Err(SendError(value)),
                Err(TrySendError::Full(returned)) => value = returned,
            }
            std::thread::sleep(IDLE_POLL);
        }
    }
}

impl<T> Receiver<T> {
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        if let Some(value) = self.0.queue.try_pop() {
            return Ok(value);
        }
        // Acquire sender shutdown before the final retry: its last publication
        // may have raced with the first empty read.
        if self.0.senders.load(Ordering::Acquire) == 0 {
            return self.0.queue.try_pop().ok_or(TryRecvError::Disconnected);
        }
        Err(TryRecvError::Empty)
    }

    pub fn front_ready(&self) -> bool {
        self.0.queue.front_ready()
    }

    /// Includes in-flight reservations. Do not let public quotes overtake a
    /// private lifecycle publication whose producer was preempted mid-write.
    pub fn has_pending(&self) -> bool {
        !self.0.queue.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.queue.is_empty()
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.0.receiver_alive.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    #[test]
    fn exact_capacity_order_and_disconnect_after_drain() {
        let (tx, rx) = bounded(2);
        tx.try_send((7, 1)).unwrap();
        tx.try_send((7, 2)).unwrap();
        assert_eq!(tx.try_send((8, 3)), Err(TrySendError::Full((8, 3))));
        drop(tx);
        assert_eq!(rx.try_recv(), Ok((7, 1)));
        assert_eq!(rx.try_recv(), Ok((7, 2)));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn unfinished_publication_returns_immediately_without_overtaking() {
        let (tx, rx) = bounded(2);
        let reserved = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let producer = {
            let (tx, reserved, resume) = (tx.clone(), reserved.clone(), resume.clone());
            std::thread::spawn(move || {
                tx.0.queue
                    .push_with(1, || {
                        reserved.wait();
                        resume.wait();
                    })
                    .unwrap();
            })
        };
        reserved.wait();
        tx.try_send(2).unwrap();
        // These calls complete while the peer remains deterministically paused.
        assert!(rx.has_pending());
        assert!(!rx.front_ready());
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        resume.wait();
        producer.join().unwrap();
        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Ok(2));
    }

    #[test]
    fn retained_head_retries_without_duplication_and_lanes_are_isolated() {
        let (tx, rx) = bounded(1);
        let (other_tx, other_rx) = bounded(1);
        tx.send((3, 1)).unwrap();
        let producer = std::thread::spawn(move || tx.send((3, 2)));
        other_tx.send((9, 1)).unwrap();
        assert_eq!(other_rx.try_recv(), Ok((9, 1)));
        assert_eq!(rx.try_recv(), Ok((3, 1)));
        producer.join().unwrap().unwrap();
        assert_eq!(rx.try_recv(), Ok((3, 2)));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn closing_consumer_returns_the_exact_retained_record() {
        let (tx, rx) = bounded(1);
        tx.send(1).unwrap();
        let producer = std::thread::spawn(move || tx.send(2));
        drop(rx);
        assert_eq!(producer.join().unwrap(), Err(SendError(2)));
    }

    #[test]
    fn concurrent_producers_deliver_every_record_once_in_per_producer_order() {
        let (tx, rx) = bounded(7);
        let producers: Vec<_> = (0..3)
            .map(|owner| {
                let tx = tx.clone();
                std::thread::spawn(move || {
                    for sequence in 0..2_000 {
                        tx.send((owner, sequence)).unwrap();
                    }
                })
            })
            .collect();
        drop(tx);
        let mut next = [0; 3];
        loop {
            match rx.try_recv() {
                Ok((owner, sequence)) => {
                    assert_eq!(sequence, next[owner]);
                    next[owner] += 1;
                }
                Err(TryRecvError::Empty) => std::thread::sleep(IDLE_POLL),
                Err(TryRecvError::Disconnected) => break,
            }
        }
        for producer in producers {
            producer.join().unwrap();
        }
        assert_eq!(next, [2_000; 3]);
    }

    #[test]
    #[ignore = "focused release microbenchmark; prints distributions, no timing assertion"]
    fn crossbeam_and_poll_lane_roundtrip_benchmark() {
        use std::hint::black_box;
        use std::time::Instant;
        const N: usize = 100_000;
        fn run(name: &str, mut operation: impl FnMut(u64)) {
            let mut samples = Vec::with_capacity(N);
            for value in 0..N as u64 {
                let began = Instant::now();
                operation(black_box(value));
                samples.push(began.elapsed().as_nanos());
            }
            samples.sort_unstable();
            println!("{name} n={N} p50_ns={} p99_ns={} p999_ns={} max_ns={} depth_high_water=1 dropped=0 boundary=prebuilt_message_push_then_pop_same_thread",
                samples[N / 2], samples[N * 99 / 100 - 1], samples[N * 999 / 1000 - 1], samples[N - 1]);
        }
        let (old_tx, old_rx) = crossbeam_channel::bounded::<u64>(10_000);
        run("crossbeam", |value| {
            old_tx.try_send(value).unwrap();
            assert_eq!(black_box(old_rx.try_recv().unwrap()), value);
        });
        let (tx, rx) = bounded::<u64>(10_000);
        run("poll_channel", |value| {
            tx.try_send(value).unwrap();
            assert_eq!(black_box(rx.try_recv().unwrap()), value);
        });
    }
}
