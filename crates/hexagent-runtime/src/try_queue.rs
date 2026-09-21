//! Preallocated FIFO whose try operations never wait for another thread.
//!
//! A producer/consumer owns one slot after a successful cursor CAS. Release
//! stamps transfer the initialized value to the next owner. An unfinished
//! transfer, or a competing CAS, returns immediately. In particular, a FIFO
//! thread must not spin waiting for a lower-priority thread it has preempted.
//! Callers retain/retry lossless records and may discard replaceable records.
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

#[repr(align(128))]
struct Cursor(AtomicUsize);

struct Slot<T> {
    stamp: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}

pub struct TryQueue<T> {
    head: Cursor,
    tail: Cursor,
    slots: Box<[Slot<T>]>,
    lap: usize,
}

// A successful cursor CAS gives exactly one thread access to a slot's value.
// Acquire/release stamps fence initialization, moving out, and slot reuse.
unsafe impl<T: Send> Send for TryQueue<T> {}
unsafe impl<T: Send> Sync for TryQueue<T> {}

impl<T> TryQueue<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        let lap = capacity
            .checked_add(1)
            .and_then(usize::checked_next_power_of_two)
            .expect("queue capacity overflows cursor lap");
        Self {
            head: Cursor(AtomicUsize::new(0)),
            tail: Cursor(AtomicUsize::new(0)),
            slots: (0..capacity)
                .map(|stamp| Slot {
                    stamp: AtomicUsize::new(stamp),
                    value: UnsafeCell::new(MaybeUninit::uninit()),
                })
                .collect(),
            lap,
        }
    }

    fn next(&self, position: usize) -> usize {
        if (position & (self.lap - 1)) + 1 < self.capacity() {
            position.wrapping_add(1)
        } else {
            (position & !(self.lap - 1)).wrapping_add(self.lap)
        }
    }

    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    /// Includes reserved but not yet published slots. A bounded, approximate
    /// observation under concurrency; never loops to obtain a stable snapshot.
    pub fn len(&self) -> usize {
        let head = self.head.0.load(Ordering::Acquire);
        let tail = self.tail.0.load(Ordering::Acquire);
        if head == tail {
            return 0;
        }
        let h = head & (self.lap - 1);
        let t = tail & (self.lap - 1);
        if t > h {
            t - h
        } else {
            self.capacity() + t - h
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Unlike len(), an in-flight publication is not readable. A scheduler
    /// must park/yield to other work instead of spinning on such a slot.
    pub fn front_ready(&self) -> bool {
        let head = self.head.0.load(Ordering::Relaxed);
        self.slots[head & (self.lap - 1)]
            .stamp
            .load(Ordering::Acquire)
            == head.wrapping_add(1)
    }

    /// Returns the exact value on capacity exhaustion OR contention. One CAS,
    /// no lock, allocation, syscall, retry loop, or peer-completion wait.
    pub fn try_push(&self, value: T) -> Result<(), T> {
        self.push_with(value, || {})
    }

    #[inline]
    fn push_with(&self, value: T, after_reserve: impl FnOnce()) -> Result<(), T> {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let slot = &self.slots[tail & (self.lap - 1)];
        if slot.stamp.load(Ordering::Acquire) != tail
            || self
                .tail
                .0
                .compare_exchange(tail, self.next(tail), Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
        {
            return Err(value);
        }
        after_reserve(); // No-op in production; deterministic preemption seam.
                         // SAFETY: the successful tail CAS exclusively owns this empty slot.
        unsafe {
            (*slot.value.get()).write(value);
        }
        slot.stamp.store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// None also means the FIFO head is reserved but not published. Never skip
    /// that head, so two successful pushes retain their reservation order.
    pub fn try_pop(&self) -> Option<T> {
        self.pop_with(|| {})
    }

    #[inline]
    fn pop_with(&self, after_reserve: impl FnOnce()) -> Option<T> {
        let head = self.head.0.load(Ordering::Relaxed);
        let slot = &self.slots[head & (self.lap - 1)];
        if slot.stamp.load(Ordering::Acquire) != head.wrapping_add(1)
            || self
                .head
                .0
                .compare_exchange(head, self.next(head), Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
        {
            return None;
        }
        after_reserve();
        // SAFETY: acquire observed the initialized value; the head CAS grants
        // exclusive read ownership until the release stamp permits slot reuse.
        let value = unsafe { (*slot.value.get()).assume_init_read() };
        slot.stamp
            .store(head.wrapping_add(self.lap), Ordering::Release);
        Some(value)
    }
}

impl<T> Drop for TryQueue<T> {
    fn drop(&mut self) {
        // Exclusive destruction: no producer can still own an in-flight slot.
        while self.try_pop().is_some() {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    #[test]
    fn fifo_exact_capacity_and_reuse_including_one_and_non_power_of_two() {
        for capacity in [1, 2, 3, 17] {
            let q = TryQueue::new(capacity);
            for lap in 0..1000 {
                for i in 0..capacity {
                    q.try_push(lap * capacity + i).unwrap();
                }
                assert_eq!(q.len(), capacity);
                assert_eq!(q.try_push(usize::MAX), Err(usize::MAX));
                for i in 0..capacity {
                    assert_eq!(q.try_pop(), Some(lap * capacity + i));
                }
                assert!(q.is_empty());
                assert!(!q.front_ready());
            }
        }
    }

    #[test]
    fn preempted_publisher_cannot_trap_higher_priority_producer_or_consumer() {
        for capacity in [1, 2] {
            let q = Arc::new(TryQueue::new(capacity));
            let reserved = Arc::new(Barrier::new(2));
            let resume = Arc::new(Barrier::new(2));
            let worker = {
                let (q, reserved, resume) = (q.clone(), reserved.clone(), resume.clone());
                std::thread::spawn(move || {
                    q.push_with(1, || {
                        reserved.wait();
                        resume.wait();
                    })
                    .unwrap()
                })
            };
            reserved.wait();
            assert_eq!(q.try_pop(), None);
            assert!(!q.front_ready());
            let next = q.try_push(2);
            assert_eq!(next.is_ok(), capacity == 2);
            assert_eq!(
                q.try_pop(),
                None,
                "must not overtake the unpublished FIFO head"
            );
            resume.wait();
            worker.join().unwrap();
            assert_eq!(q.try_pop(), Some(1));
            if capacity == 2 {
                assert_eq!(q.try_pop(), Some(2));
            }
        }
    }

    #[test]
    fn preempted_consumer_cannot_trap_producer_reusing_the_slot() {
        let q = TryQueue::new(1);
        q.try_push(1).unwrap();
        assert_eq!(q.pop_with(|| assert_eq!(q.try_push(2), Err(2))), Some(1));
        q.try_push(2).unwrap();
        assert_eq!(q.try_pop(), Some(2));
    }

    #[test]
    fn concurrent_producers_preserve_each_fifo_without_duplicates_or_loss() {
        let q = Arc::new(TryQueue::new(17));
        let producers: Vec<_> = (0..4)
            .map(|id| {
                let q = q.clone();
                std::thread::spawn(move || {
                    for sequence in 0..10_000 {
                        let mut value = (id, sequence);
                        while let Err(retained) = q.try_push(value) {
                            value = retained;
                            std::thread::yield_now();
                        }
                    }
                })
            })
            .collect();
        let mut expected = [0; 4];
        let mut received = 0;
        while received < 40_000 {
            if let Some((id, sequence)) = q.try_pop() {
                assert_eq!(expected[id], sequence);
                expected[id] += 1;
                received += 1;
            } else {
                std::thread::yield_now();
            }
        }
        for worker in producers {
            worker.join().unwrap();
        }
        assert_eq!(expected, [10_000; 4]);
        assert!(q.is_empty());
    }

    #[test]
    fn competing_consumers_and_producers_transfer_every_value_exactly_once() {
        const COUNT: usize = 20_000;
        let q = Arc::new(TryQueue::new(7));
        let seen = Arc::new((0..COUNT).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let consumed = Arc::new(AtomicUsize::new(0));
        let mut workers = Vec::new();
        for id in 0..4 {
            let q = q.clone();
            workers.push(std::thread::spawn(move || {
                for value in (id..COUNT).step_by(4) {
                    while q.try_push(value).is_err() {
                        std::thread::yield_now();
                    }
                }
            }));
        }
        for _ in 0..3 {
            let (q, seen, consumed) = (q.clone(), seen.clone(), consumed.clone());
            workers.push(std::thread::spawn(move || {
                while consumed.load(Ordering::Relaxed) < COUNT {
                    if let Some(value) = q.try_pop() {
                        assert_eq!(seen[value].fetch_add(1, Ordering::Relaxed), 0);
                        consumed.fetch_add(1, Ordering::Relaxed);
                    } else {
                        std::thread::yield_now();
                    }
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        assert!(seen.iter().all(|count| count.load(Ordering::Relaxed) == 1));
        assert!(q.is_empty());
    }

    #[test]
    fn counters_wrap_without_confusing_empty_and_full_stamps() {
        let q = TryQueue::new(3);
        let start = usize::MAX - (q.lap - 1);
        q.head.0.store(start, Ordering::Relaxed);
        q.tail.0.store(start, Ordering::Relaxed);
        for (i, slot) in q.slots.iter().enumerate() {
            slot.stamp.store(start + i, Ordering::Relaxed);
        }
        for value in 0..100 {
            q.try_push(value).unwrap();
            assert_eq!(q.len(), 1);
            assert_eq!(q.try_pop(), Some(value));
            assert!(q.is_empty());
        }
    }

    #[test]
    fn drop_releases_each_owned_value_once() {
        struct Value(Arc<AtomicUsize>);
        impl Drop for Value {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let drops = Arc::new(AtomicUsize::new(0));
        let q = TryQueue::new(3);
        for _ in 0..3 {
            assert!(q.try_push(Value(drops.clone())).is_ok());
        }
        drop(q.try_pop());
        drop(q);
        assert_eq!(drops.load(Ordering::Relaxed), 3);
    }
}
