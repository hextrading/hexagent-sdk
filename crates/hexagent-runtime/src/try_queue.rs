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

/// Identifies an observation-only timestamp without mutating the source.
///
/// # Safety
/// Some must reference an ordinary, exclusively owned u64 field inside Self
/// which may accept ANY u64 without invalidating Self or its invariants. It
/// must not name padding, shared/interior storage, a discriminant, a pointer,
/// or bytes reinterpreted from another type. The corresponding field remains
/// at the same offset when Self is moved. None requests no stamp.
/// copied_prefix_len must include every byte required for a valid owned T,
/// including its discriminant and ALL active fields. Omitted bytes may only
/// be inactive enum storage or trailing padding. A shorter prefix requires a
/// guaranteed representation (e.g. repr(C)); never guess default enum layout.
/// Selectors run before reservation and may unwind there.
pub unsafe trait PublicationStamp {
    fn publication_stamp(&self) -> Option<&u64>;
    fn copied_prefix_len(&self) -> usize where Self: Sized { std::mem::size_of::<Self>() }
}

#[repr(align(128))]
struct Cursor(AtomicUsize);

struct Slot<T> {
    stamp: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}

/// Only the runtime's immediate publication paths may hold this permit.
#[must_use = "a reserved FIFO slot must be published immediately"]
pub(crate) struct WriteReservation<'a, T> {
    slot: &'a Slot<T>,
    release_stamp: usize,
}

impl<T> WriteReservation<'_, T> {
    /// Full-value transfer for types without a compact publication layout.
    /// No callback or fallible work occurs in the reserved slot.
    #[inline(always)]
    pub(crate) fn publish_owned(self, value: T) {
        // SAFETY: the reservation exclusively owns this uninitialized slot.
        unsafe { (*self.slot.value.get()).write(value); }
        self.slot.stamp.store(self.release_stamp, Ordering::Release);
    }

    /// # Safety
    /// copied_len/field must come from publication_layout for this exact value.
    /// After this copy the caller MUST forget its source and immediately call
    /// release, without fallible work or user code in between. Keeping the
    /// source borrowed avoids a full T move into this internal helper.
    #[inline(always)]
    pub(crate) unsafe fn write_prefix(&self, value: &T, copied_len: usize, field: Option<(usize, u64)>) {
        // SAFETY: reservation exclusively owns the destination. The audited
        // prefix includes the tag and ALL active fields. Omitted bytes are
        // inactive storage/padding, permitted to remain uninitialized.
        unsafe { std::ptr::copy_nonoverlapping((value as *const T).cast::<u8>(), (*self.slot.value.get()).as_mut_ptr().cast::<u8>(), copied_len); }
        if let Some((offset, stamp)) = field {
            unsafe { (*self.slot.value.get()).as_mut_ptr().cast::<u8>().add(offset)
                .cast::<u64>().write_unaligned(stamp); }
        }
    }

    /// # Safety
    /// write_prefix initialized this reservation and the original owner has
    /// been forgotten; only the queue may subsequently access/drop the value.
    #[inline(always)]
    pub(crate) unsafe fn release(self) {
        self.slot.stamp.store(self.release_stamp, Ordering::Release);
    }

}

/// Runs selection and all bounds checks BEFORE reserving any FIFO slot.
#[inline(always)]
pub(crate) fn publication_layout<T: PublicationStamp>(value: &T, stamp: Option<u64>) -> (usize, Option<(usize, u64)>) {
    let copied_len = value.copied_prefix_len();
    assert!(copied_len <= std::mem::size_of::<T>(), "copied prefix exceeds value");
    let field = stamp.and_then(|stamp| value.publication_stamp().map(|field| {
        let offset = (field as *const u64 as usize)
            .checked_sub(value as *const T as usize).expect("stamp must be inside value");
        assert!(offset.checked_add(std::mem::size_of::<u64>())
            .is_some_and(|end| end <= copied_len), "stamp must be inside copied prefix");
        (offset, stamp)
    }));
    (copied_len, field)
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

    /// Move ownership on success; retain the exact message on full/contention.
    #[inline(always)]
    pub fn try_push_owned(&self, value: T) -> Result<(), T> {
        let Some(reservation) = self.try_reserve() else { return Err(value); };
        reservation.publish_owned(value);
        Ok(())
    }

    /// Internal linear operation: successful reservation must immediately be
    /// published, with no callback, fallible operation, or unwind in between.
    /// Returning a small permit avoids intermediate Result<large message>
    /// temporaries in Sender. Abandoning a permit would stall the FIFO head.
    #[inline(always)]
    pub(crate) fn try_reserve(&self) -> Option<WriteReservation<'_, T>> {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let slot = &self.slots[tail & (self.lap - 1)];
        if slot.stamp.load(Ordering::Acquire) != tail
            || self.tail.0.compare_exchange(tail, self.next(tail), Ordering::Relaxed, Ordering::Relaxed).is_err()
        { return None; }
        Some(WriteReservation { slot, release_stamp: tail.wrapping_add(1) })
    }

    /// Transfer directly from a caller-owned slot. On contention/full the
    /// caller retains Some(T); success leaves None. The small return avoids
    /// transporting a large error payload through intermediate Result values.
    #[inline(always)]
    pub fn try_push_slot(&self, value: &mut Option<T>) -> bool {
        if value.is_none() { return false; }
        let tail = self.tail.0.load(Ordering::Relaxed);
        let slot = &self.slots[tail & (self.lap - 1)];
        if slot.stamp.load(Ordering::Acquire) != tail
            || self.tail.0.compare_exchange(tail, self.next(tail), Ordering::Relaxed, Ordering::Relaxed).is_err()
        { return false; }
        // SAFETY: the successful CAS exclusively owns the uninitialized
        // destination; &mut Option<T> exclusively owns the disjoint source.
        // Move the bytes directly, then overwrite the source with None WITHOUT
        // dropping T. There is no callback/panic point between these operations
        // and the release publication. Only the queue may subsequently drop T.
        // Avoid take()/write(T): large niche-bearing enums otherwise materialize
        // another full-size temporary even when this function is inlined.
        unsafe {
            let source = value.as_mut().unwrap_unchecked() as *mut T;
            std::ptr::copy_nonoverlapping(source, (*slot.value.get()).as_mut_ptr(), 1);
            std::ptr::write(value, None);
        }
        slot.stamp.store(tail.wrapping_add(1), Ordering::Release);
        true
    }

    #[inline]
    pub(crate) fn push_with(&self, value: T, after_reserve: impl FnOnce()) -> Result<(), T> {
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
            let mut next = Some(2);
            assert_eq!(q.try_push_slot(&mut next), capacity == 2);
            assert_eq!(next.is_none(), capacity == 2);
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
            let mut pending = Some(value);
            assert!(q.try_push_slot(&mut pending));
            assert!(pending.is_none());
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

    #[test]
    fn slot_transfer_preserves_large_niche_enum_and_exact_drop_ownership() {
        struct Owned(Arc<AtomicUsize>);
        impl Drop for Owned {
            fn drop(&mut self) { self.0.fetch_add(1, Ordering::Relaxed); }
        }
        enum Message { Large([u64; 960], Box<Owned>), Small(Box<Owned>) }
        let drops = Arc::new(AtomicUsize::new(0));
        let q = TryQueue::new(1);
        let mut large = Some(Message::Large([42; 960], Box::new(Owned(drops.clone()))));
        assert!(q.try_push_slot(&mut large));
        assert!(large.is_none());
        let mut small = Some(Message::Small(Box::new(Owned(drops.clone()))));
        assert!(!q.try_push_slot(&mut small));
        assert!(matches!(&small, Some(Message::Small(owner)) if Arc::ptr_eq(&owner.0, &drops)));
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        match q.try_pop().unwrap() {
            Message::Large(bytes, owner) => {
                assert_eq!(bytes, [42; 960]);
                assert!(Arc::ptr_eq(&owner.0, &drops));
            }
            _ => panic!("variant corrupted"),
        }
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert!(q.try_push_slot(&mut small));
        assert!(small.is_none());
        assert!(!q.try_push_slot(&mut small));
        drop(q);
        assert_eq!(drops.load(Ordering::Relaxed), 2);
        // Zero-sized T also has a valid, aligned transfer destination.
        let zst = TryQueue::new(1);
        let mut unit = Some(());
        assert!(zst.try_push_slot(&mut unit));
        assert_eq!(unit, None);
        assert_eq!(zst.try_pop(), Some(()));
    }

    #[test]
    fn competing_slot_producers_preserve_fifo_payload_and_single_drop() {
        struct Message { owner: usize, sequence: usize, drops: Arc<AtomicUsize> }
        impl Drop for Message {
            fn drop(&mut self) { self.drops.fetch_add(1, Ordering::Relaxed); }
        }
        let q = Arc::new(TryQueue::new(7));
        let drops = Arc::new(AtomicUsize::new(0));
        let workers: Vec<_> = (0..4).map(|owner| {
            let (q, drops) = (q.clone(), drops.clone());
            std::thread::spawn(move || {
                for sequence in 0..5000 {
                    let mut pending = Some(Message { owner, sequence, drops: drops.clone() });
                    while !q.try_push_slot(&mut pending) {
                        assert_eq!(pending.as_ref().unwrap().sequence, sequence);
                        std::thread::yield_now();
                    }
                    assert!(pending.is_none());
                }
            })
        }).collect();
        let mut next = [0; 4];
        for _ in 0..20_000 {
            let message = loop {
                if let Some(message) = q.try_pop() { break message; }
                std::thread::yield_now();
            };
            assert_eq!(message.sequence, next[message.owner]);
            next[message.owner] += 1;
        }
        for worker in workers { worker.join().unwrap(); }
        assert_eq!(next, [5000; 4]);
        assert_eq!(drops.load(Ordering::Relaxed), 20_000);
        assert!(q.is_empty());
    }

    #[test]
    fn competing_owned_producers_preserve_fifo_and_single_drop() {
        struct Message { owner: usize, sequence: usize, drops: Arc<AtomicUsize> }
        impl Drop for Message { fn drop(&mut self) { self.drops.fetch_add(1, Ordering::Relaxed); } }
        let q = Arc::new(TryQueue::new(7));
        let drops = Arc::new(AtomicUsize::new(0));
        let workers: Vec<_> = (0..4).map(|owner| {
            let (q, drops) = (q.clone(), drops.clone());
            std::thread::spawn(move || {
                for sequence in 0..5000 {
                    let mut pending = Message { owner, sequence, drops: drops.clone() };
                    while let Err(retained) = q.try_push_owned(pending) {
                        assert_eq!(retained.owner, owner);
                        assert_eq!(retained.sequence, sequence);
                        pending = retained;
                        std::thread::yield_now();
                    }
                }
            })
        }).collect();
        let mut next = [0; 4];
        for _ in 0..20_000 {
            let message = loop {
                if let Some(message) = q.try_pop() { break message; }
                std::thread::yield_now();
            };
            assert_eq!(message.sequence, next[message.owner]);
            next[message.owner] += 1;
        }
        for worker in workers { worker.join().unwrap(); }
        assert_eq!(next, [5000; 4]);
        assert_eq!(drops.load(Ordering::Relaxed), 20_000);
        assert!(q.is_empty());
    }

    #[test]
    fn owned_large_enum_drop_full_zero_size_and_counter_wrap() {
        #[derive(Debug)]
        struct Owned(Arc<AtomicUsize>);
        impl Drop for Owned { fn drop(&mut self) { self.0.fetch_add(1, Ordering::Relaxed); } }
        #[derive(Debug)]
        enum Message { Large([u64; 960], Box<Owned>), Small(Box<Owned>) }
        let drops = Arc::new(AtomicUsize::new(0));
        let q = TryQueue::new(1);
        q.try_push_owned(Message::Large([42; 960], Box::new(Owned(drops.clone())))).unwrap();
        let Err(retained) = q.try_push_owned(Message::Small(Box::new(Owned(drops.clone()))))
            else { panic!("full must retain exact message"); };
        assert!(matches!(&retained, Message::Small(owner) if Arc::ptr_eq(&owner.0, &drops)));
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        match q.try_pop().unwrap() {
            Message::Large(bytes, owner) => {
                assert_eq!(bytes, [42; 960]);
                assert!(Arc::ptr_eq(&owner.0, &drops));
            },
            _ => panic!("variant corrupted"),
        }
        q.try_push_owned(retained).unwrap();
        drop(q);
        assert_eq!(drops.load(Ordering::Relaxed), 2);
        let q = TryQueue::new(3);
        let start = usize::MAX - (q.lap - 1);
        q.head.0.store(start, Ordering::Relaxed);
        q.tail.0.store(start, Ordering::Relaxed);
        for (i, slot) in q.slots.iter().enumerate() { slot.stamp.store(start + i, Ordering::Relaxed); }
        for _ in 0..100 {
            q.try_push_owned(()).unwrap();
            assert_eq!(q.try_pop(), Some(()));
            assert!(q.is_empty());
        }
    }

    #[test]
    fn owned_publication_cannot_overtake_unpublished_fifo_head() {
        let q = TryQueue::new(2);
        q.push_with(1, || {
            q.try_push_owned(2).unwrap();
            assert_eq!(q.try_push_owned(3), Err(3));
            assert_eq!(q.try_pop(), None);
        }).unwrap();
        assert_eq!(q.try_pop(), Some(1));
        assert_eq!(q.try_pop(), Some(2));
    }
}
