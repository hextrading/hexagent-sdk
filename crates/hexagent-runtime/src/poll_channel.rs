//! Bounded MPSC transfer to one polling owner, without a crossbeam wake lock
//! or a wait for a preempted peer to publish/release its reserved queue slot.
//!
//! `try_send` retains the value on contention/full. Lossless cold producers may
//! use `send`, which retains that exact value and sleeps between retries; this
//! method is not for quote callbacks. The consumer polls, and must yield when
//! `has_pending` is true but the head has not been committed. An optional futex hint wakes an idle consumer without a notification
//! channel or crossbeam waiter lock. FIFO follows successful reservations, not producer clocks.
use crate::try_queue::TryQueue;
use crossbeam_channel::{SendError, TryRecvError, TrySendError};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// Transport-neutral cold private/recovery producer. Live uses the polling
/// sender; compatibility callers keep their existing crossbeam sender. This
/// trait adds no shared state, allocations, worker or dynamic dispatch.
pub trait EventSender<T>: Clone + Send + Sync + 'static {
    fn send(&self, value: T) -> Result<(), SendError<T>>;
    fn try_send(&self, value: T) -> Result<(), TrySendError<T>>;
}

impl<T: Send + 'static> EventSender<T> for crossbeam_channel::Sender<T> {
    fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.send(value)
    }
    fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        self.try_send(value)
    }
}

impl<T: Send + 'static> EventSender<T> for Sender<T> {
    fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.send(value)
    }
    fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        self.try_send(value)
    }
}

pub const IDLE_POLL: Duration = Duration::from_micros(10);

struct Shared<T> {
    queue: TryQueue<T>,
    senders: AtomicUsize,
    receiver_alive: AtomicBool,
    wake: OnceLock<Arc<crate::wake::Wake>>,
}

pub struct Sender<T>(Arc<Shared<T>>);
/// Intentionally not Clone: one owner consumes and mutates downstream state.
pub struct Receiver<T>(Arc<Shared<T>>);

/// Local observation bounds, not a timestamp written back into a transferred
/// message. A consumer may dequeue before either returned timestamp. Join by
/// the caller's immutable publication identity; never clamp that ordering away.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PublicationTiming {
    pub push_started_mono_ns: u64,
    pub push_returned_mono_ns: u64,
    pub notify_returned_mono_ns: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotSendError { Full, Disconnected, Empty }

pub fn bounded<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    bounded_with_wake(capacity, None)
}

/// Several inboxes consumed by one dispatcher may share one startup-created
/// wake handle. Each connection owner instead owns a private handle. Queue
/// capacity, reservation ordering and overflow semantics are unchanged.
pub fn bounded_with_wake<T>(capacity: usize, wake: Option<Arc<crate::wake::Wake>>) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        queue: TryQueue::new(capacity),
        senders: AtomicUsize::new(1),
        receiver_alive: AtomicBool::new(true),
        wake: wake.map(OnceLock::from).unwrap_or_default(),
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
        if self.0.senders.fetch_sub(1, Ordering::Release) == 1 {
            if let Some(wake) = self.0.wake.get() { wake.notify(); }
        }
    }
}

impl<T> Sender<T> {
    /// Actor teardown only. After receiver ownership is released, a racing
    /// producer may finish a previously reserved slot. The actor and that
    /// producer both drain through the queue's exclusive-pop protocol, so
    /// queued reply guards are dropped promptly rather than kept by senders.
    pub(crate) fn discard_if_disconnected(&self) {
        if self.is_disconnected() { while self.0.queue.try_pop().is_some() {} }
    }
    /// Notify another (control) lane consumed by the same owner.
    pub fn notify_owner(&self) { if let Some(wake) = self.0.wake.get() { wake.notify(); } }

    /// Advisory occupancy, including reserved but unpublished messages.
    pub fn len(&self) -> usize {
        self.0.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.queue.is_empty()
    }

    /// Owner shutdown status for async cold callers retaining reply envelopes.
    pub fn is_disconnected(&self) -> bool {
        !self.0.receiver_alive.load(Ordering::Acquire)
    }

    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        if !self.0.receiver_alive.load(Ordering::Acquire) {
            return Err(TrySendError::Disconnected(value));
        }
        self.0.queue.try_push(value).map_err(TrySendError::Full)?;
        if let Some(wake) = self.0.wake.get() { wake.notify(); }
        Ok(())
    }

    /// Owned, inlined publication for large records on latency-sensitive
    /// lanes. Same capacity, failure ownership and notification as try_send.
    #[inline(always)]
    pub fn try_send_owned(&self, value: T) -> Result<(), TrySendError<T>>
    where T: crate::try_queue::PublicationStamp {
        if !self.0.receiver_alive.load(Ordering::Acquire) {
            return Err(TrySendError::Disconnected(value));
        }
        let (copied_len, field) = crate::try_queue::publication_layout(&value, None);
        let Some(reservation) = self.0.queue.try_reserve() else {
            return Err(TrySendError::Full(value));
        };
        // SAFETY: prefix was validated for this exact value before CAS.
        unsafe { reservation.write_prefix(&value, copied_len, field); }
        std::mem::forget(value);
        // SAFETY: destination is initialized and the source owner is gone.
        unsafe { reservation.release(); }
        if let Some(wake) = self.0.wake.get() { wake.notify(); }
        Ok(())
    }

    /// Owned publication with complete phase timing. Only the active prefix
    /// is copied for explicitly audited message layouts; all fields and all
    /// three clocks remain. Capacity failure retains the unchanged message.
    #[inline(always)]
    pub fn try_send_stamped(&self, value: T) -> Result<PublicationTiming, TrySendError<T>>
    where T: crate::try_queue::PublicationStamp {
        if !self.0.receiver_alive.load(Ordering::Acquire) {
            return Err(TrySendError::Disconnected(value));
        }
        let push_started_mono_ns = hexagent_types::types::monotonic_now_ns();
        let (copied_len, field) = crate::try_queue::publication_layout(&value, Some(push_started_mono_ns));
        let Some(reservation) = self.0.queue.try_reserve() else {
            return Err(TrySendError::Full(value));
        };
        // SAFETY: field describes this exact value, selected before CAS.
        // No fallible work or user code occurs before release publication.
        unsafe { reservation.write_prefix(&value, copied_len, field); }
        std::mem::forget(value);
        // SAFETY: destination is initialized and the source owner is gone.
        unsafe { reservation.release(); }
        let push_returned_mono_ns = hexagent_types::types::monotonic_now_ns();
        if let Some(wake) = self.0.wake.get() { wake.notify(); }
        Ok(PublicationTiming { push_started_mono_ns, push_returned_mono_ns,
            notify_returned_mono_ns: hexagent_types::types::monotonic_now_ns() })
    }

    /// Transfer from a caller-owned pending slot without a large by-value
    /// argument/Result temporary. Full/disconnect leaves every field unchanged.
    /// Success clears the source before release, preserving single ownership.
    #[inline(always)]
    pub fn try_send_stamped_slot(&self, value: &mut Option<T>) -> Result<PublicationTiming, SlotSendError>
    where T: crate::try_queue::PublicationStamp {
        if !self.0.receiver_alive.load(Ordering::Acquire) { return Err(SlotSendError::Disconnected); }
        let message = value.as_ref().ok_or(SlotSendError::Empty)?;
        let push_started_mono_ns = hexagent_types::types::monotonic_now_ns();
        let (copied_len, field) = crate::try_queue::publication_layout(message, Some(push_started_mono_ns));
        let reservation = self.0.queue.try_reserve().ok_or(SlotSendError::Full)?;
        // SAFETY: validated complete active prefix; unique reservation and
        // non-overlapping source. No fallible operation after reservation.
        unsafe {
            reservation.write_prefix(message, copied_len, field);
            // Do not drop the original: its ownership now belongs to the slot.
            std::ptr::write(value, None);
            reservation.release();
        }
        let push_returned_mono_ns = hexagent_types::types::monotonic_now_ns();
        if let Some(wake) = self.0.wake.get() { wake.notify(); }
        Ok(PublicationTiming { push_started_mono_ns, push_returned_mono_ns,
            notify_returned_mono_ns: hexagent_types::types::monotonic_now_ns() })
    }

    /// Diagnostic variant for physical order owners only. The actual release
    /// publication lies inside the push bracket. No allocation or extra queue.
    pub fn try_send_observed(&self, value: T) -> Result<PublicationTiming, TrySendError<T>> {
        if !self.0.receiver_alive.load(Ordering::Acquire) {
            return Err(TrySendError::Disconnected(value));
        }
        let push_started_mono_ns = hexagent_types::types::monotonic_now_ns();
        self.0.queue.try_push(value).map_err(TrySendError::Full)?;
        let push_returned_mono_ns = hexagent_types::types::monotonic_now_ns();
        if let Some(wake) = self.0.wake.get() { wake.notify(); }
        Ok(PublicationTiming { push_started_mono_ns, push_returned_mono_ns,
            notify_returned_mono_ns: hexagent_types::types::monotonic_now_ns() })
    }

    /// Stamp before transferring ownership, then publish directly from the
    /// caller's slot. All failures retain the value. The queue release lies
    /// inside [push_started, push_returned], including the stamp callback.
    /// This avoids a large Result<T> temporary and lets the command reuse the
    /// same start clock as the independently published observation sidecar.
    #[inline(always)]
    pub fn try_send_observed_slot(&self, value: &mut Option<T>,
        stamp: impl FnOnce(&mut T, u64)) -> Result<PublicationTiming, SlotSendError> {
        if !self.0.receiver_alive.load(Ordering::Acquire) { return Err(SlotSendError::Disconnected); }
        let Some(message) = value.as_mut() else { return Err(SlotSendError::Empty); };
        let push_started_mono_ns = hexagent_types::types::monotonic_now_ns();
        stamp(message, push_started_mono_ns);
        if !self.0.queue.try_push_slot(value) { return Err(SlotSendError::Full); }
        let push_returned_mono_ns = hexagent_types::types::monotonic_now_ns();
        if let Some(wake) = self.0.wake.get() { wake.notify(); }
        Ok(PublicationTiming { push_started_mono_ns, push_returned_mono_ns,
            notify_returned_mono_ns: hexagent_types::types::monotonic_now_ns() })
    }

    /// Bounded cold recovery wait. Retains the exact message on timeout/close.
    pub fn send_timeout(&self, mut value: T, timeout: Duration) -> Result<(), crossbeam_channel::SendTimeoutError<T>> {
        let started = std::time::Instant::now();
        loop {
            match self.try_send(value) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Disconnected(value)) => return Err(crossbeam_channel::SendTimeoutError::Disconnected(value)),
                Err(TrySendError::Full(returned)) => value = returned,
            }
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() { return Err(crossbeam_channel::SendTimeoutError::Timeout(value)); }
            std::thread::sleep(remaining.min(IDLE_POLL));
        }
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

/// Disabled lane: no sender exists and no disconnect event is generated.
/// Its one unused slot is reclaimed normally with the receiver.
pub fn never<T>() -> Receiver<T> {
    Receiver(Arc::new(Shared {
        queue: TryQueue::new(1), senders: AtomicUsize::new(1),
        receiver_alive: AtomicBool::new(true), wake: OnceLock::new(),
    }))
}
impl<T> Receiver<T> {
    pub(crate) fn close_and_discard(&self) {
        self.0.receiver_alive.store(false, Ordering::Release);
        while self.0.queue.try_pop().is_some() {}
    }
    /// Startup-only binding, before this owner begins parking. Producers which
    /// published before binding are covered by arm + recheck of every inbox.
    pub fn bind_wake(&self, wake: Arc<crate::wake::Wake>) {
        if let Err(wake) = self.0.wake.set(wake) {
            assert!(Arc::ptr_eq(self.0.wake.get().unwrap(), &wake), "one inbox must have exactly one wake owner");
        }
    }
    pub fn wake_handle(&self) -> Option<Arc<crate::wake::Wake>> { self.0.wake.get().cloned() }

    pub fn has_wake(&self) -> bool { self.0.wake.get().is_some() }
    pub fn arm_wake(&self) { if let Some(wake) = self.0.wake.get() { wake.arm(); } }
    pub fn cancel_wake(&self) { if let Some(wake) = self.0.wake.get() { wake.cancel(); } }
    pub fn wait_for_wake(&self, timeout: Duration) {
        if let Some(wake) = self.0.wake.get() { wake.wait(timeout); }
        else { std::thread::sleep(timeout); }
    }
    /// Cold owners/tests only. Wait without spinning on an unpublished slot.
    pub fn recv(&self) -> Result<T, crossbeam_channel::RecvError> {
        self.recv_with_control(|| {})
    }

    /// Service durable owner-local control hints while preserving the pending
    /// business reply. The control publisher must share this receiver wake.
    /// No deadline clock or periodic completion polling is introduced.
    pub fn recv_with_control(&self, mut control: impl FnMut()) -> Result<T, crossbeam_channel::RecvError> {
        // There is no caller deadline on recv. Reusing recv_timeout used to
        // read Instant on every 10us portable poll (and on immediate pickups).
        // Keep identical arm/recheck/wait and disconnect semantics, without
        // manufacturing a deadline which can never be returned to the caller.
        #[cfg(feature = "offline-delivery-probe")]
        let mut offline_loop = crate::offline_delivery_probe::ReceiveLoop::begin();
        loop {
            #[cfg(feature = "offline-delivery-probe")]
            offline_loop.check();
            match self.try_recv() {
                Ok(value) => return Ok(value),
                Err(TryRecvError::Disconnected) => return Err(crossbeam_channel::RecvError),
                Err(TryRecvError::Empty) => {}
            }
            control();
            if self.has_wake() {
                self.arm_wake();
                control();
                match self.try_recv() {
                    Ok(value) => { self.cancel_wake(); return Ok(value); }
                    Err(TryRecvError::Disconnected) => {
                        self.cancel_wake(); return Err(crossbeam_channel::RecvError);
                    }
                    Err(TryRecvError::Empty) => {}
                }
                #[cfg(feature = "offline-delivery-probe")]
                offline_loop.wait();
                self.wait_for_wake(Duration::from_secs(60));
            } else {
                #[cfg(feature = "offline-delivery-probe")]
                offline_loop.wait();
                std::thread::sleep(IDLE_POLL);
            }
        }
    }

    /// Cold shutdown/recovery only. Normal owner loops use `try_recv` and
    /// arbitrate higher-priority lanes before waiting. An unfinished producer
    /// reservation yields just like an empty queue; it is never spun on.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, crossbeam_channel::RecvTimeoutError> {
        self.recv_timeout_with_poll(timeout, IDLE_POLL)
    }

    /// Co-located FIFO owners need an explicit polling budget so their idle
    /// wakeups leave lower-priority producers runnable on the same CPU.
    pub fn recv_timeout_with_poll(&self, timeout: Duration, poll: Duration) -> Result<T, crossbeam_channel::RecvTimeoutError> {
        self.recv_timeout_impl::<false>(timeout, poll, &mut 0, || false)
    }

    /// Last return from a park/sleep in this receive, zero for an immediate
    /// pickup. This is userspace resume, not kernel runnable or packet arrival.
    pub fn recv_timeout_observed(&self, timeout: Duration, poll: Duration,
        resumed_mono_ns: &mut u64) -> Result<T, crossbeam_channel::RecvTimeoutError> {
        self.recv_timeout_impl::<true>(timeout, poll, resumed_mono_ns, || false)
    }

    /// Return Timeout early for a coalesced control-state notification so the
    /// owner can refresh health. The predicate must consume a durable pending
    /// bit; wake alone is only a hint. Business queue ownership/order is unchanged.
    pub fn recv_timeout_observed_interruptible(&self, timeout: Duration, poll: Duration,
        resumed_mono_ns: &mut u64, interrupted: impl FnMut() -> bool) -> Result<T, crossbeam_channel::RecvTimeoutError> {
        self.recv_timeout_impl::<true>(timeout, poll, resumed_mono_ns, interrupted)
    }

    fn recv_timeout_impl<const OBSERVE: bool>(&self, timeout: Duration, poll: Duration,
        resumed_mono_ns: &mut u64, mut interrupted: impl FnMut() -> bool) -> Result<T, crossbeam_channel::RecvTimeoutError> {
        #[cfg(feature = "offline-delivery-probe")]
        let mut offline_loop = crate::offline_delivery_probe::ReceiveLoop::begin();
        *resumed_mono_ns = 0;
        assert!(!poll.is_zero(), "owner polling must yield the CPU");
        let started = std::time::Instant::now();
        loop {
            #[cfg(feature = "offline-delivery-probe")]
            offline_loop.check();
            if interrupted() { return Err(crossbeam_channel::RecvTimeoutError::Timeout); }
            match self.try_recv() {
                Ok(value) => return Ok(value),
                Err(TryRecvError::Disconnected) => return Err(crossbeam_channel::RecvTimeoutError::Disconnected),
                Err(TryRecvError::Empty) => {}
            }
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(crossbeam_channel::RecvTimeoutError::Timeout);
            }
            if self.0.wake.get().is_some() {
                self.arm_wake();
                if interrupted() {
                    self.cancel_wake();
                    return Err(crossbeam_channel::RecvTimeoutError::Timeout);
                }
                match self.try_recv() {
                    Ok(value) => { self.cancel_wake(); return Ok(value); }
                    Err(TryRecvError::Disconnected) => {
                        self.cancel_wake();
                        return Err(crossbeam_channel::RecvTimeoutError::Disconnected);
                    }
                    Err(TryRecvError::Empty) => {}
                }
                #[cfg(feature = "offline-delivery-probe")]
                offline_loop.wait();
                self.wait_for_wake(remaining);
            } else {
                #[cfg(feature = "offline-delivery-probe")]
                offline_loop.wait();
                std::thread::sleep(remaining.min(poll));
            }
            if OBSERVE { *resumed_mono_ns = hexagent_types::types::monotonic_now_ns(); }
        }
    }

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
    // SAFETY: element zero is an owned inline u64 with no value invariant.
    unsafe impl crate::try_queue::PublicationStamp for [u64; 1024] {
        fn publication_stamp(&self) -> Option<&u64> { Some(&self[0]) }
    }

    #[test]
    fn untimed_receive_delayed_publication_and_last_sender_close_drain_exactly_once() {
        for use_wake in [false, true] {
            let (tx, rx) = bounded_with_wake(2, use_wake.then(|| Arc::new(crate::wake::Wake::default())));
            let (ready, started) = std::sync::mpsc::sync_channel(1);
            let join = std::thread::spawn(move || {
                ready.send(()).unwrap();
                for expected in 0..128 { assert_eq!(rx.recv().unwrap(), expected); }
                assert!(rx.recv().is_err());
            });
            started.recv_timeout(Duration::from_secs(1)).unwrap();
            std::thread::sleep(Duration::from_millis(2));
            for sequence in 0..128 { tx.send(sequence).unwrap(); }
            drop(tx); join.join().unwrap();
        }
    }

    #[test]
    fn compact_prefix_reuses_large_slots_without_losing_owned_fields_or_drops() {
        #[derive(Debug)]
        struct Owned(Arc<AtomicUsize>);
        impl Drop for Owned { fn drop(&mut self) { self.0.fetch_add(1, Ordering::Relaxed); } }
        #[repr(C)]
        #[derive(Debug)]
        enum Message { Large([u64; 960], Box<Owned>), Small { name: String, stamp: u64, owner: Box<Owned> } }
        // SAFETY: repr(C) tag precedes the payload. Small includes the entire
        // prefix through every active field; Large uses the full representation.
        unsafe impl crate::try_queue::PublicationStamp for Message {
            fn publication_stamp(&self) -> Option<&u64> { match self { Self::Small { stamp, .. } => Some(stamp), _ => None } }
            fn copied_prefix_len(&self) -> usize {
                match self {
                    Self::Small { name, stamp, owner } => {
                        let base = self as *const Self as usize;
                        let end = |address: usize, size| address - base + size;
                        end(name as *const _ as usize, std::mem::size_of_val(name))
                            .max(end(stamp as *const _ as usize, 8))
                            .max(end(owner as *const _ as usize, std::mem::size_of_val(owner)))
                    },
                    _ => std::mem::size_of::<Self>(),
                }
            }
        }
        let drops = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = bounded(1);
        for sequence in 0..1000 {
            tx.try_send_stamped(Message::Large([sequence; 960], Box::new(Owned(drops.clone())))).unwrap();
            let small = Message::Small { name: format!("owner-{sequence}"), stamp: 0, owner: Box::new(Owned(drops.clone())) };
            let Err(TrySendError::Full(small)) = tx.try_send_stamped(small) else { panic!("full retains owner"); };
            match rx.try_recv().unwrap() {
                Message::Large(bytes, owner) => { assert_eq!(bytes, [sequence; 960]); assert!(Arc::ptr_eq(&owner.0, &drops)); },
                _ => panic!("large variant lost"),
            }
            let timing = tx.try_send_stamped(small).unwrap();
            match rx.try_recv().unwrap() {
                Message::Small { name, stamp, owner } => {
                    assert_eq!(name, format!("owner-{sequence}"));
                    assert_eq!(stamp, timing.push_started_mono_ns);
                    assert!(Arc::ptr_eq(&owner.0, &drops));
                },
                _ => panic!("compact variant lost"),
            }
        }
        assert_eq!(drops.load(Ordering::Relaxed), 2000);
    }

    #[test]
    fn stamped_owned_publication_retains_large_message_on_full_and_disconnect() {
        let (tx, rx) = bounded(1);
        let timing = tx.try_send_stamped([7_u64; 1024]).unwrap();
        assert_eq!(tx.try_send_stamped([9; 1024]), Err(TrySendError::Full([9; 1024])));
        assert_eq!(tx.try_send_owned([11; 1024]), Err(TrySendError::Full([11; 1024])));
        let value = rx.try_recv().unwrap();
        assert_eq!(value[0], timing.push_started_mono_ns);
        assert_eq!(&value[1..], &[7; 1023]);
        assert!(timing.push_started_mono_ns <= timing.push_returned_mono_ns);
        assert!(timing.push_returned_mono_ns <= timing.notify_returned_mono_ns);
        tx.try_send_owned([13; 1024]).unwrap();
        assert_eq!(rx.try_recv().unwrap(), [13; 1024]);
        drop(rx);
        assert_eq!(tx.try_send_stamped([9; 1024]), Err(TrySendError::Disconnected([9; 1024])));
        assert_eq!(tx.try_send_owned([11; 1024]), Err(TrySendError::Disconnected([11; 1024])));
    }

    #[test]
    fn owned_stamp_panic_drops_once_without_reserving_fifo_head() {
        #[derive(Debug)]
        struct Message(Arc<AtomicUsize>, bool, u64);
        impl Drop for Message { fn drop(&mut self) { self.0.fetch_add(1, Ordering::Relaxed); } }
        // SAFETY: ordinary owned timestamp with no value invariant.
        unsafe impl crate::try_queue::PublicationStamp for Message {
            fn publication_stamp(&self) -> Option<&u64> {
                assert!(!self.1, "selector unwind"); Some(&self.2)
            }
        }
        let drops = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = bounded(1);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = tx.try_send_stamped(Message(drops.clone(), true, 0));
        }));
        assert!(result.is_err());
        assert!(rx.is_empty());
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        tx.try_send_stamped(Message(drops.clone(), false, 0)).unwrap();
        drop(rx.try_recv().unwrap());
        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn stamped_owned_wake_delivery_preserves_order_and_single_drop() {
        #[derive(Debug)]
        struct Message { sequence: usize, stamp: u64, dropped: Arc<AtomicUsize> }
        impl Drop for Message { fn drop(&mut self) { self.dropped.fetch_add(1, Ordering::Relaxed); } }
        // SAFETY: ordinary owned timestamp with no value invariant.
        unsafe impl crate::try_queue::PublicationStamp for Message {
            fn publication_stamp(&self) -> Option<&u64> { Some(&self.stamp) }
        }
        let dropped = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = bounded_with_wake(1, Some(Arc::new(crate::wake::Wake::default())));
        let counts = dropped.clone();
        let producer = std::thread::spawn(move || {
            for sequence in 0..5000 {
                let mut pending = Message { sequence, stamp: 0, dropped: counts.clone() };
                loop { match tx.try_send_stamped(pending) {
                    Ok(_) => break,
                    Err(TrySendError::Full(retained)) => {
                        assert_eq!(retained.sequence, sequence);
                        pending = retained;
                        std::thread::yield_now();
                    },
                    _ => panic!("receiver remains connected"),
                }}
            }
        });
        for sequence in 0..5000 {
            let message = rx.recv_timeout(Duration::from_secs(1)).unwrap();
            assert_eq!(message.sequence, sequence);
            assert!(message.stamp > 0 && message.stamp <= hexagent_types::types::monotonic_now_ns());
        }
        producer.join().unwrap();
        assert_eq!(dropped.load(Ordering::Relaxed), 5000);
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Disconnected)));
    }

    #[test]
    fn observed_slot_retains_large_payload_on_full_empty_and_disconnect() {
        let (tx, rx) = bounded(1);
        let mut pending = Some([7_u64; 1024]);
        let timing = tx.try_send_observed_slot(&mut pending, |v, now| v[0] = now).unwrap();
        assert!(pending.is_none());
        pending = Some([8; 1024]);
        assert_eq!(tx.try_send_observed_slot(&mut pending, |_, _| {}), Err(SlotSendError::Full));
        assert_eq!(pending.as_ref().unwrap()[1023], 8);
        let received = rx.try_recv().unwrap();
        assert_eq!(received[0], timing.push_started_mono_ns);
        assert_eq!(received[1023], 7);
        assert!(timing.push_started_mono_ns <= timing.push_returned_mono_ns);
        assert!(timing.push_returned_mono_ns <= timing.notify_returned_mono_ns);
        assert_eq!(tx.try_send_observed_slot(&mut None, |_, _| {}), Err(SlotSendError::Empty));
        drop(rx);
        assert_eq!(tx.try_send_observed_slot(&mut pending, |_, _| panic!("must not stamp disconnected")), Err(SlotSendError::Disconnected));
        assert_eq!(pending.unwrap()[1023], 8);
    }

    #[test]
    fn observed_slot_stamps_before_transfer_and_drops_each_message_once() {
        struct Message { sequence: usize, stamp: u64, dropped: Arc<AtomicUsize> }
        impl Drop for Message { fn drop(&mut self) { self.dropped.fetch_add(1, Ordering::Relaxed); } }
        let dropped = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = bounded_with_wake(1, Some(Arc::new(crate::wake::Wake::default())));
        let counts = Arc::clone(&dropped);
        let producer = std::thread::spawn(move || {
            for sequence in 0..5000 {
                let mut pending = Some(Message { sequence, stamp: 0, dropped: Arc::clone(&counts) });
                loop { match tx.try_send_observed_slot(&mut pending, |m, now| m.stamp = now) {
                    Ok(_) => { assert!(pending.is_none()); break; },
                    Err(SlotSendError::Full) => { assert_eq!(pending.as_ref().unwrap().sequence, sequence); std::thread::yield_now(); },
                    _ => panic!("receiver remains connected"),
                }}
            }
        });
        for sequence in 0..5000 {
            let value = rx.recv_timeout(Duration::from_secs(1)).unwrap();
            assert_eq!(value.sequence, sequence);
            assert!(value.stamp > 0 && value.stamp <= hexagent_types::types::monotonic_now_ns());
        }
        producer.join().unwrap();
        assert_eq!(dropped.load(Ordering::Relaxed), 5000);
    }

    #[test]
    fn observed_slot_callback_panic_cannot_reserve_an_unpublished_head() {
        let (tx, rx) = bounded(1);
        let mut pending = Some(9);
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = tx.try_send_observed_slot(&mut pending, |_, _| panic!("fixture"));
        })).is_err());
        assert_eq!(pending, Some(9));
        assert!(rx.is_empty());
        tx.try_send_observed_slot(&mut pending, |_, _| {}).unwrap();
        assert_eq!(rx.try_recv().unwrap(), 9);
    }

    #[test]
    fn observed_publication_retains_overflow_value_and_disconnect_semantics() {
        let (tx, rx) = bounded_with_wake(1, Some(Arc::new(crate::wake::Wake::default())));
        let timing = tx.try_send_observed(41).unwrap();
        assert!(timing.push_started_mono_ns > 0);
        assert!(timing.push_started_mono_ns <= timing.push_returned_mono_ns);
        assert!(timing.push_returned_mono_ns <= timing.notify_returned_mono_ns);
        assert!(matches!(tx.try_send_observed(42), Err(TrySendError::Full(42))));
        let mut resumed = 123;
        assert_eq!(rx.recv_timeout_observed(Duration::from_millis(10), IDLE_POLL, &mut resumed).unwrap(), 41);
        assert_eq!(resumed, 0, "an immediate receive did not resume from a park");
        tx.try_send_observed(42).unwrap();
        assert_eq!(rx.try_recv().unwrap(), 42);
        drop(rx);
        assert!(matches!(tx.try_send_observed(43), Err(TrySendError::Disconnected(43))));
    }

    #[test]
    fn observed_wake_ordering_is_lossless_under_repeated_handoffs() {
        let (tx, rx) = bounded_with_wake(1, Some(Arc::new(crate::wake::Wake::default())));
        let producer = std::thread::spawn(move || {
            for i in 0..5000 {
                let mut value = i;
                loop { match tx.try_send_observed(value) {
                    Ok(_) => break,
                    Err(TrySendError::Full(v)) => { value = v; std::thread::yield_now(); },
                    Err(_) => panic!("receiver disconnected"),
                }}
            }
        });
        for i in 0..5000 {
            let mut resumed = 0;
            assert_eq!(rx.recv_timeout_observed(Duration::from_secs(1), IDLE_POLL, &mut resumed).unwrap(), i);
            assert!(resumed <= hexagent_types::types::monotonic_now_ns());
        }
        producer.join().unwrap();
        assert!(rx.is_empty());
    }

    use std::sync::Barrier;

    #[test]
    fn sender_observes_owner_shutdown_with_queued_values() {
        let (tx, rx) = bounded(2);
        let other = tx.clone();
        assert!(!tx.is_disconnected());
        tx.try_send(7).unwrap();
        drop(rx);
        assert!(tx.is_disconnected());
        assert!(other.is_disconnected());
        assert_eq!(tx.try_send(9), Err(TrySendError::Disconnected(9)));
    }

    #[test]
    fn timed_owner_receive_yields_for_preempted_publication_and_keeps_fifo() {
        let (tx, rx) = bounded(2);
        let second = tx.clone();
        let reserved = Barrier::new(2);
        let release = AtomicBool::new(false);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                tx.0.queue.push_with((7_u32, 1_u32), || {
                    reserved.wait();
                    // A bounded escape also makes a broken spinning receiver
                    // fail this test, rather than hanging the whole test suite.
                    let started = std::time::Instant::now();
                    while !release.load(Ordering::Acquire)
                        && started.elapsed() < Duration::from_millis(250)
                    {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                }).unwrap();
            });
            reserved.wait();
            second.try_send((9, 2)).unwrap();
            assert!(rx.has_pending());
            assert!(!rx.front_ready());
            assert_eq!(second.try_send((7, 3)), Err(TrySendError::Full((7, 3))));
            let received = rx.recv_timeout_with_poll(Duration::from_millis(5), Duration::from_micros(50));
            release.store(true, Ordering::Release);
            assert_eq!(received, Err(crossbeam_channel::RecvTimeoutError::Timeout));
            assert_eq!(rx.recv_timeout(Duration::from_secs(1)), Ok((7, 1)));
            assert_eq!(rx.recv_timeout(Duration::from_secs(1)), Ok((9, 2)));
            assert!(rx.is_empty());
        });
        drop(tx);
        drop(second);
        assert_eq!(rx.recv(), Err(crossbeam_channel::RecvError));
    }

    /// Models a cold producer preempted after reserving its FIFO slot. The
    /// dispatcher must be able to return to independent quote work immediately.
    /// The old channel's public select API supplies the same reservation seam.
    #[test]
    #[ignore = "manual preempted-ingress benchmark; release --ignored --nocapture"]
    fn preempted_cold_ingress_benchmark() {
        const N: usize = 100_000;
        const HOLD: Duration = Duration::from_millis(10);
        fn report(label: &str, mut samples: Vec<u128>) {
            samples.sort_unstable();
            println!("cold_ingress mode={label} N={N} injected_preemptions=200 held_ms=10 boundary=dispatcher_try_receive_to_independent_quote_turn p50_ns={} p99_ns={} p999_ns={} max_ns={} depth_high_water=1 overflow=0",
                samples[N / 2 - 1], samples[N * 99 / 100 - 1],
                samples[N * 999 / 1000 - 1], samples[N - 1]);
        }
        let (old_tx, old_rx) = crossbeam_channel::bounded(64);
        let mut before = Vec::with_capacity(N);
        for n in 0..N {
            if n % 500 == 0 {
                let reserved = Barrier::new(2);
                std::thread::scope(|scope| {
                    scope.spawn(|| {
                        let mut select = crossbeam_channel::Select::new();
                        select.send(&old_tx);
                        let reservation = select.select();
                        reserved.wait();
                        std::thread::sleep(HOLD);
                        reservation.send(&old_tx, n).unwrap();
                    });
                    reserved.wait();
                    let start = std::time::Instant::now();
                    assert_eq!(old_rx.try_recv(), Ok(n));
                    before.push(start.elapsed().as_nanos());
                });
            } else {
                old_tx.try_send(n).unwrap();
                let start = std::time::Instant::now();
                assert_eq!(old_rx.try_recv(), Ok(n));
                before.push(start.elapsed().as_nanos());
            }
        }
        report("crossbeam", before);
        let (tx, rx) = bounded(64);
        let mut after = Vec::with_capacity(N);
        for n in 0..N {
            if n % 500 == 0 {
                let reserved = Barrier::new(2);
                let resume = Barrier::new(2);
                std::thread::scope(|scope| {
                    scope.spawn(|| {
                        tx.0.queue.push_with(n, || {
                            reserved.wait();
                            std::thread::sleep(HOLD);
                            resume.wait();
                        }).unwrap();
                    });
                    reserved.wait();
                    let start = std::time::Instant::now();
                    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
                    after.push(start.elapsed().as_nanos());
                    resume.wait();
                });
                // The exact retained message arrives once after publication.
                assert_eq!(rx.try_recv(), Ok(n));
            } else {
                tx.try_send(n).unwrap();
                let start = std::time::Instant::now();
                assert_eq!(rx.try_recv(), Ok(n));
                after.push(start.elapsed().as_nanos());
            }
        }
        report("poll", after);
    }

    #[test]
    fn cold_timeout_checks_ready_and_disconnected_before_deadline() {
        let (tx, rx) = bounded(1);
        assert_eq!(rx.recv_timeout(Duration::ZERO), Err(crossbeam_channel::RecvTimeoutError::Timeout));
        tx.try_send(7).unwrap();
        drop(tx);
        assert_eq!(rx.recv_timeout(Duration::ZERO), Ok(7));
        assert_eq!(rx.recv_timeout(Duration::ZERO), Err(crossbeam_channel::RecvTimeoutError::Disconnected));
    }

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

#[cfg(test)]
mod wake_lane_tests {
    use super::*;
    #[test]
    fn woke_lane_retains_full_value_order_and_disconnect() {
        let (tx, rx) = bounded_with_wake(2, Some(Arc::new(crate::wake::Wake::default())));
        rx.arm_wake();
        tx.try_send((1, 10)).unwrap(); tx.try_send((2, 20)).unwrap();
        assert!(matches!(tx.try_send((3, 30)), Err(TrySendError::Full((3, 30)))));
        rx.wait_for_wake(Duration::from_millis(10));
        assert_eq!(rx.recv_timeout(Duration::from_millis(10)).unwrap(), (1, 10));
        tx.try_send((3, 30)).unwrap(); drop(tx);
        assert_eq!(rx.recv().unwrap(), (2, 20)); assert_eq!(rx.recv().unwrap(), (3, 30));
        assert!(matches!(rx.recv(), Err(crossbeam_channel::RecvError)));
    }
    #[test]
    fn shared_dispatcher_wake_notifies_every_inbox_without_reordering() {
        let wake = Arc::new(crate::wake::Wake::default());
        let (a, ar) = bounded_with_wake(8, Some(wake.clone()));
        let (b, br) = bounded_with_wake(8, Some(wake));
        ar.arm_wake(); b.try_send((2, 0)).unwrap(); ar.wait_for_wake(Duration::from_millis(1));
        a.try_send((1, 0)).unwrap(); b.try_send((2, 1)).unwrap();
        assert_eq!(ar.try_recv().unwrap(), (1, 0));
        assert_eq!(br.try_recv().unwrap(), (2, 0)); assert_eq!(br.try_recv().unwrap(), (2, 1));
    }
    #[test]
    fn concurrent_publication_and_sleep_deliver_exactly_once() {
        let (tx, rx) = bounded_with_wake(8, Some(Arc::new(crate::wake::Wake::default())));
        let producer = std::thread::spawn(move || {
            for id in 0..10_000 {
                let mut value = id;
                loop { match tx.try_send(value) {
                    Ok(()) => break,
                    Err(TrySendError::Full(v)) => { value = v; std::thread::yield_now(); }
                    Err(TrySendError::Disconnected(_)) => panic!(),
                } }
            }
        });
        for id in 0..10_000 { assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), id); }
        producer.join().unwrap(); assert!(matches!(rx.try_recv(), Err(TryRecvError::Disconnected)));
    }
}

#[cfg(test)]
mod owner_notification_tests {
    use super::*;
    use crate::wake::{StateNotification, Wake};
    use std::sync::atomic::AtomicU64;

    struct Owned { stamp: u64, sequence: u64, drops: Arc<AtomicUsize>, payload: [u8; 4096] }
    impl Drop for Owned { fn drop(&mut self) { self.drops.fetch_add(1, Ordering::Relaxed); } }
    // SAFETY: the default prefix includes the entire initialized value; stamp
    // is an ordinary inline observation field valid for every u64.
    unsafe impl crate::try_queue::PublicationStamp for Owned {
        fn publication_stamp(&self) -> Option<&u64> { Some(&self.stamp) }
    }
    #[test]
    fn stamped_pending_retains_failure_and_drops_exactly_once() {
        let drops = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = bounded(1);
        let mut first = Some(Owned { stamp: 7, sequence: 1, drops: drops.clone(), payload: [13;4096] });
        let timing = tx.try_send_stamped_slot(&mut first).unwrap();
        assert!(first.is_none());
        let mut second = Some(Owned { stamp: 9, sequence: 2, drops: drops.clone(), payload: [17;4096] });
        assert_eq!(tx.try_send_stamped_slot(&mut second), Err(SlotSendError::Full));
        assert_eq!(second.as_ref().unwrap().stamp,9);
        let got=rx.try_recv().ok().unwrap();
        assert_eq!((got.sequence,got.stamp),(1,timing.push_started_mono_ns));
        assert_eq!(got.payload,[13;4096]); drop(got);
        assert_eq!(tx.try_send_stamped_slot(&mut first),Err(SlotSendError::Empty));
        drop(rx);
        assert_eq!(tx.try_send_stamped_slot(&mut second),Err(SlotSendError::Disconnected));
        assert_eq!((second.as_ref().unwrap().sequence,second.as_ref().unwrap().stamp),(2,9));
        drop(second); assert_eq!(drops.load(Ordering::Relaxed),2);
    }
    #[test]
    fn stamped_pending_multiple_producers_preserve_order_and_identity() {
        let drops=Arc::new(AtomicUsize::new(0)); let (tx,rx)=bounded(4);
        let threads:Vec<_>=(0..4).map(|id| {let tx=tx.clone();let drops=drops.clone();std::thread::spawn(move || {
            for sequence in 0..500 { let mut pending=Some(Owned {stamp:0,sequence: id*500+sequence,drops:drops.clone(),payload:[id as u8;4096]});
                while tx.try_send_stamped_slot(&mut pending).is_err() { std::thread::yield_now(); }
                assert!(pending.is_none());
            }
        })}).collect(); drop(tx);
        let mut expected=[0;4];
        for _ in 0..2000 {let value=rx.recv().unwrap();let id=(value.sequence/500) as usize;
            assert_eq!(value.sequence%500,expected[id]); expected[id]+=1;
            assert_eq!(value.payload,[id as u8;4096]);assert_ne!(value.stamp,0);
        }
        for t in threads { t.join().unwrap(); }
        assert_eq!(expected,[500;4]);assert_eq!(drops.load(Ordering::Relaxed),2000);
    }
    #[test]
    fn state_hint_before_arm_and_during_recheck_is_not_lost() {
        for during_recheck in [false,true] {
            let wake=Arc::new(Wake::default());let hint=StateNotification::default();hint.bind(wake.clone());
            let (_tx,rx)=bounded_with_wake::<u64>(1,Some(wake));
            if !during_recheck {hint.notify();}
            let mut calls=0;
            let result=rx.recv_timeout_observed_interruptible(Duration::from_secs(5),IDLE_POLL,&mut 0,||{
                calls+=1; if during_recheck && calls==2 {hint.notify();}
                hint.take_pending()
            });
            assert_eq!(result,Err(crossbeam_channel::RecvTimeoutError::Timeout));
            assert_eq!(calls,if during_recheck {2}else{1});assert!(!hint.take_pending());
        }
    }
    #[test]
    fn state_hint_interrupts_park_without_command_and_is_owner_local() {
        let wake=Arc::new(Wake::default());let hint=Arc::new(StateNotification::default());hint.bind(wake.clone());
        let other=StateNotification::default();let state=Arc::new(AtomicU64::new(0));
        let (_tx,rx)=bounded_with_wake::<u64>(1,Some(wake));
        let (ready_tx,ready_rx)=crossbeam_channel::bounded(1);let (done_tx,done_rx)=crossbeam_channel::bounded(1);
        let h=hint.clone();let observed=state.clone();
        let worker=std::thread::spawn(move || {
            ready_tx.send(()).unwrap();let mut resumed=0;
            let result=rx.recv_timeout_observed_interruptible(Duration::from_secs(5),IDLE_POLL,&mut resumed,||h.take_pending());
            assert!(matches!(result,Err(crossbeam_channel::RecvTimeoutError::Timeout)));
            done_tx.send(observed.load(Ordering::Acquire)).unwrap();
        });
        ready_rx.recv().unwrap(); std::thread::sleep(Duration::from_millis(10));
        state.store(42,Ordering::Release);for _ in 0..1000 {hint.notify();}
        assert!(!other.take_pending());assert_eq!(done_rx.recv_timeout(Duration::from_secs(1)).unwrap(),42);
        worker.join().unwrap();
        // Coalesced duplicates may remain after consumption, but cannot grow a queue.
        hint.take_pending();assert!(!hint.take_pending());
    }
    #[test]
    fn interrupt_preserves_queued_business_and_disconnect() {
        let wake=Arc::new(Wake::default());let hint=StateNotification::default();hint.bind(wake.clone());
        let (tx,rx)=bounded_with_wake(2,Some(wake));tx.try_send(11).unwrap();tx.try_send(12).unwrap();hint.notify();
        assert!(matches!(rx.recv_timeout_observed_interruptible(Duration::from_secs(1),IDLE_POLL,&mut 0,||hint.take_pending()),Err(crossbeam_channel::RecvTimeoutError::Timeout)));
        assert_eq!(rx.try_recv(),Ok(11));assert_eq!(rx.try_recv(),Ok(12));drop(tx);
        assert!(matches!(rx.recv_timeout_observed_interruptible(Duration::from_secs(1),IDLE_POLL,&mut 0,||hint.take_pending()),Err(crossbeam_channel::RecvTimeoutError::Disconnected)));
    }
}
