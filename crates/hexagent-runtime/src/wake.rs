//! Single-consumer notification for bounded polling lanes. The queue remains
//! the ordering/ownership authority: arm, recheck every inbox, then wait.
//! Linux uses its existing private futex. Explicit `event()` consumers use
//! Darwin's public atomic address-wait API when available (macOS 14.4+).
//! Default portable lanes and older macOS retain bounded polling. No thread
//! park permit, notification queue, per-message allocation or user mutex.
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

// Immutable platform capability, resolved only while constructing startup
// Wake objects. Never an account/strategy authority or a hot-path lookup.
#[cfg(target_os = "macos")]
mod darwin {
    use super::*;
    type Wait = unsafe extern "C" fn(*mut libc::c_void, u64, usize, u32, u32, u64) -> libc::c_int;
    type Notify = unsafe extern "C" fn(*mut libc::c_void, usize, u32) -> libc::c_int;
    pub(super) struct Api { pub wait: Wait, pub notify: Notify }
    impl Api {
        fn resolve(mut symbol: impl FnMut(&'static [u8]) -> *mut libc::c_void) -> Option<Self> {
            let wait = symbol(b"os_sync_wait_on_address_with_timeout\0");
            let notify = symbol(b"os_sync_wake_by_address_any\0");
            if wait.is_null() || notify.is_null() { return None; }
            // SAFETY: these public libSystem symbols have the exact C ABI
            // declared in os/os_sync_wait_on_address.h and os/clock.h. Resolve
            // dynamically so an older macOS can load the binary and poll.
            Some(unsafe { Self { wait: std::mem::transmute::<*mut libc::c_void, Wait>(wait),
                notify: std::mem::transmute::<*mut libc::c_void, Notify>(notify) } })
        }
    }
    pub(super) fn api() -> Option<&'static Api> {
        static API: OnceLock<Option<Api>> = OnceLock::new();
        API.get_or_init(|| Api::resolve(|name| unsafe {
            libc::dlsym(libc::RTLD_DEFAULT, name.as_ptr().cast())
        })).as_ref()
    }
    #[cfg(test)]
    mod tests {
        use super::*;
        #[test]
        fn unavailable_or_partial_address_wait_api_keeps_portable_fallback() {
            assert!(Api::resolve(|_| std::ptr::null_mut()).is_none());
            // No transmute/call occurs if either half of the pair is absent.
            assert!(Api::resolve(|name| if name.starts_with(b"os_sync_wait") {
                std::ptr::dangling_mut::<libc::c_void>()
            } else { std::ptr::null_mut() }).is_none());
        }
        #[test]
        fn public_api_accepts_the_declared_clock_size_and_atomic_compare() {
            let Some(api) = api() else { return; }; // older macOS uses polling
            let word = AtomicU32::new(1);
            let result = unsafe { (api.wait)(word.as_ptr().cast(), 1, 4, 0, 32, 1_000_000) };
            assert_eq!(result, -1);
            assert_eq!(std::io::Error::last_os_error().raw_os_error(), Some(libc::ETIMEDOUT));
            word.store(0, Ordering::SeqCst);
            assert!(unsafe { (api.wait)(word.as_ptr().cast(), 1, 4, 0, 32, 1_000_000_000) } >= 0);
        }
    }
}

// Independent cache line, fixed address once placed into the owner's Arc.
#[repr(align(64))]
#[derive(Default)]
pub struct Wake {
    armed: AtomicU32,
    #[cfg(target_os = "macos")]
    api: Option<&'static darwin::Api>,
}

impl Wake {
    /// Startup only. Same arm/recheck protocol as the existing Linux futex;
    /// availability fallback does not alter queue ownership or capacity.
    pub fn event() -> Self {
        Self {
            armed: AtomicU32::new(0),
            #[cfg(target_os = "macos")]
            api: darwin::api(),
        }
    }
    #[cfg(feature = "offline-delivery-probe")]
    pub fn offline_wait_kind(&self) -> &'static str {
        #[cfg(target_os = "linux")]
        { "futex" }
        #[cfg(target_os = "macos")]
        { if self.api.is_some() { "address-wait" } else { "poll" } }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        { "poll" }
    }
    pub fn arm(&self) { self.armed.store(1, Ordering::SeqCst); }
    pub fn cancel(&self) { self.armed.store(0, Ordering::SeqCst); }
    pub fn notify(&self) {
        if self.armed.swap(0, Ordering::SeqCst) == 1 {
            #[cfg(target_os = "linux")]
            unsafe {
                libc::syscall(libc::SYS_futex, self.armed.as_ptr(),
                    libc::FUTEX_WAKE | libc::FUTEX_PRIVATE_FLAG, 1);
            }
            #[cfg(target_os = "macos")]
            if let Some(api) = self.api {
                // SAFETY: same live, four-byte aligned address/size and private
                // flags as wait. ENOENT (not yet waiting) is harmless: the
                // subsequent atomic compare observes zero and cannot park.
                unsafe { (api.notify)(self.armed.as_ptr().cast(), 4, 0); }
            }
        }
    }
    pub fn wait(&self, timeout: Duration) {
        if timeout.is_zero() { self.cancel(); return; }
        #[cfg(target_os = "linux")]
        {
            let time = libc::timespec { tv_sec: timeout.as_secs().min(i64::MAX as u64) as _,
                tv_nsec: timeout.subsec_nanos() as _ };
            // EINTR/spurious return/timeout are handled by the next inbox check.
            unsafe { libc::syscall(libc::SYS_futex, self.armed.as_ptr(),
                libc::FUTEX_WAIT | libc::FUTEX_PRIVATE_FLAG, 1, &time); }
        }
        #[cfg(target_os = "macos")]
        if let Some(api) = self.api {
            // SAFETY: no user-space check/park gap: the kernel atomically
            // compares this AtomicU32 with 1. OS_CLOCK_MACH_ABSOLUTE_TIME = 32;
            // timeout is relative nanoseconds, size 4 and process-private flags.
            let result = unsafe { (api.wait)(self.armed.as_ptr().cast(), 1, 4, 0, 32,
                timeout.as_nanos().min(u64::MAX as u128) as u64) };
            if result < 0 {
                let error = std::io::Error::last_os_error().raw_os_error();
                if !matches!(error, Some(libc::EINTR | libc::ETIMEDOUT)) {
                    // A resource/unsupported error must not create a busy loop.
                    std::thread::sleep(timeout.min(Duration::from_micros(10)));
                }
            }
        } else if self.armed.load(Ordering::SeqCst) == 1 {
            std::thread::sleep(timeout.min(Duration::from_micros(10)));
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        if self.armed.load(Ordering::SeqCst) == 1 {
            std::thread::sleep(timeout.min(Duration::from_micros(10)));
        }
        self.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn publication_between_arm_and_wait_is_not_lost() {
        for wake in [Wake::default(), Wake::event()] {
            wake.arm();
            wake.notify();
            assert_eq!(wake.armed.load(Ordering::SeqCst), 0);
            wake.wait(Duration::from_millis(1));
        }
    }

    #[test]
    fn notification_after_parking_and_repeated_timeouts_are_lossless() {
        let wake = Arc::new(Wake::event());
        let value = Arc::new(AtomicU32::new(0));
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let (done_tx, done_rx) = std::sync::mpsc::sync_channel(1);
        let (w, v) = (wake.clone(), value.clone());
        let worker = std::thread::spawn(move || {
            for expected in 1..=32 {
                w.arm(); w.wait(Duration::ZERO); // previous timeout cannot consume future work
                w.arm(); ready_tx.send(expected).unwrap();
                while v.load(Ordering::Acquire) < expected {
                    w.wait(Duration::from_secs(2));
                    w.arm();
                    if v.load(Ordering::Acquire) >= expected { w.cancel(); break; }
                }
                w.cancel(); done_tx.send(expected).unwrap();
            }
        });
        for expected in 1..=32 {
            assert_eq!(ready_rx.recv_timeout(Duration::from_secs(2)).unwrap(), expected);
            if expected == 1 { std::thread::sleep(Duration::from_millis(10)); }
            value.store(expected, Ordering::Release); wake.notify();
            assert_eq!(done_rx.recv_timeout(Duration::from_secs(1)).unwrap(), expected);
        }
        worker.join().unwrap();
    }
}

/// Coalesced state-change hint, not a business message or admission authority.
/// Writers publish authoritative state first, then set pending and notify. The
/// sole consumer checks pending both before and after arming its inbox wake.
/// One bit is sufficient: the consumer always rereads the complete current state.
/// No allocation after startup, bounded storage, no lost notification on overflow.
#[derive(Default)]
pub struct StateNotification {
    pending: AtomicBool,
    wake: OnceLock<Arc<Wake>>,
}
impl StateNotification {
    pub fn bind(&self, wake: Arc<Wake>) {
        if let Err(wake) = self.wake.set(wake) {
            assert!(Arc::ptr_eq(self.wake.get().unwrap(), &wake), "state notification has one owner");
        }
    }
    pub fn notify(&self) {
        self.pending.store(true, Ordering::Release);
        if let Some(wake) = self.wake.get() { wake.notify(); }
    }
    pub fn take_pending(&self) -> bool {
        // Avoid a write to the repair producer's cache line on every idle check.
        self.pending.load(Ordering::Acquire) && self.pending.swap(false, Ordering::AcqRel)
    }
}
