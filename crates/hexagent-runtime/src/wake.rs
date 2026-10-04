//! Optional single-consumer notification for bounded polling lanes.
//! Linux publishers only perform a futex wake when the consumer has armed a
//! wait. No mutex, allocation, notification queue or extra worker. The owner
//! MUST arm, recheck its inbox, then wait: publication racing the recheck clears
//! the word and makes FUTEX_WAIT return EAGAIN. Notifications are hints; the
//! bounded inbox remains the sole ordering/ownership authority.
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

#[derive(Default)]
pub struct Wake {
    armed: AtomicU32,
}

impl Wake {
    pub fn arm(&self) {
        self.armed.store(1, Ordering::SeqCst);
    }
    pub fn cancel(&self) {
        self.armed.store(0, Ordering::SeqCst);
    }
    pub fn notify(&self) {
        if self.armed.swap(0, Ordering::SeqCst) == 1 {
            #[cfg(target_os = "linux")]
            unsafe {
                libc::syscall(
                    libc::SYS_futex,
                    self.armed.as_ptr(),
                    libc::FUTEX_WAKE | libc::FUTEX_PRIVATE_FLAG,
                    1,
                );
            }
        }
    }
    pub fn wait(&self, timeout: Duration) {
        #[cfg(target_os = "linux")]
        {
            let time = libc::timespec {
                tv_sec: timeout.as_secs().min(i64::MAX as u64) as _,
                tv_nsec: timeout.subsec_nanos() as _,
            };
            // EINTR/spurious wake/timeout are all handled by the owner's next
            // inbox check. The AtomicU32 is aligned and lives through the call.
            unsafe {
                libc::syscall(
                    libc::SYS_futex,
                    self.armed.as_ptr(),
                    libc::FUTEX_WAIT | libc::FUTEX_PRIVATE_FLAG,
                    1,
                    &time,
                );
            }
        }
        #[cfg(not(target_os = "linux"))]
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
        let wake = Wake::default();
        wake.arm();
        wake.notify();
        assert_eq!(wake.armed.load(Ordering::SeqCst), 0);
        wake.wait(Duration::from_millis(1));
    }
}
