//! Single-owner Linux timerfd deadlines. Kernel monotonic deadlines avoid the
//! millisecond timer-wheel rounding of Tokio's portable timer. No thread,
//! shared mutable timer or catch-up loop is introduced.
#[derive(Debug, Clone, Copy)]
pub struct DeadlineTick {
    pub scheduled_ns: u64,
    pub observed_ns: u64,
    pub expirations: u64,
}

impl DeadlineTick {
    pub fn lag_ns(self) -> u64 {
        self.observed_ns.saturating_sub(self.scheduled_ns)
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::DeadlineTick;
    use std::{
        io,
        os::fd::{AsRawFd, FromRawFd, OwnedFd},
        time::Duration,
    };
    use tokio::io::unix::AsyncFd;

    pub struct PreciseInterval {
        fd: AsyncFd<OwnedFd>,
        period_ns: u64,
        next_ns: u64,
    }

    fn monotonic_ns() -> io::Result<u64> {
        let mut value = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut value) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(value.tv_sec as u64 * 1_000_000_000 + value.tv_nsec as u64)
    }

    fn timespec(ns: u64) -> libc::timespec {
        libc::timespec {
            tv_sec: (ns / 1_000_000_000) as _,
            tv_nsec: (ns % 1_000_000_000) as _,
        }
    }

    impl PreciseInterval {
        /// Call on the owning runtime before entering its measured loop.
        pub fn new(period: Duration) -> io::Result<Self> {
            let period_ns = u64::try_from(period.as_nanos())
                .ok()
                .filter(|n| *n != 0)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "invalid timer period")
                })?;
            let raw = unsafe {
                libc::timerfd_create(
                    libc::CLOCK_MONOTONIC,
                    libc::TFD_NONBLOCK | libc::TFD_CLOEXEC,
                )
            };
            if raw < 0 {
                return Err(io::Error::last_os_error());
            }
            let fd = unsafe { OwnedFd::from_raw_fd(raw) };
            let next_ns = monotonic_ns()?.checked_add(period_ns).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "timer deadline overflow")
            })?;
            let spec = libc::itimerspec {
                it_interval: timespec(period_ns),
                it_value: timespec(next_ns),
            };
            if unsafe {
                libc::timerfd_settime(
                    fd.as_raw_fd(),
                    libc::TFD_TIMER_ABSTIME,
                    &spec,
                    std::ptr::null_mut(),
                )
            } != 0
            {
                return Err(io::Error::last_os_error());
            }
            Ok(Self {
                fd: AsyncFd::new(fd)?,
                period_ns,
                next_ns,
            })
        }

        /// Cancellation-safe until ready. A delayed read coalesces all missed
        /// expirations into one sample, measuring from the FIRST missed
        /// deadline so a stall is never hidden by reporting the newest tick.
        pub async fn tick(&mut self) -> io::Result<DeadlineTick> {
            loop {
                let mut ready = self.fd.readable().await?;
                let result = ready.try_io(|fd| {
                    let mut expirations = 0_u64;
                    let n = unsafe {
                        libc::read(
                            fd.get_ref().as_raw_fd(),
                            (&mut expirations as *mut u64).cast(),
                            8,
                        )
                    };
                    if n < 0 {
                        return Err(io::Error::last_os_error());
                    }
                    if n != 8 || expirations == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid timerfd read",
                        ));
                    }
                    Ok(expirations)
                });
                match result {
                    Ok(Ok(expirations)) => {
                        let tick = DeadlineTick {
                            scheduled_ns: self.next_ns,
                            observed_ns: monotonic_ns()?,
                            expirations,
                        };
                        self.next_ns = self
                            .next_ns
                            .saturating_add(self.period_ns.saturating_mul(expirations));
                        return Ok(tick);
                    }
                    Ok(Err(e)) if e.kind() == io::ErrorKind::Interrupted => continue,
                    Ok(Err(e)) => return Err(e),
                    Err(_) => continue,
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
pub use linux::PreciseInterval;

/// Unsupported platforms keep the existing portable probe; they must not
/// silently label a millisecond Tokio timer as a precise kernel deadline.
#[cfg(not(target_os = "linux"))]
pub struct PreciseInterval;

#[cfg(not(target_os = "linux"))]
impl PreciseInterval {
    pub fn new(_: std::time::Duration) -> std::io::Result<Self> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "timerfd requires Linux",
        ))
    }
    pub async fn tick(&mut self) -> std::io::Result<DeadlineTick> {
        std::future::pending().await
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test(flavor = "current_thread")]
    async fn coalesces_missed_ticks_without_hiding_oldest_deadline() {
        let mut timer = PreciseInterval::new(Duration::from_millis(2)).unwrap();
        std::thread::sleep(Duration::from_millis(14));
        let tick = timer.tick().await.unwrap();
        assert!(tick.expirations >= 6);
        assert!(tick.lag_ns() >= 10_000_000);
        let next = timer.tick().await.unwrap();
        assert_eq!(
            next.scheduled_ns,
            tick.scheduled_ns + tick.expirations * 2_000_000
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelling_a_pending_tick_does_not_consume_the_deadline() {
        let mut timer = PreciseInterval::new(Duration::from_millis(50)).unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(1), timer.tick())
            .await
            .is_err());
        let tick = timer.tick().await.unwrap();
        assert!(tick.observed_ns >= tick.scheduled_ns);
        assert!(tick.expirations >= 1);
        assert!(PreciseInterval::new(Duration::ZERO).is_err());
    }
}
