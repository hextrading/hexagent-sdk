//! Low-frequency process memory telemetry.
//!
//! Linux exposes the process totals we care about in `/proc`.  Reading and
//! parsing those files is intentionally kept on a background telemetry thread;
//! no strategy or quote-path code calls this module.

use std::io;
use std::sync::OnceLock;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProcessMemoryStats {
    pub vm_rss_bytes: u64,
    pub vm_hwm_bytes: u64,
    pub vm_lck_bytes: u64,
    pub vm_size_bytes: u64,
    pub threads: u64,
    pub private_dirty_bytes: u64,
    pub locked_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AllocatorStats {
    pub reserved_bytes: u64,
    pub committed_bytes: u64,
}

pub type AllocatorStatsProvider = fn() -> AllocatorStats;

static ALLOCATOR_STATS_PROVIDER: OnceLock<AllocatorStatsProvider> = OnceLock::new();

/// Register the application's allocator-specific statistics provider.
///
/// The SDK does not force an allocator on its consumers. Applications using
/// mimalloc can register its exact reserved/committed counters at startup.
pub fn register_allocator_stats_provider(
    provider: AllocatorStatsProvider,
) -> Result<(), AllocatorStatsProvider> {
    ALLOCATOR_STATS_PROVIDER.set(provider)
}

pub fn allocator_stats() -> AllocatorStats {
    ALLOCATOR_STATS_PROVIDER
        .get()
        .copied()
        .map(|provider| provider())
        .unwrap_or_default()
}

/// Application-specific allocator work, called only by the existing pinned
/// SCHED_OTHER latency worker. Register before starting that worker. Keeping
/// this optional hook here avoids imposing an allocator on SDK consumers.
/// The function pointer is immutable after startup; there is no new queue or
/// mutable account access on quote/private lanes.
pub type AllocatorMaintenanceProvider = fn();
static ALLOCATOR_MAINTENANCE_PROVIDER: OnceLock<AllocatorMaintenanceProvider> = OnceLock::new();

pub fn register_allocator_maintenance_provider(
    provider: AllocatorMaintenanceProvider,
) -> Result<(), AllocatorMaintenanceProvider> {
    ALLOCATOR_MAINTENANCE_PROVIDER.set(provider)
}

pub(crate) fn allocator_maintenance_provider() -> Option<AllocatorMaintenanceProvider> {
    ALLOCATOR_MAINTENANCE_PROVIDER.get().copied()
}

/// State belongs solely to the background worker. A delayed tick performs one
/// collection and resets its deadline: no catch-up burst can starve draining.
pub(crate) struct AllocatorMaintenance {
    provider: Option<AllocatorMaintenanceProvider>,
    next: std::time::Instant,
}

impl AllocatorMaintenance {
    pub(crate) fn new(
        provider: Option<AllocatorMaintenanceProvider>,
        now: std::time::Instant,
    ) -> Self {
        Self {
            provider,
            next: now + std::time::Duration::from_secs(1),
        }
    }

    pub(crate) fn tick(&mut self, now: std::time::Instant) {
        if now < self.next {
            return;
        }
        if let Some(provider) = self.provider {
            let start = crate::latency::Instant::now();
            provider();
            crate::latency::record("runtime.allocator.background_maintenance", start);
        }
        self.next = std::time::Instant::now().max(now) + std::time::Duration::from_secs(1);
    }
}

#[cfg(target_os = "linux")]
pub fn process_memory_stats() -> io::Result<ProcessMemoryStats> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    let smaps_rollup = std::fs::read_to_string("/proc/self/smaps_rollup")?;
    Ok(parse_process_memory_stats(&status, &smaps_rollup))
}

#[cfg(not(target_os = "linux"))]
pub fn process_memory_stats() -> io::Result<ProcessMemoryStats> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "process memory telemetry requires Linux /proc",
    ))
}

#[cfg(any(target_os = "linux", test))]
fn parse_process_memory_stats(status: &str, smaps_rollup: &str) -> ProcessMemoryStats {
    ProcessMemoryStats {
        vm_rss_bytes: proc_kib_value(status, "VmRSS").saturating_mul(1024),
        vm_hwm_bytes: proc_kib_value(status, "VmHWM").saturating_mul(1024),
        vm_lck_bytes: proc_kib_value(status, "VmLck").saturating_mul(1024),
        vm_size_bytes: proc_kib_value(status, "VmSize").saturating_mul(1024),
        threads: proc_scalar_value(status, "Threads"),
        private_dirty_bytes: proc_kib_value(smaps_rollup, "Private_Dirty").saturating_mul(1024),
        locked_bytes: proc_kib_value(smaps_rollup, "Locked").saturating_mul(1024),
    }
}

#[cfg(any(target_os = "linux", test))]
fn proc_kib_value(text: &str, key: &str) -> u64 {
    proc_scalar_value(text, key)
}

#[cfg(any(target_os = "linux", test))]
fn proc_scalar_value(text: &str, key: &str) -> u64 {
    text.lines()
        .find_map(|line| {
            let (candidate, value) = line.split_once(':')?;
            (candidate == key)
                .then(|| value.split_whitespace().next()?.parse::<u64>().ok())
                .flatten()
        })
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_status_and_smaps_rollup_units() {
        let status = "\
VmSize:\t  500000 kB\n\
VmLck:\t      64 kB\n\
VmHWM:\t  220000 kB\n\
VmRSS:\t  180000 kB\n\
Threads:\t17\n";
        let smaps = "\
Private_Dirty:     12345 kB\n\
Locked:               48 kB\n";

        assert_eq!(
            parse_process_memory_stats(status, smaps),
            ProcessMemoryStats {
                vm_rss_bytes: 180_000 * 1024,
                vm_hwm_bytes: 220_000 * 1024,
                vm_lck_bytes: 64 * 1024,
                vm_size_bytes: 500_000 * 1024,
                threads: 17,
                private_dirty_bytes: 12_345 * 1024,
                locked_bytes: 48 * 1024,
            }
        );
    }
}

#[cfg(test)]
mod maintenance_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    fn count() {
        CALLS.fetch_add(1, Ordering::Relaxed);
    }

    #[test]
    fn maintenance_is_optional_rate_limited_and_never_catches_up_in_a_burst() {
        let now = std::time::Instant::now();
        let mut absent = AllocatorMaintenance::new(None, now);
        absent.tick(now + std::time::Duration::from_secs(100));
        assert_eq!(CALLS.load(Ordering::Relaxed), 0);
        let mut work = AllocatorMaintenance::new(Some(count), now);
        work.tick(now);
        work.tick(now + std::time::Duration::from_millis(999));
        assert_eq!(CALLS.load(Ordering::Relaxed), 0);
        work.tick(now + std::time::Duration::from_secs(1));
        assert_eq!(CALLS.load(Ordering::Relaxed), 1);
        let delayed = now + std::time::Duration::from_secs(100);
        work.tick(delayed);
        work.tick(delayed);
        work.tick(delayed + std::time::Duration::from_millis(999));
        assert_eq!(CALLS.load(Ordering::Relaxed), 2);
        work.tick(delayed + std::time::Duration::from_secs(1));
        assert_eq!(CALLS.load(Ordering::Relaxed), 3);
    }
}
