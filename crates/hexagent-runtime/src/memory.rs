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
