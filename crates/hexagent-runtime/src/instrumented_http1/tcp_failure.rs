//! Failure-only socket evidence. A duplicated descriptor owns the socket
//! lifetime while the HTTP future and driver may retire concurrently. Never
//! retain a borrowed descriptor in the request task or query a reused fd.
#[cfg(target_os = "linux")]
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TcpFailureStatus {
    #[default]
    NotAttempted,
    Sampled,
    Unsupported,
    NoWrite,
    SocketRetired,
    GenerationMismatch,
    DuplicateFailed,
    GetsockoptFailed,
    ShortInfo,
}
impl TcpFailureStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotAttempted => "not_attempted",
            Self::Sampled => "sampled",
            Self::Unsupported => "unsupported",
            Self::NoWrite => "no_write",
            Self::SocketRetired => "socket_retired",
            Self::GenerationMismatch => "generation_mismatch",
            Self::DuplicateFailed => "duplicate_failed",
            Self::GetsockoptFailed => "getsockopt_failed",
            Self::ShortInfo => "short_info",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TcpFailureBoundary {
    #[default]
    None,
    Timeout,
    TransportError,
    DriverDrop,
}
impl TcpFailureBoundary {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Timeout => "timeout",
            Self::TransportError => "transport_error",
            Self::DriverDrop => "driver_drop",
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TcpFailureInfo {
    pub status: TcpFailureStatus,
    pub boundary: TcpFailureBoundary,
    pub generation: u64,
    pub sampled_mono_ns: u64,
    pub baseline_sampled: bool,
    pub errno: i32,
    pub returned_len: u32,
    pub rtt_us: u32,
    pub rttvar_us: u32,
    pub total_retrans: u32,
    pub retrans_delta: u32,
    pub unacked: u32,
    pub lost: u32,
    pub retrans: u32,
    pub rto_us: u32,
    pub state: u8,
    pub backoff: u8,
    pub probes: u8,
}

#[cfg(target_os = "linux")]
struct SocketLease {
    generation: u64,
    fd: Option<std::os::fd::OwnedFd>,
    errno: i32,
}
#[cfg(target_os = "linux")]
struct RetiredSample {
    request_epoch: u64,
    info: TcpFailureInfo,
}

/// Socket publication is cold connect/drop only. Failure readers take an
/// immutable lease. Success requests never load either ArcSwap or sample an
/// additional TCP_INFO; they only retain the first-write generation.
#[derive(Default)]
pub(super) struct FailureProbe {
    #[cfg(target_os = "linux")]
    socket: arc_swap::ArcSwapOption<SocketLease>,
    #[cfg(target_os = "linux")]
    retired: arc_swap::ArcSwapOption<RetiredSample>,
}

impl FailureProbe {
    #[cfg(target_os = "linux")]
    pub(super) fn install(&self, fd: std::os::fd::RawFd, generation: u64) {
        use std::os::fd::FromRawFd;
        // Called while the connector still owns the original stream.
        let duplicate = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
        let (fd, errno) = if duplicate >= 0 {
            (Some(unsafe { std::os::fd::OwnedFd::from_raw_fd(duplicate) }), 0)
        } else {
            (None, std::io::Error::last_os_error().raw_os_error().unwrap_or(-1))
        };
        self.socket.store(Some(Arc::new(SocketLease { generation, fd, errno })));
    }

    #[cfg(target_os = "linux")]
    pub(super) fn retiring(&self, fd: std::os::fd::RawFd, generation: u64,
        epoch: u64, write_generation: u64, baseline: Option<u32>) {
        // Sample before self.inner closes. Retain the request epoch, so a late
        // old driver cannot supply evidence for a subsequent request.
        if epoch != 0 && generation == write_generation {
            let info = sample(fd, generation, baseline, TcpFailureBoundary::DriverDrop);
            self.retired.store(Some(Arc::new(RetiredSample { request_epoch: epoch, info })));
        }
        let lease = self.socket.load();
        if lease.as_ref().is_some_and(|s| s.generation == generation) {
            // A newer connector may publish between load and CAS; never clear it.
            self.socket.compare_and_swap(&*lease, None);
        }
    }

    pub(super) fn capture(&self, epoch: u64, generation: u64,
        baseline: Option<u32>, boundary: TcpFailureBoundary) -> TcpFailureInfo {
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (epoch, baseline);
            TcpFailureInfo { status: TcpFailureStatus::Unsupported, boundary, generation,
                ..Default::default() }
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            if generation == 0 {
                return TcpFailureInfo { status: TcpFailureStatus::NoWrite, boundary,
                    ..Default::default() };
            }
            let lease = self.socket.load();
            let unavailable = match lease.as_ref() {
                Some(lease) if lease.generation == generation => {
                    if let Some(fd) = lease.fd.as_ref() {
                        return sample(fd.as_raw_fd(), generation, baseline, boundary);
                    }
                    TcpFailureInfo { status: TcpFailureStatus::DuplicateFailed, errno: lease.errno,
                        boundary, generation, ..Default::default() }
                }
                Some(_) => TcpFailureInfo { status: TcpFailureStatus::GenerationMismatch,
                    boundary, generation, ..Default::default() },
                None => TcpFailureInfo { status: TcpFailureStatus::SocketRetired,
                    boundary, generation, ..Default::default() },
            };
            let retired = self.retired.load();
            if let Some(r) = retired.as_ref().filter(|r|
                r.request_epoch == epoch && r.info.generation == generation) {
                return r.info;
            }
            unavailable
        }
    }
}

#[cfg(target_os = "linux")]
fn sample(fd: std::os::fd::RawFd, generation: u64, baseline: Option<u32>,
    boundary: TcpFailureBoundary) -> TcpFailureInfo {
    let mut info: libc::tcp_info = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of_val(&info) as libc::socklen_t;
    let result = unsafe {
        libc::getsockopt(fd, libc::IPPROTO_TCP, libc::TCP_INFO,
            (&mut info as *mut libc::tcp_info).cast(), &mut len)
    };
    let errno = if result == 0 { 0 } else {
        std::io::Error::last_os_error().raw_os_error().unwrap_or(-1)
    };
    let status = if result != 0 { TcpFailureStatus::GetsockoptFailed }
        else if (len as usize) < std::mem::size_of::<libc::tcp_info>() { TcpFailureStatus::ShortInfo }
        else { TcpFailureStatus::Sampled };
    TcpFailureInfo {
        status, boundary, generation, errno, returned_len: len,
        sampled_mono_ns: hexagent_types::types::monotonic_now_ns(),
        baseline_sampled: baseline.is_some(),
        rtt_us: info.tcpi_rtt, rttvar_us: info.tcpi_rttvar,
        total_retrans: info.tcpi_total_retrans,
        unacked: info.tcpi_unacked, lost: info.tcpi_lost, retrans: info.tcpi_retrans,
        rto_us: info.tcpi_rto, state: info.tcpi_state, backoff: info.tcpi_backoff, probes: info.tcpi_probes,
        retrans_delta: baseline.map_or(0, |b| info.tcpi_total_retrans.wrapping_sub(b)),
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::{net::{TcpListener, TcpStream}, os::fd::AsRawFd};
    fn pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        (client, listener.accept().unwrap().0)
    }
    #[test]
    fn lease_owns_socket_and_retirement_is_epoch_and_generation_fenced() {
        let probe = FailureProbe::default();
        let (client, _server) = pair();
        probe.install(client.as_raw_fd(), 1);
        drop(client); // Request lease remains valid, even if original fd is reused.
        let evidence = probe.capture(10, 1, None, TcpFailureBoundary::Timeout);
        assert_eq!(evidence.status, TcpFailureStatus::Sampled);
        assert!(!evidence.baseline_sampled);
        let (new_client, _new_server) = pair();
        probe.install(new_client.as_raw_fd(), 2);
        assert_eq!(probe.capture(10, 1, None, TcpFailureBoundary::Timeout).status,
            TcpFailureStatus::GenerationMismatch);
        probe.retiring(new_client.as_raw_fd(), 2, 11, 2, Some(0));
        let retired = probe.capture(11, 2, Some(0), TcpFailureBoundary::TransportError);
        assert_eq!(retired.status, TcpFailureStatus::Sampled);
        assert_eq!(retired.boundary, TcpFailureBoundary::DriverDrop);
        assert!(retired.baseline_sampled);
        assert_eq!(probe.capture(12, 2, Some(0), TcpFailureBoundary::Timeout).status,
            TcpFailureStatus::SocketRetired);
        assert_eq!(probe.capture(12, 0, None, TcpFailureBoundary::Timeout).status,
            TcpFailureStatus::NoWrite);
    }
    #[test]
    fn old_driver_cannot_clear_new_lease_and_fd_errors_are_explicit() {
        let probe = FailureProbe::default();
        let (old, _old_server) = pair();
        let (new, _new_server) = pair();
        probe.install(old.as_raw_fd(), 1);
        probe.install(new.as_raw_fd(), 2);
        probe.retiring(old.as_raw_fd(), 1, 11, 2, Some(0));
        assert_eq!(probe.capture(11, 2, Some(0), TcpFailureBoundary::Timeout).status,
            TcpFailureStatus::Sampled);
        probe.install(-1, 3);
        let failure = probe.capture(12, 3, None, TcpFailureBoundary::Timeout);
        assert_eq!(failure.status, TcpFailureStatus::DuplicateFailed);
        assert_ne!(failure.errno, 0);
        assert_eq!(sample(-1, 3, None, TcpFailureBoundary::TransportError).status,
            TcpFailureStatus::GetsockoptFailed);
    }
}
