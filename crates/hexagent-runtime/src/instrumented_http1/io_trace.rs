use super::*;
use hyper::rt::{Read, ReadBufCursor, Write};
use hyper_util::client::legacy::connect::{Connected, Connection};
use std::io::{self, IoSlice};

/// Application plaintext boundaries, after TLS. These are local observations:
/// response_wait includes remote/network time AND local runtime scheduling.
/// A successful write means accepted by the transport, not acknowledged by TCP.
#[derive(Clone, Copy, Debug, Default)]
pub struct Http1IoTimings {
    pub first_write_mono_ns: u64,
    pub first_read_mono_ns: u64,
    pub first_write_offset_ns: u64,
    pub write_span_ns: u64,
    pub first_read_offset_ns: u64,
    pub response_wait_ns: u64,
    pub header_decode_ns: u64,
    pub written_bytes: u64,
    pub read_bytes: u64,
    pub flush_offset_ns: u64,
    /// Linux TCP_INFO on this owned socket at first write/first read.
    /// RTT is smoothed TCP RTT, not an exchange processing-time estimate.
    pub tcp_sampled: bool,
    pub tcp_rtt_us: u32,
    pub tcp_rttvar_us: u32,
    pub tcp_retrans_delta: u32,
}

pub(super) struct IoTrace {
    origin: Instant,
    active_start: AtomicU64,
    first_write: AtomicU64,
    first_write_mono: AtomicU64,
    first_read_mono: AtomicU64,
    last_write: AtomicU64,
    first_read: AtomicU64,
    written: AtomicU64,
    read: AtomicU64,
    flush: AtomicU64,
    tcp_sampled: AtomicU8,
    tcp_rtt_us: AtomicU32,
    tcp_rttvar_us: AtomicU32,
    tcp_retrans_before: AtomicU32,
    tcp_retrans_after: AtomicU32,
}

impl Default for IoTrace {
    fn default() -> Self {
        Self {
            origin: Instant::now(),
            active_start: AtomicU64::new(0),
            first_write: AtomicU64::new(0),
            first_write_mono: AtomicU64::new(0),
            first_read_mono: AtomicU64::new(0),
            last_write: AtomicU64::new(0),
            first_read: AtomicU64::new(0),
            written: AtomicU64::new(0),
            read: AtomicU64::new(0),
            flush: AtomicU64::new(0),
            tcp_sampled: AtomicU8::new(0),
            tcp_rtt_us: AtomicU32::new(0),
            tcp_rttvar_us: AtomicU32::new(0),
            tcp_retrans_before: AtomicU32::new(0),
            tcp_retrans_after: AtomicU32::new(0),
        }
    }
}

impl IoTrace {
    fn now(&self) -> u64 {
        duration_ns(self.origin.elapsed()).saturating_add(1)
    }

    pub(super) fn begin(&self) -> IoGuard<'_> {
        self.active_start.store(0, Ordering::Release);
        for value in [
            &self.first_write,
            &self.first_write_mono,
            &self.first_read_mono,
            &self.last_write,
            &self.first_read,
            &self.written,
            &self.read,
            &self.flush,
        ] {
            value.store(0, Ordering::Relaxed);
        }
        self.tcp_sampled.store(0, Ordering::Relaxed);
        for value in [
            &self.tcp_rtt_us,
            &self.tcp_rttvar_us,
            &self.tcp_retrans_before,
            &self.tcp_retrans_after,
        ] {
            value.store(0, Ordering::Relaxed);
        }
        self.active_start.store(self.now(), Ordering::Release);
        IoGuard(self)
    }

    pub(super) fn snapshot(&self, headers_ns: u64) -> Http1IoTimings {
        let start = self.active_start.load(Ordering::Acquire);
        let write = self.first_write.load(Ordering::Acquire);
        let last = self.last_write.load(Ordering::Acquire);
        let read = self.first_read.load(Ordering::Acquire);
        let flushed = self.flush.load(Ordering::Acquire);
        Http1IoTimings {
            first_write_mono_ns: self.first_write_mono.load(Ordering::Acquire),
            first_read_mono_ns: self.first_read_mono.load(Ordering::Acquire),
            first_write_offset_ns: write.saturating_sub(start),
            write_span_ns: last.saturating_sub(write),
            first_read_offset_ns: read.saturating_sub(start),
            response_wait_ns: if read != 0 && last != 0 {
                read.saturating_sub(last.max(flushed))
            } else {
                0
            },
            header_decode_ns: if read != 0 {
                headers_ns.saturating_sub(read.saturating_sub(start))
            } else {
                0
            },
            written_bytes: self.written.load(Ordering::Acquire),
            read_bytes: self.read.load(Ordering::Acquire),
            flush_offset_ns: flushed.saturating_sub(start),
            tcp_sampled: self.tcp_sampled.load(Ordering::Acquire) == 3,
            tcp_rtt_us: self.tcp_rtt_us.load(Ordering::Relaxed),
            tcp_rttvar_us: self.tcp_rttvar_us.load(Ordering::Relaxed),
            tcp_retrans_delta: if self.tcp_sampled.load(Ordering::Acquire) == 3 {
                self.tcp_retrans_after
                    .load(Ordering::Relaxed)
                    .saturating_sub(self.tcp_retrans_before.load(Ordering::Relaxed))
            } else {
                0
            },
        }
    }
}

pub(super) struct IoGuard<'a>(&'a IoTrace);
impl Drop for IoGuard<'_> {
    fn drop(&mut self) {
        self.0.active_start.store(0, Ordering::Release);
    }
}

pub(super) struct TimedIo<T> {
    pub inner: T,
    pub trace: Arc<ConnectTrace>,
    pub generation: u64,
    pub flush_pending: bool,
    #[cfg(target_os = "linux")]
    pub socket_fd: std::os::fd::RawFd,
}

impl<T> TimedIo<T> {
    fn active(&self) -> bool {
        self.trace.io.active_start.load(Ordering::Acquire) != 0
            && self.generation == self.trace.generation.load(Ordering::Acquire)
    }
    fn wrote(&mut self, bytes: usize) {
        if bytes != 0 && self.active() {
            let io = &self.trace.io;
            let now = io.now();
            if io
                .first_write
                .compare_exchange(0, now, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                io.first_write_mono.store(hexagent_types::types::monotonic_now_ns(), Ordering::Release);
                self.sample_tcp(true);
            }
            io.last_write.store(now, Ordering::Release);
            io.written.fetch_add(bytes as u64, Ordering::Relaxed);
            self.flush_pending = true;
        }
    }

    fn sample_tcp(&self, _before: bool) {
        #[cfg(target_os = "linux")]
        {
            let mut info: libc::tcp_info = unsafe { std::mem::zeroed() };
            let mut len = std::mem::size_of_val(&info) as libc::socklen_t;
            // The descriptor is borrowed from self.inner, which is still owned
            // by this driver. No global/raw descriptor survives stream drop.
            let result = unsafe {
                libc::getsockopt(
                    self.socket_fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_INFO,
                    (&mut info as *mut libc::tcp_info).cast(),
                    &mut len,
                )
            };
            if result == 0 && len as usize >= std::mem::size_of::<libc::tcp_info>() {
                let io = &self.trace.io;
                if _before {
                    io.tcp_retrans_before
                        .store(info.tcpi_total_retrans, Ordering::Relaxed);
                }
                io.tcp_retrans_after
                    .store(info.tcpi_total_retrans, Ordering::Relaxed);
                io.tcp_rtt_us.store(info.tcpi_rtt, Ordering::Relaxed);
                io.tcp_rttvar_us.store(info.tcpi_rttvar, Ordering::Relaxed);
                io.tcp_sampled
                    .fetch_or(if _before { 1 } else { 2 }, Ordering::Release);
            }
        }
    }
}

impl<T> Drop for TimedIo<T> {
    fn drop(&mut self) {
        // A retiring old socket must not invalidate a newer connection. This
        // per-client monotone watermark crosses to readers as a compact value.
        self.trace.closed_generation.fetch_max(self.generation, Ordering::Release);
    }
}

impl<T: Connection> Connection for TimedIo<T> {
    fn connected(&self) -> Connected {
        self.inner.connected()
    }
}

impl<T: Read + Unpin> Read for TimedIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        // Reborrow only the unfilled region. Read's contract guarantees the
        // inner reader initializes every byte it marks filled; propagate that
        // exact prefix to the original cursor even on a partial/error read.
        let mut borrowed = hyper::rt::ReadBuf::uninit(unsafe { buf.as_mut() });
        let result = Pin::new(&mut self.inner).poll_read(cx, borrowed.unfilled());
        let bytes = borrowed.filled().len();
        unsafe {
            buf.advance(bytes);
        }
        if bytes != 0 && self.active() && self.trace.io.first_write.load(Ordering::Acquire) != 0 {
            let io = &self.trace.io;
            if io
                .first_read
                .compare_exchange(0, io.now(), Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                io.first_read_mono.store(hexagent_types::types::monotonic_now_ns(), Ordering::Release);
                self.sample_tcp(false);
            }
            io.read.fetch_add(bytes as u64, Ordering::Relaxed);
        }
        result
    }
}

impl<T: Write + Unpin> Write for TimedIo<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(bytes)) = result {
            self.wrote(bytes);
        }
        result
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        if let Poll::Ready(Ok(bytes)) = result {
            self.wrote(bytes);
        }
        result
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let result = Pin::new(&mut self.inner).poll_flush(cx);
        if matches!(result, Poll::Ready(Ok(()))) && self.flush_pending {
            if self.active() {
                self.trace
                    .io
                    .flush
                    .store(self.trace.io.now(), Ordering::Release);
            }
            self.flush_pending = false;
        }
        result
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
