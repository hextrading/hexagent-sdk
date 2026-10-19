//! Opt-in offline evidence. Each participating thread owns its preallocated
//! vector; join transfers the finished vector to the fixture. No global map,
//! locks, export, or allocation in a span. Never enabled by live features.
use std::cell::RefCell;

#[derive(Clone, Copy, Debug)]
#[repr(u8)]
pub enum Phase { Audit = 1, AuditEnqueue = 2, HandlerReturn = 3,
    Publish = 4, Receive = 5, Fast = 6, FullSend = 7, Diagnostic = 8,
    Router = 9, CommandPublication = 10, HttpPoll = 11, HttpWritePoll = 12,
    HttpStart = 13, HttpSubmit = 14, HttpDequeue = 15, ReceiveLoop = 16 }

#[derive(Clone, Copy, Debug)]
pub struct Record {
    pub owner: u16,
    pub phase: Phase,
    pub key: u64,
    pub start: u64,
    pub end: u64,
    pub cpu_ns: Option<u64>,
    pub aux: u64,
    pub status: u8,
}

struct Trace { owner: u16, records: Vec<Record>, dropped: usize }
thread_local! { static TRACE: RefCell<Option<Trace>> = const { RefCell::new(None) }; }

pub fn prepare(owner: u16, capacity: usize, enabled: bool) {
    TRACE.with_borrow_mut(|t| {
        assert!(t.is_none(), "one probe owner per thread");
        if enabled { *t = Some(Trace { owner, records: Vec::with_capacity(capacity), dropped: 0 }); }
    });
}

pub fn enabled() -> bool { TRACE.with_borrow(|t| t.is_some()) }

pub fn key(oid: Option<&str>) -> Option<u64> {
    let oid = oid?;
    if !oid.is_ascii() || !oid.starts_with("0x") || oid.len() != 34 { return None; }
    let wave = u64::from_str_radix(&oid[2..18], 16).ok()?;
    let slot = u64::from_str_radix(&oid[18..], 16).ok()?;
    (slot < 16).then(|| wave.checked_mul(16)?.checked_add(slot)).flatten()
}

fn record(mut r: Record) {
    TRACE.with_borrow_mut(|trace| {
        if let Some(t) = trace {
            r.owner = t.owner;
            if t.records.len() == t.records.capacity() { t.dropped += 1; }
            else { t.records.push(r); }
        }
    });
}

pub struct Span { phase: Phase, key: u64, start: u64, cpu: u64 }
impl Span {
    pub fn begin(phase: Phase, oid: Option<&str>) -> Option<Self> {
        if !enabled() { return None; }
        Self::begin_id(phase, key(oid)?)
    }
    pub fn begin_id(phase: Phase, key: u64) -> Option<Self> {
        if !enabled() { return None; }
        Self::begin_at(phase, key, hexagent_types::types::monotonic_now_ns())
    }
    pub fn begin_at(phase: Phase, key: u64, start: u64) -> Option<Self> {
        enabled().then(|| Self { phase, key, start, cpu: crate::latency::thread_cpu_ns() })
    }
    pub fn finish(self, status: u8) {
        self.finish_at(hexagent_types::types::monotonic_now_ns(), status, None, 0);
    }
    pub fn finish_at(self, end: u64, status: u8, key: Option<u64>, aux: u64) {
        let cpu = crate::latency::thread_cpu_ns();
        record(Record { owner: 0, phase: self.phase, key: key.unwrap_or(self.key),
            start: self.start, end,
            cpu_ns: (self.cpu > 0 && cpu >= self.cpu).then_some(cpu.saturating_sub(self.cpu)),
            aux, status });
    }
}

pub fn point(phase: Phase, oid: Option<&str>) {
    if !enabled() { return; }
    if let Some(key) = key(oid) {
        let at = hexagent_types::types::monotonic_now_ns();
        record(Record { owner: 0, phase, key, start: at, end: at, cpu_ns: None, aux: 0, status: 0 });
    }
}

/// Reuse existing command publication clocks; cpu remains explicitly unknown.
pub fn command_publication(t: crate::poll_channel::PublicationTiming) {
    if enabled() { record(Record { owner: 0, phase: Phase::CommandPublication, key: 0,
        start: t.push_started_mono_ns, end: t.push_returned_mono_ns, cpu_ns: None,
        aux: t.notify_returned_mono_ns, status: 0 }); }
}

/// Fixture DELETE identity only. No JSON allocation, and no parsing at all
/// when the thread-local offline trace is disabled. Live builds omit this.
pub fn body_key(body: &[u8]) -> Option<u64> {
    if !enabled() { return None; }
    let prefix = b"\"orderID\":\"";
    let start = body.windows(prefix.len()).position(|w| w == prefix)? + prefix.len();
    let value = body.get(start..start + 34)?;
    if body.get(start + 34) != Some(&b'"') { return None; }
    key(std::str::from_utf8(value).ok())
}

pub fn point_id(phase: Phase, key: u64) {
    if !enabled() { return; }
    let at = hexagent_types::types::monotonic_now_ns();
    record(Record { owner: 0, phase, key, start: at, end: at, cpu_ns: None, aux: 0, status: 0 });
}

/// Per-receive counters, owned by the receiving thread; no clock in each loop.
pub struct ReceiveLoop { span: Option<Span>, checks: u64, waits: u64 }
impl ReceiveLoop {
    pub fn begin() -> Self { Self { span: Span::begin_id(Phase::ReceiveLoop, 0), checks: 0, waits: 0 } }
    pub fn check(&mut self) { self.checks += 1; }
    pub fn wait(&mut self) { self.waits += 1; }
}
impl Drop for ReceiveLoop {
    fn drop(&mut self) {
        if let Some(span) = self.span.take() {
            span.finish_at(hexagent_types::types::monotonic_now_ns(), 0, Some(self.checks), self.waits);
        }
    }
}

pub fn take() -> (Vec<Record>, usize) {
    TRACE.with_borrow_mut(|t| t.take().map_or_else(|| (Vec::new(), 0), |t| (t.records, t.dropped)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn probe_is_owner_local_bounded_and_preserves_order() {
        std::thread::spawn(|| {
            prepare(3, 2, true);
            for id in 0..3 { Span::begin_id(Phase::Publish, id).unwrap().finish(0); }
            let other = std::thread::spawn(|| { prepare(9, 1, true); take() }).join().unwrap();
            assert_eq!(other.0.len(), 0);
            let (rows, dropped) = take();
            assert_eq!((rows.len(), dropped), (2, 1));
            assert_eq!((rows[0].owner, rows[0].key, rows[1].key), (3, 0, 1));
            assert!(rows.iter().all(|r| r.end >= r.start));
            assert!(Span::begin_id(Phase::Fast, 1).is_none());
        }).join().unwrap();
    }
    #[test]
    fn identities_do_not_alias_wave_or_slot() {
        assert_eq!(key(Some("0x00000000000000010000000000000002")), Some(18));
        assert_eq!(key(Some("0x00000000000000020000000000000002")), Some(34));
        assert_eq!(key(Some("unknown")), None);
        assert_eq!(key(Some("0x00000000000000010000000000000010")), None);
    }

    #[test]
    fn offline_body_identity_requires_enabled_owner_and_exact_delimited_order_id() {
        std::thread::spawn(|| {
            let body = br#"{"orderID":"0x00000000000000010000000000000002"}"#;
            assert_eq!(body_key(body), None);
            prepare(300, 1, true);
            assert_eq!(body_key(body), Some(18));
            assert_eq!(body_key(br#"{"orderID":"0x000000000000000100000000000000020"}"#), None);
            assert_eq!(body_key(br#"{"orderID":"0x0000000000000001"}"#), None);
            assert_eq!(body_key(br#"{"orderID":"0x00000000000000010000000000000010"}"#), None);
            take();
        }).join().unwrap();
    }
}
