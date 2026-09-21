//! Opt-in public-feed evidence, independent of replaceable strategy snapshots.
//!
//! WS owners enqueue fixed metadata or transfer one of 64 preallocated raw
//! buffers. The existing recorder thread is the only consumer and writer.
//! No writer, formatting, heap growth, lock or wait runs in a quote callback.
//! Overflow is sticky/incomplete, never silently promoted into continuity.
use super::{BookProtocolKind, BookProtocolOwnerScope, BookProtocolRecord, MarketRecorder};
use anyhow::{Context, Result};
use arrayvec::ArrayString;
use crossbeam_channel::{bounded, Receiver, Sender};
use serde::Serialize;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

pub const BOOK_PROTOCOL_CAPACITY: usize = 8192;
const RAW_POOL_CAPACITY: usize = 64;
const RAW_FRAME_CAPACITY: usize = 128 * 1024;
const TOKEN_CAPACITY: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BookProtocolRoute {
    token: ArrayString<128>,
    event_id: ArrayString<128>,
    /// Zero explicitly means unavailable; never derived from receipt time.
    epoch: u64,
}
impl BookProtocolRoute {
    pub fn new(token: &str, event_id: &str, epoch: Option<u64>) -> Result<Self> {
        anyhow::ensure!(!token.is_empty(), "empty protocol token");
        Ok(Self {
            token: ArrayString::from(token)
                .map_err(|_| anyhow::anyhow!("protocol token too long"))?,
            event_id: ArrayString::from(event_id)
                .map_err(|_| anyhow::anyhow!("protocol event id too long"))?,
            epoch: epoch.unwrap_or(0),
        })
    }
}

#[derive(Default)]
struct LaneStats {
    claimed: AtomicBool,
    next_connection: AtomicU64,
    active_sessions: AtomicU64,
    enqueued: AtomicU64,
    consumed: AtomicU64,
    high_water: AtomicU64,
    overflow: AtomicU64,
    queue_overflow: AtomicU64,
    raw_pool_overflow: AtomicU64,
    raw_pool_high_water: AtomicU64,
    raw_pool_in_use: AtomicU64,
    oversize: AtomicU64,
    invalid: AtomicU64,
    gaps: AtomicU64,
    write_errors: AtomicU64,
}

struct CompactProtocol {
    token: ArrayString<128>,
    event_id: ArrayString<128>,
    epoch: u64,
    connection: u64,
    kind: BookProtocolKind,
    wire: ArrayString<128>,
    sequence: Option<u64>,
    previous: Option<u64>,
    recorder_sequence: u64,
    source_ns: Option<u64>,
    raw_timestamp: Option<u64>,
    receive_ns: u64,
    hash: Option<ArrayString<256>>,
    frame_sequence: u64,
}
impl CompactProtocol {
    fn into_record(self) -> BookProtocolRecord {
        BookProtocolRecord {
            owner_scope: BookProtocolOwnerScope::PublicFeed,
            iid: "feed:polymarket".into(),
            token: self.token.to_string(),
            event_id: (!self.event_id.is_empty()).then(|| self.event_id.to_string()),
            event_epoch: self.epoch,
            connection_id: self.connection,
            session_id: self.connection,
            kind: self.kind,
            wire_message_type: self.wire.to_string(),
            venue_sequence: self.sequence,
            venue_previous_sequence: self.previous,
            recorder_sequence: self.recorder_sequence,
            exchange_timestamp_ns: self.source_ns,
            raw_exchange_timestamp: self.raw_timestamp,
            local_timestamp_ns: self.receive_ns,
            venue_book_hash: self.hash.map(|h| h.to_string()),
            frame_sequence: self.frame_sequence,
        }
    }
}

enum ProtocolMessage {
    Record(CompactProtocol),
    Raw {
        connection: u64,
        frame: u64,
        receive_ns: u64,
        role: &'static str,
        bytes: Vec<u8>,
    },
}

/// Queue ownership is capture-local, never process-global or account-owned.
pub struct BookProtocolLane {
    sink: BookProtocolSink,
    rx: Receiver<ProtocolMessage>,
    wake_rx: Receiver<()>,
}
#[derive(Clone)]
pub struct BookProtocolSink {
    tx: Sender<ProtocolMessage>,
    free_tx: Sender<Vec<u8>>,
    free_rx: Receiver<Vec<u8>>,
    stats: Arc<LaneStats>,
    capture_id: u64,
    wake_tx: Sender<()>,
}
impl BookProtocolLane {
    #[cfg(test)]
    pub(crate) fn recycle_test_messages(&self) {
        while let Ok(message) = self.rx.try_recv() {
            if let ProtocolMessage::Raw { mut bytes, .. } = message {
                bytes.clear();
                self.sink
                    .stats
                    .raw_pool_in_use
                    .fetch_sub(1, Ordering::Relaxed);
                self.sink
                    .free_tx
                    .try_send(bytes)
                    .expect("return raw pool slot");
            }
            self.sink.stats.consumed.fetch_add(1, Ordering::Relaxed);
        }
        while self.wake_rx.try_recv().is_ok() {}
    }
    #[cfg(test)]
    pub(crate) fn test_counters(&self) -> (u64, u64, usize, u64, u64) {
        (
            self.sink.stats.high_water.load(Ordering::Relaxed),
            self.sink.stats.overflow.load(Ordering::Relaxed),
            self.rx.len(),
            self.sink.stats.raw_pool_high_water.load(Ordering::Relaxed),
            self.sink.stats.raw_pool_overflow.load(Ordering::Relaxed),
        )
    }
    pub fn new(capture_id: u64) -> Self {
        let (tx, rx) = bounded(BOOK_PROTOCOL_CAPACITY);
        let (wake_tx, wake_rx) = bounded(1);
        let (free_tx, free_rx) = bounded(RAW_POOL_CAPACITY);
        for _ in 0..RAW_POOL_CAPACITY {
            free_tx
                .try_send(Vec::with_capacity(RAW_FRAME_CAPACITY))
                .expect("fresh raw pool");
        }
        Self {
            sink: BookProtocolSink {
                tx,
                free_tx,
                free_rx,
                stats: Arc::new(LaneStats::default()),
                capture_id,
                wake_tx,
            },
            rx,
            wake_rx,
        }
    }
    pub fn sink(&self) -> BookProtocolSink {
        self.sink.clone()
    }
    /// Startup-only claim; refuses a second recorder consumer.
    pub fn consumer(&self, output_dir: &Path) -> Result<BookProtocolConsumer> {
        anyhow::ensure!(
            self.sink
                .stats
                .claimed
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok(),
            "protocol recorder already has its sole consumer"
        );
        let dir = output_dir.join("book_protocol");
        std::fs::create_dir_all(&dir)?;
        let base = dir.join(self.sink.capture_id.to_string());
        let raw = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(base.with_extension("frames.jsonl"))?;
        let result = BookProtocolConsumer {
            rx: self.rx.clone(),
            wake_rx: self.wake_rx.clone(),
            sink: self.sink.clone(),
            raw: BufWriter::with_capacity(256 * 1024, raw),
            base,
            initialized: false,
            finished: false,
        };
        result.write_manifest(false)?;
        Ok(result)
    }
}
impl BookProtocolSink {
    fn enqueue(&self, message: ProtocolMessage) {
        match self.tx.try_send(message) {
            Ok(()) => {
                self.stats.enqueued.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .high_water
                    .fetch_max(self.tx.len() as u64, Ordering::Relaxed);
            }
            Err(error) => {
                self.stats.overflow.fetch_add(1, Ordering::Relaxed);
                self.stats.queue_overflow.fetch_add(1, Ordering::Relaxed);
                if let ProtocolMessage::Raw { mut bytes, .. } = error.into_inner() {
                    bytes.clear();
                    self.stats.raw_pool_in_use.fetch_sub(1, Ordering::Relaxed);
                    let _ = self.free_tx.try_send(bytes);
                }
            }
        }
        let _ = self.wake_tx.try_send(());
    }
    pub fn session(&self, routes: &[BookProtocolRoute], receive_ns: u64) -> BookProtocolSession {
        let connection = self.stats.next_connection.fetch_add(1, Ordering::Relaxed) + 1;
        self.stats.active_sessions.fetch_add(1, Ordering::Relaxed);
        let mut session = BookProtocolSession {
            sink: self.clone(),
            connection,
            sequence: 0,
            frame: 0,
            routes: arrayvec::ArrayVec::new(),
            last_receive_ns: receive_ns.max(1),
        };
        for route in routes {
            if let Some(existing) = session.routes.iter().find(|r| r.token == route.token) {
                if existing != route {
                    self.stats.invalid.fetch_add(1, Ordering::Relaxed);
                }
            } else if session.routes.try_push(route.clone()).is_err() {
                self.stats.invalid.fetch_add(1, Ordering::Relaxed);
            }
        }
        session.control(
            BookProtocolKind::Reconnect,
            "ws_connection_opened",
            receive_ns.max(1),
        );
        session
    }
}

/// Single physical WS owner; session identity survives standby promotion.
pub struct BookProtocolSession {
    sink: BookProtocolSink,
    connection: u64,
    sequence: u64,
    frame: u64,
    routes: arrayvec::ArrayVec<BookProtocolRoute, TOKEN_CAPACITY>,
    last_receive_ns: u64,
}
impl BookProtocolSession {
    pub fn receive_ns(&self) -> u64 {
        self.last_receive_ns
    }
    pub fn capture_raw(&mut self, text: &[u8], receive_ns: u64, role: &'static str) {
        self.frame = self.frame.saturating_add(1);
        self.last_receive_ns = receive_ns.max(1);
        if text.len() > RAW_FRAME_CAPACITY {
            self.sink.stats.oversize.fetch_add(1, Ordering::Relaxed);
            self.sink.stats.invalid.fetch_add(1, Ordering::Relaxed);
            self.gap("raw_frame_capacity_exceeded", receive_ns);
            return;
        }
        let Ok(mut bytes) = self.sink.free_rx.try_recv() else {
            self.sink.stats.overflow.fetch_add(1, Ordering::Relaxed);
            self.sink
                .stats
                .raw_pool_overflow
                .fetch_add(1, Ordering::Relaxed);
            self.gap("raw_buffer_pool_exhausted", receive_ns);
            return;
        };
        let in_use = self
            .sink
            .stats
            .raw_pool_in_use
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        self.sink
            .stats
            .raw_pool_high_water
            .fetch_max(in_use, Ordering::Relaxed);
        bytes.extend_from_slice(text);
        self.sink.enqueue(ProtocolMessage::Raw {
            connection: self.connection,
            frame: self.frame,
            receive_ns,
            role,
            bytes,
        });
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &mut self,
        token: &str,
        kind: BookProtocolKind,
        wire: &str,
        sequence: Option<u64>,
        previous: Option<u64>,
        raw_timestamp: Option<u64>,
        source_ns: Option<u64>,
        receive_ns: u64,
        hash: Option<&str>,
    ) {
        let Some(route) = self.routes.iter().find(|r| r.token.as_str() == token) else {
            self.sink.stats.invalid.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let (Ok(wire), Some(hash)) = (
            ArrayString::from(wire),
            hash.map(ArrayString::from).transpose().ok(),
        ) else {
            self.sink.stats.invalid.fetch_add(1, Ordering::Relaxed);
            return;
        };
        self.sequence = self.sequence.saturating_add(1);
        self.last_receive_ns = receive_ns.max(1);
        self.sink.enqueue(ProtocolMessage::Record(CompactProtocol {
            token: route.token.clone(),
            event_id: route.event_id.clone(),
            epoch: route.epoch,
            connection: self.connection,
            kind,
            wire,
            sequence,
            previous,
            recorder_sequence: self.sequence,
            source_ns,
            raw_timestamp,
            receive_ns: receive_ns.max(1),
            hash,
            frame_sequence: self.frame,
        }));
    }
    fn control(&mut self, kind: BookProtocolKind, reason: &str, receive_ns: u64) {
        for index in 0..self.routes.len() {
            let token = self.routes[index].token.clone();
            self.record(
                &token, kind, reason, None, None, None, None, receive_ns, None,
            );
        }
    }
    pub fn gap(&mut self, reason: &str, receive_ns: u64) {
        self.sink.stats.gaps.fetch_add(1, Ordering::Relaxed);
        self.control(BookProtocolKind::Gap, reason, receive_ns.max(1));
    }
    pub fn invalid_frame(&mut self, receive_ns: u64) {
        self.sink.stats.invalid.fetch_add(1, Ordering::Relaxed);
        self.gap("malformed_or_unsupported_protocol_frame", receive_ns);
    }
}
impl Drop for BookProtocolSession {
    fn drop(&mut self) {
        self.gap("ws_session_closed_or_retired", crate::types::now_ns());
        self.sink
            .stats
            .active_sessions
            .fetch_sub(1, Ordering::Release);
    }
}

pub struct BookProtocolConsumer {
    rx: Receiver<ProtocolMessage>,
    wake_rx: Receiver<()>,
    sink: BookProtocolSink,
    raw: BufWriter<std::fs::File>,
    base: PathBuf,
    initialized: bool,
    finished: bool,
}
impl BookProtocolConsumer {
    pub fn checkpoint(&mut self, recorder: &mut MarketRecorder) -> Result<()> {
        let result = self.raw.flush().and_then(|_| {
            recorder
                .flush_book_protocol()
                .map_err(std::io::Error::other)
        });
        if let Err(error) = result {
            self.sink.stats.write_errors.fetch_add(1, Ordering::Relaxed);
            let _ = self.write_manifest(false);
            return Err(error.into());
        }
        self.write_manifest(false)
    }
    /// Recorder worker sleeps on either its existing market lane or one
    /// coalesced evidence wakeup. No new polling worker or periodic busy loop.
    pub fn recv_market<T>(
        &self,
        rx: &Receiver<T>,
        timeout: std::time::Duration,
    ) -> std::result::Result<T, crossbeam_channel::RecvTimeoutError> {
        // A single wakeup can represent more than one drain batch. Never
        // sleep with undrained evidence after consuming its coalesced wakeup.
        if !self.rx.is_empty() {
            return Err(crossbeam_channel::RecvTimeoutError::Timeout);
        }
        crossbeam_channel::select! {
            recv(rx) -> result => result.map_err(|_| crossbeam_channel::RecvTimeoutError::Disconnected),
            recv(self.wake_rx) -> _ => Err(crossbeam_channel::RecvTimeoutError::Timeout),
            default(timeout) => Err(crossbeam_channel::RecvTimeoutError::Timeout),
        }
    }
    /// Compose the public mailbox's readiness notification, preserving its
    /// ordered-burst/latest replacement policy through `try_recv` only.
    pub fn recv_public_market(
        &self,
        rx: &crate::exchange::PublicMarketReceiver,
        timeout: std::time::Duration,
    ) -> std::result::Result<hexagent_types::types::MarketEvent, crossbeam_channel::RecvTimeoutError>
    {
        let receive = || {
            rx.try_recv().map_err(|error| match error {
                crossbeam_channel::TryRecvError::Empty => {
                    crossbeam_channel::RecvTimeoutError::Timeout
                }
                crossbeam_channel::TryRecvError::Disconnected => {
                    crossbeam_channel::RecvTimeoutError::Disconnected
                }
            })
        };
        match receive() {
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
            result => return result,
        }
        if !self.rx.is_empty() {
            return Err(crossbeam_channel::RecvTimeoutError::Timeout);
        }
        crossbeam_channel::select! {
            recv(self.wake_rx) -> _ => Err(crossbeam_channel::RecvTimeoutError::Timeout),
            default(timeout.min(rx.poll_interval())) => receive(),
        }
    }
    pub fn drain(&mut self, recorder: &mut MarketRecorder, limit: usize) -> Result<()> {
        if !self.initialized {
            recorder.start_book_protocol(&self.base.with_extension("records.jsonl"))?;
            self.initialized = true;
        }
        for _ in 0..limit {
            let Ok(message) = self.rx.try_recv() else {
                break;
            };
            let result = match message {
                ProtocolMessage::Record(record) => {
                    recorder.record_book_protocol(&record.into_record())
                }
                ProtocolMessage::Raw {
                    connection,
                    frame,
                    receive_ns,
                    role,
                    mut bytes,
                } => {
                    #[derive(Serialize)]
                    struct Raw<'a> {
                        owner_scope: &'static str,
                        feed_id: &'static str,
                        connection_id: u64,
                        session_id: u64,
                        frame_sequence: u64,
                        local_timestamp_ns: u64,
                        lane_role: &'static str,
                        delivery_claim: &'static str,
                        payload_utf8: &'a str,
                    }
                    let result = (|| -> Result<()> {
                        let payload_utf8 =
                            std::str::from_utf8(&bytes).context("WS text is not UTF-8")?;
                        serde_json::to_writer(
                            &mut self.raw,
                            &Raw {
                                owner_scope: "public_feed",
                                feed_id: "feed:polymarket",
                                connection_id: connection,
                                session_id: connection,
                                frame_sequence: frame,
                                local_timestamp_ns: receive_ns,
                                lane_role: role,
                                delivery_claim: "received_only_not_strategy_applied",
                                payload_utf8,
                            },
                        )?;
                        self.raw.write_all(b"\n")?;
                        Ok(())
                    })();
                    bytes.clear();
                    self.sink
                        .stats
                        .raw_pool_in_use
                        .fetch_sub(1, Ordering::Relaxed);
                    if self.sink.free_tx.try_send(bytes).is_err() {
                        self.sink.stats.invalid.fetch_add(1, Ordering::Relaxed);
                    }
                    result
                }
            };
            self.sink.stats.consumed.fetch_add(1, Ordering::Relaxed);
            if let Err(error) = result {
                self.sink.stats.write_errors.fetch_add(1, Ordering::Relaxed);
                let _ = self.write_manifest(false);
                return Err(error);
            }
        }
        Ok(())
    }
    fn write_manifest(&self, closed: bool) -> Result<()> {
        let stats = &self.sink.stats;
        let load = |value: &AtomicU64| value.load(Ordering::Acquire);
        let complete = closed
            && load(&stats.active_sessions) == 0
            && load(&stats.raw_pool_in_use) == 0
            && self.rx.is_empty()
            && load(&stats.enqueued) == load(&stats.consumed)
            && load(&stats.overflow) == 0
            && load(&stats.invalid) == 0
            && load(&stats.write_errors) == 0;
        let value = serde_json::json!({ "schema_version": 1, "owner_scope": "public_feed", "feed_id": "feed:polymarket",
            "capture_id": self.sink.capture_id, "closed": closed, "archive_complete": complete,
            "continuity_claim": "none_raw_observations_only", "active_sessions": load(&stats.active_sessions),
            "enqueued": load(&stats.enqueued), "consumed": load(&stats.consumed), "queued": self.rx.len(),
            "capacity": BOOK_PROTOCOL_CAPACITY, "raw_pool_capacity": RAW_POOL_CAPACITY,
            "raw_frame_capacity": RAW_FRAME_CAPACITY, "high_water": load(&stats.high_water),
            "overflow": load(&stats.overflow), "invalid": load(&stats.invalid), "gaps": load(&stats.gaps),
            "queue_overflow": load(&stats.queue_overflow), "raw_pool_overflow": load(&stats.raw_pool_overflow),
            "raw_pool_high_water": load(&stats.raw_pool_high_water), "raw_pool_in_use": load(&stats.raw_pool_in_use),
            "oversize_frames": load(&stats.oversize),
            "write_errors": load(&stats.write_errors), "clock_domain": "client_wall",
            "normalization": "raw payload retained; sequence/hash only venue-provided; timestamp numeric retained",
            "owner_binding": "not a strategy journal; requires frozen offline subscription authority",
            "delivery_claim": "received_only_not_strategy_applied",
            "standby_metadata": "raw text frames only; no selected-book/application assertion" });
        let tmp = self.base.with_extension("manifest.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(&value)?)?;
        std::fs::rename(tmp, self.base.with_extension("manifest.json"))?;
        Ok(())
    }
    /// Existing recorder shutdown boundary, outside feed/strategy threads.
    /// A still-live or aborted producer leaves the archive explicitly incomplete.
    pub fn finish(&mut self, recorder: &mut MarketRecorder) -> Result<()> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            self.drain(recorder, BOOK_PROTOCOL_CAPACITY)?;
            if self.sink.stats.active_sessions.load(Ordering::Acquire) == 0
                || std::time::Instant::now() >= deadline
            {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        self.checkpoint(recorder)?;
        if self.sink.stats.active_sessions.load(Ordering::Acquire) != 0 {
            self.write_manifest(false)?;
            anyhow::bail!(
                "protocol evidence shutdown timed out with live producers; counters are not final"
            );
        }
        self.write_manifest(true)?;
        self.finished = true;
        Ok(())
    }
}
impl Drop for BookProtocolConsumer {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.write_manifest(false);
        }
    }
}

#[cfg(test)]
mod wake_tests {
    use super::*;
    use crate::exchange::{market_event_channel, publish_market_event};
    use crate::types::{Exchange, MarketEvent, SpotPrice};
    use std::time::Duration;

    #[test]
    fn recorder_public_receive_preserves_ordered_and_latest_mailbox_semantics() {
        let dir = tempfile::tempdir().unwrap();
        let lane = BookProtocolLane::new(81);
        let consumer = lane.consumer(dir.path()).unwrap();
        let (publisher, receiver) = market_event_channel(2);
        for timestamp_ns in 1..=3 {
            publish_market_event(
                &publisher,
                MarketEvent::SpotPrice(SpotPrice {
                    source: "test".into(),
                    symbol: "btc/usd".into(),
                    price: 1.0,
                    timestamp_ns,
                    local_timestamp_ns: timestamp_ns,
                }),
            )
            .unwrap();
        }
        publish_market_event(
            &publisher,
            MarketEvent::Connected {
                exchange: Exchange::Polymarket,
            },
        )
        .unwrap();
        assert!(matches!(
            consumer
                .recv_public_market(&receiver, Duration::ZERO)
                .unwrap(),
            MarketEvent::Connected { .. }
        ));
        for expected in [2, 3] {
            match consumer
                .recv_public_market(&receiver, Duration::ZERO)
                .unwrap()
            {
                MarketEvent::SpotPrice(spot) => assert_eq!(spot.timestamp_ns, expected),
                event => panic!("unexpected event: {event:?}"),
            }
        }
        drop(publisher);
        assert!(matches!(
            consumer.recv_public_market(&receiver, Duration::ZERO),
            Err(crossbeam_channel::RecvTimeoutError::Disconnected)
        ));
    }

    #[test]
    fn recorder_coalesced_wakeup_cannot_strand_an_undrained_batch() {
        let dir = tempfile::tempdir().unwrap();
        let lane = BookProtocolLane::new(82);
        let consumer = lane.consumer(dir.path()).unwrap();
        let route = BookProtocolRoute::new("token", "cid", None).unwrap();
        let _session = lane.sink().session(&[route], 1);
        consumer.wake_rx.try_recv().unwrap();
        assert!(!consumer.rx.is_empty());
        let (_publisher, receiver) = market_event_channel(2);
        let (_tx, rx) = crossbeam_channel::bounded::<()>(1);
        let start = std::time::Instant::now();
        assert!(matches!(
            consumer.recv_market(&rx, Duration::from_secs(2)),
            Err(crossbeam_channel::RecvTimeoutError::Timeout)
        ));
        assert!(matches!(
            consumer.recv_public_market(&receiver, Duration::from_secs(2)),
            Err(crossbeam_channel::RecvTimeoutError::Timeout)
        ));
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "queued evidence must bypass the sleep even with no wake token"
        );
    }
}
