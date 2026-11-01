//! One connection's liveness state, owned only by the existing feed task.
//! No new thread, queue, shared mutable state or per-report diagnostic output.

use futures_util::{Sink, Stream, StreamExt};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::{Instant, Interval, MissedTickBehavior};
use tokio_tungstenite::tungstenite::{protocol::CloseFrame, Error, Message};

use crate::exchange::{publish_market_event, ws_send, PublicMarketPublisher};

#[derive(Clone, Copy)]
pub(super) struct Settings {
    pub(super) read_timeout: Duration,
    pub(super) ping_interval: Duration,
    pub(super) pong_timeout: Duration,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            read_timeout: Duration::from_secs(60),
            ping_interval: Duration::from_secs(30),
            // Local recovery policy, not a claimed server requirement. Keep
            // this below the ping interval so only one Ping is outstanding.
            pong_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Debug)]
enum EndReason {
    ReadTimeout,
    PongTimeout,
    ServerClose(Option<CloseFrame<'static>>),
    ReadError(Error),
    Eof,
    PingSendFailed(String),
    PongSendFailed(String),
    Shutdown,
    ConsumerClosed,
}

impl EndReason {
    fn label(&self) -> &'static str {
        match self {
            Self::ReadTimeout => "read_timeout",
            Self::PongTimeout => "pong_timeout",
            Self::ServerClose(_) => "server_close",
            Self::ReadError(Error::Io(_)) => "read_io_error",
            Self::ReadError(Error::Protocol(_)) => "read_protocol_error",
            Self::ReadError(_) => "read_error",
            Self::Eof => "stream_eof",
            Self::PingSendFailed(_) => "ping_send_failed",
            Self::PongSendFailed(_) => "pong_send_failed",
            Self::Shutdown => "shutdown",
            Self::ConsumerClosed => "consumer_closed",
        }
    }

    fn close_code(&self) -> Option<u16> {
        match self {
            Self::ServerClose(Some(frame)) => Some(frame.code.into()),
            _ => None,
        }
    }

    fn write_error(&self) -> Option<&str> {
        match self {
            Self::PingSendFailed(error) | Self::PongSendFailed(error) => Some(error),
            _ => None,
        }
    }
}

struct PendingPing {
    payload: [u8; 8],
    started_at: Instant,
}

struct Health {
    settings: Settings,
    connected_at: Instant,
    read_expires_at: Instant,
    last_frame: Option<Instant>,
    last_report: Option<Instant>,
    last_report_source_ns: Option<u64>,
    last_pong: Option<Instant>,
    last_rtt: Option<Duration>,
    pending_ping: Option<PendingPing>,
    frames: u64,
    reports: u64,
    unparsed_data_frames: u64,
    pings: u64,
    pings_sent: u64,
    control_write: Option<&'static str>,
    matched_pongs: u64,
    unmatched_pongs: u64,
}

enum Action<T> {
    Inbound(T),
    Ping,
    End(EndReason),
}

impl Health {
    fn new(settings: Settings) -> Self {
        let now = Instant::now();
        Self {
            settings,
            connected_at: now,
            read_expires_at: now + settings.read_timeout,
            last_frame: None,
            last_report: None,
            last_report_source_ns: None,
            last_pong: None,
            last_rtt: None,
            pending_ping: None,
            frames: 0,
            reports: 0,
            unparsed_data_frames: 0,
            pings: 0,
            pings_sent: 0,
            control_write: None,
            matched_pongs: 0,
            unmatched_pongs: 0,
        }
    }

    fn pong_expires_at(&self) -> Instant {
        self.pending_ping
            .as_ref()
            .map_or(self.read_expires_at, |ping| {
                ping.started_at + self.settings.pong_timeout
            })
    }

    async fn next<F: Future>(&self, inbound: F, ping: &mut Interval) -> Action<F::Output> {
        // Reconstructing Sleep is safe: these absolute deadlines survive
        // select cancellation. Only an inbound frame renews the read clock.
        // Expired deadlines win even when a frame or another Ping is ready.
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(self.read_expires_at) => Action::End(EndReason::ReadTimeout),
            _ = tokio::time::sleep_until(self.pong_expires_at()), if self.pending_ping.is_some() => Action::End(EndReason::PongTimeout),
            _ = ping.tick(), if self.pending_ping.is_none() => Action::Ping,
            frame = inbound => Action::Inbound(frame),
        }
    }

    async fn control_write<F: Future>(&self, write: F) -> Result<F::Output, EndReason> {
        // A blocked sink must not suspend either watchdog inside a branch.
        // ws_send also retains its independent, bounded write timeout.
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(self.read_expires_at) => Err(EndReason::ReadTimeout),
            _ = tokio::time::sleep_until(self.pong_expires_at()), if self.pending_ping.is_some() => Err(EndReason::PongTimeout),
            result = write => Ok(result),
        }
    }

    fn received(&mut self) {
        let now = Instant::now();
        self.read_expires_at = now + self.settings.read_timeout;
        self.last_frame = Some(now);
        self.frames += 1;
    }

    fn begin_ping(&mut self) -> [u8; 8] {
        debug_assert!(self.pending_ping.is_none());
        self.pings += 1;
        let payload = self.pings.to_be_bytes();
        // Start before the write: time blocked flushing is part of the bound.
        self.pending_ping = Some(PendingPing {
            payload,
            started_at: Instant::now(),
        });
        payload
    }

    fn received_pong(&mut self, payload: &[u8]) {
        if let Some(ping) = self
            .pending_ping
            .as_ref()
            .filter(|ping| ping.payload == payload)
        {
            let now = Instant::now();
            self.last_rtt = Some(now - ping.started_at);
            self.last_pong = Some(now);
            self.matched_pongs += 1;
            self.pending_ping = None;
        } else {
            // Unsolicited, duplicate or stale Pong is valid transport traffic,
            // but cannot acknowledge the outstanding Ping or extend its bound.
            self.unmatched_pongs += 1;
        }
    }
}

pub(super) struct SessionEnd {
    reason: EndReason,
    health: Health,
    ended_at: Instant,
}

impl SessionEnd {
    pub(super) fn should_stop(&self) -> bool {
        matches!(self.reason, EndReason::Shutdown | EndReason::ConsumerClosed)
    }

    pub(super) fn log(&self, stream_id: uuid::Uuid, session: u64, retry: Option<Duration>) {
        let h = &self.health;
        let age_ms = |at: Option<Instant>| at.map(|at| (self.ended_at - at).as_millis());
        let raw_os_error = match &self.reason {
            EndReason::ReadError(Error::Io(error)) => error.raw_os_error(),
            _ => None,
        };
        // Control-plane only, once per disconnect. Debug escaping prevents a
        // peer's close reason from injecting lines; no auth or report payload.
        log::log!(
            if self.should_stop() { log::Level::Info } else { log::Level::Warn },
            "[ChainlinkStream] Session ended stream_id={} session={} reason={} detail={:?} close_code={:?} raw_os_error={:?} write_error={:?} uptime_ms={} last_frame_age_ms={:?} last_report_age_ms={:?} last_report_source_ns={:?} last_pong_age_ms={:?} pending_ping_age_ms={:?} last_pong_rtt_us={:?} frames={} reports={} unparsed_data_frames={} ping_attempts={} pings_sent={} control_write={:?} matched_pongs={} unmatched_pongs={} retry_ms={:?}",
            stream_id, session, self.reason.label(), self.reason, self.reason.close_code(), raw_os_error,
            self.reason.write_error(), (self.ended_at - h.connected_at).as_millis(),
            age_ms(h.last_frame), age_ms(h.last_report), h.last_report_source_ns,
            age_ms(h.last_pong), age_ms(h.pending_ping.as_ref().map(|ping| ping.started_at)),
            h.last_rtt.map(|rtt| rtt.as_micros()), h.frames, h.reports, h.unparsed_data_frames,
            h.pings, h.pings_sent, h.control_write, h.matched_pongs, h.unmatched_pongs, retry.map(|delay| delay.as_millis()),
        );
    }
}

pub(super) async fn run<W, R>(
    mut write: W,
    mut read: R,
    feed_ids: &[String],
    symbols: &[String],
    event_tx: &PublicMarketPublisher,
    shutdown: &AtomicBool,
    settings: Settings,
) -> SessionEnd
where
    W: Sink<Message> + Unpin,
    W::Error: std::fmt::Display,
    R: Stream<Item = Result<Message, Error>> + Unpin,
{
    let mut health = Health::new(settings);
    let mut ping = tokio::time::interval_at(
        health.connected_at + settings.ping_interval,
        settings.ping_interval,
    );
    ping.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let reason = loop {
        if shutdown.load(Ordering::Relaxed) {
            break EndReason::Shutdown;
        }
        let message = match health.next(read.next(), &mut ping).await {
            Action::End(reason) => break reason,
            Action::Ping => {
                let payload = health.begin_ping();
                health.control_write = Some("ping");
                match health
                    .control_write(ws_send(&mut write, Message::Ping(payload.to_vec())))
                    .await
                {
                    Ok(Ok(())) => {
                        health.pings_sent += 1;
                        health.control_write = None;
                        continue;
                    }
                    Ok(Err(error)) => break EndReason::PingSendFailed(error),
                    Err(reason) => break reason,
                }
            }
            Action::Inbound(Some(Ok(message))) => message,
            Action::Inbound(Some(Err(error))) => break EndReason::ReadError(error),
            Action::Inbound(None) => break EndReason::Eof,
        };
        health.received();
        let parsed: Option<serde_json::Value> = match message {
            Message::Text(text) => serde_json::from_str(&text).ok(),
            Message::Binary(bytes) => serde_json::from_slice(&bytes).ok(),
            Message::Pong(payload) => {
                health.received_pong(&payload);
                continue;
            }
            Message::Ping(payload) => {
                health.control_write = Some("pong");
                match health
                    .control_write(ws_send(&mut write, Message::Pong(payload)))
                    .await
                {
                    Ok(Ok(())) => {
                        health.control_write = None;
                        continue;
                    }
                    Ok(Err(error)) => break EndReason::PongSendFailed(error),
                    Err(reason) => break reason,
                }
            }
            Message::Close(frame) => break EndReason::ServerClose(frame),
            _ => continue,
        };
        if let Some(event) = parsed
            .as_ref()
            .and_then(|data| super::parse_report(data, feed_ids, symbols))
        {
            health.reports += 1;
            health.last_report = health.last_frame;
            if let crate::types::MarketEvent::SpotPrice(price) = &event {
                health.last_report_source_ns = Some(price.timestamp_ns);
            }
            if publish_market_event(event_tx, event).is_err() {
                break EndReason::ConsumerClosed;
            }
        } else {
            health.unparsed_data_frames += 1;
        }
    };
    SessionEnd {
        reason,
        health,
        ended_at: Instant::now(),
    }
}

#[cfg(test)]
#[path = "stream_session_tests.rs"]
mod tests;
