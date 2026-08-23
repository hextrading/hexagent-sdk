use log::warn;
use std::net::SocketAddr;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

const INCIDENT_CORRELATION_WINDOW: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug)]
pub(crate) enum NetworkSignal {
    PeerCollision,
    DualWsSilence,
    HttpPlaceTimeout,
    HttpCancelTimeout,
}

impl NetworkSignal {
    fn name(self) -> &'static str {
        match self {
            Self::PeerCollision => "peer_ip_collision",
            Self::DualWsSilence => "dual_ws_silence",
            Self::HttpPlaceTimeout => "http_place_timeout",
            Self::HttpCancelTimeout => "http_cancel_timeout",
        }
    }

    fn bit(self) -> u8 {
        match self {
            Self::PeerCollision => 1 << 0,
            Self::DualWsSilence => 1 << 1,
            Self::HttpPlaceTimeout => 1 << 2,
            Self::HttpCancelTimeout => 1 << 3,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct PeerContext {
    active: Option<SocketAddr>,
    standby: Option<SocketAddr>,
}

#[derive(Debug)]
struct Incident {
    id: u64,
    started_at: Instant,
    last_signal_at: Instant,
    signals: u8,
}

#[derive(Debug, Default)]
struct NetworkIncidentTracker {
    next_id: u64,
    peers: PeerContext,
    current: Option<Incident>,
}

#[derive(Debug)]
struct IncidentSnapshot {
    id: u64,
    age: Duration,
    signals: u8,
    peers: PeerContext,
}

impl NetworkIncidentTracker {
    fn update_peers(&mut self, active: Option<SocketAddr>, standby: Option<SocketAddr>) {
        self.peers = PeerContext { active, standby };
    }

    fn record(&mut self, now: Instant, signal: NetworkSignal) -> IncidentSnapshot {
        let expired = self.current.as_ref().is_none_or(|incident| {
            now.saturating_duration_since(incident.last_signal_at) > INCIDENT_CORRELATION_WINDOW
        });
        if expired {
            self.next_id = self.next_id.saturating_add(1);
            self.current = Some(Incident {
                id: self.next_id,
                started_at: now,
                last_signal_at: now,
                signals: 0,
            });
        }
        let incident = self.current.as_mut().expect("incident initialized");
        incident.last_signal_at = now;
        incident.signals |= signal.bit();
        IncidentSnapshot {
            id: incident.id,
            age: now.saturating_duration_since(incident.started_at),
            signals: incident.signals,
            peers: self.peers,
        }
    }
}

fn tracker() -> &'static Mutex<NetworkIncidentTracker> {
    // This lock is used only on connection-state changes and anomaly paths;
    // no successful quote or order path touches it.
    static TRACKER: OnceLock<Mutex<NetworkIncidentTracker>> = OnceLock::new();
    TRACKER.get_or_init(|| Mutex::new(NetworkIncidentTracker::default()))
}

pub(crate) fn update_ws_peers(active: Option<SocketAddr>, standby: Option<SocketAddr>) {
    tracker().lock().unwrap().update_peers(active, standby);
}

pub(crate) fn record(signal: NetworkSignal, detail: &str) {
    let snapshot = tracker().lock().unwrap().record(Instant::now(), signal);
    let peer_ip_collision = match (snapshot.peers.active, snapshot.peers.standby) {
        (Some(active), Some(standby)) => active.ip() == standby.ip(),
        _ => false,
    };
    warn!(
        "[polymarket_network_incident] incident_id={} signal={} incident_age_ms={} active_peer={:?} standby_peer={:?} peer_ip_collision={} signal_mask=0x{:02x} detail={}",
        snapshot.id,
        signal.name(),
        snapshot.age.as_millis(),
        snapshot.peers.active,
        snapshot.peers.standby,
        peer_ip_collision,
        snapshot.signals,
        detail,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn related_signals_share_incident_and_peer_context() {
        let mut tracker = NetworkIncidentTracker::default();
        let started = Instant::now();
        let active = "192.0.2.1:443".parse().unwrap();
        let standby = "198.51.100.2:443".parse().unwrap();
        tracker.update_peers(Some(active), Some(standby));

        let ws = tracker.record(started, NetworkSignal::DualWsSilence);
        let http = tracker.record(
            started + Duration::from_secs(2),
            NetworkSignal::HttpPlaceTimeout,
        );

        assert_eq!(ws.id, http.id);
        assert_eq!(http.peers.active, Some(active));
        assert_eq!(http.peers.standby, Some(standby));
        assert_ne!(http.signals & NetworkSignal::DualWsSilence.bit(), 0);
        assert_ne!(http.signals & NetworkSignal::HttpPlaceTimeout.bit(), 0);
    }

    #[test]
    fn expired_signal_starts_new_incident() {
        let mut tracker = NetworkIncidentTracker::default();
        let started = Instant::now();
        let first = tracker.record(started, NetworkSignal::DualWsSilence);
        let next = tracker.record(
            started + INCIDENT_CORRELATION_WINDOW + Duration::from_millis(1),
            NetworkSignal::HttpCancelTimeout,
        );
        assert_ne!(first.id, next.id);
    }
}
