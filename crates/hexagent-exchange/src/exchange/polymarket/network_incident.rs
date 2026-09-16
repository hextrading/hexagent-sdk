use arc_swap::ArcSwap;
use log::warn;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

const INCIDENT_CORRELATION_WINDOW: Duration = Duration::from_secs(10);
pub(crate) const HTTP_SLOW_SUCCESS_THRESHOLD: Duration = Duration::from_millis(500);

#[inline]
fn http_success_requires_retirement(role: crate::http1_pool::Role, elapsed: Duration) -> bool {
    // Historical query/replay latency is independent of the order route.
    matches!(
        role,
        crate::http1_pool::Role::Fast | crate::http1_pool::Role::Cancel
    ) && elapsed >= HTTP_SLOW_SUCCESS_THRESHOLD
}

/// Retire only the measured order connection. Account-owned admission consumes
/// actual business outcomes; network diagnostics never gate another account,
/// role or strategy through process-global mutable state.
pub(crate) fn note_http_slow_success(
    role: crate::http1_pool::Role,
    slot: usize,
    elapsed: Duration,
) -> bool {
    if !http_success_requires_retirement(role, elapsed) {
        return false;
    }
    warn!(
        "[connection_health_slow_success] action=retire_connection_generation trigger_role={:?} trigger_slot={} elapsed_ms={} admission_owner=account",
        role, slot, elapsed.as_millis(),
    );
    true
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NetworkSignal {
    PeerCollision,
    DualWsSilence,
    HttpPlaceTimeout,
    HttpCancelTimeout,
}

/// A standby-only slow-consumer close is not evidence that the active market
/// lane or HTTP cluster is impaired. DNS can immediately return the active
/// peer for the replacement candidate; suppress that expected peer-collision
/// edge so it does not manufacture a fresh cross-transport incident.
static PEER_COLLISION_SUPPRESSED_UNTIL_NS: AtomicU64 = AtomicU64::new(0);

pub(crate) fn suppress_peer_collision_for(duration: Duration) {
    let until =
        crate::types::now_ns().saturating_add(duration.as_nanos().min(u64::MAX as u128) as u64);
    PEER_COLLISION_SUPPRESSED_UNTIL_NS.fetch_max(until, Ordering::AcqRel);
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

#[derive(Clone, Debug)]
struct Incident {
    id: u64,
    started_at: Instant,
    last_signal_at: Instant,
    signals: u8,
}

#[derive(Clone, Debug, Default)]
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

fn tracker() -> &'static ArcSwap<NetworkIncidentTracker> {
    static TRACKER: OnceLock<ArcSwap<NetworkIncidentTracker>> = OnceLock::new();
    TRACKER.get_or_init(|| ArcSwap::from_pointee(NetworkIncidentTracker::default()))
}

pub(crate) fn update_ws_peers(active: Option<SocketAddr>, standby: Option<SocketAddr>) {
    tracker().rcu(|current| {
        let mut next = (**current).clone();
        next.update_peers(active, standby);
        std::sync::Arc::new(next)
    });
}

pub(crate) fn record(signal: NetworkSignal, detail: &str) {
    if signal == NetworkSignal::PeerCollision
        && crate::types::now_ns() < PEER_COLLISION_SUPPRESSED_UNTIL_NS.load(Ordering::Acquire)
    {
        return;
    }
    let tracker = tracker();
    let snapshot = loop {
        let current = tracker.load_full();
        let mut next = (*current).clone();
        let snapshot = next.record(Instant::now(), signal);
        let observed = tracker.compare_and_swap(&current, std::sync::Arc::new(next));
        if std::sync::Arc::ptr_eq(&observed, &current) {
            break snapshot;
        }
    };
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

    #[test]
    fn only_order_roles_retire_slow_success_at_the_existing_threshold() {
        use crate::http1_pool::Role;
        for role in [Role::Fast, Role::Cancel] {
            assert!(!http_success_requires_retirement(
                role,
                HTTP_SLOW_SUCCESS_THRESHOLD - Duration::from_nanos(1),
            ));
            assert!(http_success_requires_retirement(
                role,
                HTTP_SLOW_SUCCESS_THRESHOLD,
            ));
        }
        for role in [Role::Query, Role::Reconcile, Role::GapReplay] {
            for elapsed in [HTTP_SLOW_SUCCESS_THRESHOLD, Duration::from_secs(30)] {
                assert!(!http_success_requires_retirement(role, elapsed));
            }
        }
    }
}
