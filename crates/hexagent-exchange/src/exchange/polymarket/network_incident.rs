use arc_swap::ArcSwap;
use log::warn;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

const INCIDENT_CORRELATION_WINDOW: Duration = Duration::from_secs(10);
const CONNECTION_FAILURE_CLUSTER_WINDOW_NS: u64 = 750_000_000;
const CONNECTION_FAILURE_CLUSTER_CONNECTIONS: u32 = 2;
const CONNECTION_FAILURE_CLUSTER_PLACE_BLOCK_NS: u64 = 5_000_000_000;
const PLACE_GATE_REJECTION_LOG_INTERVAL_NS: u64 = 1_000_000_000;
pub(crate) const HTTP_SLOW_SUCCESS_THRESHOLD: Duration = Duration::from_millis(500);

#[inline]
fn http_success_requires_retirement(elapsed: Duration) -> bool {
    elapsed >= HTTP_SLOW_SUCCESS_THRESHOLD
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConnectionFailureKind {
    Timeout,
    Transport,
}

impl ConnectionFailureKind {
    fn name(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Transport => "transport",
        }
    }
}

#[derive(Debug, Default)]
struct ConnectionFailureClusterGate {
    window_started_ns: AtomicU64,
    connection_mask: AtomicU64,
    place_blocked_until_ns: AtomicU64,
}

impl ConnectionFailureClusterGate {
    fn note(&self, now_ns: u64, role: crate::http1_pool::Role, slot: usize) -> (u32, bool) {
        loop {
            let started = self.window_started_ns.load(Ordering::Acquire);
            if started != 0
                && now_ns.saturating_sub(started) <= CONNECTION_FAILURE_CLUSTER_WINDOW_NS
            {
                break;
            }
            if self
                .window_started_ns
                .compare_exchange(started, now_ns.max(1), Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.connection_mask.store(0, Ordering::Release);
                break;
            }
        }
        // The fixed mask covers the configured admission topology without a
        // mutex or growable global map. Saturation folds only slots beyond a
        // role's documented mask width into its final bit; ordinary account
        // pools retain exact role/slot identity, including Fast slots > 7.
        let (role_offset, role_width) = match role {
            crate::http1_pool::Role::Fast => (0, 24),
            crate::http1_pool::Role::Cancel => (24, 16),
            crate::http1_pool::Role::Reconcile => (40, 8),
            crate::http1_pool::Role::GapReplay => (48, 8),
            crate::http1_pool::Role::Query => (56, 8),
        };
        let bit = 1u64 << (role_offset + slot.min(role_width - 1));
        let mask = self.connection_mask.fetch_or(bit, Ordering::AcqRel) | bit;
        let connections = mask.count_ones();
        if connections < CONNECTION_FAILURE_CLUSTER_CONNECTIONS {
            return (connections, false);
        }
        let until = now_ns.saturating_add(CONNECTION_FAILURE_CLUSTER_PLACE_BLOCK_NS);
        let previous = self
            .place_blocked_until_ns
            .fetch_max(until, Ordering::AcqRel);
        (connections, previous <= now_ns)
    }

    fn place_blocked(&self, now_ns: u64) -> bool {
        now_ns < self.place_blocked_until_ns.load(Ordering::Acquire)
    }
}

fn connection_failure_gate() -> &'static ConnectionFailureClusterGate {
    static GATE: OnceLock<ConnectionFailureClusterGate> = OnceLock::new();
    GATE.get_or_init(ConnectionFailureClusterGate::default)
}

pub(crate) fn note_http_connection_failure(
    role: crate::http1_pool::Role,
    slot: usize,
    kind: ConnectionFailureKind,
) {
    let now_ns = crate::types::now_ns();
    let (connections, entered) = connection_failure_gate().note(now_ns, role, slot);
    if entered {
        warn!(
            "[connection_failure_cluster] connections={} window_ms={} place_block_ms={} action=pause_new_place_allow_cancel_reconcile trigger_kind={} trigger_role={:?} trigger_slot={}",
            connections,
            CONNECTION_FAILURE_CLUSTER_WINDOW_NS / 1_000_000,
            CONNECTION_FAILURE_CLUSTER_PLACE_BLOCK_NS / 1_000_000,
            kind.name(),
            role,
            slot,
        );
    }
}

/// A response can prove that its connection is alive and still be unsafe for
/// reuse. Every slow-success retires the exact measured logical-slot
/// generation; correlation across two independent connections additionally
/// pauses fresh placements while cancel and reconcile continue on their
/// isolated, idempotent lanes.
pub(crate) fn note_http_slow_success(
    role: crate::http1_pool::Role,
    slot: usize,
    elapsed: Duration,
) -> bool {
    if !http_success_requires_retirement(elapsed) {
        return false;
    }
    let now_ns = crate::types::now_ns();
    let (connections, entered) = connection_failure_gate().note(now_ns, role, slot);
    warn!(
        "[connection_health_slow_success] action=retire_connection_generation trigger_role={:?} trigger_slot={} elapsed_ms={} cluster_connections={} placement_gate_active={}",
        role,
        slot,
        elapsed.as_millis(),
        connections,
        connections >= CONNECTION_FAILURE_CLUSTER_CONNECTIONS,
    );
    if entered {
        warn!(
            "[connection_health_cluster] connections={} window_ms={} place_block_ms={} action=pause_new_place_allow_cancel_reconcile trigger_kind=slow_success trigger_role={:?} trigger_slot={} elapsed_ms={}",
            connections,
            CONNECTION_FAILURE_CLUSTER_WINDOW_NS / 1_000_000,
            CONNECTION_FAILURE_CLUSTER_PLACE_BLOCK_NS / 1_000_000,
            role,
            slot,
            elapsed.as_millis(),
        );
    }
    true
}

#[inline]
pub(crate) fn place_blocked_by_connection_failure_cluster() -> bool {
    connection_failure_gate().place_blocked(crate::types::now_ns())
}

/// Count admission rejections without emitting one console record per order.
/// Lifecycle updates remain lossless; this is console-only aggregation.
pub(crate) fn note_place_gate_rejections(count: usize) {
    static PENDING: AtomicU64 = AtomicU64::new(0);
    static TOTAL: AtomicU64 = AtomicU64::new(0);
    static LAST_LOG_NS: AtomicU64 = AtomicU64::new(0);
    let count = count as u64;
    PENDING.fetch_add(count, Ordering::Relaxed);
    let total = TOTAL.fetch_add(count, Ordering::Relaxed).saturating_add(count);
    let now_ns = crate::types::now_ns();
    loop {
        let last = LAST_LOG_NS.load(Ordering::Acquire);
        if last != 0 && now_ns.saturating_sub(last) < PLACE_GATE_REJECTION_LOG_INTERVAL_NS {
            return;
        }
        if LAST_LOG_NS
            .compare_exchange(last, now_ns.max(1), Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let rejected = PENDING.swap(0, Ordering::AcqRel);
            log::info!(
                "[connection_gate_admission] rejected_orders={} rejected_total={} console_per_order_suppressed=true",
                rejected,
                total,
            );
            return;
        }
    }
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
    fn distinct_connection_failures_gate_only_after_threshold_and_expire() {
        let gate = ConnectionFailureClusterGate::default();
        assert_eq!(gate.note(1, crate::http1_pool::Role::Fast, 0), (1, false));
        assert_eq!(gate.note(2, crate::http1_pool::Role::Fast, 0), (1, false));
        assert_eq!(gate.note(3, crate::http1_pool::Role::Cancel, 0), (2, true));
        assert_eq!(gate.note(4, crate::http1_pool::Role::Cancel, 1), (3, false));
        assert!(gate.place_blocked(5));
        assert!(!gate.place_blocked(4 + CONNECTION_FAILURE_CLUSTER_PLACE_BLOCK_NS));
    }

    #[test]
    fn slow_success_uses_distinct_role_slot_evidence() {
        let gate = ConnectionFailureClusterGate::default();
        assert_eq!(gate.note(1, crate::http1_pool::Role::Fast, 2), (1, false));
        assert_eq!(gate.note(2, crate::http1_pool::Role::Fast, 2), (1, false));
        assert_eq!(
            gate.note(3, crate::http1_pool::Role::Reconcile, 2),
            (2, true)
        );
        assert!(gate.place_blocked(4));
    }

    #[test]
    fn every_slow_success_requests_exact_generation_retirement() {
        assert!(!http_success_requires_retirement(
            HTTP_SLOW_SUCCESS_THRESHOLD - Duration::from_nanos(1),
        ));
        assert!(http_success_requires_retirement(
            HTTP_SLOW_SUCCESS_THRESHOLD,
        ));
    }

    #[test]
    fn fast_slots_above_seven_remain_distinct_cluster_evidence() {
        let gate = ConnectionFailureClusterGate::default();
        assert_eq!(gate.note(1, crate::http1_pool::Role::Fast, 8), (1, false));
        assert_eq!(gate.note(2, crate::http1_pool::Role::Fast, 9), (2, true));
    }
}
