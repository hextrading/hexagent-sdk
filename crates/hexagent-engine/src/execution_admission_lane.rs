//! Bounded, replaceable execution-health snapshots. Never carries lifecycle events.
use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError, TrySendError};
use hexagent_types::types::{Exchange, ExecutionAdmission, ExecutionAdmissionState};
use std::time::{Duration, Instant};

pub(crate) const HEARTBEAT: Duration = Duration::from_millis(100);
const LEASE: Duration = Duration::from_secs(1);

/// One producer owns replacement. The receiver consumes immutable copies only.
/// Coalescing is safe for full snapshots; connection snapshots additionally carry
/// cumulative fault counters so an overwritten failure cannot become a recovery.
pub(crate) struct SnapshotPublisher<T> {
    tx: Sender<T>,
    replace_rx: Receiver<T>,
    pub replaced: u64,
}

pub(crate) fn snapshot_lane<T>() -> (SnapshotPublisher<T>, Receiver<T>) {
    let (tx, rx) = bounded(1);
    (
        SnapshotPublisher {
            tx,
            replace_rx: rx.clone(),
            replaced: 0,
        },
        rx,
    )
}

impl<T> SnapshotPublisher<T> {
    pub fn publish(&mut self, value: T) {
        if let Err(TrySendError::Full(value)) = self.tx.try_send(value) {
            if self.replace_rx.try_recv().is_ok() {
                self.replaced = self.replaced.saturating_add(1);
            }
            // Only this producer can fill the slot. A concurrent consumer can
            // only make room; this retry therefore cannot encounter Full.
            let _ = self.tx.try_send(value);
        }
    }
}

/// Owned by the strategy worker, independent of strategy/account mutable state.
/// A hung or disconnected dispatcher cannot leave an old Healthy snapshot live.
pub(crate) struct AdmissionConsumer {
    rx: Option<Receiver<ExecutionAdmission>>,
    last: Option<ExecutionAdmission>,
    received: Instant,
    expired: bool,
}

impl AdmissionConsumer {
    pub fn new(rx: Option<Receiver<ExecutionAdmission>>) -> Self {
        Self {
            rx,
            last: None,
            received: Instant::now(),
            expired: false,
        }
    }

    pub fn receiver(&self) -> Option<&Receiver<ExecutionAdmission>> {
        self.rx.as_ref()
    }

    fn pause(&mut self) -> Option<ExecutionAdmission> {
        if self.expired {
            return None;
        }
        self.expired = true;
        let paused = ExecutionAdmission {
            exchange: Exchange::Polymarket,
            epoch: self.last.map_or(1, |last| last.epoch.saturating_add(1)),
            state: ExecutionAdmissionState::Paused,
            available_place_slots: 0,
            observed_at_ns: hexagent_types::types::now_ns(),
        };
        self.last = Some(paused);
        Some(paused)
    }

    pub fn receive(
        &mut self,
        message: Result<ExecutionAdmission, crossbeam_channel::RecvError>,
    ) -> Option<ExecutionAdmission> {
        let Ok(snapshot) = message else {
            self.rx = None;
            return self.pause();
        };
        if self
            .last
            .is_some_and(|previous| snapshot.epoch <= previous.epoch)
        {
            // Replayed messages must neither refresh nor postpone the lease
            // check, even if they keep the select arm continuously readable.
            return if self.received.elapsed() >= LEASE {
                self.pause()
            } else {
                None
            };
        }
        let now = hexagent_types::types::now_ns();
        self.last = Some(snapshot);
        self.expired = false;
        // A paused worker must not grant a fresh lease to queued stale health.
        if snapshot.observed_at_ns > now
            || now.saturating_sub(snapshot.observed_at_ns) >= LEASE.as_nanos() as u64
        {
            return self.pause();
        }
        self.received = Instant::now() - Duration::from_nanos(now - snapshot.observed_at_ns);
        Some(snapshot)
    }

    pub fn poll(&mut self) -> Option<ExecutionAdmission> {
        let rx = self.rx.as_ref()?;
        match rx.try_recv() {
            Ok(snapshot) => return self.receive(Ok(snapshot)),
            Err(TryRecvError::Disconnected) => {
                return self.receive(Err(crossbeam_channel::RecvError))
            }
            Err(TryRecvError::Empty) => {}
        }
        if self.received.elapsed() >= LEASE {
            self.pause()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(epoch: u64, state: ExecutionAdmissionState) -> ExecutionAdmission {
        ExecutionAdmission {
            exchange: Exchange::Polymarket,
            epoch,
            state,
            available_place_slots: u16::from(state != ExecutionAdmissionState::Paused),
            observed_at_ns: hexagent_types::types::now_ns(),
        }
    }

    #[test]
    fn duplicate_storm_cannot_postpone_expiry() {
        let (mut tx, rx) = snapshot_lane();
        let mut consumer = AdmissionConsumer::new(Some(rx));
        tx.publish(snapshot(8, ExecutionAdmissionState::Healthy));
        assert_eq!(
            consumer.poll().unwrap().state,
            ExecutionAdmissionState::Healthy
        );
        consumer.received = Instant::now() - LEASE;
        tx.publish(snapshot(8, ExecutionAdmissionState::Healthy));
        assert_eq!(
            consumer.poll().unwrap().state,
            ExecutionAdmissionState::Paused
        );
        tx.publish(snapshot(7, ExecutionAdmissionState::Healthy));
        assert!(consumer.poll().is_none());
    }

    #[test]
    fn queued_old_healthy_does_not_gain_a_new_lease_after_worker_stall() {
        let (mut tx, rx) = snapshot_lane();
        let mut consumer = AdmissionConsumer::new(Some(rx));
        let mut old = snapshot(8, ExecutionAdmissionState::Healthy);
        old.observed_at_ns = old.observed_at_ns.saturating_sub(2_000_000_000);
        tx.publish(old);
        let paused = consumer.poll().unwrap();
        assert_eq!(paused.state, ExecutionAdmissionState::Paused);
        assert_eq!(paused.epoch, 9);
        tx.publish(snapshot(10, ExecutionAdmissionState::Recovering));
        assert_eq!(
            consumer.poll().unwrap().state,
            ExecutionAdmissionState::Recovering
        );
    }

    #[test]
    fn overflow_retains_latest_pause_and_isolates_instances() {
        let (mut a, arx) = snapshot_lane();
        let (mut b, brx) = snapshot_lane();
        a.publish(snapshot(1, ExecutionAdmissionState::Healthy));
        a.publish(snapshot(2, ExecutionAdmissionState::Paused));
        b.publish(snapshot(1, ExecutionAdmissionState::Healthy));
        assert_eq!(arx.len(), 1);
        assert_eq!(a.replaced, 1);
        assert_eq!(arx.recv().unwrap().state, ExecutionAdmissionState::Paused);
        assert_eq!(brx.recv().unwrap().state, ExecutionAdmissionState::Healthy);
    }

    #[test]
    fn expired_or_closed_lane_fails_closed_and_new_epoch_recovers() {
        let (mut tx, rx) = snapshot_lane();
        let mut consumer = AdmissionConsumer::new(Some(rx));
        tx.publish(snapshot(1, ExecutionAdmissionState::Healthy));
        assert!(consumer.poll().is_some());
        consumer.received = Instant::now() - LEASE;
        assert_eq!(
            consumer.poll().unwrap().state,
            ExecutionAdmissionState::Paused
        );
        assert!(consumer.poll().is_none());
        tx.publish(snapshot(1, ExecutionAdmissionState::Healthy));
        assert!(consumer.poll().is_none());
        tx.publish(snapshot(3, ExecutionAdmissionState::Recovering));
        assert_eq!(
            consumer.poll().unwrap().state,
            ExecutionAdmissionState::Recovering
        );
        drop(tx);
        assert_eq!(
            consumer.poll().unwrap().state,
            ExecutionAdmissionState::Paused
        );
    }
}
