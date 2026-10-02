//! Startup-bound transport evidence, never account/risk authority.
//!
//! Public CLOB readers and cold account HTTP workers send compact failures to
//! the owning execution router. The bounded FIFO holds 16 hints. Saturation
//! retains an additional capacity-one reset intent, so it cannot silently
//! leave old order connections admitted. Only the router drains either lane.
//! No worker, lock, registry, or quote-path allocation is introduced.

use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use std::net::IpAddr;
use std::sync::OnceLock;

pub const PEER_FAILURE_CAPACITY: usize = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerFailureSource {
    PublicWs,
    AccountHttp,
    OrderHttp,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PeerFailure {
    pub peer: IpAddr,
    pub source: PeerFailureSource,
    pub observed_at_ns: u64,
}

#[derive(Clone)]
pub struct PeerFailureSender {
    tx: Sender<PeerFailure>,
    overflow_tx: Sender<()>,
}

impl PeerFailureSender {
    pub fn publish(&self, failure: PeerFailure) {
        if matches!(self.tx.try_send(failure), Err(TrySendError::Full(_))) {
            // A full fallback already retains the same retire-all intent.
            let _ = self.overflow_tx.try_send(());
        }
    }
}

pub struct PeerFailureReceiver {
    rx: Receiver<PeerFailure>,
    overflow_rx: Receiver<()>,
}

impl PeerFailureReceiver {
    pub fn has_pending(&self) -> bool { !self.rx.is_empty() || !self.overflow_rx.is_empty() }

    /// Bounded work even while producers are publishing. Caller owns all
    /// ordering/deduplication and generation-retirement state.
    pub fn drain(&self) -> (Option<PeerFailure>, bool) {
        let overflow = self.overflow_rx.try_recv().is_ok();
        let mut newest: Option<PeerFailure> = None;
        for _ in 0..PEER_FAILURE_CAPACITY {
            let Ok(failure) = self.rx.try_recv() else {
                break;
            };
            if newest.is_none_or(|old| failure.observed_at_ns > old.observed_at_ns) {
                newest = Some(failure);
            }
        }
        (newest, overflow)
    }
}

/// The account adapter creates this at startup. Claim is startup-only and
/// succeeds once, including when several instance routes share the adapter.
pub struct PeerFailureMailbox {
    sender: PeerFailureSender,
    receiver: PeerFailureReceiver,
    claimed: OnceLock<()>,
}

impl Default for PeerFailureMailbox {
    fn default() -> Self {
        let (tx, rx) = bounded(PEER_FAILURE_CAPACITY);
        let (overflow_tx, overflow_rx) = bounded(1);
        Self {
            sender: PeerFailureSender { tx, overflow_tx },
            receiver: PeerFailureReceiver { rx, overflow_rx },
            claimed: OnceLock::new(),
        }
    }
}

impl PeerFailureMailbox {
    pub fn sender(&self) -> PeerFailureSender {
        self.sender.clone()
    }

    pub fn claim_receiver(&self) -> Option<PeerFailureReceiver> {
        self.claimed.set(()).ok()?;
        Some(PeerFailureReceiver {
            rx: self.receiver.rx.clone(),
            overflow_rx: self.receiver.overflow_rx.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failure(stamp: u64) -> PeerFailure {
        PeerFailure {
            peer: "192.0.2.1".parse().unwrap(),
            source: PeerFailureSource::AccountHttp,
            observed_at_ns: stamp,
        }
    }

    #[test]
    fn one_consumer_and_account_isolation() {
        let first = PeerFailureMailbox::default();
        let other = PeerFailureMailbox::default();
        let receiver = first.claim_receiver().unwrap();
        assert!(first.claim_receiver().is_none());
        first.sender().publish(failure(3));
        assert_eq!(receiver.drain(), (Some(failure(3)), false));
        assert_eq!(other.claim_receiver().unwrap().drain(), (None, false));
    }

    #[test]
    fn reordering_and_duplicates_keep_newest_evidence() {
        let mailbox = PeerFailureMailbox::default();
        let receiver = mailbox.claim_receiver().unwrap();
        for stamp in [5, 3, 5, 1] {
            mailbox.sender().publish(failure(stamp));
        }
        assert_eq!(receiver.drain(), (Some(failure(5)), false));
        assert_eq!(receiver.drain(), (None, false));
    }

    #[test]
    fn full_fifo_retains_reset_intent_and_recovers() {
        let mailbox = PeerFailureMailbox::default();
        let receiver = mailbox.claim_receiver().unwrap();
        for stamp in 1..=100 {
            mailbox.sender().publish(failure(stamp));
        }
        assert_eq!(
            receiver.drain(),
            (Some(failure(PEER_FAILURE_CAPACITY as u64)), true)
        );
        assert_eq!(receiver.drain(), (None, false));
        mailbox.sender().publish(failure(101));
        assert_eq!(receiver.drain(), (Some(failure(101)), false));
    }
}
