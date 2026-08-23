//! Process-run shutdown coordination.
//!
//! A token has two monotonic phases:
//! - `Requested`: producers and non-critical observers stop accepting work.
//! - `Finished`: producer threads have joined; lossless account/audit writers
//!   drain their bounded lanes and may exit.
//!
//! Every subscriber gets both phase changes.  The broadcast lane is used only
//! during shutdown, so it stays entirely outside latency-sensitive paths.

use crossbeam_channel::{bounded, Receiver, Sender};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

const RUNNING: u8 = 0;
const REQUESTED: u8 = 1;
const FINISHED: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownPhase {
    Requested,
    Finished,
}

impl ShutdownPhase {
    fn as_u8(self) -> u8 {
        match self {
            Self::Requested => REQUESTED,
            Self::Finished => FINISHED,
        }
    }

    fn from_u8(value: u8) -> Option<Self> {
        match value {
            REQUESTED => Some(Self::Requested),
            FINISHED => Some(Self::Finished),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct ShutdownState {
    phase: AtomicU8,
    requested: Arc<AtomicBool>,
    subscribers: Mutex<Vec<Sender<ShutdownPhase>>>,
}

/// Cloneable broadcast shutdown token shared by one engine run.
#[derive(Debug, Clone)]
pub struct ShutdownToken {
    state: Arc<ShutdownState>,
}

impl Default for ShutdownToken {
    fn default() -> Self {
        Self::new()
    }
}

impl ShutdownToken {
    pub fn new() -> Self {
        Self {
            state: Arc::new(ShutdownState {
                phase: AtomicU8::new(RUNNING),
                requested: Arc::new(AtomicBool::new(false)),
                subscribers: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Subscribe to phase changes. A late subscriber immediately receives the
    /// current phase, so startup/shutdown races cannot strand a worker.
    pub fn subscribe(&self) -> Receiver<ShutdownPhase> {
        let (tx, rx) = bounded(2);
        let mut subscribers = self
            .state
            .subscribers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let phase = self.state.phase.load(Ordering::Acquire);
        if let Some(phase) = ShutdownPhase::from_u8(phase) {
            let _ = tx.try_send(phase);
        }
        if phase < FINISHED {
            subscribers.push(tx);
        }
        rx
    }

    pub fn is_requested(&self) -> bool {
        self.state.requested.load(Ordering::Acquire)
    }

    /// Compatibility view for async loops that already poll an atomic flag.
    /// The flag is owned by this token and flips only through [`Self::request`]
    /// or [`Self::finish`]. New blocking workers should prefer [`Self::subscribe`].
    pub fn requested_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.state.requested)
    }

    pub fn is_finished(&self) -> bool {
        self.state.phase.load(Ordering::Acquire) >= FINISHED
    }

    /// Begin coordinated shutdown. Idempotent.
    pub fn request(&self) {
        self.advance(ShutdownPhase::Requested);
    }

    /// Announce that work producers have stopped. Lossless workers should
    /// drain their bounded queues before exiting after this phase.
    pub fn finish(&self) {
        self.advance(ShutdownPhase::Finished);
    }

    fn advance(&self, target: ShutdownPhase) {
        let target_value = target.as_u8();
        self.state.requested.store(true, Ordering::Release);
        let previous = self.state.phase.fetch_max(target_value, Ordering::AcqRel);
        if previous >= target_value {
            return;
        }
        let mut subscribers = self
            .state
            .subscribers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        subscribers.retain(|subscriber| subscriber.try_send(target).is_ok());
        if target == ShutdownPhase::Finished {
            subscribers.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn broadcasts_both_shutdown_phases_to_every_subscriber() {
        let token = ShutdownToken::new();
        let first = token.subscribe();
        let second = token.subscribe();

        token.request();
        assert_eq!(
            first.recv_timeout(Duration::from_millis(50)),
            Ok(ShutdownPhase::Requested)
        );
        assert_eq!(
            second.recv_timeout(Duration::from_millis(50)),
            Ok(ShutdownPhase::Requested)
        );

        token.finish();
        assert_eq!(
            first.recv_timeout(Duration::from_millis(50)),
            Ok(ShutdownPhase::Finished)
        );
        assert_eq!(
            second.recv_timeout(Duration::from_millis(50)),
            Ok(ShutdownPhase::Finished)
        );
    }

    #[test]
    fn late_subscriber_observes_finished_state() {
        let token = ShutdownToken::new();
        token.request();
        token.finish();
        assert_eq!(
            token.subscribe().recv_timeout(Duration::from_millis(50)),
            Ok(ShutdownPhase::Finished),
        );
    }
}
