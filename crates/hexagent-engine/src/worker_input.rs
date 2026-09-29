//! Owner-local input arbitration. Ready messages never register channel
//! waiters. Only the idle path parks, for at most IDLE_POLL: market producers
//! publish to a bounded polling FIFO and never enter a consumer's wake lock.
use super::*;
use crossbeam_channel::{RecvError, TryRecvError};

pub(super) enum WorkerInput {
    Admission(Result<ExecutionAdmission, RecvError>),
    PrivateControl(Result<crate::exchange::PrivateFeedControl, RecvError>),
    DirectPrivate(Result<RoutedOrderUpdate, RecvError>),
    PrivateUpdate(Result<OrderUpdate, RecvError>),
    CompatUpdate(Result<QueuedOrderUpdate, RecvError>),
    History(Result<HistoricalLoadResult, RecvError>),
    Market(Result<QueuedMarketEvent, RecvError>),
    Idle,
}

fn ready<T>(rx: &Receiver<T>) -> Option<Result<T, RecvError>> {
    match rx.try_recv() {
        Ok(value) => Some(Ok(value)),
        Err(TryRecvError::Disconnected) => Some(Err(RecvError)),
        Err(TryRecvError::Empty) => None,
    }
}

/// Priority is identical in the ready and idle paths. Every invocation handles
/// at most one input, so queued private/lifecycle events are checked again
/// before each market callback. Absent admission owners supply None; other
/// disabled legacy lanes use never receivers. Admission is polled again after
/// the bounded idle sleep, without registering a cross-thread wake lock.
pub(super) fn next_input(
    admission: Option<&hexagent_runtime::latest_snapshot::Receiver<ExecutionAdmission>>,
    control: &Receiver<crate::exchange::PrivateFeedControl>,
    direct: &Receiver<RoutedOrderUpdate>,
    private: &Receiver<OrderUpdate>,
    compat: &Receiver<QueuedOrderUpdate>,
    history: &Receiver<HistoricalLoadResult>,
    market: &hexagent_runtime::poll_channel::Receiver<QueuedMarketEvent>,
    watchdog_wait: std::time::Duration,
) -> WorkerInput {
    macro_rules! take {
        ($rx:expr, $variant:ident) => {
            if let Some(message) = ready($rx) {
                return WorkerInput::$variant(message);
            }
        };
    }
    if let Some(admission) = admission {
        match admission.try_recv() {
            Ok(value) => return WorkerInput::Admission(Ok(value)),
            Err(TryRecvError::Disconnected) => return WorkerInput::Admission(Err(RecvError)),
            Err(TryRecvError::Empty) => {},
        }
    }
    take!(control, PrivateControl);
    take!(direct, DirectPrivate);
    take!(private, PrivateUpdate);
    take!(compat, CompatUpdate);
    take!(history, History);
    let pending = market.len();
    match market.try_recv() {
        Ok(message) => {
            crate::latency::observe_ns("strategy.market.pending_depth", pending as u64);
            return WorkerInput::Market(Ok(message));
        }
        Err(TryRecvError::Disconnected) => return WorkerInput::Market(Err(RecvError)),
        Err(TryRecvError::Empty) => {}
    }
    // An unpublished market head is not permission to spin: its producer may
    // have been preempted. Timed idle parking also leaves the CPU available.
    crossbeam_channel::select_biased! {
        recv(control) -> message => WorkerInput::PrivateControl(message),
        recv(direct) -> message => WorkerInput::DirectPrivate(message),
        recv(private) -> message => WorkerInput::PrivateUpdate(message),
        recv(compat) -> message => WorkerInput::CompatUpdate(message),
        recv(history) -> message => WorkerInput::History(message),
        default(watchdog_wait.min(hexagent_runtime::poll_channel::IDLE_POLL)) => WorkerInput::Idle,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latest_admission_precedes_ready_market_and_disconnect_fails_closed() {
        let (mut publisher, receiver) = crate::execution_admission_lane::snapshot_lane();
        let mut admission = crate::execution_admission_lane::AdmissionConsumer::new(Some(receiver));
        let (market_tx, market) = hexagent_runtime::poll_channel::bounded(1);
        market_tx.try_send(QueuedMarketEvent::Direct(QueuedMarketPayload {
            event: Arc::new(MarketEvent::Exit), enqueued_ns: 1,
        })).unwrap();
        let next = |admission: &crate::execution_admission_lane::AdmissionConsumer| {
            next_input(admission.receiver(), &crossbeam_channel::never(),
                &crossbeam_channel::never(), &crossbeam_channel::never(),
                &crossbeam_channel::never(), &crossbeam_channel::never(),
                &market, std::time::Duration::ZERO)
        };
        for (epoch, state) in [(1, ExecutionAdmissionState::Healthy), (2, ExecutionAdmissionState::Paused)] {
            publisher.publish(ExecutionAdmission {
                exchange: Exchange::Polymarket, epoch, state,
                available_place_slots: u16::from(state == ExecutionAdmissionState::Healthy),
                observed_at_ns: crate::types::now_ns(),
            });
        }
        let WorkerInput::Admission(message) = next(&admission) else { panic!("latest admission first") };
        assert_eq!(admission.receive(message).unwrap().state, ExecutionAdmissionState::Paused);
        assert!(matches!(next(&admission), WorkerInput::Market(Ok(_))));
        assert!(matches!(next(&admission), WorkerInput::Idle));
        drop(publisher);
        let WorkerInput::Admission(message) = next(&admission) else { panic!("disconnect delivered") };
        assert_eq!(admission.receive(message).unwrap().state, ExecutionAdmissionState::Paused);
        assert!(admission.receiver().is_none());
        assert!(matches!(next(&admission), WorkerInput::Idle));
    }

    #[test]
    fn control_reconnect_and_history_precede_market_without_loss_or_duplicates() {
        let (control_tx, control) = bounded(2);
        let (history_tx, history) = bounded(1);
        let (market_tx, market) = hexagent_runtime::poll_channel::bounded(2);
        let next = || {
            next_input(
                None,
                &control,
                &crossbeam_channel::never(),
                &crossbeam_channel::never(),
                &crossbeam_channel::never(),
                &history,
                &market,
                std::time::Duration::ZERO,
            )
        };
        use crate::exchange::PrivateFeedControl;
        control_tx
            .send(PrivateFeedControl::Disconnected(Exchange::Polymarket))
            .unwrap();
        control_tx
            .send(PrivateFeedControl::Connected(Exchange::Polymarket))
            .unwrap();
        history_tx
            .send(HistoricalLoadResult {
                epoch: 7,
                ts_event: 0,
                loaded: Vec::new(),
            })
            .unwrap();
        market_tx
            .try_send(QueuedMarketEvent::Direct(QueuedMarketPayload {
                event: Arc::new(MarketEvent::Exit),
                enqueued_ns: 1,
            }))
            .unwrap();
        assert!(matches!(
            next(),
            WorkerInput::PrivateControl(Ok(PrivateFeedControl::Disconnected(_)))
        ));
        assert!(matches!(
            next(),
            WorkerInput::PrivateControl(Ok(PrivateFeedControl::Connected(_)))
        ));
        assert!(matches!(
            next(),
            WorkerInput::History(Ok(HistoricalLoadResult { epoch: 7, .. }))
        ));
        assert!(matches!(next(), WorkerInput::Market(Ok(_))));
        assert!(matches!(next(), WorkerInput::Idle));
        drop(market_tx);
        assert!(matches!(next(), WorkerInput::Market(Err(_))));
    }
}
