//! One account's ingress state moves into its lifecycle owner at startup.
//! Each turn handles at most one repair, one advisory acknowledgement, one
//! bounded live-routing burst, one replay record and one lifecycle apply.
//! Large WS frames retain their iterator
//! locally; a recovery fence cannot overtake any record of its frame. The
//! lifecycle loop services its own bounded mailboxes between these turns.
//! No account/strategy state is shared with another account owner.
use super::*;

const LIVE_ROUTE_BUDGET: usize = 32;
const PENDING_APPLY_CAPACITY: usize = 64;

type ReplayCompletion =
    tokio::sync::oneshot::Sender<std::result::Result<ReplayApplySummary, String>>;
type ReplayResult = tokio::sync::oneshot::Receiver<std::result::Result<ReplayApplySummary, String>>;

pub(crate) trait PrivateIngress: Send {
    fn install(&self, shared: &SharedState);
    fn step(
        &mut self,
        shared: &SharedState,
        positions: &mut LivePositionManager,
        replay: &mut PrivateReplayOwner,
    ) -> bool;
    fn register<'a>(&'a self, wait: &mut crossbeam_channel::Select<'a>);
}

struct LiveFrame {
    events: std::vec::IntoIter<PrivateEventDelta>,
    generation: Option<u64>,
}
struct ReplayFrame {
    events: std::vec::IntoIter<PrivateEventDelta>,
    generation: Option<u64>,
    certificate: Option<u64>,
    completion: ReplayCompletion,
    pending: Option<ReplayResult>,
    summary: ReplayApplySummary,
}

struct UnifiedIngress<T> {
    updates: T,
    shutdown: Arc<AtomicBool>,
    live_rx: crossbeam_channel::Receiver<PrivateApplyCommand>,
    replay_rx: crossbeam_channel::Receiver<PrivateApplyCommand>,
    ack_rx: crossbeam_channel::Receiver<Vec<PrivateRouteIdentity>>,
    repair_rx: crossbeam_channel::Receiver<PrivateExecutionRepairReply>,
    feedback: PrivateColdFeedback,
    route: PrivateRouteDedupe,
    committed: PrivateRouteDedupe,
    live: Option<LiveFrame>,
    replay: Option<ReplayFrame>,
    pending_apply: VecDeque<PrivateColdCommand>,
    fence: Option<(u64, tokio::sync::oneshot::Sender<bool>)>,
}

pub(crate) fn prepare(
    shared: &SharedState,
    updates: impl hexagent_runtime::poll_channel::EventSender<RoutedOrderUpdate>,
    shutdown: Arc<AtomicBool>,
) -> Result<(PrivateApplyLane, Box<dyn PrivateIngress>)> {
    let (live_tx, live_rx) = crossbeam_channel::bounded(PRIVATE_APPLY_QUEUE_CAPACITY);
    let (replay_tx, replay_rx) = crossbeam_channel::bounded(PRIVATE_APPLY_QUEUE_CAPACITY);
    let (ack_tx, ack_rx) = crossbeam_channel::bounded(PRIVATE_APPLY_QUEUE_CAPACITY);
    let (repair_tx, repair_rx) = crossbeam_channel::bounded(1);
    let reconnect_generation = Arc::new(AtomicU64::new(0));
    let reconnect_notify = Arc::new(tokio::sync::Notify::new());
    let execution = PrivateExecutionCache::new(
        shared
            .account_state
            .private_execution_seed_checked()
            .map_err(|error| anyhow!(error))?,
    )
    .map_err(|error| anyhow!(error))?;
    let feedback = PrivateColdFeedback {
        execution_ack: Some(execution.ack_lane()),
        ack_tx,
        repair_tx,
        reconnect_generation: Arc::clone(&reconnect_generation),
        reconnect_notify: Arc::clone(&reconnect_notify),
    };
    let lane = PrivateApplyLane {
        live_tx,
        replay_tx,
        reconnect_generation,
        reconnect_notify,
    };
    let mut route = PrivateRouteDedupe::new();
    route.execution_cache = Some(execution);
    Ok((
        lane,
        Box::new(UnifiedIngress {
            updates,
            shutdown,
            live_rx,
            replay_rx,
            ack_rx,
            repair_rx,
            feedback,
            route,
            committed: PrivateRouteDedupe::new(),
            live: None,
            replay: None,
            pending_apply: VecDeque::with_capacity(PENDING_APPLY_CAPACITY),
            fence: None,
        }),
    ))
}

impl<T: hexagent_runtime::poll_channel::EventSender<RoutedOrderUpdate>> UnifiedIngress<T> {
    fn fail(&self, shared: &SharedState, error: &str) {
        shared.user_feed_health.set_recovering(true);
        shared.enqueue_private_apply_failure_diagnostic(error);
        self.feedback
            .reconnect_generation
            .fetch_add(1, Ordering::AcqRel);
        self.feedback.reconnect_notify.notify_one();
    }

    fn route_one(
        &mut self,
        shared: &SharedState,
        event: PrivateEventDelta,
        generation: Option<u64>,
        certificate: Option<u64>,
        completion: Option<ReplayCompletion>,
    ) {
        let routed =
            validate_terminal_replay_scope(&shared.user_feed_health, certificate, generation)
                .and_then(|()| {
                    route_private_batch(
                        shared,
                        &self.updates,
                        std::iter::once(event),
                        generation,
                        &mut self.route,
                        if completion.is_none() {
                            Some(&self.committed)
                        } else {
                            None
                        },
                    )
                });
        match routed {
            Err(error) => {
                self.fail(shared, &error);
                if completion.is_none() {
                    self.live = None;
                }
                if let Some(completion) = completion {
                    let _ = completion.send(Err(error));
                }
            }
            Ok(routed) if routed.events.is_empty() => {
                if let Some(completion) = completion {
                    let _ = completion.send(Ok(ReplayApplySummary {
                        applied: 0,
                        durable_skips: routed.durable_skips,
                    }));
                }
            }
            Ok(routed) => {
                // Owner-local FIFO, preallocated at startup. Reserve capacity
                // before consuming input; do not self-send or block routing on
                // bookkeeping for preceding records in the same short burst.
                assert!(self.pending_apply.len() < PENDING_APPLY_CAPACITY);
                self.pending_apply.push_back(PrivateColdCommand {
                    events: routed.events,
                    identities: routed.identities,
                    durable_skips: routed.durable_skips,
                    recovery_generation: generation,
                    expected_recovery_certificate: certificate,
                    completion,
                    routed_at: crate::latency::Instant::now(),
                    feedback: self.feedback.clone(),
                });
                crate::latency::record_ns(
                    "polymarket.user.pending_apply_depth",
                    self.pending_apply.len() as u64,
                );
                #[cfg(test)]
                shared
                    .private_pending_apply_high
                    .fetch_max(self.pending_apply.len(), Ordering::Relaxed);
            }
        }
    }
}

impl<T: hexagent_runtime::poll_channel::EventSender<RoutedOrderUpdate>> PrivateIngress
    for UnifiedIngress<T>
{
    fn install(&self, shared: &SharedState) {
        shared
            .user_feed_health
            .install_private_update_sink(self.updates.clone());
    }

    fn step(
        &mut self,
        shared: &SharedState,
        positions: &mut LivePositionManager,
        replay: &mut PrivateReplayOwner,
    ) -> bool {
        let mut progressed = false;
        if let Ok(reply) = self.repair_rx.try_recv() {
            progressed = true;
            if let Err(error) =
                finish_private_execution_repair(shared, &self.updates, &mut self.route, reply)
            {
                self.fail(shared, &error);
            }
        }
        if let Ok(identities) = self.ack_rx.try_recv() {
            progressed = true;
            for identity in identities {
                self.route
                    .execution_cache
                    .as_mut()
                    .unwrap()
                    .acknowledge(identity);
                self.committed.remember(identity);
            }
        }
        for _ in 0..LIVE_ROUTE_BUDGET {
            // Keep one credit for replay even under continuous live input.
            if self.pending_apply.len() >= PENDING_APPLY_CAPACITY - 1
                || self.fence.is_some()
                || shared.user_feed_health.private_delivery_pending()
            {
                break;
            }
            if self.live.is_none() {
                if let Ok(command) = self.live_rx.try_recv() {
                    progressed = true;
                    match command {
                        PrivateApplyCommand::Live {
                            events,
                            recovery_generation,
                            enqueued_at,
                        } => {
                            crate::latency::record(
                                "polymarket.user.ws_enqueue_to_owner_dequeue",
                                enqueued_at,
                            );
                            self.live = Some(LiveFrame {
                                events: events.into_iter(),
                                generation: recovery_generation,
                            });
                        }
                        PrivateApplyCommand::RecoveryFence {
                            generation,
                            completion,
                        } => {
                            self.fence = Some((generation, completion));
                            break;
                        }
                        PrivateApplyCommand::Replay { completion, .. } => {
                            let _ = completion
                                .send(Err("replay command reached private live lane".into()));
                        }
                    }
                }
            }
            if let Some(mut frame) = self.live.take() {
                if let Some(event) = frame.events.next() {
                    progressed = true;
                    let generation = frame.generation;
                    if frame.events.len() > 0 {
                        self.live = Some(frame);
                    }
                    self.route_one(shared, event, generation, None, None);
                }
            }
        }
        if self.pending_apply.len() < PENDING_APPLY_CAPACITY
            && !shared.user_feed_health.private_delivery_pending()
        {
            if self.replay.is_none() {
                if let Ok(command) = self.replay_rx.try_recv() {
                    progressed = true;
                    match command {
                        PrivateApplyCommand::Replay {
                            events,
                            recovery_generation,
                            expected_recovery_certificate,
                            completion,
                        } => {
                            match validate_terminal_replay_scope(
                                &shared.user_feed_health,
                                expected_recovery_certificate,
                                recovery_generation,
                            ) {
                                Err(error) => {
                                    let _ = completion.send(Err(error));
                                }
                                Ok(()) => {
                                    self.replay = Some(ReplayFrame {
                                        events: events.into_iter(),
                                        generation: recovery_generation,
                                        certificate: expected_recovery_certificate,
                                        completion,
                                        pending: None,
                                        summary: ReplayApplySummary::default(),
                                    });
                                }
                            }
                        }
                        PrivateApplyCommand::RecoveryFence { completion, .. } => {
                            let _ = completion.send(false);
                        }
                        PrivateApplyCommand::Live { .. } => {
                            self.fail(shared, "live command reached private replay lane")
                        }
                    }
                }
            }
            if let Some(mut frame) = self.replay.take() {
                let result = frame.pending.as_mut().map(|done| done.try_recv());
                match result {
                    Some(Ok(Ok(summary))) => {
                        progressed = true;
                        frame.summary.applied += summary.applied;
                        frame.summary.durable_skips += summary.durable_skips;
                        frame.pending = None;
                    }
                    Some(Ok(Err(error))) => {
                        let _ = frame.completion.send(Err(error));
                        return true;
                    }
                    Some(Err(tokio::sync::oneshot::error::TryRecvError::Closed)) => {
                        let _ = frame
                            .completion
                            .send(Err("private replay completion dropped".into()));
                        return true;
                    }
                    _ => {}
                }
                if frame.pending.is_none() {
                    if let Some(event) = frame.events.next() {
                        progressed = true;
                        let (done, result) = tokio::sync::oneshot::channel();
                        self.route_one(
                            shared,
                            event,
                            frame.generation,
                            frame.certificate,
                            Some(done),
                        );
                        frame.pending = Some(result);
                    } else {
                        let _ = frame.completion.send(Ok(frame.summary));
                        return true;
                    }
                }
                self.replay = Some(frame);
            }
        }
        // Never defer the entire bookkeeping burst to an unbounded queue.
        // One FIFO commit per turn runs even while output delivery is blocked.
        if let Some(command) = self.pending_apply.pop_front() {
            apply_private_cold_command(shared, positions, replay, command);
            progressed = true;
        }
        if self.pending_apply.is_empty() && !shared.user_feed_health.private_delivery_pending() {
            if let Some((generation, completion)) = self.fence.take() {
                let finished = !self.route.repair_inflight
                    && shared
                        .user_feed_health
                        .finish_recovery_delivery_enrollment(generation);
                let _ = completion.send(finished);
                progressed = true;
            }
        }
        progressed
    }

    fn register<'a>(&'a self, wait: &mut crossbeam_channel::Select<'a>) {
        if !self.shutdown.load(Ordering::Acquire) {
            if self.fence.is_none() && self.pending_apply.len() < PENDING_APPLY_CAPACITY - 1 {
                wait.recv(&self.live_rx);
            }
            if self.replay.is_none() && self.pending_apply.len() < PENDING_APPLY_CAPACITY {
                wait.recv(&self.replay_rx);
            }
            wait.recv(&self.ack_rx);
            wait.recv(&self.repair_rx);
        }
    }
}
