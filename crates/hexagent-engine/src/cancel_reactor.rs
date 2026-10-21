//! Opt-in per-core normal-cancel actor group. Safety owners stay independent.
//! Every physical slot owns one context, one in-flight HTTP reply and one
//! retained lifecycle update. Full outboxes/slow HTTP do not block other slots.
//! Blocking probe/compatibility work transfers (never shares) its boxed context
//! to that slot's pinned worker and returns it on a bounded capacity-one lane.
use super::*;
use hexagent_runtime::{bounded_reactor::Cursor, poll_channel, wake::Wake};

#[cfg(all(test, feature = "offline-http-reactor-bench"))]
#[path = "actual_reactor_benchmark.rs"]
mod actual_benchmark;

// One wake for the entire bounded reactor. Active batches remain disarmed,
// coalescing notifications until every inbox has been rechecked. Keep the
// existing hot-lane platform policy; event waits are the cold-worker default.
pub(super) fn new_reactor_wake() -> Arc<Wake> {
    let wake = Wake::default();
    #[cfg(all(test, feature = "offline-http-reactor-bench"))]
    let wake = if std::env::var("HEX_ACTUAL_REACTOR_EVENT").as_deref() == Ok("1") {
        Wake::event()
    } else { wake };
    #[cfg(all(test, feature = "offline-http-reactor-bench"))]
    let wake = if std::env::var("HEX_ACTUAL_REACTOR_POLL").as_deref() == Ok("1") {
        Wake::default()
    } else { wake };
    Arc::new(wake)
}
fn control_wake() -> Wake {
    let wake = Wake::event();
    #[cfg(all(test, feature = "offline-http-reactor-bench"))]
    let wake = if std::env::var("HEX_ACTUAL_CONTROL_POLL").as_deref() == Ok("1") {
        Wake::default()
    } else { wake };
    wake
}

struct Context {
    router: LiveRouter,
    permit: hexagent_runtime::http1_pool::Permit,
    metrics: Arc<PolyConnectionLaneMetrics>,
    health: Option<SnapshotPublisher<LaneObservation>>,
    sequence: u64,
    prewarm: Arc<str>,
    next_maintenance: u64,
    maintenance_started: u64,
    maintenance_completed: u64,
}
struct Control {
    context: Box<Context>,
    command: PolyConnectionCommand,
    occupancy: PolyConnectionOccupancyGuard,
}
struct Delivery {
    tx: ExecutorUpdateSender,
    update: RoutedOrderUpdate,
    // Keep construction time when retrying publication. Restamping an update
    // must not hide time retained behind a full lifecycle outbox.
    built_ns: u64,
}

struct Pending {
    instance_id: String,
    exchange: Exchange,
    client_order_id: String,
    pending: crate::exchange::polymarket::trade::ReactorPendingCancel,
    update_tx: ExecutorUpdateSender,
}

pub(super) struct Owner {
    context: Option<Box<Context>>,
    rx: poll_channel::Receiver<PolyConnectionCommand>,
    pending: Option<Pending>,
    delivery: Option<Delivery>,
    occupancy: Option<PolyConnectionOccupancyGuard>,
    control: poll_channel::Sender<Control>,
    returned: poll_channel::Receiver<Box<Context>>,
    control_handle: Option<thread::JoinHandle<()>>,
    closed: bool,
}

impl Owner {
    pub(super) fn new(
        mut router: LiveRouter,
        permit: hexagent_runtime::http1_pool::Permit,
        rx: poll_channel::Receiver<PolyConnectionCommand>,
        metrics: Arc<PolyConnectionLaneMetrics>,
        health: Option<SnapshotPublisher<LaneObservation>>,
        prewarm: Arc<str>,
        wake: Arc<Wake>,
        binding: crate::os_tune::ExecutionBinding,
        name: String,
    ) -> Self {
        permit.bind_owner_health_wake(wake.clone());
        for route in router.poly_routes.values_mut() {
            route.bind_cancel_reactor_wake(wake.clone());
        }
        // This fixed cold owner has no work until an explicit control transfer.
        // On Linux both modes use futex. Portable polling on six idle controls
        // otherwise competes with actual HTTP/owner work on developer hosts.
        let (control, jobs) =
            poll_channel::bounded_with_wake::<Control>(1, Some(Arc::new(control_wake())));
        let (back, returned) = poll_channel::bounded_with_wake(1, Some(wake));
        let control_handle = thread::Builder::new()
            .name(name.clone())
            .spawn(move || {
                crate::os_tune::pin_execution_binding(&name, binding);
                crate::latency::prepare_polymarket_order_stages();
                // This explicitly woken lane parks until publication/close (or
                // recv's 60 s timeout); it does not use the no-Wake polling path.
                while let Ok(Control {
                    mut context,
                    command,
                    occupancy,
                }) = jobs.recv()
                {
                    for route in context.router.poly_routes.values_mut() {
                        route.swap_cancel_control_reply_slots();
                    }
                    match command {
                        PolyConnectionCommand::ProbeHttp { request, .. } => {
                            // Normal-cancel role: identical owner/client identity
                            // checks and typed probe failure/recovery as legacy.
                            request.execute_on(context.permit.current_pooled_client());
                        }
                        PolyConnectionCommand::Fallback {
                            signal,
                            stale_ms,
                            update_tx,
                            enqueued_at,
                        } => {
                            crate::latency::record_ns(
                                "polymarket.control.connection_owner_queue",
                                enqueued_at.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                            );
                            for update in
                                execute_fallback_signal(&mut context.router, *signal, stale_ms)
                            {
                                if send_executor_update(&update_tx, update).is_err() {
                                    break;
                                }
                            }
                        }
                        _ => unreachable!("only blocking cold control transfers context"),
                    }
                    for route in context.router.poly_routes.values_mut() {
                        route.swap_cancel_control_reply_slots();
                    }
                    drop(occupancy);
                    if back.try_send(context).is_err() {
                        break;
                    }
                }
            })
            .expect("spawn cancel compatibility owner");
        Self {
            context: Some(Box::new(Context {
                router,
                permit,
                metrics,
                health,
                sequence: 0,
                prewarm,
                next_maintenance: 0,
                maintenance_started: 0,
                maintenance_completed: 0,
            })),
            rx,
            pending: None,
            delivery: None,
            occupancy: None,
            control,
            returned,
            control_handle: Some(control_handle),
            closed: false,
        }
    }

    fn retain_update(&mut self, tx: ExecutorUpdateSender, update: OrderUpdate) {
        #[cfg(feature = "offline-http-reactor-bench")]
        hexagent_runtime::offline_delivery_probe::point(hexagent_runtime::offline_delivery_probe::Phase::HandlerReturn, update.exchange_order_id.as_deref());
        self.delivery = Some(Delivery {
            built_ns: update.timestamp_ns,
            update: RoutedOrderUpdate {
                owner: tx.owner,
                update,
                timing: LifecycleTiming::default(),
            },
            tx,
        });
    }

    fn poll(&mut self, resumed: u64, now: u64) -> bool {
        if self.closed {
            return false;
        }
        if self.context.is_none() {
            match self.returned.try_recv() {
                Ok(context) => {
                    self.context = Some(context);
                    return true;
                }
                Err(crossbeam_channel::TryRecvError::Empty) => return false,
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    panic!("cancel compatibility owner lost its physical context")
                }
            }
        }
        if let Some(mut delivery) = self.delivery.take() {
            // The retained record remains lossless on Full. Stamp each actual
            // publication attempt, matching the existing producer boundary.
            if delivery.update.update.exchange == Exchange::Polymarket {
                delivery.update.update.timestamp_ns = now_ns();
            }
            let built_to_publish = delivery
                .update
                .update
                .timestamp_ns
                .saturating_sub(delivery.built_ns);
            #[cfg(feature = "offline-http-reactor-bench")]
            let publish_probe = hexagent_runtime::offline_delivery_probe::Span::begin(hexagent_runtime::offline_delivery_probe::Phase::Publish, delivery.update.update.exchange_order_id.as_deref());
            let result = delivery.tx.tx.try_send(delivery.update);
            #[cfg(feature = "offline-http-reactor-bench")]
            if let Some(probe) = publish_probe { probe.finish(match &result { Ok(()) => 0, Err(crossbeam_channel::TrySendError::Full(_)) => 1, Err(_) => 2 }); }
            match result {
                Err(crossbeam_channel::TrySendError::Full(update)) => {
                    delivery.update = update;
                    self.delivery = Some(delivery);
                    return false;
                }
                // Same terminal disconnected behavior as legacy ignored send;
                // Full is NEVER treated as disconnected or successful delivery.
                Ok(()) => {
                    // Reuse the existing two update timestamps: no new clock
                    // read on the publication path. Includes work after reply
                    // construction (e.g. attempt audit), not just Full retries.
                    crate::latency::record_ns(
                        "polymarket.cancel.response_built_to_publish",
                        built_to_publish,
                    );
                    self.occupancy.take();
                    return true;
                }
                Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                    self.occupancy.take();
                    return true;
                }
            }
        }
        if let Some(Pending { pending, .. }) = self.pending.as_mut() {
            if !pending.try_ready() {
                return false;
            }
            let Pending {
                instance_id,
                exchange,
                client_order_id,
                pending,
                update_tx,
            } = self.pending.take().unwrap();
            let context = self.context.as_mut().unwrap();
            let update = match context.router.poly_route_mut(&instance_id) {
                Ok(route) => route.complete_reactor_cancel(exchange, &client_order_id, pending),
                Err(error) => {
                    error!("[Executor] Polymarket cancel completion route error: {error}");
                    exec_rejected_cancel(client_order_id, exchange)
                }
            };
            context
                .metrics
                .cancel_reply_pending
                .store(false, Ordering::Release);
            self.retain_update(update_tx, update);
            return true;
        }
        let context = self.context.as_mut().unwrap();
        if context.permit.take_owner_health_notification() || now >= context.next_maintenance {
            context.maintain(now);
            // One state transition per actor visit. Do not hide command prep
            // and HTTP dispatch in the same budget entry as health maintenance.
            return true;
        }
        let mut command = match self.rx.try_recv() {
            Ok(command) => command,
            // Health publication is real work and consumes the turn budget
            // even without a command; do not sweep all actors unbudgeted.
            Err(crossbeam_channel::TryRecvError::Empty) => return false,
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                self.closed = true;
                return false;
            }
        };
        if let Some(trace) = command.hot_path_mut() {
            trace.owner_dequeued_mono_ns = crate::types::monotonic_now_ns();
            trace.owner_resumed_mono_ns = resumed;
            trace.owner_maintenance_started_mono_ns = context.maintenance_started;
            trace.owner_maintenance_completed_mono_ns = context.maintenance_completed;
        }
        context.metrics.queue_depth.store(0, Ordering::Release);
        let occupancy = PolyConnectionOccupancyGuard::new(
            context.metrics.clone(),
            context.permit.current_pooled_client().connection_snapshot(),
        );
        match command {
            PolyConnectionCommand::Cancel {
                cancel_trigger,
                instance_id,
                exchange,
                client_order_id,
                timestamp_ns,
                update_tx,
                enqueued_at,
            } => {
                crate::latency::record_ns(
                    "polymarket.cancel.connection_owner_queue",
                    enqueued_at.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                );
                let client = context.permit.current_pooled_client();
                let pending = match context.router.poly_route_mut(&instance_id) {
                    Ok(route) => {
                        route.set_gen_ns_hint(timestamp_ns);
                        Some(route.cancel_fire_with_trigger(
                            &client_order_id,
                            client,
                            cancel_trigger,
                        ))
                    }
                    Err(error) => {
                        error!("[Executor] Polymarket cancel owner route error: {error}");
                        context
                            .metrics
                            .cancel_reply_pending
                            .store(false, Ordering::Release);
                        None
                    }
                };
                self.occupancy = Some(occupancy);
                if let Some(pending) = pending {
                    let pending = pending.into_reactor();
                    self.pending = Some(Pending {
                        instance_id,
                        exchange,
                        client_order_id,
                        pending,
                        update_tx,
                    });
                } else {
                    self.retain_update(update_tx, exec_rejected_cancel(client_order_id, exchange));
                }
            }
            PolyConnectionCommand::RefreshTransport {
                prewarm_url,
                expected_generation,
                avoid_peer,
            } => {
                let client = context.permit.current_pooled_client();
                if client.connection_snapshot().pool_generation == expected_generation {
                    client.note_instrumented_transport_failure_avoiding(
                        prewarm_url.to_string(),
                        avoid_peer,
                    );
                }
                drop(occupancy);
            }
            command @ (PolyConnectionCommand::ProbeHttp { .. }
            | PolyConnectionCommand::Fallback { .. }) => {
                let job = Control {
                    context: self.context.take().unwrap(),
                    command,
                    occupancy,
                };
                if self.control.try_send(job).is_err() {
                    panic!("physical context transfer cannot overflow or disconnect");
                }
            }
            _ => unreachable!("place/reconcile/safety retain their dedicated owners"),
        }
        true
    }
}

impl Context {
    fn maintain(&mut self, now: u64) {
        self.maintenance_started = crate::types::monotonic_now_ns();
        if self.permit.instrumented_transport_closed() && !self.permit.health_snapshot().quarantined
        {
            self.permit
                .current_pooled_client()
                .note_instrumented_transport_failure_with_cause(
                    self.prewarm.to_string(),
                    hexagent_runtime::http1_pool::TransportRepairCause::IdleSocketClosed,
                    0,
                );
        }
        if let Some(publisher) = self.health.as_mut() {
            let business = self.permit.business_outcome();
            let (failures, slow) = self.permit.business_outcome_totals();
            let (cumulative_no_response, no_response_generation) =
                self.permit.business_no_response_evidence();
            let (cumulative_slow_retired, slow_retired_generation) =
                self.permit.business_slow_retirement_evidence();
            self.sequence += 1;
            publisher.publish(LaneObservation {
                observed_mono_ns: crate::types::monotonic_now_ns(),
                repair: self.permit.transport_repair_snapshot(),
                sequence: self.sequence,
                observed_at_ns: now_ns(),
                health: self.permit.health_snapshot(),
                business,
                busy: self.metrics.occupied.load(Ordering::Acquire),
                cumulative_failures: failures,
                cumulative_slow: slow,
                cumulative_no_response,
                cumulative_slow_retired,
                slow_retired_generation,
                no_response_generation,
                no_response_peer: self.permit.business_no_response_peer(),
            });
        }
        self.maintenance_completed = crate::types::monotonic_now_ns();
        self.next_maintenance =
            now.saturating_add(HEARTBEAT.as_nanos().min(u64::MAX as u128) as u64);
    }
}

fn idle_wait(owners: &[Owner], now: u64) -> Duration {
    // Command, HTTP reply, health and context-return lanes all notify this
    // reactor. Only lifecycle outbox capacity has no consumer-space signal.
    let mut wait = if owners.iter().any(|owner| owner.delivery.is_some()) {
        Duration::from_micros(50)
    } else {
        HEARTBEAT
    };
    for owner in owners {
        if owner.closed || owner.pending.is_some() || owner.delivery.is_some() {
            continue;
        }
        if let Some(context) = owner.context.as_ref() {
            wait = wait.min(Duration::from_nanos(
                context.next_maintenance.saturating_sub(now),
            ));
        }
    }
    wait
}

pub(super) fn run(mut owners: Vec<Owner>, wake: Arc<Wake>, batch: usize) {
    crate::latency::prepare_polymarket_order_stages();
    crate::latency::prepare_thread_stages(&["polymarket.cancel.response_built_to_publish"]);
    let mut cursor = Cursor::new(owners.len(), batch.min(owners.len()));
    let mut resumed = crate::types::monotonic_now_ns();
    loop {
        let now = crate::types::monotonic_now_ns();
        let mut turn = cursor.poll(|index| owners[index].poll(resumed, now));
        if owners.iter().all(|owner| owner.closed) {
            wake.cancel();
            break;
        }
        if turn.progressed == 0 {
            // Active batches stay disarmed: publishers need not enter futex
            // while this core is already draining. Recheck every actor after
            // arming so publication/health/reply races cannot be lost.
            wake.arm();
            turn = cursor.poll(|index| owners[index].poll(resumed, now));
        }
        if turn.progressed > 0 {
            wake.cancel();
            // Linux FIFO needs an explicit handoff to equal-priority owners.
            // macOS runs this reactor under ordinary preemptive scheduling:
            // sched_yield's priority-depressing path caused millisecond tails
            // in the actual HTTP fixture. Actor rotation/budget stay bounded;
            // empty work still parks through Wake and the maintenance deadline.
            #[cfg(not(target_os = "macos"))]
            thread::yield_now();
        } else {
            wake.wait(idle_wait(&owners, now));
        }
        resumed = crate::types::monotonic_now_ns();
    }
    let handles: Vec<_> = owners
        .iter_mut()
        .filter_map(|owner| owner.control_handle.take())
        .collect();
    drop(owners); // close control inboxes before joining idle workers
    for handle in handles {
        handle.join().expect("cancel control owner panicked");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn full_outbox_retains_identity_occupancy_and_other_owner_progress() {
        // Account pools are process-global startup state. Exercise the real
        // owner in a child test process so unrelated engine shutdown tests do
        // not inherit a synthetic account with no matching endpoint.
        const CHILD: &str = "HEXAGENT_CANCEL_REACTOR_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "engine::cancel_reactor::tests::full_outbox_retains_identity_occupancy_and_other_owner_progress", "--test-threads=1"])
                .env(CHILD, "1").status().unwrap();
            assert!(status.success());
            return;
        }
        use hexagent_runtime::http1_pool::{self, Role};
        let mut accounts = HashMap::new();
        accounts.insert("reactor-test".to_string(), vec!["one".to_string()]);
        http1_pool::init_account_pools(&accounts).unwrap();
        let config: Config = toml::from_str("[general]\nmode = 'live'").unwrap();
        let wake = Arc::new(Wake::default());
        let mut owners = Vec::new();
        let mut senders = Vec::new();
        let mut metrics = Vec::new();
        let (out, received) = account_completion_channel(1);
        out.send(RoutedOrderUpdate {
            owner: 99,
            update: exec_rejected_cancel("sentinel".into(), Exchange::Polymarket),
            timing: LifecycleTiming::default(),
        })
        .unwrap();
        for slot in 0..2 {
            let permit =
                http1_pool::take_account_execution_slot("reactor-test", Role::Cancel, slot)
                    .unwrap();
            let (tx, rx) = poll_channel::bounded_with_wake(1, Some(wake.clone()));
            let m = Arc::new(PolyConnectionLaneMetrics::new(
                "reactor-test",
                Role::Cancel,
                slot,
            ));
            let lane = PolyConnectionLane {
                tx,
                metrics: m.clone(),
                preparation: None,
            };
            let binding = crate::os_tune::reserve_execution_binding(
                "reactor-test",
                crate::os_tune::ExecutionThreadRole::PolymarketCancel,
            );
            owners.push(Owner::new(
                LiveRouter::new_with_poly_map(&config, &HashMap::new()),
                permit,
                rx,
                m.clone(),
                None,
                Arc::from("http://127.0.0.1:1/"),
                wake.clone(),
                binding,
                format!("test-cancel-control-{slot}"),
            ));
            metrics.push(m);
            senders.push(lane);
        }
        for slot in 0..2 {
            senders[slot]
                .try_send(PolyConnectionCommand::Cancel {
                    cancel_trigger: Default::default(),
                    instance_id: format!("unknown-{slot}"),
                    exchange: Exchange::Polymarket,
                    client_order_id: format!("unknown-{slot}-order"),
                    timestamp_ns: 0,
                    update_tx: ExecutorUpdateSender {
                        owner: slot as u16,
                        tx: out.clone().into(),
                    },
                    enqueued_at: Instant::now(),
                })
                .unwrap();
        }
        // Both unknown-owner failures become typed updates. The first full
        // output cannot prevent the second physical actor from dequeuing.
        for owner in &mut owners {
            assert!(owner.poll(1, 1)); // maintenance only consumes this visit
            assert!(owner.delivery.is_none());
            assert_eq!(owner.rx.len(), 1, "maintenance cannot hide command work in its budget");
            assert!(owner.poll(1, 1));
        }
        assert!(owners.iter().all(|owner| owner.delivery.is_some()));
        let built: Vec<_> = owners
            .iter()
            .map(|owner| owner.delivery.as_ref().unwrap().built_ns)
            .collect();
        for owner in &mut owners {
            assert!(!owner.poll(2, 2));
        }
        assert_eq!(idle_wait(&owners, 2), Duration::from_micros(50));
        assert_eq!(
            built,
            owners
                .iter()
                .map(|owner| owner.delivery.as_ref().unwrap().built_ns)
                .collect::<Vec<_>>()
        );
        assert!(metrics.iter().all(|m| m.occupied.load(Ordering::Acquire)));
        assert_eq!(received.recv().unwrap().owner, 99);
        for (slot, owner) in owners.iter_mut().enumerate() {
            assert!(owner.poll(3, 3));
            let update = received.recv().unwrap();
            assert_eq!(update.owner, slot as u16);
            assert_eq!(
                update.update.client_order_id,
                format!("unknown-{slot}-order")
            );
            assert_eq!(update.update.status, OrderStatus::ExecutorRejected);
            assert!(!metrics[slot].occupied.load(Ordering::Acquire));
            assert!(!owner.poll(4, 4));
        }
        assert!(idle_wait(&owners, 4) > Duration::from_millis(90));
        owners[0].context.as_mut().unwrap().next_maintenance = 10;
        assert_eq!(idle_wait(&owners, 4), Duration::from_nanos(6));
        // Cold control transfers the entire context; neither thread borrows
        // the other's LiveRouter/permit. An unknown route never performs HTTP.
        senders[0]
            .try_send(PolyConnectionCommand::Fallback {
                signal: Box::new(Signal::PolymarketCancelAllOrders {
                    reason: "test".into(),
                    market: None,
                    asset_ids: vec![],
                    instance_id: "unknown".into(),
                }),
                stale_ms: 0,
                update_tx: ExecutorUpdateSender {
                    owner: 7,
                    tx: out.clone().into(),
                },
                enqueued_at: Instant::now(),
            })
            .unwrap();
        assert!(owners[0].poll(5, 5));
        assert!(owners[0].context.is_none());
        let deadline = Instant::now() + Duration::from_secs(2);
        while owners[0].context.is_none() {
            owners[0].poll(6, 6);
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        assert!(!metrics[0].occupied.load(Ordering::Acquire));
        while received.try_recv().is_ok() {}
        out.try_send(RoutedOrderUpdate {
            owner: 99,
            update: exec_rejected_cancel("sentinel-2".into(), Exchange::Polymarket),
            timing: LifecycleTiming::default(),
        })
        .unwrap();
        for slot in 0..2 {
            senders[slot]
                .try_send(PolyConnectionCommand::Cancel {
                    cancel_trigger: Default::default(),
                    instance_id: format!("unknown-{slot}"),
                    exchange: Exchange::Polymarket,
                    client_order_id: format!("queued-{slot}"),
                    timestamp_ns: 0,
                    update_tx: ExecutorUpdateSender {
                        owner: slot as u16,
                        tx: out.clone().into(),
                    },
                    enqueued_at: Instant::now(),
                })
                .unwrap();
        }
        let reactor = thread::spawn(move || run(owners, wake, 2));
        let deadline = Instant::now() + Duration::from_secs(2);
        // Exercise the actual driver: neither owner may block on the occupied
        // outbox while the other still has a command queued.
        while metrics
            .iter()
            .any(|m| m.queue_depth.load(Ordering::Acquire) != 0)
        {
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        assert!(metrics.iter().all(|m| m.occupied.load(Ordering::Acquire)));
        assert_eq!(received.recv().unwrap().owner, 99);
        let mut delivered = Vec::new();
        for _ in 0..2 {
            let update = received.recv_timeout(Duration::from_secs(2)).unwrap();
            delivered.push((update.owner, update.update.client_order_id));
        }
        delivered.sort();
        assert_eq!(
            delivered,
            vec![(0, "queued-0".into()), (1, "queued-1".into())]
        );
        drop(senders);
        reactor.join().unwrap(); // notification-driven disconnect joins every worker
        assert!(metrics.iter().all(|m| !m.occupied.load(Ordering::Acquire)));
        assert!(received.try_recv().is_err());
    }
}
