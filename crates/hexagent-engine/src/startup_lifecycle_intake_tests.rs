//! Exercise the actual dedicated strategy worker. Channels below are test
//! fixtures; production keeps its existing capacities and receiver ownership.
use super::*;

#[derive(Debug, PartialEq, Eq)]
enum Observed {
    Lifecycle(String, u64, LifecycleSource),
    Watchdog(bool),
    Market,
}

struct IntakeStrategy {
    id: &'static str,
    observed: Sender<Observed>,
    release: Arc<AtomicBool>,
    paused: bool,
    pause_on_first: bool,
    received: usize,
    last_callback_was_market: bool,
}

impl Strategy for IntakeStrategy {
    fn name(&self) -> &str {
        "startup-intake-test"
    }
    fn instance_id(&self) -> &str {
        self.id
    }
    fn startup_lifecycle_intake_paused(&self) -> bool {
        assert!(
            !self.last_callback_was_market,
            "bootstrap hook was polled after a market callback"
        );
        self.paused
    }
    fn on_lifecycle_update_owned_into(
        &mut self,
        envelope: LifecycleEnvelope,
        _out: &mut SignalBatch,
    ) -> Result<(), SignalBatchOverflow> {
        self.last_callback_was_market = false;
        assert!(
            !self.paused,
            "worker consumed an envelope while owner capacity was exhausted"
        );
        self.received += 1;
        if self.pause_on_first && self.received == 1 {
            self.paused = true;
        }
        self.observed
            .send(Observed::Lifecycle(
                envelope.update.client_order_id,
                envelope.sequence,
                envelope.source,
            ))
            .unwrap();
        Ok(())
    }
    fn on_watchdog_into(
        &mut self,
        _now_ns: u64,
        _out: &mut SignalBatch,
    ) -> Result<(), SignalBatchOverflow> {
        self.last_callback_was_market = false;
        if self.release.load(Ordering::Acquire) {
            self.paused = false;
        }
        let _ = self.observed.try_send(Observed::Watchdog(self.paused));
        Ok(())
    }
    fn on_orderbook(&mut self, _book: &OrderBookSnapshot) {
        self.last_callback_was_market = true;
        let _ = self.observed.try_send(Observed::Market);
    }
}

fn update(coid: &str) -> OrderUpdate {
    OrderUpdate {
        order_slot: OrderSlot::with_generation(7, 1),
        client_order_id: coid.into(),
        exchange: Exchange::Polymarket,
        symbol: "token".into(),
        side: Side::Buy,
        exchange_order_id: None,
        status: OrderStatus::Accepted,
        liquidity: None,
        filled_quantity: 0.0,
        remaining_quantity: 2.0,
        avg_fill_price: 0.5,
        timestamp_ns: 1,
        exchange_event_timestamp_ns: None,
        trade_id: None,
        trade_fee: None,
        order_audit: None,
        error: None,
    }
}

struct Worker {
    market: Sender<QueuedMarketEvent>,
    direct: Sender<RoutedOrderUpdate>,
    compat: Sender<QueuedOrderUpdate>,
    direct_rx: Receiver<RoutedOrderUpdate>,
    compat_rx: Receiver<QueuedOrderUpdate>,
    observed: Receiver<Observed>,
    release: Arc<AtomicBool>,
    join: Option<thread::JoinHandle<()>>,
    _signals: Receiver<RoutedSignal>,
}

impl Worker {
    fn new(id: &'static str, paused: bool, pause_on_first: bool) -> Self {
        let (market, market_rx) = bounded(8);
        let (direct, direct_rx) = bounded(4);
        let (compat, compat_rx) = bounded(4);
        let (observed_tx, observed) = bounded(64);
        let (signals, signal_rx) = bounded(8);
        let (shutdown_ack, _shutdown_ack_rx) = bounded(1);
        let release = Arc::new(AtomicBool::new(false));
        let strategy = IntakeStrategy {
            id,
            observed: observed_tx,
            release: Arc::clone(&release),
            paused,
            pause_on_first,
            received: 0,
            last_callback_was_market: false,
        };
        let worker_direct_rx = direct_rx.clone();
        let worker_compat_rx = compat_rx.clone();
        let join = thread::Builder::new()
            .name(format!("startup-intake-{id}"))
            .spawn(move || {
                Engine::run_strategy_worker(
                    Box::new(strategy),
                    market_rx,
                    Arc::new(LatestMarketStore::default()),
                    worker_compat_rx,
                    worker_direct_rx,
                    SignalSender::system(signals).with_owner(0),
                    Vec::new(),
                    id,
                    0,
                    Arc::new(AtomicU64::new(0)),
                    Arc::new(AtomicBool::new(false)),
                    Arc::new(AtomicBool::new(false)),
                    shutdown_ack,
                    Arc::new(Instant::now()),
                    None,
                )
            })
            .unwrap();
        Self {
            market,
            direct,
            compat,
            direct_rx,
            compat_rx,
            observed,
            release,
            join: Some(join),
            _signals: signal_rx,
        }
    }
    fn send_direct(&self, coid: &str) {
        self.direct
            .send(RoutedOrderUpdate {
                owner: 0,
                update: update(coid),
                timing: LifecycleTiming::default(),
            })
            .unwrap();
    }
    fn send_compat(&self, coid: &str) {
        self.compat
            .send(QueuedOrderUpdate {
                update: update(coid),
                timing: LifecycleTiming::default(),
                enqueued_at: Instant::now(),
                source: LifecycleSource::Execution,
            })
            .unwrap();
    }
    fn wait_for(&self, mut predicate: impl FnMut(&Observed) -> bool) -> Observed {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let event = self
                .observed
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .expect("strategy worker made no progress");
            if predicate(&event) {
                return event;
            }
        }
    }
    fn stop(&mut self) {
        if let Some(join) = self.join.take() {
            let _ = self
                .market
                .send(QueuedMarketEvent::Direct(QueuedMarketPayload {
                    event: Arc::new(MarketEvent::Exit),
                    enqueued_ns: market_queue_monotonic_ns(),
                }));
            join.join().unwrap();
        }
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.stop();
    }
}

#[test]
fn startup_lifecycle_intake_retains_full_buffer_message_and_resumes_both_lanes_in_order() {
    let mut worker = Worker::new("intake-a", false, true);
    worker.send_direct("direct-first");
    assert_eq!(
        worker.wait_for(|v| matches!(v, Observed::Lifecycle(..))),
        Observed::Lifecycle("direct-first".into(), 1, LifecycleSource::PrivateFeed)
    );
    worker.send_direct("direct-second");
    worker.send_compat("compat-first");
    worker.send_compat("compat-second");
    worker.wait_for(|v| *v == Observed::Watchdog(true));
    assert_eq!((worker.direct_rx.len(), worker.compat_rx.len()), (1, 2));
    worker
        .market
        .send(QueuedMarketEvent::Direct(QueuedMarketPayload {
            event: Arc::new(MarketEvent::OrderBook(OrderBookSnapshot {
                exchange: Exchange::Binance,
                symbol: "BTCUSDT".into(),
                bids: vec![],
                asks: vec![],
                exchange_timestamp_ns: 1,
                local_timestamp_ns: 1,
            })),
            enqueued_ns: market_queue_monotonic_ns(),
        }))
        .unwrap();
    worker.wait_for(|v| *v == Observed::Market);
    worker.wait_for(|v| *v == Observed::Watchdog(true));
    assert_eq!((worker.direct_rx.len(), worker.compat_rx.len()), (1, 2));
    worker.release.store(true, Ordering::Release);
    for expected in [
        Observed::Lifecycle("direct-second".into(), 2, LifecycleSource::PrivateFeed),
        Observed::Lifecycle("compat-first".into(), 3, LifecycleSource::Execution),
        Observed::Lifecycle("compat-second".into(), 4, LifecycleSource::Execution),
    ] {
        assert_eq!(
            worker.wait_for(|v| matches!(v, Observed::Lifecycle(..))),
            expected
        );
    }
    assert_eq!((worker.direct_rx.len(), worker.compat_rx.len()), (0, 0));
    worker.stop();
}

#[test]
fn startup_lifecycle_intake_pause_is_instance_local_and_does_not_block_watchdog() {
    let mut paused = Worker::new("intake-paused", true, false);
    let mut ready = Worker::new("intake-ready", false, false);
    paused.send_compat("paused-owner");
    ready.send_compat("ready-owner");
    assert_eq!(
        ready.wait_for(|v| matches!(v, Observed::Lifecycle(..))),
        Observed::Lifecycle("ready-owner".into(), 1, LifecycleSource::Execution)
    );
    paused.wait_for(|v| *v == Observed::Watchdog(true));
    assert_eq!(paused.compat_rx.len(), 1);
    paused.release.store(true, Ordering::Release);
    assert_eq!(
        paused.wait_for(|v| matches!(v, Observed::Lifecycle(..))),
        Observed::Lifecycle("paused-owner".into(), 1, LifecycleSource::Execution)
    );
    paused.stop();
    ready.stop();
}

#[test]
fn startup_lifecycle_intake_shutdown_drain_does_not_bypass_paused_owner() {
    let mut worker = Worker::new("intake-shutdown", true, false);
    worker.send_direct("retained-direct");
    worker.send_compat("retained-compat");
    worker.wait_for(|v| *v == Observed::Watchdog(true));
    worker.stop();
    // A test receiver clone observes ownership remained in each upstream lane.
    // Process exit durability still relies on the existing replay/reconciliation.
    assert_eq!(
        worker.direct_rx.try_recv().unwrap().update.client_order_id,
        "retained-direct"
    );
    assert_eq!(
        worker.compat_rx.try_recv().unwrap().update.client_order_id,
        "retained-compat"
    );
    assert!(!worker
        .observed
        .try_iter()
        .any(|v| matches!(v, Observed::Lifecycle(..))));
}

#[test]
fn startup_lifecycle_intake_selector_preserves_all_three_existing_input_lanes() {
    let (private_tx, private_rx) = bounded(1);
    let (direct_tx, direct_rx) = bounded(1);
    let (compat_tx, compat_rx) = bounded(1);
    let private_never = crossbeam_channel::never();
    let direct_never = crossbeam_channel::never();
    let compat_never = crossbeam_channel::never();
    private_tx.send(update("feed")).unwrap();
    direct_tx
        .send(RoutedOrderUpdate {
            owner: 0,
            update: update("direct"),
            timing: LifecycleTiming::default(),
        })
        .unwrap();
    compat_tx
        .send(QueuedOrderUpdate {
            update: update("compat"),
            timing: LifecycleTiming::default(),
            enqueued_at: Instant::now(),
            source: LifecycleSource::Execution,
        })
        .unwrap();
    assert!(
        startup_lifecycle_receiver(true, &private_rx, &private_never)
            .try_recv()
            .is_err()
    );
    assert!(startup_lifecycle_receiver(true, &direct_rx, &direct_never)
        .try_recv()
        .is_err());
    assert!(startup_lifecycle_receiver(true, &compat_rx, &compat_never)
        .try_recv()
        .is_err());
    assert_eq!(
        (private_rx.len(), direct_rx.len(), compat_rx.len()),
        (1, 1, 1)
    );
    assert_eq!(
        startup_lifecycle_receiver(false, &private_rx, &private_never)
            .try_recv()
            .unwrap()
            .client_order_id,
        "feed"
    );
    assert_eq!(
        startup_lifecycle_receiver(false, &direct_rx, &direct_never)
            .try_recv()
            .unwrap()
            .update
            .client_order_id,
        "direct"
    );
    assert_eq!(
        startup_lifecycle_receiver(false, &compat_rx, &compat_never)
            .try_recv()
            .unwrap()
            .update
            .client_order_id,
        "compat"
    );
}

#[test]
#[ignore = "manual old/new ready-lane select microbenchmark, no network or scheduling"]
fn startup_lifecycle_intake_ready_select_benchmark() {
    const N: usize = 100_000;
    let lanes = [bounded::<u64>(1), bounded::<u64>(1), bounded::<u64>(1)];
    let never = crossbeam_channel::never();
    let mut baseline = Vec::with_capacity(N);
    let mut current = Vec::with_capacity(N);
    for index in 0..N {
        for new in if index & 1 == 0 {
            [false, true]
        } else {
            [true, false]
        } {
            lanes[index % 3].0.try_send(index as u64).unwrap();
            let paused = std::hint::black_box(false);
            let started = Instant::now();
            let value = if new {
                let private = startup_lifecycle_receiver(paused, &lanes[0].1, &never);
                let direct = startup_lifecycle_receiver(paused, &lanes[1].1, &never);
                let compat = startup_lifecycle_receiver(paused, &lanes[2].1, &never);
                crossbeam_channel::select_biased! {
                    recv(private) -> value => value.unwrap(),
                    recv(direct) -> value => value.unwrap(),
                    recv(compat) -> value => value.unwrap(),
                }
            } else {
                crossbeam_channel::select_biased! {
                    recv(&lanes[0].1) -> value => value.unwrap(),
                    recv(&lanes[1].1) -> value => value.unwrap(),
                    recv(&lanes[2].1) -> value => value.unwrap(),
                }
            };
            std::hint::black_box(value);
            let elapsed = started.elapsed().as_nanos() as u64;
            if new {
                current.push(elapsed);
            } else {
                baseline.push(elapsed);
            }
        }
    }
    for (name, samples) in [("old", &mut baseline), ("new", &mut current)] {
        samples.sort_unstable();
        let p = |n: usize, d: usize| samples[(samples.len() - 1) * n / d];
        eprintln!("startup_lifecycle_select version={name} boundary=three_ready_lanes_selection_and_dequeue n={N} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth=1 overflow=0 callback_and_network_excluded=true", p(1,2), p(99,100), p(999,1000), p(1,1));
    }
}

struct FaultStrategy {
    faulted: bool,
    dropped: Arc<AtomicBool>,
    exit_calls: Arc<AtomicUsize>,
    // A real original-generation certificate must survive quarantine without
    // being ACKed. Controlled teardown drops it without committing.
    _retained: crate::exchange::polymarket::live_position::DeferredRecoveryUpdateAck,
}
impl Strategy for FaultStrategy {
    fn name(&self) -> &str {
        "startup-deadline-test"
    }
    fn startup_lifecycle_intake_paused(&self) -> bool {
        true
    }
    fn on_exit(&mut self) {
        self.exit_calls.fetch_add(1, Ordering::AcqRel);
    }
    fn startup_lifecycle_failure(&self) -> Option<&'static str> {
        self.faulted
            .then_some("startup lifecycle made no progress before its deadline")
    }
    fn on_lifecycle_update_owned_into(
        &mut self,
        _: LifecycleEnvelope,
        _: &mut SignalBatch,
    ) -> Result<(), SignalBatchOverflow> {
        panic!("faulted startup cannot consume private ownership")
    }
    fn on_watchdog_into(
        &mut self,
        _: u64,
        out: &mut SignalBatch,
    ) -> Result<(), SignalBatchOverflow> {
        self.faulted = true;
        // This pending batch must be discarded when the watchdog reports a
        // terminal handoff fault. Only the dedicated emergency cancel may emit.
        crate::types::extend_signal_batch(out, [Signal::Exit])
    }
}
impl Drop for FaultStrategy {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}

#[test]
fn startup_lifecycle_intake_terminal_fault_retains_ownership_until_each_controlled_exit() {
    use crate::exchange::polymarket::live_position::UserFeedHealth;
    for exit_mode in 0..3 {
        let health = Arc::new(UserFeedHealth::new());
        let generation = health.begin_recovery_delivery();
        let retained = update("retained-original-epoch");
        health
            .register_recovery_update(generation, "fault-owner", &retained)
            .unwrap();
        health.finish_recovery_delivery_enrollment(generation);
        let token = health
            .deferred_recovery_update_ack("fault-owner", &retained)
            .unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let exit_calls = Arc::new(AtomicUsize::new(0));
        let (market_tx, market_rx) = bounded(1);
        let mut market_tx = Some(market_tx);
        let (direct_tx, direct_rx) = bounded(1);
        let (compat_tx, compat_rx) = bounded(1);
        direct_tx
            .send(RoutedOrderUpdate {
                owner: 0,
                update: update("queued-direct"),
                timing: LifecycleTiming::default(),
            })
            .unwrap();
        compat_tx
            .send(QueuedOrderUpdate {
                update: update("queued-compat"),
                timing: LifecycleTiming::default(),
                enqueued_at: Instant::now(),
                source: LifecycleSource::Execution,
            })
            .unwrap();
        let (signals, signals_rx) = bounded(4);
        let (shutdown_ack, shutdown_ack_rx) = bounded(1);
        let shutdown = Arc::new(AtomicBool::new(false));
        let quarantined = Arc::new(AtomicBool::new(false));
        let worker_shutdown = Arc::clone(&shutdown);
        let worker_quarantined = Arc::clone(&quarantined);
        let worker_direct = direct_rx.clone();
        let worker_compat = compat_rx.clone();
        let strategy = FaultStrategy {
            faulted: false,
            dropped: Arc::clone(&dropped),
            exit_calls: Arc::clone(&exit_calls),
            _retained: token,
        };
        let worker = thread::spawn(move || {
            Engine::run_strategy_worker(
                Box::new(strategy),
                market_rx,
                Arc::new(LatestMarketStore::default()),
                worker_compat,
                worker_direct,
                SignalSender::system(signals).with_owner(0),
                Vec::new(),
                "fault-owner",
                0,
                Arc::new(AtomicU64::new(0)),
                worker_quarantined,
                worker_shutdown,
                shutdown_ack,
                Arc::new(Instant::now()),
                None,
            )
        });
        let emergency = signals_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(emergency.owner, 0);
        assert!(
            matches!(emergency.signal, Signal::PolymarketCancelAllOrders { ref instance_id, .. } if instance_id == "fault-owner")
        );
        assert!(
            signals_rx.is_empty(),
            "partial callback signals must not escape"
        );
        assert!(quarantined.load(Ordering::Acquire));
        assert!(!dropped.load(Ordering::Acquire));
        assert_eq!(exit_calls.load(Ordering::Acquire), 0);
        assert!(!worker.is_finished());
        assert_eq!((direct_rx.len(), compat_rx.len()), (1, 1));
        assert_eq!(
            health.recovery_delivery_progress(generation),
            Some((true, 1))
        );
        match exit_mode {
            0 => {
                shutdown.store(true, Ordering::Release);
                assert_eq!(
                    shutdown_ack_rx
                        .recv_timeout(Duration::from_secs(2))
                        .unwrap(),
                    0
                );
            }
            1 => market_tx
                .as_ref()
                .unwrap()
                .send(QueuedMarketEvent::Direct(QueuedMarketPayload {
                    event: Arc::new(MarketEvent::Exit),
                    enqueued_ns: market_queue_monotonic_ns(),
                }))
                .unwrap(),
            _ => {
                drop(market_tx.take());
            }
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        while !worker.is_finished() && Instant::now() < deadline {
            thread::yield_now();
        }
        assert!(
            worker.is_finished(),
            "quarantine ignored controlled shutdown mode {exit_mode}"
        );
        worker.join().unwrap();
        assert!(dropped.load(Ordering::Acquire));
        assert_eq!(exit_calls.load(Ordering::Acquire), 1);
        assert_eq!((direct_rx.len(), compat_rx.len()), (1, 1));
        assert_eq!(
            health.recovery_delivery_progress(generation),
            Some((true, 1))
        );
    }
}
