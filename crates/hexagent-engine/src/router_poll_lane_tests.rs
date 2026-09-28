use super::*;

struct LifecycleProbe {
    iid: &'static str,
    trace: Sender<(&'static str, u64)>,
}

impl Strategy for LifecycleProbe {
    fn name(&self) -> &str {
        "root-poll-test"
    }
    fn instance_id(&self) -> &str {
        self.iid
    }
    fn subscribed_symbols(&self) -> Vec<String> {
        vec!["btc/usd".into()]
    }
    fn on_shutdown(&mut self) -> Vec<Signal> {
        self.trace.try_send((self.iid, u64::MAX)).unwrap();
        Vec::new()
    }
    fn on_spot_price(&mut self, _: &SpotPrice) {
        self.trace.try_send((self.iid, 0)).unwrap();
    }
    fn on_order_update(
        &mut self,
        update: &OrderUpdate,
    ) -> Result<SignalBatch, SignalBatchOverflow> {
        self.trace
            .try_send((self.iid, update.timestamp_ns))
            .unwrap();
        Ok(SignalBatch::new())
    }
}

#[test]
fn live_root_drains_execution_before_market_preserves_replay_and_exits() {
    run_root_poll_lanes(false, false);
}

#[test]
fn live_private_recovery_poll_lane_preserves_owner_replay_and_priority() {
    run_root_poll_lanes(true, false);
}

#[test]
fn early_duplicate_completion_is_retained_until_router_enters_shutdown() {
    run_root_poll_lanes(true, true);
}

fn run_root_poll_lanes(with_recovery: bool, early_completion: bool) {
    let config: Config = toml::from_str("[general]\nmode = 'live'\n").unwrap();
    let engine = Engine::new(config, StrategyRegistry::new());
    let (trace_tx, trace_rx) = bounded(16);
    let strategies: Vec<Box<dyn Strategy>> = ["owner0", "owner1"]
        .into_iter()
        .map(|iid| {
            Box::new(LifecycleProbe {
                iid,
                trace: trace_tx.clone(),
            }) as Box<dyn Strategy>
        })
        .collect();
    let (market_tx, market_rx) = crate::exchange::market_event_channel(16);
    let private_rx = crossbeam_channel::never();
    let (private_tx, private_poll_rx) = hexagent_runtime::poll_channel::bounded(16);
    let (root_tx, root_rx) = hexagent_runtime::poll_channel::bounded(16);
    // Two owners interleaved; a duplicate timestamp deliberately represents
    // replay. Transport must not invent deduplication or change ownership.
    let mut tail_template = None;
    for (owner, timestamp_ns) in [(1, 11), (0, 21), (1, 11), (0, 22), (1, 12)] {
        let routed = RoutedOrderUpdate {
            owner,
            update: OrderUpdate {
                order_slot: Default::default(),
                client_order_id: "opaque".into(),
                exchange: Exchange::Hexmarket,
                symbol: "market".into(),
                side: Side::Buy,
                exchange_order_id: Some("oid".into()),
                status: OrderStatus::Accepted,
                liquidity: None,
                filled_quantity: 0.0,
                remaining_quantity: 1.0,
                avg_fill_price: 0.0,
                timestamp_ns,
                exchange_event_timestamp_ns: None,
                trade_id: None,
                trade_fee: None,
                order_audit: None,
                error: None,
            },
            timing: LifecycleTiming::default(),
        };
        if with_recovery {
            let mut replay = routed.clone();
            replay.update.timestamp_ns += 100;
            private_tx.send(replay).unwrap();
        }
        tail_template = Some(routed.clone());
        root_tx.send(routed).unwrap();
    }
    crate::exchange::publish_market_event(
        &market_tx,
        MarketEvent::SpotPrice(SpotPrice {
            source: "chainlink".into(),
            symbol: "btc/usd".into(),
            price: 100.0,
            timestamp_ns: now_ns(),
            local_timestamp_ns: now_ns(),
        }),
    )
    .unwrap();
    let (signal_tx, signal_rx) = bounded(16);
    let (done_tx, done_rx) = completion_lane();
    let router = engine.spawn_per_instance_strategy_threads(
        strategies,
        market_rx,
        SignalSender::system(signal_tx),
        private_rx,
        Some(private_poll_rx),
        Some(root_rx),
        None,
        Vec::new(),
        Some(done_rx),
        &HashMap::new(),
        HashMap::new(),
    );
    let mut traces = HashMap::<&str, Vec<u64>>::new();
    for _ in 0..(if with_recovery { 12 } else { 7 }) {
        let (iid, event) = trace_rx
            .recv_timeout(std::time::Duration::from_secs(3))
            .unwrap_or_else(|error| panic!("{error:?}, received={traces:?}"));
        traces.entry(iid).or_default().push(event);
    }
    if with_recovery {
        assert_eq!(traces["owner0"], [121, 122, 21, 22, 0]);
        assert_eq!(traces["owner1"], [111, 111, 112, 11, 11, 12, 0]);
        assert_eq!(market_tx.consumer_progress().private_pending_high_water, 5);
    } else {
        assert_eq!(traces["owner0"], [21, 22, 0]);
        assert_eq!(market_tx.consumer_progress().private_pending_high_water, 0);
        assert_eq!(traces["owner1"], [11, 11, 12, 0]);
    }
    assert_eq!(market_tx.consumer_progress().executor_pending_high_water, 5);
    if early_completion {
        // Supervisor can finish cancellation before the market Exit reaches
        // the router. No event may disappear before shutdown_in_progress.
        for _ in 0..100 { done_tx.publish(); }
    }
    crate::exchange::publish_market_event(&market_tx, MarketEvent::Exit).unwrap();
    assert!(matches!(
        signal_rx
            .recv_timeout(std::time::Duration::from_secs(3))
            .unwrap()
            .signal,
        Signal::BeginShutdown
    ));
    if !early_completion {
        // Final completion races with queued lifecycle updates; both owners
        // must apply their exact tail before generating final reports.
        for (owner, timestamp_ns) in [(0, 301), (1, 401)] {
            let mut update = tail_template.as_ref().unwrap().clone();
            update.owner = owner;
            update.update.timestamp_ns = timestamp_ns;
            root_tx.send(update).unwrap();
        }
        for _ in 0..100 { done_tx.publish(); }
    }
    assert!(matches!(
        signal_rx
            .recv_timeout(std::time::Duration::from_secs(3))
            .unwrap()
            .signal,
        Signal::Exit
    ));
    router.join().unwrap();
    let mut tail = HashMap::<&str, Vec<u64>>::new();
    while let Ok((iid, event)) = trace_rx.try_recv() { tail.entry(iid).or_default().push(event); }
    if early_completion {
        assert_eq!(tail["owner0"], [u64::MAX]);
        assert_eq!(tail["owner1"], [u64::MAX]);
    } else {
        assert_eq!(tail["owner0"], [301, u64::MAX]);
        assert_eq!(tail["owner1"], [401, u64::MAX]);
    }
}

/// Exercise the actual root router and three independently owned strategy
/// threads. Ordered public trades are never coalesced; a 1,000-event burst
/// models the 961-record live root backlog. No exchange or orders are used.
#[test]
#[ignore = "manual end-to-end burst benchmark; run release with --ignored --nocapture"]
fn root_router_ordered_burst_benchmark() {
    let schedule: Option<Vec<u64>> = std::env::var("HEXAGENT_ROUTER_BENCH_TRACE").ok().map(|path| {
        std::fs::read_to_string(path).unwrap().lines().map(|line| line.parse().unwrap()).collect()
    });
    let burst = schedule.as_ref().map_or(1000, Vec::len);
    const WAVES: usize = 20;
    struct Probe {
        iid: String,
        samples: Vec<u64>,
        result: Sender<Vec<u64>>,
        count: usize,
        burst: usize,
    }
    impl Strategy for Probe {
        fn name(&self) -> &str { "root-burst-probe" }
        fn instance_id(&self) -> &str { &self.iid }
        fn subscribed_symbols(&self) -> Vec<String> { vec!["BTCUSDT".into()] }
        fn on_trade_tick(&mut self, t: &TradeTick) {
            let age = crate::types::monotonic_now_ns().saturating_sub(t.exchange_timestamp_ns);
            if t.price < 0.0 { self.result.try_send(Vec::new()).unwrap(); return; }
            assert_eq!(t.price as usize, self.count, "ordered trade lost, duplicated or reordered");
            self.count += 1;
            self.samples.push(age);
            if self.samples.len() == self.burst {
                self.result.try_send(std::mem::replace(&mut self.samples, Vec::with_capacity(self.burst))).unwrap();
            }
        }
    }
    let config: Config = toml::from_str("[general]\nmode = 'live'\n").unwrap();
    let engine = Engine::new(config, StrategyRegistry::new());
    let (result_tx, result_rx) = bounded(3);
    let strategies = (0..3).map(|i| Box::new(Probe {
        iid: format!("probe{i}"), samples: Vec::with_capacity(burst),
        result: result_tx.clone(), count: 0, burst,
    }) as Box<dyn Strategy>).collect();
    let (market_tx, market_rx) = crate::exchange::market_event_channel(CHANNEL_CAPACITY);
    let (_private_tx, private_rx) = hexagent_runtime::poll_channel::bounded(16);
    let (_execution_tx, execution_rx) = hexagent_runtime::poll_channel::bounded(16);
    let (signal_tx, _signal_rx) = bounded(16);
    let router = engine.spawn_per_instance_strategy_threads(
        strategies, market_rx, SignalSender::system(signal_tx), crossbeam_channel::never(),
        Some(private_rx), Some(execution_rx), None, Vec::new(), None,
        &HashMap::new(), HashMap::new(),
    );
    let trade = |sequence: f64| MarketEvent::Trade(TradeTick {
        exchange: Exchange::Binance, symbol: "BTCUSDT".into(), exchange_trade_id: None,
        price: sequence, quantity: 1.0, side: Side::Buy,
        exchange_timestamp_ns: crate::types::monotonic_now_ns(), local_timestamp_ns: now_ns(),
    });
    crate::exchange::publish_market_event(&market_tx, trade(-1.0)).unwrap();
    for _ in 0..3 { assert!(result_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap().is_empty()); }
    let mut samples = Vec::with_capacity(WAVES * burst * 3);
    for wave in 0..WAVES {
        // Payload allocation belongs to the producer and is outside its stamp.
        let events: Vec<_> = (0..burst).map(|i| trade((wave * burst + i) as f64)).collect();
        let wave_started = crate::types::monotonic_now_ns();
        for (i, mut event) in events.into_iter().enumerate() {
            if let Some(offsets) = &schedule {
                while crate::types::monotonic_now_ns().saturating_sub(wave_started) < offsets[i] {
                    std::hint::spin_loop();
                }
            }
            if let MarketEvent::Trade(t) = &mut event {
                t.exchange_timestamp_ns = crate::types::monotonic_now_ns();
                t.local_timestamp_ns = now_ns();
            }
            crate::exchange::publish_market_event(&market_tx, event).unwrap();
        }
        for _ in 0..3 { samples.extend(result_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap()); }
    }
    let progress = market_tx.consumer_progress();
    drop(market_tx);
    router.join().unwrap();
    samples.sort_unstable();
    let p = |q| samples[(samples.len() - 1) * q / 1000];
    println!("root_burst boundary=producer_stamp_before_root_enqueue_to_strategy_callback unit=ns events={} input_events={} waves={WAVES} burst={burst} p50={} p99={} p999={} max={} root_pending={} root_high_water={} root_contention_drops={} root_max_poll_gap_ns={} ordered_loss=0 duplicates=0", samples.len(), WAVES * burst,
        p(500), p(990), p(999), samples.last().unwrap(), progress.pending,
        progress.pending_high_water, progress.contention_drops, progress.max_poll_gap_ns);
}
