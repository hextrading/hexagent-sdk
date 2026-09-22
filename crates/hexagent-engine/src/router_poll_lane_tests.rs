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
    run_root_poll_lanes(false);
}

#[test]
fn live_private_recovery_poll_lane_preserves_owner_replay_and_priority() {
    run_root_poll_lanes(true);
}

fn run_root_poll_lanes(with_recovery: bool) {
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
    let (done_tx, done_rx) = bounded(1);
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
    crate::exchange::publish_market_event(&market_tx, MarketEvent::Exit).unwrap();
    assert!(matches!(
        signal_rx
            .recv_timeout(std::time::Duration::from_secs(3))
            .unwrap()
            .signal,
        Signal::BeginShutdown
    ));
    done_tx.send(()).unwrap();
    assert!(matches!(
        signal_rx
            .recv_timeout(std::time::Duration::from_secs(3))
            .unwrap()
            .signal,
        Signal::Exit
    ));
    router.join().unwrap();
}
