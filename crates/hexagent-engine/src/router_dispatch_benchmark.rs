//! Frozen pre-change router reference and focused offline A/B. Test-only.
use super::*;
    fn baseline_route(
        event: Arc<MarketEvent>,
        sym_to_instances: &HashMap<SymbolId, RouteMask>,
        token_to_instances: &mut HashMap<SymbolId, RouteMask>,
        latest_key_ids: &mut LatestMarketKeyRegistry,
        market_lanes: &[MarketEventLane],
    ) -> RouteMask {
        let send_to = |mut mask: RouteMask, latest_key: Option<LatestMarketKeyHandle>| {
            let mut dropped = 0u64;
            while mask != 0 {
                let i = mask.trailing_zeros() as usize;
                mask &= mask - 1;
                if let Some(lane) = market_lanes.get(i) {
                    if !enqueue_market_event(lane, Arc::clone(&event), latest_key) {
                        dropped |= 1u64 << i;
                    }
                }
            }
            dropped
        };

        // EventEnd is an ordered router-control message. The router is the
        // sole writer of both maps and the free-list. Retire every dynamic
        // Polymarket token before the following EventStart/new books, discard
        // pending payloads for the old generation, then make the IDs reusable.
        if let MarketEvent::EventEnd {
            exchange: Exchange::Polymarket,
            retired_symbols,
            ..
        } = event.as_ref()
        {
            for lane in market_lanes {
                lane.barrier_epoch.fetch_add(1, Ordering::Relaxed);
            }
            for symbol in retired_symbols {
                let symbol_id = SymbolId::of(symbol);
                token_to_instances.remove(&symbol_id);
                for kind in [0, 1] {
                    let key = LatestMarketKey {
                        kind,
                        exchange: Exchange::Polymarket,
                        symbol: symbol_id,
                    };
                    if let Some(handle) = latest_key_ids.remove(&key) {
                        retire_latest_market_key(handle, market_lanes);
                        latest_key_ids.release(handle);
                    }
                }
            }
            return 0;
        }

        // Instrument(BinaryOption) → attribute its token_ids to the owner
        // instance(s) of its slug, then deliver to those owners.
        if let MarketEvent::Instrument(Instrument::BinaryOption(bo)) = event.as_ref() {
            let route_symbol = if bo.series_slug.trim().is_empty() {
                bo.slug.as_str()
            } else {
                bo.series_slug.as_str()
            };
            let route_key = SymbolId::of(route_symbol);
            if let Some(&owners) = sym_to_instances.get(&route_key) {
                for tok in &bo.clob_token_ids {
                    token_to_instances.insert(SymbolId::of(tok), owners);
                }
                return send_to(owners, None);
            } else {
                warn!(
                    "[strategy_router] dropping unroutable Polymarket instrument series={} event_slug={}",
                    route_symbol, bo.slug,
                );
                return 0;
            }
        }

        let all_instances = match market_lanes.len() {
            64 => u64::MAX,
            len => (1u64 << len) - 1,
        };
        let targets: RouteMask = match event.as_ref() {
            // Lifecycle / spot-instrument → all instances.
            MarketEvent::Connected { .. } | MarketEvent::Disconnected { .. } => all_instances,
            MarketEvent::Instrument(Instrument::Spot(spot)) => sym_to_instances
                .get(&SymbolId::of(&spot.symbol))
                .copied()
                .unwrap_or_default(),
            // Polymarket market data keyed by dynamic token_id.
            MarketEvent::OrderBook(ob) if ob.exchange == Exchange::Polymarket => token_to_instances
                .get(&SymbolId::of(&ob.symbol))
                .copied()
                .unwrap_or_default(),
            MarketEvent::Trade(t) if t.exchange == Exchange::Polymarket => token_to_instances
                .get(&SymbolId::of(&t.symbol))
                .copied()
                .unwrap_or_default(),
            MarketEvent::Quote(q) if q.exchange == Exchange::Polymarket => token_to_instances
                .get(&SymbolId::of(&q.symbol))
                .copied()
                .unwrap_or_default(),
            MarketEvent::TickSizeChange(tsc) => token_to_instances
                .get(&SymbolId::of(&tsc.symbol))
                .copied()
                .or_else(|| sym_to_instances.get(&SymbolId::of(&tsc.symbol)).copied())
                .unwrap_or_default(),
            // Spot venues keyed by stable symbol.
            MarketEvent::OrderBook(ob) => sym_to_instances
                .get(&SymbolId::of(&ob.symbol))
                .copied()
                .unwrap_or_default(),
            MarketEvent::Trade(t) => sym_to_instances
                .get(&SymbolId::of(&t.symbol))
                .copied()
                .unwrap_or_default(),
            MarketEvent::Quote(q) => sym_to_instances
                .get(&SymbolId::of(&q.symbol))
                .copied()
                .unwrap_or_default(),
            MarketEvent::Bar(b) => sym_to_instances
                .get(&SymbolId::of(&b.symbol))
                .copied()
                .unwrap_or_default(),
            MarketEvent::SpotPrice(sp) => sym_to_instances
                .get(&SymbolId::of(&sp.symbol))
                .copied()
                .unwrap_or_default(),
            MarketEvent::AssetCtx(ac) => sym_to_instances
                .get(&SymbolId::of(&ac.symbol))
                .copied()
                .unwrap_or_default(),
            MarketEvent::MarketDataHealth(health) => token_to_instances
                .get(&SymbolId::of(&health.symbol))
                .copied()
                .or_else(|| sym_to_instances.get(&SymbolId::of(&health.symbol)).copied())
                .unwrap_or_default(),
            MarketEvent::EventStart { symbol, .. } => sym_to_instances
                .get(&SymbolId::of(symbol))
                .copied()
                .unwrap_or_default(),
            MarketEvent::EventEnd { .. } | MarketEvent::Exit => 0,
            MarketEvent::Instrument(Instrument::BinaryOption(_)) => unreachable!(),
        };
        if targets == 0 {
            return 0;
        }
        // Register coverable keys only after routing resolved at least one
        // owner. Unknown dynamic token traffic must not consume fixed slots.
        let latest_key =
            latest_market_key(event.as_ref()).and_then(|key| latest_key_ids.get_or_register(key));
        send_to(targets, latest_key)
    }
    fn ob(exchange: Exchange, symbol: &str) -> MarketEvent {
        MarketEvent::OrderBook(OrderBookSnapshot { receipt: Default::default(),
            exchange,
            symbol: symbol.into(),
            bids: vec![],
            asks: vec![],
            exchange_timestamp_ns: 1,
            local_timestamp_ns: 1,
        })
    }
    fn quote(exchange: Exchange, symbol: &str) -> MarketEvent {
        MarketEvent::Quote(QuoteTick {
            delivery: Default::default(),
            exchange,
            symbol: symbol.into(),
            bid_price: 0.4,
            bid_qty: 1.0,
            ask_price: 0.6,
            ask_qty: 1.0,
            exchange_timestamp_ns: 1,
            local_timestamp_ns: 1,
        })
    }
    fn trade(exchange: Exchange, symbol: &str) -> MarketEvent {
        MarketEvent::Trade(TradeTick {
            exchange,
            symbol: symbol.into(),
            exchange_trade_id: None,
            price: 0.5,
            quantity: 1.0,
            side: Side::Buy,
            exchange_timestamp_ns: 1,
            local_timestamp_ns: 1,
        })
    }
    fn spot(symbol: &str) -> MarketEvent {
        MarketEvent::SpotPrice(SpotPrice {
            source: "chainlink".into(),
            symbol: symbol.into(),
            price: 1.0,
            timestamp_ns: 1,
            local_timestamp_ns: 1,
        })
    }
    fn test_market_lane(capacity: usize) -> (MarketEventLane, hexagent_runtime::poll_channel::Receiver<QueuedMarketEvent>) {
        let (tx, rx) = hexagent_runtime::poll_channel::bounded(capacity);
        (
            MarketEventLane {
                tx,
                latest: Arc::new(LatestMarketStore::default()),
                barrier_epoch: Arc::new(AtomicU64::new(0)),
            },
            rx,
        )
    }

#[test]
#[ignore = "focused same-process router A/B, raw samples optional"]
fn router_dispatch_ab() {
    use std::time::Instant;
    let _ = market_queue_monotonic_ns();
    let output = std::env::var_os("ROUTER_BENCH_OUT").map(std::path::PathBuf::from);
    if let Some(dir) = &output { std::fs::create_dir_all(dir).unwrap(); }
    type Route = fn(Arc<MarketEvent>, &HashMap<SymbolId,RouteMask>, &mut HashMap<SymbolId,RouteMask>, &mut LatestMarketKeyRegistry, &[MarketEventLane])->RouteMask;
    let routes: [Route;2] = [baseline_route, Engine::route_market_event];
    let token = "12345678901234567890123456789012345678901234567890123456789012345678901234567";
    let cases = [("token_book",ob(Exchange::Polymarket,token),token),
        ("token_quote",quote(Exchange::Polymarket,token),token),
        ("spot_book",ob(Exchange::Binance,"BTCUSDT"),"BTCUSDT"),
        ("spot_price",spot("BTC/USD"),"BTC/USD"),
        ("trade",trade(Exchange::Polymarket,token),token)];
    for (case,event,symbol) in cases {
        let event=Arc::new(event);
        for fanout in [1,3] {
            let pairs:Vec<_>=(0..fanout).map(|_|test_market_lane(8)).collect();
            let lanes:Vec<_>=pairs.iter().map(|(lane,_)|lane.clone()).collect();
            let sym=HashMap::from([(SymbolId::of(symbol),(1<<fanout)-1)]);
            let mut tokens=sym.clone();
            let mut keys=LatestMarketKeyRegistry::default();
            for turn in 0..6 {
                let order=if turn%2==0 {[0,1]}else{[1,0]};
                for mode in order {
                    let mut samples=Vec::with_capacity(100_000);
                    let mut depth=0;
                    for i in 0..102_000 {
                        let start=Instant::now();
                        let dropped=routes[mode](Arc::clone(&event),&sym,&mut tokens,&mut keys,&lanes);
                        let dispatch=start.elapsed().as_nanos() as u64;
                        assert_eq!(dropped,0);
                        for (lane,rx) in &pairs {
                            depth=depth.max(rx.len());
                            let payload=resolve_market_event(rx.try_recv().unwrap(),&lane.latest).unwrap();
                            assert!(Arc::ptr_eq(&payload.event,&event));
                            std::hint::black_box(payload);
                        }
                        let cycle=start.elapsed().as_nanos() as u64;
                        if i>=2000 { samples.push((dispatch,cycle)); }
                    }
                    if let Some(dir)=&output {
                        use std::io::Write;
                        let mut out=std::io::BufWriter::new(std::fs::File::create(dir.join(format!("{case}-f{fanout}-r{turn}-m{mode}.csv"))).unwrap());
                        writeln!(out,"dispatch,cycle").unwrap();
                        for (a,b) in &samples {writeln!(out,"{a},{b}").unwrap();}
                    }
                    let mut ns:Vec<_>=samples.iter().map(|s|s.0).collect();ns.sort_unstable();
                    println!("case={case} fanout={fanout} round={turn} mode={mode} n={} p50={} p99={} p999={} max={} hwm={depth} overflow=0",ns.len(),ns[50000],ns[98999],ns[99899],ns[99999]);
                }
            }
        }
    }
}
