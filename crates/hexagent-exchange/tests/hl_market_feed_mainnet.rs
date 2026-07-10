//! Live Hyperliquid **mainnet** market-feed verification (ignored by
//! default; read-only, no credentials needed).
//!
//! Verifies the fast-l2Book (~0.5s / 5 levels) + trades + activeAssetCtx
//! subscription end-to-end against the real mainnet WS:
//!
//! ```sh
//! cargo test -p hexagent-exchange --test hl_market_feed_mainnet -- --ignored --nocapture
//! ```

use std::time::{Duration, Instant};

use hexagent_exchange::exchange::hyperliquid::HyperliquidMarket;
use hexagent_exchange::exchange::ExchangeMarket;
use hexagent_exchange::types::MarketEvent;

const WS: &str = "wss://api.hyperliquid.xyz/ws";

#[test]
#[ignore = "hits live mainnet WS"]
fn mainnet_l2book_fast_trades_asset_ctx() {
    let _ = hexagent_exchange::async_rt::init();
    let mut feed = HyperliquidMarket::new(WS);
    feed.subscribe(&["BTC".to_string(), "xyz:SKHX".to_string()]).unwrap();
    feed.connect().unwrap();

    let mut n_ob = 0u32;
    let mut n_trade = 0u32;
    let mut n_ctx = 0u32;
    let mut ob_depth = (0usize, 0usize);
    let mut ob_times: Vec<Instant> = Vec::new();
    let mut ctx_sample: Option<hexagent_exchange::types::AssetCtxTick> = None;

    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        match feed.next_event() {
            Ok(Some(MarketEvent::OrderBook(ob))) => {
                n_ob += 1;
                ob_depth = (ob.bids.len(), ob.asks.len());
                if ob.symbol == "BTC" { ob_times.push(Instant::now()); }
                assert!(ob.best_bid().unwrap().price > 0.0);
                assert!(ob.best_ask().unwrap().price > ob.best_bid().unwrap().price);
            }
            Ok(Some(MarketEvent::Trade(t))) => {
                n_trade += 1;
                assert!(t.price > 0.0 && t.quantity > 0.0);
            }
            Ok(Some(MarketEvent::AssetCtx(ac))) => {
                n_ctx += 1;
                assert!(ac.mark_px > 0.0, "markPx must parse");
                assert!(ac.oracle_px > 0.0, "oraclePx must parse");
                assert!(ac.open_interest > 0.0, "openInterest must parse");
                ctx_sample = Some(ac);
            }
            Ok(_) => {}
            Err(e) => panic!("feed died: {e}"),
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    feed.disconnect();

    println!("15s: orderbooks={n_ob} trades={n_trade} asset_ctx={n_ctx} last_depth={ob_depth:?}");
    if let Some(ac) = &ctx_sample {
        println!(
            "ctx sample: {} mark={} oracle={} funding={} OI={} impact=({},{})",
            ac.symbol, ac.mark_px, ac.oracle_px, ac.funding, ac.open_interest,
            ac.impact_bid_px, ac.impact_ask_px,
        );
    }

    // fast l2Book: ~0.5s cadence → expect ≥15 BTC books in 15s (default
    // throttled feed would give ~3); depth capped at 5 levels.
    assert!(ob_times.len() >= 15, "fast l2Book not active? BTC books={}", ob_times.len());
    assert!(ob_depth.0 <= 5 && ob_depth.1 <= 5, "fast mode is 5 levels, got {ob_depth:?}");
    assert!(n_ctx >= 10, "activeAssetCtx ~1/s per coin, 2 coins × 15s: got {n_ctx}");
    assert!(n_trade > 0, "BTC should print trades in 15s");
}
