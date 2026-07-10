//! Live Polymarket-RTDS chainlink feed verification (ignored by default;
//! read-only, no credentials).
//!
//! Regression test for the one-subscription-per-topic RTDS limitation:
//! subscribing btc+eth+sol must yield SpotPrice events for ALL THREE
//! symbols (the old per-symbol filtered subscribe only ever received the
//! first).
//!
//! ```sh
//! cargo test -p hexagent-exchange --test chainlink_rtds_mainnet -- --ignored --nocapture
//! ```

use std::collections::HashMap;
use std::time::{Duration, Instant};

use hexagent_exchange::exchange::chainlink::ChainlinkMarket;
use hexagent_exchange::exchange::ExchangeMarket;
use hexagent_exchange::types::MarketEvent;

#[test]
#[ignore = "hits live Polymarket RTDS WS"]
fn rtds_multi_symbol_subscription_receives_all() {
    let _ = hexagent_exchange::async_rt::init();
    let mut feed = ChainlinkMarket::new();
    feed.subscribe(&["btc/usd".to_string(), "eth/usd".to_string(), "sol/usd".to_string()])
        .unwrap();
    feed.connect().unwrap();

    let mut counts: HashMap<String, u32> = HashMap::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        match feed.next_event() {
            Ok(Some(MarketEvent::SpotPrice(sp))) => {
                assert!(sp.price > 0.0);
                *counts.entry(sp.symbol).or_default() += 1;
            }
            Ok(_) => {}
            Err(e) => panic!("feed died: {e}"),
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    feed.disconnect();

    println!("30s spot-price counts: {counts:?}");
    for sym in ["btc/usd", "eth/usd", "sol/usd"] {
        assert!(
            counts.get(sym).copied().unwrap_or(0) >= 5,
            "no/too few pushes for {sym}: {counts:?}"
        );
    }
    // Client-side filter must still drop unsubscribed topic symbols
    // (xrp/doge/bnb/hype stream on the unfiltered topic).
    assert_eq!(counts.len(), 3, "unsubscribed symbols leaked through: {counts:?}");
}
