//! Live Lighter **mainnet public** feed verification (ignored by default).
//!
//! Read-only: verifies orderBookDetails metadata, the WS order_book local
//! book maintenance and the trade tick stream against the real venue — no
//! credentials, no orders.
//!
//! ```sh
//! cargo test -p hexagent-exchange --test lighter_feeds_public -- --ignored --nocapture
//! ```

use std::time::{Duration, Instant};

use hexagent_exchange::exchange::lighter::{info, LighterMarket};
use hexagent_exchange::exchange::ExchangeMarket;
use hexagent_exchange::types::MarketEvent;

const SYMBOL: &str = "BTC";
const REST: &str = "https://mainnet.zklighter.elliot.ai";
const WS: &str = "wss://mainnet.zklighter.elliot.ai/stream";

#[test]
#[ignore]
fn mainnet_meta() {
    let _ = hexagent_exchange::async_rt::init();
    let meta = info::fetch_meta(REST).expect("orderBookDetails");
    let btc = meta.market(SYMBOL).expect("BTC market");
    println!(
        "BTC market_id={} price_decimals={} size_decimals={} min_base={} min_quote={}",
        btc.market_id, btc.price_decimals, btc.size_decimals, btc.min_base_amount, btc.min_quote_amount
    );
    assert_eq!(btc.market_id, 1);
    assert!(btc.price_decimals >= 1 && btc.size_decimals >= 1);
    assert_eq!(meta.symbol_for(1), Some("BTC"));

    let nonce = info::fetch_next_nonce(REST, 1, 0).expect("nextNonce");
    println!("nextNonce(account=1,key=0) = {}", nonce);
    assert!(nonce >= 0);
}

#[test]
#[ignore]
fn mainnet_market_feed() {
    let _ = hexagent_exchange::async_rt::init();
    let meta = info::fetch_meta(REST).expect("orderBookDetails");
    let mut mkt = LighterMarket::new(WS, meta);
    mkt.subscribe(&[SYMBOL.to_string()]).expect("subscribe");
    mkt.connect().expect("connect");

    let deadline = Instant::now() + Duration::from_secs(30);
    let (mut books, mut trades) = (0u32, 0u32);
    let mut last_bid = 0.0f64;
    let mut last_ask = 0.0f64;
    while Instant::now() < deadline && (books < 50 || trades < 1) {
        match mkt.next_event() {
            Ok(Some(MarketEvent::OrderBook(ob))) => {
                assert_eq!(ob.symbol, SYMBOL);
                let bid = ob.bids.first().map(|l| l.price).unwrap_or(0.0);
                let ask = ob.asks.first().map(|l| l.price).unwrap_or(0.0);
                assert!(bid > 0.0 && ask > bid, "crossed/empty book: bid={} ask={}", bid, ask);
                // Sanity: BTC perp in a plausible price range and tight spread.
                assert!(bid > 1_000.0 && ask < 10_000_000.0);
                assert!((ask - bid) / bid < 0.05, "spread implausibly wide");
                last_bid = bid;
                last_ask = ask;
                books += 1;
            }
            Ok(Some(MarketEvent::Trade(t))) => {
                assert_eq!(t.symbol, SYMBOL);
                assert!(t.price > 0.0 && t.quantity > 0.0);
                trades += 1;
            }
            Ok(Some(_)) => {}
            Ok(None) => std::thread::sleep(Duration::from_millis(20)),
            Err(e) => panic!("feed error: {}", e),
        }
    }
    println!("got {} book updates, {} trades; last touch {:.1}/{:.1}", books, trades, last_bid, last_ask);
    mkt.disconnect();
    assert!(books >= 50, "expected a stream of book updates, got {}", books);
    assert!(trades >= 1, "expected at least one trade tick, got {}", trades);
}
