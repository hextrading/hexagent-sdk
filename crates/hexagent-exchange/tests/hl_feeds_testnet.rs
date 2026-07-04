//! Live Hyperliquid **testnet** feed verification (ignored by default).
//!
//! Verifies the market-data feed (l2Book orderbook + trades) and the user feed
//! (userFills fill pushes + orderUpdates order-status pushes) end-to-end against
//! the real testnet exchange. Requires a funded testnet account + agent key:
//!
//! ```sh
//! HL_TEST_ACCOUNT=0x… HL_TEST_KEY=0x… \
//!   cargo test -p hexagent-exchange --test hl_feeds_testnet -- --ignored --nocapture
//! ```

use std::time::{Duration, Instant};

use hexagent_exchange::exchange::hyperliquid::{auth::HlAuth, info, HyperliquidMarket, HyperliquidTrade};
use hexagent_exchange::exchange::{ExchangeMarket, ExchangeTrade};
use hexagent_exchange::types::{Exchange, MarketEvent, OrderRequest, OrderType, Side};

const COIN: &str = "BTC";
const WS: &str = "wss://api.hyperliquid-testnet.xyz/ws";
const SZ: f64 = 0.0002;

fn creds() -> (String, String) {
    (
        std::env::var("HL_TEST_ACCOUNT").expect("set HL_TEST_ACCOUNT"),
        std::env::var("HL_TEST_KEY").expect("set HL_TEST_KEY"),
    )
}

fn build_trade() -> HyperliquidTrade {
    let (account, key) = creds();
    let auth = HlAuth::new(&key, &account, "testnet", "", "").expect("auth");
    let meta = info::fetch_meta(&auth.info_url()).expect("meta");
    HyperliquidTrade::new(auth, meta, "feedtest")
}

fn ioc(side: Side, px: f64) -> OrderRequest {
    let mut o = OrderRequest::new_limit(Exchange::Hyperliquid, COIN.to_string(), side, px, SZ);
    o.order_type = OrderType::Fak;
    o.post_only = false;
    o.price = Some(px);
    o.instance_id = "feedtest".to_string();
    o
}

// ── Market feed: l2Book orderbook + trades ──────────────────────────────────
#[test]
#[ignore]
fn testnet_market_feed() {
    let _ = hexagent_exchange::async_rt::init();
    let mut mkt = HyperliquidMarket::new(WS);
    mkt.subscribe(&[COIN.to_string()]).expect("subscribe");
    mkt.connect().expect("connect");
    std::thread::sleep(Duration::from_secs(2)); // connect + first snapshot

    // Generate a couple of public trades on our own account so the trades feed
    // has something to deliver even if testnet is quiet.
    let mut tr = build_trade();
    let b = info::fetch_l2_book(&HlAuth::new(&creds().1, &creds().0, "testnet", "", "").unwrap().info_url(), COIN).unwrap();
    let ask = b.levels[1].first().unwrap().px.parse::<f64>().unwrap();
    let _ = tr.submit_order(&ioc(Side::Buy, ask * 1.02)); // fill → trade
    let mut mkt_sell = OrderRequest::new_market(Exchange::Hyperliquid, COIN.to_string(), Side::Sell, SZ);
    mkt_sell.instance_id = "feedtest".to_string();
    let _ = tr.submit_order(&mkt_sell); // flatten → trade

    let (mut ob, mut td) = (0u32, 0u32);
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(10) {
        match mkt.next_event() {
            Ok(Some(MarketEvent::OrderBook(o))) if o.exchange == Exchange::Hyperliquid => ob += 1,
            Ok(Some(MarketEvent::Trade(t))) if t.exchange == Exchange::Hyperliquid => td += 1,
            Ok(Some(_)) => {}
            Ok(None) => std::thread::sleep(Duration::from_millis(30)),
            Err(e) => panic!("market feed error: {}", e),
        }
    }
    mkt.disconnect();
    let _ = tr.cancel_all(Exchange::Hyperliquid, COIN);
    println!("[market feed] OrderBook events={} Trade events={}", ob, td);
    assert!(ob > 0, "l2Book orderbook feed must deliver events");
    assert!(td > 0, "trades feed must deliver events");
    println!("MARKET FEED OK (orderbook + trades)");
}

// ── User feed: userFills (fill push) + orderUpdates (order-status push) ──────
#[test]
#[ignore]
fn testnet_user_feed() {
    let _ = hexagent_exchange::async_rt::init();
    let (account, _key) = creds();
    let (rx, _shutdown) =
        hexagent_exchange::exchange::hyperliquid::user_feed::spawn_user_feed(WS, &account);
    std::thread::sleep(Duration::from_secs(2)); // connect + snapshot
    while rx.try_recv().is_ok() {} // drain snapshot

    let mut tr = build_trade();
    let info_url = HlAuth::new(&creds().1, &account, "testnet", "", "").unwrap().info_url();
    let b = info::fetch_l2_book(&info_url, COIN).unwrap();
    let bid = b.levels[0].first().unwrap().px.parse::<f64>().unwrap();
    let ask = b.levels[1].first().unwrap().px.parse::<f64>().unwrap();

    // Resting post-only → orderUpdates "open" (status push, no fill).
    let mut rest = OrderRequest::new_limit(Exchange::Hyperliquid, COIN.to_string(), Side::Buy, bid * 0.99, SZ);
    rest.order_type = OrderType::LimitMaker;
    rest.instance_id = "feedtest".to_string();
    tr.submit_order(&rest).expect("rest submit");
    // IOC fill → userFills (fill push) + orderUpdates "filled".
    tr.submit_order(&ioc(Side::Buy, ask * 1.02)).expect("ioc submit");
    // Cancel resting → orderUpdates "canceled".
    let _ = tr.cancel_order(Exchange::Hyperliquid, &rest.client_order_id);
    // Market sell to flatten the IOC buy.
    let mut mkt_sell = OrderRequest::new_market(Exchange::Hyperliquid, COIN.to_string(), Side::Sell, SZ);
    mkt_sell.instance_id = "feedtest".to_string();
    tr.submit_order(&mkt_sell).expect("market submit");

    let (mut fills, mut statuses) = (0u32, 0u32);
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(6) {
        while let Ok(u) = rx.try_recv() {
            if u.trade_id.is_some() {
                fills += 1;
                println!("[user feed] FILL side={:?} sz={} @ {}", u.side, u.filled_quantity, u.avg_fill_price);
            } else {
                statuses += 1;
                println!("[user feed] ORDER-STATUS {:?} coid={} oid={:?}", u.status, u.client_order_id, u.exchange_order_id);
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let _ = tr.cancel_all(Exchange::Hyperliquid, COIN);
    println!("[user feed] fills(userFills)={} order-status(orderUpdates)={}", fills, statuses);
    assert!(fills > 0, "userFills fill push must arrive");
    assert!(statuses > 0, "orderUpdates order-status push must arrive");
    println!("USER FEED OK (fill pushes + order-status pushes)");
}
