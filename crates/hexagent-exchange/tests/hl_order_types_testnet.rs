//! Live Hyperliquid **testnet** order-type verification (ignored by default).
//!
//! Exercises post-only (Alo), IOC, FAK, and market orders end-to-end against
//! the real testnet exchange, asserting the resulting `OrderStatus`. Requires a
//! funded testnet account + agent key via env:
//!
//! ```sh
//! HL_TEST_ACCOUNT=0x… HL_TEST_KEY=0x… \
//!   cargo test -p hexagent-exchange --test hl_order_types_testnet -- --ignored --nocapture
//! ```
//!
//! Net effect is flat: the IOC buy is offset by a market sell; the resting
//! post-only order is cancelled; the crossing post-only order is rejected.

use hexagent_exchange::exchange::hyperliquid::{auth::HlAuth, info, HyperliquidTrade};
use hexagent_exchange::exchange::ExchangeTrade;
use hexagent_exchange::types::{Exchange, OrderRequest, OrderStatus, OrderType, Side};

const COIN: &str = "BTC";
const SZ: f64 = 0.0002; // ~$12 notional — above the $10 min, tiny

fn touch(info_url: &str) -> (f64, f64) {
    let b = info::fetch_l2_book(info_url, COIN).expect("l2Book");
    let bid = b.levels[0].first().unwrap().px.parse::<f64>().unwrap();
    let ask = b.levels[1].first().unwrap().px.parse::<f64>().unwrap();
    (bid, ask)
}

fn req(side: Side, ty: OrderType, post_only: bool, price: Option<f64>) -> OrderRequest {
    let mut o = OrderRequest::new_limit(Exchange::Hyperliquid, COIN.to_string(), side, price.unwrap_or(0.0), SZ);
    o.order_type = ty;
    o.post_only = post_only;
    o.price = price;
    o.instance_id = "otest".to_string();
    o
}

#[test]
#[ignore]
fn testnet_order_types() {
    let account = std::env::var("HL_TEST_ACCOUNT").expect("set HL_TEST_ACCOUNT");
    let key = std::env::var("HL_TEST_KEY").expect("set HL_TEST_KEY");
    let _ = hexagent_exchange::async_rt::init();

    let auth = HlAuth::new(&key, &account, "testnet", "", "").expect("auth");
    let info_url = auth.info_url();
    let meta = info::fetch_meta(&info_url).expect("meta");
    let mut tr = HyperliquidTrade::new(auth, meta, "otest");

    // ── 1. post-only (Alo) resting: buy 1% below the bid → rests (Accepted) ──
    let (bid, ask) = touch(&info_url);
    let rest = req(Side::Buy, OrderType::LimitMaker, true, Some(bid * 0.99));
    let u = tr.submit_order(&rest).expect("post-only submit");
    println!("[1] post-only rest  -> {:?} oid={:?}", u.status, u.exchange_order_id);
    assert_eq!(u.status, OrderStatus::Accepted, "post-only below market must rest");
    let c = tr.cancel_order(Exchange::Hyperliquid, &rest.client_order_id).expect("cancel");
    println!("    cancel resting  -> {:?}", c.status);
    assert_eq!(c.status, OrderStatus::Cancelled);

    // ── 2. post-only (Alo) crossing: buy above the ask → Rejected (would take) ──
    let cross = req(Side::Buy, OrderType::LimitMaker, true, Some(ask * 1.001));
    let u = tr.submit_order(&cross).expect("post-only cross submit");
    println!("[2] post-only cross -> {:?} err={:?}", u.status, u.error);
    assert_eq!(u.status, OrderStatus::Rejected, "crossing post-only must be rejected");

    // ── 3. IOC / FAK: aggressive buy 2% through the ask → Filled ──
    // (FAK maps to the same HL `Ioc` tif; verified via OrderType::Fak.)
    let ioc = req(Side::Buy, OrderType::Fak, false, Some(ask * 1.02));
    let u = tr.submit_order(&ioc).expect("ioc submit");
    println!("[3] IOC/FAK buy     -> {:?} filled={} @ {}", u.status, u.filled_quantity, u.avg_fill_price);
    assert_eq!(u.status, OrderStatus::Filled, "aggressive IOC must fill");

    // ── 4. Market: price unset → aggressive IOC. Sell to flatten the IOC buy ──
    let mut mkt = OrderRequest::new_market(Exchange::Hyperliquid, COIN.to_string(), Side::Sell, SZ);
    mkt.instance_id = "otest".to_string();
    let u = tr.submit_order(&mkt).expect("market submit");
    println!("[4] market sell     -> {:?} filled={} @ {}", u.status, u.filled_quantity, u.avg_fill_price);
    assert_eq!(u.status, OrderStatus::Filled, "market order must fill");

    // Safety net: cancel anything still resting under this coin.
    let _ = tr.cancel_all(Exchange::Hyperliquid, COIN);
    println!("ALL ORDER TYPES OK (post-only rest+reject, IOC/FAK fill, market fill)");
}
