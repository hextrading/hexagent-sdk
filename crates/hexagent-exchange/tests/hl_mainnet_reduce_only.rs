//! Live Hyperliquid **MAINNET** reduce-only verification (ignored).
//!
//! From a FLAT position, an aggressive reduce-only order must NOT open a
//! position (there is nothing to reduce) — HL caps/cancels it. If reduce-only
//! were ignored it would open a short. Zero-risk: it can't create a position.
//!
//! ```sh
//! HL_MAIN_ACCOUNT=0x… HL_MAIN_KEY=0x… \
//!   cargo test -p hexagent-exchange --test hl_mainnet_reduce_only -- --ignored --nocapture
//! ```

use hexagent_exchange::exchange::hyperliquid::{auth::HlAuth, fetch_positions, info, HyperliquidTrade};
use hexagent_exchange::exchange::ExchangeTrade;
use hexagent_exchange::types::{Exchange, OrderRequest, OrderType, Side};

#[test]
#[ignore]
fn mainnet_reduce_only_from_flat() {
    let account = std::env::var("HL_MAIN_ACCOUNT").expect("set HL_MAIN_ACCOUNT");
    let key = std::env::var("HL_MAIN_KEY").expect("set HL_MAIN_KEY");
    let _ = hexagent_exchange::async_rt::init();
    let auth = HlAuth::new(&key, &account, "mainnet", "", "").expect("auth");
    let info_url = auth.info_url();
    let meta = info::fetch_meta(&info_url).expect("meta");
    let mut tr = HyperliquidTrade::new(auth.clone(), meta, "rotest");

    let net0 = fetch_positions(&info_url, &auth.account_address).ok()
        .and_then(|p| p.get("BTC").map(|x| x.quantity)).unwrap_or(0.0);
    println!("[mainnet] starting BTC position = {}", net0);
    assert!(net0.abs() < 1e-9, "test requires a flat start; got {}", net0);

    let b = info::fetch_l2_book(&info_url, "BTC").unwrap();
    let bid = b.levels[0].first().unwrap().px.parse::<f64>().unwrap();

    // Aggressive reduce-only IOC SELL from flat — would fill (open a short) if
    // reduce-only were NOT honoured.
    let px = bid * 0.995;
    let mut o = OrderRequest::new_limit(Exchange::Hyperliquid, "BTC".to_string(), Side::Sell, px, 0.0002);
    o.order_type = OrderType::Fak;
    o.post_only = false;
    o.reduce_only = true;
    o.price = Some(px);
    o.instance_id = "rotest".to_string();
    let u = tr.submit_order(&o).expect("submit reached venue");
    println!("[mainnet] reduce-only SELL from flat -> {:?} filled={} err={:?}", u.status, u.filled_quantity, u.error);

    std::thread::sleep(std::time::Duration::from_millis(1500));
    let net1 = fetch_positions(&info_url, &auth.account_address).ok()
        .and_then(|p| p.get("BTC").map(|x| x.quantity)).unwrap_or(0.0);
    println!("[mainnet] position after = {}", net1);

    // Safety: if somehow a position opened, close it.
    if net1.abs() > 1e-9 {
        let side = if net1 > 0.0 { Side::Sell } else { Side::Buy };
        let mut c = OrderRequest::new_market(Exchange::Hyperliquid, "BTC".to_string(), side, net1.abs());
        c.instance_id = "rotest".to_string();
        let _ = tr.submit_order(&c);
    }
    assert!(net1.abs() < 1e-9, "reduce-only from flat must NOT open a position, got {}", net1);
    println!("REDUCE-ONLY OK — HL honoured reduce_only (no position opened from flat)");
}
