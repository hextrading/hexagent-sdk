//! Live Hyperliquid **MAINNET** place/cancel reachability smoke (ignored).
//!
//! Sends ONE tiny post-only order far below market (cannot fill), then cancels
//! if it rested. Verifies the mainnet signing / agent-approval / venue path.
//! With an unfunded perp wallet the place is expected to be rejected for
//! insufficient margin — which still proves signing + venue reachability.
//!
//! ```sh
//! HL_MAIN_ACCOUNT=0x… HL_MAIN_KEY=0x… \
//!   cargo test -p hexagent-exchange --test hl_mainnet_smoke -- --ignored --nocapture
//! ```

use hexagent_exchange::exchange::hyperliquid::{auth::HlAuth, info, HyperliquidTrade};
use hexagent_exchange::exchange::ExchangeTrade;
use hexagent_exchange::types::{Exchange, OrderRequest, OrderStatus, OrderType, Side};

#[test]
#[ignore]
fn mainnet_place_cancel_smoke() {
    let account = std::env::var("HL_MAIN_ACCOUNT").expect("set HL_MAIN_ACCOUNT");
    let key = std::env::var("HL_MAIN_KEY").expect("set HL_MAIN_KEY");
    let _ = hexagent_exchange::async_rt::init();

    let auth = HlAuth::new(&key, &account, "mainnet", "", "").expect("auth");
    println!("[mainnet] info_url={} account={} signer={}", auth.info_url(), auth.account_address, auth.signer_address);
    let meta = info::fetch_meta(&auth.info_url()).expect("meta");
    let mut tr = HyperliquidTrade::new(auth.clone(), meta, "mainsmoke");

    // ── 1. Read account balance (unified USDC, not accountValue) ──
    let bal = hexagent_exchange::exchange::hyperliquid::fetch_balance(&auth.info_url(), &auth.account_address)
        .expect("fetch_balance");
    println!("[mainnet] available USDC balance = {:.4}", bal);
    assert!(bal > 0.0, "account must have unified USDC to place an order");

    let book = info::fetch_l2_book(&auth.info_url(), "BTC").expect("l2Book");
    let bid = book.levels[0].first().unwrap().px.parse::<f64>().unwrap();
    let ask = book.levels[1].first().unwrap().px.parse::<f64>().unwrap();
    println!("[mainnet] BTC book: bid={} ask={}", bid, ask);

    // Post-only BUY 10% below the bid: rests deep, cannot fill. Size 0.0002
    // (~$20 notional > $10 min). With an unfunded perp wallet this is rejected.
    let px = bid * 0.90;
    let mut o = OrderRequest::new_limit(Exchange::Hyperliquid, "BTC".to_string(), Side::Buy, px, 0.0002);
    o.order_type = OrderType::LimitMaker;
    o.post_only = true;
    o.price = Some(px);
    o.instance_id = "mainsmoke".to_string();

    let u = tr.submit_order(&o).expect("submit reached the venue");
    println!("[mainnet] PLACE post-only @ {:.1} -> {:?} oid={:?} err={:?}", px, u.status, u.exchange_order_id, u.error);

    match u.status {
        OrderStatus::Accepted => {
            println!("[mainnet] order RESTED — perp wallet is funded; cancelling…");
            let c = tr.cancel_order(Exchange::Hyperliquid, &o.client_order_id).expect("cancel");
            println!("[mainnet] CANCEL -> {:?}", c.status);
            assert_eq!(c.status, OrderStatus::Cancelled, "cancel must succeed");
            // Safety net.
            let _ = tr.cancel_all(Exchange::Hyperliquid, "BTC");
            println!("MAINNET PLACE+CANCEL OK (real resting order placed and cancelled)");
        }
        OrderStatus::Rejected => {
            // Business rejection (e.g. insufficient margin) still proves the
            // signing / agent-approval / venue path works end-to-end.
            println!("MAINNET REACHABILITY OK — signing/agent/venue work; place rejected: {:?}", u.error);
        }
        other => panic!("unexpected mainnet place status: {:?} err={:?}", other, u.error),
    }
}
