//! Live Hyperliquid **MAINNET** fill + push verification (ignored, real money).
//!
//! Exercises, on mainnet with the smallest viable size (~$13 notional), a real
//! **fill** and both user-feed pushes: `userFills` (fill push) and
//! `orderUpdates` (order-status push). Self-flattens (IOC buy offset by a market
//! sell; resting order cancelled), and closes any residual as a safety net.
//!
//! ```sh
//! HL_MAIN_ACCOUNT=0x… HL_MAIN_KEY=0x… \
//!   cargo test -p hexagent-exchange --test hl_mainnet_fill -- --ignored --nocapture
//! ```

use std::time::{Duration, Instant};

use hexagent_exchange::exchange::hyperliquid::{auth::HlAuth, info, HyperliquidTrade};
use hexagent_exchange::exchange::ExchangeTrade;
use hexagent_exchange::types::{Exchange, OrderRequest, OrderType, Side};

const COIN: &str = "BTC";
const WS: &str = "wss://api.hyperliquid.xyz/ws";
const SZ: f64 = 0.0002; // ~$13 notional — just above the $10 min

fn mkt(side: Side) -> OrderRequest {
    let mut o = OrderRequest::new_market(Exchange::Hyperliquid, COIN.to_string(), side, SZ);
    o.instance_id = "mainfill".to_string();
    o
}

#[test]
#[ignore]
fn mainnet_fill_and_pushes() {
    let account = std::env::var("HL_MAIN_ACCOUNT").expect("set HL_MAIN_ACCOUNT");
    let key = std::env::var("HL_MAIN_KEY").expect("set HL_MAIN_KEY");
    let _ = hexagent_exchange::async_rt::init();
    let auth = HlAuth::new(&key, &account, "mainnet", "", "").expect("auth");
    let info_url = auth.info_url();
    let meta = info::fetch_meta(&info_url).expect("meta");
    let mut tr = HyperliquidTrade::new(auth.clone(), meta, "mainfill");

    let bal = hexagent_exchange::exchange::hyperliquid::fetch_balance(&info_url, &auth.account_address).unwrap();
    println!("[mainnet] balance USDC={:.4}", bal);

    let (rx, _sd) = hexagent_exchange::exchange::hyperliquid::user_feed::spawn_user_feed(WS, &auth.account_address);
    std::thread::sleep(Duration::from_secs(2));
    while rx.try_recv().is_ok() {} // drain snapshot

    let b = info::fetch_l2_book(&info_url, COIN).unwrap();
    let bid = b.levels[0].first().unwrap().px.parse::<f64>().unwrap();
    let ask = b.levels[1].first().unwrap().px.parse::<f64>().unwrap();
    println!("[mainnet] book bid={} ask={}", bid, ask);

    // Resting post-only far below → orderUpdates "open" (status push, no fill).
    let mut rest = OrderRequest::new_limit(Exchange::Hyperliquid, COIN.to_string(), Side::Buy, bid * 0.90, SZ);
    rest.order_type = OrderType::LimitMaker;
    rest.instance_id = "mainfill".to_string();
    let u = tr.submit_order(&rest).expect("rest submit");
    println!("[mainnet] place resting -> {:?}", u.status);

    // IOC buy crossing the ask → FILL (userFills + orderUpdates "filled").
    let mut ioc = OrderRequest::new_limit(Exchange::Hyperliquid, COIN.to_string(), Side::Buy, ask * 1.005, SZ);
    ioc.order_type = OrderType::Fak;
    ioc.post_only = false;
    ioc.price = Some(ask * 1.005);
    ioc.instance_id = "mainfill".to_string();
    let u = tr.submit_order(&ioc).expect("ioc submit");
    println!("[mainnet] IOC buy -> {:?} filled={} @ {}", u.status, u.filled_quantity, u.avg_fill_price);

    // Cancel the resting → orderUpdates "canceled".
    let _ = tr.cancel_order(Exchange::Hyperliquid, &rest.client_order_id);
    // Market sell → flatten (fills).
    let u = tr.submit_order(&mkt(Side::Sell)).expect("market sell");
    println!("[mainnet] market sell -> {:?} filled={} @ {}", u.status, u.filled_quantity, u.avg_fill_price);

    // Collect pushes.
    let (mut fills, mut statuses) = (0u32, 0u32);
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(6) {
        while let Ok(u) = rx.try_recv() {
            if u.trade_id.is_some() {
                fills += 1;
                println!("[mainnet] FILL {:?} sz={} @ {}", u.side, u.filled_quantity, u.avg_fill_price);
            } else {
                statuses += 1;
                println!("[mainnet] ORDER-STATUS {:?} oid={:?}", u.status, u.exchange_order_id);
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    // Safety net: cancel all + close any residual position.
    let _ = tr.cancel_all(Exchange::Hyperliquid, COIN);
    if let Ok(pos) = hexagent_exchange::exchange::hyperliquid::fetch_positions(&info_url, &auth.account_address) {
        if let Some(p) = pos.get(COIN) {
            if p.quantity.abs() > 1e-9 {
                let side = if p.quantity > 0.0 { Side::Sell } else { Side::Buy };
                println!("[mainnet] residual {:.6} — closing with market {:?}", p.quantity, side);
                let mut c = OrderRequest::new_market(Exchange::Hyperliquid, COIN.to_string(), side, p.quantity.abs());
                c.instance_id = "mainfill".to_string();
                let _ = tr.submit_order(&c);
            }
        }
    }

    println!("[mainnet] fills(userFills)={} order-status(orderUpdates)={}", fills, statuses);
    assert!(fills > 0, "userFills fill push must arrive");
    assert!(statuses > 0, "orderUpdates order-status push must arrive");
    println!("MAINNET FILL + PUSHES OK");
}
