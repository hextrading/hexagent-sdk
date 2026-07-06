//! Live Lighter **MAINNET** fill + user-feed push verification (ignored, real
//! money). Exercises, with the smallest viable size (0.0002 BTC, ~$13
//! notional), a real **fill** and both private user-feed channels:
//! `account_all_trades` (fill push) and `account_all_orders` (order-status
//! push). Self-flattens with a reduce-only IOC, and closes any residual as a
//! safety net so nothing is left resting or open.
//!
//! ```sh
//! LIGHTER_TEST_KEY=0x… LIGHTER_TEST_ACCOUNT=732630 LIGHTER_TEST_APIKEY=4 \
//!   cargo test -p hexagent-exchange --test lighter_mainnet_fill -- --ignored --nocapture
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Deserialize;

use hexagent_exchange::exchange::lighter::{
    auth::{LighterAuth, Network},
    info, position,
    signer::LighterSigner,
    user_feed, LighterTrade,
};
use hexagent_exchange::exchange::ExchangeTrade;
use hexagent_exchange::types::{Exchange, Liquidity, OrderRequest, OrderType, Side};

const SYMBOL: &str = "BTC";
const MARKET_ID: i16 = 1;
const SZ: f64 = 0.0002; // ~$13 notional — just above the $10 min_quote

fn creds() -> (String, i64, u8) {
    (
        std::env::var("LIGHTER_TEST_KEY").expect("set LIGHTER_TEST_KEY"),
        std::env::var("LIGHTER_TEST_ACCOUNT").expect("set LIGHTER_TEST_ACCOUNT").parse().unwrap(),
        std::env::var("LIGHTER_TEST_APIKEY").expect("set LIGHTER_TEST_APIKEY").parse().unwrap(),
    )
}

#[derive(Debug, Deserialize)]
struct Lvl {
    price: String,
}
#[derive(Debug, Deserialize)]
struct Ob {
    #[serde(default)]
    bids: Vec<Lvl>,
    #[serde(default)]
    asks: Vec<Lvl>,
}

fn top_of_book(rest: &str) -> (f64, f64) {
    let ob: Ob = info::get_json(format!(
        "{}/api/v1/orderBookOrders?market_id={}&limit=1",
        rest, MARKET_ID
    ))
    .expect("orderBookOrders");
    (
        ob.bids[0].price.parse().unwrap(),
        ob.asks[0].price.parse().unwrap(),
    )
}

#[test]
#[ignore]
fn mainnet_fill_and_pushes() {
    let (key, account_index, api_key_index) = creds();
    let _ = hexagent_exchange::async_rt::init();
    let net = Network::from_str("mainnet");

    let auth = LighterAuth::new(&key, account_index, api_key_index, "mainnet", "", "").expect("auth");
    let rest = auth.rest_base();
    let ws = auth.ws_url();
    let meta = info::fetch_meta(&rest).expect("orderBookDetails");

    let bal = position::fetch_balance(&rest, account_index).expect("balance");
    println!("[mainnet] balance available={} collateral={}", bal.available_balance, bal.collateral);
    assert!(bal.available_balance > 1.0, "need ~1 USDC free margin for 0.0002 BTC");

    // ── user feed: account_all_trades (fills) + account_all_orders (status) ──
    let signer = Arc::new(
        LighterSigner::new(&key, account_index, api_key_index, net.chain_id()).expect("signer"),
    );
    let mut market_symbols = HashMap::new();
    market_symbols.insert(MARKET_ID, SYMBOL.to_string());
    let (rx, _sd) = user_feed::spawn_user_feed(&ws, signer, market_symbols);
    std::thread::sleep(Duration::from_secs(2));
    while rx.try_recv().is_ok() {} // drain any initial state

    let mut tr = LighterTrade::new(auth, meta.clone(), "mainfill");
    let (bid, ask) = top_of_book(&rest);
    println!("[mainnet] book bid={} ask={}", bid, ask);

    // 1) resting post-only far below → account_all_orders "open" status push.
    let mut rest_o = OrderRequest::new_limit(Exchange::Lighter, SYMBOL.to_string(), Side::Buy, bid * 0.90, SZ);
    rest_o.order_type = OrderType::LimitMaker;
    rest_o.post_only = true;
    rest_o.instance_id = "mainfill".to_string();
    let u = tr.submit_order(&rest_o).expect("rest submit");
    println!("[mainnet] place resting post-only -> {:?}", u.status);

    // 2) IOC buy crossing the ask → FILL (account_all_trades + orders "filled").
    let mut ioc = OrderRequest::new_limit(Exchange::Lighter, SYMBOL.to_string(), Side::Buy, ask * 1.005, SZ);
    ioc.order_type = OrderType::Fak;
    ioc.post_only = false;
    ioc.instance_id = "mainfill".to_string();
    let u = tr.submit_order(&ioc).expect("ioc buy");
    println!("[mainnet] IOC buy -> {:?} (fills arrive async on the feed)", u.status);

    // 3) cancel the resting order → account_all_orders "canceled" status push.
    let _ = tr.cancel_order(Exchange::Lighter, &rest_o.client_order_id);

    // 4) reduce-only IOC sell → flatten the long we just opened.
    std::thread::sleep(Duration::from_millis(1500)); // let the buy settle into a position
    let (bid2, _) = top_of_book(&rest);
    let mut flat = OrderRequest::new_limit(Exchange::Lighter, SYMBOL.to_string(), Side::Sell, bid2 * 0.995, SZ);
    flat.order_type = OrderType::Fak;
    flat.post_only = false;
    flat.reduce_only = true;
    flat.instance_id = "mainfill".to_string();
    let u = tr.submit_order(&flat).expect("reduce-only sell");
    println!("[mainnet] reduce-only IOC sell -> {:?}", u.status);

    // ── collect pushes for a few seconds ──
    let (mut fills, mut statuses) = (0u32, 0u32);
    let (mut buy_fill, mut sell_fill) = (false, false);
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(8) {
        while let Ok(u) = rx.try_recv() {
            if u.trade_id.is_some() {
                fills += 1;
                match u.side {
                    Side::Buy => buy_fill = true,
                    Side::Sell => sell_fill = true,
                }
                let liq = match u.liquidity {
                    Some(Liquidity::Maker) => "maker",
                    Some(Liquidity::Taker) => "taker",
                    None => "?",
                };
                println!(
                    "[mainnet] FILL {:?} sz={} @ {} liq={} coid={} tid={:?}",
                    u.side, u.filled_quantity, u.avg_fill_price, liq, u.client_order_id, u.trade_id
                );
            } else {
                statuses += 1;
                println!(
                    "[mainnet] ORDER-STATUS {:?} coid={} oid={:?}",
                    u.status, u.client_order_id, u.exchange_order_id
                );
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    // ── safety net: cancel all + close any residual position ──
    let _ = tr.cancel_all(Exchange::Lighter, SYMBOL);
    std::thread::sleep(Duration::from_millis(1000));
    if let Ok(pos) = position::fetch_positions(&rest, account_index) {
        if let Some(p) = pos.get(SYMBOL) {
            if p.quantity.abs() > 1e-9 {
                let side = if p.quantity > 0.0 { Side::Sell } else { Side::Buy };
                let (b, a) = top_of_book(&rest);
                let px = if side == Side::Sell { b * 0.99 } else { a * 1.01 };
                println!("[mainnet] residual {:.6} — closing reduce-only {:?}", p.quantity, side);
                let mut c = OrderRequest::new_limit(Exchange::Lighter, SYMBOL.to_string(), side, px, p.quantity.abs());
                c.order_type = OrderType::Fak;
                c.post_only = false;
                c.reduce_only = true;
                c.instance_id = "mainfill".to_string();
                let _ = tr.submit_order(&c);
            } else {
                println!("[mainnet] residual position: flat ✓");
            }
        } else {
            println!("[mainnet] residual position: none ✓");
        }
    }

    println!(
        "[mainnet] fills(account_all_trades)={} order-status(account_all_orders)={} buy_fill={} sell_fill={}",
        fills, statuses, buy_fill, sell_fill
    );
    assert!(fills > 0, "account_all_trades fill push must arrive");
    assert!(statuses > 0, "account_all_orders order-status push must arrive");
    println!("MAINNET FILL + PUSHES OK");
}
