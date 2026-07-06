//! Live Aster **mainnet** smoke: balance read + place + cancel (ignored by
//! default). Exercises the production adapter end-to-end against the real
//! exchange with a far-from-market post-only order — net effect is nothing
//! resting and no fills. Requires a funded account's API-wallet key via env:
//!
//! ```sh
//! ASTER_TEST_USER=0x… ASTER_TEST_KEY=0x… \
//!   cargo test -p hexagent-exchange --test aster_live_mainnet -- --ignored --nocapture
//! ```

use hexagent_exchange::exchange::aster::{auth::AsterAuth, info, position, AsterTrade};
use hexagent_exchange::exchange::aster::signer::signed_query;
use hexagent_exchange::exchange::ExchangeTrade;
use hexagent_exchange::types::{Exchange, OrderRequest, OrderStatus, OrderType, Side};

const SYMBOL: &str = "BTCUSDT";
const SZ: f64 = 0.001; // min lot, ~$60 notional

/// Signed GET /fapi/v3/openOrders through the production request path.
fn open_orders(auth: &AsterAuth) -> Vec<serde_json::Value> {
    let query = signed_query(auth, vec![("symbol", SYMBOL.to_string())]).expect("sign");
    let url = format!("{}?{}", auth.fapi_url("openOrders"), query);
    let text = info::http_request("GET", &url).expect("openOrders");
    serde_json::from_str(&text).expect("parse openOrders")
}

#[test]
#[ignore]
fn mainnet_balance_place_cancel() {
    let user = std::env::var("ASTER_TEST_USER").expect("set ASTER_TEST_USER");
    let key = std::env::var("ASTER_TEST_KEY").expect("set ASTER_TEST_KEY");
    let _ = hexagent_exchange::async_rt::init();

    let auth = AsterAuth::new(&key, &user, "mainnet", "", "").expect("auth");

    // ── 1. balance: signed account read ──
    let balances = position::fetch_balances(&auth).expect("fetch_balances");
    let usdt = balances.iter().find(|b| b.asset == "USDT").expect("USDT balance row");
    println!("[1] balance USDT = {} (available {})", usdt.balance, usdt.available_balance);
    assert!(usdt.balance.parse::<f64>().unwrap() > 0.0, "funded account expected");

    // ── 2. place: far-below GTX bid (5% under the book) must rest ──
    let meta = info::fetch_meta(&auth.rest_base()).expect("exchangeInfo");
    let depth = info::fetch_depth(&auth.rest_base(), SYMBOL, 5).expect("depth");
    let best_bid: f64 = depth.bids[0][0].parse().unwrap();
    let mut tr = AsterTrade::new(auth.clone(), meta, "livetest");

    let mut o = OrderRequest::new_limit(
        Exchange::Aster, SYMBOL.to_string(), Side::Buy, best_bid * 0.95, SZ,
    );
    o.order_type = OrderType::LimitMaker;
    o.post_only = true;
    o.instance_id = "livetest".to_string();
    let u = tr.submit_order(&o).expect("submit");
    println!("[2] place far GTX bid -> {:?} oid={:?}", u.status, u.exchange_order_id);
    assert_eq!(u.status, OrderStatus::Accepted, "far post-only must rest");
    let oid = u.exchange_order_id.clone().expect("orderId");

    // resting server-side?
    let open = open_orders(&auth);
    println!("    openOrders -> {} (looking for oid {})", open.len(), oid);
    assert!(
        open.iter().any(|x| x.get("orderId").and_then(|v| v.as_u64()).map(|n| n.to_string()) == Some(oid.clone())),
        "placed order must appear in openOrders"
    );

    // ── 3. cancel by client_order_id ──
    let c = tr.cancel_order(Exchange::Aster, &o.client_order_id).expect("cancel");
    println!("[3] cancel -> {:?}", c.status);
    assert_eq!(c.status, OrderStatus::Cancelled);

    let open = open_orders(&auth);
    println!("    openOrders after cancel -> {}", open.len());
    assert!(
        !open.iter().any(|x| x.get("orderId").and_then(|v| v.as_u64()).map(|n| n.to_string()) == Some(oid.clone())),
        "cancelled order must be gone"
    );

    // ── 4. reduce-only from a FLAT position must be rejected (-2022) —
    //       proves the flag actually reaches the venue and is enforced. ──
    // Price must stay inside the ±2% PERCENT_PRICE band or the filter
    // rejects (-4024) before the reduce-only check is reached.
    let mut ro = OrderRequest::new_limit(
        Exchange::Aster, SYMBOL.to_string(), Side::Sell, best_bid * 0.999, SZ,
    );
    ro.order_type = OrderType::Fak; // IOC
    ro.post_only = false;
    ro.reduce_only = true;
    ro.instance_id = "livetest".to_string();
    let u = tr.submit_order(&ro).expect("reduce-only submit");
    println!("[4] reduce-only from flat -> {:?} err={:?}", u.status, u.error);
    assert_eq!(u.status, OrderStatus::Rejected, "reduce-only from flat must reject");
    assert!(
        u.error.as_deref().unwrap_or("").contains("-2022")
            || u.error.as_deref().unwrap_or("").to_lowercase().contains("reduceonly"),
        "expected ReduceOnly rejection, got {:?}", u.error
    );
}
