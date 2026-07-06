//! Live Lighter **mainnet** smoke: key-binding check + balance read + place +
//! cancel + cancel-all (ignored by default). Exercises the production adapter
//! end-to-end against the real exchange with far-from-market post-only orders
//! — net effect is nothing resting and no fills (maker fee is 0 anyway).
//! Requires a funded account's registered API key via env:
//!
//! ```sh
//! LIGHTER_TEST_KEY=0x… LIGHTER_TEST_ACCOUNT=732630 LIGHTER_TEST_APIKEY=4 \
//!   cargo test -p hexagent-exchange --test lighter_live_mainnet -- --ignored --nocapture
//! ```

use serde::Deserialize;

use hexagent_exchange::exchange::lighter::{auth::LighterAuth, info, position, LighterTrade};
use hexagent_exchange::exchange::ExchangeTrade;
use hexagent_exchange::types::{Exchange, OrderRequest, OrderStatus, OrderType, Side};

const SYMBOL: &str = "BTC";

fn creds() -> (String, i64, u8) {
    (
        std::env::var("LIGHTER_TEST_KEY").expect("set LIGHTER_TEST_KEY"),
        std::env::var("LIGHTER_TEST_ACCOUNT")
            .expect("set LIGHTER_TEST_ACCOUNT")
            .parse()
            .expect("LIGHTER_TEST_ACCOUNT must be an integer account index"),
        std::env::var("LIGHTER_TEST_APIKEY")
            .expect("set LIGHTER_TEST_APIKEY")
            .parse()
            .expect("LIGHTER_TEST_APIKEY must be 0-254"),
    )
}

#[derive(Debug, Deserialize)]
struct ApiKeyEntry {
    api_key_index: u8,
    public_key: String,
}

#[derive(Debug, Deserialize)]
struct ApiKeysResponse {
    api_keys: Vec<ApiKeyEntry>,
}

#[derive(Debug, Deserialize)]
struct ActiveOrder {
    client_order_index: i64,
}

#[derive(Debug, Deserialize)]
struct ActiveOrdersResponse {
    #[serde(default)]
    orders: Vec<ActiveOrder>,
}

/// Authenticated GET accountActiveOrders through the production path — also
/// live-verifies `create_auth_token` (a bad token is rejected with code 20013).
fn active_orders(auth: &LighterAuth, market_id: i16) -> Vec<ActiveOrder> {
    let deadline = (hexagent_exchange::types::now_ns() / 1_000_000_000) as i64 + 600;
    let token = auth.signer.create_auth_token(deadline);
    let resp: ActiveOrdersResponse = info::get_json_auth(
        format!(
            "{}/api/v1/accountActiveOrders?account_index={}&market_id={}",
            auth.rest_base(),
            auth.account_index(),
            market_id
        ),
        token,
    )
    .expect("accountActiveOrders");
    resp.orders
}

#[test]
#[ignore]
fn mainnet_balance_place_cancel() {
    let (key, account_index, api_key_index) = creds();
    let _ = hexagent_exchange::async_rt::init();

    let auth = LighterAuth::new(&key, account_index, api_key_index, "mainnet", "", "").expect("auth");

    // ── 0. key binding: our derived pubkey must equal the registered one ──
    // (fail fast before any tx: a mismatched key/index pair would only burn
    // nonces on signature rejects)
    let keys: ApiKeysResponse = info::get_json(format!(
        "{}/api/v1/apikeys?account_index={}&api_key_index={}",
        auth.rest_base(),
        account_index,
        api_key_index
    ))
    .expect("apikeys");
    let registered = keys
        .api_keys
        .iter()
        .find(|k| k.api_key_index == api_key_index)
        .expect("api key slot not registered");
    println!("[0] registered pubkey  = {}", registered.public_key);
    println!("    derived pubkey     = {}", auth.signer.pubkey_hex());
    assert_eq!(
        registered.public_key,
        auth.signer.pubkey_hex(),
        "private key does not match the registered API key at this index"
    );

    // ── 1. balance ──
    let bal = position::fetch_balance(&auth.rest_base(), account_index).expect("fetch_balance");
    println!("[1] balance: available={} collateral={}", bal.available_balance, bal.collateral);
    assert!(bal.collateral > 0.0, "funded account expected");

    // ── 2. place: far-below post-only bid (5% under the touch) must rest ──
    let meta = info::fetch_meta(&auth.rest_base()).expect("orderBookDetails");
    let m = meta.market(SYMBOL).expect("BTC market").clone();
    let best = {
        #[derive(Debug, Deserialize)]
        struct Ob {
            #[serde(default)]
            bids: Vec<Lvl>,
        }
        #[derive(Debug, Deserialize)]
        struct Lvl {
            price: String,
        }
        let ob: Ob = info::get_json(format!(
            "{}/api/v1/orderBookOrders?market_id={}&limit=1",
            auth.rest_base(),
            m.market_id
        ))
        .expect("orderBookOrders");
        ob.bids[0].price.parse::<f64>().unwrap()
    };
    let px = best * 0.95;
    // min_base_amount, and at least min_quote (+5% headroom) at our price.
    let min_base: f64 = m.min_base_amount.parse().unwrap_or(0.0002);
    let min_quote: f64 = m.min_quote_amount.parse().unwrap_or(10.0);
    let step = 10f64.powi(-(m.size_decimals as i32));
    let sz = (min_base.max(min_quote * 1.05 / px) / step).ceil() * step;
    println!("[2] best_bid={} -> post-only bid px={:.1} sz={}", best, px, sz);

    let mut tr = LighterTrade::new(auth, meta, "livetest");
    let mut o = OrderRequest::new_limit(Exchange::Lighter, SYMBOL.to_string(), Side::Buy, px, sz);
    o.order_type = OrderType::LimitMaker;
    o.post_only = true;
    o.instance_id = "livetest".to_string();
    let u = tr.submit_order(&o).expect("submit");
    println!("    place -> {:?} coi={:?}", u.status, u.exchange_order_id);
    assert_eq!(u.status, OrderStatus::Accepted);
    let coi: i64 = u.exchange_order_id.clone().unwrap().parse().unwrap();

    // Sequencer settles fast, but give it a moment before the read-back.
    std::thread::sleep(std::time::Duration::from_millis(1500));
    let auth2 = LighterAuth::new(&key, account_index, api_key_index, "mainnet", "", "").expect("auth");
    let open = active_orders(&auth2, 1);
    println!("    activeOrders -> {:?}", open.iter().map(|o| o.client_order_index).collect::<Vec<_>>());
    assert!(
        open.iter().any(|x| x.client_order_index == coi),
        "placed order must appear in accountActiveOrders"
    );

    // ── 3. cancel by client_order_id ──
    let c = tr.cancel_order(Exchange::Lighter, &o.client_order_id).expect("cancel");
    println!("[3] cancel -> {:?} err={:?}", c.status, c.error);
    assert_eq!(c.status, OrderStatus::Cancelled);
    std::thread::sleep(std::time::Duration::from_millis(1500));
    let open = active_orders(&auth2, 1);
    assert!(
        !open.iter().any(|x| x.client_order_index == coi),
        "cancelled order must be gone from accountActiveOrders"
    );
    println!("    activeOrders after cancel -> {}", open.len());

    // ── 4. place again, then cancel_all sweeps it ──
    let mut o2 = OrderRequest::new_limit(Exchange::Lighter, SYMBOL.to_string(), Side::Buy, px, sz);
    o2.order_type = OrderType::LimitMaker;
    o2.post_only = true;
    o2.instance_id = "livetest".to_string();
    let u2 = tr.submit_order(&o2).expect("submit 2");
    assert_eq!(u2.status, OrderStatus::Accepted);
    std::thread::sleep(std::time::Duration::from_millis(1500));
    let n_before = active_orders(&auth2, 1).len();
    let swept = tr.cancel_all(Exchange::Lighter, SYMBOL).expect("cancel_all");
    println!("[4] cancel_all -> {} acks (resting before: {})", swept.len(), n_before);
    std::thread::sleep(std::time::Duration::from_millis(1500));
    let open = active_orders(&auth2, 1);
    assert!(open.is_empty(), "cancel_all must leave nothing resting, got {}", open.len());
    println!("    activeOrders after cancel_all -> 0 ✓");
}
