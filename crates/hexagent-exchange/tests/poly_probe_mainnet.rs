//! Live Polymarket **MAINNET** RTT-probe order round-trip (ignored, reserves
//! ~$1 collateral for the few seconds between place and cancel). Verifies the
//! exact order the RTT probe fires — `build_signed_order_dispatch` (poly_1271
//! → deposit-wallet maker + ERC-7739 wrap) + the probe's wire body
//! (`postOnly GTC BUY @0.01 × 100`) — is **accepted and rests** (HTTP 200,
//! orderID) and then cancels cleanly. Regression test for the 2026-07
//! incident where the probe used the plain (unwrapped) signing path and the
//! server rejected 100% of probes with http_400, silently degrading the
//! RTT gate's samples.
//!
//! `POLY_TEST_TOKEN_ID` must be the **high-priced** side of an active
//! non-negRisk binary market (so 0.01 rests far below the book and postOnly
//! can never cross).
//!
//! ```sh
//! POLY_TEST_PK=0x… POLY_TEST_FUNDER=0x… POLY_TEST_API_KEY=… \
//! POLY_TEST_API_SECRET=… POLY_TEST_PASSPHRASE=… POLY_TEST_TOKEN_ID=… \
//!   cargo test -p hexagent-exchange --test poly_probe_mainnet -- --ignored --nocapture
//! ```

use hexagent_exchange::exchange::polymarket::auth::PolyAuth;
use hexagent_exchange::exchange::polymarket::signer::SignatureType;
use hexagent_exchange::exchange::polymarket::signer_v2::OrderSignerV2;

const CLOB: &str = "https://clob.polymarket.com";
const PROBE_PRICE: f64 = 0.01; // == rtt_probe::FULL_PROBE_PRICE
const PROBE_SIZE: f64 = 100.0; // == rtt_probe::FULL_PROBE_SIZE

fn env(k: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| panic!("set {}", k))
}

#[test]
#[ignore]
fn poly_probe_place_rests_and_cancels() {
    let pk = env("POLY_TEST_PK");
    let funder = env("POLY_TEST_FUNDER");
    let api_key = env("POLY_TEST_API_KEY");
    let api_secret = env("POLY_TEST_API_SECRET");
    let passphrase = env("POLY_TEST_PASSPHRASE");
    let token = env("POLY_TEST_TOKEN_ID");

    // Same construction as SharedState init for a poly_1271 account
    // (neg_risk=false: probe targets the plain-CTFExchange 5m series).
    let signer = OrderSignerV2::new(&pk, false, SignatureType::Poly1271, "")
        .expect("signer")
        .with_funder(&funder);
    let signed = signer
        .build_signed_order_dispatch(&token, PROBE_PRICE, PROBE_SIZE, hexagent_exchange::types::Side::Buy)
        .expect("build_signed_order_dispatch");
    assert_eq!(signed.order.maker, signed.order.signer, "poly_1271: maker == signer == funder");
    assert_eq!(signed.order.signature_type, 3, "poly_1271 forces sig type 3");

    // Wire body — byte-for-byte the shape rtt_probe::fire_full_probe sends.
    let salt_u64: u64 = signed.order.salt.parse::<u128>().map(|v| v as u64).unwrap_or(0);
    let body = serde_json::json!({
        "owner": api_key,
        "orderType": "GTC",
        "postOnly": true,
        "deferExec": false,
        "order": {
            "salt": salt_u64,
            "maker": signed.order.maker,
            "signer": signed.order.signer,
            "taker": signed.order.taker,
            "tokenId": signed.order.token_id,
            "makerAmount": signed.order.maker_amount,
            "takerAmount": signed.order.taker_amount,
            "side": "BUY",
            "signatureType": signed.order.signature_type,
            "timestamp": signed.order.timestamp,
            "expiration": signed.order.expiration,
            "metadata": signed.order.metadata,
            "builder": signed.order.builder,
            "signature": signed.signature,
        }
    })
    .to_string();

    // POLY_ADDRESS auth header carries the EOA (signer_address), not the
    // deposit wallet — with_funder only rewrote maker_address.
    let eoa = signer.signer_address.clone();
    let auth = PolyAuth::new(&api_key, &api_secret, &passphrase, &eoa).expect("auth");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");
    let client = reqwest::Client::new();

    // ── Place: must be ACCEPTED (rest), not rejected ────────────────
    let headers = auth.sign_request("POST", "/order", &body);
    let (status, text) = rt.block_on(async {
        let mut req = client.post(format!("{}{}", CLOB, "/order")).body(body.clone());
        for (k, v) in headers.as_pairs() {
            req = req.header(k, v);
        }
        req = req.header("Content-Type", "application/json");
        let resp = req.send().await.expect("place send");
        (resp.status().as_u16(), resp.text().await.unwrap_or_default())
    });
    println!("place → HTTP {}: {}", status, text);
    assert_eq!(status, 200, "probe place must be accepted, got {}: {}", status, text);
    let pj: serde_json::Value = serde_json::from_str(&text).expect("place json");
    let oid = pj["orderID"].as_str().unwrap_or(&signed.order_hash).to_string();
    assert!(!oid.is_empty(), "orderID missing in accepted place");

    // ── Cancel: the resting order must cancel cleanly ───────────────
    let cbody = serde_json::json!({ "orderID": oid }).to_string();
    let cheaders = auth.sign_request("DELETE", "/order", &cbody);
    let (cstatus, ctext) = rt.block_on(async {
        let mut req = client
            .delete(format!("{}{}", CLOB, "/order"))
            .body(cbody.clone());
        for (k, v) in cheaders.as_pairs() {
            req = req.header(k, v);
        }
        req = req.header("Content-Type", "application/json");
        let resp = req.send().await.expect("cancel send");
        (resp.status().as_u16(), resp.text().await.unwrap_or_default())
    });
    println!("cancel → HTTP {}: {}", cstatus, ctext);
    assert_eq!(cstatus, 200, "cancel must succeed, got {}: {}", cstatus, ctext);
    let cj: serde_json::Value = serde_json::from_str(&ctext).expect("cancel json");
    let canceled = cj["canceled"]
        .as_array()
        .map(|a| a.iter().any(|v| v.as_str() == Some(oid.as_str())))
        .unwrap_or(false);
    assert!(canceled, "orderID {} not in canceled list: {}", oid, ctext);
    println!("✓ probe order rested and canceled (oid={})", oid);
}
