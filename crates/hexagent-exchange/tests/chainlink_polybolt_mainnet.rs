//! Price-only authenticated smoke test; never loads wallet keys or builds an executor.
use hexagent_exchange::{
    exchange::{chainlink::PolyBoltMarket, ExchangeMarket},
    types::MarketEvent,
};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
#[test]
#[ignore = "requires maker02 CLOB credentials and live PolyBolt prices"]
fn polybolt_six_price_subscriptions() {
    let path = std::env::var("POLYBOLT_SECRETS_FILE").expect("secure server credential file path");
    let account = std::env::var("POLYBOLT_AUTH_ACCOUNT").unwrap_or_else(|_| "zhu02".into());
    let text = std::fs::read_to_string(path).unwrap();
    let secrets: toml::Value = toml::from_str(&text).unwrap();
    let p = &secrets["poly"][&account];
    let mut feed = PolyBoltMarket::new(
        p["api_key"].as_str().unwrap(),
        p["api_secret"].as_str().unwrap(),
        p["api_passphrase"].as_str().unwrap(),
    );
    let symbols = [
        "btc/usd",
        "eth/usd",
        "sol/usd",
        "btc/usd/twap/60s",
        "eth/usd/twap/60s",
        "sol/usd/twap/60s",
    ];
    let _ = hexagent_exchange::async_rt::init();
    feed.subscribe(&symbols.map(String::from)).unwrap();
    feed.connect().unwrap();
    let mut counts = HashMap::new();
    let mut last = HashMap::new();
    let until = Instant::now() + Duration::from_secs(35);
    while Instant::now() < until {
        match feed.next_event().unwrap() {
            Some(MarketEvent::SpotPrice(p)) => {
                assert_eq!(p.source, "chainlink");
                assert!(p.price > 1. && p.price < 1_000_000.);
                assert!(p.local_timestamp_ns >= p.timestamp_ns);
                assert!(p.timestamp_ns > *last.get(&p.symbol).unwrap_or(&0));
                last.insert(p.symbol.clone(), p.timestamp_ns);
                *counts.entry(p.symbol).or_insert(0usize) += 1;
            }
            _ => std::thread::sleep(Duration::from_millis(1)),
        }
    }
    feed.disconnect();
    println!("35s PolyBolt counts: {counts:?}");
    for symbol in symbols {
        assert!(
            counts.get(symbol).copied().unwrap_or(0) >= 10,
            "missing {symbol}"
        );
    }
    assert_eq!(counts.len(), 6);
}
