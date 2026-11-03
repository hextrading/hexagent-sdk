//! Authenticated Chainlink reference prices from PolyBolt v1.
//! Owner: existing public-feed reactor. No strategy/account state or new threads.
//! Live-only: two-minute subscription snapshots must not acquire fresh receive
//! clocks or enter strike/settlement caches. They still consume channel sequence.
use crate::exchange::{
    ws_send, ExchangeMarket, PublicMarketPublisher, PublicMarketReceiver, WS_CONNECT_TIMEOUT,
};
use crate::types::{now_ns, MarketEvent, SpotPrice};
use anyhow::{anyhow, bail, Result};
use futures_util::StreamExt;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

const URL: &str = "wss://ws-live-v2.polymarket.com/ws";
const CHANNELS: [&str; 2] = ["price.crypto", "price.crypto.twap"];
const PRICE_STALL: Duration = Duration::from_secs(15);

/// No Debug implementation: authentication material must never enter logs.
pub struct PolyBoltMarket {
    auth: String,
    symbols: Vec<String>,
    rx: Option<PublicMarketReceiver>,
    shutdown: Arc<AtomicBool>,
}
impl PolyBoltMarket {
    pub fn new(key: &str, secret: &str, passphrase: &str) -> Self {
        let auth = if [key, secret, passphrase].iter().any(|v| v.is_empty()) {
            String::new()
        } else {
            json!({"op":"auth","auth":{"apiKey":key,"secret":secret,"passphrase":passphrase}})
                .to_string()
        };
        Self {
            auth,
            symbols: Vec::new(),
            rx: None,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
}

struct PriceState {
    label: String,
    wire_symbol: String,
    channel: usize,
    source_ns: u64,
    price: f64,
    fresh_at: Instant,
    accepted: bool,
}
impl PriceState {
    fn new(label: &str) -> Result<Self> {
        let (base, channel) = label
            .strip_suffix("/twap/60s")
            .map_or((label, 0), |s| (s, 1));
        let asset = base
            .strip_suffix("/usd")
            .ok_or_else(|| anyhow!("PolyBolt expects asset/usd[/twap/60s] labels"))?;
        if asset.is_empty()
            || !asset
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        {
            bail!("invalid PolyBolt asset label");
        }
        Ok(Self {
            label: label.to_owned(),
            wire_symbol: format!("{asset}usd"),
            channel,
            source_ns: 0,
            price: 0.,
            fresh_at: Instant::now(),
            accepted: false,
        })
    }
    fn observe(&mut self, source_ns: u64, price: f64, now: Instant) -> Result<bool> {
        if source_ns == self.source_ns && price != self.price {
            bail!("conflicting oracle observation");
        }
        if source_ns <= self.source_ns {
            return Ok(false);
        }
        self.source_ns = source_ns;
        self.price = price;
        self.fresh_at = now;
        Ok(true)
    }
}

#[derive(Default)]
struct Sequence([Option<u64>; 2]);
impl Sequence {
    fn accept(&mut self, channel: usize, data: &Value) -> Result<()> {
        if data.get("v").and_then(Value::as_u64) != Some(1) {
            bail!("unsupported PolyBolt envelope version");
        }
        let n = data
            .get("seq")
            .and_then(Value::as_u64)
            .filter(|n| *n > 0)
            .ok_or_else(|| anyhow!("invalid PolyBolt sequence"))?;
        let dropped = match data.get("dropped") {
            None => 0,
            Some(v) => v
                .as_u64()
                .ok_or_else(|| anyhow!("invalid dropped counter"))?,
        };
        if dropped != 0 || self.0[channel].is_some_and(|old| old.checked_add(1) != Some(n)) {
            bail!("PolyBolt channel gap/replay or dropped delivery");
        }
        self.0[channel] = Some(n);
        Ok(())
    }
}

fn decode(payload: &Value, channel: usize) -> Result<(u64, f64)> {
    if payload.get("source").and_then(Value::as_str) != Some("chainlink") {
        bail!("PolyBolt provider is not Chainlink");
    }
    if channel == 1 && payload.get("window_seconds").and_then(Value::as_u64) != Some(60) {
        bail!("PolyBolt TWAP window is not 60 seconds");
    }
    let timestamp = payload
        .get("timestamp")
        .and_then(Value::as_u64)
        .filter(|n| *n > 0)
        .and_then(|n| n.checked_mul(1_000_000))
        .ok_or_else(|| anyhow!("invalid PolyBolt source clock"))?;
    // PolyBolt exact prices are DECIMAL strings, unlike legacy RTDS E18 integers.
    let price = payload
        .get("full_accuracy_value")
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|p| p.is_finite() && *p > 0.)
        .ok_or_else(|| anyhow!("invalid PolyBolt decimal price"))?;
    Ok((timestamp, price))
}

async fn task(
    url: String,
    auth: String,
    mut prices: Vec<PriceState>,
    tx: PublicMarketPublisher,
    shutdown: Arc<AtomicBool>,
    tls: crate::exchange::ws_tls::WsTlsConfig,
) {
    let mut backoff = crate::exchange::ReconnectBackoff::new(500, 30_000);
    while !shutdown.load(Ordering::Relaxed) {
        let connected = Instant::now();
        let result: Result<()> = async {
            let (stream, _) = tokio::time::timeout(WS_CONNECT_TIMEOUT,
                tokio_tungstenite::connect_async_tls_with_config(&url, None, false, Some(tls.connector()))).await??;
            let (mut write, mut read) = stream.split();
            ws_send(&mut write, Message::Text(auth.clone())).await.map_err(|_| anyhow!("PolyBolt auth send failed"))?;
            // Authentication acknowledgement is consumed without logging its body.
            tokio::time::timeout(Duration::from_secs(15), async {
                loop {
                    match read.next().await {
                        Some(Ok(Message::Ping(p))) => ws_send(&mut write, Message::Pong(p)).await.map_err(anyhow::Error::msg)?,
                        Some(Ok(Message::Text(s))) => {
                            let x: Value = serde_json::from_str(&s).map_err(|_| anyhow!("invalid PolyBolt auth response"))?;
                            if x.get("op").and_then(Value::as_str) == Some("authed") { return Ok::<(), anyhow::Error>(()); }
                            bail!("PolyBolt authentication rejected");
                        }
                        _ => bail!("PolyBolt authentication connection closed"),
                    }
                }
            }).await??;
            for (i, p) in prices.iter_mut().enumerate() {
                p.accepted = false; p.fresh_at = Instant::now();
                let mut filter = json!({"symbol":p.wire_symbol,"provider":"chainlink"});
                if p.channel == 1 { filter["window_seconds"] = json!(60); }
                // One rid per subscription makes acceptance exact, even when
                // many symbols share a channel. <= 16 frames stays below 20/s.
                ws_send(&mut write, Message::Text(json!({"op":"subscribe","rid":i.to_string(),
                    "subscriptions":[{"channel":CHANNELS[p.channel],"filter":filter}]}).to_string())).await.map_err(anyhow::Error::msg)?;
            }
            let mut seq = Sequence::default(); // scoped to this connection/channel
            let mut timer = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = timer.tick() => {
                        if shutdown.load(Ordering::Relaxed) { return Ok(()); }
                        if prices.iter().any(|p| p.fresh_at.elapsed() >= PRICE_STALL) {
                            bail!("PolyBolt requested price or subscription stalled for 15 seconds");
                        }
                    }
                    message = read.next() => {
                        let received_ns = now_ns(); // before JSON decoding; source clock remains distinct
                        let message = message.ok_or_else(|| anyhow!("PolyBolt closed"))??;
                        match message {
                            Message::Ping(p) => ws_send(&mut write, Message::Pong(p)).await.map_err(anyhow::Error::msg)?,
                            Message::Close(_) => bail!("PolyBolt server closed"),
                            Message::Text(s) => {
                                let data: Value = serde_json::from_str(&s).map_err(|_| anyhow!("invalid PolyBolt JSON"))?;
                                match data.get("op").and_then(Value::as_str) {
                                    Some("pong") => continue,
                                    Some("subscribed") => {
                                        let i = data.get("rid").and_then(Value::as_str).and_then(|s| s.parse::<usize>().ok())
                                            .filter(|i| *i < prices.len()).ok_or_else(|| anyhow!("unexpected subscription acknowledgement"))?;
                                        if data.get("channel").and_then(Value::as_str) != Some(CHANNELS[prices[i].channel])
                                            || data.get("provider").is_some_and(|p| p.as_str() != Some("chainlink")) {
                                            bail!("unexpected subscription channel/provider");
                                        }
                                        prices[i].accepted = true; continue;
                                    }
                                    Some(_) => bail!("PolyBolt rejected subscription/control request"),
                                    None => {}
                                }
                                let channel = CHANNELS.iter().position(|c| data.get("channel").and_then(Value::as_str) == Some(c))
                                    .ok_or_else(|| anyhow!("unexpected PolyBolt channel"))?;
                                seq.accept(channel, &data)?;
                                let payload = data.get("payload").ok_or_else(|| anyhow!("missing PolyBolt payload"))?;
                                if payload.get("source").and_then(Value::as_str) != Some("chainlink") { bail!("PolyBolt provider changed"); }
                                let symbol = payload.get("symbol").and_then(Value::as_str).ok_or_else(|| anyhow!("missing PolyBolt symbol"))?;
                                let Some(p) = prices.iter_mut().find(|p| p.channel == channel && p.wire_symbol == symbol) else { continue; };
                                if !p.accepted { bail!("price received before subscription acceptance"); }
                                if data.get("snapshot").and_then(Value::as_bool) == Some(true) { continue; }
                                let (source_ns, price) = decode(payload, channel)?;
                                if !p.observe(source_ns, price, Instant::now())? { continue; }
                                let event = MarketEvent::SpotPrice(SpotPrice { source: "chainlink".to_owned(), symbol: p.label.clone(), price,
                                    timestamp_ns: source_ns, local_timestamp_ns: received_ns });
                                // Existing bounded public lane; receiver destruction ends this owner.
                                if crate::exchange::publish_market_event(&tx, event).is_err() { shutdown.store(true, Ordering::Relaxed); return Ok(()); }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }.await;
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        if let Err(error) = result {
            log::warn!("[Chainlink/PolyBolt] {}", error);
        }
        if connected.elapsed() >= Duration::from_secs(30) {
            backoff.reset();
        }
        tokio::time::sleep(backoff.next_delay()).await;
    }
}

impl ExchangeMarket for PolyBoltMarket {
    fn subscribe(&mut self, symbols: &[String]) -> Result<()> {
        if symbols.is_empty() || symbols.len() > 16 {
            bail!("PolyBolt price adapter supports 1..=16 subscriptions per connection");
        }
        for (i, s) in symbols.iter().enumerate() {
            PriceState::new(s)?;
            if symbols[..i].contains(s) {
                bail!("duplicate PolyBolt subscription");
            }
        }
        self.symbols = symbols.to_vec();
        Ok(())
    }
    fn connect(&mut self) -> Result<()> {
        if self.auth.is_empty() {
            bail!("PolyBolt requires CLOB API key, secret and passphrase");
        }
        if self.symbols.is_empty() {
            bail!("PolyBolt requires subscribed price symbols");
        }
        let tls = crate::exchange::ws_tls::WsTlsConfig::load_native()?;
        let prices = self
            .symbols
            .iter()
            .map(|s| PriceState::new(s))
            .collect::<Result<Vec<_>>>()?;
        let (tx, rx) = crate::exchange::public_market_channel();
        self.rx = Some(rx);
        self.shutdown = Arc::new(AtomicBool::new(false));
        crate::async_rt::handle().spawn(task(
            URL.to_owned(),
            self.auth.clone(),
            prices,
            tx,
            self.shutdown.clone(),
            tls,
        ));
        Ok(())
    }
    fn next_event(&mut self) -> Result<Option<MarketEvent>> {
        match self
            .rx
            .as_ref()
            .ok_or_else(|| anyhow!("PolyBolt not connected"))?
            .try_recv()
        {
            Ok(e) => Ok(Some(e)),
            Err(crossbeam_channel::TryRecvError::Empty) => Ok(None),
            Err(crossbeam_channel::TryRecvError::Disconnected) => bail!("PolyBolt task ended"),
        }
    }
    fn disconnect(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.rx = None;
    }
    fn name(&self) -> &str {
        "chainlink"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decimal_source_clock_and_provider_are_explicit() {
        let p = json!({"symbol":"btcusd","source":"chainlink","full_accuracy_value":"76457.775242712579047424","value":1,"timestamp":1793688862000u64,"window_seconds":60});
        let (ts, price) = decode(&p, 1).unwrap();
        assert_eq!(ts, 1793688862000000000);
        assert!((price - 76457.77524271258).abs() < 1e-9);
        for (k, v) in [
            ("source", json!("pyth")),
            ("window_seconds", json!(30)),
            ("timestamp", json!(0)),
            ("timestamp", json!(u64::MAX)),
            ("full_accuracy_value", json!("NaN")),
            ("full_accuracy_value", json!("-1")),
        ] {
            let mut bad = p.clone();
            bad[k] = v;
            assert!(decode(&bad, 1).is_err());
        }
    }
    #[test]
    fn channel_sequences_include_snapshots_and_reset_on_reconnect() {
        let mut s = Sequence::default();
        s.accept(0, &json!({"v":1,"seq":1,"snapshot":true}))
            .unwrap();
        s.accept(1, &json!({"v":1,"seq":1})).unwrap();
        s.accept(0, &json!({"v":1,"seq":2})).unwrap();
        for x in [
            json!({"v":1,"seq":2}),
            json!({"v":1,"seq":4}),
            json!({"v":1,"seq":3,"dropped":1}),
            json!({"v":2,"seq":3}),
        ] {
            assert!(s.accept(0, &x).is_err());
        }
        Sequence::default()
            .accept(0, &json!({"v":1,"seq":1}))
            .unwrap();
    }
    #[test]
    fn replay_does_not_refresh_liveness_or_cross_symbol_state() {
        let mut a = PriceState::new("btc/usd").unwrap();
        let b = PriceState::new("btc/usd/twap/60s").unwrap();
        let now = Instant::now();
        assert!(a.observe(10, 100., now).unwrap());
        assert!(!a.observe(10, 100., now + PRICE_STALL).unwrap());
        assert_eq!(a.fresh_at, now);
        assert!(!a.observe(9, 99., now).unwrap());
        assert!(a.observe(10, 101., now).is_err());
        assert_eq!(b.source_ns, 0);
        a.accepted = false;
        a.fresh_at = now + PRICE_STALL; // reconnect keeps source watermark
        assert!(!a.observe(10, 100., now + PRICE_STALL).unwrap());
        assert!(a.observe(11, 101., now + PRICE_STALL).unwrap());
    }
    #[test]
    fn missing_credentials_and_bad_subscriptions_fail_closed() {
        let mut a = PolyBoltMarket::new("", "", "");
        assert!(a.connect().is_err());
        for labels in [
            vec![],
            vec!["btcusd".into()],
            vec!["btc/usd".into(), "btc/usd".into()],
            vec!["btc/usd".into(); 17],
        ] {
            assert!(a.subscribe(&labels).is_err());
        }
        a.subscribe(&["btc/usd".into(), "btc/usd/twap/60s".into()])
            .unwrap();
    }
    #[tokio::test]
    async fn wire_auth_snapshots_replay_gap_and_reconnect() {
        use futures_util::SinkExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for session in 0..2 {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
                let auth: Value =
                    serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap())
                        .unwrap();
                assert_eq!(auth["op"], "auth");
                ws.send(Message::Text(json!({"op":"authed"}).to_string()))
                    .await
                    .unwrap();
                let sub: Value =
                    serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap())
                        .unwrap();
                assert_eq!(sub["subscriptions"][0]["filter"]["provider"], "chainlink");
                ws.send(Message::Text(
                    json!({"op":"subscribed","rid":"0","channel":"price.crypto"}).to_string(),
                ))
                .await
                .unwrap();
                ws.send(Message::Text(json!({"v":1,"channel":"price.crypto","seq":1,"snapshot":true,"payload":{"symbol":"btcusd","source":"chainlink","data":[{"timestamp":999,"full_accuracy_value":"999"}]}}).to_string())).await.unwrap();
                for (seq, stamp) in if session == 0 {
                    vec![(2, 1000), (3, 1000), (5, 1500)]
                } else {
                    vec![(2, 1000), (3, 2000)]
                } {
                    ws.send(Message::Text(json!({"v":1,"channel":"price.crypto","seq":seq,"ts":stamp+100,"payload":{"symbol":"btcusd","source":"chainlink","timestamp":stamp,"full_accuracy_value":"123.25"}}).to_string())).await.unwrap();
                }
                if session == 1 {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        });
        let (tx, rx) = crate::exchange::public_market_channel();
        let stop = Arc::new(AtomicBool::new(false));
        let owner = tokio::spawn(task(
            url,
            "{\"op\":\"auth\"}".into(),
            vec![PriceState::new("btc/usd").unwrap()],
            tx,
            stop.clone(),
            crate::exchange::ws_tls::WsTlsConfig::load_native().unwrap(),
        ));
        let mut stamps = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            while stamps.len() < 2 {
                if let Ok(MarketEvent::SpotPrice(p)) = rx.try_recv() {
                    stamps.push(p.timestamp_ns);
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(stamps, vec![1_000_000_000, 2_000_000_000]);
        assert!(rx.try_recv().is_err());
        stop.store(true, Ordering::Relaxed);
        owner.await.unwrap();
        server.abort();
    }
    #[test]
    #[ignore = "focused timing benchmark; run optimized on maker02"]
    fn price_decode_benchmark() {
        use std::hint::black_box;
        let old = json!({"symbol":"btc/usd","timestamp":1793688862000u64,"window_s":60,"full_accuracy_value":"76457775242712579047424"});
        let new = json!({"symbol":"btcusd","source":"chainlink","timestamp":1793688862000u64,"window_seconds":60,"full_accuracy_value":"76457.775242712579047424"});
        let mut envelope = json!({"v":1,"seq":1});
        let mut seq = Sequence::default();
        for arm in ["legacy_rtds_decode", "polybolt_decode_and_sequence"] {
            let mut samples = Vec::with_capacity(200_000);
            for n in 1..=200_000u64 {
                envelope["seq"] = json!(n);
                let begin = Instant::now();
                if arm == "legacy_rtds_decode" {
                    black_box(
                        super::super::market::decode_rtds_tick(
                            "crypto_prices_twap_sixty",
                            black_box(&old),
                            1,
                        )
                        .unwrap(),
                    );
                } else {
                    seq.accept(1, black_box(&envelope)).unwrap();
                    let (timestamp_ns, price) = decode(black_box(&new), 1).unwrap();
                    black_box(SpotPrice {
                        source: "chainlink".to_owned(),
                        symbol: "btc/usd/twap/60s".to_owned(),
                        price,
                        timestamp_ns,
                        local_timestamp_ns: 1,
                    });
                }
                samples.push(begin.elapsed().as_nanos());
            }
            samples.sort_unstable();
            println!("{arm}: n={} median_ns={} p99_ns={} p999_ns={} maximum_ns={} queue_depth=0 overflow=0 boundary=parsed_payload_to_validated_SpotPrice_excludes_JSON_network_queues_orders",samples.len(),samples[100_000],samples[198_000],samples[199_800],samples[199_999]);
        }
    }
}
