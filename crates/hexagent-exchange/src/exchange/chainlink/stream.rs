//! Chainlink Data Streams WebSocket feed.
//!
//! Connects to the Chainlink Data Streams WebSocket API to receive real-time
//! price reports. Authentication uses HMAC-SHA256 signed headers.
//!
//! Mainnet: wss://ws.dataengine.chain.link
//! Testnet: wss://ws.testnet-dataengine.chain.link
//!
//! Feed IDs are hex-encoded identifiers (e.g. BTC/USD testnet:
//! 0x00037da06d56d083fe599397a4769a042d63aa73dc4ef57709d31e9971a5b439)
//!
//! Emits MarketEvent::SpotPrice with source = "chainlink_stream".

use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use log::{info, warn};
use sha2::{Sha256, Digest};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::Request;
use tokio_tungstenite::tungstenite::Message;

use crate::exchange::ExchangeMarket;
use crate::types::*;

const DEFAULT_WS_URL: &str = "wss://ws.dataengine.chain.link";
const WS_PATH: &str = "/api/v1/ws";

pub struct ChainlinkStreamMarket {
    /// Feed IDs (hex strings, e.g. "0x00037da0...")
    feed_ids: Vec<String>,
    /// Symbol labels for each feed ID (e.g. "btc/usd"), same order as feed_ids
    symbols: Vec<String>,
    api_key: String,
    user_secret: String,
    ws_url: String,
    event_rx: Option<crossbeam_channel::Receiver<MarketEvent>>,
    ws_shutdown: Arc<AtomicBool>,
}

impl ChainlinkStreamMarket {
    pub fn new(api_key: &str, user_secret: &str, ws_url: &str) -> Self {
        Self {
            feed_ids: Vec::new(),
            symbols: Vec::new(),
            api_key: api_key.to_string(),
            user_secret: user_secret.to_string(),
            ws_url: if ws_url.is_empty() { DEFAULT_WS_URL.to_string() } else { ws_url.to_string() },
            event_rx: None,
            ws_shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
}

/// Generate HMAC-SHA256 authentication headers for Chainlink Data Streams.
fn auth_headers(
    api_key: &str,
    user_secret: &str,
    method: &str,
    path: &str,
) -> Result<(String, String, String)> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .to_string();

    let body_hash = hex::encode(Sha256::digest(b""));
    let signing_string = format!("{} {} {} {} {}", method, path, body_hash, api_key, timestamp);

    let mut mac = Hmac::<Sha256>::new_from_slice(user_secret.as_bytes())
        .map_err(|e| anyhow!("HMAC key error: {}", e))?;
    hmac::Mac::update(&mut mac, signing_string.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());

    Ok((api_key.to_string(), timestamp, signature))
}

async fn chainlink_stream_ws_task(
    ws_url: String,
    feed_ids: Vec<String>,
    symbols: Vec<String>,
    api_key: String,
    user_secret: String,
    event_tx: crossbeam_channel::Sender<MarketEvent>,
    shutdown: Arc<AtomicBool>,
) {
    let mut backoff = crate::exchange::ReconnectBackoff::new(200, 30_000);

    loop {
        if shutdown.load(Ordering::Relaxed) { break; }

        let feed_ids_param = feed_ids.join(",");
        let path = format!("{}?feedIDs={}", WS_PATH, feed_ids_param);
        let full_url = format!("{}{}", ws_url, path);

        info!("[ChainlinkStream] Connecting to {}", full_url);

        let (auth, timestamp, signature) = match auth_headers(&api_key, &user_secret, "GET", &path) {
            Ok(v) => v,
            Err(e) => {
                warn!("[ChainlinkStream] auth build failed: {}", e);
                let delay = backoff.next_delay();
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        info!("[ChainlinkStream] Auth: api_key_len={}, ts={}", api_key.len(), timestamp);

        let mut request: Request<()> = match full_url.as_str().into_client_request() {
            Ok(r) => r,
            Err(e) => {
                warn!("[ChainlinkStream] bad url: {}", e);
                let delay = backoff.next_delay();
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        let headers = request.headers_mut();
        match auth.parse() {
            Ok(v) => { headers.insert("Authorization", v); }
            Err(e) => { warn!("[ChainlinkStream] bad auth header: {}", e); continue; }
        }
        match timestamp.parse() {
            Ok(v) => { headers.insert("X-Authorization-Timestamp", v); }
            Err(e) => { warn!("[ChainlinkStream] bad ts header: {}", e); continue; }
        }
        match signature.parse() {
            Ok(v) => { headers.insert("X-Authorization-Signature-SHA256", v); }
            Err(e) => { warn!("[ChainlinkStream] bad sig header: {}", e); continue; }
        }

        let (stream, response) = match tokio_tungstenite::connect_async(request).await {
            Ok(v) => v,
            Err(e) => {
                let delay = backoff.next_delay();
                warn!("[ChainlinkStream] WS connect failed: {}, retry in {:.1}s", e, delay.as_secs_f64());
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        info!("[ChainlinkStream] Connected ({})", response.status());
        backoff.reset();
        let (mut write, mut read) = stream.split();

        info!("[ChainlinkStream] Connected, streaming {} feeds: {:?}", feed_ids.len(), symbols);

        let mut ping_interval = tokio::time::interval(Duration::from_secs(30));
        ping_interval.tick().await;

        // Stall watchdog: chainlink streams emit per-feed reports
        // every few seconds at most. 60 s of silence is anomalous and
        // matches the engine-level data_timeout for chainlink (30 s);
        // we go slightly looser here so the engine layer fires first
        // when active and this in-task guard handles only true zombie
        // hangs the engine watchdog can't break out of cleanly.
        const STALE_THRESHOLD: Duration = Duration::from_secs(60);

        loop {
            tokio::select! {
                biased;
                _ = ping_interval.tick() => {
                    if let Err(e) = write.send(Message::Ping(Vec::new())).await {
                        warn!("[ChainlinkStream] Ping send failed: {}", e);
                        break;
                    }
                }
                read_result = tokio::time::timeout(STALE_THRESHOLD, read.next()) => {
                    let msg = match read_result {
                        Ok(Some(Ok(m))) => m,
                        Ok(Some(Err(e))) => { warn!("[ChainlinkStream] WS read error: {}", e); break; }
                        Ok(None) => { warn!("[ChainlinkStream] WS closed"); break; }
                        Err(_elapsed) => {
                            warn!("[ChainlinkStream] No message for {:.0}s (stall watchdog) — reconnecting",
                                STALE_THRESHOLD.as_secs_f64());
                            break;
                        }
                    };
                    let parsed: Option<serde_json::Value> = match msg {
                        Message::Binary(data) => serde_json::from_slice(&data).ok(),
                        Message::Text(text) => serde_json::from_str(&text).ok(),
                        Message::Ping(payload) => {
                            let _ = write.send(Message::Pong(payload)).await;
                            None
                        }
                        Message::Close(reason) => {
                            warn!("[ChainlinkStream] Server closed: {:?}", reason);
                            break;
                        }
                        _ => None,
                    };
                    if let Some(data) = parsed {
                        if let Some(event) = parse_report(&data, &feed_ids, &symbols) {
                            if event_tx.send(event).is_err() { return; }
                        }
                    }
                }
            }
            if shutdown.load(Ordering::Relaxed) { return; }
        }

        if shutdown.load(Ordering::Relaxed) { break; }
        let delay = backoff.next_delay();
        tokio::time::sleep(delay).await;
    }
    info!("[ChainlinkStream] WS task exiting");
}

impl ExchangeMarket for ChainlinkStreamMarket {
    fn connect(&mut self) -> Result<()> {
        if self.feed_ids.is_empty() {
            return Err(anyhow!("No feed IDs configured"));
        }
        if self.api_key.is_empty() || self.user_secret.is_empty() {
            return Err(anyhow!("Chainlink Data Streams requires api_key and api_secret"));
        }

        let (event_tx, event_rx) = crossbeam_channel::unbounded::<MarketEvent>();
        self.event_rx = Some(event_rx);
        // Per-task shutdown Arc — see binance/market.rs commentary.
        let shutdown = Arc::new(AtomicBool::new(false));
        self.ws_shutdown = shutdown.clone();

        crate::async_rt::handle().spawn(chainlink_stream_ws_task(
            self.ws_url.clone(),
            self.feed_ids.clone(),
            self.symbols.clone(),
            self.api_key.clone(),
            self.user_secret.clone(),
            event_tx,
            shutdown,
        ));
        Ok(())
    }

    fn subscribe(&mut self, symbols: &[String]) -> Result<()> {
        // symbols format: "feed_id:label" (e.g. "0x00037da0...:btc/usd")
        // or just "feed_id" (label defaults to feed_id)
        self.feed_ids.clear();
        self.symbols.clear();
        for sym in symbols {
            if let Some((feed_id, label)) = sym.split_once(':') {
                self.feed_ids.push(feed_id.to_string());
                self.symbols.push(label.to_string());
            } else {
                self.feed_ids.push(sym.clone());
                self.symbols.push(sym.clone());
            }
        }
        Ok(())
    }

    fn next_event(&mut self) -> Result<Option<MarketEvent>> {
        let rx = self.event_rx.as_ref().ok_or_else(|| anyhow!("Not connected"))?;
        match rx.try_recv() {
            Ok(event) => Ok(Some(event)),
            Err(crossbeam_channel::TryRecvError::Empty) => Ok(None),
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                Err(anyhow!("ChainlinkStream WS task ended unexpectedly"))
            }
        }
    }

    fn disconnect(&mut self) {
        self.ws_shutdown.store(true, Ordering::Relaxed);
        self.event_rx = None;
        info!("[ChainlinkStream] Disconnected");
    }

    fn name(&self) -> &str { "chainlink" }
}

/// Parse a Data Streams report message and extract benchmark_price.
fn parse_report(
    data: &serde_json::Value,
    feed_ids: &[String],
    symbols: &[String],
) -> Option<MarketEvent> {
    let report = data.get("report")?;
    let feed_id = report.get("feedID").and_then(|v| v.as_str())?;
    let observations_ts = report.get("observationsTimestamp")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let full_report_hex = report.get("fullReport").and_then(|v| v.as_str())?;

    let price = match ChainlinkStreamMarket::decode_benchmark_price(full_report_hex) {
        Ok(p) => p,
        Err(e) => {
            warn!("[ChainlinkStream] Failed to decode report: {} (hex_len={}, keys={:?})",
                e, full_report_hex.len(), report.as_object().map(|o| o.keys().collect::<Vec<_>>()));
            return None;
        }
    };

    if price <= 0.0 { return None; }

    let symbol = feed_ids.iter().zip(symbols.iter())
        .find(|(fid, _)| fid.eq_ignore_ascii_case(feed_id))
        .map(|(_, label)| label.clone())
        .unwrap_or_else(|| feed_id.to_string());

    Some(MarketEvent::SpotPrice(SpotPrice {
        source: "chainlink_stream".to_string(),
        symbol,
        price,
        timestamp_ns: observations_ts * 1_000_000_000,
        local_timestamp_ns: now_ns(),
    }))
}

impl ChainlinkStreamMarket {
    /// Decode benchmark_price from a hex-encoded full report.
    ///
    /// The full report is ABI-encoded: report_context (bytes32[3]) + report_blob (bytes).
    /// Within the report blob, benchmark_price is an int192 at a known offset.
    ///
    /// Report V3 blob layout (each field is 32 bytes ABI-encoded):
    ///   [0]  feedId (bytes32)
    ///   [1]  validFromTimestamp (uint32)
    ///   [2]  observationsTimestamp (uint32)
    ///   [3]  nativeFee (uint192)
    ///   [4]  linkFee (uint192)
    ///   [5]  expiresAt (uint32)
    ///   [6]  benchmarkPrice (int192)
    ///   [7]  bid (int192)
    ///   [8]  ask (int192)
    pub fn decode_benchmark_price(full_report_hex: &str) -> Result<f64> {
        let hex_str = full_report_hex.strip_prefix("0x").unwrap_or(full_report_hex);
        let bytes = hex::decode(hex_str)?;

        if bytes.len() < 3 * 32 + 64 {
            return Err(anyhow!("Report too short: {} bytes", bytes.len()));
        }

        let offset = read_uint256_as_usize(&bytes, 96);
        if offset + 32 > bytes.len() {
            return Err(anyhow!("Report blob offset out of bounds (total={}, offset={})",
                bytes.len(), offset));
        }
        let blob_len = read_uint256_as_usize(&bytes, offset);
        let blob_start = offset + 32;
        if blob_start + blob_len > bytes.len() {
            return Err(anyhow!("Report blob data out of bounds (total={}, blob_start={}, blob_len={})",
                bytes.len(), blob_start, blob_len));
        }

        let bp_offset = blob_start + 6 * 32;
        if bp_offset + 32 > bytes.len() {
            return Err(anyhow!("benchmarkPrice offset out of bounds"));
        }

        let bp_bytes = &bytes[bp_offset..bp_offset + 32];
        let bp_i256 = read_int256(bp_bytes);

        Ok(bp_i256 as f64 / 1e18)
    }
}

/// Fetch Chainlink BTC/USD price at a specific timestamp via Data Streams REST API.
/// Returns the benchmark_price from the report closest to the given timestamp.
/// `api_key`/`api_secret`: Chainlink Data Streams credentials.
/// `feed_id`: hex feed ID (e.g. "0x00039d9e...").
/// `timestamp_secs`: Unix timestamp in seconds.
pub fn fetch_price_at_timestamp(
    api_key: &str,
    api_secret: &str,
    feed_id: &str,
    timestamp_secs: u64,
) -> Result<f64> {
    let rest_url = "https://api.dataengine.chain.link";
    let path = format!("/api/v1/reports?feedID={}&timestamp={}", feed_id, timestamp_secs);

    let ts_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis().to_string();
    let body_hash = hex::encode(Sha256::digest(b""));
    let signing_string = format!("GET {} {} {} {}", path, body_hash, api_key, ts_ms);
    let mut mac = Hmac::<Sha256>::new_from_slice(api_secret.as_bytes())
        .map_err(|e| anyhow!("HMAC key error: {}", e))?;
    hmac::Mac::update(&mut mac, signing_string.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());

    let url = format!("{}{}", rest_url, path);
    let api_key_s = api_key.to_string();
    let ts_ms_s = ts_ms.clone();

    let body = crate::async_rt::block_on_runtime(async move {
        let client = crate::async_rt::http_client_auto();
        let resp = client
            .get(&url)
            .header("Authorization", &api_key_s)
            .header("X-Authorization-Timestamp", &ts_ms_s)
            .header("X-Authorization-Signature-SHA256", &signature)
            .send()
            .await
            .map_err(|e| anyhow!("Chainlink REST error: {}", e))?;
        let text = resp.text().await.map_err(|e| anyhow!("body: {}", e))?;
        Ok::<String, anyhow::Error>(text)
    })?;

    let body: serde_json::Value = serde_json::from_str(&body)?;
    let report = body.get("report")
        .ok_or_else(|| anyhow!("No 'report' in response"))?;
    let full_report = report.get("fullReport")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("No 'fullReport' in report"))?;

    ChainlinkStreamMarket::decode_benchmark_price(full_report)
}

fn read_uint256_as_usize(data: &[u8], offset: usize) -> usize {
    let word = &data[offset..offset + 32];
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&word[24..32]);
    u64::from_be_bytes(buf) as usize
}

/// Read an int256 from a 32-byte ABI word as i128.
/// For int192, the value is sign-extended to 256 bits.
fn read_int256(word: &[u8]) -> i128 {
    let negative = word[0] & 0x80 != 0;
    let mut buf = [0u8; 16];
    buf.copy_from_slice(&word[16..32]);
    let magnitude = u128::from_be_bytes(buf);

    if negative {
        -((!magnitude).wrapping_add(1) as i128)
    } else {
        magnitude as i128
    }
}
