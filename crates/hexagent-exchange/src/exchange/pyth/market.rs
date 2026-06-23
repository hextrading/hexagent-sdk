//! Pyth Hermes REST API market feed — polls latest prices every 1s.
//!
//! Endpoint: https://hermes.pyth.network/v2/updates/price/latest
//! Rate limit: ≤30 requests per 10s, so 1 req/s is safe.
//!
//! Subscribed symbols (e.g. "btc/usd", "usdt/usd") are mapped to Pyth feed IDs
//! and fetched in a single batch request.

use anyhow::Result;
use log::{info, warn};
use std::collections::HashMap;
use std::time::Duration;

use crate::exchange::ExchangeMarket;
use crate::types::*;

const HERMES_ENDPOINT: &str = "https://hermes.pyth.network";
const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Known Pyth price feed IDs (hex, with 0x prefix).
/// Full list: https://www.pyth.network/price-feeds
fn known_feed_id(symbol: &str) -> Option<&'static str> {
    match symbol.to_lowercase().as_str() {
        "btc/usd" => Some("0xe62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43"),
        "eth/usd" => Some("0xff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace"),
        "sol/usd" => Some("0xef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d"),
        "usdt/usd" => Some("0x2b89b9dc8fdf9f34709a5b106b472f0f39bb6ca9ce04b0fd7f2e971688e2e53b"),
        "usdc/usd" => Some("0xeaa020c61cc479712813461ce153894a96a6c00b21ed0cfc2798d1f9a9e9c94a"),
        "bnb/usd" => Some("0x2f95862b045670cd22bee3114c39763a4a08beeb663b145d283c31d7d1101c4f"),
        "xrp/usd" => Some("0xec5d399846a9209f3fe5881d70aae9268c94339ff9817c800ea6057718a63dc5"),
        "doge/usd" => Some("0xdcef50dd0a4cd2dcc17e45df1676dcb336a11a61c69df7a0299b0150c672d25c"),
        "avax/usd" => Some("0x93da3352f9f1d105fdfe4971cfa80e9dd777bfc5d0f683ebb6e1294b92137bb7"),
        "link/usd" => Some("0x8ac0c70fff57e9aefdf5edf44b51d62c2d433653cbb2cf5cc06bb115af04d221"),
        _ => None,
    }
}

struct FeedMapping {
    /// Original symbol as subscribed (e.g. "btc/usd")
    symbol: String,
    /// Pyth feed ID (hex)
    feed_id: String,
}

pub struct PythHermesMarket {
    feeds: Vec<FeedMapping>,
    /// Pending events from last poll
    pending: Vec<MarketEvent>,
    /// Whether connect() was called
    connected: bool,
}

impl PythHermesMarket {
    pub fn new() -> Self {
        Self {
            feeds: Vec::new(),
            pending: Vec::new(),
            connected: false,
        }
    }
}

impl ExchangeMarket for PythHermesMarket {
    fn connect(&mut self) -> Result<()> {
        info!("[Pyth] Hermes endpoint: {}", HERMES_ENDPOINT);
        self.connected = true;
        info!("[Pyth] Connected, {} feeds: {:?}", self.feeds.len(),
            self.feeds.iter().map(|f| f.symbol.as_str()).collect::<Vec<_>>());
        Ok(())
    }

    fn subscribe(&mut self, symbols: &[String]) -> Result<()> {
        for sym in symbols {
            let sym_lower = sym.to_lowercase();
            if let Some(feed_id) = known_feed_id(&sym_lower) {
                self.feeds.push(FeedMapping {
                    symbol: sym_lower,
                    feed_id: feed_id.to_string(),
                });
            } else {
                warn!("[Pyth] Unknown symbol '{}', skipping", sym);
            }
        }
        Ok(())
    }

    fn next_event(&mut self) -> Result<Option<MarketEvent>> {
        // Drain pending events first
        if let Some(event) = self.pending.pop() {
            return Ok(Some(event));
        }

        if !self.connected || self.feeds.is_empty() {
            return Ok(None);
        }

        // Sleep to rate limit (1 req/s)
        std::thread::sleep(POLL_INTERVAL);

        // Build batch request URL
        let mut url = format!("{}/v2/updates/price/latest?parsed=true", HERMES_ENDPOINT);
        for feed in &self.feeds {
            url.push_str(&format!("&ids[]={}", feed.feed_id));
        }

        // Route through shared async reqwest client (h2 + keepalive pool).
        let text = match crate::async_rt::blocking_get_text(&url) {
            Ok(t) => t,
            Err(e) => {
                warn!("[Pyth] Request failed: {}", e);
                return Ok(None);
            }
        };
        let body: serde_json::Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(e) => {
                warn!("[Pyth] Parse failed: {}", e);
                return Ok(None);
            }
        };

        // Build feed_id → symbol lookup
        let id_to_symbol: HashMap<&str, &str> = self.feeds.iter()
            .map(|f| (f.feed_id.as_str(), f.symbol.as_str()))
            .collect();

        let parsed = match body.get("parsed").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return Ok(None),
        };

        let local_ts = now_ns();

        for item in parsed {
            let raw_id = match item.get("id").and_then(|v| v.as_str()) {
                Some(id) => id,
                None => continue,
            };
            let feed_id = if raw_id.starts_with("0x") { raw_id.to_string() } else { format!("0x{}", raw_id) };

            let symbol = match id_to_symbol.get(feed_id.as_str()) {
                Some(s) => *s,
                None => continue,
            };

            let price_obj = match item.get("price") {
                Some(p) => p,
                None => continue,
            };

            let price_raw = price_obj.get("price").and_then(|v| v.as_str())
                .and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
            let expo = price_obj.get("expo").and_then(|v| v.as_i64()).unwrap_or(0);
            let publish_time = price_obj.get("publish_time").and_then(|v| v.as_u64()).unwrap_or(0);

            let price = price_raw as f64 * 10f64.powi(expo as i32);

            self.pending.push(MarketEvent::SpotPrice(SpotPrice {
                source: "pyth".to_string(),
                symbol: symbol.to_string(),
                price,
                timestamp_ns: publish_time * 1_000_000_000,
                local_timestamp_ns: local_ts,
            }));
        }

        // Return first event (rest in pending)
        Ok(self.pending.pop())
    }

    fn disconnect(&mut self) {
        self.connected = false;
        info!("[Pyth] Disconnected");
    }

    fn name(&self) -> &str { "pyth" }
}
