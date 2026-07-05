//! Aster REST helpers + public metadata queries.
//!
//! `exchangeInfo` is fetched at startup to build the per-symbol
//! tick-size / step-size map ([`AsterMeta`]); `depth` provides a one-shot
//! book snapshot. Live book and fills come over the WS feeds, not here.
//!
//! All V3 requests (signed and public alike) send params in the **query
//! string** with an empty body — the form the official demo uses — so a
//! single blocking helper covers GET/POST/PUT/DELETE.

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use serde::Deserialize;

use crate::async_rt;

/// Blocking HTTP request with params already encoded into `url`. Returns the
/// response body; non-2xx → error with body text (Aster errors are
/// `{"code":-XXXX,"msg":"…"}`).
pub fn http_request(method: &str, url: &str) -> Result<String> {
    let url = url.to_string();
    let method = method.to_string();
    // HTTP/1.1-only client — Aster's h2 frontend is broken for signed
    // requests: byte-identical orders get a spurious `-2019 Margin is
    // insufficient` over h2 but succeed over h1.1 (curl-verified
    // 2026-07-05). ALPN would negotiate h2, so it must be disabled.
    let client = async_rt::http_client_h1();
    async_rt::block_on_runtime(async move {
        let req = match method.as_str() {
            "GET" => client.get(&url),
            "POST" => client.post(&url),
            "PUT" => client.put(&url),
            "DELETE" => client.delete(&url),
            m => return Err(anyhow!("aster: unsupported HTTP method {}", m)),
        };
        let resp = req
            .header("Content-Type", "application/x-www-form-urlencoded")
            .send()
            .await
            .map_err(|e| anyhow!("{} {}: {}", method, url, e))?;
        let status = resp.status();
        let text = resp.text().await.map_err(|e| anyhow!("read body: {}", e))?;
        if !status.is_success() {
            return Err(anyhow!("{} {} -> {}: {}", method, url, status, text));
        }
        Ok::<String, anyhow::Error>(text)
    })
}

/// `http_request` + JSON parse.
pub fn http_json<T: for<'de> Deserialize<'de>>(method: &str, url: &str) -> Result<T> {
    let text = http_request(method, url)?;
    serde_json::from_str::<T>(&text)
        .map_err(|e| anyhow!("parse {} response: {} — body: {}", url, e, text))
}

// ── exchangeInfo → per-symbol trading rules ──────────────────────

#[derive(Debug, Clone, Deserialize)]
struct ExchangeInfoFilter {
    #[serde(rename = "filterType")]
    filter_type: String,
    #[serde(default, rename = "tickSize")]
    tick_size: Option<String>,
    #[serde(default, rename = "stepSize")]
    step_size: Option<String>,
    #[serde(default, rename = "minQty")]
    min_qty: Option<String>,
    #[serde(default)]
    notional: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ExchangeInfoSymbol {
    symbol: String,
    #[serde(default)]
    status: String,
    #[serde(default, rename = "pricePrecision")]
    price_precision: u32,
    #[serde(default, rename = "quantityPrecision")]
    quantity_precision: u32,
    #[serde(default)]
    filters: Vec<ExchangeInfoFilter>,
}

#[derive(Debug, Clone, Deserialize)]
struct ExchangeInfoResponse {
    symbols: Vec<ExchangeInfoSymbol>,
}

/// Per-symbol trading rules from `exchangeInfo`.
#[derive(Debug, Clone, Default)]
pub struct SymbolRules {
    /// PRICE_FILTER.tickSize (0 → no constraint reported).
    pub tick_size: f64,
    /// LOT_SIZE.stepSize.
    pub step_size: f64,
    /// LOT_SIZE.minQty.
    pub min_qty: f64,
    /// MIN_NOTIONAL.notional (price·qty floor).
    pub min_notional: f64,
    /// Decimal places accepted in `price` params.
    pub price_precision: u32,
    /// Decimal places accepted in `quantity` params.
    pub quantity_precision: u32,
}

/// Symbol → trading rules, from `GET /fapi/v3/exchangeInfo`.
#[derive(Debug, Clone, Default)]
pub struct AsterMeta {
    rules: HashMap<String, SymbolRules>,
}

impl AsterMeta {
    pub fn rules(&self, symbol: &str) -> Option<&SymbolRules> {
        self.rules.get(symbol)
    }
}

/// Fetch `exchangeInfo` and build the symbol → rules map (TRADING symbols
/// keep their rules even if paused — the map is for formatting, not gating).
pub fn fetch_meta(rest_base: &str) -> Result<AsterMeta> {
    let url = format!("{}/fapi/v3/exchangeInfo", rest_base.trim_end_matches('/'));
    let resp: ExchangeInfoResponse = http_json("GET", &url)?;
    let mut rules = HashMap::new();
    for s in resp.symbols {
        let mut r = SymbolRules {
            price_precision: s.price_precision,
            quantity_precision: s.quantity_precision,
            ..Default::default()
        };
        for f in &s.filters {
            match f.filter_type.as_str() {
                "PRICE_FILTER" => {
                    r.tick_size = f.tick_size.as_deref().and_then(|v| v.parse().ok()).unwrap_or(0.0);
                }
                "LOT_SIZE" => {
                    r.step_size = f.step_size.as_deref().and_then(|v| v.parse().ok()).unwrap_or(0.0);
                    r.min_qty = f.min_qty.as_deref().and_then(|v| v.parse().ok()).unwrap_or(0.0);
                }
                "MIN_NOTIONAL" => {
                    r.min_notional = f.notional.as_deref().and_then(|v| v.parse().ok()).unwrap_or(0.0);
                }
                _ => {}
            }
        }
        let _ = &s.status; // informational
        rules.insert(s.symbol, r);
    }
    Ok(AsterMeta { rules })
}

// ── depth snapshot (one-shot; live book comes over WS) ────────────

#[derive(Debug, Clone, Deserialize)]
pub struct DepthSnapshot {
    /// `[price, qty]` string pairs, best-first.
    pub bids: Vec<[String; 2]>,
    pub asks: Vec<[String; 2]>,
}

/// `GET /fapi/v3/depth?symbol=…&limit=…` — used for market-order pricing
/// checks and smoke tests.
pub fn fetch_depth(rest_base: &str, symbol: &str, limit: u32) -> Result<DepthSnapshot> {
    let url = format!(
        "{}/fapi/v3/depth?symbol={}&limit={}",
        rest_base.trim_end_matches('/'),
        symbol,
        limit
    );
    http_json("GET", &url)
}
