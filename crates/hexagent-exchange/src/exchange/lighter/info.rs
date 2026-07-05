//! Lighter REST queries (public, unsigned GET).
//!
//! Used at startup for market metadata (`orderBookDetails` → market index +
//! price/size decimals) and nonce sync (`nextNonce`). Live book and fills
//! come over the WS feed, not here.
//!
//! All zkLighter REST responses wrap payloads in `{code, message?, ...}`
//! with `code == 200` on success.

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use serde::Deserialize;

use crate::async_rt;

/// Blocking GET returning the parsed JSON body after the `code` check.
pub fn get_json<T: for<'de> Deserialize<'de> + Send + 'static>(url: String) -> Result<T> {
    let client = async_rt::http_client_auto();
    async_rt::block_on_runtime(async move {
        let resp = client
            .get(&url)
            .send()
            .await
            .map_err(|e| anyhow!("GET {}: {}", url, e))?;
        let status = resp.status();
        let text = resp.text().await.map_err(|e| anyhow!("read body: {}", e))?;
        if !status.is_success() {
            return Err(anyhow!("GET {} -> {}: {}", url, status, text));
        }
        let code = serde_json::from_str::<ResultCode>(&text)
            .map(|r| r.code)
            .unwrap_or(200);
        if code != 200 {
            return Err(anyhow!("GET {} -> app code {}: {}", url, code, text));
        }
        serde_json::from_str::<T>(&text)
            .map_err(|e| anyhow!("parse {} response: {} — body: {}", url, e, text))
    })
}

#[derive(Debug, Deserialize)]
struct ResultCode {
    #[serde(default = "default_code")]
    code: i32,
}
fn default_code() -> i32 {
    200
}

// ── market metadata ───────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct OrderBookDetail {
    pub symbol: String,
    pub market_id: i16,
    #[serde(default)]
    pub status: String,
    /// Price decimals: wire price = price * 10^price_decimals (u32).
    pub price_decimals: u32,
    /// Size decimals: wire base amount = size * 10^size_decimals (i64).
    pub size_decimals: u32,
    #[serde(default)]
    pub min_base_amount: String,
    #[serde(default)]
    pub min_quote_amount: String,
}

#[derive(Debug, Deserialize)]
struct OrderBookDetailsResponse {
    order_book_details: Vec<OrderBookDetail>,
}

/// Market metadata: symbol (e.g. "BTC") → market index + decimals.
#[derive(Debug, Clone, Default)]
pub struct LighterMeta {
    markets: HashMap<String, OrderBookDetail>,
}

impl LighterMeta {
    pub fn market(&self, symbol: &str) -> Option<&OrderBookDetail> {
        self.markets.get(symbol)
    }
    pub fn market_index(&self, symbol: &str) -> Option<i16> {
        self.markets.get(symbol).map(|m| m.market_id)
    }
    /// Reverse lookup for the WS feeds (market_id → symbol).
    pub fn symbol_for(&self, market_id: i16) -> Option<&str> {
        self.markets
            .values()
            .find(|m| m.market_id == market_id)
            .map(|m| m.symbol.as_str())
    }
}

/// Fetch all order books and build the symbol → metadata map.
pub fn fetch_meta(rest_base: &str) -> Result<LighterMeta> {
    let resp: OrderBookDetailsResponse =
        get_json(format!("{}/api/v1/orderBookDetails", rest_base))?;
    let mut markets = HashMap::new();
    for m in resp.order_book_details {
        markets.insert(m.symbol.clone(), m);
    }
    Ok(LighterMeta { markets })
}

// ── nonce ─────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct NextNonceResponse {
    nonce: i64,
}

/// Next usable nonce for `(account_index, api_key_index)`.
pub fn fetch_next_nonce(rest_base: &str, account_index: i64, api_key_index: u8) -> Result<i64> {
    let resp: NextNonceResponse = get_json(format!(
        "{}/api/v1/nextNonce?account_index={}&api_key_index={}",
        rest_base, account_index, api_key_index
    ))?;
    Ok(resp.nonce)
}
