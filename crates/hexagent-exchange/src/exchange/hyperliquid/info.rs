//! Hyperliquid REST `/info` queries (public, unsigned POST).
//!
//! Used at startup to build the `coin → asset index` + `szDecimals` map
//! (`meta`), and by [`super::position`] for `clearinghouseState`. Live book
//! and fills come over the WS feed, not here.

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use serde::Deserialize;
use serde_json::json;

use crate::async_rt;

/// Blocking POST to `/info` with a JSON body, returning the parsed response.
pub fn post_info<T: for<'de> Deserialize<'de> + Send + 'static>(
    info_url: &str,
    body: serde_json::Value,
) -> Result<T> {
    let url = info_url.to_string();
    let client = async_rt::http_client_query();
    async_rt::block_on_runtime(async move {
        let resp = client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| anyhow!("POST {}: {}", url, e))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| anyhow!("read body: {}", e))?;
        if !status.is_success() {
            return Err(anyhow!("POST {} -> {}: {}", url, status, text));
        }
        serde_json::from_str::<T>(&text)
            .map_err(|e| anyhow!("parse {} response: {} — body: {}", url, e, text))
    })
}

// ── meta ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
struct MetaAsset {
    name: String,
    #[serde(rename = "szDecimals")]
    sz_decimals: u32,
}

#[derive(Debug, Clone, Deserialize)]
struct MetaResponse {
    universe: Vec<MetaAsset>,
}

/// Perp universe: coin name → asset index and size decimals.
#[derive(Debug, Clone, Default)]
pub struct HlMeta {
    coin_to_asset: HashMap<String, u32>,
    sz_decimals: HashMap<String, u32>,
}

impl HlMeta {
    pub fn asset_index(&self, coin: &str) -> Option<u32> {
        self.coin_to_asset.get(coin).copied()
    }
    pub fn sz_decimals(&self, coin: &str) -> Option<u32> {
        self.sz_decimals.get(coin).copied()
    }
}

/// Fetch the perpetuals `meta` and build the coin→index / szDecimals map.
pub fn fetch_meta(info_url: &str) -> Result<HlMeta> {
    let resp: MetaResponse = post_info(info_url, json!({ "type": "meta" }))?;
    let mut coin_to_asset = HashMap::new();
    let mut sz_decimals = HashMap::new();
    for (i, a) in resp.universe.iter().enumerate() {
        coin_to_asset.insert(a.name.clone(), i as u32);
        sz_decimals.insert(a.name.clone(), a.sz_decimals);
    }
    Ok(HlMeta { coin_to_asset, sz_decimals })
}

// ── l2 book (snapshot; live comes over WS) ────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct L2Level {
    pub px: String,
    pub sz: String,
    #[allow(dead_code)]
    pub n: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct L2Book {
    pub coin: String,
    /// `[bids, asks]`, each best-first.
    pub levels: Vec<Vec<L2Level>>,
    pub time: u64,
}

/// Fetch a one-shot L2 book snapshot (used by the `hexbot market` smoke path).
pub fn fetch_l2_book(info_url: &str, coin: &str) -> Result<L2Book> {
    post_info(info_url, json!({ "type": "l2Book", "coin": coin }))
}
