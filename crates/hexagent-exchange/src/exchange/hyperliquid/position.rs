//! Hyperliquid positions via REST `clearinghouseState`.
//!
//! `szi` is the **signed** net position (negative = short). We keep the sign
//! in `Position.quantity` so the strategy's inventory skew reads directly off
//! it. Keyed by coin (e.g. "BTC").

use std::collections::HashMap;

use anyhow::Result;
use log::info;
use serde::Deserialize;
use serde_json::json;

use crate::account::position::Position;

use super::info::post_info;

#[derive(Debug, Clone, Deserialize)]
struct HlPosition {
    coin: String,
    szi: String,
    #[serde(rename = "entryPx")]
    entry_px: Option<String>,
    #[serde(rename = "positionValue")]
    position_value: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct AssetPosition {
    position: HlPosition,
}

#[derive(Debug, Clone, Deserialize)]
struct ClearinghouseState {
    #[serde(rename = "assetPositions")]
    asset_positions: Vec<AssetPosition>,
}

/// Fetch current perp positions for `account_address`. Map key = coin.
pub fn fetch_positions(info_url: &str, account_address: &str) -> Result<HashMap<String, Position>> {
    let state: ClearinghouseState = post_info(
        info_url,
        json!({ "type": "clearinghouseState", "user": account_address }),
    )?;

    let mut positions = HashMap::new();
    for ap in &state.asset_positions {
        let p = &ap.position;
        let qty: f64 = p.szi.parse().unwrap_or(0.0);
        if qty == 0.0 {
            continue;
        }
        let avg_price = p.entry_px.as_deref().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let current_value = p
            .position_value
            .as_deref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        positions.insert(
            p.coin.clone(),
            Position { quantity: qty, avg_price, current_value },
        );
    }

    info!(
        "[Hyperliquid] Fetched {} positions for {}",
        positions.len(),
        account_address,
    );
    for (coin, pos) in &positions {
        info!(
            "[Hyperliquid]   {} szi={:.6} entryPx={:.2} value={:.2}",
            coin, pos.quantity, pos.avg_price, pos.current_value,
        );
    }
    Ok(positions)
}
