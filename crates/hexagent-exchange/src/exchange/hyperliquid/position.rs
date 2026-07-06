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
    /// Free collateral withdrawable from the perp side (= perp margin not tied
    /// up by positions/orders). Does NOT include the spot USDC balance.
    #[serde(default)]
    withdrawable: String,
}

#[derive(Debug, Clone, Deserialize)]
struct SpotBalance {
    coin: String,
    total: String,
}

#[derive(Debug, Clone, Deserialize)]
struct SpotState {
    #[serde(default)]
    balances: Vec<SpotBalance>,
}

/// Available USDC for placing orders in HL's **unified spot/perp account**.
///
/// HL shares USDC collateral across spot and perp, so a perp order draws on
/// the spot USDC too. The tradeable balance is therefore the free perp margin
/// (`withdrawable`) plus the spot USDC balance — **not** `marginSummary
/// .accountValue`, which is total equity and includes open-position
/// mark-to-market value. Use this to decide whether the account can place.
pub fn fetch_balance(info_url: &str, account_address: &str) -> Result<f64> {
    let perp: ClearinghouseState = post_info(
        info_url,
        json!({ "type": "clearinghouseState", "user": account_address }),
    )?;
    let spot: SpotState = post_info(
        info_url,
        json!({ "type": "spotClearinghouseState", "user": account_address }),
    )?;
    let perp_free: f64 = perp.withdrawable.parse().unwrap_or(0.0);
    let spot_usdc: f64 = spot
        .balances
        .iter()
        .find(|b| b.coin == "USDC")
        .and_then(|b| b.total.parse().ok())
        .unwrap_or(0.0);
    let total = perp_free + spot_usdc;
    info!(
        "[Hyperliquid] available USDC={:.4} (perp_free={:.4} + spot_usdc={:.4}) for {}",
        total, perp_free, spot_usdc, account_address,
    );
    Ok(total)
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
