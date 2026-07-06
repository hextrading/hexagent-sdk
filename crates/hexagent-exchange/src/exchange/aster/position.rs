//! Aster positions via REST `GET /fapi/v3/positionRisk` (signed).
//!
//! `positionAmt` is the **signed** net position (negative = short). We keep
//! the sign in `Position.quantity` so the strategy's inventory skew reads
//! directly off it. Keyed by symbol (e.g. "BTCUSDT").

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use log::info;
use serde::Deserialize;

use crate::account::position::Position;

use super::auth::AsterAuth;
use super::info::http_request;
use super::signer::signed_query;

#[derive(Debug, Clone, Deserialize)]
struct PositionRisk {
    symbol: String,
    #[serde(rename = "positionAmt")]
    position_amt: String,
    #[serde(default, rename = "entryPrice")]
    entry_price: String,
    #[serde(default, rename = "markPrice")]
    mark_price: String,
}

/// Fetch current perp positions for the account behind `auth`. Map key =
/// symbol. Zero positions are skipped.
pub fn fetch_positions(auth: &AsterAuth) -> Result<HashMap<String, Position>> {
    let query = signed_query(auth, Vec::new())?;
    let url = format!("{}?{}", auth.fapi_url("positionRisk"), query);
    let text = http_request("GET", &url)?;
    let rows: Vec<PositionRisk> = serde_json::from_str(&text)
        .map_err(|e| anyhow!("parse positionRisk: {} — body: {}", e, text))?;

    let mut positions = HashMap::new();
    for p in &rows {
        let qty: f64 = p.position_amt.parse().unwrap_or(0.0);
        if qty == 0.0 {
            continue;
        }
        let avg_price: f64 = p.entry_price.parse().unwrap_or(0.0);
        let mark: f64 = p.mark_price.parse().unwrap_or(0.0);
        positions.insert(
            p.symbol.clone(),
            Position { quantity: qty, avg_price, current_value: qty * mark },
        );
    }

    info!("[Aster] Fetched {} positions for user {}", positions.len(), auth.user_address);
    for (sym, pos) in &positions {
        info!(
            "[Aster]   {} amt={:.6} entry={:.2} value={:.2}",
            sym, pos.quantity, pos.avg_price, pos.current_value,
        );
    }
    Ok(positions)
}

// ── balances ──────────────────────────────────────────────────────

/// One asset's wallet balance from `GET /fapi/v3/balance` (signed).
#[derive(Debug, Clone, Deserialize)]
pub struct AsterBalance {
    pub asset: String,
    /// Wallet balance (string decimal, exchange precision).
    pub balance: String,
    /// Balance available for new orders (margin not locked).
    #[serde(default, rename = "availableBalance")]
    pub available_balance: String,
}

/// Fetch per-asset futures wallet balances for the account behind `auth`.
pub fn fetch_balances(auth: &AsterAuth) -> Result<Vec<AsterBalance>> {
    let query = signed_query(auth, Vec::new())?;
    let url = format!("{}?{}", auth.fapi_url("balance"), query);
    let text = http_request("GET", &url)?;
    serde_json::from_str(&text).map_err(|e| anyhow!("parse balance: {} — body: {}", e, text))
}
