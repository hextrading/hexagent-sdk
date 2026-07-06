//! Lighter positions via REST `account?by=index`.
//!
//! Position size arrives as an unsigned string plus a `sign` field
//! (1 = long, -1 = short); we fold the sign into `Position.quantity` so the
//! strategy's inventory skew reads directly off it. Keyed by symbol ("BTC").

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use log::info;
use serde::Deserialize;

use crate::account::position::Position;

use super::info::get_json;

#[derive(Debug, Clone, Deserialize)]
struct LighterPosition {
    symbol: String,
    #[serde(default)]
    sign: i8,
    position: String,
    #[serde(default)]
    avg_entry_price: String,
    #[serde(default)]
    position_value: String,
}

#[derive(Debug, Clone, Deserialize)]
struct LighterAccount {
    #[serde(default)]
    positions: Vec<LighterPosition>,
    #[serde(default)]
    available_balance: String,
    #[serde(default)]
    collateral: String,
}

/// Account balance snapshot (USDC).
#[derive(Debug, Clone, Default)]
pub struct Balance {
    /// Free collateral (not tied up as margin).
    pub available_balance: f64,
    /// Total collateral.
    pub collateral: f64,
}

/// Fetch the USDC balance for `account_index`.
pub fn fetch_balance(rest_base: &str, account_index: i64) -> Result<Balance> {
    let resp: AccountResponse = get_json(format!(
        "{}/api/v1/account?by=index&value={}",
        rest_base, account_index
    ))?;
    let account = resp
        .accounts
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("lighter: account {} not found", account_index))?;
    Ok(Balance {
        available_balance: account.available_balance.parse().unwrap_or(0.0),
        collateral: account.collateral.parse().unwrap_or(0.0),
    })
}

#[derive(Debug, Clone, Deserialize)]
struct AccountResponse {
    accounts: Vec<LighterAccount>,
}

/// Fetch current perp positions for `account_index`. Map key = symbol.
pub fn fetch_positions(rest_base: &str, account_index: i64) -> Result<HashMap<String, Position>> {
    let resp: AccountResponse = get_json(format!(
        "{}/api/v1/account?by=index&value={}",
        rest_base, account_index
    ))?;
    let account = resp
        .accounts
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("lighter: account {} not found", account_index))?;

    let mut positions = HashMap::new();
    for p in &account.positions {
        let qty_abs: f64 = p.position.parse().unwrap_or(0.0);
        if qty_abs == 0.0 {
            continue;
        }
        let qty = if p.sign < 0 { -qty_abs } else { qty_abs };
        let avg_price: f64 = p.avg_entry_price.parse().unwrap_or(0.0);
        let current_value: f64 = p.position_value.parse().unwrap_or(0.0);
        positions.insert(
            p.symbol.clone(),
            Position { quantity: qty, avg_price, current_value },
        );
    }

    info!(
        "[Lighter] Fetched {} positions for account {}",
        positions.len(),
        account_index,
    );
    for (sym, pos) in &positions {
        info!(
            "[Lighter]   {} qty={:.6} entryPx={:.2} value={:.2}",
            sym, pos.quantity, pos.avg_price, pos.current_value,
        );
    }
    Ok(positions)
}
