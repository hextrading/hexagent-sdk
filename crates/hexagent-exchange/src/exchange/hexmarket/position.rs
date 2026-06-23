use std::collections::HashMap;

use anyhow::{anyhow, Result};
use log::info;
use rust_decimal::prelude::ToPrimitive;

use super::sdk::{HexClient, HexClientConfig};

use crate::account::position::Position;

use super::auth::{api_url_prefix_or_default, resolve_auth};

/// Create a HexClient with L2 credentials resolved from private key or mnemonic.
fn make_client(private_key: &str, mnemonic: &str, api_url_prefix: &str) -> Result<HexClient> {
    let api_url_prefix = api_url_prefix_or_default(api_url_prefix);
    let auth = resolve_auth(private_key, mnemonic, api_url_prefix)?;
    let client = HexClient::new(HexClientConfig {
        api_url: api_url_prefix.to_string(),
    });
    client.set_credentials(&auth.pubkey, auth.credentials);
    Ok(client)
}

/// Fetch current positions from HexMarket API via SDK.
///
/// Returns a map of `outcomeId` → `Position`.
pub fn fetch_positions(private_key: &str, mnemonic: &str, api_url_prefix: &str) -> Result<HashMap<String, Position>> {
    let client = make_client(private_key, mnemonic, api_url_prefix)?;
    info!("[Hexmarket] Fetching positions");

    let sdk_positions = client
        .get_positions()
        .map_err(|e| anyhow!("SDK error: {}", e))?;

    let mut positions = HashMap::new();
    for p in &sdk_positions {
        if p.quantity <= 0 {
            continue;
        }
        positions.insert(
            p.outcome_id.to_string(),
            Position {
                quantity: p.quantity as f64,
                avg_price: p.avg_price.and_then(|d| d.to_f64()).unwrap_or(0.0),
                current_value: 0.0,
            },
        );
    }

    info!(
        "[Hexmarket] Fetched {} positions ({} raw records)",
        positions.len(),
        sdk_positions.len(),
    );
    for (outcome_id, pos) in &positions {
        info!(
            "[Hexmarket]   {} qty={:.4} avg_price={:.4}",
            outcome_id, pos.quantity, pos.avg_price,
        );
    }

    Ok(positions)
}

/// Open order info for syncing local state.
#[derive(Debug, Clone, serde::Serialize)]
pub struct OpenOrderInfo {
    pub outcome_id: String,
    pub client_order_id: String,
    pub side: String,
    pub price: f64,
    pub quantity: f64,
    pub remaining_quantity: f64,
}

/// Fetch all open orders from HexMarket API.
pub fn fetch_open_orders(private_key: &str, mnemonic: &str, api_url_prefix: &str) -> Result<Vec<OpenOrderInfo>> {
    use rust_decimal::prelude::ToPrimitive;
    let client = make_client(private_key, mnemonic, api_url_prefix)?;
    info!("[Hexmarket] Fetching open orders");

    let orders = client
        .get_open_orders(None)
        .map_err(|e| anyhow!("SDK error: {}", e))?;

    let infos: Vec<OpenOrderInfo> = orders.iter().map(|o| {
        OpenOrderInfo {
            outcome_id: o.outcome_id.to_string(),
            client_order_id: o.client_order_id.clone().unwrap_or_default(),
            side: o.side.clone(),
            price: o.price.to_f64().unwrap_or(0.0),
            quantity: o.quantity as f64,
            remaining_quantity: o.remaining_quantity as f64,
        }
    }).collect();

    info!("[Hexmarket] {} open orders", infos.len());
    for o in &infos {
        info!(
            "[Hexmarket]   {} {} {} qty={} remaining={} @ {} coid={}",
            o.outcome_id, o.side, o.price, o.quantity, o.remaining_quantity,
            o.price, o.client_order_id,
        );
    }

    Ok(infos)
}

/// Fetch USDC balance from HexMarket API via SDK.
pub fn fetch_balance(private_key: &str, mnemonic: &str, api_url_prefix: &str) -> Result<f64> {
    let client = make_client(private_key, mnemonic, api_url_prefix)?;
    info!("[Hexmarket] Fetching balance");

    let balance = client
        .get_balance()
        .map_err(|e| anyhow!("SDK error: {}", e))?;

    let available = balance.usdc_balance as f64 / 1_000_000.0;
    info!("[Hexmarket] Available balance: {:.4}", available);
    Ok(available)
}
