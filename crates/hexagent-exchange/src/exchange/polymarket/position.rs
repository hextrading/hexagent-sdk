use std::collections::HashMap;

use anyhow::Result;
use log::info;
use serde::Deserialize;

use crate::account::position::Position;

const DATA_API_BASE: &str = "https://data-api.polymarket.com";

/// Raw position record from Polymarket Data API.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct ApiPosition {
    /// CLOB token ID (decimal string representing a 256-bit ERC1155 id).
    /// This is the "symbol" we key by internally — one entry per outcome.
    #[serde(default)]
    asset: String,
    condition_id: String,
    size: f64,
    avg_price: f64,
    /// Mark-to-market USDC value = size × cur_price. For settled events the
    /// API returns cur_price = 1 (winner) or 0 (loser), so current_value
    /// reflects the redeemable dollar value directly.
    #[serde(default)]
    current_value: f64,
    outcome: String,
    title: Option<String>,
}

/// Fetch current positions from Polymarket Data API.
///
/// Returns a map of `clob_token_id` → `Position`. Each outcome (Up / Down /
/// Yes / No / etc.) is a separate CLOB token with its own id, so they get
/// separate entries — we do NOT collapse by conditionId.
///
/// API: `GET https://data-api.polymarket.com/positions?user={wallet}&sizeThreshold=0`
pub fn fetch_positions(wallet_address: &str) -> Result<HashMap<String, Position>> {
    let url = format!(
        "{}/positions?user={}&sizeThreshold=0&limit=500",
        DATA_API_BASE, wallet_address,
    );
    info!("[Polymarket] Fetching positions for {}", wallet_address);

    // Route through the shared async runtime + HTTP/2 client.
    let client = crate::async_rt::http_client();
    let resp: Vec<ApiPosition> = crate::async_rt::block_on_runtime(async move {
        let r = client.get(&url).send().await
            .map_err(|e| anyhow::anyhow!("fetch_positions: {}", e))?;
        r.json::<Vec<ApiPosition>>().await
            .map_err(|e| anyhow::anyhow!("fetch_positions parse: {}", e))
    })?;

    let mut positions = HashMap::new();
    for p in &resp {
        if p.size <= 0.0 {
            continue;
        }
        // Skip records without an asset field (shouldn't happen against
        // the public data-api, but defensive).
        if p.asset.is_empty() {
            log::warn!("[Polymarket] Position record missing 'asset' field — skipped (cid={} outcome={})",
                p.condition_id, p.outcome);
            continue;
        }
        // Key by CLOB token id — matches what BinaryOption.clob_token_ids
        // uses so downstream `PositionManager::get_quantity(token_id)`
        // lookups hit.
        positions.insert(
            p.asset.clone(),
            Position {
                quantity: p.size,
                avg_price: p.avg_price,
                current_value: p.current_value,
            },
        );
    }

    info!(
        "[Polymarket] Fetched {} positions ({} raw records)",
        positions.len(),
        resp.len(),
    );
    for (token_id, pos) in &positions {
        let short: String = token_id.chars().take(16).collect();
        info!(
            "[Polymarket]   token={}... qty={:.4} avg_price={:.4}",
            short, pos.quantity, pos.avg_price,
        );
    }

    Ok(positions)
}

/// Raw balance record from Polymarket Data API.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiBalance {
    #[serde(default)]
    balance: f64,
}

/// USDC.e on Polygon — v1 CLOB collateral (6 decimals).
pub const USDC_E_ADDRESS: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
/// pUSD on Polygon — v2 CLOB collateral (6 decimals).
pub const PUSD_ADDRESS:   &str = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB";

/// Pick the active collateral token address for the current CLOB
/// version. v1 trades against USDC.e, v2 against pUSD. Caller passes
/// the flag derived from `ExchangeConfig.clob_version`
/// (`ClobVersion::V2 == is_v2`).
pub fn active_collateral_token(is_v2: bool) -> &'static str {
    if is_v2 { PUSD_ADDRESS } else { USDC_E_ADDRESS }
}

/// Fetch balance AND positions concurrently via the async runtime.
///
/// The balance read picks **USDC.e** for v1 and **pUSD** for v2 based
/// on `is_v2`. Callers with direct access to `ClobVersion` should
/// forward `version == V2`; CLI / tooling callers without a config
/// context can use the legacy bare wrappers below which default to
/// v1 (USDC.e).
///
/// Nearly halves the critical path vs calling the two reads
/// sequentially — the on-chain `eth_call` and the data-api `positions`
/// request run on the same tokio runtime and pipeline their TLS+request
/// time. Either side failing yields defaults (0.0 / empty map) rather
/// than propagating.
pub fn fetch_balance_and_positions(
    wallet_address: &str,
) -> (f64, HashMap<String, Position>) {
    // Default v1 (USDC.e). Kept for backward compatibility with CLI
    // callers that don't have a CLOB-version context. `strategy.rs`
    // and `live_position.rs` should use the `_versioned` variant.
    fetch_balance_and_positions_versioned(wallet_address, /*is_v2=*/ false)
}

/// Explicit-version variant. Picks USDC.e or pUSD based on `is_v2`.
pub fn fetch_balance_and_positions_versioned(
    wallet_address: &str,
    is_v2: bool,
) -> (f64, HashMap<String, Position>) {
    let token = active_collateral_token(is_v2);
    let wb = wallet_address.to_string();
    let tok = token.to_string();
    let t_bal = std::thread::Builder::new()
        .name("poly-fetch-balance".into())
        .spawn(move || fetch_balance_for_token(&wb, &tok))
        .expect("spawn fetch-balance thread");
    let wp = wallet_address.to_string();
    let t_pos = std::thread::Builder::new()
        .name("poly-fetch-positions".into())
        .spawn(move || fetch_positions(&wp))
        .expect("spawn fetch-positions thread");

    let balance = t_bal.join().ok().and_then(|r| r.ok()).unwrap_or_else(|| {
        log::warn!("[Polymarket] fetch_balance failed — using 0");
        0.0
    });
    let positions = t_pos.join().ok().and_then(|r| r.ok()).unwrap_or_else(|| {
        log::warn!("[Polymarket] fetch_positions failed — using empty");
        HashMap::new()
    });
    (balance, positions)
}

/// Legacy bare balance fetch — returns USDC.e. Kept for CLI callers
/// (`wallet.rs` et al.) that don't track CLOB version. Strategy and
/// live-position paths should use `fetch_balance_for_token` directly
/// with `active_collateral_token(is_v2)`.
pub fn fetch_balance(wallet_address: &str) -> Result<f64> {
    fetch_balance_for_token(wallet_address, USDC_E_ADDRESS)
}

/// Fetch an ERC-20 balance from Polygon via `eth_call balanceOf`,
/// falling back to Polymarket's `/balance` data-api when the on-chain
/// read fails (the API only knows USDC.e — fallback is skipped for
/// non-USDC.e tokens).
pub fn fetch_balance_for_token(wallet_address: &str, token: &str) -> Result<f64> {
    info!("[Polymarket] Fetching balance for {} (token={})", wallet_address, token);

    // Primary: on-chain balanceOf(address) via Polygon RPC
    let selector: [u8; 4] = [0x70, 0xa0, 0x82, 0x31]; // balanceOf(address)
    let mut calldata = Vec::with_capacity(4 + 32);
    calldata.extend_from_slice(&selector);
    let addr_hex = wallet_address.strip_prefix("0x").unwrap_or(wallet_address);
    let addr_bytes = hex::decode(addr_hex).unwrap_or_else(|_| vec![0u8; 20]);
    let mut padded = [0u8; 32];
    let start = 32 - addr_bytes.len().min(32);
    padded[start..].copy_from_slice(&addr_bytes[..addr_bytes.len().min(32)]);
    calldata.extend_from_slice(&padded);
    let data = format!("0x{}", hex::encode(&calldata));

    if let Some(result) = super::deploy_wallet::eth_call(token, &data) {
        let hex_str = result.strip_prefix("0x").unwrap_or(&result);
        let trimmed = hex_str.trim_start_matches('0');
        let wei = if trimmed.is_empty() { 0u128 } else {
            u128::from_str_radix(trimmed, 16).unwrap_or(0)
        };
        let balance = wei as f64 / 1_000_000.0; // 6 decimals for both USDC.e and pUSD
        info!("[Polymarket] Balance: {:.4} (on-chain, token={})", balance, token);
        return Ok(balance);
    }

    // Fallback data-api: only wired for USDC.e (v1). pUSD / other
    // tokens skip straight to the error.
    if token.eq_ignore_ascii_case(USDC_E_ADDRESS) {
        let url = format!("{}/balance?user={}", DATA_API_BASE, wallet_address);
        let client = crate::async_rt::http_client();
        let balance = crate::async_rt::block_on_runtime(async move {
            let r = client.get(&url).send().await
                .map_err(|e| anyhow::anyhow!("fetch_balance: {}", e))?;
            if r.status().as_u16() == 404 {
                return Ok(0.0);
            }
            if !r.status().is_success() {
                return Err(anyhow::anyhow!("fetch_balance: status {}", r.status()));
            }
            let bal: ApiBalance = r.json().await
                .map_err(|e| anyhow::anyhow!("fetch_balance parse: {}", e))?;
            Ok::<f64, anyhow::Error>(bal.balance)
        })?;
        info!("[Polymarket] Balance: {:.4} (data-api, USDC.e)", balance);
        return Ok(balance);
    }

    Err(anyhow::anyhow!(
        "eth_call balanceOf failed for token {} and no data-api fallback for non-USDC.e tokens",
        token,
    ))
}
