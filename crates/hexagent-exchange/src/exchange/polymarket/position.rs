use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, Result};
use futures_util::{stream, StreamExt};
use log::info;
use num_bigint::BigUint;
use num_traits::ToPrimitive;
use serde::Deserialize;

use crate::account::position::Position;

const DATA_API_BASE: &str = "https://data-api.polymarket.com";
const CLOB_API_BASE: &str = "https://clob.polymarket.com";

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
    #[serde(default)]
    redeemable: bool,
}

#[derive(Debug, Deserialize)]
struct ClobMarketResolution {
    #[serde(default, alias = "conditionId")]
    condition_id: String,
    #[serde(default)]
    closed: bool,
    #[serde(default)]
    tokens: Vec<ClobMarketToken>,
}

#[derive(Debug, Deserialize)]
struct ClobMarketToken {
    #[serde(default, alias = "tokenId")]
    token_id: String,
    #[serde(default)]
    winner: Option<bool>,
}

#[derive(Debug)]
pub struct PositionSnapshot {
    pub positions: HashMap<String, Position>,
    pub settled_token_values: HashMap<String, f64>,
}

fn position_snapshot_from_rows(resp: &[ApiPosition]) -> PositionSnapshot {
    let resolved_conditions: HashSet<&str> = resp.iter()
        .filter(|position| position.redeemable)
        .map(|position| position.condition_id.as_str())
        .collect();
    let mut positions = HashMap::new();
    let mut settled_token_values = HashMap::new();
    for p in resp {
        if p.size <= 0.0 { continue; }
        if p.asset.is_empty() {
            log::warn!("[Polymarket] Position record missing 'asset' field — skipped (cid={} outcome={})",
                p.condition_id, p.outcome);
            continue;
        }
        positions.insert(p.asset.clone(), Position {
            quantity: p.size,
            avg_price: p.avg_price,
            current_value: p.current_value,
        });
        if resolved_conditions.contains(p.condition_id.as_str()) {
            let unit_value = p.current_value / p.size;
            let settled_value = if unit_value.is_finite() && unit_value.abs() <= 1e-8 {
                Some(0.0)
            } else if unit_value.is_finite() && (unit_value - 1.0).abs() <= 1e-8 {
                Some(1.0)
            } else {
                None
            };
            if let Some(value) = settled_value {
                settled_token_values.insert(p.asset.clone(), value);
            }
        }
    }
    PositionSnapshot { positions, settled_token_values }
}

fn authoritative_resolution(
    expected_condition_id: &str,
    market: ClobMarketResolution,
) -> Result<Option<HashMap<String, f64>>> {
    if market.condition_id.trim() != expected_condition_id.trim() {
        return Err(anyhow!(
            "settlement condition mismatch: expected {}, got {}",
            expected_condition_id,
            market.condition_id,
        ));
    }
    if !market.closed {
        return Ok(None);
    }
    if market.tokens.len() != 2 {
        return Err(anyhow!(
            "closed binary market {} returned {} tokens",
            expected_condition_id,
            market.tokens.len(),
        ));
    }

    let mut values = HashMap::with_capacity(2);
    let mut winners = 0usize;
    for token in market.tokens {
        let token_id = token.token_id.trim();
        if token_id.is_empty() {
            return Err(anyhow!("closed market {} returned an empty token id", expected_condition_id));
        }
        let winner = token.winner.ok_or_else(|| anyhow!(
            "closed market {} has no authoritative winner flag for token {}",
            expected_condition_id,
            token_id,
        ))?;
        if winner { winners += 1; }
        if values.insert(token_id.to_string(), if winner { 1.0 } else { 0.0 }).is_some() {
            return Err(anyhow!("closed market {} returned duplicate token ids", expected_condition_id));
        }
    }
    if winners != 1 {
        return Err(anyhow!(
            "closed market {} returned {} winning tokens",
            expected_condition_id,
            winners,
        ));
    }
    Ok(Some(values))
}

async fn fetch_authoritative_resolutions(
    condition_ids: HashSet<String>,
) -> HashMap<String, f64> {
    let client = crate::async_rt::http_client();
    let results = stream::iter(condition_ids.into_iter().map(|condition_id| {
        let client = client.clone();
        async move {
            let url = format!("{}/markets/{}", CLOB_API_BASE, condition_id);
            let response = client.get(&url).send().await
                .map_err(|error| anyhow!("fetch settlement {}: {}", condition_id, error))?;
            if !response.status().is_success() {
                return Err(anyhow!(
                    "fetch settlement {}: status {}",
                    condition_id,
                    response.status(),
                ));
            }
            let market = response.json::<ClobMarketResolution>().await
                .map_err(|error| anyhow!("parse settlement {}: {}", condition_id, error))?;
            authoritative_resolution(&condition_id, market)
        }
    }))
    .buffer_unordered(8)
    .collect::<Vec<_>>()
    .await;

    let mut values = HashMap::new();
    for result in results {
        match result {
            Ok(Some(resolution)) => values.extend(resolution),
            Ok(None) => {}
            Err(error) => log::warn!(
                "[Polymarket] Authoritative settlement lookup unavailable; keeping provisional value: {}",
                error,
            ),
        }
    }
    values
}

/// Fetch current positions from Polymarket Data API.
///
/// Returns a map of `clob_token_id` → `Position`. Each outcome (Up / Down /
/// Yes / No / etc.) is a separate CLOB token with its own id, so they get
/// separate entries — we do NOT collapse by conditionId.
///
/// API: `GET https://data-api.polymarket.com/positions?user={wallet}&sizeThreshold=0`
pub fn fetch_position_snapshot(wallet_address: &str) -> Result<PositionSnapshot> {
    info!("[Polymarket] Fetching positions for {}", wallet_address);

    // Route through the shared async runtime + HTTP/2 client.
    let client = crate::async_rt::http_client();
    let wallet = wallet_address.to_string();
    let resp: Vec<ApiPosition> = crate::async_rt::block_on_runtime(async move {
        const PAGE_SIZE: usize = 500;
        const MAX_PAGES: usize = 100;
        let mut all = Vec::new();
        for page in 0..MAX_PAGES {
            let offset = page * PAGE_SIZE;
            let url = format!(
                "{}/positions?user={}&sizeThreshold=0&limit={}&offset={}",
                DATA_API_BASE, wallet, PAGE_SIZE, offset,
            );
            let r = client.get(&url).send().await.map_err(|e| {
                anyhow::anyhow!("fetch_positions page={} offset={}: {}", page + 1, offset, e)
            })?;
            if !r.status().is_success() {
                return Err(anyhow::anyhow!(
                    "fetch_positions page={} offset={}: status {}",
                    page + 1, offset, r.status(),
                ));
            }
            let mut rows = r.json::<Vec<ApiPosition>>().await.map_err(|e| {
                anyhow::anyhow!(
                    "fetch_positions parse page={} offset={}: {}",
                    page + 1, offset, e,
                )
            })?;
            let complete = rows.len() < PAGE_SIZE;
            all.append(&mut rows);
            if complete { return Ok(all); }
        }
        Err(anyhow::anyhow!(
            "fetch_positions exceeded {} pages; refusing a potentially incomplete authoritative snapshot",
            MAX_PAGES,
        ))
    })?;

    // Query every condition represented by a non-zero historical position.
    // This is deliberately broader than `redeemable`: after a new process
    // starts, Data API metadata can be incomplete or rounded, while the CLOB
    // market endpoint still carries the final winner flags. Failed/active
    // lookups remain provisional and are retried on the next account refresh.
    let conditions: HashSet<String> = resp.iter()
        .filter(|position| position.size.is_finite() && position.size > 0.0)
        .map(|position| position.condition_id.trim())
        .filter(|condition_id| !condition_id.is_empty())
        .map(str::to_string)
        .collect();
    let authoritative = crate::async_rt::block_on_runtime(
        fetch_authoritative_resolutions(conditions),
    );
    let mut snapshot = position_snapshot_from_rows(&resp);
    snapshot.settled_token_values.extend(authoritative);

    info!(
        "[Polymarket] Fetched {} positions ({} raw records)",
        snapshot.positions.len(),
        resp.len(),
    );
    for (token_id, pos) in &snapshot.positions {
        let short: String = token_id.chars().take(16).collect();
        info!(
            "[Polymarket]   token={}... qty={:.4} avg_price={:.4}",
            short, pos.quantity, pos.avg_price,
        );
    }

    Ok(snapshot)
}

pub fn fetch_positions(wallet_address: &str) -> Result<HashMap<String, Position>> {
    Ok(fetch_position_snapshot(wallet_address)?.positions)
}

/// Raw balance record from Polymarket Data API.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiBalance {
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
    try_fetch_balance_and_positions_versioned(wallet_address, is_v2).unwrap_or_else(|error| {
        log::warn!(
            "[Polymarket] complete balance/positions fetch failed — using empty fallback: {}",
            error,
        );
        (0.0, HashMap::new())
    })
}

/// Strict account snapshot used by live shared-account reconciliation. Unlike
/// the compatibility wrapper above, failure of either the collateral balance
/// or the account-wide positions request fails the entire snapshot. Callers
/// must retry rather than interpreting a transport failure as a real zero.
pub fn try_fetch_balance_and_positions_versioned(
    wallet_address: &str,
    is_v2: bool,
) -> Result<(f64, HashMap<String, Position>)> {
    let token = active_collateral_token(is_v2);
    let wb = wallet_address.to_string();
    let tok = token.to_string();
    let t_bal = std::thread::Builder::new()
        .name("poly-fetch-balance".into())
        .spawn(move || fetch_balance_for_token(&wb, &tok))
        .map_err(|error| anyhow::anyhow!("spawn fetch-balance thread: {error}"))?;
    let wp = wallet_address.to_string();
    let t_pos = std::thread::Builder::new()
        .name("poly-fetch-positions".into())
        .spawn(move || fetch_positions(&wp))
        .map_err(|error| anyhow::anyhow!("spawn fetch-positions thread: {error}"))?;

    let balance = t_bal.join()
        .map_err(|_| anyhow::anyhow!("fetch-balance thread panicked"))??;
    let positions = t_pos.join()
        .map_err(|_| anyhow::anyhow!("fetch-positions thread panicked"))??;
    Ok((balance, positions))
}

/// Strict account snapshot plus authoritative historical 0/1 outcomes from
/// the same Data API response.
pub fn try_fetch_balance_positions_and_settlements_versioned(
    wallet_address: &str,
    is_v2: bool,
) -> Result<(f64, HashMap<String, Position>, HashMap<String, f64>)> {
    let token = active_collateral_token(is_v2);
    let wb = wallet_address.to_string();
    let tok = token.to_string();
    let t_bal = std::thread::Builder::new()
        .name("poly-fetch-balance".into())
        .spawn(move || fetch_balance_for_token(&wb, &tok))
        .map_err(|error| anyhow::anyhow!("spawn fetch-balance thread: {error}"))?;
    let wp = wallet_address.to_string();
    let t_pos = std::thread::Builder::new()
        .name("poly-fetch-positions".into())
        .spawn(move || fetch_position_snapshot(&wp))
        .map_err(|error| anyhow::anyhow!("spawn fetch-positions thread: {error}"))?;

    let balance = t_bal.join()
        .map_err(|_| anyhow::anyhow!("fetch-balance thread panicked"))??;
    let snapshot = t_pos.join()
        .map_err(|_| anyhow::anyhow!("fetch-positions thread panicked"))??;
    Ok((balance, snapshot.positions, snapshot.settled_token_values))
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
    let addr_bytes = parse_evm_address(wallet_address)?;
    parse_evm_address(token).map_err(|error| anyhow!("invalid collateral token address: {error}"))?;
    let mut padded = [0u8; 32];
    padded[12..].copy_from_slice(&addr_bytes);
    calldata.extend_from_slice(&padded);
    let data = format!("0x{}", hex::encode(&calldata));

    if let Some(result) = super::deploy_wallet::eth_call(token, &data) {
        let balance = parse_erc20_balance_result(&result)?;
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
            if !r.status().is_success() {
                return Err(anyhow::anyhow!("fetch_balance: status {}", r.status()));
            }
            let bal: ApiBalance = r.json().await
                .map_err(|e| anyhow::anyhow!("fetch_balance parse: {}", e))?;
            validate_balance(bal.balance)
        })?;
        info!("[Polymarket] Balance: {:.4} (data-api, USDC.e)", balance);
        return Ok(balance);
    }

    Err(anyhow::anyhow!(
        "eth_call balanceOf failed for token {} and no data-api fallback for non-USDC.e tokens",
        token,
    ))
}

fn parse_evm_address(address: &str) -> Result<[u8; 20]> {
    let encoded = address
        .strip_prefix("0x")
        .or_else(|| address.strip_prefix("0X"))
        .unwrap_or(address);
    if encoded.len() != 40 {
        return Err(anyhow!("EVM address must contain exactly 40 hex characters"));
    }
    let decoded = hex::decode(encoded).map_err(|error| anyhow!("invalid EVM address hex: {error}"))?;
    decoded.try_into().map_err(|_| anyhow!("EVM address must decode to 20 bytes"))
}

fn parse_erc20_balance_result(result: &str) -> Result<f64> {
    let encoded = result
        .strip_prefix("0x")
        .or_else(|| result.strip_prefix("0X"))
        .ok_or_else(|| anyhow!("eth_call balance response is missing 0x prefix"))?;
    if encoded.len() != 64 {
        return Err(anyhow!(
            "eth_call balance response must be one ABI word (64 hex characters), got {}",
            encoded.len(),
        ));
    }
    let bytes = hex::decode(encoded)
        .map_err(|error| anyhow!("invalid eth_call balance hex: {error}"))?;
    let raw = BigUint::from_bytes_be(&bytes);
    let balance = raw.to_f64()
        .ok_or_else(|| anyhow!("eth_call balance does not fit in finite f64"))?
        / 1_000_000.0;
    validate_balance(balance)
}

fn validate_balance(balance: f64) -> Result<f64> {
    if !balance.is_finite() || balance < 0.0 {
        return Err(anyhow!("balance must be finite and non-negative, got {balance}"));
    }
    Ok(balance)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(asset: &str, condition: &str, size: f64, value: f64, redeemable: bool) -> ApiPosition {
        ApiPosition {
            asset: asset.to_string(), condition_id: condition.to_string(), size,
            avg_price: 0.4, current_value: value, outcome: asset.to_string(),
            title: None, redeemable,
        }
    }

    #[test]
    fn redeemable_condition_discovers_zero_and_one_outcomes() {
        let snapshot = position_snapshot_from_rows(&[
            row("winner", "resolved", 3.0, 3.0, true),
            row("loser", "resolved", 4.0, 0.0, false),
            row("active", "active", 2.0, 1.0, false),
        ]);
        assert_eq!(snapshot.settled_token_values.get("winner"), Some(&1.0));
        assert_eq!(snapshot.settled_token_values.get("loser"), Some(&0.0));
        assert!(!snapshot.settled_token_values.contains_key("active"));
    }

    #[test]
    fn redeemable_metadata_does_not_authorize_non_binary_unit_values() {
        let snapshot = position_snapshot_from_rows(&[
            row("ambiguous", "resolved", 2.0, 1.0, true),
        ]);
        assert!(snapshot.settled_token_values.is_empty());
        assert_eq!(snapshot.positions["ambiguous"].quantity, 2.0);
    }

    #[test]
    fn closed_market_winner_flags_converge_both_tokens() {
        let resolution = authoritative_resolution("condition", ClobMarketResolution {
            condition_id: "condition".to_string(),
            closed: true,
            tokens: vec![
                ClobMarketToken { token_id: "up".to_string(), winner: Some(false) },
                ClobMarketToken { token_id: "down".to_string(), winner: Some(true) },
            ],
        }).unwrap().unwrap();
        assert_eq!(resolution.get("up"), Some(&0.0));
        assert_eq!(resolution.get("down"), Some(&1.0));
    }

    #[test]
    fn active_or_ambiguous_market_is_not_authoritative() {
        assert!(authoritative_resolution("condition", ClobMarketResolution {
            condition_id: "condition".to_string(),
            closed: false,
            tokens: vec![],
        }).unwrap().is_none());
        assert!(authoritative_resolution("condition", ClobMarketResolution {
            condition_id: "condition".to_string(),
            closed: true,
            tokens: vec![
                ClobMarketToken { token_id: "up".to_string(), winner: Some(false) },
                ClobMarketToken { token_id: "down".to_string(), winner: Some(false) },
            ],
        }).is_err());
    }

    #[test]
    fn abnormal_balance_payloads_are_never_zero_fallbacks() {
        assert!(parse_evm_address("0x1234").is_err());
        assert!(parse_evm_address("0x000000000000000000000000000000000000000g").is_err());
        assert!(parse_erc20_balance_result("0x0").is_err());
        assert!(parse_erc20_balance_result("not-hex").is_err());
        assert!(validate_balance(f64::NAN).is_err());
        assert!(validate_balance(-1.0).is_err());
    }

    #[test]
    fn valid_abi_balance_decodes_six_decimals() {
        let encoded = format!("0x{:064x}", 12_345_678u64);
        assert_eq!(parse_erc20_balance_result(&encoded).unwrap(), 12.345678);
    }
}
