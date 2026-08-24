use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use log::info;
use num_bigint::BigUint;
use num_traits::ToPrimitive;
use serde::Deserialize;

use crate::account::position::Position;

const DATA_API_BASE: &str = "https://data-api.polymarket.com";
const CLOB_API_BASE: &str = "https://clob.polymarket.com";
const SETTLEMENT_REQUEST_SPACING: Duration = Duration::from_millis(250);
const SETTLEMENT_IN_FLIGHT_LEASE: Duration = Duration::from_secs(5 * 60);
const ACTIVE_MARKET_RECHECK: Duration = Duration::from_secs(60);
const DEFAULT_SETTLEMENT_RETRY: Duration = Duration::from_secs(5);
const DEFAULT_RATE_LIMIT_RETRY: Duration = Duration::from_secs(30);
const SETTLEMENT_MAX_CONCURRENT_REQUESTS: usize = 1;

/// Raw position record from Polymarket Data API.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct ApiPosition {
    /// CLOB token ID (decimal string representing a 256-bit ERC1155 id).
    /// This is the "symbol" we key by internally — one entry per outcome.
    asset: String,
    condition_id: String,
    size: f64,
    avg_price: f64,
    /// Mark-to-market USDC value = size × cur_price. For settled events the
    /// API returns cur_price = 1 (winner) or 0 (loser), so current_value
    /// reflects the redeemable dollar value directly.
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

fn validate_position_row(position: &ApiPosition, row: usize) -> Result<()> {
    if position.asset.trim().is_empty()
        || position.condition_id.trim().is_empty()
        || position.outcome.trim().is_empty()
    {
        return Err(anyhow!(
            "position row {row} is missing asset, conditionId, or outcome"
        ));
    }
    if !position.size.is_finite() || position.size < 0.0 {
        return Err(anyhow!(
            "position row {row} has invalid size {}",
            position.size
        ));
    }
    if !position.avg_price.is_finite() || !(0.0..=1.0).contains(&position.avg_price) {
        return Err(anyhow!(
            "position row {row} has invalid avgPrice {}",
            position.avg_price
        ));
    }
    if !position.current_value.is_finite()
        || position.current_value < 0.0
        || position.current_value > position.size + 1e-8
    {
        return Err(anyhow!(
            "position row {row} has invalid currentValue {} for size {}",
            position.current_value,
            position.size,
        ));
    }
    Ok(())
}

fn position_snapshot_from_rows(resp: &[ApiPosition]) -> Result<PositionSnapshot> {
    for (row, position) in resp.iter().enumerate() {
        validate_position_row(position, row)?;
    }
    let resolved_conditions: HashSet<&str> = resp
        .iter()
        .filter(|position| position.redeemable)
        .map(|position| position.condition_id.as_str())
        .collect();
    let mut positions = HashMap::new();
    let mut settled_token_values = HashMap::new();
    for p in resp {
        if p.size <= 0.0 {
            continue;
        }
        if positions
            .insert(
                p.asset.clone(),
                Position {
                    quantity: p.size,
                    avg_price: p.avg_price,
                    current_value: p.current_value,
                },
            )
            .is_some()
        {
            return Err(anyhow!(
                "position snapshot contains duplicate asset {}",
                p.asset
            ));
        }
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
    Ok(PositionSnapshot {
        positions,
        settled_token_values,
    })
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
            return Err(anyhow!(
                "closed market {} returned an empty token id",
                expected_condition_id
            ));
        }
        let winner = token.winner.ok_or_else(|| {
            anyhow!(
                "closed market {} has no authoritative winner flag for token {}",
                expected_condition_id,
                token_id,
            )
        })?;
        if winner {
            winners += 1;
        }
        if values
            .insert(token_id.to_string(), if winner { 1.0 } else { 0.0 })
            .is_some()
        {
            return Err(anyhow!(
                "closed market {} returned duplicate token ids",
                expected_condition_id
            ));
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

#[derive(Debug)]
enum SettlementLookupEntry {
    InFlight { lease_until: Instant },
    RetryAt(Instant),
    Ready(HashMap<String, f64>),
}

#[derive(Debug)]
enum SettlementLookupDecision {
    Fetch,
    Deferred,
    Cached(HashMap<String, f64>),
}

#[derive(Debug, Default)]
struct SettlementLookupState {
    entries: HashMap<String, SettlementLookupEntry>,
    next_request_at: Option<Instant>,
    /// Retry-After from 429 is endpoint-wide, not condition-specific.
    global_retry_at: Option<Instant>,
}

impl SettlementLookupState {
    fn claim(&mut self, condition_id: &str, now: Instant) -> SettlementLookupDecision {
        if let Some(SettlementLookupEntry::Ready(values)) = self.entries.get(condition_id) {
            return SettlementLookupDecision::Cached(values.clone());
        }
        if self.global_retry_at.is_some_and(|retry_at| retry_at > now) {
            return SettlementLookupDecision::Deferred;
        }
        match self.entries.get(condition_id) {
            Some(SettlementLookupEntry::InFlight { lease_until }) if *lease_until > now => {
                return SettlementLookupDecision::Deferred;
            }
            Some(SettlementLookupEntry::RetryAt(retry_at)) if *retry_at > now => {
                return SettlementLookupDecision::Deferred;
            }
            _ => {}
        }
        self.entries.insert(
            condition_id.to_string(),
            SettlementLookupEntry::InFlight {
                lease_until: now + SETTLEMENT_IN_FLIGHT_LEASE,
            },
        );
        SettlementLookupDecision::Fetch
    }

    fn reserve_request_delay(&mut self, now: Instant) -> Duration {
        let request_at = self
            .next_request_at
            .unwrap_or(now)
            .max(self.global_retry_at.unwrap_or(now))
            .max(now);
        self.next_request_at = Some(request_at + SETTLEMENT_REQUEST_SPACING);
        request_at.saturating_duration_since(now)
    }

    /// Recheck after waiting for the global semaphore/rate slot: another CID
    /// may have received 429 while this request was queued.
    fn request_allowed(&mut self, condition_id: &str, now: Instant) -> bool {
        if let Some(retry_at) = self.global_retry_at.filter(|retry_at| *retry_at > now) {
            self.entries.insert(
                condition_id.to_string(),
                SettlementLookupEntry::RetryAt(retry_at),
            );
            return false;
        }
        match self.entries.get(condition_id) {
            Some(SettlementLookupEntry::InFlight { lease_until }) if *lease_until > now => true,
            Some(SettlementLookupEntry::RetryAt(retry_at)) if *retry_at <= now => {
                self.entries.insert(
                    condition_id.to_string(),
                    SettlementLookupEntry::InFlight {
                        lease_until: now + SETTLEMENT_IN_FLIGHT_LEASE,
                    },
                );
                true
            }
            _ => false,
        }
    }

    fn complete(
        &mut self,
        condition_id: &str,
        resolution: Option<HashMap<String, f64>>,
        now: Instant,
    ) {
        let entry = match resolution {
            Some(values) => SettlementLookupEntry::Ready(values),
            None => SettlementLookupEntry::RetryAt(now + ACTIVE_MARKET_RECHECK),
        };
        self.entries.insert(condition_id.to_string(), entry);
    }

    fn defer_after_error(
        &mut self,
        condition_id: &str,
        retry_after: Duration,
        rate_limited: bool,
        now: Instant,
    ) {
        let retry_at = now + retry_after;
        self.entries.insert(
            condition_id.to_string(),
            SettlementLookupEntry::RetryAt(retry_at),
        );
        if !rate_limited {
            return;
        }

        let global_retry_at = self
            .global_retry_at
            .map_or(retry_at, |current| current.max(retry_at));
        self.global_retry_at = Some(global_retry_at);
        // Invalidate queued logical flights so one 429 suppresses the rest of
        // the batch instead of allowing every CID to hit the same limit.
        for entry in self.entries.values_mut() {
            match entry {
                SettlementLookupEntry::Ready(_) => {}
                SettlementLookupEntry::InFlight { .. } => {
                    *entry = SettlementLookupEntry::RetryAt(global_retry_at);
                }
                SettlementLookupEntry::RetryAt(current) if *current < global_retry_at => {
                    *current = global_retry_at;
                }
                SettlementLookupEntry::RetryAt(_) => {}
            }
        }
    }
}

fn settlement_lookup_state() -> &'static Mutex<SettlementLookupState> {
    static STATE: OnceLock<Mutex<SettlementLookupState>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(SettlementLookupState::default()))
}

fn with_settlement_lookup_state<T>(f: impl FnOnce(&mut SettlementLookupState) -> T) -> T {
    let mut state = settlement_lookup_state()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    f(&mut state)
}

fn settlement_request_limiter() -> &'static tokio::sync::Semaphore {
    static LIMITER: OnceLock<tokio::sync::Semaphore> = OnceLock::new();
    LIMITER.get_or_init(|| tokio::sync::Semaphore::new(SETTLEMENT_MAX_CONCURRENT_REQUESTS))
}

#[derive(Debug)]
struct SettlementFetchError {
    message: String,
    retry_after: Duration,
    rate_limited: bool,
}

fn parse_retry_after(value: Option<&str>) -> Duration {
    value
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_RATE_LIMIT_RETRY)
        .clamp(Duration::from_secs(1), Duration::from_secs(60 * 60))
}

fn response_retry_after(response: &reqwest::Response) -> Duration {
    parse_retry_after(
        response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok()),
    )
}

async fn fetch_authoritative_resolution(
    client: &reqwest::Client,
    condition_id: &str,
) -> std::result::Result<Option<HashMap<String, f64>>, SettlementFetchError> {
    let url = format!("{}/markets/{}", CLOB_API_BASE, condition_id);
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|error| SettlementFetchError {
            message: format!("fetch settlement {}: {}", condition_id, error),
            retry_after: DEFAULT_SETTLEMENT_RETRY,
            rate_limited: false,
        })?;
    let status = response.status();
    if !status.is_success() {
        let retry_after = if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            response_retry_after(&response)
        } else if status.is_server_error() {
            DEFAULT_SETTLEMENT_RETRY
        } else {
            ACTIVE_MARKET_RECHECK
        };
        return Err(SettlementFetchError {
            message: format!("fetch settlement {}: status {}", condition_id, status),
            retry_after,
            rate_limited: status == reqwest::StatusCode::TOO_MANY_REQUESTS,
        });
    }
    let market = response
        .json::<ClobMarketResolution>()
        .await
        .map_err(|error| SettlementFetchError {
            message: format!("parse settlement {}: {}", condition_id, error),
            retry_after: DEFAULT_SETTLEMENT_RETRY,
            rate_limited: false,
        })?;
    authoritative_resolution(condition_id, market).map_err(|error| SettlementFetchError {
        message: error.to_string(),
        retry_after: ACTIVE_MARKET_RECHECK,
        rate_limited: false,
    })
}

async fn fetch_authoritative_resolutions(condition_ids: HashSet<String>) -> HashMap<String, f64> {
    let client = crate::async_rt::http_client();
    let mut values = HashMap::new();
    for condition_id in condition_ids {
        let decision =
            with_settlement_lookup_state(|state| state.claim(&condition_id, Instant::now()));
        match decision {
            SettlementLookupDecision::Cached(resolution) => {
                values.extend(resolution);
                continue;
            }
            SettlementLookupDecision::Deferred => continue,
            SettlementLookupDecision::Fetch => {}
        }

        let Ok(_permit) = settlement_request_limiter().acquire().await else {
            with_settlement_lookup_state(|state| {
                state.defer_after_error(
                    &condition_id,
                    DEFAULT_SETTLEMENT_RETRY,
                    false,
                    Instant::now(),
                );
            });
            continue;
        };
        let delay =
            with_settlement_lookup_state(|state| state.reserve_request_delay(Instant::now()));
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        let allowed = with_settlement_lookup_state(|state| {
            state.request_allowed(&condition_id, Instant::now())
        });
        if !allowed {
            continue;
        }

        let result = fetch_authoritative_resolution(&client, &condition_id).await;
        match result {
            Ok(resolution) => {
                if let Some(resolution) = resolution.as_ref() {
                    values.extend(resolution.clone());
                }
                with_settlement_lookup_state(|state| {
                    state.complete(&condition_id, resolution, Instant::now());
                });
            }
            Err(error) => {
                with_settlement_lookup_state(|state| {
                    state.defer_after_error(
                        &condition_id,
                        error.retry_after,
                        error.rate_limited,
                        Instant::now(),
                    );
                });
                log::warn!(
                    "[Polymarket] Authoritative settlement lookup unavailable; keeping provisional value and suppressing duplicate requests for {:?} global_cooldown={}: {}",
                    error.retry_after,
                    error.rate_limited,
                    error.message,
                );
            }
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
fn fetch_position_rows(wallet_address: &str) -> Result<Vec<ApiPosition>> {
    info!("[Polymarket] Fetching positions for {}", wallet_address);

    // Route through the shared async runtime + HTTP/2 client.
    let client = crate::async_rt::http_client();
    let wallet = wallet_address.to_string();
    crate::async_rt::block_on_runtime(async move {
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
                    page + 1,
                    offset,
                    r.status(),
                ));
            }
            let mut rows = r.json::<Vec<ApiPosition>>().await.map_err(|e| {
                anyhow::anyhow!(
                    "fetch_positions parse page={} offset={}: {}",
                    page + 1,
                    offset,
                    e,
                )
            })?;
            let complete = rows.len() < PAGE_SIZE;
            all.append(&mut rows);
            if complete {
                return Ok(all);
            }
        }
        Err(anyhow::anyhow!(
            "fetch_positions exceeded {} pages; refusing a potentially incomplete authoritative snapshot",
            MAX_PAGES,
        ))
    })
}

fn log_position_snapshot(snapshot: &PositionSnapshot, raw_records: usize) {
    info!(
        "[Polymarket] Fetched {} positions ({} raw records)",
        snapshot.positions.len(),
        raw_records,
    );
    for (token_id, pos) in &snapshot.positions {
        let short: String = token_id.chars().take(16).collect();
        log::debug!(
            "[Polymarket] token={}... qty={:.4} avg_price={:.4}",
            short,
            pos.quantity,
            pos.avg_price,
        );
    }
}

/// Fast startup view: the Data API response is authoritative for current
/// positions and already carries direct redeemable 0/1 values. Historical
/// CLOB market-resolution audits are intentionally excluded so account
/// admission never waits for a long serial settlement sweep.
pub fn fetch_position_snapshot_fast(wallet_address: &str) -> Result<PositionSnapshot> {
    let rows = fetch_position_rows(wallet_address)?;
    let snapshot = position_snapshot_from_rows(&rows)?;
    log_position_snapshot(&snapshot, rows.len());
    Ok(snapshot)
}

fn fetch_position_snapshot_with_conditions(
    wallet_address: &str,
    mut settlement_conditions: HashSet<String>,
) -> Result<PositionSnapshot> {
    let resp = fetch_position_rows(wallet_address)?;

    // Query every condition represented by a non-zero historical position.
    // This is deliberately broader than `redeemable`: after a new process
    // starts, Data API metadata can be incomplete or rounded, while the CLOB
    // market endpoint still carries the final winner flags. Failed/active
    // lookups remain provisional and are retried on the next account refresh.
    settlement_conditions.extend(
        resp.iter()
            .filter(|position| position.size.is_finite() && position.size > 0.0)
            .map(|position| position.condition_id.trim())
            .filter(|condition_id| !condition_id.is_empty())
            .map(str::to_string),
    );
    let authoritative =
        crate::async_rt::block_on_runtime(fetch_authoritative_resolutions(settlement_conditions));
    let mut snapshot = position_snapshot_from_rows(&resp)?;
    snapshot.settled_token_values.extend(authoritative);

    log_position_snapshot(&snapshot, resp.len());

    Ok(snapshot)
}

pub fn fetch_position_snapshot(wallet_address: &str) -> Result<PositionSnapshot> {
    fetch_position_snapshot_with_conditions(wallet_address, HashSet::new())
}

pub fn fetch_positions(wallet_address: &str) -> Result<HashMap<String, Position>> {
    Ok(fetch_position_snapshot_fast(wallet_address)?.positions)
}

pub fn fetch_authoritative_settlements_for_conditions(
    condition_ids: HashSet<String>,
) -> HashMap<String, f64> {
    crate::async_rt::block_on_runtime(fetch_authoritative_resolutions(condition_ids))
}

/// pUSD on Polygon — v2 CLOB collateral (6 decimals).
pub const PUSD_ADDRESS: &str = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB";

/// CLOB V2 collateral. The bool remains in the compatibility-shaped strict
/// fetch API but can no longer select legacy USDC.e.
pub fn active_collateral_token(_is_v2: bool) -> &'static str {
    PUSD_ADDRESS
}

fn run_cold_rpc_pair<A, B, FA, FB>(left: FA, right: FB) -> Result<(A, B)>
where
    A: Send + 'static,
    B: Send + 'static,
    FA: FnOnce() -> Result<A> + Send + 'static,
    FB: FnOnce() -> Result<B> + Send + 'static,
{
    let (left_tx, left_rx) = crossbeam_channel::bounded(1);
    let (right_tx, right_rx) = crossbeam_channel::bounded(1);
    hexagent_runtime::cold_rpc_jobs::try_submit(move || {
        let _ = left_tx.send(left());
    })
    .map_err(|error| anyhow::anyhow!("submit cold RPC left job: {error}"))?;
    hexagent_runtime::cold_rpc_jobs::try_submit(move || {
        let _ = right_tx.send(right());
    })
    .map_err(|error| anyhow::anyhow!("submit cold RPC right job: {error}"))?;
    let left = left_rx
        .recv()
        .map_err(|error| anyhow::anyhow!("cold RPC left owner disconnected: {error}"))??;
    let right = right_rx
        .recv()
        .map_err(|error| anyhow::anyhow!("cold RPC right owner disconnected: {error}"))??;
    Ok((left, right))
}

/// Strict account snapshot used by live shared-account reconciliation. Unlike
/// the former fallback API, failure of either the collateral balance or the
/// account-wide positions request fails the entire snapshot. Callers
/// must retry rather than interpreting a transport failure as a real zero.
pub fn try_fetch_balance_and_positions_versioned(
    wallet_address: &str,
    is_v2: bool,
) -> Result<(f64, HashMap<String, Position>)> {
    let token = active_collateral_token(is_v2);
    let wb = wallet_address.to_string();
    let tok = token.to_string();
    let wp = wallet_address.to_string();
    let (balance, positions) = run_cold_rpc_pair(
        move || fetch_balance_for_token(&wb, &tok),
        move || fetch_positions(&wp),
    )?;
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
    let wp = wallet_address.to_string();
    let (balance, snapshot) = run_cold_rpc_pair(
        move || fetch_balance_for_token(&wb, &tok),
        move || fetch_position_snapshot(&wp),
    )?;
    Ok((balance, snapshot.positions, snapshot.settled_token_values))
}

/// Startup-critical balance/position snapshot. Settlement values directly
/// present in the Data API are returned immediately; ledger-only historical
/// condition resolution runs on a separate background lane.
pub fn try_fetch_balance_positions_fast_versioned(
    wallet_address: &str,
    is_v2: bool,
) -> Result<(f64, HashMap<String, Position>, HashMap<String, f64>)> {
    let token = active_collateral_token(is_v2);
    let wb = wallet_address.to_string();
    let tok = token.to_string();
    let wp = wallet_address.to_string();
    let (balance, snapshot) = run_cold_rpc_pair(
        move || fetch_balance_for_token(&wb, &tok),
        move || fetch_position_snapshot_fast(&wp),
    )?;
    Ok((balance, snapshot.positions, snapshot.settled_token_values))
}

/// Strict cold-start snapshot that also resolves conditions remembered only
/// by the persistent account ledger. Auto-redeem may remove every corresponding
/// Data API position row while the bot is stopped.
pub fn try_fetch_balance_positions_and_settlements_for_conditions_versioned(
    wallet_address: &str,
    is_v2: bool,
    settlement_conditions: HashSet<String>,
) -> Result<(f64, HashMap<String, Position>, HashMap<String, f64>)> {
    let token = active_collateral_token(is_v2);
    let wb = wallet_address.to_string();
    let tok = token.to_string();
    let wp = wallet_address.to_string();
    let (balance, snapshot) = run_cold_rpc_pair(
        move || fetch_balance_for_token(&wb, &tok),
        move || fetch_position_snapshot_with_conditions(&wp, settlement_conditions),
    )?;
    Ok((balance, snapshot.positions, snapshot.settled_token_values))
}

/// Bare balance fetch for the production CLOB V2 pUSD collateral.
pub fn fetch_balance(wallet_address: &str) -> Result<f64> {
    fetch_balance_for_token(wallet_address, PUSD_ADDRESS)
}

/// Fetch an ERC-20 balance from Polygon via strict `eth_call balanceOf`.
pub fn fetch_balance_for_token(wallet_address: &str, token: &str) -> Result<f64> {
    info!(
        "[Polymarket] Fetching balance for {} (token={})",
        wallet_address, token
    );

    // Primary: on-chain balanceOf(address) via Polygon RPC
    let selector: [u8; 4] = [0x70, 0xa0, 0x82, 0x31]; // balanceOf(address)
    let mut calldata = Vec::with_capacity(4 + 32);
    calldata.extend_from_slice(&selector);
    let addr_bytes = parse_evm_address(wallet_address)?;
    parse_evm_address(token)
        .map_err(|error| anyhow!("invalid collateral token address: {error}"))?;
    let mut padded = [0u8; 32];
    padded[12..].copy_from_slice(&addr_bytes);
    calldata.extend_from_slice(&padded);
    let data = format!("0x{}", hex::encode(&calldata));

    if let Some(result) = super::deploy_wallet::eth_call(token, &data) {
        let balance = parse_erc20_balance_result(&result)?;
        info!(
            "[Polymarket] Balance: {:.4} (on-chain, token={})",
            balance, token
        );
        return Ok(balance);
    }

    Err(anyhow::anyhow!(
        "eth_call balanceOf failed for CLOB V2 collateral token {}",
        token,
    ))
}

fn parse_evm_address(address: &str) -> Result<[u8; 20]> {
    let encoded = address
        .strip_prefix("0x")
        .or_else(|| address.strip_prefix("0X"))
        .unwrap_or(address);
    if encoded.len() != 40 {
        return Err(anyhow!(
            "EVM address must contain exactly 40 hex characters"
        ));
    }
    let decoded =
        hex::decode(encoded).map_err(|error| anyhow!("invalid EVM address hex: {error}"))?;
    decoded
        .try_into()
        .map_err(|_| anyhow!("EVM address must decode to 20 bytes"))
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
    let bytes =
        hex::decode(encoded).map_err(|error| anyhow!("invalid eth_call balance hex: {error}"))?;
    let raw = BigUint::from_bytes_be(&bytes);
    let balance = raw
        .to_f64()
        .ok_or_else(|| anyhow!("eth_call balance does not fit in finite f64"))?
        / 1_000_000.0;
    validate_balance(balance)
}

fn validate_balance(balance: f64) -> Result<f64> {
    if !balance.is_finite() || balance < 0.0 {
        return Err(anyhow!(
            "balance must be finite and non-negative, got {balance}"
        ));
    }
    Ok(balance)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settlement_lookup_is_singleflight_and_caches_final_resolution() {
        let now = Instant::now();
        let mut state = SettlementLookupState::default();
        assert!(matches!(
            state.claim("condition", now),
            SettlementLookupDecision::Fetch
        ));
        assert!(matches!(
            state.claim("condition", now),
            SettlementLookupDecision::Deferred
        ));

        let expected = HashMap::from([("winner".to_string(), 1.0)]);
        state.complete("condition", Some(expected.clone()), now);
        match state.claim("condition", now) {
            SettlementLookupDecision::Cached(actual) => assert_eq!(actual, expected),
            other => panic!("expected cached settlement, got {other:?}"),
        }
    }

    #[test]
    fn settlement_lookup_respects_rate_limit_retry_deadline() {
        let now = Instant::now();
        let mut state = SettlementLookupState::default();
        assert!(matches!(
            state.claim("condition", now),
            SettlementLookupDecision::Fetch
        ));
        state.defer_after_error("condition", Duration::from_secs(30), false, now);
        assert!(matches!(
            state.claim("condition", now + Duration::from_secs(29)),
            SettlementLookupDecision::Deferred
        ));
        assert!(matches!(
            state.claim("condition", now + Duration::from_secs(30)),
            SettlementLookupDecision::Fetch
        ));
    }

    #[test]
    fn settlement_retry_after_uses_header_with_safe_default_and_bounds() {
        assert_eq!(parse_retry_after(Some("17")), Duration::from_secs(17));
        assert_eq!(parse_retry_after(None), DEFAULT_RATE_LIMIT_RETRY);
        assert_eq!(parse_retry_after(Some("invalid")), DEFAULT_RATE_LIMIT_RETRY);
        assert_eq!(parse_retry_after(Some("0")), Duration::from_secs(1));
        assert_eq!(parse_retry_after(Some("99999")), Duration::from_secs(3600));
    }

    #[test]
    fn settlement_requests_are_globally_spaced() {
        let now = Instant::now();
        let mut state = SettlementLookupState::default();
        assert_eq!(state.reserve_request_delay(now), Duration::ZERO);
        assert_eq!(state.reserve_request_delay(now), SETTLEMENT_REQUEST_SPACING);
    }

    #[test]
    fn settlement_429_defers_other_cids_and_invalidates_queued_flights() {
        let now = Instant::now();
        let mut state = SettlementLookupState::default();
        assert!(matches!(
            state.claim("condition-a", now),
            SettlementLookupDecision::Fetch
        ));
        assert!(matches!(
            state.claim("condition-b", now),
            SettlementLookupDecision::Fetch
        ));

        let cached = HashMap::from([("winner".to_string(), 1.0)]);
        state.complete("resolved", Some(cached.clone()), now);
        state.defer_after_error("condition-a", Duration::from_secs(30), true, now);
        assert!(matches!(
            state.claim("condition-c", now + Duration::from_secs(29)),
            SettlementLookupDecision::Deferred
        ));
        assert!(!state.request_allowed("condition-b", now + Duration::from_secs(1)));
        assert!(matches!(
            state.claim("resolved", now + Duration::from_secs(1)),
            SettlementLookupDecision::Cached(values) if values == cached
        ));
        assert!(matches!(
            state.claim("condition-c", now + Duration::from_secs(30)),
            SettlementLookupDecision::Fetch
        ));
        assert!(state.request_allowed("condition-b", now + Duration::from_secs(30)));
    }

    fn row(asset: &str, condition: &str, size: f64, value: f64, redeemable: bool) -> ApiPosition {
        ApiPosition {
            asset: asset.to_string(),
            condition_id: condition.to_string(),
            size,
            avg_price: 0.4,
            current_value: value,
            outcome: asset.to_string(),
            title: None,
            redeemable,
        }
    }

    #[test]
    fn redeemable_condition_discovers_zero_and_one_outcomes() {
        let snapshot = position_snapshot_from_rows(&[
            row("winner", "resolved", 3.0, 3.0, true),
            row("loser", "resolved", 4.0, 0.0, false),
            row("active", "active", 2.0, 1.0, false),
        ])
        .unwrap();
        assert_eq!(snapshot.settled_token_values.get("winner"), Some(&1.0));
        assert_eq!(snapshot.settled_token_values.get("loser"), Some(&0.0));
        assert!(!snapshot.settled_token_values.contains_key("active"));
    }

    #[test]
    fn redeemable_metadata_does_not_authorize_non_binary_unit_values() {
        let snapshot =
            position_snapshot_from_rows(&[row("ambiguous", "resolved", 2.0, 1.0, true)]).unwrap();
        assert!(snapshot.settled_token_values.is_empty());
        assert_eq!(snapshot.positions["ambiguous"].quantity, 2.0);
    }

    #[test]
    fn closed_market_winner_flags_converge_both_tokens() {
        let resolution = authoritative_resolution(
            "condition",
            ClobMarketResolution {
                condition_id: "condition".to_string(),
                closed: true,
                tokens: vec![
                    ClobMarketToken {
                        token_id: "up".to_string(),
                        winner: Some(false),
                    },
                    ClobMarketToken {
                        token_id: "down".to_string(),
                        winner: Some(true),
                    },
                ],
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(resolution.get("up"), Some(&0.0));
        assert_eq!(resolution.get("down"), Some(&1.0));
    }

    #[test]
    fn active_or_ambiguous_market_is_not_authoritative() {
        assert!(authoritative_resolution(
            "condition",
            ClobMarketResolution {
                condition_id: "condition".to_string(),
                closed: false,
                tokens: vec![],
            }
        )
        .unwrap()
        .is_none());
        assert!(authoritative_resolution(
            "condition",
            ClobMarketResolution {
                condition_id: "condition".to_string(),
                closed: true,
                tokens: vec![
                    ClobMarketToken {
                        token_id: "up".to_string(),
                        winner: Some(false)
                    },
                    ClobMarketToken {
                        token_id: "down".to_string(),
                        winner: Some(false)
                    },
                ],
            }
        )
        .is_err());
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
    fn one_abnormal_position_rejects_the_entire_snapshot() {
        let mut malformed = row("bad", "active", 2.0, 1.0, false);
        malformed.current_value = f64::NAN;
        assert!(
            position_snapshot_from_rows(&[row("good", "active", 1.0, 0.5, false), malformed,])
                .is_err()
        );
        assert!(position_snapshot_from_rows(&[
            row("duplicate", "a", 1.0, 0.5, false),
            row("duplicate", "b", 1.0, 0.5, false),
        ])
        .is_err());
    }

    #[test]
    fn valid_abi_balance_decodes_six_decimals() {
        let encoded = format!("0x{:064x}", 12_345_678u64);
        assert_eq!(parse_erc20_balance_result(&encoded).unwrap(), 12.345678);
    }
}
