//! CLOB v2 per-market fee / flags fetch.
//!
//! v2 moves fee computation entirely to the protocol: the signed
//! order no longer carries `feeRateBps`. At match time the server
//! computes
//!
//!     fee = C × feeRate × (p × (1 − p)) ^ exponent
//!
//! using per-market values that the client looks up once via
//! `GET /markets/{conditionId}` (the "getClobMarketInfo" RPC named in
//! the v2 migration docs). The client still needs these locally for:
//!
//!   * Quoter fee estimation (before fill decisions).
//!   * Backtest replay (computes fills + PnL offline).
//!   * PnL accounting post-fill.
//!
//! We fetch this on a background thread and cache it on `EventContext`.
//! Each worker retries transient failures; the strategy may respawn a worker
//! later, and keeps taker orders disabled until authoritative metadata lands.
//!
//! **Endpoint + schema are provisional**: per the migration doc the
//! precise URL path + JSON field names weren't published at the time
//! this was written. Use `hexbot market <conditionId>`
//! to probe a live v2 instance and confirm before cutover. The parser
//! below accepts several plausible field-name variants to soften the
//! landing.

use anyhow::{anyhow, Result};
use log::{info, warn};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// Parsed per-market fee / flags from the v2 CLOB.
#[derive(Debug, Clone)]
pub struct MarketInfoV2 {
    /// Fee rate as a fraction (e.g. 0.02 = 2%). Mirrors
    /// `BinaryOption::fee_rate` so the strategy can overwrite that
    /// field and leave downstream fee math untouched.
    pub fee_rate: f64,
    /// Fee curve exponent (e.g. 1.0). Mirrors `BinaryOption::fee_exponent`.
    pub fee_exponent: f64,
    /// Fee rate in basis points (rounded to u32). Mirrors
    /// `BinaryOption::base_fee`, which is what `OrderManager` reads.
    /// Populated so both representations stay in sync when a fetch
    /// lands.
    pub fee_rate_bps: u32,
    /// Polymarket's "taker_only" fee flag. Despite the name it does
    /// **NOT** restrict the order types the market accepts — resting
    /// maker quotes are fully allowed. It means "only taker orders
    /// are charged the fee":
    ///   * taker fill → `fee = C × rate × (p × (1 − p)) ^ exp`
    ///   * maker fill → `fee = 0` (no rebate either)
    /// When `taker_only = false`, makers pay a (rebated) share of
    /// the taker fee — see `rebate_rate` in `FeeSchedule`.
    ///
    /// For our maker-biased Polymaker strategy this is strictly
    /// favourable: zero cost on the maker side of every fill. The
    /// field is kept in this struct for PnL accounting correctness
    /// (so backtest fee math agrees with live) and operator audit.
    pub taker_only: bool,
    /// Raw JSON response for diagnostic dumps (CLI test tool).
    #[allow(dead_code)]
    pub raw: Value,
}

/// Default URL template.
///
/// Confirmed endpoint by probing against `clob-v2.polymarket.com`
/// and cross-checking with Polymarket's official v2 SDK
/// (`@polymarket/clob-client-v2`, `GET_CLOB_MARKET = "/clob-markets/"`
/// invoked by `getClobMarketInfo(conditionID)`).
///
/// The `/markets/{conditionId}` endpoint also exists but returns
/// v1-style static `taker_base_fee` / `maker_base_fee` instead of
/// the v2 dynamic `fd.r` / `fd.e` / `fd.to` fields we need.
const DEFAULT_PATH_TEMPLATE: &str = "/clob-markets/{conditionId}";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MarketInfoKey {
    api_url_prefix: String,
    condition_id: String,
    path_template: String,
}

enum MarketInfoFlight {
    Fetching(Vec<crossbeam_channel::Sender<Option<MarketInfoV2>>>),
    Ready {
        fetched_at: Instant,
        value: MarketInfoV2,
    },
}

enum MarketInfoOwnerCommand {
    Subscribe {
        key: MarketInfoKey,
        subscriber: crossbeam_channel::Sender<Option<MarketInfoV2>>,
        leader: crossbeam_channel::Sender<bool>,
    },
    Finish {
        key: MarketInfoKey,
        result: Option<MarketInfoV2>,
    },
}

const MARKET_INFO_OWNER_CAPACITY: usize = 256;
const MARKET_INFO_CACHE_CAPACITY: usize = 1_024;
const MARKET_INFO_WAITER_CAPACITY: usize = 4_096;
static MARKET_INFO_OWNER: OnceLock<crossbeam_channel::Sender<MarketInfoOwnerCommand>> =
    OnceLock::new();
static MARKET_INFO_QUEUE_HIGH_WATER: AtomicUsize = AtomicUsize::new(0);
static MARKET_INFO_QUEUE_OVERFLOW: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketInfoOwnerMetrics {
    pub queue_capacity: usize,
    pub cache_capacity: usize,
    pub waiter_capacity: usize,
    pub queue_high_water: usize,
    pub queue_overflow: u64,
}

pub fn market_info_owner_metrics() -> MarketInfoOwnerMetrics {
    MarketInfoOwnerMetrics {
        queue_capacity: MARKET_INFO_OWNER_CAPACITY,
        cache_capacity: MARKET_INFO_CACHE_CAPACITY,
        waiter_capacity: MARKET_INFO_WAITER_CAPACITY,
        queue_high_water: MARKET_INFO_QUEUE_HIGH_WATER.load(Ordering::Relaxed),
        queue_overflow: MARKET_INFO_QUEUE_OVERFLOW.load(Ordering::Relaxed),
    }
}

fn market_info_owner() -> crossbeam_channel::Sender<MarketInfoOwnerCommand> {
    MARKET_INFO_OWNER.get_or_init(|| {
        let (tx, rx) = crossbeam_channel::bounded(MARKET_INFO_OWNER_CAPACITY);
        std::thread::Builder::new().name("poly-market-info-owner".to_string()).spawn(move || {
            crate::os_tune::pin_background("poly-market-info-owner");
            let mut entries = HashMap::<MarketInfoKey, MarketInfoFlight>::new();
            let mut waiter_count = 0usize;
            while let Ok(command) = rx.recv() {
                entries.retain(|_, entry| match entry {
                    MarketInfoFlight::Fetching(_) => true,
                    MarketInfoFlight::Ready { fetched_at, .. } => {
                        fetched_at.elapsed() < Duration::from_secs(2 * 60 * 60)
                    }
                });
                match command {
                    MarketInfoOwnerCommand::Subscribe { key, subscriber, leader } => {
                        if !entries.contains_key(&key)
                            && entries.len() >= MARKET_INFO_CACHE_CAPACITY
                        {
                            let oldest_ready = entries
                                .iter()
                                .filter_map(|(key, entry)| match entry {
                                    MarketInfoFlight::Ready { fetched_at, .. } => {
                                        Some((key.clone(), *fetched_at))
                                    }
                                    MarketInfoFlight::Fetching(_) => None,
                                })
                                .min_by_key(|(_, fetched_at)| *fetched_at)
                                .map(|(key, _)| key);
                            if let Some(oldest_ready) = oldest_ready {
                                entries.remove(&oldest_ready);
                            }
                        }
                        let has_entry_capacity = entries.len() < MARKET_INFO_CACHE_CAPACITY;
                        match entries.get_mut(&key) {
                            Some(MarketInfoFlight::Fetching(waiters))
                                if waiter_count < MARKET_INFO_WAITER_CAPACITY =>
                            {
                                waiters.push(subscriber);
                                waiter_count += 1;
                                let _ = leader.try_send(false);
                            }
                            Some(MarketInfoFlight::Fetching(_)) => {
                                MARKET_INFO_QUEUE_OVERFLOW.fetch_add(1, Ordering::Relaxed);
                                let _ = subscriber.try_send(None);
                                let _ = leader.try_send(false);
                            }
                            Some(MarketInfoFlight::Ready { value, .. }) => {
                                let _ = subscriber.try_send(Some(value.clone()));
                                let _ = leader.try_send(false);
                            }
                            None if has_entry_capacity => {
                                entries.insert(key, MarketInfoFlight::Fetching(vec![subscriber]));
                                waiter_count += 1;
                                let _ = leader.try_send(true);
                            }
                            None => {
                                MARKET_INFO_QUEUE_OVERFLOW.fetch_add(1, Ordering::Relaxed);
                                let _ = subscriber.try_send(None);
                                let _ = leader.try_send(false);
                            }
                        }
                    }
                    MarketInfoOwnerCommand::Finish { key, result } => {
                        let (waiters, accept_result) = match entries.remove(&key) {
                            Some(MarketInfoFlight::Fetching(waiters)) => (waiters, true),
                            Some(ready @ MarketInfoFlight::Ready { .. }) => {
                                entries.insert(key.clone(), ready);
                                (Vec::new(), false)
                            }
                            None => (Vec::new(), false),
                        };
                        waiter_count = waiter_count.saturating_sub(waiters.len());
                        if accept_result {
                            if let Some(value) = result.as_ref() {
                            entries.insert(key, MarketInfoFlight::Ready {
                                fetched_at: Instant::now(),
                                value: value.clone(),
                            });
                            }
                        }
                        for waiter in waiters {
                            let _ = waiter.try_send(result.clone());
                        }
                    }
                }
            }
        }).expect("failed to spawn market-info owner");
        tx
    }).clone()
}

fn market_info_key(
    api_url_prefix: String,
    condition_id: String,
    path_template: String,
) -> MarketInfoKey {
    MarketInfoKey {
        api_url_prefix: api_url_prefix.trim_end_matches('/').to_string(),
        condition_id: condition_id.to_ascii_lowercase(),
        path_template: if path_template.is_empty() {
            DEFAULT_PATH_TEMPLATE.to_string()
        } else {
            path_template
        },
    }
}

fn subscribe_market_info(
    key: &MarketInfoKey,
) -> (crossbeam_channel::Receiver<Option<MarketInfoV2>>, bool) {
    let (tx, rx) = crossbeam_channel::bounded(1);
    let (leader_tx, leader_rx) = crossbeam_channel::bounded(1);
    let owner = market_info_owner();
    let admitted_depth = owner.len().saturating_add(1).min(MARKET_INFO_OWNER_CAPACITY);
    if owner.send_timeout(MarketInfoOwnerCommand::Subscribe {
        key: key.clone(),
        subscriber: tx.clone(),
        leader: leader_tx,
    }, Duration::from_secs(2)).is_err() {
        MARKET_INFO_QUEUE_OVERFLOW.fetch_add(1, Ordering::Relaxed);
        let _ = tx.try_send(None);
        return (rx, false);
    }
    MARKET_INFO_QUEUE_HIGH_WATER.fetch_max(admitted_depth, Ordering::Relaxed);
    let leader = leader_rx.recv().unwrap_or(false);
    (rx, leader)
}

fn finish_market_info_fetch(key: &MarketInfoKey, result: Option<MarketInfoV2>) {
    let owner = market_info_owner();
    let admitted_depth = owner.len().saturating_add(1).min(MARKET_INFO_OWNER_CAPACITY);
    if let Err(error) = owner.send_timeout(MarketInfoOwnerCommand::Finish {
        key: key.clone(),
        result,
    }, Duration::from_secs(2)) {
        MARKET_INFO_QUEUE_OVERFLOW.fetch_add(1, Ordering::Relaxed);
        warn!("[market_info_v2] owner saturated while finishing fetch: {error}");
    } else {
        MARKET_INFO_QUEUE_HIGH_WATER.fetch_max(admitted_depth, Ordering::Relaxed);
    }
}

/// Synchronously fetch market info via the v2 CLOB REST API.
///
/// `api_url_prefix` is the CLOB host root (e.g.
/// `https://clob-v2.polymarket.com`). Leave `path_template` empty to
/// use `/markets/{conditionId}`; set explicitly when the real v2
/// endpoint is different.
pub fn fetch_clob_market_info(
    api_url_prefix: &str,
    condition_id: &str,
    path_template: &str,
) -> Result<MarketInfoV2> {
    let path = if path_template.is_empty() {
        DEFAULT_PATH_TEMPLATE.replace("{conditionId}", condition_id)
    } else {
        path_template
            .replace("{conditionId}", condition_id)
            .replace("{condition_id}", condition_id)
    };
    let url = format!("{}{}", api_url_prefix.trim_end_matches('/'), path);

    let raw = crate::async_rt::blocking_get_text(&url)
        .map_err(|e| anyhow!("market-info fetch {} failed: {}", url, e))?;
    let json: Value = serde_json::from_str(&raw)
        .map_err(|e| anyhow!("market-info parse {} failed: {} (body: {})", url, e, &raw[..raw.len().min(200)]))?;
    parse_market_info_for_condition(&json, condition_id)
        .map_err(|e| anyhow!("{}: url={}  body={}", e, url, &raw[..raw.len().min(200)]))
}

/// Enqueue a fetch on the bounded runtime job executor; return a channel the
/// strategy can `try_recv` on each tick. Never blocks the caller.
pub fn spawn_market_info_v2_fetch(
    api_url_prefix: String,
    condition_id: String,
    path_template: String,
) -> crossbeam_channel::Receiver<Option<MarketInfoV2>> {
    let key = market_info_key(api_url_prefix, condition_id, path_template);
    let (rx, is_leader) = subscribe_market_info(&key);
    if !is_leader {
        return rx;
    }
    let worker_key = key.clone();
    let submit_result = hexagent_runtime::background_jobs::try_submit(move || {
        const ATTEMPTS: u32 = 4;
        let mut backoff = std::time::Duration::from_millis(200);
        let mut result = None;
        for attempt in 1..=ATTEMPTS {
            match fetch_clob_market_info(
                &worker_key.api_url_prefix,
                &worker_key.condition_id,
                &worker_key.path_template,
            ) {
                Ok(market_info) => {
                    info!(
                            "[market_info_v2] fetched cid={}... fee_rate={:.4} fee_exponent={:.2} bps={} taker_only={} attempt={}",
                            &worker_key.condition_id[..worker_key.condition_id.len().min(16)],
                            market_info.fee_rate,
                            market_info.fee_exponent,
                            market_info.fee_rate_bps,
                            market_info.taker_only,
                            attempt,
                        );
                    result = Some(market_info);
                    break;
                }
                Err(error) => {
                    warn!(
                        "[market_info_v2] fetch attempt {}/{} failed cid={}...: {}",
                        attempt,
                        ATTEMPTS,
                        &worker_key.condition_id[..worker_key.condition_id.len().min(16)],
                        error,
                    );
                    if attempt < ATTEMPTS {
                        std::thread::sleep(backoff);
                        backoff = (backoff * 2).min(std::time::Duration::from_secs(2));
                    }
                }
            }
        }
        finish_market_info_fetch(&worker_key, result);
    });
    if let Err(error) = submit_result {
        warn!("[market_info_v2] failed to enqueue fetch job: {}", error);
        finish_market_info_fetch(&key, None);
    }
    rx
}

/// Parse the v2 `getClobMarketInfo` response.
///
/// Primary shape (confirmed against Polymarket's v2 SDK and live
/// `clob-v2.polymarket.com` responses):
///
/// ```json
/// {
///   "c":   "<condition_id>",
///   "t":   [ { "t": "<token_id>", "o": "Yes" }, ... ],
///   "mos": 5, "mts": 0.001,
///   "ao":  true, "nr": true, ...
///   "fd":  { "r": <rate>, "e": <exponent>, "to": <takerOnly> }
/// }
/// ```
///
/// The `fd` ("fee details") object may be **absent** on a structurally
/// complete market response with zero fees — the server simply omits it.
/// Only that complete shape is treated as
/// `(fee_rate=0, exponent=1, taker_only=false)`; empty/error payloads are not.
///
/// Accepts alternate field names as fallbacks for robustness in case
/// Polymarket renames them later:
///   - fee rate:     `fd.r`, `feeRate`, `fee_rate`, `takerFeeRate`,
///                   `fd.feeRate`
///   - exponent:     `fd.e`, `feeExponent`, `fee_exponent`,
///                   `fd.feeExponent`
///   - taker_only:   `fd.to`, `takerOnly`, `onlyTaker`, `fd.takerOnly`
///   - bps (legacy): `feeRateBps`, `takerBaseFee`, `baseFee` — used if
///                   no `fee_rate` float is present, divided by 1e4.
pub fn parse_market_info(json: &Value) -> Result<MarketInfoV2> {
    parse_market_info_inner(json, None)
}

fn parse_market_info_for_condition(json: &Value, condition_id: &str) -> Result<MarketInfoV2> {
    parse_market_info_inner(json, Some(condition_id))
}

fn parse_market_info_inner(json: &Value, expected_condition_id: Option<&str>) -> Result<MarketInfoV2> {
    let envelope = json.as_object().ok_or_else(|| anyhow!("market-info response is not an object"))?;
    if envelope.get("success").and_then(Value::as_bool) == Some(false) {
        return Err(anyhow!("market-info response reports success=false"));
    }
    for key in ["error", "errorMsg", "error_message"] {
        if envelope.get(key).is_some_and(|value| !value.is_null()) {
            return Err(anyhow!("market-info response contains {}", key));
        }
    }

    // Peel `{ "data": {...} }` wrappers, but never turn an explicitly null
    // data payload into a schema-less fee-free market.
    let root = match envelope.get("data") {
        Some(value) => value.as_object()
            .map(|_| value)
            .ok_or_else(|| anyhow!("market-info data is not an object"))?,
        None => json,
    };
    let root_obj = root.as_object().ok_or_else(|| anyhow!("market-info payload is not an object"))?;
    if root_obj.get("fd").is_some_and(|value| !value.is_object()) {
        return Err(anyhow!("market-info fee details are not an object"));
    }

    let response_condition_id = ["c", "conditionId", "condition_id"]
        .iter()
        .find_map(|key| root_obj.get(*key).and_then(Value::as_str));
    if let Some(expected) = expected_condition_id {
        let actual = response_condition_id
            .ok_or_else(|| anyhow!("market-info payload has no condition id"))?;
        if !actual.eq_ignore_ascii_case(expected) {
            return Err(anyhow!(
                "market-info condition mismatch: expected {}, got {}",
                expected,
                actual,
            ));
        }
    }

    // Helpers accept both "root-level key" and "nested path via '.'".
    let lookup = |keys: &[&str]| -> Option<Value> {
        for k in keys {
            let parts: Vec<&str> = k.split('.').collect();
            let mut cur = root;
            let mut ok = true;
            for p in &parts {
                match cur.get(*p) { Some(v) => cur = v, None => { ok = false; break; } }
            }
            if ok { return Some(cur.clone()); }
        }
        None
    };
    let as_f64 = |v: &Value| -> Option<f64> {
        v.as_f64()
            .or_else(|| v.as_i64().map(|i| i as f64))
            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
    };
    let as_bool = |v: &Value| -> Option<bool> {
        v.as_bool()
            .or_else(|| v.as_str().and_then(|s| match s.to_ascii_lowercase().as_str() {
                "true" | "1" => Some(true),
                "false" | "0" => Some(false),
                _ => None,
            }))
    };
    let as_u32 = |v: &Value| -> Option<u32> {
        v.as_u64().and_then(|u| u32::try_from(u).ok())
            .or_else(|| v.as_f64().filter(|f| f.is_finite() && *f >= 0.0 && *f <= u32::MAX as f64).map(|f| f.round() as u32))
            .or_else(|| v.as_str().and_then(|s| s.parse::<u32>().ok()))
    };

    let fee_rate_v = lookup(&[
        "fd.r", "feeRate", "fee_rate", "takerFeeRate", "fd.feeRate",
    ]);
    let fee_exp_v = lookup(&[
        "fd.e", "feeExponent", "fee_exponent", "fd.feeExponent", "feeRateExponent",
    ]);
    let taker_only_v = lookup(&[
        "fd.to", "takerOnly", "onlyTaker", "fd.takerOnly", "takerOnlyMarket",
    ]);
    let bps_v = lookup(&[
        "feeRateBps", "takerBaseFee", "baseFee", "fee_rate_bps",
    ]);

    let fee_rate = match fee_rate_v.as_ref() {
        Some(value) => Some(as_f64(value).ok_or_else(|| anyhow!("invalid fee rate"))?),
        None => None,
    };
    let fee_exponent = match fee_exp_v.as_ref() {
        Some(value) => as_f64(value).ok_or_else(|| anyhow!("invalid fee exponent"))?,
        None => 1.0,
    };
    let taker_only = match taker_only_v.as_ref() {
        Some(value) => as_bool(value).ok_or_else(|| anyhow!("invalid taker-only flag"))?,
        None => false,
    };
    let fee_rate_bps = match bps_v.as_ref() {
        Some(value) => Some(as_u32(value).ok_or_else(|| anyhow!("invalid fee rate bps"))?),
        None => None,
    };

    if !fee_exponent.is_finite() {
        return Err(anyhow!("fee exponent is not finite"));
    }

    if fee_rate.is_none() && fee_rate_bps.is_none() {
        let has_tokens = ["t", "tokens"].iter().any(|key| {
            root_obj.get(*key).and_then(Value::as_array).is_some_and(|tokens| !tokens.is_empty())
        });
        if response_condition_id.is_none() || !has_tokens {
            return Err(anyhow!(
                "market-info payload lacks both fee data and a complete market schema"
            ));
        }
    }

    // Derive missing representations, treating "no fee data" as zero
    // (Polymarket omits `fd` on fee-free markets — this is valid).
    let (fee_rate_final, fee_rate_bps_final) = match (fee_rate, fee_rate_bps) {
        (Some(r), Some(bps)) => (r, bps),
        (Some(r), None)      => (r, (r * 10_000.0).round() as u32),
        (None, Some(bps))    => (bps as f64 / 10_000.0, bps),
        (None, None)         => (0.0, 0), // complete fee-free market schema
    };
    crate::types::BinaryOption::validate_polymarket_fee_curve(
        fee_rate_final, fee_exponent, fee_rate_bps_final,
    ).map_err(|error| anyhow!(error))?;

    Ok(MarketInfoV2 {
        fee_rate: fee_rate_final,
        fee_exponent,
        fee_rate_bps: fee_rate_bps_final,
        taker_only,
        raw: json.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Canonical v2 shape: `fd` object with short-name subfields.
    #[test]
    fn parse_canonical_fd_object() {
        let json: Value = serde_json::json!({
            "c":  "0xabc",
            "ao": true,
            "fd": { "r": 0.02, "e": 1.0, "to": false },
        });
        let mi = parse_market_info(&json).unwrap();
        assert!((mi.fee_rate - 0.02).abs() < 1e-9);
        assert!((mi.fee_exponent - 1.0).abs() < 1e-9);
        assert_eq!(mi.fee_rate_bps, 200);
        assert!(!mi.taker_only);
    }

    #[test]
    fn parse_fd_taker_only_true() {
        let json: Value = serde_json::json!({
            "fd": { "r": 0.01, "e": 1.5, "to": true },
        });
        let mi = parse_market_info(&json).unwrap();
        assert!(mi.taker_only);
        assert!((mi.fee_exponent - 1.5).abs() < 1e-9);
    }

    /// When `fd` is absent (fee-free market) treat as zero fees.
    #[test]
    fn parse_missing_fd_is_zero_fees() {
        let json: Value = serde_json::json!({
            "c": "0xabc",
            "ao": true,
            "t": [{ "t": "up" }, { "t": "down" }],
        });
        let mi = parse_market_info(&json).unwrap();
        assert_eq!(mi.fee_rate, 0.0);
        assert_eq!(mi.fee_rate_bps, 0);
        assert_eq!(mi.fee_exponent, 1.0);
        assert!(!mi.taker_only);
    }

    /// Legacy camelCase fallback still works.
    #[test]
    fn parse_legacy_camelcase() {
        let json: Value = serde_json::json!({
            "feeRate": 0.02, "feeExponent": 1.0, "feeRateBps": 200, "takerOnly": false,
        });
        let mi = parse_market_info(&json).unwrap();
        assert!((mi.fee_rate - 0.02).abs() < 1e-9);
        assert_eq!(mi.fee_rate_bps, 200);
    }

    #[test]
    fn parse_wrapped_data_key() {
        let json: Value = serde_json::json!({
            "data": { "fd": { "r": 0.01, "to": true } }
        });
        let mi = parse_market_info(&json).unwrap();
        assert!((mi.fee_rate - 0.01).abs() < 1e-9);
        assert_eq!(mi.fee_rate_bps, 100);
        assert!(mi.taker_only);
    }

    #[test]
    fn parse_derives_fee_rate_from_bps() {
        let json: Value = serde_json::json!({ "takerBaseFee": 250 });
        let mi = parse_market_info(&json).unwrap();
        assert_eq!(mi.fee_rate_bps, 250);
        assert!((mi.fee_rate - 0.025).abs() < 1e-9);
        assert!((mi.fee_exponent - 1.0).abs() < 1e-9);
    }

    #[test]
    fn parse_accepts_string_numbers() {
        let json: Value = serde_json::json!({
            "fd": { "r": "0.02", "e": "1.5" }
        });
        let mi = parse_market_info(&json).unwrap();
        assert!((mi.fee_rate - 0.02).abs() < 1e-9);
        assert!((mi.fee_exponent - 1.5).abs() < 1e-9);
    }

    #[test]
    fn parse_rejects_non_authoritative_zero_fee_payloads() {
        for json in [
            serde_json::json!({}),
            serde_json::json!({ "data": null }),
            serde_json::json!({ "success": false }),
            serde_json::json!({ "error": "upstream unavailable" }),
            serde_json::json!({ "c": "0xabc", "ao": true }),
        ] {
            assert!(parse_market_info(&json).is_err(), "accepted {json}");
        }
    }

    #[test]
    fn parse_rejects_invalid_fee_values() {
        for json in [
            serde_json::json!({ "fd": { "r": "NaN", "e": 1.0 } }),
            serde_json::json!({ "fd": { "r": -0.01, "e": 1.0 } }),
            serde_json::json!({ "fd": { "r": 0.01, "e": 0.0 } }),
            serde_json::json!({ "feeRateBps": 10001 }),
        ] {
            assert!(parse_market_info(&json).is_err(), "accepted {json}");
        }
    }

    #[test]
    fn fetched_market_info_must_name_the_requested_condition() {
        let json = serde_json::json!({
            "c": "0xdef",
            "t": [{ "t": "up" }],
        });
        assert!(parse_market_info_for_condition(&json, "0xabc").is_err());
        assert!(parse_market_info_for_condition(&json, "0xdef").is_ok());
    }

    #[test]
    fn condition_singleflight_fans_out_and_caches_success() {
        let key = market_info_key(
            "https://example.invalid/".to_string(),
            "0xSINGLEFLIGHT-TEST".to_string(),
            String::new(),
        );
        let (first, first_is_leader) = subscribe_market_info(&key);
        let (second, second_is_leader) = subscribe_market_info(&key);
        assert!(first_is_leader);
        assert!(!second_is_leader);

        let expected = MarketInfoV2 {
            fee_rate: 0.01,
            fee_exponent: 1.0,
            fee_rate_bps: 100,
            taker_only: true,
            raw: serde_json::json!({"test": true}),
        };
        finish_market_info_fetch(&key, Some(expected.clone()));
        assert_eq!(first.recv().unwrap().unwrap().fee_rate_bps, 100);
        assert_eq!(second.recv().unwrap().unwrap().fee_rate_bps, 100);

        let (cached, cached_is_leader) = subscribe_market_info(&key);
        assert!(!cached_is_leader);
        assert_eq!(cached.recv().unwrap().unwrap().fee_rate_bps, 100);
    }

    #[test]
    fn market_info_owner_reports_cached_roundtrip_tail_and_bounded_overflow() {
        const EVENTS: usize = 4_096;
        let key = market_info_key(
            "https://example.invalid/".to_string(),
            format!("0xOWNER-LATENCY-{}", std::process::id()),
            String::new(),
        );
        let (initial, leader) = subscribe_market_info(&key);
        assert!(leader);
        finish_market_info_fetch(
            &key,
            Some(MarketInfoV2 {
                fee_rate: 0.01,
                fee_exponent: 1.0,
                fee_rate_bps: 100,
                taker_only: true,
                raw: serde_json::Value::Null,
            }),
        );
        assert_eq!(initial.recv().unwrap().unwrap().fee_rate_bps, 100);

        let mut samples = Vec::with_capacity(EVENTS);
        for _ in 0..EVENTS {
            let started = Instant::now();
            let (cached, is_leader) = subscribe_market_info(&key);
            assert!(!is_leader);
            assert_eq!(cached.recv().unwrap().unwrap().fee_rate_bps, 100);
            samples.push(started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
        }
        samples.sort_unstable();
        let percentile = |numerator: usize, denominator: usize| {
            samples[(samples.len() - 1) * numerator / denominator]
        };
        let metrics = market_info_owner_metrics();
        eprintln!(
            "market-info owner: boundary=subscribe_send_to_cached_reply n={} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_high_water={} overflow={}",
            samples.len(),
            percentile(1, 2),
            percentile(99, 100),
            percentile(999, 1_000),
            samples.last().copied().unwrap_or_default(),
            metrics.queue_high_water,
            metrics.queue_overflow,
        );
        assert!(metrics.queue_high_water > 0);
        assert_eq!(metrics.queue_overflow, 0);

        let (full_tx, _full_rx) = crossbeam_channel::bounded::<u8>(MARKET_INFO_OWNER_CAPACITY);
        for _ in 0..MARKET_INFO_OWNER_CAPACITY {
            full_tx.try_send(1).unwrap();
        }
        assert!(matches!(
            full_tx.try_send(1),
            Err(crossbeam_channel::TrySendError::Full(1))
        ));
    }
}
