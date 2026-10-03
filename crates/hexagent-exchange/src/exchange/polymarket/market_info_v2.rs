//! CLOB v2 per-market fee / flags fetch.
//!
//! v2 moves fee computation entirely to the protocol: the signed
//! order no longer carries `feeRateBps`. At match time the server
//! computes
//!
//! ```text
//! fee = C × feeRate × (p × (1 − p)) ^ exponent
//! ```
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
    Fetching {
        generation: u64,
        waiters: Vec<crossbeam_channel::Sender<Option<MarketInfoV2>>>,
    },
    Ready {
        fetched_at: Instant,
        value: MarketInfoV2,
    },
}

enum MarketInfoOwnerCommand {
    Subscribe {
        key: MarketInfoKey,
        subscriber: crossbeam_channel::Sender<Option<MarketInfoV2>>,
    },
    Finish {
        key: MarketInfoKey,
        generation: u64,
        result: Option<MarketInfoV2>,
    },
}

const MARKET_INFO_OWNER_CAPACITY: usize = 256;
const MARKET_INFO_CACHE_CAPACITY: usize = 1_024;
const MARKET_INFO_WAITER_CAPACITY: usize = 4_096;
static MARKET_INFO_OWNER: OnceLock<hexagent_runtime::poll_channel::Sender<MarketInfoOwnerCommand>> =
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

/// Mutable cache belongs only to the existing background metadata owner.
#[derive(Default)]
struct MarketInfoOwnerState {
    entries: HashMap<MarketInfoKey, MarketInfoFlight>,
    waiter_count: usize,
    next_generation: u64,
}

impl MarketInfoOwnerState {
    /// Returns a generation only for a newly admitted flight. The owner, never
    /// the strategy, starts that fetch. No acknowledgement round trip exists.
    fn subscribe(
        &mut self,
        key: MarketInfoKey,
        subscriber: crossbeam_channel::Sender<Option<MarketInfoV2>>,
    ) -> Option<u64> {
        self.entries.retain(|_, entry| match entry {
            MarketInfoFlight::Fetching { .. } => true,
            MarketInfoFlight::Ready { fetched_at, .. } => {
                fetched_at.elapsed() < Duration::from_secs(2 * 60 * 60)
            }
        });
        match self.entries.get_mut(&key) {
            Some(MarketInfoFlight::Ready { value, .. }) => {
                let _ = subscriber.try_send(Some(value.clone()));
                return None;
            }
            Some(MarketInfoFlight::Fetching { waiters, .. })
                if self.waiter_count < MARKET_INFO_WAITER_CAPACITY =>
            {
                waiters.push(subscriber);
                self.waiter_count += 1;
                return None;
            }
            _ => {}
        }
        if !self.entries.contains_key(&key) && self.entries.len() >= MARKET_INFO_CACHE_CAPACITY {
            let oldest_ready = self
                .entries
                .iter()
                .filter_map(|(key, entry)| match entry {
                    MarketInfoFlight::Ready { fetched_at, .. } => Some((key.clone(), *fetched_at)),
                    MarketInfoFlight::Fetching { .. } => None,
                })
                .min_by_key(|(_, at)| *at)
                .map(|(key, _)| key);
            if let Some(key) = oldest_ready {
                self.entries.remove(&key);
            }
        }
        if self.waiter_count >= MARKET_INFO_WAITER_CAPACITY
            || self.entries.len() >= MARKET_INFO_CACHE_CAPACITY
            || self.entries.contains_key(&key)
        {
            MARKET_INFO_QUEUE_OVERFLOW.fetch_add(1, Ordering::Relaxed);
            let _ = subscriber.try_send(None);
            return None;
        }
        self.next_generation = self
            .next_generation
            .checked_add(1)
            .expect("market-info generation exhausted");
        let generation = self.next_generation;
        self.entries.insert(
            key,
            MarketInfoFlight::Fetching {
                generation,
                waiters: vec![subscriber],
            },
        );
        self.waiter_count += 1;
        Some(generation)
    }

    fn finish(&mut self, key: MarketInfoKey, generation: u64, result: Option<MarketInfoV2>) {
        // A late/duplicate result must not finish a newer retry for this key.
        if !matches!(self.entries.get(&key), Some(MarketInfoFlight::Fetching { generation: active, .. }) if *active == generation)
        {
            return;
        }
        let Some(MarketInfoFlight::Fetching { waiters, .. }) = self.entries.remove(&key) else {
            unreachable!()
        };
        self.waiter_count -= waiters.len();
        if let Some(value) = result.as_ref() {
            self.entries.insert(
                key,
                MarketInfoFlight::Ready {
                    fetched_at: Instant::now(),
                    value: value.clone(),
                },
            );
        }
        for waiter in waiters {
            let _ = waiter.try_send(result.clone());
        }
    }
}

/// Startup only. Existing background affinity/topology role, bounded FIFO256.
/// Callers on strategy threads only use the already installed sender.
pub fn prewarm_market_info_owner() -> Result<()> {
    hexagent_runtime::background_jobs::prewarm().map_err(|error| anyhow!(error))?;
    MARKET_INFO_OWNER.get_or_init(|| {
        let (tx, rx) = hexagent_runtime::poll_channel::bounded(MARKET_INFO_OWNER_CAPACITY);
        let finish_tx = tx.clone();
        std::thread::Builder::new()
            .name("poly-market-info-owner".to_string())
            .spawn(move || {
                crate::os_tune::pin_background("poly-market-info-owner");
                let mut state = MarketInfoOwnerState::default();
                loop {
                    let command = match rx
                        .recv_timeout_with_poll(Duration::from_secs(1), Duration::from_millis(1))
                    {
                        Ok(command) => command,
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                    };
                    match command {
                        MarketInfoOwnerCommand::Subscribe { key, subscriber } => {
                            let key = market_info_key(
                                key.api_url_prefix,
                                key.condition_id,
                                key.path_template,
                            );
                            if let Some(generation) = state.subscribe(key.clone(), subscriber) {
                                let worker_key = key.clone();
                                let completion = finish_tx.clone();
                                if hexagent_runtime::background_jobs::try_submit(move || {
                                    let result = fetch_market_info_with_retry(&worker_key);
                                    // Cold producer: retain completion on full. The owner never
                                    // waits for this worker, so capacity cannot strand a flight.
                                    let _ = completion.send(MarketInfoOwnerCommand::Finish {
                                        key: worker_key,
                                        generation,
                                        result,
                                    });
                                })
                                .is_err()
                                {
                                    MARKET_INFO_QUEUE_OVERFLOW.fetch_add(1, Ordering::Relaxed);
                                    state.finish(key, generation, None);
                                }
                            }
                        }
                        MarketInfoOwnerCommand::Finish {
                            key,
                            generation,
                            result,
                        } => state.finish(key, generation, result),
                    }
                }
            })
            .expect("failed to spawn market-info owner");
        tx
    });
    Ok(())
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
    owner: Option<&hexagent_runtime::poll_channel::Sender<MarketInfoOwnerCommand>>,
    key: MarketInfoKey,
) -> crossbeam_channel::Receiver<Option<MarketInfoV2>> {
    // One bounded response slot per market-control request (not per quote).
    let (tx, rx) = crossbeam_channel::bounded(1);
    let command = MarketInfoOwnerCommand::Subscribe {
        key,
        subscriber: tx,
    };
    let admitted_depth = owner.map_or(0, |owner| {
        owner
            .len()
            .saturating_add(1)
            .min(MARKET_INFO_OWNER_CAPACITY)
    });
    let rejected = match owner {
        Some(owner) => owner
            .try_send(command)
            .err()
            .map(|error| error.into_inner()),
        None => Some(command),
    };
    if let Some(MarketInfoOwnerCommand::Subscribe { subscriber, .. }) = rejected {
        MARKET_INFO_QUEUE_OVERFLOW.fetch_add(1, Ordering::Relaxed);
        let _ = subscriber.try_send(None);
    } else {
        MARKET_INFO_QUEUE_HIGH_WATER.fetch_max(admitted_depth, Ordering::Relaxed);
    }
    rx
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
    let json: Value = serde_json::from_str(&raw).map_err(|e| {
        anyhow!(
            "market-info parse {} failed: {} (body: {})",
            url,
            e,
            &raw[..raw.len().min(200)]
        )
    })?;
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
    subscribe_market_info(
        MARKET_INFO_OWNER.get(),
        MarketInfoKey {
            api_url_prefix,
            condition_id,
            path_template,
        },
    )
}

fn fetch_market_info_with_retry(worker_key: &MarketInfoKey) -> Option<MarketInfoV2> {
    const ATTEMPTS: u32 = 4;
    let mut backoff = std::time::Duration::from_millis(200);
    for attempt in 1..=ATTEMPTS {
        match fetch_clob_market_info(
            &worker_key.api_url_prefix,
            &worker_key.condition_id,
            &worker_key.path_template,
        ) {
            Ok(market_info) => {
                info!("[market_info_v2] fetched cid={}... fee_rate={:.4} fee_exponent={:.2} bps={} taker_only={} attempt={}",
                    &worker_key.condition_id[..worker_key.condition_id.len().min(16)], market_info.fee_rate,
                    market_info.fee_exponent, market_info.fee_rate_bps, market_info.taker_only, attempt);
                return Some(market_info);
            }
            Err(error) => {
                warn!(
                    "[market_info_v2] fetch attempt {}/{} failed cid={}...: {}",
                    attempt,
                    ATTEMPTS,
                    &worker_key.condition_id[..worker_key.condition_id.len().min(16)],
                    error
                );
                if attempt < ATTEMPTS {
                    std::thread::sleep(backoff);
                    backoff = (backoff * 2).min(std::time::Duration::from_secs(2));
                }
            }
        }
    }
    None
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

    fn test_key(id: &str) -> MarketInfoKey {
        market_info_key("https://example.invalid/".into(), id.into(), String::new())
    }

    fn test_value(bps: u32) -> MarketInfoV2 {
        MarketInfoV2 {
            fee_rate: bps as f64 / 10_000.0,
            fee_exponent: 1.0,
            fee_rate_bps: bps,
            taker_only: true,
            raw: Value::Null,
        }
    }

    #[test]
    fn background_owner_fetches_once_and_delivers_to_all_subscribers() {
        use std::io::{Read, Write};
        crate::async_rt::init().unwrap();
        prewarm_market_info_owner().unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (arrived_tx, arrived) = crossbeam_channel::bounded(1);
        let (release, release_rx) = crossbeam_channel::bounded(1);
        let server = std::thread::spawn(move || {
            listener.set_nonblocking(true).unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            let (mut socket, _) = loop {
                match listener.accept() {
                    Ok(connection) => break connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(Instant::now() < deadline, "metadata fetch was not dispatched");
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("{error}"),
                }
            };
            socket.set_nonblocking(false).unwrap();
            socket.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            let mut request = Vec::new();
            let mut bytes = [0; 1024];
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                let read = socket.read(&mut bytes).unwrap();
                assert!(read > 0);
                request.extend_from_slice(&bytes[..read]);
            }
            assert!(String::from_utf8(request).unwrap().starts_with("GET /clob-markets/0xabc "));
            arrived_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let body = r#"{"c":"0xabc","fd":{"r":0.01,"e":1.0,"to":true}}"#;
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
            // Drop the listener: a second HTTP fetch cannot produce success.
        });
        let first = spawn_market_info_v2_fetch(url.clone(), "0xABC".into(), String::new());
        arrived.recv_timeout(Duration::from_secs(5)).unwrap();
        let second = spawn_market_info_v2_fetch(url.clone(), "0xabc".into(), String::new());
        assert!(first.is_empty());
        assert!(second.is_empty());
        release.send(()).unwrap();
        for result in [first, second] {
            assert_eq!(result.recv_timeout(Duration::from_secs(5)).unwrap().unwrap().fee_rate_bps, 100);
        }
        server.join().unwrap();
        let cached = spawn_market_info_v2_fetch(url, "0xabc".into(), String::new());
        assert_eq!(cached.recv_timeout(Duration::from_secs(5)).unwrap().unwrap().fee_rate_bps, 100);
    }

    #[test]
    fn condition_singleflight_fans_out_and_caches_success() {
        let mut state = MarketInfoOwnerState::default();
        let key = test_key("0xABC");
        let (tx1, first) = crossbeam_channel::bounded(1);
        let (tx2, second) = crossbeam_channel::bounded(1);
        let generation = state.subscribe(key.clone(), tx1).unwrap();
        assert_eq!(state.subscribe(test_key("0xabc"), tx2), None);
        state.finish(key.clone(), generation, Some(test_value(100)));
        assert_eq!(first.try_recv().unwrap().unwrap().fee_rate_bps, 100);
        assert_eq!(second.try_recv().unwrap().unwrap().fee_rate_bps, 100);
        let (tx, cached) = crossbeam_channel::bounded(1);
        assert_eq!(state.subscribe(key.clone(), tx), None);
        assert_eq!(cached.try_recv().unwrap().unwrap().fee_rate_bps, 100);
        assert_eq!(state.waiter_count, 0);
        state.finish(key, generation, Some(test_value(200)));
        assert!(
            first.try_recv().is_err(),
            "duplicate result cannot duplicate delivery"
        );
    }

    #[test]
    fn failed_fetch_retry_rejects_stale_completion_and_isolates_keys() {
        let mut state = MarketInfoOwnerState::default();
        let key = test_key("retry");
        let (tx, rx) = crossbeam_channel::bounded(1);
        let old = state.subscribe(key.clone(), tx).unwrap();
        state.finish(key.clone(), old, None);
        assert!(rx.try_recv().unwrap().is_none());
        let (tx, retry) = crossbeam_channel::bounded(1);
        let new = state.subscribe(key.clone(), tx).unwrap();
        let (other_tx, other) = crossbeam_channel::bounded(1);
        let other_gen = state.subscribe(test_key("other"), other_tx).unwrap();
        state.finish(key.clone(), old, Some(test_value(900)));
        assert!(retry.try_recv().is_err());
        state.finish(key, new, Some(test_value(100)));
        assert_eq!(retry.try_recv().unwrap().unwrap().fee_rate_bps, 100);
        assert!(other.try_recv().is_err());
        state.finish(test_key("other"), other_gen, Some(test_value(200)));
        assert_eq!(other.try_recv().unwrap().unwrap().fee_rate_bps, 200);
        assert_eq!(state.waiter_count, 0);
    }

    #[test]
    fn stalled_owner_cannot_block_submit_and_full_or_missing_owner_returns_failure() {
        let (tx, rx) = hexagent_runtime::poll_channel::bounded(1);
        // Owner has not run at all: submission still completes synchronously.
        let first = subscribe_market_info(Some(&tx), test_key("first"));
        assert_eq!(rx.len(), 1);
        assert!(first.try_recv().is_err());
        let rejected = subscribe_market_info(Some(&tx), test_key("second"));
        assert!(rejected.try_recv().unwrap().is_none());
        let MarketInfoOwnerCommand::Subscribe { key, subscriber } = rx.try_recv().unwrap() else {
            panic!()
        };
        assert_eq!(
            key.condition_id, "first",
            "full queue preserves its admitted FIFO head"
        );
        let mut state = MarketInfoOwnerState::default();
        let generation = state.subscribe(key.clone(), subscriber).unwrap();
        state.finish(key, generation, Some(test_value(100)));
        assert_eq!(first.try_recv().unwrap().unwrap().fee_rate_bps, 100);
        assert!(subscribe_market_info(None, test_key("uninitialized"))
            .try_recv()
            .unwrap()
            .is_none());
        drop(rx);
        assert!(subscribe_market_info(Some(&tx), test_key("disconnected"))
            .try_recv()
            .unwrap()
            .is_none());
    }

    #[test]
    fn waiter_and_cache_limits_fail_closed_without_orphaning_admitted_requests() {
        let mut state = MarketInfoOwnerState::default();
        let key = test_key("waiters");
        let (tx, first) = crossbeam_channel::bounded(1);
        let generation = state.subscribe(key.clone(), tx).unwrap();
        for _ in 1..MARKET_INFO_WAITER_CAPACITY {
            let (tx, _) = crossbeam_channel::bounded(1);
            assert_eq!(state.subscribe(key.clone(), tx), None);
        }
        let (tx, full) = crossbeam_channel::bounded(1);
        assert_eq!(
            state.subscribe(test_key("new-flight-at-waiter-limit"), tx),
            None
        );
        assert!(full.try_recv().unwrap().is_none());
        assert_eq!(state.waiter_count, MARKET_INFO_WAITER_CAPACITY);
        state.finish(key, generation, Some(test_value(100)));
        assert_eq!(state.waiter_count, 0);
        assert_eq!(first.try_recv().unwrap().unwrap().fee_rate_bps, 100);
        let mut state = MarketInfoOwnerState::default();
        for i in 0..MARKET_INFO_CACHE_CAPACITY {
            let (tx, _) = crossbeam_channel::bounded(1);
            assert!(state
                .subscribe(test_key(&format!("pending-{i}")), tx)
                .is_some());
        }
        let (tx, full) = crossbeam_channel::bounded(1);
        assert_eq!(state.subscribe(test_key("over-capacity"), tx), None);
        assert!(full.try_recv().unwrap().is_none());
        assert_eq!(state.entries.len(), MARKET_INFO_CACHE_CAPACITY);
    }

    #[test]
    fn cached_metadata_expires_and_new_generation_can_fetch_again() {
        let mut state = MarketInfoOwnerState::default();
        state.entries.insert(
            test_key("expired"),
            MarketInfoFlight::Ready {
                fetched_at: Instant::now() - Duration::from_secs(2 * 60 * 60 + 1),
                value: test_value(100),
            },
        );
        let (tx, rx) = crossbeam_channel::bounded(1);
        assert!(state.subscribe(test_key("expired"), tx).is_some());
        assert!(rx.try_recv().is_err());
    }

    #[test]
    #[ignore = "focused benchmark; run release --ignored --nocapture"]
    fn market_info_submit_tail_benchmark() {
        const N: usize = 4096;
        fn report(label: &str, mut samples: Vec<u64>, high_water: usize) {
            samples.sort_unstable();
            eprintln!("market-info {label}: n={} median_ns={} p99_ns={} p999_ns={} max_ns={} queue_high_water={} overflow=0 owner_delay_us=200",
                samples.len(), samples[(samples.len()-1)/2], samples[(samples.len()-1)*99/100],
                samples[(samples.len()-1)*999/1000], samples.last().unwrap(), high_water);
        }
        // Reproduce the old leader acknowledgement boundary. Fixed cold work
        // exposes the dependency; this is not a claim about production RTT.
        let (old_tx, old_rx) = crossbeam_channel::bounded::<crossbeam_channel::Sender<bool>>(1);
        let old_owner = std::thread::spawn(move || {
            while let Ok(reply) = old_rx.recv() {
                std::thread::sleep(Duration::from_micros(200));
                reply.send(false).unwrap();
            }
        });
        let mut old = Vec::with_capacity(N);
        for _ in 0..N {
            let start = Instant::now();
            let (tx, rx) = crossbeam_channel::bounded(1);
            old_tx.send_timeout(tx, Duration::from_secs(2)).unwrap();
            rx.recv().unwrap();
            old.push(start.elapsed().as_nanos() as u64);
        }
        drop(old_tx);
        old_owner.join().unwrap();
        let (tx, rx) = hexagent_runtime::poll_channel::bounded(1);
        let owner = std::thread::spawn(move || {
            let mut state = MarketInfoOwnerState::default();
            for _ in 0..N {
                let MarketInfoOwnerCommand::Subscribe { key, subscriber } =
                    rx.recv_timeout(Duration::from_secs(1)).unwrap()
                else {
                    panic!()
                };
                std::thread::sleep(Duration::from_micros(200));
                if let Some(generation) = state.subscribe(key.clone(), subscriber) {
                    state.finish(key, generation, Some(test_value(100)));
                }
            }
        });
        let mut submit = Vec::with_capacity(N);
        let mut end_to_end = Vec::with_capacity(N);
        for _ in 0..N {
            let key = test_key("bench");
            let start = Instant::now();
            let reply = subscribe_market_info(Some(&tx), key);
            submit.push(start.elapsed().as_nanos() as u64);
            assert_eq!(
                reply
                    .recv_timeout(Duration::from_secs(1))
                    .unwrap()
                    .unwrap()
                    .fee_rate_bps,
                100
            );
            end_to_end.push(start.elapsed().as_nanos() as u64);
        }
        owner.join().unwrap();
        report("before_strategy_submit_to_leader_ack", old, 1);
        report(
            "after_strategy_submit_including_response_slot_allocation",
            submit,
            1,
        );
        report("after_metadata_delivery", end_to_end, 1);
    }
}
