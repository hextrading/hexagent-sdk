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
//! We fetch this **once per event** at `on_instrument` time on a
//! background thread, then cache on `EventContext`. Falls back to the
//! gamma-api-supplied `base_fee` / `fee_rate` / `fee_exponent` fields
//! if the fetch fails.
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
    parse_market_info(&json).map_err(|e| anyhow!("{}: url={}  body={}", e, url, &raw[..raw.len().min(200)]))
}

/// Spawn a fetch on a dedicated short-lived thread; return a channel
/// the strategy can `try_recv` on each tick. Never blocks the caller.
pub fn spawn_market_info_v2_fetch(
    api_url_prefix: String,
    condition_id: String,
    path_template: String,
) -> crossbeam_channel::Receiver<Option<MarketInfoV2>> {
    let (tx, rx) = crossbeam_channel::bounded(1);
    let _ = std::thread::Builder::new()
        .name("clob-v2-market-info".into())
        .spawn(move || {
            let result = match fetch_clob_market_info(&api_url_prefix, &condition_id, &path_template) {
                Ok(info) => {
                    info!(
                        "[market_info_v2] fetched cid={}... fee_rate={:.4} fee_exponent={:.2} bps={} taker_only={}",
                        &condition_id[..condition_id.len().min(16)],
                        info.fee_rate, info.fee_exponent, info.fee_rate_bps, info.taker_only,
                    );
                    Some(info)
                }
                Err(e) => {
                    warn!(
                        "[market_info_v2] fetch failed cid={}... — strategy will fall back to \
                         gamma-api base_fee / fee_rate. Error: {}",
                        &condition_id[..condition_id.len().min(16)], e,
                    );
                    None
                }
            };
            let _ = tx.send(result);
        });
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
/// The `fd` ("fee details") object may be **absent** on markets with
/// zero fees — the server simply omits it. That's not an error; we
/// treat missing `fd` as `(fee_rate=0, exponent=1, taker_only=false)`.
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
    // Peel `{ "data": {...} }` wrappers.
    let root = json.get("data").unwrap_or(json);

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
        v.as_u64().map(|u| u as u32)
            .or_else(|| v.as_f64().map(|f| f.round() as u32))
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

    let fee_rate      = fee_rate_v.as_ref().and_then(as_f64);
    let fee_exponent  = fee_exp_v.as_ref().and_then(as_f64).unwrap_or(1.0);
    let taker_only    = taker_only_v.as_ref().and_then(as_bool).unwrap_or(false);
    let fee_rate_bps  = bps_v.as_ref().and_then(as_u32);

    // Derive missing representations, treating "no fee data" as zero
    // (Polymarket omits `fd` on fee-free markets — this is valid).
    let (fee_rate_final, fee_rate_bps_final) = match (fee_rate, fee_rate_bps) {
        (Some(r), Some(bps)) => (r, bps),
        (Some(r), None)      => (r, (r * 10_000.0).round() as u32),
        (None, Some(bps))    => (bps as f64 / 10_000.0, bps),
        (None, None)         => (0.0, 0), // fee-free market — explicit zero
    };

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
        let json: Value = serde_json::json!({ "c": "0xabc", "ao": true });
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
}
