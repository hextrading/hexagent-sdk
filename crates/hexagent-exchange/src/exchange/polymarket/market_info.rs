//! `hexbot market` — probe the v2 `getClobMarketInfo`
//! endpoint for a condition_id. Prints the raw response and the
//! parsed `MarketInfoV2` side-by-side.
//!
//! Primary use pre-cutover:
//!   * Confirm the real endpoint URL (default is `/markets/{conditionId}`
//!     which is a reasonable guess; override via env).
//!   * Confirm our parser picks up the actual field names used by the
//!     server (we accept several variants — see `market_info_v2.rs`).
//!
//! Usage:
//!   POLYMARKET_V2_API_URL=https://clob-v2.polymarket.com \
//!   hexbot market <condition_id> [path_template]
//!
//! `path_template` is optional; defaults to `/markets/{conditionId}`.
//! Use it if the v2 endpoint turns out to differ, e.g.:
//!   hexbot market 0xabc... "/clob-market-info/{conditionId}"

use anyhow::{anyhow, Result};
use serde_json::Value;

use super::market_info_v2::{fetch_clob_market_info, parse_market_info};

pub fn run_market() -> Result<()> {
    let args: Vec<String> = crate::exchange::polymarket::cli_account::cli_args().collect();
    if args.is_empty() {
        eprintln!(
            "Usage: hexbot market <condition_id> [path_template]\n\
             \n\
             <condition_id>: 32-byte hex, `0x` + 64 hex chars. The on-chain\n\
                 condition hash from `BinaryOption.condition_id` — NOT the\n\
                 20-byte Ethereum address and NOT the outcome token ID.\n\
             [path_template]: URL path with `{{conditionId}}` placeholder.\n\
                 Default: '/clob-markets/{{conditionId}}' (v2 SDK endpoint).\n\
             \n\
             Override API URL via POLYMARKET_V2_API_URL (default:\n\
             https://clob-v2.polymarket.com).\n\
             \n\
             Examples:\n\
             \thexbot market 0xfd029a...8ce1\n\
             \thexbot market 0xfd029a...8ce1 '/markets/{{conditionId}}'"
        );
        return Err(anyhow!("missing condition_id"));
    }
    let condition_id = args[0].trim().to_string();

    // Input validation — catch common mistakes before round-tripping
    // a 404 through the server.
    let hex_clean = condition_id.strip_prefix("0x").unwrap_or(&condition_id);
    if !hex_clean.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(anyhow!(
            "condition_id '{}' is not valid hex — expected `0x` + 64 hex chars",
            condition_id,
        ));
    }
    match hex_clean.len() {
        64 => {} // correct
        40 => return Err(anyhow!(
            "condition_id '{}' is 20 bytes (Ethereum-address length). \
             The v2 market-info endpoint takes a 32-byte condition_id \
             (`0x` + 64 hex chars), e.g. `0xfd029a...8ce1`. Find it in \
             `BinaryOption.condition_id` or gamma-api market responses.",
            condition_id,
        )),
        other => return Err(anyhow!(
            "condition_id '{}' is {} bytes of hex, expected 32 (64 hex chars)",
            condition_id, other / 2,
        )),
    }

    let path_template = args.get(1).cloned().unwrap_or_default();

    let api_url = std::env::var("POLYMARKET_V2_API_URL")
        .unwrap_or_else(|_| "https://clob-v2.polymarket.com".to_string());

    println!("── Request ──────────────────────────────────────");
    println!("API URL : {}", api_url);
    println!("Path    : {}", if path_template.is_empty() { "/clob-markets/{conditionId}" } else { &path_template });
    println!("cid     : {}", condition_id);
    println!();

    // Step 1: fetch + parse (normal path).
    match fetch_clob_market_info(&api_url, &condition_id, &path_template) {
        Ok(info) => {
            println!("── Parsed ───────────────────────────────────────");
            println!("fee_rate      : {:.6}", info.fee_rate);
            println!("fee_exponent  : {:.4}", info.fee_exponent);
            println!("fee_rate_bps  : {}", info.fee_rate_bps);
            println!("taker_only    : {}", info.taker_only);
            println!();
            println!("── Raw JSON ─────────────────────────────────────");
            println!("{}", serde_json::to_string_pretty(&info.raw).unwrap_or_default());
            println!();
            println!("✅ fetch + parse OK.");
            println!("   Confirm: fee_rate/fee_rate_bps match your expectation for this market.");
            println!("   If yes, v2 market-info wiring is correct; enable in config via:");
            println!("      [[exchanges]] clob_version = \"v2\"");
        }
        Err(e) => {
            println!("── Error ────────────────────────────────────────");
            println!("{}", e);
            println!();
            // Best-effort: fetch raw text and try to display, so the
            // operator can inspect unexpected shapes.
            let path = if path_template.is_empty() {
                format!("/clob-markets/{}", condition_id)
            } else {
                path_template
                    .replace("{conditionId}", &condition_id)
                    .replace("{condition_id}", &condition_id)
            };
            let url = format!("{}{}", api_url.trim_end_matches('/'), path);
            println!("Attempting raw GET for diagnostics: {}", url);
            match crate::async_rt::blocking_get_text(&url) {
                Ok(raw) => {
                    println!("Raw response body:");
                    println!("{}", &raw[..raw.len().min(2000)]);
                    if let Ok(val) = serde_json::from_str::<Value>(&raw) {
                        println!();
                        println!("Parsed as JSON; trying parser again with verbose diagnostics:");
                        match parse_market_info(&val) {
                            Ok(info) => println!("(Actually succeeded: {:?})", info),
                            Err(p) => println!("parse_market_info says: {}", p),
                        }
                    }
                }
                Err(re) => println!("Raw GET also failed: {}", re),
            }
            return Err(anyhow!("v2 market-info probe failed"));
        }
    }
    Ok(())
}
