//! `hexbot active_event` — pretty-print the currently-trading event
//! for a Polymarket series, including per-market outcome labels,
//! CLOB token IDs, condition IDs, and fee info.
//!
//! Primary use: quickly inspect what the bot would be quoting on if
//! started right now. Also handy for piping a real condition_id into
//! `hexbot market`.
//!
//! Usage:
//!   hexbot active_event                  # reads series from config/live_polymaker.toml
//!   hexbot active_event <config-path>    # reads series from the given config
//!   hexbot active_event --slug <slug>    # use this series slug directly, no config
//!
//! Slug sourcing priority:
//!   1. --slug <s> flag
//!   2. `[[exchanges]] name="polymarket" symbols[0]` stripped of "series:" prefix
//!   3. Error out with hint

use anyhow::{anyhow, Result};
use std::path::Path;

pub fn run_active_event() -> Result<()> {
    let args: Vec<String> = crate::exchange::polymarket::cli_account::cli_args().collect();

    let slug = resolve_series_slug(&args)?;
    println!("Resolving active event for series: {}", slug);
    println!();

    let (series_id, event) = super::market::fetch_active_event(&slug)?;

    // ── Header ────────────────────────────────────────────────────
    println!("── Series ───────────────────────────────────────");
    println!("slug       : {}", slug);
    println!("series_id  : {}", series_id);
    println!();

    // ── Event ─────────────────────────────────────────────────────
    println!("── Event ────────────────────────────────────────");
    println!("id         : {}", event.id);
    println!("slug       : {}", event.slug);
    println!("title      : {}", event.title);
    println!("active     : {}", event.active);
    println!("closed     : {}", event.closed);
    println!("end_date   : {}", event.end_date);
    if !event.description.is_empty() {
        let desc = if event.description.len() > 200 {
            format!("{}...", &event.description[..200])
        } else {
            event.description.clone()
        };
        println!("description: {}", desc);
    }
    println!();

    // ── Markets ───────────────────────────────────────────────────
    if event.markets.is_empty() {
        println!("(no markets in this event — unusual; check the gamma-api response directly)");
        return Ok(());
    }

    for (i, m) in event.markets.iter().enumerate() {
        println!("── Market #{} ────────────────────────────────────", i + 1);
        println!("question    : {}", m.question);
        println!("condition_id: {}", m.condition_id);
        println!("market_id   : {}", m.id);
        if !m.slug.is_empty() {
            println!("slug        : {}", m.slug);
        }
        println!("active      : {}  closed: {}", m.active, m.closed);
        println!("tick_size   : {}  min_size: {}", m.tick_size, m.order_min_size);
        println!("volume      : {:.2}  liquidity: {:.2}", m.volume, m.liquidity);
        println!("base_fee    : {} bps", m.base_fee);
        if m.fee_schedule.rate > 0.0 || m.fee_schedule.exponent > 0.0 {
            println!(
                "fee_schedule: rate={:.6} exponent={:.4} taker_only={} rebate_rate={:.4}",
                m.fee_schedule.rate, m.fee_schedule.exponent,
                m.fee_schedule.taker_only, m.fee_schedule.rebate_rate,
            );
        }

        // Outcome rows — pair labels with token ids (and mid prices if present).
        let n = m.outcomes.len().max(m.clob_token_ids.len());
        if n > 0 {
            println!();
            println!("  Outcomes:");
            println!("  {:<12} {:<10} {:<80}", "label", "price", "clob_token_id");
            for idx in 0..n {
                let label = m.outcomes.get(idx).map(String::as_str).unwrap_or("?");
                let price = m.outcome_prices.get(idx).map(String::as_str).unwrap_or("-");
                let tok   = m.clob_token_ids.get(idx).map(String::as_str).unwrap_or("-");
                println!("  {:<12} {:<10} {}", label, price, tok);
            }
        }
        println!();
    }

    // ── Tail: copy-paste helpers ─────────────────────────────────
    println!("── Copy-paste helpers ──────────────────────────");
    for m in &event.markets {
        if !m.condition_id.is_empty() {
            println!("hexbot market {}", m.condition_id);
        }
    }

    Ok(())
}

/// Resolve the series slug from (a) `--slug <s>` CLI flag, (b) the
/// config file's polymarket `symbols[0]` with the `series:` prefix
/// stripped, (c) error out.
///
/// Positional arg if present is treated as the config path (matches
/// `hexbot <subcmd> <config-path>` pattern). Default config path is
/// `config/live_polymaker.toml` (matches `hexbot positions` /
/// `active_orders` / `cancel_order`).
///
/// `pub(crate)` so `hexbot probe` can resolve the same series slug it
/// would quote on without duplicating the `--slug` / config cascade.
pub(crate) fn resolve_series_slug(args: &[String]) -> Result<String> {
    // --slug <s>
    if let Some(pos) = args.iter().position(|a| a == "--slug" || a == "-s") {
        if let Some(s) = args.get(pos + 1) {
            return Ok(s.clone());
        }
        return Err(anyhow!("--slug requires a value"));
    }

    // Otherwise load config. First positional arg or default.
    let config_path = args.iter()
        .find(|a| !a.starts_with('-'))
        .cloned()
        .unwrap_or_else(|| "config/live_polymaker.toml".to_string());

    let cfg = crate::config::Config::load(Path::new(&config_path))
        .map_err(|e| anyhow!(
            "Failed to load config '{}': {}. Pass `--slug <s>` to skip config.",
            config_path, e,
        ))?;

    let poly = cfg.exchanges.iter()
        .find(|e| e.name == "polymarket")
        .ok_or_else(|| anyhow!(
            "No [[exchanges]] name=\"polymarket\" section in '{}'. \
             Add one with symbols=[\"series:<slug>\"], or pass --slug <s>.",
            config_path,
        ))?;

    let raw = poly.symbols.first().cloned().unwrap_or_default();
    if raw.is_empty() {
        return Err(anyhow!(
            "polymarket.symbols is empty in '{}'. Expected symbols=[\"series:<slug>\"].",
            config_path,
        ));
    }
    let slug = raw.strip_prefix("series:").unwrap_or(&raw).to_string();
    if slug.is_empty() {
        return Err(anyhow!(
            "polymarket.symbols[0]='{}' has an empty slug after the 'series:' prefix.",
            raw,
        ));
    }
    Ok(slug)
}
