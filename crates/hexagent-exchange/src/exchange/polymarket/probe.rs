//! `hexbot probe` — end-to-end smoke test of the Polymarket order REST
//! interface, for debugging request / response / round-trip latency.
//!
//! What it does, fully automatically:
//!   1. Resolve the currently-trading event for the configured series
//!      (same slug the bot would quote on; override with `--slug`).
//!   2. Pick the market's **higher-priced** outcome (Up vs Down).
//!   3. Place ONE deep, intentionally **non-marketable** BUY on that
//!      token (rests in the book, won't fill) — `POST /order`.
//!   4. Query the account's open orders — `GET /data/orders`.
//!   5. Query the account's positions — `GET /positions` (data-api).
//!   6. Cancel the order it just placed — `DELETE /order`.
//!
//! Every network call prints its method + URL + body, the raw response,
//! and the measured RTT, so this is the one command to reach for when
//! debugging the order API (auth headers, wire format, latency).
//!
//! Usage:
//!   hexbot probe                        # series from config/live_polymaker.toml
//!   hexbot probe --slug <series-slug>   # use this series, skip config
//!   hexbot probe --price 0.02 --size 50 # override the resting bid
//!   hexbot probe --no-cancel            # leave the order resting (skip step 6)
//!   hexbot probe [<config-path>]        # default config/live_polymaker.toml
//!
//! Safety: the default order is BUY 100 shares @ 0.01 = $1 notional, sent
//! **post-only** (the exchange rejects it outright if it would ever cross
//! the book), so it can only rest, never take. 0.01 is also a deep bid far
//! below the best ask (the higher-priced token's mid is ≥ 0.5). Pass
//! `--price` / `--size` to override. The order is cancelled at the end
//! unless `--no-cancel` is given.

use anyhow::{anyhow, Result};
use std::path::Path;
use std::time::{Duration, Instant};

use super::auth::AuthHeaders;
use super::new_order::{build_v1, build_v2};

/// Default CLOB host — mirrors `new_order` / `active_orders`. Serves both
/// v1 and v2 during the migration window.
const DEFAULT_API_URL: &str = "https://clob.polymarket.com";
const DATA_API_BASE: &str = "https://data-api.polymarket.com";

/// A timed HTTP response: status + raw body + measured round-trip time.
struct Timed {
    status: reqwest::StatusCode,
    text: String,
    rtt: Duration,
}

pub fn run_probe() -> Result<()> {
    // ── Parse args ────────────────────────────────────────────────
    let mut slug_override: Option<String> = None;
    let mut price_override: Option<f64> = None;
    let mut size_override: Option<f64> = None;
    let mut no_cancel = false;
    let mut dry_run = false;
    let mut positional: Vec<String> = Vec::new();
    {
        let mut it = crate::exchange::polymarket::cli_account::cli_args();
        while let Some(a) = it.next() {
            match a.as_str() {
                "--slug" | "-s" => slug_override = it.next(),
                "--price" => {
                    let v = it.next().ok_or_else(|| anyhow!("--price requires a value"))?;
                    price_override = Some(v.parse().map_err(|e| anyhow!("--price parse: {}", e))?);
                }
                "--size" => {
                    let v = it.next().ok_or_else(|| anyhow!("--size requires a value"))?;
                    size_override = Some(v.parse().map_err(|e| anyhow!("--size parse: {}", e))?);
                }
                "--no-cancel" | "--keep" => no_cancel = true,
                "--dry-run" => dry_run = true,
                "-h" | "--help" => { print_usage(); return Ok(()); }
                other => positional.push(other.to_string()),
            }
        }
    }

    // Config path: `--config`/$HEXBOT_CONFIG wins, else first positional,
    // else the live default (matches the rest of the order CLI).
    let config_path = crate::exchange::polymarket::cli_account::config_path()
        .or_else(|| positional.first().cloned())
        .unwrap_or_else(|| "config/live_polymaker.toml".to_string());

    // ── Resolve config-derived order params (CLOB version / URL / sig) ──
    let poly_cfg = crate::config::Config::load(Path::new(&config_path))
        .ok()
        .and_then(|c| c.exchanges.into_iter().find(|e| e.name == "polymarket"));

    let clob_version_s = std::env::var("POLY_CLOB_VERSION").ok()
        .or_else(|| poly_cfg.as_ref().map(|p| p.clob_version.clone()))
        .unwrap_or_default();
    let is_v2 = crate::exchange::polymarket::wallet::is_v2_from_str(&clob_version_s);
    let clob_label = if is_v2 { "v2" } else { "v1" };

    let sig_type_s = std::env::var("POLY_SIGNATURE_TYPE").unwrap_or_default();
    let sig_type = match sig_type_s.to_ascii_lowercase().as_str() {
        "gnosis_safe" | "safe" => super::signer::SignatureType::PolyGnosisSafe,
        "poly_proxy"           => super::signer::SignatureType::PolyProxy,
        "poly_1271" | "deposit_wallet" => super::signer::SignatureType::Poly1271,
        _                      => super::signer::SignatureType::Eoa,
    };
    let builder_code = std::env::var("POLY_BUILDER_CODE").unwrap_or_default();
    let neg_risk = std::env::var("NEGRISK").ok().as_deref() == Some("1");

    let api_url = std::env::var("POLYMARKET_V2_API_URL").ok()
        .or_else(|| poly_cfg.as_ref()
            .map(|p| p.api_url_prefix.clone())
            .filter(|s| !s.is_empty()))
        .unwrap_or_else(|| DEFAULT_API_URL.to_string());
    let api_url = api_url.trim_end_matches('/').to_string();

    // ── Credentials + auth (signer EOA is the auth address) ───────
    let (auth, signer_address) = load_user_auth()?;
    let private_key = std::env::var("POLY_PRIVATE_KEY")
        .map_err(|_| anyhow!("POLY_PRIVATE_KEY not set"))?;
    let (funds_wallet, funds_label) = resolve_funds_wallet(&signer_address);

    println!("═══════════════════════════════════════════════════════");
    println!("  hexbot probe — order interface smoke test");
    println!("═══════════════════════════════════════════════════════");
    println!("Config:        {}", config_path);
    println!("CLOB:          {}", clob_label);
    println!("API URL:       {}", api_url);
    println!("Sig type:      {:?}", sig_type);
    println!("Neg risk:      {}", neg_risk);
    println!("Signer (EOA):  {}", signer_address);
    println!("Funds ({}): {}", funds_label, funds_wallet);
    println!();

    // ── Step 1: resolve active event + pick higher-priced token ───
    println!("── Step 1 · Resolve active event ──────────────────────");
    // Feed `resolve_series_slug` the same arg shape it expects: a slug
    // override (if any) or the positional config path.
    let mut slug_args: Vec<String> = Vec::new();
    if let Some(s) = &slug_override {
        slug_args.push("--slug".to_string());
        slug_args.push(s.clone());
    } else {
        slug_args.push(config_path.clone());
    }
    let slug = super::active_event::resolve_series_slug(&slug_args)?;
    println!("series slug:   {}", slug);

    let (series_id, event) = super::market::fetch_active_event(&slug)?;
    println!("series_id:     {}", series_id);
    println!("event:         {}  (id={})", event.title, event.id);
    println!("end_date:      {}", event.end_date);

    // Pick the first tradeable market (active, not closed, ≥2 tokens).
    let market = event.markets.iter()
        .find(|m| m.active && !m.closed
            && m.clob_token_ids.len() >= 2
            && !m.clob_token_ids.iter().any(|t| t.is_empty()))
        .or_else(|| event.markets.iter().find(|m| m.clob_token_ids.len() >= 2))
        .ok_or_else(|| anyhow!(
            "no tradeable market with ≥2 CLOB token ids in event '{}' — \
             run `hexbot active_event` to inspect.", event.title))?;

    println!("market:        {}", market.question);
    println!("condition_id:  {}", market.condition_id);
    println!("tick_size:     {}   min_size: {}", market.tick_size, market.order_min_size);

    // Higher-priced outcome among the (typically 2) tokens.
    println!();
    println!("  {:<10} {:<10} {}", "outcome", "price", "clob_token_id");
    let n = market.clob_token_ids.len();
    let mut best_idx = 0usize;
    let mut best_px = f64::MIN;
    for idx in 0..n {
        let label = market.outcomes.get(idx).map(String::as_str).unwrap_or("?");
        let px: f64 = market.outcome_prices.get(idx)
            .and_then(|s| s.parse().ok()).unwrap_or(f64::NAN);
        if px.is_finite() && px > best_px { best_idx = idx; best_px = px; }
        println!("  {:<10} {:<10} {}", label,
            market.outcome_prices.get(idx).map(String::as_str).unwrap_or("-"),
            market.clob_token_ids[idx]);
    }
    if !best_px.is_finite() {
        // No prices from gamma (fresh market). Fall back to outcome 0 and
        // a conservative deep bid; warn the operator.
        best_idx = 0;
        best_px = 0.0;
        println!("  ⚠  no outcome prices from gamma-api — defaulting to outcome 0");
    }
    let token_id = market.clob_token_ids[best_idx].clone();
    let token_label = market.outcomes.get(best_idx).map(String::as_str).unwrap_or("?").to_string();
    println!();
    println!("→ chosen (higher price): {} @ {:.4}  token={}", token_label, best_px, token_id);

    // ── Resolve the resting bid price + size ──────────────────────
    // Default: BUY 100 shares @ 0.01 = $1 notional. 0.01 is a deep bid,
    // far below the best ask (the higher-priced token's mid is ≥ 0.5),
    // so the order rests instead of filling — exactly what a probe wants.
    let tick = if market.tick_size > 0.0 { market.tick_size } else { 0.01 };
    let price = round_to_tick(price_override.unwrap_or(0.01), tick);
    let size = size_override.unwrap_or(100.0);
    println!("→ resting BUY:  {} {} @ {:.4}  (notional ≈ ${:.2}, post-only, deep / non-marketable)",
        size, token_label, price, size * price);
    println!();

    // ── Step 2: place order (POST /order) ─────────────────────────
    println!("── Step 2 · Place order (POST /order) ─────────────────");
    let fee = 0u32;
    let (local_hash, mut body) = if is_v2 {
        build_v2(&token_id, price, size, crate::types::Side::Buy,
            &private_key, neg_risk, sig_type, &builder_code, fee, &auth.api_key)?
    } else {
        build_v1(&token_id, price, size, crate::types::Side::Buy,
            &private_key, neg_risk, sig_type, fee, &auth.api_key)?
    };
    // Force post-only: the exchange REJECTS a post-only order if it would
    // cross the book (take liquidity) instead of executing — a hard guarantee
    // the probe can never fill. `postOnly` is a wire-level wrapper field, NOT
    // part of the signed `order` object, so flipping it here doesn't change
    // `local_hash` or require re-signing.
    body["postOnly"] = serde_json::Value::Bool(true);
    let body_str = serde_json::to_string(&body)?;
    let headers = auth.sign_request("POST", "/order", &body_str);
    let place_url = format!("{}/order", api_url);

    print_request("POST", &place_url, Some(&headers), Some(&body_str));
    println!("  local_hash: {}", local_hash);
    if dry_run {
        println!();
        println!("── DRY RUN ────────────────────────────────────────────");
        println!("  --dry-run set: signed request built but NOT sent.");
        println!("  Drop --dry-run to place → query → cancel for real.");
        return Ok(());
    }
    let place = timed_request(reqwest::Method::POST, place_url, Some(headers), Some(body_str))?;
    print_response(&place);

    // Extract the server orderID for the cancel step + hash cross-check.
    let place_json: serde_json::Value = serde_json::from_str(&place.text).unwrap_or(serde_json::Value::Null);
    let server_oid = place_json.get("orderID").and_then(|v| v.as_str()).map(String::from);
    match &server_oid {
        Some(oid) => {
            let local_lc = local_hash.trim_start_matches("0x").to_ascii_lowercase();
            let srv_lc = oid.trim_start_matches("0x").to_ascii_lowercase();
            if local_lc == srv_lc {
                println!("  ✅ orderID MATCH (local hash == server orderID — {} wire format OK)", clob_label);
            } else {
                println!("  ❌ orderID MISMATCH  local={}  server={}", local_hash, oid);
            }
        }
        None => println!("  ⚠  no orderID in response — placement likely failed (see `error`/`errorMsg` above)"),
    }
    println!();

    // ── Step 3: open orders (GET /data/orders) ────────────────────
    println!("── Step 3 · Open orders (GET /data/orders) ────────────");
    let orders_path = "/data/orders";
    let oh = auth.sign_request("GET", orders_path, "");
    let orders_url = format!("{}{}", api_url, orders_path);
    print_request("GET", &orders_url, Some(&oh), None);
    let orders = timed_request(reqwest::Method::GET, orders_url, Some(oh), None)?;
    print_response_rtt(&orders);
    summarize_open_orders(&orders.text, server_oid.as_deref());
    println!();

    // ── Step 4: positions (GET /positions, data-api) ──────────────
    println!("── Step 4 · Positions (GET /positions) ────────────────");
    let pos_url = format!("{}/positions?user={}&sizeThreshold=0&limit=500", DATA_API_BASE, funds_wallet);
    print_request("GET", &pos_url, None, None);
    let positions = timed_request(reqwest::Method::GET, pos_url, None, None)?;
    print_response_rtt(&positions);
    summarize_positions(&positions.text, &market.condition_id);
    println!();

    // ── Step 5: cancel the order (DELETE /order) ──────────────────
    let mut cancel_rtt: Option<Duration> = None;
    if no_cancel {
        println!("── Step 5 · Cancel — SKIPPED (--no-cancel) ────────────");
        if let Some(oid) = &server_oid {
            println!("  Order left resting. Cancel later with:");
            println!("    hexbot cancel_order {}", oid);
        }
        println!();
    } else if let Some(oid) = &server_oid {
        println!("── Step 5 · Cancel order (DELETE /order) ──────────────");
        let cbody = serde_json::json!({ "orderID": oid });
        let cbody_str = serde_json::to_string(&cbody)?;
        let ch = auth.sign_request("DELETE", "/order", &cbody_str);
        let cancel_url = format!("{}/order", api_url);
        print_request("DELETE", &cancel_url, Some(&ch), Some(&cbody_str));
        let cancel = timed_request(reqwest::Method::DELETE, cancel_url, Some(ch), Some(cbody_str))?;
        print_response(&cancel);
        summarize_cancel(&cancel.text);
        cancel_rtt = Some(cancel.rtt);
        println!();
    } else {
        println!("── Step 5 · Cancel — SKIPPED (no orderID to cancel) ───");
        println!();
    }

    // ── RTT summary ───────────────────────────────────────────────
    println!("── RTT summary ────────────────────────────────────────");
    println!("  POST   /order        {:>8.1} ms  [{}]", ms(place.rtt), place.status);
    println!("  GET    /data/orders  {:>8.1} ms  [{}]", ms(orders.rtt), orders.status);
    println!("  GET    /positions    {:>8.1} ms  [{}]", ms(positions.rtt), positions.status);
    match cancel_rtt {
        Some(d) => println!("  DELETE /order        {:>8.1} ms", ms(d)),
        None    => println!("  DELETE /order              -      (skipped)"),
    }
    Ok(())
}

// ════════════════════════════════════════════════════════════════
// HTTP — timed, status-tolerant (returns body even on 4xx/5xx so the
// operator can SEE the error response).
// ════════════════════════════════════════════════════════════════

fn timed_request(
    method: reqwest::Method,
    url: String,
    headers: Option<AuthHeaders>,
    body: Option<String>,
) -> Result<Timed> {
    let client = crate::async_rt::http_client_query();
    crate::async_rt::block_on_runtime(async move {
        let mut req = client.request(method, &url);
        if let Some(h) = headers {
            for (k, v) in h.as_pairs() {
                req = req.header(k, v);
            }
        }
        if let Some(b) = body {
            req = req.header("Content-Type", "application/json").body(b);
        }
        let t0 = Instant::now();
        let resp = req.send().await.map_err(|e| anyhow!("HTTP send {}: {}", url, e))?;
        let status = resp.status();
        let text = resp.text().await.map_err(|e| anyhow!("read body {}: {}", url, e))?;
        let rtt = t0.elapsed();
        Ok::<Timed, anyhow::Error>(Timed { status, text, rtt })
    })
}

// ════════════════════════════════════════════════════════════════
// Printing helpers
// ════════════════════════════════════════════════════════════════

fn ms(d: Duration) -> f64 { d.as_secs_f64() * 1000.0 }

fn print_request(method: &str, url: &str, headers: Option<&AuthHeaders>, body: Option<&str>) {
    println!("  Request:    {} {}", method, url);
    if let Some(h) = headers {
        println!("  Headers:    POLY_ADDRESS={}  POLY_API_KEY={}", h.address, h.api_key);
        println!("              POLY_TIMESTAMP={}  POLY_SIGNATURE={}  POLY_PASSPHRASE={}",
            h.timestamp, redact(&h.signature, 12), redact(&h.passphrase, 4));
    }
    if let Some(b) = body {
        println!("  Body:       {}", b);
    }
}

fn print_response(res: &Timed) {
    println!("  Response:   status={}  rtt={:.1} ms", res.status, ms(res.rtt));
    println!("  Body:       {}", res.text);
}

/// Like `print_response` but only the status/RTT line — used when a
/// dedicated summarizer renders the body below.
fn print_response_rtt(res: &Timed) {
    println!("  Response:   status={}  rtt={:.1} ms", res.status, ms(res.rtt));
}

/// Show the first `keep` chars of a sensitive value, then an ellipsis.
fn redact(s: &str, keep: usize) -> String {
    if s.len() <= keep { return s.to_string(); }
    format!("{}…", &s[..keep])
}

/// Round to the market tick and clamp into the valid (tick, 1-tick) range.
fn round_to_tick(price: f64, tick: f64) -> f64 {
    let tick = if tick > 0.0 { tick } else { 0.01 };
    let inv = 1.0 / tick;
    let r = (price * inv).round() / inv;
    r.clamp(tick, 1.0 - tick)
}

/// Build the CLOB L2 auth from `POLY_*` env (creds are applied by
/// `cli_account::resolve_and_apply` before this subcommand runs). The auth
/// address is the signer EOA — Polymarket always keys L2 auth on the EOA,
/// even for Safe / deposit-wallet accounts. Returns `(auth, signer_address)`.
fn load_user_auth() -> Result<(super::auth::PolyAuth, String)> {
    let private_key = std::env::var("POLY_PRIVATE_KEY").unwrap_or_default();
    let api_key = std::env::var("POLY_API_KEY").unwrap_or_default();
    let api_secret = std::env::var("POLY_API_SECRET").unwrap_or_default();
    let passphrase = std::env::var("POLY_PASSPHRASE").unwrap_or_default();
    if api_key.is_empty() || api_secret.is_empty() {
        return Err(anyhow!(
            "missing Polymarket API credentials (POLY_API_KEY / POLY_API_SECRET). \
             Select a wallet with `--instance <id> --config <p>` or `--account <id>`."));
    }
    let signing_key = super::deploy_wallet::parse_private_key(&private_key)?;
    let signer_address = super::deploy_wallet::to_checksum_address(
        &super::signer::derive_eth_address_from_key(&signing_key));
    let auth = super::auth::PolyAuth::new(&api_key, &api_secret, &passphrase, &signer_address)?;
    Ok((auth, signer_address))
}

/// Resolve the address that actually holds funds + positions: the deposit
/// wallet for POLY_1271 accounts, else the Gnosis Safe derived from the
/// signer. Mirrors `wallet::run_positions`.
fn resolve_funds_wallet(signer_address: &str) -> (String, &'static str) {
    let safe = super::deploy_wallet::to_checksum_address(
        &super::deploy_wallet::derive_safe_address(signer_address));
    let sig_type = std::env::var("POLY_SIGNATURE_TYPE").unwrap_or_default().to_ascii_lowercase();
    if sig_type == "poly_1271" || sig_type == "deposit_wallet" {
        match super::deposit_wallet::resolve_deposit_wallet(signer_address) {
            Ok(dw) => (dw, "Deposit"),
            Err(_) => (safe, "Safe"),
        }
    } else {
        (safe, "Safe")
    }
}

/// Parse `/data/orders` and list non-terminal orders, flagging the one we
/// just placed (`probe_oid`).
fn summarize_open_orders(text: &str, probe_oid: Option<&str>) {
    let json: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => { println!("  (non-JSON response — see raw body via --no-cancel re-run)"); return; }
    };
    let Some(data) = json.get("data").and_then(|v| v.as_array()) else {
        println!("  (no `data` array — top-level keys: {:?}",
            json.as_object().map(|o| o.keys().cloned().collect::<Vec<_>>()));
        return;
    };
    let mut shown = 0usize;
    for o in data {
        let status = o.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if matches!(status.to_ascii_uppercase().as_str(),
            "MATCHED" | "CANCELED" | "CANCELLED" | "FILLED" | "REJECTED") { continue; }
        let id = o.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let side = o.get("side").and_then(|v| v.as_str()).unwrap_or("");
        let price = o.get("price").and_then(json_num_str).unwrap_or_default();
        let size = o.get("original_size").and_then(json_num_str).unwrap_or_default();
        let outcome = o.get("outcome").and_then(|v| v.as_str()).unwrap_or("");
        let mark = if probe_oid == Some(id) { "  ← this probe" } else { "" };
        println!("  • {:<4} {:<8} {:>7} @ {:<7}  {}{}", side, outcome, size, price, id, mark);
        shown += 1;
    }
    if shown == 0 {
        println!("  (no open orders returned)");
    } else if probe_oid.is_some() && !data.iter().any(|o|
        o.get("id").and_then(|v| v.as_str()) == probe_oid) {
        println!("  ⚠  the probe order is not in the open-orders list (propagation lag or rejected)");
    }
}

/// Parse data-api `/positions` and list those for `condition_id` (plus a
/// total count of all open positions).
fn summarize_positions(text: &str, condition_id: &str) {
    let arr: Vec<serde_json::Value> = serde_json::from_str(text).unwrap_or_default();
    if arr.is_empty() {
        println!("  (no open positions)");
        return;
    }
    println!("  open positions: {}", arr.len());
    let mut matched = 0usize;
    for p in &arr {
        let cid = p.get("conditionId").and_then(|v| v.as_str()).unwrap_or("");
        if !cid.eq_ignore_ascii_case(condition_id) { continue; }
        let outcome = p.get("outcome").and_then(|v| v.as_str()).unwrap_or("");
        let size = p.get("size").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let avg = p.get("avgPrice").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let cur = p.get("curPrice").and_then(|v| v.as_f64()).unwrap_or(0.0);
        println!("  • {:<6} size={:.4}  avg={:.4}  cur={:.4}   (this market)", outcome, size, avg, cur);
        matched += 1;
    }
    if matched == 0 {
        println!("  (none on the probed market {})", &condition_id[..condition_id.len().min(16)]);
    }
}

/// Render a `DELETE /order` response: `{canceled:[...], not_canceled:{...}}`.
fn summarize_cancel(text: &str) {
    let json: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return,
    };
    let canceled = json.get("canceled").and_then(|v| v.as_array());
    let not_canceled = json.get("not_canceled").and_then(|v| v.as_object());
    let cn = canceled.map(|a| a.len()).unwrap_or(0);
    let nn = not_canceled.map(|o| o.len()).unwrap_or(0);
    println!("  Canceled: {}", cn);
    if let Some(arr) = canceled {
        for v in arr { if let Some(s) = v.as_str() { println!("    ✅ {}", s); } }
    }
    if nn > 0 {
        println!("  Not canceled: {}", nn);
        if let Some(obj) = not_canceled {
            for (id, reason) in obj {
                println!("    ❌ {}  (reason: {})", id, reason.as_str().unwrap_or(""));
            }
        }
    }
    if cn == 0 && nn == 0 {
        println!("  ⚠  unrecognised cancel response shape — see raw body above");
    }
}

/// A JSON field that may be a number or a stringified number → String.
fn json_num_str(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn print_usage() {
    eprintln!(
        "Usage: hexbot probe [--slug <series>] [--price <p>] [--size <s>] \
         [--no-cancel] [--dry-run] [<config-path>]\n\n\
         End-to-end debug of the order REST interface: resolves the active\n\
         event, picks the higher-priced Up/Down token, places a deep\n\
         non-marketable BUY, queries open orders + positions, then cancels\n\
         it. Prints request / response / RTT for every call.\n\n\
         Flags:\n\
         \t--slug <series>  series slug to probe (else from config)\n\
         \t--price <p>      resting bid price (default 0.01, tick-rounded)\n\
         \t--size <s>       order size in shares (default 100 → ~$1 at 0.01)\n\
         \t--no-cancel      leave the order resting instead of cancelling\n\
         \t--dry-run        build + sign the order but DON'T send it (no live order)\n\n\
         Wallet selection: --instance <id> --config <p>  |  --account <id>"
    );
}
