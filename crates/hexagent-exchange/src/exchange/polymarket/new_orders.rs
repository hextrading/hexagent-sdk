//! `hexbot new_orders` — batch place multiple orders in a single
//! `POST /orders` call. CSV input keeps the interface usable for
//! tens of orders at once (the single-order `new_order` CLI gets
//! awkward past ~3 manual invocations).
//!
//! CSV format (one order per line; `#` line comments allowed):
//!
//!     token_id,side,price,size
//!     token_id,side,price,size,fee_bps      # v1-only; 5th column ignored in v2
//!
//! Example file `orders.csv`:
//!
//!     # Ask ladder
//!     1728766442...41,SELL,0.99,5
//!     1728766442...41,SELL,0.98,5
//!     # Bid ladder
//!     1728766442...41,BUY,0.01,5
//!     1728766442...41,BUY,0.02,5
//!
//! Usage:
//!   hexbot new_orders --file orders.csv
//!   hexbot new_orders --file orders.csv --config config/backtest_polymaker.toml
//!   hexbot new_orders --file orders.csv --host https://clob.polymarket.com
//!   hexbot new_orders --file orders.csv --dry-run
//!
//! Same config / auth conventions as `hexbot new_order`:
//! reads `clob_version` / `signature_type` / `builder_code` /
//! `api_url_prefix` from `config/live_polymaker.toml`. Env overrides
//! identical (`POLY_SIGNATURE_TYPE`, `POLY_CLOB_VERSION`,
//! `POLYMARKET_V2_API_URL`, `NEGRISK`).

use anyhow::{anyhow, Result};
use log::info;
use std::path::Path;

use super::auth::PolyAuth;
use super::signer::SignatureType;

use super::new_order::{auth_address, build_v1, build_v2};

const DEFAULT_API_URL: &str = "https://clob.polymarket.com";

pub fn run_new_orders() -> Result<()> {
    // ── Parse args ──────────────────────────────────────────────
    // `--config` is a top-level flag resolved centrally by
    // `cli_account` (stripped before this loop sees argv), so it's read
    // via the getter below rather than parsed here — that keeps it
    // position-independent and consistent with `--instance`.
    let mut file_path: Option<String> = None;
    let mut host_override: Option<String> = None;
    let mut dry_run = false;
    {
        let mut iter = crate::exchange::polymarket::cli_account::cli_args();
        while let Some(a) = iter.next() {
            match a.as_str() {
                "--file"   => file_path     = iter.next(),
                "--host"   => host_override = iter.next(),
                "--dry-run" | "-n" => dry_run = true,
                "-h" | "--help" => { print_usage(); return Ok(()); }
                other => return Err(anyhow!("unknown arg `{}`", other)),
            }
        }
    }
    let file_path = file_path.ok_or_else(|| {
        print_usage();
        anyhow!("--file <path> required")
    })?;
    let config_path = crate::exchange::polymarket::cli_account::config_path()
        .unwrap_or_else(|| "config/live_polymaker.toml".to_string());

    // ── Read + parse CSV ────────────────────────────────────────
    let content = std::fs::read_to_string(&file_path)
        .map_err(|e| anyhow!("read {}: {}", file_path, e))?;
    let mut orders: Vec<ParsedOrder> = Vec::new();
    for (line_no, raw) in content.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') { continue; }
        let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
        if parts.len() < 4 {
            return Err(anyhow!(
                "{}:{}: expected `token,side,price,size[,fee_bps]`, got `{}`",
                file_path, line_no + 1, line,
            ));
        }
        let token = parts[0].to_string();
        let side = match parts[1].to_ascii_uppercase().as_str() {
            "BUY"  => crate::types::Side::Buy,
            "SELL" => crate::types::Side::Sell,
            other => return Err(anyhow!("{}:{}: side must be BUY|SELL, got `{}`",
                file_path, line_no + 1, other)),
        };
        let price: f64 = parts[2].parse()
            .map_err(|e| anyhow!("{}:{}: price parse: {}", file_path, line_no + 1, e))?;
        let size: f64 = parts[3].parse()
            .map_err(|e| anyhow!("{}:{}: size parse: {}", file_path, line_no + 1, e))?;
        // Exclusive (0, 1): matches what the signed-order build path
        // enforces (trade.rs). The valid lower bound is the market's
        // tick_size (0.01 or 0.001), which this manual CSV tool doesn't
        // load — Polymarket rejects off-tick / out-of-range prices
        // server-side, so don't hardcode the coarse 0.01 floor here.
        if !(price > 0.0 && price < 1.0) {
            return Err(anyhow!("{}:{}: price {} out of (0, 1)", file_path, line_no + 1, price));
        }
        if size <= 0.0 {
            return Err(anyhow!("{}:{}: size {} must be > 0", file_path, line_no + 1, size));
        }
        let fee_bps: u32 = parts.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);
        orders.push(ParsedOrder { token, side, price, size, fee_bps, line_no: line_no + 1 });
    }
    if orders.is_empty() {
        return Err(anyhow!("no orders parsed from {}", file_path));
    }

    // ── Load config ────────────────────────────────────────────
    let poly_cfg = crate::config::Config::load(Path::new(&config_path))
        .ok()
        .and_then(|c| c.exchanges.into_iter().find(|e| e.name == "polymarket"));

    let clob_version_s = std::env::var("POLY_CLOB_VERSION").ok()
        .or_else(|| poly_cfg.as_ref().map(|p| p.clob_version.clone()))
        .unwrap_or_default();
    // v2 is the default — only explicit `v1`/`1` selects the legacy chain.
    let is_v2 = crate::exchange::polymarket::wallet::is_v2_from_str(&clob_version_s);
    let clob_label = if is_v2 { "v2" } else { "v1" };

    // Phase 6: signature_type / builder_code sourced from env vars
    // (populated by `cli_account::resolve_and_apply` from secrets.toml).
    // Legacy `[[exchanges]] polymarket` credential reads are removed.
    let sig_type_s = std::env::var("POLY_SIGNATURE_TYPE").unwrap_or_default();
    let sig_type = match sig_type_s.to_ascii_lowercase().as_str() {
        "gnosis_safe" | "safe" => SignatureType::PolyGnosisSafe,
        "poly_proxy"           => SignatureType::PolyProxy,
        "poly_1271" | "deposit_wallet" => SignatureType::Poly1271,
        _                      => SignatureType::Eoa,
    };

    let builder_code = std::env::var("POLY_BUILDER_CODE").unwrap_or_default();

    let api_url = host_override
        .or_else(|| std::env::var("POLYMARKET_V2_API_URL").ok())
        .or_else(|| poly_cfg.as_ref()
            .map(|p| p.api_url_prefix.clone())
            .filter(|s| !s.is_empty()))
        .unwrap_or_else(|| DEFAULT_API_URL.to_string());

    // ── Credentials ────────────────────────────────────────────
    let api_key = std::env::var("POLY_API_KEY")
        .map_err(|_| anyhow!("POLY_API_KEY not set"))?;
    let api_secret = std::env::var("POLY_API_SECRET")
        .map_err(|_| anyhow!("POLY_API_SECRET not set"))?;
    let passphrase = std::env::var("POLY_PASSPHRASE")
        .map_err(|_| anyhow!("POLY_PASSPHRASE not set"))?;
    let private_key = std::env::var("POLY_PRIVATE_KEY")
        .map_err(|_| anyhow!("POLY_PRIVATE_KEY not set"))?;
    let neg_risk = std::env::var("NEGRISK").ok().as_deref() == Some("1");

    // ── Plan summary ───────────────────────────────────────────
    println!("── Plan ──────────────────────────────────────────");
    println!("Config:    {}", config_path);
    println!("CLOB:      {}", clob_label);
    println!("API URL:   {}", api_url);
    println!("Sig type:  {:?}", sig_type);
    println!("NegRisk:   {}", neg_risk);
    println!("Orders:    {} from {}", orders.len(), file_path);
    println!();
    println!("  {:<5} {:>4}  {:>8}  {:>8}  {:>10}  {}",
        "Line", "Side", "Outcome", "Price", "Size", "TokenID (16)");
    for o in &orders {
        let tok_prefix: String = o.token.chars().take(16).collect();
        println!("  {:<5} {:>4}  {:>8}  {:>8.4}  {:>10.2}  {}…",
            o.line_no, format!("{:?}", o.side).to_uppercase(),
            "?", o.price, o.size, tok_prefix);
    }
    println!();

    if dry_run {
        println!("(dry-run: not signing, not broadcasting)");
        return Ok(());
    }

    // ── Sign every order → collect envelope array ───────────────
    let mut envelopes: Vec<serde_json::Value> = Vec::with_capacity(orders.len());
    let mut local_hashes: Vec<String> = Vec::with_capacity(orders.len());
    for o in &orders {
        let (hash, env) = if is_v2 {
            build_v2(
                &o.token, o.price, o.size, o.side,
                &private_key, neg_risk, sig_type, &builder_code,
                o.fee_bps,
                &api_key,
            )?
        } else {
            build_v1(
                &o.token, o.price, o.size, o.side,
                &private_key, neg_risk, sig_type, o.fee_bps,
                &api_key,
            )?
        };
        local_hashes.push(hash);
        envelopes.push(env);
    }

    // ── POST /orders ──────────────────────────────────────────
    let body = serde_json::Value::Array(envelopes);
    let body_str = serde_json::to_string(&body)?;
    let auth = PolyAuth::new(&api_key, &api_secret, &passphrase, &auth_address(&private_key)?)?;
    let headers = auth.sign_request("POST", "/orders", &body_str);
    let url = format!("{}/orders", api_url.trim_end_matches('/'));

    info!("[new_orders] POST {} with {} orders", url, orders.len());

    let client = crate::async_rt::http_client_query();
    let body_for_move = body_str.clone();
    let (status, text) = crate::async_rt::block_on_runtime(async move {
        let mut req = client.post(&url).body(body_for_move)
            .header("Content-Type", "application/json");
        for (k, v) in headers.as_pairs() { req = req.header(k, v); }
        let r = req.send().await.map_err(|e| anyhow!("HTTP send: {}", e))?;
        let status = r.status();
        let text = r.text().await.map_err(|e| anyhow!("read body: {}", e))?;
        Ok::<_, anyhow::Error>((status, text))
    })?;

    println!("── Response (status {}) ──────────────────────────", status);
    // Per-order pairing: the server returns an array parallel to the
    // request; index i of the response corresponds to envelopes[i]
    // corresponds to orders[i].
    let json: serde_json::Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(_) => {
            println!("(non-JSON body)");
            println!("{}", text);
            return Ok(());
        }
    };
    match json.as_array() {
        Some(arr) => render_batch_place_result(arr, &orders, &local_hashes),
        None => {
            println!("(response is not an array — raw:)");
            println!("{}", serde_json::to_string_pretty(&json).unwrap_or(text));
        }
    }

    Ok(())
}

struct ParsedOrder {
    token: String,
    side:  crate::types::Side,
    price: f64,
    size:  f64,
    fee_bps: u32,
    line_no: usize,
}

fn render_batch_place_result(
    arr: &[serde_json::Value],
    orders: &[ParsedOrder],
    local_hashes: &[String],
) {
    let mut ok = 0usize;
    let mut err = 0usize;
    for (i, r) in arr.iter().enumerate() {
        let success   = r.get("success").and_then(|v| v.as_bool()).unwrap_or(false);
        let order_id  = r.get("orderID").and_then(|v| v.as_str()).unwrap_or("");
        let error_msg = r.get("errorMsg").and_then(|v| v.as_str()).unwrap_or("");
        let row_label = orders.get(i).map(|o|
            format!("line {}: {:?} {}×{:.4}",
                o.line_no, o.side, o.size, o.price,
            )).unwrap_or_else(|| format!("#{}", i));
        if success && !order_id.is_empty() {
            let local  = local_hashes.get(i).map(|s| s.as_str()).unwrap_or("");
            let a = order_id.trim_start_matches("0x").to_ascii_lowercase();
            let b = local.trim_start_matches("0x").to_ascii_lowercase();
            let hash_match = if a == b { "✅" } else { "❌" };
            println!(" {:>3}. {}  → orderID {} {}", i + 1, row_label, order_id, hash_match);
            ok += 1;
        } else {
            println!(" {:>3}. {}  → REJECTED: {}",
                i + 1, row_label, if error_msg.is_empty() { "(unknown)" } else { error_msg });
            err += 1;
        }
    }
    println!();
    println!("Summary: {} accepted, {} rejected / {} submitted", ok, err, arr.len());
}

fn print_usage() {
    eprintln!(
        "Usage: hexbot new_orders --file <orders.csv> [--config <cfg>] [--host <url>] [--dry-run]\n\n\
         CSV format (one order per line; `#` lines ignored):\n\
         \ttoken_id,side,price,size[,fee_bps]\n\n\
         Example:\n\
         \thexbot new_orders --file orders.csv\n\
         \thexbot new_orders --file orders.csv --dry-run"
    );
}
