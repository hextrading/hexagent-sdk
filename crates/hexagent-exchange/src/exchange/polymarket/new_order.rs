//! `hexbot new_order` — place a single order on Polymarket, with the
//! CLOB wire format (v1 vs v2) picked from
//! `config/live_polymaker.toml`'s `clob_version` field.
//!
//! Mirrors the ergonomics of `hexbot positions` / `active_orders` /
//! `cancel_order`: reads the polymarket exchange section of the config
//! for `signature_type`, `api_url_prefix`, `builder_code`, and
//! `clob_version`. Env (`POLY_SIGNATURE_TYPE`, `POLYMARKET_V2_API_URL`,
//! `POLY_CLOB_VERSION`) overrides the corresponding config values when
//! an operator wants a one-off test.
//!
//! Usage:
//!   hexbot new_order <token_id> <BUY|SELL> <price> <size>
//!                    [--fee-bps N]      # v1 only; default 0
//!                    [--builder 0x...]  # v2 only; overrides config
//!                    [<config-path>]    # default config/live_polymaker.toml
//!
//! Examples:
//!   hexbot new_order 4641600677...023 BUY 0.01 5
//!   hexbot new_order 4641600677...023 BUY 0.01 5 --fee-bps 1000
//!   hexbot new_order 4641600677...023 SELL 0.99 5 config/backtest_polymaker.toml
//!
//! The token_id is the outcome `clob_token_id` (large decimal string;
//! find it via `hexbot active_event`). Price ∈ [0.01, 0.99].
//!
//! On success the server's `orderID` is compared against the locally-
//! computed EIP-712 digest — a "MATCH" confirms our signing pipeline
//! is correct; a "MISMATCH" warns of a pending schema drift.

use anyhow::{anyhow, Result};
use log::info;
use std::path::Path;

use super::auth::PolyAuth;
use super::signer::{OrderSigner, SignatureType};
use super::signer_v2::OrderSignerV2;

/// Default CLOB host when neither config nor env supplies one.
/// `clob.polymarket.com` serves both v1 and v2 during the migration
/// window and flips to v2 automatically at cutover (2026-04-28), so
/// it's the right single default regardless of `clob_version`.
/// Matches the fallback used by `active_orders` / `cancel_order` in
/// wallet.rs.
const DEFAULT_API_URL: &str = "https://clob.polymarket.com";

pub fn run_new_order() -> Result<()> {
    let args: Vec<String> = crate::exchange::polymarket::cli_account::cli_args().collect();

    // Parse positional + flag args.
    let mut positional: Vec<String> = Vec::new();
    let mut fee_bps: Option<u32> = None;
    let mut builder_cli: Option<String> = None;
    let mut iter = args.iter();
    while let Some(a) = iter.next() {
        match a.as_str() {
            "--fee-bps" => {
                let v = iter.next().ok_or_else(|| anyhow!("--fee-bps requires a value"))?;
                fee_bps = Some(v.parse().map_err(|e| anyhow!("--fee-bps parse: {}", e))?);
            }
            "--builder" => {
                builder_cli = Some(iter.next()
                    .ok_or_else(|| anyhow!("--builder requires a value"))?.clone());
            }
            "-h" | "--help" => { print_usage(); return Ok(()); }
            _ => positional.push(a.clone()),
        }
    }
    if positional.len() < 4 {
        print_usage();
        return Err(anyhow!("missing args"));
    }
    let token_id = positional[0].clone();
    let side = match positional[1].to_ascii_uppercase().as_str() {
        "BUY"  => crate::types::Side::Buy,
        "SELL" => crate::types::Side::Sell,
        other => return Err(anyhow!("side must be BUY or SELL, got '{}'", other)),
    };
    let price: f64 = positional[2].parse().map_err(|e| anyhow!("price parse: {}", e))?;
    let size:  f64 = positional[3].parse().map_err(|e| anyhow!("size parse: {}",  e))?;
    let config_path = positional.get(4).cloned()
        .unwrap_or_else(|| "config/live_polymaker.toml".to_string());

    // Exclusive (0, 1): matches what the signed-order build path enforces
    // (trade.rs). The valid lower bound is the market's tick_size (0.01 or
    // 0.001), which this manual CLI tool doesn't load — Polymarket rejects
    // off-tick / out-of-range prices server-side, so don't hardcode the
    // coarse 0.01 floor here.
    if !(price > 0.0 && price < 1.0) {
        return Err(anyhow!("price must be in (0, 1), got {}", price));
    }
    if size <= 0.0 {
        return Err(anyhow!("size must be > 0, got {}", size));
    }

    // ── Load config ──────────────────────────────────────────────
    let poly_cfg = crate::config::Config::load(Path::new(&config_path))
        .ok()
        .and_then(|c| c.exchanges.into_iter().find(|e| e.name == "polymarket"));

    // clob_version: env > config > default v2. Only an explicit `v1`/`1`
    // (env or config) selects the legacy chain; empty/missing → v2.
    let clob_version_s = std::env::var("POLY_CLOB_VERSION").ok()
        .or_else(|| poly_cfg.as_ref().map(|p| p.clob_version.clone()))
        .unwrap_or_default();
    let is_v2 = crate::exchange::polymarket::wallet::is_v2_from_str(&clob_version_s);
    let clob_label = if is_v2 { "v2" } else { "v1" };

    // Phase 6: signature_type and builder_code are per-wallet,
    // sourced from secrets.toml via `cli_account::resolve_and_apply()`
    // which populates POLY_SIGNATURE_TYPE / POLY_BUILDER_CODE before
    // this function runs. Legacy `[[exchanges]] polymarket` reads are
    // removed.
    let sig_type_s = std::env::var("POLY_SIGNATURE_TYPE").unwrap_or_default();
    let sig_type = match sig_type_s.to_ascii_lowercase().as_str() {
        "gnosis_safe" | "safe" => SignatureType::PolyGnosisSafe,
        "poly_proxy"           => SignatureType::PolyProxy,
        "poly_1271" | "deposit_wallet" => SignatureType::Poly1271,
        _                      => SignatureType::Eoa,
    };

    // builder_code (v2 only): CLI explicit flag > env > zero.
    let builder_code = builder_cli
        .or_else(|| std::env::var("POLY_BUILDER_CODE").ok())
        .unwrap_or_default();

    // API URL: env > config > default (main host). Single fallback
    // regardless of v1/v2 — `clob.polymarket.com` is valid for both.
    let api_url = std::env::var("POLYMARKET_V2_API_URL").ok()
        .or_else(|| poly_cfg.as_ref()
            .map(|p| p.api_url_prefix.clone())
            .filter(|s| !s.is_empty()))
        .unwrap_or_else(|| DEFAULT_API_URL.to_string());

    // ── Load credentials (env only) ──────────────────────────────
    let api_key = std::env::var("POLY_API_KEY")
        .map_err(|_| anyhow!("POLY_API_KEY not set"))?;
    let api_secret = std::env::var("POLY_API_SECRET")
        .map_err(|_| anyhow!("POLY_API_SECRET not set"))?;
    let passphrase = std::env::var("POLY_PASSPHRASE")
        .map_err(|_| anyhow!("POLY_PASSPHRASE not set"))?;
    let private_key = std::env::var("POLY_PRIVATE_KEY")
        .map_err(|_| anyhow!("POLY_PRIVATE_KEY not set"))?;

    // NegRisk: env only (CLI arg would be one more thing to remember).
    let neg_risk = std::env::var("NEGRISK").ok().as_deref() == Some("1");

    // ── Sign (dispatch on version) + build JSON body ─────────────
    // `--fee-bps` applies to BOTH v1 and v2 bodies:
    //   v1: signed into the typed-data (must match market exactly)
    //   v2: not signed, but server validates the wire value against
    //       the market's configured maker fee (same requirement,
    //       different enforcement surface).
    let fee = fee_bps.unwrap_or(0);
    let (order_hash, body) = if is_v2 {
        build_v2(
            &token_id, price, size, side,
            &private_key, neg_risk, sig_type, &builder_code,
            fee,
            &api_key,
        )?
    } else {
        build_v1(
            &token_id, price, size, side,
            &private_key, neg_risk, sig_type, fee,
            &api_key,
        )?
    };

    // Print a compact plan line.
    let maker_address = match if is_v2 {
        OrderSignerV2::new(&private_key, neg_risk, sig_type, &builder_code)
            .map(|s| s.maker_address)
    } else {
        OrderSigner::new(&private_key, neg_risk, sig_type)
            .map(|s| s.maker_address)
    } {
        Ok(s) => s,
        Err(_) => "<?>".to_string(),
    };

    println!("── Request ───────────────────────────────────────");
    println!("Config:      {}", config_path);
    println!("CLOB:        {}", clob_label);
    println!("API URL:     {}", api_url);
    println!("Sig type:    {:?}", sig_type);
    println!("Neg risk:    {}", neg_risk);
    println!("Maker:       {}", maker_address);
    println!("Token:       {}", token_id);
    println!("Side / Size: {} × {} @ {:.4}", format!("{:?}", side).to_uppercase(), size, price);
    if is_v2 {
        println!("Builder:     {}", if builder_code.is_empty() { "<zero>".to_string() } else { builder_code.clone() });
    } else {
        println!("Fee bps:     {}", fee_bps.unwrap_or(0));
    }
    info!("[new_order] local_hash={}", order_hash);
    println!("Local hash:  {}", order_hash);
    println!();

    // ── POST /order ──────────────────────────────────────────────
    let auth = PolyAuth::new(&api_key, &api_secret, &passphrase, &auth_address(&private_key)?)?;
    let body_str = serde_json::to_string(&body)?;
    let headers = auth.sign_request("POST", "/order", &body_str);
    let url = format!("{}/order", api_url.trim_end_matches('/'));

    let client = crate::async_rt::http_client_query();
    let body_for_move = body_str.clone();
    let (status, text) = crate::async_rt::block_on_runtime(async move {
        let mut req = client.post(&url).body(body_for_move)
            .header("Content-Type", "application/json");
        for (k, v) in headers.as_pairs() {
            req = req.header(k, v);
        }
        let r = req.send().await.map_err(|e| anyhow!("HTTP send: {}", e))?;
        let status = r.status();
        let text = r.text().await.map_err(|e| anyhow!("read body: {}", e))?;
        Ok::<_, anyhow::Error>((status, text))
    })?;

    println!("── Response ──────────────────────────────────────");
    println!("Status: {}", status);
    println!("Body:   {}", text);
    println!();

    // Best-effort hash cross-check.
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
        if let Some(server_oid) = json.get("orderID").and_then(|v| v.as_str()) {
            let local_lc  = order_hash.trim_start_matches("0x").to_ascii_lowercase();
            let server_lc = server_oid.trim_start_matches("0x").to_ascii_lowercase();
            if local_lc == server_lc {
                println!("✅ orderID MATCH: local hash == server orderID");
                println!("   → {} typehash / field order correct.", clob_label);
            } else {
                println!("❌ orderID MISMATCH");
                println!("   local : {}", order_hash);
                println!("   server: {}", server_oid);
            }
        } else {
            println!("(no orderID in response — check `error`/`errorMsg` above)");
        }
    }
    Ok(())
}

fn print_usage() {
    eprintln!(
        "Usage: hexbot new_order <token_id> <BUY|SELL> <price> <size> \
         [--fee-bps N] [--builder 0x...] [<config-path>]\n\n\
         Picks v1 or v2 CLOB wire format from the polymarket exchange's\n\
         `clob_version` field in `config/live_polymaker.toml` (default).\n\
         Env: POLY_SIGNATURE_TYPE, POLY_CLOB_VERSION, POLYMARKET_V2_API_URL,\n\
              NEGRISK=1 to target the Neg Risk CTF Exchange.\n\n\
         Examples:\n\
         \thexbot new_order 4641...023 BUY 0.01 5\n\
         \thexbot new_order 4641...023 BUY 0.01 5 --fee-bps 1000"
    );
}

/// Derive the EOA address from private key (used as `POLY_ADDRESS`
/// header — auth always keys on the EOA, even for Safe accounts).
pub(crate) fn auth_address(private_key: &str) -> Result<String> {
    let clean = private_key.strip_prefix("0x").unwrap_or(private_key);
    let bytes = hex::decode(clean).map_err(|e| anyhow!("private key: {}", e))?;
    let key = k256::ecdsa::SigningKey::from_bytes(bytes.as_slice().into())
        .map_err(|e| anyhow!("private key: {}", e))?;
    Ok(super::signer::derive_eth_address_from_key(&key))
}

/// v1 sign + body. `fee_rate_bps` is signed into the order and must
/// match the market's `takerBaseFee` or the server rejects.
pub(crate) fn build_v1(
    token_id: &str,
    price: f64, size: f64, side: crate::types::Side,
    private_key: &str, neg_risk: bool, sig_type: SignatureType,
    fee_rate_bps: u32,
    owner: &str,
) -> Result<(String, serde_json::Value)> {
    let signer = OrderSigner::new(private_key, neg_risk, sig_type)?;
    let signed = signer.build_signed_order(token_id, price, size, side, fee_rate_bps)?;
    let salt_u64: u64 = signed.order.salt.parse::<u128>().map(|v| v as u64).unwrap_or(0);
    let body = serde_json::json!({
        "owner": owner,
        "orderType": "GTC",
        "postOnly": false,
        "order": {
            "salt": salt_u64,
            "maker": signed.order.maker,
            "signer": signed.order.signer,
            "taker": signed.order.taker,
            "tokenId": signed.order.token_id,
            "makerAmount": signed.order.maker_amount,
            "takerAmount": signed.order.taker_amount,
            "expiration": signed.order.expiration,
            "nonce": signed.order.nonce,
            "feeRateBps": signed.order.fee_rate_bps,
            "side": if side == crate::types::Side::Buy { "BUY" } else { "SELL" },
            "signature": signed.signature,
            "signatureType": signed.order.signature_type,
        }
    });
    Ok((signed.order_hash, body))
}

/// v2 sign + body. Mirrors `orderToJsonV2` in clob-client-v2:
/// 11 signed fields (salt, maker, signer, tokenId, makerAmount,
/// takerAmount, side, signatureType, timestamp, metadata, builder)
/// + wire-only `taker` (zero) and `expiration` ("0"). NO `nonce`,
/// NO `feeRateBps` — both removed in v2. Fee rate is left unused
/// (server computes fees protocol-side at match time).
pub(crate) fn build_v2(
    token_id: &str,
    price: f64, size: f64, side: crate::types::Side,
    private_key: &str, neg_risk: bool, sig_type: SignatureType,
    builder_code: &str,
    _fee_rate_bps: u32,
    owner: &str,
) -> Result<(String, serde_json::Value)> {
    // POLY_1271: maker/signer = deposit-wallet funder + ERC-7739 wrap.
    let funder = std::env::var("POLY_FUNDER").unwrap_or_default();
    let signer = OrderSignerV2::new(private_key, neg_risk, sig_type, builder_code)?
        .with_funder(&funder);
    let signed = signer.build_signed_order_dispatch(token_id, price, size, side)?;
    let salt_u64: u64 = signed.order.salt.parse::<u128>().map(|v| v as u64).unwrap_or(0);
    let body = serde_json::json!({
        "owner": owner,
        "orderType": "GTC",
        "postOnly": false,
        "deferExec": false,
        "order": {
            "salt": salt_u64,
            "maker": signed.order.maker,
            "signer": signed.order.signer,
            "taker": signed.order.taker,
            "tokenId": signed.order.token_id,
            "makerAmount": signed.order.maker_amount,
            "takerAmount": signed.order.taker_amount,
            "side": if side == crate::types::Side::Buy { "BUY" } else { "SELL" },
            "signatureType": signed.order.signature_type,
            "timestamp": signed.order.timestamp,
            "expiration": signed.order.expiration,
            "metadata": signed.order.metadata,
            "builder": signed.order.builder,
            "signature": signed.signature,
        }
    });
    Ok((signed.order_hash, body))
}
