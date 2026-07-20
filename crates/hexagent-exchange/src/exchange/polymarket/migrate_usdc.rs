//! `hexbot migrate_usdce` — wrap USDC.e → pUSD in the operator's
//! Polymarket wallet, ready for CLOB v2 trading.
//!
//! Post-cutover (2026-04-28) the v2 CLOB accepts **pUSD** as collateral,
//! not USDC.e. This CLI bundles the two txs required to convert:
//!
//!   1. `<asset>.approve(Onramp, amount)`
//!   2. `Onramp.wrap(<asset>, wallet, amount)` — deposits the selected
//!      backing asset and mints pUSD 1:1.
//!
//! Native USDC is not offered (`migrate_usdc` was removed): Polymarket
//! has paused native USDC on the Collateral Onramp, so its `wrap` batch
//! always reverts. Only bridged USDC.e wraps are live.
//!
//! Both txs execute as Safe `execTransaction` calls. The gas-payer is
//! config-driven — same flag as `hexbot redeem` / `hexbot split` /
//! `hexbot approve_v2` (`gas_via_signer_wallet`):
//!   * `false` (default) → Polymarket gasless relayer (`POST /submit`).
//!     Needs `POLY_BUILDER_*` builder credentials; the signer EOA needs
//!     **no POL**. The relayer accepts arbitrary calldata, so both
//!     `approve(...)` and `wrap(...)` route through it.
//!   * `true`  → signer EOA broadcasts on-chain, POL gas paid from EOA.
//!
//! Usage:
//!   hexbot migrate_usdce all            # wrap bridged USDC.e
//!   hexbot migrate_usdce 100.5          # 6 decimals
//!   hexbot migrate_usdce 100 --dry-run  # print plan without broadcasting
//!
//! Contract addresses (Polygon mainnet, from Polymarket v2 docs):
//!   USDC.e  : 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174 (6 decimals)
//!   pUSD    : 0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB (6 decimals)
//!   Onramp  : 0x93070a847efEf7F70739046A929D47a521F5B8ee
//!
//! Requires: `POLY_PRIVATE_KEY` + `POLYGON_RPC` in `.env`. The on-chain
//! path (`gas_via_signer_wallet=true`) additionally needs a small POL
//! balance on the signer EOA (~0.01 POL × 2 txs); the relayer path does
//! not.

use anyhow::{anyhow, Result};
use log::info;

use super::deploy_wallet::{
    address_to_bytes32, derive_safe_address, u256_bytes,
};
use super::onchain_tx::{poll_onchain_tx, submit_safe_tx_onchain};
use super::signer::derive_eth_address_from_key;
use super::wallet::{
    load_wallet, poll_transaction, read_gas_via_signer_wallet_flag,
    submit_safe_tx_with_id,
};

// ════════════════════════════════════════════════════════════════
// Contract addresses (Polygon mainnet)
// ════════════════════════════════════════════════════════════════

const USDCE_ADDR:  &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
const PUSD_ADDR:   &str = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB";
const ONRAMP_ADDR: &str = "0x93070a847efEf7F70739046A929D47a521F5B8ee";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MigrateAsset {
    address: &'static str,
    label: &'static str,
    command: &'static str,
}

const USDCE: MigrateAsset = MigrateAsset {
    address: USDCE_ADDR,
    label: "USDC.e",
    command: "migrate_usdce",
};

// ERC-20 `approve(address,uint256)` = keccak256("approve(address,uint256)")[:4]
const APPROVE_SELECTOR:    [u8; 4] = [0x09, 0x5e, 0xa7, 0xb3];
// ERC-20 `balanceOf(address)` = keccak256("balanceOf(address)")[:4]
const BALANCE_OF_SELECTOR: [u8; 4] = [0x70, 0xa0, 0x82, 0x31];
// ERC-20 `allowance(address,address)` = keccak256("allowance(address,address)")[:4]
const ALLOWANCE_SELECTOR:  [u8; 4] = [0xdd, 0x62, 0xed, 0x3e];
// Onramp `wrap(address,address,uint256)` = keccak256("wrap(address,address,uint256)")[:4]
const WRAP_SELECTOR:       [u8; 4] = [0x62, 0x35, 0x56, 0x38];

/// EVM `type(uint256).max` = 2^256-1. Standard "infinite approval"
/// value; one-time cost saves the per-wrap approve tx forever.
const U256_MAX_BYTES: [u8; 32] = [0xff; 32];

/// 6-decimal token scale. Shared by USDC, USDC.e, and pUSD.
const USDC_SCALE: u128 = 1_000_000;

/// Post-broadcast confirmation wait. Polygon confirms in 2-5 s under
/// normal load; we poll for up to 60 s before giving up (the tx may
/// still confirm — we just won't wait for it).
const CONFIRM_TIMEOUT_SECS: u64 = 60;
const CONFIRM_POLL_INTERVAL_SECS: u64 = 3;

pub fn run_migrate_usdce() -> Result<()> {
    run_migrate(USDCE)
}

fn run_migrate(asset: MigrateAsset) -> Result<()> {
    let args: Vec<String> = crate::exchange::polymarket::cli_account::cli_args().collect();
    let dry_run = args.iter().any(|a| a == "--dry-run" || a == "-n");
    // By default we approve `type(uint256).max` once and the next
    // wrap runs save ~50k gas each (no approve tx). `--exact-approve`
    // restores the one-shot precise-amount behaviour for operators
    // who prefer minimal allowance exposure.
    let exact_approve = args.iter().any(|a| a == "--exact-approve");
    let amount_arg = args.iter()
        .find(|a| !a.starts_with('-'))
        .cloned()
        .ok_or_else(|| anyhow!(
            "missing amount. Usage:\n\
             \thexbot {} <amount | all> [--dry-run] [--exact-approve]\n\
             Examples:\n\
             \thexbot {} all\n\
             \thexbot {} 100\n\
             \thexbot {} 100 --exact-approve   # no infinite approval\n\
             \thexbot {} 100.5 --dry-run",
            asset.command, asset.command, asset.command, asset.command, asset.command,
        ))?;

    // ── POLY_1271: wrap the deposit wallet's selected asset → pUSD via WALLET
    //    batch (the Safe execTransaction path below doesn't apply). ──
    let sig_type_s = std::env::var("POLY_SIGNATURE_TYPE").unwrap_or_default().to_ascii_lowercase();
    if sig_type_s == "poly_1271" || sig_type_s == "deposit_wallet" {
        let wallet = super::wallet::load_wallet()?;
        let dw = super::deposit_wallet::resolve_deposit_wallet(&wallet.signer_address)?;
        let bal_wei = erc20_balance_of(asset.address, &dw).unwrap_or(0);
        let bal_usdc = bal_wei as f64 / USDC_SCALE as f64;
        let amount_wei: u128 = if amount_arg.eq_ignore_ascii_case("all") {
            bal_wei
        } else {
            let a: f64 = amount_arg.parse().map_err(|_| anyhow!("bad amount '{}'", amount_arg))?;
            (a * USDC_SCALE as f64).round() as u128
        };
        println!("── Deposit-wallet onramp: wrap {} → pUSD ──", asset.label);
        println!("Deposit wallet: {}  ({} balance: {:.6})", dw, asset.label, bal_usdc);
        println!("Wrapping:       {:.6} {}", amount_wei as f64 / USDC_SCALE as f64, asset.label);
        if amount_wei == 0 {
            return Err(anyhow!("nothing to wrap (amount=0 / DW {} balance=0)", asset.label));
        }
        if amount_wei > bal_wei {
            return Err(anyhow!("amount > DW {} balance {:.6}", asset.label, bal_usdc));
        }
        super::deposit_wallet::dw_onramp(
            &wallet.signing_key, &wallet.signer_address, &dw, &wallet.builder_auth,
            asset.address, amount_wei, dry_run,
        )?;
        println!(
            "✅ onramp {} — the CLOB re-reads the deposit wallet's balance on the next order.",
            if dry_run { "(dry-run)" } else { "confirmed" }
        );
        return Ok(());
    }

    // ── Gas-payer dispatch (same flag as approve_v2/redeem/split) ──
    // `gas_via_signer_wallet=false` (default) → Polymarket gasless
    // relayer (no POL needed on the EOA); `true` → signer EOA
    // broadcasts on-chain and pays POL.
    let gas_via_signer = read_gas_via_signer_wallet_flag();
    let wallet = MigrateWallet::load(gas_via_signer)?;
    let signer_address = wallet.signer_address().to_string();
    let safe_address   = wallet.safe_address().to_string();
    info!(
        "[{}] signer={} safe={} gas={}",
        asset.command,
        signer_address, safe_address,
        if gas_via_signer { "signer EOA (on-chain, POL)" } else { "relayer (gasless)" },
    );

    // ── Query starting backing-asset balance (on the Safe) ──────
    let balance_wei = erc20_balance_of(asset.address, &safe_address)
        .ok_or_else(|| anyhow!("Failed to read Safe {} balance on-chain", asset.label))?;
    let balance_usdc = balance_wei as f64 / USDC_SCALE as f64;
    if balance_wei == 0 {
        return Err(anyhow!(
            "Safe {} has 0 {}. Fund that wallet on Polygon first.",
            safe_address, asset.label,
        ));
    }

    // ── Parse amount ────────────────────────────────────────────
    let amount_wei: u128 = if amount_arg.eq_ignore_ascii_case("all") {
        balance_wei
    } else {
        let amount_human: f64 = amount_arg.parse()
            .map_err(|e| anyhow!("invalid amount '{}': {}", amount_arg, e))?;
        if amount_human <= 0.0 {
            return Err(anyhow!("amount must be > 0, got {}", amount_human));
        }
        let wei = (amount_human * USDC_SCALE as f64).round() as u128;
        if wei > balance_wei {
            return Err(anyhow!(
                "requested {:.6} {} > Safe balance {:.6}",
                amount_human, asset.label, balance_usdc,
            ));
        }
        wei
    };
    let amount_usdc = amount_wei as f64 / USDC_SCALE as f64;

    // Starting pUSD balance (for verification delta at the end).
    let pusd_before_wei = erc20_balance_of(PUSD_ADDR, &safe_address).unwrap_or(0);
    let pusd_before = pusd_before_wei as f64 / USDC_SCALE as f64;

    // ── Existing allowance check ────────────────────────────────
    // Skip the approve tx if `allowance(Safe, Onramp) ≥ amount`. Saves
    // ~$0.005 MATIC and a couple seconds of confirm-wait. Also
    // guarantees that after one `--unlimited` run (default), every
    // subsequent migration needs only the wrap tx.
    let existing_allowance = erc20_allowance(asset.address, &safe_address, ONRAMP_ADDR)
        .unwrap_or(0);
    let needs_approve = existing_allowance < amount_wei;

    // ── Plan summary ────────────────────────────────────────────
    println!("── Migration plan ───────────────────────────────");
    println!("Safe (funder) : {}", safe_address);
    println!("Signer (EOA)  : {}", signer_address);
    println!("Gas payer     : {}", if gas_via_signer {
        "signer EOA (on-chain, POL)"
    } else {
        "Polymarket relayer (gasless)"
    });
    println!("{:<6} before : {:>12.6}  (Safe balance)", asset.label, balance_usdc);
    println!("pUSD   before : {:>12.6}  (Safe balance)", pusd_before);
    println!("Amount        : {:>12.6}  → pUSD (via Onramp)", amount_usdc);
    println!("{:<6} after  : {:>12.6}  (projected)", asset.label, balance_usdc - amount_usdc);
    println!("pUSD   after  : {:>12.6}  (projected)", pusd_before + amount_usdc);
    println!(
        "Allowance     : {} (Safe → Onramp, existing)",
        format_allowance(existing_allowance, asset.label),
    );
    println!();

    let tx_count = if needs_approve { 2 } else { 1 };
    println!("Transactions ({} Safe execTransaction call{}):",
        tx_count, if tx_count == 1 { "" } else { "s" });
    if needs_approve {
        if exact_approve {
            println!("  1. {}.approve(Onramp, {}) — precise approval", asset.label, amount_wei);
            println!("     to   = {}", asset.address);
        } else {
            println!("  1. {}.approve(Onramp, type(uint256).max) — INFINITE approval", asset.label);
            println!("     to   = {}", asset.address);
            println!("     (one-time; future `{}` skips this step)", asset.command);
        }
        println!();
    } else {
        println!("  (approve SKIPPED — existing allowance is ≥ requested amount)");
        println!();
    }
    println!("  {}. Onramp.wrap({}, Safe, {}) — mint pUSD to Safe",
        if needs_approve { 2 } else { 1 }, asset.label, amount_wei);
    println!("     to   = {}", ONRAMP_ADDR);
    println!("     data = wrap({}, {}, {})", asset.address, safe_address, amount_wei);
    println!();

    if dry_run {
        println!("(dry-run: not broadcasting)");
        return Ok(());
    }

    // ── Step 1: approve (conditional) ───────────────────────────
    if needs_approve {
        let approve_bytes = if exact_approve {
            u256_bytes(amount_wei)
        } else {
            U256_MAX_BYTES
        };
        let approve_data = build_approve_calldata(ONRAMP_ADDR, &approve_bytes);
        info!(
            "[{}] Step 1: approve {} tx broadcasting",
            asset.command, if exact_approve { "precise" } else { "unlimited" },
        );
        let tx1 = wallet.submit_and_confirm(asset.address, &approve_data)?;
        println!("Step 1 approve tx: {}", tx1);
        println!("          ✅ confirmed");
        println!();
    } else {
        info!(
            "[{}] Skipping approve (existing allowance {} ≥ {})",
            asset.command, existing_allowance, amount_wei,
        );
    }

    // ── Step 2 (or 1 if approve skipped): wrap ──────────────────
    let wrap_data = build_wrap_calldata(asset.address, &safe_address, amount_wei);
    info!("[{}] Wrap tx broadcasting", asset.command);
    let tx2 = wallet.submit_and_confirm(ONRAMP_ADDR, &wrap_data)?;
    let step_label = if needs_approve { "Step 2 wrap tx:" } else { "Step 1 wrap tx:" };
    println!("{:<18} {}", step_label, tx2);
    println!("          ✅ confirmed");
    println!();

    // ── Verify post-balances ────────────────────────────────────
    let asset_after_wei = erc20_balance_of(asset.address, &safe_address).unwrap_or(0);
    let pusd_after_wei  = erc20_balance_of(PUSD_ADDR,  &safe_address).unwrap_or(0);
    let asset_after = asset_after_wei as f64 / USDC_SCALE as f64;
    let pusd_after  = pusd_after_wei  as f64 / USDC_SCALE as f64;
    let pusd_delta  = pusd_after - pusd_before;

    println!("── Result ──────────────────────────────────────");
    println!("{:<6} after  : {:>12.6}  (was {:.6}, Δ {:+.6})", asset.label, asset_after, balance_usdc, asset_after - balance_usdc);
    println!("pUSD   after  : {:>12.6}  (was {:.6}, Δ {:+.6})", pusd_after,  pusd_before,   pusd_delta);
    let expected = amount_usdc;
    if (pusd_delta - expected).abs() > 0.000001 {
        eprintln!(
            "⚠  pUSD delta {:.6} differs from requested {:.6} by {:.6} — \
             unexpected but tx confirmed; inspect on Polygonscan:\n  \
             wrap: https://polygonscan.com/tx/{}",
            pusd_delta, expected, pusd_delta - expected, tx2,
        );
    } else {
        println!();
        println!("✅ pUSD migration complete. Safe is ready for CLOB v2 trading.");
    }
    Ok(())
}

// ════════════════════════════════════════════════════════════════
// Wallet dispatcher — on-chain vs relayer per `gas_via_signer_wallet`
// (mirrors `ApproveWallet` in approve_v2.rs)
// ════════════════════════════════════════════════════════════════

enum MigrateWallet {
    /// `gas_via_signer_wallet=true`: signer EOA broadcasts directly and
    /// pays POL. Builder credentials NOT required.
    OnchainOnly {
        signing_key: k256::ecdsa::SigningKey,
        signer_address: String,
        safe_address: String,
    },
    /// `gas_via_signer_wallet=false` (default): Polymarket gasless
    /// relayer signs + broadcasts. Needs builder API credentials.
    Relayer(super::wallet::WalletInfo),
}

impl MigrateWallet {
    /// Build the dispatcher for the chosen gas-payer path. The on-chain
    /// path only needs `POLY_PRIVATE_KEY`; the relayer path additionally
    /// loads `POLY_BUILDER_*` (errors with a hint if they're missing).
    fn load(gas_via_signer: bool) -> Result<Self> {
        if gas_via_signer {
            let private_key = std::env::var("POLY_PRIVATE_KEY")
                .map_err(|_| super::wallet::no_wallet_creds_err())?;
            if private_key.is_empty() {
                return Err(anyhow!("POLY_PRIVATE_KEY is empty"));
            }
            let signing_key = {
                let clean = private_key.strip_prefix("0x").unwrap_or(&private_key);
                let bytes = hex::decode(clean)
                    .map_err(|e| anyhow!("POLY_PRIVATE_KEY invalid hex: {}", e))?;
                k256::ecdsa::SigningKey::from_bytes(bytes.as_slice().into())
                    .map_err(|e| anyhow!("invalid private key bytes: {}", e))?
            };
            let signer_address = to_checksum(&derive_eth_address_from_key(&signing_key));
            let safe_address   = to_checksum(&derive_safe_address(&signer_address));
            Ok(MigrateWallet::OnchainOnly { signing_key, signer_address, safe_address })
        } else {
            let info = load_wallet().map_err(|e| anyhow!(
                "Failed to load builder credentials for the gasless relayer path: {}\n\
                 Either add a [builder] section with POLY_BUILDER_* creds, \
                 or set `gas_via_signer_wallet = true` in [general] to use the \
                 on-chain path (signer EOA pays POL).", e,
            ))?;
            Ok(MigrateWallet::Relayer(info))
        }
    }

    fn signer_address(&self) -> &str {
        match self {
            MigrateWallet::OnchainOnly { signer_address, .. } => signer_address,
            MigrateWallet::Relayer(w) => &w.signer_address,
        }
    }
    fn safe_address(&self) -> &str {
        match self {
            MigrateWallet::OnchainOnly { safe_address, .. } => safe_address,
            MigrateWallet::Relayer(w) => &w.safe_address,
        }
    }

    /// Submit a Safe `execTransaction` calling `to` with `data`, poll to
    /// a terminal state, and return the Polygonscan-linkable tx hash on
    /// success (or the relayer tx-id if the chain hash wasn't surfaced).
    fn submit_and_confirm(&self, to: &str, data: &str) -> Result<String> {
        match self {
            MigrateWallet::OnchainOnly { signing_key, signer_address, safe_address } => {
                let tx = submit_safe_tx_onchain(
                    signing_key, signer_address, safe_address, to, data,
                )?;
                wait_for_confirm(&tx)?;
                Ok(tx)
            }
            MigrateWallet::Relayer(w) => {
                let (tx_id, _initial_state) = submit_safe_tx_with_id(
                    &w.builder_auth, &w.signing_key,
                    &w.signer_address, &w.safe_address,
                    to, data,
                    /*gas_via_signer=*/ false,
                )?;
                // Poll the relayer to terminal state. 30 × 3 s = 90 s,
                // matching approve_v2's relayer window.
                let mut final_state = String::new();
                let mut tx_hash = String::new();
                for _ in 0..30 {
                    std::thread::sleep(std::time::Duration::from_secs(CONFIRM_POLL_INTERVAL_SECS));
                    let (s, h) = poll_transaction(&w.builder_auth, &tx_id)?;
                    final_state = s.clone();
                    tx_hash = h;
                    if s.contains("CONFIRMED") || s.contains("MINED")
                        || s.contains("FAILED") { break; }
                }
                if final_state.contains("CONFIRMED") || final_state.contains("MINED") {
                    Ok(if tx_hash.is_empty() { tx_id } else { tx_hash })
                } else if final_state.contains("FAILED") {
                    Err(anyhow!("relayer tx FAILED (id={})", tx_id))
                } else {
                    Err(anyhow!(
                        "relayer tx not confirmed within {}s (state={}, id={})",
                        30 * CONFIRM_POLL_INTERVAL_SECS, final_state, tx_id,
                    ))
                }
            }
        }
    }
}

/// Pretty-print a raw u128 allowance in 6-decimal USDC units. Renders
/// `u128::MAX` as "∞ (unlimited)" since the ERC-20 "infinite approval"
/// convention reads back as that value (or close to it).
fn format_allowance(allow: u128, label: &str) -> String {
    if allow == u128::MAX {
        return "∞ (unlimited)".to_string();
    }
    // Anything > 1e15 USDC (1 quadrillion) is effectively unlimited too.
    if allow > 1_000_000_000_000_000_u128 * USDC_SCALE {
        return format!("~∞ ({} raw wei)", allow);
    }
    format!("{:.6} {}", allow as f64 / USDC_SCALE as f64, label)
}

// ════════════════════════════════════════════════════════════════
// Calldata builders
// ════════════════════════════════════════════════════════════════

/// ABI-encode `approve(address spender, uint256 amount)`. `amount` is
/// passed as a pre-built 32-byte big-endian blob so callers can feed
/// either an exact value (via `u256_bytes(amount_u128)`) or
/// `U256_MAX_BYTES` for the standard "infinite" approval.
fn build_approve_calldata(spender: &str, amount_bytes: &[u8; 32]) -> String {
    let mut buf = Vec::with_capacity(4 + 64);
    buf.extend_from_slice(&APPROVE_SELECTOR);
    buf.extend_from_slice(&address_to_bytes32(spender));
    buf.extend_from_slice(amount_bytes);
    format!("0x{}", hex::encode(buf))
}

/// Read ERC-20 `allowance(owner, spender)`. Returns the raw u256
/// low 128 bits (sufficient for any realistic USDC/USDC.e allowance —
/// anything > u128::MAX reads back clipped, which is fine because
/// we only compare against `amount_wei: u128`).
fn erc20_allowance(token: &str, owner: &str, spender: &str) -> Option<u128> {
    let mut calldata = Vec::with_capacity(4 + 64);
    calldata.extend_from_slice(&ALLOWANCE_SELECTOR);
    calldata.extend_from_slice(&address_to_bytes32(owner));
    calldata.extend_from_slice(&address_to_bytes32(spender));
    let data = format!("0x{}", hex::encode(&calldata));

    let result_hex = super::deploy_wallet::eth_call(token, &data)?;
    let hex_str = result_hex.strip_prefix("0x").unwrap_or(&result_hex);
    if hex_str.is_empty() { return Some(0); }
    // Response is 32 bytes big-endian; take the low 16 bytes as u128.
    // If the value exceeds u128::MAX we clip to u128::MAX — caller only
    // uses this for a `>=` comparison against amount_wei (a u128), so
    // any overflow is semantically equivalent to "very large allowance".
    let bytes = hex::decode(hex_str).ok()?;
    if bytes.len() < 32 { return None; }
    // First 16 bytes non-zero → value > u128::MAX → return MAX.
    if bytes[..16].iter().any(|&b| b != 0) {
        return Some(u128::MAX);
    }
    let mut buf = [0u8; 16];
    buf.copy_from_slice(&bytes[16..32]);
    Some(u128::from_be_bytes(buf))
}

/// ABI-encode `wrap(address asset, address to, uint256 amount)`.
fn build_wrap_calldata(asset: &str, to: &str, amount: u128) -> String {
    let mut buf = Vec::with_capacity(4 + 96);
    buf.extend_from_slice(&WRAP_SELECTOR);
    buf.extend_from_slice(&address_to_bytes32(asset));
    buf.extend_from_slice(&address_to_bytes32(to));
    buf.extend_from_slice(&u256_bytes(amount));
    format!("0x{}", hex::encode(buf))
}

/// Read ERC-20 `balanceOf(address)` for `token` at `owner`. Returns
/// raw (6-decimal-unscaled) u128 units, or `None` on RPC error.
fn erc20_balance_of(token: &str, owner: &str) -> Option<u128> {
    let mut calldata = Vec::with_capacity(4 + 32);
    calldata.extend_from_slice(&BALANCE_OF_SELECTOR);
    calldata.extend_from_slice(&address_to_bytes32(owner));
    let data = format!("0x{}", hex::encode(&calldata));

    let result_hex = super::deploy_wallet::eth_call(token, &data)?;
    let hex_str = result_hex.strip_prefix("0x").unwrap_or(&result_hex);
    let trimmed = hex_str.trim_start_matches('0');
    if trimmed.is_empty() { return Some(0); }
    u128::from_str_radix(trimmed, 16).ok()
}

/// Block until `tx_hash` confirms, polling every few seconds. Returns
/// error on on-chain failure or timeout.
fn wait_for_confirm(tx_hash: &str) -> Result<()> {
    let start = std::time::Instant::now();
    let poll  = std::time::Duration::from_secs(CONFIRM_POLL_INTERVAL_SECS);
    let limit = std::time::Duration::from_secs(CONFIRM_TIMEOUT_SECS);
    loop {
        let (state, _) = poll_onchain_tx(tx_hash)?;
        match state.as_str() {
            "CONFIRMED"    => return Ok(()),
            "STATE_FAILED" => return Err(anyhow!("tx {} reverted on-chain", tx_hash)),
            _              => {} // PENDING
        }
        if start.elapsed() > limit {
            return Err(anyhow!(
                "tx {} not confirmed within {}s (it may still confirm later — check \
                 https://polygonscan.com/tx/{})",
                tx_hash, CONFIRM_TIMEOUT_SECS, tx_hash,
            ));
        }
        std::thread::sleep(poll);
    }
}

/// EIP-55 checksum encoding for an Ethereum address. Mirrors the
/// helper in `signer.rs` (private there); duplicated here to avoid a
/// cross-module dependency for a ~15-line utility.
fn to_checksum(addr: &str) -> String {
    use sha3::{Digest, Keccak256};
    let hex_str = addr.strip_prefix("0x").unwrap_or(addr).to_lowercase();
    let mut h = Keccak256::new();
    h.update(hex_str.as_bytes());
    let hash: [u8; 32] = h.finalize().into();
    let mut out = String::with_capacity(42);
    out.push_str("0x");
    for (i, c) in hex_str.chars().enumerate() {
        if c.is_ascii_digit() {
            out.push(c);
        } else {
            let nibble = if i % 2 == 0 { hash[i / 2] >> 4 } else { hash[i / 2] & 0x0f };
            out.push(if nibble >= 8 { c.to_ascii_uppercase() } else { c });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const RECIPIENT: &str = "0x1111111111111111111111111111111111111111";

    #[test]
    fn migration_command_selects_official_usdce() {
        assert_eq!(USDCE.command, "migrate_usdce");
        assert_eq!(USDCE.address, USDCE_ADDR);
        assert_eq!(USDCE.label, "USDC.e");
    }

    #[test]
    fn wrap_calldata_encodes_the_selected_asset() {
        let calldata = build_wrap_calldata(USDCE.address, RECIPIENT, 12_345_678);
        let bytes = hex::decode(calldata.trim_start_matches("0x")).unwrap();
        assert_eq!(&bytes[..4], &WRAP_SELECTOR);
        assert_eq!(&bytes[4..36], &address_to_bytes32(USDCE.address));
        assert_eq!(&bytes[36..68], &address_to_bytes32(RECIPIENT));
        assert_eq!(&bytes[68..100], &u256_bytes(12_345_678));
    }
}
