//! `hexbot merge` — reverse of `split`. Combines equal Up + Down
//! outcome-token balances back into collateral (USDC.e in v1,
//! pUSD in v2 — the adapter handles the unwrap).
//!
//! Useful when:
//!   * A split's outcome tokens didn't fully fill and the operator
//!     wants to reclaim collateral before settlement.
//!   * Balancing an event mid-way (buy back Up + Down, merge to
//!     reduce exposure on both sides at once).
//!
//! Config-aware — same conventions as `hexbot redeem` / `split`:
//! reads `clob_version` from `config/live_polymaker.toml` and picks
//!   v1 → CTF contract directly with USDC.e
//!   v2 → CtfCollateralAdapter with pUSD
//! The two paths share ABI + calldata layout (selector 0x9e7212ad);
//! only the target contract and collateral-token slot differ.
//!
//! Usage:
//!   hexbot merge <condition_id> <amount>
//!   hexbot merge <condition_id> <amount> --dry-run
//!
//! Example:
//!   hexbot merge 0xc5bd...ce167 5.0
//!
//! Precondition: Safe must hold ≥ `amount` Up shares AND ≥ `amount`
//! Down shares of the given market. If either falls short, the
//! `mergePositions` call reverts on-chain. Check via
//! `hexbot positions` beforehand.

use anyhow::{anyhow, Result};
use log::info;

use super::deploy_wallet::{address_to_bytes32, derive_safe_address, u256_bytes};
use super::onchain_tx::{poll_onchain_tx, submit_safe_tx_onchain};
use super::signer::derive_eth_address_from_key;
use super::wallet::{ctf_target, read_clob_v2_flag, read_gas_via_signer_wallet_flag};

// `mergePositions(address,bytes32,bytes32,uint256[],uint256)` selector.
// keccak256(...)[:4] = 0x9e7212ad
const MERGE_SELECTOR: [u8; 4] = [0x9e, 0x72, 0x12, 0xad];

const USDC_SCALE: u128 = 1_000_000;

const CONFIRM_TIMEOUT_SECS: u64 = 60;
const CONFIRM_POLL_INTERVAL_SECS: u64 = 3;

pub fn run_merge() -> Result<()> {
    let args: Vec<String> = crate::exchange::polymarket::cli_account::cli_args().collect();
    let dry_run = args.iter().any(|a| a == "--dry-run" || a == "-n");
    let positional: Vec<String> = args.iter()
        .filter(|a| !a.starts_with('-'))
        .cloned().collect();

    if positional.len() < 2 {
        eprintln!(
            "Usage: hexbot merge <condition_id> <amount> [--dry-run]\n\n\
             <condition_id>: 0x + 64 hex, the market's condition hash.\n\
             <amount>:       USDC-worth of Up+Down shares to merge\n\
                             (merges `amount` Up AND `amount` Down → amount collateral).\n\n\
             Precondition: Safe holds ≥ amount of BOTH outcomes.\n\
             See `hexbot positions` to verify before running.\n\n\
             Example:\n\
             \thexbot merge 0xc5bd...ce167 5.0"
        );
        return Err(anyhow!("missing args"));
    }
    let condition_id = positional[0].clone();
    let amount: f64 = positional[1].parse()
        .map_err(|e| anyhow!("amount parse: {}", e))?;

    // Validate condition_id shape.
    let hex_clean = condition_id.strip_prefix("0x").unwrap_or(&condition_id);
    if !hex_clean.chars().all(|c| c.is_ascii_hexdigit()) || hex_clean.len() != 64 {
        return Err(anyhow!(
            "condition_id must be `0x` + 64 hex chars, got '{}' ({} chars)",
            condition_id, hex_clean.len(),
        ));
    }
    if amount <= 0.0 {
        return Err(anyhow!("amount must be > 0, got {}", amount));
    }
    let amount_wei: u128 = (amount * USDC_SCALE as f64).round() as u128;

    // ── POLY_1271: merge FROM the deposit wallet via WALLET batch
    //    (mergePositions on the CTF with pUSD), not the Safe. ──
    let sig_type_s = std::env::var("POLY_SIGNATURE_TYPE").unwrap_or_default().to_ascii_lowercase();
    if sig_type_s == "poly_1271" || sig_type_s == "deposit_wallet" {
        let wallet = super::wallet::load_wallet()?;
        let dw = super::deposit_wallet::resolve_deposit_wallet(&wallet.signer_address)?;
        println!("=== Polymarket Merge (deposit wallet) ===");
        println!("Deposit wallet: {}", dw);
        println!("Condition ID:   {}", condition_id);
        println!("Merge amount:   {} ({} wei)", amount, amount_wei);
        super::deposit_wallet::dw_merge(
            &wallet.signing_key, &wallet.signer_address, &dw, &wallet.builder_auth,
            &condition_id, amount_wei, dry_run,
        )?;
        println!("✅ merge {} — DW reclaims {} pUSD.", if dry_run { "(dry-run)" } else { "confirmed" }, amount);
        return Ok(());
    }

    // Load wallet + config-driven flags.
    let private_key = std::env::var("POLY_PRIVATE_KEY")
        .map_err(|_| anyhow!(
            "POLY_PRIVATE_KEY not set — credentials load from the secrets \
             file's [poly.<id>] block; run with --instance <id> --config <cfg>"
        ))?;
    let signing_key = {
        let clean = private_key.strip_prefix("0x").unwrap_or(&private_key);
        let bytes = hex::decode(clean).map_err(|e| anyhow!("private key hex: {}", e))?;
        k256::ecdsa::SigningKey::from_bytes(bytes.as_slice().into())
            .map_err(|e| anyhow!("private key bytes: {}", e))?
    };
    let signer_address = to_checksum(&derive_eth_address_from_key(&signing_key));
    let safe_address   = to_checksum(&derive_safe_address(&signer_address));

    let is_v2 = read_clob_v2_flag();
    let (target_contract, collateral_token) = ctf_target(is_v2, /*neg_risk=*/ false);
    let gas_via_signer = read_gas_via_signer_wallet_flag();

    println!("=== Polymarket Merge ===");
    println!("Wallet:        {}", safe_address);
    println!("Signer:        {}", signer_address);
    println!("CLOB:          {} ({})", if is_v2 { "v2" } else { "v1" },
        if is_v2 { "pUSD via CtfCollateralAdapter" } else { "USDC.e via CTF" });
    println!("Target:        {}", target_contract);
    println!("Collateral:    {}", collateral_token);
    println!("Condition ID:  {}", condition_id);
    println!("Merge amount:  {} ({} wei)", amount, amount_wei);
    println!(
        "Gas payer:     {}",
        if gas_via_signer { "signer EOA (direct on-chain, MATIC)" }
        else              { "Polymarket relayer (gasless)" }
    );
    println!();

    // Build calldata. Layout identical to `splitPosition` except selector.
    //   mergePositions(address collateralToken, bytes32 parent, bytes32 conditionId,
    //                  uint256[] partition, uint256 amount)
    let cid_bytes = hex::decode(hex_clean).unwrap_or_default();
    let mut cid_padded = [0u8; 32];
    let start = 32 - cid_bytes.len().min(32);
    cid_padded[start..].copy_from_slice(&cid_bytes[..cid_bytes.len().min(32)]);

    let mut calldata = Vec::with_capacity(4 + 32 * 8);
    calldata.extend_from_slice(&MERGE_SELECTOR);
    calldata.extend_from_slice(&address_to_bytes32(collateral_token));
    calldata.extend_from_slice(&[0u8; 32]);            // parentCollectionId = 0
    calldata.extend_from_slice(&cid_padded);           // conditionId
    calldata.extend_from_slice(&u256_bytes(160));      // offset to partition (5 × 32)
    calldata.extend_from_slice(&u256_bytes(amount_wei)); // amount
    calldata.extend_from_slice(&u256_bytes(2));        // partition.length = 2
    calldata.extend_from_slice(&u256_bytes(1));        // partition[0] = Up
    calldata.extend_from_slice(&u256_bytes(2));        // partition[1] = Down
    let data_hex = format!("0x{}", hex::encode(&calldata));

    if dry_run {
        println!("── Calldata (dry-run) ───────────────────────────");
        println!("to:   {}", target_contract);
        println!("data: {}", data_hex);
        println!();
        println!("(dry-run: not broadcasting)");
        return Ok(());
    }

    // Only the on-chain path is wired — matches `migrate_usdc` / `approve_v2`.
    // The relayer path is nonstandard for merge (not a typical redeem/split
    // maintenance action) and the operator has MATIC for gas anyway.
    if !gas_via_signer {
        return Err(anyhow!(
            "merge currently supports on-chain submission only. \
             Set `gas_via_signer_wallet = true` in config/live_polymaker.toml."
        ));
    }

    info!("[merge] Broadcasting Safe execTransaction → {}", target_contract);
    let tx = submit_safe_tx_onchain(
        &signing_key, &signer_address, &safe_address,
        target_contract, &data_hex,
    )?;
    println!("Merge tx:    {}", tx);
    wait_for_confirm(&tx)?;
    println!("          ✅ confirmed");
    println!();
    println!("✅ Merge complete. {} Up + {} Down shares → {} {} back to Safe.",
        amount, amount, amount, if is_v2 { "pUSD" } else { "USDC.e" });
    Ok(())
}

fn wait_for_confirm(tx_hash: &str) -> Result<()> {
    let start = std::time::Instant::now();
    let poll  = std::time::Duration::from_secs(CONFIRM_POLL_INTERVAL_SECS);
    let limit = std::time::Duration::from_secs(CONFIRM_TIMEOUT_SECS);
    loop {
        let (state, _) = poll_onchain_tx(tx_hash)?;
        match state.as_str() {
            "CONFIRMED"    => return Ok(()),
            "STATE_FAILED" => return Err(anyhow!("tx {} reverted on-chain", tx_hash)),
            _              => {}
        }
        if start.elapsed() > limit {
            return Err(anyhow!(
                "tx {} not confirmed within {}s — check https://polygonscan.com/tx/{}",
                tx_hash, CONFIRM_TIMEOUT_SECS, tx_hash,
            ));
        }
        std::thread::sleep(poll);
    }
}

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
