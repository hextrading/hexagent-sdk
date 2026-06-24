//! `hexbot approve_v2` — grant every allowance a Gnosis Safe needs
//! for CLOB v2 trading + split/merge, in one batch.
//!
//! The v2 collateral is pUSD (distinct from v1's USDC.e), and the
//! Exchange + CTF-adapter contracts are new — so none of the
//! approvals that `deploy_wallet` set up for v1 transfer over.
//! Without these, the v2 Exchange returns `"not enough balance /
//! allowance"` and the CTF adapter reverts split/merge.
//!
//! Eight distinct operator → contract pairs get checked; each is
//! no-op'd if already approved, else submitted as a Safe
//! `execTransaction`. Gas-payer is config-driven — same flag as
//! redeem/split (`gas_via_signer_wallet`):
//!   * `false` (default) → Polymarket gasless relayer (`POST /submit`).
//!     Verified against `Polymarket/builder-relayer-client::examples/approve.ts`;
//!     the relayer accepts both ERC-20 `approve(...)` and ERC-1155
//!     `setApprovalForAll(...)` calldata, no selector whitelist.
//!     Requires `POLY_BUILDER_*` builder credentials in `.env`.
//!   * `true`  → signer EOA broadcasts on-chain (MATIC paid from EOA).
//!
//!    #  op               token   spender (contract)                      comment
//!    1  approve(∞)       pUSD  → CTFExchangeV2                           POST /order standard
//!    2  approve(∞)       pUSD  → NegRiskCTFExchangeV2                    POST /order neg-risk
//!    3  approve(∞)       pUSD  → CtfCollateralAdapter                    split/merge standard
//!    4  approve(∞)       pUSD  → NegRiskCtfCollateralAdapter             split/merge neg-risk
//!    5  setApprovalForAll CTF   → CTFExchangeV2                          SELL outcome tokens std
//!    6  setApprovalForAll CTF   → NegRiskCTFExchangeV2                   SELL outcome tokens neg-risk
//!
//! Cost: 6 Safe `execTransaction` calls × ~80k gas each at Polygon
//! base fee ≈ 0.005-0.01 MATIC total. One-time per Safe.
//!
//! Usage:
//!   hexbot approve_v2              # check + set any missing approvals
//!   hexbot approve_v2 --dry-run    # report state, don't broadcast
//!
//! Re-running after completion is safe — every step skips if the
//! corresponding approval is already on-chain.

use anyhow::{anyhow, Result};
use log::info;

use super::deploy_wallet::{
    address_to_bytes32, check_erc1155_approval, check_erc20_allowance,
    derive_safe_address, u256_bytes,
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

/// pUSD — v2 collateral.
const PUSD_ADDRESS: &str = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB";
/// ConditionalTokens (CTF). Same contract as v1 (unchanged at cutover).
const CTF_CONTRACT: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";

/// v2 CTF Exchange — standard binary markets.
const CTF_EXCHANGE_V2:          &str = "0xE111180000d2663C0091e4f400237545B87B996B";
/// v2 Neg Risk CTF Exchange — multi-outcome markets.
const NEG_RISK_CTF_EXCHANGE_V2: &str = "0xe2222d279d744050d28e00520010520000310F59";
/// Collateral adapter that wraps pUSD ⇄ ConditionalTokens collateral
/// for `splitPosition` / `mergePositions` on standard markets.
///
/// **Address rotation 2026-05-03**: the legacy adapter
/// (`0xADa100874d…`) now returns `RelayerError "calls to legacy
/// collateral adapter are no longer accepted"`. Approvals granted
/// against the legacy address are useless going forward; re-run
/// `hexbot approve_v2` after upgrading to grant the new allowances.
const CTF_COLLATERAL_ADAPTER:       &str = "0xAdA100Db00Ca00073811820692005400218FcE1f";
/// Same, for neg-risk markets (goes through NegRiskAdapter internally).
/// Migrated 2026-05-03 alongside the standard adapter.
const NEG_RISK_CTF_COLL_ADAPTER:    &str = "0xadA2005600Dec949baf300f4C6120000bDB6eAab";

// ERC-20 approve(address,uint256) = keccak256("approve(address,uint256)")[:4]
const APPROVE_SELECTOR:             [u8; 4] = [0x09, 0x5e, 0xa7, 0xb3];
// ERC-1155 setApprovalForAll(address,bool) = keccak256("setApprovalForAll(address,bool)")[:4]
const SET_APPROVAL_FOR_ALL_SELECTOR:[u8; 4] = [0xa2, 0x2c, 0xb4, 0x65];

/// Standard "infinite" allowance — `type(uint256).max`.
const U256_MAX_BYTES: [u8; 32] = [0xff; 32];

const CONFIRM_TIMEOUT_SECS: u64 = 60;
const CONFIRM_POLL_INTERVAL_SECS: u64 = 3;

/// A single allowance step the operator might need on v2.
///
/// `pub(crate)` so `deploy_wallet` can run the same v2 approval checklist
/// as part of its setup flow (shared single source of truth for the
/// contract set).
#[derive(Clone)]
pub(crate) struct ApprovalStep {
    /// Human label printed in the plan / progress output.
    pub(crate) label: &'static str,
    /// Contract the Safe sends the tx TO (pUSD for approve, CTF for setApprovalForAll).
    pub(crate) token: &'static str,
    /// Who we're granting the allowance to (Exchange / Adapter).
    pub(crate) spender: &'static str,
    /// `Erc20Approve` = approve(spender, ∞); `Erc1155Set` = setApprovalForAll(spender, true).
    pub(crate) kind: ApprovalKind,
}

#[derive(Clone, Copy)]
pub(crate) enum ApprovalKind {
    /// approve(spender, ∞) on an ERC-20.
    Erc20Approve,
    /// setApprovalForAll(spender, true) on an ERC-1155.
    Erc1155Set,
}

pub(crate) fn v2_approval_steps() -> Vec<ApprovalStep> {
    vec![
        ApprovalStep { label: "pUSD → CTFExchange v2",            token: PUSD_ADDRESS, spender: CTF_EXCHANGE_V2,           kind: ApprovalKind::Erc20Approve },
        ApprovalStep { label: "pUSD → NegRisk CTFExchange v2",    token: PUSD_ADDRESS, spender: NEG_RISK_CTF_EXCHANGE_V2,  kind: ApprovalKind::Erc20Approve },
        ApprovalStep { label: "pUSD → CtfCollateralAdapter",      token: PUSD_ADDRESS, spender: CTF_COLLATERAL_ADAPTER,    kind: ApprovalKind::Erc20Approve },
        ApprovalStep { label: "pUSD → NegRiskCtfCollateralAdpt",  token: PUSD_ADDRESS, spender: NEG_RISK_CTF_COLL_ADAPTER, kind: ApprovalKind::Erc20Approve },
        ApprovalStep { label: "CTF → CTFExchange v2",             token: CTF_CONTRACT, spender: CTF_EXCHANGE_V2,           kind: ApprovalKind::Erc1155Set },
        ApprovalStep { label: "CTF → NegRisk CTFExchange v2",     token: CTF_CONTRACT, spender: NEG_RISK_CTF_EXCHANGE_V2,  kind: ApprovalKind::Erc1155Set },
        // ── ERC-1155 approvals for the CtfCollateralAdapter ──
        // The adapter pulls user outcome tokens via `safeTransferFrom`
        // / `safeBatchTransferFrom` on the CTF contract during
        // `splitPosition` / `mergePositions` / `redeemPositions`.
        // Without `setApprovalForAll(adapter, true)` the redeem
        // relayer fails with `ERC1155: need operator approval for 3rd
        // party transfers` (gas estimation reverts).
        ApprovalStep { label: "CTF → CtfCollateralAdapter",       token: CTF_CONTRACT, spender: CTF_COLLATERAL_ADAPTER,    kind: ApprovalKind::Erc1155Set },
        ApprovalStep { label: "CTF → NegRiskCtfCollateralAdpt",   token: CTF_CONTRACT, spender: NEG_RISK_CTF_COLL_ADAPTER, kind: ApprovalKind::Erc1155Set },
    ]
}

// ════════════════════════════════════════════════════════════════
// Entry point
// ════════════════════════════════════════════════════════════════

pub fn run_approve_v2() -> Result<()> {
    let args: Vec<String> = crate::exchange::polymarket::cli_account::cli_args().collect();
    let dry_run = args.iter().any(|a| a == "--dry-run" || a == "-n");

    // ── POLY_1271: approve the deposit wallet's v2 allowances via a WALLET
    //    batch (the Safe-allowance checklist below doesn't apply). ──
    let sig_type_s = std::env::var("POLY_SIGNATURE_TYPE").unwrap_or_default().to_ascii_lowercase();
    if sig_type_s == "poly_1271" || sig_type_s == "deposit_wallet" {
        let wallet = load_wallet()?;
        let dw = super::deposit_wallet::resolve_deposit_wallet(&wallet.signer_address)?;
        println!("── Deposit-wallet v2 approvals (pUSD→CTF/ExchangeV2/Adapter, CTF→ExchangeV2/Adapter) ──");
        println!("Deposit wallet: {}", dw);
        println!("Signer (EOA):   {}", wallet.signer_address);
        super::deposit_wallet::dw_approvals(
            &wallet.signing_key, &wallet.signer_address, &dw, &wallet.builder_auth, dry_run,
        )?;
        println!("✅ DW approvals batch {}.", if dry_run { "(dry-run)" } else { "confirmed" });
        return Ok(());
    }

    // Gas-payer dispatch — same flag as redeem/split: when `false`
    // (default) we go through Polymarket's gasless relayer, when
    // `true` the signer EOA broadcasts directly and pays MATIC.
    // Honouring the relayer path here matches `examples/approve.ts`
    // in `Polymarket/builder-relayer-client` — the relayer accepts
    // both ERC-20 `approve(...)` and ERC-1155 `setApprovalForAll(...)`
    // calldata; there is no server-side selector whitelist.
    let gas_via_signer = read_gas_via_signer_wallet_flag();

    // Load wallet. For the relayer path we additionally need builder
    // credentials (POLY_BUILDER_*); load_wallet bundles them.
    let wallet = if gas_via_signer {
        // On-chain only — builder creds not required, fall back to a
        // minimal wallet (no auth field).
        let private_key = std::env::var("POLY_PRIVATE_KEY")
            .map_err(|_| super::wallet::no_wallet_creds_err())?;
        let signing_key = {
            let clean = private_key.strip_prefix("0x").unwrap_or(&private_key);
            let bytes = hex::decode(clean).map_err(|e| anyhow!("private key hex: {}", e))?;
            k256::ecdsa::SigningKey::from_bytes(bytes.as_slice().into())
                .map_err(|e| anyhow!("private key bytes: {}", e))?
        };
        let signer_address = to_checksum(&derive_eth_address_from_key(&signing_key));
        let safe_address   = to_checksum(&derive_safe_address(&signer_address));
        ApproveWallet::OnchainOnly { signing_key, signer_address, safe_address }
    } else {
        // Relayer path needs builder auth. `load_wallet` errors if
        // POLY_BUILDER_* are missing — surface that clearly to the
        // operator with a hint to flip `gas_via_signer_wallet`.
        let info = load_wallet().map_err(|e| anyhow!(
            "Failed to load builder credentials for relayer path: {}\n\
             Either add a [builder] section to the secrets file, \
             or set `gas_via_signer_wallet = true` in [general] to use the \
             on-chain path (signer EOA pays MATIC).", e,
        ))?;
        ApproveWallet::Relayer(info)
    };
    let signer_address = wallet.signer_address().to_string();
    let safe_address   = wallet.safe_address().to_string();

    // Plan summary.
    println!("── Safe (funder) ─────────────────────────────────");
    println!("Safe:   {}", safe_address);
    println!("Signer: {}", signer_address);
    println!("Gas:    {}", if gas_via_signer {
        "signer EOA (direct on-chain, MATIC)"
    } else {
        "Polymarket relayer (gasless)"
    });
    println!();
    println!("── Approval checklist (v2 CLOB) ──────────────────");

    let steps = v2_approval_steps();
    let mut plan: Vec<(ApprovalStep, bool /*already_set*/)> = Vec::with_capacity(steps.len());
    for (i, step) in steps.iter().enumerate() {
        let already = match step.kind {
            ApprovalKind::Erc20Approve => check_erc20_allowance(&safe_address, step.token, step.spender),
            ApprovalKind::Erc1155Set   => check_erc1155_approval(&safe_address, step.token, step.spender),
        };
        let marker = if already { "✅ already set" } else { "🔲 NEEDS SET" };
        println!(" {}. {:<36}  {}", i + 1, step.label, marker);
        plan.push((step.clone(), already));
    }
    let missing = plan.iter().filter(|(_, set)| !set).count();
    println!();
    println!("Summary: {} missing / {} total — {} Safe tx{} to send",
        missing, plan.len(), missing, if missing == 1 { "" } else { "s" });

    if missing == 0 {
        println!();
        println!("✅ All v2 approvals already set. No action needed.");
        return Ok(());
    }
    println!();

    if dry_run {
        println!("(dry-run: not broadcasting)");
        return Ok(());
    }

    // Execute each missing approval as a distinct Safe execTransaction.
    // Not MultiSend-batched — keeps the CLI dependency footprint small
    // (one-shot operator flow, not a hot path).
    for (i, (step, already)) in plan.iter().enumerate() {
        if *already { continue; }
        let calldata = match step.kind {
            ApprovalKind::Erc20Approve => build_approve_calldata(step.spender),
            ApprovalKind::Erc1155Set   => build_set_approval_for_all_calldata(step.spender),
        };
        info!("[approve_v2] {}/{}: {} — broadcasting", i + 1, plan.len(), step.label);
        println!("  [{}] {} …", i + 1, step.label);
        wallet.submit_and_confirm(step.token, &calldata)?;
        println!("       ✅ confirmed");
    }

    // Post-flight re-check so the operator sees the final state.
    println!();
    println!("── Post-flight re-check ──────────────────────────");
    for (i, step) in steps.iter().enumerate() {
        let set = match step.kind {
            ApprovalKind::Erc20Approve => check_erc20_allowance(&safe_address, step.token, step.spender),
            ApprovalKind::Erc1155Set   => check_erc1155_approval(&safe_address, step.token, step.spender),
        };
        println!(" {}. {:<36}  {}", i + 1, step.label,
            if set { "✅" } else { "❌ (still missing — inspect above)" });
    }
    println!();
    println!("Safe is ready for v2 CLOB trading + split/merge.");
    Ok(())
}

// ════════════════════════════════════════════════════════════════
// Wallet dispatcher — picks on-chain vs relayer per `gas_via_signer_wallet`
// ════════════════════════════════════════════════════════════════

enum ApproveWallet {
    /// `gas_via_signer_wallet=true`: signer EOA broadcasts directly,
    /// pays MATIC. Builder credentials NOT required.
    OnchainOnly {
        signing_key: k256::ecdsa::SigningKey,
        signer_address: String,
        safe_address: String,
    },
    /// `gas_via_signer_wallet=false` (default): Polymarket gasless
    /// relayer signs + broadcasts. Needs builder API credentials.
    Relayer(super::wallet::WalletInfo),
}

impl ApproveWallet {
    fn signer_address(&self) -> &str {
        match self {
            ApproveWallet::OnchainOnly { signer_address, .. } => signer_address,
            ApproveWallet::Relayer(w) => &w.signer_address,
        }
    }
    fn safe_address(&self) -> &str {
        match self {
            ApproveWallet::OnchainOnly { safe_address, .. } => safe_address,
            ApproveWallet::Relayer(w) => &w.safe_address,
        }
    }

    /// Submit a Safe execTransaction calling `to` with `data`, then poll
    /// until terminal state (CONFIRMED/MINED → success; FAILED → error).
    fn submit_and_confirm(&self, to: &str, data: &str) -> Result<()> {
        match self {
            ApproveWallet::OnchainOnly { signing_key, signer_address, safe_address } => {
                let tx = submit_safe_tx_onchain(
                    signing_key, signer_address, safe_address, to, data,
                )?;
                println!("       tx: {}", tx);
                wait_for_confirm(&tx)
            }
            ApproveWallet::Relayer(w) => {
                let (tx_id, _initial_state) = submit_safe_tx_with_id(
                    &w.builder_auth, &w.signing_key,
                    &w.signer_address, &w.safe_address,
                    to, data,
                    /*gas_via_signer=*/ false,
                )?;
                println!("       relayer tx_id: {}", tx_id);
                // Poll relayer (or chain on the fallback hash form).
                // 30 × 2 s = 60 s window — same as redeem/split.
                let mut final_state = String::new();
                let mut tx_hash = String::new();
                for _ in 0..30 {
                    std::thread::sleep(std::time::Duration::from_secs(CONFIRM_POLL_INTERVAL_SECS));
                    match poll_transaction(&w.builder_auth, &tx_id) {
                        Ok((s, h)) => {
                            final_state = s.clone();
                            tx_hash = h;
                            if s.contains("CONFIRMED") || s.contains("MINED")
                                || s.contains("FAILED") { break; }
                        }
                        Err(e) => {
                            return Err(anyhow!("poll error: {}", e));
                        }
                    }
                }
                let link_hash = if tx_hash.is_empty() { tx_id.clone() } else { tx_hash };
                if final_state.contains("CONFIRMED") || final_state.contains("MINED") {
                    println!("       chain tx: https://polygonscan.com/tx/{}",
                        link_hash.trim_start_matches("0x"));
                    Ok(())
                } else if final_state.contains("FAILED") {
                    Err(anyhow!("relayer tx FAILED (id={})", tx_id))
                } else {
                    Err(anyhow!("relayer tx not confirmed within {}s (state={}, id={})",
                        30 * CONFIRM_POLL_INTERVAL_SECS, final_state, tx_id))
                }
            }
        }
    }
}

// ════════════════════════════════════════════════════════════════
// Calldata builders
// ════════════════════════════════════════════════════════════════

/// `approve(spender, type(uint256).max)`.
pub(crate) fn build_approve_calldata(spender: &str) -> String {
    let mut buf = Vec::with_capacity(4 + 64);
    buf.extend_from_slice(&APPROVE_SELECTOR);
    buf.extend_from_slice(&address_to_bytes32(spender));
    buf.extend_from_slice(&U256_MAX_BYTES);
    format!("0x{}", hex::encode(buf))
}

/// `setApprovalForAll(operator, true)`.
pub(crate) fn build_set_approval_for_all_calldata(operator: &str) -> String {
    let mut buf = Vec::with_capacity(4 + 64);
    buf.extend_from_slice(&SET_APPROVAL_FOR_ALL_SELECTOR);
    buf.extend_from_slice(&address_to_bytes32(operator));
    buf.extend_from_slice(&u256_bytes(1)); // bool true
    format!("0x{}", hex::encode(buf))
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
