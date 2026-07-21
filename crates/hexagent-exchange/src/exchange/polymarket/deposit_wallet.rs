//! CLOB v2 deposit-wallet (POLY_1271 / signature_type=3) infrastructure.
//!
//! CLOB v2 rejects orders from a Gnosis-Safe maker with
//! `"maker address not allowed, please use the deposit wallet flow"`.
//! The fix is to trade from a **deposit wallet** — a per-user ERC-1967
//! proxy with `signatureType=3` (POLY_1271) where `maker == signer ==
//! deposit wallet`, signing via the WALLET relayer batch.
//!
//! This module provides the deposit-wallet primitives consumed by
//! `hexbot deploy_wallet` (resolve-or-deploy the DW + set allowances) and
//! by the live maintenance path (split / redeem / merge / onramp via the
//! relayer `WALLET` batch):
//!   * [`ensure_deposit_wallet`] — find an existing DW (deterministic
//!     CREATE2 derivation verified via `eth_getCode`, with the on-chain
//!     `WalletDeployed` scan as fallback; Gamma `/public-profile` is a
//!     hint only) or, after an interactive confirm, deploy one via the
//!     relayer `WALLET-CREATE`.
//!   * [`dw_approvals`] / [`dw_split`] / [`dw_redeem`] / [`dw_merge`] /
//!     [`dw_onramp`] / [`dw_offramp_withdraw`] / [`dw_transfer_erc20`].

use anyhow::{anyhow, Result};
use k256::ecdsa::SigningKey;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use super::auth::PolyAuth;
use super::deploy_wallet::{
    address_to_bytes32, check_erc1155_approval, check_erc20_allowance, keccak256,
    to_checksum_address, u256_bytes,
};

// ════════════════════════════════════════════════════════════════
// Constants (Polygon mainnet, chain ID 137)
// ════════════════════════════════════════════════════════════════

const RELAYER_URL: &str = "https://relayer-v2.polymarket.com";
const CHAIN_ID: u64 = 137;

/// Deposit-wallet factory — the `to` target of a relayer `WALLET-CREATE`
/// (per docs.polymarket.com/trading/deposit-wallets). ⚠ The flow target
/// per the docs is this CREATE2-style singleton; PolygonScan also labels
/// `0xb6F9C7E68A38c21BeDfD873bC5a378236f7ba987` as "Deposit Wallet
/// Factory". We use the docs' WALLET-CREATE target and treat the
/// `WalletDeployed` event as the source of truth for the address.
const DEPOSIT_WALLET_FACTORY: &str = "0x00000000000Fb5C9ADea0298D729A0CB3823Cc07";
/// PolygonScan-labelled "Deposit Wallet Factory" — the likely emitter of
/// `WalletDeployed`. Both are tried as the `eth_getLogs` address filter
/// when resolving an already-deployed wallet.
const DEPOSIT_WALLET_FACTORY_ALT: &str = "0xb6F9C7E68A38c21BeDfD873bC5a378236f7ba987";
/// UUPS-era deposit-wallet implementation (wallets created before the
/// 2026-05-28 BeaconProxy migration). Official builder-relayer-client
/// `POL.DepositWalletContracts.DepositWalletImplementation` (chain 137).
const DW_UUPS_IMPLEMENTATION: &str = "0x58CA52ebe0DadfdF531Cde7062e76746de4Db1eB";
/// Current `factory.beacon()` (ERC-1967 BeaconProxy era). Used only when
/// the live `beacon()` call fails — derivation prefers the on-chain value
/// so a future beacon rotation keeps deriving NEW wallets correctly.
const DW_BEACON_FALLBACK: &str = "0x7A18EDfe055488A3128f01F563e5B479D92ffc3a";
/// `beacon()` selector on the deposit-wallet factory (official client's
/// `FACTORY_BEACON_SELECTOR`).
const FACTORY_BEACON_SELECTOR: &str = "0x49493a4d";

// L1 ClobAuth — identical struct to the working type-2 path
// (py_clob_client_v2/signing/eip712.py + hexbot deploy_wallet).

// ERC-7739 / Solady `TypedDataSign` wrapper (mirrors the order wrap in
// rs-clob-client-v2 `client.rs::sign_poly1271_order`, but with `ClobAuth`
// as the wrapped `contents`). The wallet "app domain" is the deposit
// wallet itself: name="DepositWallet", version="1", zero salt.

// `WalletDeployed(address indexed wallet, address indexed owner, bytes32 indexed id, address implementation)`
const WALLET_DEPLOYED_TOPIC0_PREIMAGE: &[u8] =
    b"WalletDeployed(address,address,bytes32,address)";


// ════════════════════════════════════════════════════════════════
// #70 test order — one unfunded type-3 order, observe the verdict
// ════════════════════════════════════════════════════════════════


// ════════════════════════════════════════════════════════════════
// DepositWallet WALLET-batch (approvals / split / redeem FROM the DW)
// ════════════════════════════════════════════════════════════════
//
// Every on-chain action BY the deposit wallet goes through a relayer
// `type:"WALLET"` request carrying an EIP-712 `Batch` (domain
// name="DepositWallet" version="1" verifyingContract=DW) signed by the
// owner EOA, with `calls: Call[]`. Reference: clob-client-v2
// examples/account/approveDepositWalletAllowances.ts. v2 mainnet (137)
// contracts: collateral=pUSD, conditionalTokens(CTF)=0x4D97…, the DW
// approves pUSD→CTF directly (NOT the Safe's CtfCollateralAdapter), so
// split goes splitPosition(pUSD, …) on the CTF.

const PUSD_TOKEN: &str = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB";
const CTF_TOKEN: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
const EXCHANGE_V2: &str = "0xE111180000d2663C0091e4f400237545B87B996B";
/// v2 CtfCollateralAdapter — wraps pUSD ⇄ USDC.e for split/merge/redeem so the
/// minted positionIds are USDC.e-space (= the CLOB's tradeable clob_token_id).
/// Splitting pUSD directly on the CTF mints pUSD-space tokens the CLOB can't
/// sell (`balance: 0`). See `hexbot token_check`.
const CTF_COLLATERAL_ADAPTER: &str = "0xAdA100Db00Ca00073811820692005400218FcE1f";
const USDC_TOKEN: &str = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"; // native USDC (6dp)
const USDCE_TOKEN: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"; // USDC.e (6dp)
/// Official AutoRedeemer (proxy; docs.polymarket.com/resources/contracts).
/// Granting it `setApprovalForAll` on the CTF is the on-chain opt-in for
/// Polymarket's auto-redeem: its keeper calls `redeemBinary(froms,
/// conditionIds)` and the payout goes to the position owner. (The UI
/// "Auto redeem your wins" toggle grants this same approval.)
const AUTO_REDEEMER: &str = "0xa1200000d0002264C9a1698e001292D00E1b00af";
const ONRAMP: &str = "0x93070a847efEf7F70739046A929D47a521F5B8ee"; // USDC/USDC.e → pUSD
const OFFRAMP: &str = "0x2957922Eb93258b93368531d39fAcCA3B4dC5854"; // pUSD → USDC/USDC.e
const WRAP_SELECTOR: [u8; 4] = [0x62, 0x35, 0x56, 0x38]; // wrap(address,address,uint256)
const UNWRAP_SELECTOR: [u8; 4] = [0x8c, 0xc7, 0x10, 0x4f]; // unwrap(address,address,uint256)
const APPROVE_SELECTOR: [u8; 4] = [0x09, 0x5e, 0xa7, 0xb3]; // approve(address,uint256)
const SET_APPROVAL_FOR_ALL_SELECTOR: [u8; 4] = [0xa2, 0x2c, 0xb4, 0x65]; // setApprovalForAll(address,bool)
const U256_MAX_HEX: &str = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
// Relayer requires the batch deadline within a window ending at now+300s;
// 240 was rejected "deadline too soon", so sit near the max (leaves ~10s
// headroom under 300 for clock skew + request latency).
const BATCH_DEADLINE_SECS: u64 = 290;

struct Call {
    target: String,
    data: String, // 0x-hex calldata; value is always 0
}

static WALLET_SUBMIT_LOCKS: OnceLock<Mutex<HashMap<String, Arc<Mutex<()>>>>> = OnceLock::new();
static PENDING_WALLET_ACTIONS: OnceLock<Mutex<HashMap<String, PendingWalletAction>>> =
    OnceLock::new();
// The relayer ultimately broadcasts WALLET maintenance actions through its own
// Polygon transaction pipeline. Keep cross-account maintenance bursts out of
// that pipeline: after one action is accepted, no other maintenance signer may
// submit until the accepted action has left STATE_NEW. Per-signer locks below
// still protect each wallet's action nonce for the remainder of its lifecycle.
static WALLET_NEW_STATE_SUBMIT_GATE: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(Clone, Debug)]
struct PendingWalletAction {
    transaction_id: String,
    nonce: u128,
}

fn wallet_submit_lock(signer: &str) -> Result<Arc<Mutex<()>>> {
    let locks = WALLET_SUBMIT_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = locks
        .lock()
        .map_err(|_| anyhow!("WALLET submit lock registry poisoned"))?;
    Ok(locks
        .entry(signer.to_ascii_lowercase())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone())
}

fn pending_wallet_action(signer: &str) -> Result<Option<PendingWalletAction>> {
    let actions = PENDING_WALLET_ACTIONS.get_or_init(|| Mutex::new(HashMap::new()));
    let actions = actions
        .lock()
        .map_err(|_| anyhow!("pending WALLET action registry poisoned"))?;
    Ok(actions.get(&signer.to_ascii_lowercase()).cloned())
}

fn remember_wallet_action(signer: &str, transaction_id: String, nonce: u128) -> Result<()> {
    let actions = PENDING_WALLET_ACTIONS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut actions = actions
        .lock()
        .map_err(|_| anyhow!("pending WALLET action registry poisoned"))?;
    actions.insert(
        signer.to_ascii_lowercase(),
        PendingWalletAction {
            transaction_id,
            nonce,
        },
    );
    Ok(())
}

fn forget_wallet_action(signer: &str) -> Result<()> {
    let actions = PENDING_WALLET_ACTIONS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut actions = actions
        .lock()
        .map_err(|_| anyhow!("pending WALLET action registry poisoned"))?;
    actions.remove(&signer.to_ascii_lowercase());
    Ok(())
}

fn deposit_wallet_domain_separator(dw: &str) -> [u8; 32] {
    keccak256(&abi_encode_words(&[
        keccak256(b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
        keccak256(b"DepositWallet"),
        keccak256(b"1"),
        u256_bytes(CHAIN_ID as u128),
        address_to_bytes32(dw),
    ]))
}

fn parse_nonce_value(value: &serde_json::Value) -> Option<u128> {
    if let Some(nonce) = value.as_str() {
        return nonce.parse::<u128>().ok();
    }
    value.as_u64().map(u128::from)
}

fn parse_wallet_nonce(json: &serde_json::Value) -> Result<u128> {
    let value = json
        .get("nonce")
        .ok_or_else(|| anyhow!("relayer WALLET nonce response has no nonce: {}", json))?;
    parse_nonce_value(value)
        .ok_or_else(|| anyhow!("invalid relayer WALLET nonce value: {}", value))
}

/// Fetch the relayer-owned WALLET nonce for the signer EOA. This is
/// intentionally not the deposit-wallet contract's on-chain `nonce()`:
/// the relayer can reserve an action before that on-chain nonce advances.
fn relayer_wallet_nonce(builder_auth: &PolyAuth, signer: &str) -> Result<u128> {
    let path = format!("/nonce?address={}&type=WALLET", signer);
    let headers = builder_auth.sign_request("GET", &path, "");
    let json = relayer_get(format!("{}{}", RELAYER_URL, path), headers)?;
    parse_wallet_nonce(&json)
}

fn build_wallet_batch_body(
    key: &SigningKey,
    eoa: &str,
    dw: &str,
    calls: &[Call],
    nonce: u128,
    deadline: u64,
) -> Result<String> {
    let call_typehash = keccak256(b"Call(address target,uint256 value,bytes data)");
    let mut calls_concat = Vec::with_capacity(calls.len() * 32);
    let mut calls_json = Vec::with_capacity(calls.len());
    for c in calls {
        let data_bytes = hex::decode(c.data.strip_prefix("0x").unwrap_or(&c.data)).unwrap_or_default();
        let call_hash = keccak256(&abi_encode_words(&[
            call_typehash,
            address_to_bytes32(&c.target),
            [0u8; 32], // value = 0
            keccak256(&data_bytes),
        ]));
        calls_concat.extend_from_slice(&call_hash);
        calls_json.push(serde_json::json!({"target": c.target, "value": "0", "data": c.data}));
    }
    let calls_hash = keccak256(&calls_concat);

    let batch_typehash = keccak256(
        b"Batch(address wallet,uint256 nonce,uint256 deadline,Call[] calls)Call(address target,uint256 value,bytes data)",
    );
    let batch_struct = keccak256(&abi_encode_words(&[
        batch_typehash,
        address_to_bytes32(dw),
        u256_bytes(nonce),
        u256_bytes(deadline as u128),
        calls_hash,
    ]));
    let digest = eip712_digest(&deposit_wallet_domain_separator(dw), &batch_struct);
    let (sig, recid) = key.sign_prehash_recoverable(&digest).map_err(|e| anyhow!("sign: {}", e))?;
    let mut sb = [0u8; 65];
    sb[..64].copy_from_slice(&sig.to_bytes());
    sb[64] = recid.to_byte() + 27;
    let signature = format!("0x{}", hex::encode(sb));

    Ok(serde_json::json!({
        "type": "WALLET",
        "from": eoa,
        "to": DEPOSIT_WALLET_FACTORY,
        "nonce": nonce.to_string(),
        "signature": signature,
        "depositWalletParams": {
            "depositWallet": dw,
            "deadline": deadline.to_string(),
            "calls": calls_json,
        }
    })
    .to_string())
}

fn is_terminal_wallet_state(state: &str) -> bool {
    matches!(
        state,
        "STATE_CONFIRMED" | "STATE_FAILED" | "STATE_INVALID"
    )
}

fn wallet_action_blocks_next_submit(state: &str) -> bool {
    state.is_empty() || state == "STATE_NEW"
}

/// Hold the cross-account submit gate until the relayer has indexed and begun
/// processing this action. There is deliberately no timeout: releasing while
/// the action is still STATE_NEW would recreate the burst that this gate is
/// intended to prevent. A terminal relayer error also means the action has
/// left STATE_NEW; the normal poll below will surface its full error.
fn wait_wallet_action_leaves_new(builder_auth: &PolyAuth, tx_id: &str) {
    loop {
        match super::wallet::poll_transaction(builder_auth, tx_id) {
            Ok((state, _hash)) if wallet_action_blocks_next_submit(&state) => {}
            Ok((state, _hash)) => {
                println!(
                    "   WALLET batch left STATE_NEW (txID={} state={}) — releasing global submit gate",
                    tx_id, state,
                );
                return;
            }
            Err(e) if e.to_string().contains("Transaction failed") => {
                println!(
                    "   WALLET batch left STATE_NEW with terminal failure (txID={}) — releasing global submit gate",
                    tx_id,
                );
                return;
            }
            Err(e) => {
                println!(
                    "   WALLET STATE_NEW gate poll error for txID={} (retrying): {}",
                    tx_id, e,
                );
            }
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

fn action_matches_signer_nonce(action: &serde_json::Value, signer: &str, nonce: u128) -> bool {
    let from = action
        .get("from")
        .or_else(|| action.get("owner"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let action_nonce = action.get("nonce").and_then(parse_nonce_value);
    from.eq_ignore_ascii_case(signer) && action_nonce == Some(nonce)
}

/// Refresh a known action by its transaction ID. Returns `false` only when
/// `/transaction` has no record yet, allowing the caller to fall back to the
/// signer+nonce scan of `/transactions`.
fn reconcile_wallet_transaction_id(
    builder_auth: &PolyAuth,
    signer: &str,
    transaction_id: &str,
    nonce: u128,
) -> Result<bool> {
    match super::wallet::poll_transaction(builder_auth, transaction_id) {
        Ok((state, _hash)) if state.is_empty() => Ok(false),
        Ok((state, hash)) if is_terminal_wallet_state(&state) => {
            println!(
                "   previous WALLET action resolved: txID={} nonce={} state={} tx={}",
                transaction_id, nonce, state, hash
            );
            forget_wallet_action(signer)?;
            Ok(true)
        }
        Ok((state, hash)) => Err(anyhow!(
            "previous WALLET action is still active: txID={} nonce={} state={} tx={}; refusing a new submission",
            transaction_id,
            nonce,
            state,
            hash,
        )),
        Err(e) if e.to_string().contains("Transaction failed") => {
            println!(
                "   previous WALLET action failed: txID={} nonce={} ({})",
                transaction_id, nonce, e
            );
            forget_wallet_action(signer)?;
            Ok(true)
        }
        Err(e) => Err(anyhow!(
            "could not refresh previous WALLET action txID={} nonce={}: {}; refusing a new submission",
            transaction_id,
            nonce,
            e,
        )),
    }
}

/// Reconcile an old action before considering a new submission. A locally
/// remembered transaction ID is authoritative and queried first. If it is
/// unavailable (including after a restart), `/transactions` is filtered by
/// the exact `(from=signer, nonce)` pair. A still-active match is a hard stop.
fn reconcile_previous_wallet_action(
    builder_auth: &PolyAuth,
    signer: &str,
    fallback_nonce: u128,
) -> Result<()> {
    let remembered = pending_wallet_action(signer)?;
    let lookup_nonce = remembered
        .as_ref()
        .map(|action| action.nonce)
        .unwrap_or(fallback_nonce);

    if let Some(action) = remembered {
        if reconcile_wallet_transaction_id(
            builder_auth,
            signer,
            &action.transaction_id,
            action.nonce,
        )? {
            return Ok(());
        }
    }

    let path = "/transactions";
    let headers = builder_auth.sign_request("GET", path, "");
    let json = relayer_get(format!("{}{}", RELAYER_URL, path), headers)?;
    let actions = json
        .as_array()
        .ok_or_else(|| anyhow!("relayer /transactions returned non-array: {}", json))?;
    for action in actions {
        if !action_matches_signer_nonce(action, signer, lookup_nonce) {
            continue;
        }
        let listed_state = action.get("state").and_then(|v| v.as_str()).unwrap_or("");
        let tx_id = action
            .get("transactionID")
            .and_then(|v| v.as_str());
        if let Some(tx_id) = tx_id {
            if reconcile_wallet_transaction_id(
                builder_auth,
                signer,
                tx_id,
                lookup_nonce,
            )? {
                continue;
            }
        }
        if is_terminal_wallet_state(listed_state) {
            forget_wallet_action(signer)?;
            continue;
        }
        return Err(anyhow!(
            "previous relayer action matched by signer+nonce and is still active: txID={} nonce={} state={}; refusing a new submission",
            tx_id.unwrap_or("UNKNOWN"),
            lookup_nonce,
            if listed_state.is_empty() { "UNKNOWN" } else { listed_state },
        ));
    }
    Ok(())
}

fn wallet_busy_error(
    builder_auth: &PolyAuth,
    signer: &str,
    nonce: u128,
    submit_error: anyhow::Error,
) -> anyhow::Error {
    match reconcile_previous_wallet_action(builder_auth, signer, nonce) {
        Ok(()) => anyhow!(
            "{}; queried prior WALLET action by transaction_id, then signer={} nonce={} fallback, but none remained active; not resubmitting automatically",
            submit_error,
            signer,
            nonce,
        ),
        Err(status) => anyhow!("{}; prior WALLET action status: {}", submit_error, status),
    }
}

/// Sign + submit a relayer `type:"WALLET"` batch. Returns the tx id.
fn submit_wallet_batch(
    key: &SigningKey,
    eoa: &str,
    dw: &str,
    builder_auth: &PolyAuth,
    calls: &[Call],
    gate_maintenance_until_started: bool,
    dry_run: bool,
) -> Result<String> {
    // Serialize only this signer. Different accounts remain concurrent, while
    // two strategies sharing one signer cannot race on the same fresh nonce.
    let submit_lock = wallet_submit_lock(eoa)?;
    let _submit_guard = submit_lock
        .lock()
        .map_err(|_| anyhow!("WALLET submit lock poisoned for {}", eoa))?;

    if !dry_run {
        // This nonce is only an exact fallback key for reconciliation. The
        // actual submission fetches again below after old-action status is
        // known, so its deadline/digest/signature use a post-reconcile nonce.
        let reconciliation_nonce = relayer_wallet_nonce(builder_auth, eoa)?;
        reconcile_previous_wallet_action(builder_auth, eoa, reconciliation_nonce)?;
    }

    // For maintenance calls, serialize only the acceptance phase across every
    // signer. Once this action leaves STATE_NEW the guard is dropped and its
    // confirmation poll continues concurrently with the next account's submit.
    let global_submit_gate = WALLET_NEW_STATE_SUBMIT_GATE.get_or_init(|| Mutex::new(()));
    let global_submit_guard = if dry_run || !gate_maintenance_until_started {
        None
    } else {
        Some(
            global_submit_gate
                .lock()
                .map_err(|_| anyhow!("global WALLET STATE_NEW submit gate poisoned"))?,
        )
    };

    // The relayer's wallet registry can lag WALLET-CREATE by a few seconds
    // even after the create tx polls STATE_CONFIRMED (observed 2026-07-14:
    // the first batch for a fresh deposit wallet 400'd "wallet … is not
    // registered"). That rejection is transient — retry it on a fixed 5s
    // backoff for up to ~90s. Every rejected attempt fetches a fresh relayer
    // WALLET nonce and rebuilds deadline, digest, and batch signature.
    // Any ambiguous error (especially wallet-busy) is terminal until the old
    // action's state has been queried; it is never retried with only a new nonce.
    const REGISTRY_RETRIES: u32 = 18;
    let mut attempt = 0u32;
    let (json, submitted_nonce) = loop {
        let nonce = relayer_wallet_nonce(builder_auth, eoa)?;
        let deadline = now_secs()? + BATCH_DEADLINE_SECS;
        let body_str = build_wallet_batch_body(key, eoa, dw, calls, nonce, deadline)?;
        if dry_run {
            println!("   (dry-run) nonce={} deadline={} batch={}", nonce, deadline, body_str);
            return Ok(String::new());
        }
        let headers = builder_auth.sign_request("POST", "/submit", &body_str);
        match relayer_post(format!("{}/submit", RELAYER_URL), headers, body_str.clone()) {
            Ok(json) => break (json, nonce),
            Err(e) if attempt < REGISTRY_RETRIES && e.to_string().contains("is not registered") => {
                attempt += 1;
                println!(
                    "   relayer wallet registry not ready yet — retry {}/{} in 5s …",
                    attempt, REGISTRY_RETRIES
                );
                std::thread::sleep(std::time::Duration::from_secs(5));
            }
            Err(e) if e.to_string().contains("wallet busy") => {
                return Err(wallet_busy_error(builder_auth, eoa, nonce, e));
            }
            Err(e) => return Err(e),
        }
    };
    let tx_id = json
        .get("transactionID")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("WALLET batch returned no transactionID: {}", json))?
        .to_string();
    if let Err(e) = remember_wallet_action(eoa, tx_id.clone(), submitted_nonce) {
        log::warn!(
            "could not remember submitted WALLET action txID={} nonce={}: {}",
            tx_id,
            submitted_nonce,
            e
        );
    }
    println!("   WALLET batch submitted (txID={}) — polling …", tx_id);
    if global_submit_guard.is_some() {
        wait_wallet_action_leaves_new(builder_auth, &tx_id);
    }
    drop(global_submit_guard);
    let tx_hash = poll_relayer_tx(builder_auth, &tx_id)?;
    if let Err(e) = forget_wallet_action(eoa) {
        log::warn!("could not clear confirmed WALLET action txID={}: {}", tx_id, e);
    }
    println!("   confirmed (tx=0x{})", tx_hash.trim_start_matches("0x"));
    Ok(tx_id)
}

fn approve_calldata(spender: &str) -> String {
    let mut d = Vec::with_capacity(4 + 64);
    d.extend_from_slice(&APPROVE_SELECTOR);
    d.extend_from_slice(&address_to_bytes32(spender));
    d.extend_from_slice(&hex::decode(U256_MAX_HEX).unwrap());
    format!("0x{}", hex::encode(d))
}

fn set_approval_for_all_calldata(operator: &str) -> String {
    let mut d = Vec::with_capacity(4 + 64);
    d.extend_from_slice(&SET_APPROVAL_FOR_ALL_SELECTOR);
    d.extend_from_slice(&address_to_bytes32(operator));
    d.extend_from_slice(&u256_bytes(1)); // true
    format!("0x{}", hex::encode(d))
}

/// `splitPosition(collateral, 0, conditionId, [1,2], amount)` — same ABI
/// layout as the Safe path (`wallet.rs::run_split_one`), but the DW calls
/// the CTF directly with pUSD as `collateralToken`.
fn split_position_calldata(collateral: &str, condition_id: &str, amount_wei: u128) -> String {
    let selector = &keccak256(b"splitPosition(address,bytes32,bytes32,uint256[],uint256)")[..4];
    let cid_bytes = hex::decode(condition_id.strip_prefix("0x").unwrap_or(condition_id)).unwrap_or_default();
    let mut cid = [0u8; 32];
    let start = 32 - cid_bytes.len().min(32);
    cid[start..].copy_from_slice(&cid_bytes[..cid_bytes.len().min(32)]);

    let mut d = Vec::with_capacity(4 + 32 * 8);
    d.extend_from_slice(selector);
    d.extend_from_slice(&address_to_bytes32(collateral)); // collateralToken
    d.extend_from_slice(&[0u8; 32]); // parentCollectionId
    d.extend_from_slice(&cid); // conditionId
    d.extend_from_slice(&u256_bytes(160)); // offset to partition (5 head slots)
    d.extend_from_slice(&u256_bytes(amount_wei)); // amount
    d.extend_from_slice(&u256_bytes(2)); // partition.length
    d.extend_from_slice(&u256_bytes(1)); // partition[0] = 1 (Up)
    d.extend_from_slice(&u256_bytes(2)); // partition[1] = 2 (Down)
    format!("0x{}", hex::encode(d))
}

/// `Onramp.wrap(asset, to, amount)` — deposits a supported backing asset
/// (USDC or USDC.e) and mints pUSD to `to`. Same ABI as
/// `migrate_usdc::build_wrap_calldata`.
fn onramp_wrap_calldata(asset: &str, to: &str, amount_wei: u128) -> String {
    let mut d = Vec::with_capacity(4 + 96);
    d.extend_from_slice(&WRAP_SELECTOR);
    d.extend_from_slice(&address_to_bytes32(asset));
    d.extend_from_slice(&address_to_bytes32(to));
    d.extend_from_slice(&u256_bytes(amount_wei));
    format!("0x{}", hex::encode(d))
}

/// `Offramp.unwrap(asset, to, amount)` — burns the caller's pUSD, sends
/// `asset` (USDC.e) to `to`. Inverse of `onramp_wrap_calldata`; same ABI as
/// `wallet.rs::build_unwrap_calldata` (Safe path).
fn offramp_unwrap_calldata(asset: &str, to: &str, amount_wei: u128) -> String {
    let mut d = Vec::with_capacity(4 + 96);
    d.extend_from_slice(&UNWRAP_SELECTOR);
    d.extend_from_slice(&address_to_bytes32(asset));
    d.extend_from_slice(&address_to_bytes32(to));
    d.extend_from_slice(&u256_bytes(amount_wei));
    format!("0x{}", hex::encode(d))
}

fn now_secs() -> Result<u64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| anyhow!("clock: {}", e))?
        .as_secs())
}

/// `redeemPositions(collateral, 0, conditionId, [1,2])` calldata.
fn redeem_calldata(collateral: &str, condition_id: &str) -> String {
    let selector = &keccak256(b"redeemPositions(address,bytes32,bytes32,uint256[])")[..4];
    let cid_bytes = hex::decode(condition_id.strip_prefix("0x").unwrap_or(condition_id)).unwrap_or_default();
    let mut cid = [0u8; 32];
    let start = 32 - cid_bytes.len().min(32);
    cid[start..].copy_from_slice(&cid_bytes[..cid_bytes.len().min(32)]);

    let mut d = Vec::with_capacity(4 + 32 * 7);
    d.extend_from_slice(selector);
    d.extend_from_slice(&address_to_bytes32(collateral)); // collateralToken
    d.extend_from_slice(&[0u8; 32]); // parentCollectionId
    d.extend_from_slice(&cid); // conditionId
    d.extend_from_slice(&u256_bytes(128)); // offset to indexSets (4 head slots)
    d.extend_from_slice(&u256_bytes(2)); // indexSets.length
    d.extend_from_slice(&u256_bytes(1)); // indexSets[0] = 1 (Up)
    d.extend_from_slice(&u256_bytes(2)); // indexSets[1] = 2 (Down)
    format!("0x{}", hex::encode(d))
}

// ── Maintenance hooks (called from wallet.rs for POLY_1271 accounts) ──

/// Split `amount_wei` pUSD → Up+Down shares FROM the deposit wallet, via a
/// `splitPosition` on the CTF in a relayer WALLET batch. Blocks until the
/// tx confirms; returns the tx id (Err on submit/confirm failure).
pub(crate) fn dw_split(
    key: &SigningKey,
    eoa: &str,
    dw: &str,
    builder_auth: &PolyAuth,
    condition_id: &str,
    amount_wei: u128,
) -> Result<String> {
    // Split VIA the CtfCollateralAdapter (not the CTF directly): the adapter
    // pulls pUSD, unwraps to USDC.e, and mints USDC.e-space outcome tokens — the
    // ones the CLOB actually trades (clob_token_id). Splitting pUSD directly on
    // the CTF minted pUSD-space tokens the CLOB can't sell (`balance: 0`).
    // Verified via `--test-split --via-adapter --collateral pusd` + token_check.
    let calls = vec![Call {
        target: CTF_COLLATERAL_ADAPTER.to_string(),
        data: split_position_calldata(PUSD_TOKEN, condition_id, amount_wei),
    }];
    submit_wallet_batch(
        key,
        eoa,
        dw,
        builder_auth,
        &calls,
        /*gate_maintenance_until_started=*/ true,
        /*dry_run=*/ false,
    )
}

/// Redeem a matured condition FROM the deposit wallet, via `redeemPositions`
/// on the CtfCollateralAdapter in a relayer WALLET batch. The adapter burns the
/// DW's USDC.e-space outcome tokens (minted by the adapter split) and pays the
/// proceeds back as pUSD. Requires `setApprovalForAll(CTF→adapter)`.
pub(crate) fn dw_redeem(
    key: &SigningKey,
    eoa: &str,
    dw: &str,
    builder_auth: &PolyAuth,
    condition_id: &str,
) -> Result<String> {
    let calls = vec![Call {
        target: CTF_COLLATERAL_ADAPTER.to_string(),
        data: redeem_calldata(PUSD_TOKEN, condition_id),
    }];
    submit_wallet_batch(
        key,
        eoa,
        dw,
        builder_auth,
        &calls,
        /*gate_maintenance_until_started=*/ true,
        /*dry_run=*/ false,
    )
}

/// `mergePositions(collateral, 0, conditionId, [1,2], amount)` — burns
/// `amount` Up + `amount` Down back into `amount` pUSD in the DW.
fn merge_position_calldata(collateral: &str, condition_id: &str, amount_wei: u128) -> String {
    let selector = &keccak256(b"mergePositions(address,bytes32,bytes32,uint256[],uint256)")[..4];
    let cid_bytes = hex::decode(condition_id.strip_prefix("0x").unwrap_or(condition_id)).unwrap_or_default();
    let mut cid = [0u8; 32];
    let start = 32 - cid_bytes.len().min(32);
    cid[start..].copy_from_slice(&cid_bytes[..cid_bytes.len().min(32)]);
    let mut d = Vec::with_capacity(4 + 32 * 8);
    d.extend_from_slice(selector);
    d.extend_from_slice(&address_to_bytes32(collateral));
    d.extend_from_slice(&[0u8; 32]); // parentCollectionId
    d.extend_from_slice(&cid);
    d.extend_from_slice(&u256_bytes(160)); // offset to partition
    d.extend_from_slice(&u256_bytes(amount_wei));
    d.extend_from_slice(&u256_bytes(2)); // partition.length
    d.extend_from_slice(&u256_bytes(1));
    d.extend_from_slice(&u256_bytes(2));
    format!("0x{}", hex::encode(d))
}

/// `transfer(to, amount)` ERC-20 calldata.
fn erc20_transfer_calldata(to: &str, amount_wei: u128) -> String {
    const TRANSFER_SELECTOR: [u8; 4] = [0xa9, 0x05, 0x9c, 0xbb]; // transfer(address,uint256)
    let mut d = Vec::with_capacity(4 + 64);
    d.extend_from_slice(&TRANSFER_SELECTOR);
    d.extend_from_slice(&address_to_bytes32(to));
    d.extend_from_slice(&u256_bytes(amount_wei));
    format!("0x{}", hex::encode(d))
}

/// Merge `amount_wei` Up+Down → pUSD FROM the deposit wallet (WALLET batch),
/// via the CtfCollateralAdapter so it burns the DW's USDC.e-space outcome tokens
/// (the ones the adapter split minted) and returns pUSD. Requires
/// `setApprovalForAll(CTF→adapter)`.
pub(crate) fn dw_merge(
    key: &SigningKey, eoa: &str, dw: &str, builder_auth: &PolyAuth,
    condition_id: &str, amount_wei: u128, dry_run: bool,
) -> Result<String> {
    let calls = vec![Call {
        target: CTF_COLLATERAL_ADAPTER.to_string(),
        data: merge_position_calldata(PUSD_TOKEN, condition_id, amount_wei),
    }];
    submit_wallet_batch(key, eoa, dw, builder_auth, &calls, false, dry_run)
}

/// Set the DW's v2 allowances in one WALLET batch. Each allowance already
/// on-chain is skipped (checked via eth_call, owner = the DW); if all six
/// are set, no batch is submitted at all. Allowances:
///   - pUSD → CTF            (split/merge pUSD-direct on the CTF — legacy path)
///   - pUSD → ExchangeV2     (pay pUSD for BUY orders)
///   - pUSD → CtfCollateralAdapter (split/merge via the adapter → USDC.e-space
///                            tokens, the ones the CLOB actually trades/sells)
///   - CTF  → ExchangeV2     (setApprovalForAll: let the exchange move the DW's
///                            outcome tokens for SELL orders)
///   - CTF  → CtfCollateralAdapter (setApprovalForAll: adapter merge/redeem
///                            burns the DW's outcome tokens)
///   - CTF  → AutoRedeemer   (setApprovalForAll: opt-in to Polymarket's
///                            auto-redeem keeper for resolved positions)
pub(crate) fn dw_approvals(
    key: &SigningKey, eoa: &str, dw: &str, builder_auth: &PolyAuth, dry_run: bool,
) -> Result<String> {
    // Idempotence: check each allowance on-chain (owner = the DW) and only
    // batch the ones still missing; skip the relayer round-trip entirely
    // when everything is already set. An RPC failure reads as "not
    // approved" — re-approving is a harmless idempotent ∞-approve.
    struct Step {
        label: &'static str,
        target: &'static str,
        spender: &'static str,
        erc1155: bool,
    }
    let steps = [
        Step { label: "pUSD → CTF",                  target: PUSD_TOKEN, spender: CTF_TOKEN,              erc1155: false },
        Step { label: "pUSD → ExchangeV2",           target: PUSD_TOKEN, spender: EXCHANGE_V2,            erc1155: false },
        Step { label: "pUSD → CtfCollateralAdapter", target: PUSD_TOKEN, spender: CTF_COLLATERAL_ADAPTER, erc1155: false },
        Step { label: "CTF → ExchangeV2",            target: CTF_TOKEN,  spender: EXCHANGE_V2,            erc1155: true },
        Step { label: "CTF → CtfCollateralAdapter",  target: CTF_TOKEN,  spender: CTF_COLLATERAL_ADAPTER, erc1155: true },
        Step { label: "CTF → AutoRedeemer",          target: CTF_TOKEN,  spender: AUTO_REDEEMER,          erc1155: true },
    ];
    let mut calls = Vec::new();
    for (i, s) in steps.iter().enumerate() {
        let already = if s.erc1155 {
            check_erc1155_approval(dw, s.target, s.spender)
        } else {
            check_erc20_allowance(dw, s.target, s.spender)
        };
        if already {
            println!("  {}/{} {:<28} already approved — skipping.", i + 1, steps.len(), s.label);
        } else {
            println!("  {}/{} {:<28} needs approval.", i + 1, steps.len(), s.label);
            let data = if s.erc1155 {
                set_approval_for_all_calldata(s.spender)
            } else {
                approve_calldata(s.spender)
            };
            calls.push(Call { target: s.target.to_string(), data });
        }
    }
    if calls.is_empty() {
        println!("  All DW allowances already set — skipping WALLET batch.");
        return Ok(String::new());
    }
    submit_wallet_batch(key, eoa, dw, builder_auth, &calls, false, dry_run)
}

/// Wrap `amount_wei` of a supported backing `asset` (USDC or USDC.e) into
/// pUSD (approve Onramp + wrap) in one WALLET batch.
pub(crate) fn dw_onramp(
    key: &SigningKey, eoa: &str, dw: &str, builder_auth: &PolyAuth,
    asset: &str, amount_wei: u128, dry_run: bool,
) -> Result<String> {
    debug_assert!(asset.eq_ignore_ascii_case(USDC_TOKEN) || asset.eq_ignore_ascii_case(USDCE_TOKEN));
    let calls = vec![
        Call { target: asset.to_string(), data: approve_calldata(ONRAMP) },
        Call { target: ONRAMP.to_string(), data: onramp_wrap_calldata(asset, dw, amount_wei) },
    ];
    submit_wallet_batch(key, eoa, dw, builder_auth, &calls, false, dry_run)
}

/// Withdraw the DW's pUSD as a supported backing `asset` (USDC or USDC.e)
/// in one WALLET batch: approve pUSD→Offramp, unwrap into the DW, then
/// transfer the underlying to `recipient`. All supported assets are 1:1
/// with pUSD and use 6 decimals. The approve is unconditional and idempotent.
pub(crate) fn dw_offramp_withdraw(
    key: &SigningKey, eoa: &str, dw: &str, builder_auth: &PolyAuth,
    asset: &str, recipient: &str, amount_wei: u128, dry_run: bool,
) -> Result<String> {
    debug_assert!(asset.eq_ignore_ascii_case(USDC_TOKEN) || asset.eq_ignore_ascii_case(USDCE_TOKEN));
    let calls = vec![
        Call { target: PUSD_TOKEN.to_string(), data: approve_calldata(OFFRAMP) },
        Call { target: OFFRAMP.to_string(), data: offramp_unwrap_calldata(asset, dw, amount_wei) },
        Call { target: asset.to_string(), data: erc20_transfer_calldata(recipient, amount_wei) },
    ];
    submit_wallet_batch(key, eoa, dw, builder_auth, &calls, false, dry_run)
}

/// Transfer `amount_wei` of an ERC-20 (`token`) FROM the DW to `to`
/// (WALLET batch). Used by `withdraw` for pUSD/USDC.e.
pub(crate) fn dw_transfer_erc20(
    key: &SigningKey, eoa: &str, dw: &str, builder_auth: &PolyAuth,
    token: &str, to: &str, amount_wei: u128, dry_run: bool,
) -> Result<String> {
    let calls = vec![Call { target: token.to_string(), data: erc20_transfer_calldata(to, amount_wei) }];
    submit_wallet_batch(key, eoa, dw, builder_auth, &calls, false, dry_run)
}

/// Resolve the deposit-wallet address for `eoa`: prefer the configured
/// `POLY_FUNDER`, else find it on-chain (derivation + WalletDeployed scan).
pub(crate) fn resolve_deposit_wallet(eoa: &str) -> Result<String> {
    let env = std::env::var("POLY_FUNDER").unwrap_or_default();
    if !env.trim().is_empty() {
        return Ok(to_checksum_address(env.trim()));
    }
    find_existing_deposit_wallet(eoa)?.ok_or_else(|| {
        anyhow!("no deposit wallet exists for EOA {} — run `hexbot deploy_wallet` first", eoa)
    })
}

/// Resolve the deposit wallet for `eoa`, deploying it (relayer
/// `WALLET-CREATE`) if it doesn't exist yet. Used by `deploy_wallet`.
pub(crate) fn ensure_deposit_wallet(builder_auth: &PolyAuth, eoa: &str) -> Result<String> {
    // ── Existence pre-check: skip WALLET-CREATE if one already exists ──
    // Authoritative signals are on-chain only: deterministic CREATE2
    // derivation verified via eth_getCode, with the `WalletDeployed` scan
    // as fallback (see `find_existing_deposit_wallet`). The Polymarket
    // Gamma `/public-profile` API is NOT an existence signal: its
    // `proxyWallet` is the account's WEBSITE proxy (a Gnosis Safe or
    // magic-link proxy), never a deposit wallet — treating it as one
    // mis-routed the WALLET batch to a Safe and the relayer 400'd with
    // "wallet … is not registered". Gamma is kept below purely as an
    // operator hint.
    match find_existing_deposit_wallet(eoa) {
        Ok(Some(dw)) => {
            println!("  Existing deposit wallet found on-chain: {}", dw);
            println!("  → already exists; skipping WALLET-CREATE.");
            return Ok(dw);
        }
        Ok(None) => {} // genuinely none — fall through to the create prompt
        // Existence UNKNOWN (RPC trouble) — refuse to offer creation. A
        // create on a false "none" dead-ends at the relayer's "already
        // deployed" and confuses the operator (seen 2026-07-14).
        Err(e) => {
            return Err(anyhow!(
                "cannot determine whether a deposit wallet already exists ({}) — \
                 not offering to create one. Fix the RPC pool and re-run.",
                e
            ));
        }
    }
    if let Some(proxy) = gamma_public_profile_proxy(eoa) {
        if !proxy.eq_ignore_ascii_case(eoa) {
            let safe = super::deploy_wallet::derive_safe_address(eoa);
            if proxy.eq_ignore_ascii_case(&safe) {
                println!("  ⚠ Gamma shows this EOA's website wallet {} — that is its", proxy);
                println!("    Gnosis Safe, NOT a deposit wallet. For the legacy Safe flow,");
                println!("    re-run with `--signature-type gnosis_safe`.");
            } else {
                println!("  ⚠ Gamma shows website proxy wallet {} for this EOA", proxy);
                println!("    (not a deposit wallet — informational only).");
            }
        }
    }
    // ── No deposit wallet on-chain → confirm before creating ──
    // Deploying is an on-chain action (relayer WALLET-CREATE), so make the
    // operator opt in explicitly rather than auto-creating — especially
    // since the existence check can miss a wallet on a flaky RPC and we
    // don't want to mint a second one by surprise.
    use std::io::Write as _;
    println!("  No existing deposit wallet found for EOA {} (on-chain WalletDeployed scan).", eoa);
    println!("  A new POLY_1271 deposit wallet will be created via the Polymarket");
    println!("  relayer (on-chain WALLET-CREATE).");
    print!("  Create it now? [y/N]: ");
    std::io::stdout().flush().ok();
    let mut confirm = String::new();
    std::io::stdin()
        .read_line(&mut confirm)
        .map_err(|e| anyhow!("failed to read confirmation: {}", e))?;
    let c = confirm.trim().to_ascii_lowercase();
    if c != "y" && c != "yes" {
        return Err(anyhow!(
            "deposit-wallet creation not confirmed — aborting (nothing deployed). \
             If the wallet already exists, the on-chain WalletDeployed scan missed it \
             (RPC failure?) — re-run once the RPC is healthy and it will be found."
        ));
    }
    println!("  Deploying…");
    match deploy_deposit_wallet(builder_auth, eoa) {
        Ok(dw) => Ok(dw),
        // Deployed between the lookup and now (or the lookup missed it).
        Err(e) if e.to_string().contains("already deployed") => {
            find_existing_deposit_wallet(eoa)?.ok_or_else(|| anyhow!(
                "relayer reports the wallet already deployed, but none was found \
                 on-chain for {} — check the RPC pool and re-run",
                eoa
            ))
        }
        Err(e) => Err(e),
    }
}

/// Polymarket Gamma public-profile lookup (no auth). Returns the
/// `proxyWallet` for `address` if Gamma has a profile for it, else None.
///
/// ⚠ INFORMATIONAL ONLY — never treat the result as a deposit wallet.
/// Gamma's `proxyWallet` is the account's website proxy (a Gnosis Safe or
/// magic-link proxy); routing a relayer WALLET batch at it fails with
/// "wallet … is not registered". It also does not reverse-resolve an EOA
/// (a fresh EOA returns `proxyWallet: null`).
fn gamma_public_profile_proxy(address: &str) -> Option<String> {
    const GAMMA_API: &str = "https://gamma-api.polymarket.com";
    // Browser UA — Gamma sits behind Cloudflare and 403s a default UA.
    const UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) \
                      AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36";
    let url = format!("{}/public-profile?address={}", GAMMA_API, address);
    let client = crate::async_rt::http_client();
    let res: Result<Option<String>> = crate::async_rt::block_on_runtime(async move {
        let resp = client.get(&url).header("User-Agent", UA).send().await
            .map_err(|e| anyhow!("gamma public-profile GET: {}", e))?;
        if !resp.status().is_success() {
            return Ok(None); // 404 / error → no profile
        }
        let v: serde_json::Value = resp.json().await
            .map_err(|e| anyhow!("gamma public-profile parse: {}", e))?;
        Ok(v.get("proxyWallet")
            .and_then(|p| p.as_str())
            .filter(|s| !s.is_empty())
            .map(to_checksum_address))
    });
    match res {
        Ok(opt) => opt,
        Err(e) => {
            log::debug!("[deploy] gamma public-profile lookup failed (non-fatal): {}", e);
            None
        }
    }
}


// ════════════════════════════════════════════════════════════════
// ERC-7739-wrapped L1 ClobAuth (the #70 candidate fix)
// ════════════════════════════════════════════════════════════════


// ════════════════════════════════════════════════════════════════
// CLOB auth call (derive / create)
// ════════════════════════════════════════════════════════════════


// ════════════════════════════════════════════════════════════════
// Relayer WALLET-CREATE deploy
// ════════════════════════════════════════════════════════════════

/// Submit a `WALLET-CREATE` to the relayer, poll for confirmation, and
/// return the deployed deposit-wallet address from the `WalletDeployed`
/// event. The payload carries no user signature (per the docs); the
/// request is authenticated with builder/relayer credentials.
fn deploy_deposit_wallet(builder_auth: &PolyAuth, signer_eoa: &str) -> Result<String> {
    let body = serde_json::json!({
        "type": "WALLET-CREATE",
        "from": signer_eoa,
        "to": DEPOSIT_WALLET_FACTORY,
    });
    let body_str = body.to_string();
    let headers = builder_auth.sign_request("POST", "/submit", &body_str);
    let json = relayer_post(format!("{}/submit", RELAYER_URL), headers, body_str)?;
    let tx_id = json
        .get("transactionID")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("relayer /submit returned no transactionID: {}", json))?
        .to_string();
    println!("  WALLET-CREATE submitted (txID={}) — polling …", tx_id);

    // Poll the relayer transaction until it carries a tx hash / confirmed.
    let tx_hash = poll_relayer_tx(builder_auth, &tx_id)?;
    println!("  Confirmed on-chain (tx=0x{})", tx_hash.trim_start_matches("0x"));

    // Read the WalletDeployed event from the receipt.
    wallet_from_receipt(&tx_hash, signer_eoa)
}

fn poll_relayer_tx(builder_auth: &PolyAuth, tx_id: &str) -> Result<String> {
    // Reuse the project's known-working relayer transaction poller
    // (handles the array response + `state`/`transactionHash`/`errorMsg`
    // fields). It returns `(state, tx_hash)` per poll, or `Err` on a
    // STATE_FAILED with an error message.
    for _ in 0..30 {
        std::thread::sleep(std::time::Duration::from_secs(2));
        match super::wallet::poll_transaction(builder_auth, tx_id) {
            Ok((state, hash)) => {
                if state == "STATE_CONFIRMED" && !hash.is_empty() {
                    return Ok(hash);
                }
                if state == "STATE_FAILED" || state == "STATE_INVALID" {
                    return Err(anyhow!("relayer transaction {} is {}", tx_id, state));
                }
                // else: still pending — keep polling.
            }
            Err(e) => {
                // Terminal failures surface as "Transaction failed: …";
                // anything else is a transient poll error — retry.
                if e.to_string().contains("Transaction failed") {
                    return Err(e);
                }
                println!("  poll error (retrying): {}", e);
            }
        }
    }

    // The bounded poll expired. Query the same transaction one final time and
    // report its authoritative action state. The caller must not respond by
    // fetching another nonce and blindly submitting the same calls again.
    match super::wallet::poll_transaction(builder_auth, tx_id) {
        Ok((state, hash)) if state == "STATE_CONFIRMED" && !hash.is_empty() => {
            Ok(hash)
        }
        Ok((state, hash)) if matches!(state.as_str(), "STATE_FAILED" | "STATE_INVALID") => {
            Err(anyhow!("relayer transaction {} is {} (tx={})", tx_id, state, hash))
        }
        Ok((state, hash)) => Err(anyhow!(
            "relayer polling timed out; old WALLET action txID={} remains state={} tx={}; refusing automatic resubmission",
            tx_id,
            if state.is_empty() { "UNKNOWN" } else { &state },
            hash,
        )),
        Err(e) => Err(anyhow!(
            "relayer polling timed out; final status query for old WALLET action txID={} failed: {}; refusing automatic resubmission",
            tx_id,
            e,
        )),
    }
}

/// Pull the deployed wallet address out of the `WalletDeployed` log in the
/// tx receipt (topic1 = wallet, indexed). Falls back to an error carrying
/// the receipt; a deploy_wallet re-run resolves the wallet via the
/// on-chain `WalletDeployed` scan instead.
fn wallet_from_receipt(tx_hash: &str, signer_eoa: &str) -> Result<String> {
    let topic0 = format!("0x{}", hex::encode(keccak256(WALLET_DEPLOYED_TOPIC0_PREIMAGE)));
    let owner_topic = format!("0x{}", hex::encode(address_to_bytes32(signer_eoa)));
    let params = serde_json::json!([tx_hash]);
    let receipt = super::onchain_tx::rpc_call("eth_getTransactionReceipt", params)
        .map_err(|e| anyhow!("eth_getTransactionReceipt: {}", e))?;
    let logs = receipt
        .get("result")
        .and_then(|r| r.get("logs"))
        .and_then(|l| l.as_array())
        .ok_or_else(|| anyhow!("receipt has no logs: {}", receipt))?;
    for log in logs {
        let topics = match log.get("topics").and_then(|t| t.as_array()) {
            Some(t) => t,
            None => continue,
        };
        let t0 = topics.first().and_then(|v| v.as_str()).unwrap_or("");
        if !t0.eq_ignore_ascii_case(&topic0) {
            continue;
        }
        // topic2 = owner (indexed); confirm it's ours, then topic1 = wallet.
        let t2 = topics.get(2).and_then(|v| v.as_str()).unwrap_or("");
        if !t2.eq_ignore_ascii_case(&owner_topic) {
            continue;
        }
        if let Some(t1) = topics.get(1).and_then(|v| v.as_str()) {
            let bytes = hex::decode(t1.strip_prefix("0x").unwrap_or(t1)).unwrap_or_default();
            if bytes.len() == 32 {
                return Ok(to_checksum_address(&format!("0x{}", hex::encode(&bytes[12..]))));
            }
        }
    }
    Err(anyhow!(
        "WalletDeployed event not found in receipt for {} — re-run deploy_wallet; \
         the on-chain WalletDeployed scan will resolve the deployed wallet. Receipt: {}",
        tx_hash, receipt
    ))
}

/// Find an already-deployed deposit wallet for `owner_eoa`.
///
///   Ok(Some(dw)) — wallet found (code-verified on-chain)
///   Ok(None)     — no wallet exists (high confidence)
///   Err          — cannot determine (RPC trouble); callers must NOT
///                  treat this as "no wallet"
///
/// Primary signal: deterministic CREATE2 derivation (official-client
/// port; UUPS then BeaconProxy era) verified via `eth_getCode`. The owner
/// is baked into the CREATE2 salt, so code at a candidate address IS this
/// owner's wallet. Cheap (2-4 point calls) and immune to `eth_getLogs`
/// range limits (2026-07-14: a pool RPC enforcing "range … exceeds limit
/// of 10000" broke the fromBlock=earliest scan mid-setup).
///
/// Fallback: the full-range `WalletDeployed` scan — catches wallets from
/// implementation eras the derivation doesn't know. When the scan is ALSO
/// unavailable (range-limited RPC) the derivation's no-code answer stands:
/// the official client trusts pure derivation with no scan at all.
fn find_existing_deposit_wallet(owner_eoa: &str) -> Result<Option<String>> {
    let mut derive_err: Option<anyhow::Error> = None;
    for (era, candidate) in derive_dw_candidates(owner_eoa) {
        match has_code(&candidate) {
            Ok(true) => {
                println!("  (resolved via deterministic derivation, {} era)", era);
                return Ok(Some(candidate));
            }
            Ok(false) => {}
            Err(e) => derive_err = Some(e),
        }
    }
    match scan_wallet_deployed_logs(owner_eoa) {
        Ok(found) => Ok(found), // scan is authoritative for every era — Some or None
        Err(scan_err) => {
            if let Some(e) = derive_err {
                // getCode failed on a candidate AND the scan failed —
                // existence is genuinely unknown.
                return Err(anyhow!("derivation: {} / WalletDeployed scan: {}", e, scan_err));
            }
            println!(
                "  (WalletDeployed scan unavailable [{}] — trusting deterministic \
                 derivation: no deposit wallet)",
                scan_err
            );
            Ok(None)
        }
    }
}

/// Full-range `WalletDeployed` log scan (topic2 = owner indexed; both
/// factories). Requires an RPC that allows `fromBlock: earliest` getLogs
/// — range-limited providers reject it, which the derivation-first caller
/// tolerates. Most recent matching event wins (topic1 = wallet, indexed).
fn scan_wallet_deployed_logs(owner_eoa: &str) -> Result<Option<String>> {
    let topic0 = format!("0x{}", hex::encode(keccak256(WALLET_DEPLOYED_TOPIC0_PREIMAGE)));
    let owner_topic = format!("0x{}", hex::encode(address_to_bytes32(owner_eoa)));
    let filter = serde_json::json!([{
        "fromBlock": "earliest",
        "toBlock": "latest",
        "address": [DEPOSIT_WALLET_FACTORY, DEPOSIT_WALLET_FACTORY_ALT],
        "topics": [topic0, serde_json::Value::Null, owner_topic],
    }]);
    let resp = super::onchain_tx::rpc_call("eth_getLogs", filter)
        .map_err(|e| anyhow!("eth_getLogs: {}", e))?;
    let logs = resp
        .get("result")
        .and_then(|r| r.as_array())
        .ok_or_else(|| anyhow!("eth_getLogs returned no result array: {}", resp))?;
    for log in logs.iter().rev() {
        if let Some(t1) = log
            .get("topics")
            .and_then(|t| t.as_array())
            .and_then(|t| t.get(1))
            .and_then(|v| v.as_str())
        {
            let bytes = hex::decode(t1.strip_prefix("0x").unwrap_or(t1)).unwrap_or_default();
            if bytes.len() == 32 {
                return Ok(Some(to_checksum_address(&format!("0x{}", hex::encode(&bytes[12..])))));
            }
        }
    }
    Ok(None)
}

// ════════════════════════════════════════════════════════════════
// Deterministic deposit-wallet derivation (CREATE2)
// ════════════════════════════════════════════════════════════════
//
// Port of the official builder-relayer-client `src/builder/derive.ts`
// (Solady v0.1.26 LibClone initcode-hash replicas). Shared pieces:
//   args = abi.encode(address factory, bytes32(owner))   // 64 bytes
//   salt = keccak256(args)
// The two implementation eras differ only in the initcode hash:
//   UUPS   (pre 2026-05-28): initCodeHashERC1967(implementation, args)
//   Beacon (since):          initCodeHashERC1967BeaconProxy(beacon, args)
// Pinned by a unit test against a known mainnet (owner → wallet) pair.

/// `abi.encode(address factory, bytes32 walletId)` where walletId =
/// `bytes32(owner)` (left-padded).
fn dw_derive_args(owner: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(64);
    v.extend_from_slice(&address_to_bytes32(DEPOSIT_WALLET_FACTORY));
    v.extend_from_slice(&address_to_bytes32(owner));
    v
}

/// Solady's 10-byte initcode prefix: `base + (args_len << 56)` big-endian
/// (folds the immutable-args length into the PUSH2 runtime-size operand).
fn solady_prefix(base: u128, args_len: usize) -> [u8; 10] {
    let combined = base + ((args_len as u128) << 56);
    let be = combined.to_be_bytes();
    let mut out = [0u8; 10];
    out.copy_from_slice(&be[6..16]);
    out
}

/// Solady `LibClone.initCodeHashERC1967(implementation, args)`.
fn init_code_hash_erc1967(implementation: &str, args: &[u8]) -> [u8; 32] {
    const C1: &str = "cc3735a920a3ca505d382bbc545af43d6000803e6038573d6000fd5b3d6000f3";
    const C2: &str = "5155f3363d3d373d3d363d7f360894a13ba1a3210667c828492db98dca3e2076";
    let mut buf = Vec::with_capacity(10 + 20 + 2 + 64 + args.len());
    buf.extend_from_slice(&solady_prefix(0x61003d3d8160233d3973, args.len()));
    buf.extend_from_slice(&address_to_bytes32(implementation)[12..]);
    buf.extend_from_slice(&[0x60, 0x09]);
    buf.extend_from_slice(&hex::decode(C2).unwrap());
    buf.extend_from_slice(&hex::decode(C1).unwrap());
    buf.extend_from_slice(args);
    keccak256(&buf)
}

/// Solady `LibClone.initCodeHashERC1967BeaconProxy(beacon, args)`.
fn init_code_hash_erc1967_beacon(beacon: &str, args: &[u8]) -> [u8; 32] {
    const C1: &str = "b3582b35133d50545afa5036515af43d6000803e604d573d6000fd5b3d6000f3";
    const C2: &str = "1b60e01b36527fa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6c";
    const C3: &str = "60195155f3363d3d373d3d363d602036600436635c60da";
    let mut buf = Vec::with_capacity(10 + 20 + 23 + 64 + args.len());
    buf.extend_from_slice(&solady_prefix(0x6100523d8160233d3973, args.len()));
    buf.extend_from_slice(&address_to_bytes32(beacon)[12..]);
    buf.extend_from_slice(&hex::decode(C3).unwrap());
    buf.extend_from_slice(&hex::decode(C2).unwrap());
    buf.extend_from_slice(&hex::decode(C1).unwrap());
    buf.extend_from_slice(args);
    keccak256(&buf)
}

/// `CREATE2(deployer, salt, init_code_hash)` → checksummed address.
fn create2_address(deployer: &str, salt: &[u8; 32], init_code_hash: &[u8; 32]) -> String {
    let mut buf = Vec::with_capacity(1 + 20 + 32 + 32);
    buf.push(0xff);
    buf.extend_from_slice(&address_to_bytes32(deployer)[12..]);
    buf.extend_from_slice(salt);
    buf.extend_from_slice(init_code_hash);
    let h = keccak256(&buf);
    to_checksum_address(&format!("0x{}", hex::encode(&h[12..])))
}

fn derive_dw_uups(owner: &str, implementation: &str) -> String {
    let args = dw_derive_args(owner);
    let salt = keccak256(&args);
    create2_address(DEPOSIT_WALLET_FACTORY, &salt, &init_code_hash_erc1967(implementation, &args))
}

fn derive_dw_beacon(owner: &str, beacon: &str) -> String {
    let args = dw_derive_args(owner);
    let salt = keccak256(&args);
    create2_address(DEPOSIT_WALLET_FACTORY, &salt, &init_code_hash_erc1967_beacon(beacon, &args))
}

/// The candidate deposit-wallet addresses for `owner` — UUPS era first
/// (matches the official client's check order), then BeaconProxy era with
/// the live `factory.beacon()` (constant fallback on call failure).
fn derive_dw_candidates(owner: &str) -> Vec<(&'static str, String)> {
    let beacon = factory_beacon().unwrap_or_else(|| DW_BEACON_FALLBACK.to_string());
    vec![
        ("UUPS", derive_dw_uups(owner, DW_UUPS_IMPLEMENTATION)),
        ("beacon", derive_dw_beacon(owner, &beacon)),
    ]
}

/// Live `factory.beacon()` — `None` on call failure / revert / zero
/// (caller falls back to the pinned constant).
fn factory_beacon() -> Option<String> {
    let res = super::deploy_wallet::eth_call(DEPOSIT_WALLET_FACTORY, FACTORY_BEACON_SELECTOR)?;
    let bytes = hex::decode(res.strip_prefix("0x").unwrap_or(&res)).ok()?;
    if bytes.len() < 32 || bytes[12..32].iter().all(|b| *b == 0) {
        return None;
    }
    Some(to_checksum_address(&format!("0x{}", hex::encode(&bytes[12..32]))))
}

/// True if `address` has contract code on-chain. RPC failure surfaces as
/// `Err` — callers must not conflate "RPC down" with "no code".
fn has_code(address: &str) -> Result<bool> {
    let v = super::onchain_tx::rpc_call("eth_getCode", serde_json::json!([address, "latest"]))?;
    let code = v
        .get("result")
        .and_then(|r| r.as_str())
        .ok_or_else(|| anyhow!("eth_getCode {}: no result ({})", address, v))?;
    let t = code.strip_prefix("0x").unwrap_or(code);
    Ok(!t.is_empty() && t.chars().any(|c| c != '0'))
}

// ════════════════════════════════════════════════════════════════
// Helpers
// ════════════════════════════════════════════════════════════════


/// abi.encode of a sequence of pre-formed 32-byte words = plain concat
/// (every element here is a static type).
fn abi_encode_words(words: &[[u8; 32]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(words.len() * 32);
    for w in words {
        out.extend_from_slice(w);
    }
    out
}

fn eip712_digest(domain_sep: &[u8; 32], struct_hash: &[u8; 32]) -> [u8; 32] {
    let mut buf = Vec::with_capacity(2 + 64);
    buf.push(0x19);
    buf.push(0x01);
    buf.extend_from_slice(domain_sep);
    buf.extend_from_slice(struct_hash);
    keccak256(&buf)
}


fn relayer_post(
    url: String,
    headers: super::auth::AuthHeaders,
    body: String,
) -> Result<serde_json::Value> {
    let client = crate::async_rt::http_client();
    crate::async_rt::block_on_runtime(async move {
        let mut req = client.post(&url).header("Content-Type", "application/json").body(body);
        for (k, v) in headers.as_builder_pairs() {
            req = req.header(k, v);
        }
        let resp = req.send().await.map_err(|e| anyhow!("{}: {}", url, e))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(anyhow!("{} ({}): {}", url, status, text));
        }
        serde_json::from_str(&text).map_err(|e| anyhow!("parse {}: {} ({})", url, e, text))
    })
}

fn relayer_get(
    url: String,
    headers: super::auth::AuthHeaders,
) -> Result<serde_json::Value> {
    let client = crate::async_rt::http_client();
    crate::async_rt::block_on_runtime(async move {
        let mut req = client.get(&url);
        for (k, v) in headers.as_builder_pairs() {
            req = req.header(k, v);
        }
        let resp = req.send().await.map_err(|e| anyhow!("{}: {}", url, e))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(anyhow!("{} ({}): {}", url, status, text));
        }
        serde_json::from_str(&text).map_err(|e| anyhow!("parse {}: {} ({})", url, e, text))
    })
}

#[cfg(test)]
mod derive_tests {
    use super::*;

    #[test]
    fn wallet_nonce_response_accepts_string_and_number() {
        assert_eq!(
            parse_wallet_nonce(&serde_json::json!({"nonce": "1873"})).unwrap(),
            1873
        );
        assert_eq!(
            parse_wallet_nonce(&serde_json::json!({"nonce": 1874})).unwrap(),
            1874
        );
        assert!(parse_wallet_nonce(&serde_json::json!({})).is_err());
    }

    #[test]
    fn global_submit_gate_waits_for_new_or_unindexed_actions() {
        assert!(wallet_action_blocks_next_submit(""));
        assert!(wallet_action_blocks_next_submit("STATE_NEW"));
        assert!(!wallet_action_blocks_next_submit("STATE_EXECUTED"));
        assert!(!wallet_action_blocks_next_submit("STATE_CONFIRMED"));
        assert!(!wallet_action_blocks_next_submit("STATE_FAILED"));
        assert!(!wallet_action_blocks_next_submit("STATE_INVALID"));
    }

    #[test]
    fn wallet_action_fallback_requires_exact_signer_and_nonce() {
        let signer = "0x111111111111111111111111111111111111AaAa";
        let action = serde_json::json!({
            "from": "0x111111111111111111111111111111111111aaaa",
            "nonce": "1873",
            "type": "WALLET",
            "state": "STATE_EXECUTED",
        });
        assert!(action_matches_signer_nonce(&action, signer, 1873));
        assert!(!action_matches_signer_nonce(&action, signer, 1874));
        assert!(!action_matches_signer_nonce(
            &action,
            "0x2222222222222222222222222222222222222222",
            1873,
        ));

        let different_type = serde_json::json!({
            "from": signer,
            "nonce": 1873,
            "type": "SAFE",
        });
        assert!(action_matches_signer_nonce(&different_type, signer, 1873));
    }

    #[test]
    fn wallet_batch_rebuilds_signature_for_nonce_and_deadline() {
        let key = SigningKey::from_slice(&[7u8; 32]).unwrap();
        let eoa = "0x1111111111111111111111111111111111111111";
        let dw = "0x2222222222222222222222222222222222222222";
        let calls = vec![Call {
            target: "0x3333333333333333333333333333333333333333".to_string(),
            data: "0x1234".to_string(),
        }];

        let first: serde_json::Value = serde_json::from_str(
            &build_wallet_batch_body(&key, eoa, dw, &calls, 1873, 2_000_000_000).unwrap(),
        )
        .unwrap();
        let new_nonce: serde_json::Value = serde_json::from_str(
            &build_wallet_batch_body(&key, eoa, dw, &calls, 1874, 2_000_000_000).unwrap(),
        )
        .unwrap();
        let new_deadline: serde_json::Value = serde_json::from_str(
            &build_wallet_batch_body(&key, eoa, dw, &calls, 1873, 2_000_000_001).unwrap(),
        )
        .unwrap();

        assert_eq!(first["nonce"], "1873");
        assert_eq!(first["depositWalletParams"]["deadline"], "2000000000");
        assert_ne!(first["signature"], new_nonce["signature"]);
        assert_ne!(first["signature"], new_deadline["signature"]);
    }

    /// Known mainnet pair (relayer WALLET-CREATE tx `0x176477af…`,
    /// 2026-07-14): owner EOA → BeaconProxy-era deposit wallet. Pins the
    /// Solady initcode-hash port — if this breaks, derivation is silently
    /// wrong for every wallet and existence checks degrade to the
    /// (range-limit-fragile) log scan.
    #[test]
    fn beacon_derivation_matches_known_mainnet_wallet() {
        assert_eq!(
            derive_dw_beacon("0xd4c118fbd2eb09232fa104b69360b65a634fd0f7", DW_BEACON_FALLBACK),
            "0xcf578Fe23a0c53ECbe77136065C01a4DcaDB67DF",
        );
    }

    /// The UUPS-era path shares `args`/`salt`/CREATE2 with the beacon path
    /// (both pinned above); this only locks the era-specific initcode-hash
    /// assembly against accidental edits.
    #[test]
    fn uups_initcode_prefix_folds_args_len() {
        // 64-byte args → PUSH2 0x007d (0x3d runtime + 0x40 args).
        assert_eq!(solady_prefix(0x61003d3d8160233d3973, 64)[..3], [0x61, 0x00, 0x7d]);
        // Beacon flavour: 0x52 runtime + 0x40 args = 0x92.
        assert_eq!(solady_prefix(0x6100523d8160233d3973, 64)[..3], [0x61, 0x00, 0x92]);
    }
}
