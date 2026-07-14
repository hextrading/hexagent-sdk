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
//!   * [`ensure_deposit_wallet`] — find an existing DW (on-chain
//!     `WalletDeployed` scan; Gamma `/public-profile` is a hint only) or,
//!     after an interactive confirm, deploy one via relayer `WALLET-CREATE`.
//!   * [`dw_approvals`] / [`dw_split`] / [`dw_redeem`] / [`dw_merge`] /
//!     [`dw_onramp`] / [`dw_offramp_withdraw`] / [`dw_transfer_erc20`].

use anyhow::{anyhow, Result};
use k256::ecdsa::SigningKey;

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
const USDCE_TOKEN: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"; // USDC.e (6dp)
/// Official AutoRedeemer (proxy; docs.polymarket.com/resources/contracts).
/// Granting it `setApprovalForAll` on the CTF is the on-chain opt-in for
/// Polymarket's auto-redeem: its keeper calls `redeemBinary(froms,
/// conditionIds)` and the payout goes to the position owner. (The UI
/// "Auto redeem your wins" toggle grants this same approval.)
const AUTO_REDEEMER: &str = "0xa1200000d0002264C9a1698e001292D00E1b00af";
const ONRAMP: &str = "0x93070a847efEf7F70739046A929D47a521F5B8ee"; // Collateral Onramp (USDC.e→pUSD)
const OFFRAMP: &str = "0x2957922Eb93258b93368531d39fAcCA3B4dC5854"; // Collateral Offramp (pUSD→USDC.e)
const WRAP_SELECTOR: [u8; 4] = [0x62, 0x35, 0x56, 0x38]; // wrap(address,address,uint256)
const UNWRAP_SELECTOR: [u8; 4] = [0x8c, 0xc7, 0x10, 0x4f]; // unwrap(address,address,uint256)
const APPROVE_SELECTOR: [u8; 4] = [0x09, 0x5e, 0xa7, 0xb3]; // approve(address,uint256)
const SET_APPROVAL_FOR_ALL_SELECTOR: [u8; 4] = [0xa2, 0x2c, 0xb4, 0x65]; // setApprovalForAll(address,bool)
const NONCE_SELECTOR: [u8; 4] = [0xaf, 0xfe, 0xd0, 0xe0]; // nonce()
const U256_MAX_HEX: &str = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
// Relayer requires the batch deadline within a window ending at now+300s;
// 240 was rejected "deadline too soon", so sit near the max (leaves ~10s
// headroom under 300 for clock skew + request latency).
const BATCH_DEADLINE_SECS: u64 = 290;

struct Call {
    target: String,
    data: String, // 0x-hex calldata; value is always 0
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

/// Read the deposit wallet's current batch `nonce()`.
fn dw_nonce(dw: &str) -> Result<u128> {
    let data = format!("0x{}", hex::encode(NONCE_SELECTOR));
    let res = super::deploy_wallet::eth_call(dw, &data)
        .ok_or_else(|| anyhow!("eth_call nonce() on {} returned nothing", dw))?;
    let bytes = hex::decode(res.strip_prefix("0x").unwrap_or(&res)).unwrap_or_default();
    let mut buf = [0u8; 16];
    if bytes.len() >= 16 {
        buf.copy_from_slice(&bytes[bytes.len() - 16..]);
    }
    Ok(u128::from_be_bytes(buf))
}

/// Sign + submit a relayer `type:"WALLET"` batch. Returns the tx id.
fn submit_wallet_batch(
    key: &SigningKey,
    eoa: &str,
    dw: &str,
    builder_auth: &PolyAuth,
    calls: &[Call],
    now_secs: u64,
    dry_run: bool,
) -> Result<String> {
    let nonce = dw_nonce(dw)?;
    let deadline = now_secs + BATCH_DEADLINE_SECS;

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

    let body = serde_json::json!({
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
    });
    let body_str = body.to_string();
    if dry_run {
        println!("   (dry-run) nonce={} deadline={} batch={}", nonce, deadline, body_str);
        return Ok(String::new());
    }
    let headers = builder_auth.sign_request("POST", "/submit", &body_str);
    let json = relayer_post(format!("{}/submit", RELAYER_URL), headers, body_str)?;
    let tx_id = json
        .get("transactionID")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("WALLET batch returned no transactionID: {}", json))?
        .to_string();
    println!("   WALLET batch submitted (txID={}) — polling …", tx_id);
    let tx_hash = poll_relayer_tx(builder_auth, &tx_id)?;
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

/// `Onramp.wrap(asset, to, amount)` — burns `asset` (USDC.e), mints pUSD
/// to `to`. Same ABI as `migrate_usdc::build_wrap_calldata`.
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
    submit_wallet_batch(key, eoa, dw, builder_auth, &calls, now_secs()?, /*dry_run=*/ false)
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
    submit_wallet_batch(key, eoa, dw, builder_auth, &calls, now_secs()?, /*dry_run=*/ false)
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
    submit_wallet_batch(key, eoa, dw, builder_auth, &calls, now_secs()?, dry_run)
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
    submit_wallet_batch(key, eoa, dw, builder_auth, &calls, now_secs()?, dry_run)
}

/// Wrap `amount_wei` of the DW's USDC.e → pUSD (approve Onramp + wrap) in
/// one WALLET batch.
pub(crate) fn dw_onramp(
    key: &SigningKey, eoa: &str, dw: &str, builder_auth: &PolyAuth, amount_wei: u128, dry_run: bool,
) -> Result<String> {
    let calls = vec![
        Call { target: USDCE_TOKEN.to_string(), data: approve_calldata(ONRAMP) },
        Call { target: ONRAMP.to_string(), data: onramp_wrap_calldata(USDCE_TOKEN, dw, amount_wei) },
    ];
    submit_wallet_batch(key, eoa, dw, builder_auth, &calls, now_secs()?, dry_run)
}

/// Withdraw the DW's pUSD as USDC.e: in ONE WALLET batch — approve
/// pUSD→Offramp, unwrap `amount_wei` pUSD → USDC.e (into the DW), then
/// transfer that USDC.e to `recipient`. The deposit-wallet analogue of
/// `wallet.rs::run_withdraw_pusd_to_usdce` (Safe path), but atomic: all
/// three calls run sequentially in a single relayer tx. pUSD↔USDC.e is 1:1
/// (both 6-decimal), so `amount_wei` is the pUSD burned == USDC.e sent. The
/// approve is unconditional (idempotent ∞-approval, same as `dw_onramp`).
pub(crate) fn dw_offramp_withdraw(
    key: &SigningKey, eoa: &str, dw: &str, builder_auth: &PolyAuth,
    recipient: &str, amount_wei: u128, dry_run: bool,
) -> Result<String> {
    let calls = vec![
        Call { target: PUSD_TOKEN.to_string(), data: approve_calldata(OFFRAMP) },
        Call { target: OFFRAMP.to_string(), data: offramp_unwrap_calldata(USDCE_TOKEN, dw, amount_wei) },
        Call { target: USDCE_TOKEN.to_string(), data: erc20_transfer_calldata(recipient, amount_wei) },
    ];
    submit_wallet_batch(key, eoa, dw, builder_auth, &calls, now_secs()?, dry_run)
}

/// Transfer `amount_wei` of an ERC-20 (`token`) FROM the DW to `to`
/// (WALLET batch). Used by `withdraw` for pUSD/USDC.e.
pub(crate) fn dw_transfer_erc20(
    key: &SigningKey, eoa: &str, dw: &str, builder_auth: &PolyAuth,
    token: &str, to: &str, amount_wei: u128, dry_run: bool,
) -> Result<String> {
    let calls = vec![Call { target: token.to_string(), data: erc20_transfer_calldata(to, amount_wei) }];
    submit_wallet_batch(key, eoa, dw, builder_auth, &calls, now_secs()?, dry_run)
}

/// Resolve the deposit-wallet address for `eoa`: prefer the configured
/// `POLY_FUNDER`, else scan `WalletDeployed` logs on-chain.
pub(crate) fn resolve_deposit_wallet(eoa: &str) -> Result<String> {
    let env = std::env::var("POLY_FUNDER").unwrap_or_default();
    if !env.trim().is_empty() {
        return Ok(to_checksum_address(env.trim()));
    }
    find_existing_deposit_wallet(eoa)
}

/// Resolve the deposit wallet for `eoa`, deploying it (relayer
/// `WALLET-CREATE`) if it doesn't exist yet. Used by `deploy_wallet`.
pub(crate) fn ensure_deposit_wallet(builder_auth: &PolyAuth, eoa: &str) -> Result<String> {
    // ── Existence pre-check: skip WALLET-CREATE if one already exists ──
    // The ONLY authoritative deposit-wallet signal is the on-chain
    // `WalletDeployed` scan (keyed by owner EOA). The Polymarket Gamma
    // `/public-profile` API is NOT an existence signal: its `proxyWallet`
    // is the account's WEBSITE proxy (a Gnosis Safe or magic-link proxy),
    // never a deposit wallet — treating it as one mis-routed the WALLET
    // batch to a Safe and the relayer 400'd with "wallet … is not
    // registered". Gamma is kept below purely as an operator hint.
    if let Ok(dw) = find_existing_deposit_wallet(eoa) {
        println!("  Existing deposit wallet found on-chain (WalletDeployed log): {}", dw);
        println!("  → already exists; skipping WALLET-CREATE.");
        return Ok(dw);
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
             If the wallet already exists, re-run with `--deposit-wallet <addr>` to skip creation."
        ));
    }
    println!("  Deploying…");
    match deploy_deposit_wallet(builder_auth, eoa) {
        Ok(dw) => Ok(dw),
        // Deployed between the lookup and now (or lookup missed it).
        Err(e) if e.to_string().contains("already deployed") => find_existing_deposit_wallet(eoa),
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
                    return Err(anyhow!("relayer reports WALLET-CREATE {}", state));
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
    Err(anyhow!("WALLET-CREATE not confirmed after polling"))
}

/// Pull the deployed wallet address out of the `WalletDeployed` log in the
/// tx receipt (topic1 = wallet, indexed). Falls back to an error with the
/// receipt so the operator can supply `--deposit-wallet` manually.
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
        "WalletDeployed event not found in receipt for {} — re-run with \
         --deposit-wallet <addr> once you know it. Receipt: {}",
        tx_hash, receipt
    ))
}

/// Find an already-deployed deposit wallet for `owner` by scanning past
/// `WalletDeployed` logs (topic2 = owner indexed). Best-effort: returns
/// `Err` if the RPC range query is rejected or no event matches — the
/// caller then asks the operator to supply `--deposit-wallet` from the
/// Polymarket UI.
fn find_existing_deposit_wallet(owner_eoa: &str) -> Result<String> {
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
    // Most recent matching event wins (topic1 = wallet, indexed).
    for log in logs.iter().rev() {
        if let Some(t1) = log
            .get("topics")
            .and_then(|t| t.as_array())
            .and_then(|t| t.get(1))
            .and_then(|v| v.as_str())
        {
            let bytes = hex::decode(t1.strip_prefix("0x").unwrap_or(t1)).unwrap_or_default();
            if bytes.len() == 32 {
                return Ok(to_checksum_address(&format!("0x{}", hex::encode(&bytes[12..]))));
            }
        }
    }
    Err(anyhow!(
        "no WalletDeployed event for owner {} (event sig / factory may differ)",
        owner_eoa
    ))
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
