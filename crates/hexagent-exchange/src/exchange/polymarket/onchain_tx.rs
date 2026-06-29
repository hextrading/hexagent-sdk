//! On-chain Safe transaction submission via the signer EOA (Polygon).
//!
//! Alternative to `deploy_wallet::submit_safe_tx` / `wallet::submit_safe_tx_with_id`,
//! which post to Polymarket's gasless relayer (`POST /submit`). When the
//! relayer is unavailable, rate-limited, or the operator just wants
//! deterministic local-paid execution, we build a Safe `execTransaction(...)`
//! call, wrap it in an EIP-1559 (type-2) Polygon transaction, sign with
//! the signer EOA, and broadcast via `eth_sendRawTransaction`.
//!
//! The Safe owner signature (same `sign_eip712_safe` / `sign_safe_tx` we
//! already produce for the relayer path) is passed unchanged as the
//! `signatures` arg of `execTransaction` — Safe accepts the eth_sign
//! variant (v ∈ {31, 32}) so no re-signing needed.
//!
//! Gas is paid in MATIC from the EOA's balance. Typical redeem or split
//! costs ≈ 0.01 MATIC on Polygon.

use anyhow::{anyhow, Context, Result};
use k256::ecdsa::SigningKey;
use log::{info, warn};
use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use super::deploy_wallet::{
    address_to_bytes32, keccak256, sign_eip712_safe, u256_bytes,
};

/// Polygon mainnet chain id.
const CHAIN_ID: u64 = 137;
/// Hardcoded gas limit for the outer EIP-1559 wrapper tx.
///
/// Sized for the longest call chain we issue:
///   Safe.execTransaction
///     → CtfCollateralAdapter.redeemPositions    (~50k overhead)
///       → CTF.redeemPositions                   (per-indexSet work)
///         → CTF.balanceOf × N                   (~10k each)
///         → CTF.safeBatchTransferFrom (burn)    (~50k)
///         → USDC.e.transfer                     (~50k for proxy + impl delegatecall)
///
/// Live observed 2026-05-04 redeem failure: total gasUsed 475k of
/// 500k, then USDC.e's proxy delegatecall ran out of gas with only
/// ~27k forwarded — leaving ≤25k headroom in the outer tx but the
/// FiatTokenV2 implementation needs ≥50k for the transfer. Bumping
/// to 800k gives ≥325k headroom over the 475k actually-consumed
/// path so the inner USDC.e transfer always has enough gas.
const DEFAULT_GAS_LIMIT: u64 = 800_000;
/// Max fee cap in wei for maxFeePerGas. Polygon base fee rarely exceeds
/// 200 gwei even in volatile windows; we pick 500 gwei ceiling for the
/// FIRST broadcast attempt. Retries escalate via `GAS_TIERS` below.
const MAX_FEE_PER_GAS_GWEI: u64 = 500;
/// Priority tip. Polygon validators accept anything ≥ 30 gwei as fast.
const MAX_PRIORITY_FEE_GWEI: u64 = 30;

/// Gas escalation tiers for retrying a `replacement transaction
/// underpriced` failure. Each tier is `(max_fee_gwei, tip_gwei)`.
///
/// Background: when a previous tx with the same nonce is stuck in
/// mempool (because we paid too little, or polygon node propagation
/// lag put us at the same nonce twice), Polygon's RPC requires the
/// replacement tx to pay **at least 10 % more** on both `max_fee` and
/// `tip`. We use a 1.4× / 2× ladder which is well past the 1.1×
/// requirement, so a single retry should clear any reasonable stuck
/// tx. The 1000 gwei ceiling is roughly 5 % of typical 5-min-event
/// PnL ($0.05 of MATIC at $20 per token), so even three retries
/// remain economically negligible relative to the cost of getting
/// the bot wedged.
///
/// Live 2026-05-16 evidence (live.log 10:25-10:57): 10+ Redeem failures
/// with `replacement transaction underpriced` over 30 min. Each one
/// blocked the maintenance pipeline → events ran with `init_up=0` →
/// bot accumulated directional positions blindly → -$25 cumulative.
pub const GAS_TIERS: [(u64, u64); 3] = [
    (500, 30),    // attempt 1: baseline
    (700, 50),    // attempt 2: 1.4× max_fee, 1.67× tip
    (1000, 100),  // attempt 3: 2.0× max_fee, 3.33× tip
];

/// Maximum broadcast attempts before giving up. Matches `GAS_TIERS.len()`.
pub const MAX_BROADCAST_ATTEMPTS: u32 = 3;

// Safe `execTransaction` selector: keccak256("execTransaction(address,uint256,bytes,uint8,uint256,uint256,uint256,address,address,bytes)")[:4]
// = 0x6a761202
const EXEC_TRANSACTION_SELECTOR: [u8; 4] = [0x6a, 0x76, 0x12, 0x02];

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// Classified broadcast outcome. Callers (e.g. `run_redeem_all`,
/// `run_split_one`) match on this to decide retry strategy:
///   - `Underpriced` → bump gas via next `GAS_TIERS` entry, retry
///   - `NonceTooLow` → resync local nonce cache, retry (rare; means
///     a tx confirmed between our `get_eth_nonce` and broadcast)
///   - `Other` → propagate up; unrecoverable
#[derive(Debug)]
pub enum BroadcastError {
    /// `replacement transaction underpriced` — there's already a tx at
    /// our nonce in mempool that paid more gas than we offered. Caller
    /// should retry with a higher gas tier from `GAS_TIERS`.
    Underpriced { nonce: u64, msg: String },
    /// `nonce too low` — our nonce is below the chain's next expected
    /// value (a tx confirmed since we last queried). Caller should
    /// resync nonce and retry.
    NonceTooLow { nonce: u64, msg: String },
    /// Any other RPC / serialization / network error.
    Other(anyhow::Error),
}

impl std::fmt::Display for BroadcastError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BroadcastError::Underpriced { nonce, msg } => {
                write!(f, "replacement underpriced (nonce={}): {}", nonce, msg)
            }
            BroadcastError::NonceTooLow { nonce, msg } => {
                write!(f, "nonce too low (nonce={}): {}", nonce, msg)
            }
            BroadcastError::Other(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for BroadcastError {}

impl BroadcastError {
    /// True iff caller should retry with a higher gas tier.
    pub fn is_retryable_with_higher_gas(&self) -> bool {
        matches!(self, BroadcastError::Underpriced { .. })
    }

    /// True iff caller should resync nonce and retry.
    pub fn is_nonce_too_low(&self) -> bool {
        matches!(self, BroadcastError::NonceTooLow { .. })
    }

    /// Classify a broadcast error string into the right variant.
    /// Polygon RPC returns these as JSON-RPC error messages; we match
    /// substrings rather than codes because the codes overlap (-32000
    /// covers both underpriced and many other node-fault classes).
    fn classify(err_str: &str, nonce: u64) -> BroadcastError {
        let lc = err_str.to_lowercase();
        if lc.contains("replacement transaction underpriced") ||
           lc.contains("replacement underpriced") {
            BroadcastError::Underpriced { nonce, msg: err_str.to_string() }
        } else if lc.contains("nonce too low") {
            BroadcastError::NonceTooLow { nonce, msg: err_str.to_string() }
        } else {
            BroadcastError::Other(anyhow!(err_str.to_string()))
        }
    }
}

/// Local nonce tracker, keyed by lowercase signer address.
///
/// Polygon's `eth_getTransactionCount(addr, "pending")` is supposed to
/// return the next-usable nonce including everything in mempool, but
/// public RPC nodes don't always propagate just-submitted txs to the
/// pending pool within ms. So two back-to-back submits from the same
/// thread can both get the same nonce back from `eth_getTransactionCount`
/// and race into "replacement underpriced".
///
/// This cache holds the *last nonce we submitted* per address. The
/// effective nonce for the next submit is:
///   ```text
///   nonce = max(chain_pending, local_last + 1)
///   ```
/// On successful broadcast we advance `local_last`. On `NonceTooLow`
/// we reset `local_last` to `chain_pending - 1` so the next attempt
/// re-queries. On other errors we don't touch the cache.
fn local_nonce_map() -> &'static Mutex<HashMap<String, u64>> {
    static MAP: OnceLock<Mutex<HashMap<String, u64>>> = OnceLock::new();
    MAP.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Resolve the nonce to use for the next broadcast from `signer`.
///
/// Returns `max(chain_pending_nonce, local_last_submitted + 1)`. This
/// is the smallest nonce that hasn't been used yet from either the
/// chain's or our local view.
fn next_nonce_for(signer: &str) -> Result<u64> {
    let chain_pending = get_eth_nonce(signer)?;
    let key = signer.to_lowercase();
    let mut map = local_nonce_map().lock().unwrap();
    let from_local = map.get(&key).copied().map(|n| n + 1).unwrap_or(0);
    let chosen = chain_pending.max(from_local);
    // Record the chosen value so the *next* call jumps past it even
    // if RPC's pending view hasn't caught up yet.
    map.insert(key, chosen);
    Ok(chosen)
}

/// Reset the local nonce cache for `signer` when chain says our local
/// view is stale (e.g. on `NonceTooLow`). Forces the next call to
/// re-fetch from chain.
fn reset_local_nonce(signer: &str) {
    let key = signer.to_lowercase();
    let mut map = local_nonce_map().lock().unwrap();
    map.remove(&key);
}

/// Submit a Safe transaction directly on-chain via the signer EOA.
///
/// Backward-compatible single-attempt API used by manual CLI paths
/// (`migrate_usdc`, `merge`). For the maintenance path which retries
/// with gas escalation, see [`submit_safe_tx_onchain_with_gas`] and
/// [`broadcast_with_escalation`].
///
/// Returns the Polygon tx hash (`0x...`-prefixed hex) once
/// `eth_sendRawTransaction` accepts it. The caller polls
/// `poll_onchain_tx(tx_hash)` to follow confirmation.
pub fn submit_safe_tx_onchain(
    signing_key: &SigningKey,
    signer: &str,
    safe: &str,
    to: &str,
    data: &str,
) -> Result<String> {
    submit_safe_tx_onchain_with_gas(
        signing_key, signer, safe, to, data, 0,
        MAX_FEE_PER_GAS_GWEI, MAX_PRIORITY_FEE_GWEI,
    ).map_err(|e| anyhow!("{}", e))
}

/// Submit a Safe transaction that transfers native POL (Polygon's gas
/// token) along with the call. `value_wei` is the amount of native POL
/// (in wei, 18 decimals) the Safe will send to `to`. For a pure POL
/// withdraw, pass `data = "0x"` and `value_wei = amount × 1e18`.
///
/// Mechanically equivalent to `submit_safe_tx_onchain` but threads the
/// `value` field through the SafeTx EIP-712 struct AND the outer
/// `execTransaction(...)` calldata — both must agree or the Safe
/// contract reverts on signature validation.
pub fn submit_safe_tx_onchain_with_value(
    signing_key: &SigningKey,
    signer: &str,
    safe: &str,
    to: &str,
    data: &str,
    value_wei: u128,
) -> Result<String> {
    submit_safe_tx_onchain_with_gas(
        signing_key, signer, safe, to, data, value_wei,
        MAX_FEE_PER_GAS_GWEI, MAX_PRIORITY_FEE_GWEI,
    ).map_err(|e| anyhow!("{}", e))
}

/// Submit a Safe transaction with caller-controlled gas. Returns
/// classified `BroadcastError` so callers can retry intelligently
/// (e.g. bump gas on `Underpriced`, resync nonce on `NonceTooLow`).
///
/// Internally uses the local nonce cache (see [`next_nonce_for`]) so
/// rapid back-to-back submits from the same address don't collide on
/// stale chain-side pending nonce.
pub fn submit_safe_tx_onchain_with_gas(
    signing_key: &SigningKey,
    signer: &str,
    safe: &str,
    to: &str,
    data: &str,
    value_wei: u128,
    max_fee_gwei: u64,
    tip_gwei: u64,
) -> std::result::Result<String, BroadcastError> {
    // 1) Build the Safe owner signature over the SafeTx EIP-712 digest.
    //    Use the on-chain Safe nonce — same as the relayer path.
    let safe_nonce = get_onchain_safe_nonce(safe)
        .map_err(BroadcastError::Other)?;
    let domain_sep = super::deploy_wallet::build_safe_tx_domain_separator(safe);

    let data_bytes = hex::decode(data.strip_prefix("0x").unwrap_or(data))
        .map_err(|e| BroadcastError::Other(anyhow!("invalid data hex: {}", e)))?;
    let data_hash = keccak256(&data_bytes);

    let struct_hash = {
        let type_hash = keccak256(
            b"SafeTx(address to,uint256 value,bytes data,uint8 operation,uint256 safeTxGas,uint256 baseGas,uint256 gasPrice,address gasToken,address refundReceiver,uint256 nonce)",
        );
        let mut buf = Vec::with_capacity(11 * 32);
        buf.extend_from_slice(&type_hash);
        buf.extend_from_slice(&address_to_bytes32(to));
        buf.extend_from_slice(&u256_bytes(value_wei));     // value
        buf.extend_from_slice(&data_hash);                  // data hash
        buf.extend_from_slice(&u256_bytes(0));             // operation (CALL)
        buf.extend_from_slice(&u256_bytes(0));             // safeTxGas
        buf.extend_from_slice(&u256_bytes(0));             // baseGas
        buf.extend_from_slice(&u256_bytes(0));             // gasPrice
        buf.extend_from_slice(&address_to_bytes32(ZERO_ADDRESS)); // gasToken
        buf.extend_from_slice(&address_to_bytes32(ZERO_ADDRESS)); // refundReceiver
        buf.extend_from_slice(&u256_bytes(safe_nonce as u128));
        keccak256(&buf)
    };

    let safe_owner_sig_hex = sign_eip712_safe(&domain_sep, &struct_hash, signing_key)
        .map_err(BroadcastError::Other)?;
    let safe_owner_sig = hex::decode(safe_owner_sig_hex.strip_prefix("0x").unwrap_or(&safe_owner_sig_hex))
        .map_err(|e| BroadcastError::Other(anyhow!("invalid safe sig hex: {}", e)))?;
    if safe_owner_sig.len() != 65 {
        return Err(BroadcastError::Other(anyhow!(
            "Safe owner signature must be 65 bytes, got {}", safe_owner_sig.len()
        )));
    }

    // 2) Build execTransaction calldata.
    let exec_calldata = build_exec_transaction_calldata(to, value_wei, &data_bytes, &safe_owner_sig);

    // 3) Build + sign the outer Polygon EIP-1559 tx.
    //    Use the LOCAL nonce manager so rapid back-to-back submits
    //    don't collide on stale chain-side pending.
    let signer_nonce = next_nonce_for(signer)
        .map_err(BroadcastError::Other)?;
    let max_fee_wei: u128 = (max_fee_gwei as u128) * 1_000_000_000u128;
    let max_priority_wei: u128 = (tip_gwei as u128) * 1_000_000_000u128;

    let safe_bytes = hex::decode(safe.strip_prefix("0x").unwrap_or(safe))
        .map_err(|e| BroadcastError::Other(anyhow!("invalid safe address: {}", e)))?;
    if safe_bytes.len() != 20 {
        return Err(BroadcastError::Other(anyhow!("safe address must be 20 bytes")));
    }

    let raw_tx = sign_eip1559_tx(
        signing_key,
        CHAIN_ID,
        signer_nonce,
        max_priority_wei,
        max_fee_wei,
        DEFAULT_GAS_LIMIT,
        &safe_bytes,
        0, // value (MATIC) — Safe doesn't need native payment
        &exec_calldata,
    ).map_err(BroadcastError::Other)?;

    info!(
        "[OnchainTx] Broadcast: signer={} → safe={} nonce={} gas_limit={} max_fee={}gwei tip={}gwei",
        &signer[..10.min(signer.len())], &safe[..10.min(safe.len())],
        signer_nonce, DEFAULT_GAS_LIMIT, max_fee_gwei, tip_gwei,
    );

    match send_raw_transaction(&raw_tx) {
        Ok(tx_hash) => {
            info!("[OnchainTx] tx submitted: {}", tx_hash);
            Ok(tx_hash)
        }
        Err(e) => {
            let err_str = e.to_string();
            let classified = BroadcastError::classify(&err_str, signer_nonce);
            // On NonceTooLow, drop the stale local cache so the next
            // attempt re-syncs to chain truth.
            if classified.is_nonce_too_low() {
                reset_local_nonce(signer);
            }
            Err(classified)
        }
    }
}

/// Broadcast with automatic gas escalation on `Underpriced` errors.
///
/// Walks through [`GAS_TIERS`] up to [`MAX_BROADCAST_ATTEMPTS`] times,
/// bumping `max_fee` and `tip` on each retry. Returns the tx hash on
/// the first success, or the last error if all tiers fail.
///
/// Used by the maintenance pipeline (`run_redeem_all`, `run_split_one`)
/// where a stuck mempool tx can wedge the entire bot — better to spend
/// $0.05 of MATIC on aggressive replacement than lose $25+ to running
/// events without seed inventory.
pub fn broadcast_with_escalation(
    signing_key: &SigningKey,
    signer: &str,
    safe: &str,
    to: &str,
    data: &str,
) -> std::result::Result<String, BroadcastError> {
    let mut last_err: Option<BroadcastError> = None;
    for (attempt, (max_fee_gwei, tip_gwei)) in GAS_TIERS.iter().enumerate() {
        if attempt > 0 {
            warn!(
                "[OnchainTx] Retry #{} with escalated gas: max_fee={}gwei tip={}gwei (prev: {})",
                attempt + 1, max_fee_gwei, tip_gwei,
                last_err.as_ref().map(|e| e.to_string()).unwrap_or_default(),
            );
            // Small backoff so the mempool / RPC has time to settle
            // the previous broadcast attempt's state.
            std::thread::sleep(Duration::from_millis(500));
        }
        match submit_safe_tx_onchain_with_gas(
            signing_key, signer, safe, to, data, 0,
            *max_fee_gwei, *tip_gwei,
        ) {
            Ok(tx_hash) => return Ok(tx_hash),
            Err(e) if e.is_retryable_with_higher_gas() => {
                last_err = Some(e);
                continue;
            }
            Err(e) if e.is_nonce_too_low() => {
                // Nonce resync needed — retry the SAME tier with fresh
                // nonce (local cache was just reset).
                warn!(
                    "[OnchainTx] NonceTooLow on tier {} — resyncing nonce and retrying same tier",
                    attempt + 1,
                );
                std::thread::sleep(Duration::from_millis(300));
                match submit_safe_tx_onchain_with_gas(
                    signing_key, signer, safe, to, data, 0,
                    *max_fee_gwei, *tip_gwei,
                ) {
                    Ok(tx_hash) => return Ok(tx_hash),
                    Err(e2) => {
                        last_err = Some(e2);
                        continue;
                    }
                }
            }
            Err(e) => {
                // Other errors aren't gas-related — escalation won't
                // help. Return immediately.
                return Err(e);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| {
        BroadcastError::Other(anyhow!("broadcast_with_escalation: no attempts made"))
    }))
}

/// Poll for on-chain tx receipt. Returns (`state`, `tx_hash`) where state
/// is shaped like the relayer's strings for drop-in compatibility:
/// `"CONFIRMED"`, `"STATE_FAILED"`, or `"PENDING"`.
pub fn poll_onchain_tx(tx_hash: &str) -> Result<(String, String)> {
    let resp = rpc_call(
        "eth_getTransactionReceipt",
        serde_json::json!([tx_hash]),
    )?;
    let result = resp.get("result");
    if result.is_none() || result.unwrap().is_null() {
        // receipt not yet produced → still pending
        return Ok(("PENDING".to_string(), tx_hash.to_string()));
    }
    let r = result.unwrap();
    let status = r.get("status").and_then(|v| v.as_str()).unwrap_or("0x0");
    let state = if status == "0x1" { "CONFIRMED" } else { "STATE_FAILED" };
    Ok((state.to_string(), tx_hash.to_string()))
}

// ════════════════════════════════════════════════════════════════
// Internals
// ════════════════════════════════════════════════════════════════

/// Resolve the Polygon RPC pool.
///
/// Preferred source is `$POLYGON_RPC_LIST` — a comma-separated pool built
/// from `[polygon].rpc_list` in the secrets file. `rpc_call` round-robins
/// across the pool (spreading load so no single node sees all the
/// concurrency) and, on a node fault, rotates to the next node before
/// failing. Falls back to `$POLYGON_RPC` (+ optional `$POLYGON_RPC_2`)
/// when the list is unset, so the legacy scalar config still works.
///
/// A node "fault" is `-32000` (often a stale-state false "insufficient
/// funds"), `-32603` ("Internal error"), or any 5xx / transport error —
/// almost always node-side (forked / overloaded / rate-limited) rather
/// than request-side, so another node is the right fix and has no effect
/// on tx semantics.
///
/// Point the pool at paid providers (Alchemy / QuickNode / paid Infura)
/// for real HA — public endpoints churn (polygon-rpc.com 401, Blast shut
/// down, llamarpc DNS gone) and silently cycling through dead providers
/// is worse than a short, healthy pool.
pub(super) fn polygon_rpc_urls() -> Result<Vec<String>> {
    // Preferred: explicit pool from `[polygon].rpc_list`.
    if let Ok(list) = std::env::var("POLYGON_RPC_LIST") {
        let urls: Vec<String> = list
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if !urls.is_empty() {
            return Ok(urls);
        }
    }
    // Back-compat: single primary (+ optional secondary).
    let primary = std::env::var("POLYGON_RPC")
        .map_err(|_| anyhow!(
            "POLYGON_RPC not resolved — add a [polygon] section (rpc_list = [\"https://…\"] \
             or rpc = \"https://…\") to the secrets file (required for \
             gas_via_signer_wallet = true). It is no longer read from .env."
        ))?;
    if primary.is_empty() {
        return Err(anyhow!(
            "POLYGON_RPC is empty — set [polygon].rpc_list or [polygon].rpc in the secrets file"
        ));
    }
    let mut urls = vec![primary];
    if let Ok(secondary) = std::env::var("POLYGON_RPC_2") {
        let trimmed = secondary.trim().to_string();
        if !trimmed.is_empty() {
            urls.push(trimmed);
        }
    }
    Ok(urls)
}

/// Dedicated RPC HTTP client with timeouts bigger than the shared
/// `http_client_auto`. On Polygon, public RPCs occasionally take
/// >800 ms just to accept the TCP connection, and a full JSON-RPC
/// round trip can take 2-3 seconds on contested endpoints. The shared
/// `http_client_auto` is tuned for internal endpoints with tight SLAs
/// (connect 800 ms, total 5 s) — too aggressive here.
fn rpc_http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            // ALPN negotiate (public RPCs vary; some h2, some h1)
            .pool_idle_timeout(Duration::from_secs(60))
            .pool_max_idle_per_host(4)
            .tcp_keepalive(Duration::from_secs(30))
            .tcp_nodelay(true)
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(15))
            .build()
            .expect("build rpc http client")
    })
}

/// Flatten an error + its `source()` chain into one line. Without this,
/// `reqwest::Error` Display prints just "error sending request for url
/// (…)" and silently drops the root cause (dns / tls / timeout).
fn describe_err<E: Error + ?Sized>(e: &E) -> String {
    let mut msg = e.to_string();
    let mut cur = e.source();
    while let Some(s) = cur {
        msg.push_str(" | caused by: ");
        msg.push_str(&s.to_string());
        cur = s.source();
    }
    msg
}

/// POST a JSON-RPC request across the RPC pool, with two layers of retry:
///   1. Transient transport / 5xx errors: up to 3 attempts per node with
///      500 ms backoff (same endpoint, same answer expected eventually).
///   2. Node-fault classes (`-32000`, `-32603`, or 5xx after retries
///      exhausted): rotate to the next node in the pool. These errors are
///      almost always node-side (forked / overloaded / rate-limited),
///      not request-side, so a different node is the right fix.
///
/// Each call starts at a round-robin offset into the pool, so concurrent
/// calls fan out across nodes (lower per-node load) while still walking
/// every node from that offset before failing (full failover).
///
/// Genuine logical RPC errors (e.g. revert reasons, malformed params)
/// surface immediately — same endpoint will give the same answer, and
/// retrying on a different node won't change reality.
///
/// Exposed as `pub(super)` so `deploy_wallet::eth_call` uses the same
/// tuned client + retry path.
pub(super) fn rpc_call(method: &str, params: serde_json::Value) -> Result<serde_json::Value> {
    let urls = polygon_rpc_urls()?;
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": 1,
    }).to_string();

    let client = rpc_http_client().clone();
    const MAX_TRANSIENT_ATTEMPTS: usize = 3;
    let mut last_err: Option<anyhow::Error> = None;

    // Round-robin starting node: each call begins at the next node in the
    // pool so concurrent calls spread out instead of all hammering node #0.
    // We still walk the whole pool from that offset, so failover tries
    // every node before giving up. (`urls` is guaranteed non-empty.)
    static RR: AtomicUsize = AtomicUsize::new(0);
    let n = urls.len();
    let start = RR.fetch_add(1, Ordering::Relaxed) % n;

    'urls: for step in 0..n {
        let url_idx = (start + step) % n;
        let url = &urls[url_idx];
        if step > 0 {
            warn!("[OnchainTx] RPC {} failing over to pool node #{} (started at #{}, {} nodes) after: {}",
                method, url_idx, start, n,
                last_err.as_ref().map(|e| e.to_string()).unwrap_or_default());
        }
        for attempt in 1..=MAX_TRANSIENT_ATTEMPTS {
            let url_cl = url.clone();
            let body_cl = body.clone();
            let client_cl = client.clone();
            let result: Result<serde_json::Value> = crate::async_rt::block_on_runtime(async move {
                let resp = match client_cl.post(&url_cl)
                    .header("Content-Type", "application/json")
                    .body(body_cl)
                    .send().await
                {
                    Ok(r) => r,
                    Err(e) => return Err(anyhow!("send: {}", describe_err(&e))),
                };
                let status = resp.status();
                let text = resp.text().await.unwrap_or_default();
                if !status.is_success() {
                    return Err(anyhow!("http {}: {}", status, text));
                }
                serde_json::from_str::<serde_json::Value>(&text)
                    .map_err(|e| anyhow!("parse: {} body={}", e, text))
            });

            match result {
                Ok(v) => {
                    if let Some(rpc_err) = v.get("error") {
                        let code = rpc_err.get("code").and_then(|c| c.as_i64());
                        let is_node_fault = matches!(code, Some(-32000) | Some(-32603));
                        if is_node_fault {
                            // Same URL won't change its mind — switch URL.
                            last_err = Some(anyhow!("rpc error: {}", rpc_err));
                            continue 'urls;
                        }
                        // Genuine logical error — retry won't help on any URL.
                        return Err(anyhow!("rpc error: {}", rpc_err));
                    }
                    if attempt > 1 || step > 0 {
                        info!("[OnchainTx] RPC {} succeeded on pool node #{} attempt {}",
                            method, url_idx, attempt);
                    }
                    return Ok(v);
                }
                Err(e) => {
                    last_err = Some(e);
                    if attempt < MAX_TRANSIENT_ATTEMPTS {
                        warn!("[OnchainTx] RPC {} URL #{} attempt {}/{} failed: {} — retrying",
                            method, url_idx, attempt, MAX_TRANSIENT_ATTEMPTS,
                            last_err.as_ref().unwrap());
                        std::thread::sleep(Duration::from_millis(500));
                        continue;
                    }
                    // Exhausted transient retries on this URL — fall through to next URL.
                }
            }
        }
    }
    Err(anyhow!("RPC {} failed across {} URL(s): {}",
        method, urls.len(),
        last_err.map(|e| e.to_string()).unwrap_or_else(|| "unknown".to_string())))
}

fn get_eth_nonce(address: &str) -> Result<u64> {
    let resp = rpc_call(
        "eth_getTransactionCount",
        serde_json::json!([address, "pending"]),
    )?;
    let hex_str = resp.get("result").and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("eth_getTransactionCount: no result ({})", resp))?;
    let clean = hex_str.strip_prefix("0x").unwrap_or(hex_str);
    let n = u64::from_str_radix(clean, 16)
        .map_err(|e| anyhow!("parse nonce: {}", e))?;
    Ok(n)
}

fn get_onchain_safe_nonce(safe: &str) -> Result<u64> {
    // nonce() selector = 0xaffed0e0
    let result = super::deploy_wallet::eth_call(safe, "0xaffed0e0")
        .ok_or_else(|| anyhow!("eth_call nonce() failed"))?;
    let clean = result.strip_prefix("0x").unwrap_or(&result).trim_start_matches('0');
    if clean.is_empty() { return Ok(0); }
    u64::from_str_radix(clean, 16)
        .map_err(|e| anyhow!("parse safe nonce: {}", e))
}

fn send_raw_transaction(raw_tx: &[u8]) -> Result<String> {
    let raw_hex = format!("0x{}", hex::encode(raw_tx));
    let resp = rpc_call(
        "eth_sendRawTransaction",
        serde_json::json!([raw_hex]),
    )?;
    let tx_hash = resp.get("result").and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("eth_sendRawTransaction: no result ({})", resp))?;
    Ok(tx_hash.to_string())
}

/// ABI-encode `execTransaction(address,uint256,bytes,uint8,uint256,uint256,uint256,address,address,bytes)`
/// including the 4-byte selector. Returns the full calldata ready to
/// place in the outer EIP-1559 tx `data` field.
fn build_exec_transaction_calldata(
    to: &str,
    value_wei: u128,
    inner_data: &[u8],
    signatures: &[u8],
) -> Vec<u8> {
    // Fixed-size head: 10 slots × 32 bytes
    //   [0]  to                           → address padded
    //   [1]  value                        → uint256 = 0
    //   [2]  offset of bytes `data`       → pointer (10 * 32 = 320 initially, but we patch below)
    //   [3]  operation                    → uint8 = 0 (CALL)
    //   [4]  safeTxGas                    → uint256 = 0
    //   [5]  baseGas                      → uint256 = 0
    //   [6]  gasPrice                     → uint256 = 0
    //   [7]  gasToken                     → address = 0
    //   [8]  refundReceiver               → address = 0
    //   [9]  offset of bytes `signatures` → pointer
    //
    // Tail: [data_len || data_padded] then [sig_len || sig_padded]
    let head_slots = 10usize;
    let head_size = head_slots * 32;

    let data_padded_len = ((inner_data.len() + 31) / 32) * 32;
    let data_tail_size = 32 + data_padded_len;   // 32 for length prefix
    let sig_padded_len = ((signatures.len() + 31) / 32) * 32;

    let mut out = Vec::with_capacity(4 + head_size + data_tail_size + 32 + sig_padded_len);
    out.extend_from_slice(&EXEC_TRANSACTION_SELECTOR);

    // Head
    out.extend_from_slice(&address_to_bytes32(to));                                 // [0] to
    out.extend_from_slice(&u256_bytes(value_wei));                                   // [1] value
    out.extend_from_slice(&u256_bytes(head_size as u128));                           // [2] offset→data (320)
    out.extend_from_slice(&u256_bytes(0));                                           // [3] operation
    out.extend_from_slice(&u256_bytes(0));                                           // [4] safeTxGas
    out.extend_from_slice(&u256_bytes(0));                                           // [5] baseGas
    out.extend_from_slice(&u256_bytes(0));                                           // [6] gasPrice
    out.extend_from_slice(&address_to_bytes32(ZERO_ADDRESS));                        // [7] gasToken
    out.extend_from_slice(&address_to_bytes32(ZERO_ADDRESS));                        // [8] refundReceiver
    out.extend_from_slice(&u256_bytes((head_size + data_tail_size) as u128));        // [9] offset→sigs

    // Tail 1: data
    out.extend_from_slice(&u256_bytes(inner_data.len() as u128));
    out.extend_from_slice(inner_data);
    out.extend_from_slice(&vec![0u8; data_padded_len - inner_data.len()]);

    // Tail 2: signatures
    out.extend_from_slice(&u256_bytes(signatures.len() as u128));
    out.extend_from_slice(signatures);
    out.extend_from_slice(&vec![0u8; sig_padded_len - signatures.len()]);

    out
}

/// Build + sign an EIP-1559 (type-2) Polygon transaction.
///
/// Returns the raw bytes `0x02 || rlp([...signed fields])` suitable for
/// `eth_sendRawTransaction`.
fn sign_eip1559_tx(
    key: &SigningKey,
    chain_id: u64,
    nonce: u64,
    max_priority_fee_wei: u128,
    max_fee_wei: u128,
    gas_limit: u64,
    to: &[u8],      // 20 bytes
    value: u128,
    data: &[u8],
) -> Result<Vec<u8>> {
    // Step 1: serialize the unsigned tx payload (without v, r, s).
    //   rlp([chainId, nonce, maxPriorityFeePerGas, maxFeePerGas, gasLimit,
    //        to, value, data, accessList])
    let unsigned = rlp_encode_list(&[
        rlp_encode_uint(chain_id as u128),
        rlp_encode_uint(nonce as u128),
        rlp_encode_uint(max_priority_fee_wei),
        rlp_encode_uint(max_fee_wei),
        rlp_encode_uint(gas_limit as u128),
        rlp_encode_bytes(to),
        rlp_encode_uint(value),
        rlp_encode_bytes(data),
        rlp_encode_list(&[]), // empty access list
    ]);

    // Step 2: hash = keccak256(0x02 || unsigned_rlp)
    let mut preimage = Vec::with_capacity(1 + unsigned.len());
    preimage.push(0x02);
    preimage.extend_from_slice(&unsigned);
    let tx_hash = keccak256(&preimage);

    // Step 3: sign. EIP-1559 uses parity (0 or 1), NOT v=27/28.
    let (sig, recid) = key.sign_prehash_recoverable(&tx_hash)
        .context("sign EIP-1559 tx")?;
    let sig_bytes = sig.to_bytes();
    let r = &sig_bytes[..32];
    let s = &sig_bytes[32..];
    let y_parity = recid.to_byte() as u128; // 0 or 1

    // Step 4: serialize signed tx:
    //   rlp([chainId, nonce, maxPriorityFee, maxFee, gasLimit, to, value,
    //        data, accessList, yParity, r, s])
    let signed = rlp_encode_list(&[
        rlp_encode_uint(chain_id as u128),
        rlp_encode_uint(nonce as u128),
        rlp_encode_uint(max_priority_fee_wei),
        rlp_encode_uint(max_fee_wei),
        rlp_encode_uint(gas_limit as u128),
        rlp_encode_bytes(to),
        rlp_encode_uint(value),
        rlp_encode_bytes(data),
        rlp_encode_list(&[]),
        rlp_encode_uint(y_parity),
        rlp_encode_bytes(trim_leading_zeros(r)),
        rlp_encode_bytes(trim_leading_zeros(s)),
    ]);

    let mut raw = Vec::with_capacity(1 + signed.len());
    raw.push(0x02);
    raw.extend_from_slice(&signed);
    Ok(raw)
}

// ════════════════════════════════════════════════════════════════
// Minimal RLP encoder (yellow paper appendix B)
// ════════════════════════════════════════════════════════════════

/// Encode a byte string per RLP rules.
fn rlp_encode_bytes(bytes: &[u8]) -> Vec<u8> {
    if bytes.len() == 1 && bytes[0] < 0x80 {
        // Single byte in [0x00, 0x7f] encodes as itself.
        return vec![bytes[0]];
    }
    if bytes.len() < 56 {
        let mut out = Vec::with_capacity(1 + bytes.len());
        out.push(0x80 + bytes.len() as u8);
        out.extend_from_slice(bytes);
        return out;
    }
    // Long form: prefix 0xb7 + len_of_len, then big-endian length, then bytes.
    let len_be = encode_len_be(bytes.len() as u64);
    let mut out = Vec::with_capacity(1 + len_be.len() + bytes.len());
    out.push(0xb7 + len_be.len() as u8);
    out.extend_from_slice(&len_be);
    out.extend_from_slice(bytes);
    out
}

/// Encode a list of already-encoded items.
fn rlp_encode_list(items: &[Vec<u8>]) -> Vec<u8> {
    let total: usize = items.iter().map(|i| i.len()).sum();
    if total < 56 {
        let mut out = Vec::with_capacity(1 + total);
        out.push(0xc0 + total as u8);
        for it in items { out.extend_from_slice(it); }
        return out;
    }
    let len_be = encode_len_be(total as u64);
    let mut out = Vec::with_capacity(1 + len_be.len() + total);
    out.push(0xf7 + len_be.len() as u8);
    out.extend_from_slice(&len_be);
    for it in items { out.extend_from_slice(it); }
    out
}

/// Encode an unsigned integer per RLP (big-endian, no leading zeros;
/// zero → empty byte string).
fn rlp_encode_uint(n: u128) -> Vec<u8> {
    if n == 0 {
        return rlp_encode_bytes(&[]);
    }
    let bytes = n.to_be_bytes();
    let first_nonzero = bytes.iter().position(|&b| b != 0).unwrap_or(bytes.len() - 1);
    rlp_encode_bytes(&bytes[first_nonzero..])
}

fn encode_len_be(len: u64) -> Vec<u8> {
    let bytes = len.to_be_bytes();
    let first = bytes.iter().position(|&b| b != 0).unwrap_or(7);
    bytes[first..].to_vec()
}

fn trim_leading_zeros(bytes: &[u8]) -> &[u8] {
    let mut i = 0;
    while i < bytes.len() && bytes[i] == 0 { i += 1; }
    &bytes[i..]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rlp_single_byte() {
        assert_eq!(rlp_encode_bytes(&[0x7f]), vec![0x7f]);
        assert_eq!(rlp_encode_bytes(&[0x00]), vec![0x00]);
    }

    #[test]
    fn rlp_short_bytes() {
        assert_eq!(rlp_encode_bytes(&[0x80]), vec![0x81, 0x80]);
        assert_eq!(rlp_encode_bytes(b"dog"), vec![0x83, b'd', b'o', b'g']);
    }

    #[test]
    fn rlp_empty_list() {
        assert_eq!(rlp_encode_list(&[]), vec![0xc0]);
    }

    #[test]
    fn rlp_uint_zero() {
        // RLP convention: integer 0 → empty byte string (0x80).
        assert_eq!(rlp_encode_uint(0), vec![0x80]);
    }

    #[test]
    fn rlp_uint_127() {
        assert_eq!(rlp_encode_uint(127), vec![0x7f]);
    }

    #[test]
    fn rlp_uint_1024() {
        // 1024 = 0x0400; trimmed = 0x04 0x00; rlp = [0x82, 0x04, 0x00]
        assert_eq!(rlp_encode_uint(1024), vec![0x82, 0x04, 0x00]);
    }

    // ── BroadcastError classification ──────────────────────────────

    #[test]
    fn broadcast_err_classifies_underpriced() {
        // The exact substring Polygon RPCs return for the stuck-tx
        // case observed 2026-05-16 in live.log.
        let err = BroadcastError::classify(
            "rpc error: {\"code\":-32000,\"message\":\"replacement transaction underpriced\"}",
            10038,
        );
        assert!(err.is_retryable_with_higher_gas(),
            "underpriced must be retryable: {:?}", err);
        assert!(!err.is_nonce_too_low());
        match err {
            BroadcastError::Underpriced { nonce, .. } => assert_eq!(nonce, 10038),
            other => panic!("expected Underpriced, got {:?}", other),
        }
    }

    #[test]
    fn broadcast_err_classifies_nonce_too_low() {
        let err = BroadcastError::classify(
            "rpc error: {\"code\":-32000,\"message\":\"nonce too low\"}",
            10038,
        );
        assert!(!err.is_retryable_with_higher_gas());
        assert!(err.is_nonce_too_low(), "nonce-too-low flag: {:?}", err);
    }

    #[test]
    fn broadcast_err_classifies_other() {
        let err = BroadcastError::classify(
            "rpc error: {\"code\":-32603,\"message\":\"Internal error\"}",
            10038,
        );
        // Neither retryable-with-gas nor nonce-too-low → falls through
        // to Other. Caller should NOT retry via gas escalation (won't
        // help; the node itself is sick).
        assert!(!err.is_retryable_with_higher_gas());
        assert!(!err.is_nonce_too_low());
        assert!(matches!(err, BroadcastError::Other(_)),
            "expected Other, got {:?}", err);
    }

    #[test]
    fn gas_tiers_strictly_increasing() {
        // Required property: each tier MUST be ≥ 1.1× previous on both
        // axes for Polygon to accept the replacement. Our 1.4× / 2×
        // schedule satisfies this with margin.
        for i in 1..GAS_TIERS.len() {
            let (prev_fee, prev_tip) = GAS_TIERS[i - 1];
            let (cur_fee, cur_tip) = GAS_TIERS[i];
            assert!(cur_fee as f64 >= 1.1 * prev_fee as f64,
                "tier {} max_fee {} not >= 1.1x prev {}", i, cur_fee, prev_fee);
            assert!(cur_tip as f64 >= 1.1 * prev_tip as f64,
                "tier {} tip {} not >= 1.1x prev {}", i, cur_tip, prev_tip);
        }
    }

    #[test]
    fn gas_tiers_match_documented_schedule() {
        // Lock in the (500, 700, 1000) gwei max_fee schedule so an
        // accidental edit elsewhere doesn't silently change the
        // economic profile.
        assert_eq!(GAS_TIERS[0].0, 500);
        assert_eq!(GAS_TIERS[1].0, 700);
        assert_eq!(GAS_TIERS[2].0, 1000);
        assert_eq!(MAX_BROADCAST_ATTEMPTS, GAS_TIERS.len() as u32);
    }

    // ── Local nonce manager ────────────────────────────────────────
    //
    // We can't easily test the real `next_nonce_for` because it calls
    // out to `get_eth_nonce` (network). The helpers below test the
    // pure HashMap-keyed cache semantics, which is the bit that
    // actually prevents the same-nonce race.

    #[test]
    fn local_nonce_cache_isolates_addresses() {
        // Use synthetic addresses so we don't collide with any real
        // session state.
        let addr_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa01";
        let addr_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb02";
        {
            let mut map = local_nonce_map().lock().unwrap();
            map.insert(addr_a.to_lowercase(), 42);
            map.insert(addr_b.to_lowercase(), 999);
        }
        let map = local_nonce_map().lock().unwrap();
        assert_eq!(map.get(&addr_a.to_lowercase()).copied(), Some(42));
        assert_eq!(map.get(&addr_b.to_lowercase()).copied(), Some(999));
    }

    #[test]
    fn local_nonce_reset_drops_entry() {
        let addr = "0xcccccccccccccccccccccccccccccccccccccc03";
        {
            let mut map = local_nonce_map().lock().unwrap();
            map.insert(addr.to_lowercase(), 100);
        }
        reset_local_nonce(addr);
        let map = local_nonce_map().lock().unwrap();
        assert!(map.get(&addr.to_lowercase()).is_none(),
            "reset must remove the entry so next call re-syncs from chain");
    }
}
