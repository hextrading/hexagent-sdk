//! `hexbot deposit_wallet_setup` — **Phase-1 SPIKE** for the CLOB v2
//! deposit-wallet (POLY_1271 / signature_type=3) migration.
//!
//! ## Why this exists
//!
//! CLOB v2 rejects orders from our Gnosis-Safe maker with
//! `"maker address not allowed, please use the deposit wallet flow"`.
//! The fix is to trade from a **deposit wallet** (a per-user ERC-1967
//! proxy) with `signatureType=3` (POLY_1271), where `maker == signer ==
//! deposit wallet` and the order carries an ERC-7739-wrapped ERC-1271
//! signature.
//!
//! The one genuinely *unproven* piece is **API-key binding**: every
//! official SDK (py/rs-clob-client-v2) binds the CLOB API key to the
//! signer **EOA** in its L1 `ClobAuth` (`POLY_ADDRESS = signer.address()`,
//! raw ECDSA, no ERC-7739 wrap — see
//! `py_clob_client_v2/headers/headers.py::create_level_1_headers`).
//! With a deposit-wallet maker that yields
//! `"order signer address has to be the address of the API KEY"`
//! (py-clob-client-v2 issue #70). The recommended-but-unshipped fix:
//! set `POLY_ADDRESS` to the **deposit wallet** and **ERC-7739-wrap**
//! the L1 `ClobAuth` signature so the CLOB validates it via the wallet's
//! ERC-1271. Whether the CLOB *server* accepts that is the GO/NO-GO this
//! spike answers — cheaply, before we build Phases 2-6.
//!
//! ## Usage
//!
//! ```text
//! hexbot deposit_wallet_setup --instance zhu01 --deposit-wallet 0xABC… [--dry-run] [--create]
//! ```
//!
//! * `--deposit-wallet 0x…` — an already-deployed deposit wallet (e.g.
//!   created in the Polymarket UI). The SDKs do **not** derive this
//!   offline; it comes from the deploy event / UI. If omitted, the spike
//!   deploys one via the relayer `WALLET-CREATE` and reads the address
//!   back from the `WalletDeployed` event.
//! * `--dry-run` — derive nothing on-chain; compute + print the wrapped
//!   `ClobAuth` digest, signature, and the exact L1 headers, but make
//!   **no** network calls. Safe to run anytime.
//! * `--create` — POST `/auth/api-key` (create) in addition to the GET
//!   `/auth/derive-api-key` (derive) attempt.
//!
//! This command is **isolated**: it never touches the live trading path
//! and writes nothing to the secrets file. It only reports whether the
//! CLOB will bind a key to the deposit wallet.

use anyhow::{anyhow, Result};
use k256::ecdsa::SigningKey;

use super::auth::PolyAuth;
use super::deploy_wallet::{address_to_bytes32, keccak256, to_checksum_address, u256_bytes};
use super::signer::{derive_eth_address_from_key, SignatureType};
use super::signer_v2::OrderSignerV2;
use crate::types::Side;

// ════════════════════════════════════════════════════════════════
// Constants (Polygon mainnet, chain ID 137)
// ════════════════════════════════════════════════════════════════

const RELAYER_URL: &str = "https://relayer-v2.polymarket.com";
const CLOB_URL: &str = "https://clob.polymarket.com";
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
const CLOB_AUTH_DOMAIN_NAME: &str = "ClobAuthDomain";
const CLOB_AUTH_VERSION: &str = "1";
const CLOB_AUTH_MESSAGE: &str = "This message attests that I control the given wallet";
const CLOB_AUTH_TYPE_STRING: &str =
    "ClobAuth(address address,string timestamp,uint256 nonce,string message)";

// ERC-7739 / Solady `TypedDataSign` wrapper (mirrors the order wrap in
// rs-clob-client-v2 `client.rs::sign_poly1271_order`, but with `ClobAuth`
// as the wrapped `contents`). The wallet "app domain" is the deposit
// wallet itself: name="DepositWallet", version="1", zero salt.
const DEPOSIT_WALLET_NAME: &str = "DepositWallet";
const DEPOSIT_WALLET_VERSION: &str = "1";
const SOLADY_CLOB_AUTH_TYPE_STRING: &str = concat!(
    "TypedDataSign(ClobAuth contents,string name,string version,uint256 chainId,",
    "address verifyingContract,bytes32 salt)",
    "ClobAuth(address address,string timestamp,uint256 nonce,string message)",
);

// `WalletDeployed(address indexed wallet, address indexed owner, bytes32 indexed id, address implementation)`
const WALLET_DEPLOYED_TOPIC0_PREIMAGE: &[u8] =
    b"WalletDeployed(address,address,bytes32,address)";

// ════════════════════════════════════════════════════════════════
// CLI entry point
// ════════════════════════════════════════════════════════════════

pub fn run_deposit_wallet_setup() -> Result<()> {
    let args: Vec<String> = super::cli_account::cli_args().collect();
    let dry_run = args.iter().any(|a| a == "--dry-run" || a == "-n");
    let do_create = args.iter().any(|a| a == "--create");
    let do_test_order = args.iter().any(|a| a == "--test-order");
    let do_test_approvals = args.iter().any(|a| a == "--test-approvals");
    let test_split_amt = flag_value(&args, "--test-split");
    let test_redeem_cid = flag_value(&args, "--test-redeem");
    let test_onramp_amt = flag_value(&args, "--test-onramp");
    let do_sync_balance = args.iter().any(|a| a == "--sync-balance");
    let slug = flag_value(&args, "--slug").unwrap_or_else(|| "btc-up-or-down-5m".to_string());
    let deposit_wallet_arg = flag_value(&args, "--deposit-wallet");
    // test-split routing: `--via-adapter` targets the CtfCollateralAdapter
    // (→ USDC.e-space tokens), else CTF-direct; `--collateral usdce|pusd`
    // picks the collateralToken arg (default pUSD).
    let split_via_adapter = args.iter().any(|a| a == "--via-adapter");
    let split_collateral = flag_value(&args, "--collateral").unwrap_or_else(|| "pusd".to_string());

    // ── Signer EOA (from the resolved [poly.<id>] private_key) ──
    let private_key = std::env::var("POLY_PRIVATE_KEY")
        .map_err(|_| anyhow!("POLY_PRIVATE_KEY not set — run with --instance <id> --config <p> \
            so cli_account resolves the [poly.<id>] credentials"))?;
    let signing_key = parse_private_key(&private_key)?;
    let signer_eoa = to_checksum_address(&derive_eth_address_from_key(&signing_key));

    println!("=== Deposit Wallet Setup — SPIKE (CLOB v2 / POLY_1271) ===");
    println!();
    println!("Signer (EOA):   {}", signer_eoa);

    // ── Resolve the deposit wallet address ──
    let deposit_wallet = match deposit_wallet_arg {
        Some(addr) => {
            let cs = to_checksum_address(&addr);
            println!("Deposit wallet: {}  (provided)", cs);
            cs
        }
        None if dry_run => {
            // Nothing to deploy in dry-run and none provided — use a
            // placeholder so we can still show the wrapped-auth shape.
            println!("Deposit wallet: <none provided> — dry-run will use the signer EOA as a \
                placeholder address to illustrate the payload shape.");
            signer_eoa.clone()
        }
        None => {
            // Deploy via relayer WALLET-CREATE, then read the address
            // from the WalletDeployed event. If the relayer reports the
            // wallet already exists, look it up on-chain instead.
            let builder_auth = load_builder_auth(&signer_eoa)?;
            println!();
            println!("No --deposit-wallet given → deploying via relayer WALLET-CREATE …");
            match deploy_deposit_wallet(&builder_auth, &signer_eoa) {
                Ok(addr) => {
                    println!("Deployed deposit wallet: {}", addr);
                    addr
                }
                Err(e) if e.to_string().contains("already deployed") => {
                    println!("  Relayer: wallet already deployed — looking it up on-chain …");
                    find_existing_deposit_wallet(&signer_eoa).map_err(|le| anyhow!(
                        "deposit wallet already exists but on-chain lookup failed ({}). \
                         Grab the deposit address from the Polymarket UI (your account's deposit \
                         address) and re-run with --deposit-wallet 0x…", le))?
                }
                Err(e) => return Err(e),
            }
        }
    };

    // ── --test-order: place ONE unfunded type-3 order to clear the #70
    //    server check, then return (skips the L1/L2 auth probes) ──
    if do_test_order {
        return test_order(&private_key, &signer_eoa, &deposit_wallet, &slug, dry_run);
    }
    // ── --test-approvals: set/confirm the DW's v2 allowances via a WALLET
    //    batch (pUSD→CTF, pUSD→ExchangeV2, CTF→ExchangeV2) ──
    if do_test_approvals {
        return test_approvals(&signing_key, &signer_eoa, &deposit_wallet, dry_run);
    }
    // ── --test-split <usdc>: ONE splitPosition from the DW (isolated; use
    //    a tiny amount to confirm the CTF/collateral path before wiring) ──
    if let Some(amt) = test_split_amt {
        return test_split(&signing_key, &signer_eoa, &deposit_wallet, &slug, &amt,
            split_via_adapter, &split_collateral, dry_run);
    }
    // ── --test-redeem <conditionId>: ONE redeemPositions from the DW for a
    //    RESOLVED condition (reuses --via-adapter / --collateral) ──
    if let Some(cid) = test_redeem_cid {
        return test_redeem(&signing_key, &signer_eoa, &deposit_wallet, &cid,
            split_via_adapter, &split_collateral, dry_run);
    }
    // ── --test-onramp <usdce>: wrap the DW's USDC.e → pUSD via the Onramp
    //    (one WALLET batch: approve USDC.e→Onramp + Onramp.wrap) ──
    if let Some(amt) = test_onramp_amt {
        return test_onramp(&signing_key, &signer_eoa, &deposit_wallet, &amt, dry_run);
    }
    // ── --sync-balance: refresh the CLOB's cached balance/allowance for the
    //    DW (signature_type=3) so it sees freshly-deposited pUSD ──
    if do_sync_balance {
        println!();
        println!("── CLOB balance-cache sync (GET /balance-allowance/update?signature_type=3) ──");
        match l2_balance_update(&signer_eoa) {
            Ok(j) => println!("   updated → {}", j),
            Err(e) => println!("   update → {}", e),
        }
        return Ok(());
    }

    // ── Build the ERC-7739-wrapped L1 ClobAuth ──
    let timestamp = current_unix_secs()?;
    let nonce: u64 = 0;
    let (digest, wrapped_sig) =
        wrapped_clob_auth_signature(&signing_key, &deposit_wallet, &timestamp, nonce);

    println!();
    println!("── ERC-7739-wrapped L1 ClobAuth ──────────────────");
    println!("POLY_ADDRESS:   {}  (deposit wallet, NOT the EOA)", deposit_wallet);
    println!("POLY_TIMESTAMP: {}", timestamp);
    println!("POLY_NONCE:     {}", nonce);
    println!("digest:         0x{}", hex::encode(digest));
    println!("POLY_SIGNATURE: {}", wrapped_sig);
    println!("  (len={} bytes — wrapped, vs 65 for a raw EOA sig)", (wrapped_sig.len() - 2) / 2);

    if dry_run {
        println!();
        println!("(dry-run: no network calls made)");
        return Ok(());
    }

    // ── L1-auth variant matrix ──
    // One run, several POLY_ADDRESS/POLY_SIGNATURE constructions, so a
    // single 401 doesn't leave us guessing. The EOA-standard variant is a
    // **control**: it's exactly what the working type-2 derive does, so a
    // 200 there proves the transport is sound and isolates the 401 to the
    // deposit-wallet / wrapped variants.
    let eoa_sig = unwrapped_clob_auth_signature(&signing_key, &signer_eoa, &timestamp, nonce);
    let dw_unwrapped = unwrapped_clob_auth_signature(&signing_key, &deposit_wallet, &timestamp, nonce);

    let variants: [(&str, &str, &str); 3] = [
        ("EOA standard (CONTROL — must be 200)", &signer_eoa, &eoa_sig),
        ("DW + ERC-7739 wrapped (the #70 fix)", &deposit_wallet, &wrapped_sig),
        ("DW + raw EOA sig (no wrap)", &deposit_wallet, &dw_unwrapped),
    ];

    for (label, poly_address, sig) in variants {
        println!();
        println!("── Variant: {} ──", label);
        println!("   POLY_ADDRESS = {}", poly_address);
        match call_api_key(poly_address, &timestamp, nonce, sig, /*create=*/ false) {
            Ok(json) => report_creds("derive", &json, poly_address),
            Err(e) => println!("   derive → {}", e),
        }
        if do_create {
            match call_api_key(poly_address, &timestamp, nonce, sig, /*create=*/ true) {
                Ok(json) => report_creds("create", &json, poly_address),
                Err(e) => println!("   create → {}", e),
            }
        }
    }

    // ── The REAL type-3 auth path ──
    // The SDK never binds a key to the deposit wallet (the L1 matrix above
    // is informational). It uses the EOA's L2 key and passes
    // `signature_type` as a request param; the server resolves the deposit
    // wallet from the api-key's account. So the actual GO/NO-GO is whether
    // the existing EOA key + signature_type=3 reaches the deposit wallet.
    println!();
    println!("── L2 balance-allowance probe (EOA key + signature_type param) ──");
    println!("   POLY_ADDRESS (L2) = {}  deposit wallet (server-resolved) = {}",
        signer_eoa, deposit_wallet);
    for st in [2u8, 3u8] {
        match l2_balance_probe(&signer_eoa, st) {
            Ok(j) => println!("   signature_type={} → 200: {}", st, j),
            Err(e) => println!("   signature_type={} → {}", st, e),
        }
    }

    println!();
    println!("Read (the L2 probe is the verdict; L1 matrix is informational):");
    println!("  • sig_type=3 → 200 (balance/allowance, 0 ok if unfunded) → GO: the EOA key");
    println!("    reaches the deposit wallet. Next: Phase-5 order wrap + a live type-3 order.");
    println!("  • sig_type=3 → error → the EOA key isn't linked to the DW for type-3; we dig");
    println!("    into how Polymarket associates api key ↔ deposit wallet (UI-created link?).");
    Ok(())
}

/// L2 (HMAC) probe of `GET /balance-allowance?signature_type=<st>` using the
/// **existing EOA api key** (env `POLY_API_KEY/_API_SECRET/_PASSPHRASE`,
/// populated by `cli_account::resolve_and_apply`). Mirrors the SDK's
/// `get_balance_allowance`: the path (no query) is HMAC-signed; params ride
/// in the URL; the server resolves the deposit wallet from the account.
fn l2_balance_probe(eoa: &str, signature_type: u8) -> Result<serde_json::Value> {
    let api_key = std::env::var("POLY_API_KEY").unwrap_or_default();
    let secret = std::env::var("POLY_API_SECRET").unwrap_or_default();
    let passphrase = std::env::var("POLY_PASSPHRASE").unwrap_or_default();
    if api_key.is_empty() || secret.is_empty() || passphrase.is_empty() {
        return Err(anyhow!(
            "missing L2 creds (POLY_API_KEY/_API_SECRET/_PASSPHRASE) — resolve_and_apply should \
             set these from the [poly.<id>] block"
        ));
    }
    let auth = PolyAuth::new(&api_key, &secret, &passphrase, eoa)?;
    let path = "/balance-allowance";
    let headers = auth.sign_request("GET", path, "");
    let url = format!(
        "{}{}?signature_type={}&asset_type=COLLATERAL",
        CLOB_URL, path, signature_type
    );
    let pairs: Vec<(String, String)> = headers
        .as_pairs()
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let client = crate::async_rt::http_client();
    crate::async_rt::block_on_runtime(async move {
        let mut req = client.get(&url);
        for (k, v) in &pairs {
            req = req.header(k.as_str(), v.as_str());
        }
        let resp = req.send().await.map_err(|e| anyhow!("request: {}", e))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(anyhow!("HTTP {} — {}", status, text));
        }
        serde_json::from_str::<serde_json::Value>(&text)
            .map_err(|e| anyhow!("parse: {} (body={})", e, text))
    })
}

/// L2 GET `/balance-allowance/update?asset_type=COLLATERAL&signature_type=3`
/// — forces the CLOB to re-read the deposit wallet's on-chain balance into
/// its cache. Needed after funding, else orders reject on a stale balance=0.
fn l2_balance_update(eoa: &str) -> Result<serde_json::Value> {
    let api_key = std::env::var("POLY_API_KEY").unwrap_or_default();
    let secret = std::env::var("POLY_API_SECRET").unwrap_or_default();
    let passphrase = std::env::var("POLY_PASSPHRASE").unwrap_or_default();
    if api_key.is_empty() || secret.is_empty() || passphrase.is_empty() {
        return Err(anyhow!("missing L2 creds (POLY_API_KEY/_API_SECRET/_PASSPHRASE)"));
    }
    let auth = PolyAuth::new(&api_key, &secret, &passphrase, eoa)?;
    let path = "/balance-allowance/update";
    let headers = auth.sign_request("GET", path, "");
    let url = format!("{}{}?signature_type=3&asset_type=COLLATERAL", CLOB_URL, path);
    let pairs: Vec<(String, String)> = headers
        .as_pairs()
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let client = crate::async_rt::http_client();
    crate::async_rt::block_on_runtime(async move {
        let mut req = client.get(&url);
        for (k, v) in &pairs {
            req = req.header(k.as_str(), v.as_str());
        }
        let resp = req.send().await.map_err(|e| anyhow!("request: {}", e))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(anyhow!("HTTP {} — {}", status, text));
        }
        serde_json::from_str::<serde_json::Value>(&text).map_err(|e| anyhow!("parse: {} ({})", e, text))
    })
}

// ════════════════════════════════════════════════════════════════
// #70 test order — one unfunded type-3 order, observe the verdict
// ════════════════════════════════════════════════════════════════

/// Place ONE deposit-wallet (POLY_1271) order with the EOA L2 key. The DW
/// is unfunded (balance 0), so a well-formed order should be rejected for
/// *balance*, not signer — which clears the #70 "order signer ≠ api key"
/// risk. postOnly + far-below-market price means it can't take even if the
/// DW were funded.
fn test_order(
    private_key: &str,
    eoa: &str,
    deposit_wallet: &str,
    slug: &str,
    dry_run: bool,
) -> Result<()> {
    println!();
    println!("── #70 test order (type-3, unfunded, postOnly far-from-market) ──");
    let (series_id, event) = super::market::fetch_active_event(slug)?;
    let token_id = event
        .all_token_ids()
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("active event {} has no clob token ids", series_id))?;
    println!("   series_id={} token={}…", series_id, &token_id[..16.min(token_id.len())]);

    let builder_code = std::env::var("POLY_BUILDER_CODE").unwrap_or_default();
    let signer = OrderSignerV2::new(private_key, /*neg_risk=*/ false, SignatureType::Poly1271, &builder_code)?;
    let (price, size) = (0.01_f64, 100.0_f64); // postOnly BUY, ~$1 notional, far below market
    let signed = signer.build_signed_order_poly1271(deposit_wallet, &token_id, price, size, Side::Buy)?;
    let o = &signed.order;
    println!("   maker=signer={} | postOnly BUY {}@{} (~${:.2}) | sigType={}",
        deposit_wallet, size, price, price * size, o.signature_type);

    let api_key = std::env::var("POLY_API_KEY").unwrap_or_default();
    let salt_u64 = o.salt.parse::<u128>().map(|v| v as u64).unwrap_or(0);
    let body = serde_json::json!({
        "owner": api_key,
        "orderType": "GTC",
        "postOnly": true,
        "deferExec": false,
        "order": {
            "salt": salt_u64,
            "maker": o.maker,
            "signer": o.signer,
            "taker": o.taker,
            "tokenId": o.token_id,
            "makerAmount": o.maker_amount,
            "takerAmount": o.taker_amount,
            "side": "BUY",
            "signatureType": o.signature_type,
            "timestamp": o.timestamp,
            "expiration": o.expiration,
            "metadata": o.metadata,
            "builder": o.builder,
            "signature": signed.signature,
        }
    });
    let body_str = body.to_string();

    if dry_run {
        println!("   (dry-run) POST /order body: {}", body_str);
        return Ok(());
    }

    let secret = std::env::var("POLY_API_SECRET").unwrap_or_default();
    let passphrase = std::env::var("POLY_PASSPHRASE").unwrap_or_default();
    let auth = PolyAuth::new(&api_key, &secret, &passphrase, eoa)?;
    let headers = auth.sign_request("POST", "/order", &body_str);
    let pairs: Vec<(String, String)> = headers
        .as_pairs()
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let url = format!("{}/order", CLOB_URL);
    let client = crate::async_rt::http_client();
    let (status, text) = crate::async_rt::block_on_runtime(async move {
        let mut req = client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body_str);
        for (k, v) in &pairs {
            req = req.header(k.as_str(), v.as_str());
        }
        let resp = req.send().await.map_err(|e| anyhow!("POST /order: {}", e))?;
        let st = resp.status();
        let tx = resp.text().await.unwrap_or_default();
        Ok::<_, anyhow::Error>((st, tx))
    })?;

    println!("   POST /order → HTTP {} : {}", status, text);
    println!();
    println!("Read:");
    println!("  • 'not enough balance' / insufficient funds → #70 CLEARED (signer accepted);");
    println!("    remaining work is just funding the DW + wiring. GO for Phases 4+6.");
    println!("  • 'order signer address has to be the address of the API KEY' → #70 REAL;");
    println!("    the EOA key can't sign for the DW maker → deeper rework needed.");
    println!("  • 200/success → it RESTED (cancel it); type-3 fully works end-to-end.");
    Ok(())
}

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

/// Set the DW's v2 allowances in one WALLET batch:
///   - pUSD → CTF            (split/merge pUSD-direct on the CTF — legacy path)
///   - pUSD → ExchangeV2     (pay pUSD for BUY orders)
///   - pUSD → CtfCollateralAdapter (split/merge via the adapter → USDC.e-space
///                            tokens, the ones the CLOB actually trades/sells)
///   - CTF  → ExchangeV2     (setApprovalForAll: let the exchange move the DW's
///                            outcome tokens for SELL orders)
///   - CTF  → CtfCollateralAdapter (setApprovalForAll: adapter merge/redeem
///                            burns the DW's outcome tokens)
pub(crate) fn dw_approvals(
    key: &SigningKey, eoa: &str, dw: &str, builder_auth: &PolyAuth, dry_run: bool,
) -> Result<String> {
    let calls = vec![
        Call { target: PUSD_TOKEN.to_string(), data: approve_calldata(CTF_TOKEN) },
        Call { target: PUSD_TOKEN.to_string(), data: approve_calldata(EXCHANGE_V2) },
        Call { target: PUSD_TOKEN.to_string(), data: approve_calldata(CTF_COLLATERAL_ADAPTER) },
        Call { target: CTF_TOKEN.to_string(), data: set_approval_for_all_calldata(EXCHANGE_V2) },
        Call { target: CTF_TOKEN.to_string(), data: set_approval_for_all_calldata(CTF_COLLATERAL_ADAPTER) },
    ];
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
    // Mirrors `scripts/poly_wallet_info.py --resolve`. The authoritative
    // deposit-wallet signal is the on-chain `WalletDeployed` scan (keyed
    // by owner EOA); the Polymarket Gamma `/public-profile` API is a
    // SECONDARY signal — it's keyed by the proxy/wallet address (does NOT
    // reverse-resolve an EOA), so it only fires when the EOA is itself a
    // registered Polymarket wallet.
    if let Ok(dw) = find_existing_deposit_wallet(eoa) {
        println!("  Existing deposit wallet found on-chain (WalletDeployed log): {}", dw);
        println!("  → already exists; skipping WALLET-CREATE.");
        return Ok(dw);
    }
    if let Some(proxy) = gamma_public_profile_proxy(eoa) {
        if !proxy.eq_ignore_ascii_case(eoa) {
            println!("  Existing wallet found via Polymarket Gamma API (public-profile): {}", proxy);
            println!("  → already exists; skipping WALLET-CREATE.");
            return Ok(proxy);
        }
    }
    // ── Not found by either check → deploy ──
    println!("  No existing wallet found (on-chain scan + Gamma API) — deploying…");
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
/// ⚠ Gamma is keyed by the PROXY/wallet address, NOT the signer EOA — it
/// does not reverse-resolve an EOA to its deposit wallet (a fresh EOA
/// returns `proxyWallet: null`). So this only fires when the queried
/// address is itself a registered Polymarket wallet; it's a secondary
/// signal to the on-chain `WalletDeployed` scan (see `--resolve`).
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

/// `--test-onramp <usdce>`: wrap the deposit wallet's USDC.e → pUSD in one
/// WALLET batch — `USDC.e.approve(Onramp, ∞)` then
/// `Onramp.wrap(USDC.e, DW, amount)` (executed sequentially in the same
/// tx). The DW must already hold ≥`<usdce>` USDC.e.
fn test_onramp(
    key: &SigningKey,
    eoa: &str,
    dw: &str,
    amt_s: &str,
    dry_run: bool,
) -> Result<()> {
    let amount_usdce: f64 = amt_s
        .parse()
        .map_err(|_| anyhow!("--test-onramp needs a USDC.e amount, got '{}'", amt_s))?;
    let amount_wei = (amount_usdce * 1_000_000.0).round().max(0.0) as u128;
    println!();
    println!("── DW onramp: wrap {} USDC.e → pUSD via WALLET batch ──", amount_usdce);
    let builder_auth = load_builder_auth(eoa)?;
    dw_onramp(key, eoa, dw, &builder_auth, amount_wei, dry_run)?;
    if !dry_run {
        println!("   ✅ wrap confirmed — DW should now hold ~{} pUSD (check `hexbot positions` /", amount_usdce);
        println!("      the L2 balance probe). Ready to --test-split / fund trading.");
    }
    Ok(())
}

/// `--test-approvals`: set/confirm the DW's three v2 allowances via one
/// WALLET batch. Idempotent (re-approving ∞ is harmless). Also proves the
/// WALLET-batch mechanism end-to-end before we trust it for split/redeem.
fn test_approvals(key: &SigningKey, eoa: &str, dw: &str, dry_run: bool) -> Result<()> {
    println!();
    println!("── DW approvals via WALLET batch (pUSD→CTF, pUSD→ExchangeV2, CTF→ExchangeV2) ──");
    let builder_auth = load_builder_auth(eoa)?;
    dw_approvals(key, eoa, dw, &builder_auth, dry_run)?;
    if !dry_run {
        println!("   ✅ approvals batch confirmed — WALLET-batch path works for the DW.");
    }
    Ok(())
}

/// `--test-split <usdc>`: ONE `splitPosition` from the DW for the current
/// event, isolated, so we confirm the CTF/collateral path on-chain with a
/// tiny amount BEFORE wiring split into the live maintenance loop.
fn test_split(
    key: &SigningKey,
    eoa: &str,
    dw: &str,
    slug: &str,
    amt_s: &str,
    via_adapter: bool,
    collateral_sel: &str,
    dry_run: bool,
) -> Result<()> {
    let amount_usdc: f64 = amt_s
        .parse()
        .map_err(|_| anyhow!("--test-split needs a USDC amount, got '{}'", amt_s))?;
    let amount_wei = (amount_usdc * 1_000_000.0).round().max(0.0) as u128;

    // Routing: --via-adapter targets the CtfCollateralAdapter (mints USDC.e-space
    // tokens = clob_token_id, sellable); else CTF-direct (mints collateral-space).
    let (target, target_label) = if via_adapter {
        (CTF_COLLATERAL_ADAPTER, "CtfCollateralAdapter 0xAdA100")
    } else {
        (CTF_TOKEN, "CTF 0x4D97")
    };
    let (collateral, collateral_label) = match collateral_sel.to_ascii_lowercase().as_str() {
        "usdce" | "usdc" | "usdc.e" => (USDCE_TOKEN, "USDC.e"),
        _ => (PUSD_TOKEN, "pUSD"),
    };
    println!();
    println!("── DW test split {} via WALLET batch (target={}, collateralArg={}) ──",
        amount_usdc, target_label, collateral_label);
    if via_adapter {
        println!("   (requires `approve(pUSD→adapter)` — run `--test-approvals` first if unset)");
    }
    let (series_id, event) = super::market::fetch_active_event(slug)?;
    let market = event.markets.first()
        .ok_or_else(|| anyhow!("active event {} has no markets", series_id))?;
    let condition_id = market.condition_id.clone();
    if condition_id.is_empty() {
        return Err(anyhow!("active event {} has no condition_id", series_id));
    }
    println!("   series_id={} condition_id={}", series_id, condition_id);
    if !market.clob_token_ids.is_empty() {
        println!("   clob_token_ids (the SELLable tokens to match):");
        for (i, t) in market.clob_token_ids.iter().enumerate() {
            println!("     [{}] {}", i, t);
        }
    }

    let builder_auth = load_builder_auth(eoa)?;
    let calls = vec![Call {
        target: target.to_string(),
        data: split_position_calldata(collateral, &condition_id, amount_wei),
    }];
    submit_wallet_batch(key, eoa, dw, &builder_auth, &calls, now_secs()?, dry_run)?;
    println!();
    println!("Verify which token got minted:");
    println!("  hexbot token_check {} <up_clob_token_id> <down_clob_token_id>", condition_id);
    println!("  then check the DW's on-chain ERC1155 balanceOf for each clob_token_id.");
    println!("  • DW now holds {0} of the clob_token_id → this (target,collateral) is CORRECT;",
        amount_usdc);
    println!("    wire it into dw_split. (Sells will then see real balance.)");
    println!("  • DW holds 0 of clob_token_id (minted a different positionId) or reverted →");
    println!("    wrong (target,collateral); try the other --collateral / drop --via-adapter.");
    Ok(())
}

/// `--test-redeem <conditionId>`: ONE isolated `redeemPositions` FROM the DW for
/// a RESOLVED condition, to verify redeem-via-adapter before wiring it into the
/// live maintenance loop. Reuses `--via-adapter` (target adapter vs CTF) and
/// `--collateral usdce|pusd` (the collateralToken arg).
fn test_redeem(
    key: &SigningKey,
    eoa: &str,
    dw: &str,
    cid_arg: &str,
    via_adapter: bool,
    collateral_sel: &str,
    dry_run: bool,
) -> Result<()> {
    let condition_id = cid_arg.trim();
    if !(condition_id.starts_with("0x") && condition_id.len() == 66) {
        return Err(anyhow!(
            "--test-redeem needs a 0x conditionId (32 bytes / 66 chars), got '{}'", condition_id
        ));
    }
    let (target, target_label) = if via_adapter {
        (CTF_COLLATERAL_ADAPTER, "CtfCollateralAdapter 0xAdA100")
    } else {
        (CTF_TOKEN, "CTF 0x4D97")
    };
    let (collateral, collateral_label) = match collateral_sel.to_ascii_lowercase().as_str() {
        "usdce" | "usdc" | "usdc.e" => (USDCE_TOKEN, "USDC.e"),
        _ => (PUSD_TOKEN, "pUSD"),
    };
    println!();
    println!("── DW test redeem cid={} via WALLET batch (target={}, collateralArg={}) ──",
        condition_id, target_label, collateral_label);
    if via_adapter {
        println!("   (adapter redeem burns the DW's outcome tokens → needs");
        println!("    setApprovalForAll(CTF→adapter); run `--test-approvals` first if unset)");
    }
    println!("   NOTE: the condition MUST be RESOLVED on-chain — redeemPositions reverts otherwise.");

    let builder_auth = load_builder_auth(eoa)?;
    let calls = vec![Call {
        target: target.to_string(),
        data: redeem_calldata(collateral, condition_id),
    }];
    submit_wallet_batch(key, eoa, dw, &builder_auth, &calls, now_secs()?, dry_run)?;
    println!();
    println!("Verify:");
    println!("  hexbot token_check {} <up_clob> <down_clob> --wallet {}", condition_id, dw);
    println!("  • CONFIRMED + the DW's clob_token_id balanceOf → 0 + pUSD balance ↑");
    println!("    → redeem-via-adapter is CORRECT → safe to wire into dw_redeem.");
    println!("  • reverted / relayer 500 → adapter redeem needs a different approval/collateral;");
    println!("    report it and we adjust (or fall back to CTF-direct USDC.e redeem).");
    Ok(())
}

// ════════════════════════════════════════════════════════════════
// ERC-7739-wrapped L1 ClobAuth (the #70 candidate fix)
// ════════════════════════════════════════════════════════════════

/// Returns `(digest, wrapped_signature_hex)`.
///
/// Mirrors `sign_poly1271_order` but wraps the L1 `ClobAuth` struct
/// instead of an `Order`, under the `ClobAuthDomain` app domain:
///
/// 1. `contents = ClobAuth{address: depositWallet, timestamp, nonce, message}`
/// 2. `tdsHash = keccak256(abi.encode(
///        keccak256(SOLADY_TYPE_STRING), contents_hash,
///        keccak256("DepositWallet"), keccak256("1"),
///        chainId, depositWallet /*verifyingContract*/, 0 /*salt*/))`
/// 3. `digest = keccak256(0x1901 || clobAuthDomainSep || tdsHash)`
/// 4. `inner = EOA.sign(digest)` (65-byte ECDSA, v=27/28)
/// 5. `wrapped = 0x || inner || clobAuthDomainSep || contents_hash ||
///        CLOB_AUTH_TYPE_STRING || uint16(len(CLOB_AUTH_TYPE_STRING))`
fn wrapped_clob_auth_signature(
    key: &SigningKey,
    deposit_wallet: &str,
    timestamp: &str,
    nonce: u64,
) -> ([u8; 32], String) {
    let contents_hash = clob_auth_contents_hash(deposit_wallet, timestamp, nonce);
    let app_domain_sep = clob_auth_domain_separator();

    // typed_data_sign struct hash (all 7 fields are static 32-byte words).
    let tds_hash = keccak256(&abi_encode_words(&[
        keccak256(SOLADY_CLOB_AUTH_TYPE_STRING.as_bytes()),
        contents_hash,
        keccak256(DEPOSIT_WALLET_NAME.as_bytes()),
        keccak256(DEPOSIT_WALLET_VERSION.as_bytes()),
        u256_bytes(CHAIN_ID as u128),
        address_to_bytes32(deposit_wallet),
        [0u8; 32],
    ]));

    let digest = eip712_digest(&app_domain_sep, &tds_hash);

    let (sig, recid) = key
        .sign_prehash_recoverable(&digest)
        .expect("prehash sign");
    let mut inner = [0u8; 65];
    inner[..64].copy_from_slice(&sig.to_bytes());
    inner[64] = recid.to_byte() + 27;

    let type_string = CLOB_AUTH_TYPE_STRING.as_bytes();
    let type_len = u16::try_from(type_string.len()).expect("type string fits u16");

    let mut wrapped = String::from("0x");
    wrapped.push_str(&hex::encode(inner));
    wrapped.push_str(&hex::encode(app_domain_sep));
    wrapped.push_str(&hex::encode(contents_hash));
    wrapped.push_str(&hex::encode(type_string));
    wrapped.push_str(&hex::encode(type_len.to_be_bytes()));

    (digest, wrapped)
}

/// Standard (un-wrapped) L1 `ClobAuth` signature — exactly what the
/// working type-2 `derive_api_credentials` produces: sign the EIP-712
/// digest directly with the EOA key, v=27/28. `clob_auth_address` is the
/// value placed in the `ClobAuth.address` field (and should match
/// `POLY_ADDRESS`).
fn unwrapped_clob_auth_signature(
    key: &SigningKey,
    clob_auth_address: &str,
    timestamp: &str,
    nonce: u64,
) -> String {
    let struct_hash = clob_auth_contents_hash(clob_auth_address, timestamp, nonce);
    let digest = eip712_digest(&clob_auth_domain_separator(), &struct_hash);
    let (sig, recid) = key.sign_prehash_recoverable(&digest).expect("prehash sign");
    let mut bytes = [0u8; 65];
    bytes[..64].copy_from_slice(&sig.to_bytes());
    bytes[64] = recid.to_byte() + 27;
    format!("0x{}", hex::encode(bytes))
}

/// EIP-712 struct hash of `ClobAuth{address,timestamp,nonce,message}`.
/// `address` here is whatever the caller passes (deposit wallet or EOA).
fn clob_auth_contents_hash(deposit_wallet: &str, timestamp: &str, nonce: u64) -> [u8; 32] {
    keccak256(&abi_encode_words(&[
        keccak256(CLOB_AUTH_TYPE_STRING.as_bytes()),
        address_to_bytes32(deposit_wallet),
        keccak256(timestamp.as_bytes()),
        u256_bytes(nonce as u128),
        keccak256(CLOB_AUTH_MESSAGE.as_bytes()),
    ]))
}

/// `ClobAuthDomain` separator: name + version + chainId (no verifyingContract).
fn clob_auth_domain_separator() -> [u8; 32] {
    keccak256(&abi_encode_words(&[
        keccak256(b"EIP712Domain(string name,string version,uint256 chainId)"),
        keccak256(CLOB_AUTH_DOMAIN_NAME.as_bytes()),
        keccak256(CLOB_AUTH_VERSION.as_bytes()),
        u256_bytes(CHAIN_ID as u128),
    ]))
}

// ════════════════════════════════════════════════════════════════
// CLOB auth call (derive / create)
// ════════════════════════════════════════════════════════════════

fn call_api_key(
    deposit_wallet: &str,
    timestamp: &str,
    nonce: u64,
    wrapped_sig: &str,
    create: bool,
) -> Result<serde_json::Value> {
    let (method, path) = if create {
        (reqwest::Method::POST, "/auth/api-key")
    } else {
        (reqwest::Method::GET, "/auth/derive-api-key")
    };
    let url = format!("{}{}", CLOB_URL, path);
    let addr = deposit_wallet.to_string();
    let sig = wrapped_sig.to_string();
    let ts = timestamp.to_string();
    let nonce_s = nonce.to_string();
    let client = crate::async_rt::http_client();
    crate::async_rt::block_on_runtime(async move {
        let req = client
            .request(method, &url)
            .header("POLY_ADDRESS", addr)
            .header("POLY_SIGNATURE", sig)
            .header("POLY_TIMESTAMP", ts)
            .header("POLY_NONCE", nonce_s);
        let resp = req.send().await.map_err(|e| anyhow!("request: {}", e))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(anyhow!("HTTP {} — {}", status, text));
        }
        serde_json::from_str::<serde_json::Value>(&text)
            .map_err(|e| anyhow!("parse: {} (body={})", e, text))
    })
}

fn report_creds(which: &str, json: &serde_json::Value, poly_address: &str) {
    let key = json.get("apiKey").and_then(|v| v.as_str()).unwrap_or("");
    if key.is_empty() {
        println!("   {} → 200 but no apiKey: {}", which, json);
        return;
    }
    println!("   ✅ {} → 200, apiKey={} (bound to POLY_ADDRESS={})", which, key, poly_address);
}

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

fn load_builder_auth(signer_eoa: &str) -> Result<PolyAuth> {
    let key = std::env::var("POLY_BUILDER_API_KEY").unwrap_or_default();
    let secret = std::env::var("POLY_BUILDER_SECRET").unwrap_or_default();
    let passphrase = std::env::var("POLY_BUILDER_PASSPHRASE").unwrap_or_default();
    if key.is_empty() || secret.is_empty() || passphrase.is_empty() {
        return Err(anyhow!(
            "deploy path needs builder credentials (POLY_BUILDER_API_KEY / _SECRET / \
             _PASSPHRASE). Either add a [builder] block to the secrets file, or pass an \
             already-deployed --deposit-wallet <addr> to skip deployment."
        ));
    }
    PolyAuth::new(&key, &secret, &passphrase, signer_eoa)
}

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

fn parse_private_key(input: &str) -> Result<SigningKey> {
    let clean = input.strip_prefix("0x").unwrap_or(input);
    let bytes = hex::decode(clean).map_err(|e| anyhow!("private key hex: {}", e))?;
    SigningKey::from_bytes(bytes.as_slice().into()).map_err(|e| anyhow!("private key: {}", e))
}

fn flag_value(args: &[String], flag: &str) -> Option<String> {
    args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1)).cloned()
}

fn current_unix_secs() -> Result<String> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| anyhow!("clock: {}", e))?
        .as_secs()
        .to_string())
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

#[cfg(test)]
mod tests {
    use super::*;

    // Hardhat account #0 — deterministic, widely published.
    const HARDHAT_KEY: &str = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
    const DEPOSIT_WALLET: &str = "0x000000000000000000000000000000000000dEaD";

    fn key() -> SigningKey {
        parse_private_key(HARDHAT_KEY).unwrap()
    }

    /// The wrapped signature must follow Solady's ERC-7739 layout:
    /// `inner(65) || appDomainSep(32) || contentsHash(32) || contentsType ||
    /// uint16(len(contentsType))`, and be deterministic for fixed inputs.
    #[test]
    fn wrapped_clob_auth_layout_and_determinism() {
        let (_d1, s1) = wrapped_clob_auth_signature(&key(), DEPOSIT_WALLET, "1700000000", 0);
        let (_d2, s2) = wrapped_clob_auth_signature(&key(), DEPOSIT_WALLET, "1700000000", 0);
        assert_eq!(s1, s2, "deterministic for fixed inputs");
        assert!(s1.starts_with("0x"));

        let bytes = hex::decode(s1.strip_prefix("0x").unwrap()).unwrap();
        let type_str = CLOB_AUTH_TYPE_STRING.as_bytes();
        let expected_len = 65 + 32 + 32 + type_str.len() + 2;
        assert_eq!(bytes.len(), expected_len, "ERC-7739 wrapped layout length");

        // Trailing uint16 = contentsType length (big-endian).
        let tail = &bytes[bytes.len() - 2..];
        assert_eq!(u16::from_be_bytes([tail[0], tail[1]]) as usize, type_str.len());

        // The contentsType string sits just before that uint16.
        let type_start = 65 + 32 + 32;
        assert_eq!(&bytes[type_start..type_start + type_str.len()], type_str);

        // The appDomainSep embedded in the wrapper matches our domain fn.
        assert_eq!(&bytes[65..65 + 32], clob_auth_domain_separator().as_slice());
    }

    /// `ClobAuth.address` is bound to the deposit wallet (the #70 fix), so
    /// changing the deposit wallet must change the contents hash + sig.
    #[test]
    fn binds_to_deposit_wallet_not_eoa() {
        let (_d, a) = wrapped_clob_auth_signature(&key(), DEPOSIT_WALLET, "1700000000", 0);
        let other = "0x0000000000000000000000000000000000001234";
        let (_d2, b) = wrapped_clob_auth_signature(&key(), other, "1700000000", 0);
        assert_ne!(a, b, "signature must depend on the deposit wallet address");
    }
}
