//! Polymarket Safe wallet deployment, **v2** token approvals, and API
//! credential generation — per-instance.
//!
//! `hexbot deploy_wallet --instance <id> --config <path>`
//!
//! Three-step setup flow (each step is idempotent — re-running skips work
//! already done):
//! 1. Deploy a Gnosis Safe proxy wallet via Builder Relayer API
//!    (skipped if the Safe is already deployed on-chain).
//! 2. Grant the full **v2 CLOB** approval set (pUSD/CTF → v2 Exchanges +
//!    collateral adapters — the same checklist as `hexbot approve_v2`).
//!    Each allowance already on-chain is skipped.
//! 3. Derive CLOB API credentials (derive_api_key via L1 EIP-712 auth).
//!
//! The resulting credentials (api_key / api_secret / api_passphrase /
//! private_key / signature_type) are written to the `[poly.<id>]` block of
//! the secrets file named by the `--config`'s `general.secrets_file`,
//! preserving every other instance's block. Builder credentials
//! (`POLY_BUILDER_*`) and `POLYGON_RPC` are still read from `.env`.
//!
//! References:
//! - <https://docs.polymarket.com/market-makers/getting-started>
//! - <https://github.com/Polymarket/safe-wallet-integration>

use std::io::Write;

use anyhow::{anyhow, Result};
use k256::ecdsa::SigningKey;
use sha3::{Digest, Keccak256};

use super::auth::PolyAuth;

// ════════════════════════════════════════════════════════════════
// Constants (Polygon mainnet, chain ID 137)
// ════════════════════════════════════════════════════════════════

const RELAYER_URL: &str = "https://relayer-v2.polymarket.com";
const CLOB_URL: &str = "https://clob.polymarket.com";
const CHAIN_ID: u64 = 137;

// Contracts. Safe factory + init-code hash drive CREATE2 address
// derivation + deploy; CTF/Exchange/collateral addresses for the v2
// approval set live in `approve_v2.rs` (single source of truth).
const SAFE_FACTORY: &str = "0xaacFeEa03eb1561C4e67d661e40682Bd20E3541b";
const SAFE_INIT_CODE_HASH: &str = "0x2bce2127ff07fb632d16c8347c4ebf501f4841168bed00d9e6ef715ddb6fcecf";
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

// EIP-712 domains
const SAFE_CREATE_DOMAIN_NAME: &str = "Polymarket Contract Proxy Factory";
const CLOB_AUTH_DOMAIN_NAME: &str = "ClobAuthDomain";
const CLOB_AUTH_MESSAGE: &str = "This message attests that I control the given wallet";

// Function selectors — read-only allowance checks (the approve /
// setApprovalForAll write selectors moved to `approve_v2.rs`).
const ERC20_ALLOWANCE_SELECTOR: [u8; 4] = [0xdd, 0x62, 0xed, 0x3e]; // allowance(address,address)
const ERC1155_IS_APPROVED_SELECTOR: [u8; 4] = [0xe9, 0x85, 0xe9, 0xc5]; // isApprovedForAll(address,address)

// Polygon RPC endpoints are sourced exclusively from `$POLYGON_RPC`
// (operator's `.env`). The public-fallback array that used to live here
// was removed — public endpoints churn (polygon-rpc.com now 401,
// BlastAPI shut down, llamarpc DNS gone, etc.) and a stale list is
// worse than no list. For HA, set `POLYGON_RPC` to a paid provider
// (Alchemy / QuickNode / paid Infura).

// ════════════════════════════════════════════════════════════════
// CLI entry point
// ════════════════════════════════════════════════════════════════

pub fn run_deploy_wallet() -> Result<()> {
    use crate::exchange::polymarket::cli_account;

    // ── Resolve the target secrets block from --instance / --config ──
    // deploy_wallet WRITES the `[poly.<id>]` block, so (unlike every other
    // subcommand) it can't go through `cli_account::resolve_and_apply`,
    // which requires the block to already exist. It resolves the
    // secrets-file path itself from the config's `general.secrets_file`.
    // `--instance` is optional: a config with exactly one strategy
    // auto-resolves; multiple require an explicit --instance.
    let instance_cli = cli_account::instance_id();
    let config_path = cli_account::config_path().unwrap_or_default();
    let (instance_id, secrets_path) =
        cli_account::resolve_secrets_write_path(&instance_cli, &config_path)?;

    // Push the shared `[builder]`/`[polygon]`/`[chainlink]` secrets into env so
    // the relayer-auth creds (read just below) + `POLYGON_RPC` resolve. The
    // `--config` path already did this via `Config::load` (→ apply_shared_to_env),
    // but the `--account` path resolves only the write-path and skips Config::load,
    // so the shared blocks would otherwise never reach the env (deploy_wallet then
    // reports the builder creds "missing" even when `[builder]` is present). The
    // per-instance `[poly.<id>]` block is intentionally NOT required here —
    // deploy_wallet CREATES it.
    // A missing file → Ok(default) (handled inside load); a malformed file →
    // Err, which we propagate (clearer than the downstream "missing builder
    // creds" once the shared blocks fail to load).
    crate::config::SecretsFile::load(&secrets_path)?.apply_shared_to_env();

    // ── Builder credentials (relayer auth — required for deploy +
    //    gasless approvals) ──
    let builder_key = std::env::var("POLY_BUILDER_API_KEY").unwrap_or_default();
    let builder_secret = std::env::var("POLY_BUILDER_SECRET").unwrap_or_default();
    let builder_passphrase = std::env::var("POLY_BUILDER_PASSPHRASE").unwrap_or_default();
    let mut missing = Vec::new();
    if builder_key.is_empty() { missing.push("POLY_BUILDER_API_KEY"); }
    if builder_secret.is_empty() { missing.push("POLY_BUILDER_SECRET"); }
    if builder_passphrase.is_empty() { missing.push("POLY_BUILDER_PASSPHRASE"); }
    if !missing.is_empty() {
        eprintln!("Error: Missing required builder credentials:");
        for var in &missing { eprintln!("  - {}", var); }
        eprintln!();
        eprintln!("Add a [builder] section to the secrets file ({})", secrets_path.display());
        eprintln!("(obtain the values from your Polymarket Builder Profile):");
        eprintln!("  [builder]");
        eprintln!("  api_key        = \"<key>\"");
        eprintln!("  api_secret     = \"<secret>\"");
        eprintln!("  api_passphrase = \"<passphrase>\"");
        return Err(anyhow!("Missing builder credentials"));
    }

    println!("=== Polymarket Safe Wallet Setup (v2) ===");
    println!();
    println!("Instance:  {}", instance_id);
    println!("Config:    {}", config_path);
    println!("Secrets:   {}", secrets_path.display());

    // ── Overwrite guard: confirm if the block already exists ──
    if let Some(existing_key) = peek_poly_api_key(&secrets_path, &instance_id) {
        println!();
        println!("⚠  [poly.{}] already exists (api_key={}).",
            instance_id, mask_value(&existing_key));
        println!("   Continuing will OVERWRITE it with freshly derived credentials.");
        print!("   Type the instance id ('{}') to confirm overwrite: ", instance_id);
        std::io::stdout().flush()?;
        let mut confirm = String::new();
        std::io::stdin().read_line(&mut confirm)?;
        if confirm.trim() != instance_id {
            return Err(anyhow!("overwrite not confirmed — aborting (nothing written)"));
        }
    }

    // ── Private key OR mnemonic ──
    println!();
    println!("Enter the signer wallet private key (0x-hex / 64 hex chars)");
    println!("  OR a BIP39 mnemonic (12/15/18/21/24 words, space-separated):");
    print!("> ");
    std::io::stdout().flush()?;
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    let input = input.trim();
    if input.is_empty() {
        return Err(anyhow!("No input provided"));
    }

    let signing_key = parse_key_or_mnemonic(input)?;
    let signer_address = super::signer::derive_eth_address_from_key(&signing_key);
    let safe_address = derive_safe_address(&signer_address);
    let private_key_hex = format!("0x{}", hex::encode(signing_key.to_bytes()));

    println!();
    println!("Signer (EOA) address: {}", signer_address);
    println!("Safe proxy address:   {}", safe_address);

    let builder_auth = PolyAuth::new(&builder_key, &builder_secret, &builder_passphrase, &signer_address)?;

    // Wallet flavour: default = deposit wallet (POLY_1271); `--gnosis-safe`
    // keeps the legacy Safe flow for accounts not migrating.
    let legacy_safe = std::env::args().any(|a| a == "--gnosis-safe");

    let (signature_type, funder): (&str, String) = if legacy_safe {
        // ── Step 1: Deploy Safe (skip if already deployed) ──
        println!();
        println!("Step 1/3: Deploy Safe wallet");
        print!("  Checking deployment status... ");
        std::io::stdout().flush()?;
        let deployed = check_deployed(&builder_auth, &safe_address).unwrap_or(false);
        if deployed {
            println!("already deployed — skipping.");
        } else {
            println!("not deployed.");
            print!("  Deploying... ");
            std::io::stdout().flush()?;
            let result = deploy_safe(&builder_auth, &signing_key, &signer_address, &safe_address)?;
            println!("submitted ({})", result);
            println!("  Waiting for deployment confirmation...");
            std::thread::sleep(std::time::Duration::from_secs(5));
            let confirmed = check_deployed(&builder_auth, &safe_address).unwrap_or(false);
            if confirmed {
                println!("  Deployment confirmed.");
            } else {
                println!("  Deployment may still be pending. Continuing with approvals...");
            }
        }

        // ── Step 2: v2 approvals (skip each that's already on-chain) ──
        println!();
        println!("Step 2/3: Approve tokens (v2 CLOB)");
        let steps = super::approve_v2::v2_approval_steps();
        let mut sent = 0usize;
        for (i, step) in steps.iter().enumerate() {
            use super::approve_v2::ApprovalKind;
            print!("  {}/{} {:<34} ", i + 1, steps.len(), step.label);
            std::io::stdout().flush()?;
            let already = match step.kind {
                ApprovalKind::Erc20Approve => check_erc20_allowance(&safe_address, step.token, step.spender),
                ApprovalKind::Erc1155Set   => check_erc1155_approval(&safe_address, step.token, step.spender),
            };
            if already {
                println!("already approved — skipping.");
                continue;
            }
            let calldata = match step.kind {
                ApprovalKind::Erc20Approve => super::approve_v2::build_approve_calldata(step.spender),
                ApprovalKind::Erc1155Set   => super::approve_v2::build_set_approval_for_all_calldata(step.spender),
            };
            print!("approving... ");
            std::io::stdout().flush()?;
            submit_safe_tx(&builder_auth, &signing_key, &signer_address, &safe_address, step.token, &calldata)?;
            println!("done.");
            sent += 1;
        }
        if sent == 0 {
            println!("  All v2 approvals already set — skipped.");
        }
        ("gnosis_safe", String::new())
    } else {
        // ── Step 1: Deposit wallet (resolve existing or deploy) ──
        println!();
        println!("Step 1/3: Deposit wallet (POLY_1271) — resolve or deploy");
        let dw = super::deposit_wallet::ensure_deposit_wallet(&builder_auth, &signer_address)?;
        println!("  Deposit wallet: {}", dw);

        // ── Step 2: Deposit-wallet allowances (WALLET batch) ──
        println!();
        println!("Step 2/3: Approve deposit-wallet allowances (pUSD→CTF/ExchangeV2/Adapter, CTF→ExchangeV2/Adapter/AutoRedeemer)");
        super::deposit_wallet::dw_approvals(&signing_key, &signer_address, &dw, &builder_auth, /*dry_run=*/ false)?;
        println!("  done.");
        ("poly_1271", dw)
    };

    // ── Step 3: Generate API Credentials ──
    println!();
    println!("Step 3/3: Generate API credentials");
    print!("  Deriving API key... ");
    std::io::stdout().flush()?;
    let creds = derive_api_credentials(&signing_key, &signer_address)?;
    println!("done.");
    println!("  API Key:    {}", creds.api_key);
    println!("  Passphrase: {}", mask_value(&creds.passphrase));

    // ── Write to the secrets file [poly.<instance_id>] ──
    println!();
    println!("Writing credentials to {} [poly.{}] ...", secrets_path.display(), instance_id);
    let overwritten = write_poly_secrets(&secrets_path, &instance_id, &PolySecretsWrite {
        api_key: &creds.api_key,
        api_secret: &creds.secret,
        api_passphrase: &creds.passphrase,
        private_key: &private_key_hex,
        signature_type,
        funder: &funder,
    })?;
    println!("Done — {} [poly.{}].", if overwritten { "overwrote" } else { "created" }, instance_id);

    println!();
    println!("=== Setup Complete (v2) ===");
    println!();
    println!("The strategy instance `{}` in {} will load these credentials automatically.",
        instance_id, config_path);
    println!("CLI ops on this wallet:");
    println!("  hexbot positions --instance {} --config {}", instance_id, config_path);

    Ok(())
}

// ════════════════════════════════════════════════════════════════
// Step 1: Safe Deployment
// ════════════════════════════════════════════════════════════════

/// Derive Safe address via CREATE2.
pub fn derive_safe_address(signer_address: &str) -> String {
    let signer_bytes = decode_address(signer_address);
    let mut padded = [0u8; 32];
    padded[12..].copy_from_slice(&signer_bytes);
    let salt = keccak256(&padded);

    let factory_bytes = decode_address(SAFE_FACTORY);
    let init_hash = hex::decode(
        SAFE_INIT_CODE_HASH.strip_prefix("0x").unwrap_or(SAFE_INIT_CODE_HASH)
    ).unwrap_or_default();

    let mut buf = Vec::with_capacity(1 + 20 + 32 + 32);
    buf.push(0xff);
    buf.extend_from_slice(&factory_bytes);
    buf.extend_from_slice(&salt);
    buf.extend_from_slice(&init_hash);
    let hash = keccak256(&buf);
    format!("0x{}", hex::encode(&hash[12..]))
}

/// Helper: run a builder-signed relayer request (GET or POST) synchronously
/// via the shared async runtime + h2 client. Returns parsed JSON on 2xx.
fn relayer_request(
    method: reqwest::Method,
    url: String,
    headers: super::auth::AuthHeaders,
    body: Option<String>,
) -> Result<serde_json::Value> {
    let client = crate::async_rt::http_client();
    crate::async_rt::block_on_runtime(async move {
        let mut req = client.request(method, &url);
        for (k, v) in headers.as_builder_pairs() {
            req = req.header(k, v);
        }
        if let Some(b) = body {
            req = req.header("Content-Type", "application/json").body(b);
        }
        let resp = req.send().await
            .map_err(|e| anyhow!("{} failed: {}", url, e))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(anyhow!("{} failed ({}): {}", url, status, text));
        }
        serde_json::from_str(&text)
            .map_err(|e| anyhow!("parse {} response: {} (body={})", url, e, text))
    })
}

/// Check whether the Safe proxy at `safe_address` is deployed on-chain.
///
/// Order of authority:
///   1. **`eth_getCode(safe_address)`** — the chain itself. Any non-empty
///      bytecode at the address means the proxy was deployed (regardless
///      of which factory / version / migration path produced it).
///      This is the canonical source of truth and is unaffected by
///      relayer-side caching / endpoint semantics.
///   2. Polymarket relayer `GET /deployed?address=...` — fallback only
///      if the on-chain RPC is unreachable. Some post-cutover Safes
///      (especially v2-migrated wallets) are not surfaced by the
///      relayer's deployed-flag endpoint even when they exist on-chain,
///      so trusting the relayer alone produces false negatives.
pub fn check_deployed(auth: &PolyAuth, safe_address: &str) -> Result<bool> {
    // 1. Authoritative: query chain bytecode.
    let params = serde_json::json!([safe_address, "latest"]);
    match super::onchain_tx::rpc_call("eth_getCode", params) {
        Ok(v) => {
            if let Some(code) = v.get("result").and_then(|r| r.as_str()) {
                let trimmed = code.strip_prefix("0x").unwrap_or(code);
                // "0x" or "" or all zeros → no contract deployed.
                let deployed = !trimmed.is_empty()
                    && trimmed.chars().any(|c| c != '0');
                return Ok(deployed);
            }
            log::warn!(
                "[check_deployed] eth_getCode returned no result field, falling back to relayer ({})",
                v,
            );
        }
        Err(e) => {
            log::warn!(
                "[check_deployed] eth_getCode RPC failed, falling back to relayer: {}",
                e,
            );
        }
    }

    // 2. Fallback: relayer's /deployed endpoint.
    let path = format!("/deployed?address={}", safe_address);
    let headers = auth.sign_request("GET", &path, "");
    let url = format!("{}{}", RELAYER_URL, path);
    let json = relayer_request(reqwest::Method::GET, url, headers, None)?;
    Ok(json.get("deployed").and_then(|v| v.as_bool()).unwrap_or(false))
}

fn deploy_safe(auth: &PolyAuth, key: &SigningKey, signer: &str, safe: &str) -> Result<String> {
    // EIP-712: CreateProxy — domain has name + chainId + verifyingContract (no version)
    let domain_sep = build_domain_separator_no_version(
        SAFE_CREATE_DOMAIN_NAME, CHAIN_ID, SAFE_FACTORY);

    let struct_type_hash = keccak256(
        b"CreateProxy(address paymentToken,uint256 payment,address paymentReceiver)");
    let mut struct_buf = Vec::with_capacity(4 * 32);
    struct_buf.extend_from_slice(&struct_type_hash);
    struct_buf.extend_from_slice(&address_to_bytes32(ZERO_ADDRESS));
    struct_buf.extend_from_slice(&u256_bytes(0));
    struct_buf.extend_from_slice(&address_to_bytes32(ZERO_ADDRESS));
    let struct_hash = keccak256(&struct_buf);

    let signature = sign_eip712(&domain_sep, &struct_hash, key)?;

    let body = serde_json::json!({
        "from": signer,
        "to": SAFE_FACTORY.to_lowercase(),
        "proxyWallet": safe,
        "data": "0x",
        "signature": signature,
        "signatureParams": {
            "paymentToken": ZERO_ADDRESS,
            "payment": "0",
            "paymentReceiver": ZERO_ADDRESS,
        },
        "type": "SAFE-CREATE",
    });

    let body_str = body.to_string();
    eprintln!("[DEBUG] deploy body: {}", serde_json::to_string_pretty(&body).unwrap_or_default());
    let headers = auth.sign_request("POST", "/submit", &body_str);
    let url = format!("{}/submit", RELAYER_URL);
    let json = relayer_request(reqwest::Method::POST, url, headers, Some(body_str))?;
    let tx_id = json.get("transactionID").and_then(|v| v.as_str()).unwrap_or("?");
    let state = json.get("state").and_then(|v| v.as_str()).unwrap_or("?");
    Ok(format!("txID={}, state={}", tx_id, state))
}

// ════════════════════════════════════════════════════════════════
// Step 2: Token Approvals
// ════════════════════════════════════════════════════════════════
//
// The v2 approval set (pUSD/CTF → v2 Exchanges + collateral adapters) is
// defined once in `approve_v2::v2_approval_steps()`; `run_deploy_wallet`
// iterates it directly. The old v1 `approve_erc20` / `approve_erc1155`
// helpers (USDC.e → CTF, CTF → v1 Exchange) were removed at the v2
// cutover.

/// Submit a Safe transaction via the relayer.
pub fn submit_safe_tx(
    auth: &PolyAuth, key: &SigningKey, signer: &str, safe: &str,
    to: &str, data: &str,
) -> Result<()> {
    // Get nonce
    let nonce = get_safe_nonce(auth, safe)?;

    // Build Safe transaction hash (EIP-712)
    let domain_sep = build_safe_tx_domain_separator(safe);

    let safe_tx_type_hash = keccak256(
        b"SafeTx(address to,uint256 value,bytes data,uint8 operation,uint256 safeTxGas,uint256 baseGas,uint256 gasPrice,address gasToken,address refundReceiver,uint256 nonce)");

    let data_bytes = hex::decode(data.strip_prefix("0x").unwrap_or(data)).unwrap_or_default();
    let data_hash = keccak256(&data_bytes);

    let mut struct_buf = Vec::with_capacity(11 * 32);
    struct_buf.extend_from_slice(&safe_tx_type_hash);
    struct_buf.extend_from_slice(&address_to_bytes32(to));   // to
    struct_buf.extend_from_slice(&u256_bytes(0));             // value
    struct_buf.extend_from_slice(&data_hash);                 // data (hashed)
    struct_buf.extend_from_slice(&u256_bytes(0));             // operation (CALL)
    struct_buf.extend_from_slice(&u256_bytes(0));             // safeTxGas
    struct_buf.extend_from_slice(&u256_bytes(0));             // baseGas
    struct_buf.extend_from_slice(&u256_bytes(0));             // gasPrice
    struct_buf.extend_from_slice(&address_to_bytes32(ZERO_ADDRESS)); // gasToken
    struct_buf.extend_from_slice(&address_to_bytes32(ZERO_ADDRESS)); // refundReceiver
    struct_buf.extend_from_slice(&u256_bytes(nonce as u128)); // nonce
    let struct_hash = keccak256(&struct_buf);

    let digest = eip712_digest(&domain_sep, &struct_hash);
    eprintln!("[DEBUG] SafeTx EIP-712:");
    eprintln!("  domain_sep:  0x{}", hex::encode(domain_sep));
    eprintln!("  struct_hash: 0x{}", hex::encode(struct_hash));
    eprintln!("  digest:      0x{}", hex::encode(digest));
    eprintln!("  to:          {}", to);
    eprintln!("  data_hash:   0x{}", hex::encode(data_hash));
    eprintln!("  nonce:       {}", nonce);

    let signature = sign_eip712_safe(&domain_sep, &struct_hash, key)?;

    let body = serde_json::json!({
        "from": to_checksum_address(signer),
        "to": to_checksum_address(to),
        "proxyWallet": to_checksum_address(safe),
        "value": "0",
        "data": data,
        "nonce": nonce.to_string(),
        "signature": signature,
        "signatureParams": {
            "gasPrice": "0",
            "operation": "0",
            "safeTxnGas": "0",
            "baseGas": "0",
            "gasToken": ZERO_ADDRESS,
            "refundReceiver": ZERO_ADDRESS,
        },
        "type": "SAFE",
        "metadata": "",
    });

    let body_str = body.to_string();
    eprintln!("[DEBUG] nonce={} approval body: {}", nonce, serde_json::to_string_pretty(&body).unwrap_or_default());
    let headers = auth.sign_request("POST", "/submit", &body_str);
    let url = format!("{}/submit", RELAYER_URL);
    let json = relayer_request(reqwest::Method::POST, url, headers, Some(body_str))?;
    let state = json.get("state").and_then(|v| v.as_str()).unwrap_or("?");
    if state == "STATE_NEW" || state.contains("SUBMITTED") {
        // Wait briefly for confirmation
        std::thread::sleep(std::time::Duration::from_secs(3));
    }
    Ok(())
}

pub fn get_safe_nonce(auth: &PolyAuth, safe: &str) -> Result<u64> {
    let path = format!("/nonce?address={}&type=SAFE", safe);
    let headers = auth.sign_request("GET", &path, "");
    let url = format!("{}{}", RELAYER_URL, path);
    let json = relayer_request(reqwest::Method::GET, url, headers, None)?;
    let nonce = json.get("nonce")
        .and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
        .unwrap_or(0);
    Ok(nonce)
}

pub(super) fn build_safe_tx_domain_separator(safe_address: &str) -> [u8; 32] {
    // GnosisSafe domain: no name/version, just chainId + verifyingContract
    let type_hash = keccak256(b"EIP712Domain(uint256 chainId,address verifyingContract)");
    let mut buf = Vec::with_capacity(3 * 32);
    buf.extend_from_slice(&type_hash);
    buf.extend_from_slice(&u256_bytes(CHAIN_ID as u128));
    buf.extend_from_slice(&address_to_bytes32(safe_address));
    keccak256(&buf)
}

// ════════════════════════════════════════════════════════════════
// Step 3: API Credential Generation
// ════════════════════════════════════════════════════════════════

struct ApiCredentials {
    api_key: String,
    secret: String,
    passphrase: String,
}

fn derive_api_credentials(key: &SigningKey, signer_address: &str) -> Result<ApiCredentials> {
    let timestamp = format!("{}", std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs());
    let nonce: u64 = 0;

    // EIP-712: ClobAuth(address address, string timestamp, uint256 nonce, string message)
    let domain_sep = build_domain_separator_no_contract(CLOB_AUTH_DOMAIN_NAME, "1", CHAIN_ID);

    let struct_type_hash = keccak256(
        b"ClobAuth(address address,string timestamp,uint256 nonce,string message)");
    let mut struct_buf = Vec::with_capacity(5 * 32);
    struct_buf.extend_from_slice(&struct_type_hash);
    struct_buf.extend_from_slice(&address_to_bytes32(signer_address));
    struct_buf.extend_from_slice(&keccak256(timestamp.as_bytes()));
    struct_buf.extend_from_slice(&u256_bytes(nonce as u128));
    struct_buf.extend_from_slice(&keccak256(CLOB_AUTH_MESSAGE.as_bytes()));
    let struct_hash = keccak256(&struct_buf);

    let signature = sign_eip712(&domain_sep, &struct_hash, key)?;

    let checksum_addr = to_checksum_address(signer_address);

    // Polymarket CLOB exposes two L1 endpoints that take the SAME headers:
    //   * POST /auth/api-key        — CREATE the key (first-time wallet)
    //   * GET  /auth/derive-api-key — DERIVE the already-created key
    // `derive` is deterministic but only succeeds AFTER a key exists; a
    // brand-new deploy_wallet has never created one, so derive alone 400s
    // with "Could not derive api key!". Mirror py-clob-client's
    // create_or_derive: try CREATE first, fall back to DERIVE when the key
    // already exists (re-running deploy_wallet — CREATE then 400s).
    let json: serde_json::Value =
        match clob_create_api_key(&checksum_addr, &signature, &timestamp, nonce) {
            Ok(j) if has_api_creds(&j) => j,
            _ => clob_derive_api_key(&checksum_addr, &signature, &timestamp, nonce)?,
        };
    let api_key = json.get("apiKey").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let secret = json.get("secret").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let passphrase = json.get("passphrase").and_then(|v| v.as_str()).unwrap_or("").to_string();

    if api_key.is_empty() || secret.is_empty() {
        return Err(anyhow!("Failed to derive API credentials: {:?}", json));
    }

    Ok(ApiCredentials { api_key, secret, passphrase })
}

/// True when the body carries a usable api key + secret (CREATE returns the
/// new pair on success; an "already exists" body lacks them → derive).
fn has_api_creds(j: &serde_json::Value) -> bool {
    let nonempty = |k: &str| j.get(k).and_then(|v| v.as_str()).map(|s| !s.is_empty()).unwrap_or(false);
    nonempty("apiKey") && nonempty("secret")
}

fn clob_create_api_key(address: &str, signature: &str, timestamp: &str, nonce: u64) -> Result<serde_json::Value> {
    clob_l1_api_key_request(reqwest::Method::POST, "/auth/api-key", address, signature, timestamp, nonce)
}

fn clob_derive_api_key(address: &str, signature: &str, timestamp: &str, nonce: u64) -> Result<serde_json::Value> {
    clob_l1_api_key_request(reqwest::Method::GET, "/auth/derive-api-key", address, signature, timestamp, nonce)
}

/// Send an L1-authenticated CLOB api-key request (POLY_ADDRESS / SIGNATURE /
/// TIMESTAMP / NONCE headers, no body) and parse the JSON response. The
/// create and derive paths differ ONLY in method + path, so they share this.
fn clob_l1_api_key_request(
    method: reqwest::Method,
    path: &str,
    address: &str,
    signature: &str,
    timestamp: &str,
    nonce: u64,
) -> Result<serde_json::Value> {
    let url = format!("{}{}", CLOB_URL, path);
    let addr = address.to_string();
    let sig = signature.to_string();
    let ts = timestamp.to_string();
    let nonce_s = nonce.to_string();
    let label = format!("{} {}", method, path);
    let client = crate::async_rt::http_client();
    crate::async_rt::block_on_runtime(async move {
        let resp = client.request(method, &url)
            .header("POLY_ADDRESS", addr)
            .header("POLY_SIGNATURE", sig)
            .header("POLY_TIMESTAMP", ts)
            .header("POLY_NONCE", nonce_s)
            .send().await
            .map_err(|e| anyhow!("{} failed: {}", label, e))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(anyhow!("{} failed ({}): {}", label, status, text));
        }
        serde_json::from_str::<serde_json::Value>(&text)
            .map_err(|e| anyhow!("parse {}: {} (body={})", label, e, text))
    })
}

// ════════════════════════════════════════════════════════════════
// Helpers
// ════════════════════════════════════════════════════════════════

/// Accept EITHER a raw private key (0x-hex / 64 hex chars) OR a BIP39
/// mnemonic. Detection: a private key is always a single token, so any
/// multi-word input is treated as a mnemonic (BIP39 validates it and
/// errors clearly if the phrase / checksum is wrong).
pub fn parse_key_or_mnemonic(input: &str) -> Result<SigningKey> {
    let trimmed = input.trim();
    if trimmed.split_whitespace().count() >= 2 {
        derive_eth_key_from_mnemonic(trimmed)
    } else {
        parse_private_key(trimmed)
    }
}

pub fn parse_private_key(input: &str) -> Result<SigningKey> {
    let hex_clean = input.strip_prefix("0x").unwrap_or(input);
    if hex_clean.len() == 64 && hex_clean.chars().all(|c| c.is_ascii_hexdigit()) {
        let bytes = hex::decode(hex_clean).map_err(|e| anyhow!("Invalid hex: {}", e))?;
        SigningKey::from_bytes(bytes.as_slice().into()).map_err(|e| anyhow!("Invalid key: {}", e))
    } else {
        Err(anyhow!("Invalid input. Expected a 0x-prefixed private key (64 hex chars) \
            or a BIP39 mnemonic (12/15/18/21/24 space-separated words)."))
    }
}

/// Derive the Ethereum signer key from a BIP39 mnemonic using the standard
/// BIP44 path `m/44'/60'/0'/0/0` (MetaMask / first account), empty BIP39
/// passphrase. Verified against the well-known Hardhat test mnemonic (see
/// the unit test below) so a wrong derivation can't ship silently.
pub fn derive_eth_key_from_mnemonic(phrase: &str) -> Result<SigningKey> {
    let mnemonic = bip39::Mnemonic::parse(phrase.trim())
        .map_err(|e| anyhow!("invalid BIP39 mnemonic: {}", e))?;
    let seed = mnemonic.to_seed(""); // 64-byte seed, empty passphrase

    // BIP32 master key/chain-code from the seed.
    let i = hmac_sha512(b"Bitcoin seed", &seed);
    let mut key: [u8; 32] = i[..32].try_into().unwrap();
    let mut chain: [u8; 32] = i[32..].try_into().unwrap();

    // BIP44 Ethereum: m / 44' / 60' / 0' / 0 / 0
    const H: u32 = 0x8000_0000; // hardened offset
    for &index in &[44 | H, 60 | H, H /*0'*/, 0, 0] {
        let (k, c) = ckd_priv(&key, &chain, index)?;
        key = k;
        chain = c;
    }
    SigningKey::from_bytes((&key).into()).map_err(|e| anyhow!("derived key invalid: {}", e))
}

fn hmac_sha512(hmac_key: &[u8], data: &[u8]) -> [u8; 64] {
    use hmac::{Hmac, Mac};
    let mut mac = <Hmac<sha2::Sha512>>::new_from_slice(hmac_key)
        .expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

/// BIP32 CKDpriv: derive the child (private key, chain code) at `index`.
fn ckd_priv(k_par: &[u8; 32], c_par: &[u8; 32], index: u32) -> Result<([u8; 32], [u8; 32])> {
    use hmac::{Hmac, Mac};
    let mut mac = <Hmac<sha2::Sha512>>::new_from_slice(c_par)
        .expect("HMAC accepts any key length");
    if index & 0x8000_0000 != 0 {
        // Hardened: 0x00 || ser256(k_par) || ser32(index)
        mac.update(&[0u8]);
        mac.update(k_par);
    } else {
        // Normal: serP(point(k_par)) || ser32(index) — compressed pubkey.
        let sk = SigningKey::from_bytes(k_par.into())
            .map_err(|e| anyhow!("parent key invalid: {}", e))?;
        let pub_compressed = sk.verifying_key().to_encoded_point(true);
        mac.update(pub_compressed.as_bytes());
    }
    mac.update(&index.to_be_bytes());
    let i = mac.finalize().into_bytes();

    // child_key = (parse256(I_L) + k_par) mod n ; child_chain = I_R
    let child_key = scalar_add_mod_n(&i[..32], k_par)?;
    let child_chain: [u8; 32] = i[32..].try_into().unwrap();
    Ok((child_key, child_chain))
}

/// `(parse256(il) + k_par) mod n`, per BIP32. Errors if `I_L >= n` or the
/// result is zero (each ~2^-127 — operator just retries with another path,
/// but in practice never happens for standard wallets).
fn scalar_add_mod_n(il: &[u8], k_par: &[u8; 32]) -> Result<[u8; 32]> {
    use k256::elliptic_curve::ff::PrimeField;
    let il_s = Option::<k256::Scalar>::from(
        k256::Scalar::from_repr(*k256::FieldBytes::from_slice(il)),
    )
    .ok_or_else(|| anyhow!("BIP32: I_L >= curve order (retry derivation)"))?;
    let kp_s = Option::<k256::Scalar>::from(
        k256::Scalar::from_repr(*k256::FieldBytes::from_slice(k_par)),
    )
    .ok_or_else(|| anyhow!("BIP32: parent key >= curve order"))?;
    let sum = il_s + kp_s;
    if bool::from(<k256::Scalar as k256::elliptic_curve::ff::Field>::is_zero(&sum)) {
        return Err(anyhow!("BIP32: derived zero key (retry derivation)"));
    }
    Ok(sum.to_bytes().into())
}

#[cfg(test)]
mod mnemonic_tests {
    use super::*;

    #[test]
    fn hardhat_vector_m44_60_0_0_0() {
        // The canonical Hardhat / Anvil test mnemonic. Account #0 at
        // m/44'/60'/0'/0/0 has a fixed, widely-published key + address.
        let phrase = "test test test test test test test test test test test junk";
        let key = derive_eth_key_from_mnemonic(phrase).expect("derive");
        let priv_hex = hex::encode(key.to_bytes());
        assert_eq!(
            priv_hex,
            "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
            "BIP44 m/44'/60'/0'/0/0 private key"
        );
        // Address (lower-case, no checksum) must be Hardhat account #0.
        let addr = super::super::signer::derive_eth_address_from_key(&key).to_lowercase();
        assert_eq!(addr, "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266");
    }

    #[test]
    fn parse_dispatches_key_vs_mnemonic() {
        // Single hex token → key path.
        let k = parse_key_or_mnemonic(
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
        )
        .unwrap();
        assert_eq!(
            hex::encode(k.to_bytes()),
            "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
        );
        // Multi-word → mnemonic path, same wallet.
        let m = parse_key_or_mnemonic(
            "test test test test test test test test test test test junk",
        )
        .unwrap();
        assert_eq!(hex::encode(m.to_bytes()), hex::encode(k.to_bytes()));
    }
}

/// Build EIP-712 domain separator with name + version + chainId (no verifyingContract).
fn build_domain_separator_no_contract(name: &str, version: &str, chain_id: u64) -> [u8; 32] {
    let type_hash = keccak256(b"EIP712Domain(string name,string version,uint256 chainId)");
    let mut buf = Vec::with_capacity(4 * 32);
    buf.extend_from_slice(&type_hash);
    buf.extend_from_slice(&keccak256(name.as_bytes()));
    buf.extend_from_slice(&keccak256(version.as_bytes()));
    buf.extend_from_slice(&u256_bytes(chain_id as u128));
    keccak256(&buf)
}

/// Build EIP-712 domain separator with name + chainId + verifyingContract (no version).
/// Used by Safe factory (CreateProxy).
fn build_domain_separator_no_version(name: &str, chain_id: u64, contract: &str) -> [u8; 32] {
    let type_hash = keccak256(b"EIP712Domain(string name,uint256 chainId,address verifyingContract)");
    let mut buf = Vec::with_capacity(4 * 32);
    buf.extend_from_slice(&type_hash);
    buf.extend_from_slice(&keccak256(name.as_bytes()));
    buf.extend_from_slice(&u256_bytes(chain_id as u128));
    buf.extend_from_slice(&address_to_bytes32(contract));
    keccak256(&buf)
}

/// Sign EIP-712 hash with standard v (27/28). Used for CreateProxy and ClobAuth.
fn sign_eip712(domain_sep: &[u8; 32], struct_hash: &[u8; 32], key: &SigningKey) -> Result<String> {
    let digest = eip712_digest(domain_sep, struct_hash);
    let (sig, recid) = key.sign_prehash_recoverable(&digest)
        .map_err(|e| anyhow!("Signing failed: {}", e))?;
    let mut sig_bytes = [0u8; 65];
    sig_bytes[..64].copy_from_slice(&sig.to_bytes());
    sig_bytes[64] = recid.to_byte() + 27; // standard: v = 27 or 28
    Ok(format!("0x{}", hex::encode(sig_bytes)))
}

/// Sign EIP-712 hash with eth_sign prefix and Safe-adjusted v (31/32).
/// Used for SafeTx execution via Polymarket relayer.
///
/// The Polymarket Python SDK signs Safe transactions as:
///   1. eip712_hash = keccak256(\x19\x01 + domain_sep + struct_hash)
///   2. eth_sign_hash = keccak256("\x19Ethereum Signed Message:\n32" + eip712_hash)
///   3. signature = ecdsaSign(eth_sign_hash), with v = recid + 31
///
/// v=31/32 tells the Safe contract the signature used eth_sign prefix.
pub(super) fn sign_eip712_safe(domain_sep: &[u8; 32], struct_hash: &[u8; 32], key: &SigningKey) -> Result<String> {
    let eip712_hash = eip712_digest(domain_sep, struct_hash);
    // Apply eth_sign prefix: "\x19Ethereum Signed Message:\n32" + hash
    let mut eth_msg = Vec::with_capacity(28 + 32);
    eth_msg.extend_from_slice(b"\x19Ethereum Signed Message:\n32");
    eth_msg.extend_from_slice(&eip712_hash);
    let eth_sign_hash = keccak256(&eth_msg);

    let (sig, recid) = key.sign_prehash_recoverable(&eth_sign_hash)
        .map_err(|e| anyhow!("Signing failed: {}", e))?;
    let mut sig_bytes = [0u8; 65];
    sig_bytes[..64].copy_from_slice(&sig.to_bytes());
    sig_bytes[64] = recid.to_byte() + 31; // Safe-adjusted: v = 31 or 32
    Ok(format!("0x{}", hex::encode(sig_bytes)))
}

fn eip712_digest(domain_sep: &[u8; 32], struct_hash: &[u8; 32]) -> [u8; 32] {
    let mut buf = Vec::with_capacity(2 + 32 + 32);
    buf.push(0x19);
    buf.push(0x01);
    buf.extend_from_slice(domain_sep);
    buf.extend_from_slice(struct_hash);
    keccak256(&buf)
}

// ════════════════════════════════════════════════════════════════
// On-chain approval checks via Polygon RPC
// ════════════════════════════════════════════════════════════════

/// Call eth_call on Polygon. Uses POLYGON_RPC from .env if set, otherwise tries fallback RPCs.
///
/// Routes through the shared async reqwest client (HTTP/2 + keepalive pool) to
/// reuse TLS sessions across calls — avoids a cold TCP+TLS handshake on every
/// RPC invocation (~150-200ms savings on subsequent calls).
pub fn eth_call(to: &str, data: &str) -> Option<String> {
    // Delegate to `onchain_tx::rpc_call` so eth_call inherits the
    // dedicated Polygon RPC client (5 s connect / 15 s total — required
    // for Infura / public endpoints whose connect often exceeds the
    // shared auto client's 800 ms cap), the 3-attempt retry, and the
    // full error cause chain in warn logs.
    //
    // Kept returning `Option<String>` for backward compat — callers
    // (balance checks, approval lookups, Safe nonce fetch) treat None
    // as "unknown" and degrade gracefully.
    let params = serde_json::json!([{"to": to, "data": data}, "latest"]);
    match super::onchain_tx::rpc_call("eth_call", params) {
        Ok(v) => {
            let result = v.get("result").and_then(|r| r.as_str())?;
            if result.is_empty() || result == "0x" {
                return None;
            }
            Some(result.to_string())
        }
        Err(e) => {
            // Log, don't swallow — previously this was silent, making
            // "eth_call nonce() failed" impossible to debug. Rate-
            // limited to warn since callers can often retry on their
            // own terms (maintenance loop, redeem CLI).
            log::warn!("[eth_call] to={} failed: {}", &to[..10.min(to.len())], e);
            None
        }
    }
}

/// Check if ERC20 allowance(owner, spender) > 0.
pub(crate) fn check_erc20_allowance(owner: &str, token: &str, spender: &str) -> bool {
    let mut data = Vec::with_capacity(4 + 32 + 32);
    data.extend_from_slice(&ERC20_ALLOWANCE_SELECTOR);
    data.extend_from_slice(&address_to_bytes32(owner));
    data.extend_from_slice(&address_to_bytes32(spender));
    let calldata = format!("0x{}", hex::encode(&data));

    match eth_call(token, &calldata) {
        Some(result) => {
            let hex_str = result.strip_prefix("0x").unwrap_or(&result);
            let approved = !hex_str.chars().all(|c| c == '0');
            eprintln!("[DEBUG] allowance({}, {}) on {} = {} (approved={})",
                &owner[..10], &spender[..10], &token[..10], &result[..20], approved);
            approved
        }
        None => {
            eprintln!("[DEBUG] allowance check failed (RPC error)");
            false
        }
    }
}

/// Check if ERC1155 isApprovedForAll(owner, operator) returns true.
pub(crate) fn check_erc1155_approval(owner: &str, token: &str, operator: &str) -> bool {
    let mut data = Vec::with_capacity(4 + 32 + 32);
    data.extend_from_slice(&ERC1155_IS_APPROVED_SELECTOR);
    data.extend_from_slice(&address_to_bytes32(owner));
    data.extend_from_slice(&address_to_bytes32(operator));
    let calldata = format!("0x{}", hex::encode(&data));

    match eth_call(token, &calldata) {
        Some(result) => {
            let hex_str = result.strip_prefix("0x").unwrap_or(&result);
            let approved = !hex_str.chars().all(|c| c == '0');
            eprintln!("[DEBUG] isApprovedForAll({}, {}) on {} = {} (approved={})",
                &owner[..10], &operator[..10], &token[..10], &result[..20], approved);
            approved
        }
        None => {
            eprintln!("[DEBUG] isApprovedForAll check failed (RPC error)");
            false
        }
    }
}

/// EIP-55 checksum encoding for an Ethereum address.
pub fn to_checksum_address(addr: &str) -> String {
    let hex_str = addr.strip_prefix("0x").unwrap_or(addr).to_lowercase();
    let hash = keccak256(hex_str.as_bytes());
    let mut checksummed = String::with_capacity(42);
    checksummed.push_str("0x");
    for (i, c) in hex_str.chars().enumerate() {
        if c.is_ascii_digit() {
            checksummed.push(c);
        } else {
            // Each hex char is 4 bits; nibble index i corresponds to hash byte i/2, high/low nibble
            let hash_nibble = if i % 2 == 0 { hash[i / 2] >> 4 } else { hash[i / 2] & 0x0f };
            if hash_nibble >= 8 {
                checksummed.push(c.to_ascii_uppercase());
            } else {
                checksummed.push(c);
            }
        }
    }
    checksummed
}

fn mask_value(s: &str) -> String {
    if s.len() <= 8 { return "***".to_string(); }
    format!("{}...{}", &s[..4], &s[s.len()-4..])
}

pub fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(data);
    hasher.finalize().into()
}

pub fn u256_bytes(val: u128) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[16..].copy_from_slice(&val.to_be_bytes());
    bytes
}

pub fn address_to_bytes32(addr: &str) -> [u8; 32] {
    let bytes = decode_address(addr);
    let mut padded = [0u8; 32];
    padded[12..].copy_from_slice(&bytes);
    padded
}

pub fn decode_address(addr: &str) -> [u8; 20] {
    let hex_str = addr.strip_prefix("0x").unwrap_or(addr);
    let bytes = hex::decode(hex_str).unwrap_or_else(|_| vec![0u8; 20]);
    let mut result = [0u8; 20];
    let len = bytes.len().min(20);
    result[20 - len..].copy_from_slice(&bytes[..len]);
    result
}

// ════════════════════════════════════════════════════════════════
// secrets.toml writer (`[poly.<instance_id>]`)
// ════════════════════════════════════════════════════════════════

/// One instance's Polymarket credentials, ready to serialise into the
/// `[poly.<instance_id>]` block. Mirrors `config::PolymarketSecrets`.
pub(crate) struct PolySecretsWrite<'a> {
    pub api_key: &'a str,
    pub api_secret: &'a str,
    pub api_passphrase: &'a str,
    pub private_key: &'a str,
    pub signature_type: &'a str,
    /// CLOB v2 deposit-wallet address (POLY_1271). Empty → field omitted.
    pub funder: &'a str,
}

/// Peek at the existing `[poly.<instance_id>]` block, returning its
/// `api_key` (for a masked "about to overwrite" prompt) if present.
/// Parses the RAW file (no `${VAR}` expansion) so it works regardless of
/// whether other blocks use env placeholders. `None` = file missing,
/// unparseable, or block absent.
pub(crate) fn peek_poly_api_key(path: &std::path::Path, instance_id: &str) -> Option<String> {
    let raw = std::fs::read_to_string(path).ok()?;
    let doc = raw.parse::<toml_edit::DocumentMut>().ok()?;
    doc.get("poly")?
        .get(instance_id)?
        .get("api_key")?
        .as_str()
        .map(|s| s.to_string())
}

/// Write (creating) or overwrite the `[poly.<instance_id>]` block in the
/// secrets TOML at `path`, preserving every OTHER block + comments +
/// `${VAR}` placeholders. Atomic (temp file + rename in the same dir),
/// file mode 0600, creates parent dirs. Returns whether the block already
/// existed (true = overwritten).
pub(crate) fn write_poly_secrets(
    path: &std::path::Path,
    instance_id: &str,
    w: &PolySecretsWrite,
) -> Result<bool> {
    use toml_edit::{value, DocumentMut, Item, Table};

    let mut doc: DocumentMut = if path.exists() {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| anyhow!("read secrets {}: {}", path.display(), e))?;
        raw.parse::<DocumentMut>()
            .map_err(|e| anyhow!("parse secrets {}: {}", path.display(), e))?
    } else {
        DocumentMut::new()
    };

    if doc.get("poly").is_none() {
        let mut t = Table::new();
        // Render child blocks as `[poly.<id>]` without emitting a bare
        // `[poly]` header line.
        t.set_implicit(true);
        doc["poly"] = Item::Table(t);
    }
    let poly = doc["poly"]
        .as_table_mut()
        .ok_or_else(|| anyhow!("secrets {}: `poly` is not a table", path.display()))?;

    let existed = poly.contains_key(instance_id);

    let mut t = Table::new();
    t["api_key"] = value(w.api_key);
    t["api_secret"] = value(w.api_secret);
    t["api_passphrase"] = value(w.api_passphrase);
    t["private_key"] = value(w.private_key);
    t["signature_type"] = value(w.signature_type);
    if !w.funder.is_empty() {
        t["funder"] = value(w.funder);
    }
    poly[instance_id] = Item::Table(t);

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .map_err(|e| anyhow!("create dir {}: {}", parent.display(), e))?;
        }
    }

    // Atomic: write to a sibling temp file, fsync, rename over the target.
    let mut tmp_os = path.as_os_str().to_owned();
    tmp_os.push(".tmp");
    let tmp = std::path::PathBuf::from(tmp_os);
    {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|e| anyhow!("open temp {}: {}", tmp.display(), e))?;
        f.write_all(doc.to_string().as_bytes())?;
        f.flush()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = f.set_permissions(std::fs::Permissions::from_mode(0o600));
        }
    }
    std::fs::rename(&tmp, path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        anyhow!("rename {} → {}: {}", tmp.display(), path.display(), e)
    })?;
    Ok(existed)
}

#[cfg(test)]
mod secrets_writer_tests {
    use super::*;

    #[test]
    fn writes_block_and_preserves_others() {
        let dir = std::env::temp_dir().join(format!("hexbot_secrets_test_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("secrets.toml");
        // Pre-existing file with another instance using a ${VAR} placeholder
        // + a comment, to prove both survive the edit.
        std::fs::write(
            &path,
            "# keep me\n[poly.alice]\napi_key = \"key-A\"\napi_secret = \"${ALICE_SECRET}\"\napi_passphrase = \"pp-A\"\nprivate_key = \"0xaaaa\"\nsignature_type = \"gnosis_safe\"\n",
        )
        .unwrap();

        let w = PolySecretsWrite {
            api_key: "key-B",
            api_secret: "secret-B",
            api_passphrase: "pp-B",
            private_key: "0xbbbb",
            signature_type: "gnosis_safe",
            funder: "",
        };
        let existed = write_poly_secrets(&path, "bob", &w).unwrap();
        assert!(!existed, "bob block should be new");

        let out = std::fs::read_to_string(&path).unwrap();
        // New block present.
        assert!(out.contains("[poly.bob]"), "bob block written:\n{out}");
        assert!(out.contains("api_key = \"key-B\""));
        // Other instance + its placeholder + the comment preserved verbatim.
        assert!(out.contains("[poly.alice]"));
        assert!(out.contains("${ALICE_SECRET}"), "placeholder preserved:\n{out}");
        assert!(out.contains("# keep me"), "comment preserved:\n{out}");

        // Overwrite path: returns existed=true and replaces values.
        let w2 = PolySecretsWrite { api_key: "key-B2", ..w };
        let existed2 = write_poly_secrets(&path, "bob", &w2).unwrap();
        assert!(existed2, "bob block now exists");
        let out2 = std::fs::read_to_string(&path).unwrap();
        assert!(out2.contains("api_key = \"key-B2\""));
        assert!(!out2.contains("key-B\""), "old value gone:\n{out2}");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
