//! CLI commands for Polymarket wallet management: deposit info and stablecoin withdrawal.

use std::io::Write;

use anyhow::{anyhow, Result};

use super::auth::PolyAuth;
use super::deploy_wallet::{
    self, check_deployed, derive_safe_address, parse_private_key,
    address_to_bytes32, u256_bytes, to_checksum_address,
};
use super::signer::{derive_eth_address_from_key, parse_signature_type, SignatureType};

/// Bridged USDC.e, the legacy Polymarket v1 collateral.
const USDCE_ADDRESS: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
/// Polymarket USD (pUSD) — the v2 collateral token. 6 decimals, same
/// scale as USDC/USDC.e. See `migrate_usdc.rs` for wrapping either asset.
const PUSD_ADDRESS: &str = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB";
/// CollateralOfframp — unwraps pUSD back into USDC or USDC.e (the reverse
/// of the CollateralOnramp's `wrap`; see migrate_usdc.rs). Polygon mainnet, from
/// the Polymarket v2 docs (docs.polymarket.com/resources/contracts).
const OFFRAMP_ADDRESS: &str = "0x2957922Eb93258b93368531d39fAcCA3B4dC5854";
const RELAYER_URL: &str = "https://relayer-v2.polymarket.com";
const CLOB_URL: &str = "https://clob.polymarket.com";

// transfer(address,uint256) selector
const ERC20_TRANSFER_SELECTOR: [u8; 4] = [0xa9, 0x05, 0x9c, 0xbb];
// approve(address,uint256) selector
const ERC20_APPROVE_SELECTOR: [u8; 4] = [0x09, 0x5e, 0xa7, 0xb3];
// allowance(address,address) selector
const ERC20_ALLOWANCE_SELECTOR: [u8; 4] = [0xdd, 0x62, 0xed, 0x3e];
// CollateralOfframp.unwrap(address asset, address to, uint256 amount)
// selector — keccak256("unwrap(address,address,uint256)")[:4].
const UNWRAP_SELECTOR: [u8; 4] = [0x8c, 0xc7, 0x10, 0x4f];
/// EVM `type(uint256).max` — standard ERC-20 "infinite approval" value.
const U256_MAX_BYTES: [u8; 32] = [0xff; 32];

/// Read `gas_via_signer_wallet` from a polymaker live config file, if
/// present. Used by CLI commands (redeem, split) to share the operator's
/// policy choice with the live maintenance thread.
///
/// Search order:
///   1. `$HEXBOT_CONFIG`            (whatever the live engine was started with)
///   2. `config/live_polymaker.toml` (conventional default)
///   3. fall back to `false` (relayer / gasless)
///
/// Parse is best-effort — any error returns false so a malformed config
/// never blocks a redeem/split invocation.
pub(crate) fn read_gas_via_signer_wallet_flag() -> bool {
    let paths = [
        crate::exchange::polymarket::cli_account::config_path(),
        std::env::var("HEXBOT_CONFIG").ok(),
        Some("config/live_polymaker.toml".to_string()),
    ];
    for p in paths.iter().flatten() {
        let Ok(text) = std::fs::read_to_string(p) else { continue; };
        let Ok(val) = text.parse::<toml::Value>() else { continue; };
        // Canonical location: `[general].gas_via_signer_wallet`.
        if let Some(b) = val.get("general")
            .and_then(|g| g.get("gas_via_signer_wallet"))
            .and_then(|v| v.as_bool())
        {
            return b;
        }
        // Legacy fallback: per-strategy `params` (or directly under the
        // [[strategies]] entry). Walk the array looking for polymaker.
        if let Some(arr) = val.get("strategies").and_then(|v| v.as_array()) {
            for s in arr {
                let is_poly = s.get("name").and_then(|v| v.as_str()) == Some("polymaker");
                if !is_poly { continue; }
                let params = s.get("params");
                let flag = params
                    .and_then(|p| p.get("gas_via_signer_wallet"))
                    .and_then(|v| v.as_bool());
                // Some configs nest the param directly under the
                // [[strategies]] entry rather than under `params`.
                let flag = flag.or_else(|| {
                    s.get("gas_via_signer_wallet").and_then(|v| v.as_bool())
                });
                if let Some(b) = flag {
                    return b;
                }
            }
        }
    }
    false
}

// ════════════════════════════════════════════════════════════════
// HTTP helpers — all go through the shared async runtime + h2 client
// ════════════════════════════════════════════════════════════════

/// Builder-signed relayer request (POST /submit, GET /nonce, GET /transaction).
fn relayer_http(
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

/// User-auth CLOB DELETE (POLY_API_KEY / POLY_ADDRESS / POLY_SIGNATURE ...).
/// Accepts an optional JSON body; body is included in the HMAC signing
/// base so the server's auth verification agrees.
fn user_clob_delete(
    url: String,
    headers: super::auth::AuthHeaders,
    body: Option<String>,
) -> Result<serde_json::Value> {
    let client = crate::async_rt::http_client();
    crate::async_rt::block_on_runtime(async move {
        let mut req = client.delete(&url);
        for (k, v) in headers.as_pairs() {
            req = req.header(k, v);
        }
        if let Some(b) = body {
            req = req.header("Content-Type", "application/json").body(b);
        }
        let resp = req.send().await
            .map_err(|e| anyhow!("DELETE {} failed: {}", url, e))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(anyhow!("DELETE {} failed ({}): {}", url, status, text));
        }
        serde_json::from_str(&text)
            .map_err(|e| anyhow!("parse {}: {} (body={})", url, e, text))
    })
}

/// User-auth CLOB GET (POLY_API_KEY / POLY_ADDRESS / POLY_SIGNATURE ...).
fn user_clob_get(
    url: String,
    headers: super::auth::AuthHeaders,
) -> Result<serde_json::Value> {
    let client = crate::async_rt::http_client();
    crate::async_rt::block_on_runtime(async move {
        let mut req = client.get(&url);
        for (k, v) in headers.as_pairs() {
            req = req.header(k, v);
        }
        let resp = req.send().await
            .map_err(|e| anyhow!("GET {} failed: {}", url, e))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(anyhow!("GET {} failed ({}): {}", url, status, text));
        }
        serde_json::from_str(&text)
            .map_err(|e| anyhow!("parse {}: {} (body={})", url, e, text))
    })
}

/// User-auth CLOB GET that only checks the HTTP status — for endpoints that
/// return an empty / non-JSON body on success (e.g. `/balance-allowance/update`,
/// which 200s with no body). Returns `Ok(())` on 2xx, `Err` with the body
/// otherwise.
fn user_clob_get_ok(
    url: String,
    headers: super::auth::AuthHeaders,
) -> Result<()> {
    let client = crate::async_rt::http_client();
    crate::async_rt::block_on_runtime(async move {
        let mut req = client.get(&url);
        for (k, v) in headers.as_pairs() {
            req = req.header(k, v);
        }
        let resp = req.send().await
            .map_err(|e| anyhow!("GET {} failed: {}", url, e))?;
        let status = resp.status();
        if status.is_success() {
            Ok(())
        } else {
            let text = resp.text().await.unwrap_or_default();
            Err(anyhow!("GET {} failed ({}): {}", url, status, text))
        }
    })
}

/// Unauth JSON GET (gamma-api / data-api). Returns `Value::Null` on failure
/// (mirroring the tolerant behavior of the former sync call sites).
fn unauth_get_json(url: &str) -> serde_json::Value {
    match crate::async_rt::blocking_get_text(url) {
        Ok(text) => serde_json::from_str(&text).unwrap_or(serde_json::Value::Null),
        Err(_) => serde_json::Value::Null,
    }
}

// ════════════════════════════════════════════════════════════════
// Shared helpers
// ════════════════════════════════════════════════════════════════

pub(crate) struct WalletInfo {
    pub signer_address: String,
    /// The funds/positions wallet for gnosis_safe / poly_proxy (the
    /// CREATE2-derived Polymarket proxy). For a bare EOA (signatureType=0)
    /// this is **aliased to `signer_address`** at load time — an EOA has no
    /// Safe proxy and holds its own collateral. Do NOT feed this to Safe
    /// `execTransaction` paths without first checking `is_eoa()`.
    pub safe_address: String,
    pub signing_key: k256::ecdsa::SigningKey,
    pub builder_auth: PolyAuth,
    /// `POLY_SIGNATURE_TYPE` (e.g. "gnosis_safe", "poly_1271").
    pub signature_type: String,
    /// CLOB v2 deposit-wallet address (`POLY_FUNDER`); non-empty only for
    /// `signature_type = "poly_1271"`. When set, maintenance (split/redeem)
    /// routes through the WALLET-batch path against this wallet instead of
    /// the Gnosis Safe.
    pub deposit_wallet: String,
}

impl WalletInfo {
    /// Returns the deposit-wallet address iff this is a POLY_1271 account
    /// with a configured funder — i.e. maintenance must use the WALLET
    /// batch path rather than the Safe path.
    pub fn deposit_wallet_active(&self) -> Option<&str> {
        let st = self.signature_type.to_ascii_lowercase();
        if (st == "poly_1271" || st == "deposit_wallet") && !self.deposit_wallet.is_empty() {
            Some(self.deposit_wallet.as_str())
        } else {
            None
        }
    }

    /// True iff this is a bare EOA account (signatureType=0) with no
    /// deposit wallet — funds/positions live at the signer EOA itself, and
    /// there is no Gnosis Safe proxy. For EOA, `load_wallet_from` sets
    /// `safe_address == signer_address`, so on-chain balance reads keyed off
    /// `safe_address`/`primary_address` already hit the right wallet; this
    /// helper lets the maintenance / display paths branch away from the
    /// Safe `execTransaction` machinery (which an EOA can't use).
    pub fn is_eoa(&self) -> bool {
        self.deposit_wallet_active().is_none()
            && matches!(parse_signature_type(&self.signature_type), SignatureType::Eoa)
    }

    /// The address that actually holds funds + positions for this account:
    /// the deposit wallet for POLY_1271, the signer EOA for a bare EOA
    /// (`safe_address` was aliased to the signer at load time), else the
    /// Gnosis Safe. Use for balance/position reads and deposit/withdraw
    /// display.
    pub fn primary_address(&self) -> &str {
        self.deposit_wallet_active().unwrap_or(&self.safe_address)
    }

    /// Human label for the primary wallet kind.
    pub fn wallet_kind(&self) -> &'static str {
        if self.deposit_wallet_active().is_some() {
            "deposit wallet"
        } else if self.is_eoa() {
            "EOA"
        } else {
            "Safe"
        }
    }
}

/// Result of one maintenance run (one redeem-all pass + one split for
/// next event). Surfaced via `Arc<Mutex<MaintenanceStatus>>` to the
/// strategy so the RTT gate can force PROBE when seed inventory wasn't
/// successfully prepared.
///
/// Why this matters: `splitPosition` not landing means the bot starts
/// the next event with `init_up=0, init_down=0` — but the polymaker
/// strategy assumes it has the configured 30/30 hedged seed. In that
/// mismatch every quote acquires raw directional exposure, and a
/// losing 5-min outcome takes the entire delta straight off PnL.
/// (Observed 2026-05-16: 11 events with init=0 → -$25.19 cumulative.)
///
/// States are mutually exclusive and represent the most recent run.
#[derive(Debug, Clone, PartialEq)]
pub enum MaintenanceStatus {
    /// No maintenance has been attempted yet this session.
    NotStarted,
    /// Maintenance thread spawned but hasn't finished yet (poll loop
    /// still running). Strategy should treat as "uncertain" → safer
    /// to PROBE than assume seed inventory.
    Running,
    /// Maintenance completed and the split tx landed on-chain. Bot can
    /// trade with the assumption that `init_up = init_down = split_amount`.
    Succeeded,
    /// Redeem step failed (RPC reject, all gas tiers exhausted, etc).
    /// May or may not affect split — but signals chain instability.
    RedeemFailed { reason: String },
    /// Split step failed (or timed out as PENDING). Critical: no seed
    /// inventory for next event → strategy MUST PROBE.
    SplitFailedOrPending { reason: String },
    /// Last spawn was deliberately skipped (e.g. RTT gate said
    /// unhealthy). Strategy already knows; this is a sentinel for
    /// the maintenance pipeline status.
    Skipped { reason: String },
}

impl MaintenanceStatus {
    /// True iff the most-recent maintenance run produced usable seed
    /// inventory for the next event. The RTT gate / strategy uses this
    /// as a precondition for entering TRADE mode.
    pub fn produced_seed_inventory(&self) -> bool {
        matches!(self, MaintenanceStatus::Succeeded)
    }

    /// Compact human label for logging.
    pub fn label(&self) -> &'static str {
        match self {
            MaintenanceStatus::NotStarted => "NotStarted",
            MaintenanceStatus::Running => "Running",
            MaintenanceStatus::Succeeded => "Succeeded",
            MaintenanceStatus::RedeemFailed { .. } => "RedeemFailed",
            MaintenanceStatus::SplitFailedOrPending { .. } => "SplitFailedOrPending",
            MaintenanceStatus::Skipped { .. } => "Skipped",
        }
    }
}

/// Thread-safe handle to the current maintenance status. Maintenance
/// thread holds an `Arc` and writes; strategy holds an `Arc` and reads
/// at `lock_in_next_event_mode` time.
pub type MaintenanceStatusHandle = std::sync::Arc<std::sync::Mutex<MaintenanceStatus>>;

/// Build a new `MaintenanceStatusHandle` starting in `NotStarted`.
pub fn new_maintenance_status_handle() -> MaintenanceStatusHandle {
    std::sync::Arc::new(std::sync::Mutex::new(MaintenanceStatus::NotStarted))
}

/// Standard "no wallet credentials" error. Credentials are sourced ONLY
/// from the secrets file (per-instance `[poly.<id>]`); `.env` is no longer
/// a credential source.
pub(crate) fn no_wallet_creds_err() -> anyhow::Error {
    anyhow!(
        "no wallet credentials resolved from the secrets file.\n  \
         Pass --instance <id> --config <cfg> (or --config <cfg> with a single \
         strategy) so the signer key + API creds load from the secrets file's \
         [poly.<id>] block.\n  Credentials are NOT read from .env. If this \
         wallet isn't set up yet, run:\n    \
         hexbot deploy_wallet --instance <id> --config <cfg>"
    )
}

pub(crate) fn load_wallet() -> Result<WalletInfo> {
    // Single-account / CLI path: read the per-account creds from the
    // global POLY_* env (set by apply_creds_to_env / apply_account_to_env).
    let private_key = std::env::var("POLY_PRIVATE_KEY").unwrap_or_default();
    let signature_type = std::env::var("POLY_SIGNATURE_TYPE").unwrap_or_default();
    let deposit_wallet = std::env::var("POLY_FUNDER").unwrap_or_default();
    load_wallet_from(&private_key, &signature_type, &deposit_wallet)
}

/// Build a `WalletInfo` from explicit per-account creds. The builder
/// (relayer) credentials remain shared across all accounts — sourced
/// from the `[builder]` secrets block (POLY_BUILDER_*), one attribution
/// code for the operator's whole wallet set.
fn load_wallet_from(
    private_key: &str,
    signature_type: &str,
    deposit_wallet: &str,
) -> Result<WalletInfo> {
    if private_key.is_empty() {
        return Err(no_wallet_creds_err());
    }

    let signing_key = parse_private_key(private_key)?;
    let signer_address = to_checksum_address(&derive_eth_address_from_key(&signing_key));
    // Funds/positions wallet resolution is signature-type dependent:
    //   * EOA (signatureType=0) — the account trades directly from the
    //     signer EOA, which itself holds USDC + conditional tokens. There
    //     is NO Gnosis Safe proxy, so the funds wallet IS the signer.
    //   * gnosis_safe / poly_proxy — funds live in the CREATE2-derived
    //     Polymarket proxy (Safe). poly_1271 additionally carries the
    //     deposit wallet in `deposit_wallet` (primary_address prefers it).
    // Deriving the Safe unconditionally (the prior behaviour) pointed every
    // balance read + maintenance op at an empty derived-Safe address for a
    // bare EOA. Gate the Safe derivation on the sig type so gnosis / 1271
    // stay byte-identical while EOA resolves to the EOA.
    let safe_address = if matches!(parse_signature_type(signature_type), SignatureType::Eoa) {
        signer_address.clone()
    } else {
        to_checksum_address(&derive_safe_address(&signer_address))
    };

    // Builder credentials for relayer — from the secrets file's [builder]
    // section (pushed to POLY_BUILDER_* on Config::load), never from .env.
    let builder_key = std::env::var("POLY_BUILDER_API_KEY").unwrap_or_default();
    let builder_secret = std::env::var("POLY_BUILDER_SECRET").unwrap_or_default();
    let builder_passphrase = std::env::var("POLY_BUILDER_PASSPHRASE").unwrap_or_default();

    if builder_key.is_empty() || builder_secret.is_empty() {
        return Err(anyhow!(
            "builder credentials not resolved — add a [builder] section \
             (api_key / api_secret / api_passphrase) to the secrets file. \
             POLY_BUILDER_* are no longer read from .env."
        ));
    }

    let builder_auth = PolyAuth::new(&builder_key, &builder_secret, &builder_passphrase, &signer_address)?;

    Ok(WalletInfo {
        signer_address, safe_address, signing_key, builder_auth,
        signature_type: signature_type.to_string(),
        deposit_wallet: deposit_wallet.to_string(),
    })
}

/// Per-account wallet-creds registry for the live multi-account
/// maintenance path. Keyed by `account_id`, populated once per account
/// in `build_poly_shared_states_map`. The maintenance thread looks up
/// ITS account's creds here instead of the global POLY_* env, which a
/// second account would otherwise overwrite (last-account-wins).
#[derive(Clone)]
struct AccountWalletCreds {
    private_key: String,
    signature_type: String,
    funder: String,
}

static ACCOUNT_WALLETS: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<String, AccountWalletCreds>>> =
    std::sync::OnceLock::new();

/// Register an account's split/redeem creds so the maintenance thread
/// can resolve the RIGHT wallet under multi-account live (no global-env
/// clobber). Idempotent; safe to call once per account at startup.
pub fn register_account_wallet(
    account_id: &str,
    private_key: &str,
    signature_type: &str,
    funder: &str,
) {
    if account_id.is_empty() {
        return;
    }
    let map = ACCOUNT_WALLETS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
    map.lock().unwrap().insert(
        account_id.to_string(),
        AccountWalletCreds {
            private_key: private_key.to_string(),
            signature_type: signature_type.to_string(),
            funder: funder.to_string(),
        },
    );
}

/// Resolve a `WalletInfo` for a specific account. Uses the per-account
/// registry when populated (multi-account live), else falls back to the
/// global POLY_* env (`load_wallet`) for single-account / CLI paths.
pub(crate) fn load_wallet_for_account(account_id: &str) -> Result<WalletInfo> {
    if !account_id.is_empty() {
        if let Some(map) = ACCOUNT_WALLETS.get() {
            let creds = map.lock().unwrap().get(account_id).cloned();
            if let Some(c) = creds {
                return load_wallet_from(&c.private_key, &c.signature_type, &c.funder);
            }
        }
    }
    load_wallet()
}

// balanceOf(address) selector
const ERC20_BALANCE_OF_SELECTOR: [u8; 4] = [0x70, 0xa0, 0x82, 0x31];

/// Fetch USDC.e balance directly from chain via eth_call (not Polymarket Data API).
fn fetch_usdce_balance(safe_address: &str) -> f64 {
    fetch_erc20_balance_6dec(USDCE_ADDRESS, safe_address)
}

/// Fetch pUSD (v2 collateral) balance via eth_call.
fn fetch_pusd_balance(safe_address: &str) -> f64 {
    fetch_erc20_balance_6dec(PUSD_ADDRESS, safe_address)
}

/// Print the Safe's stablecoin balances with **pUSD (v2 collateral) as the
/// primary line**, followed by bridged USDC.e. Native USDC is intentionally
/// omitted from `hexbot deposit`. When USDC.e remains, nudge the operator to
/// wrap it into pUSD (the token the v2 bot trades).
fn print_stablecoin_balances(safe_address: &str) {
    let pusd = fetch_pusd_balance(safe_address);
    let usdce = fetch_usdce_balance(safe_address);
    println!("{:<6} balance: {:>12.6}   (v2 collateral — used for trading)", "pUSD", pusd);
    if usdce.abs() < 0.000001 {
        println!("{:<6} balance: {:>12.6}", "USDC.e", usdce);
    } else {
        println!(
            "{:<6} balance: {:>12.6}   ⚠ run `hexbot migrate_usdce all` to convert to v2 pUSD",
            "USDC.e", usdce,
        );
    }
}

/// Generic 6-decimal ERC-20 balance reader. USDC, USDC.e, and pUSD all
/// use 6 decimals; dedup the wrapper via this helper.
fn fetch_erc20_balance_6dec(token: &str, owner: &str) -> f64 {
    let mut data = Vec::with_capacity(4 + 32);
    data.extend_from_slice(&ERC20_BALANCE_OF_SELECTOR);
    data.extend_from_slice(&address_to_bytes32(owner));
    let calldata = format!("0x{}", hex::encode(&data));

    match deploy_wallet::eth_call(token, &calldata) {
        Some(result) => {
            let hex_str = result.strip_prefix("0x").unwrap_or(&result);
            let wei = u128::from_str_radix(hex_str.trim_start_matches('0'), 16).unwrap_or(0);
            wei as f64 / 1_000_000.0
        }
        None => 0.0,
    }
}

/// Fetch native POL (formerly MATIC) balance for an address via
/// `eth_getBalance`. POL is the native token that pays gas on Polygon,
/// so this is what the operator monitors before using
/// `gas_via_signer_wallet = true`. 18 decimals, same as ETH / MATIC.
///
/// Returns `Err` if the RPC call itself failed (so callers can
/// distinguish "wallet truly empty" from "node didn't respond" — the
/// latter is the live-engine startup check's main false-positive
/// concern, since the original incident was a node falsely reporting
/// `balance 0`).
pub(crate) fn fetch_pol_balance(address: &str) -> Result<f64> {
    let params = serde_json::json!([address, "latest"]);
    let v = super::onchain_tx::rpc_call("eth_getBalance", params)
        .map_err(|e| anyhow!("eth_getBalance({}) failed: {}", &address[..10.min(address.len())], e))?;
    let Some(result) = v.get("result").and_then(|r| r.as_str()) else {
        return Err(anyhow!("eth_getBalance({}): no result field ({})",
            &address[..10.min(address.len())], v));
    };
    let hex_str = result.strip_prefix("0x").unwrap_or(result).trim_start_matches('0');
    if hex_str.is_empty() { return Ok(0.0); }
    let wei = u128::from_str_radix(hex_str, 16)
        .map_err(|e| anyhow!("eth_getBalance: parse hex '{}': {}", result, e))?;
    Ok(wei as f64 / 1e18)
}

// ════════════════════════════════════════════════════════════════
// deposit command
// ════════════════════════════════════════════════════════════════

pub fn run_deposit() -> Result<()> {
    let wallet = load_wallet()?;

    println!("=== Polymarket Wallet ===");
    println!();
    let primary = wallet.primary_address().to_string();
    println!("Signer (EOA):  {}", wallet.signer_address);
    println!("{} ({}): {}", "Trading wallet", wallet.wallet_kind(), primary);

    // Check deployment
    print!("Status:        ");
    let deployed = check_deployed(&wallet.builder_auth, &primary).unwrap_or(false);
    if !deployed {
        println!("NOT DEPLOYED");
        println!();
        println!("Wallet is not deployed. Cannot receive deposits.");
        println!("Run `hexbot deploy_wallet` first.");
        return Ok(());
    }
    println!("Deployed");

    // Balances — pUSD (v2 collateral) and bridged USDC.e.
    print_stablecoin_balances(&primary);

    println!();
    println!("To deposit, send USDC.e (Polygon network) to:");
    println!("  {}", primary);
    println!("Then run `hexbot migrate_usdce all` to wrap it into pUSD.");

    Ok(())
}

// ════════════════════════════════════════════════════════════════
// withdraw command
// ════════════════════════════════════════════════════════════════

pub fn run_withdraw() -> Result<()> {
    let wallet = load_wallet()?;

    // POLY_1271: withdraw (ERC-20 transfer) FROM the deposit wallet.
    if let Some(dw) = wallet.deposit_wallet_active() {
        return run_withdraw_dw(&wallet, &dw.to_string());
    }

    println!("=== Polymarket Withdraw ===");
    println!();
    println!("Safe wallet: {}", wallet.safe_address);

    // Check deployment
    let deployed = check_deployed(&wallet.builder_auth, &wallet.safe_address).unwrap_or(false);
    if !deployed {
        println!("Wallet is not deployed. Run `hexbot deploy_wallet` first.");
        return Ok(());
    }

    // Fetch balances. pUSD is the v2 collateral (what the bot trades and
    // what `migrate_usdce`/wrap produces); USDC.e is the legacy v1 stable.
    // Native USDC is intentionally absent: Polymarket has paused it on both
    // the Onramp and the Offramp (pUSD is ~100% USDC.e-backed), so every
    // unwrap-to-USDC batch reverts at the relayer simulation.
    let pusd_balance = fetch_pusd_balance(&wallet.safe_address);
    let usdce_balance = fetch_usdce_balance(&wallet.safe_address);
    let pol_balance = fetch_pol_balance(&wallet.safe_address).unwrap_or(0.0);
    println!("pUSD   balance: {:.6}   (v2 collateral)", pusd_balance);
    println!("USDC.e balance: {:.6}   (legacy v1)", usdce_balance);
    println!("POL    balance: {:.6}", pol_balance);

    // Token choice
    println!();
    println!("Which token to withdraw?");
    println!("  1) pUSD → USDC.e  (unwrap via Offramp, then withdraw bridged USDC.e — gasless)");
    println!("  2) pUSD           (withdraw pUSD as-is; recipient receives pUSD — gasless)");
    println!("  3) USDC.e         (legacy v1 — gasless)");
    println!("  4) POL            (native, on-chain — signer must have ~0.01 POL for gas)");
    print!("> ");
    std::io::stdout().flush()?;
    let mut token_str = String::new();
    std::io::stdin().read_line(&mut token_str)?;
    let token_choice = token_str.trim();

    match token_choice {
        "1" | "pusd-usdce" => run_withdraw_pusd_to_asset(
            &wallet, pusd_balance, USDCE_ADDRESS, "USDC.e",
        ),
        "2" | "pusd" | "pUSD" | "PUSD" => {
            run_withdraw_erc20(&wallet, PUSD_ADDRESS, "pUSD", pusd_balance)
        }
        "3" | "usdce" | "USDC.e" | "USDCE" => {
            run_withdraw_erc20(&wallet, USDCE_ADDRESS, "USDC.e", usdce_balance)
        }
        "4" | "pol" | "POL" => run_withdraw_pol(&wallet, pol_balance),
        other => Err(anyhow!("Unknown token choice '{}'. Expected 1-4.", other)),
    }
}

/// Withdraw FROM the deposit wallet via a WALLET batch (all gasless via the
/// relayer). Supports pUSD + USDC.e direct ERC-20 transfers and offramping
/// pUSD to bridged USDC.e. Native USDC is not offered — Polymarket has
/// paused it on the Offramp, so the unwrap batch always reverts. POL
/// withdraw remains Safe-only.
fn run_withdraw_dw(wallet: &WalletInfo, dw: &str) -> Result<()> {
    use std::io::Write;
    println!("=== Polymarket Withdraw (deposit wallet) ===");
    println!();
    println!("Deposit wallet: {}", dw);
    let pusd = fetch_pusd_balance(dw);
    let usdce = fetch_usdce_balance(dw);
    println!("pUSD   balance: {:.6}   (v2 collateral)", pusd);
    println!("USDC.e balance: {:.6}   (legacy v1)", usdce);
    println!();
    println!("Which token to withdraw? (FROM the deposit wallet, gasless via relayer)");
    println!("  1) pUSD           (ERC-20 transfer as-is)");
    println!("  2) USDC.e         (ERC-20 transfer as-is)");
    println!("  3) pUSD → USDC.e  (unwrap via Offramp, then withdraw bridged USDC.e)");
    print!("> ");
    std::io::stdout().flush()?;
    let mut choice = String::new();
    std::io::stdin().read_line(&mut choice)?;
    let choice = choice.trim();

    // Offramp path is a 3-call batch (approve + unwrap + transfer), not a
    // plain ERC-20 transfer — handle it separately.
    if matches!(choice, "3" | "pusd-usdce" | "offramp") {
        return run_withdraw_dw_offramp(wallet, dw, pusd, USDCE_ADDRESS, "USDC.e");
    }

    let (token, label, bal) = match choice {
        "1" | "pusd" | "pUSD" | "PUSD" => (PUSD_ADDRESS, "pUSD", pusd),
        "2" | "usdce" | "USDC.e" | "USDCE" => (USDCE_ADDRESS, "USDC.e", usdce),
        o => return Err(anyhow!("Unknown choice '{}'. Expected 1-3.", o)),
    };
    if bal <= 0.0 {
        println!("No {} to withdraw.", label);
        return Ok(());
    }

    println!();
    println!("Enter recipient address (0x...):");
    print!("> ");
    std::io::stdout().flush()?;
    let mut recipient = String::new();
    std::io::stdin().read_line(&mut recipient)?;
    let recipient = recipient.trim().to_string();
    let rc = recipient.strip_prefix("0x").unwrap_or(&recipient);
    if rc.len() != 40 || !rc.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(anyhow!("recipient must be 0x + 40 hex chars, got '{}'", recipient));
    }

    println!("Amount ({}, or 'all' for {:.6}):", label, bal);
    print!("> ");
    std::io::stdout().flush()?;
    let mut amt = String::new();
    std::io::stdin().read_line(&mut amt)?;
    let amt = amt.trim();
    let amount_wei: u128 = if amt.eq_ignore_ascii_case("all") {
        (bal * 1_000_000.0).round() as u128
    } else {
        let a: f64 = amt.parse().map_err(|_| anyhow!("bad amount '{}'", amt))?;
        (a * 1_000_000.0).round() as u128
    };
    if amount_wei == 0 {
        return Err(anyhow!("amount is 0"));
    }

    println!();
    println!("Confirm: transfer {:.6} {} → {}", amount_wei as f64 / 1_000_000.0, label, recipient);
    print!("Type 'yes' to proceed: ");
    std::io::stdout().flush()?;
    let mut confirm = String::new();
    std::io::stdin().read_line(&mut confirm)?;
    if confirm.trim() != "yes" {
        return Err(anyhow!("not confirmed — aborting"));
    }

    super::deposit_wallet::dw_transfer_erc20(
        &wallet.signing_key, &wallet.signer_address, dw, &wallet.builder_auth,
        token, &recipient, amount_wei, /*dry_run=*/ false,
    )?;
    println!("✅ transferred {:.6} {} → {}", amount_wei as f64 / 1_000_000.0, label, recipient);
    Ok(())
}

/// Withdraw the deposit wallet's pUSD as a supported backing asset: unwrap
/// via the Offramp, then transfer that asset to a recipient, atomically in
/// one gasless WALLET batch. pUSD, USDC, and USDC.e are all 1:1, 6-decimal.
fn run_withdraw_dw_offramp(
    wallet: &WalletInfo,
    dw: &str,
    pusd_balance: f64,
    underlying: &str,
    underlying_label: &str,
) -> Result<()> {
    use std::io::Write;
    if pusd_balance <= 0.0 {
        println!("No pUSD to withdraw.");
        return Ok(());
    }

    // Recipient for the unwrapped backing asset.
    println!();
    println!("Enter recipient address (0x...) to receive {}:", underlying_label);
    print!("> ");
    std::io::stdout().flush()?;
    let mut recipient = String::new();
    std::io::stdin().read_line(&mut recipient)?;
    let recipient = recipient.trim().to_string();
    let rc = recipient.strip_prefix("0x").unwrap_or(&recipient);
    if rc.len() != 40 || !rc.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(anyhow!("recipient must be 0x + 40 hex chars, got '{}'", recipient));
    }

    // Amount of pUSD to unwrap (== backing asset received, 1:1).
    println!("Amount of pUSD to unwrap & withdraw (or 'all' for {:.6}):", pusd_balance);
    print!("> ");
    std::io::stdout().flush()?;
    let mut amt = String::new();
    std::io::stdin().read_line(&mut amt)?;
    let amt = amt.trim();
    let amount: f64 = if amt.eq_ignore_ascii_case("all") {
        pusd_balance
    } else {
        amt.parse().map_err(|_| anyhow!("Invalid amount '{}'", amt))?
    };
    if amount <= 0.0 {
        return Err(anyhow!("Amount must be positive"));
    }
    if amount > pusd_balance {
        return Err(anyhow!("Insufficient balance. Available: {:.6} pUSD", pusd_balance));
    }
    let amount_wei = (amount * 1_000_000.0).round() as u128;
    if amount_wei == 0 {
        return Err(anyhow!("amount rounds to 0"));
    }

    // Plan + confirm.
    println!();
    println!("Plan — unwrap {:.6} pUSD → {}, then withdraw to {} (one WALLET batch):", amount, underlying_label, recipient);
    println!("  1. pUSD.approve(Offramp {}, ∞)", OFFRAMP_ADDRESS);
    println!("  2. Offramp.unwrap({}, DW, {:.6})", underlying_label, amount);
    println!("  3. {}.transfer({}, {:.6})", underlying_label, recipient, amount);
    println!("  Gas: Polymarket relayer (gasless)");
    println!();
    println!("⚠  Money-op on the deposit wallet — Offramp-unwrap is unproven on the DW path; test a small amount first.");
    print!("Type 'yes' to proceed: ");
    std::io::stdout().flush()?;
    let mut confirm = String::new();
    std::io::stdin().read_line(&mut confirm)?;
    if confirm.trim() != "yes" {
        return Err(anyhow!("not confirmed — aborting"));
    }

    super::deposit_wallet::dw_offramp_withdraw(
        &wallet.signing_key, &wallet.signer_address, dw, &wallet.builder_auth,
        underlying, &recipient, amount_wei, /*dry_run=*/ false,
    )?;

    println!();
    println!("✅ Withdrawal complete — {:.6} {} sent to {}", amount, underlying_label, recipient);
    let new_pusd = fetch_pusd_balance(dw);
    let new_underlying = fetch_erc20_balance_6dec(underlying, dw);
    println!("  pUSD   balance: {:.6}  (deposit wallet)", new_pusd);
    println!("  {:<6} balance: {:.6}  (deposit wallet)", underlying_label, new_underlying);
    Ok(())
}

/// ERC-20 stablecoin withdraw via Polymarket relayer (gasless). Works for
/// both pUSD (v2 collateral) and USDC.e (legacy) — both are 6-decimal
/// ERC-20s moved with identical `transfer(to,amount)` calldata; only the
/// token contract address and display label differ.
fn run_withdraw_erc20(wallet: &WalletInfo, token_addr: &str, label: &str, balance: f64) -> Result<()> {
    if balance <= 0.0 {
        println!("No {} to withdraw.", label);
        return Ok(());
    }

    // Prompt for recipient
    println!();
    println!("Enter recipient address (0x...):");
    print!("> ");
    std::io::stdout().flush()?;
    let mut recipient = String::new();
    std::io::stdin().read_line(&mut recipient)?;
    let recipient = recipient.trim();

    if !recipient.starts_with("0x") || recipient.len() != 42 {
        return Err(anyhow!("Invalid address format. Expected 0x + 40 hex chars."));
    }

    // Prompt for amount
    println!("Enter amount to withdraw ({}, e.g. 10.5):", label);
    print!("> ");
    std::io::stdout().flush()?;
    let mut amount_str = String::new();
    std::io::stdin().read_line(&mut amount_str)?;
    let amount: f64 = amount_str.trim().parse()
        .map_err(|_| anyhow!("Invalid amount"))?;

    if amount <= 0.0 {
        return Err(anyhow!("Amount must be positive"));
    }
    if amount > balance {
        return Err(anyhow!("Insufficient balance. Available: {:.6} {}", balance, label));
    }

    // Confirm
    println!();
    println!("Withdraw {:.6} {} to {}", amount, label, recipient);
    println!("Confirm? (y/n):");
    print!("> ");
    std::io::stdout().flush()?;
    let mut confirm = String::new();
    std::io::stdin().read_line(&mut confirm)?;
    if !confirm.trim().eq_ignore_ascii_case("y") {
        println!("Cancelled.");
        return Ok(());
    }

    // Build ERC20 transfer calldata
    let amount_wei = (amount * 1_000_000.0).round() as u128; // pUSD / USDC.e both 6 decimals
    let mut data = Vec::with_capacity(4 + 32 + 32);
    data.extend_from_slice(&ERC20_TRANSFER_SELECTOR);
    data.extend_from_slice(&address_to_bytes32(recipient));
    data.extend_from_slice(&u256_bytes(amount_wei));
    let calldata = format!("0x{}", hex::encode(&data));

    // Submit via relayer
    print!("Submitting withdrawal... ");
    std::io::stdout().flush()?;

    let (tx_id, initial_state) = submit_safe_tx_with_id(
        &wallet.builder_auth, &wallet.signing_key,
        &wallet.signer_address, &wallet.safe_address,
        &token_addr.to_lowercase(), &calldata,
        false, // withdraw CLI always uses relayer (gasless)
    )?;

    println!("submitted.");
    println!("  Transaction ID: {}", tx_id);
    println!("  State: {}", initial_state);

    // Poll for confirmation
    println!("Waiting for confirmation...");
    let mut last_state = initial_state;
    for _ in 0..60 { // max 60 * 5s = 5 minutes
        std::thread::sleep(std::time::Duration::from_secs(5));

        let (state, tx_hash) = poll_transaction(&wallet.builder_auth, &tx_id)?;
        if state != last_state {
            println!("  State: {}", state);
            last_state = state.clone();
        }

        match state.as_str() {
            "STATE_CONFIRMED" => {
                println!();
                println!("Withdrawal confirmed!");
                if !tx_hash.is_empty() {
                    println!("  Tx hash: {}", tx_hash);
                    println!("  https://polygonscan.com/tx/{}", tx_hash);
                }
                let new_balance = fetch_erc20_balance_6dec(token_addr, &wallet.safe_address);
                println!("  New balance: {:.6} {}", new_balance, label);
                return Ok(());
            }
            "STATE_FAILED" | "STATE_INVALID" => {
                return Err(anyhow!("Withdrawal failed: {}", state));
            }
            _ => continue,
        }
    }

    println!("Timed out waiting for confirmation. Check PolygonScan manually.");
    Ok(())
}

/// Withdraw pUSD as a supported backing asset via the CollateralOfframp,
/// then send that asset to `recipient`. Three gasless relayer txs are each
/// awaited before the next (Safe nonce ordering + step dependencies):
///   1. pUSD.approve(Offramp, ∞)              — skipped if allowance is enough
///   2. Offramp.unwrap(asset, Safe, amount)   — burns pUSD, returns the asset
///   3. asset.transfer(recipient, amount)     — standard ERC-20 withdrawal
fn run_withdraw_pusd_to_asset(
    wallet: &WalletInfo,
    pusd_balance: f64,
    underlying: &str,
    underlying_label: &str,
) -> Result<()> {
    if pusd_balance <= 0.0 {
        println!("No pUSD to withdraw.");
        return Ok(());
    }

    // Recipient
    println!();
    println!("Enter recipient address (0x...) to receive {}:", underlying_label);
    print!("> ");
    std::io::stdout().flush()?;
    let mut recipient = String::new();
    std::io::stdin().read_line(&mut recipient)?;
    let recipient = recipient.trim().to_string();
    if !recipient.starts_with("0x") || recipient.len() != 42 {
        return Err(anyhow!("Invalid address format. Expected 0x + 40 hex chars."));
    }

    // Amount (pUSD and both backing assets are 1:1, all 6-decimal).
    println!("Enter amount of pUSD to unwrap & withdraw (e.g. 10.5):");
    print!("> ");
    std::io::stdout().flush()?;
    let mut amount_str = String::new();
    std::io::stdin().read_line(&mut amount_str)?;
    let amount: f64 = amount_str.trim().parse()
        .map_err(|_| anyhow!("Invalid amount"))?;
    if amount <= 0.0 {
        return Err(anyhow!("Amount must be positive"));
    }
    if amount > pusd_balance {
        return Err(anyhow!("Insufficient balance. Available: {:.6} pUSD", pusd_balance));
    }
    let amount_wei = (amount * 1_000_000.0).round() as u128;

    // Pre-flight allowance check (decides whether step 1 is needed).
    let allowance = fetch_allowance_6dec(PUSD_ADDRESS, &wallet.safe_address, OFFRAMP_ADDRESS);
    let needs_approve = allowance < amount_wei;

    // Plan + confirm.
    println!();
    println!("Plan — unwrap {:.6} pUSD → {}, then withdraw to {}:", amount, underlying_label, recipient);
    if needs_approve {
        println!("  1. pUSD.approve(Offramp {}, ∞)", OFFRAMP_ADDRESS);
    } else {
        println!("  1. (approve SKIPPED — pUSD→Offramp allowance already sufficient)");
    }
    println!("  2. Offramp.unwrap({}, Safe, {:.6})", underlying_label, amount);
    println!("  3. {}.transfer({}, {:.6})", underlying_label, recipient, amount);
    println!("  Gas: Polymarket relayer (gasless)");
    println!("Confirm? (y/n):");
    print!("> ");
    std::io::stdout().flush()?;
    let mut confirm = String::new();
    std::io::stdin().read_line(&mut confirm)?;
    if !confirm.trim().eq_ignore_ascii_case("y") {
        println!("Cancelled.");
        return Ok(());
    }
    println!();

    // Step 1: approve pUSD → Offramp (one-time, unlimited).
    if needs_approve {
        let calldata = build_approve_calldata(OFFRAMP_ADDRESS, &U256_MAX_BYTES);
        relayer_submit_and_confirm(wallet, PUSD_ADDRESS, &calldata,
            "Step 1/3 approve pUSD→Offramp (∞)")?;
    } else {
        println!("  Step 1/3 approve — SKIPPED (allowance already sufficient)");
    }

    // Step 2: unwrap pUSD into the selected backing asset in the Safe.
    let unwrap_calldata = build_unwrap_calldata(underlying, &wallet.safe_address, amount_wei);
    relayer_submit_and_confirm(wallet, OFFRAMP_ADDRESS, &unwrap_calldata,
        &format!("Step 2/3 unwrap pUSD→{}", underlying_label))?;

    // Step 3: send the unwrapped asset to the recipient.
    let transfer_calldata = build_transfer_calldata(&recipient, amount_wei);
    relayer_submit_and_confirm(wallet, underlying, &transfer_calldata,
        &format!("Step 3/3 withdraw {} → recipient", underlying_label))?;

    println!();
    println!("Withdrawal complete — {:.6} {} sent to {}", amount, underlying_label, recipient);
    let new_pusd = fetch_pusd_balance(&wallet.safe_address);
    let new_underlying = fetch_erc20_balance_6dec(underlying, &wallet.safe_address);
    println!("  pUSD   balance: {:.6}  (Safe)", new_pusd);
    println!("  {:<6} balance: {:.6}  (Safe)", underlying_label, new_underlying);
    Ok(())
}

/// Read ERC-20 `allowance(owner, spender)` in raw 6-decimal units. An
/// "infinite" approval (2^256-1) overflows u128 and is reported as
/// `u128::MAX` — fine, since callers only `>=`-compare against an amount.
fn fetch_allowance_6dec(token: &str, owner: &str, spender: &str) -> u128 {
    let mut data = Vec::with_capacity(4 + 64);
    data.extend_from_slice(&ERC20_ALLOWANCE_SELECTOR);
    data.extend_from_slice(&address_to_bytes32(owner));
    data.extend_from_slice(&address_to_bytes32(spender));
    let calldata = format!("0x{}", hex::encode(&data));
    match deploy_wallet::eth_call(token, &calldata) {
        Some(result) => {
            let hex_str = result.strip_prefix("0x").unwrap_or(&result).trim_start_matches('0');
            if hex_str.is_empty() { return 0; }
            u128::from_str_radix(hex_str, 16).unwrap_or(u128::MAX)
        }
        None => 0,
    }
}

/// ABI-encode `approve(spender, amount)`. `amount_bytes` is a pre-built
/// 32-byte big-endian blob (use `U256_MAX_BYTES` for infinite approval).
fn build_approve_calldata(spender: &str, amount_bytes: &[u8; 32]) -> String {
    let mut buf = Vec::with_capacity(4 + 64);
    buf.extend_from_slice(&ERC20_APPROVE_SELECTOR);
    buf.extend_from_slice(&address_to_bytes32(spender));
    buf.extend_from_slice(amount_bytes);
    format!("0x{}", hex::encode(buf))
}

/// ABI-encode `unwrap(address asset, address to, uint256 amount)` for the
/// CollateralOfframp. `asset` is the underlying to receive (USDC/USDC.e), `to`
/// the recipient of that underlying, `amount` the pUSD to burn (6-dec).
fn build_unwrap_calldata(asset: &str, to: &str, amount_wei: u128) -> String {
    let mut buf = Vec::with_capacity(4 + 96);
    buf.extend_from_slice(&UNWRAP_SELECTOR);
    buf.extend_from_slice(&address_to_bytes32(asset));
    buf.extend_from_slice(&address_to_bytes32(to));
    buf.extend_from_slice(&u256_bytes(amount_wei));
    format!("0x{}", hex::encode(buf))
}

/// ABI-encode `transfer(to, amount)`.
fn build_transfer_calldata(to: &str, amount_wei: u128) -> String {
    let mut buf = Vec::with_capacity(4 + 64);
    buf.extend_from_slice(&ERC20_TRANSFER_SELECTOR);
    buf.extend_from_slice(&address_to_bytes32(to));
    buf.extend_from_slice(&u256_bytes(amount_wei));
    format!("0x{}", hex::encode(buf))
}

/// Submit one Safe `execTransaction` through the gasless relayer and block
/// until it reaches a terminal state. `Ok` on STATE_CONFIRMED; `Err` on
/// FAILED/INVALID or a 5-minute confirmation timeout.
fn relayer_submit_and_confirm(wallet: &WalletInfo, to: &str, calldata: &str, step: &str) -> Result<()> {
    print!("  {} … ", step);
    std::io::stdout().flush()?;
    let (tx_id, initial_state) = submit_safe_tx_with_id(
        &wallet.builder_auth, &wallet.signing_key,
        &wallet.signer_address, &wallet.safe_address,
        &to.to_lowercase(), calldata,
        false, // relayer (gasless)
    )?;
    println!("submitted (id={}, {})", tx_id, initial_state);
    let mut last_state = initial_state;
    for _ in 0..60 { // 60 × 5 s = 5 min
        std::thread::sleep(std::time::Duration::from_secs(5));
        let (state, tx_hash) = poll_transaction(&wallet.builder_auth, &tx_id)?;
        if state != last_state {
            println!("    state: {}", state);
            last_state = state.clone();
        }
        match state.as_str() {
            "STATE_CONFIRMED" => {
                if !tx_hash.is_empty() {
                    println!("    https://polygonscan.com/tx/{}", tx_hash);
                }
                return Ok(());
            }
            "STATE_FAILED" | "STATE_INVALID" => {
                return Err(anyhow!("{} failed on-chain: {}", step, state));
            }
            _ => continue,
        }
    }
    Err(anyhow!("{}: timed out waiting for confirmation (check PolygonScan)", step))
}

/// POL withdraw: Safe sends native POL to recipient via execTransaction
/// with `value = amount_wei`, `data = 0x`. The Polymarket relayer is
/// USDC-pinned so this MUST go through the on-chain path — the signer
/// EOA broadcasts execTransaction directly and pays gas in POL out of
/// its own balance. ~0.005-0.01 POL covers gas at typical Polygon
/// prices; if the signer is empty the broadcast errors out cleanly.
fn run_withdraw_pol(wallet: &WalletInfo, balance: f64) -> Result<()> {
    if balance <= 0.0 {
        println!("No POL to withdraw.");
        return Ok(());
    }

    // Signer gas check — bail early with a clear error if signer EOA
    // can't pay the broadcast fee. Typical execTransaction gas ≈ 80k;
    // at 30 gwei that's ~0.0024 POL.
    let signer_pol = fetch_pol_balance(&wallet.signer_address).unwrap_or(0.0);
    println!();
    println!("Signer EOA POL balance: {:.6} (needed for gas)", signer_pol);
    if signer_pol < 0.005 {
        return Err(anyhow!(
            "Signer EOA has insufficient POL for gas ({:.6} < 0.005). \
             Fund the signer address first.",
            signer_pol,
        ));
    }

    // Prompt for recipient (default to signer EOA — the common case).
    println!();
    println!("Enter recipient address (0x...) [default: signer EOA {}]:",
             wallet.signer_address);
    print!("> ");
    std::io::stdout().flush()?;
    let mut recipient = String::new();
    std::io::stdin().read_line(&mut recipient)?;
    let recipient = recipient.trim();
    let recipient = if recipient.is_empty() {
        wallet.signer_address.as_str()
    } else {
        if !recipient.starts_with("0x") || recipient.len() != 42 {
            return Err(anyhow!("Invalid address format. Expected 0x + 40 hex chars."));
        }
        recipient
    };

    // Prompt for amount. "all" leaves a 0.001 POL dust so the Safe can
    // still pay any pending tx the operator may have queued elsewhere
    // (cheap hedge against accidental zero-balance lockouts).
    println!("Enter amount to withdraw (POL, or 'all'):");
    print!("> ");
    std::io::stdout().flush()?;
    let mut amount_str = String::new();
    std::io::stdin().read_line(&mut amount_str)?;
    let amount_input = amount_str.trim().to_string();

    let amount: f64 = if amount_input.eq_ignore_ascii_case("all") {
        (balance - 0.001).max(0.0)
    } else {
        amount_input.parse().map_err(|_| anyhow!("Invalid amount"))?
    };

    if amount <= 0.0 {
        return Err(anyhow!("Amount must be positive"));
    }
    if amount > balance {
        return Err(anyhow!("Insufficient balance. Available: {:.6} POL", balance));
    }

    // Confirm
    println!();
    println!("Withdraw {:.6} POL from Safe → {}", amount, recipient);
    println!("Path: on-chain (signer pays gas, ~0.003 POL)");
    println!("Confirm? (y/n):");
    print!("> ");
    std::io::stdout().flush()?;
    let mut confirm = String::new();
    std::io::stdin().read_line(&mut confirm)?;
    if !confirm.trim().eq_ignore_ascii_case("y") {
        println!("Cancelled.");
        return Ok(());
    }

    // Build SafeTx: to=recipient, value=amount_wei, data=empty.
    // Safe will execute `recipient.call{value: amount_wei}("")`.
    let amount_wei: u128 = (amount * 1e18) as u128;
    print!("Submitting on-chain Safe execTransaction... ");
    std::io::stdout().flush()?;

    let tx_hash = super::onchain_tx::submit_safe_tx_onchain_with_value(
        &wallet.signing_key,
        &wallet.signer_address,
        &wallet.safe_address,
        recipient,
        "0x",            // empty inner calldata
        amount_wei,
    )?;
    println!("submitted.");
    println!("  Tx hash: {}", tx_hash);
    println!("  https://polygonscan.com/tx/{}", tx_hash);

    // Poll on-chain receipt for confirmation. ~4-8s typical on Polygon.
    println!("Waiting for confirmation...");
    for i in 0..30 {  // max 30 × 2s = 60s
        std::thread::sleep(std::time::Duration::from_secs(2));
        match super::onchain_tx::poll_onchain_tx(&tx_hash) {
            Ok((state, _)) => {
                match state.as_str() {
                    "CONFIRMED" => {
                        println!();
                        println!("Withdrawal confirmed!");
                        let new_balance = fetch_pol_balance(&wallet.safe_address)
                            .unwrap_or(0.0);
                        println!("  New Safe POL balance: {:.6}", new_balance);
                        return Ok(());
                    }
                    "STATE_FAILED" => {
                        return Err(anyhow!("Tx reverted on-chain: {}", tx_hash));
                    }
                    _ => {
                        if i % 5 == 4 {
                            print!(".");
                            std::io::stdout().flush()?;
                        }
                        continue;
                    }
                }
            }
            Err(e) => {
                eprintln!("\n  poll error (will retry): {}", e);
                continue;
            }
        }
    }

    println!();
    println!("Timed out waiting for confirmation. Check PolygonScan manually:");
    println!("  https://polygonscan.com/tx/{}", tx_hash);
    Ok(())
}

/// Submit Safe transaction and return (transactionID, state).
///
/// When `gas_via_signer=true`, bypasses the Polymarket relayer and
/// broadcasts directly on-chain via the signer EOA (paying gas in MATIC
/// from its balance). The returned `transactionID` is then the Polygon
/// tx hash, and `poll_transaction` routes the poll to the chain instead
/// of the relayer's `/transaction?id=...` endpoint.
pub(crate) fn submit_safe_tx_with_id(
    auth: &PolyAuth, key: &k256::ecdsa::SigningKey,
    signer: &str, safe: &str, to: &str, data: &str,
    gas_via_signer: bool,
) -> Result<(String, String)> {
    if gas_via_signer {
        // On-chain path: EOA-paid execTransaction broadcast.
        // Returned `tx_hash` is the Polygon tx hash; state starts as
        // "PENDING" and resolves via `poll_transaction`.
        let tx_hash = super::onchain_tx::submit_safe_tx_onchain(
            key, signer, safe, to, data,
        )?;
        return Ok((tx_hash, "PENDING".to_string()));
    }
    // Legacy gasless-relayer path.
    // Use on-chain nonce (more reliable than relayer /nonce endpoint)
    let nonce = get_onchain_safe_nonce(safe);

    let domain_sep = build_safe_tx_domain(safe);
    let safe_tx_type_hash = deploy_wallet::keccak256(
        b"SafeTx(address to,uint256 value,bytes data,uint8 operation,uint256 safeTxGas,uint256 baseGas,uint256 gasPrice,address gasToken,address refundReceiver,uint256 nonce)");

    let data_bytes = hex::decode(data.strip_prefix("0x").unwrap_or(data)).unwrap_or_default();
    let data_hash = deploy_wallet::keccak256(&data_bytes);

    let zero = "0x0000000000000000000000000000000000000000";
    let mut struct_buf = Vec::with_capacity(11 * 32);
    struct_buf.extend_from_slice(&safe_tx_type_hash);
    struct_buf.extend_from_slice(&address_to_bytes32(to));
    struct_buf.extend_from_slice(&u256_bytes(0)); // value
    struct_buf.extend_from_slice(&data_hash);
    struct_buf.extend_from_slice(&u256_bytes(0)); // operation
    struct_buf.extend_from_slice(&u256_bytes(0)); // safeTxGas
    struct_buf.extend_from_slice(&u256_bytes(0)); // baseGas
    struct_buf.extend_from_slice(&u256_bytes(0)); // gasPrice
    struct_buf.extend_from_slice(&address_to_bytes32(zero)); // gasToken
    struct_buf.extend_from_slice(&address_to_bytes32(zero)); // refundReceiver
    struct_buf.extend_from_slice(&u256_bytes(nonce as u128));
    let struct_hash = deploy_wallet::keccak256(&struct_buf);

    let domain_type_hash = deploy_wallet::keccak256(b"EIP712Domain(uint256 chainId,address verifyingContract)");
    let _ = domain_type_hash; // domain_sep already computed
    let signature = sign_safe_tx(&domain_sep, &struct_hash, key)?;

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
            "gasToken": zero,
            "refundReceiver": zero,
        },
        "type": "SAFE",
        "metadata": "",
    });

    let body_str = body.to_string();
    let headers = auth.sign_request("POST", "/submit", &body_str);
    let url = format!("{}/submit", RELAYER_URL);
    let json = relayer_http(reqwest::Method::POST, url, headers, Some(body_str))?;
    let tx_id = json.get("transactionID").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let state = json.get("state").and_then(|v| v.as_str()).unwrap_or("").to_string();
    Ok((tx_id, state))
}

/// Get Safe nonce directly from chain (more reliable than relayer /nonce).
fn get_onchain_safe_nonce(safe_address: &str) -> u64 {
    // nonce() selector = 0xaffed0e0
    let calldata = format!("0xaffed0e0");
    match deploy_wallet::eth_call(safe_address, &calldata) {
        Some(result) => {
            let hex_str = result.strip_prefix("0x").unwrap_or(&result);
            u64::from_str_radix(hex_str.trim_start_matches('0'), 16).unwrap_or(0)
        }
        None => 0,
    }
}

fn build_safe_tx_domain(safe_address: &str) -> [u8; 32] {
    let type_hash = deploy_wallet::keccak256(b"EIP712Domain(uint256 chainId,address verifyingContract)");
    let mut buf = Vec::with_capacity(3 * 32);
    buf.extend_from_slice(&type_hash);
    buf.extend_from_slice(&u256_bytes(137)); // Polygon chainId
    buf.extend_from_slice(&address_to_bytes32(safe_address));
    deploy_wallet::keccak256(&buf)
}

fn sign_safe_tx(domain_sep: &[u8; 32], struct_hash: &[u8; 32], key: &k256::ecdsa::SigningKey) -> Result<String> {
    // EIP-712 digest
    let mut buf = Vec::with_capacity(2 + 32 + 32);
    buf.push(0x19);
    buf.push(0x01);
    buf.extend_from_slice(domain_sep);
    buf.extend_from_slice(struct_hash);
    let eip712_hash = deploy_wallet::keccak256(&buf);

    // eth_sign prefix (Safe-specific)
    let mut eth_msg = Vec::with_capacity(28 + 32);
    eth_msg.extend_from_slice(b"\x19Ethereum Signed Message:\n32");
    eth_msg.extend_from_slice(&eip712_hash);
    let digest = deploy_wallet::keccak256(&eth_msg);

    let (sig, recid) = key.sign_prehash_recoverable(&digest)
        .map_err(|e| anyhow!("Signing failed: {}", e))?;
    let mut sig_bytes = [0u8; 65];
    sig_bytes[..64].copy_from_slice(&sig.to_bytes());
    sig_bytes[64] = recid.to_byte() + 31; // Safe v = 31/32
    Ok(format!("0x{}", hex::encode(sig_bytes)))
}

/// Poll transaction status from relayer (`tx_id` = relayer UUID) or
/// directly on-chain (`tx_id` = Polygon tx hash, starts with `0x`).
/// The on-chain path kicks in whenever `tx_id` looks like a 32-byte
/// hash — the on-chain submit path always returns that form, the
/// relayer's form is a UUID without `0x` prefix.
pub(crate) fn poll_transaction(auth: &PolyAuth, tx_id: &str) -> Result<(String, String)> {
    if tx_id.starts_with("0x") && tx_id.len() == 66 {
        return super::onchain_tx::poll_onchain_tx(tx_id);
    }
    let path = format!("/transaction?id={}", tx_id);
    let headers = auth.sign_request("GET", &path, "");
    let url = format!("{}{}", RELAYER_URL, path);
    let json = relayer_http(reqwest::Method::GET, url, headers, None)?;
    // API returns an array — take first element
    let entry = if json.is_array() {
        json.get(0).cloned().unwrap_or(serde_json::Value::Null)
    } else {
        json
    };
    let state = entry.get("state").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let tx_hash = entry.get("transactionHash").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let error_msg = entry.get("errorMsg").and_then(|v| v.as_str()).unwrap_or("").to_string();
    if !error_msg.is_empty() && state == "STATE_FAILED" {
        return Err(anyhow!("Transaction failed: {}", error_msg));
    }
    Ok((state, tx_hash))
}

// ════════════════════════════════════════════════════════════════
// positions command
// ════════════════════════════════════════════════════════════════

const DATA_API_BASE: &str = "https://data-api.polymarket.com";
const CTF_CONTRACT: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
/// v2 adapter for `splitPosition` / `mergePositions` / `redeemPositions`
/// on standard binary markets. Exposes the SAME Solidity ABI as the v1
/// CTF (selectors `0x72ce4275` / `0x9e7212ad` / `0x01b7037c`) but
/// internally handles pUSD ⇄ USDC.e wrap/unwrap. The `collateralToken`
/// argument is ignored by the adapter; we still pass pUSD in that slot
/// for semantic clarity.
///
/// **Address rotation 2026-05-03**: Polymarket migrated the CTF
/// collateral adapters to a new pair of addresses. The legacy
/// adapters (`0xADa100874d…` / `0xAdA200001000…`) now return
/// `RelayerError "calls to legacy collateral adapter … are no longer
/// accepted"` from `POST /submit`, breaking redeem/split/merge
/// through the gasless relayer path. Direct on-chain calls to the
/// old addresses also fail (deployer disabled them).
const CTF_COLLATERAL_ADAPTER_V2: &str = "0xAdA100Db00Ca00073811820692005400218FcE1f";
/// Same, for neg-risk (multi-outcome) markets. BTC up-or-down 5m is
/// standard — this address is reserved for if/when we add neg-risk
/// market support. Migrated 2026-05-03 alongside the standard adapter.
#[allow(dead_code)]
const NEG_RISK_CTF_COLLATERAL_ADAPTER_V2: &str = "0xadA2005600Dec949baf300f4C6120000bDB6eAab";

/// Interpret a `clob_version` string. v2 is now the default: ONLY an
/// explicit `v1` / `1` selects v1; everything else — `v2`, `2`, empty,
/// unknown — resolves to v2. (Pre-2026-06 this was inverted, defaulting
/// to v1; the cutover is complete so the legacy chain is opt-in only.)
pub(crate) fn is_v2_from_str(s: &str) -> bool {
    !matches!(s.trim().to_ascii_lowercase().as_str(), "v1" | "1")
}

/// Read `clob_version` from the polymarket exchange section of the
/// config. Honours `--config <path>` (or `$HEXBOT_CONFIG`) when present,
/// else falls back to `config/live_polymaker.toml`. Returns `true`
/// unless the config explicitly pins `clob_version = "v1"` — v2 is the
/// default for missing config / missing key / empty value.
///
/// Mirrors `read_gas_via_signer_wallet_flag`'s "CLI inherits config"
/// pattern so operators don't have to pass the flag on every
/// `hexbot redeem` / `hexbot split` invocation.
pub(crate) fn read_clob_v2_flag() -> bool {
    let path = crate::exchange::polymarket::cli_account::config_path()
        .unwrap_or_else(|| "config/live_polymaker.toml".to_string());
    let cfg = match crate::config::Config::load(std::path::Path::new(&path)) {
        Ok(c) => c,
        // No readable config → assume the modern default (v2).
        Err(_) => return true,
    };
    cfg.exchanges.iter()
        .find(|e| e.name == "polymarket")
        .map(|p| is_v2_from_str(&p.clob_version))
        .unwrap_or(true)
}

/// Pick (target contract, collateralToken arg) for split / merge /
/// redeem based on CLOB version + neg-risk flag.
///   v1: → (CTF contract, USDC.e)
///   v2 std: → (CtfCollateralAdapter, pUSD)
///   v2 neg-risk: → (NegRiskCtfCollateralAdapter, pUSD)
pub(crate) fn ctf_target(is_v2: bool, neg_risk: bool) -> (&'static str, &'static str) {
    if is_v2 {
        let target = if neg_risk { NEG_RISK_CTF_COLLATERAL_ADAPTER_V2 } else { CTF_COLLATERAL_ADAPTER_V2 };
        (target, PUSD_ADDRESS)
    } else {
        (CTF_CONTRACT, USDCE_ADDRESS)
    }
}
// redeemPositions(address,bytes32,bytes32,uint256[]) selector
const REDEEM_SELECTOR: [u8; 4] = [0x01, 0xb7, 0x03, 0x7c];

// ── CTF view-function selectors (on the Conditional-Tokens Framework
//    contract `CTF_CONTRACT`) used to compute the ERC-1155 positionId of a
//    single outcome leg in a GIVEN collateral's id-space, and to read its
//    on-chain balance. This is how we tell whether a resolved market is
//    still redeemable: the data-api `asset` id is in the USDC.e (v1)
//    collateral space, but v2 holdings are pUSD-collateralized and have
//    DIFFERENT positionIds — so on-chain balanceOf MUST be computed from the
//    active collateral, never matched against the data-api asset id.
const GET_COLLECTION_ID_SELECTOR: [u8; 4] = [0x85, 0x62, 0x96, 0xf7]; // getCollectionId(bytes32,bytes32,uint256)
const GET_POSITION_ID_SELECTOR:   [u8; 4] = [0x39, 0xdd, 0x75, 0x30]; // getPositionId(address,bytes32)
const ERC1155_BALANCE_OF_SELECTOR: [u8; 4] = [0x00, 0xfd, 0xd5, 0x8e]; // balanceOf(address,uint256)

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct PositionRecord {
    #[serde(default, rename = "conditionId")]
    condition_id: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    outcome: String,
    /// 0-based outcome index from the data-api (Up/Yes=0, Down/No=1). The
    /// on-chain ERC-1155 positionId leg is `indexSet = 1 << outcome_index`.
    #[serde(default, rename = "outcomeIndex")]
    outcome_index: i64,
    #[serde(default)]
    size: f64,
    #[serde(default, rename = "avgPrice")]
    avg_price: f64,
    #[serde(default, rename = "curPrice")]
    cur_price: f64,
    #[serde(default, rename = "initialValue")]
    initial_value: f64,
    #[serde(default, rename = "currentValue")]
    current_value: f64,
    #[serde(default, rename = "cashPnl")]
    cash_pnl: f64,
    #[serde(default, rename = "percentPnl")]
    percent_pnl: f64,
    #[serde(default)]
    redeemable: bool,
    #[serde(default)]
    slug: String,
    #[serde(default, rename = "eventSlug")]
    event_slug: String,
    #[serde(default, rename = "endDate")]
    end_date: String,
}

/// Compute the on-chain ERC-1155 positionId of one outcome leg in `collateral`'s
/// id-space, via the CTF's pure view functions (getCollectionId → getPositionId).
/// Returns a 0x-hex uint256 ready for an ERC-1155 balanceOf, or None on RPC error.
fn ctf_position_id(condition_id: &str, index_set: u64, collateral: &str) -> Option<String> {
    let cid_bytes = hex::decode(condition_id.strip_prefix("0x").unwrap_or(condition_id)).ok()?;
    let mut cid_padded = [0u8; 32];
    let start = 32 - cid_bytes.len().min(32);
    cid_padded[start..].copy_from_slice(&cid_bytes[..cid_bytes.len().min(32)]);

    // getCollectionId(parentCollectionId=0, conditionId, indexSet)
    let mut data = Vec::with_capacity(4 + 32 * 3);
    data.extend_from_slice(&GET_COLLECTION_ID_SELECTOR);
    data.extend_from_slice(&[0u8; 32]);
    data.extend_from_slice(&cid_padded);
    data.extend_from_slice(&u256_bytes(index_set as u128));
    let collection = deploy_wallet::eth_call(CTF_CONTRACT, &format!("0x{}", hex::encode(&data)))?;
    let collection_bytes = hex::decode(collection.strip_prefix("0x").unwrap_or(&collection)).ok()?;
    if collection_bytes.len() < 32 { return None; }

    // getPositionId(collateral, collectionId)
    let mut data = Vec::with_capacity(4 + 32 * 2);
    data.extend_from_slice(&GET_POSITION_ID_SELECTOR);
    data.extend_from_slice(&address_to_bytes32(collateral));
    data.extend_from_slice(&collection_bytes[..32]);
    deploy_wallet::eth_call(CTF_CONTRACT, &format!("0x{}", hex::encode(&data)))
}

/// ERC-1155 balanceOf(owner, positionId) on the CTF, in token units (6
/// decimals, matching the collateral). `position_id_hex` is a 0x uint256.
fn ctf_erc1155_balance(owner: &str, position_id_hex: &str) -> f64 {
    let pid = position_id_hex.strip_prefix("0x").unwrap_or(position_id_hex);
    let pid_bytes = match hex::decode(pid) { Ok(b) => b, Err(_) => return 0.0 };
    let mut pid_padded = [0u8; 32];
    let start = 32 - pid_bytes.len().min(32);
    pid_padded[start..].copy_from_slice(&pid_bytes[..pid_bytes.len().min(32)]);
    let mut data = Vec::with_capacity(4 + 32 * 2);
    data.extend_from_slice(&ERC1155_BALANCE_OF_SELECTOR);
    data.extend_from_slice(&address_to_bytes32(owner));
    data.extend_from_slice(&pid_padded);
    match deploy_wallet::eth_call(CTF_CONTRACT, &format!("0x{}", hex::encode(&data))) {
        Some(result) => {
            let h = result.strip_prefix("0x").unwrap_or(&result);
            let trimmed = h.trim_start_matches('0');
            if trimmed.is_empty() { return 0.0; }
            u128::from_str_radix(trimmed, 16).unwrap_or(0) as f64 / 1_000_000.0
        }
        None => 0.0,
    }
}

/// On-chain ERC-1155 balances of the Up (indexSet 1) and Down (indexSet 2)
/// outcome legs of `condition_id`, held by `owner`, in the ACTIVE collateral's
/// id-space (pUSD for v2, USDC.e for v1). Returns `(up_qty, down_qty)` in token
/// units (6 decimals).
///
/// This is the authoritative source for split-seed inventory: the data-api
/// `/positions` feed keys a deposit wallet's holdings by the USDC.e (v1)
/// positionId, so the pUSD (v2) tokens minted by `dw_split` never match the
/// event's `clob_token_ids` and read 0 — which makes the strategy believe it
/// is flat and quote only the buy side. Costs ~6 eth_calls (2 view calls per
/// leg + balanceOf), so call it off the strategy thread.
pub fn ctf_event_outcome_balances(
    owner: &str,
    condition_id: &str,
    is_v2: bool,
) -> (f64, f64) {
    // CLOB outcome tokens are USDC.e-collateralized in BOTH v1 (direct) and v2
    // (v2 mints them via the CtfCollateralAdapter); pUSD is only the cash leg,
    // so the position id-space is always USDC.e.
    let _ = is_v2;
    let collateral = USDCE_ADDRESS;
    let up = ctf_position_id(condition_id, 1, collateral)
        .map(|pid| ctf_erc1155_balance(owner, &pid))
        .unwrap_or(0.0);
    let down = ctf_position_id(condition_id, 2, collateral)
        .map(|pid| ctf_erc1155_balance(owner, &pid))
        .unwrap_or(0.0);
    (up, down)
}

pub fn run_positions() -> Result<()> {
    // Config path: `--config <path>` (or $HEXBOT_CONFIG) wins, else the
    // legacy positional `hexbot positions <config.toml>`, else the live
    // polymaker config so `hexbot positions` without args mirrors what the
    // bot would actually run. Only used to label the output — all data we
    // fetch (balances, positions) is data-api / on-chain.
    //
    // Read positionals through `cli_args()` (NOT raw `std::env::args()`) so
    // the global `--account <id>` / `--instance <id>` flags — and their
    // *values* — are stripped first; otherwise `hexbot positions --account
    // zhu02` would mistake the account id `zhu02` for the config path.
    let positional: Option<String> = crate::exchange::polymarket::cli_account::cli_args()
        .find(|a| !a.starts_with('-'));
    let config_path = crate::exchange::polymarket::cli_account::config_path()
        .or(positional)
        .unwrap_or_else(|| "config/live_polymaker.toml".to_string());

    let private_key = std::env::var("POLY_PRIVATE_KEY").unwrap_or_default();
    if private_key.is_empty() {
        return Err(no_wallet_creds_err());
    }
    let signing_key = parse_private_key(&private_key)?;
    let signer_address = to_checksum_address(&derive_eth_address_from_key(&signing_key));
    let safe_address = to_checksum_address(&derive_safe_address(&signer_address));

    // POLY_1271 holds funds + positions in the deposit wallet, not the Safe.
    let sig_type = std::env::var("POLY_SIGNATURE_TYPE").unwrap_or_default().to_ascii_lowercase();
    let is_dw = sig_type == "poly_1271" || sig_type == "deposit_wallet";
    let (wallet_addr, wallet_label) = if is_dw {
        let dw = super::deposit_wallet::resolve_deposit_wallet(&signer_address)
            .unwrap_or_else(|_| safe_address.clone());
        (dw, "Deposit")
    } else {
        (safe_address.clone(), "Safe")
    };

    println!("=== Polymarket Positions ===");
    println!("Config:        {}", config_path);
    println!("Wallet ({}): {}", wallet_label, wallet_addr);
    println!("Signer (EOA):  {}", signer_address);
    println!();

    // pUSD (v2 collateral) is the trading balance.
    let pusd = fetch_pusd_balance(&wallet_addr);
    println!("{:<6} balance: {:>12.4}", "pUSD", pusd);
    println!();

    // "balance" below is used for net-worth math.
    let balance = pusd;

    // Fetch positions
    let url = format!("{}/positions?user={}&sizeThreshold=0&limit=500", DATA_API_BASE, wallet_addr);
    let mut positions: Vec<PositionRecord> = match crate::async_rt::blocking_get_text(&url) {
        Ok(text) => serde_json::from_str(&text).unwrap_or_default(),
        Err(e) => {
            println!("Failed to fetch positions: {}", e);
            return Ok(());
        }
    };

    if positions.is_empty() {
        println!("No open positions.");
        return Ok(());
    }

    // Sort by event_slug (case-insensitive) so the same event's Up/Down
    // rows land next to each other and consecutive runs produce stable
    // diffable output. Tiebreaker on outcome keeps Up before Down.
    positions.sort_by(|a, b| {
        a.event_slug.to_ascii_lowercase()
            .cmp(&b.event_slug.to_ascii_lowercase())
            .then_with(|| a.outcome.cmp(&b.outcome))
    });

    // Round to 0.00001 precision.
    let round5 = |x: f64| (x * 100_000.0).round() / 100_000.0;

    // Print header — widths sized for 5-decimal values. Size = the data-api
    // `/positions` size (the open feed we enumerate); no CLOB / on-chain reads.
    let slug_w = 32;
    println!("{:<50} {:<slug$} {:>8} {:>11} {:>9} {:>12} {:>5}",
        "Market", "Event Slug", "Outcome", "Size", "CurPx", "Value", "Redm",
        slug = slug_w);
    let sep_w = 50 + 1 + slug_w + 1 + 8 + 1 + 11 + 1 + 9 + 1 + 12 + 1 + 5;
    println!("{}", "-".repeat(sep_w));

    let mut total_value = 0.0;

    // NOTE: enumeration is data-api-driven over the OPEN /positions feed only —
    // a conditionId absent from it can't be surfaced here (neither the CLOB nor
    // ERC-1155 offers owner-side enumeration). Resolved markets drop out of this
    // feed and are no longer scanned/printed here; use `hexbot redeem` to find
    // and claim still-held winning legs.
    for p in &positions {
        let title = if p.title.len() > 48 { format!("{}...", &p.title[..45]) } else { p.title.clone() };
        let slug = if p.event_slug.chars().count() > slug_w {
            let take = slug_w.saturating_sub(1);
            let head: String = p.event_slug.chars().take(take).collect();
            format!("{}…", head)
        } else {
            p.event_slug.clone()
        };
        let redeem = if p.redeemable { "Y" } else { "" };
        let size = round5(p.size); // data-api `/positions` size

        let cur = round5(p.cur_price);
        let val = round5(size * p.cur_price);
        println!("{:<50} {:<slug$} {:>8} {:>11.5} {:>9.5} {:>12.4} {:>5}",
            title, slug, p.outcome, size, cur, val, redeem,
            slug = slug_w);
        total_value += val;
    }

    let total_value = round5(total_value);
    let balance = round5(balance);

    println!("{}", "-".repeat(sep_w));
    println!("{:<50} {:<slug$} {:>8} {:>11} {:>9} {:>12.4}",
        format!("Total ({} positions)", positions.len()), "", "", "", "", total_value,
        slug = slug_w);
    println!();
    println!(
        "Total balance: {:.4} USD (pUSD {:.4} + positions {:.4})",
        balance + total_value, pusd, total_value,
    );
    // Size = the data-api `/positions` size; Value = Size × CurPx.
    println!("(Size = data-api /positions size)");

    Ok(())
}

// ════════════════════════════════════════════════════════════════
// token_check command (read-only diagnostic)
// ════════════════════════════════════════════════════════════════

/// Parse a decimal uint256 string into a 32-byte big-endian array.
/// Returns None on a non-digit or on overflow past 256 bits.
fn dec_to_bytes32(dec: &str) -> Option<[u8; 32]> {
    let mut bytes = [0u8; 32];
    for ch in dec.trim().chars() {
        let d = ch.to_digit(10)? as u16;
        let mut carry = d; // bytes = bytes * 10 + d
        for b in bytes.iter_mut().rev() {
            let v = (*b as u16) * 10 + carry;
            *b = (v & 0xff) as u8;
            carry = v >> 8;
        }
        if carry != 0 { return None; } // > 256 bits
    }
    Some(bytes)
}

/// Parse a 0x-hex uint256 into a 32-byte big-endian array.
fn hex_to_bytes32(s: &str) -> Option<[u8; 32]> {
    let h = s.strip_prefix("0x").unwrap_or(s);
    let raw = hex::decode(if h.len() % 2 == 1 { format!("0{}", h) } else { h.to_string() }).ok()?;
    if raw.len() > 32 { return None; }
    let mut out = [0u8; 32];
    out[32 - raw.len()..].copy_from_slice(&raw);
    Some(out)
}

/// Render a 32-byte big-endian uint256 as a decimal string.
fn bytes32_to_dec(b: &[u8; 32]) -> String {
    if b.iter().all(|&x| x == 0) { return "0".to_string(); }
    let mut n = *b;
    let mut digits = Vec::new();
    while !n.iter().all(|&x| x == 0) {
        let mut rem = 0u16;
        for byte in n.iter_mut() {
            let cur = (rem << 8) | (*byte as u16);
            *byte = (cur / 10) as u8;
            rem = cur % 10;
        }
        digits.push((b'0' + rem as u8) as char);
    }
    digits.iter().rev().collect()
}

/// Read-only diagnostic: does the on-chain ERC-1155 positionId derived from a
/// `conditionId` + outcome match the CLOB's tradeable `clob_token_id`?
///
/// For each provided leg, computes `ctf_position_id(cid, 1<<i, collateral)` for
/// BOTH pUSD (v2) and USDC.e (v1) collateral and flags which (if either) equals
/// the provided clob_token_id. If neither matches the token the bot trades, the
/// split seed (minted in one collateral's id-space) is the wrong token to sell
/// on the CLOB — the root of the `balance: 0` SELL rejects.
///
/// Usage: `hexbot token_check <conditionId> <up_token_id> [down_token_id]`
pub fn run_token_check() -> Result<()> {
    // RPC: respect an explicit POLYGON_RPC env (operator-set); only fall back to
    // loading the config's [polygon] section when it's unset, so a one-off CLI
    // run can resolve RPC. No wallet creds needed — these are pure view calls.
    if std::env::var("POLYGON_RPC").map(|v| v.is_empty()).unwrap_or(true) {
        if let Some(p) = crate::exchange::polymarket::cli_account::config_path() {
            let _ = crate::config::Config::load(std::path::Path::new(&p));
        }
    }
    // Args AFTER the subcommand, with global flags (--config/--instance) already
    // stripped by cli_args(). Pull out `--wallet <addr>` (and its value), then
    // the remaining positionals are cid / up_token / down_token — robust to
    // `--config=…` appearing BEFORE the subcommand.
    let cargs: Vec<String> = crate::exchange::polymarket::cli_account::cli_args().collect();
    let mut wallet = String::new();
    let mut positional: Vec<String> = Vec::new();
    let mut skip_next = false;
    for a in &cargs {
        if skip_next { skip_next = false; continue; }
        if a == "--wallet" { skip_next = true; continue; }
        if let Some(w) = a.strip_prefix("--wallet=") { wallet = w.to_string(); continue; }
        if a.starts_with('-') { continue; }
        positional.push(a.clone());
    }
    // (re-scan for the `--wallet <value>` form since the loop above skipped it)
    if wallet.is_empty() {
        if let Some(i) = cargs.iter().position(|a| a == "--wallet") {
            wallet = cargs.get(i + 1).cloned().unwrap_or_default();
        }
    }
    let cid = match positional.first() {
        Some(c) if !c.is_empty() => c.clone(),
        _ => return Err(anyhow!(
            "usage: hexbot token_check <conditionId> <up_token_id> [down_token_id] [--wallet <addr>]"
        )),
    };
    let up_tok = positional.get(1).cloned().unwrap_or_default();
    let down_tok = positional.get(2).cloned().unwrap_or_default();

    println!("=== Token-id check ===");
    println!("conditionId : {}", cid);
    println!("pUSD (v2)   : {}", PUSD_ADDRESS);
    println!("USDC.e (v1) : {}", USDCE_ADDRESS);
    if !wallet.is_empty() { println!("wallet      : {}", wallet); }
    println!();

    let legs: [(&str, u64, &str); 2] = [
        ("Up   (indexSet 1)", 1, up_tok.as_str()),
        ("Down (indexSet 2)", 2, down_tok.as_str()),
    ];
    for (label, index_set, provided) in legs {
        if provided.is_empty() { continue; }
        println!("── {} ──", label);
        println!("  CLOB token_id : {}", provided);
        let provided_b = dec_to_bytes32(provided);
        if provided_b.is_none() {
            println!("  (could not parse provided token_id as decimal uint256)");
        }
        for (cname, collateral) in [("pUSD ", PUSD_ADDRESS), ("USDC.e", USDCE_ADDRESS)] {
            match ctf_position_id(&cid, index_set, collateral) {
                Some(hex) => {
                    let derived_b = hex_to_bytes32(&hex);
                    let matched = match (&provided_b, &derived_b) {
                        (Some(a), Some(d)) => a == d,
                        _ => false,
                    };
                    let dec = derived_b.map(|b| bytes32_to_dec(&b)).unwrap_or_default();
                    let bal = if wallet.is_empty() {
                        String::new()
                    } else {
                        format!("  | DW balanceOf={:.4}", ctf_erc1155_balance(&wallet, &hex))
                    };
                    println!("  {} positionId: {}  {}{}",
                        cname, dec, if matched { "← MATCH ✓" } else { "" }, bal);
                }
                None => println!(
                    "  {} positionId: <eth_call failed — check POLYGON_RPC / config>", cname),
            }
        }
        println!();
    }
    println!("If the CLOB token_id matches USDC.e (not pUSD), the v2 split seed");
    println!("(minted in pUSD-space by dw_split) is NOT the token the CLOB trades →");
    println!("SELLs hit `balance: 0`. The seed must be minted in the matching space.");
    Ok(())
}

// ════════════════════════════════════════════════════════════════
// redeem command
// ════════════════════════════════════════════════════════════════

/// Fetch `addr`'s open positions from the data-api, keep the legs the feed
/// flags `redeemable` (size > 0), print a numbered table, and prompt the
/// operator for a selection. Returns the chosen conditionIds (deduped, in
/// display order); an empty Vec means "nothing to do / cancelled" and the
/// caller should just return.
///
/// Data-api `/positions` ONLY — deliberately no on-chain `balanceOf` gate and
/// no `/closed-positions` scan: this surfaces and acts on exactly what the open
/// feed reports as redeemable. (The live-maintenance auto-redeem keeps its
/// on-chain ghost gate + closed scan in `run_redeem_all`; that path is separate
/// and unchanged.)
fn prompt_redeemable_from_dataapi(addr: &str) -> Result<Vec<String>> {
    let url = format!("{}/positions?user={}&sizeThreshold=0&limit=500", DATA_API_BASE, addr);
    let positions: Vec<PositionRecord> = match crate::async_rt::blocking_get_text(&url) {
        Ok(text) => serde_json::from_str(&text).unwrap_or_default(),
        Err(e) => return Err(anyhow!("Failed to fetch positions: {}", e)),
    };

    // Filter redeemable positions, group by conditionId (preserve insertion order).
    let mut condition_ids: Vec<String> = Vec::new();
    let mut grouped: std::collections::HashMap<String, Vec<&PositionRecord>> =
        std::collections::HashMap::new();
    for p in &positions {
        if p.redeemable && p.size > 0.0 {
            if !grouped.contains_key(&p.condition_id) {
                condition_ids.push(p.condition_id.clone());
            }
            grouped.entry(p.condition_id.clone()).or_default().push(p);
        }
    }

    if condition_ids.is_empty() {
        println!("No redeemable positions found.");
        return Ok(Vec::new());
    }

    // Numbered table.
    println!("Redeemable positions:");
    println!("{:>3}  {:<48} {:>8} {:>8} {:>10}", "#", "Market", "Outcome", "Size", "Value");
    println!("{}", "-".repeat(83));
    let mut total_value = 0.0;
    for (idx, cid) in condition_ids.iter().enumerate() {
        for (j, p) in grouped[cid].iter().enumerate() {
            let title = if p.title.len() > 46 { format!("{}...", &p.title[..43]) } else { p.title.clone() };
            let num = if j == 0 { format!("{}", idx + 1) } else { String::new() };
            println!("{:>3}  {:<48} {:>8} {:>8.2} {:>10.4}", num, title, p.outcome, p.size, p.current_value);
            total_value += p.current_value;
        }
    }
    println!("{}", "-".repeat(83));
    println!("     Total: {:.4} USDC ({} conditions)", total_value, condition_ids.len());
    println!();

    // Prompt for selection.
    println!("Enter number(s) to redeem (e.g. 1, 1,3), 'a' for all, or 'n' to cancel:");
    print!("> ");
    std::io::stdout().flush()?;
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    let input = input.trim();
    if input.eq_ignore_ascii_case("n") || input.is_empty() {
        println!("Cancelled.");
        return Ok(Vec::new());
    }

    let selected_indices: Vec<usize> = if input.eq_ignore_ascii_case("a") {
        (0..condition_ids.len()).collect()
    } else {
        input.split(',')
            .filter_map(|s| s.trim().parse::<usize>().ok())
            .filter(|&n| n >= 1 && n <= condition_ids.len())
            .map(|n| n - 1)
            .collect()
    };
    if selected_indices.is_empty() {
        println!("No valid selection. Cancelled.");
        return Ok(Vec::new());
    }

    Ok(selected_indices.iter().map(|&i| condition_ids[i].clone()).collect())
}

pub fn run_redeem() -> Result<()> {
    let wallet = load_wallet()?;

    // ── POLY_1271: redeem matured positions FROM the deposit wallet via the
    //    WALLET-batch `dw_redeem` path. Data-api `/positions` only — the legs
    //    its feed flags redeemable, interactively selected. No on-chain
    //    balanceOf, no `/closed-positions` scan. ──
    if let Some(dw) = wallet.deposit_wallet_active().map(str::to_string) {
        println!("=== Polymarket Redeem (deposit wallet) ===");
        println!("Wallet:   {}", dw);
        println!();

        let selected_cids = prompt_redeemable_from_dataapi(&dw)?;
        if selected_cids.is_empty() {
            return Ok(());
        }
        println!("Redeeming {} condition(s)...", selected_cids.len());
        for (i, cid) in selected_cids.iter().enumerate() {
            let cid_short: String = cid.chars().take(16).collect();
            print!("  [{}/{}] Redeeming {}... ", i + 1, selected_cids.len(), cid_short);
            std::io::stdout().flush()?;
            match super::deposit_wallet::dw_redeem(
                &wallet.signing_key, &wallet.signer_address, &dw, &wallet.builder_auth, cid,
            ) {
                Ok(tx) => println!("done. tx: https://polygonscan.com/tx/{}", tx.trim_start_matches("0x")),
                Err(e) => println!("FAILED: {}", e),
            }
        }
        return Ok(());
    }

    // Read config-driven flags so CLI and live-mode background
    // maintenance share the same policy:
    //   - gas_via_signer_wallet → on-chain vs relayer submission
    //   - clob_version          → v1 CTF vs v2 adapter target
    let gas_via_signer = read_gas_via_signer_wallet_flag();
    let is_v2 = read_clob_v2_flag();
    let (target_contract, collateral_token) = ctf_target(is_v2, /*neg_risk=*/ false);

    println!("=== Polymarket Redeem ===");
    println!("Wallet:   {}", wallet.safe_address);
    println!("CLOB:     {} ({})", if is_v2 { "v2" } else { "v1" },
        if is_v2 { "pUSD via CtfCollateralAdapter" } else { "USDC.e via CTF" });
    println!("Target:   {}", target_contract);
    println!(
        "Gas payer: {}",
        if gas_via_signer {
            "signer EOA (direct on-chain, paid in MATIC)"
        } else {
            "Polymarket relayer (gasless)"
        }
    );
    println!();

    // Data-api only: fetch the Safe's open positions, keep the legs the feed
    // flags redeemable, and prompt for selection. No on-chain balanceOf, no
    // `/closed-positions` scan.
    let selected_cids = prompt_redeemable_from_dataapi(&wallet.safe_address)?;
    if selected_cids.is_empty() {
        return Ok(());
    }
    println!("Redeeming {} condition(s)...", selected_cids.len());

    // Execute redeem for selected conditionIds
    for (i, cid) in selected_cids.iter().enumerate() {
        let cid_short = if cid.len() > 16 { &cid[..16] } else { cid };
        print!("  [{}/{}] Redeeming {}... ", i + 1, selected_cids.len(), cid_short);
        std::io::stdout().flush()?;

        // Build redeemPositions calldata
        // redeemPositions(address collateralToken, bytes32 parentCollectionId, bytes32 conditionId, uint256[] indexSets)
        let condition_bytes = hex::decode(cid.strip_prefix("0x").unwrap_or(cid)).unwrap_or_default();
        let mut cid_padded = [0u8; 32];
        let start = 32 - condition_bytes.len().min(32);
        cid_padded[start..].copy_from_slice(&condition_bytes[..condition_bytes.len().min(32)]);

        // ABI encode with dynamic array offset
        let mut calldata = Vec::with_capacity(4 + 32 * 7);
        calldata.extend_from_slice(&REDEEM_SELECTOR);
        calldata.extend_from_slice(&address_to_bytes32(collateral_token)); // collateralToken (pUSD in v2)
        calldata.extend_from_slice(&[0u8; 32]);                             // parentCollectionId = 0
        calldata.extend_from_slice(&cid_padded);                            // conditionId
        calldata.extend_from_slice(&u256_bytes(128));                        // offset to indexSets array (4 * 32 = 128)
        calldata.extend_from_slice(&u256_bytes(2));                          // array length = 2
        calldata.extend_from_slice(&u256_bytes(1));                          // indexSets[0] = 1
        calldata.extend_from_slice(&u256_bytes(2));                          // indexSets[1] = 2
        let data_hex = format!("0x{}", hex::encode(&calldata));

        match submit_safe_tx_with_id(
            &wallet.builder_auth, &wallet.signing_key,
            &wallet.signer_address, &wallet.safe_address,
            target_contract, &data_hex,
            gas_via_signer,
        ) {
            Ok((tx_id, _state)) => {
                // Poll for confirmation
                let mut final_state = String::new();
                let mut tx_hash = String::new();
                for _ in 0..30 {
                    std::thread::sleep(std::time::Duration::from_secs(2));
                    match poll_transaction(&wallet.builder_auth, &tx_id) {
                        Ok((s, h)) => {
                            final_state = s.clone();
                            tx_hash = h;
                            if s.contains("CONFIRMED") || s.contains("MINED") {
                                break;
                            }
                            if s.contains("FAILED") {
                                break;
                            }
                        }
                        Err(e) => {
                            println!("poll error: {}", e);
                            break;
                        }
                    }
                }
                // For on-chain path, tx_id itself IS the tx hash; fall back
                // to it when the relayer path didn't populate tx_hash yet.
                let link_hash = if tx_hash.is_empty() { tx_id.clone() } else { tx_hash.clone() };
                if final_state.contains("CONFIRMED") || final_state.contains("MINED") {
                    println!("done. tx: https://polygonscan.com/tx/{}", link_hash.trim_start_matches("0x"));
                } else if final_state.contains("FAILED") {
                    println!("FAILED.");
                } else {
                    println!("pending ({})", final_state);
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    // Show updated balance
    println!();
    let new_balance = fetch_usdce_balance(&wallet.safe_address);
    println!("Updated USDC.e balance: {:.6}", new_balance);

    Ok(())
}

// ════════════════════════════════════════════════════════════════
// orders command
// ════════════════════════════════════════════════════════════════

fn load_user_auth() -> Result<(PolyAuth, String)> {
    let private_key = std::env::var("POLY_PRIVATE_KEY").unwrap_or_default();
    let api_key = std::env::var("POLY_API_KEY").unwrap_or_default();
    let api_secret = std::env::var("POLY_API_SECRET").unwrap_or_default();
    let passphrase = std::env::var("POLY_PASSPHRASE").unwrap_or_default();

    if api_key.is_empty() || api_secret.is_empty() {
        return Err(no_wallet_creds_err());
    }

    let signing_key = parse_private_key(&private_key)?;
    let signer_address = to_checksum_address(&derive_eth_address_from_key(&signing_key));
    let auth = PolyAuth::new(&api_key, &api_secret, &passphrase, &signer_address)?;
    Ok((auth, signer_address))
}

/// Map the configured `signature_type` string to the CLOB numeric code
/// (0=EOA, 1=PolyProxy, 2=GnosisSafe, 3=Poly1271/deposit-wallet).
fn poly_signature_type_num(s: &str) -> u8 {
    match s.trim().to_ascii_lowercase().as_str() {
        "poly_1271" | "deposit_wallet" | "3" => 3,
        "poly_gnosis_safe" | "gnosis_safe" | "2" => 2,
        "poly_proxy" | "1" => 1,
        _ => 0,
    }
}

/// One `GET /balance-allowance/update` poke. Non-fatal — logs and returns.
fn balance_allowance_update_one(
    auth: &PolyAuth,
    base: &str,
    asset_type: &str,
    token_id: Option<&str>,
    sig_type: u8,
) {
    // Polymarket L2 auth signs the request PATH ONLY — the query string is
    // NOT part of the HMAC message (matches py-clob-client: `request_path =
    // "/balance-allowance/update"`, params live in the URL only). Signing the
    // query too yields `401 Unauthorized/Invalid api key`.
    const SIGN_PATH: &str = "/balance-allowance/update";
    let query = match token_id {
        Some(t) => format!(
            "?asset_type={}&token_id={}&signature_type={}",
            asset_type, t, sig_type,
        ),
        None => format!("?asset_type={}&signature_type={}", asset_type, sig_type),
    };
    let headers = auth.sign_request("GET", SIGN_PATH, "");
    let url = format!("{}{}{}", base.trim_end_matches('/'), SIGN_PATH, query);
    let label = token_id.map(|t| &t[..t.len().min(12)]).unwrap_or("pUSD");
    // `/balance-allowance/update` 200s with an empty body — check status only,
    // don't require JSON (a strict parse here logged a spurious EOF "failure"
    // even though the cache refresh succeeded).
    match user_clob_get_ok(url, headers) {
        Ok(()) => log::info!("[BalanceSync] {} {} synced (sig_type={})", asset_type, label, sig_type),
        Err(e) => log::warn!("[BalanceSync] {} {} update failed: {}", asset_type, label, e),
    }
}

/// Proactively refresh the CLOB's cached balance/allowance for `instance_id`'s
/// pUSD collateral plus this event's Up/Down conditional tokens, called at
/// event start.
///
/// After the maintenance split mints fresh Up/Down tokens (and spends pUSD)
/// on-chain, the CLOB's cached balance/allowance view lags — so the first SELL
/// (against the seed) or BUY (against pUSD) can be wrongly rejected with
/// "not enough balance / allowance". Polymarket's fix is to poke
/// `GET /balance-allowance/update` per asset (signature_type=3 for deposit
/// wallets) so the CLOB re-reads on-chain via the ERC-1271 layer.
/// <https://docs.polymarket.com/trading/deposit-wallets#order-says-not-enough-balance>
///
/// Credentials are CONFIG-sourced (NOT env): loads the live config + secrets
/// file and resolves `instance_id` → its strategy's `account_id` (the wallet
/// key in `[poly.<account>]`), then `poly_for(account_id)`. Balance/allowance
/// is an ACCOUNT-level resource (one wallet, possibly shared by several
/// instances), so it must be keyed by account_id — not instance_id — mirroring
/// the engine's `sc.account_id()` creds path. Non-fatal end to end — every
/// failure path logs and returns, so a sync miss just falls back to the
/// existing reactive "not enough balance" retry loop. Call off the strategy
/// thread (3 blocking HTTP GETs).
pub fn sync_balance_allowance_for_event(instance_id: &str, up_token_id: &str, down_token_id: &str) {
    let cfg_path = crate::exchange::polymarket::cli_account::config_path()
        .unwrap_or_else(|| "config/live_polymaker.toml".to_string());
    let cfg = match crate::config::Config::load(std::path::Path::new(&cfg_path)) {
        Ok(c) => c,
        Err(e) => { log::warn!("[BalanceSync] skipped (config load: {})", e); return; }
    };
    let secrets_path = crate::config::SecretsFile::resolve_path_with_override(
        std::path::Path::new(&cfg_path), &cfg.general.secrets_file,
    );
    let secrets = match crate::config::SecretsFile::load(&secrets_path) {
        Ok(s) => s,
        Err(e) => { log::warn!("[BalanceSync] skipped (secrets load: {})", e); return; }
    };
    // Balance/allowance is an account-level resource: resolve this instance's
    // `account_id` (falls back to instance_id when unset, i.e. the legacy
    // one-wallet-per-strategy path) and key creds by it. Under multi-account
    // live the secrets blocks are `[poly.<account_id>]`, so looking up by
    // instance_id (e.g. `btc01`) misses and silently skips the sync.
    let account_id = cfg.strategies.iter()
        .find(|sc| sc.instance_id == instance_id)
        .map(|sc| sc.account_id().to_string())
        .unwrap_or_else(|| instance_id.to_string());
    let creds = match secrets.poly_for(&account_id) {
        Ok(c) => c,
        Err(e) => { log::warn!("[BalanceSync] skipped (no creds for account {} (instance {}): {})", account_id, instance_id, e); return; }
    };
    if creds.api_key.is_empty() || creds.api_secret.is_empty() {
        log::warn!("[BalanceSync] skipped — account {} (instance {}) has no CLOB L2 creds", account_id, instance_id);
        return;
    }
    let signer = match parse_private_key(&creds.private_key) {
        Ok(k) => to_checksum_address(&derive_eth_address_from_key(&k)),
        Err(e) => { log::warn!("[BalanceSync] skipped (key parse: {})", e); return; }
    };
    let auth = match PolyAuth::new(&creds.api_key, &creds.api_secret, &creds.api_passphrase, &signer) {
        Ok(a) => a,
        Err(e) => { log::warn!("[BalanceSync] skipped (auth: {})", e); return; }
    };
    let sig_num = poly_signature_type_num(&creds.signature_type);
    let base = cfg.exchanges.iter()
        .find(|e| e.name == "polymarket")
        .map(|p| p.api_url_prefix.clone())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| CLOB_URL.to_string());

    balance_allowance_update_one(&auth, &base, "COLLATERAL", None, sig_num);
    for tok in [up_token_id, down_token_id] {
        if !tok.is_empty() {
            balance_allowance_update_one(&auth, &base, "CONDITIONAL", Some(tok), sig_num);
        }
    }
}

#[derive(serde::Deserialize)]
#[allow(dead_code)]
struct OpenOrder {
    #[serde(default)]
    id: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    market: String,
    #[serde(default)]
    asset_id: String,
    #[serde(default)]
    side: String,
    #[serde(default, deserialize_with = "de_string_or_number")]
    original_size: String,
    #[serde(default, deserialize_with = "de_string_or_number")]
    size_matched: String,
    #[serde(default, deserialize_with = "de_string_or_number")]
    price: String,
    #[serde(default)]
    outcome: String,
    #[serde(default)]
    order_type: String,
    /// Server returns this as a string ISO timestamp (v1) OR as a
    /// Unix-seconds JSON Number (v2). Deserialize both into a
    /// raw-string form; format on display via `format_created_at`.
    #[serde(default, deserialize_with = "de_string_or_number")]
    created_at: String,
}

/// Accept either JSON string OR numeric field as a `String`. Guards
/// against v1-vs-v2 schema drift on timestamps / decimals that are
/// sometimes stringified and sometimes raw.
fn de_string_or_number<'de, D>(de: D) -> Result<String, D::Error>
where D: serde::Deserializer<'de>
{
    use serde::Deserialize;
    let v = serde_json::Value::deserialize(de)?;
    Ok(match v {
        serde_json::Value::String(s) => s,
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    })
}

/// Turn a `created_at` value (ISO string OR Unix-seconds integer) into
/// a 16-char ISO display: `2026-04-24T10:17`.
fn format_created_at(s: &str) -> String {
    // Numeric → parse as Unix seconds, format via chrono.
    if let Ok(secs) = s.parse::<i64>() {
        if let Some(dt) = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0) {
            return dt.format("%Y-%m-%dT%H:%M").to_string();
        }
    }
    // String ISO → trim to 16 chars.
    if s.len() >= 16 { s[..16].to_string() } else { s.to_string() }
}

/// `hexbot active_orders` — live-order listing, config-aware:
/// reads `config/live_polymaker.toml` (override via positional arg)
/// to determine which CLOB host (v1 vs v2) to query
/// and surfaces that version in the header. Matches `hexbot positions`
/// ergonomics.
///
/// Usage:
///   hexbot active_orders                  # default config/live_polymaker.toml
///   hexbot active_orders <config-path>
pub fn run_active_orders() -> Result<()> {
    let (auth, _signer) = load_user_auth()?;

    // Parse args: `--host <url>`, `--debug`/`-v`, and optional
    // positional <config-path>. Flag values (URL after --host) must
    // NOT leak into the positional list.
    let mut host_override: Option<String> = None;
    let mut debug_flag = false;
    let mut positional: Vec<String> = Vec::new();
    {
        let mut iter = crate::exchange::polymarket::cli_account::cli_args();
        while let Some(a) = iter.next() {
            match a.as_str() {
                "--host" => { host_override = iter.next(); }
                "--debug" | "-v" => { debug_flag = true; }
                _ => positional.push(a),
            }
        }
    }
    let config_path = positional.into_iter().next()
        .unwrap_or_else(|| "config/live_polymaker.toml".to_string());
    let poly_cfg = crate::config::Config::load(std::path::Path::new(&config_path))
        .ok()
        .and_then(|c| c.exchanges.into_iter().find(|e| e.name == "polymarket"));

    let clob_version = poly_cfg.as_ref()
        .map(|p| p.clob_version.clone()).unwrap_or_default();
    let configured_url = poly_cfg.as_ref()
        .map(|p| p.api_url_prefix.clone()).unwrap_or_default();

    // v2 default: only explicit "v1"/"1" → v1; empty/missing → v2.
    let clob_display: &str = match clob_version.as_str() {
        s if s.eq_ignore_ascii_case("v1") || s == "1" => "v1",
        "v2" | "V2" | "2" | "" => "v2",
        s => s,
    };
    // URL resolution: `api_url_prefix` in config wins; otherwise use
    // the main `clob.polymarket.com` host. Polymarket serves both v1
    // and v2 on the same host during the migration window and auto-
    // flips to v2 at cutover, so we can keep a single default for
    // both `clob_version` settings.
    // `--host <url>` lets operators override the resolved host
    // without touching config / env. Useful when config points to
    // the v2 testing host but the real open orders live on main.
    let base_url = host_override
        .as_ref()
        .filter(|s| !s.is_empty())
        .cloned()
        .unwrap_or_else(|| {
            if !configured_url.is_empty() { configured_url } else { CLOB_URL.to_string() }
        });

    println!("=== Polymarket Active Orders ===");
    println!("Config:  {}", config_path);
    println!("CLOB:    {}", clob_display);
    print!  ("API URL: {}", base_url);
    if host_override.is_some() { println!("  (via --host override)"); } else { println!(); }
    println!();

    // `--debug` flag dumps the raw JSON response — useful when our
    // filter / parser drops rows silently (which is how a v1 "LIVE"
    // hard-coded filter would swallow v2 "OPEN" or similar).
    let debug = debug_flag;

    // Paginate /data/orders from the resolved host.
    let mut all_orders: Vec<OpenOrder> = Vec::new();
    let mut total_raw = 0usize;       // everything the server sent us
    let mut status_counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut cursor = String::new();
    loop {
        let path = if cursor.is_empty() {
            "/data/orders".to_string()
        } else {
            format!("/data/orders?next_cursor={}", cursor)
        };
        let headers = auth.sign_request("GET", &path, "");
        let url = format!("{}{}", base_url.trim_end_matches('/'), path);
        let json = user_clob_get(url, headers)?;

        if debug {
            println!("── Raw response (debug) ───────────────────────");
            println!("{}", serde_json::to_string_pretty(&json).unwrap_or_else(|_| format!("{:?}", json)));
            println!();
        }

        if let Some(data) = json.get("data").and_then(|v| v.as_array()) {
            total_raw += data.len();
            for item in data {
                let status_raw = item.get("status").and_then(|v| v.as_str()).unwrap_or("<none>").to_string();
                *status_counts.entry(status_raw.clone()).or_insert(0) += 1;
                if let Ok(order) = serde_json::from_value::<OpenOrder>(item.clone()) {
                    // Accept any non-terminal status. Empirically v1 uses
                    // "LIVE"; v2 might use "OPEN"/"ACTIVE". Filter by
                    // exclusion so schema drift doesn't silently hide
                    // real open orders.
                    let terminal = matches!(
                        order.status.to_ascii_uppercase().as_str(),
                        "MATCHED" | "CANCELED" | "CANCELLED" | "FILLED" | "REJECTED"
                    );
                    if !terminal {
                        all_orders.push(order);
                    }
                } else if debug {
                    println!("⚠  failed to parse into OpenOrder: {:?}", item);
                }
            }
        } else if debug {
            println!("⚠  response has no `data` array — top-level keys: {:?}",
                json.as_object().map(|o| o.keys().cloned().collect::<Vec<_>>()));
        }
        let next = json.get("next_cursor").and_then(|v| v.as_str()).unwrap_or("");
        if next.is_empty() || next == "LTE=" { break; }
        cursor = next.to_string();
    }

    if all_orders.is_empty() {
        if total_raw == 0 {
            println!("No active orders. (server returned 0 rows on {})", base_url);
            // If we're on the v2 testing host, remind operator that
            // production orders live on the main host pre-cutover.
            if base_url.contains("clob-v2.polymarket.com") {
                println!();
                println!("⚠  You're querying the v2 testing host. Pre-cutover, live orders");
                println!("    live on `clob.polymarket.com`. Try:");
                println!("      hexbot active_orders --host https://clob.polymarket.com");
            }
            println!("Re-run with `--debug` to dump the raw JSON response.");
        } else {
            println!(
                "No active orders after filtering — server returned {} row{} but all were terminal.",
                total_raw, if total_raw == 1 { "" } else { "s" },
            );
            println!("Status breakdown: {:?}", status_counts);
            println!("Re-run with `--debug` to inspect the raw rows.");
        }
        return Ok(());
    }

    // Group by market (condition_id) and resolve titles.
    let mut market_titles: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let mut grouped: Vec<String> = Vec::new();
    for o in &all_orders {
        if !market_titles.contains_key(&o.market) {
            grouped.push(o.market.clone());
            market_titles.insert(o.market.clone(), fetch_market_title(&o.market));
        }
    }

    let mut total_orders = 0usize;
    let mut total_notional = 0.0_f64;
    for (gi, market_id) in grouped.iter().enumerate() {
        if gi > 0 { println!(); }
        let title = market_titles.get(market_id).map(|s| s.as_str()).unwrap_or("Unknown");
        let orders: Vec<&OpenOrder> = all_orders.iter().filter(|o| o.market == *market_id).collect();

        println!("  {} ({})", title, &market_id[..16.min(market_id.len())]);
        println!("  {:>4}  {:>8}  {:>8}  {:>10}  {:>10}  {:>10}  {:>8}  {:<16}",
            "Side", "Outcome", "Price", "Size", "Matched", "Rem.Not.", "Type", "Created");
        println!("  {}", "-".repeat(94));

        // One order = one row for the numeric fields + one line above
        // with the full 66-char OrderID so `hexbot cancel_order` can
        // copy-paste it directly without reassembling from a truncated
        // prefix. Keeps the numeric table narrow/readable.
        for (ix, o) in orders.iter().enumerate() {
            let size:    f64 = o.original_size.parse().unwrap_or(0.0);
            let matched: f64 = o.size_matched.parse().unwrap_or(0.0);
            let price:   f64 = o.price.parse().unwrap_or(0.0);
            let remaining_notional = (size - matched).max(0.0) * price;
            let created = format_created_at(&o.created_at);

            if ix > 0 { println!(); }
            println!("  OrderID: {}", o.id);
            println!("  {:>4}  {:>8}  {:>8.4}  {:>10.2}  {:>10.2}  {:>10.4}  {:>8}  {:<16}",
                o.side, o.outcome, price, size, matched, remaining_notional, o.order_type, created);
            total_notional += remaining_notional;
        }
        total_orders += orders.len();
    }

    println!();
    println!(
        "Total: {} active orders across {} markets — notional (remaining, bids) ≈ {:.4}",
        total_orders, grouped.len(), total_notional,
    );
    println!();
    println!("Cancel: `hexbot cancel_order <OrderID>`   or   `hexbot cancel_order --all`");
    Ok(())
}

/// `hexbot cancel_order` — cancel one order by ID, or all open orders.
/// Config-aware in the same way as `hexbot active_orders` /
/// `hexbot positions` (reads `config/live_polymaker.toml` unless a
/// positional config path is given).
///
/// Usage:
///   hexbot cancel_order <orderID>             # cancel one
///   hexbot cancel_order <orderID> <cfg-path>  # + override config
///   hexbot cancel_order --all                 # cancel ALL open orders
///   hexbot cancel_order --all <cfg-path>
///
/// `<orderID>` is the `0x` + 64-hex Polymarket server order hash
/// (visible in the `OrderID` column of `hexbot active_orders`). The
/// 16-char prefix shown there is NOT enough — the full value must
/// be supplied. The CLOB API accepts it unchanged (case insensitive).
///
/// Shared CLOB host / version semantics with active_orders: both v1
/// and v2 use the same `DELETE /order` and `DELETE /cancel-all`
/// endpoints on `clob.polymarket.com`; the `clob_version` setting
/// only affects the header of the output, not the request shape.
pub fn run_cancel_order() -> Result<()> {
    let (auth, _signer) = load_user_auth()?;

    // Parse args — first positional is either the orderID or `--all`.
    let positional: Vec<String> = crate::exchange::polymarket::cli_account::cli_args()
        .filter(|a| a != "--all")
        .collect();
    let cancel_all = crate::exchange::polymarket::cli_account::cli_args().any(|a| a == "--all");

    let order_id = if cancel_all { None } else {
        Some(positional.first().cloned().ok_or_else(|| anyhow!(
            "missing orderID. Usage:\n\
             \thexbot cancel_order <orderID>\n\
             \thexbot cancel_order --all\n\
             \n\
             Get the orderID column from `hexbot active_orders`."
        ))?)
    };
    // Config path: next positional after the orderID (or first
    // positional if --all). Default live_polymaker.toml.
    let cfg_skip = if cancel_all { 0 } else { 1 };
    let config_path = positional.into_iter().nth(cfg_skip)
        .unwrap_or_else(|| "config/live_polymaker.toml".to_string());

    let poly_cfg = crate::config::Config::load(std::path::Path::new(&config_path))
        .ok()
        .and_then(|c| c.exchanges.into_iter().find(|e| e.name == "polymarket"));
    let clob_version = poly_cfg.as_ref()
        .map(|p| p.clob_version.clone()).unwrap_or_default();
    let configured_url = poly_cfg.as_ref()
        .map(|p| p.api_url_prefix.clone()).unwrap_or_default();
    // v2 default: only explicit "v1"/"1" → v1; empty/missing → v2.
    let clob_display: &str = match clob_version.as_str() {
        s if s.eq_ignore_ascii_case("v1") || s == "1" => "v1",
        "v2" | "V2" | "2" | "" => "v2",
        s => s,
    };
    let base_url = if !configured_url.is_empty() {
        configured_url
    } else {
        CLOB_URL.to_string()
    };

    println!("=== Polymarket Cancel Order ===");
    println!("Config:  {}", config_path);
    println!("CLOB:    {}", clob_display);
    println!("API URL: {}", base_url);
    if let Some(ref oid) = order_id {
        println!("Target:  {} (single)", oid);
        if !oid.starts_with("0x") || oid.len() != 66 {
            println!(
                "⚠  OrderID format looks off: expected `0x` + 64 hex chars ({} given). \
                 Polymarket may still accept it — proceeding.",
                oid.len(),
            );
        }
    } else {
        println!("Target:  ALL open orders (--all)");
    }
    println!();

    // Build + send DELETE request.
    let (path, body_str) = match order_id.as_ref() {
        Some(oid) => {
            let body = serde_json::json!({ "orderID": oid });
            ("/order", serde_json::to_string(&body)?)
        }
        None => ("/cancel-all", String::new()),
    };
    let headers = auth.sign_request("DELETE", path, &body_str);
    let url = format!("{}{}", base_url.trim_end_matches('/'), path);

    let resp = user_clob_delete(
        url,
        headers,
        if body_str.is_empty() { None } else { Some(body_str) },
    )?;

    // Render response — common shape is {canceled: [...], not_canceled: {...}}
    let canceled = resp.get("canceled").and_then(|v| v.as_array());
    let not_canceled = resp.get("not_canceled").and_then(|v| v.as_object());

    let canceled_n = canceled.map(|a| a.len()).unwrap_or(0);
    let nc_n       = not_canceled.map(|o| o.len()).unwrap_or(0);

    println!("── Response ──────────────────────────────────────");
    println!("Canceled:     {}", canceled_n);
    if let Some(arr) = canceled {
        for v in arr {
            if let Some(s) = v.as_str() {
                println!("  ✅ {}", s);
            }
        }
    }
    if nc_n > 0 {
        println!("Not canceled: {}", nc_n);
        if let Some(obj) = not_canceled {
            for (id, reason) in obj {
                let r = reason.as_str().unwrap_or("");
                println!("  ❌ {}  (reason: {})", id, r);
            }
        }
    }
    if canceled_n == 0 && nc_n == 0 {
        // Schema mismatch — dump raw JSON so operator can inspect
        println!("(Unrecognised response shape — raw JSON:)");
        println!("{}", serde_json::to_string_pretty(&resp).unwrap_or_default());
    }
    Ok(())
}

/// `hexbot cancel_orders` — batch-cancel multiple orders in a single
/// `DELETE /orders` call. Complements the single `cancel_order` and
/// the nuclear `cancel_order --all`.
///
/// Usage:
///   hexbot cancel_orders <id1> <id2> <id3> ...
///   hexbot cancel_orders --file ids.txt
///   hexbot cancel_orders <id1> --config <cfg> --host <url>
///
/// IDs file: one `0x...` OrderID per line; `#`-comments and blank
/// lines ignored. Combines naturally with `hexbot active_orders >
/// ids.txt` style grep-pipelines (operator filters the OrderID lines
/// themselves and strips the `OrderID: ` prefix).
pub fn run_cancel_orders() -> Result<()> {
    let (auth, _signer) = load_user_auth()?;

    // Parse flags + positionals (OrderIDs).
    // `--config` is a top-level flag resolved centrally by `cli_account`
    // (stripped before this loop sees argv); read via the getter below.
    let mut file_path: Option<String> = None;
    let mut host_override: Option<String> = None;
    let mut positional_ids: Vec<String> = Vec::new();
    {
        let mut iter = crate::exchange::polymarket::cli_account::cli_args();
        while let Some(a) = iter.next() {
            match a.as_str() {
                "--file"   => file_path     = iter.next(),
                "--host"   => host_override = iter.next(),
                "-h" | "--help" => {
                    eprintln!(
                        "Usage: hexbot cancel_orders <id1> <id2> ...\n\
                         \thexbot cancel_orders --file ids.txt\n\
                         \thexbot cancel_orders <id1> --config <cfg> --host <url>\n\n\
                         IDs file: one `0x...` OrderID per line; `#` lines ignored."
                    );
                    return Ok(());
                }
                other if other.starts_with('-') => return Err(anyhow!("unknown flag `{}`", other)),
                other => positional_ids.push(other.to_string()),
            }
        }
    }

    // Load IDs from file if given; merge with positionals.
    let mut ids: Vec<String> = positional_ids;
    if let Some(p) = file_path.as_ref() {
        let content = std::fs::read_to_string(p).map_err(|e| anyhow!("read {}: {}", p, e))?;
        for line in content.lines() {
            let l = line.trim();
            if l.is_empty() || l.starts_with('#') { continue; }
            // Accept `OrderID: 0x...` prefix so output of `active_orders`
            // can be piped in directly after a `grep OrderID: | awk '{print $2}'`.
            let id = l.strip_prefix("OrderID:").unwrap_or(l).trim().to_string();
            ids.push(id);
        }
    }
    if ids.is_empty() {
        return Err(anyhow!(
            "no OrderIDs given. Pass on CLI or via `--file <path>`.\n\
             Run `hexbot cancel_orders --help` for usage."
        ));
    }
    // Dedup + basic sanity.
    let initial_len = ids.len();
    ids.sort();
    ids.dedup();
    if ids.len() < initial_len {
        println!("(deduplicated {} → {} unique IDs)", initial_len, ids.len());
    }
    for id in &ids {
        if !id.starts_with("0x") || id.len() != 66 {
            println!("⚠  OrderID `{}` looks malformed (expected 0x + 64 hex); sending anyway", id);
        }
    }

    // Resolve config → CLOB version + host.
    let config_path = crate::exchange::polymarket::cli_account::config_path()
        .unwrap_or_else(|| "config/live_polymaker.toml".to_string());
    let poly_cfg = crate::config::Config::load(std::path::Path::new(&config_path))
        .ok()
        .and_then(|c| c.exchanges.into_iter().find(|e| e.name == "polymarket"));
    let clob_version = poly_cfg.as_ref()
        .map(|p| p.clob_version.clone()).unwrap_or_default();
    let configured_url = poly_cfg.as_ref()
        .map(|p| p.api_url_prefix.clone()).unwrap_or_default();
    // v2 default: only explicit "v1"/"1" → v1; empty/missing → v2.
    let clob_display: &str = match clob_version.as_str() {
        s if s.eq_ignore_ascii_case("v1") || s == "1" => "v1",
        "v2" | "V2" | "2" | "" => "v2",
        s => s,
    };
    let base_url = host_override.clone()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            if !configured_url.is_empty() { configured_url } else { CLOB_URL.to_string() }
        });

    println!("=== Polymarket Cancel Orders (batch) ===");
    println!("Config:  {}", config_path);
    println!("CLOB:    {}", clob_display);
    print!("API URL: {}", base_url);
    if host_override.is_some() { println!("  (via --host override)"); } else { println!(); }
    println!("Orders:  {}", ids.len());
    println!();

    // DELETE /orders with body = JSON array of ID strings.
    let body = serde_json::Value::Array(
        ids.iter().map(|s| serde_json::Value::String(s.clone())).collect()
    );
    let body_str = serde_json::to_string(&body)?;
    let headers = auth.sign_request("DELETE", "/orders", &body_str);
    let url = format!("{}/orders", base_url.trim_end_matches('/'));
    let resp = user_clob_delete(url, headers, Some(body_str))?;

    // Standard response shape: { canceled: [..], not_canceled: {..} }
    let canceled    = resp.get("canceled").and_then(|v| v.as_array());
    let not_canceled= resp.get("not_canceled").and_then(|v| v.as_object());
    let canceled_n  = canceled.map(|a| a.len()).unwrap_or(0);
    let nc_n        = not_canceled.map(|o| o.len()).unwrap_or(0);

    println!("── Response ──────────────────────────────────────");
    println!("Canceled:     {}", canceled_n);
    if let Some(arr) = canceled {
        for v in arr {
            if let Some(s) = v.as_str() {
                println!("  ✅ {}", s);
            }
        }
    }
    if nc_n > 0 {
        println!("Not canceled: {}", nc_n);
        if let Some(obj) = not_canceled {
            for (id, reason) in obj {
                let r = reason.as_str().unwrap_or("");
                println!("  ❌ {}  (reason: {})", id, r);
            }
        }
    }
    if canceled_n == 0 && nc_n == 0 {
        println!("(Unrecognised response shape — raw JSON:)");
        println!("{}", serde_json::to_string_pretty(&resp).unwrap_or_default());
    }
    Ok(())
}

// ════════════════════════════════════════════════════════════════
// raw_trades / trades commands
// ════════════════════════════════════════════════════════════════

/// Prompt for an event slug, fetch the event via the Polymarket Gamma API,
/// initialize a `BinaryOption` from the event's first active market, and then
/// fetch the authenticated user's trades on that market sorted by
/// `match_time` ascending.
///
/// Returns `(funder_address, instrument, trades)`.
fn prompt_and_fetch_trades() -> Result<(String, crate::types::BinaryOption, Vec<serde_json::Value>)> {
    println!("Enter event slug:");
    print!("> ");
    std::io::stdout().flush()?;
    let mut slug = String::new();
    std::io::stdin().read_line(&mut slug)?;
    let slug = slug.trim().to_string();

    if slug.is_empty() {
        return Err(anyhow!("Event slug is required."));
    }

    // Fetch event and build BinaryOption from the first active market
    // (fall back to the first market if none are marked active).
    let event = super::market::fetch_event_by_slug_with_log(&slug, false)?;
    let chosen = event.markets.iter()
        .find(|m| m.active && !m.closed)
        .cloned()
        .or_else(|| event.markets.first().cloned())
        .ok_or_else(|| anyhow!("Event '{}' has no markets", slug))?;
    let mut bo: crate::types::BinaryOption = chosen.into();
    bo.slug = event.slug.clone();

    if bo.condition_id.is_empty() {
        return Err(anyhow!("Event '{}' market has empty condition_id", slug));
    }

    let (auth, signer) = load_user_auth()?;
    // The user's funder (Safe/proxy wallet) — address that owns orders on Polymarket.
    let funder = to_checksum_address(&derive_safe_address(&signer));

    // Paginate through the authenticated user's trades on the given market.
    // Endpoint: GET https://clob.polymarket.com/trades
    // Signing matches py-clob-client: path without query string, empty body.
    // Scope by `market` only (no `maker_address`): L2 auth already restricts
    // /trades to this account, so dropping the maker filter returns BOTH our
    // maker and taker legs (the maker-only filter silently hid taker fills).
    // `funder` is still used below to classify each row as maker vs taker.
    let mut all_trades: Vec<serde_json::Value> = Vec::new();
    let mut cursor = String::new();

    loop {
        let headers = auth.sign_request("GET", "/trades", "");
        let query = if cursor.is_empty() {
            format!("?market={}", bo.condition_id)
        } else {
            format!("?market={}&next_cursor={}", bo.condition_id, cursor)
        };
        let url = format!("{}/trades{}", CLOB_URL, query);
        let json = user_clob_get(url, headers)?;

        // Response may be a bare array or { data: [...], next_cursor }.
        let (page, next) = if let Some(arr) = json.as_array() {
            (arr.clone(), String::new())
        } else {
            let data = json.get("data").and_then(|v| v.as_array()).cloned().unwrap_or_default();
            let next = json.get("next_cursor").and_then(|v| v.as_str()).unwrap_or("").to_string();
            (data, next)
        };

        all_trades.extend(page);

        if next.is_empty() || next == "LTE=" {
            break;
        }
        cursor = next;
    }

    // Sort by match_time ascending (oldest first).
    all_trades.sort_by(|a, b| {
        let ta = a.get("match_time")
            .and_then(|v| v.as_str().and_then(|s| s.parse::<i64>().ok()).or_else(|| v.as_i64()))
            .unwrap_or(0);
        let tb = b.get("match_time")
            .and_then(|v| v.as_str().and_then(|s| s.parse::<i64>().ok()).or_else(|| v.as_i64()))
            .unwrap_or(0);
        ta.cmp(&tb)
    });

    Ok((funder, bo, all_trades))
}

/// Print each raw trade record (from the server's JSON array) in chronological order.
pub fn run_raw_trades() -> Result<()> {
    let (_funder, _bo, all_trades) = prompt_and_fetch_trades()?;
    for trade in &all_trades {
        println!("{}", serde_json::to_string(trade)?);
    }
    Ok(())
}

/// (Legacy) Fetch `(fee_rate, fee_exponent)` for a market from the Gamma API
/// by condition_id. Kept for reference but unused — `run_trades` now reads the
/// fee schedule directly from the `BinaryOption` built in `prompt_and_fetch_trades`.
#[allow(dead_code)]
fn fetch_market_fee_schedule(condition_id: &str) -> (f64, f64) {
    let url = format!("{}/markets?condition_id={}&limit=1", GAMMA_API_BASE, condition_id);
    let json = unauth_get_json(&url);
    let m = json.as_array().and_then(|a| a.first()).cloned().unwrap_or_default();
    let fs = m.get("feeSchedule").cloned().unwrap_or_default();
    let rate = fs.get("rate")
        .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
        .unwrap_or(0.0);
    let exp = fs.get("exponent")
        .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
        .unwrap_or(0.0);
    (rate, exp)
}

const GAMMA_API_BASE: &str = "https://gamma-api.polymarket.com";

/// Parse the raw trades into a normalized list from the current user's perspective.
///
/// Per-record rule:
/// - If `maker_address == funder_address`, the user was the TAKER; use the top-level
///   `asset_id`, `outcome`, `price`, `side`, `size`, `status`.
/// - Otherwise, the user was a MAKER; iterate `maker_orders` and emit one record for
///   each entry whose `maker_address == funder_address`, using that entry's
///   `asset_id`, `outcome`, `price`, `side`, `size`, combined with the top-level
///   trade `status`.
///
/// The resulting list is printed as a single pretty-printed JSON array.
///
/// When `verbose` is true, also prints each raw trade record (as returned by the
/// CLOB `/trades` endpoint) on its own JSON line before the parsed table.
pub fn run_trades(verbose: bool) -> Result<()> {
    let (funder, bo, raw_trades) = prompt_and_fetch_trades()?;

    if verbose {
        println!("--- raw trades (chronological) ---");
        for trade in &raw_trades {
            println!("{}", serde_json::to_string(trade)?);
        }
        println!("--- end raw ({} record{}) ---", raw_trades.len(),
            if raw_trades.len() == 1 { "" } else { "s" });
        println!();
    }
    let funder_lc = funder.to_lowercase();

    // Polymarket fee curve for this market (from event API feeSchedule on BinaryOption).
    //   usdc_fee = C × fee_rate × (p × (1 − p)) ^ fee_exponent
    let fee_rate = bo.fee_rate;
    let fee_exponent = bo.fee_exponent;

    let get_str = |v: &serde_json::Value, k: &str| -> serde_json::Value {
        v.get(k).cloned().unwrap_or(serde_json::Value::Null)
    };

    let mut parsed: Vec<serde_json::Value> = Vec::new();

    // Helper: parse a JSON number or numeric-string field into f64.
    let parse_num = |v: Option<&serde_json::Value>| -> f64 {
        match v {
            Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(0.0),
            Some(serde_json::Value::String(s)) => s.parse().unwrap_or(0.0),
            _ => 0.0,
        }
    };
    // Round to 0.00001 precision.
    let round5 = |x: f64| (x * 100_000.0).round() / 100_000.0;

    for trade in &raw_trades {
        let top_maker = trade.get("maker_address").and_then(|v| v.as_str()).unwrap_or("");
        let status = get_str(trade, "status");
        let trade_id = get_str(trade, "id");
        let last_update = get_str(trade, "last_update");
        // `match_time` is the trade creation/match timestamp (unix seconds).
        let match_time = get_str(trade, "match_time");

        if top_maker.eq_ignore_ascii_case(&funder_lc) {
            // User is the TAKER on this trade. Polymarket fee curve:
            //   usdc_fee = C × fee_rate × (p × (1 − p)) ^ fee_exponent
            // BUY  → charged in shares: shares_fee = usdc_fee / p   (usdc_fee = 0)
            // SELL → charged in USDC:   usdc_fee                    (shares_fee = 0)
            // Per-trade size and fees are rounded to 0.00001.
            let size = round5(parse_num(trade.get("size")));
            let price = parse_num(trade.get("price"));
            let side_raw = trade.get("side").and_then(|v| v.as_str()).unwrap_or("");

            let pp = (price.clamp(0.0, 1.0) * (1.0 - price.clamp(0.0, 1.0))).max(0.0);
            let fee_notional = if fee_rate > 0.0 && size > 0.0 {
                size * fee_rate * pp.powf(fee_exponent)
            } else { 0.0 };
            let (usdc_fee_raw, shares_fee_raw) = match side_raw.to_ascii_uppercase().as_str() {
                "BUY"  => (0.0, if price > 0.0 { fee_notional / price } else { 0.0 }),
                "SELL" => (fee_notional, 0.0),
                _      => (0.0, 0.0),
            };
            let usdc_fee = round5(usdc_fee_raw);
            let shares_fee = round5(shares_fee_raw);

            parsed.push(serde_json::json!({
                "trade_id":    trade_id,
                "match_time":  match_time,
                "last_update": last_update,
                "asset_id":    get_str(trade, "asset_id"),
                // For taker fills the data-api carries our submitted
                // order's hash on the trade's top-level — Polymarket
                // labels it `taker_order_id` (Nautilus / py-clob-client
                // both confirm this field name).
                "order_id":    get_str(trade, "taker_order_id"),
                "outcome":     get_str(trade, "outcome"),
                "price":       get_str(trade, "price"),
                "side":        get_str(trade, "side"),
                "size":        size,
                "usdc_fee":    usdc_fee,
                "shares_fee":  shares_fee,
                "status":      status,
                "role":        "TAKER",
            }));
        } else {
            // User is a MAKER: pull matching entries from maker_orders. Makers pay no fee.
            let empty: Vec<serde_json::Value> = Vec::new();
            let makers = trade.get("maker_orders")
                .and_then(|v| v.as_array())
                .unwrap_or(&empty);
            for mo in makers {
                let mo_addr = mo.get("maker_address").and_then(|v| v.as_str()).unwrap_or("");
                if !mo_addr.eq_ignore_ascii_case(&funder_lc) {
                    continue;
                }
                // maker_orders entries use `matched_amount` for the filled size.
                let size = round5(parse_num(mo.get("matched_amount")));
                parsed.push(serde_json::json!({
                    "trade_id":    trade_id.clone(),
                    "match_time":  match_time.clone(),
                    "last_update": last_update.clone(),
                    "asset_id":    get_str(mo, "asset_id"),
                    // Each maker_orders entry has its own resting order
                    // hash on `order_id`. That's the same hash the strategy
                    // would have logged when it placed the maker quote.
                    "order_id":    get_str(mo, "order_id"),
                    "outcome":     get_str(mo, "outcome"),
                    "price":       get_str(mo, "price"),
                    "side":        get_str(mo, "side"),
                    "size":        size,
                    "usdc_fee":    0.0,
                    "shares_fee":  0.0,
                    "status":      status.clone(),
                    "role":        "MAKER",
                }));
            }
        }
    }

    // Print as a table.
    if parsed.is_empty() {
        println!("No trades.");
        return Ok(());
    }

    let tid_w = 36;
    let ct_w = 19;  // "YYYY-MM-DD HH:MM:SS" (trade match_time)
    let ts_w = 19;  // "YYYY-MM-DD HH:MM:SS" (last_update)
    let role_w = 5;
    let side_w = 4;
    let outcome_w = 8;
    let price_w = 8;
    let size_w = 12;
    let ufee_w = 10;
    let sfee_w = 10;
    let status_w = 22;
    // OrderID column: Polymarket order hashes are 0x + 64 hex (66
    // chars). 20 columns shows the leading "0x" + 17 hex chars +
    // ellipsis — enough to disambiguate among the handful of
    // recent orders without overwhelming the table width. The full
    // hash is available via `hexbot trades -v` (raw JSON dump).
    let oid_w = 20;
    let total_w = tid_w + 2 + ct_w + 2 + ts_w + 2 + role_w + 2 + side_w + 2 + outcome_w + 2
        + price_w + 2 + size_w + 2 + ufee_w + 2 + sfee_w + 2 + status_w + 2 + oid_w;

    println!(
        "{:<tid$}  {:<ct$}  {:<ts$}  {:<role$}  {:<side$}  {:<outcome$}  {:>price$}  {:>size$}  {:>ufee$}  {:>sfee$}  {:<status$}  {:<oid$}",
        "Trade ID", "Created", "LastUpdate", "Role", "Side", "Outcome", "Price", "Size",
        "UsdcFee", "SharesFee", "Status", "OrderID",
        tid = tid_w, ct = ct_w, ts = ts_w, role = role_w, side = side_w, outcome = outcome_w,
        price = price_w, size = size_w, ufee = ufee_w, sfee = sfee_w,
        status = status_w, oid = oid_w,
    );
    println!("{}", "-".repeat(total_w));

    let stringify = |v: &serde_json::Value| -> String {
        match v {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Null => String::new(),
            other => other.to_string(),
        }
    };
    let parse_f = |v: &serde_json::Value| -> f64 {
        match v {
            serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0),
            serde_json::Value::String(s) => s.parse().unwrap_or(0.0),
            _ => 0.0,
        }
    };
    let truncate = |s: &str, w: usize| -> String {
        if s.chars().count() <= w { s.to_string() } else {
            let take = w.saturating_sub(1);
            let head: String = s.chars().take(take).collect();
            format!("{}…", head)
        }
    };

    let format_ts = |v: &serde_json::Value| -> String {
        let secs = match v {
            serde_json::Value::Number(n) => n.as_i64().unwrap_or(0),
            serde_json::Value::String(s) => s.parse::<i64>().unwrap_or(0),
            _ => 0,
        };
        if secs == 0 { return String::new(); }
        chrono::DateTime::from_timestamp(secs, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_default()
    };

    for rec in &parsed {
        let tid     = stringify(&rec["trade_id"]);
        let ct      = format_ts(&rec["match_time"]);
        let ts      = format_ts(&rec["last_update"]);
        let role    = stringify(&rec["role"]);
        let side    = stringify(&rec["side"]);
        let outcome = stringify(&rec["outcome"]);
        let price   = parse_f(&rec["price"]);
        let size    = parse_f(&rec["size"]);
        let ufee    = parse_f(&rec["usdc_fee"]);
        let sfee    = parse_f(&rec["shares_fee"]);
        let status  = stringify(&rec["status"]);
        let oid     = stringify(&rec["order_id"]);

        println!(
            "{:<tid$}  {:<ct$}  {:<ts$}  {:<role$}  {:<side$}  {:<outcome$}  {:>price$.4}  {:>size$.5}  {:>ufee$.5}  {:>sfee$.5}  {:<status$}  {:<oid$}",
            truncate(&tid, tid_w),
            truncate(&ct, ct_w),
            truncate(&ts, ts_w),
            truncate(&role, role_w),
            truncate(&side, side_w),
            truncate(&outcome, outcome_w),
            price,
            size,
            ufee,
            sfee,
            truncate(&status, status_w),
            truncate(&oid, oid_w),
            tid = tid_w, ct = ct_w, ts = ts_w, role = role_w, side = side_w, outcome = outcome_w,
            price = price_w, size = size_w, ufee = ufee_w, sfee = sfee_w,
            status = status_w, oid = oid_w,
        );
    }
    println!("{}", "-".repeat(total_w));
    println!("Total: {} trade(s)", parsed.len());

    // Cumulative position summary by outcome (net = BUY - SELL), with total fees.
    // Preserve first-seen order of outcomes for stable, readable output.
    #[derive(Default, Clone, Copy)]
    struct OutcomeAgg { buy: f64, sell: f64, usdc_fee: f64, shares_fee: f64 }
    let mut outcome_order: Vec<String> = Vec::new();
    let mut by_outcome: std::collections::HashMap<String, OutcomeAgg> =
        std::collections::HashMap::new();
    for rec in &parsed {
        let outcome = stringify(&rec["outcome"]);
        if outcome.is_empty() { continue; }
        let side = stringify(&rec["side"]).to_ascii_uppercase();
        let size = parse_f(&rec["size"]);
        let ufee = parse_f(&rec["usdc_fee"]);
        let sfee = parse_f(&rec["shares_fee"]);
        let entry = by_outcome.entry(outcome.clone()).or_insert_with(|| {
            outcome_order.push(outcome.clone());
            OutcomeAgg::default()
        });
        match side.as_str() {
            "BUY" => entry.buy += size,
            "SELL" => entry.sell += size,
            _ => {}
        }
        entry.usdc_fee += ufee;
        entry.shares_fee += sfee;
    }

    if !outcome_order.is_empty() {
        println!();
        println!("Position summary (net = BUY - SELL):");
        for outcome in &outcome_order {
            let a = by_outcome[outcome];
            println!(
                "  {:<6} net={:>+12.5}  (buy={:>12.5}, sell={:>12.5})  usdc_fee={:>10.5}  shares_fee={:>10.5}",
                outcome, a.buy - a.sell, a.buy, a.sell, a.usdc_fee, a.shares_fee
            );
        }
    }

    Ok(())
}

/// Fetch event/market title from Gamma API by condition_id.
fn fetch_market_title(condition_id: &str) -> String {
    let url = format!("https://gamma-api.polymarket.com/markets?condition_id={}&limit=1", condition_id);
    let json = unauth_get_json(&url);
    if let Some(arr) = json.as_array() {
        if let Some(m) = arr.first() {
            return m.get("question").and_then(|v| v.as_str())
                .unwrap_or("Unknown").to_string();
        }
    }
    "Unknown".to_string()
}

// ════════════════════════════════════════════════════════════════
// Background maintenance: redeem matured positions + split for next event
// ════════════════════════════════════════════════════════════════

/// Compute the 4-byte function selector for a Solidity signature.
fn compute_selector(sig: &str) -> [u8; 4] {
    use sha3::{Digest, Keccak256};
    let mut hasher = Keccak256::new();
    hasher.update(sig.as_bytes());
    let hash = hasher.finalize();
    [hash[0], hash[1], hash[2], hash[3]]
}

/// Outcome of `run_redeem_all`. Aggregates the per-tx fates so the
/// caller (maintenance thread) can decide whether to mark
/// `MaintenanceStatus::RedeemFailed` vs proceed to the split step.
///
/// "No work" (nothing to redeem) and "all succeeded" are both treated
/// as healthy — the split step doesn't depend on redeem result on its
/// own (it only needs sufficient USDC, and a failed redeem leaves USDC
/// untouched).
#[derive(Debug, Default)]
struct RedeemResult {
    /// Number of conditionIds we attempted to redeem.
    attempted: usize,
    /// Number whose final state was CONFIRMED/MINED.
    confirmed: usize,
    /// Number that failed submission outright (broadcast error after
    /// all gas-tier retries).
    submit_failed: usize,
    /// Number that submitted but didn't reach CONFIRMED/MINED within
    /// the poll timeout (left as PENDING).
    pending_timeout: usize,
    /// Compact summary of the first failure reason — useful in
    /// MaintenanceStatus::RedeemFailed { reason }.
    first_failure: Option<String>,
}

impl RedeemResult {
    fn all_ok(&self) -> bool {
        self.attempted == self.confirmed
    }
    fn summary(&self) -> String {
        format!(
            "attempted={} confirmed={} submit_failed={} pending_timeout={}",
            self.attempted, self.confirmed, self.submit_failed, self.pending_timeout,
        )
    }
}

/// Execute redemption of all currently redeemable positions for the wallet.
/// Each redemption is an on-chain Safe tx submitted serially + polled to
/// completion. Logs the full redeemable list (table) before starting, then
/// per-tx state as each one lands.
///
/// When `gas_via_signer=true`, uses
/// [`super::onchain_tx::broadcast_with_escalation`] which retries failed
/// broadcasts up to 3 times with increasing gas (500 → 700 → 1000 gwei)
/// — the cure for "replacement transaction underpriced" wedge that
/// blocked maintenance in the 2026-05-16 10:00-12:00 session.
fn run_redeem_all(wallet: &WalletInfo, gas_via_signer: bool) -> RedeemResult {
    // Same v1/v2 dispatch as the `hexbot redeem` CLI.
    let is_v2 = read_clob_v2_flag();
    let (target_contract, collateral_token) = ctf_target(is_v2, /*neg_risk=*/ false);
    log::info!(
        "[Maintenance] Redeem target: {} ({}, collateral={})",
        target_contract,
        if is_v2 { "v2 CtfCollateralAdapter" } else { "v1 CTF" },
        collateral_token,
    );
    log::info!("[Maintenance] Step 1/2: Redeem matured positions");
    // POLY_1271 holds positions in the deposit wallet, not the Safe.
    let redeem_addr = wallet.deposit_wallet_active().unwrap_or(&wallet.safe_address);
    let url = format!(
        "{}/positions?user={}&sizeThreshold=0&limit=500",
        DATA_API_BASE, redeem_addr,
    );
    let mut result = RedeemResult::default();

    let positions: Vec<PositionRecord> = match crate::async_rt::blocking_get_text(&url) {
        Ok(text) => serde_json::from_str(&text).unwrap_or_default(),
        Err(e) => {
            log::warn!("[Maintenance] Fetch positions failed: {}", e);
            result.first_failure = Some(format!("fetch positions: {}", e));
            return result;
        }
    };

    // Group redeemable positions by conditionId (preserve discovery order).
    let mut condition_ids: Vec<String> = Vec::new();
    let mut grouped: std::collections::HashMap<String, Vec<&PositionRecord>> =
        std::collections::HashMap::new();
    for p in &positions {
        if p.redeemable && p.size > 0.0 {
            if !grouped.contains_key(&p.condition_id) {
                condition_ids.push(p.condition_id.clone());
            }
            grouped.entry(p.condition_id.clone()).or_default().push(p);
        }
    }

    // On-chain balance gate for the open-feed set. Polymarket's data-api keeps
    // flagging resolved positions (especially value-0 LOSING legs) as
    // `redeemable=true` with a stale `size` long after the wallet's on-chain
    // balance hit 0. `redeemPositions` on a 0-balance position MINES
    // SUCCESSFULLY (a no-op), so without this gate maintenance re-"redeems" the
    // same data-api ghosts every cycle forever (observed: identical cids
    // confirmed 4×). Keep a cid only if at least one outcome leg is still held
    // on-chain (active-collateral id-space). ~6 eth_calls/cid; the open
    // redeemable set is naturally small.
    {
        let before = condition_ids.len();
        condition_ids.retain(|cid| {
            let (up, down) = ctf_event_outcome_balances(redeem_addr, cid, is_v2);
            let held = up > 0.000_001 || down > 0.000_001;
            if !held {
                grouped.remove(cid);
                log::info!(
                    "[Maintenance]   skip ghost cid={}... (data-api redeemable but on-chain balance 0)",
                    cid.chars().take(16).collect::<String>(),
                );
            }
            held
        });
        let dropped = before - condition_ids.len();
        if dropped > 0 {
            log::info!(
                "[Maintenance] Skipped {} data-api ghost(s) with 0 on-chain balance",
                dropped,
            );
        }
    }

    if condition_ids.is_empty() {
        log::info!("[Maintenance] No redeemable positions — nothing to redeem.");
        return result;
    }
    result.attempted = condition_ids.len();

    // ── Pre-redeem listing (mirrors `hexbot redeem` CLI table) ──
    let mut total_value = 0.0;
    log::info!("[Maintenance] Found {} redeemable condition(s):", condition_ids.len());
    log::info!(
        "[Maintenance]   {:>3}  {:<46} {:>8} {:>8} {:>10}",
        "#", "Market", "Outcome", "Size", "Value",
    );
    for (idx, cid) in condition_ids.iter().enumerate() {
        // Closed-scan cids have no open-feed legs in `grouped` — already logged
        // above; skip the per-leg table rows (and don't panic on the index).
        let Some(legs) = grouped.get(cid) else { continue };
        for (j, p) in legs.iter().enumerate() {
            let title = if p.title.len() > 44 {
                format!("{}...", &p.title[..41])
            } else {
                p.title.clone()
            };
            let num = if j == 0 { format!("{}", idx + 1) } else { String::new() };
            log::info!(
                "[Maintenance]   {:>3}  {:<46} {:>8} {:>8.2} {:>10.4}",
                num, title, p.outcome, p.size, p.current_value,
            );
            total_value += p.current_value;
        }
    }
    log::info!(
        "[Maintenance]   Total: {:.4} USDC across {} condition(s)",
        total_value, condition_ids.len(),
    );

    for (i, cid) in condition_ids.iter().enumerate() {
        let cid_short: String = cid.chars().take(16).collect();

        // POLY_1271: redeem FROM the deposit wallet via WALLET batch.
        if let Some(dw) = wallet.deposit_wallet_active() {
            match super::deposit_wallet::dw_redeem(
                &wallet.signing_key, &wallet.signer_address, dw,
                &wallet.builder_auth, cid,
            ) {
                Ok(tx) => {
                    result.confirmed += 1;
                    log::info!(
                        "[Maintenance] DW redeem [{}/{}] cid={} done tx={}",
                        i + 1, condition_ids.len(), cid_short, tx,
                    );
                }
                Err(e) => {
                    result.submit_failed += 1;
                    if result.first_failure.is_none() {
                        result.first_failure = Some(format!("DW redeem cid={}: {}", cid_short, e));
                    }
                    log::warn!("[Maintenance] DW redeem failed cid={}: {}", cid_short, e);
                }
            }
            continue;
        }

        let cid_bytes = hex::decode(cid.strip_prefix("0x").unwrap_or(cid)).unwrap_or_default();
        let mut cid_padded = [0u8; 32];
        let start = 32 - cid_bytes.len().min(32);
        cid_padded[start..].copy_from_slice(&cid_bytes[..cid_bytes.len().min(32)]);

        let mut calldata = Vec::with_capacity(4 + 32 * 7);
        calldata.extend_from_slice(&REDEEM_SELECTOR);
        calldata.extend_from_slice(&address_to_bytes32(collateral_token));
        calldata.extend_from_slice(&[0u8; 32]);    // parentCollectionId = 0
        calldata.extend_from_slice(&cid_padded);   // conditionId
        calldata.extend_from_slice(&u256_bytes(128)); // offset to indexSets
        calldata.extend_from_slice(&u256_bytes(2));   // length = 2
        calldata.extend_from_slice(&u256_bytes(1));   // indexSets[0] = 1
        calldata.extend_from_slice(&u256_bytes(2));   // indexSets[1] = 2
        let data_hex = format!("0x{}", hex::encode(&calldata));

        // Submit. Dispatch by account kind:
        //   * EOA (signatureType=0) — the EOA holds the outcome tokens and
        //     redeems them by calling `target_contract` DIRECTLY (msg.sender
        //     == EOA), paying its own POL. No Safe execTransaction, no
        //     relayer. Needs the CTF→adapter approval (`hexbot approve_v2`
        //     grants it for the EOA). ⚠ EOA auto-redeem is gated behind the
        //     same default-off maintenance flags as every other account; the
        //     v2 adapter path here has not been live-validated for a bare
        //     EOA — verify on a funded test wallet before enabling.
        //   * on-chain Safe (`gas_via_signer=true`) — gas escalation
        //     500→700→1000 gwei across 3 attempts.
        //   * relayer (`gas_via_signer=false`) — single-shot; gas is the
        //     relayer's problem.
        let submit_outcome: std::result::Result<(String, String), String> = if wallet.is_eoa() {
            match super::onchain_tx::submit_eoa_tx_onchain(
                &wallet.signing_key,
                &wallet.signer_address,
                target_contract,
                &data_hex,
            ) {
                Ok(tx_hash) => Ok((tx_hash, "PENDING".to_string())),
                Err(e) => Err(e.to_string()),
            }
        } else if gas_via_signer {
            match super::onchain_tx::broadcast_with_escalation(
                &wallet.signing_key,
                &wallet.signer_address,
                &wallet.safe_address,
                target_contract,
                &data_hex,
            ) {
                Ok(tx_hash) => Ok((tx_hash, "PENDING".to_string())),
                Err(e) => Err(e.to_string()),
            }
        } else {
            match submit_safe_tx_with_id(
                &wallet.builder_auth, &wallet.signing_key,
                &wallet.signer_address, &wallet.safe_address,
                target_contract, &data_hex,
                gas_via_signer,
            ) {
                Ok(pair) => Ok(pair),
                Err(e) => Err(e.to_string()),
            }
        };

        match submit_outcome {
            Ok((tx_id, _)) => {
                let mut final_state = String::new();
                let mut tx_hash = String::new();
                for _ in 0..30 {
                    std::thread::sleep(std::time::Duration::from_secs(2));
                    match poll_transaction(&wallet.builder_auth, &tx_id) {
                        Ok((s, h)) => {
                            final_state = s.clone();
                            tx_hash = h;
                            if s.contains("CONFIRMED") || s.contains("MINED") || s.contains("FAILED") {
                                break;
                            }
                        }
                        Err(e) => { log::warn!("[Maintenance] Redeem poll error: {}", e); break; }
                    }
                }
                if final_state.contains("CONFIRMED") || final_state.contains("MINED") {
                    result.confirmed += 1;
                    log::info!(
                        "[Maintenance] Redeem [{}/{}] cid={} done tx=0x{}",
                        i + 1, condition_ids.len(), cid_short, tx_hash.trim_start_matches("0x"),
                    );
                } else {
                    result.pending_timeout += 1;
                    if result.first_failure.is_none() {
                        result.first_failure = Some(format!(
                            "cid={} pending_timeout final_state={}", cid_short, final_state,
                        ));
                    }
                    log::warn!(
                        "[Maintenance] Redeem [{}/{}] cid={} final_state={}",
                        i + 1, condition_ids.len(), cid_short, final_state,
                    );
                }
            }
            Err(e) => {
                result.submit_failed += 1;
                if result.first_failure.is_none() {
                    result.first_failure = Some(format!("cid={} submit_failed: {}", cid_short, e));
                }
                log::warn!("[Maintenance] Redeem submit failed cid={}: {}", cid_short, e);
            }
        }
    }
    result
}

/// Final disposition of `run_split_one`. Returned so the caller can
/// promote the maintenance pipeline's overall status.
#[derive(Debug, Clone, PartialEq)]
enum SplitOutcome {
    /// `splitPosition` confirmed on-chain — bot has seed inventory.
    Confirmed,
    /// Pre-flight (balance check / amount=0 / no v2 etc.) skipped the
    /// split. Contains the reason for diagnostics.
    Skipped(String),
    /// Submit failed after all gas-tier retries. Caller should
    /// downgrade strategy to PROBE.
    SubmitFailed(String),
    /// Submitted but didn't reach CONFIRMED/MINED within the poll
    /// window (left as PENDING). Caller should treat as effectively
    /// failed for the immediately-next event.
    PendingTimeout(String),
}

/// Run one splitPosition for `condition_id` with `amount_usdc` USDC → equal
/// shares of Up + Down tokens on that event. Only proceeds if the wallet's
/// USDC balance is ≥ the requested amount.
///
/// When `gas_via_signer=true`, uses
/// [`super::onchain_tx::broadcast_with_escalation`] — 500/700/1000 gwei
/// gas tiers — so a stuck mempool tx can't wedge seed-inventory creation.
fn run_split_one(
    wallet: &WalletInfo,
    condition_id: &str,
    amount_usdc: f64,
    min_safety_balance_usdc: f64,
    gas_via_signer: bool,
) -> SplitOutcome {
    if amount_usdc <= 0.0 {
        log::info!("[Maintenance] Split disabled (amount_usdc={}).", amount_usdc);
        return SplitOutcome::Skipped(format!("amount_usdc={}", amount_usdc));
    }

    // ── POLY_1271 deposit-wallet path: split FROM the DW via a WALLET
    //    batch (splitPosition on the CTF directly with pUSD collateral),
    //    not the Gnosis Safe's CtfCollateralAdapter + execTransaction. ──
    if let Some(dw) = wallet.deposit_wallet_active() {
        let balance = fetch_pusd_balance(dw);
        let effective_floor = amount_usdc.max(min_safety_balance_usdc);
        let cid_short: String = condition_id.chars().take(16).collect();
        if balance < effective_floor {
            log::warn!(
                "[Maintenance] [ALERT] DW split skipped: pUSD {:.4} on {} < floor {:.4} \
                 (requested {:.4}) — next event PROBE until topped up.",
                balance, &dw[..10.min(dw.len())], effective_floor, amount_usdc,
            );
            return SplitOutcome::Skipped(format!(
                "DW pUSD {:.4} < floor {:.4}", balance, effective_floor
            ));
        }
        let amount_wei: u128 = (amount_usdc * 1_000_000.0).round().max(0.0) as u128;
        if amount_wei == 0 {
            return SplitOutcome::Skipped("amount_wei=0".to_string());
        }
        log::info!(
            "[Maintenance] DW split: cid={} amount_usdc={:.4} (pUSD={:.4}) via WALLET batch",
            cid_short, amount_usdc, balance,
        );
        return match super::deposit_wallet::dw_split(
            &wallet.signing_key, &wallet.signer_address, dw,
            &wallet.builder_auth, condition_id, amount_wei,
        ) {
            Ok(tx) => {
                log::info!("[Maintenance] DW split confirmed cid={} tx={}", cid_short, tx);
                SplitOutcome::Confirmed
            }
            Err(e) => {
                log::warn!("[Maintenance] DW split failed cid={}: {}", cid_short, e);
                SplitOutcome::SubmitFailed(e.to_string())
            }
        };
    }

    // Same v1/v2 dispatch as the CLI.
    let is_v2 = read_clob_v2_flag();
    let (target_contract, collateral_token) = ctf_target(is_v2, /*neg_risk=*/ false);
    log::info!(
        "[Maintenance] Split target: {} ({}, collateral={})",
        target_contract,
        if is_v2 { "v2 CtfCollateralAdapter" } else { "v1 CTF" },
        collateral_token,
    );

    // Read the balance of the **active** collateral token, not the
    // legacy USDC.e for both modes. v2 deployments hold pUSD in the
    // safe (post-migrate); the v2 CtfCollateralAdapter unwraps to
    // USDC.e on the fly when calling splitPosition. Querying USDC.e
    // here always returns 0 on a v2-migrated safe — that's the
    // 50-in-a-row "Split skipped: USDC balance 0.0000" wedge
    // observed 2026-05-04 05:04→09:09 (live.log). The downstream
    // calldata at line ~2153 already passes `collateral_token`
    // (= pUSD in v2) correctly; this guard just needs to match.
    let balance = if is_v2 {
        fetch_pusd_balance(&wallet.safe_address)
    } else {
        fetch_usdce_balance(&wallet.safe_address)
    };
    let token_name = if is_v2 { "pUSD" } else { "USDC.e" };
    // Two-tier guard:
    //
    //   1. Hard floor = amount_usdc           — chain-reality: split tx
    //                                            would revert otherwise.
    //   2. Soft floor = min_safety_balance_usdc — operator-set safety
    //                                            margin. Drawdown-aware
    //                                            circuit breaker: if the
    //                                            safe's pUSD has bled
    //                                            below this threshold,
    //                                            assume something went
    //                                            wrong upstream (large
    //                                            negative-pnl event,
    //                                            unrecovered fill, etc.)
    //                                            and refuse to seed the
    //                                            NEXT event. The strategy
    //                                            converts the resulting
    //                                            SplitFailedOrPending
    //                                            status into PROBE mode
    //                                            (no quoting) via
    //                                            maintenance_hold_check,
    //                                            so we stop trading
    //                                            until ops tops the
    //                                            wallet back up.
    //
    // Effective floor = max(hard, soft). Hard floor never goes below
    // amount_usdc because the split tx would just revert; soft floor
    // catches the slower wallet-bleeding case BEFORE it becomes a
    // chain-level failure.
    let effective_floor = amount_usdc.max(min_safety_balance_usdc);
    if balance < effective_floor {
        // Distinguish "chain would reject" from "operator safety
        // tripped" so an on-call engineer reading logs can tell which
        // happened. ALERT prefix is grep-friendly for log forwarders.
        let alert = if balance < amount_usdc {
            format!(
                "[ALERT] Split skipped: {} balance {:.4} on safe {} < requested {:.4} (chain-floor)",
                token_name, balance,
                &wallet.safe_address[..10.min(wallet.safe_address.len())],
                amount_usdc,
            )
        } else {
            format!(
                "[ALERT] Split skipped: {} balance {:.4} on safe {} below safety floor {:.4} \
                 (requested split {:.4} — chain would succeed but operator threshold tripped, \
                 forcing next event PROBE until wallet is topped up)",
                token_name, balance,
                &wallet.safe_address[..10.min(wallet.safe_address.len())],
                min_safety_balance_usdc, amount_usdc,
            )
        };
        log::warn!("[Maintenance] {}", alert);
        return SplitOutcome::Skipped(format!(
            "{} balance {:.4} < safety floor {:.4} (requested {:.4})",
            token_name, balance, effective_floor, amount_usdc,
        ));
    }

    // USDC has 6 decimals.
    let amount_wei: u128 = (amount_usdc * 1_000_000.0).round().max(0.0) as u128;
    if amount_wei == 0 {
        log::warn!("[Maintenance] Split skipped: amount_wei=0 after rounding.");
        return SplitOutcome::Skipped("amount_wei=0".to_string());
    }

    let cid_bytes = hex::decode(condition_id.strip_prefix("0x").unwrap_or(condition_id))
        .unwrap_or_default();
    let mut cid_padded = [0u8; 32];
    let start = 32 - cid_bytes.len().min(32);
    cid_padded[start..].copy_from_slice(&cid_bytes[..cid_bytes.len().min(32)]);

    // splitPosition(address,bytes32,bytes32,uint256[],uint256)
    //  head = 5 slots (160 bytes); partition tail = 3 slots (96 bytes)
    let selector = compute_selector(
        "splitPosition(address,bytes32,bytes32,uint256[],uint256)",
    );
    let mut calldata = Vec::with_capacity(4 + 32 * 8);
    calldata.extend_from_slice(&selector);
    calldata.extend_from_slice(&address_to_bytes32(collateral_token));   // collateralToken (pUSD in v2)
    calldata.extend_from_slice(&[0u8; 32]);                               // parentCollectionId = 0
    calldata.extend_from_slice(&cid_padded);                              // conditionId
    calldata.extend_from_slice(&u256_bytes(160));                         // offset to partition (5*32)
    calldata.extend_from_slice(&u256_bytes(amount_wei));                  // amount (USDC wei; 6-dec)
    calldata.extend_from_slice(&u256_bytes(2));                           // partition.length = 2
    calldata.extend_from_slice(&u256_bytes(1));                           // partition[0] = 1 (Up)
    calldata.extend_from_slice(&u256_bytes(2));                           // partition[1] = 2 (Down)
    let data_hex = format!("0x{}", hex::encode(&calldata));

    let cid_short: String = condition_id.chars().take(16).collect();
    log::info!(
        "[Maintenance] Split request: cid={} amount_usdc={:.4} (balance={:.4})",
        cid_short, amount_usdc, balance,
    );

    // EOA (signatureType=0) splits by calling `target_contract` DIRECTLY
    // (msg.sender == EOA holds the collateral), paying its own POL — no
    // Safe execTransaction, no relayer. Needs the pUSD→adapter approval
    // (`hexbot approve_v2` grants it for the EOA). ⚠ Same default-off
    // maintenance gate as every account; the v2 adapter split has not been
    // live-validated for a bare EOA — verify on a funded test wallet first.
    let submit_outcome: std::result::Result<(String, String), String> = if wallet.is_eoa() {
        match super::onchain_tx::submit_eoa_tx_onchain(
            &wallet.signing_key,
            &wallet.signer_address,
            target_contract,
            &data_hex,
        ) {
            Ok(tx_hash) => Ok((tx_hash, "PENDING".to_string())),
            Err(e) => Err(e.to_string()),
        }
    } else if gas_via_signer {
        match super::onchain_tx::broadcast_with_escalation(
            &wallet.signing_key,
            &wallet.signer_address,
            &wallet.safe_address,
            target_contract,
            &data_hex,
        ) {
            Ok(tx_hash) => Ok((tx_hash, "PENDING".to_string())),
            Err(e) => Err(e.to_string()),
        }
    } else {
        match submit_safe_tx_with_id(
            &wallet.builder_auth, &wallet.signing_key,
            &wallet.signer_address, &wallet.safe_address,
            target_contract, &data_hex,
            gas_via_signer,
        ) {
            Ok(pair) => Ok(pair),
            Err(e) => Err(e.to_string()),
        }
    };

    match submit_outcome {
        Ok((tx_id, _)) => {
            let mut final_state = String::new();
            let mut tx_hash = String::new();
            for _ in 0..30 {
                std::thread::sleep(std::time::Duration::from_secs(2));
                match poll_transaction(&wallet.builder_auth, &tx_id) {
                    Ok((s, h)) => {
                        final_state = s.clone();
                        tx_hash = h;
                        if s.contains("CONFIRMED") || s.contains("MINED") || s.contains("FAILED") {
                            break;
                        }
                    }
                    Err(e) => { log::warn!("[Maintenance] Split poll error: {}", e); break; }
                }
            }
            if final_state.contains("CONFIRMED") || final_state.contains("MINED") {
                log::info!(
                    "[Maintenance] Split cid={} done tx=0x{}",
                    cid_short, tx_hash.trim_start_matches("0x"),
                );
                SplitOutcome::Confirmed
            } else {
                log::warn!(
                    "[Maintenance] Split cid={} final_state={}",
                    cid_short, final_state,
                );
                SplitOutcome::PendingTimeout(format!("final_state={}", final_state))
            }
        }
        Err(e) => {
            log::warn!("[Maintenance] Split submit failed cid={}: {}", cid_short, e);
            SplitOutcome::SubmitFailed(e)
        }
    }
}

/// Spawn a one-shot detached maintenance thread that runs three steps in
/// strict serial order so only one HTTP/RPC connection is in use at a time:
///
///   1. Redeem all currently redeemable positions (serial Safe txs, each
///      submit+poll to finality).
///   2. If `split_series_id` is provided, fetch the NEXT upcoming event in
///      that series via gamma-api (`end_date_min = split_end_date_min_secs`)
///      and log its title / id / slug / start / end.
///   3. If a next event was found AND `split_amount_usdc > 0` AND the
///      wallet's USDC balance is ≥ `split_amount_usdc`, submit
///      `splitPosition` for that event so the strategy starts the next
///      event with seed Up + Down inventory.
///
/// Returns immediately after spawning; caller can `.join()` the handle if
/// they need the thread to finish (e.g. the `hexbot split` CLI).
/// `min_safety_balance_usdc` — soft floor: if pUSD balance < this,
/// split is refused even when the chain-level hard floor
/// (= `split_amount_usdc`) is satisfied. Default is
/// `split_amount_usdc` itself (no extra margin); strategy / CLI
/// override to e.g. $50 to gate trading on a healthy wallet reserve.
/// A queued maintenance job: redeem (optional) + split-seed for ONE
/// account's next event. Submitted to the global executor, which runs
/// it on the per-account worker thread.
pub struct MaintenanceJob {
    pub split_series_id: Option<String>,
    pub split_end_date_min_secs: u64,
    pub split_amount_usdc: f64,
    pub min_safety_balance_usdc: f64,
    pub gas_via_signer: bool,
    /// When `false`, the redeem step (Step 1) is skipped entirely — the
    /// split-seed step still runs. The live auto-maintenance task passes
    /// its `maintenance_redeem_enabled` config (default off); the `hexbot
    /// split` CLI passes `true`. The separate `hexbot redeem` CLI is NOT
    /// routed through here and always redeems.
    pub redeem_enabled: bool,
    pub status: Option<MaintenanceStatusHandle>,
    /// Account whose wallet runs this split/redeem. Resolves per-account
    /// creds from the registry (multi-account safe); empty → global env.
    /// Also the executor key: jobs with the same `account_id` run serially
    /// on one worker; different accounts run in parallel on their own.
    pub account_id: String,
}

// ── Global maintenance executor ──
// Replaces the old per-call detached thread. One worker thread per
// `account_id` (lazily spawned) drains that account's queue SERIALLY, so
// two instances sharing a wallet (e.g. BTC + ETH whose 5-min events end
// at the same instant) never run split/redeem concurrently on the same
// wallet — which would race on the shared USDC pool and the signer's
// on-chain nonce. Different accounts get different workers → parallel.
type MaintenanceQueueItem = (MaintenanceJob, Option<crossbeam_channel::Sender<()>>);

static MAINTENANCE_QUEUES: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<String, crossbeam_channel::Sender<MaintenanceQueueItem>>>,
> = std::sync::OnceLock::new();

/// Enqueue a job onto its account's serial worker, spawning the worker on
/// first use. Writes `Running` to the status handle IMMEDIATELY (before
/// the worker may even pick it up) so a job that waits in queue behind a
/// sibling on the same account still gates that instance's quoting — the
/// strategy's grace-window poll must not see `NotStarted` and resume early.
fn enqueue_maintenance(job: MaintenanceJob, done: Option<crossbeam_channel::Sender<()>>) {
    if let Some(ref s) = job.status {
        *s.lock().unwrap() = MaintenanceStatus::Running;
    }
    let account = job.account_id.clone();
    let queues = MAINTENANCE_QUEUES
        .get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
    let mut map = queues.lock().unwrap();
    let tx = map.entry(account.clone()).or_insert_with(|| {
        let (tx, rx) = crossbeam_channel::unbounded::<MaintenanceQueueItem>();
        let worker_label = if account.is_empty() { "env".to_string() } else { account.clone() };
        std::thread::Builder::new()
            .name(format!("poly-maint-{}", worker_label))
            .spawn(move || {
                // Serial drain: never two on-chain maintenance runs for the
                // same wallet at once.
                while let Ok((job, done)) = rx.recv() {
                    run_maintenance_job(job);
                    if let Some(d) = done {
                        let _ = d.send(());
                    }
                }
            })
            .expect("Failed to spawn maintenance worker");
        tx
    });
    if let Err(e) = tx.send((job, done)) {
        log::warn!("[Maintenance] enqueue failed (worker gone): {}", e);
    }
}

/// Fire-and-forget submit (live strategy). Returns immediately; the
/// per-account worker runs the job serially and updates `job.status`.
pub fn submit_maintenance(job: MaintenanceJob) {
    enqueue_maintenance(job, None);
}

/// Blocking submit (CLI `hexbot split`): enqueue then wait for the
/// per-account worker to finish THIS job, preserving the old
/// `spawn_maintenance_thread(...).join()` semantics so all log output
/// appears before the command returns.
pub fn run_maintenance_blocking(job: MaintenanceJob) {
    let (done_tx, done_rx) = crossbeam_channel::unbounded();
    enqueue_maintenance(job, Some(done_tx));
    let _ = done_rx.recv();
}

/// Run one maintenance job to completion on the calling (worker) thread:
/// redeem (if enabled) → fetch next event → split-seed → write terminal
/// status. Body is unchanged from the former `spawn_maintenance_thread`
/// closure (now driven by the executor instead of a per-call thread).
fn run_maintenance_job(job: MaintenanceJob) {
    let MaintenanceJob {
        split_series_id,
        split_end_date_min_secs,
        split_amount_usdc,
        min_safety_balance_usdc,
        gas_via_signer,
        redeem_enabled,
        status,
        account_id,
    } = job;
    {
            // Write `Running` immediately so the strategy can distinguish
            // "spawn requested but thread hasn't started yet" from
            // "in-progress" — the gap is normally <1ms but worth being
            // explicit about.
            if let Some(ref s) = status {
                *s.lock().unwrap() = MaintenanceStatus::Running;
            }

            log::info!(
                "[Maintenance] Starting: series_id={:?} split_amount_usdc={} gas_via_signer={} redeem_enabled={}",
                split_series_id, split_amount_usdc, gas_via_signer, redeem_enabled,
            );
            let wallet = match load_wallet_for_account(&account_id) {
                Ok(w) => w,
                Err(e) => {
                    log::warn!("[Maintenance] load_wallet failed: {}", e);
                    if let Some(s) = status {
                        *s.lock().unwrap() = MaintenanceStatus::RedeemFailed {
                            reason: format!("load_wallet: {}", e),
                        };
                    }
                    return;
                }
            };

            // ── Step 1: redeem matured positions ──
            // Gated by `redeem_enabled`. When off, redeem is skipped (no
            // on-chain ops) and treated as healthy — `RedeemResult::default()`
            // reports attempted=0 → `all_ok()` is true — so the split-seed
            // step proceeds and the final status isn't degraded. Operators
            // redeem manually via the `hexbot redeem` CLI.
            let redeem_result = if redeem_enabled {
                run_redeem_all(&wallet, gas_via_signer)
            } else {
                log::info!(
                    "[Maintenance] Step 1/2: Redeem SKIPPED (redeem_enabled=false); \
                     split-seed still runs. Use `hexbot redeem` for manual redeem."
                );
                RedeemResult::default()
            };
            let redeem_ok = redeem_result.all_ok();

            // ── Step 2: look up next event (serial, after redeem) ──
            log::info!("[Maintenance] Step 2/2: Split seed inventory for next event");
            let next_cid: Option<String> = match split_series_id.as_deref() {
                None => {
                    log::info!("[Maintenance] Split skipped: no series_id provided.");
                    None
                }
                Some(sid) => {
                    match super::market::fetch_next_event(sid, split_end_date_min_secs) {
                        Ok(Some(event)) => {
                            event.markets.first()
                                .map(|m| m.condition_id.clone())
                                .filter(|s| !s.is_empty())
                        }
                        Ok(None) => {
                            log::info!("[Maintenance] Split skipped: no matching next event.");
                            None
                        }
                        Err(e) => {
                            log::warn!("[Maintenance] fetch_next_event failed: {}", e);
                            None
                        }
                    }
                }
            };

            // ── Step 3: split seed inventory ──
            // Split is the GATING step for whether the next event has
            // seed inventory. Even if redeem failed, a healthy split
            // means trading is viable for the next event (the failed
            // redeem just leaves stale shares we can pick up later).
            let split_outcome = if let Some(cid) = next_cid {
                if split_amount_usdc > 0.0 {
                    Some(run_split_one(
                        &wallet, &cid, split_amount_usdc,
                        min_safety_balance_usdc, gas_via_signer,
                    ))
                } else {
                    log::info!("[Maintenance] Split skipped: split_amount_usdc={}.", split_amount_usdc);
                    Some(SplitOutcome::Skipped(format!("amount_usdc={}", split_amount_usdc)))
                }
            } else {
                None
            };

            // ── Final status update ──
            // Priority of failure signals (worst first):
            //   1. Split actually failed → critical, gate must PROBE
            //   2. Split pending timeout → critical, gate must PROBE
            //   3. Redeem failed but split OK → degraded but tradeable
            //   4. Everything clean → Succeeded
            let final_status = match &split_outcome {
                Some(SplitOutcome::Confirmed) => {
                    if redeem_ok {
                        MaintenanceStatus::Succeeded
                    } else {
                        // Split OK, but redeem had problems — seed
                        // inventory exists so we can still trade.
                        // Surface the redeem failure for ops visibility.
                        log::warn!(
                            "[Maintenance] Redeem partially failed ({}) but split confirmed — \
                             treating as Succeeded for next-event tradeability",
                            redeem_result.summary(),
                        );
                        MaintenanceStatus::Succeeded
                    }
                }
                Some(SplitOutcome::PendingTimeout(reason)) => {
                    MaintenanceStatus::SplitFailedOrPending {
                        reason: format!("split pending timeout: {}", reason),
                    }
                }
                Some(SplitOutcome::SubmitFailed(reason)) => {
                    MaintenanceStatus::SplitFailedOrPending {
                        reason: format!("split submit failed: {}", reason),
                    }
                }
                Some(SplitOutcome::Skipped(reason)) => {
                    // Split deliberately skipped (no series_id, no
                    // balance, etc.). Caller may still want to trade
                    // — but seed inventory is missing. Conservative:
                    // treat as needing PROBE.
                    MaintenanceStatus::SplitFailedOrPending {
                        reason: format!("split skipped: {}", reason),
                    }
                }
                None => {
                    // No next event found — nothing to split for.
                    // If redeem ran ok and there's just no next event,
                    // we're idle; treat as Skipped (gate will keep its
                    // own state).
                    MaintenanceStatus::Skipped {
                        reason: "no next event found".to_string(),
                    }
                }
            };

            if let Some(s) = status {
                log::info!(
                    "[Maintenance] Complete. status={} redeem={{{}}}",
                    final_status.label(), redeem_result.summary(),
                );
                *s.lock().unwrap() = final_status;
            } else {
                log::info!("[Maintenance] Complete.");
            }
    }
}

// ════════════════════════════════════════════════════════════════
// split CLI — manually trigger the poly-maintenance thread (test)
// ════════════════════════════════════════════════════════════════

/// CLI entrypoint: `hexbot split <series_slug> <amount_usdc>`.
///
/// Submits the same maintenance job the live strategy fires ~30s before
/// an event ends (now via `run_maintenance_blocking` on the global
/// executor):
///   1. redeem all redeemable positions for the wallet
///   2. resolve `<series_slug>` → series_id, then query the gamma-api
///      for the next upcoming event (end_date_min = now + 60s, earliest
///      ascending)
///   3. split `<amount_usdc>` of USDC into Up + Down shares on that event
///
/// Blocks until the executor finishes this job so all log output is
/// visible before the command returns.
pub fn run_split() -> Result<()> {
    let args: Vec<String> = crate::exchange::polymarket::cli_account::cli_args().collect();
    if args.len() < 2 {
        eprintln!("Usage: hexbot split <series_slug> <amount_usdc>");
        eprintln!("  e.g.  hexbot split btc-updown-5m 1.0");
        eprintln!();
        eprintln!("Exercises the poly-maintenance thread once:");
        eprintln!("  - redeem all currently redeemable positions");
        eprintln!("  - split <amount_usdc> USDC for the NEXT event of <series_slug>");
        eprintln!();
        eprintln!("Normal production triggers this automatically ~30s before");
        eprintln!("each event ends; this CLI is for manual / test invocation.");
        return Ok(());
    }
    let series_slug = &args[0];
    let amount_usdc: f64 = args[1].parse()
        .map_err(|e| anyhow!("Invalid amount '{}': {}", args[1], e))?;

    let is_v2 = read_clob_v2_flag();
    let (target_contract, collateral_token) = ctf_target(is_v2, /*neg_risk=*/ false);

    println!("=== Polymarket Split (test trigger) ===");
    println!("Series slug : {}", series_slug);
    println!("Split amount: {} USDC", amount_usdc);
    println!("CLOB        : {} ({})",
        if is_v2 { "v2" } else { "v1" },
        if is_v2 { "pUSD via CtfCollateralAdapter" } else { "USDC.e via CTF" });
    println!("Target      : {}", target_contract);
    println!("Collateral  : {}", collateral_token);
    println!();

    // Only series_id resolution happens up-front; everything else runs
    // inside the maintenance thread so the log order matches the actual
    // execution order: load → redeem → fetch-next → split → complete.
    let series_id = super::market::resolve_series_id(series_slug)?;
    println!("Resolved series_id = {}", series_id);

    // Parse event duration from the series slug (e.g. "btc-up-or-down-5m"
    // → 300s). Fall back to 300s if the slug has no recognizable suffix;
    // this still excludes the current event in typical 5-minute-cycle
    // series.
    let duration_secs = super::market::parse_slug_duration_secs(series_slug)
        .unwrap_or(300);
    println!("Parsed event duration = {}s", duration_secs);
    println!();

    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?.as_secs();
    // end_date_min = now + event_duration. The current event's end_date is
    // in [now, now + duration), so it's reliably excluded; the next event
    // (start ≥ current end, end = current end + duration) passes the
    // filter and is returned as the earliest-ascending match.
    let end_date_min_secs = now_secs + duration_secs;

    // CLI `hexbot split` also honors `gas_via_signer_wallet` from live
    // config — same policy surface as `hexbot redeem` and the live
    // maintenance thread.
    let gas_via_signer = read_gas_via_signer_wallet_flag();
    println!(
        "Gas payer: {}",
        if gas_via_signer {
            "signer EOA (direct on-chain)"
        } else {
            "Polymarket relayer (gasless)"
        }
    );
    // CLI doesn't need to track status — strategy is the consumer.
    // Safety floor = amount_usdc (no extra margin): operator invoking
    // `hexbot split` manually is intentional and shouldn't be blocked
    // by the strategy-side circuit breaker.
    // `hexbot split` is a manual operator action documented to redeem +
    // split, so it always redeems (redeem_enabled=true) — independent of
    // the live maintenance task's `maintenance_redeem_enabled` default-off.
    run_maintenance_blocking(MaintenanceJob {
        split_series_id: Some(series_id),
        split_end_date_min_secs: end_date_min_secs,
        split_amount_usdc: amount_usdc,
        min_safety_balance_usdc: amount_usdc,
        gas_via_signer,
        redeem_enabled: true,
        status: None,
        // CLI: empty account_id → resolve creds from the global POLY_*
        // env (set by `apply_account_to_env` for the `--account` flag).
        account_id: String::new(),
    });
    println!();
    println!("=== Maintenance complete ===");
    Ok(())
}


#[cfg(test)]
mod maintenance_status_tests {
    use super::*;

    #[test]
    fn offramp_calldata_routes_native_and_bridged_usdc() {
        const RECIPIENT: &str = "0x1111111111111111111111111111111111111111";
        for underlying in [USDC_ADDRESS, USDCE_ADDRESS] {
            let calldata = build_unwrap_calldata(underlying, RECIPIENT, 42_000_000);
            let bytes = hex::decode(calldata.trim_start_matches("0x")).unwrap();
            assert_eq!(&bytes[..4], &UNWRAP_SELECTOR);
            assert_eq!(&bytes[4..36], &address_to_bytes32(underlying));
            assert_eq!(&bytes[36..68], &address_to_bytes32(RECIPIENT));
            assert_eq!(&bytes[68..100], &u256_bytes(42_000_000));
        }
        assert_ne!(USDC_ADDRESS, USDCE_ADDRESS);
    }

    #[test]
    fn produced_seed_inventory_only_on_succeeded() {
        assert!(MaintenanceStatus::Succeeded.produced_seed_inventory());

        assert!(!MaintenanceStatus::NotStarted.produced_seed_inventory());
        assert!(!MaintenanceStatus::Running.produced_seed_inventory());
        assert!(!MaintenanceStatus::RedeemFailed { reason: "x".to_string() }
            .produced_seed_inventory());
        assert!(!MaintenanceStatus::SplitFailedOrPending { reason: "x".to_string() }
            .produced_seed_inventory());
        assert!(!MaintenanceStatus::Skipped { reason: "x".to_string() }
            .produced_seed_inventory());
    }

    #[test]
    fn labels_are_stable_for_log_filters() {
        // Tests rely on these exact strings appearing in [Maintenance]
        // log lines — bumping them silently would break ops grep.
        assert_eq!(MaintenanceStatus::NotStarted.label(), "NotStarted");
        assert_eq!(MaintenanceStatus::Running.label(), "Running");
        assert_eq!(MaintenanceStatus::Succeeded.label(), "Succeeded");
        assert_eq!(MaintenanceStatus::RedeemFailed { reason: String::new() }.label(),
            "RedeemFailed");
        assert_eq!(MaintenanceStatus::SplitFailedOrPending { reason: String::new() }.label(),
            "SplitFailedOrPending");
        assert_eq!(MaintenanceStatus::Skipped { reason: String::new() }.label(),
            "Skipped");
    }

    #[test]
    fn status_handle_is_thread_safe_and_starts_not_started() {
        let h = new_maintenance_status_handle();
        assert_eq!(*h.lock().unwrap(), MaintenanceStatus::NotStarted);

        // Simulate maintenance thread updating it.
        let h2 = h.clone();
        let t = std::thread::spawn(move || {
            *h2.lock().unwrap() = MaintenanceStatus::Running;
            std::thread::sleep(std::time::Duration::from_millis(5));
            *h2.lock().unwrap() = MaintenanceStatus::Succeeded;
        });
        t.join().unwrap();
        assert_eq!(*h.lock().unwrap(), MaintenanceStatus::Succeeded);
    }

    #[test]
    fn split_outcome_pending_or_failed_blocks_next_event() {
        // The strategy contract: anything other than Succeeded / NotStarted /
        // Skipped must NOT allow trading the next event (returns false from
        // produced_seed_inventory, and the strategy's allow_proceed match
        // excludes these variants).
        for blocking in [
            MaintenanceStatus::Running,
            MaintenanceStatus::RedeemFailed { reason: String::new() },
            MaintenanceStatus::SplitFailedOrPending { reason: String::new() },
        ] {
            assert!(!blocking.produced_seed_inventory(),
                "{:?} must NOT pass produced_seed_inventory check", blocking);
        }
    }
}
