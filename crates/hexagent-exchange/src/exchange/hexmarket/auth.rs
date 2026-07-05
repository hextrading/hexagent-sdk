//! HexMarket authentication: derive API credentials from wallet private key,
//! fetch via API, and cache locally.

use anyhow::{anyhow, Result};
use bip39::Mnemonic;
use ed25519_dalek::SigningKey;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use super::sdk::auth::{
    build_api_key_message, build_auth_message, build_auth_token, ed25519_sign, now_secs,
    pubkey_b58,
};
use super::sdk::ApiCredentials;

const DEFAULT_API_URL_PREFIX: &str = "https://apidev.hexmarket.xyz";

/// Cached API credentials stored on disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedCredentials {
    pubkey: String,
    api_key: String,
    secret: String,
    passphrase: String,
}

/// Response from `POST /auth/api-key`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateApiKeyResponse {
    api_key: String,
    secret: String,
    passphrase: String,
}

/// Parse a base58-encoded private key into an Ed25519 SigningKey.
///
/// Accepts either:
/// - 64-byte keypair (first 32 bytes = seed, last 32 bytes = public key) — Solana format
/// - 32-byte seed
pub fn parse_private_key(private_key_b58: &str) -> Result<SigningKey> {
    let bytes = bs58::decode(private_key_b58)
        .into_vec()
        .map_err(|e| anyhow!("Invalid base58 private key: {}", e))?;

    match bytes.len() {
        64 => {
            // Solana keypair format: [seed(32) | pubkey(32)]
            let seed: [u8; 32] = bytes[..32]
                .try_into()
                .map_err(|_| anyhow!("Invalid keypair bytes"))?;
            Ok(SigningKey::from_bytes(&seed))
        }
        32 => {
            let seed: [u8; 32] = bytes
                .try_into()
                .map_err(|_| anyhow!("Invalid seed bytes"))?;
            Ok(SigningKey::from_bytes(&seed))
        }
        n => Err(anyhow!(
            "Private key must be 32 or 64 bytes, got {} bytes",
            n
        )),
    }
}

/// Parse a BIP39 mnemonic phrase into an Ed25519 SigningKey.
///
/// Derives the seed from the mnemonic (no passphrase), then uses the first 32 bytes
/// as the Ed25519 seed — matching Solana's key derivation convention.
pub fn parse_mnemonic(mnemonic_phrase: &str) -> Result<SigningKey> {
    let mnemonic = Mnemonic::parse(mnemonic_phrase)
        .map_err(|e| anyhow!("Invalid mnemonic: {}", e))?;
    let seed_bytes = mnemonic.to_seed("");
    // Use first 32 bytes of the 64-byte seed as Ed25519 seed
    let seed: [u8; 32] = seed_bytes[..32]
        .try_into()
        .map_err(|_| anyhow!("Failed to derive seed from mnemonic"))?;
    Ok(SigningKey::from_bytes(&seed))
}

/// Resolve a SigningKey from either a private key or mnemonic.
/// Private key takes priority if both are provided.
pub fn resolve_signing_key(private_key: &str, mnemonic: &str) -> Result<SigningKey> {
    if !private_key.is_empty() {
        parse_private_key(private_key)
    } else if !mnemonic.is_empty() {
        parse_mnemonic(mnemonic)
    } else {
        Err(anyhow!("No private_key or mnemonic configured"))
    }
}

/// Path to the cached credentials file for a given pubkey.
fn cache_path(pubkey: &str) -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home)
        .join(".hexbot")
        .join(format!("hexmarket_creds_{}.json", pubkey))
}

/// Load cached credentials from disk.
fn load_cached_credentials(pubkey: &str) -> Option<CachedCredentials> {
    let path = cache_path(pubkey);
    let data = std::fs::read_to_string(&path).ok()?;
    let creds: CachedCredentials = serde_json::from_str(&data).ok()?;
    if creds.pubkey == pubkey {
        Some(creds)
    } else {
        None
    }
}

/// Save credentials to disk cache.
fn save_cached_credentials(creds: &CachedCredentials) -> Result<()> {
    let path = cache_path(&creds.pubkey);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let data = serde_json::to_string_pretty(creds)?;
    std::fs::write(&path, data)?;
    info!(
        "[Hexmarket] Credentials cached to {}",
        path.display()
    );
    Ok(())
}

/// Fetch API credentials from the server using L1 wallet auth.
///
/// Signs `"hexmarket:create_api_key\n{nonce}"` with the wallet keypair,
/// builds an auth token, and POSTs to `/auth/api-key`.
fn fetch_api_credentials(signing_key: &SigningKey, api_url_prefix: &str) -> Result<CreateApiKeyResponse> {
    let pubkey = pubkey_b58(signing_key);
    let timestamp = now_secs();
    let nonce: u32 = (timestamp % u32::MAX as u64) as u32;

    // Sign the API key creation message
    let api_key_msg = build_api_key_message(nonce);
    let api_key_sig = ed25519_sign(signing_key, &api_key_msg);

    // Build the L1 auth token for the Authorization header
    let auth_msg = build_auth_message(timestamp);
    let auth_sig = ed25519_sign(signing_key, &auth_msg);
    let auth_token = build_auth_token(&pubkey, timestamp, &auth_sig);

    let url = format!("{}/auth/api-key", api_url_prefix);
    info!("[Hexmarket] Creating API key for {}", pubkey);

    let body = serde_json::json!({
        "nonce": nonce,
        "signature": api_key_sig,
    }).to_string();

    // Route through the shared runtime + h1.1 Query pool (one-shot call).
    let client = crate::http1_pool::client(crate::http1_pool::Role::Query);
    let bearer = format!("Bearer {}", auth_token);
    let creds: CreateApiKeyResponse = crate::async_rt::block_on_runtime(async move {
        let resp = client.post(&url)
            .header("Authorization", bearer)
            .header("Content-Type", "application/json")
            .body(body)
            .send().await
            .map_err(|e| anyhow!("POST /auth/api-key failed: {}", e))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(anyhow!("POST /auth/api-key failed ({}): {}", status, text));
        }
        serde_json::from_str::<CreateApiKeyResponse>(&text)
            .map_err(|e| anyhow!("Invalid response from /auth/api-key: {} (body={})", e, text))
    })?;

    info!("[Hexmarket] API key created successfully");
    Ok(creds)
}

/// Return the API host, using default if empty.
pub fn api_url_prefix_or_default(api_url_prefix: &str) -> &str {
    if api_url_prefix.is_empty() { DEFAULT_API_URL_PREFIX } else { api_url_prefix }
}

/// Return the WS host, using default if empty.
pub fn wss_url_or_default(wss_host: &str) -> &str {
    const DEFAULT_WSS_URL: &str = "wss://apidev.hexmarket.xyz/ws";
    if wss_host.is_empty() { DEFAULT_WSS_URL } else { wss_host }
}

/// Resolved authentication context: signing key + API credentials.
pub struct HexAuth {
    pub signing_key: SigningKey,
    pub pubkey: String,
    pub credentials: ApiCredentials,
}

/// Resolve HexMarket authentication from a private key or mnemonic.
///
/// 1. Parse the private key or mnemonic to derive the signing key and public key
/// 2. Try to load cached API credentials from `~/.hexbot/hexmarket_creds_{pubkey}.json`
/// 3. If not cached, call `POST /auth/api-key` with L1 wallet auth to create them
/// 4. Cache the credentials for future use
pub fn resolve_auth(private_key: &str, mnemonic: &str, api_url_prefix: &str) -> Result<HexAuth> {
    let signing_key = resolve_signing_key(private_key, mnemonic)?;
    let pubkey = pubkey_b58(&signing_key);
    info!("[Hexmarket] Wallet pubkey: {}", pubkey);

    // Try cached credentials first
    if let Some(cached) = load_cached_credentials(&pubkey) {
        info!("[Hexmarket] Using cached API credentials");
        return Ok(HexAuth {
            signing_key,
            pubkey,
            credentials: ApiCredentials {
                api_key: cached.api_key,
                secret: cached.secret,
                passphrase: cached.passphrase,
            },
        });
    }

    // Fetch from API
    info!("[Hexmarket] No cached credentials, fetching from API...");
    let resp = fetch_api_credentials(&signing_key, api_url_prefix)?;

    // Cache to disk
    let cached = CachedCredentials {
        pubkey: pubkey.clone(),
        api_key: resp.api_key.clone(),
        secret: resp.secret.clone(),
        passphrase: resp.passphrase.clone(),
    };
    if let Err(e) = save_cached_credentials(&cached) {
        warn!("[Hexmarket] Failed to cache credentials: {}", e);
    }

    Ok(HexAuth {
        signing_key,
        pubkey,
        credentials: ApiCredentials {
            api_key: resp.api_key,
            secret: resp.secret,
            passphrase: resp.passphrase,
        },
    })
}
