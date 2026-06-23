//! Hexmarket L1 (wallet) + L2 (HMAC API-key) authentication primitives.
//!
//! Ported from the upstream `hexmarket_sdk_sync` crate — identical wire
//! format so cached credentials and server-side signatures still line up.

use anyhow::{anyhow, Result};
use base64::engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD};
use base64::Engine as _;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};

/// Stored API credentials returned by `POST /auth/api-key`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiCredentials {
    pub api_key: String,
    pub secret: String,
    pub passphrase: String,
}

/// L2 headers to attach to authenticated API requests.
#[derive(Debug, Clone)]
pub struct L2Headers {
    pub address: String,
    pub api_key: String,
    pub passphrase: String,
    pub timestamp: String,
    pub signature: String,
}

impl L2Headers {
    /// Returns the five HEX-* header name→value pairs for composing
    /// reqwest requests.
    pub fn as_pairs(&self) -> [(&'static str, &str); 5] {
        [
            ("HEX-ADDRESS", self.address.as_str()),
            ("HEX-API-KEY", self.api_key.as_str()),
            ("HEX-PASSPHRASE", self.passphrase.as_str()),
            ("HEX-TIMESTAMP", self.timestamp.as_str()),
            ("HEX-SIGNATURE", self.signature.as_str()),
        ]
    }
}

// ────────────────────────────────────────────────────────────
// L2 HMAC signing
// ────────────────────────────────────────────────────────────

/// Build the HMAC-SHA256 payload: `{timestamp}{METHOD}{path}[{body}]`.
pub fn build_l2_signing_payload(
    timestamp: u64,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> String {
    let mut payload = format!("{}{}{}", timestamp, method, path);
    if let Some(b) = body {
        payload.push_str(b);
    }
    payload
}

/// Sign a payload with the API secret using HMAC-SHA256.
/// Returns a base64url-encoded (no padding) signature.
pub fn sign_l2(secret_b64: &str, payload: &str) -> Result<String> {
    let secret_bytes = URL_SAFE_NO_PAD
        .decode(secret_b64)
        .or_else(|_| URL_SAFE.decode(secret_b64))
        .map_err(|_| anyhow!("Invalid API secret"))?;

    let mut mac = Hmac::<Sha256>::new_from_slice(&secret_bytes)
        .map_err(|_| anyhow!("Invalid API secret"))?;
    mac.update(payload.as_bytes());
    let result = mac.finalize();
    Ok(URL_SAFE_NO_PAD.encode(result.into_bytes()))
}

/// Build L2 authentication headers for an API request.
pub fn build_l2_headers(
    creds: &ApiCredentials,
    pubkey: &str,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> Result<L2Headers> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let payload = build_l2_signing_payload(timestamp, method, path, body);
    let signature = sign_l2(&creds.secret, &payload)?;
    Ok(L2Headers {
        address: pubkey.to_string(),
        api_key: creds.api_key.clone(),
        passphrase: creds.passphrase.clone(),
        timestamp: timestamp.to_string(),
        signature,
    })
}

// ────────────────────────────────────────────────────────────
// L1 wallet auth
// ────────────────────────────────────────────────────────────

const AUTH_MESSAGE_PREFIX: &str = "hexmarket:auth\n";

/// `hexmarket:auth\n{timestamp}` — signed for the Authorization bearer token.
pub fn build_auth_message(timestamp: u64) -> Vec<u8> {
    format!("{}{}", AUTH_MESSAGE_PREFIX, timestamp).into_bytes()
}

/// Build a signed auth token: `{pubkey}.{timestamp}.{signature_b58}`.
pub fn build_auth_token(pubkey: &str, timestamp: u64, signature_b58: &str) -> String {
    format!("{}.{}.{}", pubkey, timestamp, signature_b58)
}

/// Canonical order message for on-chain-settled L1 wallet signing.
/// Byte-for-byte identical to the server's `build_order_message`.
pub fn build_order_message(
    outcome_id: &str,
    side: &str,
    price: &str,
    quantity: u64,
    nonce: u64,
) -> Vec<u8> {
    format!(
        "hexmarket:place_order\noutcome_id:{}\nside:{}\nprice:{}\nquantity:{}\nnonce:{}",
        outcome_id, side, price, quantity, nonce
    )
    .into_bytes()
}

/// `hexmarket:create_api_key\n{nonce}` — signed to create API credentials.
pub fn build_api_key_message(nonce: u32) -> Vec<u8> {
    format!("hexmarket:create_api_key\n{}", nonce).into_bytes()
}

/// Current Unix timestamp in seconds.
pub fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

// ────────────────────────────────────────────────────────────
// Ed25519 signing helpers
// ────────────────────────────────────────────────────────────

/// Sign a message with an Ed25519 keypair, returning the base58 signature.
pub fn ed25519_sign(keypair: &ed25519_dalek::SigningKey, message: &[u8]) -> String {
    use ed25519_dalek::Signer;
    let sig = keypair.sign(message);
    bs58::encode(sig.to_bytes()).into_string()
}

/// Base58-encoded public key for the given signing key.
pub fn pubkey_b58(keypair: &ed25519_dalek::SigningKey) -> String {
    bs58::encode(keypair.verifying_key().to_bytes()).into_string()
}
