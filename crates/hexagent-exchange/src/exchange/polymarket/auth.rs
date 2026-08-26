//! Polymarket CLOB L2 Authentication — HMAC-SHA256 request signing.
//!
//! Every CLOB REST request requires 5 headers:
//! - POLY_API_KEY, POLY_ADDRESS, POLY_SIGNATURE, POLY_TIMESTAMP, POLY_PASSPHRASE
//!
//! Signature: base64(HMAC-SHA256(base64_decode(secret), timestamp + method + path [+ body]))

use anyhow::{anyhow, Result};
use base64::engine::general_purpose::{STANDARD as B64_STD, URL_SAFE as B64_URL, URL_SAFE_NO_PAD as B64_URL_NOPAD};
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::sync::Arc;

type HmacSha256 = Hmac<Sha256>;

/// Polymarket L2 authentication credentials.
#[derive(Clone)]
pub struct PolyAuth {
    pub api_key: String,
    secret: Vec<u8>, // base64-decoded HMAC secret
    /// Original base64-encoded secret as supplied by the operator,
    /// preserved so per-instance user_feed WS handshakes can re-sign
    /// without needing the raw bytes routed separately through engine
    /// plumbing. Phase 2b multi-instance work.
    api_secret_b64: String,
    pub passphrase: String,
    pub wallet_address: String,
    api_key_template: Arc<str>,
    address_template: Arc<str>,
    passphrase_template: Arc<str>,
}

/// Authentication headers for a single request.
#[derive(Clone)]
pub struct AuthHeaders {
    pub api_key: Arc<str>,
    pub address: Arc<str>,
    pub signature: String,
    pub timestamp: String,
    pub passphrase: Arc<str>,
}

impl PolyAuth {
    /// Create from raw credentials.
    /// `api_secret_b64` is the base64-encoded HMAC secret from Polymarket.
    pub fn new(
        api_key: &str,
        api_secret_b64: &str,
        passphrase: &str,
        wallet_address: &str,
    ) -> Result<Self> {
        // Try standard base64 first, then URL-safe variants (Polymarket uses URL-safe)
        let secret = B64_STD.decode(api_secret_b64)
            .or_else(|_| B64_URL.decode(api_secret_b64))
            .or_else(|_| B64_URL_NOPAD.decode(api_secret_b64))
            .map_err(|e| anyhow!("Failed to base64-decode API secret: {}", e))?;
        Ok(Self {
            api_key: api_key.to_string(),
            secret,
            api_secret_b64: api_secret_b64.to_string(),
            passphrase: passphrase.to_string(),
            wallet_address: wallet_address.to_string(),
            api_key_template: Arc::from(api_key),
            address_template: Arc::from(wallet_address),
            passphrase_template: Arc::from(passphrase),
        })
    }

    /// Read-only accessor for the operator-supplied base64 HMAC secret.
    /// Used by the per-instance user_feed spawner to re-sign the
    /// authenticated WebSocket handshake without re-plumbing creds
    /// through engine config. Returns the original string verbatim
    /// (URL-safe or standard variants both preserved).
    #[inline]
    pub fn api_secret_b64(&self) -> &str {
        &self.api_secret_b64
    }

    /// Sign a request and return the authentication headers.
    ///
    /// - `method`: uppercase HTTP method ("GET", "POST", "DELETE")
    /// - `path`: URL path with leading slash (e.g. "/order", "/data/orders")
    /// - `body`: request body string (empty for GET/DELETE without body)
    pub fn sign_request(&self, method: &str, path: &str, body: &str) -> AuthHeaders {
        let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
        self.sign_request_at(method, path, body, timestamp)
    }

    /// Sign with an explicit exchange timestamp. Credential diagnostics use
    /// public CLOB server time to distinguish key/account binding failures
    /// from host clock skew.
    pub fn sign_request_at(
        &self,
        method: &str,
        path: &str,
        body: &str,
        timestamp_secs: u64,
    ) -> AuthHeaders {
        let timestamp = timestamp_secs.to_string();

        let mut mac = HmacSha256::new_from_slice(&self.secret)
            .expect("HMAC accepts any key size");
        mac.update(timestamp.as_bytes());
        mac.update(method.as_bytes());
        mac.update(path.as_bytes());
        if !body.is_empty() {
            mac.update(body.as_bytes());
        }
        let signature = B64_URL.encode(mac.finalize().into_bytes());

        AuthHeaders {
            api_key: Arc::clone(&self.api_key_template),
            address: Arc::clone(&self.address_template),
            signature,
            timestamp,
            passphrase: Arc::clone(&self.passphrase_template),
        }
    }
}

impl AuthHeaders {
    /// Returns the user-auth header name→value pairs for composing
    /// reqwest requests (async HTTP/2 path).
    pub fn as_pairs(&self) -> [(&'static str, &str); 5] {
        [
            ("POLY_API_KEY", self.api_key.as_ref()),
            ("POLY_ADDRESS", self.address.as_ref()),
            ("POLY_SIGNATURE", self.signature.as_str()),
            ("POLY_TIMESTAMP", self.timestamp.as_str()),
            ("POLY_PASSPHRASE", self.passphrase.as_ref()),
        ]
    }

    /// Returns the builder-auth header name→value pairs.
    pub fn as_builder_pairs(&self) -> [(&'static str, &str); 4] {
        [
            ("POLY_BUILDER_API_KEY", self.api_key.as_ref()),
            ("POLY_BUILDER_SIGNATURE", self.signature.as_str()),
            ("POLY_BUILDER_TIMESTAMP", self.timestamp.as_str()),
            ("POLY_BUILDER_PASSPHRASE", self.passphrase.as_ref()),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sign_request_format() {
        // Verify that signing produces a non-empty base64 string
        let auth = PolyAuth::new(
            "test-key",
            &B64_STD.encode(b"test-secret"),
            "test-pass",
            "0x1234",
        ).unwrap();
        let headers = auth.sign_request("GET", "/order/0xabc", "");
        assert!(!headers.signature.is_empty());
        assert_eq!(headers.api_key.as_ref(), "test-key");
        assert_eq!(headers.address.as_ref(), "0x1234");
        assert_eq!(headers.passphrase.as_ref(), "test-pass");
        assert!(!headers.timestamp.is_empty());
    }

    #[test]
    fn explicit_timestamp_signing_is_stable() {
        let auth = PolyAuth::new("key", "c2VjcmV0", "pass", "0xabc").unwrap();
        let first = auth.sign_request_at("GET", "/auth/api-keys", "", 1_700_000_000);
        let second = auth.sign_request_at("GET", "/auth/api-keys", "", 1_700_000_000);
        assert_eq!(first.timestamp, "1700000000");
        assert_eq!(first.signature, second.signature);
    }
}
