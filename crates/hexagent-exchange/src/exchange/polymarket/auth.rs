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
use arrayvec::ArrayString;
use std::fmt::Write as _;

type HmacSha256 = Hmac<Sha256>;

/// Polymarket L2 authentication credentials.
#[derive(Clone)]
pub struct PolyAuth {
    pub api_key: String,
    signing_template: HmacSha256, // startup-keyed state; cloned per request
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
    pub signature: ArrayString<44>,
    pub timestamp: ArrayString<20>,
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
            signing_template: HmacSha256::new_from_slice(&secret).expect("HMAC accepts any key size"),
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
        let mut timestamp = ArrayString::<20>::new();
        write!(&mut timestamp, "{timestamp_secs}").expect("u64 fits in 20 decimal bytes");
        let mut mac = self.signing_template.clone();
        mac.update(timestamp.as_bytes());
        mac.update(method.as_bytes());
        mac.update(path.as_bytes());
        if !body.is_empty() {
            mac.update(body.as_bytes());
        }
        let mut encoded = [0_u8; 44];
        let len = B64_URL.encode_slice(mac.finalize().into_bytes(), &mut encoded)
            .expect("SHA256 base64 fits in 44 bytes");
        let signature = ArrayString::from(std::str::from_utf8(&encoded[..len]).expect("base64 ASCII"))
            .expect("fixed signature capacity");

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

    fn legacy_signature(secret: &[u8], method: &str, path: &str, body: &str, timestamp: u64) -> (String, String) {
        let timestamp = timestamp.to_string();
        let mut mac = HmacSha256::new_from_slice(secret).unwrap();
        mac.update(timestamp.as_bytes()); mac.update(method.as_bytes()); mac.update(path.as_bytes());
        mac.update(body.as_bytes());
        (timestamp, B64_URL.encode(mac.finalize().into_bytes()))
    }

    #[test]
    fn inline_auth_matches_legacy_bytes_for_all_methods_timestamps_and_utf8_bodies() {
        for secret in [vec![], b"secret".to_vec(), vec![0xab; 128]] {
            let auth = PolyAuth::new("key", &B64_URL.encode(&secret), "pass", "wallet").unwrap();
            for timestamp in [0, 1_700_000_000, u64::MAX] {
                for (method, path, body) in [("GET", "/data/orders", ""), ("POST", "/order", "{\"text\":\"测试\\n\"}"), ("DELETE", "/order", "{\"orderID\":\"0xabc\"}")] {
                    let expected = legacy_signature(&secret, method, path, body, timestamp);
                    let actual = auth.sign_request_at(method, path, body, timestamp);
                    assert_eq!(actual.timestamp.as_str(), expected.0);
                    assert_eq!(actual.signature.as_str(), expected.1);
                    assert_eq!(actual.signature.len(), 44);
                }
            }
        }
    }

    #[test]
    #[ignore = "focused release HMAC benchmark; no network"]
    fn residual_auth_benchmark() {
        let secret = b"deterministic-offline-benchmark-secret";
        let auth = PolyAuth::new("key", &B64_URL.encode(secret), "pass", "wallet").unwrap();
        let body = "x".repeat(1024);
        for inline in [false, true] {
            let mut samples = Vec::with_capacity(100_000);
            for i in 0..101_000 {
                let start = std::time::Instant::now();
                if inline { std::hint::black_box(auth.sign_request_at("POST", "/order", &body, 1_790_000_000)); }
                else { std::hint::black_box(legacy_signature(secret, "POST", "/order", &body, 1_790_000_000)); }
                if i >= 1000 { samples.push(start.elapsed().as_nanos() as u64); }
            }
            samples.sort_unstable();
            let n = samples.len();
            eprintln!("residual_auth mode={} n={} median_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth=0 overflow=0 boundary=timestamp_hmac_base64 body_bytes={}",
                if inline { "inline" } else { "legacy" }, n, (samples[n/2-1]+samples[n/2])/2,
                samples[(n*99).div_ceil(100)-1], samples[(n*999).div_ceil(1000)-1], samples[n-1], body.len());
        }
    }

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
        assert_eq!(first.timestamp.as_str(), "1700000000");
        assert_eq!(first.signature, second.signature);
    }
}
