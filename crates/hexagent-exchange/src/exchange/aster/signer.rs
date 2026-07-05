//! Aster **V3** request signing.
//!
//! Authoritative reference: the demo in `asterdex/api-docs`
//! (`V3(Recommended)/EN/aster-finance-futures-api-v3.md`). The scheme:
//!
//!   1. append `nonce` (current time in **microseconds**, ±10 s window) and
//!      `signer` (API agent wallet address) to the request params;
//!   2. urlencode the params in **insertion order** (python
//!      `urllib.parse.urlencode` semantics: `quote_plus`, i.e. space → `+`,
//!      unreserved = `[A-Za-z0-9_.\-~]`, everything else `%XX` uppercase);
//!   3. EIP-712-sign that string as `Message(string msg)` under the domain
//!      `AsterSignTransaction` / version `1` / chainId 1666 (mainnet) or
//!      714 (testnet) / verifyingContract `0x0…0`;
//!   4. append `&signature=0x<r||s||v hex>` (v = recid + 27) and send the
//!      whole thing as the query string (empty body).
//!
//! The KATs in the test module were generated with `eth_account` 0.13.7
//! (`Account.sign_message(encode_typed_data(...))`) — the same library the
//! official demo uses — so they pin both the urlencoding and the full
//! EIP-712 pipeline.

use anyhow::{anyhow, Result};
use k256::ecdsa::SigningKey;
use sha3::{Digest, Keccak256};

use super::auth::AsterAuth;

// ════════════════════════════════════════════════════════════════
// EIP-712 Message(string msg) under AsterSignTransaction
// ════════════════════════════════════════════════════════════════

fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(data);
    hasher.finalize().into()
}

fn u256_be(v: u128) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[16..].copy_from_slice(&v.to_be_bytes());
    out
}

/// EIP-712 digest of `Message(string msg)` under the Aster domain.
fn message_eip712_digest(msg: &str, chain_id: u64) -> [u8; 32] {
    let domain_separator = {
        let type_hash = keccak256(
            b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
        );
        let name_hash = keccak256(b"AsterSignTransaction");
        let version_hash = keccak256(b"1");
        let chain = u256_be(chain_id as u128);
        let contract = [0u8; 32]; // 0x0000…0000
        let mut buf = Vec::with_capacity(5 * 32);
        buf.extend_from_slice(&type_hash);
        buf.extend_from_slice(&name_hash);
        buf.extend_from_slice(&version_hash);
        buf.extend_from_slice(&chain);
        buf.extend_from_slice(&contract);
        keccak256(&buf)
    };

    let struct_hash = {
        let type_hash = keccak256(b"Message(string msg)");
        let msg_hash = keccak256(msg.as_bytes());
        let mut buf = Vec::with_capacity(2 * 32);
        buf.extend_from_slice(&type_hash);
        buf.extend_from_slice(&msg_hash);
        keccak256(&buf)
    };

    let mut buf = Vec::with_capacity(2 + 32 + 32);
    buf.push(0x19);
    buf.push(0x01);
    buf.extend_from_slice(&domain_separator);
    buf.extend_from_slice(&struct_hash);
    keccak256(&buf)
}

/// Sign `msg` (the urlencoded param string) → `0x` + 130-hex `r||s||v`
/// (v = recid + 27), the wire form `&signature=` expects.
pub fn sign_message(key: &SigningKey, msg: &str, chain_id: u64) -> Result<String> {
    let digest = message_eip712_digest(msg, chain_id);
    let (sig, recid) = key
        .sign_prehash_recoverable(&digest)
        .map_err(|e| anyhow!("sign_prehash: {}", e))?;
    let bytes = sig.to_bytes(); // 64 bytes: r(32) || s(32)
    Ok(format!("0x{}{:02x}", hex::encode(bytes), recid.to_byte() + 27))
}

// ════════════════════════════════════════════════════════════════
// urlencoding (python urllib.parse.urlencode / quote_plus semantics)
// ════════════════════════════════════════════════════════════════

/// Percent-encode one component: unreserved `[A-Za-z0-9_.\-~]` pass through,
/// space → `+`, everything else `%XX` (uppercase hex). Matches python's
/// `quote_plus` — the signature is computed over this exact string, so the
/// encoding must be byte-identical to what the server verifies.
fn quote_plus(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

/// `urlencode` a param list in insertion order: `k1=v1&k2=v2&…`.
pub fn urlencode(params: &[(&str, String)]) -> String {
    params
        .iter()
        .map(|(k, v)| format!("{}={}", quote_plus(k), quote_plus(v)))
        .collect::<Vec<_>>()
        .join("&")
}

// ════════════════════════════════════════════════════════════════
// nonce (microseconds, strictly increasing per process)
// ════════════════════════════════════════════════════════════════

/// Current time in microseconds, strictly increasing across calls in this
/// process (the server rejects reused nonces; ±10 s validity window).
pub fn next_nonce_micros() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static LAST: AtomicU64 = AtomicU64::new(0);
    let now = crate::types::now_ns() / 1_000;
    LAST.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |prev| {
        Some(now.max(prev + 1))
    })
    .map(|prev| now.max(prev + 1))
    .unwrap_or(now)
}

/// Build the fully-signed query string for a V3 request: appends `nonce` +
/// `signer` to `params`, urlencodes in order, signs, and appends
/// `&signature=…`. Send as the query string (or form body) verbatim.
pub fn signed_query(auth: &AsterAuth, mut params: Vec<(&str, String)>) -> Result<String> {
    params.push(("nonce", next_nonce_micros().to_string()));
    params.push(("signer", auth.signer_address.clone()));
    let encoded = urlencode(&params);
    let sig = sign_message(&auth.key, &encoded, auth.network.chain_id())?;
    Ok(format!("{}&signature={}", encoded, sig))
}

// ════════════════════════════════════════════════════════════════
// Known-answer tests (generated with eth_account 0.13.7)
// ════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::super::hyperliquid::signer::{derive_eth_address, parse_signing_key};

    const TEST_KEY: &str =
        "0x0123456789012345678901234567890123456789012345678901234567890123";
    const TEST_SIGNER: &str = "0x14791697260e4c9a71f18484c9f997b308e59325";

    #[test]
    fn signer_address_derivation() {
        let key = parse_signing_key(TEST_KEY).unwrap();
        assert_eq!(derive_eth_address(&key), TEST_SIGNER);
    }

    /// KAT 1: realistic order params, mainnet chainId 1666.
    #[test]
    fn kat_order_params_mainnet() {
        let key = parse_signing_key(TEST_KEY).unwrap();
        let params: Vec<(&str, String)> = vec![
            ("symbol", "BTCUSDT".into()),
            ("type", "LIMIT".into()),
            ("side", "BUY".into()),
            ("timeInForce", "GTX".into()),
            ("quantity", "0.001".into()),
            ("price", "50000.0".into()),
            ("newClientOrderId", "550e8400-e29b-41d4-a716-446655440000".into()),
            ("nonce", "1748310859508867".into()),
            ("signer", TEST_SIGNER.into()),
        ];
        let msg = urlencode(&params);
        assert_eq!(
            msg,
            "symbol=BTCUSDT&type=LIMIT&side=BUY&timeInForce=GTX&quantity=0.001&price=50000.0&newClientOrderId=550e8400-e29b-41d4-a716-446655440000&nonce=1748310859508867&signer=0x14791697260e4c9a71f18484c9f997b308e59325"
        );
        let sig = sign_message(&key, &msg, 1666).unwrap();
        assert_eq!(
            sig,
            "0xfc53fb0e8877eab93669d4ca08415fa10db6e39aee717cb4ae7836c5a467c1ba54d527659dc32d6ab5a2dca649cc1ff6a2dbec04dccde6e8b33072208687f0af1b"
        );
    }

    /// KAT 2: listenKey-style empty params, testnet chainId 714.
    #[test]
    fn kat_listenkey_params_testnet() {
        let key = parse_signing_key(TEST_KEY).unwrap();
        let msg = urlencode(&[
            ("nonce", "1748310859508867".to_string()),
            ("signer", TEST_SIGNER.to_string()),
        ]);
        let sig = sign_message(&key, &msg, 714).unwrap();
        assert_eq!(
            sig,
            "0x9e1a50202b55540461f3385c2a543f9f330fda5ad3c7b4a1068f063f8e162b2f13f623f4f840470a06b183e1487a27726c84c495108c44f873da70f4bbb204e01c"
        );
    }

    /// KAT 3: params needing percent-escapes (batchOrders JSON).
    #[test]
    fn kat_escaped_params() {
        let key = parse_signing_key(TEST_KEY).unwrap();
        let params: Vec<(&str, String)> = vec![
            ("batchOrders", r#"[{"symbol":"BTCUSDT","price":"50000.0"}]"#.into()),
            ("nonce", "1".into()),
            ("signer", TEST_SIGNER.into()),
        ];
        let msg = urlencode(&params);
        assert_eq!(
            msg,
            "batchOrders=%5B%7B%22symbol%22%3A%22BTCUSDT%22%2C%22price%22%3A%2250000.0%22%7D%5D&nonce=1&signer=0x14791697260e4c9a71f18484c9f997b308e59325"
        );
        let sig = sign_message(&key, &msg, 1666).unwrap();
        assert_eq!(
            sig,
            "0x5b1412482a15cb6e2492264268fc85130c77976d17755f51f35f8949189fddb8550652f22c67ccfaa7513e3b39bb5a15d41f240fe99014aea49eb30c24fb7e8d1c"
        );
    }

    #[test]
    fn nonce_strictly_increasing() {
        let a = next_nonce_micros();
        let b = next_nonce_micros();
        let c = next_nonce_micros();
        assert!(b > a && c > b);
    }
}
