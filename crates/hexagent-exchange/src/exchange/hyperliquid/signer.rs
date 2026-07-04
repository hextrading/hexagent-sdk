//! Hyperliquid **L1 action** signing (orders / cancels / modifies).
//!
//! Authoritative reference: `hyperliquid-python-sdk`
//! (`hyperliquid/utils/signing.py`) and `hyperliquid-rust-sdk`
//! (`src/signature/`). The scheme is a "phantom agent" EIP-712 signature:
//!
//!   1. **action hash (connectionId)** — msgpack-encode the action struct
//!      (named map, field order = declaration order), then append:
//!        `nonce` as 8 bytes big-endian,
//!        then `0x00` (no vault) OR `0x01 ++ vault_address(20B)`,
//!        then, iff `expires_after` is set, `0x00 ++ expires_after(8B BE)`.
//!      `connectionId = keccak256(that buffer)`.
//!   2. **phantom agent** — EIP-712 struct `Agent(string source, bytes32
//!      connectionId)` where `source = "a"` (mainnet) / `"b"` (testnet).
//!   3. **domain** — `Exchange` / version `1` / chainId **1337** /
//!      verifyingContract `0x0…0`.
//!   4. sign `keccak256(0x1901 ++ domainSeparator ++ agentStructHash)` with
//!      the agent (or EOA) secp256k1 key → `{r, s, v}` with `v = recid + 27`.
//!
//! The KATs in the test module are copied verbatim from the python SDK's
//! `signing_test.py`; they pin the msgpack byte order + the full signing
//! pipeline. Do NOT change field declaration order in the wire structs
//! without re-checking those vectors — msgpack is order-sensitive and the
//! HL server re-hashes the action to verify the signature.

use anyhow::{anyhow, Result};
use k256::ecdsa::SigningKey;
use serde::Serialize;
use sha3::{Digest, Keccak256};

// ════════════════════════════════════════════════════════════════
// Wire structs (serialize order MUST match the python SDK)
// ════════════════════════════════════════════════════════════════

/// `{"limit": {"tif": "Gtc"|"Alo"|"Ioc"}}`
#[derive(Serialize, Clone, Debug)]
pub struct LimitWire {
    pub tif: String,
}

#[derive(Serialize, Clone, Debug)]
pub struct OrderTypeWire {
    pub limit: LimitWire,
}

/// One order in the `order` action. Field order: a, b, p, s, r, t, [c].
#[derive(Serialize, Clone, Debug)]
pub struct OrderWire {
    /// asset index (perp: index into `meta.universe`)
    pub a: u32,
    /// is_buy
    pub b: bool,
    /// price (normalized decimal string)
    pub p: String,
    /// size (normalized decimal string)
    pub s: String,
    /// reduce_only
    pub r: bool,
    /// order type
    pub t: OrderTypeWire,
    /// client order id (0x + 32 hex), optional
    #[serde(skip_serializing_if = "Option::is_none")]
    pub c: Option<String>,
}

/// `{"type":"order","orders":[...],"grouping":"na"}` (+ optional builder)
#[derive(Serialize, Clone, Debug)]
pub struct OrderAction {
    #[serde(rename = "type")]
    pub ty: String,
    pub orders: Vec<OrderWire>,
    pub grouping: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub builder: Option<BuilderWire>,
}

#[derive(Serialize, Clone, Debug)]
pub struct BuilderWire {
    /// builder address (lowercased 0x-hex)
    pub b: String,
    /// fee in tenths of a basis point
    pub f: u64,
}

/// Cancel-by-oid entry: `{"a": asset, "o": oid}`
#[derive(Serialize, Clone, Debug)]
pub struct CancelWire {
    pub a: u32,
    pub o: u64,
}

#[derive(Serialize, Clone, Debug)]
pub struct CancelAction {
    #[serde(rename = "type")]
    pub ty: String,
    pub cancels: Vec<CancelWire>,
}

/// Cancel-by-cloid entry: `{"asset": asset, "cloid": "0x…"}`
#[derive(Serialize, Clone, Debug)]
pub struct CancelCloidWire {
    pub asset: u32,
    pub cloid: String,
}

#[derive(Serialize, Clone, Debug)]
pub struct CancelByCloidAction {
    #[serde(rename = "type")]
    pub ty: String,
    pub cancels: Vec<CancelCloidWire>,
}

/// EIP-712 `{r,s,v}` signature as sent in the request body.
#[derive(Serialize, Clone, Debug, PartialEq, Eq)]
pub struct Signature {
    pub r: String,
    pub s: String,
    pub v: u8,
}

// ════════════════════════════════════════════════════════════════
// Signing
// ════════════════════════════════════════════════════════════════

/// Sign an L1 action (order/cancel/modify) with the phantom-agent scheme.
///
/// * `action` — any wire struct above; serialized via `to_vec_named`.
/// * `vault` — vault/subaccount address to act on behalf of, else `None`.
/// * `nonce` — request nonce (live: ms since epoch).
/// * `expires_after` — optional action-expiry (ms since epoch).
/// * `is_mainnet` — selects the phantom-agent `source` ("a"/"b").
pub fn sign_l1_action<T: Serialize>(
    key: &SigningKey,
    action: &T,
    vault: Option<&str>,
    nonce: u64,
    expires_after: Option<u64>,
    is_mainnet: bool,
) -> Result<Signature> {
    let connection_id = action_hash(action, vault, nonce, expires_after)?;
    let source = if is_mainnet { "a" } else { "b" };
    let digest = agent_eip712_digest(source, &connection_id);
    sign_digest(key, &digest)
}

/// `keccak256( msgpack(action) ++ nonce_be8 ++ vault_marker ++ [expires] )`.
pub fn action_hash<T: Serialize>(
    action: &T,
    vault: Option<&str>,
    nonce: u64,
    expires_after: Option<u64>,
) -> Result<[u8; 32]> {
    let mut data = rmp_serde::to_vec_named(action)
        .map_err(|e| anyhow!("msgpack action: {}", e))?;
    data.extend_from_slice(&nonce.to_be_bytes());
    match vault {
        None => data.push(0x00),
        Some(addr) => {
            data.push(0x01);
            data.extend_from_slice(&address_to_bytes(addr)?);
        }
    }
    if let Some(exp) = expires_after {
        data.push(0x00);
        data.extend_from_slice(&exp.to_be_bytes());
    }
    Ok(keccak256(&data))
}

/// EIP-712 digest of `Agent(string source, bytes32 connectionId)` under the
/// Hyperliquid `Exchange` domain.
fn agent_eip712_digest(source: &str, connection_id: &[u8; 32]) -> [u8; 32] {
    let domain_separator = {
        let type_hash = keccak256(
            b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
        );
        let name_hash = keccak256(b"Exchange");
        let version_hash = keccak256(b"1");
        let chain_id = u256_be(1337);
        let contract = [0u8; 32]; // 0x0000…0000
        let mut buf = Vec::with_capacity(5 * 32);
        buf.extend_from_slice(&type_hash);
        buf.extend_from_slice(&name_hash);
        buf.extend_from_slice(&version_hash);
        buf.extend_from_slice(&chain_id);
        buf.extend_from_slice(&contract);
        keccak256(&buf)
    };

    let struct_hash = {
        let type_hash = keccak256(b"Agent(string source,bytes32 connectionId)");
        let source_hash = keccak256(source.as_bytes());
        let mut buf = Vec::with_capacity(3 * 32);
        buf.extend_from_slice(&type_hash);
        buf.extend_from_slice(&source_hash);
        buf.extend_from_slice(connection_id);
        keccak256(&buf)
    };

    let mut buf = Vec::with_capacity(2 + 32 + 32);
    buf.push(0x19);
    buf.push(0x01);
    buf.extend_from_slice(&domain_separator);
    buf.extend_from_slice(&struct_hash);
    keccak256(&buf)
}

/// secp256k1 sign of a 32-byte prehash → `{r,s,v}` (v = recid + 27).
fn sign_digest(key: &SigningKey, digest: &[u8; 32]) -> Result<Signature> {
    let (sig, recid) = key
        .sign_prehash_recoverable(digest)
        .map_err(|e| anyhow!("sign_prehash: {}", e))?;
    let bytes = sig.to_bytes(); // 64 bytes: r(32) || s(32)
    let r = format!("0x{}", trim_leading_zeros_hex(&hex::encode(&bytes[..32])));
    let s = format!("0x{}", trim_leading_zeros_hex(&hex::encode(&bytes[32..])));
    Ok(Signature { r, s, v: recid.to_byte() + 27 })
}

// ════════════════════════════════════════════════════════════════
// Number / string helpers
// ════════════════════════════════════════════════════════════════

/// Normalize an f64 to Hyperliquid wire form: fixed to 8 decimals then strip
/// trailing zeros (and a bare trailing `.`). Mirrors the python SDK's
/// `float_to_wire`. Integers render without a decimal point ("100", not
/// "100.0").
pub fn float_to_wire(x: f64) -> String {
    let s = format!("{:.8}", x);
    let trimmed = if s.contains('.') {
        s.trim_end_matches('0').trim_end_matches('.').to_string()
    } else {
        s
    };
    if trimmed == "-0" { "0".to_string() } else { trimmed }
}

/// Left-trim zero nibbles from a hex string (the HL SDK emits `r`/`s` with no
/// leading zeros, e.g. `0x53749d…` not `0x0053749d…`). Never empties to "".
fn trim_leading_zeros_hex(h: &str) -> String {
    let t = h.trim_start_matches('0');
    if t.is_empty() { "0".to_string() } else { t.to_string() }
}

fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// 20-byte address from a `0x`-hex string.
fn address_to_bytes(addr: &str) -> Result<[u8; 20]> {
    let clean = addr.strip_prefix("0x").unwrap_or(addr);
    let bytes = hex::decode(clean).map_err(|e| anyhow!("bad address hex: {}", e))?;
    if bytes.len() != 20 {
        return Err(anyhow!("address must be 20 bytes, got {}", bytes.len()));
    }
    let mut out = [0u8; 20];
    out.copy_from_slice(&bytes);
    Ok(out)
}

/// Big-endian 32-byte encoding of a u128 (for uint256 domain fields).
fn u256_be(v: u128) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[16..].copy_from_slice(&v.to_be_bytes());
    out
}

/// Derive the lowercased 0x Ethereum address from a signing key. Used to log
/// the agent address and to default `account_address` for EOA signing.
pub fn derive_eth_address(key: &SigningKey) -> String {
    use k256::ecdsa::VerifyingKey;
    let vk = VerifyingKey::from(key);
    let point = vk.to_encoded_point(false);
    let hash = keccak256(&point.as_bytes()[1..]); // skip 0x04 prefix
    format!("0x{}", hex::encode(&hash[12..]))
}

/// Parse a `0x`-hex private key (32 bytes) into a `SigningKey`.
pub fn parse_signing_key(private_key_hex: &str) -> Result<SigningKey> {
    let clean = private_key_hex.strip_prefix("0x").unwrap_or(private_key_hex);
    let bytes = hex::decode(clean).map_err(|e| anyhow!("bad private key hex: {}", e))?;
    if bytes.len() != 32 {
        return Err(anyhow!("private key must be 32 bytes, got {}", bytes.len()));
    }
    SigningKey::from_bytes(bytes.as_slice().into())
        .map_err(|e| anyhow!("invalid private key: {}", e))
}

// ════════════════════════════════════════════════════════════════
// Known-answer tests (verbatim from hyperliquid-python-sdk signing_test.py)
// ════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    const TEST_KEY: &str =
        "0x0123456789012345678901234567890123456789012345678901234567890123";

    #[derive(Serialize)]
    struct DummyAction {
        #[serde(rename = "type")]
        ty: String,
        num: u64,
    }

    fn dummy() -> DummyAction {
        // float_to_int_for_hashing(1000) = round(1000 * 1e8) = 100_000_000_000
        DummyAction { ty: "dummy".to_string(), num: 100_000_000_000 }
    }

    #[test]
    fn kat_dummy_action_no_vault() {
        let key = parse_signing_key(TEST_KEY).unwrap();
        let main = sign_l1_action(&key, &dummy(), None, 0, None, true).unwrap();
        assert_eq!(
            main.r,
            "0x53749d5b30552aeb2fca34b530185976545bb22d0b3ce6f62e31be961a59298"
        );
        assert_eq!(
            main.s,
            "0x755c40ba9bf05223521753995abb2f73ab3229be8ec921f350cb447e384d8ed8"
        );
        assert_eq!(main.v, 27);

        let test = sign_l1_action(&key, &dummy(), None, 0, None, false).unwrap();
        assert_eq!(
            test.r,
            "0x542af61ef1f429707e3c76c5293c80d01f74ef853e34b76efffcb57e574f9510"
        );
        assert_eq!(
            test.s,
            "0x17b8b32f086e8cdede991f1e2c529f5dd5297cbe8128500e00cbaf766204a613"
        );
        assert_eq!(test.v, 28);
    }

    fn order_action() -> OrderAction {
        // coin=ETH → asset index 1; is_buy, sz=100, px=100, Gtc, cloid=…0001
        OrderAction {
            ty: "order".to_string(),
            orders: vec![OrderWire {
                a: 1,
                b: true,
                p: float_to_wire(100.0),
                s: float_to_wire(100.0),
                r: false,
                t: OrderTypeWire { limit: LimitWire { tif: "Gtc".to_string() } },
                c: Some("0x00000000000000000000000000000001".to_string()),
            }],
            grouping: "na".to_string(),
            builder: None,
        }
    }

    #[test]
    fn kat_order_action_with_cloid() {
        let key = parse_signing_key(TEST_KEY).unwrap();
        let main = sign_l1_action(&key, &order_action(), None, 0, None, true).unwrap();
        assert_eq!(
            main.r,
            "0x41ae18e8239a56cacbc5dad94d45d0b747e5da11ad564077fcac71277a946e3"
        );
        assert_eq!(
            main.s,
            "0x3c61f667e747404fe7eea8f90ab0e76cc12ce60270438b2058324681a00116da"
        );
        assert_eq!(main.v, 27);

        let test = sign_l1_action(&key, &order_action(), None, 0, None, false).unwrap();
        assert_eq!(
            test.r,
            "0xeba0664bed2676fc4e5a743bf89e5c7501aa6d870bdb9446e122c9466c5cd16d"
        );
        assert_eq!(
            test.s,
            "0x7f3e74825c9114bc59086f1eebea2928c190fdfbfde144827cb02b85bbe90988"
        );
        assert_eq!(test.v, 28);
    }

    #[test]
    fn kat_dummy_action_with_vault() {
        let key = parse_signing_key(TEST_KEY).unwrap();
        let vault = "0x1719884eb866cb12b2287399b15f7db5e7d775ea";
        let main = sign_l1_action(&key, &dummy(), Some(vault), 0, None, true).unwrap();
        assert_eq!(
            main.r,
            "0x3c548db75e479f8012acf3000ca3a6b05606bc2ec0c29c50c515066a326239"
        );
        assert_eq!(
            main.s,
            "0x4d402be7396ce74fbba3795769cda45aec00dc3125a984f2a9f23177b190da2c"
        );
        assert_eq!(main.v, 28);

        let test = sign_l1_action(&key, &dummy(), Some(vault), 0, None, false).unwrap();
        assert_eq!(
            test.r,
            "0xe281d2fb5c6e25ca01601f878e4d69c965bb598b88fac58e475dd1f5e56c362b"
        );
        assert_eq!(
            test.s,
            "0x7ddad27e9a238d045c035bc606349d075d5c5cd00a6cd1da23ab5c39d4ef0f60"
        );
        assert_eq!(test.v, 27);
    }

    #[test]
    fn float_to_wire_examples() {
        assert_eq!(float_to_wire(100.0), "100");
        assert_eq!(float_to_wire(0.1), "0.1");
        assert_eq!(float_to_wire(50000.5), "50000.5");
        assert_eq!(float_to_wire(0.0), "0");
    }
}
