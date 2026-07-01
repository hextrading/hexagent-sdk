//! Polymarket CLOB EIP-712 order signing.
//!
//! Signs orders using the secp256k1 ECDSA (Ethereum) standard.
//! Domain: "Polymarket CTF Exchange", version "1", chainId 137 (Polygon).
//!
//! Reference: <https://github.com/Polymarket/python-order-utils>

use anyhow::{anyhow, Result};
use k256::ecdsa::SigningKey;
use sha3::{Digest, Keccak256};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

// ════════════════════════════════════════════════════════════════
// Constants
// ════════════════════════════════════════════════════════════════

/// Polygon mainnet chain ID.
const CHAIN_ID: u64 = 137;

/// CTF Exchange contract (standard binary markets).
pub const CTF_EXCHANGE: &str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";

/// Neg Risk CTF Exchange contract (multi-outcome markets).
pub const NEG_RISK_CTF_EXCHANGE: &str = "0xC5d563A36AE78145C45a50134d48A1215220f80a";

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// EIP-712 domain type hash:
/// keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)")
fn eip712_domain_type_hash() -> [u8; 32] {
    keccak256(b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)")
}

/// Order struct type hash:
/// keccak256("Order(uint256 salt,address maker,address signer,address taker,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint256 expiration,uint256 nonce,uint256 feeRateBps,uint8 side,uint8 signatureType)")
fn order_type_hash() -> [u8; 32] {
    keccak256(b"Order(uint256 salt,address maker,address signer,address taker,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint256 expiration,uint256 nonce,uint256 feeRateBps,uint8 side,uint8 signatureType)")
}

// ════════════════════════════════════════════════════════════════
// Types
// ════════════════════════════════════════════════════════════════

/// Signature types for Polymarket orders.
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum SignatureType {
    Eoa = 0,
    PolyProxy = 1,
    PolyGnosisSafe = 2,
    /// CLOB v2 deposit wallet (ERC-1967 proxy). Order `maker == signer ==
    /// deposit wallet`, validated via the wallet's ERC-1271 with an
    /// ERC-7739-wrapped signature. See `deposit_wallet.rs` /
    /// `signer_v2::build_signed_order_poly1271`.
    Poly1271 = 3,
}

/// Parse a config `signature_type` string into a [`SignatureType`]. Accepts the
/// same aliases the `[[exchanges]] polymarket.signature_type` / `[poly.*]`
/// knobs accept; unknown values fall back to `Eoa`. (Moved out of the engine so
/// both the engine and the polymaker strategy resolve it the same way.)
pub fn parse_signature_type(s: &str) -> SignatureType {
    match s.to_lowercase().as_str() {
        "gnosis_safe" | "safe" | "poly_gnosis_safe" => SignatureType::PolyGnosisSafe,
        "poly_proxy" | "proxy" => SignatureType::PolyProxy,
        "poly_1271" | "deposit_wallet" | "1271" => SignatureType::Poly1271,
        _ => SignatureType::Eoa,
    }
}

/// Order fields for EIP-712 signing.
#[derive(Debug, Clone)]
pub struct ClobOrder {
    pub salt: String,            // random u256 decimal string
    pub maker: String,           // 0x... address
    pub signer: String,          // 0x... address
    pub taker: String,           // 0x0...0 for open orders
    pub token_id: String,        // outcome token ID (large decimal string)
    pub maker_amount: String,    // 6-decimal USDC (integer string)
    pub taker_amount: String,    // 6-decimal conditional token (integer string)
    pub expiration: String,      // unix timestamp or "0"
    pub nonce: String,           // "0"
    pub fee_rate_bps: String,    // basis points, e.g. "0"
    pub side: u8,                // 0 = Buy, 1 = Sell
    pub signature_type: u8,      // 0 = EOA
}

/// Signed order ready for submission.
#[derive(Debug, Clone)]
pub struct SignedOrder {
    pub order: ClobOrder,
    pub signature: String, // 0x... hex (65 bytes: r + s + v)
    /// Pre-computed Polymarket `orderID` — the full EIP-712 digest
    /// (`keccak256(0x1901 || domain_separator || struct_hash)`). This is
    /// byte-identical to the `orderID` the CLOB API returns in its
    /// POST /order response, so we can register it in the coid↔orderID
    /// maps and issue cancels / status queries by orderID BEFORE we
    /// know whether the HTTP submit succeeded.
    pub order_hash: String,
}

// ════════════════════════════════════════════════════════════════
// OrderSigner
// ════════════════════════════════════════════════════════════════

/// Signs Polymarket CLOB orders using EIP-712 typed data.
pub struct OrderSigner {
    signing_key: SigningKey,
    /// EOA address derived from private key (the actual signer).
    pub signer_address: String,
    /// Maker/funder address: same as signer for EOA, Safe proxy address for GnosisSafe.
    pub maker_address: String,
    exchange_address: String,
    pub signature_type: SignatureType,
}

impl OrderSigner {
    /// Create from hex-encoded private key.
    /// Addresses are derived once: signer (EOA) and maker (Safe proxy for GnosisSafe, EOA otherwise).
    pub fn new(
        private_key_hex: &str,
        neg_risk: bool,
        sig_type: SignatureType,
    ) -> Result<Self> {
        let hex_clean = private_key_hex.strip_prefix("0x").unwrap_or(private_key_hex);
        let key_bytes = hex::decode(hex_clean)
            .map_err(|e| anyhow!("Invalid private key hex: {}", e))?;
        let signing_key = SigningKey::from_bytes(key_bytes.as_slice().into())
            .map_err(|e| anyhow!("Invalid private key: {}", e))?;

        let (signer_address, maker_address) = derive_addresses(private_key_hex, sig_type)
            .ok_or_else(|| anyhow!("Failed to derive addresses from private key"))?;

        let exchange_address = if neg_risk {
            NEG_RISK_CTF_EXCHANGE.to_string()
        } else {
            CTF_EXCHANGE.to_string()
        };

        Ok(Self { signing_key, signer_address, maker_address, exchange_address, signature_type: sig_type })
    }

    /// Sign an order, returning the hex-encoded signature with 0x prefix.
    pub fn sign_order(&self, order: &ClobOrder) -> Result<String> {
        let _t = crate::latency::TimedStage::new("polymarket.signer.sign");
        let digest = self.order_digest(order);

        // Sign with secp256k1
        let (sig, recid) = self.signing_key
            .sign_prehash_recoverable(&digest)
            .map_err(|e| anyhow!("Signing failed: {}", e))?;

        // Encode as r (32) + s (32) + v (1) where v = recid + 27
        let mut sig_bytes = [0u8; 65];
        sig_bytes[..64].copy_from_slice(&sig.to_bytes());
        sig_bytes[64] = recid.to_byte() + 27;

        Ok(format!("0x{}", hex::encode(sig_bytes)))
    }

    /// Compute the full EIP-712 digest for an order:
    /// `keccak256(0x1901 || domain_separator || struct_hash)`.
    ///
    /// This is the value signed by `sign_order`, and it is byte-identical
    /// to the `orderID` returned by the Polymarket CLOB (and emitted by
    /// the on-chain CTFExchange contract). Exposed so the strategy can
    /// register the orderID BEFORE the submit HTTP call completes — on
    /// timeout / unknown-state the pre-computed hash is still authoritative
    /// and can be used directly with `GET /data/order/{orderID}` or
    /// `DELETE /order/{orderID}` without any reconcile step.
    pub fn order_digest(&self, order: &ClobOrder) -> [u8; 32] {
        let domain_sep = self.domain_separator();
        let struct_hash = order_struct_hash(order);
        let mut buf = Vec::with_capacity(2 + 32 + 32);
        buf.push(0x19);
        buf.push(0x01);
        buf.extend_from_slice(&domain_sep);
        buf.extend_from_slice(&struct_hash);
        keccak256(&buf)
    }

    /// Hex-encoded (`0x...`) form of `order_digest` — the Polymarket
    /// `orderID` you'd get back from POST /order.
    pub fn order_hash_hex(&self, order: &ClobOrder) -> String {
        format!("0x{}", hex::encode(self.order_digest(order)))
    }

    /// Build a signed order from an OrderRequest-like input.
    pub fn build_signed_order(
        &self,
        token_id: &str,
        price: f64,
        size: f64,
        side: crate::types::Side,
        fee_rate_bps: u32,
    ) -> Result<SignedOrder> {
        let (maker_amount, taker_amount) = compute_amounts(price, size, side);
        let clob_side = match side {
            crate::types::Side::Buy => 0u8,
            crate::types::Side::Sell => 1u8,
        };

        let order = ClobOrder {
            salt: account_salt(&self.maker_address),
            maker: self.maker_address.clone(),   // Safe proxy or EOA
            signer: self.signer_address.clone(), // always EOA
            taker: ZERO_ADDRESS.to_string(),
            token_id: token_id.to_string(),
            maker_amount,
            taker_amount,
            expiration: "0".to_string(),
            nonce: "0".to_string(),
            fee_rate_bps: fee_rate_bps.to_string(),
            side: clob_side,
            signature_type: self.signature_type as u8,
        };

        let signature = self.sign_order(&order)?;
        let order_hash = self.order_hash_hex(&order);
        Ok(SignedOrder { order, signature, order_hash })
    }

    fn domain_separator(&self) -> [u8; 32] {
        let type_hash = eip712_domain_type_hash();
        let name_hash = keccak256(b"Polymarket CTF Exchange");
        let version_hash = keccak256(b"1");
        let chain_id = u256_bytes(CHAIN_ID as u128);
        let contract = address_to_bytes32(&self.exchange_address);

        // abi.encode(typeHash, nameHash, versionHash, chainId, verifyingContract)
        let mut buf = Vec::with_capacity(5 * 32);
        buf.extend_from_slice(&type_hash);
        buf.extend_from_slice(&name_hash);
        buf.extend_from_slice(&version_hash);
        buf.extend_from_slice(&chain_id);
        buf.extend_from_slice(&contract);
        keccak256(&buf)
    }
}

// ════════════════════════════════════════════════════════════════
// Helpers
// ════════════════════════════════════════════════════════════════

/// Compute maker_amount and taker_amount from price and size.
///
/// Polymarket uses 6-decimal USDC (1 USDC = 1_000_000 units).
/// Conditional tokens also use 6-decimal precision.
///
/// BUY: pay (size * price) USDC, receive (size) tokens
///   makerAmount = round(size * price * 1e6)
///   takerAmount = round(size * 1e6)
///
/// SELL: pay (size) tokens, receive (size * price) USDC
///   makerAmount = round(size * 1e6)
///   takerAmount = round(size * price * 1e6)
pub fn compute_amounts(price: f64, size: f64, side: crate::types::Side) -> (String, String) {
    let scale = 1_000_000.0; // 6 decimals
    match side {
        crate::types::Side::Buy => {
            let maker = (size * price * scale).round() as u128;
            let taker = (size * scale).round() as u128;
            (maker.to_string(), taker.to_string())
        }
        crate::types::Side::Sell => {
            let maker = (size * scale).round() as u128;
            let taker = (size * price * scale).round() as u128;
            (maker.to_string(), taker.to_string())
        }
    }
}

/// Process start time in whole Unix seconds, captured once. Occupies the salt's
/// high 32 bits, so it is directly human-readable (decode a salt → `salt >> 32`
/// is the run's start second) and distinguishes salts across restarts. Fits in
/// u32 until year 2106.
fn startup_secs() -> u32 {
    static SECS: OnceLock<u32> = OnceLock::new();
    *SECS.get_or_init(|| {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as u32)
            .unwrap_or(0)
    })
}

/// Per-account monotonic order counters, keyed by lowercased maker address.
fn salt_counters() -> &'static Mutex<HashMap<String, u64>> {
    static COUNTERS: OnceLock<Mutex<HashMap<String, u64>>> = OnceLock::new();
    COUNTERS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Per-account salt as a decimal string (fits in u64, so the submission layer's
/// `as u64` downcast is lossless and the server-recomputed EIP-712 hash matches
/// the locally-signed one).
///
/// Layout (64 bits): `[ high 32: startup Unix seconds | low 32: counter ]`
///   - high 32 = process start second → differs across restarts, and is
///     directly readable (`salt >> 32` = run start second)
///   - low 32  = per-account counter, incremented once per order within a run
///
/// Guarantees:
///   - within one run an account never reuses a salt (counter strictly ++),
///     and its salts increase monotonically;
///   - two runs started in different seconds produce disjoint salts.
///
/// Notes:
///   - The counter is per-account (keyed by maker), so two accounts in one run
///     share the same high half and can emit equal salts. That is harmless:
///     the orderID hashes the full struct including `maker`, so distinct
///     wallets always get distinct orderIDs.
///   - Two runs started within the *same* second would overlap salt sequences;
///     an actual orderID collision would additionally require the same account,
///     same counter, same order params, and (v2) same ms `timestamp` field —
///     not reachable in practice.
///   - The 32-bit counter wraps only after 2^32 (~4.3e9) orders for one account
///     within one run — never reached in practice.
pub fn account_salt(maker: &str) -> String {
    let key = maker.to_ascii_lowercase();
    let counter = {
        let mut map = salt_counters().lock().unwrap_or_else(|e| e.into_inner());
        let c = map.entry(key).or_insert(0);
        let v = *c;
        *c = c.wrapping_add(1);
        v
    };
    let salt = ((startup_secs() as u64) << 32) | (counter & 0xFFFF_FFFF);
    salt.to_string()
}

/// Compute keccak256 hash.
fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Public wrapper for deploy_wallet module.
pub fn derive_eth_address_from_key(key: &k256::ecdsa::SigningKey) -> String {
    derive_eth_address(key)
}

/// Derive both signer (EOA) and wallet (funder) addresses from a private key hex string.
/// For GnosisSafe, wallet = Safe proxy via CREATE2. For EOA, wallet = signer.
/// Returns (signer_address, wallet_address).
pub fn derive_addresses(private_key_hex: &str, sig_type: SignatureType) -> Option<(String, String)> {
    let hex_clean = private_key_hex.strip_prefix("0x").unwrap_or(private_key_hex);
    if hex_clean.is_empty() { return None; }
    let bytes = hex::decode(hex_clean).ok()?;
    if bytes.len() != 32 { return None; }
    let key = k256::ecdsa::SigningKey::from_bytes(bytes.as_slice().into()).ok()?;
    let eoa = to_checksum_address(&derive_eth_address(&key));
    let wallet = match sig_type {
        SignatureType::PolyGnosisSafe => {
            to_checksum_address(&crate::exchange::polymarket::deploy_wallet::derive_safe_address(&eoa))
        }
        _ => eoa.clone(),
    };
    Some((eoa, wallet))
}

/// EIP-55 checksum encoding for an Ethereum address.
fn to_checksum_address(addr: &str) -> String {
    let hex_str = addr.strip_prefix("0x").unwrap_or(addr).to_lowercase();
    let hash = keccak256(hex_str.as_bytes());
    let mut checksummed = String::with_capacity(42);
    checksummed.push_str("0x");
    for (i, c) in hex_str.chars().enumerate() {
        if c.is_ascii_digit() {
            checksummed.push(c);
        } else {
            let nibble = if i % 2 == 0 { hash[i / 2] >> 4 } else { hash[i / 2] & 0x0f };
            checksummed.push(if nibble >= 8 { c.to_ascii_uppercase() } else { c });
        }
    }
    checksummed
}

/// Derive Ethereum address from signing key.
fn derive_eth_address(key: &SigningKey) -> String {
    use k256::ecdsa::VerifyingKey;
    let verifying_key = VerifyingKey::from(key);
    let pubkey_bytes = verifying_key.to_encoded_point(false);
    // Skip the 0x04 prefix byte, hash the 64-byte uncompressed public key
    let hash = keccak256(&pubkey_bytes.as_bytes()[1..]);
    // Take last 20 bytes
    format!("0x{}", hex::encode(&hash[12..]))
}

/// Compute EIP-712 struct hash for an Order.
fn order_struct_hash(order: &ClobOrder) -> [u8; 32] {
    let type_hash = order_type_hash();

    let mut buf = Vec::with_capacity(14 * 32); // typeHash + 12 fields + padding
    buf.extend_from_slice(&type_hash);
    buf.extend_from_slice(&u256_from_decimal(&order.salt));
    buf.extend_from_slice(&address_to_bytes32(&order.maker));
    buf.extend_from_slice(&address_to_bytes32(&order.signer));
    buf.extend_from_slice(&address_to_bytes32(&order.taker));
    buf.extend_from_slice(&u256_from_decimal(&order.token_id));
    buf.extend_from_slice(&u256_from_decimal(&order.maker_amount));
    buf.extend_from_slice(&u256_from_decimal(&order.taker_amount));
    buf.extend_from_slice(&u256_from_decimal(&order.expiration));
    buf.extend_from_slice(&u256_from_decimal(&order.nonce));
    buf.extend_from_slice(&u256_from_decimal(&order.fee_rate_bps));
    buf.extend_from_slice(&u256_bytes(order.side as u128));      // uint8 → uint256
    buf.extend_from_slice(&u256_bytes(order.signature_type as u128)); // uint8 → uint256

    keccak256(&buf)
}

/// Parse a decimal string to 32-byte big-endian u256.
/// Handles values up to 2^256 - 1 (78 digits) that exceed u128 range.
fn u256_from_decimal(s: &str) -> [u8; 32] {
    if s.is_empty() || s == "0" {
        return [0u8; 32];
    }
    // Try u128 first (fast path for small values)
    if let Ok(val) = s.parse::<u128>() {
        return u256_bytes(val);
    }
    // Large decimal: manual base-10 to base-256 conversion
    // Process as two u128 halves: val = high * 2^128 + low
    let mut result = [0u8; 32];
    let mut digits: Vec<u8> = s.bytes().map(|b| b - b'0').collect();
    // Repeatedly divide by 256 to extract bytes from LSB to MSB
    for i in (0..32).rev() {
        let mut remainder = 0u32;
        for d in digits.iter_mut() {
            let val = remainder * 10 + *d as u32;
            *d = (val / 256) as u8;
            remainder = val % 256;
        }
        result[i] = remainder as u8;
        // Trim leading zeros for efficiency
        while digits.first() == Some(&0) && digits.len() > 1 {
            digits.remove(0);
        }
        if digits.len() == 1 && digits[0] == 0 {
            break;
        }
    }
    result
}

/// Convert u128 to 32-byte big-endian representation.
fn u256_bytes(val: u128) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[16..].copy_from_slice(&val.to_be_bytes());
    bytes
}

/// Convert 0x-prefixed hex address to 32-byte left-padded representation.
fn address_to_bytes32(addr: &str) -> [u8; 32] {
    let hex_str = addr.strip_prefix("0x").unwrap_or(addr);
    let addr_bytes = hex::decode(hex_str).unwrap_or_else(|_| vec![0u8; 20]);
    let mut bytes = [0u8; 32];
    let start = 32 - addr_bytes.len().min(32);
    bytes[start..].copy_from_slice(&addr_bytes[..addr_bytes.len().min(32)]);
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u256_from_decimal() {
        let result = u256_from_decimal("1000000");
        assert_eq!(result[31], 0x40); // 1000000 = 0xF4240
        assert_eq!(result[30], 0x42);
        assert_eq!(result[29], 0x0F);
    }

    #[test]
    fn test_u256_from_decimal_large() {
        // Token ID larger than u128: 50303916472381649224674364401111317755258653723694532482715411789597335197187
        let token = "50303916472381649224674364401111317755258653723694532482715411789597335197187";
        let result = u256_from_decimal(token);
        // Should NOT be all zeros (u128 overflow case)
        assert!(result.iter().any(|&b| b != 0), "Large token ID should not be zero");
        // Verify by converting back: hex representation
        let hex_str = hex::encode(result);
        assert_eq!(hex_str, "6f3701fbd48acf387d96086d3bfd5767655c99d2bb6105e8f295346af0e99603");
    }

    #[test]
    fn test_address_to_bytes32() {
        let addr = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";
        let result = address_to_bytes32(addr);
        // First 12 bytes should be zero (padding)
        assert_eq!(&result[..12], &[0u8; 12]);
        // Last 20 bytes should be the address
        assert_eq!(result[12], 0x4b);
    }

    #[test]
    fn test_compute_amounts_buy() {
        let (maker, taker) = compute_amounts(0.55, 100.0, crate::types::Side::Buy);
        assert_eq!(maker, "55000000");  // 100 * 0.55 * 1e6
        assert_eq!(taker, "100000000"); // 100 * 1e6
    }

    #[test]
    fn test_compute_amounts_sell() {
        let (maker, taker) = compute_amounts(0.55, 100.0, crate::types::Side::Sell);
        assert_eq!(maker, "100000000"); // 100 * 1e6
        assert_eq!(taker, "55000000");  // 100 * 0.55 * 1e6
    }

    /// Helper: build a deterministic ClobOrder + signer so tests can
    /// assert exact hash / signature bytes.
    fn fixture_signer() -> OrderSigner {
        // Hardhat account #0 private key — public test key, deterministic
        // address 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266.
        OrderSigner::new(
            "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
            false, // CTFExchange (not neg-risk)
            SignatureType::Eoa,
        ).unwrap()
    }

    fn fixture_order() -> ClobOrder {
        ClobOrder {
            salt: "1234567890".to_string(),
            maker: "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266".to_string(),
            signer: "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266".to_string(),
            taker: ZERO_ADDRESS.to_string(),
            token_id: "50303916472381649224674364401111317755258653723694532482715411789597335197187".to_string(),
            maker_amount: "55000000".to_string(),   // 55 USDC @ 6 decimals
            taker_amount: "100000000".to_string(),  // 100 tokens @ 6 decimals
            expiration: "0".to_string(),
            nonce: "0".to_string(),
            fee_rate_bps: "0".to_string(),
            side: 0,            // Buy
            signature_type: 0,  // EOA
        }
    }

    /// Lock-in test: keccak256 struct hash stays stable across refactors.
    /// If this ever breaks and you didn't mean to change the order struct
    /// layout, the server will reject every order — stop and investigate
    /// before updating the expected value.
    #[test]
    fn test_order_struct_hash_stable() {
        let order = fixture_order();
        let h = order_struct_hash(&order);
        assert_eq!(
            hex::encode(h),
            // Regression anchor — captured from our own implementation.
            // To cross-check correctness against Polymarket's server:
            //   1. Run py-order-utils `get_order_hash` on `fixture_order()`
            //      (same field values), OR
            //   2. Place a live order once, compare `signer.order_hash_hex`
            //      against the `orderID` in the CLOB POST /order response
            //      — the SharedState mismatch warning fires automatically.
            // If this ever flips, stop and investigate: either the order
            // struct encoding changed (don't want — server will reject all
            // orders) or the hash primitive changed (unlikely).
            "e3f6d84b5ee22a5834f09b93cdf200330f7063811048e534ef9edf8d5709a266",
        );
    }

    /// Lock-in: the full EIP-712 digest (orderID the server returns).
    #[test]
    fn test_order_digest_stable() {
        let signer = fixture_signer();
        let digest = signer.order_digest(&fixture_order());
        assert_eq!(
            hex::encode(digest),
            "1bd9b41cb6d61c1c921990aefc7fbfc6465f0b2268a978746b36c27310ec364c",
        );
        // And the hex helper agrees.
        let hex_form = signer.order_hash_hex(&fixture_order());
        assert_eq!(hex_form, format!("0x{}", hex::encode(digest)));
    }

    /// `build_signed_order` attaches the same hash that `order_hash_hex`
    /// would compute for the resulting ClobOrder.
    #[test]
    fn test_signed_order_hash_matches_standalone() {
        let signer = fixture_signer();
        let signed = signer.build_signed_order(
            "50303916472381649224674364401111317755258653723694532482715411789597335197187",
            0.55, 100.0, crate::types::Side::Buy, 0,
        ).unwrap();
        let direct = signer.order_hash_hex(&signed.order);
        assert_eq!(signed.order_hash, direct);
        assert!(signed.order_hash.starts_with("0x"));
        assert_eq!(signed.order_hash.len(), 2 + 64);
    }

    #[test]
    fn test_derive_address_deterministic() {
        // Well-known test private key
        let key_hex = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
        let key_bytes = hex::decode(key_hex).unwrap();
        let signing_key = SigningKey::from_bytes(key_bytes.as_slice().into()).unwrap();
        let addr = derive_eth_address(&signing_key);
        // This is Hardhat's account #0
        assert_eq!(addr, "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266");
    }

    #[test]
    fn account_salt_layout_and_monotonicity() {
        let a = "0xaAaAaAaaAaAaAaaAaAAAAAAAAaaaAaAaAaaAaaAa";
        let b = "0xBbBbBBBbbBBBbbbBbbBbBBbBBBbBbBBBBBbBbBbb";

        // Same account: low 32 bits (counter) strictly increase by 1; high 32
        // bits (startup seconds) stay constant within a run.
        let s0: u64 = account_salt(a).parse().unwrap();
        let s1: u64 = account_salt(a).parse().unwrap();
        let s2: u64 = account_salt(a).parse().unwrap();
        assert!(s0 < s1 && s1 < s2, "salts must be monotonic within a run");
        assert_eq!(s1 - s0, 1, "counter increments by exactly 1");
        assert_eq!(s2 - s1, 1);
        assert_eq!(s0 >> 32, s1 >> 32, "high half constant within a run");

        // High half is the readable startup second (matches startup_secs()).
        assert_eq!(s0 >> 32, startup_secs() as u64);

        // Case-insensitive keying: same maker in different case shares the
        // counter (no reuse), so it continues the sequence rather than resetting.
        let s3: u64 = account_salt(&a.to_uppercase()).parse().unwrap();
        assert_eq!(s3, s2 + 1);

        // Different account: independent counter, but shares the startup-second
        // high half (harmless — orderID is differentiated by the maker field).
        let sb: u64 = account_salt(b).parse().unwrap();
        assert_eq!(sb >> 32, s0 >> 32, "accounts share the startup-second high half");
        assert_eq!(sb & 0xFFFF_FFFF, 0, "account b's own counter starts at 0");

        // Fits in u64 (the submission layer downcasts salt to u64 — must be lossless).
        assert!(account_salt(a).parse::<u128>().unwrap() <= u64::MAX as u128);
    }
}
