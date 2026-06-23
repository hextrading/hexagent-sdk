//! Polymarket CLOB **v2** EIP-712 order signing (2026-04-28 cutover).
//!
//! **Authoritative source**: `github.com/Polymarket/clob-client-v2`
//! (separate repo from clob-client which is v1-only). Key files:
//!
//!   * `src/order-utils/model/ctfExchangeV2TypedData.ts` — struct +
//!     domain constants (11 fields, version "2")
//!   * `src/order-utils/exchangeOrderBuilderV2.ts` — buildOrder +
//!     buildOrderTypedData
//!   * `src/types/ordersV2.ts` — `orderToJsonV2` wire body shape
//!   * `src/config.ts` — v2 Exchange contract addresses
//!
//! v2 changes vs v1:
//!   * New Exchange contract addresses (`exchangeV2` / `negRiskExchangeV2`)
//!   * Domain `version` bumps "1" → "2"
//!   * Order struct **drops** `taker`, `expiration`, `nonce`, `feeRateBps`
//!     from the signed typed-data (fee now computed protocol-side; nonces
//!     removed entirely; `taker` and `expiration` are wire-only)
//!   * Order struct **adds** `timestamp` (ms since epoch), `metadata`
//!     (bytes32 reserved, zero), `builder` (bytes32, attribution code)
//!
//! Signing flow is IDENTICAL to v1: EOA signs the EIP-712 digest, Gnosis
//! Safe is the `maker` with signatureType=2 indicating the on-chain
//! exchange should validate the EOA sig against the Safe's owners.

use anyhow::{anyhow, Result};
use k256::ecdsa::SigningKey;
use sha3::{Digest, Keccak256};

use super::signer::{
    SignatureType,
    compute_amounts,
    random_salt,
    derive_addresses,
};

// ════════════════════════════════════════════════════════════════
// v2 Exchange addresses + domain
// ════════════════════════════════════════════════════════════════

const CHAIN_ID: u64 = 137;

/// v2 CTF Exchange (standard binary markets).
pub const CTF_EXCHANGE_V2: &str = "0xE111180000d2663C0091e4f400237545B87B996B";

/// v2 Neg Risk CTF Exchange (multi-outcome markets).
pub const NEG_RISK_CTF_EXCHANGE_V2: &str = "0xe2222d279d744050d28e00520010520000310F59";

fn eip712_domain_type_hash() -> [u8; 32] {
    keccak256(b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)")
}

// ── ERC-7739 / POLY_1271 (deposit-wallet) signing constants ──
// Must stay byte-identical to `order_v2_type_hash`'s preimage; a debug
// test asserts `keccak256(ORDER_TYPE_STRING) == order_v2_type_hash()`.
const ORDER_TYPE_STRING: &str = "Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)";
/// Solady `TypedDataSign` wrapper type string = wrapper + appended
/// `contents` (Order) type, per rs-clob-client-v2 / py-clob-client-v2.
const SOLADY_ORDER_TYPE_STRING: &str = concat!(
    "TypedDataSign(Order contents,string name,string version,uint256 chainId,",
    "address verifyingContract,bytes32 salt)",
    "Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,",
    "uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)",
);
const DEPOSIT_WALLET_NAME: &str = "DepositWallet";
const DEPOSIT_WALLET_VERSION: &str = "1";

/// v2 Order typehash. Field order MUST match
/// `CTF_EXCHANGE_V2_ORDER_STRUCT` in ctfExchangeV2TypedData.ts.
fn order_v2_type_hash() -> [u8; 32] {
    keccak256(b"Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)")
}

// ════════════════════════════════════════════════════════════════
// v2 Order + SignedOrder
// ════════════════════════════════════════════════════════════════

/// v2 order fields. The 11 signed fields match the SDK's OrderV2
/// interface exactly; `taker` and `expiration` are wire-only (NOT in
/// the struct hash) and are here so the caller can copy them straight
/// onto the wire envelope.
#[derive(Debug, Clone)]
pub struct OrderV2 {
    // ── Signed fields (in CTF_EXCHANGE_V2_ORDER_STRUCT order) ──
    pub salt: String,
    pub maker: String,
    pub signer: String,
    pub token_id: String,
    pub maker_amount: String,
    pub taker_amount: String,
    pub side: u8,             // 0 = BUY, 1 = SELL (uint8 in typed-data)
    pub signature_type: u8,
    pub timestamp: String,    // unix epoch MILLISECONDS per `Date.now()` in SDK
    pub metadata: String,     // bytes32 hex — zeros by default
    pub builder: String,      // bytes32 hex — attribution code or zeros

    // ── Wire-only fields (NOT signed) ──
    pub taker: String,        // zero address (orderToJsonV2 echoes this)
    pub expiration: String,   // unix seconds or "0"
}

#[derive(Debug, Clone)]
pub struct SignedOrderV2 {
    pub order: OrderV2,
    pub signature: String,
    pub order_hash: String,
}

// ════════════════════════════════════════════════════════════════
// OrderSignerV2
// ════════════════════════════════════════════════════════════════

pub struct OrderSignerV2 {
    signing_key: SigningKey,
    pub signer_address: String,
    pub maker_address: String,
    exchange_address: String,
    builder_code: [u8; 32],
    pub signature_type: SignatureType,
    /// Deposit-wallet address for POLY_1271 (the order `maker`/`signer`).
    /// `None` for other signature types. Set via [`Self::with_funder`].
    funder: Option<String>,
}

impl OrderSignerV2 {
    pub fn new(
        private_key_hex: &str,
        neg_risk: bool,
        sig_type: SignatureType,
        builder_code_hex: &str,
    ) -> Result<Self> {
        let hex_clean = private_key_hex.strip_prefix("0x").unwrap_or(private_key_hex);
        let key_bytes = hex::decode(hex_clean)
            .map_err(|e| anyhow!("Invalid private key hex: {}", e))?;
        let signing_key = SigningKey::from_bytes(key_bytes.as_slice().into())
            .map_err(|e| anyhow!("Invalid private key: {}", e))?;

        let (signer_address, maker_address) = derive_addresses(private_key_hex, sig_type)
            .ok_or_else(|| anyhow!("Failed to derive addresses from private key"))?;

        let exchange_address = if neg_risk {
            NEG_RISK_CTF_EXCHANGE_V2.to_string()
        } else {
            CTF_EXCHANGE_V2.to_string()
        };

        let builder_code = parse_bytes32(builder_code_hex)?;

        Ok(Self {
            signing_key, signer_address, maker_address,
            exchange_address, builder_code, signature_type: sig_type,
            funder: None,
        })
    }

    /// Attach the deposit-wallet (funder) address used as `maker`/`signer`
    /// for POLY_1271 orders. Empty string = no-op (leaves `None`).
    ///
    /// This ALSO overwrites `maker_address` with the funder. Rationale:
    /// for POLY_1271 the on-book order `maker` IS the deposit wallet (see
    /// `build_signed_order_poly1271`, which sets both `maker` and `signer`
    /// to `funder`), whereas `derive_addresses` set `maker_address` to the
    /// EOA-derived address. Downstream fill ingestion keys off
    /// `signer.maker_address`:
    ///   * WS live maker-leg match (`user_feed.rs`: `maker_orders[].maker_address`)
    ///   * REST gap recovery (`/trades?maker_address=…`)
    /// Leaving `maker_address` as the EOA silently dropped EVERY maker fill
    /// (the EOA owns no orders) — the ledger never decremented, so the
    /// strategy over-quoted SELL against phantom inventory and the CLOB
    /// rejected it with `not enough balance`. Aligning the field with the
    /// real order maker fixes both ingestion paths in one place. The
    /// EOA-only `build_signed_order` path (which reads `maker_address`) is
    /// never reached once `funder` is set — POLY_1271 dispatches to
    /// `build_signed_order_poly1271`, which uses `funder` directly.
    pub fn with_funder(mut self, funder: &str) -> Self {
        if !funder.trim().is_empty() {
            let f = funder.trim().to_string();
            self.maker_address = f.clone();
            self.funder = Some(f);
        }
        self
    }

    /// Build + sign a v2 order, dispatching on `signature_type`: POLY_1271
    /// uses the deposit-wallet (funder) maker + ERC-7739 wrap; everything
    /// else uses the standard EOA-signed path.
    pub fn build_signed_order_dispatch(
        &self,
        token_id: &str,
        price: f64,
        size: f64,
        side: crate::types::Side,
    ) -> Result<SignedOrderV2> {
        if matches!(self.signature_type, SignatureType::Poly1271) {
            let funder = self.funder.as_deref().ok_or_else(|| {
                anyhow!("signature_type=poly_1271 requires a deposit-wallet address — \
                         set [poly.<id>].funder in the secrets file")
            })?;
            self.build_signed_order_poly1271(funder, token_id, price, size, side)
        } else {
            self.build_signed_order(token_id, price, size, side)
        }
    }

    pub fn sign_order(&self, order: &OrderV2) -> Result<String> {
        let _t = crate::latency::TimedStage::new("polymarket.signer_v2.sign");
        let digest = self.order_digest(order);
        let (sig, recid) = self.signing_key
            .sign_prehash_recoverable(&digest)
            .map_err(|e| anyhow!("Signing failed: {}", e))?;
        let mut sig_bytes = [0u8; 65];
        sig_bytes[..64].copy_from_slice(&sig.to_bytes());
        sig_bytes[64] = recid.to_byte() + 27;
        Ok(format!("0x{}", hex::encode(sig_bytes)))
    }

    pub fn order_digest(&self, order: &OrderV2) -> [u8; 32] {
        let domain_sep = self.domain_separator();
        let struct_hash = order_v2_struct_hash(order);
        let mut buf = Vec::with_capacity(2 + 32 + 32);
        buf.push(0x19);
        buf.push(0x01);
        buf.extend_from_slice(&domain_sep);
        buf.extend_from_slice(&struct_hash);
        keccak256(&buf)
    }

    pub fn order_hash_hex(&self, order: &OrderV2) -> String {
        format!("0x{}", hex::encode(self.order_digest(order)))
    }

    /// Build + sign a v2 order from a price/size/side triple.
    /// `timestamp` is stamped with current wall-clock milliseconds (matching
    /// the SDK's `Date.now().toString()` default).
    pub fn build_signed_order(
        &self,
        token_id: &str,
        price: f64,
        size: f64,
        side: crate::types::Side,
    ) -> Result<SignedOrderV2> {
        let (maker_amount, taker_amount) = compute_amounts(price, size, side);
        let clob_side = match side {
            crate::types::Side::Buy => 0u8,
            crate::types::Side::Sell => 1u8,
        };
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let order = OrderV2 {
            salt: random_salt(),
            maker: self.maker_address.clone(),
            signer: self.signer_address.clone(),
            token_id: token_id.to_string(),
            maker_amount,
            taker_amount,
            side: clob_side,
            signature_type: self.signature_type as u8,
            timestamp: now_ms.to_string(),
            metadata: format!("0x{}", hex::encode([0u8; 32])),
            builder: format!("0x{}", hex::encode(self.builder_code)),
            // wire-only
            taker: "0x0000000000000000000000000000000000000000".to_string(),
            expiration: "0".to_string(),
        };

        let signature = self.sign_order(&order)?;
        let order_hash = self.order_hash_hex(&order);
        Ok(SignedOrderV2 { order, signature, order_hash })
    }

    /// Build + sign a **POLY_1271 (deposit-wallet)** v2 order. `maker` and
    /// `signer` are BOTH set to `funder` (the deposit wallet); the order is
    /// signed by the EOA key but wrapped per ERC-7739 so the deposit
    /// wallet's ERC-1271 validates it. `signature_type` is forced to 3
    /// regardless of how this signer was constructed.
    pub fn build_signed_order_poly1271(
        &self,
        funder: &str,
        token_id: &str,
        price: f64,
        size: f64,
        side: crate::types::Side,
    ) -> Result<SignedOrderV2> {
        let (maker_amount, taker_amount) = compute_amounts(price, size, side);
        let clob_side = match side {
            crate::types::Side::Buy => 0u8,
            crate::types::Side::Sell => 1u8,
        };
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let order = OrderV2 {
            salt: random_salt(),
            maker: funder.to_string(),
            signer: funder.to_string(),
            token_id: token_id.to_string(),
            maker_amount,
            taker_amount,
            side: clob_side,
            signature_type: SignatureType::Poly1271 as u8,
            timestamp: now_ms.to_string(),
            metadata: format!("0x{}", hex::encode([0u8; 32])),
            builder: format!("0x{}", hex::encode(self.builder_code)),
            taker: "0x0000000000000000000000000000000000000000".to_string(),
            expiration: "0".to_string(),
        };

        let signature = self.sign_order_poly1271(&order)?;
        let order_hash = self.order_hash_hex(&order);
        Ok(SignedOrderV2 { order, signature, order_hash })
    }

    /// ERC-7739-wrapped POLY_1271 order signature. Mirrors
    /// rs-clob-client-v2 `sign_poly1271_order` and py-clob-client-v2
    /// `_build_poly_1271_order_signature`:
    /// `0x || inner(65) || appDomainSep(32) || contentsHash(32) ||
    ///  ORDER_TYPE_STRING || uint16(len)`. The wallet "app domain"
    /// verifyingContract is `order.signer` (= the deposit wallet).
    fn sign_order_poly1271(&self, order: &OrderV2) -> Result<String> {
        let contents_hash = order_v2_struct_hash(order);
        let app_domain_sep = self.domain_separator();

        let mut tds = Vec::with_capacity(7 * 32);
        tds.extend_from_slice(&keccak256(SOLADY_ORDER_TYPE_STRING.as_bytes()));
        tds.extend_from_slice(&contents_hash);
        tds.extend_from_slice(&keccak256(DEPOSIT_WALLET_NAME.as_bytes()));
        tds.extend_from_slice(&keccak256(DEPOSIT_WALLET_VERSION.as_bytes()));
        tds.extend_from_slice(&u256_bytes(CHAIN_ID as u128));
        tds.extend_from_slice(&address_to_bytes32(&order.signer));
        tds.extend_from_slice(&[0u8; 32]);
        let tds_hash = keccak256(&tds);

        let mut digest_in = Vec::with_capacity(2 + 64);
        digest_in.push(0x19);
        digest_in.push(0x01);
        digest_in.extend_from_slice(&app_domain_sep);
        digest_in.extend_from_slice(&tds_hash);
        let digest = keccak256(&digest_in);

        let (sig, recid) = self
            .signing_key
            .sign_prehash_recoverable(&digest)
            .map_err(|e| anyhow!("Signing failed: {}", e))?;
        let mut inner = [0u8; 65];
        inner[..64].copy_from_slice(&sig.to_bytes());
        inner[64] = recid.to_byte() + 27;

        let type_str = ORDER_TYPE_STRING.as_bytes();
        let type_len = u16::try_from(type_str.len()).expect("order type string fits u16");

        let mut wrapped = String::from("0x");
        wrapped.push_str(&hex::encode(inner));
        wrapped.push_str(&hex::encode(app_domain_sep));
        wrapped.push_str(&hex::encode(contents_hash));
        wrapped.push_str(&hex::encode(type_str));
        wrapped.push_str(&hex::encode(type_len.to_be_bytes()));
        Ok(wrapped)
    }

    fn domain_separator(&self) -> [u8; 32] {
        let type_hash = eip712_domain_type_hash();
        let name_hash = keccak256(b"Polymarket CTF Exchange");
        let version_hash = keccak256(b"2");
        let chain_id = u256_bytes(CHAIN_ID as u128);
        let contract = address_to_bytes32(&self.exchange_address);

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
// Struct hash — field order MUST match `order_v2_type_hash`
// (ctfExchangeV2TypedData.ts `CTF_EXCHANGE_V2_ORDER_STRUCT`)
// ════════════════════════════════════════════════════════════════

fn order_v2_struct_hash(order: &OrderV2) -> [u8; 32] {
    let type_hash = order_v2_type_hash();
    let mut buf = Vec::with_capacity(12 * 32);
    buf.extend_from_slice(&type_hash);
    buf.extend_from_slice(&u256_from_decimal(&order.salt));
    buf.extend_from_slice(&address_to_bytes32(&order.maker));
    buf.extend_from_slice(&address_to_bytes32(&order.signer));
    buf.extend_from_slice(&u256_from_decimal(&order.token_id));
    buf.extend_from_slice(&u256_from_decimal(&order.maker_amount));
    buf.extend_from_slice(&u256_from_decimal(&order.taker_amount));
    buf.extend_from_slice(&u256_bytes(order.side as u128));
    buf.extend_from_slice(&u256_bytes(order.signature_type as u128));
    buf.extend_from_slice(&u256_from_decimal(&order.timestamp));
    buf.extend_from_slice(&parse_bytes32(&order.metadata).unwrap_or([0u8; 32]));
    buf.extend_from_slice(&parse_bytes32(&order.builder).unwrap_or([0u8; 32]));
    keccak256(&buf)
}

// ════════════════════════════════════════════════════════════════
// Helpers
// ════════════════════════════════════════════════════════════════

fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut h = Keccak256::new();
    h.update(data);
    h.finalize().into()
}

fn u256_bytes(val: u128) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[16..].copy_from_slice(&val.to_be_bytes());
    bytes
}

fn address_to_bytes32(addr: &str) -> [u8; 32] {
    let hex_str = addr.strip_prefix("0x").unwrap_or(addr);
    let addr_bytes = hex::decode(hex_str).unwrap_or_else(|_| vec![0u8; 20]);
    let mut bytes = [0u8; 32];
    let start = 32 - addr_bytes.len().min(32);
    bytes[start..].copy_from_slice(&addr_bytes[..addr_bytes.len().min(32)]);
    bytes
}

fn u256_from_decimal(s: &str) -> [u8; 32] {
    if s.is_empty() || s == "0" { return [0u8; 32]; }
    if let Ok(val) = s.parse::<u128>() { return u256_bytes(val); }
    let mut result = [0u8; 32];
    let mut digits: Vec<u8> = s.bytes().map(|b| b - b'0').collect();
    for i in (0..32).rev() {
        let mut remainder = 0u32;
        for d in digits.iter_mut() {
            let val = remainder * 10 + *d as u32;
            *d = (val / 256) as u8;
            remainder = val % 256;
        }
        result[i] = remainder as u8;
        while digits.first() == Some(&0) && digits.len() > 1 { digits.remove(0); }
        if digits.len() == 1 && digits[0] == 0 { break; }
    }
    result
}

fn parse_bytes32(s: &str) -> Result<[u8; 32]> {
    if s.is_empty() { return Ok([0u8; 32]); }
    let hex_clean = s.strip_prefix("0x").unwrap_or(s);
    if hex_clean.is_empty() { return Ok([0u8; 32]); }
    let bytes = hex::decode(hex_clean)
        .map_err(|e| anyhow!("Invalid bytes32 hex '{}': {}", s, e))?;
    if bytes.len() > 32 { return Err(anyhow!("bytes32 too long: {} bytes", bytes.len())); }
    let mut out = [0u8; 32];
    let start = 32 - bytes.len();
    out[start..].copy_from_slice(&bytes);
    Ok(out)
}

// ════════════════════════════════════════════════════════════════
// Tests
// ════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_signed_order_shape() {
        let signer = OrderSignerV2::new(
            "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
            false,
            SignatureType::Eoa,
            "",
        ).unwrap();
        let signed = signer.build_signed_order(
            "50303916472381649224674364401111317755258653723694532482715411789597335197187",
            0.55, 100.0, crate::types::Side::Buy,
        ).unwrap();
        assert_eq!(signed.order.side, 0);
        assert_eq!(signed.order.maker_amount, "55000000");
        assert_eq!(signed.order.taker_amount, "100000000");
        let ts: u64 = signed.order.timestamp.parse().unwrap();
        assert!(ts > 1735689600000, "timestamp must be current-ish ms: got {}", ts);
        assert_eq!(signed.order.metadata, format!("0x{}", hex::encode([0u8; 32])));
        assert_eq!(signed.order.builder,  format!("0x{}", hex::encode([0u8; 32])));
        assert_eq!(signed.order.taker, "0x0000000000000000000000000000000000000000");
        assert_eq!(signed.order.expiration, "0");
        assert!(signed.order_hash.starts_with("0x"));
        assert_eq!(signed.order_hash.len(), 66);
        assert!(signed.signature.starts_with("0x"));
        assert_eq!(signed.signature.len(), 132);
    }

    #[test]
    fn order_type_string_matches_typehash() {
        // The appended contentsType string MUST hash to the same typehash
        // used in the struct hash, or the ERC-7739 wrap is invalid.
        assert_eq!(keccak256(ORDER_TYPE_STRING.as_bytes()), order_v2_type_hash());
    }

    #[test]
    fn poly1271_order_wrap_layout() {
        let signer = OrderSignerV2::new(
            "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
            false,
            SignatureType::Poly1271,
            "",
        )
        .unwrap();
        let funder = "0xDd83a0a683A3E979BcC30d82799fff141b4B29a8";
        let signed = signer
            .build_signed_order_poly1271(
                funder,
                "50303916472381649224674364401111317755258653723694532482715411789597335197187",
                0.55,
                100.0,
                crate::types::Side::Buy,
            )
            .unwrap();

        // maker == signer == funder, type 3.
        assert_eq!(signed.order.maker, funder);
        assert_eq!(signed.order.signer, funder);
        assert_eq!(signed.order.signature_type, 3);

        // ERC-7739 wrapped layout: inner(65) + appSep(32) + contents(32) +
        // ORDER_TYPE_STRING + uint16(len).
        let bytes = hex::decode(signed.signature.strip_prefix("0x").unwrap()).unwrap();
        let type_str = ORDER_TYPE_STRING.as_bytes();
        assert_eq!(bytes.len(), 65 + 32 + 32 + type_str.len() + 2);
        let tail = &bytes[bytes.len() - 2..];
        assert_eq!(u16::from_be_bytes([tail[0], tail[1]]) as usize, type_str.len());
        let ts_start = 65 + 32 + 32;
        assert_eq!(&bytes[ts_start..ts_start + type_str.len()], type_str);
    }

    #[test]
    fn test_custom_builder_code_takes_effect() {
        let with_builder = OrderSignerV2::new(
            "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
            false, SignatureType::Eoa,
            "0x0000000000000000000000000000000000000000000000000000000000000001",
        ).unwrap();
        let signed = with_builder.build_signed_order(
            "50303916472381649224674364401111317755258653723694532482715411789597335197187",
            0.55, 100.0, crate::types::Side::Buy,
        ).unwrap();
        let expected = format!("0x{}", hex::encode({
            let mut b = [0u8; 32]; b[31] = 1; b
        }));
        assert_eq!(signed.order.builder, expected);
    }

    #[test]
    fn with_funder_aligns_maker_address_with_funder() {
        // POLY_1271: the on-book order maker is the deposit wallet (funder),
        // not the EOA. `with_funder` must align `maker_address` with it so
        // downstream fill matching (user_feed WS maker-leg + REST gap
        // recovery, both keyed off `signer.maker_address`) sees the right
        // address — otherwise EVERY maker fill is dropped and the ledger
        // over-states inventory.
        let key = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
        let funder = "0xDd83a0a683A3E979BcC30d82799fff141b4B29a8";

        // Before: derive_addresses fixes POLY_1271 maker to the EOA.
        let base = OrderSignerV2::new(key, false, SignatureType::Poly1271, "").unwrap();
        assert_eq!(base.maker_address, base.signer_address);

        // After: maker_address tracks the funder/DW; signer stays the EOA.
        let with = base.with_funder(funder);
        assert_eq!(with.maker_address, funder);
        assert_ne!(with.maker_address, with.signer_address);

        // The on-book order it builds uses that same funder as `maker` —
        // i.e. order.maker == signer.maker_address (what user_feed matches).
        let signed = with.build_signed_order_dispatch(
            "50303916472381649224674364401111317755258653723694532482715411789597335197187",
            0.55, 100.0, crate::types::Side::Buy,
        ).unwrap();
        assert_eq!(signed.order.maker, with.maker_address);

        // Empty funder is a no-op (leaves maker_address as the EOA).
        let noop = OrderSignerV2::new(key, false, SignatureType::Poly1271, "")
            .unwrap()
            .with_funder("");
        assert_eq!(noop.maker_address, noop.signer_address);
    }
}
