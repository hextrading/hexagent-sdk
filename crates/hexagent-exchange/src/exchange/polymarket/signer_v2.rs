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
    AccountSaltSequence,
    SignatureType,
    compute_amounts,
    derive_addresses,
    validate_signing_inputs,
    validate_u256_decimal,
};
use std::sync::{Arc, OnceLock};

// ════════════════════════════════════════════════════════════════
// v2 Exchange addresses + domain
// ════════════════════════════════════════════════════════════════

const CHAIN_ID: u64 = 137;

/// v2 CTF Exchange (standard binary markets).
pub const CTF_EXCHANGE_V2: &str = "0xE111180000d2663C0091e4f400237545B87B996B";

/// v2 Neg Risk CTF Exchange (multi-outcome markets).
pub const NEG_RISK_CTF_EXCHANGE_V2: &str = "0xe2222d279d744050d28e00520010520000310F59";

fn eip712_domain_type_hash() -> [u8; 32] {
    static H: OnceLock<[u8; 32]> = OnceLock::new();
    *H.get_or_init(|| keccak256(b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"))
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
    static H: OnceLock<[u8; 32]> = OnceLock::new();
    *H.get_or_init(|| keccak256(b"Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)"))
}

/// keccak256 of `SOLADY_ORDER_TYPE_STRING` — constant, memoized.
fn solady_order_type_hash() -> [u8; 32] {
    static H: OnceLock<[u8; 32]> = OnceLock::new();
    *H.get_or_init(|| keccak256(SOLADY_ORDER_TYPE_STRING.as_bytes()))
}

/// keccak256 of `DEPOSIT_WALLET_NAME` / `_VERSION` — constant, memoized.
fn deposit_wallet_name_hash() -> [u8; 32] {
    static H: OnceLock<[u8; 32]> = OnceLock::new();
    *H.get_or_init(|| keccak256(DEPOSIT_WALLET_NAME.as_bytes()))
}
fn deposit_wallet_version_hash() -> [u8; 32] {
    static H: OnceLock<[u8; 32]> = OnceLock::new();
    *H.get_or_init(|| keccak256(DEPOSIT_WALLET_VERSION.as_bytes()))
}

/// `bytes32(0)` as the 0x-prefixed hex the wire format expects.
const METADATA_ZERO_HEX: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";

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
    /// EIP-712 domain separator — constant per (exchange, chain); cached
    /// so the hot order path pays 0 instead of 8 keccaks for it (it was
    /// recomputed once for the signature and once for the orderID).
    domain_sep: [u8; 32],
    maker_word: [u8; 32],
    signer_word: [u8; 32],
    /// `builder_code` in wire form (`0x…` hex), formatted once.
    builder_hex: String,
    /// Shared with the account route's v1 signer. The order hot path performs
    /// one relaxed fetch-add and never enters a global registry.
    salt_sequence: Arc<AccountSaltSequence>,
}

impl OrderSignerV2 {
    pub fn new(
        private_key_hex: &str,
        neg_risk: bool,
        sig_type: SignatureType,
        builder_code_hex: &str,
    ) -> Result<Self> {
        Self::new_with_salt_sequence(
            private_key_hex,
            neg_risk,
            sig_type,
            builder_code_hex,
            Arc::new(AccountSaltSequence::new()),
        )
    }

    pub(crate) fn new_with_salt_sequence(
        private_key_hex: &str,
        neg_risk: bool,
        sig_type: SignatureType,
        builder_code_hex: &str,
        salt_sequence: Arc<AccountSaltSequence>,
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

        let domain_sep = compute_domain_separator_v2(&exchange_address);
        let builder_hex = format!("0x{}", hex::encode(builder_code));
        Ok(Self {
            signing_key,
            exchange_address, builder_code, signature_type: sig_type,
            funder: None,
            maker_word: address_to_bytes32(&maker_address),
            signer_word: address_to_bytes32(&signer_address),
            domain_sep, builder_hex, salt_sequence, signer_address, maker_address,
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
    ///   * REST gap recovery (`/data/trades?maker_address=…`)
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
            self.maker_word = address_to_bytes32(&f);
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
        validate_order_v2_numbers(order)?;
        self.sign_digest(&self.order_digest(order))
    }

    /// Sign a precomputed digest — split out so the build paths hash the
    /// order struct exactly once for both signature and orderID.
    fn sign_digest(&self, digest: &[u8; 32]) -> Result<String> {
        let _t = crate::latency::TimedStage::new("polymarket.signer_v2.sign");
        let (sig, recid) = self.signing_key
            .sign_prehash_recoverable(digest)
            .map_err(|e| anyhow!("Signing failed: {}", e))?;
        let mut sig_bytes = [0u8; 65];
        sig_bytes[..64].copy_from_slice(&sig.to_bytes());
        sig_bytes[64] = recid.to_byte() + 27;
        Ok(prefixed_hex(&sig_bytes))
    }

    pub fn order_digest(&self, order: &OrderV2) -> [u8; 32] {
        eip712_digest(&self.domain_sep, &order_v2_struct_hash(order))
    }

    pub fn order_hash_hex(&self, order: &OrderV2) -> String {
        prefixed_hex(&self.order_digest(order))
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
        validate_signing_inputs(token_id, price, size)?;
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
            salt: self.salt_sequence.next_decimal(),
            maker: self.maker_address.clone(),
            signer: self.signer_address.clone(),
            token_id: token_id.to_string(),
            maker_amount,
            taker_amount,
            side: clob_side,
            signature_type: self.signature_type as u8,
            timestamp: now_ms.to_string(),
            metadata: METADATA_ZERO_HEX.to_string(),
            builder: self.builder_hex.clone(),
            // wire-only
            taker: "0x0000000000000000000000000000000000000000".to_string(),
            expiration: "0".to_string(),
        };
        validate_order_v2_numbers(&order)?;

        let digest = eip712_digest(&self.domain_sep, &order_v2_struct_hash_prepared(
            &order, self.maker_word, self.signer_word, self.builder_code));
        let signature = self.sign_digest(&digest)?;
        let order_hash = prefixed_hex(&digest);
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
        validate_signing_inputs(token_id, price, size)?;
        let (maker_amount, taker_amount) = compute_amounts(price, size, side);
        let clob_side = match side {
            crate::types::Side::Buy => 0u8,
            crate::types::Side::Sell => 1u8,
        };
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let salt = self.salt_sequence.next_decimal();
        let order = OrderV2 {
            salt,
            maker: funder.to_string(),
            signer: funder.to_string(),
            token_id: token_id.to_string(),
            maker_amount,
            taker_amount,
            side: clob_side,
            signature_type: SignatureType::Poly1271 as u8,
            timestamp: now_ms.to_string(),
            metadata: METADATA_ZERO_HEX.to_string(),
            builder: self.builder_hex.clone(),
            taker: "0x0000000000000000000000000000000000000000".to_string(),
            expiration: "0".to_string(),
        };
        validate_order_v2_numbers(&order)?;

        // The ERC-7739 signature and the orderID share the same struct
        // hash — compute it once (previously the whole struct was hashed
        // a second time inside `order_hash_hex`).
        // The cached word belongs to this immutable signer; public callers
        // may also supply another wallet, which must retain its own identity.
        let funder_word = if self.funder.as_deref() == Some(funder) {
            self.maker_word
        } else { address_to_bytes32(funder) };
        let contents_hash = order_v2_struct_hash_prepared(
            &order, funder_word, funder_word, self.builder_code);
        let signature = self.sign_order_poly1271(contents_hash, funder_word)?;
        let order_hash = prefixed_hex(&eip712_digest(&self.domain_sep, &contents_hash));
        Ok(SignedOrderV2 { order, signature, order_hash })
    }

    /// ERC-7739-wrapped POLY_1271 order signature. Mirrors
    /// rs-clob-client-v2 `sign_poly1271_order` and py-clob-client-v2
    /// `_build_poly_1271_order_signature`:
    /// `0x || inner(65) || appDomainSep(32) || contentsHash(32) ||
    ///  ORDER_TYPE_STRING || uint16(len)`. The wallet "app domain"
    /// verifyingContract is `order.signer` (= the deposit wallet).
    fn sign_order_poly1271(&self, contents_hash: [u8; 32], funder_word: [u8; 32]) -> Result<String> {
        let app_domain_sep = self.domain_separator();

        let tds_hash = hash_words(&[
            solady_order_type_hash(), contents_hash, deposit_wallet_name_hash(),
            deposit_wallet_version_hash(), u256_bytes(CHAIN_ID as u128), funder_word, [0; 32],
        ]);
        let digest = eip712_digest(&app_domain_sep, &tds_hash);

        let (sig, recid) = self
            .signing_key
            .sign_prehash_recoverable(&digest)
            .map_err(|e| anyhow!("Signing failed: {}", e))?;
        let mut inner = [0u8; 65];
        inner[..64].copy_from_slice(&sig.to_bytes());
        inner[64] = recid.to_byte() + 27;

        let type_str = ORDER_TYPE_STRING.as_bytes();
        let type_len = u16::try_from(type_str.len()).expect("order type string fits u16");

        let mut wrapped = String::with_capacity(2 + 2 * (65 + 32 + 32 + type_str.len() + 2));
        wrapped.push_str("0x");
        append_hex(&mut wrapped, &inner);
        append_hex(&mut wrapped, &app_domain_sep);
        append_hex(&mut wrapped, &contents_hash);
        append_hex(&mut wrapped, type_str);
        append_hex(&mut wrapped, &type_len.to_be_bytes());
        Ok(wrapped)
    }

    fn domain_separator(&self) -> [u8; 32] {
        self.domain_sep
    }
}

/// v2 domain separator (version "2"). Constant per exchange address;
/// computed once in `OrderSignerV2::new` and cached.
fn compute_domain_separator_v2(exchange_address: &str) -> [u8; 32] {
    let type_hash = eip712_domain_type_hash();
    let name_hash = keccak256(b"Polymarket CTF Exchange");
    let version_hash = keccak256(b"2");
    let chain_id = u256_bytes(CHAIN_ID as u128);
    let contract = address_to_bytes32(exchange_address);

    let mut buf = Vec::with_capacity(5 * 32);
    buf.extend_from_slice(&type_hash);
    buf.extend_from_slice(&name_hash);
    buf.extend_from_slice(&version_hash);
    buf.extend_from_slice(&chain_id);
    buf.extend_from_slice(&contract);
    keccak256(&buf)
}

// ════════════════════════════════════════════════════════════════
// Struct hash — field order MUST match `order_v2_type_hash`
// (ctfExchangeV2TypedData.ts `CTF_EXCHANGE_V2_ORDER_STRUCT`)
// ════════════════════════════════════════════════════════════════

fn order_v2_struct_hash(order: &OrderV2) -> [u8; 32] {
    hash_order_words(order, address_to_bytes32(&order.maker), address_to_bytes32(&order.signer),
        parse_bytes32(&order.metadata).unwrap_or([0; 32]), parse_bytes32(&order.builder).unwrap_or([0; 32]))
}

/// Built orders use constructor-validated immutable addresses/builder and zero
/// metadata. Arbitrary prebuilt orders continue through the compatibility hash.
fn order_v2_struct_hash_prepared(order: &OrderV2, maker: [u8; 32], signer: [u8; 32], builder: [u8; 32]) -> [u8; 32] {
    hash_order_words(order, maker, signer, [0; 32], builder)
}

fn hash_order_words(order: &OrderV2, maker: [u8; 32], signer: [u8; 32], metadata: [u8; 32], builder: [u8; 32]) -> [u8; 32] {
    hash_words(&[
        order_v2_type_hash(), u256_from_decimal(&order.salt), maker, signer,
        u256_from_decimal(&order.token_id), u256_from_decimal(&order.maker_amount),
        u256_from_decimal(&order.taker_amount), u256_bytes(order.side as u128),
        u256_bytes(order.signature_type as u128), u256_from_decimal(&order.timestamp), metadata, builder,
    ])
}

fn hash_words<const N: usize>(words: &[[u8; 32]; N]) -> [u8; 32] {
    let mut h = Keccak256::new();
    for word in words { h.update(word); }
    h.finalize().into()
}

fn eip712_digest(domain: &[u8; 32], contents: &[u8; 32]) -> [u8; 32] {
    let mut bytes = [0u8; 66];
    bytes[..2].copy_from_slice(&[0x19, 0x01]);
    bytes[2..34].copy_from_slice(domain);
    bytes[34..].copy_from_slice(contents);
    keccak256(&bytes)
}

fn append_hex(target: &mut String, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes { target.push(HEX[(byte >> 4) as usize] as char); target.push(HEX[(byte & 15) as usize] as char); }
}
fn prefixed_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("0x");
    append_hex(&mut out, bytes);
    out
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
    let text = addr.strip_prefix("0x").unwrap_or(addr);
    let mut word = [0; 32];
    // Preserve the compatibility parser's zero fallback and first-32-byte
    // truncation, without allocating a temporary decoded Vec.
    if text.len() % 2 != 0 || !text.bytes().all(|b| b.is_ascii_hexdigit()) { return word; }
    let n = (text.len() / 2).min(32);
    let _ = hex::decode_to_slice(&text[..n * 2], &mut word[32 - n..]);
    word
}

fn u256_from_decimal(s: &str) -> [u8; 32] {
    let s = s.trim_start_matches('0');
    if s.is_empty() { return [0; 32]; }
    if let Ok(val) = s.parse::<u128>() { return u256_bytes(val); }
    let mut result = [0u8; 32];
    // Validated decimal -> base-256 directly. No heap digits or remove(0).
    // Existing arbitrary prebuilt digest callers retain modulo-uint256 behavior.
    for digit in s.bytes() {
        let mut carry = (digit - b'0') as u16;
        for byte in result.iter_mut().rev() {
            carry += *byte as u16 * 10;
            *byte = carry as u8;
            carry >>= 8;
        }
    }
    result
}

fn validate_order_v2_numbers(order: &OrderV2) -> Result<()> {
    validate_u256_decimal("salt", &order.salt, true)?;
    validate_u256_decimal("token_id", &order.token_id, false)?;
    validate_u256_decimal("maker_amount", &order.maker_amount, false)?;
    validate_u256_decimal("taker_amount", &order.taker_amount, false)?;
    validate_u256_decimal("timestamp", &order.timestamp, false)?;
    validate_u256_decimal("expiration", &order.expiration, true)?;
    if order.side > 1 {
        return Err(anyhow!("Invalid side: {}", order.side));
    }
    if order.signature_type > SignatureType::Poly1271 as u8 {
        return Err(anyhow!("Invalid signature_type: {}", order.signature_type));
    }
    Ok(())
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

    // Independent decimal long division used by the previous implementation.
    fn reference_decimal(s: &str) -> [u8; 32] {
        let mut out = [0; 32];
        let mut digits: Vec<u8> = s.bytes().map(|b| b - b'0').collect();
        for byte in out.iter_mut().rev() {
            let mut remainder = 0u32;
            for digit in &mut digits {
                let value = remainder * 10 + *digit as u32;
                *digit = (value / 256) as u8;
                remainder = value % 256;
            }
            *byte = remainder as u8;
        }
        out
    }

    #[test]
    fn allocation_free_decimal_encoding_matches_full_uint256_reference() {
        for text in ["0", "0000", "1", "000340282366920938463463374607431768211455",
            "340282366920938463463374607431768211456",
            "115792089237316195423570985008687907853269984665640564039457584007913129639935"] {
            assert_eq!(u256_from_decimal(text), reference_decimal(text));
        }
        let mut seed = 0x123456789abcdefu64;
        for _ in 0..512 {
            let mut text = String::from("1");
            for _ in 0..75 {
                seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17;
                text.push((b'0' + (seed % 10) as u8) as char);
            }
            assert_eq!(u256_from_decimal(&text), reference_decimal(&text));
        }
    }

    #[test]
    fn prepared_signer_words_preserve_wallet_identity_digest_and_signature() {
        let key = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
        let token = "50303916472381649224674364401111317755258653723694532482715411789597335197187";
        for kind in [SignatureType::Eoa, SignatureType::Poly1271] {
            for neg_risk in [false, true] {
                let signer = OrderSignerV2::new(key, neg_risk, kind, "0x1111111111111111111111111111111111111111111111111111111111111111")
                    .unwrap().with_funder("0x1234567890123456789012345678901234567890");
                for side in [crate::types::Side::Buy, crate::types::Side::Sell] {
                    let signed = signer.build_signed_order_dispatch(token, 0.37, 20.0, side).unwrap();
                    assert_eq!(signed.order_hash, signer.order_hash_hex(&signed.order));
                    if matches!(kind, SignatureType::Eoa) {
                        assert_eq!(signed.signature, signer.sign_order(&signed.order).unwrap());
                    } else {
                        let alternate = signer.build_signed_order_poly1271(
                            "0x9999999999999999999999999999999999999999", token, 0.37, 20.0, side).unwrap();
                        assert_eq!(alternate.order_hash, signer.order_hash_hex(&alternate.order));
                        assert_ne!(alternate.order.maker, signed.order.maker);
                        let bytes = hex::decode(&alternate.signature[2..]).unwrap();
                        assert_eq!(&bytes[65..97], &signer.domain_sep);
                        assert_eq!(&bytes[97..129], &order_v2_struct_hash(&alternate.order));
                        assert_eq!(&bytes[129..bytes.len() - 2], ORDER_TYPE_STRING.as_bytes());
                        assert_eq!(&bytes[bytes.len() - 2..], &(ORDER_TYPE_STRING.len() as u16).to_be_bytes());
                    }
                }
            }
        }
    }

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
    fn v2_signer_rejects_invalid_numeric_inputs_and_prebuilt_fields() {
        let signer = OrderSignerV2::new(
            "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
            false,
            SignatureType::Eoa,
            "",
        ).unwrap();
        assert!(signer.build_signed_order("invalid-token", 0.5, 1.0,
            crate::types::Side::Buy).is_err());
        assert!(signer.build_signed_order("1", f64::INFINITY, 1.0,
            crate::types::Side::Buy).is_err());

        let mut signed = signer.build_signed_order("1", 0.5, 1.0,
            crate::types::Side::Buy).unwrap();
        signed.order.timestamp = "not-a-number".to_string();
        assert!(signer.sign_order(&signed.order).is_err());
    }

    #[test]
    fn order_type_string_matches_typehash() {
        // The appended contentsType string MUST hash to the same typehash
        // used in the struct hash, or the ERC-7739 wrap is invalid.
        assert_eq!(keccak256(ORDER_TYPE_STRING.as_bytes()), order_v2_type_hash());
    }

    /// Lock-in: v2 **EOA (signatureType=0)** signing vectors, cross-checked
    /// against the official Polymarket v2 order-utils (py-clob-client-v2
    /// `ctf_exchange_v2_typed_data.py` / `exchange_order_builder_v2.py`).
    ///
    /// The values below were computed independently (raw keccak256 over the
    /// v2 struct/domain, NOT via this code path) so they cross-validate the
    /// implementation rather than merely pinning it:
    ///   * v2 Order typehash — keccak256 of the 11-field v2 struct string.
    ///   * domain separator for `name="Polymarket CTF Exchange"`,
    ///     `version="2"`, chainId=137, verifyingContract=CTF_EXCHANGE_V2.
    ///   * struct hash + EIP-712 digest for an EOA order with a FIXED
    ///     timestamp (build_signed_order stamps live ms, so the fixture
    ///     builds `OrderV2` directly to keep the digest deterministic).
    ///
    /// EOA property: `maker == signer == EOA` and `signatureType == 0`.
    /// If this ever flips without an intended v2 struct/domain change, the
    /// server will reject every EOA order — stop and investigate.
    #[test]
    fn v2_eoa_signing_vectors_stable() {
        // Typehash of the v2 Order struct string.
        assert_eq!(
            hex::encode(order_v2_type_hash()),
            "bb86318a2138f5fa8ae32fbe8e659f8fcf13cc6ae4014a707893055433818589",
        );

        // Hardhat account #0 key → EOA 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266.
        // CTFExchangeV2 (not neg-risk), no builder code.
        let signer = OrderSignerV2::new(
            "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
            false,
            SignatureType::Eoa,
            "",
        )
        .unwrap();

        // Domain separator (version "2", CTF_EXCHANGE_V2).
        assert_eq!(
            hex::encode(signer.domain_separator()),
            "3264e159346253e26a64e00b69032db0e7d32f94628de3e6eecb50304d7af3d2",
        );

        let eoa = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";
        // Fixed-timestamp EOA order (BUY 100 @ 0.55): maker == signer == EOA,
        // signatureType 0, metadata/builder zeroed.
        let order = OrderV2 {
            salt: "1234567890".to_string(),
            maker: eoa.to_string(),
            signer: eoa.to_string(),
            token_id:
                "50303916472381649224674364401111317755258653723694532482715411789597335197187"
                    .to_string(),
            maker_amount: "55000000".to_string(),
            taker_amount: "100000000".to_string(),
            side: 0,             // BUY
            signature_type: 0,   // EOA
            timestamp: "1700000000000".to_string(),
            metadata: format!("0x{}", hex::encode([0u8; 32])),
            builder: format!("0x{}", hex::encode([0u8; 32])),
            taker: "0x0000000000000000000000000000000000000000".to_string(),
            expiration: "0".to_string(),
        };
        assert_eq!(order.maker, order.signer, "EOA: maker must equal signer");
        assert_eq!(order.signature_type, SignatureType::Eoa as u8);

        assert_eq!(
            hex::encode(order_v2_struct_hash(&order)),
            "f47f6bdd8279fb7d314db4f7632d7c0ad8b13d86fece1607f16b9f892a3aab82",
        );
        assert_eq!(
            hex::encode(signer.order_digest(&order)),
            "0f61ee6f1fb96526390b7c70417e7aa72215d532eba63aabf62a06c0b6f18e76",
        );
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
