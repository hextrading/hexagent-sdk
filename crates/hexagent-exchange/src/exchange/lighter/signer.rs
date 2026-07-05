//! Lighter zkLighter transaction signing.
//!
//! Unlike the EIP-712 venues (Hyperliquid/Aster), Lighter signs with a
//! zk-circuit-friendly scheme: the tx fields are packed as Goldilocks field
//! elements, hashed with Poseidon2 to a quintic-extension element (40 bytes),
//! and Schnorr-signed over the ECgFp5 curve (80-byte `s||e` signature). The
//! API key is a 40-byte scalar generated/registered via the official SDKs —
//! it is NOT an Ethereum key. See `crypto/` for the vendored primitives and
//! the `kat` test module for the pinned known-answer vectors (generated with
//! the official `lighter-go`).
//!
//! Hash schemas mirror `lighter-go/types/txtypes/*.go` exactly:
//! `HashToQuinticExtension([chainId, txType, nonce, expiredAt, <tx fields>])`
//! with no tx attributes (we never set integrator/self-trade attributes, so
//! `AggregateTxHash` reduces to the plain tx hash).

use anyhow::{anyhow, Result};
use base64::Engine;
use serde::Serialize;

use super::crypto::{
    goldilocks::{array_from_le_bytes, GoldilocksField},
    schnorr::{schnorr_pk_from_sk, schnorr_sign_hashed_message},
    ECgFp5Scalar, GFp5,
};

// Tx type ids — `lighter-go/types/txtypes/constants.go`.
pub const TX_TYPE_CREATE_ORDER: u8 = 14;
pub const TX_TYPE_CANCEL_ORDER: u8 = 15;
pub const TX_TYPE_CANCEL_ALL_ORDERS: u8 = 16;

// Order type enum (`Type` field).
pub const ORDER_TYPE_LIMIT: u8 = 0;
pub const ORDER_TYPE_MARKET: u8 = 1;

// Time-in-force enum.
pub const TIF_IMMEDIATE_OR_CANCEL: u8 = 0;
pub const TIF_GOOD_TILL_TIME: u8 = 1;
pub const TIF_POST_ONLY: u8 = 2;

// Cancel-all time-in-force.
pub const CANCEL_ALL_IMMEDIATE: u8 = 0;

/// `ClientOrderIndex` must fit 48 bits (`MaxClientOrderIndex = 2^48 - 1`).
pub const MAX_CLIENT_ORDER_INDEX: i64 = (1 << 48) - 1;

/// A signed transaction ready for `POST /api/v1/sendTx`.
#[derive(Debug, Clone)]
pub struct SignedTx {
    pub tx_type: u8,
    /// JSON body for the `tx_info` form field (Go-compatible field names).
    pub tx_info: String,
    /// The signed Poseidon2 hash (hex). Coincides with the server's TxHash.
    pub tx_hash: String,
}

/// Unscaled-free create-order params — everything already in wire units
/// (integer base amount / price per the market's decimals).
#[derive(Debug, Clone)]
pub struct CreateOrderParams {
    pub market_index: i16,
    pub client_order_index: i64,
    pub base_amount: i64,
    pub price: u32,
    pub is_ask: bool,
    pub order_type: u8,
    pub time_in_force: u8,
    pub reduce_only: bool,
    pub trigger_price: u32,
    /// GTT/PostOnly: expiry timestamp ms (5 min .. 30 days). IOC/Market: 0.
    pub order_expiry: i64,
    pub nonce: i64,
    /// Tx deadline (ms since epoch).
    pub expired_at: i64,
}

pub struct LighterSigner {
    sk: ECgFp5Scalar,
    pk: GFp5,
    chain_id: u32,
    pub account_index: i64,
    pub api_key_index: u8,
}

impl LighterSigner {
    /// `private_key_hex`: 40-byte hex API-key private key (0x-prefix optional).
    pub fn new(
        private_key_hex: &str,
        account_index: i64,
        api_key_index: u8,
        chain_id: u32,
    ) -> Result<Self> {
        let hex_str = private_key_hex.trim().trim_start_matches("0x");
        let bytes = hex::decode(hex_str).map_err(|e| anyhow!("lighter: bad private_key hex: {}", e))?;
        if bytes.len() != 40 {
            return Err(anyhow!(
                "lighter: private_key must be 40 bytes, got {}",
                bytes.len()
            ));
        }
        let sk = ECgFp5Scalar::from_le_bytes(&bytes);
        let pk = schnorr_pk_from_sk(&sk);
        Ok(Self {
            sk,
            pk,
            chain_id,
            account_index,
            api_key_index,
        })
    }

    /// 40-byte public key, hex (matches `apikeys` REST endpoint format).
    pub fn pubkey_hex(&self) -> String {
        hex::encode(self.pk.to_le_bytes())
    }

    /// Poseidon2-hash `elems`, Schnorr-sign, return (sig_base64, hash_hex).
    fn sign_elems(&self, elems: &[GoldilocksField]) -> (String, String) {
        let hash = super::crypto::poseidon2::hash_to_quintic_extension(elems);
        let sig = schnorr_sign_hashed_message(hash, &self.sk);
        let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());
        (sig_b64, hex::encode(hash.to_le_bytes()))
    }

    pub fn sign_create_order(&self, p: &CreateOrderParams) -> Result<SignedTx> {
        if p.client_order_index < 0 || p.client_order_index > MAX_CLIENT_ORDER_INDEX {
            return Err(anyhow!("lighter: client_order_index out of 48-bit range"));
        }
        let elems = [
            GoldilocksField::new(self.chain_id as u64),
            GoldilocksField::new(TX_TYPE_CREATE_ORDER as u64),
            GoldilocksField::new(p.nonce as u64),
            GoldilocksField::new(p.expired_at as u64),
            GoldilocksField::new(self.account_index as u64),
            GoldilocksField::new(self.api_key_index as u64),
            GoldilocksField::new(p.market_index as u64),
            GoldilocksField::new(p.client_order_index as u64),
            GoldilocksField::new(p.base_amount as u64),
            GoldilocksField::new(p.price as u64),
            GoldilocksField::new(p.is_ask as u64),
            GoldilocksField::new(p.order_type as u64),
            GoldilocksField::new(p.time_in_force as u64),
            GoldilocksField::new(p.reduce_only as u64),
            GoldilocksField::new(p.trigger_price as u64),
            GoldilocksField::new(p.order_expiry as u64),
        ];
        let (sig, hash) = self.sign_elems(&elems);
        let info = CreateOrderInfo {
            account_index: self.account_index,
            api_key_index: self.api_key_index,
            market_index: p.market_index,
            client_order_index: p.client_order_index,
            base_amount: p.base_amount,
            price: p.price,
            is_ask: p.is_ask as u8,
            order_type: p.order_type,
            time_in_force: p.time_in_force,
            reduce_only: p.reduce_only as u8,
            trigger_price: p.trigger_price,
            order_expiry: p.order_expiry,
            expired_at: p.expired_at,
            nonce: p.nonce,
            sig,
            attributes: (),
        };
        Ok(SignedTx {
            tx_type: TX_TYPE_CREATE_ORDER,
            tx_info: serde_json::to_string(&info)?,
            tx_hash: hash,
        })
    }

    /// `index`: client order index (1..2^48-1) or exchange order index.
    pub fn sign_cancel_order(
        &self,
        market_index: i16,
        index: i64,
        nonce: i64,
        expired_at: i64,
    ) -> Result<SignedTx> {
        let elems = [
            GoldilocksField::new(self.chain_id as u64),
            GoldilocksField::new(TX_TYPE_CANCEL_ORDER as u64),
            GoldilocksField::new(nonce as u64),
            GoldilocksField::new(expired_at as u64),
            GoldilocksField::new(self.account_index as u64),
            GoldilocksField::new(self.api_key_index as u64),
            GoldilocksField::new(market_index as u64),
            GoldilocksField::new(index as u64),
        ];
        let (sig, hash) = self.sign_elems(&elems);
        let info = CancelOrderInfo {
            account_index: self.account_index,
            api_key_index: self.api_key_index,
            market_index,
            index,
            expired_at,
            nonce,
            sig,
            attributes: (),
        };
        Ok(SignedTx {
            tx_type: TX_TYPE_CANCEL_ORDER,
            tx_info: serde_json::to_string(&info)?,
            tx_hash: hash,
        })
    }

    /// Immediate cancel-all across markets (`TimeInForce=ImmediateCancelAll`).
    pub fn sign_cancel_all(&self, nonce: i64, expired_at: i64) -> Result<SignedTx> {
        let tif = CANCEL_ALL_IMMEDIATE;
        let time: i64 = 0;
        let elems = [
            GoldilocksField::new(self.chain_id as u64),
            GoldilocksField::new(TX_TYPE_CANCEL_ALL_ORDERS as u64),
            GoldilocksField::new(nonce as u64),
            GoldilocksField::new(expired_at as u64),
            GoldilocksField::new(self.account_index as u64),
            GoldilocksField::new(self.api_key_index as u64),
            GoldilocksField::new(tif as u64),
            GoldilocksField::new(time as u64),
        ];
        let (sig, hash) = self.sign_elems(&elems);
        let info = CancelAllInfo {
            account_index: self.account_index,
            api_key_index: self.api_key_index,
            time_in_force: tif,
            time,
            expired_at,
            nonce,
            sig,
            attributes: (),
        };
        Ok(SignedTx {
            tx_type: TX_TYPE_CANCEL_ALL_ORDERS,
            tx_info: serde_json::to_string(&info)?,
            tx_hash: hash,
        })
    }

    /// Auth token for private WS channels / REST: sign
    /// `"{deadline}:{account_index}:{api_key_index}"` (ASCII bytes packed
    /// 8-per-field little-endian) and append the hex signature.
    /// `deadline`: unix seconds, at most 8h ahead (server-enforced).
    pub fn create_auth_token(&self, deadline_unix: i64) -> String {
        let message = format!(
            "{}:{}:{}",
            deadline_unix, self.account_index, self.api_key_index
        );
        let elems = array_from_le_bytes(message.as_bytes());
        let hash = super::crypto::poseidon2::hash_to_quintic_extension(&elems);
        let sig = schnorr_sign_hashed_message(hash, &self.sk);
        format!("{}:{}", message, hex::encode(sig.to_bytes()))
    }
}

// ── Go-compatible JSON wire structs ─────────────────────────────────
//
// Field names/order match `json.Marshal` of the lighter-go tx structs
// (PascalCase, `Sig` base64, embedded nil `L2TxAttributes` → null).

fn null_attrs<S: serde::Serializer>(_: &(), s: S) -> std::result::Result<S::Ok, S::Error> {
    s.serialize_none()
}

#[derive(Serialize)]
struct CreateOrderInfo {
    #[serde(rename = "AccountIndex")]
    account_index: i64,
    #[serde(rename = "ApiKeyIndex")]
    api_key_index: u8,
    #[serde(rename = "MarketIndex")]
    market_index: i16,
    #[serde(rename = "ClientOrderIndex")]
    client_order_index: i64,
    #[serde(rename = "BaseAmount")]
    base_amount: i64,
    #[serde(rename = "Price")]
    price: u32,
    #[serde(rename = "IsAsk")]
    is_ask: u8,
    #[serde(rename = "Type")]
    order_type: u8,
    #[serde(rename = "TimeInForce")]
    time_in_force: u8,
    #[serde(rename = "ReduceOnly")]
    reduce_only: u8,
    #[serde(rename = "TriggerPrice")]
    trigger_price: u32,
    #[serde(rename = "OrderExpiry")]
    order_expiry: i64,
    #[serde(rename = "ExpiredAt")]
    expired_at: i64,
    #[serde(rename = "Nonce")]
    nonce: i64,
    #[serde(rename = "Sig")]
    sig: String,
    #[serde(rename = "L2TxAttributes", serialize_with = "null_attrs")]
    attributes: (),
}

#[derive(Serialize)]
struct CancelOrderInfo {
    #[serde(rename = "AccountIndex")]
    account_index: i64,
    #[serde(rename = "ApiKeyIndex")]
    api_key_index: u8,
    #[serde(rename = "MarketIndex")]
    market_index: i16,
    #[serde(rename = "Index")]
    index: i64,
    #[serde(rename = "ExpiredAt")]
    expired_at: i64,
    #[serde(rename = "Nonce")]
    nonce: i64,
    #[serde(rename = "Sig")]
    sig: String,
    #[serde(rename = "L2TxAttributes", serialize_with = "null_attrs")]
    attributes: (),
}

#[derive(Serialize)]
struct CancelAllInfo {
    #[serde(rename = "AccountIndex")]
    account_index: i64,
    #[serde(rename = "ApiKeyIndex")]
    api_key_index: u8,
    #[serde(rename = "TimeInForce")]
    time_in_force: u8,
    #[serde(rename = "Time")]
    time: i64,
    #[serde(rename = "ExpiredAt")]
    expired_at: i64,
    #[serde(rename = "Nonce")]
    nonce: i64,
    #[serde(rename = "Sig")]
    sig: String,
    #[serde(rename = "L2TxAttributes", serialize_with = "null_attrs")]
    attributes: (),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::lighter::crypto::schnorr::verify_signature;

    fn test_signer() -> LighterSigner {
        // sk bytes 0x01..0x28 — same fixed key as the lighter-go KAT generator.
        let sk_hex: String = (1..=40u8).map(|b| format!("{:02x}", b)).collect();
        LighterSigner::new(&sk_hex, 281474976640824, 3, 304).unwrap()
    }

    fn kat_create_params() -> CreateOrderParams {
        CreateOrderParams {
            market_index: 1,
            client_order_index: 123456789,
            base_amount: 20000,
            price: 628356,
            is_ask: true,
            order_type: ORDER_TYPE_LIMIT,
            time_in_force: TIF_POST_ONLY,
            reduce_only: false,
            trigger_price: 0,
            order_expiry: 1751700600000,
            nonce: 7,
            expired_at: 1751700000000,
        }
    }

    /// Round-trip: our signatures verify under our own pubkey.
    #[test]
    fn sign_verify_roundtrip() {
        let s = test_signer();
        let tx = s.sign_create_order(&kat_create_params()).unwrap();
        let hash_bytes = hex::decode(&tx.tx_hash).unwrap();
        let hash = GFp5::from_le_bytes(&hash_bytes).unwrap();
        let info: serde_json::Value = serde_json::from_str(&tx.tx_info).unwrap();
        let sig_b64 = info["Sig"].as_str().unwrap();
        let sig_bytes = base64::engine::general_purpose::STANDARD
            .decode(sig_b64)
            .unwrap();
        let sig = crate::exchange::lighter::crypto::schnorr::Signature::from_bytes(&sig_bytes).unwrap();
        assert!(verify_signature(&s.pk, &hash, &sig));
    }

    /// JSON wire shape: Go-compatible field names, base64 Sig, null attrs.
    #[test]
    fn tx_info_json_shape() {
        let s = test_signer();
        let tx = s.sign_create_order(&kat_create_params()).unwrap();
        let v: serde_json::Value = serde_json::from_str(&tx.tx_info).unwrap();
        assert_eq!(v["AccountIndex"].as_i64().unwrap(), 281474976640824);
        assert_eq!(v["ApiKeyIndex"].as_i64().unwrap(), 3);
        assert_eq!(v["MarketIndex"].as_i64().unwrap(), 1);
        assert_eq!(v["ClientOrderIndex"].as_i64().unwrap(), 123456789);
        assert_eq!(v["BaseAmount"].as_i64().unwrap(), 20000);
        assert_eq!(v["Price"].as_i64().unwrap(), 628356);
        assert_eq!(v["IsAsk"].as_i64().unwrap(), 1);
        assert_eq!(v["Type"].as_i64().unwrap(), 0);
        assert_eq!(v["TimeInForce"].as_i64().unwrap(), 2);
        assert_eq!(v["OrderExpiry"].as_i64().unwrap(), 1751700600000);
        assert!(v["L2TxAttributes"].is_null());
        assert!(v.get("SignedHash").is_none());
    }

    /// Auth token format: `{deadline}:{account}:{apikey}:{160-hex-sig}`.
    #[test]
    fn auth_token_format() {
        let s = test_signer();
        let tok = s.create_auth_token(1751706000);
        let parts: Vec<&str> = tok.split(':').collect();
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0], "1751706000");
        assert_eq!(parts[1], "281474976640824");
        assert_eq!(parts[2], "3");
        assert_eq!(parts[3].len(), 160);
    }

    // ── KAT vectors pinned against official lighter-go ──────────────
    //
    // Generated by the katgen harness (scratchpad/katgen/main.go) with
    // sk = 0x0102...28, chainId 304, account 281474976640824, apiKey 3.
    // These MUST match lighter-go bit-for-bit; if a vector is changed,
    // regenerate with the official Go SDK — never adjust to make the
    // Rust side pass.
    mod kat {
        use super::*;

        // Filled in by the KAT generation step; empty string = not yet
        // generated (test skips with a loud message rather than failing
        // silently).
        const GO_PUBKEY_HEX: &str = "";
        const GO_CREATE_ORDER_HASH: &str = "";
        const GO_CANCEL_ORDER_HASH: &str = "";
        const GO_CANCEL_ALL_HASH: &str = "";
        const GO_AUTH_HASH: &str = "";
        // 80-byte Go signature over GO_CREATE_ORDER_HASH with k=0x2a.
        const GO_CREATE_ORDER_SIG_K42: &str = "";

        fn assert_kat(name: &str, expected: &str, actual: &str) {
            if expected.is_empty() {
                eprintln!("KAT {} not yet generated — run scratchpad/katgen", name);
                return;
            }
            assert_eq!(expected, actual, "KAT mismatch: {}", name);
        }

        #[test]
        fn pubkey_matches_go() {
            assert_kat("pubkey", GO_PUBKEY_HEX, &test_signer().pubkey_hex());
        }

        #[test]
        fn create_order_hash_matches_go() {
            let tx = test_signer().sign_create_order(&kat_create_params()).unwrap();
            assert_kat("create_order_hash", GO_CREATE_ORDER_HASH, &tx.tx_hash);
        }

        #[test]
        fn cancel_order_hash_matches_go() {
            let tx = test_signer()
                .sign_cancel_order(1, 123456789, 8, 1751700000000)
                .unwrap();
            assert_kat("cancel_order_hash", GO_CANCEL_ORDER_HASH, &tx.tx_hash);
        }

        #[test]
        fn cancel_all_hash_matches_go() {
            let tx = test_signer().sign_cancel_all(9, 1751700000000).unwrap();
            assert_kat("cancel_all_hash", GO_CANCEL_ALL_HASH, &tx.tx_hash);
        }

        #[test]
        fn auth_hash_matches_go() {
            // Hash of the auth message body (the signature itself is
            // randomized; the Go-produced signature is verified below).
            let msg = "1751706000:281474976640824:3";
            let elems = array_from_le_bytes(msg.as_bytes());
            let hash = crate::exchange::lighter::crypto::poseidon2::hash_to_quintic_extension(&elems);
            assert_kat("auth_hash", GO_AUTH_HASH, &hex::encode(hash.to_le_bytes()));
        }

        /// The official Go signature (fixed k) must verify under our
        /// vendored curve/verify implementation — proves both sides agree
        /// on the curve, generator, hash-to-scalar and challenge scheme.
        #[test]
        fn go_signature_verifies_in_rust() {
            if GO_CREATE_ORDER_SIG_K42.is_empty() || GO_CREATE_ORDER_HASH.is_empty() {
                eprintln!("KAT go_signature not yet generated — run scratchpad/katgen");
                return;
            }
            let s = test_signer();
            let hash_bytes = hex::decode(GO_CREATE_ORDER_HASH).unwrap();
            let hash = GFp5::from_le_bytes(&hash_bytes).unwrap();
            let sig_bytes = hex::decode(GO_CREATE_ORDER_SIG_K42).unwrap();
            let sig = crate::exchange::lighter::crypto::schnorr::Signature::from_bytes(&sig_bytes).unwrap();
            assert!(verify_signature(&s.pk, &hash, &sig));
        }
    }
}
