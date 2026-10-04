// Exact wire representation extracted from baseline trade.rs.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(untagged)]
pub(crate) enum PolyOrderBody {
    V2(WireBodyV2),
}

/// One-pass `DELETE /order` body for the hot cancel path.
#[derive(serde::Serialize)]
struct CancelBody<'a> {
    #[serde(rename = "orderID")]
    order_id: &'a str,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct WireBodyV2 {
    pub owner: String,
    #[serde(rename = "orderType")]
    pub order_type: &'static str,
    #[serde(rename = "postOnly")]
    pub post_only: bool,
    #[serde(rename = "deferExec")]
    pub defer_exec: bool,
    pub order: WireOrderV2,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct WireOrderV2 {
    pub salt: u64,
    pub maker: String,
    pub signer: String,
    pub taker: String,
    #[serde(rename = "tokenId")]
    pub token_id: String,
    #[serde(rename = "makerAmount")]
    #[serde(serialize_with = "decimal_string")]
    pub maker_amount: u128,
    #[serde(rename = "takerAmount")]
    #[serde(serialize_with = "decimal_string")]
    pub taker_amount: u128,
    pub side: &'static str,
    #[serde(rename = "signatureType")]
    pub signature_type: u8,
    #[serde(serialize_with = "decimal_string")]
    pub timestamp: u64,
    pub expiration: String,
    pub metadata: String,
    pub builder: String,
    pub signature: String,
}

fn decimal_string<T: std::fmt::Display, S: serde::Serializer>(value: &T, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(value)
}
