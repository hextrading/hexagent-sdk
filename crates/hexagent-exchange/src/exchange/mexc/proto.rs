//! MEXC WebSocket protobuf message definitions.
//! Hand-written prost structs matching mexcdevelop/websocket-proto.

/// Orderbook item: price + quantity
#[derive(Clone, PartialEq, prost::Message)]
pub struct LimitDepthItem {
    #[prost(string, tag = "1")]
    pub price: String,
    #[prost(string, tag = "2")]
    pub quantity: String,
}

/// PublicLimitDepthsV3Api — 5/10/20 level orderbook snapshot
#[derive(Clone, PartialEq, prost::Message)]
pub struct PublicLimitDepths {
    #[prost(message, repeated, tag = "1")]
    pub asks: Vec<LimitDepthItem>,
    #[prost(message, repeated, tag = "2")]
    pub bids: Vec<LimitDepthItem>,
    #[prost(string, tag = "3")]
    pub event_type: String,
    #[prost(string, tag = "4")]
    pub version: String,
}

/// Trade item
#[derive(Clone, PartialEq, prost::Message)]
pub struct AggreDealsItem {
    #[prost(string, tag = "1")]
    pub price: String,
    #[prost(string, tag = "2")]
    pub quantity: String,
    /// 1 = Buy, 2 = Sell
    #[prost(int32, tag = "3")]
    pub trade_type: i32,
    #[prost(int64, tag = "4")]
    pub time: i64,
}

/// PublicAggreDealsV3Api — aggregated trades
#[derive(Clone, PartialEq, prost::Message)]
pub struct PublicAggreDeals {
    #[prost(message, repeated, tag = "1")]
    pub deals: Vec<AggreDealsItem>,
    #[prost(string, tag = "2")]
    pub event_type: String,
}

/// The oneof body variants we care about
#[derive(Clone, PartialEq, prost::Oneof)]
pub enum WrapperBody {
    /// PublicLimitDepthsV3Api (field 303)
    #[prost(message, tag = "303")]
    PublicLimitDepths(PublicLimitDepths),
    /// PublicAggreDealsV3Api (field 314)
    #[prost(message, tag = "314")]
    PublicAggreDeals(PublicAggreDeals),
}

/// PushDataV3ApiWrapper — outer envelope for all MEXC WebSocket binary messages
#[derive(Clone, PartialEq, prost::Message)]
pub struct PushDataWrapper {
    /// Channel name (e.g. "spot@public.limit.depth.v3.api.pb@BTCUSDT@5")
    #[prost(string, tag = "1")]
    pub channel: String,
    /// Trading pair
    #[prost(string, optional, tag = "3")]
    pub symbol: Option<String>,
    /// Message creation time (ms)
    #[prost(int64, optional, tag = "5")]
    pub create_time: Option<i64>,
    /// Message push time (ms)
    #[prost(int64, optional, tag = "6")]
    pub send_time: Option<i64>,
    /// Body — oneof with different data types
    #[prost(oneof = "WrapperBody", tags = "303, 314")]
    pub body: Option<WrapperBody>,
}
