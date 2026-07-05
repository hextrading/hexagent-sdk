//! Lighter (zkLighter) perpetuals venue adapter.
//!
//! * [`crypto`] — vendored Goldilocks/GFp5/Poseidon2/ECgFp5/Schnorr primitives.
//! * [`signer`] — zk-native tx signing (Poseidon2 hash + Schnorr over ECgFp5).
//! * [`auth`] — credentials (API-key scalar + account/api-key index) + network.
//! * [`info`] — REST metadata (orderBookDetails, nextNonce).
//! * [`trade`] — [`LighterTrade`]: the `ExchangeTrade` sendTx impl.
//! * [`market`] — [`LighterMarket`]: the `ExchangeMarket` WS feed impl.
//! * [`user_feed`] — WS account trades/orders → `OrderUpdate`.
//! * [`position`] — REST account → `Position`.

pub mod crypto;
pub mod signer;
pub mod auth;
pub mod info;
pub mod trade;
pub mod market;
pub mod user_feed;
pub mod position;

pub use market::LighterMarket;
pub use trade::LighterTrade;
pub use position::fetch_positions;
