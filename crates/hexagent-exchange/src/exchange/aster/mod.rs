//! Aster (asterdex.com) perpetual-futures venue adapter — **V3 Pro API**.
//!
//! The V3 API is Binance-futures-shaped (REST `fapi.asterdex.com`, WS
//! `fstream.asterdex.com`) but authenticates with an **EIP-712 wallet
//! signature** instead of HMAC: every TRADE/USER_DATA/USER_STREAM request
//! carries `signer` (API agent wallet) + `nonce` (microseconds) + `signature`
//! (EIP-712 `Message(string msg)` over the urlencoded param string). The
//! signer→user mapping is established server-side when the API wallet is
//! registered, so requests don't carry the master address.
//!
//! * [`signer`] — EIP-712 `AsterSignTransaction` signing + urlencoding.
//! * [`auth`]   — credentials + network (mainnet/testnet) + host URLs.
//! * [`info`]   — REST `exchangeInfo` (tick/step size) + `depth` snapshot.
//! * [`trade`]  — [`AsterTrade`]: the `ExchangeTrade` order-execution impl.
//! * [`market`] — [`AsterMarket`]: the `ExchangeMarket` WS feed impl.
//! * [`user_feed`] — listenKey WS `ORDER_TRADE_UPDATE` → `OrderUpdate`.
//! * [`position`] — `positionRisk` → `Position`.

pub mod signer;
pub mod auth;
pub mod info;
pub mod trade;
pub mod market;
pub mod user_feed;
pub mod position;

pub use market::AsterMarket;
pub use trade::AsterTrade;
pub use position::fetch_positions;
