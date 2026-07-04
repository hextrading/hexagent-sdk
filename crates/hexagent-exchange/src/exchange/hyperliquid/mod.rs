//! Hyperliquid perpetuals venue adapter.
//!
//! * [`signer`] — L1-action phantom-agent EIP-712 signing.
//! * [`info`] — REST `/info` queries (meta, l2Book, clearinghouseState, …).
//! * [`auth`] — credentials + network (mainnet/testnet) + host URLs.
//! * [`trade`] — [`HyperliquidTrade`]: the `ExchangeTrade` order-execution impl.
//! * [`market`] — [`HyperliquidMarket`]: the `ExchangeMarket` WS feed impl.
//! * [`user_feed`] — WS `userFills`/`orderUpdates` → `OrderUpdate`.
//! * [`position`] — `clearinghouseState` → `Position`.

pub mod signer;
pub mod info;
pub mod auth;
pub mod trade;
pub mod market;
pub mod user_feed;
pub mod position;

pub use market::HyperliquidMarket;
pub use trade::HyperliquidTrade;
pub use position::fetch_positions;
