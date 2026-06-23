use anyhow::{anyhow, Result};
use log::info;

use crate::exchange::ExchangeTrade;
use crate::types::*;

/// Binance live order executor.
/// Currently a stub -- implement Binance REST API calls here.
pub struct BinanceTrade {
    // api_key: String,
    // api_secret: String,
}

impl BinanceTrade {
    pub fn new() -> Self {
        Self {}
    }
}

impl ExchangeTrade for BinanceTrade {
    fn submit_order(&mut self, order: &OrderRequest) -> Result<OrderUpdate> {
        info!(
            "[BinanceTrade] Submit {:?} {} {} @ {:?} qty={}",
            order.exchange, order.side, order.symbol, order.price, order.quantity
        );
        // TODO: Implement Binance REST API order submission
        // - Sign request with HMAC-SHA256
        // - POST to https://api.binance.com/api/v3/order
        Err(anyhow!("Binance live execution not yet implemented"))
    }

    fn cancel_order(&mut self, exchange: Exchange, client_order_id: &str) -> Result<OrderUpdate> {
        info!("[BinanceTrade] Cancel {} on {:?}", client_order_id, exchange);
        Err(anyhow!("Binance live cancel not yet implemented"))
    }

    fn cancel_all(&mut self, exchange: Exchange, symbol: &str) -> Result<Vec<OrderUpdate>> {
        info!("[BinanceTrade] Cancel all {} on {:?}", symbol, exchange);
        Err(anyhow!("Binance live cancel-all not yet implemented"))
    }

    fn name(&self) -> &str {
        "binance-live"
    }
}
