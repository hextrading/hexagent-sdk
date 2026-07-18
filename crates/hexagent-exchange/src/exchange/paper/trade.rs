use anyhow::Result;
use log::info;
use std::collections::HashMap;

use crate::exchange::ExchangeTrade;
use crate::types::*;

/// Paper trading executor that simulates order fills locally.
pub struct PaperTrade {
    orders: HashMap<String, OrderRequest>,
    fill_count: u64,
}

impl PaperTrade {
    pub fn new() -> Self {
        Self {
            orders: HashMap::new(),
            fill_count: 0,
        }
    }

    pub fn fill_count(&self) -> u64 {
        self.fill_count
    }
}

impl ExchangeTrade for PaperTrade {
    fn submit_order(&mut self, order: &OrderRequest) -> Result<OrderUpdate> {
        info!(
            "[Paper] {} {} {} @ {:?} qty={}",
            order.exchange, order.side, order.symbol, order.price, order.quantity
        );

        let fill_price = order.price.unwrap_or(0.0);

        // Simulate immediate fill
        let update = OrderUpdate {
            client_order_id: order.client_order_id.clone(),
            exchange: order.exchange,
            symbol: order.symbol.clone(),
            side: order.side,
            exchange_order_id: Some(format!("paper-{}", uuid::Uuid::new_v4())),
            status: OrderStatus::Filled,
            liquidity: Some(Liquidity::Taker),
            filled_quantity: order.quantity,
            remaining_quantity: 0.0,
            avg_fill_price: fill_price,
            timestamp_ns: now_ns(),
            trade_id: None,
            order_audit: None,
            error: None,
        };

        self.fill_count += 1;
        self.orders
            .insert(order.client_order_id.clone(), order.clone());

        Ok(update)
    }

    fn cancel_order(&mut self, exchange: Exchange, client_order_id: &str) -> Result<OrderUpdate> {
        let order = self.orders.remove(client_order_id);
        Ok(OrderUpdate {
            client_order_id: client_order_id.to_string(),
            exchange,
            symbol: order.map(|o| o.symbol).unwrap_or_default(),
            side: Side::Buy, // placeholder for cancels
            exchange_order_id: None,
            status: OrderStatus::Cancelled,
            liquidity: None,
            filled_quantity: 0.0,
            remaining_quantity: 0.0,
            avg_fill_price: 0.0,
            timestamp_ns: now_ns(),
            trade_id: None,
            order_audit: None,
            error: None,
        })
    }

    fn cancel_all(&mut self, exchange: Exchange, symbol: &str) -> Result<Vec<OrderUpdate>> {
        let to_cancel: Vec<String> = self
            .orders
            .iter()
            .filter(|(_, o)| o.exchange == exchange && o.symbol == symbol)
            .map(|(id, _)| id.clone())
            .collect();

        let mut updates = Vec::new();
        for id in to_cancel {
            updates.push(self.cancel_order(exchange, &id)?);
        }
        Ok(updates)
    }

    fn name(&self) -> &str {
        "paper"
    }
}
