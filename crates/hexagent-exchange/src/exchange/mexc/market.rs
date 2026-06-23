//! MEXC WebSocket market feed — 5-level orderbook depth + trades (protobuf).
//! Endpoint: wss://wbs.mexc.com/ws
//! Channels:
//!   spot@public.limit.depth.v3.api.pb@{SYMBOL}@5 — top 5 levels
//!   spot@public.aggre.deals.v3.api.pb@100ms@{SYMBOL} — aggregated trades
//! Symbols: "BTCUSDT" format (no separator).
//! Data is pushed as binary protobuf messages (PushDataV3ApiWrapper).

use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use prost::Message as ProstMessage;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

use crate::exchange::ExchangeMarket;
use crate::types::*;
use super::proto::{PushDataWrapper, WrapperBody};

const MEXC_WS_URL: &str = "wss://wbs-api.mexc.com/ws";
const PING_INTERVAL: Duration = Duration::from_secs(15);
/// Per-task read-side stall watchdog — see binance/market.rs.
const STALE_THRESHOLD: Duration = Duration::from_secs(30);

pub struct MexcMarket {
    symbols: Vec<String>,
    event_rx: Option<crossbeam_channel::Receiver<MarketEvent>>,
    ws_shutdown: Arc<AtomicBool>,
}

impl MexcMarket {
    pub fn new() -> Self {
        Self {
            symbols: Vec::new(),
            event_rx: None,
            ws_shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
}

async fn mexc_ws_task(
    symbols: Vec<String>,
    event_tx: crossbeam_channel::Sender<MarketEvent>,
    shutdown: Arc<AtomicBool>,
) {
    let mut backoff = crate::exchange::ReconnectBackoff::new(200, 30_000);

    loop {
        if shutdown.load(Ordering::Relaxed) { break; }

        info!("[MEXC] Connecting to {}", MEXC_WS_URL);
        let stream = match tokio_tungstenite::connect_async(MEXC_WS_URL).await {
            Ok((s, _)) => s,
            Err(e) => {
                let delay = backoff.next_delay();
                warn!("[MEXC] WS connect failed: {}, retry in {:.1}s", e, delay.as_secs_f64());
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        backoff.reset();
        let (mut write, mut read) = stream.split();

        let params: Vec<String> = symbols.iter()
            .flat_map(|s| vec![
                format!("spot@public.limit.depth.v3.api.pb@{}@5", s),
                format!("spot@public.aggre.deals.v3.api.pb@100ms@{}", s),
            ])
            .collect();
        let sub = serde_json::json!({"method": "SUBSCRIPTION", "params": params});
        if let Err(e) = write.send(Message::Text(sub.to_string())).await {
            warn!("[MEXC] subscribe failed: {}", e);
            continue;
        }
        info!("[MEXC] Connected, subscribed to {:?}", symbols);

        let mut ping_interval = tokio::time::interval(PING_INTERVAL);
        ping_interval.tick().await;

        loop {
            tokio::select! {
                biased;
                _ = ping_interval.tick() => {
                    if let Err(e) = write.send(Message::Text(r#"{"method":"PING"}"#.to_string())).await {
                        warn!("[MEXC] Ping send failed: {}", e);
                        break;
                    }
                }
                read_result = tokio::time::timeout(STALE_THRESHOLD, read.next()) => {
                    let msg = match read_result {
                        Ok(Some(Ok(m))) => m,
                        Ok(Some(Err(e))) => { warn!("[MEXC] WS read error: {}", e); break; }
                        Ok(None) => { warn!("[MEXC] WS closed"); break; }
                        Err(_elapsed) => {
                            warn!("[MEXC] No message for {:.0}s (stall watchdog) — reconnecting",
                                STALE_THRESHOLD.as_secs_f64());
                            break;
                        }
                    };
                    match msg {
                        Message::Binary(data) => {
                            let wrapper = match PushDataWrapper::decode(data.as_slice()) {
                                Ok(w) => w,
                                Err(_) => continue,
                            };
                            let symbol = wrapper.symbol.as_deref().unwrap_or("").to_string();
                            let ts_ms = wrapper.send_time.unwrap_or(0) as u64;

                            match wrapper.body {
                                Some(WrapperBody::PublicLimitDepths(depth)) => {
                                    let bids: Vec<PriceLevel> = depth.bids.iter()
                                        .filter_map(|item| {
                                            Some(PriceLevel {
                                                price: item.price.parse().ok()?,
                                                quantity: item.quantity.parse().ok()?,
                                            })
                                        })
                                        .collect();
                                    let asks: Vec<PriceLevel> = depth.asks.iter()
                                        .filter_map(|item| {
                                            Some(PriceLevel {
                                                price: item.price.parse().ok()?,
                                                quantity: item.quantity.parse().ok()?,
                                            })
                                        })
                                        .collect();
                                    if bids.is_empty() || asks.is_empty() { continue; }
                                    let event = MarketEvent::OrderBook(OrderBookSnapshot {
                                        exchange: Exchange::Mexc,
                                        symbol,
                                        bids,
                                        asks,
                                        exchange_timestamp_ns: ts_ms * 1_000_000,
                                        local_timestamp_ns: now_ns(),
                                    });
                                    if event_tx.send(event).is_err() { return; }
                                }
                                Some(WrapperBody::PublicAggreDeals(deals)) => {
                                    for deal in &deals.deals {
                                        let price: f64 = match deal.price.parse() {
                                            Ok(p) if p > 0.0 => p,
                                            _ => continue,
                                        };
                                        let quantity: f64 = deal.quantity.parse().unwrap_or(0.0);
                                        let side = if deal.trade_type == 1 { Side::Buy } else { Side::Sell };
                                        let event = MarketEvent::Trade(TradeTick {
                                            exchange: Exchange::Mexc,
                                            symbol: symbol.clone(),
                                            price,
                                            quantity,
                                            side,
                                            exchange_timestamp_ns: deal.time as u64 * 1_000_000,
                                            local_timestamp_ns: now_ns(),
                                        });
                                        if event_tx.send(event).is_err() { return; }
                                    }
                                }
                                _ => {}
                            }
                        }
                        Message::Text(_) => {}
                        Message::Ping(payload) => {
                            let _ = write.send(Message::Pong(payload)).await;
                        }
                        Message::Close(_) => {
                            warn!("[MEXC] WebSocket closed");
                            break;
                        }
                        _ => {}
                    }
                }
            }
            if shutdown.load(Ordering::Relaxed) { return; }
        }

        if shutdown.load(Ordering::Relaxed) { break; }
        let delay = backoff.next_delay();
        tokio::time::sleep(delay).await;
    }
    info!("[MEXC] WS task exiting");
}

impl ExchangeMarket for MexcMarket {
    fn connect(&mut self) -> Result<()> {
        let (event_tx, event_rx) = crossbeam_channel::unbounded::<MarketEvent>();
        self.event_rx = Some(event_rx);
        // Per-task shutdown Arc — see binance/market.rs.
        let shutdown = Arc::new(AtomicBool::new(false));
        self.ws_shutdown = shutdown.clone();
        let symbols = self.symbols.clone();
        crate::async_rt::handle().spawn(mexc_ws_task(symbols, event_tx, shutdown));
        Ok(())
    }

    fn subscribe(&mut self, symbols: &[String]) -> Result<()> {
        self.symbols = symbols.to_vec();
        Ok(())
    }

    fn next_event(&mut self) -> Result<Option<MarketEvent>> {
        let rx = self.event_rx.as_ref().ok_or_else(|| anyhow!("Not connected"))?;
        match rx.try_recv() {
            Ok(event) => Ok(Some(event)),
            Err(crossbeam_channel::TryRecvError::Empty) => Ok(None),
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                Err(anyhow!("MEXC WS task ended unexpectedly"))
            }
        }
    }

    fn disconnect(&mut self) {
        self.ws_shutdown.store(true, Ordering::Relaxed);
        self.event_rx = None;
        info!("[MEXC] Disconnected");
    }

    fn name(&self) -> &str { "mexc" }
}
