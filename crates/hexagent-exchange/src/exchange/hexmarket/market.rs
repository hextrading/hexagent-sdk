use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use rust_decimal::prelude::ToPrimitive;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

use super::sdk::{HexClient, HexClientConfig};
use super::sdk::types::ListEventsParams;

use crate::exchange::ExchangeMarket;
use crate::types::*;

/// Interval between keepalive pings.
const PING_INTERVAL: Duration = Duration::from_secs(25);
/// Per-task read-side stall watchdog. Hexmarket pushes book/trade
/// updates frequently during active markets; 60 s of silence is
/// anomalous. See `binance/market.rs` for the rationale.
const STALE_THRESHOLD: Duration = Duration::from_secs(60);

/// Interval between heartbeat QuoteTicks (1 second in nanoseconds).
/// Ensures the engine's quote_interval timer keeps firing even when the market is quiet.
const QUOTE_HEARTBEAT_NS: u64 = 1_000_000_000;

/// Tracks a subscribed outcome
#[allow(dead_code)]
struct OutcomeState {
    outcome_id: String,
    market_id: String,
    label: String,
    event_slug: String,
}

pub struct HexmarketMarket {
    /// Maps outcome_id → OutcomeState
    outcomes: HashMap<String, OutcomeState>,
    /// Symbol configs from subscribe() — event slugs
    symbols: Vec<String>,
    /// Number of loaded events
    event_count: usize,
    /// Number of loaded markets
    market_count: usize,
    event_rx: Option<crossbeam_channel::Receiver<MarketEvent>>,
    ws_shutdown: Arc<AtomicBool>,
    pending_events: VecDeque<MarketEvent>,
    /// Timestamp of last heartbeat QuoteTick (nanoseconds).
    last_heartbeat_ns: u64,
    /// SDK client for REST API calls
    client: HexClient,
    /// API URL prefix
    #[allow(dead_code)]
    api_url_prefix: String,
    /// WebSocket URL
    wss_url: String,
}

/// Convert a SDK MarketDetail into a BinaryOption instrument.
fn market_detail_to_binary_option(
    event: &super::sdk::HexEvent,
    md: &super::sdk::MarketDetail,
) -> BinaryOption {
    let outcome_ids: Vec<String> = md.outcomes.iter().map(|o| o.id.to_string()).collect();
    let outcome_labels: Vec<String> = md.outcomes.iter().map(|o| o.label.clone()).collect();
    let outcome_prices: Vec<String> = md.outcomes
        .iter()
        .map(|o| {
            o.price
                .map(|p| format!("{:.2}", p))
                .unwrap_or_else(|| "0.00".to_string())
        })
        .collect();
    let tick_size = md.market.price_increment
        .and_then(|p| p.to_f64())
        .unwrap_or(0.001);
    let liquidity: f64 = md.outcomes
        .iter()
        .map(|o| o.liquidity.and_then(|l| l.to_f64()).unwrap_or(0.0))
        .sum();
    let volume: f64 = md.outcomes
        .iter()
        .map(|o| o.total_volume.and_then(|v| v.to_f64()).unwrap_or(0.0))
        .sum();

    BinaryOption {
        exchange: Exchange::Hexmarket,
        id: md.market.id.to_string(),
        question: md.market.title.clone(),
        condition_id: md.market.id.to_string(),
        slug: event.slug.clone(),
        clob_token_ids: outcome_ids,
        outcomes: outcome_labels,
        outcome_prices,
        active: md.market.status == "active",
        closed: md.market.status == "resolved" || md.market.status == "voided",
        volume,
        liquidity,
        tick_size,
        order_min_size: 1.0,
        group_item_title: md.market.title.clone(),
        event_start_time: String::new(),
        base_fee: 0,
        fee_exponent: 0.0,
        fee_rate: 0.0,
    }
}

impl HexmarketMarket {
    pub fn new(api_url_prefix: &str, wss_url: &str) -> Self {
        use super::auth::{api_url_prefix_or_default, wss_url_or_default};
        let api_url_prefix = api_url_prefix_or_default(api_url_prefix).to_string();
        let wss_url = wss_url_or_default(wss_url).to_string();

        let client = HexClient::new(HexClientConfig {
            api_url: api_url_prefix.clone(),
        });

        Self {
            outcomes: HashMap::new(),
            symbols: Vec::new(),
            event_count: 0,
            market_count: 0,
            event_rx: None,
            ws_shutdown: Arc::new(AtomicBool::new(false)),
            pending_events: VecDeque::new(),
            last_heartbeat_ns: 0,
            client,
            api_url_prefix,
            wss_url,
        }
    }

    /// Fetch initial orderbook snapshot via SDK for an outcome
    #[allow(dead_code)]
    fn fetch_orderbook_snapshot(&self, outcome_id: &str) -> Result<MarketEvent> {
        let book = self
            .client
            .get_orderbook(outcome_id)
            .map_err(|e| anyhow!("SDK error: {}", e))?;

        let now = now_ns();
        let bids = book
            .bids
            .iter()
            .map(|l| PriceLevel {
                price: l.price.to_f64().unwrap_or(0.0),
                quantity: l.quantity as f64,
            })
            .collect();
        let asks = book
            .asks
            .iter()
            .map(|l| PriceLevel {
                price: l.price.to_f64().unwrap_or(0.0),
                quantity: l.quantity as f64,
            })
            .collect();

        Ok(MarketEvent::OrderBook(OrderBookSnapshot {
            exchange: Exchange::Hexmarket,
            symbol: outcome_id.to_string(),
            bids,
            asks,
            exchange_timestamp_ns: now,
            local_timestamp_ns: now,
        }))
    }
}

/// Fetch events from HexMarket API via SDK
pub fn fetch_events(
    api_url_prefix: &str,
    status: Option<&str>,
    limit: u32,
) -> Result<Vec<super::sdk::EventListItem>> {
    use super::auth::api_url_prefix_or_default;
    let client = HexClient::new(HexClientConfig {
        api_url: api_url_prefix_or_default(api_url_prefix).to_string(),
    });

    let params = ListEventsParams {
        status: status.map(|s| s.to_string()),
        limit: Some(limit as i64),
        ..Default::default()
    };
    info!("[Hexmarket] Fetching events (status={:?}, limit={})", status, limit);

    let events = client
        .list_events(&params)
        .map_err(|e| anyhow!("SDK error: {}", e))?;
    info!("[Hexmarket] Fetched {} events", events.len());
    Ok(events)
}

/// Parse a WebSocket message into MarketEvent(s).
/// A book update produces both an OrderBook and a QuoteTick (to trigger the engine's on_quote).
fn parse_ws_message(text: &str) -> Vec<MarketEvent> {
    // simd-json drop-in for SIMD parse speedup.
    let mut buf = text.as_bytes().to_vec();
    let data: serde_json::Value = match simd_json::serde::from_slice(&mut buf) {
        Ok(v) => v,
        Err(_) => return vec![],
    };

    let event_type = data.get("event_type").and_then(|v| v.as_str()).unwrap_or("");
    let asset_id = data.get("asset_id").and_then(|v| v.as_str()).unwrap_or("");
    if asset_id.is_empty() {
        return vec![];
    }

    match event_type {
        "book" => {
            let parse_levels = |key: &str| -> Vec<PriceLevel> {
                data.get(key)
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|level| {
                                Some(PriceLevel {
                                    price: level.get("price")?.as_f64()?,
                                    quantity: level
                                        .get("quantity")
                                        .and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))?,
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default()
            };
            let now = now_ns();
            let bids = parse_levels("bids");
            let asks = parse_levels("asks");

            let best_bid = bids.last().map(|l| (l.price, l.quantity));
            let best_ask = asks.last().map(|l| (l.price, l.quantity));

            let mut events = vec![MarketEvent::OrderBook(OrderBookSnapshot {
                exchange: Exchange::Hexmarket,
                symbol: asset_id.to_string(),
                bids,
                asks,
                exchange_timestamp_ns: now,
                local_timestamp_ns: now,
            })];

            let (bp, bq) = best_bid.unwrap_or((0.0, 0.0));
            let (ap, aq) = best_ask.unwrap_or((0.0, 0.0));
            events.push(MarketEvent::Quote(QuoteTick {
                exchange: Exchange::Hexmarket,
                symbol: asset_id.to_string(),
                bid_price: bp,
                bid_qty: bq,
                ask_price: ap,
                ask_qty: aq,
                exchange_timestamp_ns: now,
                local_timestamp_ns: now,
            }));

            events
        }
        "trade" | "last_trade_price" => {
            let price = match data.get("price").and_then(|v| v.as_f64()) {
                Some(p) => p,
                None => return vec![],
            };
            let quantity = data
                .get("quantity")
                .and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                .unwrap_or(0.0);
            let side_str = data.get("side").and_then(|v| v.as_str()).unwrap_or("buy");
            let now = now_ns();
            vec![MarketEvent::Trade(TradeTick {
                exchange: Exchange::Hexmarket,
                symbol: asset_id.to_string(),
                price,
                quantity,
                side: if side_str == "sell" { Side::Sell } else { Side::Buy },
                exchange_timestamp_ns: now,
                local_timestamp_ns: now,
            })]
        }
        "price_change" => {
            let price = match data.get("price").and_then(|v| v.as_f64()) {
                Some(p) => p,
                None => return vec![],
            };
            let now = now_ns();
            vec![MarketEvent::Quote(QuoteTick {
                exchange: Exchange::Hexmarket,
                symbol: asset_id.to_string(),
                bid_price: price,
                bid_qty: 0.0,
                ask_price: price,
                ask_qty: 0.0,
                exchange_timestamp_ns: now,
                local_timestamp_ns: now,
            })]
        }
        _ => vec![],
    }
}

async fn hexmarket_ws_task(
    url: String,
    asset_ids: Vec<String>,
    event_tx: crossbeam_channel::Sender<MarketEvent>,
    shutdown: Arc<AtomicBool>,
) {
    let mut backoff = crate::exchange::ReconnectBackoff::new(200, 30_000);

    loop {
        if shutdown.load(Ordering::Relaxed) { break; }

        info!("[Hexmarket] Connecting to {}", url);
        let stream = match tokio_tungstenite::connect_async(&url).await {
            Ok((s, _)) => s,
            Err(e) => {
                let delay = backoff.next_delay();
                warn!("[Hexmarket] WS connect failed: {}, retry in {:.1}s", e, delay.as_secs_f64());
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        backoff.reset();
        let (mut write, mut read) = stream.split();

        if !asset_ids.is_empty() {
            let msg = serde_json::json!({
                "assets_ids": asset_ids,
                "type": "market"
            });
            if let Err(e) = write.send(Message::Text(msg.to_string())).await {
                warn!("[Hexmarket] subscribe failed: {}", e);
                continue;
            }
            info!("[Hexmarket] Subscribed to {} outcomes", asset_ids.len());
        }

        let mut ping_interval = tokio::time::interval(PING_INTERVAL);
        ping_interval.tick().await;

        loop {
            tokio::select! {
                biased;
                _ = ping_interval.tick() => {
                    if let Err(e) = write.send(Message::Text("PING".to_string())).await {
                        warn!("[Hexmarket] Ping send failed: {}", e);
                        break;
                    }
                }
                read_result = tokio::time::timeout(STALE_THRESHOLD, read.next()) => {
                    let msg = match read_result {
                        Ok(Some(Ok(m))) => m,
                        Ok(Some(Err(e))) => { warn!("[Hexmarket] WS read error: {}", e); break; }
                        Ok(None) => { warn!("[Hexmarket] WS closed"); break; }
                        Err(_elapsed) => {
                            warn!("[Hexmarket] No message for {:.0}s (stall watchdog) — reconnecting",
                                STALE_THRESHOLD.as_secs_f64());
                            break;
                        }
                    };
                    match msg {
                        Message::Text(text) => {
                            let t_parse = crate::latency::Instant::now();
                            for event in parse_ws_message(&text) {
                                if event_tx.send(event).is_err() { return; }
                            }
                            crate::latency::record("hexmarket.ws.parse", t_parse);
                        }
                        Message::Ping(payload) => {
                            let _ = write.send(Message::Pong(payload)).await;
                        }
                        Message::Close(_) => {
                            warn!("[Hexmarket] WebSocket closed by server");
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
    info!("[Hexmarket] WS task exiting");
}

impl ExchangeMarket for HexmarketMarket {
    fn connect(&mut self) -> Result<()> {
        let url = format!("{}/market", self.wss_url.trim_end_matches('/'));
        let asset_ids: Vec<String> = self.outcomes.keys().cloned().collect();
        let (event_tx, event_rx) = crossbeam_channel::unbounded::<MarketEvent>();
        self.event_rx = Some(event_rx);
        // Per-task shutdown Arc — see binance/market.rs commentary.
        let shutdown = Arc::new(AtomicBool::new(false));
        self.ws_shutdown = shutdown.clone();

        crate::async_rt::handle().spawn(hexmarket_ws_task(url, asset_ids, event_tx, shutdown));

        info!(
            "[Hexmarket] WS task launched, subscribed to {} outcomes across {} markets in {} events",
            self.outcomes.len(),
            self.market_count,
            self.event_count,
        );
        Ok(())
    }

    fn subscribe(&mut self, symbols: &[String]) -> Result<()> {
        self.symbols = symbols.to_vec();

        for symbol in symbols {
            info!("[Hexmarket] Fetching event by slug: {}", symbol);
            let event_detail = self
                .client
                .get_event(symbol)
                .map_err(|e| anyhow!("SDK error fetching event '{}': {}", symbol, e))?;

            let total_outcomes: usize = event_detail.markets.iter()
                .map(|md| md.outcomes.len())
                .sum();
            info!(
                "[Hexmarket] Event '{}': {} markets, {} outcomes",
                event_detail.event.title,
                event_detail.markets.len(),
                total_outcomes,
            );

            for md in &event_detail.markets {
                let market_id = md.market.id.to_string();

                if md.outcomes.is_empty() {
                    warn!(
                        "[Hexmarket] Market '{}' ({}) has no outcomes, skipping",
                        md.market.title, market_id
                    );
                    continue;
                }

                let outcome_labels: Vec<&str> =
                    md.outcomes.iter().map(|o| o.label.as_str()).collect();
                info!(
                    "[Hexmarket]   Market '{}' ({}): {} outcomes {:?}",
                    md.market.title,
                    md.market.market_type,
                    md.outcomes.len(),
                    outcome_labels,
                );

                for outcome in &md.outcomes {
                    let outcome_id = outcome.id.to_string();
                    self.outcomes.insert(
                        outcome_id.clone(),
                        OutcomeState {
                            outcome_id,
                            market_id: market_id.clone(),
                            label: outcome.label.clone(),
                            event_slug: symbol.clone(),
                        },
                    );
                }

                let binary_option = market_detail_to_binary_option(&event_detail.event, md);
                self.pending_events.push_back(MarketEvent::Instrument(
                    Instrument::BinaryOption(binary_option),
                ));

                self.market_count += 1;
            }

            self.event_count += 1;
        }

        info!(
            "[Hexmarket] {} events, {} markets, {} outcomes, {} pending events",
            self.event_count,
            self.market_count,
            self.outcomes.len(),
            self.pending_events.len(),
        );
        Ok(())
    }

    fn next_event(&mut self) -> Result<Option<MarketEvent>> {
        // Drain pending events first
        if let Some(event) = self.pending_events.pop_front() {
            return Ok(Some(event));
        }

        let rx = self.event_rx.as_ref().ok_or_else(|| anyhow!("Not connected"))?;
        match rx.try_recv() {
            Ok(event) => Ok(Some(event)),
            Err(crossbeam_channel::TryRecvError::Empty) => {
                // Emit periodic heartbeat QuoteTick to keep the engine's
                // quote_interval timer firing even when the market is quiet.
                let now = now_ns();
                if now - self.last_heartbeat_ns >= QUOTE_HEARTBEAT_NS {
                    self.last_heartbeat_ns = now;
                    if let Some(outcome_id) = self.outcomes.keys().next() {
                        return Ok(Some(MarketEvent::Quote(QuoteTick {
                            exchange: Exchange::Hexmarket,
                            symbol: outcome_id.clone(),
                            bid_price: 0.0,
                            bid_qty: 0.0,
                            ask_price: 0.0,
                            ask_qty: 0.0,
                            exchange_timestamp_ns: now,
                            local_timestamp_ns: now,
                        })));
                    }
                }
                Ok(None)
            }
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                Err(anyhow!("Hexmarket WS task ended unexpectedly"))
            }
        }
    }

    fn disconnect(&mut self) {
        self.ws_shutdown.store(true, Ordering::Relaxed);
        self.event_rx = None;
        info!("[Hexmarket] Disconnected");
    }

    fn name(&self) -> &str {
        "hexmarket"
    }
}
