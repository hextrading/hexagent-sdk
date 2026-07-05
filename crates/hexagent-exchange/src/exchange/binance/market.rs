use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

use crate::exchange::ExchangeMarket;
use crate::types::*;

/// Default WebSocket HOST roots (no trailing path). URL builders
/// downstream append:
///   * spot single-symbol:  `<host>/stream?streams=...`
///   * spot legacy multi:   `<host>/ws/<stream1>/<stream2>...`
///   * futures multi:       `<host>/market/stream?streams=...`
///
/// These are compile-time constants but the operator can override
/// them via `BinanceMarket::with_ws_base(...)`, wired from
/// `exchanges[].wss_url` in the TOML. The override path exists so a
/// Binance endpoint migration can be hot-fixed by editing config +
/// restarting, instead of requiring a code rebuild — which is what
/// kept `binance_futures` silent for 305 h between 2026-04-27 and
/// 05-10 (the old `/stream` path still accepted WS upgrades but
/// emitted zero data — and the URL was compile-time-pinned).
///
/// The override should be the HOST root only (no path), e.g.
/// `wss://fstream.binance.com`. Each URL builder concatenates its
/// own stream path. If the operator pastes a full URL with a path,
/// we trim trailing path elements before use (defensive — see
/// `normalise_ws_host`).
const BINANCE_WS_HOST_DEFAULT: &str = "wss://stream.binance.com:9443";
/// Binance USDⓈ-M Futures default WS host.
///
/// Updated 2026-04-24 per the latest API docs: the combined-stream
/// path moved from `/stream` to `/market/stream`. The old path now
/// returns immediately (no data, no error), which is how
/// `binance_futures usdtusd@assetIndex` started flap-reconnecting
/// every 10 s in live. (The migration was in the PATH; the HOST
/// didn't change — `fstream.binance.com` is still correct.)
const BINANCE_FUTURES_WS_HOST_DEFAULT: &str = "wss://fstream.binance.com";

/// Strip any trailing path so `with_ws_base(host)` accepts both
///   `wss://fstream.binance.com`             (just host)
/// and `wss://fstream.binance.com/market/stream`  (with full path)
/// — operators tend to paste whichever they see in Binance docs.
fn normalise_ws_host(raw: &str) -> String {
    let s = raw.trim().trim_end_matches('/');
    // Find `:port`-or-host boundary, then strip path.
    // Use `.find('/')` after the `://` prefix.
    if let Some(scheme_end) = s.find("://") {
        let after_scheme = &s[scheme_end + 3..];
        if let Some(path_idx) = after_scheme.find('/') {
            return s[..scheme_end + 3 + path_idx].to_string();
        }
    }
    s.to_string()
}
/// Default REST bases for runtime liveness probing. When the dead-
/// endpoint counter fires, the WS task issues a single short-timeout
/// GET `/api/v3/ping` (spot) or `/fapi/v1/ping` (futures) against the
/// matching REST host. The result is folded into the error message so
/// the operator can tell at a glance:
///   * WS dead, REST alive → endpoint migrated, redeploy / config fix
///   * WS dead, REST dead  → network / firewall / DNS — check upstream
const BINANCE_REST_BASE_DEFAULT: &str = "https://api.binance.com";
const BINANCE_FUTURES_REST_BASE_DEFAULT: &str = "https://fapi.binance.com";

const PING_INTERVAL: Duration = Duration::from_secs(30);
/// REST liveness probe timeout. Kept short — a healthy REST ping
/// returns in < 200 ms; if it doesn't respond in 3 s, that itself is
/// diagnostic (upstream slow / blocked / DNS).
const REST_PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// Per-task read-side watchdog: if no message arrives within this many
/// seconds, force a reconnect. Defends against TCP zombies (silent
/// half-close, NAT timeout, server reboot without RST) that
/// `read.next().await` doesn't surface as an error and `write.send()`
/// can't detect either while the local TCP buffer still has space.
///
/// Per endpoint:
///   - spot streams (`@depth10@100ms` + `@trade` + `@kline_1m`): push
///     ~10 Hz combined; even a 5 s gap is anomalous → 30 s window.
///   - futures `@assetIndex`: push ~1 Hz. Empirical observation
///     2026-04-24 noted occasional multi-second silences during
///     normal operation, hence a 90 s window. The engine-side data-
///     timeout watchdog independently sits at 60 s for futures, so
///     anything between 60 s and 90 s falls to the engine layer
///     (which kills + respawns the task); only longer hangs hit this
///     in-task guard.
const STALE_THRESHOLD_SPOT: Duration = Duration::from_secs(30);
const STALE_THRESHOLD_FUTURES: Duration = Duration::from_secs(90);

/// Dead-endpoint alert threshold. After this many consecutive
/// reconnect cycles complete WITHOUT receiving a single data Text
/// message, escalate the per-cycle `warn!` to a single `error!` so
/// operator alerting fires. At default thresholds:
///   * spot:    10 cycles × 30 s stall = ≈ 5 min of silence
///   * futures: 10 cycles × 90 s stall = ≈ 15 min of silence
/// Either gives an unambiguous signal that the endpoint is dead
/// (vs. a transient network hiccup which resolves within 1-2 cycles)
/// while keeping enough lead time that a brief Binance maintenance
/// won't spam alerts.
const SILENT_ALERT_THRESHOLD: u32 = 10;

/// Outcome of a one-shot REST liveness probe. Folded into the
/// dead-endpoint error message so the operator can tell at a glance
/// whether the issue is "WS only" (endpoint migrated) or "WS + REST"
/// (network / DNS / firewall).
#[derive(Debug, Clone, Copy)]
enum RestProbeResult {
    Ok,
    Failed,
    TimedOut,
}

impl RestProbeResult {
    fn label(self) -> &'static str {
        match self {
            RestProbeResult::Ok       => "REST=alive (WS-only outage — likely endpoint migrated)",
            RestProbeResult::Failed   => "REST=failed (network / DNS / firewall — check upstream)",
            RestProbeResult::TimedOut => "REST=timeout (upstream slow / blocked)",
        }
    }
}

/// One-shot GET `<rest_base>/<ping_path>` with a short timeout.
/// Used as a diagnostic at the dead-endpoint alert site — not a
/// fail-fast gate at startup (a transient REST blip shouldn't kill
/// a long-running recorder).
async fn probe_rest_liveness(rest_base: &str, futures: bool) -> RestProbeResult {
    let ping_path = if futures { "/fapi/v1/ping" } else { "/api/v3/ping" };
    let url = format!("{}{}", rest_base, ping_path);
    // Shared h1.1 Query pool; the outer tokio timeout enforces the probe
    // budget (tighter than the pool client's own ceiling).
    let client = crate::http1_pool::client(crate::http1_pool::Role::Query);
    match tokio::time::timeout(REST_PROBE_TIMEOUT, client.get(&url).send()).await {
        Ok(Ok(resp)) if resp.status().is_success() => RestProbeResult::Ok,
        Ok(Ok(_)) => RestProbeResult::Failed,   // non-2xx
        Ok(Err(_)) => RestProbeResult::Failed,  // network / DNS / TLS
        Err(_) => RestProbeResult::TimedOut,
    }
}

/// Update the dead-endpoint counter at the end of each WS connect
/// cycle. Returns `Some(cycles)` when the cycle should fire an
/// operator-visible alert; `None` otherwise.
///
/// State transition:
///   * `got_data=true`  → reset counter to 0, reset alert pacing
///   * `got_data=false` → increment counter; emit alert when it
///                        crosses `*next_alert`, then double the
///                        next alert threshold so alarms don't spam
///                        every cycle.
fn update_silent_cycle_counter(
    got_data: bool,
    cycles: &mut u32,
    next_alert: &mut u32,
) -> Option<u32> {
    if got_data {
        *cycles = 0;
        *next_alert = SILENT_ALERT_THRESHOLD;
        None
    } else {
        *cycles = cycles.saturating_add(1);
        if *cycles >= *next_alert {
            let fired_at = *cycles;
            *next_alert = next_alert.saturating_mul(2);
            Some(fired_at)
        } else {
            None
        }
    }
}

pub struct BinanceMarket {
    symbols: Vec<String>,
    event_rx: Option<crossbeam_channel::Receiver<MarketEvent>>,
    ws_shutdown: Arc<AtomicBool>,
    api_key: String,
    /// If true, connect to futures endpoint (fstream.binance.com) for asset index streams.
    futures: bool,
    /// Kline interval to subscribe to in spot mode (`"1m"` legacy
    /// default, `"1s"` for sub-minute HAR-RV configs). Binance Spot
    /// supports `1s` natively via `<symbol>@kline_1s` since Jan 2024.
    /// Ignored in futures mode (futures subscribes via assetIndex
    /// streams, not klines).
    kline_interval: String,
    /// Optional `data_dir` root (i.e. `backtest.data_dir`). When set,
    /// runtime WS-reconnect gap-fill bars fetched via REST are also
    /// persisted to `{data_dir}/histdata/binance/{SYMBOL}/{interval}/`,
    /// so a subsequent process restart finds them locally and skips
    /// the REST round-trip. `None` keeps the legacy behaviour (no
    /// persistence — gap-fill bars only flow into the vol model).
    data_dir: Option<PathBuf>,
    /// Optional override for the WS base URL — exposed so config-side
    /// `exchanges[].wss_url` can hot-fix a Binance endpoint migration
    /// without a code rebuild. `None` = use the compile-time
    /// `BINANCE_WS_BASE_DEFAULT` / `BINANCE_FUTURES_WS_BASE_DEFAULT`.
    ws_base_override: Option<String>,
    /// Optional override for the REST base used by the liveness probe.
    /// Operators set this to track non-default endpoints (e.g. a
    /// regional REST host or a private gateway).
    rest_base_override: Option<String>,
}

impl BinanceMarket {
    pub fn new(api_key: String, futures: bool) -> Self {
        Self::with_kline_interval(api_key, futures, "1m".to_string())
    }

    /// Constructor that accepts a custom spot kline interval. Use
    /// this when the strategy requires sub-minute bars (e.g.
    /// `hist_bar_interval = "1s"`). For futures mode the interval is
    /// stored but not used (futures stream set is interval-agnostic).
    pub fn with_kline_interval(api_key: String, futures: bool, kline_interval: String) -> Self {
        Self {
            symbols: Vec::new(),
            event_rx: None,
            ws_shutdown: Arc::new(AtomicBool::new(false)),
            api_key,
            futures,
            kline_interval,
            data_dir: None,
            ws_base_override: None,
            rest_base_override: None,
        }
    }

    /// Fluent builder: attach a `data_dir` root so runtime WS-reconnect
    /// gap-fill bars are persisted to local parquet for re-use on the
    /// next restart. Without this call, gap-fill bars only flow into
    /// the live vol model (the legacy Phase-C behaviour).
    ///
    /// Expected `data_dir` value: the strategy's `backtest.data_dir`
    /// (same root used by `load_hist_bars` at startup). The persistence
    /// layout — `histdata/binance/{SYMBOL}/{interval}/{YYYYMM}/{YYYYMMDD}.parquet`
    /// — matches what the startup loader reads from, so the next
    /// process boot can resume from where this one left off without
    /// re-fetching the gap from REST.
    pub fn with_data_dir(mut self, data_dir: PathBuf) -> Self {
        self.data_dir = Some(data_dir);
        self
    }

    /// Override the compile-time default WS host. Empty / blank
    /// string is treated as "no override". The argument may include
    /// a path (`wss://fstream.binance.com/market/stream`); the path
    /// is stripped because each URL builder appends its own path.
    ///
    /// Wired from `exchanges[].wss_url` in the TOML. Existing operator
    /// configs that don't set `wss_url` keep the prior behaviour
    /// (compile-time default) without any TOML edits.
    pub fn with_ws_base(mut self, ws_base: String) -> Self {
        let trimmed = ws_base.trim();
        if !trimmed.is_empty() {
            self.ws_base_override = Some(normalise_ws_host(trimmed));
        }
        self
    }

    /// Override the compile-time default REST base URL used by the
    /// liveness probe (fires when the dead-endpoint counter triggers).
    /// Empty string = no override. Trailing slash trimmed.
    pub fn with_rest_base(mut self, rest_base: String) -> Self {
        let trimmed = rest_base.trim().trim_end_matches('/');
        if !trimmed.is_empty() {
            self.rest_base_override = Some(trimmed.to_string());
        }
        self
    }

    /// Resolve the active WS host root, honouring `with_ws_base()`.
    /// Returns the HOST only (no path); callers append their own path.
    fn ws_host(&self) -> &str {
        match (&self.ws_base_override, self.futures) {
            (Some(s), _) => s.as_str(),
            (None, true)  => BINANCE_FUTURES_WS_HOST_DEFAULT,
            (None, false) => BINANCE_WS_HOST_DEFAULT,
        }
    }

    /// Resolve the active REST base for liveness probe.
    fn rest_base(&self) -> &str {
        match (&self.rest_base_override, self.futures) {
            (Some(s), _) => s.as_str(),
            (None, true)  => BINANCE_FUTURES_REST_BASE_DEFAULT,
            (None, false) => BINANCE_REST_BASE_DEFAULT,
        }
    }

    /// Build the FULL combined-stream URL (legacy multi-symbol path).
    ///
    /// **Spot mode**: kept for backwards-compatibility only — the spot
    /// `connect()` path now prefers `build_single_symbol_url()` and
    /// spawns one WS task per symbol, so multi-symbol spot deployments
    /// can't mislabel partial-depth OBs (2026-05-13 04:00 contamination
    /// root cause).
    ///
    /// **Futures mode**: still uses this path. Futures events
    /// (`assetIndexUpdate`) carry `"s"` in their JSON so multi-symbol
    /// routing is unambiguous regardless of stream wrapper.
    #[allow(dead_code)]
    fn build_stream_url(&self) -> String {
        let host = self.ws_host();
        // Futures: <host>/market/stream?streams=usdtusd@assetIndex/...
        if self.futures {
            let streams: Vec<String> = self.symbols.iter()
                .map(|s| {
                    if let Some((sym, stream_type)) = s.split_once('@') {
                        format!("{}@{}", sym.to_lowercase(), stream_type)
                    } else {
                        s.to_lowercase()
                    }
                })
                .collect();
            return format!("{}/market/stream?streams={}", host, streams.join("/"));
        }

        // Spot legacy multi-stream form: <host>/ws/<stream1>/<stream2>...
        // (Used only by dead-code path / tests; live spot prefers
        // `build_single_symbol_spot_url`.)
        let kline_iv = self.kline_interval.as_str();
        let streams: Vec<String> = self
            .symbols
            .iter()
            .flat_map(|s| {
                let lower = s.to_lowercase();
                vec![
                    format!("{}@depth10@100ms", lower),
                    format!("{}@trade", lower),
                    format!("{}@kline_{}", lower, kline_iv),
                ]
            })
            .collect();

        if streams.is_empty() {
            format!("{}/ws", host)
        } else {
            format!("{}/ws/{}", host, streams.join("/"))
        }
    }

    /// Build a SINGLE-symbol spot stream URL. Each spot WS task uses
    /// one of these; multi-symbol deployments spawn N tasks in parallel.
    ///
    /// URL uses the `/stream?streams=...` combined form even for one
    /// symbol — this makes every message arrive wrapped as
    /// `{"stream":"<sym>@<type>","data":{...}}`, so the parser can
    /// recover the symbol from the wrapper. The plain `/ws/<stream>`
    /// path strips the wrapper, leaving partial-depth OBs symbol-less.
    fn build_single_symbol_spot_url(&self, symbol: &str) -> String {
        debug_assert!(!symbol.is_empty(), "build_single_symbol_spot_url with empty symbol");
        let lower = symbol.to_lowercase();
        let kline_iv = self.kline_interval.as_str();
        let streams = format!(
            "{lo}@depth10@100ms/{lo}@trade/{lo}@kline_{iv}",
            lo = lower, iv = kline_iv,
        );
        // Combined-stream path on the resolved host (default = the
        // public spot host). Format documented at
        // https://binance-docs.github.io/apidocs/spot/en/#combined-streams
        format!("{}/stream?streams={}", self.ws_host(), streams)
    }
}

/// Parse Diff. Depth Stream ("e":"depthUpdate") — has "s", "b", "a", "E" fields.
fn parse_depth_update(data: &serde_json::Value) -> Option<MarketEvent> {
    let symbol = data.get("s")?.as_str()?;
    let exchange_ts = data.get("E")?.as_u64()? * 1_000_000;

    let parse_levels = |key: &str| -> Vec<PriceLevel> {
        data.get(key)
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|level| {
                        let a = level.as_array()?;
                        Some(PriceLevel {
                            price: a.first()?.as_str()?.parse().ok()?,
                            quantity: a.get(1)?.as_str()?.parse().ok()?,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    };

    Some(MarketEvent::OrderBook(OrderBookSnapshot {
        exchange: Exchange::Binance,
        symbol: symbol.to_uppercase(),
        bids: parse_levels("b"),
        asks: parse_levels("a"),
        exchange_timestamp_ns: exchange_ts,
        local_timestamp_ns: now_ns(),
    }))
}

/// Parse Partial Book Depth Stream (@depth5/10/20@100ms) — has "bids", "asks", "lastUpdateId"
/// but NO "e", "s", or "E" fields. Symbol must be inferred from context.
fn parse_partial_depth(data: &serde_json::Value, symbol_hint: &str) -> Option<MarketEvent> {
    // Partial depth has "lastUpdateId", "bids", "asks" at top level
    data.get("lastUpdateId")?;

    let parse_levels = |key: &str| -> Vec<PriceLevel> {
        data.get(key)
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|level| {
                        let a = level.as_array()?;
                        Some(PriceLevel {
                            price: a.first()?.as_str()?.parse().ok()?,
                            quantity: a.get(1)?.as_str()?.parse().ok()?,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    };

    Some(MarketEvent::OrderBook(OrderBookSnapshot {
        exchange: Exchange::Binance,
        symbol: symbol_hint.to_uppercase(),
        bids: parse_levels("bids"),
        asks: parse_levels("asks"),
        exchange_timestamp_ns: now_ns(), // no exchange timestamp in partial depth
        local_timestamp_ns: now_ns(),
    }))
}

fn parse_trade_message(data: &serde_json::Value) -> Option<MarketEvent> {
    let symbol = data.get("s")?.as_str()?;
    let price: f64 = data.get("p")?.as_str()?.parse().ok()?;
    let quantity: f64 = data.get("q")?.as_str()?.parse().ok()?;
    let is_buyer_maker = data.get("m")?.as_bool()?;
    let exchange_ts = data.get("E")?.as_u64()? * 1_000_000;

    Some(MarketEvent::Trade(TradeTick {
        exchange: Exchange::Binance,
        symbol: symbol.to_uppercase(),
        price,
        quantity,
        side: if is_buyer_maker { Side::Sell } else { Side::Buy },
        exchange_timestamp_ns: exchange_ts,
        local_timestamp_ns: now_ns(),
    }))
}

fn parse_book_ticker_message(data: &serde_json::Value) -> Option<MarketEvent> {
    let symbol = data.get("s")?.as_str()?;
    let bid_price: f64 = data.get("b")?.as_str()?.parse().ok()?;
    let bid_qty: f64 = data.get("B")?.as_str()?.parse().ok()?;
    let ask_price: f64 = data.get("a")?.as_str()?.parse().ok()?;
    let ask_qty: f64 = data.get("A")?.as_str()?.parse().ok()?;
    // bookTicker uses "u" (updateId) rather than "E" (event time) in some streams;
    // fall back to local time if "E" is absent.
    let exchange_ts = data
        .get("E")
        .and_then(|v| v.as_u64())
        .map(|ms| ms * 1_000_000)
        .unwrap_or_else(now_ns);

    Some(MarketEvent::Quote(QuoteTick {
        exchange: Exchange::Binance,
        symbol: symbol.to_uppercase(),
        bid_price,
        bid_qty,
        ask_price,
        ask_qty,
        exchange_timestamp_ns: exchange_ts,
        local_timestamp_ns: now_ns(),
    }))
}

fn parse_kline_message(data: &serde_json::Value) -> Option<MarketEvent> {
    let symbol = data.get("s")?.as_str()?;
    let exchange_ts = data.get("E")?.as_u64()? * 1_000_000;
    let k = data.get("k")?;

    if k.get("x")?.as_bool()? {
        Some(MarketEvent::Bar(BarData {
            exchange: Exchange::Binance,
            symbol: symbol.to_uppercase(),
            interval: k.get("i")?.as_str()?.to_string(),
            open_time_ns: k.get("t")?.as_u64()? * 1_000_000,
            close_time_ns: k.get("T")?.as_u64()? * 1_000_000,
            open: k.get("o")?.as_str()?.parse().ok()?,
            high: k.get("h")?.as_str()?.parse().ok()?,
            low: k.get("l")?.as_str()?.parse().ok()?,
            close: k.get("c")?.as_str()?.parse().ok()?,
            volume: k.get("v")?.as_str()?.parse().ok()?,
            // Binance ws kline: q=quote_volume, V=taker_buy_base.
            quote_volume: k.get("q").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
            taker_buy_base: k.get("V").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
            is_closed: k.get("x")?.as_bool()?,
            exchange_timestamp_ns: exchange_ts,
            local_timestamp_ns: now_ns(),
        }))
    } else {
        None
    }

}

/// Parse Binance Futures assetIndexUpdate event.
/// Stream: <symbol>@assetIndex, e.g. USDTUSD@assetIndex
/// Fields: "e"="assetIndexUpdate", "E"=timestamp_ms, "s"=symbol, "i"=index_price
fn parse_asset_index(data: &serde_json::Value) -> Option<MarketEvent> {
    let symbol_raw = data.get("s")?.as_str()?;
    let index_price: f64 = data.get("i")?.as_str()?.parse().ok()?;
    let exchange_ts = data.get("E")?.as_u64()? * 1_000_000; // ms → ns

    // Convert symbol: "USDTUSD" → "usdt/usd"
    let symbol = if symbol_raw.len() > 3 && symbol_raw.ends_with("USD") {
        let base = &symbol_raw[..symbol_raw.len() - 3];
        format!("{}/usd", base.to_lowercase())
    } else {
        symbol_raw.to_lowercase()
    };

    Some(MarketEvent::SpotPrice(SpotPrice {
        source: "binance_futures".to_string(),
        symbol,
        price: index_price,
        timestamp_ns: exchange_ts,
        local_timestamp_ns: now_ns(),
    }))
}

/// Extract the symbol (UPPERCASE) from a Binance combined-stream name,
/// e.g. `btcusdt@depth10@100ms` → `Some("BTCUSDT")`.
///
/// Stream names always have the form `<symbol>@<stream_type>` where
/// `<symbol>` is lowercase ASCII and `<stream_type>` may itself contain
/// `@` (e.g. `@depth10@100ms`). We split on the FIRST `@` only.
///
/// Returns `None` for empty input or names without an `@` separator
/// (defensive — Binance never sends such names, but a future schema
/// change shouldn't crash the parser).
fn parse_stream_symbol(stream_key: &str) -> Option<String> {
    let (sym, _) = stream_key.split_once('@')?;
    if sym.is_empty() { return None; }
    Some(sym.to_uppercase())
}

/// Parse a single WS frame into a `MarketEvent`. Pure parser — does
/// not touch the event channel; the caller handles dispatch (so
/// closed-kline gap detection + REST gap-fill can wrap the send).
fn parse_message_to_event(
    text: &str,
    futures: bool,
    symbol_hint: &str,
) -> Option<MarketEvent> {
    // simd-json drop-in for SIMD parse speedup.
    let mut buf = text.as_bytes().to_vec();
    let raw: serde_json::Value = match simd_json::serde::from_slice(&mut buf) {
        Ok(v) => v,
        Err(_) => return None,
    };

    // Combined stream: `{"stream":"<symbol>@<type>","data":{...}}`.
    //
    // **Symbol resolution (long-term fix for 2026-05-13 04:00 contamination)**:
    // Partial depth (`@depth5/10/20`) JSON has no `"s"` field — historically
    // the parser fell back to a single shared `symbol_hint` per WS task,
    // which silently mislabeled every partial-depth OB when the same task
    // was subscribed to multiple symbols (e.g. BTCUSDT + ETHUSDT + SOLUSDT
    // all stored as BTCUSDT in `data/binance/BTCUSDT/`).
    //
    // The combined-stream wrapper exposes the real symbol via its `stream`
    // key, so we extract it here and prefer it over `symbol_hint` whenever
    // available. `symbol_hint` is kept as the fallback for raw single-
    // stream connections (no `stream` wrapper) and for safety when the
    // stream key parses unexpectedly.
    let (data, resolved_symbol): (serde_json::Value, Option<String>) =
        if let Some(stream_val) = raw.get("stream") {
            let stream_key = stream_val.as_str().unwrap_or("");
            let sym = parse_stream_symbol(stream_key);
            let data = raw.get("data").cloned().unwrap_or_else(|| raw.clone());
            (data, sym)
        } else {
            (raw, None)
        };
    // For partial depth (the symbol-less JSON variant), prefer the
    // resolved-from-stream symbol over the WS task's shared hint.
    let partial_depth_symbol: &str = resolved_symbol.as_deref().unwrap_or(symbol_hint);

    let event_type = data.get("e").and_then(|e| e.as_str());
    if futures {
        log::trace!("[BinanceFutures] msg: e={:?} keys={:?}",
            event_type, data.as_object().map(|o| o.keys().collect::<Vec<_>>()));
    }

    let event = match event_type {
        // These parsers all read symbol from `data.s` (Binance includes
        // it in the typed JSON), so they're unaffected by combined-stream
        // mislabeling — but pass `partial_depth_symbol` through anyway
        // since some Binance variants emit empty `"s"`.
        Some("depthUpdate") => parse_depth_update(&data),
        Some("trade") => parse_trade_message(&data),
        Some("bookTicker") => parse_book_ticker_message(&data),
        Some("kline") => parse_kline_message(&data),
        Some("assetIndexUpdate") => parse_asset_index(&data),
        // Partial depth: NO symbol in JSON → use the resolved-from-stream
        // symbol if we have one, else fall back to `symbol_hint`.
        None if data.get("lastUpdateId").is_some() => {
            parse_partial_depth(&data, partial_depth_symbol)
        }
        _ => {
            let ev = parse_book_ticker_message(&data);
            if ev.is_none() {
                debug!("[Binance] Unknown event type: {:?}", event_type);
            }
            ev
        }
    };

    event
}

/// Kline-interval string (e.g. `"1s"`, `"1m"`) → nanoseconds. Returns
/// `None` for unrecognised intervals; the caller suppresses gap-fill
/// in that case (treating the message as legacy non-gap-checked).
fn kline_interval_to_ns(interval: &str) -> Option<u64> {
    let secs: u64 = match interval {
        "1s" => 1,
        "5s" => 5,
        "10s" => 10,
        "1m" => 60,
        "3m" => 180,
        "5m" => 300,
        "15m" => 900,
        "30m" => 1800,
        "1h" => 3600,
        "2h" => 7200,
        "4h" => 14400,
        "1d" => 86400,
        _ => return None,
    };
    Some(secs * 1_000_000_000)
}

/// Per-symbol kline timeline state. Tracks the LAST CLOSED kline's
/// `open_time_ns` we've emitted to the strategy event channel so the
/// gap detector can fire when a new closed kline arrives more than
/// `2 × kline_interval` after it.
///
/// Reset behaviour: state SURVIVES WS disconnect/reconnect (lives in
/// `binance_ws_task`'s outer scope). On reconnect, the first new
/// closed kline triggers gap-fill back to where we left off — which
/// is the user's primary requirement for Phase C continuity.
type KlineGapState = HashMap<String, u64>;

/// Dispatch a parsed `MarketEvent` to the strategy, with closed-kline
/// gap detection + REST-API gap-fill inline.
///
/// Returns `true` on success, `false` if the event channel is closed
/// (caller should exit the WS task).
///
/// Gap-fill protocol:
///   1. Closed kline arrives with `open_time_ns = T`.
///   2. Look up `last_open` for this symbol in `gap_state`.
///   3. If `last_open > 0 && T > last_open + 2·interval_ns`, fetch
///      `[last_open + interval_ns, T)` via Binance REST and emit each
///      returned bar BEFORE the live one. `vol_model`'s monotonic
///      guard handles any REST-vs-WS overlap (de-dup is automatic).
///   4. Update `gap_state[symbol] = T` and emit the live bar.
///
/// REST call uses `spawn_blocking` so the blocking HTTP I/O doesn't
/// stall the async runtime (we're on a `current_thread` runtime as
/// of 2026-05 — `block_in_place` is unavailable).
async fn dispatch_event(
    event: MarketEvent,
    event_tx: &crossbeam_channel::Sender<MarketEvent>,
    gap_state: &mut KlineGapState,
    data_dir: Option<&PathBuf>,
) -> bool {
    // Only closed klines get the gap-fill treatment. Sub-bar updates
    // (`is_closed=false`) are filtered out earlier in
    // `parse_kline_message`, so we don't see them here. Other event
    // types (OB, trade, asset-index, …) pass straight through.
    if let MarketEvent::Bar(ref bar) = event {
        if bar.is_closed {
            if let Some(interval_ns) = kline_interval_to_ns(&bar.interval) {
                let last_open = *gap_state.get(&bar.symbol).unwrap_or(&0);
                let cur_open = bar.open_time_ns;
                // Gap detection: > 2× interval since last emit, AND we
                // have a prior reference (last_open > 0).
                if last_open > 0 && cur_open > last_open.saturating_add(2 * interval_ns) {
                    let gap_start = last_open + interval_ns;
                    let gap_end = cur_open;
                    let gap_secs = (gap_end - gap_start) / 1_000_000_000;
                    info!(
                        "[Binance] kline gap detected: symbol={} interval={} \
                         last_open={} cur_open={} gap={}s — fetching REST fill",
                        bar.symbol, bar.interval, last_open, cur_open, gap_secs,
                    );
                    let symbol = bar.symbol.clone();
                    let interval = bar.interval.clone();
                    // Phase C+: if a `data_dir` is wired, the same
                    // spawn_blocking that does the REST fetch also
                    // writes the result to histdata parquet — so the
                    // next process restart finds these bars locally
                    // and skips the REST round-trip on the same gap.
                    // Persistence runs INSIDE spawn_blocking (file I/O
                    // is sync) so the async runtime stays unblocked.
                    let persist_root = data_dir.cloned();
                    let symbol_persist = symbol.clone();
                    let interval_persist = interval.clone();
                    let fetch_result = tokio::task::spawn_blocking(move || {
                        let fetched = crate::exchange::binance::fetch_klines(
                            &symbol, &interval, gap_start, gap_end,
                        );
                        // Persist successful fetches before returning.
                        // A persistence failure here is logged but does
                        // NOT fail the gap-fill — the bars still flow
                        // to the live vol model via the returned Vec.
                        if let (Some(root), Ok(ref bars)) = (&persist_root, &fetched) {
                            if !bars.is_empty() {
                                let hist_dir = root
                                    .join("histdata")
                                    .join("binance")
                                    .join(&symbol_persist)
                                    .join(&interval_persist);
                                if let Err(e) = crate::recorder::hist_reader::save_bars_to_local(
                                    &hist_dir, bars, &interval_persist,
                                ) {
                                    warn!(
                                        "[Binance] gap-fill persist failed for {} \
                                         interval={} ({} bars): {} — bars still \
                                         dispatched live but next restart will \
                                         re-fetch the gap",
                                        symbol_persist, interval_persist, bars.len(), e,
                                    );
                                } else {
                                    info!(
                                        "[Binance] gap-fill persisted: {} bars → {}",
                                        bars.len(), hist_dir.display(),
                                    );
                                }
                            }
                        }
                        fetched
                    }).await;
                    match fetch_result {
                        Ok(Ok(fill_bars)) => {
                            info!(
                                "[Binance] kline gap-fill ok: {} bars for {} \
                                 covering {}s",
                                fill_bars.len(), bar.symbol, gap_secs,
                            );
                            for fb in fill_bars {
                                if event_tx.send(MarketEvent::Bar(fb)).is_err() {
                                    return false;
                                }
                            }
                        }
                        Ok(Err(e)) => {
                            warn!(
                                "[Binance] kline gap-fill REST error for {}: {} \
                                 — proceeding with live bar (vol model will see \
                                 a gap)",
                                bar.symbol, e,
                            );
                        }
                        Err(join_err) => {
                            warn!(
                                "[Binance] kline gap-fill spawn_blocking failed \
                                 for {}: {} — proceeding with live bar",
                                bar.symbol, join_err,
                            );
                        }
                    }
                }
                gap_state.insert(bar.symbol.clone(), cur_open);
            }
        }
    }

    event_tx.send(event).is_ok()
}

// `rest_base` is the resolved REST host root (e.g.
// `https://fapi.binance.com`), honoring `BinanceMarket::rest_base()`
// + `exchanges[].api_url_prefix`. Used only by the dead-endpoint
// liveness probe at the silent-cycle alert site.
async fn binance_ws_task(
    url: String,
    futures: bool,
    symbol_hint: String,
    event_tx: crossbeam_channel::Sender<MarketEvent>,
    shutdown: Arc<AtomicBool>,
    data_dir: Option<PathBuf>,
    rest_base: String,
) {
    let tag = if futures { "BinanceFutures" } else { "Binance" };
    let mut backoff = crate::exchange::ReconnectBackoff::new(200, 30_000);
    let stale_threshold = if futures { STALE_THRESHOLD_FUTURES } else { STALE_THRESHOLD_SPOT };

    // Phase C: per-symbol kline timeline state. Lives outside the
    // connect-reconnect loop so a WS reconnect triggers REST gap-fill
    // back to wherever the last closed kline left off (Spot kline 1s
    // ack lag is ~50 ms — a 30s reconnect dropout yields ~30 missing
    // bars REST-filled at the next live kline).
    let mut kline_gap_state: KlineGapState = HashMap::new();

    // Dead-endpoint detection: count consecutive reconnect cycles
    // that received zero data Text messages before the WS dropped /
    // stalled. A healthy connection cycles ~once per day from
    // server-side rotations and resets the counter the moment a Text
    // arrives; a dead endpoint (e.g. Binance migrated WS URL on
    // 2026-04-24 — the recorder's 12.5-day silence between 04-27 and
    // 05-10 looked exactly like "TCP connects fine, zero messages,
    // stall watchdog fires, reconnect, repeat") keeps the counter
    // climbing forever.
    //
    // At `SILENT_ALERT_THRESHOLD` consecutive silent cycles we
    // escalate to `error!` (vs the per-cycle `warn!`) so the
    // operator's monitoring catches it. We also keep escalating at
    // exponentially-spaced multiples after the first alert so a
    // standing alarm gets re-fired periodically without spamming
    // every cycle.
    let mut consecutive_silent_cycles: u32 = 0;
    let mut next_alert_at: u32 = SILENT_ALERT_THRESHOLD;

    loop {
        if shutdown.load(Ordering::Relaxed) { break; }

        info!("[{}] Connecting to {}", tag, url);
        let stream = match tokio_tungstenite::connect_async(&url).await {
            Ok((s, _)) => s,
            Err(e) => {
                let delay = backoff.next_delay();
                warn!("[{}] WS connect failed: {}, retry in {:.1}s", tag, e, delay.as_secs_f64());
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        backoff.reset();
        info!("[{}] Connected", tag);
        let (mut write, mut read) = stream.split();

        let mut ping_interval = tokio::time::interval(PING_INTERVAL);
        ping_interval.tick().await;

        // Per-cycle data witness — set true the moment ANY Text
        // message is parsed. Used to decide at cycle-end whether to
        // increment `consecutive_silent_cycles` or reset it. We don't
        // count Ping/Pong/Close frames as "data" because Binance
        // sends server-initiated Ping on a dead-but-still-routed
        // connection too — only an actual application Text payload
        // proves the subscription is live.
        let mut got_data_this_cycle = false;

        loop {
            tokio::select! {
                biased;
                _ = ping_interval.tick() => {
                    if let Err(e) = write.send(Message::Ping(Vec::new())).await {
                        warn!("[{}] Ping send failed: {}", tag, e);
                        break;
                    }
                }
                // Read with a stall watchdog. `read.next().await` with
                // no timeout would block forever on a TCP zombie (silent
                // half-close, NAT timeout, server reboot without RST).
                // Wrapping in `tokio::time::timeout` gives us a hard
                // upper bound on inactivity; on Elapsed we force-break
                // and the outer loop reconnects.
                read_result = tokio::time::timeout(stale_threshold, read.next()) => {
                    let msg = match read_result {
                        Ok(Some(Ok(m))) => m,
                        Ok(Some(Err(e))) => {
                            warn!("[{}] WS read error: {} — reconnecting", tag, e);
                            break;
                        }
                        Ok(None) => {
                            warn!("[{}] WS closed — reconnecting", tag);
                            break;
                        }
                        Err(_elapsed) => {
                            warn!(
                                "[{}] No message for {:.0}s (stall watchdog) — reconnecting",
                                tag, stale_threshold.as_secs_f64(),
                            );
                            break;
                        }
                    };
                    match msg {
                        Message::Text(text) => {
                            // Mark the cycle as productive BEFORE parsing —
                            // even a malformed message that fails parse_*
                            // is proof the subscription is alive (vs. the
                            // dead-endpoint case where zero bytes arrive).
                            got_data_this_cycle = true;
                            // Phase C wiring: pure parse → async dispatch.
                            // Non-kline events flow straight through;
                            // closed klines go through gap-detection +
                            // REST-fill before being forwarded.
                            if let Some(event) = parse_message_to_event(
                                &text, futures, &symbol_hint,
                            ) {
                                if !dispatch_event(
                                    event, &event_tx, &mut kline_gap_state, data_dir.as_ref(),
                                ).await {
                                    return;
                                }
                            }
                        }
                        Message::Ping(payload) => {
                            let _ = write.send(Message::Pong(payload)).await;
                        }
                        Message::Close(_) => {
                            warn!("[{}] Server closed WS — reconnecting", tag);
                            break;
                        }
                        _ => {}
                    }
                }
            }
            if shutdown.load(Ordering::Relaxed) { return; }
        }

        // Inner loop exited → this connect cycle is done. Note the
        // prior counter value so the "data resumed" recovery info!
        // can quote the number of silent cycles that just ended.
        let was_silent = consecutive_silent_cycles;
        let alert = update_silent_cycle_counter(
            got_data_this_cycle,
            &mut consecutive_silent_cycles,
            &mut next_alert_at,
        );
        if got_data_this_cycle && was_silent > 0 {
            info!(
                "[{}] Data resumed after {} silent reconnect cycle(s) — resetting alert state",
                tag, was_silent,
            );
        }
        if let Some(cycles) = alert {
            // Dead-endpoint signature: TCP/handshake succeeds, the
            // server may even send Ping (so the stall watchdog gives
            // the connection time), but ZERO data Text messages
            // arrive across many cycles. This is the failure mode
            // that left binance_futures silent for 305 h between
            // 2026-04-27 and 05-10 — the WS URL had been migrated
            // and the old binary kept reconnecting to a
            // still-accepting-but-mute endpoint.
            //
            // Issue a single REST liveness probe to disambiguate
            // "WS-only outage" (endpoint migrated) from "everything
            // dead" (network / DNS / firewall). Result is folded
            // into the error message so the operator gets the
            // diagnosis without a second log line to correlate.
            let probe = probe_rest_liveness(&rest_base, futures).await;
            error!(
                "[{}] DEAD ENDPOINT? {} consecutive reconnect cycles with zero Text \
                 messages (≈ {:.0}s of silence). ws={}  rest={}  → {}",
                tag,
                cycles,
                stale_threshold.as_secs_f64() * cycles as f64,
                url,
                rest_base,
                probe.label(),
            );
        }
        // `got_data_this_cycle` is re-declared at the top of the
        // outer loop on the next iteration, so no explicit reset.

        if shutdown.load(Ordering::Relaxed) { break; }
        let delay = backoff.next_delay();
        tokio::time::sleep(delay).await;
    }
    info!("[{}] WS task exiting", tag);
}

impl ExchangeMarket for BinanceMarket {
    fn connect(&mut self) -> Result<()> {
        let (event_tx, event_rx) = crossbeam_channel::unbounded::<MarketEvent>();
        self.event_rx = Some(event_rx.clone());
        // Per-task shutdown: each connect() creates a FRESH Arc rather
        // than reusing the struct field. Old tasks (still draining a
        // previous connection — possibly hung in `read.next()` waiting
        // for a TCP zombie to surface) keep their own Arc which stays
        // `false`; they never learn shutdown=true and would otherwise
        // race the new task here when the next disconnect/connect
        // cycle resets the shared atomic. See: shared-Arc reset race
        // documented in 2026-05-10 audit.
        //
        // The same Arc is cloned into every per-symbol task spawned
        // below, so `disconnect()` setting it true shuts down every
        // WS in parallel without needing a per-task registry.
        let shutdown = Arc::new(AtomicBool::new(false));
        self.ws_shutdown = shutdown.clone();
        let futures = self.futures;

        if futures {
            // Futures mode: keep the legacy single-WS multi-stream form.
            // Futures events (`assetIndexUpdate`, etc.) all carry `"s"` in
            // their JSON so multi-symbol routing is unambiguous; there's
            // no equivalent of the partial-depth symbol-less variant here.
            let url = self.build_stream_url();
            let symbol_hint = self.symbols.first().cloned().unwrap_or_default();
            let rest_base = self.rest_base().to_string();
            // Futures uses assetIndex (not klines), so the gap-fill
            // persist path is a no-op here — but we forward `data_dir`
            // unchanged for symmetry with the spot branch.
            crate::async_rt::handle().spawn(binance_ws_task(
                url,
                futures,
                symbol_hint,
                event_tx,
                shutdown,
                self.data_dir.clone(),
                rest_base,
            ));
            info!(
                "[BinanceFutures] WS task launched (api_key_len={})",
                self.api_key.len(),
            );
        } else {
            // Spot mode: spawn ONE WS task per symbol.
            //
            // Motivation: Binance spot partial-depth (`@depth5/10/20`) JSON
            // has no `"s"` field. With one WS subscribing to multiple
            // symbols' partial-depth streams, every OB would arrive with
            // identical wrapper-stripped JSON, distinguishable only by the
            // combined-stream `"stream"` wrapper key. The parser now
            // recovers symbol from the wrapper (long-term fix), but
            // running one task per symbol gives us a second layer of
            // safety: each task has a dedicated `symbol_hint`, single-
            // symbol URL, and independent backoff/watchdog timers. A
            // hang or bad reconnect on one symbol's task doesn't stall
            // the others.
            //
            // Channel: all tasks send into the SAME `event_tx` clone;
            // the strategy thread receives them interleaved (already
            // the case in the multi-stream-single-WS design, just with
            // explicit per-task ordering now).
            if self.symbols.is_empty() {
                info!("[Binance] connect() called with no symbols — no WS task spawned");
                return Ok(());
            }
            for sym in &self.symbols {
                let url = self.build_single_symbol_spot_url(sym);
                let symbol_hint = sym.to_uppercase();
                let tx = event_tx.clone();
                let sd = shutdown.clone();
                let dd = self.data_dir.clone();
                let rest = self.rest_base().to_string();
                crate::async_rt::handle().spawn(binance_ws_task(
                    url,
                    /* futures */ false,
                    symbol_hint,
                    tx,
                    sd,
                    dd,
                    rest,
                ));
            }
            info!(
                "[Binance] {} per-symbol WS task(s) launched (symbols={:?}, api_key_len={})",
                self.symbols.len(), self.symbols, self.api_key.len(),
            );
        }
        Ok(())
    }

    fn subscribe(&mut self, symbols: &[String]) -> Result<()> {
        self.symbols = symbols.to_vec();
        info!(
            "[{}] Symbols set: {:?}",
            if self.futures { "BinanceFutures" } else { "Binance" },
            self.symbols,
        );
        Ok(())
    }

    fn next_event(&mut self) -> Result<Option<MarketEvent>> {
        let rx = self.event_rx.as_ref().ok_or_else(|| anyhow!("Not connected"))?;
        match rx.try_recv() {
            Ok(event) => Ok(Some(event)),
            Err(crossbeam_channel::TryRecvError::Empty) => Ok(None),
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                Err(anyhow!("Binance WS task ended unexpectedly"))
            }
        }
    }

    fn disconnect(&mut self) {
        self.ws_shutdown.store(true, Ordering::Relaxed);
        self.event_rx = None;
        info!(
            "[{}] Disconnected",
            if self.futures { "BinanceFutures" } else { "Binance" },
        );
    }

    fn name(&self) -> &str {
        if self.futures { "binance_futures" } else { "binance" }
    }
}

#[cfg(test)]
mod tests {
    //! Tests for the two-layer multi-symbol safety:
    //!   1. `parse_stream_symbol` recovers the real symbol from combined-
    //!      stream wrappers (long-term parser fix).
    //!   2. `build_single_symbol_spot_url` builds per-symbol URLs (short-
    //!      term fix: each spot symbol gets its own WS task).
    //!
    //! The 2026-05-13 04:00 data contamination (BTCUSDT directory filled
    //! with ETHUSDT + SOLUSDT OBs because partial-depth has no `"s"` in
    //! JSON and the WS task used a single shared `symbol_hint`) is the
    //! reference scenario that both fixes target.
    use super::*;

    /// Standard Binance stream names: `<symbol_lower>@<stream_type>`,
    /// possibly with multiple `@` (e.g. `@depth10@100ms`).
    #[test]
    fn parse_stream_symbol_extracts_symbol_from_combined_stream_name() {
        // depth + interval form
        assert_eq!(parse_stream_symbol("btcusdt@depth10@100ms").as_deref(), Some("BTCUSDT"));
        assert_eq!(parse_stream_symbol("ethusdt@depth5@100ms").as_deref(), Some("ETHUSDT"));
        assert_eq!(parse_stream_symbol("solusdt@depth20@1000ms").as_deref(), Some("SOLUSDT"));
        // trade
        assert_eq!(parse_stream_symbol("btcusdt@trade").as_deref(), Some("BTCUSDT"));
        // kline
        assert_eq!(parse_stream_symbol("btcusdt@kline_1m").as_deref(), Some("BTCUSDT"));
        // bookTicker (no interval)
        assert_eq!(parse_stream_symbol("btcusdt@bookTicker").as_deref(), Some("BTCUSDT"));
        // Futures asset index (uppercase symbol part)
        assert_eq!(parse_stream_symbol("USDTUSD@assetIndex").as_deref(), Some("USDTUSD"));
    }

    /// Malformed inputs return `None` so the caller falls back to
    /// `symbol_hint`. Defensive against schema changes — the parser
    /// must never panic on unexpected input.
    #[test]
    fn parse_stream_symbol_returns_none_for_malformed_input() {
        assert!(parse_stream_symbol("").is_none());
        assert!(parse_stream_symbol("no_at_sign").is_none(),
            "missing '@' should not resolve a symbol");
        assert!(parse_stream_symbol("@depth10").is_none(),
            "leading '@' (empty symbol) should not resolve");
    }

    /// Per-symbol URL must:
    ///   1. Use the `/stream?streams=` form so every message arrives
    ///      with the `{"stream":..., "data":...}` wrapper (lets the
    ///      parser recover symbol from the wrapper key).
    ///   2. Include exactly the three streams the strategy needs:
    ///      depth, trade, kline.
    ///   3. Lowercase the symbol part (Binance requires this).
    #[test]
    fn build_single_symbol_spot_url_includes_only_one_symbols_three_streams() {
        let m = BinanceMarket::new(String::new(), false);
        let url = m.build_single_symbol_spot_url("BTCUSDT");

        // Combined-stream form ensures `{"stream":...}` wrapper on every msg.
        assert!(url.contains("/stream?streams="),
            "must use combined-stream form, got: {url}");

        // Exactly the three streams for THIS symbol.
        assert!(url.contains("btcusdt@depth10@100ms"));
        assert!(url.contains("btcusdt@trade"));
        // Default kline interval is "1m" (legacy back-compat).
        assert!(url.contains("btcusdt@kline_1m"));

        // CRUCIAL: no other symbol may slip in. This is the regression
        // guard for the 2026-05-13 04:00 contamination.
        assert!(!url.to_lowercase().contains("ethusdt"));
        assert!(!url.to_lowercase().contains("solusdt"));

        // Spot host (not futures).
        assert!(url.starts_with("wss://stream.binance.com:9443/"));
    }

    /// Lowercasing: Binance symbol parts must be lowercase in stream
    /// names. The function accepts mixed case and normalises.
    #[test]
    fn build_single_symbol_spot_url_lowercases_symbol() {
        let m = BinanceMarket::new(String::new(), false);
        let url_a = m.build_single_symbol_spot_url("BTCUSDT");
        let url_b = m.build_single_symbol_spot_url("btcusdt");
        let url_c = m.build_single_symbol_spot_url("BtcUsdt");
        assert_eq!(url_a, url_b);
        assert_eq!(url_a, url_c);
    }

    /// Custom kline interval ("1s") flows through to the spot URL.
    /// Confirms the Phase B refactor that swaps hardcoded `kline_1m`
    /// for the configurable `kline_interval` field.
    #[test]
    fn build_single_symbol_spot_url_honors_kline_interval() {
        let m_1s = BinanceMarket::with_kline_interval(String::new(), false, "1s".to_string());
        let url = m_1s.build_single_symbol_spot_url("BTCUSDT");
        assert!(url.contains("btcusdt@kline_1s"),
            "1s kline interval should appear in URL, got: {url}");
        // Legacy 1m must NOT be present in the 1s URL.
        assert!(!url.contains("btcusdt@kline_1m"),
            "1m stream must NOT appear in 1s-configured URL, got: {url}");
    }

    // ─── Phase C: kline gap detection ──────────────────────────

    #[test]
    fn kline_interval_to_ns_supports_sub_minute() {
        assert_eq!(kline_interval_to_ns("1s"), Some(1_000_000_000));
        assert_eq!(kline_interval_to_ns("5s"), Some(5_000_000_000));
        assert_eq!(kline_interval_to_ns("10s"), Some(10_000_000_000));
        assert_eq!(kline_interval_to_ns("1m"), Some(60_000_000_000));
        assert_eq!(kline_interval_to_ns("1h"), Some(3_600_000_000_000));
        assert_eq!(kline_interval_to_ns("1d"), Some(86_400_000_000_000));
        // Unknown → None (caller skips gap fill).
        assert_eq!(kline_interval_to_ns("7m"), None);
        assert_eq!(kline_interval_to_ns(""), None);
        assert_eq!(kline_interval_to_ns("bogus"), None);
    }

    fn closed_bar(symbol: &str, interval: &str, open_time_ns: u64) -> BarData {
        BarData {
            exchange: Exchange::Binance,
            symbol: symbol.to_string(),
            interval: interval.to_string(),
            open_time_ns,
            close_time_ns: open_time_ns + kline_interval_to_ns(interval).unwrap() - 1,
            open: 100.0, high: 100.0, low: 100.0, close: 100.0,
            volume: 0.0,
            taker_buy_base: 0.0,
            quote_volume: 0.0,
            is_closed: true,
            exchange_timestamp_ns: open_time_ns,
            local_timestamp_ns: open_time_ns,
        }
    }

    /// First closed-kline ever (no prior state) MUST emit the live bar
    /// without triggering REST fetch — there's no `last_open` to gap-
    /// fill from.
    #[test]
    fn dispatch_event_first_kline_no_gap_fill() {
        let (tx, rx) = crossbeam_channel::unbounded();
        let mut state = KlineGapState::new();
        let bar = closed_bar("BTCUSDT", "1s", 1_000_000_000_000);
        // Run inline using a current-thread runtime (test harness).
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all().build().unwrap();
        let sent = rt.block_on(dispatch_event(MarketEvent::Bar(bar.clone()), &tx, &mut state, None));
        assert!(sent, "dispatch should succeed");
        // Exactly one event in the channel (the live bar; no fill bars).
        assert_eq!(rx.try_iter().count(), 1);
        // State updated.
        assert_eq!(state.get("BTCUSDT").copied(), Some(bar.open_time_ns));
    }

    /// Contiguous closed klines (cur = last + interval) MUST NOT
    /// trigger gap fill — the "2× interval" threshold prevents
    /// false positives on normal stream behaviour.
    #[test]
    fn dispatch_event_contiguous_klines_no_gap_fill() {
        let (tx, rx) = crossbeam_channel::unbounded();
        let mut state = KlineGapState::new();
        let interval_ns = 1_000_000_000u64;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all().build().unwrap();
        // 3 contiguous 1s klines.
        for i in 0..3u64 {
            let bar = closed_bar("BTCUSDT", "1s", 1_000_000_000_000 + i * interval_ns);
            let sent = rt.block_on(dispatch_event(MarketEvent::Bar(bar), &tx, &mut state, None));
            assert!(sent);
        }
        // 3 events, no REST fill needed.
        assert_eq!(rx.try_iter().count(), 3);
    }

    /// Non-kline events MUST flow through untouched (no gap state
    /// effects, no REST fetch). Regression guard against accidentally
    /// applying gap logic to OB / trade / etc.
    #[test]
    fn dispatch_event_non_kline_bypasses_gap_logic() {
        let (tx, rx) = crossbeam_channel::unbounded();
        let mut state = KlineGapState::new();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all().build().unwrap();
        let trade = MarketEvent::Trade(TradeTick {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_string(),
            price: 100.0, quantity: 1.0,
            side: Side::Buy,
            exchange_timestamp_ns: 0, local_timestamp_ns: 0,
        });
        let sent = rt.block_on(dispatch_event(trade, &tx, &mut state, None));
        assert!(sent);
        assert_eq!(rx.try_iter().count(), 1);
        assert!(state.is_empty(),
            "non-kline events must not pollute gap_state");
    }

    /// Open / non-closed klines never reach dispatch_event (filtered in
    /// parse_kline_message), but defensively confirm that a Bar with
    /// `is_closed=false` doesn't update state if it does slip through.
    /// NOTE: in practice this branch is dead code in production but
    /// the contract is "we only act on closed bars."
    #[test]
    fn dispatch_event_open_kline_does_not_update_state() {
        // parse_kline_message currently returns None for is_closed=false,
        // so this case is theoretical — but we exercise the dispatch
        // function directly to check robustness.
        let (tx, _rx) = crossbeam_channel::unbounded();
        let mut state = KlineGapState::new();
        let mut bar = closed_bar("BTCUSDT", "1s", 1_000_000_000_000);
        bar.is_closed = false;  // simulate open bar
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all().build().unwrap();
        let sent = rt.block_on(dispatch_event(MarketEvent::Bar(bar), &tx, &mut state, None));
        assert!(sent);
        assert!(state.is_empty(),
            "open-kline gap_state must NOT be updated by an unclosed bar");
    }

    // NOTE: we don't unit-test the actual REST fetch branch — that
    // would require either a mock HTTP server or live network access.
    // The branch's correctness is verified by the existing
    // `fetch_klines` tests + the live behaviour during operator BT.

    // ─── Phase C+: data_dir persist-on-gap-fill wiring ─────────

    /// `with_data_dir` is a fluent builder; chaining it MUST preserve
    /// the kline interval set by `with_kline_interval`. This guards
    /// against accidental field-shadowing in the builder.
    #[test]
    fn with_data_dir_preserves_kline_interval() {
        let dir = std::path::PathBuf::from("/tmp/hexbot-test-data");
        let m = BinanceMarket::with_kline_interval(String::new(), false, "1s".to_string())
            .with_data_dir(dir.clone());
        assert_eq!(m.kline_interval, "1s");
        assert_eq!(m.data_dir.as_deref(), Some(dir.as_path()));
        // URL still honors 1s.
        let url = m.build_single_symbol_spot_url("BTCUSDT");
        assert!(url.contains("btcusdt@kline_1s"));
    }

    /// Default constructor leaves `data_dir = None` (legacy behaviour:
    /// runtime gap-fill in-memory only, no parquet persistence).
    #[test]
    fn new_constructor_data_dir_defaults_to_none() {
        let m = BinanceMarket::new(String::new(), false);
        assert!(m.data_dir.is_none(),
            "new() must default data_dir=None for back-compat");
    }

    /// dispatch_event with `data_dir=Some(...)` but no gap-fill needed
    /// (first kline, or contiguous) MUST NOT touch the filesystem.
    /// Regression guard: the persist path is gated on "gap detected
    /// AND fetch succeeded with bars", and must not eagerly create
    /// directories or write files in the no-gap case.
    #[test]
    fn dispatch_event_no_gap_does_not_touch_filesystem_even_with_data_dir() {
        let tmp = std::env::temp_dir().join(format!(
            "hexbot-phase-cplus-test-{}", std::process::id(),
        ));
        // Ensure clean slate — directory should NOT exist after dispatch.
        let _ = std::fs::remove_dir_all(&tmp);

        let (tx, _rx) = crossbeam_channel::unbounded();
        let mut state = KlineGapState::new();
        let bar = closed_bar("BTCUSDT", "1s", 2_000_000_000_000);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all().build().unwrap();
        let sent = rt.block_on(dispatch_event(
            MarketEvent::Bar(bar), &tx, &mut state, Some(&tmp),
        ));
        assert!(sent);
        // No fetch happened (first kline ever) → no persistence happened.
        // The `histdata/` subtree must not have been created.
        let hist_dir = tmp.join("histdata").join("binance").join("BTCUSDT").join("1s");
        assert!(!hist_dir.exists(),
            "no-gap dispatch must not create histdata directories \
             (would indicate eager persistence in the wrong branch)");
        // Cleanup (in case any sibling dir slipped in).
        let _ = std::fs::remove_dir_all(&tmp);
    }

    // ── Dead-endpoint counter (silent-cycle escalation) ────────────

    /// Healthy data should reset the counter and pacing back to the
    /// configured threshold, so a subsequent dead-endpoint episode
    /// fires its first alert at exactly SILENT_ALERT_THRESHOLD again
    /// (not at the previously-doubled value).
    #[test]
    fn silent_counter_resets_on_data() {
        let mut cycles = 9_u32;
        let mut next  = 10_u32;
        let alert = update_silent_cycle_counter(true, &mut cycles, &mut next);
        assert!(alert.is_none());
        assert_eq!(cycles, 0);
        assert_eq!(next, SILENT_ALERT_THRESHOLD);
    }

    /// Below threshold: counter climbs, no alert fires.
    #[test]
    fn silent_counter_no_alert_below_threshold() {
        let mut cycles = 0_u32;
        let mut next  = SILENT_ALERT_THRESHOLD;
        for expected in 1..SILENT_ALERT_THRESHOLD {
            let alert = update_silent_cycle_counter(false, &mut cycles, &mut next);
            assert!(alert.is_none(), "no alert before threshold @ cycles={}", expected);
            assert_eq!(cycles, expected);
        }
        // `next_alert_at` shouldn't have moved yet.
        assert_eq!(next, SILENT_ALERT_THRESHOLD);
    }

    /// At threshold: alert fires once and the next alert threshold
    /// doubles. Then we need SILENT_ALERT_THRESHOLD more silent
    /// cycles to fire again.
    #[test]
    fn silent_counter_fires_alert_at_threshold_and_doubles_pacing() {
        let mut cycles = 0_u32;
        let mut next  = SILENT_ALERT_THRESHOLD;

        // Run SILENT_ALERT_THRESHOLD silent cycles → first alert at the last.
        let mut alerts: Vec<u32> = Vec::new();
        for _ in 0..SILENT_ALERT_THRESHOLD {
            if let Some(c) = update_silent_cycle_counter(false, &mut cycles, &mut next) {
                alerts.push(c);
            }
        }
        assert_eq!(alerts, vec![SILENT_ALERT_THRESHOLD]);
        assert_eq!(next, SILENT_ALERT_THRESHOLD * 2);

        // Next SILENT_ALERT_THRESHOLD-1 silent cycles fire nothing.
        for _ in 0..SILENT_ALERT_THRESHOLD - 1 {
            assert!(update_silent_cycle_counter(false, &mut cycles, &mut next).is_none());
        }
        // The 10th additional silent cycle (cycles == 20) crosses 2× threshold.
        let alert = update_silent_cycle_counter(false, &mut cycles, &mut next);
        assert_eq!(alert, Some(SILENT_ALERT_THRESHOLD * 2));
        assert_eq!(next, SILENT_ALERT_THRESHOLD * 4);
    }

    // ── Config-driven URL overrides (hot-fixable endpoints) ────────

    /// `with_ws_base` accepts both `wss://host` and `wss://host/path`
    /// forms because operators paste whatever they see in Binance
    /// docs. The path is stripped so URL builders downstream can
    /// concatenate their own.
    #[test]
    fn normalise_ws_host_strips_trailing_path() {
        assert_eq!(
            normalise_ws_host("wss://fstream.binance.com"),
            "wss://fstream.binance.com",
        );
        assert_eq!(
            normalise_ws_host("wss://fstream.binance.com/"),
            "wss://fstream.binance.com",
        );
        assert_eq!(
            normalise_ws_host("wss://fstream.binance.com/market/stream"),
            "wss://fstream.binance.com",
        );
        assert_eq!(
            normalise_ws_host("wss://stream.binance.com:9443/ws"),
            "wss://stream.binance.com:9443",
        );
        // Whitespace tolerated (operator pasting from chat / docs).
        assert_eq!(
            normalise_ws_host("  wss://fstream.binance.com/market/stream  "),
            "wss://fstream.binance.com",
        );
    }

    /// Empty / blank override = no override (compile-time default
    /// stays in effect). This is the back-compat path for configs
    /// that don't set `wss_url`.
    #[test]
    fn ws_base_override_empty_string_keeps_default() {
        let m = BinanceMarket::new(String::new(), true).with_ws_base(String::new());
        assert_eq!(m.ws_host(), BINANCE_FUTURES_WS_HOST_DEFAULT);
        let m = BinanceMarket::new(String::new(), true).with_ws_base("   ".to_string());
        assert_eq!(m.ws_host(), BINANCE_FUTURES_WS_HOST_DEFAULT);
    }

    /// Non-empty override applies to both futures and spot variants
    /// and is reflected in the URL builders' output.
    #[test]
    fn ws_base_override_threaded_into_url_builders() {
        // Futures: override the host → resolved URL must use it.
        let mut m = BinanceMarket::new(String::new(), true)
            .with_ws_base("wss://example.com/market/stream".to_string());
        m.symbols = vec!["usdtusd@assetIndex".to_string()];
        let url = m.build_stream_url();
        assert!(url.starts_with("wss://example.com/market/stream?streams="),
            "futures URL: {}", url);
        assert!(!url.contains("fstream.binance.com"));

        // Spot single-symbol: override applies to the host portion.
        let m = BinanceMarket::new(String::new(), false)
            .with_ws_base("wss://example.com".to_string());
        let url = m.build_single_symbol_spot_url("BTCUSDT");
        assert!(url.starts_with("wss://example.com/stream?streams="),
            "spot single-symbol URL: {}", url);
    }

    /// REST override (used by the liveness probe) follows the same
    /// rules as the WS override.
    #[test]
    fn rest_base_override_resolves() {
        let m = BinanceMarket::new(String::new(), true);
        assert_eq!(m.rest_base(), BINANCE_FUTURES_REST_BASE_DEFAULT);

        let m = BinanceMarket::new(String::new(), true)
            .with_rest_base("https://example.com/".to_string());
        // Trailing slash trimmed so `<base>/fapi/v1/ping` stays clean.
        assert_eq!(m.rest_base(), "https://example.com");

        // Empty = no override.
        let m = BinanceMarket::new(String::new(), false).with_rest_base("".to_string());
        assert_eq!(m.rest_base(), BINANCE_REST_BASE_DEFAULT);
    }

    /// After firing alerts, a single productive cycle (got_data=true)
    /// resets both counter and pacing, so the NEXT dead-endpoint
    /// episode gets a fresh alert at SILENT_ALERT_THRESHOLD again
    /// (not at the previously-doubled 2× / 4× pacing). This guards
    /// against the case where a transient outage temporarily resolves,
    /// then recurs — the operator should get a fresh page on each
    /// recurrence, not silently-suppressed warnings.
    #[test]
    fn silent_counter_fresh_alert_after_recovery() {
        let mut cycles = 0_u32;
        let mut next  = SILENT_ALERT_THRESHOLD;
        // First episode → first alert.
        for _ in 0..SILENT_ALERT_THRESHOLD {
            update_silent_cycle_counter(false, &mut cycles, &mut next);
        }
        assert_eq!(next, SILENT_ALERT_THRESHOLD * 2);
        // Recovery.
        update_silent_cycle_counter(true, &mut cycles, &mut next);
        assert_eq!(cycles, 0);
        assert_eq!(next, SILENT_ALERT_THRESHOLD);
        // Second episode → second alert again at SILENT_ALERT_THRESHOLD.
        let mut alerts: Vec<u32> = Vec::new();
        for _ in 0..SILENT_ALERT_THRESHOLD {
            if let Some(c) = update_silent_cycle_counter(false, &mut cycles, &mut next) {
                alerts.push(c);
            }
        }
        assert_eq!(alerts, vec![SILENT_ALERT_THRESHOLD]);
    }
}
