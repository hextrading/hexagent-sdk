use crate::types::{BarData, Exchange, HistDataRequest, Instrument, OrderBookSnapshot, OrderUpdate, QuoteTick, Signal, SpotPrice, TickSizeChange, TradeTick};

/// Trait for trading strategies.
pub trait Strategy: Send {
    fn name(&self) -> &str;

    /// Per-instance identifier (e.g. "maker01"). Used to tag every log
    /// emitted while the strategy is dispatching, via a tracing span
    /// in the engine's strategy thread, so multi-strategy runs can
    /// disentangle interleaved log lines into per-strategy streams.
    /// Default empty string → spans still enter but the field is
    /// just an empty value; non-empty strategies should override
    /// (Polymaker / Hexmaker do).
    fn instance_id(&self) -> &str { "" }

    fn on_orderbook(&mut self, _ob: &OrderBookSnapshot) {}
    fn on_trade_tick(&mut self, _trade: &TradeTick) {}
    fn on_quote_tick(&mut self, _quote: &QuoteTick) {}
    fn on_quote(&mut self, _ts_event: u64) -> Vec<Signal> { Vec::new() }
    fn quote_interval_ms(&self) -> u64 { 0 }
    /// When true, only Binance OrderBook events drive the quote cadence;
    /// other venues' OrderBooks update internal state but don't trigger
    /// `on_quote`. Default false = any OrderBook triggers.
    fn quote_trigger_binance_ob_only(&self) -> bool { false }
    /// Fractional early-trigger tolerance for the quote-cadence gate: an
    /// OrderBook arriving `>= interval × (1 - frac)` after the last quote
    /// fires a quote, absorbing local-timestamp jitter. Default 0.0 = exact.
    fn quote_interval_tolerance_frac(&self) -> f64 { 0.0 }
    /// When true, EVERY OrderBook event triggers a quote (tick-by-tick),
    /// bypassing the `quote_interval_ms` cadence gate — UNLESS
    /// `cadence_rtt_throttle` is also true. Default false.
    fn quote_tick_by_tick(&self) -> bool { false }
    /// Backpressure gate: when true, the rolling tail-RTT detector has
    /// flagged the current event as congested, so tick-by-tick is
    /// suppressed and cadence falls back to the interval throttle.
    /// Default false ⇒ tick-by-tick never suppressed by this path.
    fn cadence_rtt_throttle(&self) -> bool { false }
    fn on_bar(&mut self, _bar: &BarData) {}
    fn on_spot_price(&mut self, _sp: &SpotPrice) {}
    fn on_instrument(&mut self, _inst: &Instrument) {}
    fn on_tick_size_change(&mut self, _tsc: &TickSizeChange) -> Vec<Signal> { Vec::new() }
    fn on_connected(&mut self, _exchange: Exchange) {}
    fn on_disconnected(&mut self, _exchange: Exchange, _reason: &str) {}
    fn on_exit(&mut self) {}
    /// Handle an OrderUpdate arriving from an exchange. Returning a non-empty
    /// `Vec<Signal>` lets the strategy react synchronously — e.g. fire a
    /// `Signal::ReconcilePolymarket` the moment a `NewOrderTimeout` /
    /// `CancelOrderTimeout` lands, rather than waiting for the next
    /// `on_quote` tick to notice the orphan.
    fn on_order_update(&mut self, _update: &OrderUpdate) -> Vec<Signal> { Vec::new() }
    fn load_hist_data(&self, _ts_event: u64) -> Vec<HistDataRequest> { Vec::new() }
    fn on_hist_bar(&mut self, _bar: &BarData) {}
    /// Called after all historical bars from load_hist_data have been
    /// delivered. `end_ns` is the load's target end (≈now in live, `start_ns`
    /// in BT prefetch) — used as the freshness reference and retry-throttle
    /// anchor. The strategy detects any trailing/middle backfill gap from its
    /// own resample cache.
    fn on_hist_data_loaded(&mut self, _end_ns: u64) {}

    /// Engine hook: about to start feeding prediction warm-up events via
    /// `on_orderbook` / `on_trade_tick`. Strategies that train a predictor
    /// on these events should suppress per-tick retrain until warm-up ends.
    fn on_prediction_warmup_start(&mut self) {}

    /// Engine hook: prediction warm-up replay has completed. Strategies
    /// should now run a single explicit retrain so the model is trained on
    /// the full replayed dataset, and resume normal per-tick retrain.
    /// `ts_ns` is the timestamp at which warm-up ended — wall-clock now in
    /// live, backtest `start_ns` (sim time) in backtest. Using wall-clock
    /// in backtest mis-locates the retrain window and drops every sample.
    fn on_prediction_warmup_end(&mut self, _ts_ns: u64) {}

    /// Engine hook: feed ONE spot orderbook into the strategy's apv2
    /// activity-baseline ONLY (no predictor / index / vol / inventory side
    /// effects). Used by the engine's dedicated chronological apv2 warm-up
    /// pass to pre-fill the z-baseline over a multi-day window in TRUE
    /// wall-clock (merged) order — the in-band `on_orderbook` apv2 feed is
    /// gated off during the per-exchange-sequential, 1-day prediction
    /// warm-up. Default no-op (only Polymaker overrides).
    fn on_apv2_warmup_orderbook(&mut self, _ob: &OrderBookSnapshot) {}
    /// As [`on_apv2_warmup_orderbook`], for a spot trade print.
    fn on_apv2_warmup_trade(&mut self, _trade: &TradeTick) {}

    fn on_init(&mut self) {}
    fn on_shutdown(&mut self) -> Vec<Signal> { Vec::new() }

    /// **BT engine hook**: push a one-shot per-event override of the RTT
    /// gate's `prev_event_p_ms`. The engine calls this just before the
    /// next event's `on_instrument` dispatch with the live-observed
    /// last_event_p60_ms from the `sim_rtt_mode = "exact"` per-event table.
    ///
    /// Strategies that don't own an RTT gate (or that don't honour the
    /// BT override surface) should leave this as the default no-op.
    /// PolymakerStrategy overrides it to forward into `RttGate`.
    fn set_per_event_prev_p_override(&mut self, _prev_p_ms: Option<f64>) {}
}

/// Enter a tracing span tagged with the strategy's `instance_id` so
/// every `log::info!` / `tracing::info!` emitted while the closure
/// runs is annotated `strat{iid=<id>}` in the formatted output.
/// Multi-strategy runs use this in `engine.rs` to wrap every
/// `s.on_X(...)` dispatch — interleaved log lines from concurrent
/// polymaker / hexmaker instances become trivially grep-able per
/// instance without touching every log macro in the strategy code.
#[inline]
pub fn dispatch_in_span<S: Strategy + ?Sized, R>(
    s: &mut S,
    f: impl FnOnce(&mut S) -> R,
) -> R {
    let span = tracing::info_span!("strat", iid = %s.instance_id());
    let _g = span.enter();
    f(s)
}
