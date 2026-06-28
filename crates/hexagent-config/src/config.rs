use anyhow::Result;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub general: GeneralConfig,
    #[serde(default)]
    pub recording: RecordingConfig,
    #[serde(default)]
    pub backtest: BacktestConfig,
    #[serde(default)]
    pub exchanges: Vec<ExchangeConfig>,
    #[serde(default)]
    pub strategies: Vec<StrategyConfig>,
    /// OS-level latency tuning (CPU pinning + SCHED_FIFO). Absent / empty
    /// = legacy 4-core defaults (core 0 background, 1 async-rt, 2 strategy,
    /// 3 execution). See `src/os_tune.rs` module doc for details.
    #[serde(default)]
    pub os_tune: OsTuneConfig,
}

/// OS tuning knobs. All fields optional — missing values fall back to the
/// legacy 4-core plan so existing deployments / dev machines keep working
/// without touching the TOML.
///
/// `enable_pin` / `enable_fifo` are master switches; the per-thread
/// env opt-outs (`HEXBOT_NO_PIN*`, `HEXBOT_NO_FIFO`) still apply and take
/// precedence over the config.
#[derive(Debug, Clone, Deserialize)]
pub struct OsTuneConfig {
    #[serde(default = "default_true")]
    pub enable_pin: bool,
    #[serde(default = "default_true")]
    pub enable_fifo: bool,
    pub async_rt_core: Option<usize>,
    pub strategy_core: Option<usize>,
    /// Per-instance strategy-worker cores for live/paper multi-instance
    /// runs: `instance_id → core`. A polymaker instance listed here gets
    /// its own dedicated core (co-hosted BTC/ETH never preempt each
    /// other). Instances not listed fall back to `strategy_core`.
    /// Example: `{ btc = 10, eth = 11 }`. Single-instance runs can omit
    /// this entirely (the lone instance uses `strategy_core`).
    #[serde(default)]
    pub strategy_cores: HashMap<String, usize>,
    pub execution_core: Option<usize>,
    /// Exchange name → core id. Matched against the suffix of
    /// `feed-<name>` thread names. Missing entries fall back to
    /// `execution_core`. Example: `{ binance = 6, coinbase = 7 }`.
    #[serde(default)]
    pub feed_cores: HashMap<String, usize>,
    /// Round-robin pool for hex worker threads (thread name contains
    /// `-worker-<i>`). Empty = fall back to `execution_core`.
    #[serde(default)]
    pub hex_worker_cores: Vec<usize>,
    /// Round-robin pool for non-critical background threads. Empty =
    /// single core 0 (legacy default).
    #[serde(default)]
    pub background_cores: Vec<usize>,
    pub fifo_async_rt: Option<u8>,
    pub fifo_strategy: Option<u8>,
    pub fifo_execution: Option<u8>,
}

impl Default for OsTuneConfig {
    fn default() -> Self {
        Self {
            enable_pin: true,
            enable_fifo: true,
            async_rt_core: None,
            strategy_core: None,
            strategy_cores: HashMap::new(),
            execution_core: None,
            feed_cores: HashMap::new(),
            hex_worker_cores: Vec::new(),
            background_cores: Vec::new(),
            fifo_async_rt: None,
            fifo_strategy: None,
            fifo_execution: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GeneralConfig {
    #[serde(default)]
    pub mode: RunMode,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    /// Path (absolute or relative to this config file's directory) to
    /// the secrets file containing `[poly.<instance_id>]` blocks. When
    /// empty, falls back to `$HEXBOT_SECRETS` then
    /// `<config_dir>/secrets.toml` then `./secrets.toml`.
    #[serde(default)]
    pub secrets_file: String,
    /// **All-probe latency-measurement mode** (live only). When `true`:
    ///   * split/redeem maintenance is disabled (no on-chain ops),
    ///   * every event runs in PROBE mode (the strategy never quotes),
    ///   * the RTT-probe task fires a real *resting* place + cancel each
    ///     cycle (postOnly `BUY Up @ 0.01`, never fills) and records each
    ///     place/cancel request's round-trip latency.
    ///
    /// Default `false` (normal trading). TOML key `all-probe` also
    /// accepted. Ignored outside `mode = "live"`.
    #[serde(default, alias = "all-probe")]
    pub all_probe: bool,
    /// **Write the per-request place/cancel latency CSV** (live only).
    /// When `true`, EACH real place / cancel HTTP round-trip during
    /// normal trading — plus every RTT-probe place/cancel — is logged to
    /// `latency_record`. `all_probe = true` implies this (the probe
    /// session always records). Default `false`. TOML key
    /// `latency-record-enabled` also accepted.
    #[serde(default, alias = "latency-record-enabled")]
    pub latency_record_enabled: bool,
    /// Directory for the per-request place/cancel latency CSVs. One file
    /// per run (the strategy start timestamp is embedded in the filename
    /// so concurrent / sequential runs don't collide), flushed every 5
    /// minutes aligned to the local wall clock. TOML key
    /// `latency-record` also accepted. Default `data/record/latency`.
    #[serde(default = "default_latency_record", alias = "latency-record")]
    pub latency_record: String,
    /// Gas payer for on-chain ops (redeem / split / merge / approve_v2 /
    /// strategy maintenance). `false` (default) → Polymarket gasless
    /// relayer (`POST /submit`, needs `[builder]` creds); `true` → signer
    /// EOA broadcasts directly and pays MATIC (needs `[polygon]` rpc).
    ///
    /// `None` = unset here → fall back to the legacy per-strategy
    /// `params.gas_via_signer_wallet` location (kept working for
    /// un-migrated configs), then `false`. Set it in `[general]` to make
    /// it the single canonical knob across the bot + every CLI subcommand.
    #[serde(default)]
    pub gas_via_signer_wallet: Option<bool>,
    /// **LIVE data-freshness pre-flight gate** (live only). The prediction
    /// / apv2 warm-up replays recorded ORDERBOOK + TRADE parquet — websocket
    /// capture only; orderbook history can't be fetched from REST. If the
    /// newest recorded event for ANY warm-up spot source is older than this
    /// many seconds, `run_live` ABORTS before spawning feeds rather than
    /// warming the spot predictor / apv2 baseline on stale data (or blocking
    /// quoting under `prediction_wait_for_model`). HAR-RV bars are exempt —
    /// they self-heal from REST klines in `load_hist_bars`. Default 3600s
    /// (1 h). Set `<= 0` to disable the gate.
    #[serde(default = "default_live_max_data_gap_secs")]
    pub live_max_data_gap_secs: f64,
}

fn default_latency_record() -> String {
    "data/record/latency".to_string()
}

fn default_live_max_data_gap_secs() -> f64 {
    3600.0
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum RunMode {
    #[default]
    Live,
    Record,
    Backtest,
    Paper,
}

impl std::fmt::Display for RunMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunMode::Live => write!(f, "live"),
            RunMode::Record => write!(f, "record"),
            RunMode::Backtest => write!(f, "backtest"),
            RunMode::Paper => write!(f, "paper"),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RecordingConfig {
    #[serde(default = "default_output_dir")]
    pub output_dir: String,
    #[serde(default = "default_file_prefix")]
    pub file_prefix: String,
    /// Paper trading data directory (separate from live recording).
    #[serde(default = "default_paper_data_dir")]
    pub paper_data_dir: String,
}

impl Default for RecordingConfig {
    fn default() -> Self {
        Self {
            output_dir: default_output_dir(),
            file_prefix: default_file_prefix(),
            paper_data_dir: default_paper_data_dir(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BacktestConfig {
    /// Root directory containing recorded data (same as recording.output_dir).
    #[serde(default = "default_output_dir")]
    pub data_dir: String,
    /// sim_v2 only — cancel-attribution ahead-fraction (the single
    /// microstructure knob, design §5). `< 0` (default) = proportional model
    /// `q_ahead/level`; `[0,1]` pins the fraction of attributed cancels that
    /// sit ahead of our resting order.
    #[serde(default = "default_sim_v2_ahead_frac")]
    pub sim_v2_ahead_frac: f64,
    /// sim_v2 only — ws fill-push latency multiplier applied to the half-RTT
    /// when delivering fills back to the strategy (v1 calibrated ≈ 1.5).
    #[serde(default = "default_sim_v2_fill_push_mult")]
    pub sim_v2_fill_push_mult: f64,
    /// sim_v2 only — TAKER matching-engine overhead (ms) added on top of the
    /// (time-varying) place RTT for marketable/taker fills: a `status=matched`
    /// ack traverses the matching engine. Additive model: taker_rtt ≈
    /// place_rtt(now) + overhead. Defaults from live (2026-05-28): taker −
    /// concurrent-maker overhead p50/p95/p99 ≈ 267/910/1612 ms.
    #[serde(default = "default_sim_v2_taker_overhead_p50_ms")]
    pub sim_v2_taker_overhead_p50_ms: f64,
    #[serde(default = "default_sim_v2_taker_overhead_p95_ms")]
    pub sim_v2_taker_overhead_p95_ms: f64,
    #[serde(default = "default_sim_v2_taker_overhead_p99_ms")]
    pub sim_v2_taker_overhead_p99_ms: f64,
    /// sim_v2 only — MAKER one-step "race" rate ∈ [0,1] (0 = off). When a
    /// resting order's queue GROWS in the next book snapshot, init q_ahead =
    /// rate·next + (1−rate)·now: favorable moves build the queue → we fill less
    /// → adverse selection emerges (queue+book only, one-snapshot lookahead).
    #[serde(default = "default_sim_v2_maker_race_rate")]
    pub sim_v2_maker_race_rate: f64,
    /// sim_v2 only — ADVERSE-SELECTION conditioning of the cancel attribution
    /// (`ahead_frac`) ∈ [0,∞) (0 = off → pure proportional). Cancellations are
    /// informed: when the mid moves AGAINST a resting order between snapshots,
    /// its level's cancels concentrate ahead of us (front makers pull) →
    /// ahead_frac→1 → we advance and fill the toxic flow; favorable → →0 → we
    /// hold and miss. The missing physics behind v2's maker over-fill.
    #[serde(default = "default_sim_v2_adverse_sel_rate")]
    pub sim_v2_adverse_sel_rate: f64,
    /// sim_v2 only — adverse mid-move (in ticks) mapping to FULL conditioning
    /// (`s = ±1`). Larger ⇒ a bigger move is needed to fully tilt ahead_frac.
    #[serde(default = "default_sim_v2_adverse_scale_ticks")]
    pub sim_v2_adverse_scale_ticks: f64,
    /// sim_v2 only — BOOK-THROUGH adverse fill rate ∈ [0,1] (0 = off, option C).
    /// When the contra side TOUCHES/crosses a resting order's price (bid:
    /// `eff_best_ask≤p`) AND a trade in the interval confirms a real match
    /// (sell≤p for a bid), fill `rate·(through_vol−q_ahead)` at its limit
    /// (adverse). The trade-gate filters the ~56% of locks that are flicker.
    #[serde(default = "default_sim_v2_book_through_rate")]
    pub sim_v2_book_through_rate: f64,
    /// sim_v2 only — VOLUME-NEUTRAL forward-markout adverse reprice ∈ [0,∞) (0 =
    /// off). The sim fills makers symmetrically (markout ≈ 0); live makers are
    /// adversely selected (markout ≈ −0.75c at 1-5s). Keeps the full favorable
    /// fill but settles it at limit ± vn·markout (toward the forward mid) → edge
    /// drops at preserved maker volume.
    #[serde(default = "default_sim_v2_fill_markout_vn")]
    pub sim_v2_fill_markout_vn: f64,
    /// sim_v2 only — forward horizon (ms) at which the canonical mid is peeked
    /// for the markout haircut. Data: adverse selection is sharp at 1-5s.
    #[serde(default = "default_sim_v2_fill_markout_horizon_ms")]
    pub sim_v2_fill_markout_horizon_ms: u64,
    /// sim_v2 only — TAKER one-step "race" rate ∈ [0,1] (0 = off). When fillable
    /// volume SHRINKS in the next snapshot, cap the fill at rate·next +
    /// (1−rate)·now: liquidity recedes in-flight → taker misses.
    #[serde(default = "default_sim_v2_taker_race_rate")]
    pub sim_v2_taker_race_rate: f64,
    /// sim_v2 only — MAKER race lookahead horizon (ms): the entry peek looks this
    /// far ahead for the queue-build check (0 = immediate next snapshot).
    #[serde(default = "default_sim_v2_maker_race_horizon_ms")]
    pub sim_v2_maker_race_horizon_ms: u64,
    /// sim_v2 only — TAKER race lookahead horizon (ms): the match peek looks this
    /// far ahead for the liquidity-recede check.
    #[serde(default = "default_sim_v2_taker_race_horizon_ms")]
    pub sim_v2_taker_race_horizon_ms: u64,
    /// sim_v2 only — fold the two outcome tokens into one canonical (up) book:
    /// the down token's book/trade/orders are mirrored (p↔1−p, bid↔ask /
    /// buy↔sell) into the up frame and matched against a single shared book.
    /// Removes the cross-outcome double-count of the old complement-merge.
    #[serde(default = "default_sim_v2_fold_outcomes")]
    pub sim_v2_fold_outcomes: bool,
    /// sim_v2 only — TRADE-FLOW taker competition rate ∈ [0,1] (0 = off). A
    /// marketable order competes for the touch with same-direction taker TRADES in
    /// its in-flight window — that volume was consumed by takers who beat us to the
    /// engine, so we fill only the overflow. Trades reveal sub-snapshot burst
    /// competition the book heals (re-quotes) between snapshots. `rate` scales how
    /// much of the competing volume sits ahead of us. With the taker race this is
    /// the taker-volume model (no fill-probability Bernoulli).
    #[serde(default = "default_sim_v2_taker_comp_rate")]
    pub sim_v2_taker_comp_rate: f64,
    /// sim_v2 only — taker competition in-flight window (ms) ≈ taker overhead
    /// exposure (how long the order is in flight, exposed to competitors).
    #[serde(default = "default_sim_v2_taker_comp_window_ms")]
    pub sim_v2_taker_comp_window_ms: u64,
    /// sim_v2 only — deep-queue model for a resting price BEYOND the recorded
    /// 5-level book window: `0` = least-squares linear extrapolation (legacy);
    /// `>0` = project from the OUTERMOST recorded level as `q_edge·decay^(ticks
    /// beyond window)` (`1.0` = flat at the outermost depth, `<1` = geometric
    /// thinning). The recorded book is feed-truncated, not the true book end, so
    /// this models the unobserved deeper queue our order joins.
    #[serde(default = "default_sim_v2_deep_queue_decay")]
    pub sim_v2_deep_queue_decay: f64,
    /// Backtest start time in ISO 8601 (e.g. "2026-02-13T00:00:00Z").
    #[serde(default)]
    pub start_date: String,
    /// Backtest end time in ISO 8601. If empty, defaults to start_date + 1 day.
    #[serde(default)]
    pub end_date: String,
    /// **External sim-exchange TOML** (2026-05-29). Path to a sibling
    /// config file whose top-level keys are flat `sim_*` settings —
    /// they get **merged into `[backtest]` at load time** as if they
    /// had been written inline. Lets operators keep the bulky sim
    /// knobs in a separate file that's easy to swap between runs
    /// (different calibration profiles, A/B parameter sets, etc.).
    ///
    /// Resolution: relative paths are anchored to the main config's
    /// parent directory (same convention as `params_file`). Empty
    /// (default) = no external file; all sim settings come from
    /// inline `sim_*` keys here. When non-empty, the file's keys
    /// override any inline `sim_*` keys (single source of truth wins).
    ///
    /// File shape: flat key-value at top level, e.g.
    /// ```toml
    /// # sim_exchange.toml
    /// sim_latency_calibrate_from = "live.log"
    /// sim_v2_fold_outcomes = true
    /// # ... etc
    /// ```
    #[serde(default)]
    pub simulate_config: String,
    /// Path to a hexbot live.log to auto-calibrate the latency
    /// distribution from. When non-empty, the engine parses every
    /// `[latency] polymarket.http.{place,cancel}_order …` line and
    /// **overrides** the manually-set `sim_latency_p{50,95,99}_ms`
    /// knobs with the median of each percentile across all minute
    /// windows. Censored p99 (clipped at the 500 ms client-timeout)
    /// is replaced by a lognormal extrapolation from (p50, p95).
    ///
    /// Empty (default) = use the manual knobs below as-is.
    ///
    /// Multi-file: also accepts a comma-separated list (e.g.
    /// `"live14.log,live.log"`). The calibrator merges per-minute
    /// percentile vectors and raw RTT pairs across all listed files,
    /// producing one unified CalibratedParams. File order is preserved
    /// for the AR(1) lag-1 ρ estimate; mixing non-contiguous sessions
    /// makes the cross-file lag-1 meaningless, but the percentile /
    /// counter / GPD-tail merges remain clean.
    #[serde(default)]
    pub sim_latency_calibrate_from: String,
    /// Median round-trip latency in ms. Pinpoints the 50th
    /// percentile of the empirical 5-anchor CDF; together with
    /// `sim_latency_p95_ms` and `sim_latency_p99_ms` decouples body
    /// from tail. Map directly to the per-minute statistics the live
    /// `[latency]` summary emits.
    ///
    /// Calibrated to the 2026-04-27 live session: per-minute median
    /// p50 ≈ 60 ms. Tighten / widen by AWS region:
    ///   * 30 ms  — co-located fastpath
    ///   * 60 ms  — AWS us-east-1 → Polymarket (default)
    ///   * 120 ms — cross-region or congested upstream
    #[serde(default = "default_sim_latency_p50_ms")]
    pub sim_latency_p50_ms: u64,
    /// 95th percentile RTT in ms. Anchors the body→tail transition
    /// of the empirical CDF. Live 2026-04-27 per-minute median p95
    /// ≈ 331 ms.
    #[serde(default = "default_sim_latency_p95_ms")]
    pub sim_latency_p95_ms: u64,
    /// 99th percentile RTT in ms. Anchors the deep tail. The live
    /// `[latency]` log usually shows this censored at the 500 ms
    /// client timeout; estimate the true uncensored value as
    /// `p50 · exp(σ̂ · 2.3263)` where `σ̂ = ln(p95/p50) / 1.6449`.
    /// Live 2026-04-27 censored p99 ≈ 501 ms → extrapolated ≈ 700 ms.
    #[serde(default = "default_sim_latency_p99_ms")]
    pub sim_latency_p99_ms: u64,
    /// RNG seed for the latency sampler. Non-zero = deterministic
    /// (reproducible backtests). `0` = fresh entropy each run.
    #[serde(default = "default_sim_latency_seed")]
    pub sim_latency_seed: u64,
    /// AR(1) correlation coefficient for the latent Gaussian state.
    /// Controls how long slow-network regimes last:
    ///
    ///   0.0  = iid (every sample independent)
    ///   0.65 = matches 2026-04-30 live2.log per-side log(RTT) lag-1
    ///          autocorr (current default — pooled across place/cancel)
    ///   0.95 = stronger clustering — slow-regime dwells last longer
    ///   1.0  = degenerate (clamped to 0.999 internally)
    ///
    /// Overridden by the auto-calibrator's `SidedParams.rho_lag1` when
    /// `sim_latency_calibrate_from` resolves with ≥100 paired samples
    /// per side.
    ///
    /// 2026-04-30 live2.log per-side empirical lag-1: place 0.625,
    /// cancel 0.674; the slight asymmetry is consistent across hours.
    #[serde(default = "default_sim_latency_correlation")]
    pub sim_latency_correlation: f64,

    /// Cross-correlation ρ between the place sampler's latent
    /// Gaussian and the cancel sampler's latent Gaussian. When the
    /// gateway is congested, both place and cancel slow down
    /// together — independent samplers (the legacy default = 0)
    /// underestimate the joint timeout probability that drives the
    /// worst-case strategy paths (cancel-also-times-out + reverse
    /// fills pile up).
    ///
    ///   0.0  = independent (legacy behaviour)
    ///   0.63 = empirical 2026-04-30 live2.log default
    ///   0.95 = tight coupling — both sides essentially share regime
    ///
    /// Calibrate from the auto-calibration log line
    /// `[Latency] place↔cancel minute-level corr(log p99)=…`: that
    /// figure is the Pearson correlation of `log(place_p99)` vs
    /// `log(cancel_p99)` across the per-minute summary rows. Setting
    /// `sim_latency_cross_correlation` to roughly that value gives
    /// the BT a coupled tail that matches the live regime moves.
    #[serde(default = "default_sim_latency_cross_correlation")]
    pub sim_latency_cross_correlation: f64,

    /// Client-side HTTP request deadline in ms. When the sampled
    /// round-trip latency `L1 + L2` exceeds this threshold, the
    /// backtest engine substitutes a `NewOrderTimeout` /
    /// `CancelOrderTimeout` for the strategy-bound update and stashes
    /// the real resolved update for `reconcile_orphans` to surface
    /// later. Default 500 ms matches the live fast / cancel HTTP
    /// client pools (see `async_rt::FAST_TIMEOUT` / `CANCEL_TIMEOUT`).
    #[serde(default = "default_sim_client_timeout_ms")]
    pub sim_client_timeout_ms: u64,

    /// Window in ms during which a cancel arriving after a fill is
    /// reported as `Filled` (the live "matched orders can't be
    /// canceled" path). Default 2000 ms — long enough to catch
    /// realistic RTT tails (p99 ≈ 1.3 s) yet short enough that an
    /// unrelated late cancel doesn't get mislabelled.
    #[serde(default = "default_sim_matched_cant_cancel_window_ms")]
    pub sim_matched_cant_cancel_window_ms: u64,

    /// Strategy warmup window in seconds, measured on the sim clock from
    /// the first on_quote tick. While `sim_clock_now − strategy_start <
    /// this`, the strategy returns no signals (no quotes, no cancels)
    /// even though the rest of the on_quote pipeline (poll_*, m_dynamic
    /// init, etc.) keeps running.
    ///
    /// Use case: cold-start parameters that need a few sim seconds to
    /// stabilise — m_dynamic intensity / vol baselines, prediction
    /// model first samples, RTT EWMA — so the BT's first event isn't
    /// quoted off uninitialised internals. Mirrors the empirical
    /// observation that live bots have a 5-15 s "rampup" period after
    /// startup before they trade reliably.
    ///
    /// `0.0` (default) = disabled, strategy quotes from the first
    /// tick (legacy behaviour). Recommended for BT validation: 12.
    #[serde(default)]
    pub sim_strategy_warmup_secs: f64,

    /// **RTT-simulation mode** (2026-05-29). Selects how sim draws
    /// Submit / Cancel RTT. This knob only decides whether the per-event
    /// override layer is built on top of the per-UTC-hour empirical-CDF
    /// model.
    ///
    /// ⚠ **Scope: applies ONLY when `sim_latency_calibrate_from` points at
    /// a log / parquet-archive source.** For the **directory** source
    /// (record-replay of `latency_record` CSVs) this knob is a **no-op** —
    /// the per-event table is built only when the source is NOT a directory
    /// (see `engine.rs`), and record-replay already replays exact per-request
    /// RTT by wall-clock. So if you run the record-replay directory baseline,
    /// `predict` vs `exact` makes no difference. Empty source (static knobs)
    /// is likewise unaffected.
    ///
    /// For the log/archive source:
    ///   * `"predict"` (default) — **prediction mode**, for backtest
    ///     regression. RTT is drawn from the **per-UTC-hour** empirical CDF
    ///     (`HourlyEmpirical`) + AR(1) clustering auto-calibrated from the
    ///     log(s), with a pooled fallback for sparse hours. Generalises
    ///     across days; correct when replaying a window the log wasn't
    ///     recorded on. No per-event matching, and the strategy RTT-gate
    ///     self-tracks its `prev_event_p`.
    ///   * `"exact"` — **exact event-match mode**, for sim-vs-live parity.
    ///     On top of the hourly base the engine builds a per-`event_id` RTT
    ///     table from the same log(s) and, at each Polymarket event boundary,
    ///     overrides (a) the **sampler** with that event's live per-event
    ///     distribution — including the intra-event early/late (first ~60 s
    ///     burst) segmentation and per-event timeout rate, which the hourly
    ///     base cannot express — taking priority over the hourly anchors on
    ///     covered events; and (b) the **strategy gate's** `prev_event_p60`
    ///     (drives RTT-N / quote_n) with the live observation. Events absent
    ///     from the table fall back to the hourly base. Both overrides are
    ///     independent of the hourly wiring, so `exact` is NOT redundant with
    ///     it.
    ///
    /// Per-event table bucketing: every Submit log row is keyed by
    /// `floor(submit_log_ts_secs / 300) * 300` (5-min event boundary),
    /// requiring ≥ `MIN_SAMPLES` (= 3) submits per side. Multi-file
    /// tables are merged last-write-wins on `event_secs`.
    ///
    /// Unknown / empty → `"predict"` (the engine logs the resolved mode).
    #[serde(default = "default_sim_rtt_mode")]
    pub sim_rtt_mode: String,

    /// **Multiplicative compensation for live-side strategy overhead in
    /// the per-event `prev_p` override** (2026-05-21). Only applies in
    /// `sim_rtt_mode = "exact"`.
    ///
    /// Live's `RttGate::record_sample` measures RTT as
    /// `(ack_arrival_at_strategy - on_quote_entry_ts)`. The numerator
    /// is the entry to `on_quote()` where the strategy decided to
    /// place the order; the denominator is when the OrderUpdate from
    /// the executor lands back at the strategy thread. This window
    /// includes:
    ///
    ///   1. Strategy → executor channel hop
    ///   2. Executor processing batched cancels + new orders **serially**
    ///   3. PolymarketTrade.submit_order → server HTTP → response
    ///   4. Executor → strategy channel hop
    ///
    /// `per_event_rtt` parser only sees #3 (HTTP RTT between log rows
    /// `Submit ...` and `Order accepted ...`). It misses the
    /// strategy-side batch overhead (#1-#2 and #4), which is
    /// empirically 22-38 % of the total RTT and **positively correlated
    /// with RTT magnitude** (slow regimes have heavier batch queues).
    ///
    /// On override push, the engine multiplies the parser's
    /// `prev_event_p_ms` by this factor before feeding into the gate,
    /// approximating the live gate's full measurement window.
    ///
    /// `1.0` (default) = no compensation. **The factor is
    /// dataset-dependent and must be re-checked per source log** as
    /// `live_gate_p60 / parser_http_p60`:
    ///   * live 2026-05-20 (3 events): ratio 0.61-0.78 → factor 1.28-1.64
    ///   * live 2026-05-28 (146 events): HTTP p60 81.0ms ≈ gate p60
    ///     82.3ms → ratio 1.016, i.e. **factor ≈ 1.0** (the strategy
    ///     batch overhead seen in May-20 had largely disappeared). A
    ///     stale 1.28 here inflated sim's RTT-N (N mean 1.42 vs live
    ///     1.29, 19 events high / 0 low).
    ///
    /// Only applies in `sim_rtt_mode = "exact"`. Leave at 1.0 unless a
    /// fresh `live_gate_p60 / parser_http_p60` measurement says otherwise.
    #[serde(default = "default_sim_per_event_rtt_overhead_factor")]
    pub sim_per_event_rtt_overhead_factor: f64,

    /// **Record-replay latency source** (2026-06-16). When
    /// `sim_latency_calibrate_from` resolves to a **directory** (instead
    /// of one or more `.log` files), the engine loads every `*.csv` in it
    /// — the per-request place/cancel latency records written live by
    /// `latency_record` — and draws each Submit/Cancel RTT by replaying
    /// those samples (`LatencyProfile::RecordReplay`) instead of the
    /// analytic empirical-CDF model. The two knobs below tune the lookup
    /// tiers (see `sim/latency_record_replay.rs`); they are ignored unless
    /// the calibrate path is a directory.
    ///
    /// Tier-1 (exact wall-clock) max |Δ| in ms between the order's epoch
    /// and the nearest recorded sample. Within the recorded calendar
    /// window, an order this close to a sample replays that sample's RTT
    /// verbatim. Default 300000 (5 min).
    #[serde(default = "default_sim_latency_record_abs_tol_ms")]
    pub sim_latency_record_abs_tol_ms: u64,
    /// Tier-2 (same time-of-day) max circular |Δ| in seconds between the
    /// order's UTC second-of-day and the nearest recorded sample's
    /// second-of-day. Beyond this, the lookup falls to the tier-3 nearest
    /// time-of-day bucket distribution. Default 120.
    #[serde(default = "default_sim_latency_record_tod_tol_secs")]
    pub sim_latency_record_tod_tol_secs: u64,
    /// Tier-3 time-of-day bucket width in seconds. The fallback draws the
    /// `u`-quantile of the nearest non-empty bucket; each bucket pools its
    /// clock-slice across all recorded days. Default 300 (5 min, aligned to
    /// the Polymarket event cadence). Clamped to `[1, 86400]`.
    #[serde(default = "default_sim_latency_record_tod_bucket_secs")]
    pub sim_latency_record_tod_bucket_secs: u64,
    /// **RTT-replay fallback policy** — how the Tier-1/2/3 RTT-sample lookup
    /// picks a sample when the backtest instant is OUTSIDE the recorded
    /// calendar window (Tier 1 exact-wall-clock always wins inside it,
    /// regardless of this knob). Shared by BOTH Tier-1/2/3 sources: the
    /// record-replay directory (`latency_record` CSVs) AND a log/parquet
    /// `sim_latency_calibrate_from` (whose per-request Submit↔ack /
    /// Cancel↔result RTTs are replayed the same way). RTT distribution drifts
    /// across trading days (busier days / heavier system load → fatter tails)
    /// and intra-day sessions, so for an uncovered window a recorded day
    /// CLOSE in the calendar — and ideally the same weekday — is a better
    /// proxy than the all-days pool.
    ///
    ///   * `"pooled"` — legacy: tiers 2/3 pool every recorded day by
    ///     time-of-day (no date awareness). Pre-2026-06-19 behaviour.
    ///   * `"nearest_day"` — prefer the calendar-nearest recorded day; within
    ///     it still match by time-of-day (tier 2) / draw its time-of-day
    ///     bucket distribution (tier 3).
    ///   * `"nearest_day_dow"` (default) — prefer recorded days of the SAME
    ///     NY weekday (nearest among them); only when no same-weekday day
    ///     exists fall back to the nearest day. Captures weekday RTT regime
    ///     (weekend lull vs weekday load).
    ///
    /// Unknown → `pooled`. With a single recorded day all three are identical.
    /// (Renamed from `sim_latency_record_fallback`, still accepted as alias.)
    #[serde(default = "default_rtt_sim_fallback", alias = "sim_latency_record_fallback")]
    pub rtt_sim_fallback: String,

}

/// Default median RTT = 60 ms (2026-04-27 live calibration —
/// per-minute median p50 across the trading session).
fn default_sim_v2_ahead_frac() -> f64 { -1.0 }
fn default_sim_v2_fill_push_mult() -> f64 { 1.5 }
fn default_sim_v2_taker_overhead_p50_ms() -> f64 { 267.0 }
fn default_sim_v2_taker_overhead_p95_ms() -> f64 { 910.0 }
fn default_sim_v2_taker_overhead_p99_ms() -> f64 { 1612.0 }
fn default_sim_v2_maker_race_rate() -> f64 { 0.0 }
fn default_sim_v2_adverse_sel_rate() -> f64 { 0.0 }
fn default_sim_v2_adverse_scale_ticks() -> f64 { 1.0 }
fn default_sim_v2_book_through_rate() -> f64 { 0.0 }
fn default_sim_v2_fill_markout_vn() -> f64 { 0.0 }
fn default_sim_v2_fill_markout_horizon_ms() -> u64 { 2000 }
fn default_sim_v2_taker_race_rate() -> f64 { 0.0 }
fn default_sim_v2_maker_race_horizon_ms() -> u64 { 100 }
fn default_sim_v2_taker_race_horizon_ms() -> u64 { 150 }
fn default_sim_v2_fold_outcomes() -> bool { true }
fn default_sim_v2_taker_comp_rate() -> f64 { 0.0 }
fn default_sim_v2_taker_comp_window_ms() -> u64 { 250 }
fn default_sim_v2_deep_queue_decay() -> f64 { 0.0 }
fn default_sim_latency_p50_ms() -> u64 { 60 }
/// Default 95th-percentile RTT = 331 ms (2026-04-27 live).
fn default_sim_latency_p95_ms() -> u64 { 331 }
/// Default 99th-percentile RTT = 700 ms (lognormal extrapolation
/// from the censored 2026-04-27 live p99 = 501 ms cap).
fn default_sim_latency_p99_ms() -> u64 { 700 }
fn default_sim_latency_seed() -> u64 { 42 }
fn default_sim_latency_correlation() -> f64 { 0.65 }
/// Default cross-correlation between place and cancel latent AR(1)
/// states. Empirical Pearson corr(log(place_p99), log(cancel_p99))
/// across 225 per-minute rows in 2026-04-30 live2.log = **0.631**
/// — both sides ride the same gateway, so a regime shift hits both
/// at once but neither side is the other's perfect mirror (cancel
/// server work is lighter, so its tail is somewhat decoupled).
/// Setting this to 0.0 reproduces the legacy independent-samplers
/// behaviour for back-compat.
fn default_sim_latency_cross_correlation() -> f64 { 0.63 }
fn default_sim_client_timeout_ms() -> u64 { 500 }
fn default_sim_matched_cant_cancel_window_ms() -> u64 { 2_000 }
fn default_sim_per_event_rtt_overhead_factor() -> f64 { 1.0 }
fn default_sim_latency_record_abs_tol_ms() -> u64 { 300_000 }
fn default_sim_latency_record_tod_tol_secs() -> u64 { 120 }
fn default_sim_latency_record_tod_bucket_secs() -> u64 { 300 }
fn default_rtt_sim_fallback() -> String { "nearest_day_dow".to_string() }
fn default_sim_rtt_mode() -> String { "predict".to_string() }

impl Default for BacktestConfig {
    fn default() -> Self {
        Self {
            data_dir: default_output_dir(),
            sim_v2_ahead_frac: default_sim_v2_ahead_frac(),
            sim_v2_fill_push_mult: default_sim_v2_fill_push_mult(),
            sim_v2_taker_overhead_p50_ms: default_sim_v2_taker_overhead_p50_ms(),
            sim_v2_taker_overhead_p95_ms: default_sim_v2_taker_overhead_p95_ms(),
            sim_v2_taker_overhead_p99_ms: default_sim_v2_taker_overhead_p99_ms(),
            sim_v2_maker_race_rate: default_sim_v2_maker_race_rate(),
            sim_v2_adverse_sel_rate: default_sim_v2_adverse_sel_rate(),
            sim_v2_adverse_scale_ticks: default_sim_v2_adverse_scale_ticks(),
            sim_v2_book_through_rate: default_sim_v2_book_through_rate(),
            sim_v2_fill_markout_vn: default_sim_v2_fill_markout_vn(),
            sim_v2_fill_markout_horizon_ms: default_sim_v2_fill_markout_horizon_ms(),
            sim_v2_taker_race_rate: default_sim_v2_taker_race_rate(),
            sim_v2_maker_race_horizon_ms: default_sim_v2_maker_race_horizon_ms(),
            sim_v2_taker_race_horizon_ms: default_sim_v2_taker_race_horizon_ms(),
            sim_v2_fold_outcomes: default_sim_v2_fold_outcomes(),
            sim_v2_taker_comp_rate: default_sim_v2_taker_comp_rate(),
            sim_v2_taker_comp_window_ms: default_sim_v2_taker_comp_window_ms(),
            sim_v2_deep_queue_decay: default_sim_v2_deep_queue_decay(),
            start_date: String::new(),
            end_date: String::new(),
            simulate_config: String::new(),
            sim_latency_calibrate_from: String::new(),
            sim_latency_p50_ms: default_sim_latency_p50_ms(),
            sim_latency_p95_ms: default_sim_latency_p95_ms(),
            sim_latency_p99_ms: default_sim_latency_p99_ms(),
            sim_latency_seed: default_sim_latency_seed(),
            sim_latency_correlation: default_sim_latency_correlation(),
            sim_latency_cross_correlation: default_sim_latency_cross_correlation(),
            sim_client_timeout_ms: default_sim_client_timeout_ms(),
            sim_matched_cant_cancel_window_ms: default_sim_matched_cant_cancel_window_ms(),
            sim_strategy_warmup_secs: 0.0,
            sim_rtt_mode: default_sim_rtt_mode(),
            sim_per_event_rtt_overhead_factor: default_sim_per_event_rtt_overhead_factor(),
            sim_latency_record_abs_tol_ms: default_sim_latency_record_abs_tol_ms(),
            sim_latency_record_tod_tol_secs: default_sim_latency_record_tod_tol_secs(),
            sim_latency_record_tod_bucket_secs: default_sim_latency_record_tod_bucket_secs(),
            rtt_sim_fallback: default_rtt_sim_fallback(),
        }
    }
}

/// Asset identity derived from a Polymarket event-series slug. This is the
/// single source of truth for all per-asset symbols: the strategy reads one
/// `event_series_slug` param (e.g. "eth-up-or-down-5m") and everything else —
/// binance/spot symbols, the polymarket subscription, per-venue prediction
/// symbols, and the chainlink feed-id lookup key — derives from it.
#[derive(Debug, Clone)]
pub struct AssetSymbols {
    /// Lowercase asset token, e.g. "eth".
    pub token: String,
    /// Uppercase asset, e.g. "ETH".
    pub asset: String,
    /// Binance kline/spot symbol, e.g. "ETHUSDT".
    pub binance_symbol: String,
    /// Chainlink/RTDS spot symbol label, e.g. "eth/usd".
    pub spot_symbol: String,
    /// Polymarket market subscription, e.g. "series:eth-up-or-down-5m".
    pub series_subscription: String,
}

/// Derive [`AssetSymbols`] from an event-series slug. Accepts an optional
/// "series:" prefix; the asset token is the first '-'-delimited segment
/// ("eth-up-or-down-5m" → "eth"). Returns `None` for an empty/degenerate slug.
pub fn derive_asset_symbols(event_series_slug: &str) -> Option<AssetSymbols> {
    let slug = event_series_slug
        .strip_prefix("series:")
        .unwrap_or(event_series_slug)
        .trim();
    if slug.is_empty() {
        return None;
    }
    let token = slug.split('-').next().unwrap_or("").to_ascii_lowercase();
    if token.is_empty() {
        return None;
    }
    let asset = token.to_ascii_uppercase();
    Some(AssetSymbols {
        binance_symbol: format!("{asset}USDT"),
        spot_symbol: format!("{token}/usd"),
        series_subscription: format!("series:{slug}"),
        token,
        asset,
    })
}

/// Per-venue spot / prediction symbol for an asset, by exchange name. Mirrors
/// the legacy `binance_symbol`-derived venue mapping:
///   binance/bybit → ASSETUSDT, coinbase → ASSET-USD,
///   kraken → ASSET/USD, okx → ASSET-USDT, else → ASSETUSDT.
pub fn venue_symbol(asset: &str, exchange: &str) -> String {
    match exchange {
        "coinbase" => format!("{asset}-USD"),
        "kraken" => format!("{asset}/USD"),
        "okx" => format!("{asset}-USDT"),
        _ => format!("{asset}USDT"),
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeConfig {
    pub name: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub api_key: String,
    #[serde(default)]
    pub api_secret: String,
    #[serde(default)]
    pub api_passphrase: String,
    /// Ed25519 private key (base58-encoded, 64 bytes keypair or 32 bytes seed).
    /// Used by HexMarket to derive API credentials automatically.
    #[serde(default)]
    pub private_key: String,
    /// BIP39 mnemonic phrase. Alternative to private_key for HexMarket.
    /// If set, the Ed25519 keypair is derived from the mnemonic seed.
    #[serde(default)]
    pub mnemonic: String,
    /// API host URL (e.g. "https://api.hexmarket.xyz" or "https://apidev.hexmarket.xyz").
    #[serde(default)]
    pub api_url_prefix: String,
    /// WebSocket host URL (e.g. "wss://api.hexmarket.xyz/ws" or "wss://apidev.hexmarket.xyz/ws").
    #[serde(default)]
    pub wss_url: String,
    /// Maximum parallel REST API connections (for concurrent order execution).
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    /// API rate limit: max requests per second per wallet account.
    #[serde(default = "default_rate_limit")]
    pub rate_limit_per_second: u32,
    /// Data source variant (e.g. chainlink: "rtds" or "stream"). Default: empty (auto).
    #[serde(default)]
    pub source: String,
    /// Chainlink Data Streams feed IDs (hex) per spot symbol, for the REST
    /// strike/close fetch. Keyed by the spot symbol label (e.g. "eth/usd").
    /// The strategy looks up the slug-derived spot symbol here. TOML inline
    /// table: `feed_ids = { "eth/usd" = "0x…", "btc/usd" = "0x…" }`.
    #[serde(default)]
    pub feed_ids: HashMap<String, String>,
    /// Polymarket signature type: "eoa" (default) or "gnosis_safe".
    #[serde(default)]
    pub signature_type: String,
    /// Polymarket CLOB protocol version: "v1" (default, current) or
    /// "v2" (2026-04-28 cutover). v2 uses a new Exchange contract,
    /// new EIP-712 domain version, drops `taker/expiration/nonce/feeRateBps`
    /// from the signed order, adds `timestamp/metadata/builder`, and
    /// removes `POLY_BUILDER_*` auth headers. See `signer_v2.rs`.
    #[serde(default)]
    pub clob_version: String,
    /// Optional override for the builder-attribution code (bytes32, hex).
    /// v2 only — v1 ignored. Empty / default = all-zeros (no attribution).
    #[serde(default)]
    pub builder_code: String,
    /// v2 only — URL path template for the per-market fee / flags
    /// endpoint. `{conditionId}` is replaced per event. Empty =
    /// `/markets/{conditionId}` (the default guess). Override if the
    /// real v2 endpoint differs (verify via `hexbot market`).
    #[serde(default)]
    pub market_info_v2_path: String,
    /// Whether the live router may batch order placement / cancellation
    /// onto Polymarket's `/orders` and `/orders/cancel` endpoints. When
    /// `false`, every place / cancel falls back to the single-order
    /// `/order` endpoint dispatched concurrently. Polymarket's batch
    /// endpoints currently process the array sequentially server-side
    /// and frequently return higher tail latency than N parallel singles
    /// over a single h2 connection — flip this off when the strategy is
    /// latency-critical and the per-tick batch is small. Default: true.
    #[serde(default = "default_use_batch_orders")]
    pub use_batch_orders: bool,
    /// Polymarket-only — per-request HTTP timeout (ms) for the FAST
    /// (POST /order, POST /orders) and CANCEL (DELETE /order,
    /// DELETE /orders) paths. Single flat value applied across all
    /// UTC hours.
    ///
    /// Setting `0` falls back to the legacy 500 ms (the pre-2026-05-12
    /// default); any value is honoured up to the `async_rt`
    /// client-level ceiling (2000 ms), values above the ceiling are
    /// clamped and logged.
    ///
    /// Prior versions split this across 4 session-of-day buckets
    /// (`asia`/`europe`/`us_am`/`us_pm`); that was simplified to a
    /// single knob as the operational benefit of session-aware
    /// timeouts didn't justify the configuration surface area.
    #[serde(default = "default_http_timeout_ms")]
    pub http_timeout_ms: u64,
    /// Polymarket-only — user-feed periodic gap-replay cadence (ms). Every
    /// interval the feed re-fetches recent `/trades` for the active market to
    /// recover any WS-dropped fills. Sub-second values are honoured. Default
    /// 2000 ms. See `exchange::polymarket::user_feed`.
    #[serde(default = "default_gap_replay_interval_ms")]
    pub gap_replay_interval_ms: u64,
    /// Polymarket-only — how far back (ms) the *periodic* gap-replay rewinds
    /// its `?after=` window from now. Larger = more overlap per sweep (a fill
    /// is covered by multiple sweeps). Quantised to whole seconds for the
    /// second-granular API. Default 5000 ms.
    #[serde(default = "default_gap_replay_rewind_ms")]
    pub gap_replay_periodic_rewind_ms: u64,
    /// Polymarket-only — how far back (ms) the *reconnect* gap-replay rewinds
    /// its `?after=` window from the last-seen match_time, so a fill landing
    /// around the disconnect edge isn't skipped by an exact boundary.
    /// Quantised to whole seconds. Default 5000 ms.
    #[serde(default = "default_gap_replay_rewind_ms")]
    pub gap_replay_reconnect_rewind_ms: u64,
    /// Polymarket-only — number of executor worker threads that dispatch
    /// order signals concurrently. The strategy enqueues BatchUpdateOrders /
    /// place / cancel signals; N workers pull from a shared queue so a slow
    /// HTTP dispatch (RTT spike / timeout) on one signal doesn't stall the
    /// others (previously a single serial executor blocked on each drain →
    /// "Signal stale" storms under load). All workers share each instance's
    /// SharedState, so order tracking stays consistent. Default 8.
    #[serde(default = "default_executor_workers")]
    pub executor_workers: usize,
}

fn default_http_timeout_ms() -> u64 { 1000 }
fn default_gap_replay_interval_ms() -> u64 { 2000 }
fn default_gap_replay_rewind_ms() -> u64 { 5000 }
fn default_executor_workers() -> usize { 8 }

fn default_use_batch_orders() -> bool {
    true
}

fn default_max_connections() -> usize {
    4
}
fn default_rate_limit() -> u32 {
    10
}

#[derive(Debug, Clone, Deserialize)]
pub struct StrategyConfig {
    pub name: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Per-strategy instance identifier — keys into
    /// `[poly.<instance_id>]` in secrets.toml for credentials, and
    /// tags every outbound Signal so the executor routes to the
    /// matching PolymarketTrade / SharedState. MUST be unique across
    /// enabled polymaker strategies. Lives on the strategy block
    /// (not under `params`) because it's identity, not parameters —
    /// distinguishing one strategy instance from another even when
    /// they share the same params file.
    #[serde(default)]
    pub instance_id: String,
    /// Account identity — keys into `[poly.<account_id>]` in
    /// secrets.toml for credentials/signer/funder, and groups
    /// strategies that share ONE Polymarket wallet (one SharedState,
    /// one user-feed, one heartbeat, one RTT-probe). Multiple
    /// strategy instances (distinct `instance_id`, e.g. BTC + ETH)
    /// may share the same `account_id`. Empty = fall back to
    /// `instance_id` (legacy: instance == account, one wallet per
    /// strategy). Distinct from `instance_id`, which still tags every
    /// outbound Signal and must be unique across enabled strategies.
    #[serde(default)]
    pub account_id: String,
    /// Optional path (absolute or relative to the parent config's
    /// directory) to a separate TOML file holding this strategy's
    /// params as top-level keys. Loaded and merged into `params` at
    /// `Config::load`. When inline `[strategies.params]` is also
    /// present, inline keys OVERRIDE the file's keys — so the file
    /// holds the canonical config and the inline block (if any) is
    /// for quick per-deployment tweaks. Empty = legacy behaviour
    /// (only inline params).
    #[serde(default)]
    pub params_file: String,
    #[serde(default)]
    pub params: HashMap<String, toml::Value>,
}

impl StrategyConfig {
    /// Resolved account identity: explicit `account_id` if set,
    /// otherwise falls back to `instance_id` (legacy: one wallet per
    /// strategy). Use this everywhere credentials / SharedState /
    /// user-feed are keyed.
    pub fn account_id(&self) -> &str {
        if self.account_id.is_empty() {
            &self.instance_id
        } else {
            &self.account_id
        }
    }
}

fn default_log_level() -> String {
    "info".to_string()
}
fn default_output_dir() -> String {
    "./data".to_string()
}
fn default_paper_data_dir() -> String {
    "./paper_data".to_string()
}
fn default_file_prefix() -> String {
    "market_data".to_string()
}
fn default_true() -> bool {
    true
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("read config {}: {}", path.display(), e))?;

        // **Shared secrets push** — the `[builder]` / `[chainlink]` /
        // `[polygon]` sections of the secrets file replace the old `.env`
        // keys. Push them into the env BEFORE `${VAR}` expansion so config
        // refs (e.g. chainlink `api_key = "${CHAINLINK_STREAM_API_KEY}"`)
        // AND direct env reads (builder creds, `POLYGON_RPC`) all source
        // from the secrets file. The secrets path comes from
        // `general.secrets_file`; parse the RAW content just to read that
        // plain path (it never contains `${VAR}`). Best-effort — a missing
        // secrets file leaves the env untouched, so the missing secret
        // surfaces as an explicit error at its point of use.
        {
            let secrets_hint = toml::from_str::<toml::Value>(&content)
                .ok()
                .and_then(|v| {
                    v.get("general")
                        .and_then(|g| g.get("secrets_file"))
                        .and_then(|s| s.as_str())
                        .map(|s| s.to_string())
                })
                .unwrap_or_default();
            let secrets_path = SecretsFile::resolve_path_with_override(path, &secrets_hint);
            if let Ok(sf) = SecretsFile::load(&secrets_path) {
                sf.apply_shared_to_env();
            }
        }

        let content = resolve_env_vars(&content);

        // **External simulate_config splice** (2026-05-29). Before
        // deserialising into `Config`, parse the raw TOML into a
        // `toml::Value` tree so we can splice the contents of
        // `[backtest].simulate_config` (if set) into the `[backtest]`
        // table. This lets operators factor the bulky sim_* knobs
        // into a sibling file without making `BacktestConfig`'s
        // struct shape mutable.
        //
        // Policy: external file's keys OVERRIDE inline `[backtest]`
        // values when both are present. The external file is the
        // single source of truth once `simulate_config` is set.
        let config_dir = path.parent().map(|p| p.to_path_buf())
            .unwrap_or_else(|| std::path::PathBuf::from("."));
        let mut root_value: toml::Value = toml::from_str(&content)?;
        if let Some(sim_path_str) = root_value.get("backtest")
            .and_then(|bt| bt.get("simulate_config"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
        {
            let resolved = if std::path::Path::new(&sim_path_str).is_absolute() {
                std::path::PathBuf::from(&sim_path_str)
            } else {
                config_dir.join(&sim_path_str)
            };
            let raw = std::fs::read_to_string(&resolved).map_err(|e| {
                anyhow::anyhow!(
                    "simulate_config: failed to read {}: {}",
                    resolved.display(), e,
                )
            })?;
            let expanded = resolve_env_vars(&raw);
            let sim_value: toml::Value = toml::from_str(&expanded)
                .map_err(|e| anyhow::anyhow!(
                    "simulate_config: parse error in {}: {}",
                    resolved.display(), e,
                ))?;
            let sim_table = sim_value.as_table().ok_or_else(|| anyhow::anyhow!(
                "simulate_config: {} must contain a flat key-value table at the top level",
                resolved.display(),
            ))?;
            // Splice sim_table's top-level keys into root[backtest].
            let bt_table = root_value.get_mut("backtest")
                .and_then(|v| v.as_table_mut())
                .ok_or_else(|| anyhow::anyhow!(
                    "main config has no [backtest] table — \
                     simulate_config requires it"
                ))?;
            // Track how many keys we merged so the engine startup log
            // can surface the indirection (handy for debugging which
            // file actually configured a given knob).
            let mut merged = 0usize;
            for (k, v) in sim_table.iter() {
                // Don't let the external file set `simulate_config`
                // recursively — block that key to avoid surprise
                // chains (and to keep the parse linear).
                if k == "simulate_config" { continue; }
                bt_table.insert(k.clone(), v.clone());
                merged += 1;
            }
            log::info!(
                "[config] simulate_config: merged {} key(s) from {} into [backtest]",
                merged, resolved.display(),
            );
        }
        let mut config: Config = root_value.try_into()
            .map_err(|e| anyhow::anyhow!("config deserialise error: {}", e))?;

        // Per-strategy params_file: load + merge.
        //
        // Each `[[strategies]]` entry may set `params_file = "live/<name>.toml"`
        // to keep the giant per-strategy param block out of the main config.
        // We resolve the path relative to the main config's directory (so
        // operators can keep the layout self-contained), env-expand `${VAR}`
        // tokens the same way the main file does, then merge into `params`.
        // Inline `[strategies.params]` keys (if present) OVERRIDE the
        // file-loaded values — quick tweaks win over the canonical file.
        // (`config_dir` was already resolved above for the
        // simulate_config splice; reuse it.)

        // Pre-resolve `general.secrets_file` to an absolute path so
        // downstream consumers (Engine, CLI, anywhere) don't have to
        // re-derive the config_dir. Leaves empty unchanged (= fall
        // back to the env-var / sibling / cwd cascade in
        // `SecretsFile::resolve_path`).
        if !config.general.secrets_file.is_empty() {
            let p = std::path::Path::new(&config.general.secrets_file);
            if !p.is_absolute() {
                config.general.secrets_file = config_dir.join(p)
                    .to_string_lossy().into_owned();
            }
        }

        for s in config.strategies.iter_mut() {
            if s.params_file.is_empty() { continue; }
            let resolved = if std::path::Path::new(&s.params_file).is_absolute() {
                std::path::PathBuf::from(&s.params_file)
            } else {
                config_dir.join(&s.params_file)
            };
            let raw = std::fs::read_to_string(&resolved).map_err(|e| {
                anyhow::anyhow!(
                    "strategy '{}': failed to read params_file {}: {}",
                    s.name, resolved.display(), e,
                )
            })?;
            let expanded = resolve_env_vars(&raw);
            let file_params: HashMap<String, toml::Value> = toml::from_str(&expanded)
                .map_err(|e| anyhow::anyhow!(
                    "strategy '{}': parse {} as TOML key-value map: {}",
                    s.name, resolved.display(), e,
                ))?;
            // Merge: file values first, then inline overrides (so the
            // inline block from the main config wins). Cheaper to
            // assemble a fresh map than to splice into the existing
            // one — both sides are typically <500 entries.
            let mut merged: HashMap<String, toml::Value> = file_params;
            for (k, v) in s.params.drain() {
                merged.insert(k, v);
            }
            s.params = merged;
        }

        // Phase 6: enforce one-shot migration. Polymarket credentials
        // MUST live in `secrets.toml` keyed by `instance_id`, never
        // in `[[exchanges]] polymarket`. Surface a hard error with
        // the migration path if any forbidden field is set on a
        // polymarket exchange entry.
        for e in &config.exchanges {
            if e.name != "polymarket" { continue; }
            let mut leaks: Vec<&'static str> = Vec::new();
            if !e.api_key.is_empty()        { leaks.push("api_key"); }
            if !e.api_secret.is_empty()     { leaks.push("api_secret"); }
            if !e.api_passphrase.is_empty() { leaks.push("api_passphrase"); }
            if !e.private_key.is_empty()    { leaks.push("private_key"); }
            if !e.signature_type.is_empty() { leaks.push("signature_type"); }
            if !e.builder_code.is_empty()   { leaks.push("builder_code"); }
            if !leaks.is_empty() {
                return Err(anyhow::anyhow!(
                    "config error: `[[exchanges]] polymarket` carries forbidden \
                     credential field(s) {:?}. Move these into `secrets.toml` \
                     under `[poly.<instance_id>]` and reference them via \
                     each `[[strategies]]` block's `params.instance_id`. See \
                     comment block above SecretsFile in src/config.rs for the \
                     expected layout.", leaks,
                ));
            }
        }

        Ok(config)
    }
}

// ════════════════════════════════════════════════════════════════
// Multi-strategy secrets — `secrets.toml`
// ════════════════════════════════════════════════════════════════
//
// **Format** (default path: `./secrets.toml`; override via env
// `HEXBOT_SECRETS` or per-config `secrets_path` knob):
//
//   # secrets.toml
//   [poly.makerA]
//   api_key        = "uuid-A"
//   api_secret     = "base64-A"
//   api_passphrase = "phrase-A"
//   private_key    = "0x...a"
//   signature_type = "poly_1271"     # deposit wallet (default); or
//                                    # "gnosis_safe" / "eoa"
//   funder         = "0x..."         # deposit-wallet address (poly_1271)
//
//   [poly.makerB]
//   …
//
//   # ── Shared (non-per-instance) secrets ──────────────────────────
//   # Everything that used to live in `.env` now belongs here. On
//   # `Config::load` these sections are pushed into the legacy env vars
//   # (`POLY_BUILDER_*`, `CHAINLINK_STREAM_*`, `POLYGON_RPC[_2]`) BEFORE
//   # `${VAR}` expansion, so both direct env reads and config refs source
//   # from this file. Non-empty values override `.env`; a missing/empty
//   # field falls back to `.env` (transition aid).
//
//   [builder]                         # Polymarket Builder relayer auth
//   api_key        = "uuid"           # → POLY_BUILDER_API_KEY
//   api_secret     = "base64"         # → POLY_BUILDER_SECRET
//   api_passphrase = "phrase"         # → POLY_BUILDER_PASSPHRASE
//   builder_code   = "0xbu..."        # optional order-attribution bytes32.
//                                     # The ONE attribution code for every
//                                     # wallet — both the live bot and the
//                                     # CLI source it from here (there is no
//                                     # per-instance override).
//
//   [chainlink]                       # Chainlink Data Streams (rtds)
//   api_key    = "..."                # → CHAINLINK_STREAM_API_KEY
//   api_secret = "..."                # → CHAINLINK_STREAM_API_SECRET
//
//   [polygon]                         # Polygon JSON-RPC endpoints
//   rpc   = "https://..."             # → POLYGON_RPC   (primary)
//   rpc_2 = "https://..."             # optional → POLYGON_RPC_2 (failover)
//
// Each `[strategies.params]` block references its credentials via
// `instance_id = "<name>"` which keys into `poly.<name>`. The engine
// builds one SharedState per instance (HTTP pool shared across
// strategies but auth / signer / nonce / orderID registry are
// per-instance).
//
// Env-var expansion still works inside this file via the same
// `${VAR}` syntax `resolve_env_vars` handles in `config.toml`.

/// Per-instance Polymarket credentials. Loaded from the
/// `[poly.<instance_id>]` block in `secrets.toml`.
#[derive(Debug, Clone, Deserialize)]
pub struct PolymarketSecrets {
    pub api_key: String,
    pub api_secret: String,
    pub api_passphrase: String,
    pub private_key: String,
    /// `"eoa"` or `"gnosis_safe"`. Same accepted strings as the legacy
    /// `[[exchanges]] polymarket.signature_type` knob.
    #[serde(default = "default_signature_type")]
    pub signature_type: String,
    /// CLOB v2 **deposit wallet** address (the order `maker`/`signer` and
    /// the funding wallet) when `signature_type = "poly_1271"`. Ignored
    /// for other signature types. Empty + poly_1271 = error at signing
    /// time. See [`crate::exchange::polymarket::deposit_wallet`].
    #[serde(default)]
    pub funder: String,
}

fn default_signature_type() -> String {
    // CLOB v2 default: the deposit-wallet (POLY_1271) flow. Accounts that
    // still trade from a legacy Gnosis Safe must set `signature_type =
    // "gnosis_safe"` explicitly. See `exchange::polymarket::deposit_wallet`.
    "poly_1271".to_string()
}

/// Polymarket Builder relayer credentials (`[builder]`). Drives the
/// gasless relayer path (deploy / approvals / redeem / split). Mirrors the
/// `POLY_BUILDER_*` env vars.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct BuilderSecrets {
    #[serde(default)]
    pub api_key: String,
    #[serde(default)]
    pub api_secret: String,
    #[serde(default)]
    pub api_passphrase: String,
    /// Optional builder-attribution bytes32; empty = no attribution.
    #[serde(default)]
    pub builder_code: String,
}

/// Chainlink Data Streams credentials (`[chainlink]`). Feeds the rtds
/// spot price stream. Mirrors `CHAINLINK_STREAM_API_KEY` / `_SECRET`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ChainlinkSecrets {
    #[serde(default)]
    pub api_key: String,
    #[serde(default)]
    pub api_secret: String,
}

/// Polygon JSON-RPC endpoints (`[polygon]`). Used for on-chain reads /
/// `gas_via_signer_wallet` broadcasts. Mirrors `POLYGON_RPC` (primary) and
/// `POLYGON_RPC_2` (optional failover).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PolygonSecrets {
    #[serde(default)]
    pub rpc: String,
    #[serde(default)]
    pub rpc_2: String,
}

/// Container for the whole `secrets.toml`: per-instance `[poly.<id>]`
/// blocks plus the shared `[builder]` / `[chainlink]` / `[polygon]`
/// sections that replace the corresponding `.env` keys.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SecretsFile {
    #[serde(default)]
    pub poly: HashMap<String, PolymarketSecrets>,
    #[serde(default)]
    pub builder: Option<BuilderSecrets>,
    #[serde(default)]
    pub chainlink: Option<ChainlinkSecrets>,
    #[serde(default)]
    pub polygon: Option<PolygonSecrets>,
}

impl SecretsFile {
    /// Push the shared (non-per-instance) sections — `[builder]`,
    /// `[chainlink]`, `[polygon]` — into the legacy env vars that existing
    /// consumers (direct `std::env::var` reads) and config `${VAR}`
    /// expansion read. Only NON-EMPTY values are applied, and they
    /// OVERRIDE any prior env value — consistent with how per-instance
    /// poly creds are pushed in `cli_account`. An absent/empty section or
    /// field leaves the env untouched, so `.env` still serves as a
    /// fallback during migration.
    ///
    /// SAFETY: invoked from `Config::load` (and thus from `main`/CLI
    /// subcommands) before any background thread or consumer reads these
    /// vars; no data race.
    pub fn apply_shared_to_env(&self) {
        fn set_if(name: &str, val: &str) {
            if !val.is_empty() {
                std::env::set_var(name, val);
            }
        }
        if let Some(b) = &self.builder {
            set_if("POLY_BUILDER_API_KEY", &b.api_key);
            set_if("POLY_BUILDER_SECRET", &b.api_secret);
            set_if("POLY_BUILDER_PASSPHRASE", &b.api_passphrase);
            set_if("POLY_BUILDER_CODE", &b.builder_code);
        }
        if let Some(c) = &self.chainlink {
            set_if("CHAINLINK_STREAM_API_KEY", &c.api_key);
            set_if("CHAINLINK_STREAM_API_SECRET", &c.api_secret);
        }
        if let Some(p) = &self.polygon {
            set_if("POLYGON_RPC", &p.rpc);
            set_if("POLYGON_RPC_2", &p.rpc_2);
        }
    }
}

impl SecretsFile {
    /// Load + env-expand from disk. Returns empty `SecretsFile` when
    /// the path doesn't exist (so single-account legacy configs that
    /// don't reference any instance_id still work for non-trading
    /// CLI commands).
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let content = std::fs::read_to_string(path)?;
        let content = resolve_env_vars(&content);
        let secrets: SecretsFile = toml::from_str(&content)
            .map_err(|e| anyhow::anyhow!("parse {}: {}", path.display(), e))?;
        Ok(secrets)
    }

    /// Resolve the on-disk path. Priority:
    ///   1. `config.general.secrets_file` (if set in main config;
    ///      resolved relative to the main config's directory when not
    ///      absolute).
    ///   2. `$HEXBOT_SECRETS` env var.
    ///   3. `<config_dir>/secrets.toml`.
    ///   4. `./secrets.toml`.
    pub fn resolve_path(config_path: &Path) -> std::path::PathBuf {
        Self::resolve_path_with_override(config_path, "")
    }

    /// Same as `resolve_path` but with an explicit override (typically
    /// from `Config.secrets_file`). Empty override falls through to
    /// the env var / sibling / cwd cascade.
    pub fn resolve_path_with_override(
        config_path: &Path,
        config_secrets_file: &str,
    ) -> std::path::PathBuf {
        if !config_secrets_file.is_empty() {
            let p = std::path::Path::new(config_secrets_file);
            return if p.is_absolute() {
                p.to_path_buf()
            } else {
                config_path.parent()
                    .map(|d| d.join(p))
                    .unwrap_or_else(|| p.to_path_buf())
            };
        }
        if let Ok(p) = std::env::var("HEXBOT_SECRETS") {
            return std::path::PathBuf::from(p);
        }
        if let Some(dir) = config_path.parent() {
            let candidate = dir.join("secrets.toml");
            if candidate.exists() {
                return candidate;
            }
        }
        std::path::PathBuf::from("./secrets.toml")
    }

    /// Lookup credentials by `instance_id`. Returns a descriptive
    /// `Err` listing the available instance_ids when the key is
    /// missing — common operator footgun is a typo in
    /// `[strategies.params].instance_id`.
    pub fn poly_for(&self, instance_id: &str) -> Result<&PolymarketSecrets> {
        self.poly.get(instance_id).ok_or_else(|| {
            let available: Vec<&String> = self.poly.keys().collect();
            anyhow::anyhow!(
                "secrets.toml: no `[poly.{}]` block found. \
                 Available poly instance_ids: {:?}",
                instance_id, available,
            )
        })
    }
}


/// Substitute `${ENV_VAR}` patterns in a string with environment variable values.
/// Unset variables are replaced with empty strings.
fn resolve_env_vars(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '$' && chars.peek() == Some(&'{') {
            chars.next(); // consume '{'
            let mut var_name = String::new();
            for c in chars.by_ref() {
                if c == '}' { break; }
                var_name.push(c);
            }
            if !var_name.is_empty() {
                if let Ok(val) = std::env::var(&var_name) {
                    result.push_str(&val);
                }
                // else: unset → empty string (replaced with nothing)
            }
        } else {
            result.push(ch);
        }
    }
    result
}

#[cfg(test)]
mod account_id_tests {
    use super::*;

    fn strat(instance_id: &str, account_id: &str) -> StrategyConfig {
        StrategyConfig {
            name: "polymaker".into(),
            enabled: true,
            instance_id: instance_id.into(),
            account_id: account_id.into(),
            params_file: String::new(),
            params: HashMap::new(),
        }
    }

    #[test]
    fn account_id_falls_back_to_instance_id_when_unset() {
        // Legacy single-wallet path: no account_id → account == instance.
        assert_eq!(strat("btc", "").account_id(), "btc");
    }

    #[test]
    fn explicit_account_id_decouples_from_instance_id() {
        // Two instances (BTC + ETH) sharing one wallet "main".
        assert_eq!(strat("btc", "main").account_id(), "main");
        assert_eq!(strat("eth", "main").account_id(), "main");
    }
}

#[cfg(test)]
mod shared_secrets_tests {
    use super::*;

    /// End-to-end: a config naming a secrets file with `[builder]` /
    /// `[chainlink]` / `[polygon]` sections should (a) expand the
    /// chainlink `${VAR}` config refs from the secrets `[chainlink]`
    /// block, and (b) push `[builder]` / `[polygon]` into their env vars.
    #[test]
    fn config_load_pulls_shared_secrets_from_secrets_file() {
        let dir = std::env::temp_dir().join(format!("hexbot_shared_secrets_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let cfg_path = dir.join("cfg.toml");
        let sec_path = dir.join("secrets.toml");

        std::fs::write(
            &cfg_path,
            "[general]\nsecrets_file = \"secrets.toml\"\n\
             [[exchanges]]\nname = \"chainlink\"\n\
             api_key = \"${CHAINLINK_STREAM_API_KEY}\"\n\
             api_secret = \"${CHAINLINK_STREAM_API_SECRET}\"\n",
        )
        .unwrap();
        std::fs::write(
            &sec_path,
            "[builder]\napi_key = \"bk-XYZ\"\napi_secret = \"bs-XYZ\"\napi_passphrase = \"bp-XYZ\"\n\
             [chainlink]\napi_key = \"cl-KEY-123\"\napi_secret = \"cl-SEC-123\"\n\
             [polygon]\nrpc = \"https://rpc-primary.example\"\nrpc_2 = \"https://rpc-failover.example\"\n",
        )
        .unwrap();

        let cfg = Config::load(&cfg_path).expect("config loads");

        // (a) chainlink `${VAR}` refs resolved FROM the secrets file.
        let cl = cfg.exchanges.iter().find(|e| e.name == "chainlink").expect("chainlink exchange");
        assert_eq!(cl.api_key, "cl-KEY-123", "chainlink api_key from secrets [chainlink]");
        assert_eq!(cl.api_secret, "cl-SEC-123");

        // (b) builder + polygon pushed into the legacy env vars.
        assert_eq!(std::env::var("POLY_BUILDER_API_KEY").unwrap(), "bk-XYZ");
        assert_eq!(std::env::var("POLY_BUILDER_PASSPHRASE").unwrap(), "bp-XYZ");
        assert_eq!(std::env::var("POLYGON_RPC").unwrap(), "https://rpc-primary.example");
        assert_eq!(std::env::var("POLYGON_RPC_2").unwrap(), "https://rpc-failover.example");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
