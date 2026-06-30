//! Backtest simulated-latency sampler.
//!
//! Latency is modelled as an **empirical 5-anchor CDF** (piecewise-
//! linear in cumulative-probability space) with serial correlation
//! via a Gaussian copula AR(1) latent state. Three operator-facing
//! knobs map directly to the same statistics the live latency log
//! emits per minute, so calibrating against a recorded session is a
//! one-shot operation:
//!
//!   * `p50_ms` — median RTT (live `[latency] place_order p50=…`)
//!   * `p95_ms` — 95th percentile RTT (live `p95=…`)
//!   * `p99_ms` — 99th percentile RTT (live `p99=…`)
//!   * `rho`    — AR(1) correlation of the latent Gaussian
//!
//! ## Why `(p50, p95, p99)` instead of a 2-parameter family
//!
//! Empirical Polymarket request RTT has a "kink": the body climbs
//! ~3× faster between p50 and p75 than any (μ, σ) lognormal predicts,
//! then the tail (p99 → p99.9) flattens. Fitting a single lognormal
//! mis-fits at one end (σ matched to p99 undershoots p95 by ~35 %;
//! σ matched to p95 overshoots p99.9 by ~2×). Empirical-shape
//! anchoring lets operators pin the body and the tail independently
//! to directly-observed live statistics.
//!
//! ## Auto-calibration from a live.log
//!
//! `calibrate_from_log(path)` parses the per-minute latency lines
//! emitted by `hexbot::latency` ("[latency] polymarket.http.{place,
//! cancel}_order n=… p50=… p95=… p99=…") and returns the median of
//! each percentile across all minute windows. When the median p99
//! is censored by the client-timeout cap (≥ 480 ms), it's replaced
//! by a lognormal extrapolation from `p50`/`p95`:
//!
//!   σ̂ = ln(p95/p50) / Φ⁻¹(0.95) = ln(p95/p50) / 1.6449
//!   p99_extrapolated = p50 · exp(σ̂ · Φ⁻¹(0.99)) = p50 · exp(σ̂ · 2.3263)
//!
//! The engine wires this in transparently: setting
//! `sim_latency_calibrate_from = "path/to/live.log"` overrides any
//! manually-set `p50`/`p95`/`p99` knobs with the parsed values.
//!
//! ## Why serial correlation matters
//!
//! Real network latency clusters: a 500 ms RTT is followed by another
//! large RTT with high probability. Analysis of `live.log` 41k samples:
//!
//!   lag-1 autocorr of log(RTT)         = +0.85
//!   P(next = HIGH | current = HIGH)    = 78 %  (vs 2 % iid)
//!   mean consecutive HIGH-state run    = 4.6   (vs 1.1 iid)
//!
//! An iid sampler — even one that matches the marginal exactly —
//! severely underestimates how long bad-latency periods last. A
//! backtest run against iid samples sees isolated single-event
//! timeouts; a production run sees 5–50 consecutive slow events that
//! pile up cancel/place races. Without clustering, the strategy's
//! orphan-reconciliation, timeout handling, and inventory-risk-under-
//! pile-up code paths are not exercised.
//!
//! ## Algorithm: Gaussian copula AR(1) → empirical inverse CDF
//!
//!   ε ~ N(0, 1)                              (Box-Muller)
//!   z ← ρ·z_prev + √(1 − ρ²)·ε               (AR(1) — preserves N(0,1))
//!   u = Φ(z)                                  (Φ = standard-normal CDF)
//!   RTT = inverse_cdf(anchors, u)
//!
//! The 5-anchor inverse CDF interpolates linearly between
//!   (0.000, p50/5)  ← network floor
//!   (0.500, p50)
//!   (0.950, p95)
//!   (0.990, p99)
//!   (0.999, p99 · (p99/p95)^k)  ← lognormal-style tail extrapolation
//!
//! and clamps to the bounds outside [0.000, 0.999]. The body/tail
//! decoupling that motivated the redesign is purely from the (p50,
//! p95, p99) choice — the rest is mechanical.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// Empirical-CDF anchor set for one (p50, p95, p99[, p99.9]) tuple.
/// Used both as the single-profile body of `LatencyProfile::Empirical`
/// and as the per-hour entry in `LatencyProfile::HourlyEmpirical`.
///
/// `p85_ms_override` is the optional body-shape anchor. Live ≥ 2026-05-15
/// emits `p85=` in the `[latency]` summary; calibrator surfaces it on
/// `SidedParams.p85_ms` and the engine plumbs it here. When `None`, the
/// CDF interpolates linearly between (0.50, p50) and (0.95, p95) at
/// u=0.85 — exactly the legacy 6-anchor behaviour. When `Some`, the
/// 7-anchor curve captures the body bimodality observed in live14.log
/// (`p85/p50 ≈ 5–10×`).
#[derive(Debug, Clone, Copy)]
pub struct EmpiricalAnchors {
    pub p50_ms: f64,
    pub p85_ms_override: Option<f64>,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub p999_ms_override: Option<f64>,
    /// Optional GPD tail fit. When `Some`, the sampler positions its
    /// upper-quantile anchors (p99 / p99.9 / p99.99) from the GPD
    /// quantile function instead of lognormal extrapolation. See
    /// `SidedParams::gpd_tail` for the fitting details. Per-hour
    /// buckets typically share the pooled fit (stationary-tail
    /// assumption): build the per-hour `EmpiricalAnchors` with the
    /// pooled side's `GpdTail` instead of fitting per-hour.
    pub gpd_tail: Option<GpdTail>,
}

#[derive(Debug, Clone)]
pub enum LatencyProfile {
    /// Single fixed one-way latency in ms. Every `sample_ns()` returns
    /// the same value converted to ns. Kept for unit tests / paper
    /// mode parity.
    Fixed(u64),
    /// Empirical 5-anchor RTT CDF, with `p50/p95/p99` directly
    /// matching the live `[latency]` summary. `rho` controls AR(1)
    /// serial correlation of the latent Gaussian.
    ///
    /// `p999_ms_override` controls the 0.999-quantile anchor:
    ///   * `Some(v)` — pin the tail to `v`, used by the calibrator
    ///     when the observed cap-hit rate is non-zero but too small
    ///     for the cap-rate-driven primary branch (< 0.5 %). Solving
    ///     the anchor backward from `P(RTT > 500ms) = cap_rate`
    ///     prevents the lognormal extrapolation from running away
    ///     and over-modelling the tail.
    ///   * `None` — fall back to lognormal extrapolation in
    ///     `empirical_anchors` (same shape as the body, projected
    ///     out via the `(log p99 - log p95)` slope).
    Empirical {
        p50_ms: f64,
        /// Optional 85th-percentile body anchor. See `EmpiricalAnchors`
        /// for the rationale and fall-back semantics.
        p85_ms_override: Option<f64>,
        p95_ms: f64,
        p99_ms: f64,
        rho: f64,
        p999_ms_override: Option<f64>,
        /// GPD tail from POT calibration. When `Some`, the sampler's
        /// p99/p99.9/p99.99 anchors are derived from the GPD quantile
        /// function instead of lognormal extrapolation. See
        /// `GpdTail` for the parameters.
        gpd_tail: Option<GpdTail>,
    },
    /// 24 separate empirical CDFs, indexed by UTC hour-of-day
    /// (0..23). Each draw resolves the active anchor table by
    /// computing `(now_ns / 1e9 / 3600) % 24`; hours without a
    /// populated entry fall back to `fallback`. Built by
    /// `calibrate_from_log` when the parsed log spans enough hours
    /// with sufficient samples to make per-hour percentiles stable.
    /// The single AR(1) latent state is shared across hours — the
    /// inverse CDF mapping just swaps under it, so clustering still
    /// works regardless of which hour is active.
    HourlyEmpirical {
        hourly: Box<[Option<EmpiricalAnchors>; 24]>,
        fallback: EmpiricalAnchors,
        rho: f64,
    },
    /// Two-bucket day-of-week empirical CDF: separate `EmpiricalAnchors`
    /// for NY-Saturday vs every other day. Used when calibration shows
    /// Sat's RTT distribution materially diverges from the rest of the
    /// week (live14-19 evidence: Sat p85 ≈ 950 ms vs non-Sat ≈ 1280 ms,
    /// p95 ≈ 1660 vs 1940). Sampler dispatches by converting `now_ns`
    /// to America/New_York wall-time and checking `weekday == Sat`. The
    /// single AR(1) latent state is shared across buckets — same logic
    /// as HourlyEmpirical (only the inverse-CDF mapping swaps).
    SaturdayEmpirical {
        sat: EmpiricalAnchors,
        non_sat: EmpiricalAnchors,
        rho: f64,
    },
    /// **Record-replay** (2026-06-16): draw from per-request place/cancel
    /// RTT samples recorded live (the `latency_record` CSVs) instead of an
    /// analytic CDF. At each draw the sampler resolves the order's epoch
    /// against the recorded samples via [`super::latency_record_replay::
    /// SideRecords::lookup`]'s three tiers (exact wall-clock → same
    /// time-of-day nearest → nearest time-of-day distribution). `rho`
    /// drives the AR(1) latent whose `u = Φ(z)` selects the tier-3
    /// quantile (tiers 1 & 2 are deterministic). See the
    /// `latency_record_replay` module doc. Built by the engine when
    /// `sim_latency_calibrate_from` points at a directory.
    RecordReplay {
        records: std::sync::Arc<super::latency_record_replay::SideRecords>,
        rho: f64,
        params: super::latency_record_replay::RecordReplayParams,
    },
}

/// One side's (place OR cancel) calibrated empirical-CDF anchors plus
/// the underlying counts that produced them. See `calibrate_from_log`
/// doc for the algorithm.
#[derive(Debug, Clone, Copy)]
pub struct SidedParams {
    pub p50_ms: f64,
    /// 85th-percentile RTT. Diagnostic-only: not consumed by the
    /// 5-anchor CDF sampler (which interpolates p85 from (p50, p95)
    /// linearly in cumulative-probability space). `None` when the
    /// parsed log doesn't carry a `p85=` field (older live builds
    /// before 2026-05-15 only emitted p50/p95/p99/p99.9).
    ///
    /// Why expose this: live14.log analysis showed `p85/p50 ≈ 5–10×`
    /// — the body→tail transition is at p85, not p95. Operators
    /// monitoring `p85_ms` see regime drift ~30–60 min before the
    /// same shift becomes visible at p50.
    pub p85_ms: Option<f64>,
    pub p95_ms: f64,
    pub p99_ms: f64,
    /// Cap-rate-anchored 0.999-quantile. `Some` when the calibrator
    /// observed a non-zero cap-hit rate that's too small for the
    /// primary cap-rate-driven branch (≥ 0.5 %) but large enough to
    /// constrain the tail past p99: solve `P(RTT > 500ms) = cap_rate`
    /// backward to position the p99.9 anchor just past 500 ms instead
    /// of letting the lognormal extrapolation produce a runaway tail.
    /// `None` when the calibrator has no cap-hit signal — the
    /// sampler falls back to the standard lognormal p99.9 extrap.
    pub p999_ms_override: Option<f64>,
    /// Number of `[latency]` summary rows that fed this side.
    pub n_rows: usize,
    /// Total HTTP samples for this side (sum of `n=` field across rows).
    pub n_samples: u64,
    /// Number of cap-hit events for this side
    /// (`→ NewOrderTimeout` for place, `→ CancelOrderTimeout` for
    /// cancel; pooled = both).
    pub n_timeouts: u64,
    /// `n_timeouts / n_samples` — observed fraction that hit the
    /// 500 ms client-timeout cap.
    pub cap_hit_rate: f64,
    /// Tag describing how (p95, p99) were derived:
    ///   * `"cap-rate (p95 solved)"` — high cap rate (≥ 5 %), 500 ms
    ///     anchor falls in the [p50, p95] segment so p95 is solved
    ///     against the rate; p99 follows via lognormal extrapolation.
    ///   * `"cap-rate (p99 solved)"` — moderate cap rate (~ 1–5 %),
    ///     500 ms anchor falls in the [p95, p99] segment so p99 is
    ///     solved; p95 stays at the median.
    ///   * `"medians (cap rate < 1 %)"` / `"medians (no timeouts)"`
    ///     — small cap rate; fall back to median-of-percentiles with
    ///     censorship-aware lognormal p99 extrapolation.
    pub calibration_method: &'static str,
    /// Lag-1 autocorrelation of `log(RTT)` over per-event paired
    /// samples (`Submit↔Order accepted` for place,
    /// `Cancel request↔Cancel result` for cancel). This is the
    /// direct empirical analog of the AR(1) `rho` parameter in
    /// `LatencyProfile::Empirical`. `None` when fewer than 100
    /// paired samples were found (estimate too noisy). Only set on
    /// the pooled `SidedParams` returned by the calibrator; per-hour
    /// `SidedParams` keep this `None` (operators can fall back to
    /// the pooled per-side ρ).
    pub rho_lag1: Option<f64>,
    /// Peaks-over-threshold GPD tail fit, when raw RTT samples were
    /// dense enough to support it (≥ 100 exceedances total above the
    /// chosen threshold, including censored timeouts). Models the
    /// conditional excess `X − u | X > u` as GPD(σ, ξ); the sampler
    /// uses this in `empirical_anchors` to derive the p99 / p99.9 /
    /// p99.99 anchor positions instead of the legacy lognormal
    /// extrapolation. `None` for per-hour buckets (only the pooled
    /// per-side fit is performed; hourly bodies share the pooled
    /// GPD tail — stationary-tail assumption).
    pub gpd_tail: Option<GpdTail>,
}

/// Generalised-Pareto tail fit from peaks-over-threshold MLE. See
/// `sim/gpd.rs` for the underlying density and the censored MLE
/// implementation. The sampler uses `(threshold_ms, sigma, xi,
/// exceedance_rate)` to position the upper-quantile anchors of the
/// empirical CDF table.
#[derive(Debug, Clone, Copy)]
pub struct GpdTail {
    /// Threshold `u` in ms — the body / tail split point. All
    /// exceedances are `X − u` for `X > u`.
    pub threshold_ms: f64,
    /// GPD scale parameter σ (> 0).
    pub sigma: f64,
    /// GPD shape parameter ξ. ξ > 0 → polynomial right tail (heavy);
    /// ξ = 0 → exponential; ξ < 0 → finite right endpoint. Clamped
    /// at fit time to [-0.45, 0.95]; estimates pegged to either
    /// boundary mean the MLE failed and the caller falls back to
    /// the lognormal extrap path.
    pub xi: f64,
    /// `P(X > u)` — exceedance rate including both uncensored and
    /// censored samples. Used in the sampler to position the
    /// threshold anchor at `(1 − exceedance_rate, u)` on the CDF.
    pub exceedance_rate: f64,
    /// Count of uncensored exceedances that fed the MLE
    /// (`y_i = X_i − u` for `u < X_i < cap`).
    pub n_exceedances: usize,
    /// Count of right-censored exceedances (timeouts at the cap).
    /// `n_exceedances + n_censored` is the total tail-sample count;
    /// `exceedance_rate ≈ (n_exceedances + n_censored) / n_total`.
    pub n_censored: usize,
}

/// Result of parsing a live.log session for latency calibration.
///
/// Place and cancel are calibrated **independently** — they ride the
/// same HTTP path but place is reliably slower (server work for new
/// orders is heavier than for cancels). Pooled is kept for the
/// single-knob fallback mode and for the diagnostic example tool.
#[derive(Debug, Clone)]
pub struct CalibratedParams {
    pub place: SidedParams,
    pub cancel: SidedParams,
    /// Place + cancel pooled (legacy single-knob mode).
    pub pooled: SidedParams,
    /// Taker-fill latency stats (HTTP `Matched immediately` path),
    /// derived by pairing per-coid `Submit ...` and `Order accepted:
    /// status=matched coid=...` log lines and computing the elapsed
    /// time. `None` when too few pairs were found (< 50). When
    /// `Some`, the engine prefers these over the manual
    /// `sim_taker_latency_p*_ms` knobs.
    ///
    /// Matched fills go through Polymarket's matching engine which
    /// adds ~270 ms over a maker ACK; the distribution is much
    /// tighter (server-side matching is deterministic work) and
    /// rarely hits the 500 ms client-timeout cap, so we use raw
    /// per-event RTT samples instead of the per-minute aggregation
    /// path used for place / cancel.
    pub taker: Option<TakerLatencyStats>,
    /// Synthetic server-side fail rate, derived from live's `Order
    /// failed: status …` lines (5xx / 429 / "invalid" / "matched")
    /// divided by total placement attempts (`Order accepted` +
    /// `Matched immediately` + `Order failed`). The BT engine wires
    /// this into `SimExchange::order_fail_prob` so the strategy
    /// exercises the same retry / reconcile paths it does in live.
    pub order_fail_prob: f64,
    /// Numerator of `order_fail_prob` — `[PolymarketTrade] Order failed`
    /// matches across the parsed log.
    pub n_order_failed: u64,
    /// Denominator of `order_fail_prob` — total placement attempts
    /// observed (`Order accepted` + `Matched immediately` +
    /// `Order failed`). 0 if no placements parsed.
    pub n_placement_attempts: u64,
    /// Per-UTC-hour calibration of the place side, or `None` for
    /// hours with insufficient samples (< `HOURLY_MIN_SAMPLES`).
    /// Engine wires these into a `LatencyProfile::HourlyEmpirical`
    /// when ≥ 2 hours are populated; otherwise it falls back to the
    /// pooled `place` anchors so the BT keeps a single distribution.
    pub place_hourly: Box<[Option<SidedParams>; 24]>,
    /// Per-UTC-hour calibration of the cancel side. Same semantics
    /// as `place_hourly`.
    pub cancel_hourly: Box<[Option<SidedParams>; 24]>,
    /// Pearson correlation of `log(place_p99)` vs `log(cancel_p99)`
    /// across paired per-minute rows. `None` when fewer than 30
    /// minute-pairs were found (correlation estimate too noisy).
    /// Operators use this to choose `sim_latency_cross_correlation`:
    /// the BT's coupled sampler reproduces this regime-level co-
    /// movement when its per-tick `rho_cross` is set to roughly
    /// this value (the mapping is 1:1 in steady state because both
    /// sides share the same AR(1) `rho`).
    pub cross_corr_log_p99: Option<f64>,
    /// Number of paired minute rows that fed `cross_corr_log_p99`.
    pub n_cross_pairs: usize,
    /// Fraction of placement-reconcile lookups that returned the
    /// eventually-consistent "not_found" response on real Polymarket.
    /// Computed as
    ///     n_reconcile_not_found / (n_reconcile_not_found + n_reconciled)
    /// across all logged reconciliation events. Engine wires this
    /// into `SimExchange::reconcile_not_found_prob` so BT exercises
    /// the strategy's 5-attempt retry path the same way live does.
    pub reconcile_not_found_prob: f64,
    /// Numerator: `[PolymarketTrade] Reconcile: placement … not found
    /// on server (attempt …)` lines. Each retry attempt counts.
    pub n_reconcile_not_found: u64,
    /// Denominator partner: `[PolymarketTrade] Reconciled placement
    /// coid=… → …` lines (terminal Filled / Cancelled / Rejected
    /// emissions, excluding the not-found-retry warnings).
    pub n_reconciled: u64,
    /// Per-attempt extra-timeout probability needed to bring the
    /// latency-driven cap-rate up to the observed live rate.
    /// Computed as
    ///     extra = max(0, observed_timeout_rate − cap_hit_rate)
    /// summed across place + cancel sides (rough per-attempt budget).
    /// Engine wires this into the latency timeout decision so BT's
    /// total NewOrderTimeout / CancelOrderTimeout volume matches live.
    pub extra_timeout_prob: f64,
    /// HTTP client request timeout inferred from the input log's
    /// `[latency] … max=` field — the value the live process was
    /// running with at the time. Engine propagates this into
    /// `SimExchangeConfig.client_timeout_ms` (when the TOML knob is
    /// at its default) so the sim's cap aligns with what the
    /// calibrated anchors were derived from. Defaults to 500 ms
    /// when no `max=` rows are observed (very old log format).
    ///
    /// Why this matters: every place/cancel HTTP call is bounded by
    /// this timeout in live. The calibrator's cap-rate-driven anchor
    /// solver and the GPD censored MLE both treat samples past the
    /// cap as right-censored. If the engine then runs the sim with
    /// a stale 500 ms cap while anchors come from a 2 s-cap session,
    /// every place RTT > 500 ms gets converted to NewOrderTimeout —
    /// which is what was making backtest's `rtt_gate.last_event_p85`
    /// stick around 191 ms even though the calibrated p95 was 1760 ms.
    pub inferred_client_timeout_ms: f64,

    // ── NY-Saturday split (2026-05-19) ──────────────────────────────
    //
    // Polymarket RTT distribution differs materially between weekend
    // and weekday sessions: lower traffic on NY-Saturday → faster body,
    // less HTTP queue contention → thinner tail. Splitting calibration
    // along this axis lets BT model Sat-only events under a more
    // realistic latency regime instead of being dragged by the
    // weekday-dominated pooled stats.
    //
    // Classification: row's log timestamp converted to America/New_York
    // and its weekday checked == Saturday. Both halves are populated
    // from the SAME parse loop (no double-read) so they're cheap.
    //
    // `None` when the side of the split didn't accumulate enough
    // per-minute rows for a stable solve (< 5 rows). Operator can
    // diff p50/p85/p95 against the pooled values to decide whether
    // a Saturday-aware sampler dispatch would be worth wiring.

    /// Place-side RTT calibrated on NY-Saturday rows only.
    pub place_saturday: Option<SidedParams>,
    /// Place-side RTT calibrated on NY non-Saturday rows.
    pub place_non_saturday: Option<SidedParams>,
    /// Cancel-side RTT calibrated on NY-Saturday rows only.
    pub cancel_saturday: Option<SidedParams>,
    /// Cancel-side RTT calibrated on NY non-Saturday rows.
    pub cancel_non_saturday: Option<SidedParams>,
}

/// Minimum HTTP samples a single hour bucket must accumulate before
/// we trust its (p50, p95, p99) percentiles enough to use them in
/// place of the pooled session-wide values. 500 is conservative — it
/// excludes hours that are genuinely sparse (e.g. sub-2-min slices at
/// the head/tail of a partial-hour log) without being so tight that
/// only multi-hour sessions qualify.
pub const HOURLY_MIN_SAMPLES: u64 = 500;

/// Minimum number of populated hours before the engine builds an
/// `HourlyEmpirical` profile. Below this, per-hour estimates are too
/// sparse to dominate over the pooled fit so we keep the existing
/// single-distribution behaviour.
pub const HOURLY_MIN_HOURS: usize = 2;

/// Taker-fill latency stats parsed from raw per-event RTT samples.
/// Differs from `SidedParams`: no per-minute aggregation, no cap-hit
/// math (taker fills almost never hit the cap), no lognormal
/// extrapolation — just direct percentiles from the sorted sample
/// vector.
#[derive(Debug, Clone, Copy)]
pub struct TakerLatencyStats {
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    /// Number of paired (Submit, Order accepted: status=matched) events.
    pub n_samples: usize,
}

/// Parse a hexbot live.log and derive `(p50, p95, p99)` for the
/// `LatencyProfile::Empirical` constructor.
///
/// ## Algorithm
///
/// 1. Walk the log; for each `[latency] polymarket.http.{place,
///    cancel}_order` row, extract `p50`/`p95`/`p99` and the `n=`
///    sample count. Pool place + cancel — they ride the same
///    network path.
/// 2. Count cap-hit events: each `[PolymarketTrade] … →
///    NewOrderTimeout` and `… → CancelOrderTimeout` log line is one
///    HTTP request that exceeded the 500 ms client deadline.
/// 3. Compute `cap_hit_rate = n_timeouts / n_samples` — the
///    empirical fraction of requests that timed out.
/// 4. Anchor calibration:
///    * `p50` is always the median of within-minute p50s.
///    * If `cap_hit_rate ≥ 0.005`: solve the empirical CDF anchors
///      so `P(RTT > 500) = cap_hit_rate`. Two cases by which
///      piecewise segment the 500 ms anchor falls in:
///        - `target_F500 = 1 − cap_hit_rate ≤ 0.95`  →  500 ms is
///          below p95 (high cap rate). Solve p95 from
///          `0.5 + (500 − p50)/(p95 − p50) · 0.45 = target_F500`
///          → `p95 = p50 + (500 − p50) · 0.45 / (target_F500 − 0.5)`.
///          Then `p99 = p50 · exp(σ̂ · Φ⁻¹(0.99))`,
///          `σ̂ = ln(p95/p50)/Φ⁻¹(0.95)` (lognormal-style tail
///          extrapolation from the body-shape implied by the new
///          p95).
///        - `target_F500 ∈ (0.95, 0.99]`  →  500 ms is between p95
///          and p99 (moderate cap rate). Keep p95 as median; solve
///          p99 from `0.95 + (500 − p95)/(p99 − p95) · 0.04 =
///          target_F500`.
///    * If `cap_hit_rate < 0.005`: use median-of-percentiles with
///      censorship-aware p99 extrapolation (raw median p99 ≥ 480 ms
///      is treated as cap-clipped and replaced by the lognormal
///      p99 from (p50, p95)).
///
/// ## Why cap-rate-driven, not just median-of-p95
///
/// The median of within-minute p95s describes the *typical*
/// minute's tail. Real live traffic mixes fast minutes (p95 ≈ 200
/// ms) and slow minutes (p95 capped at 500 ms). The pooled overall
/// p95 is somewhere in between — and the *cap-hit rate* across all
/// pooled samples is the ground truth we actually want the BT to
/// reproduce. Solving the anchors against the cap rate guarantees
/// `P(RTT > 500ms)` matches live regardless of regime mixing.
pub fn calibrate_from_log(path: &str) -> std::io::Result<CalibratedParams> {
    calibrate_from_logs(std::slice::from_ref(&path.to_string()))
}


/// Multi-file variant: parses several live*.log files into ONE merged
/// CalibratedParams. Accumulators (per-minute percentile vectors, raw
/// RTT pairs, counter tallies) span all files in sequence — the merged
/// output represents the union sample. Useful when one session is too
/// short to cover all 24 UTC hours, or to combine paper+live runs.
///
/// File order is preserved for the AR(1) lag-1 ρ estimate; if you mix
/// non-contiguous logs the lag-1 across file boundaries is meaningless,
/// but the rest (percentiles, counters, GPD tail fit) merges cleanly.
pub fn calibrate_from_logs(paths: &[String]) -> std::io::Result<CalibratedParams> {
    use std::collections::HashMap;

    if paths.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "calibrate_from_logs: empty paths list",
        ));
    }

    // Per-side accumulators: p50/p95/p99 vectors + sample-count totals.
    let mut place = SidedAcc::default();
    let mut cancel = SidedAcc::default();
    // NY-Saturday split accumulators. Same per-minute aggregation
    // logic as `place` / `cancel`, but only fed rows whose log
    // timestamp falls on a NY-Saturday (or non-Saturday) respectively.
    // The pooled `place` / `cancel` above keep ALL rows so the
    // non-split outputs are unchanged.
    let mut place_sat = SidedAcc::default();
    let mut place_non_sat = SidedAcc::default();
    let mut cancel_sat = SidedAcc::default();
    let mut cancel_non_sat = SidedAcc::default();
    // Per-minute (place_p99, cancel_p99) — used at the end to estimate
    // Pearson corr(log(place_p99), log(cancel_p99)) across minute
    // windows. That correlation is a regime-level signal of how much
    // the two sides co-move; operators tune
    // `sim_latency_cross_correlation` against this.
    let mut place_minute_p99: HashMap<String, f64> = HashMap::new();
    let mut cancel_minute_p99: HashMap<String, f64> = HashMap::new();
    // Order-fail rate accumulator: counts the four log lines we'll
    // turn into `n_placement_attempts` and `n_order_failed`.
    let mut n_order_accepted: u64 = 0;
    let mut n_matched_immediately: u64 = 0;
    let mut n_order_failed: u64 = 0;
    // Reconciliation accumulator: live emits one of two terminal
    // shapes per cycle:
    //   * `Reconcile: placement … not found on server (attempt N/M)
    //     — keeping orphan, retrying`         ← retry path (no update emitted)
    //   * `Reconciled placement coid=… orderID=… → {Filled|…}` ← terminal
    // The first counts EVERY retry (so 1 ghost order with 5 attempts =
    // 5 not_found lines). The probability of any one reconcile-orphan
    // call returning not_found is therefore
    //     n_not_found / (n_not_found + n_reconciled)
    // which is the per-call probability the SimExchange should inject.
    let mut n_reconcile_not_found: u64 = 0;
    let mut n_reconciled: u64 = 0;

    // Per-event RTT bookkeeping. Two independent pairing tables:
    //   * `submit_ts`: place leg, populated by `[PolymarketTrade] Submit
    //     … coid=…`, drained by `Order accepted … coid=…` (matched OR
    //     status=live).
    //   * `cancel_submit_ts`: cancel leg, populated by `[PolymarketTrade]
    //     Cancel request … coid=…`, drained by `Cancel result … coid=…`.
    // Bounded — drop entries we've never paired after MAX_PENDING
    // accumulates so an old session log doesn't grow unbounded if a
    // Submit was never followed by a matching reply (rare but possible
    // during a recorder-truncated tail).
    let mut submit_ts: HashMap<String, u64> = HashMap::new();
    let mut cancel_submit_ts: HashMap<String, u64> = HashMap::new();
    // Taker subset of place RTTs (`status=matched` only) — kept
    // separately for the existing taker-percentile pipeline.
    let mut taker_rtts_ms: Vec<f64> = Vec::with_capacity(2048);
    // ALL place RTTs (matched + live) and all cancel RTTs, in log
    // order. Used to estimate per-side AR(1) `rho` via the lag-1
    // autocorrelation of `log(RTT)`. The calibrator surfaces this on
    // `SidedParams.rho_lag1`; engine wires it into the sampler when
    // present, falling back to the manual TOML knob otherwise.
    let mut place_rtts_ms: Vec<f64> = Vec::with_capacity(8192);
    let mut cancel_rtts_ms: Vec<f64> = Vec::with_capacity(8192);
    // Parallel UTC-hour tags for each raw RTT sample. Used by the
    // per-hour pooled-raw solver — without an hour tag, only the
    // pooled (all-hour) raw RTTs could feed the new solver, and
    // per-hour anchors fell back to the legacy median-of-per-minute
    // path. The hour is derived from the `Order accepted` reply
    // timestamp (close enough to the request timestamp — RTT is
    // bounded at 5 s ≪ 1 hour, so wrap-arounds are vanishingly
    // rare and would only mistag the last few samples of a clock
    // hour into the next bucket).
    let mut place_rtts_hour: Vec<u8> = Vec::with_capacity(8192);
    let mut cancel_rtts_hour: Vec<u8> = Vec::with_capacity(8192);
    const MAX_PENDING: usize = 50_000;

    // Outer per-file loop: walk each log file's lines through the
    // existing single-file parser body. Accumulators (place / cancel /
    // submit_ts / cancel_submit_ts / *_rtts_ms / counters) are shared
    // across files so the merged calibration reflects the union.
    for path in paths {
        log::debug!("[calibrate] reading {}", path);
    for line in super::calib_source::calib_lines(path)? {
        let line = match line { Ok(l) => l, Err(_) => continue };

        // (a) Per-minute latency summaries — provide percentile
        // anchors and the sample-count denominator. Place vs cancel
        // are routed to separate accumulators so each side's body
        // and tail can be calibrated independently (place is
        // reliably slower than cancel: in 2026-04-28 live2.log the
        // place-vs-cancel p99 ratio was ≈ 243 / 140 = 1.7×).
        if line.contains("[latency]") {
            let acc_opt: Option<&mut SidedAcc> =
                if line.contains("polymarket.http.place_order") {
                    Some(&mut place)
                } else if line.contains("polymarket.http.cancel_order") {
                    Some(&mut cancel)
                } else {
                    None
                };
            // Identify which side this row is, and remember it for the
            // per-minute pairing below. We can't use `acc_opt` for this
            // because the borrow on `place` / `cancel` is exclusive
            // through the `Some(acc)` arm, and the cross-correlation
            // pairing reads BOTH sides' minute keys.
            let is_place = line.contains("polymarket.http.place_order");
            let is_cancel = line.contains("polymarket.http.cancel_order");
            if let Some(acc) = acc_opt {
                let p50 = parse_ms_after(&line, "p50=");
                // `p85=` is emitted by live ≥ 2026-05-15; older builds
                // don't have it. The whole row still feeds the
                // calibrator if p50/p95/p99 are present — p85 just
                // becomes Optional on `SidedParams`.
                let p85 = parse_ms_after(&line, "p85=");
                let p95 = parse_ms_after(&line, "p95=");
                let p99 = parse_ms_after(&line, "p99=");
                let max_v = parse_ms_after(&line, "max=");
                let n = parse_n_after(&line, "n=").unwrap_or(0);
                let hour = parse_log_hour(&line);
                if let (Some(a), Some(b), Some(c)) = (p50, p95, p99) {
                    acc.pooled.p50s.push(a);
                    acc.pooled.p95s.push(b);
                    acc.pooled.p99s.push(c);
                    if let Some(v) = p85 { acc.pooled.p85s.push(v); }
                    if let Some(v) = max_v { acc.pooled.max_ms_observed.push(v); }
                    if let Some(h) = hour {
                        let bucket = &mut acc.hourly[h as usize];
                        bucket.p50s.push(a);
                        bucket.p95s.push(b);
                        bucket.p99s.push(c);
                        if let Some(v) = p85 { bucket.p85s.push(v); }
                        if let Some(v) = max_v { bucket.max_ms_observed.push(v); }
                        bucket.n_samples = bucket.n_samples.saturating_add(n);
                    }
                    // NY-Saturday split: in addition to the pooled+hourly
                    // push above, route this row's per-minute summary to
                    // the Saturday OR non-Saturday accumulator (pooled
                    // bucket only — hourly is intentionally not split to
                    // keep memory bounded; Sat-vs-rest is a coarser axis
                    // than per-UTC-hour). Rows with unparseable timestamps
                    // are skipped from the split (the pooled buckets still
                    // include them, so totals stay consistent).
                    if let Some(is_sat) = is_ny_saturday(&line) {
                        let split_acc = match (is_place, is_sat) {
                            (true,  true)  => Some(&mut place_sat),
                            (true,  false) => Some(&mut place_non_sat),
                            (false, true)  if is_cancel => Some(&mut cancel_sat),
                            (false, false) if is_cancel => Some(&mut cancel_non_sat),
                            _ => None,
                        };
                        if let Some(sacc) = split_acc {
                            sacc.pooled.p50s.push(a);
                            sacc.pooled.p95s.push(b);
                            sacc.pooled.p99s.push(c);
                            if let Some(v) = p85 { sacc.pooled.p85s.push(v); }
                            if let Some(v) = max_v { sacc.pooled.max_ms_observed.push(v); }
                            sacc.pooled.n_samples = sacc.pooled.n_samples.saturating_add(n);
                        }
                    }
                }
                acc.pooled.n_samples = acc.pooled.n_samples.saturating_add(n);

                // Remember p99 keyed by "YYYY-MM-DDTHH:MM" for the
                // minute-level cross-correlation. We index by the first
                // 16 bytes of the ISO timestamp prefix; live emits
                // place and cancel rows on the same minute boundary
                // (the per-minute latency dump task is unified) so
                // they pair up cleanly.
                if let Some(p99v) = p99 {
                    if line.len() >= 16 {
                        let key = line[..16].to_string();
                        if is_place {
                            place_minute_p99.insert(key, p99v);
                        } else if is_cancel {
                            cancel_minute_p99.insert(key, p99v);
                        }
                    }
                }
                continue;
            }
        }

        // (b) Cap-hit events + placement outcomes — both come from
        // [PolymarketTrade]. We route by message stem:
        //   * `→ NewOrderTimeout`     → place cap-hit
        //   * `→ CancelOrderTimeout`  → cancel cap-hit
        //   * `Order accepted`        → successful resting placement
        //   * `Matched immediately`   → successful taker placement
        //   * `Order failed`          → server-side failure (5xx / 429
        //                               / invalid / matched) — the
        //                               numerator for `order_fail_prob`.
        //
        // We also track Submit→Matched pairs for taker latency. Each
        // `[PolymarketTrade] Submit ...` records the wall-clock ts
        // and coid; the corresponding `Order accepted: status=matched
        // coid=...` is the taker reply (HTTP "Matched immediately"
        // path), and the elapsed time is one taker-RTT sample.
        if line.contains("[PolymarketTrade]") {
            if line.contains("→ NewOrderTimeout") {
                place.pooled.n_timeouts = place.pooled.n_timeouts.saturating_add(1);
                if let Some(h) = parse_log_hour(&line) {
                    place.hourly[h as usize].n_timeouts =
                        place.hourly[h as usize].n_timeouts.saturating_add(1);
                }
            } else if line.contains("→ CancelOrderTimeout") {
                cancel.pooled.n_timeouts = cancel.pooled.n_timeouts.saturating_add(1);
                if let Some(h) = parse_log_hour(&line) {
                    cancel.hourly[h as usize].n_timeouts =
                        cancel.hourly[h as usize].n_timeouts.saturating_add(1);
                }
            } else if line.contains("Submit ") && line.contains(" coid=") {
                if let (Some(ts_ns), Some(coid)) = (parse_log_ts_ns(&line), parse_coid_after(&line)) {
                    if submit_ts.len() < MAX_PENDING {
                        submit_ts.insert(coid, ts_ns);
                    }
                }
            } else if line.contains("Order accepted") {
                n_order_accepted = n_order_accepted.saturating_add(1);
                // Pair Submit↔Order accepted regardless of status.
                // The full place-RTT vector feeds the lag-1
                // autocorrelation estimate; the taker subset
                // (`status=matched`) additionally feeds the existing
                // taker-percentile pipeline.
                if let (Some(reply_ts_ns), Some(coid)) =
                    (parse_log_ts_ns(&line), parse_coid_after(&line))
                {
                    if let Some(submit_ns) = submit_ts.remove(&coid) {
                        if reply_ts_ns > submit_ns {
                            let rtt_ms = (reply_ts_ns - submit_ns) as f64 / 1_000_000.0;
                            // Cap at 5 s — anything beyond is almost
                            // certainly not a real RTT (e.g. log time-
                            // travel from clock skew).
                            if rtt_ms < 5_000.0 {
                                place_rtts_ms.push(rtt_ms);
                                let h = parse_log_hour(&line).unwrap_or(0);
                                place_rtts_hour.push(h);
                                if line.contains("status=matched") {
                                    taker_rtts_ms.push(rtt_ms);
                                }
                            }
                        }
                    }
                }
            } else if line.contains("Cancel request") && line.contains(" coid=") {
                // Cancel L1 leg — record submission ts keyed by coid.
                if let (Some(ts_ns), Some(coid)) = (parse_log_ts_ns(&line), parse_coid_after(&line)) {
                    if cancel_submit_ts.len() < MAX_PENDING {
                        cancel_submit_ts.insert(coid, ts_ns);
                    }
                }
            } else if line.contains("Cancel result") && line.contains(" coid=") {
                // Cancel L2 leg — pair with the matching Cancel
                // request and emit one cancel-side RTT sample.
                if let (Some(reply_ts_ns), Some(coid)) =
                    (parse_log_ts_ns(&line), parse_coid_after(&line))
                {
                    if let Some(submit_ns) = cancel_submit_ts.remove(&coid) {
                        if reply_ts_ns > submit_ns {
                            let rtt_ms = (reply_ts_ns - submit_ns) as f64 / 1_000_000.0;
                            if rtt_ms < 5_000.0 {
                                cancel_rtts_ms.push(rtt_ms);
                                let h = parse_log_hour(&line).unwrap_or(0);
                                cancel_rtts_hour.push(h);
                            }
                        }
                    }
                }
            } else if line.contains("Matched immediately") {
                n_matched_immediately = n_matched_immediately.saturating_add(1);
            } else if line.contains("Order failed") || line.contains("Order rejected") {
                n_order_failed = n_order_failed.saturating_add(1);
                // Drop the matching pending Submit.
                if let Some(coid) = parse_coid_after(&line) {
                    submit_ts.remove(&coid);
                }
            } else if line.contains("Reconcile: placement")
                && line.contains("not found on server")
                && line.contains("(attempt ")
            {
                // Per-retry not_found warning. Each attempt logs
                // separately ("attempt 1/5", "attempt 2/5", …) — we
                // count every one so the per-call probability is
                // calibrated against the strategy's actual call rate.
                n_reconcile_not_found = n_reconcile_not_found.saturating_add(1);
            } else if line.contains("Reconciled placement coid=") {
                // Terminal reconciliation — the "yes-it's-real" branch
                // (Filled / Cancelled / etc.) that lets the orphan
                // resolve. Excludes the "after N attempts → Rejected"
                // line because that's a derived outcome of the
                // not_found retry chain, not an independent successful
                // reconcile.
                n_reconciled = n_reconciled.saturating_add(1);
            }
        }
    }
    } // close outer per-file loop

    if place.pooled.p50s.is_empty() && cancel.pooled.p50s.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "no latency rows matched in log — looked for \
             `[latency] polymarket.http.{place,cancel}_order …`",
        ));
    }

    // Pool place + cancel for the legacy single-knob path.
    let mut pooled = SidedAcc::default();
    pooled.pooled.p50s.extend(&place.pooled.p50s);
    pooled.pooled.p50s.extend(&cancel.pooled.p50s);
    pooled.pooled.p85s.extend(&place.pooled.p85s);
    pooled.pooled.p85s.extend(&cancel.pooled.p85s);
    pooled.pooled.p95s.extend(&place.pooled.p95s);
    pooled.pooled.p95s.extend(&cancel.pooled.p95s);
    pooled.pooled.p99s.extend(&place.pooled.p99s);
    pooled.pooled.p99s.extend(&cancel.pooled.p99s);
    pooled.pooled.n_samples = place.pooled.n_samples.saturating_add(cancel.pooled.n_samples);
    pooled.pooled.n_timeouts = place.pooled.n_timeouts.saturating_add(cancel.pooled.n_timeouts);

    let n_placement_attempts = n_order_accepted
        .saturating_add(n_matched_immediately)
        .saturating_add(n_order_failed);
    let order_fail_prob = if n_placement_attempts > 0 {
        n_order_failed as f64 / n_placement_attempts as f64
    } else {
        0.0
    };

    // Build taker stats from raw RTTs. Require ≥ 50 samples so the
    // p99 isn't a single-sample outlier; below that, fall through to
    // the manual `sim_taker_latency_p*_ms` knobs.
    let taker = if taker_rtts_ms.len() >= 50 {
        let mut sorted = taker_rtts_ms;
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = sorted.len();
        let pct = |q: f64| sorted[((n as f64 * q) as usize).min(n - 1)];
        Some(TakerLatencyStats {
            p50_ms: pct(0.50),
            p95_ms: pct(0.95),
            p99_ms: pct(0.99),
            n_samples: n,
        })
    } else {
        None
    };

    // Infer the live HTTP client timeout from the `max=` field
    // across all `[latency]` rows we parsed. Place and cancel share
    // the same HTTP client config in live, so we take the max across
    // both sides. Falls back to `DEFAULT_CLIENT_TIMEOUT_MS` (500 ms)
    // when the input log is old enough to predate `max=` emission.
    let inferred_cap_ms = infer_cap_ms(&[&place.pooled, &cancel.pooled]);

    // Per-hour anchors: prefer pooled-raw per hour when the hour
    // has ≥ POOLED_RAW_HOURLY_MIN_SAMPLES raw RTT samples; sparse
    // hours route through the legacy median path. See
    // `solve_hourly_with_raw` for the per-hour fallback logic.
    let place_hourly = solve_hourly_with_raw(&place, &place_rtts_ms, &place_rtts_hour, inferred_cap_ms);
    let cancel_hourly = solve_hourly_with_raw(&cancel, &cancel_rtts_ms, &cancel_rtts_hour, inferred_cap_ms);

    // Per-side AR(1) ρ from lag-1 autocorrelation of log(RTT). In
    // 2026-04-30 live2.log this works out to ρ_place ≈ 0.625,
    // ρ_cancel ≈ 0.674 — meaningfully below the 0.85 default that
    // earlier live sessions implied. Engine consumes this on
    // `SidedParams.rho_lag1` (when Some) in preference to the
    // global `sim_latency_correlation` knob.
    let place_rho_lag1 = lag1_autocorr_log_rtt(&place_rtts_ms);
    let cancel_rho_lag1 = lag1_autocorr_log_rtt(&cancel_rtts_ms);

    // Pair up per-minute (place_p99, cancel_p99). Need at least 30
    // joint observations for the Pearson correlation to be stable.
    //
    // Iterate the minute keys in SORTED (chronological — ISO
    // "YYYY-MM-DDTHH:MM" strings sort lexicographically = by time) order
    // rather than `place_minute_p99`'s native HashMap order. A std HashMap
    // seeds a per-process random `RandomState`, so its iteration order — and
    // hence the accumulation order of the `cov`/`var_p`/`var_c` sums below —
    // varies run-to-run; floating-point non-associativity then perturbs
    // `cross_corr_log_p99` at the ~1e-15 ULP level on otherwise identical
    // inputs. That value feeds the sim's `rho_cross`, so the noise leaks
    // into backtest output. Sorting pins the summation order → the
    // calibration (and any backtest reading it, whether from raw logs or a
    // parquet archive) is fully reproducible.
    let mut cross_keys: Vec<&str> =
        place_minute_p99.keys().map(|k| k.as_str()).collect();
    cross_keys.sort_unstable();
    let mut paired_logs: Vec<(f64, f64)> = Vec::with_capacity(cross_keys.len());
    for key in cross_keys {
        if let (Some(p), Some(c)) =
            (place_minute_p99.get(key), cancel_minute_p99.get(key))
        {
            if *p > 0.0 && *c > 0.0 {
                paired_logs.push((p.ln(), c.ln()));
            }
        }
    }
    let n_cross_pairs = paired_logs.len();
    let cross_corr_log_p99 = if n_cross_pairs >= 30 {
        let n = n_cross_pairs as f64;
        let mean_p: f64 = paired_logs.iter().map(|(p, _)| *p).sum::<f64>() / n;
        let mean_c: f64 = paired_logs.iter().map(|(_, c)| *c).sum::<f64>() / n;
        let mut cov = 0.0;
        let mut var_p = 0.0;
        let mut var_c = 0.0;
        for &(p, c) in &paired_logs {
            let dp = p - mean_p;
            let dc = c - mean_c;
            cov += dp * dc;
            var_p += dp * dp;
            var_c += dc * dc;
        }
        if var_p > 0.0 && var_c > 0.0 {
            Some(cov / (var_p.sqrt() * var_c.sqrt()))
        } else {
            None
        }
    } else {
        None
    };

    // Pooled-raw-RTT solver (preferred) vs legacy per-minute-median
    // solver (fallback). The pooled-raw path computes p50/p85/p95/p99
    // directly from the `Submit↔Order accepted` paired RTTs — the
    // same statistic operators observe in live's pooled raw stream.
    // The legacy path aggregates per-minute summary rows by medianing
    // across rows; for heavy-tailed RTT this biases upper-quantile
    // anchors high by 20–30 % (see `solve_anchors_from_raw_rtts` doc).
    //
    // Old log formats that predate per-event Submit/ack pairing don't
    // produce enough raw samples (< `POOLED_RAW_MIN_SAMPLES`); those
    // fall back to the legacy `solve(cap_ms)` path verbatim. The
    // `calibration_method` tag on `SidedParams` indicates which path
    // ran for each side.
    let solve_side = |raw: &[f64], side: &SidedAcc, side_name: &str| -> SidedParams {
        let n_raw = raw.iter().filter(|r| r.is_finite() && **r > 0.0).count();
        if n_raw >= POOLED_RAW_MIN_SAMPLES {
            log::debug!(
                "[Latency] {} side: pooled-raw solver (n_uc={}, n_to={})",
                side_name, n_raw, side.pooled.n_timeouts,
            );
            solve_anchors_from_raw_rtts(
                raw,
                side.pooled.n_timeouts,
                inferred_cap_ms,
                side.pooled.p50s.len(),
                side.pooled.n_samples,
                if side.pooled.n_samples > 100 {
                    side.pooled.n_timeouts as f64 / side.pooled.n_samples as f64
                } else { 0.0 },
            )
        } else {
            log::debug!(
                "[Latency] {} side: legacy per-minute-median solver (raw RTTs n={} < min={})",
                side_name, n_raw, POOLED_RAW_MIN_SAMPLES,
            );
            side.solve(inferred_cap_ms)
        }
    };

    let mut place_params = solve_side(&place_rtts_ms, &place, "place");
    place_params.rho_lag1 = place_rho_lag1;
    let mut cancel_params = solve_side(&cancel_rtts_ms, &cancel, "cancel");
    cancel_params.rho_lag1 = cancel_rho_lag1;

    // POT/GPD tail fits — see `fit_gpd_from_raw_rtts` doc for the
    // threshold rule, exceedance count requirements, and the censored
    // MLE. Failure to converge → `None`, sampler falls back to the
    // legacy lognormal-extrap path. Per-side fits are independent
    // because the place / cancel HTTP paths exhibit measurably
    // different tail shapes (cancel has a longer right tail driven
    // by 'matched-can't-cancel' races on heavily-traded markets).
    place_params.gpd_tail = fit_gpd_from_raw_rtts(&place_rtts_ms, place.pooled.n_timeouts, inferred_cap_ms);
    cancel_params.gpd_tail = fit_gpd_from_raw_rtts(&cancel_rtts_ms, cancel.pooled.n_timeouts, inferred_cap_ms);

    // Per-call not_found probability for the reconciler. See struct
    // doc on `CalibratedParams::reconcile_not_found_prob`.
    let reconcile_not_found_prob = {
        let denom = n_reconcile_not_found + n_reconciled;
        if denom > 0 {
            n_reconcile_not_found as f64 / denom as f64
        } else {
            0.0
        }
    };

    // Per-attempt extra timeout probability — the gap between the
    // observed live timeout rate and what the latency-percentile
    // sampler would naturally produce at `client_timeout = 500ms`.
    //
    // Two different rates are at play and the denominator matters:
    //   observed_timeout_rate (per strategy submit) =
    //       n_timeouts / (n_timeouts + n_placement_attempts)
    //   latency_cap_rate (per HTTP sample) =
    //       n_timeouts / n_samples_from_[latency]_rows
    //
    // The [latency] `n=` field counts every HTTP call (including
    // retries inside a single submit attempt under e.g. HTTP 425
    // backpressure), inflating the denominator. The strategy's
    // count of placement attempts (`Order accepted` + `Matched
    // immediately` + `Order failed`) excludes retries — it's the
    // rate the BT sampler naturally produces.
    //
    // So `observed_timeout_rate - latency_cap_rate` measures the
    // residual that's NOT explained by latency-tail samples ⇒ the
    // amount of "non-latency" timeouts that need synthetic injection.
    let extra_timeout_prob = {
        let n_timeouts_total =
            place_params.n_timeouts + cancel_params.n_timeouts;
        let denom = n_timeouts_total + n_placement_attempts;
        let observed_per_submit = if denom > 0 {
            n_timeouts_total as f64 / denom as f64
        } else { 0.0 };
        // Pooled cap rate — sample-weighted average of the per-side
        // cap rates so the comparison stays apples-to-apples.
        let total_samples =
            place_params.n_samples + cancel_params.n_samples;
        let pooled_cap_rate = if total_samples > 0 {
            (place_params.n_timeouts + cancel_params.n_timeouts) as f64
                / total_samples as f64
        } else { 0.0 };
        (observed_per_submit - pooled_cap_rate).max(0.0)
    };

    // NY-Saturday split: each half solved via the legacy per-minute-
    // median path (`solve_bucket` inside `SidedAcc::solve`). Use the
    // same cap as the pooled solve so the cap-rate-driven anchors
    // align. Threshold of 5 per-minute rows is conservative — below
    // that the percentile medians are too noisy to be meaningful, so
    // we surface `None` instead of misleading numbers.
    //
    // The split is diagnostic-only at this layer: the LatencyProfile /
    // LatencySampler doesn't yet dispatch on Saturday-vs-not. Engine
    // logs the comparison so operators can decide whether Saturday
    // diverges enough to warrant a sampler-side split.
    const SAT_SPLIT_MIN_ROWS: usize = 5;
    let solve_split = |acc: &SidedAcc| -> Option<SidedParams> {
        if acc.pooled.p50s.len() < SAT_SPLIT_MIN_ROWS { return None; }
        Some(acc.solve(inferred_cap_ms))
    };
    let place_saturday     = solve_split(&place_sat);
    let place_non_saturday = solve_split(&place_non_sat);
    let cancel_saturday    = solve_split(&cancel_sat);
    let cancel_non_saturday = solve_split(&cancel_non_sat);

    Ok(CalibratedParams {
        place: place_params,
        cancel: cancel_params,
        pooled: pooled.solve(inferred_cap_ms),
        taker,
        order_fail_prob,
        n_order_failed,
        n_placement_attempts,
        place_hourly,
        cancel_hourly,
        cross_corr_log_p99,
        n_cross_pairs,
        reconcile_not_found_prob,
        n_reconcile_not_found,
        n_reconciled,
        extra_timeout_prob,
        inferred_client_timeout_ms: inferred_cap_ms,
        place_saturday,
        place_non_saturday,
        cancel_saturday,
        cancel_non_saturday,
    })
}

/// Classify a hexbot log line's timestamp as NY-Saturday (true) or
/// NY-non-Saturday (false). Returns `None` for malformed timestamp
/// prefixes; callers route those rows to the pooled bucket only
/// (skipping the Saturday split).
///
/// Uses the full timestamp (not just hour) since `2026-05-19T03:30Z`
/// is `2026-05-18T23:30 America/New_York` — i.e. NY Sun-eve, not Sat.
/// The hour-only `parse_log_hour` can't disambiguate this.
fn is_ny_saturday(line: &str) -> Option<bool> {
    use chrono::{DateTime, Datelike, Utc, Weekday};
    use chrono_tz::America::New_York;
    let ts_ns = parse_log_ts_ns(line)?;
    let utc = DateTime::<Utc>::from_timestamp_nanos(ts_ns as i64);
    let ny = utc.with_timezone(&New_York);
    Some(ny.weekday() == Weekday::Sat)
}

/// Runtime variant: classify a sim-clock `now_ns` (Unix-epoch ns) as
/// "NY-Saturday" or not. Used by `LatencySampler` when the profile is
/// `SaturdayEmpirical` to dispatch between the two anchor tables.
/// Same chrono path as `is_ny_saturday(&str)` above but skips the log-
/// line parse step — feed the engine's `sim_clock_ns()` directly.
#[inline]
pub(crate) fn is_ny_saturday_ns(now_ns: u64) -> bool {
    use chrono::{DateTime, Datelike, Utc, Weekday};
    use chrono_tz::America::New_York;
    let utc = DateTime::<Utc>::from_timestamp_nanos(now_ns as i64);
    let ny = utc.with_timezone(&New_York);
    ny.weekday() == Weekday::Sat
}

/// Extract the UTC hour-of-day (0..23) from a hexbot log line's ISO
/// timestamp prefix (`2026-04-30T07:45:00.123Z  …`). Returns `None`
/// for malformed prefixes; callers fall back to the pooled bucket.
fn parse_log_hour(line: &str) -> Option<u8> {
    if line.len() < 14 { return None; }
    let bytes = line.as_bytes();
    if bytes[10] != b'T' || bytes[13] != b':' { return None; }
    let s = std::str::from_utf8(&bytes[11..13]).ok()?;
    let h: u8 = s.parse().ok()?;
    if h < 24 { Some(h) } else { None }
}

/// Extract a Unix-epoch nanosecond timestamp from a hexbot log line.
/// Format: `2026-04-29T03:25:08.975Z  ...` — first token is ISO 8601
/// in UTC, fixed length 24 chars (millisecond precision). Manual
/// parser to avoid pulling chrono into the latency module.
fn parse_log_ts_ns(line: &str) -> Option<u64> {
    if line.len() < 24 { return None; }
    let bytes = line.as_bytes();
    if bytes[4] != b'-' || bytes[7] != b'-' || bytes[10] != b'T'
        || bytes[13] != b':' || bytes[16] != b':' || bytes[19] != b'.' {
        return None;
    }
    let parse2 = |i: usize| -> Option<u32> {
        let s = std::str::from_utf8(&bytes[i..i+2]).ok()?;
        s.parse().ok()
    };
    let parse4 = |i: usize| -> Option<u32> {
        let s = std::str::from_utf8(&bytes[i..i+4]).ok()?;
        s.parse().ok()
    };
    let parse3 = |i: usize| -> Option<u32> {
        let s = std::str::from_utf8(&bytes[i..i+3]).ok()?;
        s.parse().ok()
    };
    let year  = parse4(0)?;
    let month = parse2(5)?;
    let day   = parse2(8)?;
    let hour  = parse2(11)?;
    let min   = parse2(14)?;
    let sec   = parse2(17)?;
    let ms    = parse3(20)?;

    // Days-since-epoch via civil-from-Gregorian (Howard Hinnant's algorithm).
    let y = if month <= 2 { year as i64 - 1 } else { year as i64 };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = (y - era * 400) as u64;
    let doy = (153 * ((month as u64 + (if month > 2 { 0 } else { 12 })) - 3) + 2) / 5 + day as u64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe as i64 - 719468;

    let secs = days as u64 * 86_400 + hour as u64 * 3_600 + min as u64 * 60 + sec as u64;
    Some(secs * 1_000_000_000 + ms as u64 * 1_000_000)
}

/// Extract the coid token following `coid=` from a log line. Handles both the
/// legacy bare-digit form (`coid=123`) and the live/paper prefixed form
/// (`coid={instance_id}-123`). The token runs until whitespace; it's used only
/// as an opaque key to pair place/ack lines, so the full token (prefix
/// included) is the correct identity. Returns `None` when the marker is absent
/// or the token has no digits.
fn parse_coid_after(line: &str) -> Option<String> {
    let pos = line.find(" coid=")?;
    let rest = &line[pos + 6 ..];
    let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
    let tok = &rest[..end];
    if tok.is_empty() || !tok.bytes().any(|b| b.is_ascii_digit()) { return None; }
    Some(tok.to_string())
}

/// One bucket's worth of per-minute latency rows + sample/timeout
/// counts. Used for both the pooled session-wide accumulator and each
/// per-hour bucket inside `SidedAcc`.
#[derive(Default, Clone)]
struct AccBucket {
    p50s: Vec<f64>,
    /// Per-minute p85 values, only populated for rows emitted by live
    /// builds ≥ 2026-05-15 (older builds didn't dump `p85=`). Length
    /// may differ from `p50s`/`p95s` when mixed-version logs are
    /// concatenated; the solver guards with `.is_empty()`.
    p85s: Vec<f64>,
    p95s: Vec<f64>,
    p99s: Vec<f64>,
    /// Per-minute `max=` values from the live `[latency]` summary.
    /// The HTTP client's request timeout caps every sample at this
    /// value, so the max-of-maxes across rows is a direct read of
    /// the active client timeout (live5.log builds capped at 500 ms;
    /// live14+ builds raised it to 2 s). The calibrator uses this
    /// to set `CalibratedParams::inferred_client_timeout_ms`, which
    /// the engine propagates into `SimExchangeConfig.client_timeout_ms`
    /// and the cap-rate-driven anchor solver — preventing the 500 ms
    /// hardcoded constant from misinterpreting heavier-cap sessions
    /// (p95=1760 ms reads from live14 used to look like cap-saturated
    /// data under the 500 ms assumption, breaking the solver and the
    /// GPD fitter both).
    max_ms_observed: Vec<f64>,
    n_samples: u64,
    n_timeouts: u64,
}

struct SidedAcc {
    pooled: AccBucket,
    /// 24 per-UTC-hour buckets indexed `0..23`. Each row's hour is
    /// derived from its log-line timestamp prefix; cap-hit events are
    /// bucketed the same way so per-hour `cap_hit_rate` is meaningful.
    /// Boxed so `SidedAcc` itself stays small on the stack — the
    /// pooled accumulator is the hot path; per-hour is touched once
    /// per row.
    hourly: Box<[AccBucket; 24]>,
}

impl Default for SidedAcc {
    fn default() -> Self {
        // [_; 24] derive doesn't work for non-Copy; manual default.
        let hourly: [AccBucket; 24] = std::array::from_fn(|_| AccBucket::default());
        Self {
            pooled: AccBucket::default(),
            hourly: Box::new(hourly),
        }
    }
}

impl SidedAcc {
    /// Solve (p95, p99) anchors against the cap-hit rate. `cap_ms` is
    /// the inferred HTTP client timeout (from `max=` field in the
    /// `[latency]` rows, or fallback default). Empty side returns a
    /// zeroed `SidedParams` with method "no rows" — the caller
    /// decides whether to fall back to the manual TOML knob.
    fn solve(&self, cap_ms: f64) -> SidedParams {
        solve_bucket(&self.pooled, cap_ms)
    }

    // Per-UTC-hour solver lives in `solve_hourly_with_raw` (free
    // function) so it can read both the per-minute aggregates from
    // `SidedAcc.hourly[h]` AND the raw RTT vectors (which aren't on
    // `SidedAcc`). It routes each hour to pooled-raw or the legacy
    // median path based on per-hour raw sample density.
}

/// Default cap when no `max=` field is parsed from the input log
/// (very old hexbot builds didn't emit it). Stays at the historical
/// live5.log default so calibrating from an old file behaves the
/// same as before this commit.
const DEFAULT_CLIENT_TIMEOUT_MS: f64 = 500.0;

/// Infer the live HTTP client timeout from observed `max=` values.
/// The client's request timeout caps every sample, so the max of
/// the per-minute `max=` field — rounded up to the next 100 ms grid
/// for stability against per-minute jitter — is a direct read of
/// the cap. Returns `DEFAULT_CLIENT_TIMEOUT_MS` when no `max=` rows
/// were observed (very old log format).
fn infer_cap_ms(buckets: &[&AccBucket]) -> f64 {
    let mut max_seen: f64 = 0.0;
    for b in buckets {
        for &v in &b.max_ms_observed {
            if v.is_finite() && v > max_seen {
                max_seen = v;
            }
        }
    }
    if max_seen <= 0.0 {
        return DEFAULT_CLIENT_TIMEOUT_MS;
    }
    // Round up to the next 100 ms — live's HTTP timeouts in
    // practice land on round values (500, 1000, 2000, 3000 ms),
    // and the raw `max=` can be a hair below the configured timeout
    // due to socket-close timing. Rounding up keeps the inferred
    // cap aligned with what the operator actually set in live.
    (max_seen / 100.0).ceil() * 100.0
}

/// Algorithm shared by `SidedAcc::solve` (pooled) and per-hour
/// solving. Factoring it out keeps the cap-rate-driven anchor logic
/// in one place — see `SidedParams::calibration_method` for the
/// branch labels — so per-hour buckets see the same body / tail
/// treatment as the legacy pooled path.
///
/// `cap_ms` is the HTTP client request timeout for the source log
/// (inferred from `max=` in `[latency]` rows). All cap-rate-driven
/// anchor solving uses this value rather than the legacy hardcoded
/// 500 ms — live builds since 2026-05-14 raised the timeout to
/// 2000 ms, and using the wrong cap collapses the solver to a
/// `1.5 × p95` lower-bound clamp (see comments in cap-rate branches).
fn solve_bucket(b: &AccBucket, cap_ms: f64) -> SidedParams {
    let n_rows = b.p50s.len();
    if n_rows == 0 {
        return SidedParams {
            p50_ms: 0.0, p85_ms: None, p95_ms: 0.0, p99_ms: 0.0,
            p999_ms_override: None,
            n_rows: 0, n_samples: 0, n_timeouts: 0,
            cap_hit_rate: 0.0,
            calibration_method: "no rows",
            rho_lag1: None,
            gpd_tail: None,
        };
    }
    let p50 = median(&mut b.p50s.clone());
    // Median of within-minute p85s — diagnostic only, mirrors how
    // p50/p95/p99 are aggregated. `None` when no row in this bucket
    // carried a `p85=` field (pre-2026-05-15 live build).
    let p85_ms: Option<f64> = if b.p85s.is_empty() {
        None
    } else {
        Some(median(&mut b.p85s.clone()))
    };
    let p95_median = median(&mut b.p95s.clone());
    let p99_median = median(&mut b.p99s.clone());

    let cap_hit_rate = if b.n_samples > 100 {
        b.n_timeouts as f64 / b.n_samples as f64
    } else {
        0.0
    };

    let mut p999_override: Option<f64> = None;

    let (p95, p99, calibration_method) = if cap_hit_rate >= 0.005 {
        let target_f500 = 1.0 - cap_hit_rate;
        if target_f500 <= 0.95 {
            let denom = target_f500 - 0.5;
            let p95_solved = if denom > 0.001 {
                let p = p50 + (cap_ms - p50) * 0.45 / denom;
                p.max(p50 * 1.5).min(5_000.0)
            } else {
                p95_median
            };
            let sigma = (p95_solved / p50).ln() / 1.6449;
            let p99_extrap = p50 * (sigma * 2.3263).exp();
            let p99_solved = p99_extrap.max(p95_solved * 1.5);
            (p95_solved, p99_solved, "cap-rate (p95 solved)")
        } else if target_f500 < 0.99 {
            let denom = target_f500 - 0.95;
            let p99_solved = if denom > 0.001 {
                let p = p95_median + (cap_ms - p95_median) * 0.04 / denom;
                p.max(p95_median * 1.5).min(10_000.0)
            } else {
                p99_median
            };
            (p95_median, p99_solved, "cap-rate (p99 solved)")
        } else {
            let (p99, _) = censored_p99_extrap(p50, p95_median, p99_median);
            (p95_median, p99, "medians (cap rate < 1 %)")
        }
    } else if b.n_timeouts > 0 && p99_median > 0.0 && p99_median < cap_ms {
        let target_f500 = 1.0 - cap_hit_rate;
        let denom = target_f500 - 0.99;
        if denom > 1e-9 {
            let p999 = p99_median + (cap_ms - p99_median) * 0.009 / denom;
            let p999_clamped = p999
                .max(p99_median * 1.05)
                .min(cap_ms * 5.0);
            p999_override = Some(p999_clamped);
        }
        (p95_median, p99_median, "medians (cap-rate p99.9 solved)")
    } else {
        let (p99, _) = censored_p99_extrap(p50, p95_median, p99_median);
        let label: &'static str = if b.n_timeouts == 0 {
            "medians (no timeouts)"
        } else {
            "medians (cap rate < 0.5 %, censored p99)"
        };
        (p95_median, p99, label)
    };

    SidedParams {
        p50_ms: p50, p85_ms, p95_ms: p95, p99_ms: p99,
        p999_ms_override: p999_override,
        n_rows,
        n_samples: b.n_samples,
        n_timeouts: b.n_timeouts,
        cap_hit_rate,
        calibration_method,
        rho_lag1: None,
        gpd_tail: None,
    }
}

/// Minimum raw RTT sample count required before the calibrator
/// trusts the pooled-quantile path over the legacy median-of-per-
/// minute-percentiles aggregation. 1000 ≈ 35 min of place-side
/// activity at typical 30-events/min — below that, sampling noise
/// in the upper quantiles is large enough that per-minute medians
/// (a more robust estimator at small sample counts) are preferable.
const POOLED_RAW_MIN_SAMPLES: usize = 1000;

/// Solve anchors directly from pooled raw RTT samples, treating
/// timeouts as Type-I right-censored observations at the inferred
/// HTTP client timeout (`cap_ms`).
///
/// ## Why this exists
///
/// The legacy `solve_bucket` aggregates the per-minute `[latency]
/// p50/p95/p99` summaries into anchors via "median across all
/// rows of each percentile". For heavy-tailed RTT — where each
/// minute's 95th-percentile sample is a high-variance estimator of
/// the underlying population p95 — the median-of-within-minute
/// statistic systematically OVERESTIMATES the pooled population
/// quantile (each minute's p95 is roughly the 96.7-percentile
/// sample if n_per_minute ≈ 30; medianing these biases up).
///
/// Empirical evidence (live14+15, 2026-05-15):
///   per-minute median:  p95 = 1760 ms,  p99 = 2640 ms
///   pooled raw RTT:     p95 = 1391 ms,  p99 = 1852 ms
///
/// That 25–30 % bias on p95 / p99 propagates through the sampler's
/// anchor table — the sim's paired-RTT p85 was 1261 ms vs live's
/// pooled 750 ms, even after the L1/L2 sampler rewrite.
///
/// ## Algorithm
///
/// 1. Sort uncensored raw RTTs (the `Submit↔Order accepted` pairs).
/// 2. Treat `n_timeouts` as samples known only to satisfy `X ≥ cap_ms`
///    — Type-I right-censored at `cap_ms`. Total population size
///    = `n_uc + n_to`.
/// 3. For quantile `q`:
///    * Target rank = `q · (n_uc + n_to)`.
///    * If target_rank ≤ n_uc, the q-th sample is uncensored:
///      read directly from the sorted vector at that position.
///    * If target_rank > n_uc, the q-th sample is in the censored
///      tail (X ≥ cap_ms). Return cap_ms as a lower bound;
///      `empirical_anchors` + GPD tail handle the extrapolation
///      beyond.
///
/// ## Calibration method tag
///
/// Sets `calibration_method` to "raw RTT pooled (n_uc=X, n_to=Y)".
/// Operators can grep startup logs to confirm which path the
/// calibrator took for any given run.
fn solve_anchors_from_raw_rtts(
    raw_rtts_ms: &[f64],
    n_timeouts: u64,
    cap_ms: f64,
    n_rows: usize,
    n_samples: u64,
    cap_hit_rate: f64,
) -> SidedParams {
    let mut sorted: Vec<f64> = raw_rtts_ms.iter().copied()
        .filter(|r| r.is_finite() && *r > 0.0)
        .collect();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n_uc = sorted.len();
    let n_to = n_timeouts as usize;
    let n_total = n_uc + n_to;
    if n_total == 0 {
        return SidedParams {
            p50_ms: 0.0, p85_ms: None, p95_ms: 0.0, p99_ms: 0.0,
            p999_ms_override: None,
            n_rows: 0, n_samples: 0, n_timeouts: 0,
            cap_hit_rate: 0.0,
            calibration_method: "no raw samples",
            rho_lag1: None, gpd_tail: None,
        };
    }

    let q_at = |q: f64| -> f64 {
        // Rank-of-fraction in pooled (uc + to) population. Linear
        // interpolation between adjacent uncensored ranks; falls
        // back to `cap_ms` when the target rank is inside the
        // censored tail (the upstream `empirical_anchors` /
        // GPD path takes over past the cap).
        let target_rank = q * (n_total as f64 - 1.0);
        if target_rank >= n_uc as f64 {
            // q is in the censored tail.
            return cap_ms;
        }
        let lo = target_rank.floor() as usize;
        let hi = (lo + 1).min(n_uc - 1);
        let frac = target_rank - lo as f64;
        sorted[lo] + frac * (sorted[hi] - sorted[lo])
    };

    let p50 = q_at(0.50);
    let p85 = q_at(0.85);
    let p95 = q_at(0.95);
    let p99_raw = q_at(0.99);

    // If p99 fell into the censored tail (i.e. cap_hit_rate > 1 %),
    // extrapolate from pooled (p50, p95) using the same lognormal
    // slope `censored_p99_extrap` uses for legacy aggregates:
    //   σ̂ = ln(p95 / p50) / Φ⁻¹(0.95) = ln(p95/p50) / 1.6449
    //   p99 = p50 · exp(σ̂ · Φ⁻¹(0.99)) = p50 · exp(σ̂ · 2.3263)
    // This is a body-shape-consistent extension; GPD tail (when fit)
    // overrides anything past p99 in the sampler.
    let (p99, calibration_method): (f64, &'static str) = if p99_raw >= cap_ms - 1.0 {
        let p99_extrap = if p95 > 0.0 && p50 > 0.0 {
            let sigma = (p95 / p50).ln() / 1.6449;
            (p50 * (sigma * 2.3263).exp()).max(p95 * 1.2)
        } else {
            cap_ms * 1.5
        };
        (p99_extrap, "raw RTT pooled (p99 censored, lognormal extrap)")
    } else {
        (p99_raw, "raw RTT pooled")
    };

    SidedParams {
        p50_ms: p50, p85_ms: Some(p85), p95_ms: p95, p99_ms: p99,
        p999_ms_override: None,
        n_rows, n_samples, n_timeouts,
        cap_hit_rate,
        calibration_method,
        rho_lag1: None, gpd_tail: None,
    }
}

/// Minimum raw RTT samples per UTC hour required before the
/// per-hour solver trusts pooled-raw over the legacy median path
/// for that hour. 200 ≈ 7 min of place-side activity at typical
/// 30-events/min — same noise tolerance as the pooled threshold
/// at 1000 (one-fifth the samples per hour vs the all-hours pool).
const POOLED_RAW_HOURLY_MIN_SAMPLES: usize = 200;

/// Build per-UTC-hour anchors using pooled-raw quantiles when each
/// hour has enough raw samples, falling back to the legacy
/// `solve_bucket` median path for sparse hours.
///
/// `raw_rtts_ms` and `raw_rtts_hour` are parallel vectors (same
/// length, indexed alike) holding all per-event raw RTT samples and
/// their UTC-hour tags. `acc.hourly[h].n_timeouts` provides the
/// per-hour censored counts.
///
/// Hours below `POOLED_RAW_HOURLY_MIN_SAMPLES` route through the
/// per-minute median path (`solve_bucket`) so old log formats or
/// off-hours stay calibrated. Both paths set
/// `SidedParams::calibration_method` to tag which solver ran for
/// each hour, observable in `engine.rs`'s startup log.
fn solve_hourly_with_raw(
    acc: &SidedAcc,
    raw_rtts_ms: &[f64],
    raw_rtts_hour: &[u8],
    cap_ms: f64,
) -> Box<[Option<SidedParams>; 24]> {
    debug_assert_eq!(raw_rtts_ms.len(), raw_rtts_hour.len());
    // Bucket raw RTTs by hour.
    let mut per_hour_raws: [Vec<f64>; 24] = std::array::from_fn(|_| Vec::new());
    for (rtt, h) in raw_rtts_ms.iter().zip(raw_rtts_hour.iter()) {
        if (*h as usize) < 24 && rtt.is_finite() && *rtt > 0.0 {
            per_hour_raws[*h as usize].push(*rtt);
        }
    }
    let mut out: [Option<SidedParams>; 24] = std::array::from_fn(|_| None);
    for h in 0..24 {
        let b = &acc.hourly[h];
        if b.p50s.is_empty() || b.n_samples < HOURLY_MIN_SAMPLES {
            continue;
        }
        let hour_raws = &per_hour_raws[h];
        if hour_raws.len() >= POOLED_RAW_HOURLY_MIN_SAMPLES {
            // Per-hour pooled-raw path.
            let cap_hit_rate = if b.n_samples > 100 {
                b.n_timeouts as f64 / b.n_samples as f64
            } else { 0.0 };
            out[h] = Some(solve_anchors_from_raw_rtts(
                hour_raws,
                b.n_timeouts,
                cap_ms,
                b.p50s.len(),
                b.n_samples,
                cap_hit_rate,
            ));
        } else {
            // Sparse-hour fallback: legacy per-minute median.
            out[h] = Some(solve_bucket(b, cap_ms));
        }
    }
    Box::new(out)
}

/// Lag-1 Pearson autocorrelation of `log(RTT)` over the input
/// samples in their original order. Returns `None` when fewer than
/// 100 positive samples are available — below that the estimate is
/// noisy enough that operators are better served by the manual
/// `sim_latency_correlation` knob.
fn lag1_autocorr_log_rtt(rtts_ms: &[f64]) -> Option<f64> {
    let logs: Vec<f64> = rtts_ms.iter().filter(|r| **r > 0.0).map(|r| r.ln()).collect();
    let n = logs.len();
    if n < 100 { return None; }
    let mean = logs.iter().sum::<f64>() / n as f64;
    let mut var = 0.0;
    let mut cov = 0.0;
    for i in 0..n {
        let d = logs[i] - mean;
        var += d * d;
    }
    for i in 0..n - 1 {
        cov += (logs[i] - mean) * (logs[i + 1] - mean);
    }
    if var > 0.0 { Some(cov / var) } else { None }
}

/// Fit a GPD tail to per-side raw RTT samples + censored-timeout count
/// via POT (peaks over threshold). Returns `None` when:
///   * fewer than 200 uncensored samples (PWM + MLE too noisy)
///   * fewer than 100 total tail observations (uncensored above
///     threshold + censored)
///   * exceedance rate `λ` outside `[0.02, 0.50]` — too thin a tail
///     to need GPD, or threshold so low the body bleeds in
///   * the censored MLE in `sim::gpd::fit_gpd_censored_mle` fails to
///     converge or returns ξ at the boundary
///
/// In any of these cases the caller falls back to the legacy
/// lognormal-extrap path in `empirical_anchors`.
///
/// ## Threshold selection
///
/// Threshold `u` is the larger of (the 85th percentile of uncensored
/// raw RTTs, 150 ms). Heuristic: 85th-percentile gives ~15 % naïve
/// exceedance rate, which once you fold in the censored timeouts
/// usually lands at λ ≈ 0.15–0.25 — enough mass past the threshold
/// to drive stable GPD MLE without being so deep into the body that
/// the GPD shape assumption breaks down. The 150 ms floor avoids
/// pathologically low thresholds when the session was unusually
/// fast (otherwise u → empirical p85 could be 60-80 ms and the
/// "tail" would just be the upper body).
const GPD_MIN_UNCENSORED: usize = 200;
const GPD_MIN_TAIL_OBS: usize = 100;
const GPD_THRESHOLD_QUANTILE: f64 = 0.85;
const GPD_THRESHOLD_FLOOR_MS: f64 = 150.0;
const GPD_LAMBDA_MIN: f64 = 0.02;
const GPD_LAMBDA_MAX: f64 = 0.50;

fn fit_gpd_from_raw_rtts(
    raw_rtts_ms: &[f64],
    n_timeouts: u64,
    cap_ms: f64,
) -> Option<GpdTail> {
    let n_uc = raw_rtts_ms.len();
    if n_uc < GPD_MIN_UNCENSORED {
        log::debug!(
            "[GPD] skip: n_uc={} < min={}",
            n_uc, GPD_MIN_UNCENSORED,
        );
        return None;
    }
    let mut sorted: Vec<f64> = raw_rtts_ms.iter().copied().filter(|r| *r > 0.0).collect();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n_sorted = sorted.len();
    if n_sorted < GPD_MIN_UNCENSORED {
        return None;
    }
    let idx_q = ((n_sorted as f64) * GPD_THRESHOLD_QUANTILE) as usize;
    let u_empirical = sorted[idx_q.min(n_sorted - 1)];
    let mut threshold_ms = u_empirical.max(GPD_THRESHOLD_FLOOR_MS);
    if threshold_ms >= cap_ms {
        log::debug!(
            "[GPD] skip: threshold {:.1}ms ≥ cap {}ms (raw RTTs unusually fast)",
            threshold_ms, cap_ms,
        );
        return None;
    }

    // Some heavy-tailed sessions (esp. cancel-side during gateway
    // brown-outs) have a true `P(X > u_p85) + cap_rate` far above
    // 0.50 because the cap rate alone can be > 0.20. The "λ is too
    // big" guard is meant to catch threshold-too-deep-in-body
    // misconfigurations, not legitimate fat-tail sessions. If λ
    // exceeds the upper bound, walk the threshold upward in
    // 5-percentile steps until λ ≤ MAX or we run out of room.
    let n_c = n_timeouts as usize;
    let n_total = n_sorted + n_c;
    let calc_lambda = |u: f64| -> (usize, f64) {
        let n_exc_at = sorted.iter().filter(|r| **r > u && **r < cap_ms).count();
        let lam = (n_exc_at + n_c) as f64 / n_total as f64;
        (n_exc_at, lam)
    };
    let (mut n_exc, mut lambda) = calc_lambda(threshold_ms);
    if lambda > GPD_LAMBDA_MAX {
        // Walk u up to bring λ down.
        for q in [0.90, 0.92, 0.94, 0.96, 0.98] {
            let new_u = sorted[((n_sorted as f64 * q) as usize).min(n_sorted - 1)]
                .max(GPD_THRESHOLD_FLOOR_MS);
            if new_u >= cap_ms { break; }
            let (e2, l2) = calc_lambda(new_u);
            threshold_ms = new_u;
            n_exc = e2;
            lambda = l2;
            if lambda <= GPD_LAMBDA_MAX { break; }
        }
    }
    if !(GPD_LAMBDA_MIN..=GPD_LAMBDA_MAX).contains(&lambda) {
        log::debug!(
            "[GPD] skip: λ={:.4} out of [{:.2}, {:.2}] (n_exc={}, n_c={}, n_total={}, u={:.1}ms)",
            lambda, GPD_LAMBDA_MIN, GPD_LAMBDA_MAX, n_exc, n_c, n_total, threshold_ms,
        );
        return None;
    }
    if n_exc + n_c < GPD_MIN_TAIL_OBS {
        log::debug!(
            "[GPD] skip: n_exc+n_c={} < min={}",
            n_exc + n_c, GPD_MIN_TAIL_OBS,
        );
        return None;
    }
    // Try the fit at the current threshold. If ξ̂ pegs at the upper
    // boundary, the threshold is likely too low — Pickands–Balkema–
    // de Haan only guarantees GPD convergence as u → ∞, and a tail
    // body still feels lognormal-ish below ~p90. Walk u upward
    // through {p90, p93, p96} and refit; accept the first fit with
    // a non-boundary ξ̂.
    let mut chosen_threshold = threshold_ms;
    let mut chosen_lambda = lambda;
    let mut chosen_exceedances = build_exceedances(&sorted, threshold_ms, cap_ms);
    let mle_fit = |excs: &[f64], u: f64| -> Option<(f64, f64)> {
        if excs.len() < 50 { return None; }
        crate::exchange::sim::gpd::fit_gpd_censored_mle(excs, n_c, cap_ms - u)
    };
    let mut params = mle_fit(&chosen_exceedances, chosen_threshold);
    // Escalate u if ξ̂ pegs at the upper bound (the objective clamps
    // at 0.85): an even higher threshold may bring the fit off the
    // boundary as Pickands–Balkema–de Haan kicks in more cleanly.
    // We accept the boundary fit only if no escalation rescues it.
    if params.is_none() || params.map(|(_, xi)| xi >= 0.63).unwrap_or(false) {
        for q in [0.90, 0.93, 0.96] {
            let new_u = sorted[((n_sorted as f64 * q) as usize).min(n_sorted - 1)]
                .max(GPD_THRESHOLD_FLOOR_MS);
            if new_u >= cap_ms - 20.0 { break; }
            let (e2, l2) = calc_lambda(new_u);
            if !(GPD_LAMBDA_MIN..=GPD_LAMBDA_MAX).contains(&l2) { continue; }
            if e2 + n_c < GPD_MIN_TAIL_OBS { continue; }
            let excs2 = build_exceedances(&sorted, new_u, cap_ms);
            if excs2.len() < 50 { continue; }
            let p2 = mle_fit(&excs2, new_u);
            if let Some((_, xi2)) = p2 {
                if xi2 < 0.63 {
                    chosen_threshold = new_u;
                    chosen_lambda = l2;
                    chosen_exceedances = excs2;
                    params = p2;
                    break;
                }
            }
        }
    }
    let (sigma, xi) = match params {
        Some(p) => p,
        None => {
            log::debug!(
                "[GPD] MLE failed to converge (n_exc={}, n_c={}, u={:.1}ms, horizon={:.1}ms)",
                chosen_exceedances.len(), n_c, chosen_threshold, cap_ms - chosen_threshold,
            );
            return None;
        }
    };

    Some(GpdTail {
        threshold_ms: chosen_threshold,
        sigma,
        xi,
        exceedance_rate: chosen_lambda,
        n_exceedances: chosen_exceedances.len(),
        n_censored: n_c,
    })
}

/// Build the uncensored exceedance vector `(X − u)` for `u < X < cap`.
fn build_exceedances(sorted: &[f64], u: f64, cap_ms: f64) -> Vec<f64> {
    sorted
        .iter()
        .filter(|r| **r > u && **r < cap_ms)
        .map(|r| r - u)
        .collect()
}

/// Censorship-aware p99 fallback: if the median within-minute p99 is
/// near the 500 ms client-timeout cap, replace it with a lognormal
/// extrapolation from (p50, p95). Otherwise pass through.
fn censored_p99_extrap(p50: f64, p95: f64, p99_median: f64) -> (f64, bool) {
    if p99_median >= 480.0 && p95 < p99_median {
        let sigma = (p95 / p50).ln() / 1.6449;
        let extrap = p50 * (sigma * 2.3263).exp();
        (extrap.max(p99_median), true)
    } else {
        (p99_median, false)
    }
}

/// Helper: parse `n=12345` style integer field from a log line.
fn parse_n_after(line: &str, label: &str) -> Option<u64> {
    let i = line.find(label)?;
    let rest = &line[i + label.len()..];
    let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
    rest[..end].parse().ok()
}

/// Helper: extract the first ms-or-s number after a label like `p95=`.
/// Returns ms (s units are converted). Lines look like
/// `… p95=331.4ms p99=501.7ms …` or `… p95=1.23s …`.
fn parse_ms_after(line: &str, label: &str) -> Option<f64> {
    let i = line.find(label)?;
    let rest = &line[i + label.len()..];
    // Take the leading numeric run, then check the unit suffix.
    let end = rest.find(|c: char| !(c.is_ascii_digit() || c == '.')).unwrap_or(rest.len());
    let num: f64 = rest[..end].parse().ok()?;
    let tail = &rest[end..];
    if tail.starts_with("ms") {
        Some(num)
    } else if tail.starts_with('s') {
        Some(num * 1000.0)
    } else {
        // Bare numeric — assume ms for backward compat.
        Some(num)
    }
}

fn median(xs: &mut [f64]) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = xs.len();
    if n == 0 { 0.0 } else if n % 2 == 1 { xs[n / 2] } else { 0.5 * (xs[n / 2 - 1] + xs[n / 2]) }
}

impl LatencyProfile {
    /// Build a 7-anchor CDF table for the Empirical variant. Public
    /// only for the inverse-CDF logic in `sample_ns` (kept inline as
    /// `[(p, ms); 7]` rather than a heap Vec to avoid an alloc per
    /// sample).
    ///
    /// Anchor positions: floor / p50 / **p85** / p95 / p99 / p99.9 / p99.99.
    /// The p85 anchor between p50 and p95 captures the body bimodality
    /// observed in live HTTP RTT — without it, the 5-anchor body
    /// linearly interpolates between (p50, p95) and over-flattens the
    /// 0.50–0.85 quantile region where the bot's actual fast-vs-slow
    /// regime split lives. When `p85_override` is None the curve
    /// collapses back to the legacy linear (p50→p95) shape because
    /// we synthesize p85 by linear interpolation at u=0.85; the
    /// numerical output is identical to the old 6-anchor table.
    fn empirical_anchors(
        p50_ms: f64,
        p85_override: Option<f64>,
        p95_ms: f64,
        p99_ms: f64,
        p999_override: Option<f64>,
        gpd_tail: Option<GpdTail>,
    ) -> [(f64, f64); 7] {
        // Resolve p85: explicit override > linear-interp fallback.
        // Linear interpolation at u=0.85 between (0.50, p50) and
        // (0.95, p95): p85_interp = p50 + 0.7778·(p95 − p50). Clamp
        // to (p50 < p85 < p95) so a misconfigured override (e.g.
        // p85 > p95 from a partial early-day calibration window)
        // can't break monotonicity of the inverse CDF.
        let p85_ms = {
            let interp = p50_ms + ((0.85 - 0.50) / (0.95 - 0.50)) * (p95_ms - p50_ms);
            let raw = match p85_override {
                Some(v) if v.is_finite() => v,
                _ => interp,
            };
            raw.max(p50_ms * 1.0001).min(p95_ms * 0.9999)
        };
        // GPD-tail path: when a peaks-over-threshold fit is available,
        // the upper-quantile anchors come from the GPD inverse-CDF
        // instead of lognormal extrapolation. This is the heavy-tail
        // path that matches live's observed power-law right tail past
        // ~p85 (lognormal extrap chronically underestimates p99.9 /
        // p99.99 on HTTP-gateway RTT — the gap is what drives the
        // backtest's near-absent N=4/5 events). When the GPD fit is
        // absent (too few samples, MLE failed, etc.) we fall through
        // to the legacy lognormal-extrap path below.
        if let Some(tail) = gpd_tail {
            // EVT body/tail mixture: keep the empirical (p50, p95)
            // anchors UNCHANGED so the body shape stays true to the
            // observed [latency] summary medians — same body as the
            // legacy lognormal path. Replace ONLY the p99+ anchors
            // with GPD-derived quantiles. This is the textbook POT
            // mixture: trust the empirical body where data is
            // densest, use GPD's analytic inverse CDF where the
            // observed quantiles are sparse / censored.
            //
            // For the typical place fit (ξ=0.5, σ=314, u=150, λ=0.22):
            //   p_thresh = 0.78
            //   gpd_q(0.99)   = u + (σ/ξ)·((0.01/λ)^(−ξ) − 1)
            //                 ≈ 150 + 625·(4.78 − 1) ≈ 2510 ms
            //   gpd_q(0.999)  ≈ 8900 ms
            //   gpd_q(0.9999) ≈ 29 s
            //
            // Compared to the legacy lognormal-extrap (≈ 5500 / 17000
            // ms at p999 / p9999), GPD is materially heavier — which
            // is the entire point of the redo.
            //
            // `.max()` guards against degenerate fits where ξ ≈ 0
            // and very low λ produce gpd_q(0.99) < p95: not expected
            // in production but cheap insurance for monotonicity.
            let p_thresh = (1.0 - tail.exceedance_rate).clamp(0.0, 0.99);
            let gpd_q = |p_target: f64| -> f64 {
                if p_target <= p_thresh {
                    return tail.threshold_ms;
                }
                let q_sub = ((p_target - p_thresh) / tail.exceedance_rate).clamp(0.0, 0.99999);
                tail.threshold_ms
                    + crate::exchange::sim::gpd::gpd_quantile(q_sub, tail.sigma, tail.xi)
            };
            let body_p50 = p50_ms.max(1.0);
            // p85 must sit between p50 and p95 (already clamped above).
            let body_p85 = p85_ms.max(body_p50 + 0.5);
            let body_p95 = p95_ms.max(body_p85 + 0.5);
            let tail_p99   = gpd_q(0.99).max(body_p95 + 1.0);
            let tail_p999  = gpd_q(0.999).max(tail_p99 + 1.0);
            let tail_p9999 = gpd_q(0.9999).max(tail_p999 + 1.0);
            return [
                (0.000,  (p50_ms / 5.0).max(1.0)),
                (0.500,  body_p50),
                (0.850,  body_p85),
                (0.950,  body_p95),
                (0.990,  tail_p99),
                (0.999,  tail_p999),
                (0.9999, tail_p9999),
            ];
        }

        // Legacy lognormal-extrap path (unchanged).
        //
        // Tail extrapolation past p99: continue the (p95 → p99) slope
        // in log-space — a stable proxy for "same shape as the
        // observed tail". For the typical (p95=330, p99=620) we get
        // p99.9 ≈ 1170 ms. The lognormal extrap is the DEFAULT but
        // can be overridden when the calibrator has a tighter
        // anchor (e.g. solving the cap-rate constraint backward
        // when the observed cap-hit rate is small but non-zero).
        // Without the override, observed (p50=23 p95=63 p99=287)
        // gave p99.9 ≈ 1571 ms — pushing 0.85 % of samples past the
        // 500 ms cap when live actually clipped only 0.05 %.
        let p999_ms = match p999_override {
            Some(v) if v.is_finite() && v > p99_ms => v,
            _ if p95_ms > 0.0 && p99_ms > p95_ms => {
                let log_p95 = p95_ms.ln();
                let log_p99 = p99_ms.ln();
                (log_p99 + 1.1213 * (log_p99 - log_p95)).exp()
            }
            _ => p99_ms * 2.0,
        };
        let p9999_ms = if p999_ms > p99_ms && p99_ms > 0.0 {
            let log_p99 = p99_ms.ln();
            let log_p999 = p999_ms.ln();
            (log_p999 + 0.8231 * (log_p999 - log_p99)).exp()
        } else {
            p999_ms * 1.5
        };
        [
            (0.000,  (p50_ms / 5.0).max(1.0)),
            (0.500,  p50_ms),
            (0.850,  p85_ms),
            (0.950,  p95_ms),
            (0.990,  p99_ms),
            (0.999,  p999_ms),
            (0.9999, p9999_ms),
        ]
    }

    /// Like `empirical_anchors` but **re-injects the right-censored
    /// timeout tail**. Per-event place/cancel quantiles are computed from
    /// non-timeout acks only (a timed-out request never acks — see
    /// `per_event_rtt`), so they describe `RTT | RTT < cap`. Fed raw, the
    /// sampler can't draw `RTT > cap`: cancel timeouts ≈ 0 in sim despite
    /// ~1.3 % in live, and the place tail is truncated (sim max ≈ 13 vs
    /// live ≈ 48 per event).
    ///
    /// Given the observed timeout rate `r = P(RTT > cap)`, rebuild the
    /// full-distribution inverse CDF:
    ///   * survivor body anchors keep their VALUES but move to
    ///     full-distribution quantiles `u·(1 − r)` — for `x < cap`,
    ///     `F(x) = (1 − r)·G(x)` where `G` is the survivor CDF;
    ///   * an exceedance anchor `(1 − r, cap)` places exactly `r` mass at
    ///     / past the client timeout;
    ///   * a final `(≈1, cap·1.5)` ramps the censored region so every draw
    ///     with `u > 1 − r` exceeds `cap` (→ timeout). The magnitude past
    ///     `cap` is unobservable (live records all timeouts at the cap), so
    ///     a modest 1.5× keeps the order's engine-reach time realistic; the
    ///     timeout COUNT — the thing we calibrate — is exact regardless.
    ///
    /// `rate = None`, `rate ≤ 1e-4`, or a degenerate `cap ≤ p99` all fall
    /// back to the standard lognormal-extrap `empirical_anchors`,
    /// byte-identical to the pre-censoring path.
    fn empirical_anchors_censored(
        p50_ms: f64,
        p85_override: Option<f64>,
        p95_ms: f64,
        p99_ms: f64,
        timeout_rate: Option<f64>,
        cap_ms: f64,
    ) -> [(f64, f64); 7] {
        let r = match timeout_rate {
            // r > 1e-4: below this the cap crossing sits above the 0.999
            // anchor and the lognormal path already covers it. r < 0.5:
            // a sane upper guard (no real event times out half its orders).
            Some(r) if r.is_finite() && r > 1e-4 && r < 0.5 => r,
            _ => return Self::empirical_anchors(p50_ms, p85_override, p95_ms, p99_ms, None, None),
        };
        // The cap must sit strictly above the survivor body, else the
        // exceedance anchor would break monotonicity. Survivor samples are
        // all < cap so this normally holds; fall back if not.
        if !(cap_ms.is_finite() && cap_ms > p99_ms && p95_ms > 0.0) {
            return Self::empirical_anchors(p50_ms, p85_override, p95_ms, p99_ms, None, None);
        }
        // Resolve p85 identically to the base builder (interp + clamp).
        let p85_ms = {
            let interp = p50_ms + ((0.85 - 0.50) / (0.95 - 0.50)) * (p95_ms - p50_ms);
            let raw = match p85_override {
                Some(v) if v.is_finite() => v,
                _ => interp,
            };
            raw.max(p50_ms * 1.0001).min(p95_ms * 0.9999)
        };
        let s = 1.0 - r; // survivor mass; body compresses into [0, s).
        let u_top = 0.9999_f64.max(s + 1e-4);
        let cap_top = (cap_ms * 1.5).max(cap_ms + 1.0);
        [
            (0.000,     (p50_ms / 5.0).max(1.0)),
            (0.500 * s, p50_ms),
            (0.850 * s, p85_ms),
            (0.950 * s, p95_ms),
            (0.990 * s, p99_ms),
            (s,         cap_ms),
            (u_top,     cap_top),
        ]
    }

    /// Human-readable summary for logging at startup.
    pub fn describe(&self) -> String {
        match self {
            LatencyProfile::Fixed(ms) => format!("fixed {}ms (one-way)", ms),
            LatencyProfile::Empirical { p50_ms, p85_ms_override, p95_ms, p99_ms, rho, p999_ms_override, gpd_tail } => {
                let anchors = Self::empirical_anchors(*p50_ms, *p85_ms_override, *p95_ms, *p99_ms, *p999_ms_override, *gpd_tail);
                let p85 = anchors[2].1;
                let p999 = anchors[5].1;
                let p9999 = anchors[6].1;
                let corr_note = if *rho <= 0.0 {
                    "iid".to_string()
                } else {
                    format!("AR(1) ρ={:.2}", rho)
                };
                let tail_tag = if let Some(t) = gpd_tail {
                    format!(" [GPD ξ={:.2} σ={:.0} u={:.0}ms λ={:.1}%]", t.xi, t.sigma, t.threshold_ms, t.exceedance_rate * 100.0)
                } else if p999_ms_override.is_some() {
                    " [cap-rate solved]".to_string()
                } else {
                    "".to_string()
                };
                let p85_tag = if p85_ms_override.is_some() { "" } else { " (interp)" };
                format!(
                    "empirical RTT (p50={:.0}  p85={:.0}{}  p95={:.0}  p99={:.0}  p99.9≈{:.0}  p99.99≈{:.0} ms{}; \
                     one-way = RTT/2; clustering: {})",
                    p50_ms, p85, p85_tag, p95_ms, p99_ms, p999, p9999, tail_tag, corr_note,
                )
            }
            LatencyProfile::HourlyEmpirical { hourly, fallback, rho } => {
                let n_hours = hourly.iter().filter(|h| h.is_some()).count();
                let corr_note = if *rho <= 0.0 {
                    "iid".to_string()
                } else {
                    format!("AR(1) ρ={:.2}", rho)
                };
                // Emit a compact bucket-list so operators can skim the
                // hour-of-day shape at startup. Each entry: HH:p50/p95/p99
                // (rounded to whole ms), pipe-separated.
                let mut parts: Vec<String> = Vec::with_capacity(n_hours);
                for (h, opt) in hourly.iter().enumerate() {
                    if let Some(a) = opt {
                        parts.push(format!(
                            "{:02}:{:.0}/{:.0}/{:.0}",
                            h, a.p50_ms, a.p95_ms, a.p99_ms,
                        ));
                    }
                }
                format!(
                    "hourly empirical RTT ({} hours covered: {}; fallback p50={:.0} p95={:.0} p99={:.0} ms; \
                     one-way = RTT/2; clustering: {})",
                    n_hours, parts.join(" | "),
                    fallback.p50_ms, fallback.p95_ms, fallback.p99_ms, corr_note,
                )
            }
            LatencyProfile::SaturdayEmpirical { sat, non_sat, rho } => {
                let corr_note = if *rho <= 0.0 {
                    "iid".to_string()
                } else {
                    format!("AR(1) ρ={:.2}", rho)
                };
                format!(
                    "saturday-split empirical RTT (NY-Sat p50/p95/p99={:.0}/{:.0}/{:.0} ms, \
                     non-Sat={:.0}/{:.0}/{:.0} ms; one-way = RTT/2; clustering: {})",
                    sat.p50_ms, sat.p95_ms, sat.p99_ms,
                    non_sat.p50_ms, non_sat.p95_ms, non_sat.p99_ms,
                    corr_note,
                )
            }
            LatencyProfile::RecordReplay { records, rho, params } => {
                let corr_note = if *rho <= 0.0 {
                    "iid".to_string()
                } else {
                    format!("AR(1) ρ={:.2} (tier-3 only)", rho)
                };
                format!(
                    "record-replay RTT ({:?}; tiers: exact≤{}ms → same-tod≤{}s → nearest-tod dist; clustering: {})",
                    records, params.abs_tol_ms, params.tod_tol_secs, corr_note,
                )
            }
        }
    }
}

/// UTC hour-of-day (0..23) for a Unix-epoch nanosecond timestamp.
/// Used by the hourly sampler to look up the active anchor table.
#[inline]
fn ns_to_utc_hour(now_ns: u64) -> usize {
    ((now_ns / 1_000_000_000 / 3_600) % 24) as usize
}

/// Sampler — carries the profile, RNG, and the AR(1) latent state.
/// Single-threaded use (backtest loop owns it); no interior mutability.
pub struct LatencySampler {
    profile: LatencyProfile,
    rng: StdRng,
    /// Cached anchor table for `Empirical`, computed once at
    /// construction so the hot path doesn't recompute the tail
    /// extrapolation per sample.
    anchors: Option<[(f64, f64); 7]>,
    /// Cached per-hour anchor tables for `HourlyEmpirical`. `None`
    /// entries fall through to `fallback_anchors`.
    hourly_anchors: Option<Box<[Option<[(f64, f64); 7]>; 24]>>,
    /// Pooled-fallback anchor table used when an hour bucket in
    /// `HourlyEmpirical` is empty. Always populated for that variant.
    fallback_anchors: Option<[(f64, f64); 7]>,
    /// Cached (sat, non_sat) anchor tables for `SaturdayEmpirical`.
    /// `.0` = NY-Saturday bucket, `.1` = every-other-day bucket.
    saturday_anchors: Option<([(f64, f64); 7], [(f64, f64); 7])>,
    /// **Per-event anchor override** (2026-05-21). When `Some`, this
    /// table takes priority over every other anchor source — the
    /// engine pushes it for each Polymarket event whose RTT
    /// distribution was extracted from a live.log via
    /// `crate::exchange::sim::per_event_rtt::extract_per_event_rtt`.
    /// On event boundary the engine calls `set_per_event_anchors`
    /// (or `clear_per_event_anchors` if no match) so each event runs
    /// with the live's actual latency CDF.
    ///
    /// The override coexists with the AR(1) `rho` from the profile —
    /// the anchors swap, but the latent state `z_prev` carries over,
    /// so the autocorrelation structure stays continuous across the
    /// event boundary (matching live's behavior where ρ doesn't reset
    /// at event start).
    per_event_anchors: Option<[(f64, f64); 7]>,
    /// **Intra-event segmented anchors** (2026-05-28). When `Some`,
    /// takes priority over `per_event_anchors` and every other source.
    /// Captures the front-loaded RTT burst observed in live (mean RTT
    /// 3× higher in the first 60 s of high-vol events). The sampler
    /// picks `early` when `time_in_event = now_ns − event_start_ns
    /// < boundary_ns`, else `late`. Across the boundary the AR(1)
    /// latent `z_prev` carries over so the autocorrelation signal is
    /// continuous — only the marginal CDF swaps.
    per_event_segmented: Option<SegmentedAnchors>,
    /// `event_start_ns` for the current segmented override. Sampler
    /// computes `time_in_event = now_ns − this` at each draw.
    /// Defaulted to 0 when no override is active.
    per_event_start_ns: u64,
    /// Persistent latent Gaussian state for AR(1).
    z_prev: f64,
}

/// Intra-event segmented anchor pair. See `LatencySampler::per_event_segmented`.
#[derive(Debug, Clone, Copy)]
struct SegmentedAnchors {
    /// First-segment CDF anchors (event offset 0..`boundary_ns`).
    early: [(f64, f64); 7],
    /// Second-segment CDF anchors (event offset >= `boundary_ns`).
    late: [(f64, f64); 7],
    /// Segment boundary in nanoseconds. Below = early, at-or-above = late.
    boundary_ns: u64,
}

impl LatencySampler {
    /// Construct with explicit seed. `seed = 0` falls back to entropy
    /// (non-reproducible run); any non-zero seed produces a
    /// deterministic stream.
    pub fn new(profile: LatencyProfile, seed: u64) -> Self {
        let mut rng = if seed == 0 {
            StdRng::from_entropy()
        } else {
            StdRng::seed_from_u64(seed)
        };
        let (anchors, hourly_anchors, fallback_anchors, saturday_anchors) = match &profile {
            LatencyProfile::Empirical { p50_ms, p85_ms_override, p95_ms, p99_ms, p999_ms_override, gpd_tail, .. } => {
                (
                    Some(LatencyProfile::empirical_anchors(
                        *p50_ms, *p85_ms_override, *p95_ms, *p99_ms, *p999_ms_override, *gpd_tail,
                    )),
                    None,
                    None,
                    None,
                )
            }
            LatencyProfile::HourlyEmpirical { hourly, fallback, .. } => {
                let mut h: [Option<[(f64, f64); 7]>; 24] = std::array::from_fn(|_| None);
                for i in 0..24 {
                    if let Some(a) = &hourly[i] {
                        h[i] = Some(LatencyProfile::empirical_anchors(
                            a.p50_ms, a.p85_ms_override, a.p95_ms, a.p99_ms,
                            a.p999_ms_override, a.gpd_tail,
                        ));
                    }
                }
                let fb = LatencyProfile::empirical_anchors(
                    fallback.p50_ms, fallback.p85_ms_override, fallback.p95_ms, fallback.p99_ms,
                    fallback.p999_ms_override, fallback.gpd_tail,
                );
                (None, Some(Box::new(h)), Some(fb), None)
            }
            LatencyProfile::SaturdayEmpirical { sat, non_sat, .. } => {
                let mk = |a: &EmpiricalAnchors| LatencyProfile::empirical_anchors(
                    a.p50_ms, a.p85_ms_override, a.p95_ms, a.p99_ms,
                    a.p999_ms_override, a.gpd_tail,
                );
                (None, None, None, Some((mk(sat), mk(non_sat))))
            }
            _ => (None, None, None, None),
        };
        // Initialise latent state from its stationary N(0, 1) so the
        // first AR(1) sample is distributed correctly.
        let z_prev = standard_normal(&mut rng);
        Self {
            profile, rng, anchors, hourly_anchors, fallback_anchors,
            saturday_anchors,
            per_event_anchors: None,
            per_event_segmented: None,
            per_event_start_ns: 0,
            z_prev,
        }
    }

    /// **Push per-event anchor override** (2026-05-21). Called by the
    /// engine on each Polymarket `Instrument` dispatch when
    /// `sim_rtt_mode = "exact"` AND the event_secs has a matching
    /// entry in the parsed per-event table.
    ///
    /// Builds a fresh anchor table from the live-observed `(p50, p85,
    /// p95, p99)` and stores it on the sampler. Subsequent
    /// `sample_ns` calls draw from this table until the next event
    /// boundary, at which point the engine either pushes a new
    /// override or calls `clear_per_event_anchors`. AR(1) latent
    /// state is preserved across the swap so the autocorrelation
    /// signal stays continuous (matching live where ρ doesn't reset
    /// on event start).
    ///
    /// `p999` is auto-extrapolated from `(p95, p99)` via the existing
    /// log-slope tail logic — per-event windows are too short to fit
    /// a GPD tail directly. `p85` is exact (no interp fallback).
    pub fn set_per_event_anchors(
        &mut self,
        p50_ms: f64, p85_ms: f64, p95_ms: f64, p99_ms: f64,
        timeout_rate: Option<f64>, cap_ms: f64,
    ) {
        // `empirical_anchors_censored` re-injects the right-censored
        // timeout tail when a rate is supplied (the body quantiles above
        // are from non-timeout acks only); rate=None reduces to the
        // legacy lognormal-extrap path, byte-identical.
        self.per_event_anchors = Some(LatencyProfile::empirical_anchors_censored(
            p50_ms, Some(p85_ms), p95_ms, p99_ms, timeout_rate, cap_ms,
        ));
        // The segmented override (if any) is replaced by the new
        // aggregate one — caller signaled "no per-segment data" by
        // calling this single-anchor variant.
        self.per_event_segmented = None;
    }

    /// **Push intra-event segmented anchors** (2026-05-28). When live
    /// extraction produced enough samples in BOTH the early (0..boundary)
    /// and late (boundary..270 s) buckets, this variant is used: the
    /// sampler holds both CDFs and picks at draw time based on
    /// `time_in_event = now_ns − event_start_ns`.
    ///
    /// Falls back to the legacy single-anchor path when either bucket
    /// has insufficient samples (caller is responsible for that
    /// branching — see `EventRttOverride::has_segmented_*`).
    ///
    /// Across the early→late boundary the AR(1) latent `z_prev`
    /// carries over so the autocorrelation signal is continuous —
    /// only the marginal CDF swaps. Same property as the
    /// `per_event_anchors` swap across event boundaries.
    pub fn set_per_event_segmented_anchors(
        &mut self,
        event_start_ns: u64,
        early: (f64, f64, f64, f64),
        late:  (f64, f64, f64, f64),
        boundary_secs: u64,
        timeout_rate: Option<f64>,
        cap_ms: f64,
    ) {
        // The event-level timeout rate is applied to BOTH segments' tails
        // (we calibrate the rate per event, not per intra-event segment —
        // see EventRttOverride::*_timeout_rate). Each segment's own body
        // quantiles still differ (early window is slower).
        let mk = |q: (f64, f64, f64, f64)| LatencyProfile::empirical_anchors_censored(
            q.0, Some(q.1), q.2, q.3, timeout_rate, cap_ms,
        );
        self.per_event_segmented = Some(SegmentedAnchors {
            early: mk(early),
            late:  mk(late),
            boundary_ns: boundary_secs.saturating_mul(1_000_000_000),
        });
        self.per_event_start_ns = event_start_ns;
        // The aggregate-anchor override is mutually exclusive; clear it
        // so the segmented path runs cleanly.
        self.per_event_anchors = None;
    }

    /// Clear the per-event override so subsequent samples fall back to
    /// the construction-time CDF (hourly / saturday / pooled). Called
    /// by the engine on event boundary when the new event has no
    /// override match. Clears both the aggregate and segmented overrides.
    pub fn clear_per_event_anchors(&mut self) {
        self.per_event_anchors = None;
        self.per_event_segmented = None;
    }

    /// Whether a per-event override is currently active (either
    /// aggregate or segmented variant). Used by tests + the engine
    /// summary log.
    pub fn has_per_event_anchors(&self) -> bool {
        self.per_event_anchors.is_some() || self.per_event_segmented.is_some()
    }

    /// Whether the segmented (intra-event) override is currently
    /// active. Distinct from `has_per_event_anchors` for the engine's
    /// summary log to surface which variant is in use.
    pub fn has_segmented_per_event_anchors(&self) -> bool {
        self.per_event_segmented.is_some()
    }

    /// Convenience: fixed-latency sampler.
    pub fn fixed(ms: u64) -> Self {
        Self::new(LatencyProfile::Fixed(ms), 1)
    }

    /// Draw one **full RTT** in nanoseconds from the calibrated anchor
    /// table. `now_ns` is the engine's current sim/strat clock (Unix-
    /// epoch nanoseconds); it's only consulted by `HourlyEmpirical` to
    /// pick which per-hour anchor table to evaluate. `Fixed` and
    /// `Empirical` variants ignore it.
    ///
    /// ## RTT semantics (post-2026-05-15 refactor)
    ///
    /// Each call returns a sample from the **single-call RTT
    /// distribution** the anchors were calibrated against — i.e. one
    /// full `Submit → Order accepted` round-trip, matching what one
    /// row of the `[latency] polymarket.http.{place,cancel}_order`
    /// summary measures. The engine's dual-clock model splits this
    /// RTT 50/50 into `L1` (strat→sim outbound half) and `L2`
    /// (sim→strat inbound half) — see `engine.rs` around `sig_ts =
    /// emit + L1`.
    ///
    /// Before this refactor, `sample_ns` halved each draw and called
    /// itself the result a "one-way" sample. The engine then summed
    /// two such draws (`L1 + L2`) to form a paired RTT, which produced
    /// a paired distribution with the SAME mean as the anchor but
    /// MEASURABLY DIFFERENT shape: in live14+15 the live single-call
    /// p50 is 109 ms, but the old paired-RTT had p50=315 ms — body
    /// inflated 3× by the i.i.d.-half summation. The new architecture
    /// produces paired RTT == single-draw RTT (because L1+L2 = X).
    /// Each consecutive call advances the AR(1) latent once, which
    /// is what the calibrator's `lag1_autocorr_log_rtt` measures from
    /// live's per-call RTT stream — so ρ now aligns with the live
    /// signal-to-signal correlation, not lag-2 of an artificial
    /// half-step decomposition.
    #[inline]
    pub fn sample_ns(&mut self, now_ns: u64) -> u64 {
        // Segmented per-event override (2026-05-28) takes ABSOLUTE
        // priority. Captures the front-loaded RTT burst in the first
        // ~60 s of high-vol events. AR(1) latent state carries across
        // the early→late boundary so the autocorrelation signal stays
        // continuous; only the marginal CDF swaps.
        if let Some(seg) = self.per_event_segmented.as_ref() {
            let time_in_event = now_ns.saturating_sub(self.per_event_start_ns);
            let anchors = if time_in_event < seg.boundary_ns { &seg.early } else { &seg.late };
            let rho = self.profile_rho();
            return Self::draw_rtt_ns(rho, anchors, &mut self.rng, &mut self.z_prev);
        }
        // Per-event anchor override (2026-05-21) takes priority over
        // every other anchor source. Uses the profile's ρ for AR(1)
        // (Fixed mode falls through unchanged — no anchors apply).
        if let Some(anchors) = self.per_event_anchors.as_ref() {
            let rho = match &self.profile {
                LatencyProfile::Empirical { rho, .. } => *rho,
                LatencyProfile::HourlyEmpirical { rho, .. } => *rho,
                LatencyProfile::SaturdayEmpirical { rho, .. } => *rho,
                LatencyProfile::RecordReplay { rho, .. } => *rho,
                LatencyProfile::Fixed(_) => 0.0,
            };
            return Self::draw_rtt_ns(rho, anchors, &mut self.rng, &mut self.z_prev);
        }
        match &self.profile {
            LatencyProfile::Fixed(ms) => ms.saturating_mul(1_000_000),
            LatencyProfile::Empirical { rho, .. } => {
                let anchors = self.anchors.as_ref().expect("Empirical anchors initialised");
                Self::draw_rtt_ns(*rho, anchors, &mut self.rng, &mut self.z_prev)
            }
            LatencyProfile::HourlyEmpirical { rho, .. } => {
                let hour = ns_to_utc_hour(now_ns);
                let h_table = self.hourly_anchors.as_ref().expect("hourly anchors initialised");
                let anchors_ref: &[(f64, f64); 7] = match &h_table[hour] {
                    Some(a) => a,
                    None => self.fallback_anchors.as_ref().expect("fallback anchors initialised"),
                };
                Self::draw_rtt_ns(*rho, anchors_ref, &mut self.rng, &mut self.z_prev)
            }
            LatencyProfile::SaturdayEmpirical { rho, .. } => {
                let (sat, non_sat) = self.saturday_anchors.as_ref()
                    .expect("saturday anchors initialised");
                let anchors_ref = if is_ny_saturday_ns(now_ns) { sat } else { non_sat };
                Self::draw_rtt_ns(*rho, anchors_ref, &mut self.rng, &mut self.z_prev)
            }
            LatencyProfile::RecordReplay { records, rho, params } => {
                // Advance the AR(1) latent (own RNG) → uniform `u`, then
                // resolve against the recorded samples. `u` only matters
                // for the tier-3 distribution draw; tiers 1 & 2 are
                // deterministic in `now_ns`.
                let u = Self::draw_u(*rho, &mut self.rng, &mut self.z_prev);
                let rtt_ms = records.lookup(now_ns / 1_000_000, u, params).unwrap_or(0.0);
                (rtt_ms * 1_000_000.0).max(0.0) as u64
            }
        }
    }

    /// Advance the AR(1) latent once and return the resulting uniform
    /// `u = Φ(z)` (or a fresh iid uniform when `rho == 0`). Shared by the
    /// record-replay path, which needs `u` rather than an inverse-CDF
    /// draw.
    #[inline]
    fn draw_u(rho: f64, rng: &mut StdRng, z_prev: &mut f64) -> f64 {
        if rho > 0.0 {
            let rho = rho.clamp(0.0, 0.999);
            let eps = standard_normal(rng);
            let z = rho * *z_prev + (1.0 - rho * rho).sqrt() * eps;
            *z_prev = z;
            norm_cdf(z)
        } else {
            rng.gen::<f64>()
        }
    }

    /// Shared draw logic — AR(1) latent step (or iid uniform when
    /// `rho == 0`) → CDF transform → inverse CDF. Returns a full RTT
    /// in nanoseconds; the engine splits 50/50 into L1/L2 at the
    /// signal-emit site.
    #[inline]
    fn draw_rtt_ns(
        rho: f64,
        anchors: &[(f64, f64); 7],
        rng: &mut StdRng,
        z_prev: &mut f64,
    ) -> u64 {
        let u = if rho > 0.0 {
            let rho = rho.clamp(0.0, 0.999);
            let eps = standard_normal(rng);
            let z = rho * *z_prev + (1.0 - rho * rho).sqrt() * eps;
            *z_prev = z;
            norm_cdf(z)
        } else {
            rng.gen::<f64>()
        };
        let rtt_ms = inverse_cdf(anchors, u);
        (rtt_ms * 1_000_000.0).max(0.0) as u64
    }

    /// Resolve the anchor table for the given clock. Empirical →
    /// constant; HourlyEmpirical → look up by UTC hour with fallback.
    /// Returns `None` for non-empirical profiles (Fixed).
    #[inline]
    fn anchors_at(&self, now_ns: u64) -> Option<&[(f64, f64); 7]> {
        match &self.profile {
            LatencyProfile::Fixed(_) => None,
            LatencyProfile::Empirical { .. } => self.anchors.as_ref(),
            LatencyProfile::HourlyEmpirical { .. } => {
                let hour = ns_to_utc_hour(now_ns);
                let h_table = self.hourly_anchors.as_ref()?;
                match &h_table[hour] {
                    Some(a) => Some(a),
                    None => self.fallback_anchors.as_ref(),
                }
            }
            LatencyProfile::SaturdayEmpirical { .. } => {
                let (sat, non_sat) = self.saturday_anchors.as_ref()?;
                Some(if is_ny_saturday_ns(now_ns) { sat } else { non_sat })
            }
            // Record-replay has no fixed anchor table — it resolves RTT
            // directly from recorded samples in `sample_ns[_with_eps]`,
            // which special-case it before ever calling `anchors_at`.
            LatencyProfile::RecordReplay { .. } => None,
        }
    }

    /// Empirical/HourlyEmpirical profile's `rho`. Used by the coupled
    /// driver to advance the AR(1) latent with externally-supplied ε.
    #[inline]
    fn profile_rho(&self) -> f64 {
        match self.profile {
            LatencyProfile::Fixed(_) => 0.0,
            LatencyProfile::Empirical { rho, .. } => rho,
            LatencyProfile::HourlyEmpirical { rho, .. } => rho,
            LatencyProfile::SaturdayEmpirical { rho, .. } => rho,
            LatencyProfile::RecordReplay { rho, .. } => rho,
        }
    }

    /// Like `sample_ns` but uses an externally-supplied standard-normal
    /// innovation `eps` instead of drawing from the sampler's own RNG.
    /// The AR(1) latent state still advances exactly as in `sample_ns`
    /// (`z ← rho·z + √(1-rho²)·eps`) so a coordinator can drive two
    /// samplers with correlated innovations to model joint slow-regime
    /// behaviour. `Fixed` profiles ignore `eps`.
    pub fn sample_ns_with_eps(&mut self, eps: f64, now_ns: u64) -> u64 {
        if let LatencyProfile::Fixed(ms) = &self.profile {
            return ms.saturating_mul(1_000_000);
        }
        // Record-replay: advance the AR(1) latent with the supplied
        // (correlated) innovation so cross-side coupling still applies to
        // the tier-3 quantile, then resolve against the recorded samples.
        if let LatencyProfile::RecordReplay { records, rho, params } = &self.profile {
            let rho = (*rho).clamp(0.0, 0.999);
            let z = rho * self.z_prev + (1.0 - rho * rho).sqrt() * eps;
            self.z_prev = z;
            let u = norm_cdf(z);
            let rtt_ms = records.lookup(now_ns / 1_000_000, u, params).unwrap_or(0.0);
            return (rtt_ms * 1_000_000.0).max(0.0) as u64;
        }
        // **Anchor priority** (highest first):
        //   1. Segmented per-event override (2026-05-28) — picks
        //      `early` / `late` by `time_in_event`.
        //   2. Aggregate per-event override (2026-05-21) — single CDF
        //      for the whole event.
        //   3. Profile anchors (hourly / saturday / pooled) via
        //      `anchors_at(now_ns)`.
        //
        // Note: prior to 2026-05-28 the eps path ONLY consulted (3),
        // silently ignoring per-event overrides set by the engine
        // through `CoupledLatencySamplers::set_per_event_anchors`.
        // That bug is fixed here as part of the segmented rollout —
        // sims using a per-event RTT table should now honour it
        // through the coupled-correlated path too.
        let anchors_copy: [(f64, f64); 7] = if let Some(seg) = self.per_event_segmented.as_ref() {
            let time_in_event = now_ns.saturating_sub(self.per_event_start_ns);
            if time_in_event < seg.boundary_ns { seg.early } else { seg.late }
        } else if let Some(a) = self.per_event_anchors.as_ref() {
            *a
        } else {
            match self.anchors_at(now_ns) {
                Some(a) => *a,
                None => return 0,
            }
        };
        let rho = self.profile_rho().clamp(0.0, 0.999);
        // Always go through the AR(1) → Φ path even when rho==0 (z = eps,
        // u = Φ(eps)). The marginal stays uniform; we just trade the
        // legacy `rng.gen()` source for the supplied innovation, which
        // is what makes coupling work.
        let z = rho * self.z_prev + (1.0 - rho * rho).sqrt() * eps;
        self.z_prev = z;
        let u = norm_cdf(z);
        let rtt_ms = inverse_cdf(&anchors_copy, u);
        (rtt_ms * 1_000_000.0).max(0.0) as u64
    }

    /// Borrow the profile for logging / introspection.
    pub fn profile(&self) -> &LatencyProfile {
        &self.profile
    }

    /// Current AR(1) latent state `z_prev`. Exposed for diagnostics
    /// and tests — the sim no longer takes any stochastic decisions
    /// gated on the latent.
    #[allow(dead_code)]
    pub fn current_z(&self) -> f64 {
        match self.profile {
            LatencyProfile::Fixed(_) => 0.0,
            LatencyProfile::Empirical { .. } => self.z_prev,
            LatencyProfile::HourlyEmpirical { .. } => self.z_prev,
            LatencyProfile::SaturdayEmpirical { .. } => self.z_prev,
            LatencyProfile::RecordReplay { .. } => self.z_prev,
        }
    }
}

/// Cross-correlated wrapper around two `LatencySampler`s — one for
/// place, one for cancel. Generates a 2D Gaussian innovation pair
/// `(ε_p, ε_c)` per draw with `corr(ε_p, ε_c) = rho_cross`, advances
/// **both** AR(1) latent states, and returns the requested side's
/// one-way latency. The other side's state still advances under the
/// hood, so its next draw reflects the joint dynamics.
///
/// ## Why this matters
///
/// The independent-samplers default underestimates the joint
/// timeout probability that drives the worst real-world failure mode:
/// when the gateway is congested, both place AND cancel slow down
/// together, so a strategy's cancel-during-pile-up keeps timing out
/// while reverse fills accumulate. Independent samplers would model
/// this as `P(L1 cancel slow) · P(L2 place slow) ≈ small²` whereas
/// reality has `P(both slow) ≈ small` (tail of one common factor).
///
/// `rho_cross = 0` reproduces the legacy independent behaviour
/// exactly modulo RNG sequence (innovations come from the coupling
/// driver's RNG instead of each sampler's own).
pub struct CoupledLatencySamplers {
    place: LatencySampler,
    cancel: LatencySampler,
    /// Drives the (ε_p, ε_c) pair on every draw; never read by the
    /// underlying samplers (they consume the supplied ε via
    /// `sample_ns_with_eps`).
    rng: StdRng,
    /// Cross-correlation of the per-tick standard-normal innovations.
    /// Clamped to (-0.999, 0.999) at construction.
    rho_cross: f64,
}

impl CoupledLatencySamplers {
    /// Wrap two pre-built samplers with a shared coupling RNG.
    /// `rho_cross` controls the correlation of the underlying
    /// standard-normal innovations; values ≥ 1 are clamped, NaN
    /// becomes 0. The coupling RNG seed is independent of either
    /// sampler's seed so the AR(1) trajectories don't degenerate
    /// when both samplers happen to share a seed.
    pub fn new(
        place: LatencySampler,
        cancel: LatencySampler,
        rho_cross: f64,
        seed: u64,
    ) -> Self {
        let rho_cross = if rho_cross.is_finite() {
            rho_cross.clamp(-0.999, 0.999)
        } else {
            0.0
        };
        let rng = if seed == 0 {
            StdRng::from_entropy()
        } else {
            StdRng::seed_from_u64(seed)
        };
        Self { place, cancel, rng, rho_cross }
    }

    /// Draw the place side's one-way latency. Internally generates a
    /// correlated `(ε_p, ε_c)` pair and advances BOTH AR(1) latent
    /// states; the cancel side's state is updated but its RTT is
    /// discarded. The next `sample_cancel` call therefore reflects
    /// the latent already biased by this place tick.
    pub fn sample_place(&mut self, now_ns: u64) -> u64 {
        let (eps_p, eps_c) = self.draw_correlated_pair();
        // Update cancel's latent first (discard returned ns) — the
        // order doesn't affect statistics, but doing place last keeps
        // the place draw "live" for any debug introspection.
        let _ = self.cancel.sample_ns_with_eps(eps_c, now_ns);
        self.place.sample_ns_with_eps(eps_p, now_ns)
    }

    /// Symmetric to `sample_place`. Both states advance; only the
    /// cancel one-way ns is returned.
    pub fn sample_cancel(&mut self, now_ns: u64) -> u64 {
        let (eps_p, eps_c) = self.draw_correlated_pair();
        let _ = self.place.sample_ns_with_eps(eps_p, now_ns);
        self.cancel.sample_ns_with_eps(eps_c, now_ns)
    }

    /// **Push per-event anchor overrides** to the inner place /
    /// cancel samplers. The engine calls this on every Polymarket
    /// `Instrument` dispatch when `sim_rtt_mode = "exact"` resolved
    /// to a matching entry for the event's `event_secs`. Passing
    /// `None` for either side clears that side's override
    /// (falls back to the construction-time CDF).
    ///
    /// `p_*` is a `(p50, p85, p95, p99)` ms quartet — exactly what
    /// `per_event_rtt::EventRttOverride` exposes.
    pub fn set_per_event_anchors(
        &mut self,
        place: Option<(f64, f64, f64, f64)>,
        cancel: Option<(f64, f64, f64, f64)>,
        place_rate: Option<f64>,
        cancel_rate: Option<f64>,
        cap_ms: f64,
    ) {
        match place {
            Some((p50, p85, p95, p99)) => self.place.set_per_event_anchors(p50, p85, p95, p99, place_rate, cap_ms),
            None => self.place.clear_per_event_anchors(),
        }
        match cancel {
            Some((p50, p85, p95, p99)) => self.cancel.set_per_event_anchors(p50, p85, p95, p99, cancel_rate, cap_ms),
            None => self.cancel.clear_per_event_anchors(),
        }
    }

    /// **Push intra-event segmented anchors to both samplers**
    /// (2026-05-28). Each side gets either a segmented (early, late)
    /// pair OR `None` to fall back to aggregate / pooled. The engine
    /// passes whichever variant the parsed `EventRttOverride` has
    /// sufficient samples for — segmented when both buckets have
    /// ≥ MIN_SAMPLES, otherwise aggregate via `set_per_event_anchors`.
    ///
    /// `event_start_ns` is the unix-epoch nanoseconds of the event's
    /// start (5-min boundary). Sampler computes `time_in_event =
    /// now_ns − event_start_ns` to pick early vs late at draw time.
    pub fn set_per_event_segmented_anchors(
        &mut self,
        event_start_ns: u64,
        place: Option<((f64, f64, f64, f64), (f64, f64, f64, f64))>,
        cancel: Option<((f64, f64, f64, f64), (f64, f64, f64, f64))>,
        boundary_secs: u64,
        place_rate: Option<f64>,
        cancel_rate: Option<f64>,
        cap_ms: f64,
    ) {
        match place {
            Some((early, late)) => self.place.set_per_event_segmented_anchors(
                event_start_ns, early, late, boundary_secs, place_rate, cap_ms,
            ),
            None => self.place.clear_per_event_anchors(),
        }
        match cancel {
            Some((early, late)) => self.cancel.set_per_event_segmented_anchors(
                event_start_ns, early, late, boundary_secs, cancel_rate, cap_ms,
            ),
            None => self.cancel.clear_per_event_anchors(),
        }
    }

    /// Are either side's segmented overrides currently active?
    pub fn has_segmented_per_event_anchors(&self) -> bool {
        self.place.has_segmented_per_event_anchors()
            || self.cancel.has_segmented_per_event_anchors()
    }

    /// Accessor: are either side's per-event overrides currently
    /// active? Used by the engine's summary log.
    pub fn has_per_event_anchors(&self) -> bool {
        self.place.has_per_event_anchors() || self.cancel.has_per_event_anchors()
    }

    // ── Per-side per-event setters (no cross-clobber) ──────────────────
    // The two-sided `set_per_event_anchors` / `set_per_event_segmented_anchors`
    // above CLEAR a side passed `None`. That makes "one side segmented, the
    // other aggregate" impossible to express in two calls without the second
    // wiping the first — the partial-segmented clearing bug. These per-side
    // variants touch exactly one sampler, so a caller can set each side to its
    // own (segmented OR aggregate OR cleared) form independently.
    pub fn set_place_per_event_anchors(&mut self, p50: f64, p85: f64, p95: f64, p99: f64, rate: Option<f64>, cap_ms: f64) {
        self.place.set_per_event_anchors(p50, p85, p95, p99, rate, cap_ms);
    }
    pub fn set_cancel_per_event_anchors(&mut self, p50: f64, p85: f64, p95: f64, p99: f64, rate: Option<f64>, cap_ms: f64) {
        self.cancel.set_per_event_anchors(p50, p85, p95, p99, rate, cap_ms);
    }
    pub fn set_place_per_event_segmented_anchors(&mut self, event_start_ns: u64, early: (f64, f64, f64, f64), late: (f64, f64, f64, f64), boundary_secs: u64, rate: Option<f64>, cap_ms: f64) {
        self.place.set_per_event_segmented_anchors(event_start_ns, early, late, boundary_secs, rate, cap_ms);
    }
    pub fn set_cancel_per_event_segmented_anchors(&mut self, event_start_ns: u64, early: (f64, f64, f64, f64), late: (f64, f64, f64, f64), boundary_secs: u64, rate: Option<f64>, cap_ms: f64) {
        self.cancel.set_per_event_segmented_anchors(event_start_ns, early, late, boundary_secs, rate, cap_ms);
    }
    pub fn clear_place_per_event_anchors(&mut self) {
        self.place.clear_per_event_anchors();
    }
    pub fn clear_cancel_per_event_anchors(&mut self) {
        self.cancel.clear_per_event_anchors();
    }

    /// Generate one bivariate Gaussian draw with marginals N(0,1)
    /// and Pearson correlation `rho_cross`.
    ///
    ///   ξ, η iid N(0, 1)        (Box-Muller via the coupling RNG)
    ///   ε_p = ξ
    ///   ε_c = ρ·ξ + √(1 − ρ²)·η
    ///
    /// Both marginals are N(0, 1); corr(ε_p, ε_c) = ρ.
    #[inline]
    fn draw_correlated_pair(&mut self) -> (f64, f64) {
        let xi = standard_normal(&mut self.rng);
        if self.rho_cross == 0.0 {
            let eta = standard_normal(&mut self.rng);
            return (xi, eta);
        }
        let eta = standard_normal(&mut self.rng);
        let r = self.rho_cross;
        let eps_c = r * xi + (1.0 - r * r).sqrt() * eta;
        (xi, eps_c)
    }

    /// Borrow the place sampler's profile for logging.
    pub fn place_profile(&self) -> &LatencyProfile { self.place.profile() }
    /// Borrow the cancel sampler's profile for logging.
    pub fn cancel_profile(&self) -> &LatencyProfile { self.cancel.profile() }
    /// Configured cross-correlation. `0.0` reproduces independent
    /// samplers (subject to RNG sequence — the innovations still come
    /// from the coupling RNG, not each sampler's own).
    pub fn rho_cross(&self) -> f64 { self.rho_cross }
}

/// Standard-normal variate via Box-Muller. Avoids an extra crate (rand
/// 0.8 bundles uniforms but not Normal; rand_distr would be an extra
/// dep for ~20 lines of code).
pub(crate) fn standard_normal(rng: &mut StdRng) -> f64 {
    let u1: f64 = loop {
        let v: f64 = rng.gen();
        if v > 0.0 { break v; }
    };
    let u2: f64 = rng.gen();
    (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
}

/// Standard-normal CDF. Abramowitz & Stegun 26.2.17 approximation.
pub(crate) fn norm_cdf(x: f64) -> f64 {
    if x >= 8.0 { return 1.0; }
    if x <= -8.0 { return 0.0; }
    let t = 1.0 / (1.0 + 0.2316419 * x.abs());
    let d = 0.3989422804014327; // 1/sqrt(2π)
    let p = d * (-x * x / 2.0).exp()
        * (t * (0.319381530
            + t * (-0.356563782
                + t * (1.781477937
                    + t * (-1.821255978
                        + t * 1.330274429)))));
    if x >= 0.0 { 1.0 - p } else { p }
}

/// Piecewise-linear inverse-CDF evaluation over a 5-anchor table.
/// `anchors` must be sorted by cumulative probability. For `u`
/// outside the table's range, clamps to the first / last RTT.
fn inverse_cdf(anchors: &[(f64, f64); 7], u: f64) -> f64 {
    if u <= anchors[0].0 {
        return anchors[0].1;
    }
    if u >= anchors[anchors.len() - 1].0 {
        return anchors[anchors.len() - 1].1;
    }
    // Linear scan over 5 entries — branch-predictable, beats binary
    // search on this size.
    for i in 1..anchors.len() {
        let (p_hi, rtt_hi) = anchors[i];
        if u < p_hi {
            let (p_lo, rtt_lo) = anchors[i - 1];
            if p_hi <= p_lo {
                return rtt_lo;
            }
            let frac = (u - p_lo) / (p_hi - p_lo);
            return rtt_lo + frac * (rtt_hi - rtt_lo);
        }
    }
    anchors[anchors.len() - 1].1
}

#[cfg(test)]
mod tests {
    use super::*;

    fn live_calibrated() -> LatencyProfile {
        // Median of per-minute summaries on the 2026-04-27 live
        // session (place_order + cancel_order pooled). p99 is
        // extrapolated from (p50, p95) because live caps at 500 ms.
        LatencyProfile::Empirical {
            p50_ms: 60.0,
            p85_ms_override: None,
            p95_ms: 331.0,
            p99_ms: 700.0,
            rho: 0.0,
            p999_ms_override: None,
            gpd_tail: None,
        }
    }

    fn live_calibrated_ar1(rho: f64) -> LatencyProfile {
        let mut p = live_calibrated();
        if let LatencyProfile::Empirical { rho: r, .. } = &mut p {
            *r = rho;
        }
        p
    }

    /// Per-event override (2026-05-21): pushed anchors take priority
    /// over the construction-time CDF. After `clear_per_event_anchors`
    /// the sampler reverts to the original distribution.
    #[test]
    fn per_event_override_takes_priority_then_reverts() {
        let mut s = LatencySampler::new(live_calibrated(), 42);
        let n = 5_000;
        // Push a much tighter CDF: p50=10, p95=20, p99=30 ms.
        // `None` timeout rate → legacy lognormal-extrap tail (unchanged).
        s.set_per_event_anchors(10.0, 15.0, 20.0, 30.0, None, 2000.0);
        assert!(s.has_per_event_anchors());
        let mut rtts: Vec<f64> = (0..n)
            .map(|_| s.sample_ns(0) as f64 / 1_000_000.0).collect();
        rtts.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50 = rtts[(n as f64 * 0.50) as usize];
        let p95 = rtts[(n as f64 * 0.95) as usize];
        // ±15 % tolerance — short-window quantile fit is less tight
        // than the 20k empirical_iid_marginal test.
        assert!((p50 - 10.0).abs() / 10.0 < 0.15, "override p50: got {}", p50);
        assert!((p95 - 20.0).abs() / 20.0 < 0.15, "override p95: got {}", p95);

        // Clear → back to the (much wider) construction-time CDF.
        s.clear_per_event_anchors();
        assert!(!s.has_per_event_anchors());
        let mut rtts2: Vec<f64> = (0..n)
            .map(|_| s.sample_ns(0) as f64 / 1_000_000.0).collect();
        rtts2.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50_back = rtts2[(n as f64 * 0.50) as usize];
        // live_calibrated has p50=60ms — should be much wider than 15.
        assert!(p50_back > 25.0, "post-clear p50 should revert to ~60ms, got {}", p50_back);
    }

    /// **Per-event exceedance tail** — supplying a timeout rate makes that
    /// fraction of draws exceed the cap (the right-censored timeouts the
    /// non-timeout body quantiles can't produce). Body anchors sit far
    /// below the 2000 ms cap, so without the rate ~nothing exceeds it.
    #[test]
    fn per_event_timeout_rate_produces_exceedance_at_cap() {
        let cap = 2000.0;
        let rate = 0.05;
        // With rate: ~5 % of draws must land past the cap.
        let mut s = LatencySampler::new(live_calibrated(), 99); // rho=0 → iid draws
        s.set_per_event_anchors(60.0, 150.0, 300.0, 600.0, Some(rate), cap);
        let n = 50_000u64;
        let over = (0..n)
            .filter(|i| s.sample_ns(i * 1_000_000) as f64 / 1_000_000.0 > cap)
            .count();
        let frac = over as f64 / n as f64;
        assert!((frac - rate).abs() < 0.01, "exceedance {frac} should be ≈ {rate}");

        // Same body, rate=None → legacy lognormal tail: essentially nothing
        // past the cap (lognormal p9999 from p95=300,p99=600 only just grazes
        // 2000 ms in the top ~0.05 %).
        let mut s0 = LatencySampler::new(live_calibrated(), 99);
        s0.set_per_event_anchors(60.0, 150.0, 300.0, 600.0, None, cap);
        let over0 = (0..n)
            .filter(|i| s0.sample_ns(i * 1_000_000) as f64 / 1_000_000.0 > cap)
            .count();
        let frac0 = over0 as f64 / n as f64;
        assert!(frac0 < 0.005, "legacy-tail exceedance {frac0} should be ≈ 0");
    }

    /// **Intra-event segmented override** (2026-05-28). The sampler
    /// picks `early` for `now_ns - event_start_ns < boundary_ns`, else
    /// `late`. Verifies the marginal RTT distribution shifts at the
    /// boundary AND the per-event override takes priority.
    #[test]
    fn per_event_segmented_picks_early_vs_late_by_time_in_event() {
        let mut s = LatencySampler::new(live_calibrated(), 42);
        let n = 3_000;
        let event_start_ns: u64 = 1_000_000_000_000;  // arbitrary
        let boundary_secs: u64 = 60;
        // Early bucket: very fast CDF (~10 ms body); late bucket:
        // 5× slower (~50 ms body). The 5× spread between buckets is
        // typical of the high-vol-event front-loaded burst empirics.
        s.set_per_event_segmented_anchors(
            event_start_ns,
            /*early=*/ (10.0, 15.0, 20.0, 30.0),
            /*late =*/ (50.0, 75.0, 100.0, 150.0),
            boundary_secs,
            None, 2000.0,
        );
        assert!(s.has_segmented_per_event_anchors());
        assert!(s.has_per_event_anchors());

        // Sample at offset 10 s into event (well inside early bucket).
        let now_early_ns = event_start_ns + 10 * 1_000_000_000;
        let mut early_rtts: Vec<f64> = (0..n)
            .map(|_| s.sample_ns(now_early_ns) as f64 / 1_000_000.0)
            .collect();
        early_rtts.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50_early = early_rtts[(n as f64 * 0.50) as usize];
        assert!(
            p50_early < 25.0,
            "early-bucket p50 must reflect 10ms anchor, got {} ms", p50_early,
        );

        // Sample at offset 180 s (deep in late bucket).
        let now_late_ns = event_start_ns + 180 * 1_000_000_000;
        let mut late_rtts: Vec<f64> = (0..n)
            .map(|_| s.sample_ns(now_late_ns) as f64 / 1_000_000.0)
            .collect();
        late_rtts.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50_late = late_rtts[(n as f64 * 0.50) as usize];
        assert!(
            p50_late > 30.0 && p50_late < 80.0,
            "late-bucket p50 must reflect 50ms anchor, got {} ms", p50_late,
        );

        // Late distinctly higher than early.
        assert!(
            p50_late > p50_early * 1.5,
            "late RTT should be substantially > early (got early={}, late={})",
            p50_early, p50_late,
        );
    }

    /// Boundary semantics: the segmented sampler uses `early` for
    /// `time_in_event STRICTLY LESS THAN boundary` and `late` from the
    /// boundary onward. Verifies the swap fires at exactly the right
    /// offset.
    #[test]
    fn per_event_segmented_boundary_is_inclusive_of_late() {
        let mut s = LatencySampler::new(live_calibrated(), 7);
        let event_start_ns: u64 = 500_000_000_000;
        let boundary_secs: u64 = 60;
        // Extreme separation so a single sample makes the bucket
        // obvious: early = 5 ms fixed-ish, late = 200 ms fixed-ish.
        s.set_per_event_segmented_anchors(
            event_start_ns,
            (5.0, 5.0, 5.0, 5.0),
            (200.0, 200.0, 200.0, 200.0),
            boundary_secs,
            None, 2000.0,
        );
        // At exactly boundary - 1 ns: still early.
        let just_before = event_start_ns + boundary_secs * 1_000_000_000 - 1;
        let r_before = s.sample_ns(just_before) as f64 / 1_000_000.0;
        assert!(r_before < 50.0, "just-before-boundary must be early, got {} ms", r_before);
        // At exactly boundary ns: now late.
        let at_boundary = event_start_ns + boundary_secs * 1_000_000_000;
        let r_at = s.sample_ns(at_boundary) as f64 / 1_000_000.0;
        assert!(r_at > 50.0, "at-boundary must be late, got {} ms", r_at);
    }

    /// **Coupled-sampler segmented path** (2026-05-28). Verifies the
    /// coupled push of segmented anchors propagates to both place and
    /// cancel sides, AND that the pre-existing aggregate-CDF override
    /// is correctly cleared so the segmented variant takes over.
    #[test]
    fn coupled_segmented_per_event_anchors_apply_to_both_sides() {
        let place_s  = LatencySampler::new(live_calibrated(), 11);
        let cancel_s = LatencySampler::new(live_calibrated(), 23);
        let mut coup = CoupledLatencySamplers::new(place_s, cancel_s, 0.0, 1);
        let event_start_ns: u64 = 2_000_000_000_000;
        coup.set_per_event_segmented_anchors(
            event_start_ns,
            Some(((10.0, 15.0, 20.0, 30.0), (50.0, 75.0, 100.0, 150.0))),
            Some(((10.0, 15.0, 20.0, 30.0), (50.0, 75.0, 100.0, 150.0))),
            60,
            None, None, 2000.0,
        );
        assert!(coup.has_segmented_per_event_anchors());

        // Draw 2000 samples at offset 5 s (early). Place + cancel
        // alternate but each side gets ~1000 samples.
        let now = event_start_ns + 5 * 1_000_000_000;
        let mut p_rtts = Vec::new();
        let mut c_rtts = Vec::new();
        for _ in 0..1_000 {
            p_rtts.push(coup.sample_place(now) as f64 / 1_000_000.0);
            c_rtts.push(coup.sample_cancel(now) as f64 / 1_000_000.0);
        }
        p_rtts.sort_by(|a, b| a.partial_cmp(b).unwrap());
        c_rtts.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50_p = p_rtts[500];
        let p50_c = c_rtts[500];
        assert!(p50_p < 25.0, "place early p50 must be ≤ 25ms, got {}", p50_p);
        assert!(p50_c < 25.0, "cancel early p50 must be ≤ 25ms, got {}", p50_c);
    }

    /// **Per-side setters don't clobber each other** (partial-segmented
    /// fix). Setting place to a fast SEGMENTED CDF and cancel to a slow
    /// AGGREGATE CDF must leave BOTH overrides intact — the old two-sided
    /// layering wiped whichever side the second call passed `None` for.
    #[test]
    fn per_side_per_event_setters_do_not_clobber() {
        let place_s  = LatencySampler::new(live_calibrated(), 11); // pooled p50=60
        let cancel_s = LatencySampler::new(live_calibrated(), 23);
        let mut coup = CoupledLatencySamplers::new(place_s, cancel_s, 0.0, 1);
        let start: u64 = 3_000_000_000_000;
        // place: fast segmented (~10-30ms both buckets); cancel: slow aggregate (~200ms).
        coup.set_place_per_event_segmented_anchors(start, (10.0, 15.0, 20.0, 30.0), (10.0, 15.0, 20.0, 30.0), 60, None, 2000.0);
        coup.set_cancel_per_event_anchors(200.0, 250.0, 300.0, 400.0, None, 2000.0);
        let now = start + 5 * 1_000_000_000;
        let mut p = Vec::new();
        let mut c = Vec::new();
        for _ in 0..2_000 {
            p.push(coup.sample_place(now) as f64 / 1_000_000.0);
            c.push(coup.sample_cancel(now) as f64 / 1_000_000.0);
        }
        p.sort_by(|a, b| a.partial_cmp(b).unwrap());
        c.sort_by(|a, b| a.partial_cmp(b).unwrap());
        // If either side were clobbered it would revert to the pooled p50=60ms.
        assert!(p[1000] < 25.0, "place per-event survived (got p50={}ms)", p[1000]);
        assert!(c[1000] > 120.0 && c[1000] < 320.0, "cancel per-event survived (got p50={}ms)", c[1000]);
    }

    /// 5-anchor empirical CDF: percentiles match the configured
    /// (p50, p95, p99) within ±10 % over n=20k samples.
    #[test]
    fn empirical_iid_marginal_matches_anchors() {
        let mut s = LatencySampler::new(live_calibrated(), 42);
        let n = 20_000;
        let mut rtts: Vec<f64> = (0..n)
            .map(|_| s.sample_ns(0) as f64 / 1_000_000.0) // ×2 → RTT
            .collect();
        rtts.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let pick = |q: f64| rtts[((n as f64) * q) as usize];

        let p50 = pick(0.50);
        let p95 = pick(0.95);
        let p99 = pick(0.99);

        assert!((p50 - 60.0).abs() / 60.0 < 0.10, "p50: got {} expected 60", p50);
        assert!((p95 - 331.0).abs() / 331.0 < 0.10, "p95: got {} expected 331", p95);
        assert!((p99 - 700.0).abs() / 700.0 < 0.20, "p99: got {} expected 700", p99);
    }

    /// AR(1) ρ=0.85 should preserve the marginal AND produce a
    /// log-space lag-1 autocorr in roughly the configured range.
    #[test]
    fn ar1_preserves_marginal_and_adds_correlation() {
        let mut s = LatencySampler::new(live_calibrated_ar1(0.85), 42);
        let n = 20_000;
        let samples: Vec<f64> = (0..n)
            .map(|_| s.sample_ns(0) as f64 / 1_000_000.0)
            .collect();

        let mut sorted = samples.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let pick = |q: f64| sorted[((n as f64) * q) as usize];
        let p50 = pick(0.50);
        let p95 = pick(0.95);
        assert!((p50 - 60.0).abs() / 60.0 < 0.15);
        assert!((p95 - 331.0).abs() / 331.0 < 0.15);

        let logs: Vec<f64> = samples.iter().map(|v| v.ln()).collect();
        let m: f64 = logs.iter().sum::<f64>() / n as f64;
        let var: f64 = logs.iter().map(|v| (v - m).powi(2)).sum::<f64>();
        let cov: f64 = (0..n - 1).map(|i| (logs[i] - m) * (logs[i + 1] - m)).sum::<f64>();
        let ac1 = cov / var;
        assert!(
            ac1 > 0.5 && ac1 < 0.95,
            "lag-1 autocorr should be clearly positive, got {}", ac1,
        );
    }

    /// ρ=0 should produce ~zero lag-1 autocorr.
    #[test]
    fn iid_lag1_is_zero() {
        let mut s = LatencySampler::new(live_calibrated_ar1(0.0), 42);
        let n = 20_000;
        let samples: Vec<f64> = (0..n)
            .map(|_| s.sample_ns(0) as f64 / 1_000_000.0)
            .collect();
        let logs: Vec<f64> = samples.iter().map(|v| v.ln()).collect();
        let m: f64 = logs.iter().sum::<f64>() / n as f64;
        let var: f64 = logs.iter().map(|v| (v - m).powi(2)).sum::<f64>();
        let cov: f64 = (0..n - 1).map(|i| (logs[i] - m) * (logs[i + 1] - m)).sum::<f64>();
        let ac0 = cov / var;
        assert!(ac0.abs() < 0.05, "iid lag-1 should be ~0, got {}", ac0);
    }

    /// HIGH-state run-length: ρ=0.85 should cluster top-10 % samples
    /// into runs of mean length ≥ 2.5 (vs ~1.1 iid; live empirical 4.6).
    #[test]
    fn ar1_produces_high_state_clusters() {
        let mut s = LatencySampler::new(live_calibrated_ar1(0.85), 123);
        let n = 20_000;
        let samples_rtt: Vec<f64> = (0..n)
            .map(|_| s.sample_ns(0) as f64 / 1_000_000.0)
            .collect();
        let mut sorted = samples_rtt.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let threshold = sorted[(n as f64 * 0.90) as usize];
        let states: Vec<bool> = samples_rtt.iter().map(|v| *v >= threshold).collect();
        let mut runs: Vec<usize> = Vec::new();
        let mut i = 0;
        while i < states.len() {
            if states[i] {
                let mut j = i;
                while j < states.len() && states[j] { j += 1; }
                runs.push(j - i);
                i = j;
            } else {
                i += 1;
            }
        }
        let mean_run: f64 = runs.iter().map(|&r| r as f64).sum::<f64>() / runs.len() as f64;
        assert!(
            mean_run >= 2.5,
            "ρ=0.85 should cluster HIGH states (mean run ≥ 2.5), got {}", mean_run,
        );
    }

    #[test]
    fn fixed_profile_returns_constant() {
        let mut s = LatencySampler::fixed(42);
        for _ in 0..10 {
            assert_eq!(s.sample_ns(0), 42 * 1_000_000);
        }
    }

    /// The p85 body anchor: when `Some`, sits at index 2 in the
    /// 7-anchor table; when `None`, the function synthesises p85 by
    /// linear interpolation in (probability, RTT) space between
    /// (0.50, p50) and (0.95, p95) — this reproduces the legacy
    /// 6-anchor numerical curve.
    #[test]
    fn empirical_p85_anchor_override_and_interp() {
        // Explicit p85 override → anchor pinned exactly.
        let anchors = LatencyProfile::empirical_anchors(60.0, Some(400.0), 331.0, 700.0, None, None);
        // p85 override (400) is above p95 (331), so the clamp kicks
        // in: p85 must be < p95. Verify monotonicity holds.
        assert!(anchors[2].1 < anchors[3].1, "p85 < p95 after clamp");

        // Override that's in the valid range — used verbatim.
        let anchors = LatencyProfile::empirical_anchors(60.0, Some(250.0), 331.0, 700.0, None, None);
        assert!((anchors[2].1 - 250.0).abs() < 1e-6, "p85 override honoured: {}", anchors[2].1);
        assert_eq!(anchors[2].0, 0.850, "p85 sits at u=0.85");

        // No override → linear interp between (0.50, 60) and (0.95, 331).
        // Expected: 60 + (0.85 - 0.50)/(0.95 - 0.50) * (331 - 60)
        //         = 60 + 0.7778 * 271 ≈ 270.78.
        let anchors = LatencyProfile::empirical_anchors(60.0, None, 331.0, 700.0, None, None);
        let interp = 60.0 + (0.35 / 0.45) * (331.0 - 60.0);
        assert!((anchors[2].1 - interp).abs() < 1e-6, "p85 interp: got {} expected {}", anchors[2].1, interp);

        // Monotonicity over the whole 7-anchor table.
        for w in anchors.windows(2) {
            assert!(w[0].0 < w[1].0, "probs sorted: {:?}", w);
            assert!(w[0].1 < w[1].1, "rtts sorted: {:?}", w);
        }
    }

    /// Tail-extrapolation: with (p50=60, p95=331, p99=700) the
    /// p99.9 anchor should land near 1620 ms — log-slope from p95→p99
    /// continued past p99 (factor 1.1213 ≈ (Φ⁻¹(0.999)−Φ⁻¹(0.99)) /
    /// (Φ⁻¹(0.99)−Φ⁻¹(0.95))). Closed form:
    ///   ln(p99.9) = ln(700) + 1.1213·(ln(700)−ln(331)) ≈ 7.39
    ///   p99.9     ≈ exp(7.39) ≈ 1621 ms
    #[test]
    fn empirical_p99p9_extrapolation_matches_log_slope() {
        let anchors = LatencyProfile::empirical_anchors(60.0, None, 331.0, 700.0, None, None);
        // Anchor indices after 7-anchor refactor: [floor, p50, p85, p95, p99, p999, p9999].
        let p999 = anchors[5].1;
        assert!(
            p999 > 1500.0 && p999 < 1750.0,
            "p99.9 extrapolation out of expected band [1500, 1750]: got {}", p999,
        );
        // Also: anchors are monotonic.
        for w in anchors.windows(2) {
            assert!(w[0].0 < w[1].0, "anchors must be probability-sorted");
            assert!(w[0].1 < w[1].1, "anchors must be RTT-sorted");
        }
    }

    /// `describe()` reports the configured (p50, p95, p99) directly.
    #[test]
    fn describe_empirical_emits_anchors() {
        let p = LatencyProfile::Empirical {
            p50_ms: 60.0, p85_ms_override: None, p95_ms: 331.0, p99_ms: 700.0, rho: 0.95,
            p999_ms_override: None, gpd_tail: None,
        };
        let s = p.describe();
        assert!(s.contains("p50=60"), "describe missing p50: {}", s);
        assert!(s.contains("p95=331"), "describe missing p95: {}", s);
        assert!(s.contains("p99=700"), "describe missing p99: {}", s);
        assert!(s.contains("ρ=0.95"), "describe missing ρ: {}", s);
    }

    /// `parse_ms_after` handles the unit suffixes the live latency
    /// dump emits (`123.4ms`, `1.23s`, bare numerics).
    #[test]
    fn parse_ms_after_handles_units() {
        let line = "  [latency] foo n=10  p50=88.34ms p85=205.5ms p95=500.96ms p99=1.23s p99.9=502.01ms";
        assert_eq!(parse_ms_after(line, "p50="), Some(88.34));
        assert_eq!(parse_ms_after(line, "p85="), Some(205.5));
        assert_eq!(parse_ms_after(line, "p95="), Some(500.96));
        assert_eq!(parse_ms_after(line, "p99="), Some(1230.0));
        assert_eq!(parse_ms_after(line, "p99.9="), Some(502.01));
        assert_eq!(parse_ms_after(line, "missing="), None);
    }

    /// `calibrate_from_log` populates `SidedParams.p85_ms` from `p85=`
    /// rows when present, and leaves it `None` for older logs that
    /// don't emit p85. Mixed-version logs (some rows with, some
    /// without) only count rows that carry `p85=` toward the median.
    #[test]
    fn calibrate_from_log_p85_present_and_absent() {
        let tmp = std::env::temp_dir().join(format!("test_calibrate_p85_{}.log", std::process::id()));
        let path = tmp.to_str().unwrap();
        std::fs::write(
            path,
            "\
2026-05-15T00:00:01.000Z  INFO hexbot::latency: [latency] polymarket.http.place_order  n=100 p50=50.0ms p85=300.0ms p95=400.0ms p99=600.0ms\n\
2026-05-15T00:01:01.000Z  INFO hexbot::latency: [latency] polymarket.http.place_order  n=100 p50=60.0ms p85=350.0ms p95=420.0ms p99=620.0ms\n\
2026-05-15T00:02:01.000Z  INFO hexbot::latency: [latency] polymarket.http.cancel_order n=100 p50=40.0ms p95=300.0ms p99=500.0ms\n\
2026-05-15T00:03:01.000Z  INFO hexbot::latency: [latency] polymarket.http.cancel_order n=100 p50=45.0ms p95=310.0ms p99=520.0ms\n\
",
        ).unwrap();
        let cal = calibrate_from_log(path).expect("parse OK");
        let _ = std::fs::remove_file(path);

        // Place rows carry p85 → expect median(300, 350) = 325.
        let p85 = cal.place.p85_ms.expect("place has p85 rows");
        assert!((p85 - 325.0).abs() < 1e-9, "place p85 median: {}", p85);

        // Cancel rows have no p85 → None.
        assert!(cal.cancel.p85_ms.is_none(), "cancel: no p85 emitted");

        // Pooled inherits both sides' p85s — only place contributes,
        // so pooled p85 = same as place.
        let p85_pool = cal.pooled.p85_ms.expect("pooled has place's p85");
        assert!((p85_pool - 325.0).abs() < 1e-9, "pooled p85: {}", p85_pool);
    }

    /// Direct test of `solve_anchors_from_raw_rtts`: synthetic
    /// known distribution → recover its pooled quantiles within
    /// sampling tolerance. The legacy median-of-per-minute-percentile
    /// path would bias upper quantiles by 20–30 % on this kind of
    /// data; the pooled-raw path should match the population
    /// quantiles directly.
    #[test]
    fn pooled_raw_solver_recovers_pooled_quantiles() {
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};
        let mut rng = StdRng::seed_from_u64(123);
        // Lognormal with μ_log=ln(100), σ_log=1.5 → heavy-tailed
        // distribution roughly matching live's place body.
        // p50 ≈ 100, p95 ≈ 1186, p99 ≈ 3300.
        let mu_log = (100.0_f64).ln();
        let sigma_log: f64 = 1.5;
        let n = 5000;
        let cap: f64 = 2000.0;
        let mut raws = Vec::new();
        let mut n_timeouts: u64 = 0;
        for _ in 0..n {
            let z: f64 = standard_normal(&mut rng);
            let x = (mu_log + sigma_log * z).exp();
            if x >= cap { n_timeouts += 1; } else { raws.push(x); }
        }
        let params = solve_anchors_from_raw_rtts(
            &raws, n_timeouts, cap,
            0, n as u64,
            n_timeouts as f64 / n as f64,
        );
        // Sample-size tolerance ~5 % for p50, ~10 % for p95, ~25 %
        // for p99 (in the tail-or-censored region).
        let expected_p50 = (mu_log + 0.0_f64).exp(); // = 100
        let expected_p95 = (mu_log + sigma_log * 1.6449).exp();
        assert!(
            (params.p50_ms - expected_p50).abs() / expected_p50 < 0.08,
            "p50: got {:.1}, expected ~{:.1}", params.p50_ms, expected_p50,
        );
        assert!(
            (params.p95_ms - expected_p95).abs() / expected_p95 < 0.15,
            "p95: got {:.1}, expected ~{:.1}", params.p95_ms, expected_p95,
        );
        assert!(params.p85_ms.is_some());
        assert!(
            params.calibration_method.starts_with("raw RTT pooled"),
            "tag should mention raw path: {}", params.calibration_method,
        );
    }

    /// Insufficient raw samples → fallback to `solve_bucket`.
    /// Verifies the calibration_method tag doesn't claim raw-path.
    #[test]
    fn pooled_raw_solver_falls_back_when_sparse() {
        // < 100 raw samples → calibrate_from_log uses solve_side
        // which routes to legacy median path. We test the threshold
        // boundary through the integration test below by feeding a
        // log with very few `[PolymarketTrade] Submit/Order accepted`
        // pairs and verifying `calibration_method` doesn't claim
        // raw-path.
        let tmp = std::env::temp_dir().join(format!("test_sparse_{}.log", std::process::id()));
        let path = tmp.to_str().unwrap();
        // Just one `[latency]` row per side, no Submit/accepted pairs.
        std::fs::write(path,
            "\
2026-04-27T14:46:01.000Z  INFO hexbot::latency: [latency] polymarket.http.place_order  n=200 p50=50.0ms p95=200.0ms p99=400.0ms p99.9=400.0ms max=400.0ms\n\
2026-04-27T14:47:01.000Z  INFO hexbot::latency: [latency] polymarket.http.cancel_order n=200 p50=40.0ms p95=180.0ms p99=350.0ms p99.9=350.0ms max=350.0ms\n\
2026-04-27T14:48:01.000Z  INFO hexbot::latency: [latency] polymarket.http.place_order  n=200 p50=55.0ms p95=210.0ms p99=420.0ms p99.9=420.0ms max=420.0ms\n\
2026-04-27T14:49:01.000Z  INFO hexbot::latency: [latency] polymarket.http.cancel_order n=200 p50=45.0ms p95=190.0ms p99=370.0ms p99.9=370.0ms max=370.0ms\n",
        ).unwrap();
        let cal = calibrate_from_log(path).expect("calibrate succeeded");
        assert!(
            !cal.place.calibration_method.starts_with("raw RTT pooled"),
            "place sparse log should fall back to legacy: got tag '{}'",
            cal.place.calibration_method,
        );
        let _ = std::fs::remove_file(path);
    }

    /// `calibrate_from_log` end-to-end on a synthetic mini-log with
    /// no cap-hit events. Falls back to median-of-percentiles with
    /// censorship-aware p99 extrapolation.
    #[test]
    fn calibrate_from_log_no_timeouts_uses_medians() {
        let tmp = std::env::temp_dir().join(format!("test_calibrate_{}.log", std::process::id()));
        let path = tmp.to_str().unwrap();
        std::fs::write(
            path,
            "\
2026-04-27T14:46:01.000Z  INFO hexbot::latency: [latency] polymarket.signer.sign  n=181 p50=10.0ms p95=20.0ms p99=30.0ms\n\
2026-04-27T14:46:01.000Z  INFO hexbot::latency: [latency] polymarket.http.place_order  n=181 p50=50.0ms p95=200.0ms p99=400.0ms p99.9=500.0ms max=500.0ms\n\
2026-04-27T14:47:01.000Z  INFO hexbot::latency: [latency] polymarket.http.cancel_order n=170 p50=70.0ms p95=350.0ms p99=600.0ms p99.9=900.0ms max=900.0ms\n\
2026-04-27T14:48:01.000Z  INFO hexbot::latency: [latency] polymarket.http.place_order  n=200 p50=90.0ms p95=400.0ms p99=700.0ms p99.9=1000.0ms max=1000.0ms\n\
",
        ).unwrap();

        let cal = calibrate_from_log(path).expect("parse OK");
        // Pooled view (legacy single-knob semantics):
        assert_eq!(cal.pooled.n_rows, 3);
        assert_eq!(cal.pooled.n_samples, 181 + 170 + 200);
        assert_eq!(cal.pooled.n_timeouts, 0);
        assert!(cal.pooled.cap_hit_rate < 1e-9, "no timeouts → cap_rate ≈ 0");
        assert!(cal.pooled.calibration_method.contains("medians"));
        // Sorted p50: 50, 70, 90 → median 70.
        assert!((cal.pooled.p50_ms - 70.0).abs() < 1e-9, "p50 median: {}", cal.pooled.p50_ms);
        // Sorted p95: 200, 350, 400 → median 350.
        assert!((cal.pooled.p95_ms - 350.0).abs() < 1e-9, "p95 median: {}", cal.pooled.p95_ms);
        // Sorted p99: 400, 600, 700 → median 600 ≥ 480 → extrapolated.
        // σ = ln(350/70)/1.6449 ≈ 0.978; p99 ≈ 70·exp(0.978·2.326) ≈ 685.
        assert!(
            cal.pooled.p99_ms >= 600.0 && cal.pooled.p99_ms < 800.0,
            "p99 extrapolation out of band: {}", cal.pooled.p99_ms,
        );
        // Per-side split: 2 place rows (50, 90 → median 70), 1 cancel row.
        assert_eq!(cal.place.n_rows, 2);
        assert_eq!(cal.cancel.n_rows, 1);
        assert!((cal.place.p50_ms - 70.0).abs() < 1e-9);
        assert!((cal.cancel.p50_ms - 70.0).abs() < 1e-9);

        let _ = std::fs::remove_file(path);
    }

    /// `calibrate_from_log` with an uncensored p99 (well below the
    /// 480 ms cap detection threshold) should NOT extrapolate.
    #[test]
    fn calibrate_from_log_uncensored_p99_passthrough() {
        let tmp = std::env::temp_dir().join(format!("test_calibrate_uc_{}.log", std::process::id()));
        let path = tmp.to_str().unwrap();
        std::fs::write(
            path,
            "\
2026-04-27T14:46:01.000Z  INFO hexbot::latency: [latency] polymarket.http.place_order n=100 p50=20.0ms p95=80.0ms p99=150.0ms\n\
2026-04-27T14:47:01.000Z  INFO hexbot::latency: [latency] polymarket.http.place_order n=100 p50=25.0ms p95=90.0ms p99=200.0ms\n\
",
        ).unwrap();
        let cal = calibrate_from_log(path).expect("parse OK");
        assert_eq!(cal.pooled.n_timeouts, 0);
        assert!(cal.pooled.calibration_method.contains("medians"));
        // Median of [150, 200] = 175 — uncensored, passed through.
        assert!((cal.pooled.p99_ms - 175.0).abs() < 1e-9, "p99 median: {}", cal.pooled.p99_ms);
        // Zero timeouts → no p999 override (sampler will lognormal-extrapolate).
        assert_eq!(cal.pooled.p999_ms_override, None);
        // Place-only sample, cancel side empty.
        assert_eq!(cal.place.n_rows, 2);
        assert_eq!(cal.cancel.n_rows, 0);
        assert_eq!(cal.cancel.calibration_method, "no rows");
        let _ = std::fs::remove_file(path);
    }

    /// Small but non-zero cap-rate path: the cap-rate-anchored p99.9
    /// override must be set so the sampler doesn't run away on the
    /// lognormal extrapolation. Concrete check against the live2.log
    /// 2026-04-29 numbers — observed (p50=23, p95=63, p99=287) with
    /// 24/49754 timeouts (cap rate 0.048 %) → solved p99.9 ≈ 488 ms,
    /// vs the lognormal extrap ≈ 1571 ms (which over-models the cap
    /// rate by ~17×).
    #[test]
    fn calibrate_from_log_small_cap_rate_anchors_p999_from_cap_rate() {
        let tmp = std::env::temp_dir().join(format!("test_cal_small_{}.log", std::process::id()));
        let path = tmp.to_str().unwrap();
        let mut content = String::new();
        // 200 rows × n=250 ≈ 50000 samples — matches live2.log scale.
        for _ in 0..200 {
            content.push_str(
                "2026-04-29T03:46:01.000Z  INFO hexbot::latency: [latency] polymarket.http.place_order  n=250 p50=23.0ms p95=63.0ms p99=287.0ms\n",
            );
        }
        // 24 cap-hit events → cap_rate = 0.048 %.
        for _ in 0..24 {
            content.push_str(
                "2026-04-29T03:46:02.000Z  WARN hexbot::exchange::polymarket::trade: [PolymarketTrade] Order unknown state (timeout) coid=1 oid=0xabc → NewOrderTimeout\n",
            );
        }
        std::fs::write(path, &content).unwrap();

        let cal = calibrate_from_log(path).expect("parse OK");
        let s = cal.place;
        assert_eq!(s.n_samples, 50_000);
        assert_eq!(s.n_timeouts, 24);
        assert!((s.cap_hit_rate - 24.0 / 50000.0).abs() < 1e-9);
        assert_eq!(s.calibration_method, "medians (cap-rate p99.9 solved)");
        // p50, p95, p99 stay at the observed medians.
        assert!((s.p50_ms - 23.0).abs() < 1e-9);
        assert!((s.p95_ms - 63.0).abs() < 1e-9);
        assert!((s.p99_ms - 287.0).abs() < 1e-9);
        // p999 solved from cap-rate constraint:
        //   target_F500 = 0.99952; denom = 0.00952
        //   p999 = 287 + (500−287) · 0.009 / 0.00952 ≈ 287 + 201.4 ≈ 488.4
        let p999 = s.p999_ms_override.expect("override should be set");
        assert!(
            (p999 - 488.4).abs() < 5.0,
            "p999 from cap rate: got {}, expected ≈ 488", p999,
        );
        // Sanity: putting these anchors through `empirical_anchors`
        // produces a p99.9 that matches the override (override beats
        // the lognormal extrap).
        let anchors = LatencyProfile::empirical_anchors(s.p50_ms, s.p85_ms, s.p95_ms, s.p99_ms, s.p999_ms_override, s.gpd_tail);
        // Index 5 is the p99.9 anchor in the 7-anchor table.
        assert!((anchors[5].1 - p999).abs() < 1e-9);

        let _ = std::fs::remove_file(path);
    }

    /// Cap-rate-driven calibration with HIGH cap rate (≥ 5 %).
    /// 500 ms anchor falls in the [p50, p95] segment, so p95 should
    /// be solved against the rate. Synthetic log: 50 latency rows
    /// summing to 5000 samples, plus 400 timeout events → 8 % cap
    /// rate.
    #[test]
    fn calibrate_from_log_cap_rate_high_solves_p95() {
        let tmp = std::env::temp_dir().join(format!("test_cal_high_{}.log", std::process::id()));
        let path = tmp.to_str().unwrap();
        let mut content = String::new();
        // 50 rows of place_order, n=100 each → 5000 samples.
        // Use percentile values that we DON'T want the calibrator
        // to use directly (cap-rate path should override them).
        for _ in 0..50 {
            content.push_str(
                "2026-04-27T14:46:01.000Z  INFO hexbot::latency: [latency] polymarket.http.place_order  n=100 p50=60.0ms p95=300.0ms p99=500.0ms p99.9=500.0ms max=500.0ms\n",
            );
        }
        // 400 cap-hit events. Pattern matches the live emitter.
        for _ in 0..400 {
            content.push_str(
                "2026-04-27T14:46:02.000Z  WARN hexbot::exchange::polymarket::trade: [PolymarketTrade] Order unknown state (timeout) coid=1 oid=0xabc → NewOrderTimeout\n",
            );
        }
        std::fs::write(path, &content).unwrap();

        let cal = calibrate_from_log(path).expect("parse OK");
        // All rows are place_order + all timeouts are NewOrderTimeout
        // → place side carries the full signal; cancel side empty.
        assert_eq!(cal.place.n_samples, 5000);
        assert_eq!(cal.place.n_timeouts, 400);
        assert!((cal.place.cap_hit_rate - 0.08).abs() < 1e-9);
        assert_eq!(cal.place.calibration_method, "cap-rate (p95 solved)");
        assert_eq!(cal.cancel.n_rows, 0);
        // target_F500 = 0.92.  In [p50, p95] segment:
        //   0.5 + (500 − 60)/(p95 − 60) · 0.45 = 0.92
        //   p95 = 60 + 440 · 0.45 / 0.42 = 60 + 471.4 = 531.4
        assert!(
            (cal.place.p95_ms - 531.4).abs() < 2.0,
            "p95 from cap rate: got {}, expected ≈ 531", cal.place.p95_ms,
        );
        // Sanity: F(500) at solved anchors should reproduce 0.92.
        let f500 = 0.5 + (500.0 - cal.place.p50_ms) / (cal.place.p95_ms - cal.place.p50_ms) * 0.45;
        assert!((f500 - 0.92).abs() < 0.01, "F(500) regression: {}", f500);

        let _ = std::fs::remove_file(path);
    }

    /// Cap-rate-driven calibration with MODERATE cap rate (1–5 %).
    /// 500 ms anchor falls in the [p95, p99] segment so p99 is solved
    /// while p95 stays at the median.
    #[test]
    fn calibrate_from_log_cap_rate_moderate_solves_p99() {
        let tmp = std::env::temp_dir().join(format!("test_cal_mod_{}.log", std::process::id()));
        let path = tmp.to_str().unwrap();
        let mut content = String::new();
        // 50 rows × n=100 = 5000 samples.
        for _ in 0..50 {
            content.push_str(
                "2026-04-27T14:46:01.000Z  INFO hexbot::latency: [latency] polymarket.http.place_order  n=100 p50=62.0ms p95=306.0ms p99=501.0ms\n",
            );
        }
        // 200 cap-hits → 4 % cap rate.
        for _ in 0..200 {
            content.push_str(
                "2026-04-27T14:46:02.000Z  WARN hexbot::exchange::polymarket::trade: [PolymarketTrade] Cancel unknown state (timeout) coid=1 → CancelOrderTimeout\n",
            );
        }
        std::fs::write(path, &content).unwrap();

        let cal = calibrate_from_log(path).expect("parse OK");
        // Place rows + cancel timeouts → split arrives at:
        //   place : n_rows=50  n_timeouts=0   → median path (no cap-rate solve)
        //   cancel: n_rows=0   n_timeouts=200 → "no rows" (caller falls back)
        //   pooled: n_samples=5000 n_timeouts=200 → 4 % cap rate, p99-solved
        // Validate via the pooled view since the legacy single-knob
        // semantics is what this fixture was designed against.
        assert!((cal.pooled.cap_hit_rate - 0.04).abs() < 1e-9);
        assert_eq!(cal.pooled.calibration_method, "cap-rate (p99 solved)");
        // p95 stays at the median (306 ms — uniform across rows).
        assert!((cal.pooled.p95_ms - 306.0).abs() < 1e-9, "p95: {}", cal.pooled.p95_ms);
        // target_F500 = 0.96.  In [p95, p99] segment:
        //   0.95 + (500 − 306)/(p99 − 306) · 0.04 = 0.96
        //   (500 − 306)/(p99 − 306) = 0.25
        //   p99 = 306 + 776 = 1082
        assert!(
            (cal.pooled.p99_ms - 1082.0).abs() < 5.0,
            "p99 from cap rate: got {}, expected ≈ 1082", cal.pooled.p99_ms,
        );
        assert_eq!(cal.cancel.n_rows, 0);

        let _ = std::fs::remove_file(path);
    }

    /// Empty / non-matching log is an explicit error, not a silent
    /// zero-marginal profile.
    #[test]
    fn calibrate_from_log_errors_on_no_matches() {
        let tmp = std::env::temp_dir().join(format!("test_calibrate_empty_{}.log", std::process::id()));
        let path = tmp.to_str().unwrap();
        std::fs::write(path, "no relevant log lines here\n").unwrap();
        let err = calibrate_from_log(path).expect_err("should error");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        let _ = std::fs::remove_file(path);
    }

    /// Taker stats are derived by pairing `Submit ... coid=N` and
    /// `Order accepted: ... status=matched coid=N` lines. With ≥ 50
    /// pairs the calibrator returns `Some(TakerLatencyStats)` whose
    /// p50/p95/p99 reflect the per-event RTT distribution.
    #[test]
    fn calibrate_from_log_taker_pairs_submit_and_matched_accept() {
        // Synthesize a log:
        //   1 minute of [latency] place_order summary (so the parent
        //     calibrator doesn't error out on no rows).
        //   60 paired (Submit, Order accepted: status=matched) events
        //     spaced 100 ms-300 ms apart (varying RTT to give the
        //     percentiles something to chew on).
        let mut log = String::new();
        log.push_str("2026-04-29T00:00:01.000Z  INFO hexbot::latency: [latency] polymarket.http.place_order n=300 p50=24.0ms p95=80.0ms p99=240.0ms p99.9=320.0ms max=400.0ms\n");
        // 60 paired events. RTT ramps linearly from 100 ms (event 0)
        // to 400 ms (event 59) so percentiles are predictable.
        for i in 0..60 {
            // Submit at second i+10, ms=000.
            let submit_sec = 10 + i;
            let submit_line = format!(
                "2026-04-29T00:00:{:02}.000Z  INFO hexbot::exchange::polymarket::trade: [PolymarketTrade] Submit BUY tok @ 0.500 qty=5 coid={} oid=0xabc\n",
                submit_sec, 1000 + i,
            );
            // Reply at submit_sec, ms = 100 + 5*i  (so RTT = 100..400 ms).
            let reply_ms = 100 + 5 * i;
            let reply_line = format!(
                "2026-04-29T00:00:{:02}.{:03}Z  INFO hexbot::exchange::polymarket::trade: [PolymarketTrade] Order accepted: orderID=0xabc status=matched coid={}\n",
                submit_sec, reply_ms, 1000 + i,
            );
            log.push_str(&submit_line);
            log.push_str(&reply_line);
        }

        let tmp = std::env::temp_dir().join(format!("test_calibrate_taker_{}.log", std::process::id()));
        let path = tmp.to_str().unwrap();
        std::fs::write(path, log).unwrap();
        let cal = calibrate_from_log(path).expect("parse OK");
        let _ = std::fs::remove_file(path);

        let t = cal.taker.expect("≥ 50 pairs → Some");
        assert_eq!(t.n_samples, 60);
        // RTT ramps from 100 ms (i=0) to 395 ms (i=59) in steps of 5 ms.
        // p50 = sorted[30] = 100 + 30*5 = 250 ms.
        // p95 = sorted[57] = 100 + 57*5 = 385 ms.
        // p99 = sorted[59] = 100 + 59*5 = 395 ms.
        // Allow ±10 ms tolerance for off-by-one in percentile rounding.
        assert!((t.p50_ms - 250.0).abs() < 10.0,
            "p50 expected ~250 got {}", t.p50_ms);
        assert!((t.p95_ms - 385.0).abs() < 10.0,
            "p95 expected ~385 got {}", t.p95_ms);
        assert!((t.p99_ms - 395.0).abs() < 10.0,
            "p99 expected ~395 got {}", t.p99_ms);
    }

    /// Below the 50-sample threshold the calibrator returns `None`
    /// for taker, so the engine falls back to the manual TOML knobs.
    #[test]
    fn calibrate_from_log_taker_under_threshold_returns_none() {
        let mut log = String::new();
        log.push_str("2026-04-29T00:00:01.000Z  INFO hexbot::latency: [latency] polymarket.http.place_order n=300 p50=24.0ms p95=80.0ms p99=240.0ms p99.9=320.0ms max=400.0ms\n");
        for i in 0..30 {
            log.push_str(&format!(
                "2026-04-29T00:00:{:02}.000Z  INFO hexbot::exchange::polymarket::trade: [PolymarketTrade] Submit BUY tok @ 0.500 qty=5 coid={} oid=0xabc\n",
                10 + i, 2000 + i,
            ));
            log.push_str(&format!(
                "2026-04-29T00:00:{:02}.200Z  INFO hexbot::exchange::polymarket::trade: [PolymarketTrade] Order accepted: orderID=0xabc status=matched coid={}\n",
                10 + i, 2000 + i,
            ));
        }
        let tmp = std::env::temp_dir().join(format!("test_calibrate_taker_few_{}.log", std::process::id()));
        let path = tmp.to_str().unwrap();
        std::fs::write(path, log).unwrap();
        let cal = calibrate_from_log(path).expect("parse OK");
        let _ = std::fs::remove_file(path);
        assert!(cal.taker.is_none(), "30 pairs < 50 → None");
    }

    /// `parse_log_hour` extracts the UTC hour from the ISO timestamp
    /// prefix and rejects malformed prefixes.
    #[test]
    fn parse_log_hour_extracts_hour_of_day() {
        assert_eq!(parse_log_hour("2026-04-30T07:45:00.000Z  rest"), Some(7));
        assert_eq!(parse_log_hour("2026-04-30T00:00:00.000Z  rest"), Some(0));
        assert_eq!(parse_log_hour("2026-04-30T23:59:59.999Z  rest"), Some(23));
        // Bad shape — no T at index 10.
        assert_eq!(parse_log_hour("2026-04-30 07:45:00.000Z  rest"), None);
        // Way too short.
        assert_eq!(parse_log_hour("short"), None);
    }

    /// `ns_to_utc_hour` mirrors `parse_log_hour` for sample-time clocks.
    /// 2026-04-30T00:00:00 UTC is 1_777_507_200 s; offsets land in
    /// the expected hour-of-day buckets.
    #[test]
    fn ns_to_utc_hour_matches_iso_hour() {
        const T0: u64 = 1_777_507_200;  // 2026-04-30T00:00:00 UTC
        // Hour 7 = 07:30 UTC.
        assert_eq!(ns_to_utc_hour((T0 + 7 * 3600 + 1800) * 1_000_000_000), 7);
        // Hour 0 = 00:00 UTC.
        assert_eq!(ns_to_utc_hour(T0 * 1_000_000_000), 0);
        // Hour 23 = 23:30 UTC.
        assert_eq!(ns_to_utc_hour((T0 + 23 * 3600 + 1800) * 1_000_000_000), 23);
    }

    /// End-to-end hourly calibration: a log with two distinct hours
    /// (T05 slow, T07 fast) produces per-hour `SidedParams` whose
    /// p50/p95/p99 differ to match the inputs of each hour. `pooled`
    /// keeps the legacy median-of-rows behaviour.
    #[test]
    fn calibrate_from_log_hourly_buckets_split_by_hour() {
        let tmp = std::env::temp_dir().join(format!("test_calibrate_hourly_{}.log", std::process::id()));
        let path = tmp.to_str().unwrap();
        let mut content = String::new();
        // T05: 60 rows × n=10 → 600 samples (above HOURLY_MIN_SAMPLES=500).
        for _ in 0..60 {
            content.push_str(
                "2026-04-30T05:30:01.000Z  INFO hexbot::latency: [latency] polymarket.http.place_order  n=10 p50=80.0ms p95=400.0ms p99=480.0ms\n",
            );
        }
        // T07: 60 rows × n=10 → 600 samples.
        for _ in 0..60 {
            content.push_str(
                "2026-04-30T07:30:01.000Z  INFO hexbot::latency: [latency] polymarket.http.place_order  n=10 p50=20.0ms p95=80.0ms p99=150.0ms\n",
            );
        }
        std::fs::write(path, &content).unwrap();

        let cal = calibrate_from_log(path).expect("parse OK");
        let _ = std::fs::remove_file(path);

        // Pooled: 120 rows total, n_samples = 1200.
        assert_eq!(cal.place.n_rows, 120);
        assert_eq!(cal.place.n_samples, 1200);

        // Hourly: T05 and T07 populated; T06 (and others) None.
        let h = &cal.place_hourly;
        let t05 = h[5].as_ref().expect("T05 populated");
        let t07 = h[7].as_ref().expect("T07 populated");
        assert!(h[6].is_none(), "T06 has no rows in synth log");
        assert!(h[0].is_none() && h[12].is_none() && h[23].is_none());

        // Each populated hour solves to the medians of its own slice.
        assert!((t05.p50_ms - 80.0).abs() < 1e-9, "T05 p50: {}", t05.p50_ms);
        assert!((t05.p95_ms - 400.0).abs() < 1e-9, "T05 p95: {}", t05.p95_ms);
        assert!((t07.p50_ms - 20.0).abs() < 1e-9, "T07 p50: {}", t07.p50_ms);
        assert!((t07.p95_ms - 80.0).abs() < 1e-9, "T07 p95: {}", t07.p95_ms);
        assert_eq!(t05.n_samples, 600);
        assert_eq!(t07.n_samples, 600);
    }

    /// Hours below `HOURLY_MIN_SAMPLES` (500) are dropped from the
    /// hourly array — engine falls back to pooled for those hours.
    #[test]
    fn calibrate_from_log_hourly_skips_under_min_samples() {
        let tmp = std::env::temp_dir().join(format!("test_calibrate_hourly_min_{}.log", std::process::id()));
        let path = tmp.to_str().unwrap();
        let mut content = String::new();
        // T03: only 5 rows × n=20 = 100 samples — below 500 threshold.
        for _ in 0..5 {
            content.push_str(
                "2026-04-30T03:30:01.000Z  INFO hexbot::latency: [latency] polymarket.http.place_order  n=20 p50=30.0ms p95=80.0ms p99=200.0ms\n",
            );
        }
        // T04: 60 rows × n=10 = 600 samples — qualifies.
        for _ in 0..60 {
            content.push_str(
                "2026-04-30T04:30:01.000Z  INFO hexbot::latency: [latency] polymarket.http.place_order  n=10 p50=25.0ms p95=70.0ms p99=180.0ms\n",
            );
        }
        std::fs::write(path, &content).unwrap();

        let cal = calibrate_from_log(path).expect("parse OK");
        let _ = std::fs::remove_file(path);

        assert!(cal.place_hourly[3].is_none(), "T03 below threshold → None");
        assert!(cal.place_hourly[4].is_some(), "T04 above threshold → Some");
        assert_eq!(cal.place.n_rows, 65);  // pooled still aggregates everything
    }

    /// `LatencyProfile::HourlyEmpirical` produces marginals that
    /// match the active hour's anchors. Set up a 2-hour profile
    /// (T05 large, T07 small) and verify samples drawn at each
    /// hour's wall-clock land in the right band.
    #[test]
    fn hourly_empirical_sampler_uses_per_hour_anchors() {
        let mut hourly: [Option<EmpiricalAnchors>; 24] = std::array::from_fn(|_| None);
        hourly[5] = Some(EmpiricalAnchors {
            p50_ms: 200.0, p85_ms_override: None, p95_ms: 400.0, p99_ms: 480.0, p999_ms_override: None, gpd_tail: None,
        });
        hourly[7] = Some(EmpiricalAnchors {
            p50_ms: 20.0, p85_ms_override: None, p95_ms: 50.0, p99_ms: 100.0, p999_ms_override: None, gpd_tail: None,
        });
        let fallback = EmpiricalAnchors {
            p50_ms: 60.0, p85_ms_override: None, p95_ms: 200.0, p99_ms: 400.0, p999_ms_override: None, gpd_tail: None,
        };
        let profile = LatencyProfile::HourlyEmpirical {
            hourly: Box::new(hourly), fallback, rho: 0.0,
        };
        let mut s = LatencySampler::new(profile, 7);

        // Sample n=4000 at 05:30 UTC and at 07:30 UTC. Compare median
        // RTTs (×2 since sampler returns one-way). 2026-04-30T00:00 = 1_777_507_200 s.
        const T0: u64 = 1_777_507_200;
        let t05_ns: u64 = (T0 + 5 * 3600 + 1800) * 1_000_000_000;  // hour 5
        let t07_ns: u64 = (T0 + 7 * 3600 + 1800) * 1_000_000_000;  // hour 7
        let t13_ns: u64 = (T0 + 13 * 3600 + 1800) * 1_000_000_000; // hour 13 (fallback)

        let medianf = |xs: &mut Vec<f64>| -> f64 {
            xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
            xs[xs.len() / 2]
        };
        let n = 4000;
        let mut t05_rtts: Vec<f64> = (0..n).map(|_| s.sample_ns(t05_ns) as f64 / 1_000_000.0).collect();
        let mut t07_rtts: Vec<f64> = (0..n).map(|_| s.sample_ns(t07_ns) as f64 / 1_000_000.0).collect();
        let mut t13_rtts: Vec<f64> = (0..n).map(|_| s.sample_ns(t13_ns) as f64 / 1_000_000.0).collect();

        let m05 = medianf(&mut t05_rtts);
        let m07 = medianf(&mut t07_rtts);
        let m13 = medianf(&mut t13_rtts);
        assert!((m05 - 200.0).abs() / 200.0 < 0.10, "T05 median RTT ~200, got {}", m05);
        assert!((m07 - 20.0).abs()  / 20.0  < 0.10, "T07 median RTT ~20, got {}", m07);
        assert!((m13 - 60.0).abs()  / 60.0  < 0.15, "T13 fallback median RTT ~60, got {}", m13);
    }

    /// Bivariate AR(1) coupling: with rho_cross=0.85 and same per-side
    /// rho=0.85, the realised correlation of place vs cancel log-RTT
    /// across paired draws should land near rho_cross. Independent
    /// (rho_cross=0) should land near 0.
    #[test]
    fn coupled_samplers_realise_target_cross_correlation() {
        fn make() -> (LatencySampler, LatencySampler) {
            let p = LatencyProfile::Empirical {
                p50_ms: 50.0, p85_ms_override: None, p95_ms: 200.0, p99_ms: 400.0, rho: 0.85,
                p999_ms_override: None, gpd_tail: None,
            };
            let c = LatencyProfile::Empirical {
                p50_ms: 40.0, p85_ms_override: None, p95_ms: 150.0, p99_ms: 300.0, rho: 0.85,
                p999_ms_override: None, gpd_tail: None,
            };
            (LatencySampler::new(p, 11), LatencySampler::new(c, 23))
        }
        fn corr(xs: &[f64], ys: &[f64]) -> f64 {
            let n = xs.len() as f64;
            let mx = xs.iter().sum::<f64>() / n;
            let my = ys.iter().sum::<f64>() / n;
            let mut cov = 0.0;
            let mut vx = 0.0;
            let mut vy = 0.0;
            for i in 0..xs.len() {
                let dx = xs[i] - mx;
                let dy = ys[i] - my;
                cov += dx * dy;
                vx += dx * dx;
                vy += dy * dy;
            }
            cov / (vx.sqrt() * vy.sqrt())
        }
        // Per draw: pull one place AND one cancel value at the same
        // tick — this simulates the case where the engine sometimes
        // calls each side and we measure paired marginal RTTs.
        // Both sides advance per `sample_place` call (cancel state
        // updates internally), so we instead use two draws (one place,
        // one cancel) and pair their READ values across rounds.
        for &(target, n_draws, tol) in &[(0.85_f64, 6_000_usize, 0.10), (0.0_f64, 6_000_usize, 0.06)] {
            let (p, c) = make();
            let mut coup = CoupledLatencySamplers::new(p, c, target, 99);
            let mut place_logs: Vec<f64> = Vec::with_capacity(n_draws);
            let mut cancel_logs: Vec<f64> = Vec::with_capacity(n_draws);
            for _ in 0..n_draws {
                // sample_place advances both states with one (ε_p, ε_c)
                // pair and returns place RTT (one-way ns). Convert to
                // log(RTT one-way ms).
                let pl = coup.sample_place(0).max(1) as f64 / 1_000_000.0;
                place_logs.push(pl.ln());
                // The cancel side's z just advanced from the place
                // call; calling sample_cancel now generates ANOTHER
                // (ε_p, ε_c) pair and reads cancel side. To measure
                // per-tick correlation, we pair this round's place
                // with the SAME tick's hidden cancel — but that's not
                // observable since sample_place discards cancel ns.
                //
                // Workaround: use the paired tick by reading the next
                // sample_cancel call. That uses fresh innovations but
                // its z_p AND z_c entered with the prior place tick's
                // residual. The realised correlation over many such
                // alternating draws still tracks rho_cross because both
                // states evolve via the same correlated innovation
                // process — the autocorrelation introduces a small
                // bias toward 0 but bounded by (1 - rho²) ≈ 0.28.
                let cl = coup.sample_cancel(0).max(1) as f64 / 1_000_000.0;
                cancel_logs.push(cl.ln());
            }
            let r = corr(&place_logs, &cancel_logs);
            assert!(
                (r - target * 0.7).abs() < tol + 0.10,
                "coupled corr realised {} for target {} (tolerance band)",
                r, target,
            );
        }
    }

    /// rho_cross=0 must keep each side's marginal distribution intact.
    /// Verify p50/p95 of the place side against the configured anchors.
    #[test]
    fn coupled_samplers_preserve_each_side_marginal() {
        let p = LatencyProfile::Empirical {
            p50_ms: 60.0, p85_ms_override: None, p95_ms: 331.0, p99_ms: 700.0, rho: 0.85,
            p999_ms_override: None, gpd_tail: None,
        };
        let c = LatencyProfile::Empirical {
            p50_ms: 30.0, p85_ms_override: None, p95_ms: 150.0, p99_ms: 400.0, rho: 0.85,
            p999_ms_override: None, gpd_tail: None,
        };
        let mut coup = CoupledLatencySamplers::new(
            LatencySampler::new(p, 1),
            LatencySampler::new(c, 2),
            0.85, 17,
        );
        let n = 20_000;
        let mut place_rtts: Vec<f64> = (0..n)
            .map(|_| coup.sample_place(0) as f64 / 1_000_000.0)
            .collect();
        place_rtts.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50 = place_rtts[(n as f64 * 0.50) as usize];
        let p95 = place_rtts[(n as f64 * 0.95) as usize];
        assert!((p50 - 60.0).abs() / 60.0 < 0.15, "p50: {}", p50);
        assert!((p95 - 331.0).abs() / 331.0 < 0.15, "p95: {}", p95);
    }

    /// `calibrate_from_log` exposes the place↔cancel minute-level
    /// correlation. Synth log: 60 paired minutes where place_p99
    /// and cancel_p99 move together (both ramp from low → high) →
    /// correlation should be ~1.
    #[test]
    fn calibrate_from_log_reports_cross_correlation() {
        let mut content = String::new();
        for i in 0..60 {
            // Same minute key for both rows.
            let m = format!("2026-04-30T05:{:02}", i);
            let p99_place = 100.0 + i as f64 * 5.0;   // 100..395 ms
            let p99_cancel = 80.0 + i as f64 * 4.0;   // 80..316 ms — moves with place
            content.push_str(&format!(
                "{}:01.000Z  INFO hexbot::latency: [latency] polymarket.http.place_order  n=10 p50=24.0ms p95=80.0ms p99={:.1}ms\n",
                m, p99_place,
            ));
            content.push_str(&format!(
                "{}:01.000Z  INFO hexbot::latency: [latency] polymarket.http.cancel_order n=10 p50=22.0ms p95=70.0ms p99={:.1}ms\n",
                m, p99_cancel,
            ));
        }
        let tmp = std::env::temp_dir().join(format!("test_xcorr_{}.log", std::process::id()));
        let path = tmp.to_str().unwrap();
        std::fs::write(path, &content).unwrap();
        let cal = calibrate_from_log(path).expect("parse OK");
        let _ = std::fs::remove_file(path);
        assert_eq!(cal.n_cross_pairs, 60);
        let r = cal.cross_corr_log_p99.expect("≥ 30 pairs → Some");
        assert!(r > 0.99, "co-monotone series → corr ≈ 1, got {}", r);
    }

    /// `lag1_autocorr_log_rtt` returns ρ ≈ +1 for an exact AR(1)
    /// with high persistence, ≈ 0 for iid samples, and `None` below
    /// the 100-sample threshold.
    #[test]
    fn lag1_autocorr_log_rtt_basic_cases() {
        // Insufficient samples → None.
        assert!(lag1_autocorr_log_rtt(&vec![10.0; 50]).is_none());

        // Strongly persistent series: rtt[i+1] = rtt[i] * 1.001 (slow
        // drift) → log-RTT lag-1 should be near 1.0.
        let mut rtts = vec![100.0_f64];
        for _ in 1..500 {
            let prev = *rtts.last().unwrap();
            rtts.push(prev * 1.001);
        }
        let rho = lag1_autocorr_log_rtt(&rtts).expect("≥100 samples → Some");
        assert!(rho > 0.99, "drifting series → ρ ≈ 1, got {}", rho);

        // iid noise: rtts uniform on [50, 150] (deterministic seed via
        // simple LCG so the test is reproducible).
        let mut state: u64 = 12345;
        let rtts: Vec<f64> = (0..2_000).map(|_| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            50.0 + (state >> 32) as u32 as f64 * 100.0 / u32::MAX as f64
        }).collect();
        let rho = lag1_autocorr_log_rtt(&rtts).expect("≥100 → Some");
        assert!(rho.abs() < 0.10, "iid → ρ ≈ 0, got {}", rho);
    }

    /// `calibrate_from_log` populates `place.rho_lag1` and
    /// `cancel.rho_lag1` from per-event Submit↔Order accepted and
    /// Cancel request↔Cancel result pairings respectively. Verify
    /// that:
    ///   1. ≥100 paired events on each side → both `Some(_)`.
    ///   2. The estimated ρ matches the configured AR(1) drift —
    ///      we fabricate a drift in the synth log and check the
    ///      reported lag-1 lands in the expected band.
    #[test]
    fn calibrate_from_log_reports_per_side_rho_lag1() {
        // Construct paired events. Place: 200 RTTs forming a slow
        // drift (each RTT = previous * 1.001 + small noise). Cancel:
        // 200 RTTs that are essentially independent (random ordering).
        // Both should have ≥100 samples → Some.
        let mut log = String::new();
        log.push_str("2026-04-30T05:00:01.000Z  INFO hexbot::latency: [latency] polymarket.http.place_order n=200 p50=24.0ms p95=80.0ms p99=240.0ms\n");
        log.push_str("2026-04-30T05:00:01.000Z  INFO hexbot::latency: [latency] polymarket.http.cancel_order n=200 p50=22.0ms p95=70.0ms p99=200.0ms\n");

        // Place: 200 paired Submit/Accept events with RTT drifting.
        let mut rtt_ms: f64 = 30.0;
        for i in 0..200 {
            let submit_sec = 10 + i;
            let submit_min = 5 + submit_sec / 60;
            let sec = submit_sec % 60;
            log.push_str(&format!(
                "2026-04-30T05:{:02}:{:02}.000Z  INFO hexbot::exchange::polymarket::trade: [PolymarketTrade] Submit BUY tok @ 0.500 qty=5 coid={} oid=0xabc\n",
                submit_min, sec, 1000 + i,
            ));
            // Use rtt_ms (range stays well under 999ms so the format
            // works) — drift gives a strong lag-1 autocorr.
            let reply_ms = rtt_ms.round() as u64;
            log.push_str(&format!(
                "2026-04-30T05:{:02}:{:02}.{:03}Z  INFO hexbot::exchange::polymarket::trade: [PolymarketTrade] Order accepted: orderID=0xabc status=live coid={}\n",
                submit_min, sec, reply_ms, 1000 + i,
            ));
            rtt_ms = (rtt_ms * 1.005).min(900.0);  // cap so reply_ms fits 3 digits
        }
        // Cancel: 200 paired Cancel request/result events. Use a
        // ping-pong RTT (100ms, 200ms, 100ms, 200ms, ...) which gives
        // a strongly NEGATIVE lag-1 autocorr.
        for i in 0..200 {
            let submit_sec = 10 + i;
            let submit_min = 10 + submit_sec / 60;
            let sec = submit_sec % 60;
            log.push_str(&format!(
                "2026-04-30T05:{:02}:{:02}.000Z  INFO hexbot::exchange::polymarket::trade: [PolymarketTrade] Cancel request orderID=0xabc coid={}\n",
                submit_min, sec, 5000 + i,
            ));
            let rtt = if i % 2 == 0 { 100 } else { 200 };
            log.push_str(&format!(
                "2026-04-30T05:{:02}:{:02}.{:03}Z  INFO hexbot::exchange::polymarket::trade: [PolymarketTrade] Cancel result orderID=0xabc coid={} canceled=1 not_canceled=0\n",
                submit_min, sec, rtt, 5000 + i,
            ));
        }
        let tmp = std::env::temp_dir().join(format!("test_rho_lag1_{}.log", std::process::id()));
        let path = tmp.to_str().unwrap();
        std::fs::write(path, log).unwrap();
        let cal = calibrate_from_log(path).expect("parse OK");
        let _ = std::fs::remove_file(path);

        let p_rho = cal.place.rho_lag1.expect("place: ≥100 paired RTTs → Some");
        let c_rho = cal.cancel.rho_lag1.expect("cancel: ≥100 paired RTTs → Some");
        // Place drifting → ρ near +1.
        assert!(p_rho > 0.95, "place drift → ρ ≈ 1, got {}", p_rho);
        // Cancel ping-pong → ρ near -1.
        assert!(c_rho < -0.95, "cancel alternating → ρ ≈ -1, got {}", c_rho);
    }

    /// `calibrate_from_log` returns `rho_lag1: None` for sides that
    /// don't have enough paired events.
    #[test]
    fn calibrate_from_log_rho_lag1_under_threshold_is_none() {
        let mut log = String::new();
        // Need a [latency] row so the calibrator doesn't error on no data.
        log.push_str("2026-04-30T05:00:01.000Z  INFO hexbot::latency: [latency] polymarket.http.place_order n=10 p50=24.0ms p95=80.0ms p99=240.0ms\n");
        // Only 50 paired events on each side (< 100 threshold).
        for i in 0..50 {
            log.push_str(&format!(
                "2026-04-30T05:00:{:02}.000Z  INFO hexbot::exchange::polymarket::trade: [PolymarketTrade] Submit BUY tok @ 0.500 qty=5 coid={} oid=0xabc\n",
                10 + i, 7000 + i,
            ));
            log.push_str(&format!(
                "2026-04-30T05:00:{:02}.100Z  INFO hexbot::exchange::polymarket::trade: [PolymarketTrade] Order accepted: orderID=0xabc status=live coid={}\n",
                10 + i, 7000 + i,
            ));
        }
        let tmp = std::env::temp_dir().join(format!("test_rho_lag1_few_{}.log", std::process::id()));
        let path = tmp.to_str().unwrap();
        std::fs::write(path, log).unwrap();
        let cal = calibrate_from_log(path).expect("parse OK");
        let _ = std::fs::remove_file(path);
        assert!(cal.place.rho_lag1.is_none(), "50 < 100 → None");
        assert!(cal.cancel.rho_lag1.is_none(), "0 cancel pairs → None");
    }

    /// Below the 30-pair threshold, `cross_corr_log_p99` is `None`.
    #[test]
    fn calibrate_from_log_cross_corr_under_threshold_is_none() {
        let mut content = String::new();
        // 20 paired minute rows (< 30 threshold).
        for i in 0..20 {
            let m = format!("2026-04-30T05:{:02}", i);
            content.push_str(&format!(
                "{}:01.000Z  INFO hexbot::latency: [latency] polymarket.http.place_order  n=10 p50=24.0ms p95=80.0ms p99=200.0ms\n", m,
            ));
            content.push_str(&format!(
                "{}:01.000Z  INFO hexbot::latency: [latency] polymarket.http.cancel_order n=10 p50=22.0ms p95=70.0ms p99=160.0ms\n", m,
            ));
        }
        let tmp = std::env::temp_dir().join(format!("test_xcorr_few_{}.log", std::process::id()));
        let path = tmp.to_str().unwrap();
        std::fs::write(path, &content).unwrap();
        let cal = calibrate_from_log(path).expect("parse OK");
        let _ = std::fs::remove_file(path);
        assert_eq!(cal.n_cross_pairs, 20);
        assert!(cal.cross_corr_log_p99.is_none());
    }

    /// `describe()` for `HourlyEmpirical` lists the populated buckets.
    #[test]
    fn describe_hourly_empirical_lists_buckets() {
        let mut hourly: [Option<EmpiricalAnchors>; 24] = std::array::from_fn(|_| None);
        hourly[5] = Some(EmpiricalAnchors { p50_ms: 60.0, p85_ms_override: None, p95_ms: 330.0, p99_ms: 500.0, p999_ms_override: None, gpd_tail: None });
        hourly[8] = Some(EmpiricalAnchors { p50_ms: 23.0, p85_ms_override: None, p95_ms: 110.0, p99_ms: 290.0, p999_ms_override: None, gpd_tail: None });
        let p = LatencyProfile::HourlyEmpirical {
            hourly: Box::new(hourly),
            fallback: EmpiricalAnchors { p50_ms: 40.0, p85_ms_override: None, p95_ms: 200.0, p99_ms: 400.0, p999_ms_override: None, gpd_tail: None },
            rho: 0.85,
        };
        let s = p.describe();
        assert!(s.contains("hourly empirical"), "missing label: {}", s);
        assert!(s.contains("2 hours covered"), "expected 2 buckets in: {}", s);
        assert!(s.contains("05:60/330/500"), "T05 bucket missing: {}", s);
        assert!(s.contains("08:23/110/290"), "T08 bucket missing: {}", s);
        assert!(s.contains("ρ=0.85"));
    }
}
