//! Generalised Pareto Distribution (GPD) — fit & quantile primitives
//! for the peaks-over-threshold (POT) tail model used by the latency
//! calibrator.
//!
//! ## Why GPD over lognormal extrapolation
//!
//! The legacy `empirical_anchors` extends the (p50, p95, p99) body to
//! p99.9 / p99.99 by continuing a lognormal slope in log-space. That
//! shape is fine for the body but is **bounded in growth** — the
//! lognormal's right tail decays super-polynomially (heavier than
//! Gaussian, lighter than power law). Real HTTP-gateway RTT has
//! sustained heavy-tail behaviour (queueing brown-outs, retry storms,
//! GC pauses, host-OS scheduler hiccups) that decays as a power law
//! past some threshold u. Pickands–Balkema–de Haan: under mild
//! regularity, the conditional excess distribution `X − u | X > u`
//! converges to GPD(σ, ξ) as u increases. ξ > 0 → polynomial tail
//! (heavy); the larger ξ the heavier.
//!
//! In practice for our place / cancel HTTP path we expect ξ ≈ 0.2–0.5.
//! Fitting GPD past the empirical p85–p90 of observed RTTs lets the
//! sampler emit p99.99 / p99.999 events that the lognormal extrap
//! would never produce — matching the bursts of multi-second RTT seen
//! in live.
//!
//! ## Right-censoring at the client-timeout cap
//!
//! Live's HTTP client times out at ~500 ms. Successfully-parsed RTT
//! samples (`Submit↔Order accepted` pairs) are therefore strictly
//! below the cap; timeouts are visible as separate `NewOrderTimeout` /
//! `CancelOrderTimeout` events and provide a count `n_c` of samples
//! known only to satisfy `X > 500`. Treating timeouts as observed at
//! 500 ms would systematically bias σ̂ downward (and ξ̂ toward 0). We
//! handle them as Type-I right-censored at horizon `c = cap − u` via
//! the standard censored MLE:
//!
//!   ℓ(σ, ξ) = Σ_{i uncensored} log g(y_i; σ, ξ) + n_c · log S(c; σ, ξ)
//!
//! where `g` is the GPD density and `S = 1 − G` is the survival
//! function. The `n_c · log S(c)` term acts as a "soft anchor": it
//! pulls the fit toward shapes that put `n_c / (n_uc + n_c)` of mass
//! above the cap, which is exactly the cap-hit rate the BT needs to
//! reproduce.
//!
//! ## Algorithm
//!
//! 1. Compute PWM (probability-weighted moments) estimates as a
//!    starting point — closed-form, robust for ξ ∈ (-0.5, 0.5),
//!    handles small samples reasonably.
//! 2. Reparameterise (σ → log σ) to keep σ > 0 unconstrained for the
//!    optimiser; clamp ξ to [-0.49, 0.99] (ξ ≥ 1 makes the mean
//!    infinite and is empirically implausible for HTTP RTT).
//! 3. Nelder-Mead minimisation of the negative censored log-likelihood
//!    in (log σ, ξ) space. 2D simplex, ≤ 200 iterations, converges to
//!    1e-7 tol on test fixtures within ~30 steps.
//! 4. Return `(σ, ξ)` on success; `None` when the optimiser fails to
//!    converge or returns a non-finite estimate (caller falls back to
//!    the lognormal-extrap path).

/// GPD probability density at excess `y > 0`. Returns 0 for y ≤ 0
/// or out-of-support y (when ξ < 0 the support has finite upper
/// bound at `-σ/ξ`).
///
/// Density:
///   g(y; σ, ξ) = (1/σ) · (1 + ξy/σ)^(−1/ξ − 1)    ξ ≠ 0
///              = (1/σ) · exp(−y/σ)                 ξ = 0
#[allow(dead_code)]
#[inline]
pub fn gpd_pdf(y: f64, sigma: f64, xi: f64) -> f64 {
    if y <= 0.0 || sigma <= 0.0 || !sigma.is_finite() || !xi.is_finite() {
        return 0.0;
    }
    if xi.abs() < 1e-10 {
        return (-y / sigma).exp() / sigma;
    }
    let arg = 1.0 + xi * y / sigma;
    if arg <= 0.0 {
        return 0.0;
    }
    arg.powf(-1.0 / xi - 1.0) / sigma
}

/// GPD CDF at excess `y ≥ 0`. For y < 0 returns 0.
///
/// CDF:
///   G(y; σ, ξ) = 1 − (1 + ξy/σ)^(−1/ξ)    ξ ≠ 0
///              = 1 − exp(−y/σ)              ξ = 0
#[inline]
pub fn gpd_cdf(y: f64, sigma: f64, xi: f64) -> f64 {
    if y <= 0.0 {
        return 0.0;
    }
    if sigma <= 0.0 || !sigma.is_finite() || !xi.is_finite() {
        return 0.0;
    }
    if xi.abs() < 1e-10 {
        return 1.0 - (-y / sigma).exp();
    }
    let arg = 1.0 + xi * y / sigma;
    if arg <= 0.0 {
        // y past the finite support (only possible when ξ < 0)
        return 1.0;
    }
    1.0 - arg.powf(-1.0 / xi)
}

/// GPD inverse CDF (quantile function). `q ∈ [0, 1)`.
///
/// Quantile:
///   Q(q; σ, ξ) = (σ/ξ) · ((1 − q)^(−ξ) − 1)    ξ ≠ 0
///              = −σ · ln(1 − q)                 ξ = 0
#[inline]
pub fn gpd_quantile(q: f64, sigma: f64, xi: f64) -> f64 {
    if q <= 0.0 {
        return 0.0;
    }
    if q >= 1.0 {
        // ξ > 0 → infinite upper support; cap to avoid INF
        return if xi > 0.0 { f64::INFINITY } else { -sigma / xi.min(-1e-10) };
    }
    if sigma <= 0.0 || !sigma.is_finite() || !xi.is_finite() {
        return 0.0;
    }
    let one_minus_q = 1.0 - q;
    if xi.abs() < 1e-10 {
        return -sigma * one_minus_q.ln();
    }
    sigma / xi * (one_minus_q.powf(-xi) - 1.0)
}

/// Censored MLE for GPD parameters.
///
/// Inputs:
///   * `exceedances`: uncensored excesses `y_i = X_i − u` for samples
///     where `u < X_i < cap`. Must all be > 0.
///   * `n_censored`: count of samples known only to satisfy `X > cap`
///     (i.e. timeouts that exceeded the threshold).
///   * `censor_horizon`: `cap − u`, the value of `y` past which
///     observations are right-censored. Must match the cap that
///     produced `n_censored`.
///
/// Returns `Some((σ, ξ))` on convergence to a finite estimate;
/// `None` when the optimiser fails (too few samples, non-finite
/// objective, ξ pegged to the boundary, etc.). Callers fall back
/// to lognormal extrapolation when `None`.
///
/// Requires at least 50 uncensored exceedances; below that the
/// estimate is too noisy to beat the lognormal path.
pub fn fit_gpd_censored_mle(
    exceedances: &[f64],
    n_censored: usize,
    censor_horizon: f64,
) -> Option<(f64, f64)> {
    if exceedances.len() < 50 {
        return None;
    }
    if !censor_horizon.is_finite() || censor_horizon <= 0.0 {
        // Caller error: no horizon → can't include the censored
        // term. Fit with `n_censored = 0` instead.
        return fit_gpd_censored_mle(exceedances, 0, 1.0).map(|p| p);
    }
    for &y in exceedances {
        if !y.is_finite() || y <= 0.0 {
            return None;
        }
    }

    // PWM starting point (Hosking & Wallis 1987). Closed-form, no
    // optimiser needed for the initial guess.
    let (sigma0, xi0) = pwm_estimator(exceedances)?;

    // Reparameterise to (log σ, ξ) so the optimiser doesn't need to
    // worry about σ > 0 explicitly. ξ is bounded by clamp in the
    // objective.
    let nll = |params: [f64; 2]| -> f64 {
        let log_sigma = params[0];
        let xi = params[1];
        if !log_sigma.is_finite() || !xi.is_finite() {
            return 1e18;
        }
        // Hard upper bound at ξ = 0.65: physical / domain prior. The
        // cancel side's pooled RTT distribution is a mixture across
        // hours-of-day where the worst slow-regime hours (cap rate
        // ~40 %) co-exist with normal hours (cap < 5 %). The
        // censored MLE on the pooled sample wants ξ → 1 because the
        // n_c term reflects the worst-hour tail. Letting ξ approach
        // 1 produces sampler quantiles that diverge physically
        // (p99.999 in minutes); for HTTP-gateway RTT that's a
        // mixture-of-regimes artifact, not a true single-distribution
        // shape. Capping at 0.65 keeps the tail very heavy (infinite
        // variance: E[X^q] = ∞ for q > 1.54) without runaway
        // quantiles, while still producing far heavier p99.9 / p99.99
        // than the legacy lognormal extrapolation. Per-hour GPD fits
        // — once raw RTTs are tagged by hour — should let cancel
        // explore higher ξ where data warrants.
        if xi < -0.49 || xi > 0.65 {
            return 1e18;
        }
        let sigma = log_sigma.exp();
        if !sigma.is_finite() || sigma <= 0.0 {
            return 1e18;
        }
        let mut ll = 0.0;
        let xi_near_zero = xi.abs() < 1e-8;
        for &y in exceedances {
            if xi_near_zero {
                // Exponential limit: log g(y) = -log σ - y/σ
                ll += -log_sigma - y / sigma;
            } else {
                let arg = 1.0 + xi * y / sigma;
                if arg <= 0.0 {
                    return 1e18;
                }
                ll += -log_sigma - (1.0 + 1.0 / xi) * arg.ln();
            }
        }
        if n_censored > 0 {
            // log S(c) = log(1 − G(c)) = (−1/ξ) · log(1 + ξc/σ)
            let log_surv = if xi_near_zero {
                -censor_horizon / sigma
            } else {
                let arg = 1.0 + xi * censor_horizon / sigma;
                if arg <= 0.0 {
                    return 1e18;
                }
                -arg.ln() / xi
            };
            ll += n_censored as f64 * log_surv;
        }
        if !ll.is_finite() {
            return 1e18;
        }
        -ll
    };

    let init = [sigma0.max(1e-6).ln(), xi0.clamp(-0.4, 0.6)];
    let opt = nelder_mead(&nll, init, 1e-7, 500)?;
    let sigma = opt[0].exp();
    let xi = opt[1];
    if !sigma.is_finite() || sigma <= 0.0 || !xi.is_finite() {
        return None;
    }
    // Only reject if the optimiser landed at the lower boundary —
    // ξ < -0.4 means the data wants a *finite-support* tail (negative
    // shape), which contradicts the assumption that we're modelling
    // an unbounded heavy-tailed distribution. Upper-boundary fits
    // (ξ ≈ 0.85) are clamped by design in the objective — accept and
    // let the caller use the clamped heavy tail rather than fall back
    // to lognormal.
    if xi <= -0.45 {
        return None;
    }
    Some((sigma, xi))
}

/// Probability-weighted moments estimator (Hosking & Wallis 1987,
/// "Parameter and quantile estimation for the generalized Pareto
/// distribution"). Used as the Nelder-Mead starting point.
///
/// Formulas:
///   a_0 = mean(y)
///   a_1 = (1/n) · Σ y_i · (n − i) / (n − 1)     for y sorted ascending
///   ξ̂ = a_0 / (a_0 − 2·a_1) − 2
///   σ̂ = 2 · a_0 · a_1 / (a_0 − 2·a_1)
///
/// Returns `None` when the closed-form estimator divides by zero or
/// yields a non-finite parameter — caller can fall back to a fixed
/// default like (mean, 0.1).
fn pwm_estimator(exceedances: &[f64]) -> Option<(f64, f64)> {
    let n = exceedances.len();
    if n < 2 {
        return None;
    }
    let mut sorted: Vec<f64> = exceedances.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let a0: f64 = sorted.iter().sum::<f64>() / n as f64;
    let n_f = n as f64;
    let mut a1 = 0.0;
    for (i, &y) in sorted.iter().enumerate() {
        // Rank weight: (n − (i+1)) / (n − 1) = (n − 1 − i) / (n − 1)
        a1 += y * (n_f - 1.0 - i as f64) / (n_f - 1.0);
    }
    a1 /= n_f;
    let denom = a0 - 2.0 * a1;
    if denom.abs() < 1e-12 || !denom.is_finite() {
        // Fallback: exponential init (ξ=0, σ=mean)
        return Some((a0.max(1e-6), 0.1));
    }
    let xi = a0 / denom - 2.0;
    let sigma = 2.0 * a0 * a1 / denom;
    if !sigma.is_finite() || sigma <= 0.0 || !xi.is_finite() {
        return Some((a0.max(1e-6), 0.1));
    }
    Some((sigma, xi.clamp(-0.4, 0.9)))
}

/// 2-D Nelder-Mead minimiser. Standard textbook implementation —
/// reflection α=1, expansion γ=2, contraction ρ=0.5, shrink σ=0.5.
/// `tol` is the absolute spread between the best and worst simplex
/// vertices' objective values; `max_iter` is the per-iteration cap.
fn nelder_mead<F>(f: &F, x0: [f64; 2], tol: f64, max_iter: usize) -> Option<[f64; 2]>
where
    F: Fn([f64; 2]) -> f64,
{
    let step = 0.5;
    let mut simplex: [([f64; 2], f64); 3] = [
        (x0, f(x0)),
        ([x0[0] + step, x0[1]], 0.0),
        ([x0[0], x0[1] + step], 0.0),
    ];
    simplex[1].1 = f(simplex[1].0);
    simplex[2].1 = f(simplex[2].0);

    let alpha = 1.0;
    let gamma = 2.0;
    let rho_c = 0.5;
    let sigma_s = 0.5;

    for _ in 0..max_iter {
        simplex.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        if (simplex[2].1 - simplex[0].1).abs() < tol {
            break;
        }
        // Centroid of the best two vertices (exclude worst)
        let centroid = [
            (simplex[0].0[0] + simplex[1].0[0]) / 2.0,
            (simplex[0].0[1] + simplex[1].0[1]) / 2.0,
        ];
        let x_r = [
            centroid[0] + alpha * (centroid[0] - simplex[2].0[0]),
            centroid[1] + alpha * (centroid[1] - simplex[2].0[1]),
        ];
        let f_r = f(x_r);
        if f_r >= simplex[0].1 && f_r < simplex[1].1 {
            simplex[2] = (x_r, f_r);
            continue;
        }
        if f_r < simplex[0].1 {
            let x_e = [
                centroid[0] + gamma * (x_r[0] - centroid[0]),
                centroid[1] + gamma * (x_r[1] - centroid[1]),
            ];
            let f_e = f(x_e);
            simplex[2] = if f_e < f_r { (x_e, f_e) } else { (x_r, f_r) };
            continue;
        }
        // Contraction
        let x_c = if f_r < simplex[2].1 {
            // outside contraction
            [
                centroid[0] + rho_c * (x_r[0] - centroid[0]),
                centroid[1] + rho_c * (x_r[1] - centroid[1]),
            ]
        } else {
            // inside contraction
            [
                centroid[0] + rho_c * (simplex[2].0[0] - centroid[0]),
                centroid[1] + rho_c * (simplex[2].0[1] - centroid[1]),
            ]
        };
        let f_c = f(x_c);
        let worst_f = if f_r < simplex[2].1 { f_r } else { simplex[2].1 };
        if f_c < worst_f {
            simplex[2] = (x_c, f_c);
            continue;
        }
        // Shrink toward the best vertex
        let x_best = simplex[0].0;
        for i in 1..3 {
            simplex[i].0 = [
                x_best[0] + sigma_s * (simplex[i].0[0] - x_best[0]),
                x_best[1] + sigma_s * (simplex[i].0[1] - x_best[1]),
            ];
            simplex[i].1 = f(simplex[i].0);
        }
    }
    simplex.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    if simplex[0].1.is_finite() && simplex[0].1 < 1e17 {
        Some(simplex[0].0)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    /// Sample n GPD draws with parameters (σ, ξ) via inverse CDF on
    /// uniform variates.
    fn gpd_sample(n: usize, sigma: f64, xi: f64, seed: u64) -> Vec<f64> {
        let mut rng = StdRng::seed_from_u64(seed);
        (0..n)
            .map(|_| {
                let u: f64 = rng.gen_range(0.0..1.0);
                gpd_quantile(u, sigma, xi)
            })
            .collect()
    }

    #[test]
    fn quantile_inverts_cdf() {
        let (sigma, xi) = (50.0, 0.3);
        for &q in &[0.1, 0.5, 0.9, 0.99] {
            let y = gpd_quantile(q, sigma, xi);
            let q_back = gpd_cdf(y, sigma, xi);
            assert!(
                (q - q_back).abs() < 1e-9,
                "q={} → y={} → q_back={}",
                q, y, q_back
            );
        }
    }

    #[test]
    fn quantile_inverts_cdf_at_xi_zero() {
        let (sigma, xi) = (50.0, 0.0);
        for &q in &[0.1, 0.5, 0.9, 0.99] {
            let y = gpd_quantile(q, sigma, xi);
            let q_back = gpd_cdf(y, sigma, xi);
            assert!(
                (q - q_back).abs() < 1e-9,
                "ξ=0: q={} → y={} → q_back={}",
                q, y, q_back
            );
        }
    }

    #[test]
    fn pdf_integrates_to_one_numerically() {
        // Sanity check: trapezoidal rule on a moderately-fine grid
        // should hit ~1.0 ± 0.01 for moderate ξ.
        let (sigma, xi) = (10.0, 0.2);
        let dy = 0.01;
        let mut integral = 0.0;
        let mut y = 0.0;
        while y < 10000.0 {
            integral += gpd_pdf(y, sigma, xi) * dy;
            y += dy;
        }
        assert!((integral - 1.0).abs() < 0.02, "∫g = {}", integral);
    }

    #[test]
    fn mle_recovers_uncensored_params() {
        // Generate synthetic GPD samples; MLE should recover within
        // tight bounds on n=5000.
        let true_sigma = 100.0;
        let true_xi = 0.30;
        let samples = gpd_sample(5000, true_sigma, true_xi, 42);
        let (sigma, xi) = fit_gpd_censored_mle(&samples, 0, 1e9).expect("MLE converged");
        // PWM + Nelder-Mead on n=5000 typically lands within ±10%
        // for σ and ±0.05 for ξ.
        assert!(
            (sigma / true_sigma - 1.0).abs() < 0.15,
            "σ̂ = {:.2}, true = {:.2}",
            sigma, true_sigma,
        );
        assert!(
            (xi - true_xi).abs() < 0.08,
            "ξ̂ = {:.3}, true = {:.3}",
            xi, true_xi,
        );
    }

    #[test]
    fn mle_recovers_censored_params() {
        // Synthetic experiment: true GPD(σ=80, ξ=0.4) sampled n=10000.
        // Censor at horizon c=400 (about the 85th percentile of GPD(80,
        // 0.4)). Uncensored samples below c are passed as-is; samples
        // above c are reported only as a count. Censored MLE should
        // recover (σ, ξ) close to truth.
        let true_sigma = 80.0;
        let true_xi = 0.40;
        let censor = 400.0;
        let raw = gpd_sample(10_000, true_sigma, true_xi, 7);
        let uncensored: Vec<f64> = raw.iter().copied().filter(|&y| y < censor).collect();
        let n_c = raw.len() - uncensored.len();
        assert!(n_c > 50, "test setup: need some censored samples, got {}", n_c);
        let (sigma, xi) = fit_gpd_censored_mle(&uncensored, n_c, censor)
            .expect("censored MLE converged");
        assert!(
            (sigma / true_sigma - 1.0).abs() < 0.20,
            "σ̂ = {:.2}, true = {:.2} (n_uc={}, n_c={})",
            sigma, true_sigma, uncensored.len(), n_c,
        );
        assert!(
            (xi - true_xi).abs() < 0.10,
            "ξ̂ = {:.3}, true = {:.3}",
            xi, true_xi,
        );
    }

    #[test]
    fn mle_ignoring_censoring_biases_results() {
        // Calibration regression test: when censoring is real, naively
        // dropping censored samples (passing n_c=0) should produce a
        // visibly biased estimate vs the proper censored fit. This
        // guards against future code paths that forget to plumb
        // `n_censored` through.
        let true_sigma = 100.0;
        let true_xi = 0.35;
        let censor = 500.0;
        let raw = gpd_sample(8000, true_sigma, true_xi, 99);
        let uncensored: Vec<f64> = raw.iter().copied().filter(|&y| y < censor).collect();
        let n_c = raw.len() - uncensored.len();

        let (sigma_c, xi_c) = fit_gpd_censored_mle(&uncensored, n_c, censor).unwrap();
        let (_sigma_n, xi_n) = fit_gpd_censored_mle(&uncensored, 0, censor).unwrap();
        // Without the censored term, ξ is biased toward 0 (the body
        // looks lighter than reality). The properly-censored fit
        // should be measurably closer to the true ξ.
        assert!(
            (xi_c - true_xi).abs() < (xi_n - true_xi).abs(),
            "censored fit (ξ={:.3}, σ={:.2}) should beat uncensored (ξ={:.3}) on bias against true ξ={}",
            xi_c, sigma_c, xi_n, true_xi,
        );
    }

    #[test]
    fn mle_returns_none_on_tiny_sample() {
        let tiny: Vec<f64> = (1..10).map(|i| i as f64).collect();
        assert!(fit_gpd_censored_mle(&tiny, 0, 1.0).is_none());
    }

    #[test]
    fn mle_rejects_invalid_inputs() {
        let with_neg: Vec<f64> = (0..100).map(|i| i as f64 - 5.0).collect();
        assert!(fit_gpd_censored_mle(&with_neg, 0, 1.0).is_none());
    }
}
