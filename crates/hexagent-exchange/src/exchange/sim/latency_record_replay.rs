//! **Record-replay latency source** (2026-06-16).
//!
//! An alternative to the live.log auto-calibration ([`super::latency::
//! calibrate_from_logs`]): instead of fitting an analytic empirical-CDF +
//! AR(1) model from `[latency]` summary rows, this source replays the
//! **per-request place/cancel RTT samples** that the live bot writes to
//! `latency_record` (see `crate::latency_record`) — the daily CSVs
//!
//! ```text
//! epoch_ms,iso_local,instance_id,kind,rtt_ms,status
//! 1781568000967,2026-06-16T00:00:00.967,recorder01,place,31.898,ok
//! ```
//!
//! ## Activation
//!
//! When `sim_latency_calibrate_from` resolves to a **directory** (rather
//! than one or more `.log` files), the engine loads every `*.csv` in it
//! into one [`RecordReplayData`] (place + cancel split on the `kind`
//! column) and drives the v2 latency sampler from it via the
//! [`super::latency::LatencyProfile::RecordReplay`] profile.
//!
//! ## The three-tier lookup
//!
//! At each draw the sampler hands us the order's wall-clock epoch
//! (`now_ms`) plus a uniform `u ∈ [0,1)` (from the AR(1) latent, so
//! clustering/cross-correlation still apply where we sample a
//! distribution). [`SideRecords::lookup`] resolves the RTT in three
//! tiers, each falling through to the next:
//!
//!   1. **Exact wall-clock** — if `now_ms` lands inside the recorded
//!      calendar window and a sample sits within `abs_tol_ms` of it,
//!      return that sample's RTT directly. This is the faithful
//!      "replay exactly what the network did at this instant" path,
//!      used when the backtest window overlaps the recording dates.
//!   2. **Same time-of-day, nearest** — otherwise map `now_ms` to its
//!      UTC second-of-day and, if some sample's time-of-day is within
//!      `tod_tol_secs` (circular), return the **closest** such sample's
//!      RTT. This reuses the recorded latency from the same clock time
//!      on a different day (the common case: record in June, backtest
//!      a May window).
//!   3. **Nearest time-of-day distribution** — if no sample shares the
//!      time-of-day (a gap in the recorded clock coverage), find the
//!      nearest non-empty time-of-day bucket (default 5 min wide, pooling
//!      that clock-slice across all recorded days) and draw its
//!      `u`-quantile from the empirical distribution there.
//!
//! Tiers 1 & 2 are deterministic given the order time (a faithful
//! replay). Tier 3 is the only stochastic tier and is seed-deterministic
//! through `u`. Place and cancel are resolved independently from their
//! own [`SideRecords`].

use std::path::Path;

/// Seconds in a UTC day.
const SECS_PER_DAY: u32 = 86_400;
/// Default tier-3 time-of-day bucket width (seconds) — 5 min, aligned to
/// the Polymarket 5-min event cadence. Each bucket pools that slice of the
/// clock across all recorded days, so a 5-min window holds ~5× the samples
/// a 1-min bucket would, giving the tier-3 distribution a fuller body.
pub const DEFAULT_TOD_BUCKET_SECS: u32 = 300;

/// Tunable tier boundaries for [`SideRecords::lookup`]. Defaults match
/// the per-request recorder's ~0.5 Hz/side cadence (a covered minute
/// holds tens of samples), so tier 1 fires whenever the backtest instant
/// is genuinely inside the recorded window and tier 2 whenever the same
/// clock-minute was ever recorded.
#[derive(Debug, Clone, Copy)]
pub struct RecordReplayParams {
    /// Tier-1 max |Δ| (ms) between the order epoch and the nearest
    /// recorded sample for an exact-wall-clock hit.
    pub abs_tol_ms: u64,
    /// Tier-2 max circular |Δ| (seconds) between the order's
    /// second-of-day and the nearest recorded sample's second-of-day.
    pub tod_tol_secs: u32,
    /// How the date/time-of-day selection picks a sample/bucket when the
    /// instant is outside the recorded window. Tier 1 (exact) is unaffected.
    pub fallback: RecordReplayFallback,
}

impl Default for RecordReplayParams {
    fn default() -> Self {
        Self { abs_tol_ms: 300_000, tod_tol_secs: 120, fallback: RecordReplayFallback::Pooled }
    }
}

/// Tier-2/3 fallback policy (see [`RecordReplayParams::fallback`]). RTT
/// distribution drifts day-to-day (traffic / system-load regime) and by
/// intra-day session; for a backtest instant outside the recorded window,
/// the calendar-nearest recorded day is a better proxy than the all-days
/// pool. Tier 1 (exact wall-clock) is never affected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RecordReplayFallback {
    /// Legacy: tiers 2/3 pool every recorded day by time-of-day.
    #[default]
    Pooled,
    /// Prefer the calendar-nearest recorded day; within it match by
    /// time-of-day (tier 2) / draw its tod-bucket distribution (tier 3).
    NearestDay,
    /// Prefer recorded days of the **same NY day-of-week** as the query
    /// (nearest among them); only when no same-weekday day exists fall back
    /// to the nearest day regardless of weekday. RTT regime differs by
    /// weekday (weekend lull, weekday load), so the same weekday is the
    /// closest proxy. This is the default.
    NearestDayDow,
}

impl RecordReplayFallback {
    /// Parse from a TOML string (case-insensitive, `-`→`_`). Unknown / empty
    /// → `Pooled` (the byte-identical-to-legacy default).
    pub fn from_str(s: &str) -> Self {
        match s.to_ascii_lowercase().replace('-', "_").as_str() {
            "nearest_day" | "nearest" => RecordReplayFallback::NearestDay,
            "nearest_day_dow" | "nearest_dow" | "nearest_day_weektype" => {
                RecordReplayFallback::NearestDayDow
            }
            _ => RecordReplayFallback::Pooled,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            RecordReplayFallback::Pooled => "pooled",
            RecordReplayFallback::NearestDay => "nearest_day",
            RecordReplayFallback::NearestDayDow => "nearest_day_dow",
        }
    }
}

/// Milliseconds in a UTC day — the unit used to bucket samples into
/// calendar days for the date-aware fallback.
const MS_PER_DAY: u64 = 86_400_000;

/// NY day-of-week of `epoch_ms` as `0=Mon .. 6=Sun` (America/New_York —
/// the project's trading-session calendar, mirroring `latency::is_ny_saturday`).
/// Used to group recorded days by weekday for the `NearestDayDow` fallback.
fn ny_weekday(epoch_ms: u64) -> u8 {
    use chrono::{DateTime, Datelike, Utc};
    use chrono_tz::America::New_York;
    let utc = DateTime::<Utc>::from_timestamp_nanos((epoch_ms as i64) * 1_000_000);
    utc.with_timezone(&New_York).weekday().num_days_from_monday() as u8
}

/// One recorded calendar day's time-of-day views, for the date-aware
/// fallback (tiers 2/3 restricted to a single day).
struct DayRecords {
    /// UTC epoch-day index (`epoch_ms / MS_PER_DAY`).
    day: u64,
    /// NY day-of-week (`0=Mon .. 6=Sun`), from this day's UTC noon.
    dow: u8,
    /// `(sec_of_day, rtt_ms)` for this day only, sorted by `sec_of_day`.
    by_tod: Vec<(u32, f32)>,
    /// This day's time-of-day buckets (`bucket_secs` wide), each sorted asc.
    tod_buckets: Vec<Vec<f32>>,
    /// Per-bucket nearest non-empty bucket within THIS day (tier 3, O(1)).
    nearest_nonempty_bucket: Vec<u32>,
}

/// One side's (place OR cancel) recorded RTT samples plus the indices the
/// three-tier lookup needs. Cheap to share via `Arc` — the sampler only
/// reads it.
pub struct SideRecords {
    /// `(epoch_ms, rtt_ms)` sorted by `epoch_ms`. Tier 1.
    by_epoch: Vec<(u64, f32)>,
    /// `(sec_of_day, rtt_ms)` sorted by `sec_of_day` (0..86400). Tier 2.
    by_tod: Vec<(u32, f32)>,
    /// Time-of-day RTT buckets of `bucket_secs` width, each sorted
    /// ascending. Tier 3 draws the `u`-quantile of the nearest non-empty
    /// bucket. A bucket pools its clock-slice across all recorded days.
    tod_buckets: Vec<Vec<f32>>,
    /// For each tod bucket, the nearest (circular) bucket whose samples are
    /// non-empty. Lets tier 3 resolve in O(1). Empty when no samples.
    nearest_nonempty_bucket: Vec<u32>,
    /// Per-recorded-day time-of-day views, sorted by `day` ascending. Used
    /// only by the date-aware fallback policies (`NearestDay*`); the
    /// `Pooled` default ignores it. With a single recorded day this holds
    /// one entry whose views equal the pooled ones above.
    days: Vec<DayRecords>,
    /// Width of each `tod_buckets` slot in seconds (e.g. 300 = 5 min).
    bucket_secs: u32,
    min_epoch_ms: u64,
    max_epoch_ms: u64,
}

impl std::fmt::Debug for SideRecords {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SideRecords")
            .field("n", &self.by_epoch.len())
            .field("min_epoch_ms", &self.min_epoch_ms)
            .field("max_epoch_ms", &self.max_epoch_ms)
            .field("bucket_secs", &self.bucket_secs)
            .field("covered_buckets", &self.tod_buckets.iter().filter(|b| !b.is_empty()).count())
            .finish()
    }
}

impl SideRecords {
    /// Build from raw `(epoch_ms, rtt_ms)` samples with the default 5-min
    /// tier-3 bucket. Empty input yields an empty record set (`n() == 0`),
    /// which the engine treats as "fall back to the analytic model".
    pub fn from_samples(samples: Vec<(u64, f32)>) -> Self {
        Self::from_samples_with_bucket(samples, DEFAULT_TOD_BUCKET_SECS)
    }

    /// As [`Self::from_samples`] but with an explicit tier-3 time-of-day
    /// bucket width in seconds (clamped to `[1, 86400]`).
    pub fn from_samples_with_bucket(mut samples: Vec<(u64, f32)>, bucket_secs: u32) -> Self {
        let bucket_secs = bucket_secs.clamp(1, SECS_PER_DAY);
        // ceil-div so the last (partial) bucket still exists when bucket_secs
        // doesn't evenly divide the day; sec/bucket is clamped to the last.
        let n_buckets = SECS_PER_DAY.div_ceil(bucket_secs) as usize;
        let mut tod_buckets: Vec<Vec<f32>> = vec![Vec::new(); n_buckets];
        let mut by_tod: Vec<(u32, f32)> = Vec::with_capacity(samples.len());
        let (mut min_e, mut max_e) = (u64::MAX, 0u64);
        for &(epoch_ms, rtt) in &samples {
            min_e = min_e.min(epoch_ms);
            max_e = max_e.max(epoch_ms);
            let sec_of_day = ((epoch_ms / 1000) % SECS_PER_DAY as u64) as u32;
            by_tod.push((sec_of_day, rtt));
            let b = ((sec_of_day / bucket_secs) as usize).min(n_buckets - 1);
            tod_buckets[b].push(rtt);
        }
        if samples.is_empty() {
            min_e = 0;
            max_e = 0;
        }
        // Sort the three views.
        samples.sort_by_key(|&(e, _)| e);
        by_tod.sort_by_key(|&(s, _)| s);
        for b in tod_buckets.iter_mut() {
            b.sort_by(|a, c| a.partial_cmp(c).unwrap_or(std::cmp::Ordering::Equal));
        }
        let nearest_nonempty_bucket = Self::build_nearest_nonempty(&tod_buckets);
        // Per-day views for the date-aware fallback. `samples` is sorted by
        // epoch above, so each calendar day's rows are contiguous.
        let days = Self::build_days(&samples, bucket_secs, n_buckets);
        Self {
            by_epoch: samples,
            by_tod,
            tod_buckets,
            nearest_nonempty_bucket,
            days,
            bucket_secs,
            min_epoch_ms: min_e,
            max_epoch_ms: max_e,
        }
    }

    /// Group epoch-sorted `samples` into per-calendar-day [`DayRecords`]
    /// (sorted by day ascending). Each day gets its own time-of-day views,
    /// mirroring the pooled build but restricted to that day's rows.
    fn build_days(samples: &[(u64, f32)], bucket_secs: u32, n_buckets: usize) -> Vec<DayRecords> {
        let mut days: Vec<DayRecords> = Vec::new();
        let mut i = 0usize;
        while i < samples.len() {
            let day = samples[i].0 / MS_PER_DAY;
            let mut by_tod: Vec<(u32, f32)> = Vec::new();
            let mut tod_buckets: Vec<Vec<f32>> = vec![Vec::new(); n_buckets];
            while i < samples.len() && samples[i].0 / MS_PER_DAY == day {
                let (epoch_ms, rtt) = samples[i];
                let sec = ((epoch_ms / 1000) % SECS_PER_DAY as u64) as u32;
                by_tod.push((sec, rtt));
                let b = ((sec / bucket_secs) as usize).min(n_buckets - 1);
                tod_buckets[b].push(rtt);
                i += 1;
            }
            by_tod.sort_by_key(|&(s, _)| s);
            for b in tod_buckets.iter_mut() {
                b.sort_by(|a, c| a.partial_cmp(c).unwrap_or(std::cmp::Ordering::Equal));
            }
            let nearest_nonempty_bucket = Self::build_nearest_nonempty(&tod_buckets);
            let dow = ny_weekday(day * MS_PER_DAY + MS_PER_DAY / 2);
            days.push(DayRecords { day, dow, by_tod, tod_buckets, nearest_nonempty_bucket });
        }
        days
    }

    /// Recorded-day indices ordered by proximity to `now_ms` for the
    /// date-aware fallback: `(weekday mismatch, |Δ days|, day)` — so the
    /// chosen primary is the nearest day (under `NearestDayDow`: the nearest
    /// SAME-NY-weekday day, falling back to the nearest day when no same-
    /// weekday day exists), with deterministic tie-breaks.
    fn day_order(&self, now_ms: u64, fallback: RecordReplayFallback) -> Vec<usize> {
        let now_day = (now_ms / MS_PER_DAY) as i64;
        let now_dow = ny_weekday(now_ms);
        let mut idx: Vec<usize> = (0..self.days.len()).collect();
        idx.sort_by_key(|&i| {
            let d = &self.days[i];
            let cal = (d.day as i64 - now_day).unsigned_abs();
            let dow_penalty = match fallback {
                RecordReplayFallback::NearestDayDow => (d.dow != now_dow) as u64,
                _ => 0,
            };
            (dow_penalty, cal, d.day)
        });
        idx
    }

    /// Precompute, for every tod bucket, the nearest bucket (by circular
    /// distance over the bucket ring) holding at least one sample. Returns
    /// an empty vector when there are no samples at all.
    fn build_nearest_nonempty(tod_buckets: &[Vec<f32>]) -> Vec<u32> {
        let n = tod_buckets.len();
        let nonempty: Vec<usize> = (0..n).filter(|&b| !tod_buckets[b].is_empty()).collect();
        if nonempty.is_empty() {
            return Vec::new();
        }
        let mut out = vec![0u32; n];
        for (b, slot) in out.iter_mut().enumerate() {
            let mut best = nonempty[0];
            let mut best_d = circular_dist(b, best, n);
            for &cand in &nonempty[1..] {
                let d = circular_dist(b, cand, n);
                if d < best_d {
                    best_d = d;
                    best = cand;
                }
            }
            *slot = best as u32;
        }
        out
    }

    /// Number of recorded samples on this side.
    #[inline]
    pub fn n(&self) -> usize {
        self.by_epoch.len()
    }

    pub fn min_epoch_ms(&self) -> u64 {
        self.min_epoch_ms
    }
    pub fn max_epoch_ms(&self) -> u64 {
        self.max_epoch_ms
    }

    /// Resolve an RTT (ms) for an order placed at `now_ms` (Unix-epoch
    /// ms), using `u ∈ [0,1)` only when the lookup falls to the tier-3
    /// distribution draw. See the module doc for the three tiers.
    ///
    /// Returns `None` only when the side is empty (caller falls back to
    /// the analytic model).
    pub fn lookup(&self, now_ms: u64, u: f64, params: &RecordReplayParams) -> Option<f64> {
        if self.by_epoch.is_empty() {
            return None;
        }

        // ── Tier 1: exact wall-clock ──────────────────────────────────
        // Only when the instant is inside the recorded calendar window
        // (± abs_tol). Inside that window, snap to the nearest sample.
        if now_ms.saturating_add(params.abs_tol_ms) >= self.min_epoch_ms
            && now_ms <= self.max_epoch_ms.saturating_add(params.abs_tol_ms)
        {
            let idx = nearest_by_epoch(&self.by_epoch, now_ms);
            let (e, rtt) = self.by_epoch[idx];
            if abs_diff(e, now_ms) <= params.abs_tol_ms {
                return Some(rtt as f64);
            }
        }

        let now_sec = ((now_ms / 1000) % SECS_PER_DAY as u64) as u32;

        // Date-aware fallback: restrict tiers 2/3 to recorded days ordered by
        // calendar proximity (see `day_order`). Tier 1 above is unchanged.
        if params.fallback != RecordReplayFallback::Pooled && !self.days.is_empty() {
            let order = self.day_order(now_ms, params.fallback);
            // ── Tier 2 (date-aware): nearest day with a within-tol tod sample.
            for &di in &order {
                let day = &self.days[di];
                if let Some((d, rtt)) = nearest_by_tod(&day.by_tod, now_sec) {
                    if d <= params.tod_tol_secs {
                        return Some(rtt as f64);
                    }
                }
            }
            // ── Tier 3 (date-aware): the nearest day's tod-bucket distribution.
            let day = &self.days[order[0]];
            let b = ((now_sec / self.bucket_secs) as usize).min(day.tod_buckets.len() - 1);
            let nb = day.nearest_nonempty_bucket[b] as usize;
            let bucket = &day.tod_buckets[nb];
            let len = bucket.len();
            let q = (u.clamp(0.0, 1.0) * len as f64).floor() as usize;
            return Some(bucket[q.min(len - 1)] as f64);
        }

        // ── Tier 2 (pooled): same time-of-day, nearest sample ─────────
        if let Some((d, rtt)) = nearest_by_tod(&self.by_tod, now_sec) {
            if d <= params.tod_tol_secs {
                return Some(rtt as f64);
            }
        }

        // ── Tier 3 (pooled): nearest time-of-day bucket distribution ──
        let b = ((now_sec / self.bucket_secs) as usize).min(self.tod_buckets.len() - 1);
        let nb = self.nearest_nonempty_bucket[b] as usize;
        let bucket = &self.tod_buckets[nb];
        // `bucket` is non-empty by construction of nearest_nonempty.
        let len = bucket.len();
        let q = (u.clamp(0.0, 1.0) * len as f64).floor() as usize;
        let idx = q.min(len - 1);
        Some(bucket[idx] as f64)
    }
}

/// Both sides loaded from a latency-record directory.
#[derive(Debug)]
pub struct RecordReplayData {
    pub place: std::sync::Arc<SideRecords>,
    pub cancel: std::sync::Arc<SideRecords>,
    /// Count of CSV files read.
    pub n_files: usize,
}

impl RecordReplayData {
    /// Load every `*.csv` under `dir` into place/cancel record sets.
    /// Rows are split on the `kind` column (`probe_place`/`probe_cancel`
    /// fold into place/cancel); rows with an unparseable
    /// `epoch_ms`/`rtt_ms`, a non-finite or out-of-range RTT, or an
    /// unrecognised `kind` are skipped. All `status` values are kept —
    /// `timeout` rows carry the censored ~client-timeout RTT, which is a
    /// real tail sample.
    ///
    /// ⚠ Known contamination: live-box CSVs recorded before the probe's
    /// poly_1271 signing fix (≤ 2026-07-13, instance zhu*) carry the
    /// probe legs as `kind=place status=http_400` fast-reject rows
    /// (~36 ms validation short-circuits, ~40% of place rows) that bias
    /// the replay place distribution low. Record-box CSVs (gnosis_safe
    /// recorder01) are clean. Prefer recorder CSVs, or pre-filter zhu
    /// `http_400` rows out of pre-fix live CSVs before replaying them.
    ///
    /// `bucket_secs` sets the tier-3 time-of-day bucket width (e.g. 300 =
    /// 5 min); see [`SideRecords::from_samples_with_bucket`].
    pub fn load_dir(dir: &Path, bucket_secs: u32) -> std::io::Result<Self> {
        use std::io::{BufRead, BufReader};

        // Deterministic file order (sorted) so repeated loads are stable.
        let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                p.extension().and_then(|x| x.to_str()).map(|x| x.eq_ignore_ascii_case("csv"))
                    .unwrap_or(false)
            })
            .collect();
        files.sort();

        let mut place: Vec<(u64, f32)> = Vec::new();
        let mut cancel: Vec<(u64, f32)> = Vec::new();
        for path in &files {
            let f = std::fs::File::open(path)?;
            for line in BufReader::new(f).lines() {
                let line = match line { Ok(l) => l, Err(_) => continue };
                // Skip the header and blank lines.
                if line.is_empty() || line.starts_with("epoch_ms") {
                    continue;
                }
                // epoch_ms,iso_local,instance_id,kind,rtt_ms,status
                let mut it = line.split(',');
                let epoch_ms = match it.next().and_then(|s| s.trim().parse::<u64>().ok()) {
                    Some(v) => v,
                    None => continue,
                };
                let _iso = it.next();
                let _iid = it.next();
                let kind = match it.next() {
                    Some(k) => k.trim(),
                    None => continue,
                };
                let rtt_ms = match it.next().and_then(|s| s.trim().parse::<f64>().ok()) {
                    Some(v) => v,
                    None => continue,
                };
                // Sanity: drop non-finite / negative / absurd RTTs (> 60 s).
                if !rtt_ms.is_finite() || rtt_ms < 0.0 || rtt_ms > 60_000.0 {
                    continue;
                }
                let sample = (epoch_ms, rtt_ms as f32);
                // `probe_place` / `probe_cancel` are the RTT probe's
                // synthetic resting place + cancel legs — same endpoints
                // and pools as real order flow, recorded under their own
                // kind so offline analysis can separate them. For replay
                // they are valid latency samples: fold into place/cancel
                // (record-mode CSVs consist of nothing else).
                match kind {
                    "place" | "probe_place" => place.push(sample),
                    "cancel" | "probe_cancel" => cancel.push(sample),
                    _ => {}
                }
            }
        }

        Ok(Self {
            place: std::sync::Arc::new(SideRecords::from_samples_with_bucket(place, bucket_secs)),
            cancel: std::sync::Arc::new(SideRecords::from_samples_with_bucket(cancel, bucket_secs)),
            n_files: files.len(),
        })
    }
}

/// Circular distance between two bucket indices on a ring of `n` buckets.
#[inline]
fn circular_dist(a: usize, b: usize, n: usize) -> usize {
    let d = if a >= b { a - b } else { b - a };
    d.min(n - d)
}

/// Circular distance (in seconds) between two second-of-day values.
#[inline]
fn circular_sec_dist(a: u32, b: u32) -> u32 {
    let d = if a >= b { a - b } else { b - a };
    d.min(SECS_PER_DAY - d)
}

#[inline]
fn abs_diff(a: u64, b: u64) -> u64 {
    if a >= b { a - b } else { b - a }
}

/// Index of the sample in `by_epoch` (sorted by epoch) nearest to `x`.
fn nearest_by_epoch(by_epoch: &[(u64, f32)], x: u64) -> usize {
    match by_epoch.binary_search_by_key(&x, |&(e, _)| e) {
        Ok(i) => i,
        Err(i) => {
            if i == 0 {
                0
            } else if i >= by_epoch.len() {
                by_epoch.len() - 1
            } else if x - by_epoch[i - 1].0 <= by_epoch[i].0 - x {
                i - 1
            } else {
                i
            }
        }
    }
}

/// Nearest sample by **circular** second-of-day distance. Returns
/// `(distance_secs, rtt_ms)` or `None` when empty. Checks the binary-
/// search neighbours plus the wrap-around ends (first/last), which is
/// sufficient because circular nearest on a sorted ring is always one of
/// those four candidates.
fn nearest_by_tod(by_tod: &[(u32, f32)], now_sec: u32) -> Option<(u32, f32)> {
    if by_tod.is_empty() {
        return None;
    }
    let mut best: Option<(u32, f32)> = None;
    let mut consider = |idx: usize| {
        let (s, rtt) = by_tod[idx];
        let d = circular_sec_dist(s, now_sec);
        if best.map(|(bd, _)| d < bd).unwrap_or(true) {
            best = Some((d, rtt));
        }
    };
    let pos = by_tod.partition_point(|&(s, _)| s < now_sec);
    if pos < by_tod.len() {
        consider(pos);
    }
    if pos > 0 {
        consider(pos - 1);
    }
    // Wrap-around ends close the ring.
    consider(0);
    consider(by_tod.len() - 1);
    best
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(day: u64, h: u64, m: u64, s: u64) -> u64 {
        // day = days since epoch; build a deterministic epoch_ms.
        ((day * 86_400 + h * 3_600 + m * 60 + s) * 1000) as u64
    }

    #[test]
    fn empty_side_returns_none() {
        let r = SideRecords::from_samples(vec![]);
        assert_eq!(r.n(), 0);
        assert!(r.lookup(123_456, 0.5, &RecordReplayParams::default()).is_none());
    }

    #[test]
    fn tier1_exact_wallclock_snaps_to_nearest() {
        // One recorded day; samples every 10 s with rtt = second-of-day.
        let mut s = Vec::new();
        for sec in (0..120).step_by(10) {
            s.push((ms(100, 0, 0, sec), sec as f32));
        }
        let r = SideRecords::from_samples(s);
        let p = RecordReplayParams { abs_tol_ms: 5_000, tod_tol_secs: 1, ..Default::default() };
        // Order at 00:00:33 → nearest sample is the 30 s one (rtt 30),
        // |Δ| = 3 s ≤ 5 s tol → tier 1.
        let got = r.lookup(ms(100, 0, 0, 33), 0.99, &p).unwrap();
        assert_eq!(got, 30.0, "tier1 should snap to nearest recorded sample");
    }

    #[test]
    fn tier1_outside_window_falls_through() {
        // Sample only on day 100; query day 200 same clock time → outside
        // the abs window, so tier 1 must not fire (tier 2 handles it).
        let r = SideRecords::from_samples(vec![(ms(100, 1, 0, 0), 42.0)]);
        let p = RecordReplayParams { abs_tol_ms: 60_000, tod_tol_secs: 120, ..Default::default() };
        // Day 200, 01:00:05 → tier 2 (same tod, 5 s away) returns 42.
        let got = r.lookup(ms(200, 1, 0, 5), 0.5, &p).unwrap();
        assert_eq!(got, 42.0, "tier2 reuses same time-of-day sample across days");
    }

    #[test]
    fn tier2_picks_closest_time_of_day() {
        // Two samples at the same clock minute on different days; an order
        // on a third day should map to whichever is closer in tod.
        let r = SideRecords::from_samples(vec![
            (ms(100, 8, 0, 10), 11.0), // tod = 08:00:10
            (ms(101, 8, 0, 50), 22.0), // tod = 08:00:50
        ]);
        let p = RecordReplayParams { abs_tol_ms: 0, tod_tol_secs: 300, ..Default::default() };
        // Order at day 300, 08:00:15 → closest tod is 08:00:10 (5 s) vs
        // 08:00:50 (35 s) → 11.0.
        assert_eq!(r.lookup(ms(300, 8, 0, 15), 0.5, &p).unwrap(), 11.0);
        // Order at 08:00:45 → closest is 08:00:50 → 22.0.
        assert_eq!(r.lookup(ms(300, 8, 0, 45), 0.5, &p).unwrap(), 22.0);
    }

    #[test]
    fn tier2_circular_wraparound() {
        // Sample near end-of-day; an order just after midnight is closest
        // to it via the wrap-around.
        let r = SideRecords::from_samples(vec![(ms(100, 23, 59, 50), 7.0)]);
        let p = RecordReplayParams { abs_tol_ms: 0, tod_tol_secs: 60, ..Default::default() };
        // 00:00:05 is 15 s (circular) from 23:59:50 → within 60 s tol.
        assert_eq!(r.lookup(ms(300, 0, 0, 5), 0.5, &p).unwrap(), 7.0);
    }

    #[test]
    fn tier3_nearest_bucket_distribution() {
        // Populate the 08:00–08:05 bucket with a spread of RTTs; query a
        // far-away time-of-day (12:00) with no nearby samples → tier 3 draws
        // the u-quantile of the nearest non-empty bucket (08:00).
        let mut s = Vec::new();
        for (i, rtt) in [10.0f32, 20.0, 30.0, 40.0, 50.0].iter().enumerate() {
            s.push((ms(100, 8, 0, (i * 5) as u64), *rtt));
        }
        let r = SideRecords::from_samples(s);
        // tod_tol small so 12:00 doesn't match 08:00 in tier 2.
        let p = RecordReplayParams { abs_tol_ms: 0, tod_tol_secs: 60, ..Default::default() };
        // u = 0.0 → smallest (10), u≈0.99 → largest (50), u=0.5 → 30.
        assert_eq!(r.lookup(ms(300, 12, 0, 0), 0.0, &p).unwrap(), 10.0);
        assert_eq!(r.lookup(ms(300, 12, 0, 0), 0.99, &p).unwrap(), 50.0);
        assert_eq!(r.lookup(ms(300, 12, 0, 0), 0.5, &p).unwrap(), 30.0);
    }

    #[test]
    fn tier3_pools_across_the_five_minute_bucket() {
        // Samples land in DIFFERENT minutes but the SAME 5-min bucket
        // (08:00–08:05): 08:00:30, 08:02:00, 08:04:30. Tier 3 must pool all
        // three (a 1-min bucket would have isolated them). Query 20:00 (far)
        // → nearest non-empty bucket is the 08:00 one with all three values.
        let r = SideRecords::from_samples_with_bucket(
            vec![
                (ms(100, 8, 0, 30), 10.0),
                (ms(100, 8, 2, 0), 20.0),
                (ms(100, 8, 4, 30), 30.0),
            ],
            300,
        );
        let p = RecordReplayParams { abs_tol_ms: 0, tod_tol_secs: 60, ..Default::default() };
        assert_eq!(r.lookup(ms(300, 20, 0, 0), 0.0, &p).unwrap(), 10.0);
        assert_eq!(r.lookup(ms(300, 20, 0, 0), 0.5, &p).unwrap(), 20.0);
        assert_eq!(r.lookup(ms(300, 20, 0, 0), 0.99, &p).unwrap(), 30.0);
        // A 1-min bucket would NOT pool these: the nearest non-empty minute
        // to 20:00 is 08:04 (single sample 30.0), so every u → 30.0.
        let r1 = SideRecords::from_samples_with_bucket(
            vec![
                (ms(100, 8, 0, 30), 10.0),
                (ms(100, 8, 2, 0), 20.0),
                (ms(100, 8, 4, 30), 30.0),
            ],
            60,
        );
        assert_eq!(r1.lookup(ms(300, 20, 0, 0), 0.0, &p).unwrap(), 30.0);
    }

    #[test]
    fn load_dir_splits_place_and_cancel() {
        let dir = std::env::temp_dir().join(format!("hexbot_rr_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let csv = "epoch_ms,iso_local,instance_id,kind,rtt_ms,status\n\
                   1781568000967,2026-06-16T00:00:00.967,recorder01,place,31.898,ok\n\
                   1781568000998,2026-06-16T00:00:00.998,recorder01,cancel,30.037,ok\n\
                   1781568001100,2026-06-16T00:00:01.100,recorder01,place,99999999.0,ok\n\
                   1781568001200,2026-06-16T00:00:01.200,recorder01,place,40.0,timeout\n";
        std::fs::write(dir.join("20260616.csv"), csv).unwrap();
        let data = RecordReplayData::load_dir(&dir, DEFAULT_TOD_BUCKET_SECS).unwrap();
        assert_eq!(data.n_files, 1);
        // 2 valid place (the 1e8 ms row is dropped as absurd), 1 cancel.
        assert_eq!(data.place.n(), 2);
        assert_eq!(data.cancel.n(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── Date-aware fallback (NearestDay / NearestDayDow) ──────────────

    /// Epoch ms at a real calendar date + UTC time-of-day (so NY week-type
    /// classification is meaningful for the dow tests).
    fn at(y: i32, mo: u32, d: u32, h: u64, mi: u64, s: u64) -> u64 {
        let day = (chrono::NaiveDate::from_ymd_opt(y, mo, d).unwrap()
            - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as u64;
        (day * 86_400 + h * 3_600 + mi * 60 + s) * 1000
    }

    #[test]
    fn fallback_from_str_parses() {
        use RecordReplayFallback::*;
        assert_eq!(RecordReplayFallback::from_str("pooled"), Pooled);
        assert_eq!(RecordReplayFallback::from_str("nearest_day"), NearestDay);
        assert_eq!(RecordReplayFallback::from_str("nearest-day"), NearestDay);
        assert_eq!(RecordReplayFallback::from_str("nearest_day_dow"), NearestDayDow);
        assert_eq!(RecordReplayFallback::from_str("garbage"), Pooled); // safe default
    }

    /// Tier 2 date-aware: among days that recorded this time-of-day, the
    /// CALENDAR-NEAREST day wins — even when a farther day has a closer tod.
    /// Pooled would pick the closer-tod (farther-day) sample instead.
    #[test]
    fn nearest_day_tier2_prefers_calendar_near_day() {
        let r = SideRecords::from_samples(vec![
            (at(2026, 6, 18, 8, 0, 0), 11.0),  // near day, tod 25 s from query
            (at(2026, 6, 15, 8, 0, 30), 22.0), // far day, tod 5 s from query
        ]);
        let now = at(2026, 6, 19, 8, 0, 25); // outside [06-15..06-18] → no tier 1
        let pooled = RecordReplayParams { abs_tol_ms: 0, tod_tol_secs: 60, fallback: RecordReplayFallback::Pooled, ..Default::default() };
        let nd = RecordReplayParams { abs_tol_ms: 0, tod_tol_secs: 60, fallback: RecordReplayFallback::NearestDay, ..Default::default() };
        // Pooled: closest tod across all days = 08:00:30 (day 06-15) → 22.
        assert_eq!(r.lookup(now, 0.5, &pooled).unwrap(), 22.0);
        // NearestDay: nearest calendar day 06-18 → its 08:00:00 (25 s) → 11.
        assert_eq!(r.lookup(now, 0.5, &nd).unwrap(), 11.0);
    }

    /// Tier 3 date-aware: distribution comes from the nearest day ONLY,
    /// not pooled across all days.
    #[test]
    fn nearest_day_tier3_uses_nearest_day_distribution() {
        let r = SideRecords::from_samples(vec![
            (at(2026, 6, 18, 8, 0, 0), 10.0),
            (at(2026, 6, 18, 8, 1, 0), 20.0),
            (at(2026, 6, 18, 8, 2, 0), 30.0),
            (at(2026, 6, 10, 8, 0, 0), 100.0),
            (at(2026, 6, 10, 8, 1, 0), 200.0),
            (at(2026, 6, 10, 8, 2, 0), 300.0),
        ]);
        let now = at(2026, 6, 19, 20, 0, 0); // far tod → tier 3; nearest day = 06-18
        let nd = RecordReplayParams { abs_tol_ms: 0, tod_tol_secs: 60, fallback: RecordReplayFallback::NearestDay, ..Default::default() };
        let pooled = RecordReplayParams { abs_tol_ms: 0, tod_tol_secs: 60, fallback: RecordReplayFallback::Pooled, ..Default::default() };
        // NearestDay: only 06-18's bucket [10,20,30].
        assert_eq!(r.lookup(now, 0.0, &nd).unwrap(), 10.0);
        assert_eq!(r.lookup(now, 0.99, &nd).unwrap(), 30.0);
        // Pooled: both days merged [10,20,30,100,200,300].
        assert_eq!(r.lookup(now, 0.0, &pooled).unwrap(), 10.0);
        assert_eq!(r.lookup(now, 0.99, &pooled).unwrap(), 300.0);
    }

    /// NearestDayDow prefers a same-NY-weekday recorded day over a
    /// calendar-CLOSER day of a different weekday.
    #[test]
    fn nearest_day_dow_prefers_same_weekday() {
        // 06-20 = Saturday (rtt 20); 06-12 = Friday (rtt 30, same DOW as query).
        let r = SideRecords::from_samples(vec![
            (at(2026, 6, 20, 12, 0, 0), 20.0),
            (at(2026, 6, 12, 12, 0, 0), 30.0),
        ]);
        let now = at(2026, 6, 19, 12, 0, 0); // Friday; nearest calendar day = Sat 06-20
        let nd = RecordReplayParams { abs_tol_ms: 0, tod_tol_secs: 60, fallback: RecordReplayFallback::NearestDay, ..Default::default() };
        let dow = RecordReplayParams { abs_tol_ms: 0, tod_tol_secs: 60, fallback: RecordReplayFallback::NearestDayDow, ..Default::default() };
        // NearestDay: calendar-nearest is Sat 06-20 (1 day) → 20.
        assert_eq!(r.lookup(now, 0.5, &nd).unwrap(), 20.0);
        // NearestDayDow: query is Friday → prefer the same-weekday Fri 06-12 (30,
        // 7 days away) over the closer-but-Saturday 06-20.
        assert_eq!(r.lookup(now, 0.5, &dow).unwrap(), 30.0);
    }

    /// NearestDayDow falls back to the nearest day when NO recorded day
    /// shares the query's weekday.
    #[test]
    fn nearest_day_dow_falls_back_to_nearest_when_no_same_weekday() {
        // 06-20 = Saturday (rtt 20); 06-16 = Tuesday (rtt 30). Query Friday has
        // no same-weekday recorded day → nearest day (Sat 06-20) wins.
        let r = SideRecords::from_samples(vec![
            (at(2026, 6, 20, 12, 0, 0), 20.0),
            (at(2026, 6, 16, 12, 0, 0), 30.0),
        ]);
        let now = at(2026, 6, 19, 12, 0, 0); // Friday
        let dow = RecordReplayParams { abs_tol_ms: 0, tod_tol_secs: 60, fallback: RecordReplayFallback::NearestDayDow, ..Default::default() };
        assert_eq!(r.lookup(now, 0.5, &dow).unwrap(), 20.0);
    }

    /// With a single recorded day, all fallback modes equal Pooled (sanity:
    /// date-awareness only changes multi-day behaviour).
    #[test]
    fn single_day_all_modes_agree() {
        let r = SideRecords::from_samples(vec![
            (at(2026, 6, 18, 8, 0, 0), 10.0),
            (at(2026, 6, 18, 8, 1, 0), 20.0),
        ]);
        let now = at(2026, 6, 19, 20, 0, 0);
        for fb in [RecordReplayFallback::Pooled, RecordReplayFallback::NearestDay, RecordReplayFallback::NearestDayDow] {
            let p = RecordReplayParams { abs_tol_ms: 0, tod_tol_secs: 60, fallback: fb, ..Default::default() };
            assert_eq!(r.lookup(now, 0.0, &p).unwrap(), 10.0);
            assert_eq!(r.lookup(now, 0.99, &p).unwrap(), 20.0);
        }
    }
}
