//! OS-level latency tuning: CPU pinning, SCHED_FIFO real-time scheduling,
//! memory locking.
//!
//! Operations are best-effort by default. With `strict_core_isolation=true`,
//! topology/env conflicts fail startup and Linux affinity, SCHED_FIFO, or
//! selective strategy-stack locking failures abort the process rather than trading with degraded
//! tail-latency guarantees. On non-Linux platforms (macOS dev machines) the
//! affinity / real-time calls remain no-ops so the binary compiles and runs
//! without privileges.
//!
//! ## Core plan
//!
//! The plan is resolved once at startup from the `[os_tune]` TOML block
//! (via `init_from_config`). Missing values fall back to the legacy 4-core
//! defaults so small dev / test hosts keep working:
//!
//!   - `BACKGROUND = 0`  (system + IRQs + non-critical I/O)
//!   - `ASYNC_RT   = 1`  (`hexbot-async-rt`)
//!   - `STRATEGY   = 2`  (`strategy`)
//!   - `EXECUTION  = 3`  (`execution`, `feed-*`, hex worker pool)
//!
//! On larger hosts the TOML can fan out the `EXECUTION` slot into
//! per-feed + per-worker cores, which is the biggest single tail-latency
//! win on 16+ core boxes (no more binance / coinbase / chainlink feeds
//! serializing through one core). Example for AWS c7gn.4xlarge (16 vCPU):
//!
//! ```toml
//! [os_tune]
//! async_rt_core    = 2
//! async_clob_core  = 5
//! strategy_core    = 3
//! execution_core   = 4
//! feed_cores       = { polymarket = 5, binance = 6, binance_futures = 7, coinbase = 8, chainlink = 9 }
//! hex_worker_cores = [10]
//! background_cores = [0, 1]
//! ```
//!
//! Routing inside `pin_execution(name)`:
//!   - `feed-<exchange>` → `feed_cores[<exchange>]` (fallback: `execution_core`)
//!   - `<inst_id>-worker-<i>` → round-robin `hex_worker_cores` (fallback: `execution_core`)
//!   - everything else → `execution_core`
//!
//! ### Host-side one-time config (Linux, for the 16-core plan above)
//! ```bash
//! # /etc/default/grub — isolate cores 2-10 from the kernel scheduler
//! GRUB_CMDLINE_LINUX="... isolcpus=2-10 nohz_full=2-10 rcu_nocbs=2-10 \
//!     rcu_nocb_poll irqaffinity=0-1 nowatchdog nosoftlockup \
//!     nmi_watchdog=0 mce=ignore_ce skew_tick=1"
//! grub2-mkconfig -o /boot/grub2/grub.cfg && reboot
//! cpupower frequency-set -g performance
//! systemctl disable --now irqbalance    # otherwise it re-spreads IRQs
//! echo 0003 > /sys/class/net/eth0/queues/rx-0/rps_cpus   # RPS to cores 0-1
//! ```
//!
//! systemd unit grants caps without running as root:
//! ```ini
//! [Service]
//! AmbientCapabilities=CAP_SYS_NICE CAP_IPC_LOCK
//! LimitMEMLOCK=infinity
//! ```

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

#[cfg(target_os = "linux")]
use std::sync::atomic::AtomicBool;

#[allow(unused_imports)]
use log::{error, info, warn};

use crate::config::OsTuneConfig;

// ── Legacy 4-core defaults (used when no [os_tune] block is present) ──
const DEFAULT_BACKGROUND_CORE: usize = 0;
const DEFAULT_ASYNC_RT_CORE: usize = 1;
const DEFAULT_STRATEGY_CORE: usize = 2;
const DEFAULT_EXECUTION_CORE: usize = 3;

const DEFAULT_PRIO_ASYNC_RT: u8 = 70;
const DEFAULT_PRIO_STRATEGY: u8 = 60;
const DEFAULT_PRIO_EXECUTION: u8 = 50;

/// Pages of future strategy stack to fault in before the first quote.  The
/// default pthread stack is materially larger than this, while 512 KiB covers
/// the observed strategy call depth with headroom and avoids eagerly making
/// every process mapping resident.
#[cfg(target_os = "linux")]
const STRATEGY_STACK_PRETOUCH_BYTES: usize = 512 * 1024;
#[cfg(target_os = "linux")]
const STACK_PRETOUCH_FRAME_BYTES: usize = 32 * 1024;
#[cfg(target_os = "linux")]
const STRATEGY_STACK_LOCK_GUARD_BYTES: usize = 64 * 1024;

/// Resolved at startup from `OsTuneConfig`. All fields are concrete
/// core ids / priorities; optional config entries have been filled in
/// with legacy defaults.
#[derive(Debug, Clone)]
pub struct CorePlan {
    pub enable_pin: bool,
    pub enable_fifo: bool,
    pub strict_core_isolation: bool,
    pub allow_background_on_execution_core: bool,
    pub allow_strategy_router_on_execution_core: bool,
    pub allow_private_apply_on_completion_core: bool,
    pub async_rt: usize,
    /// Dedicated public-CLOB socket runtime core. `None` preserves the legacy
    /// placement beside `feed-polymarket`.
    pub async_clob: Option<usize>,
    /// Core for the order-I/O runtime thread (`hexbot-async-ord`).
    /// `None` = leave the thread unpinned at normal priority (safe
    /// default — pinning it onto an already-claimed core with FIFO
    /// would starve one of the two).
    pub async_ord: Option<usize>,
    pub strategy: usize,
    /// Per-instance strategy-worker cores (live/paper): `instance_id →
    /// core`. Every strategy runs in its own worker thread; entries here also
    /// place those workers on dedicated cores. Instances absent from this map
    /// fall back to `strategy`.
    pub strategy_cores: HashMap<String, usize>,
    /// Per-account private order/trade application cores.
    pub private_apply_cores: HashMap<String, usize>,
    /// Per-account cold ledger/lifecycle cores. These stay SCHED_OTHER and
    /// must be disjoint from the FIFO private-apply cores in strict mode.
    pub private_cold_cores: HashMap<String, usize>,
    pub execution: usize,
    pub feed_cores: HashMap<String, usize>,
    pub hex_worker_cores: Vec<usize>,
    /// Round-robin pool for Polymarket dispatch workers (`poly-exec-<i>`).
    /// Empty = fall back to `execution`.
    pub poly_exec_cores: Vec<usize>,
    /// Must-complete cancel dispatch workers. Empty config inherits exec.
    pub poly_cancel_cores: Vec<usize>,
    /// Response completion/accounting workers. Empty config inherits exec.
    pub poly_completion_cores: Vec<usize>,
    pub background_cores: Vec<usize>,
    pub fifo_async_rt: u8,
    pub fifo_strategy: u8,
    pub fifo_execution: u8,
    pub fifo_polymarket_feed: u8,
    pub fifo_completion: u8,
}

impl CorePlan {
    fn legacy_default() -> Self {
        Self {
            enable_pin: true,
            enable_fifo: true,
            strict_core_isolation: false,
            allow_background_on_execution_core: false,
            allow_strategy_router_on_execution_core: false,
            allow_private_apply_on_completion_core: false,
            async_rt: DEFAULT_ASYNC_RT_CORE,
            async_clob: None,
            async_ord: None,
            strategy: DEFAULT_STRATEGY_CORE,
            strategy_cores: HashMap::new(),
            private_apply_cores: HashMap::new(),
            private_cold_cores: HashMap::new(),
            execution: DEFAULT_EXECUTION_CORE,
            feed_cores: HashMap::new(),
            hex_worker_cores: Vec::new(),
            poly_exec_cores: Vec::new(),
            poly_cancel_cores: Vec::new(),
            poly_completion_cores: Vec::new(),
            background_cores: vec![DEFAULT_BACKGROUND_CORE],
            fifo_async_rt: DEFAULT_PRIO_ASYNC_RT,
            fifo_strategy: DEFAULT_PRIO_STRATEGY,
            fifo_execution: DEFAULT_PRIO_EXECUTION,
            fifo_polymarket_feed: DEFAULT_PRIO_EXECUTION,
            fifo_completion: DEFAULT_PRIO_EXECUTION,
        }
    }

    fn from_config(cfg: &OsTuneConfig) -> Self {
        let bg = if cfg.background_cores.is_empty() {
            vec![DEFAULT_BACKGROUND_CORE]
        } else {
            cfg.background_cores.clone()
        };
        let poly_cancel_cores = if cfg.poly_cancel_cores.is_empty() {
            cfg.poly_exec_cores.clone()
        } else {
            cfg.poly_cancel_cores.clone()
        };
        let poly_completion_cores = if cfg.poly_completion_cores.is_empty() {
            cfg.poly_exec_cores.clone()
        } else {
            cfg.poly_completion_cores.clone()
        };
        Self {
            enable_pin: cfg.enable_pin,
            enable_fifo: cfg.enable_fifo,
            strict_core_isolation: cfg.strict_core_isolation,
            allow_background_on_execution_core: cfg.allow_background_on_execution_core,
            allow_strategy_router_on_execution_core: cfg
                .allow_strategy_router_on_execution_core,
            allow_private_apply_on_completion_core: cfg
                .allow_private_apply_on_completion_core,
            async_rt: cfg.async_rt_core.unwrap_or(DEFAULT_ASYNC_RT_CORE),
            async_clob: cfg.async_clob_core,
            async_ord: cfg.async_ord_core,
            strategy: cfg.strategy_core.unwrap_or(DEFAULT_STRATEGY_CORE),
            strategy_cores: cfg.strategy_cores.clone(),
            private_apply_cores: cfg.private_apply_cores.clone(),
            private_cold_cores: cfg.private_cold_cores.clone(),
            execution: cfg.execution_core.unwrap_or(DEFAULT_EXECUTION_CORE),
            feed_cores: cfg.feed_cores.clone(),
            hex_worker_cores: cfg.hex_worker_cores.clone(),
            poly_exec_cores: cfg.poly_exec_cores.clone(),
            poly_cancel_cores,
            poly_completion_cores,
            background_cores: bg,
            fifo_async_rt: cfg.fifo_async_rt.unwrap_or(DEFAULT_PRIO_ASYNC_RT),
            fifo_strategy: cfg.fifo_strategy.unwrap_or(DEFAULT_PRIO_STRATEGY),
            fifo_execution: cfg.fifo_execution.unwrap_or(DEFAULT_PRIO_EXECUTION),
            fifo_polymarket_feed: cfg
                .fifo_polymarket_feed
                .or(cfg.fifo_execution)
                .unwrap_or(DEFAULT_PRIO_EXECUTION),
            fifo_completion: cfg
                .fifo_completion
                .or(cfg.fifo_execution)
                .unwrap_or(DEFAULT_PRIO_EXECUTION),
        }
    }

    /// Route an execution-tier thread to its core based on name:
    ///   - `feed-<exchange>`           → `feed_cores[<exchange>]` else execution
    ///   - `poly-exec-<i>`   → `poly_exec_cores`
    ///   - `poly-cancel-<i>` → `poly_cancel_cores`
    ///   - `poly-done-<i>`   → `poly_completion_cores`
    ///   - `<inst_id>-worker-<i>`      → round-robin `hex_worker_cores` else execution
    ///   - anything else               → execution
    fn route_execution(&self, thread_name: &str) -> usize {
        if let Some(ex) = thread_name.strip_prefix("feed-") {
            if let Some(&core) = self.feed_cores.get(ex) {
                return core;
            }
        }
        if thread_name.starts_with("poly-exec-") && !self.poly_exec_cores.is_empty() {
            let i = POLY_EXEC_RR.fetch_add(1, Ordering::Relaxed) % self.poly_exec_cores.len();
            return self.poly_exec_cores[i];
        }
        if thread_name.starts_with("poly-cancel-") && !self.poly_cancel_cores.is_empty() {
            let i = POLY_CANCEL_RR.fetch_add(1, Ordering::Relaxed) % self.poly_cancel_cores.len();
            return self.poly_cancel_cores[i];
        }
        if thread_name.starts_with("poly-done-") && !self.poly_completion_cores.is_empty() {
            let i = POLY_COMPLETION_RR.fetch_add(1, Ordering::Relaxed)
                % self.poly_completion_cores.len();
            return self.poly_completion_cores[i];
        }
        if thread_name.contains("-worker-") && !self.hex_worker_cores.is_empty() {
            let i = HEX_WORKER_RR.fetch_add(1, Ordering::Relaxed) % self.hex_worker_cores.len();
            return self.hex_worker_cores[i];
        }
        self.execution
    }

    /// Validate the production invariant that every enabled strategy worker
    /// has a genuinely dedicated CPU and no latency-critical runtime role can
    /// silently fall back onto it. Public feeds may intentionally share one
    /// core with each other (for example Coinbase + Chainlink); worker pools
    /// may intentionally contain many threads per configured pool core.
    fn validate_strategy_isolation(&self, instance_ids: &[String]) -> Result<(), String> {
        if !self.strict_core_isolation {
            return Ok(());
        }
        if !self.enable_pin {
            return Err("strict_core_isolation requires enable_pin=true".into());
        }
        if !self.enable_fifo {
            return Err("strict_core_isolation requires enable_fifo=true".into());
        }
        if self.async_ord.is_none() {
            return Err("strict_core_isolation requires async_ord_core".into());
        }
        let distinct_poly_cores: HashSet<_> = self
            .poly_exec_cores
            .iter()
            .chain(&self.poly_cancel_cores)
            .chain(&self.poly_completion_cores)
            .copied()
            .collect();
        if distinct_poly_cores.len() < 2 {
            return Err(
                "strict_core_isolation requires at least two distinct Polymarket dispatch/completion cores".into(),
            );
        }
        let dispatch_cores: HashSet<_> = self
            .poly_exec_cores
            .iter()
            .chain(&self.poly_cancel_cores)
            .copied()
            .collect();
        if self
            .poly_completion_cores
            .iter()
            .any(|core| dispatch_cores.contains(core))
        {
            return Err(
                "strict_core_isolation requires poly_completion_cores to be disjoint from place/cancel dispatch cores"
                    .into(),
            );
        }

        let mut exclusive: HashMap<usize, String> = HashMap::new();
        let claim = |core: usize,
                     role: String,
                     claims: &mut HashMap<usize, String>|
         -> Result<(), String> {
            if let Some(existing) = claims.insert(core, role.clone()) {
                return Err(format!(
                    "core {} is assigned to both {} and {}",
                    core, existing, role
                ));
            }
            Ok(())
        };

        claim(self.async_rt, "async_rt".into(), &mut exclusive)?;
        if let Some(core) = self.async_clob {
            claim(core, "async_clob".into(), &mut exclusive)?;
        }
        claim(self.async_ord.unwrap(), "async_ord".into(), &mut exclusive)?;
        claim(self.execution, "execution".into(), &mut exclusive)?;
        if self.strategy == self.execution {
            if !self.allow_strategy_router_on_execution_core {
                return Err(
                    "strategy router overlaps execution_core without allow_strategy_router_on_execution_core=true"
                        .into(),
                );
            }
        } else {
            claim(
                self.strategy,
                "strategy_router_or_fallback".into(),
                &mut exclusive,
            )?;
        }

        let mut seen_ids = HashSet::new();
        for instance_id in instance_ids {
            if !seen_ids.insert(instance_id) {
                return Err(format!(
                    "duplicate enabled strategy instance_id `{}`",
                    instance_id
                ));
            }
            let core = self
                .strategy_cores
                .get(instance_id)
                .copied()
                .ok_or_else(|| {
                    format!(
                        "enabled strategy `{}` has no strategy_cores entry",
                        instance_id
                    )
                })?;
            claim(core, format!("strategy:{instance_id}"), &mut exclusive)?;
        }

        let mut private_apply: Vec<_> = self.private_apply_cores.iter().collect();
        private_apply.sort_by(|left, right| left.0.cmp(right.0));
        for (account_id, &core) in private_apply {
            claim(
                core,
                format!("private_account_apply:{account_id}"),
                &mut exclusive,
            )?;
            let cold_core = self.private_cold_cores.get(account_id).copied().ok_or_else(|| {
                format!(
                    "strict_core_isolation requires private_cold_cores entry for account `{account_id}`"
                )
            })?;
            claim(
                cold_core,
                format!("private_account_cold:{account_id}"),
                &mut exclusive,
            )?;
        }
        for account_id in self.private_cold_cores.keys() {
            if !self.private_apply_cores.contains_key(account_id) {
                return Err(format!(
                    "private_cold_cores account `{account_id}` has no private_apply_cores entry"
                ));
            }
        }

        // Feed-to-feed sharing is allowed, but a feed may not overlap an
        // exclusive role. Deduplicate values before checking shared feeds.
        let feed_cores: HashSet<usize> = self.feed_cores.values().copied().collect();
        for core in &feed_cores {
            if let Some(role) = exclusive.get(core) {
                return Err(format!("feed core {} overlaps {}", core, role));
            }
        }

        let mut poly_cores = HashSet::new();
        for (&core, pool_name) in self
            .poly_exec_cores
            .iter()
            .map(|core| (core, "poly_exec_cores"))
            .chain(
                self.poly_cancel_cores
                    .iter()
                    .map(|core| (core, "poly_cancel_cores")),
            )
            .chain(
                self.poly_completion_cores
                    .iter()
                    .map(|core| (core, "poly_completion_cores")),
            )
        {
            // Sharing between dispatch roles is explicit and supported; only
            // duplicate entries inside one list are configuration mistakes.
            let list = match pool_name {
                "poly_exec_cores" => &self.poly_exec_cores,
                "poly_cancel_cores" => &self.poly_cancel_cores,
                _ => &self.poly_completion_cores,
            };
            if list.iter().filter(|&&candidate| candidate == core).count() > 1 {
                return Err(format!("{} contains duplicate core {}", pool_name, core));
            }
            if !poly_cores.insert(core) {
                continue;
            }
            if let Some(role) = exclusive.get(&core) {
                let allowed_private_completion = pool_name == "poly_completion_cores"
                    && self.allow_private_apply_on_completion_core
                    && role.starts_with("private_account_apply:");
                if !allowed_private_completion {
                    return Err(format!("poly-exec/done core {} overlaps {}", core, role));
                }
            }
            if feed_cores.contains(&core) {
                return Err(format!("poly-exec/done core {} overlaps a feed core", core));
            }
        }

        let mut hex_cores = HashSet::new();
        for &core in &self.hex_worker_cores {
            if !hex_cores.insert(core) {
                return Err(format!("hex_worker_cores contains duplicate core {}", core));
            }
            if let Some(role) = exclusive.get(&core) {
                return Err(format!("hex-worker core {} overlaps {}", core, role));
            }
            if feed_cores.contains(&core) || poly_cores.contains(&core) {
                return Err(format!(
                    "hex-worker core {} overlaps another worker pool",
                    core
                ));
            }
        }

        let mut latency_cores: HashSet<usize> = exclusive.keys().copied().collect();
        latency_cores.extend(feed_cores);
        latency_cores.extend(poly_cores);
        latency_cores.extend(hex_cores);
        for &core in &self.background_cores {
            if latency_cores.contains(&core) {
                if self.allow_background_on_execution_core && core == self.execution {
                    continue;
                }
                return Err(format!(
                    "background core {} overlaps a latency-critical core",
                    core
                ));
            }
        }
        Ok(())
    }

    /// Round-robin a background thread across `background_cores` so
    /// 16-core hosts can spread recorder / join / heartbeat threads
    /// over 2 or more IRQ cores.
    fn route_background(&self) -> usize {
        let n = self.background_cores.len().max(1);
        let i = BACKGROUND_RR.fetch_add(1, Ordering::Relaxed) % n;
        *self
            .background_cores
            .get(i)
            .unwrap_or(&DEFAULT_BACKGROUND_CORE)
    }
}

static CORE_PLAN: OnceLock<CorePlan> = OnceLock::new();
static HEX_WORKER_RR: AtomicUsize = AtomicUsize::new(0);
static POLY_EXEC_RR: AtomicUsize = AtomicUsize::new(0);
static POLY_CANCEL_RR: AtomicUsize = AtomicUsize::new(0);
static POLY_COMPLETION_RR: AtomicUsize = AtomicUsize::new(0);
static BACKGROUND_RR: AtomicUsize = AtomicUsize::new(0);
#[cfg(target_os = "linux")]
static SELECTIVE_MEMORY_LOCKING: AtomicBool = AtomicBool::new(false);

/// Install the CorePlan resolved from the TOML `[os_tune]` block. Must be
/// called once at process startup, **before** any thread calls
/// `pin_async_rt`, `pin_execution`, etc. Idempotent — later calls are
/// silently ignored so test harnesses can call it multiple times.
pub fn init_from_config(cfg: &OsTuneConfig) {
    let plan = CorePlan::from_config(cfg);
    // Emit a one-shot summary so operators can grep for "core plan" and
    // cross-check against `/proc/cmdline` isolcpus.
    info!(
        "[os_tune] core plan: async_rt={} async_clob={:?} async_ord={:?} strategy={} execution={} feeds={:?} private_apply={:?} private_cold={:?} hex_workers={:?} poly_exec={:?} poly_cancel={:?} poly_completion={:?} background={:?} fifo(async={} strat={} exec={} poly_feed={} completion={}) enable_pin={} enable_fifo={} strict_isolation={} allow_background_on_execution={} allow_strategy_router_on_execution={} allow_private_apply_on_completion={}",
        plan.async_rt, plan.async_clob, plan.async_ord, plan.strategy, plan.execution,
        plan.feed_cores, plan.private_apply_cores, plan.private_cold_cores,
        plan.hex_worker_cores,
        plan.poly_exec_cores, plan.poly_cancel_cores, plan.poly_completion_cores,
        plan.background_cores,
        plan.fifo_async_rt, plan.fifo_strategy, plan.fifo_execution,
        plan.fifo_polymarket_feed, plan.fifo_completion,
        plan.enable_pin, plan.enable_fifo, plan.strict_core_isolation,
        plan.allow_background_on_execution_core,
        plan.allow_strategy_router_on_execution_core,
        plan.allow_private_apply_on_completion_core,
    );
    let _ = CORE_PLAN.set(plan);
}

/// Install a CorePlan with CPU pinning **and** SCHED_FIFO disabled. For CLI
/// subcommands (`positions`, `redeem`, …) — quick read-only / one-shot ops
/// that must not grab the reserved cores or real-time priority the live bot
/// uses. Idempotent (later `set` ignored), and silent (no log line) since CLI
/// runs suppress logging. Call instead of `init_from_config`.
pub fn init_disabled() {
    let mut plan = CorePlan::legacy_default();
    plan.enable_pin = false;
    plan.enable_fifo = false;
    let _ = CORE_PLAN.set(plan);
}

/// Fail-fast validation for production live/paper per-instance topology.
/// No-op unless `[os_tune].strict_core_isolation = true`.
/// Call after [`init_from_config`] and before spawning runtime/feed threads.
pub fn validate_strategy_isolation(instance_ids: &[String]) -> Result<(), String> {
    let p = plan();
    p.validate_strategy_isolation(instance_ids)?;
    if p.strict_core_isolation {
        for name in [
            "HEXBOT_NO_PIN",
            "HEXBOT_NO_PIN_ASYNC_RT",
            "HEXBOT_NO_PIN_STRATEGY",
            "HEXBOT_NO_PIN_EXECUTION",
            "HEXBOT_NO_PIN_BACKGROUND",
            "HEXBOT_NO_FIFO",
        ] {
            if std::env::var(name).ok().as_deref() == Some("1") {
                return Err(format!(
                    "strict_core_isolation is incompatible with {}=1",
                    name
                ));
            }
        }
    }
    Ok(())
}

fn plan() -> &'static CorePlan {
    CORE_PLAN.get_or_init(CorePlan::legacy_default)
}

#[cfg(target_os = "linux")]
fn abort_if_strict(message: &str) {
    if plan().strict_core_isolation {
        error!(
            "[os_tune] strict core isolation failed: {}; aborting before degraded scheduling can trade",
            message,
        );
        std::process::abort();
    }
}

/// Pin the current thread to a specific CPU core.
///
/// Linux: uses `sched_setaffinity` via `core_affinity`. Succeeds even
/// without elevated privileges for cores inside the process's allowed set.
///
/// ### Opt-outs (env vars, take precedence over config)
/// - `HEXBOT_NO_PIN=1`            — disable ALL pinning
/// - `HEXBOT_NO_PIN_ASYNC_RT=1`   — don't pin the tokio runtime thread
/// - `HEXBOT_NO_PIN_STRATEGY=1`   — don't pin the strategy thread
/// - `HEXBOT_NO_PIN_EXECUTION=1`  — don't pin execution-tier workers
///                                  (execution, feed-*, per-instance pool)
/// - `HEXBOT_NO_PIN_BACKGROUND=1` — don't pin background-tier workers
///
/// macOS / other: no-op; the OS only advertises best-effort affinity and
/// `isolcpus` doesn't exist.
pub fn pin_current(core_id: usize, thread_name: &str) {
    if std::env::var("HEXBOT_NO_PIN").ok().as_deref() == Some("1") {
        return;
    }
    if !plan().enable_pin {
        return;
    }
    // Fine-grained opt-outs per tier. Matched against the resolved core id.
    let p = plan();
    let skip = if core_id == p.async_rt || p.async_clob == Some(core_id) {
        "HEXBOT_NO_PIN_ASYNC_RT"
    } else if p.private_apply_cores.values().any(|&c| c == core_id) {
        // A disabled strategy may leave its configured core available for a
        // live account-apply worker.  Classify that worker by its active role,
        // not by the dormant strategy_cores entry for the same CPU.
        "HEXBOT_NO_PIN_EXECUTION"
    } else if core_id == p.strategy || p.strategy_cores.values().any(|&c| c == core_id) {
        "HEXBOT_NO_PIN_STRATEGY"
    } else if core_id == p.execution
        || p.feed_cores.values().any(|&c| c == core_id)
        || p.hex_worker_cores.iter().any(|&c| c == core_id)
        || p.poly_exec_cores.iter().any(|&c| c == core_id)
        || p.poly_cancel_cores.iter().any(|&c| c == core_id)
        || p.poly_completion_cores.iter().any(|&c| c == core_id)
    {
        "HEXBOT_NO_PIN_EXECUTION"
    } else if p.background_cores.iter().any(|&c| c == core_id) {
        "HEXBOT_NO_PIN_BACKGROUND"
    } else {
        ""
    };
    if !skip.is_empty() && std::env::var(skip).ok().as_deref() == Some("1") {
        info!(
            "[os_tune] Pin '{}' → core {} SKIPPED ({}=1)",
            thread_name, core_id, skip
        );
        return;
    }
    #[cfg(target_os = "linux")]
    {
        let target = core_affinity::CoreId { id: core_id };
        // Include TID so operators can cross-check with
        // `ps -eLo pid,tid,comm,psr,cls,state | grep hexbot`.
        let tid = unsafe { libc::syscall(libc::SYS_gettid) };
        if core_affinity::set_for_current(target) {
            info!(
                "[os_tune] Pinned '{}' (tid={}) → core {}",
                thread_name, tid, core_id
            );
        } else {
            warn!(
                "[os_tune] Pin '{}' (tid={}) → core {} FAILED (core out of range or affinity mask restricted)",
                thread_name, tid, core_id,
            );
            abort_if_strict(&format!(
                "pin '{}' (tid={}) to core {}",
                thread_name, tid, core_id
            ));
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (core_id, thread_name);
    }
}

/// Switch the current thread to `SCHED_FIFO` with the given priority.
/// Priority range is 1 (lowest) to 99 (highest). `CAP_SYS_NICE` required.
///
/// **Opt-out**: set `HEXBOT_NO_FIFO=1` to skip. Useful when:
///   - container / cgroup can't grant `CAP_SYS_NICE`
///   - kernel has `rt_runtime_us` throttling tight enough to starve
///   - debugging whether FIFO is implicated in a specific issue
///
/// Failure is logged and falls back to SCHED_OTHER by default. Under
/// `strict_core_isolation`, it aborts the process before degraded scheduling
/// can trade.
pub fn set_fifo(priority: u8, thread_name: &str) {
    if std::env::var("HEXBOT_NO_FIFO").ok().as_deref() == Some("1") {
        return;
    }
    if !plan().enable_fifo {
        return;
    }
    #[cfg(target_os = "linux")]
    {
        let param = libc::sched_param {
            sched_priority: priority as i32,
        };
        let rc =
            unsafe { libc::pthread_setschedparam(libc::pthread_self(), libc::SCHED_FIFO, &param) };
        if rc == 0 {
            info!("[os_tune] SCHED_FIFO prio={} → '{}'", priority, thread_name);
        } else {
            let err = std::io::Error::from_raw_os_error(rc);
            warn!(
                "[os_tune] SCHED_FIFO prio={} for '{}' failed: {} (need CAP_SYS_NICE; \
                 falling back to SCHED_OTHER — tail latency guarantees degraded)",
                priority, thread_name, err,
            );
            abort_if_strict(&format!(
                "SCHED_FIFO prio={} for '{}': {}",
                priority, thread_name, err
            ));
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (priority, thread_name);
    }
}

/// Pin the tokio async-runtime thread to its dedicated core with
/// `PRIO_ASYNC_RT`. Called from `async_rt::init`.
pub fn pin_async_rt(thread_name: &str) {
    let p = plan();
    pin_current(p.async_rt, thread_name);
    set_fifo(p.fifo_async_rt, thread_name);
}

/// Pin the order-I/O runtime thread (`hexbot-async-ord`) when
/// `async_ord_core` is configured; otherwise leave it floating at
/// normal priority. Deliberately NOT defaulted onto `async_rt`'s core:
/// two SCHED_FIFO threads at equal priority on one core would let a
/// feed batch on the general runtime starve order wakeups — the exact
/// head-of-line problem the split removes.
pub fn pin_async_ord(thread_name: &str) {
    let p = plan();
    if let Some(core) = p.async_ord {
        pin_current(core, thread_name);
        set_fifo(p.fifo_async_rt, thread_name);
    }
}

/// Pin the dedicated public-CLOB reader beside the synchronous Polymarket
/// feed-drain thread when that feed core is configured.  The reader runs at
/// async-runtime FIFO priority while the drain thread runs at execution
/// priority, so socket polling preempts downstream decoding/drain work and
/// immediately yields again after bridging events.  With no explicit
/// Polymarket feed core, leave the reader unpinned at normal priority rather
/// than silently colliding with another FIFO role.
pub fn pin_async_clob(thread_name: &str) {
    let p = plan();
    if let Some(core) = p
        .async_clob
        .or_else(|| p.feed_cores.get("polymarket").copied())
    {
        pin_current(core, thread_name);
        set_fifo(p.fifo_async_rt, thread_name);
    }
}

/// Pin the strategy decision thread to its dedicated core with
/// `PRIO_STRATEGY`.
pub fn pin_strategy(thread_name: &str) {
    let p = plan();
    pin_current(p.strategy, thread_name);
    set_fifo(p.fifo_strategy, thread_name);
    lock_and_pretouch_strategy_stack(thread_name);
}

/// Pin a per-instance strategy worker thread (live/paper). Resolves
/// `strategy_cores[instance_id]` for a dedicated core, else falls back to the
/// shared `strategy` core. Uses `PRIO_STRATEGY` FIFO priority.
pub fn pin_strategy_instance(thread_name: &str, instance_id: &str) {
    let p = plan();
    let core = p
        .strategy_cores
        .get(instance_id)
        .copied()
        .unwrap_or(p.strategy);
    pin_current(core, thread_name);
    set_fifo(p.fifo_strategy, thread_name);
    lock_and_pretouch_strategy_stack(thread_name);
}

#[cfg(any(target_os = "linux", test))]
fn strategy_stack_lock_range(
    stack_base: usize,
    stack_size: usize,
    stack_pointer: usize,
    requested_bytes: usize,
    page_size: usize,
) -> Option<(usize, usize)> {
    if stack_size == 0 || requested_bytes == 0 || page_size == 0 {
        return None;
    }
    let stack_end = stack_base.checked_add(stack_size)?;
    if !(stack_base..stack_end).contains(&stack_pointer) {
        return None;
    }

    // pthread stacks grow down on supported Linux targets. Include the page
    // containing SP and a bounded future region below it, clamped to the
    // usable stack mapping returned by pthread_getattr_np.
    let raw_start = stack_pointer
        .saturating_sub(requested_bytes)
        .max(stack_base);
    let start = raw_start
        .saturating_sub(raw_start % page_size)
        .max(stack_base);
    let raw_end = stack_pointer.saturating_add(1).min(stack_end);
    let end = raw_end
        .checked_add(page_size - 1)?
        .checked_div(page_size)?
        .checked_mul(page_size)?
        .min(stack_end);
    (end > start).then_some((start, end - start))
}

#[cfg(target_os = "linux")]
fn lock_current_strategy_stack(thread_name: &str) {
    if !SELECTIVE_MEMORY_LOCKING.load(Ordering::Acquire) {
        return;
    }

    let marker = 0_u8;
    let stack_pointer = std::ptr::addr_of!(marker) as usize;
    let (stack_base, stack_size) = unsafe {
        let mut attr = std::mem::MaybeUninit::<libc::pthread_attr_t>::uninit();
        let get_attr_rc = libc::pthread_getattr_np(libc::pthread_self(), attr.as_mut_ptr());
        if get_attr_rc != 0 {
            let error = std::io::Error::from_raw_os_error(get_attr_rc);
            warn!(
                "[os_tune] inspect strategy stack for '{}' failed: {}",
                thread_name, error,
            );
            abort_if_strict(&format!(
                "inspect strategy stack for '{}': {}",
                thread_name, error
            ));
            return;
        }

        let mut stack_addr: *mut libc::c_void = std::ptr::null_mut();
        let mut stack_size = 0_usize;
        let get_stack_rc =
            libc::pthread_attr_getstack(attr.as_ptr(), &mut stack_addr, &mut stack_size);
        let destroy_rc = libc::pthread_attr_destroy(attr.as_mut_ptr());
        if get_stack_rc != 0 {
            let error = std::io::Error::from_raw_os_error(get_stack_rc);
            warn!(
                "[os_tune] resolve strategy stack for '{}' failed: {}",
                thread_name, error,
            );
            abort_if_strict(&format!(
                "resolve strategy stack for '{}': {}",
                thread_name, error
            ));
            return;
        }
        if destroy_rc != 0 {
            warn!(
                "[os_tune] pthread_attr_destroy for '{}' failed: {}",
                thread_name,
                std::io::Error::from_raw_os_error(destroy_rc),
            );
        }
        (stack_addr as usize, stack_size)
    };

    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    let page_size = if page_size > 0 {
        page_size as usize
    } else {
        4_096
    };
    let requested_bytes =
        STRATEGY_STACK_PRETOUCH_BYTES.saturating_add(STRATEGY_STACK_LOCK_GUARD_BYTES);
    let Some((lock_start, lock_len)) = strategy_stack_lock_range(
        stack_base,
        stack_size,
        stack_pointer,
        requested_bytes,
        page_size,
    ) else {
        warn!(
            "[os_tune] strategy stack range for '{}' is invalid: base=0x{:x} size={} sp=0x{:x}",
            thread_name, stack_base, stack_size, stack_pointer,
        );
        abort_if_strict(&format!(
            "invalid strategy stack range for '{}'",
            thread_name
        ));
        return;
    };

    // mlock2(MLOCK_ONFAULT) changes only this explicitly bounded mapping.
    // Fall back to eager mlock of the same small range on pre-4.4 kernels.
    let onfault_rc = unsafe {
        libc::syscall(
            libc::SYS_mlock2,
            lock_start as *const libc::c_void,
            lock_len,
            libc::MLOCK_ONFAULT,
        )
    };
    let method = if onfault_rc == 0 {
        Some("mlock2(MLOCK_ONFAULT)")
    } else {
        let onfault_error = std::io::Error::last_os_error();
        if matches!(
            onfault_error.raw_os_error(),
            Some(libc::ENOSYS) | Some(libc::EINVAL)
        ) {
            let fallback_rc = unsafe { libc::mlock(lock_start as *const libc::c_void, lock_len) };
            if fallback_rc == 0 {
                Some("mlock")
            } else {
                let fallback_error = std::io::Error::last_os_error();
                warn!(
                    "[os_tune] selective strategy stack lock for '{}' failed: onfault={}; fallback={}",
                    thread_name, onfault_error, fallback_error,
                );
                abort_if_strict(&format!(
                    "selective strategy stack lock for '{}': {}",
                    thread_name, fallback_error
                ));
                None
            }
        } else {
            warn!(
                "[os_tune] selective strategy stack lock for '{}' failed: {}",
                thread_name, onfault_error,
            );
            abort_if_strict(&format!(
                "selective strategy stack lock for '{}': {}",
                thread_name, onfault_error
            ));
            None
        }
    };
    if let Some(method) = method {
        info!(
            "[os_tune] locked strategy stack '{}' via {}: {} KiB (whole-process heap remains pageable)",
            thread_name,
            method,
            lock_len / 1024,
        );
    }
}

/// Lock the bounded future portion of the current strategy stack and fault it
/// in once before the event loop. Cold file mappings, recorder buffers and
/// allocator arenas remain pageable.
fn lock_and_pretouch_strategy_stack(thread_name: &str) {
    #[cfg(target_os = "linux")]
    lock_current_strategy_stack(thread_name);
    #[cfg(not(target_os = "linux"))]
    let _ = thread_name;
    pretouch_strategy_stack();
}

/// Grow and fault the future portion of the current strategy stack once at
/// thread startup.
fn pretouch_strategy_stack() {
    #[cfg(target_os = "linux")]
    {
        #[inline(never)]
        fn touch(remaining: usize) {
            let mut frame = [0_u8; STACK_PRETOUCH_FRAME_BYTES];
            let page_size = 4096;
            for offset in (0..frame.len()).step_by(page_size) {
                // Volatile writes keep every page touch observable in release
                // builds; keeping `frame` live across recursion forces real
                // stack growth instead of reusing one frame.
                unsafe { std::ptr::write_volatile(frame.as_mut_ptr().add(offset), 0) };
            }
            if remaining > STACK_PRETOUCH_FRAME_BYTES {
                touch(remaining - STACK_PRETOUCH_FRAME_BYTES);
            }
            std::hint::black_box(&frame);
        }

        touch(STRATEGY_STACK_PRETOUCH_BYTES);
    }
}

/// Pin the authenticated private account-apply worker to its account-specific
/// core.  It uses completion priority: private fills should preempt order
/// signing and housekeeping, while public market-data and strategy decisions
/// retain their higher priorities.
pub fn pin_private_account_apply(thread_name: &str, account_id: &str) {
    let p = plan();
    if let Some(core) = p.private_apply_cores.get(account_id).copied() {
        pin_current(core, thread_name);
        set_fifo(p.fifo_completion, thread_name);
    } else {
        pin_background(thread_name);
    }
}

/// Shared-ledger/audit/persistence half of the private feed. Keep it
/// SCHED_OTHER on an account-specific cold CPU that is disjoint from the FIFO
/// owner-fast worker. Co-locating them lets a private-event microburst preempt
/// the cold writer for the whole burst, producing 20-50ms account/lifecycle
/// tails even when the host is otherwise idle.
pub fn pin_private_account_cold(thread_name: &str, account_id: &str) {
    demote_current_to_other(thread_name);
    let p = plan();
    if let Some(core) = p.private_cold_cores.get(account_id).copied() {
        pin_current(core, thread_name);
    } else if let Some(core) = p.private_apply_cores.get(account_id).copied() {
        // Backwards-compatible non-strict fallback. Strict topology
        // validation requires a disjoint private_cold_cores entry.
        pin_current(core, thread_name);
    } else {
        let core = p.route_background();
        pin_current(core, thread_name);
    }
}

/// Pin a critical execution-path thread (`execution` dispatcher,
/// `feed-*`, per-instance hex worker pool) with `PRIO_EXECUTION`.
/// Routing:
///   - `feed-<exchange>`              → `feed_cores[<exchange>]` else execution
///   - `poly-exec-*`   → round-robin `poly_exec_cores`
///   - `poly-cancel-*` → round-robin `poly_cancel_cores`
///   - `poly-done-*`   → round-robin `poly_completion_cores`
///   - `<inst_id>-worker-<i>`         → round-robin `hex_worker_cores` else execution
///   - anything else          → `execution_core`
pub fn pin_execution(thread_name: &str) {
    let p = plan();
    let core = p.route_execution(thread_name);
    pin_current(core, thread_name);
    let priority = if thread_name == "feed-polymarket" {
        p.fifo_polymarket_feed
    } else if thread_name.starts_with("poly-done-") {
        p.fifo_completion
    } else {
        p.fifo_execution
    };
    set_fifo(priority, thread_name);
}

/// Pin a non-critical I/O-bound background thread to the background
/// pool. `SCHED_OTHER` (no FIFO). Use for: recorder (flushes every
/// 60 s), latency-dump, paper-exec, async-task joiner threads
/// (poly-heartbeat-join, poly-user-feed-join, hex-user-feed-join).
pub fn pin_background(thread_name: &str) {
    // pthreads inherit their creator's scheduling policy. Several background
    // join/persistence workers are spawned by FIFO runtime threads, so affinity
    // alone is not enough: explicitly demote before sharing an execution CPU.
    demote_current_to_other(thread_name);
    let core = plan().route_background();
    pin_current(core, thread_name);
}

fn demote_current_to_other(_thread_name: &str) {
    #[cfg(target_os = "linux")]
    {
        let param = libc::sched_param { sched_priority: 0 };
        let rc =
            unsafe { libc::pthread_setschedparam(libc::pthread_self(), libc::SCHED_OTHER, &param) };
        if rc != 0 {
            let err = std::io::Error::from_raw_os_error(rc);
            warn!(
                "[os_tune] failed to demote background '{}' to SCHED_OTHER: {}",
                _thread_name, err,
            );
            abort_if_strict(&format!(
                "demote background '{}' to SCHED_OTHER: {}",
                _thread_name, err
            ));
        }
    }
}

/// Pin the main (bootstrap) thread + any children spawned before
/// `init_from_config` runs to a small "housekeeping" CPU set.
///
/// Why separate from `pin_background` / `pin_current`:
///   - Must fire BEFORE `tracing_appender::non_blocking` spawns its
///     worker so the worker inherits the same mask; that call happens
///     at the very top of `main()`, well before config loads.
///   - `pin_current` would lazy-init `CORE_PLAN` via `get_or_init`,
///     locking it into legacy defaults and making `init_from_config`
///     (called a few lines later) a no-op. This function bypasses
///     `CORE_PLAN` entirely, so `init_from_config` can still install
///     the real plan.
///
/// Default mask = cores {0, 1} for backwards compatibility. Production
/// deployments that reserve those CPUs exclusively for the kernel/IRQs must
/// set `HEXBOT_BOOTSTRAP_CORE` to a bot-owned core. Respects
/// `HEXBOT_NO_PIN=1`.
pub fn pin_main_early(thread_name: &str) {
    if std::env::var("HEXBOT_NO_PIN").ok().as_deref() == Some("1") {
        return;
    }
    #[cfg(target_os = "linux")]
    {
        let configured_core = std::env::var("HEXBOT_BOOTSTRAP_CORE")
            .ok()
            .and_then(|value| value.parse::<usize>().ok());
        let cores: Vec<usize> = configured_core.map_or_else(|| vec![0_usize, 1], |core| vec![core]);
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            for &c in &cores {
                libc::CPU_SET(c, &mut set);
            }
            let rc = libc::sched_setaffinity(
                0, // current thread
                std::mem::size_of::<libc::cpu_set_t>(),
                &set,
            );
            if rc == 0 {
                let tid = libc::syscall(libc::SYS_gettid);
                info!(
                    "[os_tune] Pinned '{}' (tid={}) → cores {:?} (early, pre-config)",
                    thread_name, tid, cores,
                );
            } else {
                let err = std::io::Error::last_os_error();
                warn!(
                    "[os_tune] pin_main_early '{}' failed: {} — main thread and tracing-appender worker \
                     will keep the process affinity inherited from the service manager",
                    thread_name, err,
                );
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = thread_name;
    }
}

/// Enable bounded locking for strategy-thread stacks.
///
/// Deliberately does not call `mlockall`: `MCL_FUTURE | MCL_ONFAULT` still
/// locks every future heap page after its first touch, which makes recorder
/// batches and allocator high-water arenas permanently resident over a long
/// run. [`pin_strategy`] and [`pin_strategy_instance`] instead lock and
/// pre-touch only their own bounded stack ranges.
pub fn configure_selective_memory_locking() {
    #[cfg(target_os = "linux")]
    {
        SELECTIVE_MEMORY_LOCKING.store(true, Ordering::Release);
        info!(
            "[os_tune] selective memory locking enabled: strategy stacks only; \
             recorder/Arrow/mimalloc/future heap remain pageable"
        );
    }
    #[cfg(not(target_os = "linux"))]
    {}
}

/// Backward-compatible entry point. It now enables selective strategy-stack
/// locking and never registers the whole process with `mlockall`.
pub fn mlockall_best_effort() {
    configure_selective_memory_locking();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selective_stack_lock_range_is_page_aligned_and_bounded() {
        let base = 0x1000;
        let size = 0x10_000;
        let (start, len) =
            strategy_stack_lock_range(base, size, 0xf123, 0x4000, 0x1000).expect("valid range");
        assert_eq!(start, 0xb000);
        assert_eq!(len, 0x5000);
        assert_eq!(start % 0x1000, 0);
        assert_eq!(len % 0x1000, 0);
        assert!(start >= base);
        assert!(start + len <= base + size);

        let (clamped_start, clamped_len) =
            strategy_stack_lock_range(base, size, 0x1800, 0x4000, 0x1000)
                .expect("lower-edge range");
        assert_eq!((clamped_start, clamped_len), (base, 0x1000));
        assert!(strategy_stack_lock_range(base, size, base + size, 0x4000, 0x1000).is_none());
        assert!(strategy_stack_lock_range(base, size, 0x1800, 0x4000, 0).is_none());
    }

    fn five_instance_config() -> OsTuneConfig {
        let mut cfg = OsTuneConfig::default();
        cfg.strict_core_isolation = true;
        cfg.async_ord_core = Some(2);
        cfg.async_rt_core = Some(3);
        cfg.execution_core = Some(4);
        cfg.strategy_core = Some(5);
        cfg.feed_cores = HashMap::from([
            ("polymarket".into(), 6),
            ("binance".into(), 7),
            ("coinbase".into(), 8),
            ("chainlink".into(), 8),
        ]);
        cfg.strategy_cores = HashMap::from([
            ("btc01".into(), 9),
            ("btc02".into(), 10),
            ("btc03".into(), 11),
            ("btc04".into(), 12),
            ("btc05".into(), 13),
        ]);
        cfg.poly_exec_cores = vec![14];
        cfg.poly_cancel_cores = vec![14];
        cfg.poly_completion_cores = vec![15];
        cfg.background_cores = vec![0, 1];
        cfg
    }

    fn five_instances() -> Vec<String> {
        (1..=5).map(|i| format!("btc{i:02}")).collect()
    }

    #[test]
    fn strict_plan_accepts_five_dedicated_strategy_cores() {
        let mut cfg = five_instance_config();
        cfg.async_clob_core = Some(16);
        let plan = CorePlan::from_config(&cfg);
        assert_eq!(plan.async_clob, Some(16));
        assert_eq!(plan.validate_strategy_isolation(&five_instances()), Ok(()));
    }

    #[test]
    fn strict_plan_rejects_async_clob_overlap() {
        let mut cfg = five_instance_config();
        cfg.async_clob_core = Some(6);
        let err = CorePlan::from_config(&cfg)
            .validate_strategy_isolation(&five_instances())
            .unwrap_err();
        assert!(err.contains("feed core 6") && err.contains("async_clob"));
    }

    #[test]
    fn strict_plan_allows_private_apply_on_dormant_strategy_cores_only() {
        let mut cfg = five_instance_config();
        cfg.async_clob_core = Some(16);
        cfg.private_apply_cores = HashMap::from([("zhu02".into(), 11), ("zhu03".into(), 12)]);
        cfg.private_cold_cores = HashMap::from([("zhu02".into(), 17), ("zhu03".into(), 18)]);
        let plan = CorePlan::from_config(&cfg);
        let enabled = vec!["btc01".to_string(), "btc02".to_string()];
        assert_eq!(plan.validate_strategy_isolation(&enabled), Ok(()));

        let err = plan
            .validate_strategy_isolation(&five_instances())
            .unwrap_err();
        assert!(
            err.contains("strategy:btc03") && err.contains("private_account_apply:zhu02"),
            "unexpected validation error: {err}",
        );
    }

    #[test]
    fn strict_plan_requires_disjoint_private_cold_core() {
        let mut cfg = five_instance_config();
        cfg.async_clob_core = Some(16);
        cfg.private_apply_cores = HashMap::from([("zhu02".into(), 11)]);
        let enabled = vec!["btc01".to_string(), "btc02".to_string()];

        let missing = CorePlan::from_config(&cfg)
            .validate_strategy_isolation(&enabled)
            .unwrap_err();
        assert!(missing.contains("private_cold_cores entry"));

        cfg.private_cold_cores = HashMap::from([("zhu02".into(), 11)]);
        let overlap = CorePlan::from_config(&cfg)
            .validate_strategy_isolation(&enabled)
            .unwrap_err();
        assert!(
            overlap.contains("private_account_apply:zhu02")
                && overlap.contains("private_account_cold:zhu02"),
            "unexpected validation error: {overlap}",
        );

        cfg.private_cold_cores = HashMap::from([("zhu02".into(), 17)]);
        assert_eq!(
            CorePlan::from_config(&cfg).validate_strategy_isolation(&enabled),
            Ok(())
        );
    }

    #[test]
    fn strict_plan_allows_only_opted_in_router_execution_overlap() {
        let mut cfg = five_instance_config();
        cfg.async_clob_core = Some(16);
        cfg.strategy_core = cfg.execution_core;
        let instances = five_instances();

        let err = CorePlan::from_config(&cfg)
            .validate_strategy_isolation(&instances)
            .unwrap_err();
        assert!(err.contains("allow_strategy_router_on_execution_core"));

        cfg.allow_strategy_router_on_execution_core = true;
        assert_eq!(
            CorePlan::from_config(&cfg).validate_strategy_isolation(&instances),
            Ok(())
        );
    }

    #[test]
    fn strict_plan_allows_private_apply_only_on_opted_in_completion_core() {
        let mut cfg = five_instance_config();
        cfg.async_clob_core = Some(16);
        cfg.private_apply_cores = HashMap::from([("zhu02".into(), 15)]);
        cfg.private_cold_cores = HashMap::from([("zhu02".into(), 17)]);
        let instances = five_instances();

        let err = CorePlan::from_config(&cfg)
            .validate_strategy_isolation(&instances)
            .unwrap_err();
        assert!(err.contains("poly-exec/done core 15"));

        cfg.allow_private_apply_on_completion_core = true;
        assert_eq!(
            CorePlan::from_config(&cfg).validate_strategy_isolation(&instances),
            Ok(())
        );

        cfg.private_apply_cores = HashMap::from([("zhu02".into(), 14)]);
        let dispatch_overlap = CorePlan::from_config(&cfg)
            .validate_strategy_isolation(&instances)
            .unwrap_err();
        assert!(dispatch_overlap.contains("poly-exec/done core 14"));
    }

    #[test]
    fn strict_plan_allows_only_opted_in_background_execution_overlap() {
        let mut cfg = five_instance_config();
        cfg.background_cores = vec![4];
        let instances = five_instances();
        let err = CorePlan::from_config(&cfg)
            .validate_strategy_isolation(&instances)
            .unwrap_err();
        assert!(err.contains("background core 4"));

        cfg.allow_background_on_execution_core = true;
        assert_eq!(
            CorePlan::from_config(&cfg).validate_strategy_isolation(&instances),
            Ok(())
        );

        cfg.background_cores = vec![3];
        let err = CorePlan::from_config(&cfg)
            .validate_strategy_isolation(&instances)
            .unwrap_err();
        assert!(err.contains("background core 3"));
    }

    #[test]
    fn role_specific_execution_priorities_are_resolved() {
        let mut cfg = five_instance_config();
        cfg.fifo_execution = Some(50);
        cfg.fifo_polymarket_feed = Some(71);
        cfg.fifo_completion = Some(55);
        let plan = CorePlan::from_config(&cfg);
        assert_eq!(plan.fifo_execution, 50);
        assert_eq!(plan.fifo_polymarket_feed, 71);
        assert_eq!(plan.fifo_completion, 55);
    }

    #[test]
    fn strict_plan_rejects_missing_or_overlapping_strategy_core() {
        let mut missing = five_instance_config();
        missing.strategy_cores.remove("btc05");
        let err = CorePlan::from_config(&missing)
            .validate_strategy_isolation(&five_instances())
            .unwrap_err();
        assert!(err.contains("btc05") && err.contains("no strategy_cores entry"));

        let mut overlap = five_instance_config();
        overlap.strategy_cores.insert("btc05".into(), 4);
        let err = CorePlan::from_config(&overlap)
            .validate_strategy_isolation(&five_instances())
            .unwrap_err();
        assert!(err.contains("execution") && err.contains("strategy:btc05"));
    }

    #[test]
    fn polymarket_dispatch_and_completion_use_separate_pools() {
        let plan = CorePlan::from_config(&five_instance_config());
        assert!(plan
            .poly_exec_cores
            .contains(&plan.route_execution("poly-exec-0")));
        assert!(plan
            .poly_cancel_cores
            .contains(&plan.route_execution("poly-cancel-0")));
        assert!(plan
            .poly_completion_cores
            .contains(&plan.route_execution("poly-done-0")));
        assert_ne!(
            plan.route_execution("poly-exec-1"),
            plan.route_execution("poly-done-1"),
        );

        let mut overlap = five_instance_config();
        overlap.poly_exec_cores = vec![14, 15];
        overlap.poly_completion_cores = vec![15];
        let err = CorePlan::from_config(&overlap)
            .validate_strategy_isolation(&five_instances())
            .unwrap_err();
        assert!(err.contains("poly_completion_cores") && err.contains("disjoint"));
    }
}
