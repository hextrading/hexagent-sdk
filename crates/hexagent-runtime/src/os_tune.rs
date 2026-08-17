//! OS-level latency tuning: CPU pinning, SCHED_FIFO real-time scheduling,
//! memory locking.
//!
//! Operations are best-effort by default. With `strict_core_isolation=true`,
//! topology/env conflicts fail startup and Linux affinity, SCHED_FIFO, or
//! mlockall failures abort the process rather than trading with degraded
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

/// Resolved at startup from `OsTuneConfig`. All fields are concrete
/// core ids / priorities; optional config entries have been filled in
/// with legacy defaults.
#[derive(Debug, Clone)]
pub struct CorePlan {
    pub enable_pin: bool,
    pub enable_fifo: bool,
    pub strict_core_isolation: bool,
    pub allow_background_on_execution_core: bool,
    pub async_rt: usize,
    /// Core for the order-I/O runtime thread (`hexbot-async-ord`).
    /// `None` = leave the thread unpinned at normal priority (safe
    /// default — pinning it onto an already-claimed core with FIFO
    /// would starve one of the two).
    pub async_ord: Option<usize>,
    pub strategy: usize,
    /// Per-instance strategy-worker cores (live/paper multi-instance):
    /// `instance_id → core`. A polymaker instance whose `instance_id`
    /// is a key here gets its own dedicated core (so co-hosted BTC/ETH
    /// instances run on separate cores and never preempt each other).
    /// Instances absent from this map fall back to `strategy`.
    pub strategy_cores: HashMap<String, usize>,
    pub execution: usize,
    pub feed_cores: HashMap<String, usize>,
    pub hex_worker_cores: Vec<usize>,
    /// Round-robin pool for Polymarket dispatch workers (`poly-exec-<i>`).
    /// Empty = fall back to `execution`.
    pub poly_exec_cores: Vec<usize>,
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
            async_rt: DEFAULT_ASYNC_RT_CORE,
            async_ord: None,
            strategy: DEFAULT_STRATEGY_CORE,
            strategy_cores: HashMap::new(),
            execution: DEFAULT_EXECUTION_CORE,
            feed_cores: HashMap::new(),
            hex_worker_cores: Vec::new(),
            poly_exec_cores: Vec::new(),
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
        Self {
            enable_pin: cfg.enable_pin,
            enable_fifo: cfg.enable_fifo,
            strict_core_isolation: cfg.strict_core_isolation,
            allow_background_on_execution_core: cfg.allow_background_on_execution_core,
            async_rt: cfg.async_rt_core.unwrap_or(DEFAULT_ASYNC_RT_CORE),
            async_ord: cfg.async_ord_core,
            strategy: cfg.strategy_core.unwrap_or(DEFAULT_STRATEGY_CORE),
            strategy_cores: cfg.strategy_cores.clone(),
            execution: cfg.execution_core.unwrap_or(DEFAULT_EXECUTION_CORE),
            feed_cores: cfg.feed_cores.clone(),
            hex_worker_cores: cfg.hex_worker_cores.clone(),
            poly_exec_cores: cfg.poly_exec_cores.clone(),
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
    ///   - `poly-exec-<i>`/`poly-done-<i>` → round-robin `poly_exec_cores` else execution
    ///   - `<inst_id>-worker-<i>`      → round-robin `hex_worker_cores` else execution
    ///   - anything else               → execution
    fn route_execution(&self, thread_name: &str) -> usize {
        if let Some(ex) = thread_name.strip_prefix("feed-") {
            if let Some(&core) = self.feed_cores.get(ex) {
                return core;
            }
        }
        if (thread_name.starts_with("poly-exec-") || thread_name.starts_with("poly-done-"))
            && !self.poly_exec_cores.is_empty()
        {
            let i = POLY_EXEC_RR.fetch_add(1, Ordering::Relaxed) % self.poly_exec_cores.len();
            return self.poly_exec_cores[i];
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
        if self.poly_exec_cores.len() < 2 {
            return Err(
                "strict_core_isolation requires at least two distinct poly_exec_cores".into(),
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
        claim(self.async_ord.unwrap(), "async_ord".into(), &mut exclusive)?;
        claim(
            self.strategy,
            "strategy_router_or_fallback".into(),
            &mut exclusive,
        )?;
        claim(self.execution, "execution".into(), &mut exclusive)?;

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

        // Feed-to-feed sharing is allowed, but a feed may not overlap an
        // exclusive role. Deduplicate values before checking shared feeds.
        let feed_cores: HashSet<usize> = self.feed_cores.values().copied().collect();
        for core in &feed_cores {
            if let Some(role) = exclusive.get(core) {
                return Err(format!("feed core {} overlaps {}", core, role));
            }
        }

        let mut poly_cores = HashSet::new();
        for &core in &self.poly_exec_cores {
            if !poly_cores.insert(core) {
                return Err(format!("poly_exec_cores contains duplicate core {}", core));
            }
            if let Some(role) = exclusive.get(&core) {
                return Err(format!("poly-exec/done core {} overlaps {}", core, role));
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
static BACKGROUND_RR: AtomicUsize = AtomicUsize::new(0);

/// Install the CorePlan resolved from the TOML `[os_tune]` block. Must be
/// called once at process startup, **before** any thread calls
/// `pin_async_rt`, `pin_execution`, etc. Idempotent — later calls are
/// silently ignored so test harnesses can call it multiple times.
pub fn init_from_config(cfg: &OsTuneConfig) {
    let plan = CorePlan::from_config(cfg);
    // Emit a one-shot summary so operators can grep for "core plan" and
    // cross-check against `/proc/cmdline` isolcpus.
    info!(
        "[os_tune] core plan: async_rt={} async_ord={:?} strategy={} execution={} feeds={:?} hex_workers={:?} poly_exec_done={:?} background={:?} fifo(async={} strat={} exec={} poly_feed={} completion={}) enable_pin={} enable_fifo={} strict_isolation={} allow_background_on_execution={}",
        plan.async_rt, plan.async_ord, plan.strategy, plan.execution,
        plan.feed_cores, plan.hex_worker_cores, plan.poly_exec_cores, plan.background_cores,
        plan.fifo_async_rt, plan.fifo_strategy, plan.fifo_execution,
        plan.fifo_polymarket_feed, plan.fifo_completion,
        plan.enable_pin, plan.enable_fifo, plan.strict_core_isolation,
        plan.allow_background_on_execution_core,
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

/// Fail-fast validation for production live/paper multi-instance topology.
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
    let skip = if core_id == p.async_rt {
        "HEXBOT_NO_PIN_ASYNC_RT"
    } else if core_id == p.strategy || p.strategy_cores.values().any(|&c| c == core_id) {
        "HEXBOT_NO_PIN_STRATEGY"
    } else if core_id == p.execution
        || p.feed_cores.values().any(|&c| c == core_id)
        || p.hex_worker_cores.iter().any(|&c| c == core_id)
        || p.poly_exec_cores.iter().any(|&c| c == core_id)
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
    if let Some(&core) = p.feed_cores.get("polymarket") {
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
}

/// Pin a per-instance strategy worker thread (live/paper
/// multi-instance fan-out). Resolves `strategy_cores[instance_id]` for
/// a dedicated core, else falls back to the shared `strategy` core.
/// Same `PRIO_STRATEGY` FIFO priority as the single-thread path.
pub fn pin_strategy_instance(thread_name: &str, instance_id: &str) {
    let p = plan();
    let core = p
        .strategy_cores
        .get(instance_id)
        .copied()
        .unwrap_or(p.strategy);
    pin_current(core, thread_name);
    set_fifo(p.fifo_strategy, thread_name);
}

/// Pin a critical execution-path thread (`execution` dispatcher,
/// `feed-*`, per-instance hex worker pool) with `PRIO_EXECUTION`.
/// Routing:
///   - `feed-<exchange>`              → `feed_cores[<exchange>]` else execution
///   - `poly-exec-*` / `poly-done-*`  → round-robin `poly_exec_cores` else execution
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
    #[cfg(target_os = "linux")]
    {
        let param = libc::sched_param { sched_priority: 0 };
        let rc = unsafe {
            libc::pthread_setschedparam(libc::pthread_self(), libc::SCHED_OTHER, &param)
        };
        if rc != 0 {
            let err = std::io::Error::from_raw_os_error(rc);
            warn!(
                "[os_tune] failed to demote background '{}' to SCHED_OTHER: {}",
                thread_name, err,
            );
            abort_if_strict(&format!(
                "demote background '{}' to SCHED_OTHER: {}",
                thread_name, err
            ));
        }
    }
    let core = plan().route_background();
    pin_current(core, thread_name);
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
        let cores: Vec<usize> = configured_core
            .map_or_else(|| vec![0_usize, 1], |core| vec![core]);
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

/// Lock all current and future process memory to RAM. Blocks major page
/// faults that otherwise manifest as multi-ms stalls.
///
/// Requires `CAP_IPC_LOCK` and a sufficient `RLIMIT_MEMLOCK` ceiling (set
/// via `LimitMEMLOCK=infinity` in a systemd unit, or `ulimit -l unlimited`).
/// Silently degrades to a warning if either is missing.
pub fn mlockall_best_effort() {
    #[cfg(target_os = "linux")]
    {
        let rc = unsafe { libc::mlockall(libc::MCL_CURRENT | libc::MCL_FUTURE) };
        if rc == 0 {
            info!("[os_tune] mlockall OK (MCL_CURRENT | MCL_FUTURE)");
        } else {
            let errno = std::io::Error::last_os_error();
            warn!(
                "[os_tune] mlockall failed: {} (need CAP_IPC_LOCK + RLIMIT_MEMLOCK; \
                 major page faults may produce multi-ms latency spikes)",
                errno,
            );
            abort_if_strict(&format!("mlockall: {}", errno));
        }
    }
    #[cfg(not(target_os = "linux"))]
    {}
}

#[cfg(test)]
mod tests {
    use super::*;

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
        cfg.poly_exec_cores = vec![14, 15];
        cfg.background_cores = vec![0, 1];
        cfg
    }

    fn five_instances() -> Vec<String> {
        (1..=5).map(|i| format!("btc{i:02}")).collect()
    }

    #[test]
    fn strict_plan_accepts_five_dedicated_strategy_cores() {
        let plan = CorePlan::from_config(&five_instance_config());
        assert_eq!(plan.validate_strategy_isolation(&five_instances()), Ok(()));
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
    fn completion_threads_use_polymarket_worker_pool() {
        let plan = CorePlan::from_config(&five_instance_config());
        assert!(plan
            .poly_exec_cores
            .contains(&plan.route_execution("poly-exec-0")));
        assert!(plan
            .poly_exec_cores
            .contains(&plan.route_execution("poly-done-0")));
    }
}
