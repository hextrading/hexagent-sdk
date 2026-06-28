//! OS-level latency tuning: CPU pinning, SCHED_FIFO real-time scheduling,
//! memory locking.
//!
//! All operations are best-effort. On non-Linux platforms (macOS dev
//! machines) the affinity / real-time calls are no-ops so the binary
//! compiles and runs without privileges. On Linux the calls require
//! `CAP_SYS_NICE` (affinity + SCHED_FIFO) and `CAP_IPC_LOCK` (mlockall);
//! failures are logged as warnings, not errors — the process continues
//! with degraded tail-latency guarantees rather than refusing to start.
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
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

#[allow(unused_imports)]
use log::{info, warn};

use crate::config::OsTuneConfig;

// ── Legacy 4-core defaults (used when no [os_tune] block is present) ──
const DEFAULT_BACKGROUND_CORE: usize = 0;
const DEFAULT_ASYNC_RT_CORE:   usize = 1;
const DEFAULT_STRATEGY_CORE:   usize = 2;
const DEFAULT_EXECUTION_CORE:  usize = 3;

const DEFAULT_PRIO_ASYNC_RT:  u8 = 70;
const DEFAULT_PRIO_STRATEGY:  u8 = 60;
const DEFAULT_PRIO_EXECUTION: u8 = 50;

/// Resolved at startup from `OsTuneConfig`. All fields are concrete
/// core ids / priorities; optional config entries have been filled in
/// with legacy defaults.
#[derive(Debug, Clone)]
pub struct CorePlan {
    pub enable_pin: bool,
    pub enable_fifo: bool,
    pub async_rt: usize,
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
    pub background_cores: Vec<usize>,
    pub fifo_async_rt: u8,
    pub fifo_strategy: u8,
    pub fifo_execution: u8,
}

impl CorePlan {
    fn legacy_default() -> Self {
        Self {
            enable_pin: true,
            enable_fifo: true,
            async_rt: DEFAULT_ASYNC_RT_CORE,
            strategy: DEFAULT_STRATEGY_CORE,
            strategy_cores: HashMap::new(),
            execution: DEFAULT_EXECUTION_CORE,
            feed_cores: HashMap::new(),
            hex_worker_cores: Vec::new(),
            background_cores: vec![DEFAULT_BACKGROUND_CORE],
            fifo_async_rt: DEFAULT_PRIO_ASYNC_RT,
            fifo_strategy: DEFAULT_PRIO_STRATEGY,
            fifo_execution: DEFAULT_PRIO_EXECUTION,
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
            async_rt: cfg.async_rt_core.unwrap_or(DEFAULT_ASYNC_RT_CORE),
            strategy: cfg.strategy_core.unwrap_or(DEFAULT_STRATEGY_CORE),
            strategy_cores: cfg.strategy_cores.clone(),
            execution: cfg.execution_core.unwrap_or(DEFAULT_EXECUTION_CORE),
            feed_cores: cfg.feed_cores.clone(),
            hex_worker_cores: cfg.hex_worker_cores.clone(),
            background_cores: bg,
            fifo_async_rt: cfg.fifo_async_rt.unwrap_or(DEFAULT_PRIO_ASYNC_RT),
            fifo_strategy: cfg.fifo_strategy.unwrap_or(DEFAULT_PRIO_STRATEGY),
            fifo_execution: cfg.fifo_execution.unwrap_or(DEFAULT_PRIO_EXECUTION),
        }
    }

    /// Route an execution-tier thread to its core based on name:
    ///   - `feed-<exchange>`           → `feed_cores[<exchange>]` else execution
    ///   - `<inst_id>-worker-<i>`      → round-robin `hex_worker_cores` else execution
    ///   - anything else               → execution
    fn route_execution(&self, thread_name: &str) -> usize {
        if let Some(ex) = thread_name.strip_prefix("feed-") {
            if let Some(&core) = self.feed_cores.get(ex) {
                return core;
            }
        }
        if thread_name.contains("-worker-") && !self.hex_worker_cores.is_empty() {
            let i = HEX_WORKER_RR.fetch_add(1, Ordering::Relaxed) % self.hex_worker_cores.len();
            return self.hex_worker_cores[i];
        }
        self.execution
    }

    /// Round-robin a background thread across `background_cores` so
    /// 16-core hosts can spread recorder / join / heartbeat threads
    /// over 2 or more IRQ cores.
    fn route_background(&self) -> usize {
        let n = self.background_cores.len().max(1);
        let i = BACKGROUND_RR.fetch_add(1, Ordering::Relaxed) % n;
        *self.background_cores.get(i).unwrap_or(&DEFAULT_BACKGROUND_CORE)
    }
}

static CORE_PLAN: OnceLock<CorePlan> = OnceLock::new();
static HEX_WORKER_RR: AtomicUsize = AtomicUsize::new(0);
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
        "[os_tune] core plan: async_rt={} strategy={} execution={} feeds={:?} hex_workers={:?} background={:?} fifo(async={} strat={} exec={}) enable_pin={} enable_fifo={}",
        plan.async_rt, plan.strategy, plan.execution,
        plan.feed_cores, plan.hex_worker_cores, plan.background_cores,
        plan.fifo_async_rt, plan.fifo_strategy, plan.fifo_execution,
        plan.enable_pin, plan.enable_fifo,
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

fn plan() -> &'static CorePlan {
    CORE_PLAN.get_or_init(CorePlan::legacy_default)
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
    } else if core_id == p.strategy {
        "HEXBOT_NO_PIN_STRATEGY"
    } else if core_id == p.execution
        || p.feed_cores.values().any(|&c| c == core_id)
        || p.hex_worker_cores.iter().any(|&c| c == core_id)
    {
        "HEXBOT_NO_PIN_EXECUTION"
    } else if p.background_cores.iter().any(|&c| c == core_id) {
        "HEXBOT_NO_PIN_BACKGROUND"
    } else {
        ""
    };
    if !skip.is_empty() && std::env::var(skip).ok().as_deref() == Some("1") {
        info!("[os_tune] Pin '{}' → core {} SKIPPED ({}=1)", thread_name, core_id, skip);
        return;
    }
    #[cfg(target_os = "linux")]
    {
        let target = core_affinity::CoreId { id: core_id };
        // Include TID so operators can cross-check with
        // `ps -eLo pid,tid,comm,psr,cls,state | grep hexbot`.
        let tid = unsafe { libc::syscall(libc::SYS_gettid) };
        if core_affinity::set_for_current(target) {
            info!("[os_tune] Pinned '{}' (tid={}) → core {}", thread_name, tid, core_id);
        } else {
            warn!(
                "[os_tune] Pin '{}' (tid={}) → core {} FAILED (core out of range or affinity mask restricted)",
                thread_name, tid, core_id,
            );
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
/// Failure of the syscall itself (even when opted in) is non-fatal —
/// logged as warn and the thread continues as SCHED_OTHER.
pub fn set_fifo(priority: u8, thread_name: &str) {
    if std::env::var("HEXBOT_NO_FIFO").ok().as_deref() == Some("1") {
        return;
    }
    if !plan().enable_fifo {
        return;
    }
    #[cfg(target_os = "linux")]
    {
        let param = libc::sched_param { sched_priority: priority as i32 };
        let rc = unsafe {
            libc::pthread_setschedparam(
                libc::pthread_self(),
                libc::SCHED_FIFO,
                &param,
            )
        };
        if rc == 0 {
            info!("[os_tune] SCHED_FIFO prio={} → '{}'", priority, thread_name);
        } else {
            let err = std::io::Error::from_raw_os_error(rc);
            warn!(
                "[os_tune] SCHED_FIFO prio={} for '{}' failed: {} (need CAP_SYS_NICE; \
                 falling back to SCHED_OTHER — tail latency guarantees degraded)",
                priority, thread_name, err,
            );
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
    let core = p.strategy_cores.get(instance_id).copied().unwrap_or(p.strategy);
    pin_current(core, thread_name);
    set_fifo(p.fifo_strategy, thread_name);
}

/// Pin a critical execution-path thread (`execution` dispatcher,
/// `feed-*`, per-instance hex worker pool) with `PRIO_EXECUTION`.
/// Routing:
///   - `feed-<exchange>`      → `feed_cores[<exchange>]` else `execution_core`
///   - `<inst_id>-worker-<i>` → round-robin `hex_worker_cores` else `execution_core`
///   - anything else          → `execution_core`
pub fn pin_execution(thread_name: &str) {
    let p = plan();
    let core = p.route_execution(thread_name);
    pin_current(core, thread_name);
    set_fifo(p.fifo_execution, thread_name);
}

/// Pin a non-critical I/O-bound background thread to the background
/// pool. `SCHED_OTHER` (no FIFO). Use for: recorder (flushes every
/// 60 s), latency-dump, paper-exec, async-task joiner threads
/// (poly-heartbeat-join, poly-user-feed-join, hex-user-feed-join).
pub fn pin_background(thread_name: &str) {
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
/// Default mask = cores {0, 1}, matching the typical
/// `irqaffinity=0-1` grub range. These are the same cores the kernel
/// runs IRQ/softirq on, and main-thread + tracing appender worker
/// are both idle/sporadic — sharing with kernel housekeeping is fine.
/// Respects `HEXBOT_NO_PIN=1`.
pub fn pin_main_early(thread_name: &str) {
    if std::env::var("HEXBOT_NO_PIN").ok().as_deref() == Some("1") {
        return;
    }
    #[cfg(target_os = "linux")]
    {
        let cores = [0_usize, 1];
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
                     will keep default affinity ({{0,1,11-15}} with isolcpus=2-10)",
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
        }
    }
    #[cfg(not(target_os = "linux"))]
    {}
}
