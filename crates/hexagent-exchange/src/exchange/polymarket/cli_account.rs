//! CLI multi-wallet resolution for Polymarket subcommands.
//!
//! All `hexbot redeem/split/withdraw/new_order/...` subcommands read
//! credentials from `POLY_*` environment variables (legacy single-account
//! flow). This helper provides a one-shot override so the same binary can
//! drive any wallet, selected one of two ways:
//!
//!   1. `--instance <id> --config <path>`  (config-aware; preferred)
//!        Loads `<path>`, finds the `[[strategies]]` block whose
//!        `instance_id == <id>`, resolves the secrets file from that
//!        config's `general.secrets_file`, then loads `[poly.<id>]`.
//!        Operates on exactly the wallet that strategy instance trades
//!        with — name a configured strategy, no need to know the secrets
//!        layout or set `$HEXBOT_SECRETS`.
//!
//!          hexbot --instance maker02 --config config/live_polymaker.toml positions
//!          hexbot positions --instance maker02 --config config/live_polymaker.toml   (also works)
//!
//!   2. `--account <id>`  (low-level escape hatch)
//!        Names the `[poly.<id>]` secrets block directly, resolving the
//!        secrets file via `$HEXBOT_SECRETS` → `./secrets.toml`. No config
//!        lookup or validation.
//!
//! In both cases the matching credentials are pushed into `POLY_*` env vars
//! BEFORE the subcommand runs. Subcommands replace `std::env::args().skip(2)`
//! with `cli_account::cli_args()` so the `--instance` / `--config` /
//! `--account` pairs never reach their positional parsers.
//!
//! Resolution precedence (first match wins):
//!   1. `--instance <id>` CLI flag (requires `--config <path>`)
//!   2. `--account <id>` CLI flag
//!   3. `$HEXBOT_INSTANCE` env var (with `--config` / `$HEXBOT_CONFIG`)
//!   4. `$HEXBOT_ACCOUNT` env var
//!   5. No override — legacy behaviour (POLY_* read from .env / shell)
//!
//! `--instance` and `--account` are mutually exclusive on the CLI.

use anyhow::{anyhow, Result};
use std::path::Path;

use crate::config::{Config, PolymarketSecrets, SecretsFile};

/// Strip every `--<name> <value>` / `--<name>=<value>` pair from `args`,
/// returning the captured value (last occurrence wins) or empty string if
/// the flag is absent. `name` must include the leading dashes, e.g.
/// `"--account"`.
fn strip_flag(args: &mut Vec<String>, name: &str) -> String {
    let eq_prefix = format!("{}=", name);
    let mut found = String::new();
    let mut i = 0;
    while i < args.len() {
        if args[i] == name {
            if i + 1 < args.len() {
                found = args.remove(i + 1);
                args.remove(i);
            } else {
                args.remove(i);
            }
            continue;
        }
        if let Some(rest) = args[i].strip_prefix(&eq_prefix) {
            found = rest.to_string();
            args.remove(i);
            continue;
        }
        i += 1;
    }
    found
}

/// `std::env::args()` with the top-level wallet/config flags
/// (`--account`, `--instance`, `--config`) stripped. Use this in main.rs to
/// determine the subcommand: `clean_args().get(1)`, and inside each
/// subcommand (via `cli_args()`) so those flags never bleed into the
/// positional parsers — regardless of whether the operator placed them
/// before or after the subcommand name.
pub fn clean_args() -> Vec<String> {
    let mut v: Vec<String> = std::env::args().collect();
    let _ = strip_flag(&mut v, "--account");
    let _ = strip_flag(&mut v, "--instance");
    let _ = strip_flag(&mut v, "--config");
    let _ = strip_flag(&mut v, "--secrets");
    v
}

/// Default secrets file for the `--account` low-level path when neither
/// `--secrets` nor `$HEXBOT_SECRETS` is given. Operator deployments keep
/// the file here; dev/local runs override with `--secrets <path>`.
pub const DEFAULT_SECRETS_PATH: &str = "/etc/secrets/polymaker/secrets.toml";

/// The `--secrets <path>` value (or `--secrets=<path>`), falling back to
/// `$HEXBOT_SECRETS`. Returns `None` when neither is set (callers then use
/// [`DEFAULT_SECRETS_PATH`]). A top-level flag stripped by `clean_args`,
/// so it's position-independent and never reaches positional parsers.
pub fn secrets_path() -> Option<String> {
    let mut v: Vec<String> = std::env::args().collect();
    let from_cli = strip_flag(&mut v, "--secrets");
    if !from_cli.is_empty() {
        return Some(from_cli);
    }
    std::env::var("HEXBOT_SECRETS").ok().filter(|s| !s.is_empty())
}

/// Resolve the secrets-file path for the `--account` path:
/// `--secrets` → `$HEXBOT_SECRETS` → [`DEFAULT_SECRETS_PATH`].
pub fn resolve_secrets_file() -> std::path::PathBuf {
    secrets_path()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(DEFAULT_SECRETS_PATH))
}

/// Subcommand-positional iterator: `clean_args().into_iter().skip(2)`.
/// Drop-in replacement for `std::env::args().skip(2)` inside each
/// `run_*` subcommand body.
pub fn cli_args() -> std::vec::IntoIter<String> {
    let mut v = clean_args();
    if v.len() >= 2 { v.drain(..2); } else { v.clear(); }
    v.into_iter()
}

/// The `--config <path>` value (or `--config=<path>`), falling back to
/// `$HEXBOT_CONFIG`. Returns `None` when neither is set. `--config` is a
/// top-level flag consumed centrally (stripped by `clean_args`), so
/// subcommands that want the config path (`new_orders`, `cancel_orders`,
/// …) read it through this getter rather than re-parsing it — that keeps
/// `--config` position-independent and consistent with `--instance`.
pub fn config_path() -> Option<String> {
    let mut v: Vec<String> = std::env::args().collect();
    let from_cli = strip_flag(&mut v, "--config");
    if !from_cli.is_empty() {
        return Some(from_cli);
    }
    std::env::var("HEXBOT_CONFIG").ok().filter(|s| !s.is_empty())
}

/// The `--instance <id>` value (or `--instance=<id>`), falling back to
/// `$HEXBOT_INSTANCE`. Empty string when neither is set. Used by
/// `deploy_wallet`, which resolves the secrets block itself (it writes
/// credentials that don't exist yet, so it can't go through
/// `resolve_and_apply`).
pub fn instance_id() -> String {
    let mut v: Vec<String> = std::env::args().collect();
    let from_cli = strip_flag(&mut v, "--instance");
    if !from_cli.is_empty() {
        return from_cli;
    }
    std::env::var("HEXBOT_INSTANCE").unwrap_or_default()
}

/// Resolve `(instance_id, secrets-file path)` for a WRITE operation
/// (`deploy_wallet`).
///
/// Unlike [`apply_instance_to_env`], this does NOT require the
/// `[poly.<id>]` block to already exist — `deploy_wallet` is what creates
/// it. It loads `config_path` to read `general.secrets_file` (the
/// operator asked us to write into "the config's secrets file") and
/// resolves it through the same cascade the running bot uses.
///
/// Instance selection (`instance_id` is the `--instance` value, possibly
/// empty):
///   * non-empty → used as-is (a missing match against the config's
///     `[[strategies]]` only WARNS — an operator may provision a wallet
///     before wiring its strategy block);
///   * empty + config has exactly ONE strategy instance → auto-resolved
///     (so a single-strategy config needs no `--instance`);
///   * empty + zero / multiple instances → error asking for `--instance`.
pub fn resolve_secrets_write_path(instance_id: &str, config_path: &str) -> Result<(String, std::path::PathBuf)> {
    // ── Account mode: `--account <id> [--secrets <path>]`, no --config ──
    // Writes the `[poly.<id>]` block straight into the secrets file
    // (default /etc/secrets/polymaker/secrets.toml). Preferred for
    // operator runs that don't have (or need) a live config on the box.
    if config_path.is_empty() {
        let account = resolve_account_id();
        if !account.is_empty() {
            let secrets = resolve_secrets_file();
            eprintln!("[cli] account='{}' (secrets={})", account, secrets.display());
            return Ok((account, secrets));
        }
        return Err(anyhow!(
            "deploy_wallet needs a target: either `--account <id> [--secrets <path>]` \
             (writes [poly.<id>] into the secrets file; default {}), or \
             `--config <path>` (writes into that config's general.secrets_file). \
             Set --account/$HEXBOT_ACCOUNT or --config/$HEXBOT_CONFIG.",
            DEFAULT_SECRETS_PATH,
        ));
    }
    let cfg_path = Path::new(config_path);
    if !cfg_path.exists() {
        return Err(anyhow!("--config {}: file not found", config_path));
    }
    let config = Config::load(cfg_path)
        .map_err(|e| anyhow!("--config {}: {}", config_path, e))?;

    // Strategy instance ids configured in this config (enabled, non-empty).
    let configured: Vec<&str> = config
        .strategies
        .iter()
        .filter(|s| s.enabled && !s.instance_id.is_empty())
        .map(|s| s.instance_id.as_str())
        .collect();

    // Resolve which instance to write. `--instance` wins; otherwise
    // AUTO-RESOLVE the single configured strategy (so a one-strategy config
    // needs no flag). Ambiguous / empty configs require an explicit
    // `--instance`.
    let resolved: String = if !instance_id.is_empty() {
        if !configured.contains(&instance_id) {
            eprintln!(
                "[cli] warning: instance '{}' is not a configured [[strategies]] block in {} \
                 (known: {:?}). Proceeding — remember to add it before running the bot.",
                instance_id, config_path, configured,
            );
        }
        instance_id.to_string()
    } else {
        // Dedup while keeping the "exactly one distinct id" semantics.
        let mut uniq: Vec<&str> = Vec::new();
        for i in &configured { if !uniq.contains(i) { uniq.push(i); } }
        match uniq.as_slice() {
            [one] => {
                eprintln!("[cli] auto-resolved instance '{}' (only strategy in {})", one, config_path);
                one.to_string()
            }
            [] => return Err(anyhow!(
                "config {} has no [[strategies]] block with an instance_id — pass \
                 --instance <id> to name the secrets block to create.",
                config_path,
            )),
            many => return Err(anyhow!(
                "config {} defines {} strategy instances {:?} — pass --instance <id> \
                 to choose which wallet to deploy.",
                config_path, many.len(), many,
            )),
        }
    };

    let secrets_path = SecretsFile::resolve_path_with_override(cfg_path, &config.general.secrets_file);
    Ok((resolved, secrets_path))
}

/// Resolve the `--account <id>` id from CLI args, or `$HEXBOT_ACCOUNT`.
pub fn resolve_account_id() -> String {
    let mut v: Vec<String> = std::env::args().collect();
    let from_cli = strip_flag(&mut v, "--account");
    if !from_cli.is_empty() {
        return from_cli;
    }
    std::env::var("HEXBOT_ACCOUNT").unwrap_or_default()
}

/// Push a resolved credential set into the `POLY_*` env vars.
///
/// SAFETY: `set_var` is called from main() / engine setup before any
/// background thread / subcommand reads `POLY_*`; no race exists.
pub fn apply_creds_to_env(creds: &PolymarketSecrets) {
    std::env::set_var("POLY_API_KEY", &creds.api_key);
    std::env::set_var("POLY_API_SECRET", &creds.api_secret);
    std::env::set_var("POLY_PASSPHRASE", &creds.api_passphrase);
    std::env::set_var("POLY_PRIVATE_KEY", &creds.private_key);
    std::env::set_var("POLY_SIGNATURE_TYPE", &creds.signature_type);
    // Deposit-wallet (POLY_1271) maker/funder — consumed by the maintenance
    // path (split/redeem via WALLET batch) and the v2 order signer.
    std::env::set_var("POLY_FUNDER", &creds.funder);
    // builder_code is no longer per-instance: it comes solely from the
    // shared `[builder]` block (POLY_BUILDER_CODE, set by
    // `SecretsFile::apply_shared_to_env`).
}

/// Load the secrets file and apply the chosen `[poly.<account_id>]`
/// credentials to `POLY_*` env vars. No-op when `account_id` is empty.
/// Resolves the secrets file via `--secrets` → `$HEXBOT_SECRETS` →
/// [`DEFAULT_SECRETS_PATH`] (`/etc/secrets/polymaker/secrets.toml`).
pub fn apply_account_to_env(account_id: &str) -> Result<Option<String>> {
    if account_id.is_empty() {
        return Ok(None);
    }
    let path = resolve_secrets_file();
    if !Path::new(&path).exists() {
        return Err(anyhow!(
            "--account {} requested but secrets file not found at {} \
             (pass --secrets <path>, set $HEXBOT_SECRETS, or place it at the \
             default {})",
            account_id, path.display(), DEFAULT_SECRETS_PATH,
        ));
    }
    let secrets = SecretsFile::load(&path)?;
    // Push the shared `[builder]`/`[chainlink]`/`[polygon]` blocks first
    // (so builder_code + builder relayer creds come from `[builder]`),
    // then the per-instance `[poly.<id>]` creds.
    secrets.apply_shared_to_env();
    let creds = secrets.poly_for(account_id)?;
    apply_creds_to_env(creds);
    eprintln!("[cli] account='{}' (secrets={})", account_id, path.display());
    Ok(Some(account_id.to_string()))
}

/// Config-aware wallet resolution. Loads `config_path`, requires a
/// `[[strategies]]` block whose `instance_id == instance_id`, resolves the
/// secrets file from that config's `general.secrets_file` cascade, then
/// loads `[poly.<instance_id>]` into `POLY_*`.
///
/// Errors (all fatal — the operator named a specific wallet and we must not
/// silently fall back to the wrong one):
///   * `config_path` empty                  → `--instance` needs `--config`
///   * config / secrets file missing        → bad path
///   * `instance_id` not a configured strategy → typo; lists known ids
///   * `[poly.<instance_id>]` block missing  → secrets gap; lists known ids
pub fn apply_instance_to_env(instance_id: &str, config_path: &str) -> Result<String> {
    if config_path.is_empty() {
        return Err(anyhow!(
            "--instance {} requires --config <path> (the live config that \
             defines the strategy instances and their secrets file). \
             Set --config or $HEXBOT_CONFIG.",
            instance_id,
        ));
    }
    let cfg_path = Path::new(config_path);
    if !cfg_path.exists() {
        return Err(anyhow!("--config {}: file not found", config_path));
    }
    let config = Config::load(cfg_path)
        .map_err(|e| anyhow!("--config {}: {}", config_path, e))?;

    // The instance must correspond to a configured strategy. This catches
    // typos before we touch any wallet and documents the wallet⇄strategy
    // binding the operator is actually reaching for.
    let known: Vec<&str> = config
        .strategies
        .iter()
        .map(|s| s.instance_id.as_str())
        .filter(|s| !s.is_empty())
        .collect();
    if !config.strategies.iter().any(|s| s.instance_id == instance_id) {
        return Err(anyhow!(
            "config {}: no [[strategies]] block with instance_id = \"{}\". \
             Known instance_ids: {:?}",
            config_path, instance_id, known,
        ));
    }

    // Secrets file follows the config's own cascade
    // (general.secrets_file → $HEXBOT_SECRETS → <config_dir>/secrets.toml →
    // ./secrets.toml), so the wallet matches what the running bot would use.
    let secrets_path =
        SecretsFile::resolve_path_with_override(cfg_path, &config.general.secrets_file);
    if !secrets_path.exists() {
        return Err(anyhow!(
            "instance {}: secrets file not found at {} \
             (config general.secrets_file = {:?})",
            instance_id,
            secrets_path.display(),
            config.general.secrets_file,
        ));
    }
    let secrets = SecretsFile::load(&secrets_path)?;
    let creds = secrets.poly_for(instance_id)?;
    apply_creds_to_env(creds);
    eprintln!(
        "[cli] instance='{}' (config={}, secrets={})",
        instance_id, config_path, secrets_path.display(),
    );
    Ok(instance_id.to_string())
}

/// Resolve + apply the selected wallet in one call from main.rs. No-op when
/// none of `--instance` / `--account` / `$HEXBOT_INSTANCE` / `$HEXBOT_ACCOUNT`
/// is set. CLI flags take precedence over env vars; `--instance` and
/// `--account` are mutually exclusive on the CLI.
pub fn resolve_and_apply() -> Result<Option<String>> {
    let mut v: Vec<String> = std::env::args().collect();
    // Order matters only for stripping; each flag is independent.
    let cli_account = strip_flag(&mut v, "--account");
    let cli_instance = strip_flag(&mut v, "--instance");
    let cli_config = strip_flag(&mut v, "--config");

    if !cli_instance.is_empty() && !cli_account.is_empty() {
        return Err(anyhow!(
            "--instance and --account are mutually exclusive — pick one \
             (--instance names a configured strategy; --account names a \
             [poly.<id>] secrets block directly)"
        ));
    }

    // Config path for instance resolution: CLI flag wins, else $HEXBOT_CONFIG.
    let cfg = if !cli_config.is_empty() {
        cli_config
    } else {
        std::env::var("HEXBOT_CONFIG").unwrap_or_default()
    };

    if !cli_instance.is_empty() {
        return apply_instance_to_env(&cli_instance, &cfg).map(Some);
    }
    if !cli_account.is_empty() {
        return apply_account_to_env(&cli_account);
    }

    // No CLI flag — fall back to env vars (instance first, then account).
    let env_instance = std::env::var("HEXBOT_INSTANCE").unwrap_or_default();
    if !env_instance.is_empty() {
        return apply_instance_to_env(&env_instance, &cfg).map(Some);
    }
    let env_account = std::env::var("HEXBOT_ACCOUNT").unwrap_or_default();
    if !env_account.is_empty() {
        return apply_account_to_env(&env_account);
    }

    // No explicit selector at all — AUTO-RESOLVE the wallet from the
    // config's single strategy instance. Credentials come ONLY from the
    // secrets file; there is NO `.env` credential fallback.
    let cfg_path = if cfg.is_empty() {
        "config/live_polymaker.toml".to_string()
    } else {
        cfg
    };
    auto_resolve_from_config(&cfg_path)
}

/// Auto-resolve the wallet when neither `--instance` nor `--account` was
/// given. Loads `config_path`, and:
///   * exactly ONE enabled strategy instance → apply its `[poly.<id>]`
///     creds (hard error if that block / the secrets file is missing —
///     this is the "couldn't read it → tell the operator" path);
///   * multiple instances → hard error asking for `--instance`;
///   * no config / no instances → no-op (the subcommand surfaces its own
///     "no credentials" error if it actually needs a wallet).
fn auto_resolve_from_config(config_path: &str) -> Result<Option<String>> {
    let cfg_path = Path::new(config_path);
    if !cfg_path.exists() {
        return Ok(None);
    }
    let config = Config::load(cfg_path)
        .map_err(|e| anyhow!("--config {}: {}", config_path, e))?;
    let mut instances: Vec<&str> = Vec::new();
    for s in &config.strategies {
        if s.enabled && !s.instance_id.is_empty() && !instances.contains(&s.instance_id.as_str()) {
            instances.push(s.instance_id.as_str());
        }
    }
    match instances.as_slice() {
        [] => Ok(None),
        [one] => apply_instance_to_env(one, config_path).map(Some),
        many => Err(anyhow!(
            "config {} defines {} wallet instances {:?} — pass --instance <id> \
             to choose one (credentials load from the secrets file; there is no \
             .env fallback)",
            config_path, many.len(), many,
        )),
    }
}
