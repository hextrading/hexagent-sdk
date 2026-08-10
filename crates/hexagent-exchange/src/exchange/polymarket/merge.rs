//! `hexbot merge` — merge equal Up+Down outcome inventory back into
//! collateral through the same account-aware maintenance executor used by
//! live strategies.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Result};

use super::wallet::{
    ctf_outcome_token_ids, read_gas_via_signer_wallet_flag,
    run_merge_maintenance_blocking, MergeMaintenanceJob,
};

fn safe_account_filename(account_id: &str) -> String {
    account_id.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' { ch } else { '_' }
        })
        .collect()
}

fn configured_ledger_path(account_id: &str) -> Result<PathBuf> {
    let config_path = crate::exchange::polymarket::cli_account::config_path()
        .ok_or_else(|| anyhow!(
            "merge needs --config <path> to locate the durable account ledger, \
             or pass --ledger <ledger.json> explicitly"
        ))?;
    let config = crate::config::Config::load(Path::new(&config_path))
        .map_err(|error| anyhow!("--config {}: {}", config_path, error))?;
    let exchange = config.exchanges.iter()
        .find(|exchange| exchange.name == "polymarket")
        .ok_or_else(|| anyhow!("config {} has no polymarket exchange", config_path))?;
    if exchange.account_ledger_dir.trim().is_empty() {
        return Err(anyhow!(
            "polymarket.account_ledger_dir is empty; live merge requires a durable ledger"
        ));
    }
    Ok(PathBuf::from(&exchange.account_ledger_dir)
        .join(format!("{}.json", safe_account_filename(account_id))))
}

pub fn run_merge() -> Result<()> {
    let args: Vec<String> = crate::exchange::polymarket::cli_account::cli_args().collect();
    let mut dry_run = false;
    let mut owner = String::new();
    let mut ledger_path: Option<PathBuf> = None;
    let mut positional = Vec::new();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--dry-run" | "-n" => dry_run = true,
            "--owner" => {
                index += 1;
                owner = args.get(index).cloned()
                    .ok_or_else(|| anyhow!("--owner requires an instance id"))?;
            }
            "--ledger" => {
                index += 1;
                ledger_path = Some(PathBuf::from(args.get(index).cloned()
                    .ok_or_else(|| anyhow!("--ledger requires a path"))?));
            }
            value if value.starts_with("--owner=") => {
                owner = value.trim_start_matches("--owner=").to_string();
            }
            value if value.starts_with("--ledger=") => {
                ledger_path = Some(PathBuf::from(value.trim_start_matches("--ledger=")));
            }
            value if value.starts_with('-') => {
                return Err(anyhow!("unknown merge option `{}`", value));
            }
            value => positional.push(value.to_string()),
        }
        index += 1;
    }

    if positional.len() != 2 {
        eprintln!(
            "Usage: hexbot merge <condition_id> <amount> \
             [--instance <id> --config <config>] \
             [--owner <instance>] [--ledger <ledger.json>] [--dry-run]\n\n\
             A live merge must name one virtual owner and use the durable \
             shared-account ledger. --instance supplies the owner automatically; \
             --account callers must also pass --owner."
        );
        return Err(anyhow!("expected condition_id and amount"));
    }
    let condition_id = positional[0].clone();
    let condition_hex = condition_id.strip_prefix("0x").unwrap_or(&condition_id);
    if condition_hex.len() != 64 || !condition_hex.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(anyhow!("condition_id must be 0x + 64 hex characters"));
    }
    let amount_usdc: f64 = positional[1].parse()
        .map_err(|error| anyhow!("amount parse: {}", error))?;
    if !amount_usdc.is_finite() || amount_usdc <= 0.0 {
        return Err(anyhow!("amount must be positive, got {}", amount_usdc));
    }

    let account_id = std::env::var("HEXBOT_RESOLVED_ACCOUNT_ID").unwrap_or_default();
    if account_id.is_empty() && !dry_run {
        return Err(anyhow!(
            "live merge requires --instance <id> --config <path> or --account <id>"
        ));
    }
    if owner.is_empty() {
        owner = std::env::var("HEXBOT_RESOLVED_INSTANCE_ID").unwrap_or_default();
    }
    if owner.is_empty() && !dry_run {
        return Err(anyhow!(
            "live merge has no virtual owner; use --instance <id> or pass --owner <instance>"
        ));
    }

    let (up_token_id, down_token_id) = ctf_outcome_token_ids(&condition_id)?;
    let account_state = if dry_run {
        None
    } else {
        let path = match ledger_path {
            Some(path) => path,
            None => configured_ledger_path(&account_id)?,
        };
        Some(Arc::new(
            hexagent_account::account::shared_account::SharedAccount::new_persistent(
                account_id.clone(), &path,
            ).map_err(anyhow::Error::msg)?,
        ))
    };

    println!("=== Polymarket account merge ===");
    println!("Account:       {}", if account_id.is_empty() { "dry-run" } else { &account_id });
    println!("Owner:         {}", if owner.is_empty() { "dry-run" } else { &owner });
    println!("Condition ID:  {}", condition_id);
    println!("Up token:      {}", up_token_id);
    println!("Down token:    {}", down_token_id);
    println!("Merge amount:  {}", amount_usdc);

    run_merge_maintenance_blocking(MergeMaintenanceJob {
        condition_id,
        up_token_id,
        down_token_id,
        amount_usdc,
        gas_via_signer: read_gas_via_signer_wallet_flag(),
        dry_run,
        account_id,
        instance_id: owner,
        account_state,
    })?;
    println!("{} merge complete", if dry_run { "Dry-run" } else { "Confirmed" });
    Ok(())
}
