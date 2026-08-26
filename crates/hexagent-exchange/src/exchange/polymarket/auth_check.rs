//! Read-only CLOB credential/account-binding verification with an explicit,
//! operator-invoked repair path.
//!
//! The command never logs credential material.  It verifies the stored L2
//! tuple against `/auth/api-keys`, derives the signer-bound tuple through L1
//! auth, validates that tuple, and only then atomically replaces the selected
//! `[poly.<account>]` block when `--repair` was supplied.

use anyhow::{anyhow, Result};
use k256::ecdsa::SigningKey;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use super::auth::PolyAuth;
use super::deploy_wallet::{
    derive_api_credentials, derive_existing_api_credentials, fetch_clob_server_time_secs,
    write_poly_secrets, PolySecretsWrite,
};
use super::signer::derive_eth_address_from_key;
use crate::config::{PolymarketSecrets, SecretsFile};

const CLOB_URL: &str = "https://clob.polymarket.com";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum L2Check {
    Valid,
    Unauthorized,
    Unavailable,
}

fn parse_signing_key(private_key: &str) -> Result<SigningKey> {
    let raw = private_key
        .trim()
        .strip_prefix("0x")
        .unwrap_or(private_key.trim());
    let bytes = hex::decode(raw).map_err(|error| anyhow!("private key is not hex: {error}"))?;
    if bytes.len() != 32 {
        return Err(anyhow!("private key must decode to 32 bytes"));
    }
    SigningKey::from_bytes(bytes.as_slice().into())
        .map_err(|error| anyhow!("private key is invalid: {error}"))
}

fn local_time_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn check_l2(creds: &PolymarketSecrets, signer_address: &str, server_time: u64) -> Result<L2Check> {
    let auth = PolyAuth::new(
        &creds.api_key,
        &creds.api_secret,
        &creds.api_passphrase,
        signer_address,
    )?;
    let headers = auth.sign_request_at("GET", "/auth/api-keys", "", server_time);
    crate::async_rt::block_on_runtime(async move {
        let mut request = crate::async_rt::http_client()
            .get(format!("{CLOB_URL}/auth/api-keys"))
            .timeout(std::time::Duration::from_secs(5));
        for (key, value) in headers.as_pairs() {
            request = request.header(key, value);
        }
        let response = match request.send().await {
            Ok(response) => response,
            Err(_) => return Ok(L2Check::Unavailable),
        };
        if response.status().is_success() {
            Ok(L2Check::Valid)
        } else if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            Ok(L2Check::Unauthorized)
        } else {
            Ok(L2Check::Unavailable)
        }
    })
}

fn resolved_context() -> Result<(String, PathBuf)> {
    let account_id = std::env::var("HEXBOT_RESOLVED_ACCOUNT_ID")
        .map_err(|_| anyhow!("auth_check requires --account or --instance/--config"))?;
    let secrets_path = std::env::var("HEXBOT_RESOLVED_SECRETS_PATH")
        .map(PathBuf::from)
        .map_err(|_| anyhow!("resolved secrets path is unavailable"))?;
    Ok((account_id, secrets_path))
}

fn backup_secrets(path: &Path) -> Result<PathBuf> {
    let timestamp = local_time_secs();
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("secrets.toml");
    let backup = path.with_file_name(format!("{file_name}.auth-backup.{timestamp}"));
    std::fs::copy(path, &backup)
        .map_err(|error| anyhow!("backup {}: {error}", backup.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&backup, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(backup)
}

/// CLI entry point. `--repair` is deliberately required for the only write.
pub fn run_auth_check() -> Result<()> {
    let repair = std::env::args().any(|argument| argument == "--repair");
    let (account_id, secrets_path) = resolved_context()?;
    let secrets = SecretsFile::load(&secrets_path)?;
    let stored = secrets.poly_for(&account_id)?.clone();
    let signing_key = parse_signing_key(&stored.private_key)?;
    let signer_address = derive_eth_address_from_key(&signing_key);
    if stored.signature_type.eq_ignore_ascii_case("poly_1271") && stored.funder.trim().is_empty() {
        return Err(anyhow!(
            "account {account_id}: poly_1271 requires a non-empty deposit-wallet funder"
        ));
    }

    let server_time = fetch_clob_server_time_secs()?;
    let skew_secs = local_time_secs() as i128 - server_time as i128;
    let stored_check = check_l2(&stored, &signer_address, server_time)?;
    println!(
        "account={} signer={} funder={} signature_type={} server_clock_skew_s={:+} stored_l2={:?}",
        account_id,
        signer_address,
        if stored.funder.is_empty() {
            "<signer>"
        } else {
            &stored.funder
        },
        stored.signature_type,
        skew_secs,
        stored_check,
    );

    let derived = match derive_existing_api_credentials(&signing_key, &signer_address) {
        Ok(credentials) => credentials,
        Err(error) if repair => {
            eprintln!("signer-bound key does not exist; creating one for explicit repair: {error}");
            derive_api_credentials(&signing_key, &signer_address)?
        }
        Err(error) => {
            return Err(anyhow!(
                "read-only signer-bound credential lookup failed: {error}; use --repair to authorize creation"
            ))
        }
    };
    let derived_creds = PolymarketSecrets {
        api_key: derived.api_key,
        api_secret: derived.secret,
        api_passphrase: derived.passphrase,
        private_key: stored.private_key.clone(),
        signature_type: stored.signature_type.clone(),
        funder: stored.funder.clone(),
    };
    let derived_check = check_l2(&derived_creds, &signer_address, server_time)?;
    let tuple_matches = stored.api_key == derived_creds.api_key
        && stored.api_secret == derived_creds.api_secret
        && stored.api_passphrase == derived_creds.api_passphrase;
    println!(
        "account={} derived_l2={:?} stored_matches_signer_binding={}",
        account_id, derived_check, tuple_matches,
    );

    if stored_check == L2Check::Valid && tuple_matches {
        println!("auth_check=ok write_performed=false");
        return Ok(());
    }
    if derived_check != L2Check::Valid {
        return Err(anyhow!(
            "signer-derived credentials did not pass L2 validation; refusing to write"
        ));
    }
    if !repair {
        return Err(anyhow!(
            "stored credentials are stale or mis-bound; re-run auth_check with --repair"
        ));
    }

    let backup = backup_secrets(&secrets_path)?;
    write_poly_secrets(
        &secrets_path,
        &account_id,
        &PolySecretsWrite {
            api_key: &derived_creds.api_key,
            api_secret: &derived_creds.api_secret,
            api_passphrase: &derived_creds.api_passphrase,
            private_key: &derived_creds.private_key,
            signature_type: &derived_creds.signature_type,
            funder: &derived_creds.funder,
        },
    )?;
    println!(
        "auth_check=repaired write_performed=true backup={}",
        backup.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_key_parser_rejects_wrong_length_without_echoing_material() {
        let error = parse_signing_key("abcd").unwrap_err().to_string();
        assert!(error.contains("32 bytes"));
        assert!(!error.contains("abcd"));
    }
}
