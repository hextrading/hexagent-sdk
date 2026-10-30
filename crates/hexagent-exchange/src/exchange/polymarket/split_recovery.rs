//! Cold wallet-owner recovery only. No quote callbacks, workers or shared gates.
use super::deploy_wallet::keccak256;
use anyhow::{anyhow, Result};
use hexagent_account::account::shared_account::MaintenanceWalletSubmission;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

/// An operation's funds stay reserved while this timer postpones external I/O.
/// The existing per-account wallet actor is the only writer. Restart resets
/// the timer, never the durable request identity or its economic reservation.
#[derive(Default)]
pub(super) struct RecoveryBackoff {
    next: Option<Instant>,
    failures: u32,
}

pub(super) fn retry_delay(attempt: u32, identity: &[u8], paused: bool) -> Duration {
    let base = if paused { 5_000u64 } else { 1_000 };
    let delay = base
        .saturating_mul(1u64 << attempt.saturating_sub(1).min(6))
        .min(60_000);
    let jitter = identity.iter().fold(attempt as u64, |h, b| {
        h.wrapping_mul(131).wrapping_add(*b as u64)
    }) % 1_000;
    Duration::from_millis(delay + jitter)
}

impl RecoveryBackoff {
    pub(super) fn remaining(&self, now: Instant) -> Duration {
        self.next
            .map_or(Duration::ZERO, |next| next.saturating_duration_since(now))
    }
    pub(super) fn failed(&mut self, now: Instant, identity: &[u8]) {
        self.failures = self.failures.saturating_add(1);
        self.next = Some(now + retry_delay(self.failures, identity, true));
    }
    pub(super) fn reset(&mut self) {
        *self = Self::default();
    }
}

fn word(value: &Value) -> Result<[u8; 32]> {
    let raw = value
        .as_str()
        .and_then(|s| s.strip_prefix("0x"))
        .ok_or_else(|| anyhow!("RPC result is not a hex ABI word"))?;
    let bytes = hex::decode(raw)?;
    bytes
        .try_into()
        .map_err(|_| anyhow!("RPC result is not exactly one ABI word"))
}

/// DepositWallet.execute atomically checks nonce equality, increments nonce,
/// enforces block.timestamp <= deadline, and reverts all calls on failure.
/// Consequently an unchanged/lower nonce at a finalized block AFTER deadline
/// proves this signed batch never executed and can never execute later.
///
/// A higher nonce proves neither success nor failure of this economic intent:
/// retain the reservation and recover the exact transaction/event instead.
/// All point reads use the SAME canonical block hash. Missing finality tags,
/// archive state, owner mismatch, malformed replies or reorgs are errors.
#[derive(Debug)]
pub(super) struct UnusedNonceProof {
    pub block_number: String,
    pub block_hash: String,
    pub timestamp: u64,
}

pub(super) fn expired_nonce_unused<R>(
    submission: &MaintenanceWalletSubmission,
    mut rpc: R,
) -> Result<Option<UnusedNonceProof>>
where
    R: FnMut(&str, Value) -> Result<Value>,
{
    let nonce = submission.nonce.parse::<u128>()?;
    if submission.deadline_secs == 0
        || submission.body_hash.len() != 64
        || !submission.body_hash.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(anyhow!("invalid durable WALLET request identity"));
    }
    for address in [&submission.signer, &submission.deposit_wallet] {
        if address.len() != 42
            || !address.starts_with("0x")
            || hex::decode(&address[2..])?.len() != 20
        {
            return Err(anyhow!("invalid durable WALLET owner address"));
        }
    }
    let chain = rpc("eth_chainId", json!([]))?;
    if chain["result"].as_str() != Some("0x89") {
        return Err(anyhow!("split recovery requires Polygon mainnet"));
    }
    let header = rpc("eth_getBlockByNumber", json!(["finalized", false]))?;
    let block = &header["result"];
    let timestamp = block["timestamp"]
        .as_str()
        .and_then(|s| s.strip_prefix("0x"))
        .and_then(|s| u64::from_str_radix(s, 16).ok())
        .ok_or_else(|| anyhow!("missing finalized block timestamp"))?;
    if timestamp <= submission.deadline_secs {
        return Ok(None);
    }
    let hash = block["hash"]
        .as_str()
        .ok_or_else(|| anyhow!("missing finalized block hash"))?;
    if word(&block["hash"])? == [0;32] {return Err(anyhow!("zero finalized block hash"));}
    let number = block["number"]
        .as_str()
        .ok_or_else(|| anyhow!("missing finalized block number"))?;
    u64::from_str_radix(number.strip_prefix("0x").ok_or_else(||anyhow!("invalid finalized block number"))?,16)?;
    let at = json!({"blockHash":hash,"requireCanonical":true});
    let mut call = |signature: &[u8]| -> Result<[u8; 32]> {
        let selector = format!("0x{}", hex::encode(&keccak256(signature)[..4]));
        let r = rpc(
            "eth_call",
            json!([{"to":submission.deposit_wallet,"data":selector},at]),
        )?;
        word(&r["result"])
    };
    let owner = call(b"owner()")?;
    if owner[..12] != [0; 12]
        || !hex::encode(&owner[12..]).eq_ignore_ascii_case(&submission.signer[2..])
    {
        return Err(anyhow!(
            "finalized WALLET owner differs from journal signer"
        ));
    }
    let counter = call(b"nonce()")?;
    let mut expected = [0u8; 32];
    expected[16..].copy_from_slice(&nonce.to_be_bytes());
    let unchanged = rpc("eth_getBlockByNumber", json!([number, false]))?;
    if unchanged["result"]["hash"].as_str() != Some(hash) {
        return Err(anyhow!("finalized recovery anchor changed"));
    }
    Ok((counter <= expected).then(|| UnusedNonceProof {
        block_number: number.into(),
        block_hash: hash.into(),
        timestamp,
    }))
}

/// Cold recovery acknowledgement: never start a subsequent submission until
/// the account owner applied and durably flushed this exact terminal result.
pub(super) fn finalize_expired<R>(
    account: &hexagent_account::account::shared_account::SharedAccount,
    operation_id: &str,
    intent: &MaintenanceWalletSubmission,
    rpc: R,
) -> Result<bool>
where
    R: FnMut(&str, Value) -> Result<Value>,
{
    use hexagent_account::account::shared_account::MaintenanceOperationStatus;
    let Some(proof) = expired_nonce_unused(intent, rpc)? else {
        return Ok(false);
    };
    account.fail_maintenance_operation(operation_id,format!(
        "expired unexecuted WALLET nonce={} deadline={} finalized_number={} finalized_hash={} finalized_timestamp={}",
        intent.nonce,intent.deadline_secs,proof.block_number,proof.block_hash,proof.timestamp));
    let until = Instant::now() + Duration::from_secs(2);
    loop {
        if account
            .maintenance_operation(operation_id)
            .is_some_and(|op| op.status == MaintenanceOperationStatus::Failed)
        {
            break;
        }
        if Instant::now() >= until {
            return Err(anyhow!(
                "terminal split journal owner acknowledgement timed out"
            ));
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    account
        .flush_persistence(Duration::from_secs(2))
        .map_err(anyhow::Error::msg)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn intent() -> MaintenanceWalletSubmission {
        MaintenanceWalletSubmission {
            signer: format!("0x{}", "11".repeat(20)),
            deposit_wallet: format!("0x{}", "22".repeat(20)),
            nonce: "7".into(),
            body_hash: "aa".repeat(32),
            deadline_secs: 100,
            retry_until_secs: None,
        }
    }
    fn fixture(method: &str, p: Value, counter: u64) -> Result<Value> {
        Ok(match method {
            "eth_chainId" => json!({"result":"0x89"}),
            "eth_getBlockByNumber" => {
                json!({"result":{"timestamp":"0x65","number":"0xa","hash":format!("0x{}","ab".repeat(32))}})
            }
            "eth_call" => {
                assert_eq!(p[1]["requireCanonical"], true);
                assert_eq!(p[1]["blockHash"], format!("0x{}", "ab".repeat(32)));
                if p[0]["data"] == format!("0x{}", hex::encode(&keccak256(b"owner()")[..4])) {
                    json!({"result":format!("0x{}{}","00".repeat(12),"11".repeat(20))})
                } else {
                    json!({"result":format!("0x{counter:064x}")})
                }
            }
            _ => panic!("unexpected RPC {method}"),
        })
    }
    #[test]
    fn finalized_unused_nonce_proves_absence_but_consumed_nonce_is_unknown() {
        for counter in [0, 6, 7] {
            assert!(
                expired_nonce_unused(&intent(), |m, p| fixture(m, p, counter))
                    .unwrap()
                    .is_some()
            );
        }
        assert!(!expired_nonce_unused(&intent(), |m, p| fixture(m, p, 8))
            .unwrap()
            .is_some());
    }
    #[test]
    fn query_errors_malformed_owner_unfinalized_and_reorg_never_release_funds() {
        for broken in 0..6 {
            let r = expired_nonce_unused(&intent(), |m, p| {
                if broken == 0 {
                    return Err(anyhow!("429"));
                }
                let mut r = fixture(m, p.clone(), 7)?;
                if broken == 1 && m == "eth_call" {
                    r["result"] = json!("0x");
                }
                if broken == 2 && m == "eth_call" {
                    r["result"] = json!(format!("0x{}", "33".repeat(32)));
                }
                if broken == 3 && m == "eth_getBlockByNumber" {
                    r["result"] = Value::Null;
                }
                if broken == 4 && m == "eth_chainId" {
                    r["result"] = json!("0x1");
                }
                if broken == 5 && m == "eth_getBlockByNumber" && p[0] != "finalized" {
                    r["result"]["hash"] = json!(format!("0x{}", "cd".repeat(32)));
                }
                Ok(r)
            });
            assert!(r.is_err(), "fault {broken}");
        }
        assert!(!expired_nonce_unused(&intent(), |m, p| {
            let mut r = fixture(m, p, 7)?;
            if m == "eth_getBlockByNumber" {
                r["result"]["timestamp"] = json!("0x64");
            }
            Ok(r)
        })
        .unwrap()
        .is_some());
    }
    #[test]
    fn bounded_backoff_is_owner_local_and_resets_after_success() {
        let start = Instant::now();
        let mut a = RecoveryBackoff::default();
        let b = RecoveryBackoff::default();
        for _ in 0..100 {
            a.failed(start, b"account-a");
        }
        assert!(a.remaining(start) >= Duration::from_secs(60));
        assert!(a.remaining(start) < Duration::from_secs(61));
        assert_eq!(b.remaining(start), Duration::ZERO);
        assert_ne!(
            retry_delay(1, b"account-a", true),
            retry_delay(1, b"account-b", true)
        );
        a.reset();
        assert_eq!(a.remaining(start), Duration::ZERO);
    }

    #[test]
    fn expired_503_journal_releases_once_survives_restart_and_admits_next_event() {
        use hexagent_account::account::shared_account::{MaintenanceOperationKind, SharedAccount};
        use std::collections::HashMap;
        let path = std::env::temp_dir().join(format!(
            "split-503-recovery-{}-{}.json",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        {
            let account = SharedAccount::new_persistent("split-recovery", &path).unwrap();
            account.register_instance("a", 1.0);
            account.register_instance("b", 1.0);
            account
                .apply_physical_snapshot(200.0, HashMap::new())
                .unwrap();
            account
                .reserve_maintenance_operation(
                    "expired",
                    MaintenanceOperationKind::Split,
                    "old-event",
                    "UP",
                    "DOWN",
                    &HashMap::from([("a".into(), 25.0)]),
                )
                .unwrap();
            account
                .prepare_maintenance_wallet_submission("expired", intent())
                .unwrap();
            account.mark_maintenance_operation_uncertain("expired", "503 relayer is paused");
            assert!(account.is_uncertain());
            assert_eq!(account.monitoring_snapshot().reserved_cash, 25.0);
            // An unavailable RPC cannot stand in for a negative chain proof.
            assert!(
                finalize_expired(&account, "expired", &intent(), |_, _| Err(anyhow!("429")))
                    .is_err()
            );
            assert_eq!(account.monitoring_snapshot().reserved_cash, 25.0);
            assert!(
                finalize_expired(&account, "expired", &intent(), |m, p| fixture(m, p, 7)).unwrap()
            );
            assert!(
                finalize_expired(&account, "expired", &intent(), |m, p| fixture(m, p, 7)).unwrap()
            );
            account.mark_maintenance_operation_uncertain("expired", "late duplicate timeout");
            assert!(!account.is_uncertain());
            assert_eq!(account.monitoring_snapshot().reserved_cash, 0.0);
            assert_eq!(account.monitoring_snapshot().physical_cash, 200.0);
            account.flush_persistence(Duration::from_secs(2)).unwrap();
        }
        {
            let account = SharedAccount::new_persistent("split-recovery", &path).unwrap();
            assert!(account.pending_maintenance_operations().is_empty());
            account
                .apply_physical_snapshot(200.0, std::collections::HashMap::new())
                .unwrap();
            account
                .reserve_maintenance_operation(
                    "next",
                    MaintenanceOperationKind::Split,
                    "next-event",
                    "U2",
                    "D2",
                    &HashMap::from([("b".into(), 30.0)]),
                )
                .unwrap();
            assert_eq!(account.monitoring_snapshot().reserved_cash, 30.0);
            account.fail_maintenance_operation("next", "test cleanup");
        }
    }
}
