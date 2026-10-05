//! Startup-only exact request join. Repeated cancels have distinct attempt and
//! dispatch identities. Runtime lookup is a prealigned immutable vector index.
use super::Command;
use anyhow::{Context, Result};
use hexagent_exchange::exchange::sim_v2::CancelTiming;
use serde::Deserialize;
use std::{
    collections::HashSet,
    fs::File,
    io::{BufRead, BufReader},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    coid: String,
    iid: String,
    token: String,
    event_id: String,
    epoch: u64,
    attempt_id: u64,
    dispatched_ns: u64,
    completed_ns: u64,
    lower_ns: Option<u64>,
    upper_ns: Option<u64>,
    arrival_ns: Option<u64>,
    effective_ns: Option<u64>,
    return_ns: Option<u64>,
    provenance: String,
}

impl Record {
    fn validate(&self, c: &Command) -> Result<Option<CancelTiming>> {
        anyhow::ensure!(
            c.kind == "cancel"
                && self.coid == c.coid
                && self.iid == c.iid
                && self.token == c.token
                && self.event_id == c.event_id
                && self.epoch == c.epoch
                && self.attempt_id == c.attempt_id
                && self.dispatched_ns == c.dispatched_ns
                && self.completed_ns == c.completed_ns
                && !self.provenance.is_empty(),
            "cancel transport identity/order mismatch"
        );
        match (
            self.lower_ns,
            self.upper_ns,
            self.arrival_ns,
            self.effective_ns,
            self.return_ns,
        ) {
            (None, None, None, None, None) => Ok(None),
            (Some(lo), Some(hi), Some(arrival), Some(effective), Some(ret)) => {
                anyhow::ensure!(
                    !c.observed_http_status
                        .as_deref()
                        .is_some_and(|s| s.to_ascii_lowercase().contains("timeout")),
                    "censored cancel cannot supply observed response bounds"
                );
                CancelTiming::from_transport_stages(
                    c.dispatched_ns,
                    c.completed_ns,
                    lo,
                    hi,
                    arrival,
                    effective,
                    ret,
                )
                .map(Some)
                .map_err(|e| anyhow::anyhow!(e))
            }
            _ => anyhow::bail!("incomplete cancel transport evidence"),
        }
    }
}

pub(super) fn load(path: &str, commands: &[Command]) -> Result<Vec<Option<CancelTiming>>> {
    let mut lines = BufReader::new(File::open(path)?).lines();
    let mut seen = HashSet::with_capacity(commands.len());
    let mut selected = Vec::with_capacity(commands.len());
    for c in commands {
        let timing = if c.kind == "cancel" {
            anyhow::ensure!(
                seen.insert((&c.iid, &c.coid, c.attempt_id, c.dispatched_ns)),
                "duplicate cancel request in recorded commands"
            );
            let line = lines.next().context("missing cancel transport row")??;
            anyhow::ensure!(
                line.len() <= 16_384,
                "cancel transport row exceeds startup bound"
            );
            serde_json::from_str::<Record>(&line)?.validate(c)?
        } else {
            None
        };
        selected.push(timing);
    }
    anyhow::ensure!(lines.next().is_none(), "extra cancel transport row");
    Ok(selected)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (Command, serde_json::Value) {
        let v = serde_json::json!({"kind":"cancel","coid":"btc01-a","iid":"btc01","event_id":"e","token":"up","side":"BUY","order_type":"Limit","price":0.5,"quantity":10.,"post_only":false,"reduce_only":false,"fee_rate_bps":700,"dispatched_ns":100,"completed_ns":200,"trigger_exchange_ns":0,"trigger_local_ns":0,"epoch":1,"attempt_id":3});
        let c = serde_json::from_value(v).unwrap();
        let r = serde_json::json!({"coid":"btc01-a","iid":"btc01","event_id":"e","token":"up","epoch":1,"attempt_id":3,"dispatched_ns":100,"completed_ns":200,"lower_ns":110,"upper_ns":195,"arrival_ns":120,"effective_ns":150,"return_ns":20,"provenance":"client bounds; modeled point"});
        (c, r)
    }
    #[test]
    fn cancel_transport_binds_owner_attempt_and_censored_response() {
        let (c, r) = fixture();
        let row: Record = serde_json::from_value(r.clone()).unwrap();
        let t = row.validate(&c).unwrap().unwrap();
        assert_eq!(t.post_effect_ns, 30);
        let (mut c, _) = fixture();
        c.iid = "btc02".into();
        assert!(row.validate(&c).is_err());
        let (mut c, _) = fixture();
        c.attempt_id = 4;
        assert!(row.validate(&c).is_err());
        let (mut c, _) = fixture();
        c.observed_http_status = Some("CancelOrderTimeout".into());
        assert!(row.validate(&c).is_err());
        let (c, mut r) = fixture();
        r["effective_ns"] = 196.into();
        assert!(serde_json::from_value::<Record>(r)
            .unwrap()
            .validate(&c)
            .is_err());
    }
    #[test]
    fn cancel_transport_fallback_is_explicit_and_partial_evidence_fails() {
        let (c, mut r) = fixture();
        r["lower_ns"] = serde_json::Value::Null;
        assert!(serde_json::from_value::<Record>(r.clone())
            .unwrap()
            .validate(&c)
            .is_err());
        for k in ["upper_ns", "arrival_ns", "effective_ns", "return_ns"] {
            r[k] = serde_json::Value::Null;
        }
        assert!(serde_json::from_value::<Record>(r)
            .unwrap()
            .validate(&c)
            .unwrap()
            .is_none());
    }
    #[test]
    fn cancel_transport_order_duplicates_and_replay_are_checked() {
        let path =
            std::env::temp_dir().join(format!("cancel-transport-{}.jsonl", std::process::id()));
        let (c, r) = fixture();
        let encoded = serde_json::to_string(&r).unwrap();
        std::fs::write(&path, format!("{encoded}\n")).unwrap();
        let a = load(path.to_str().unwrap(), &[c]).unwrap();
        let (c, _) = fixture();
        assert_eq!(a, load(path.to_str().unwrap(), &[c]).unwrap());
        std::fs::write(&path, format!("{encoded}\n{encoded}\n")).unwrap();
        let (c, _) = fixture();
        assert!(load(path.to_str().unwrap(), &[c]).is_err());
        let (c, _) = fixture();
        let (d, _) = fixture();
        assert!(load(path.to_str().unwrap(), &[c, d]).is_err());
        std::fs::write(&path, "").unwrap();
        let (c, _) = fixture();
        assert!(load(path.to_str().unwrap(), &[c]).is_err());
        std::fs::remove_file(path).unwrap();
    }
}
