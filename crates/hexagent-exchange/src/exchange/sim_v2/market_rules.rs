//! Immutable, startup-validated venue rules. A contemporary API response is
//! not evidence for an older market. Historical observations and sensitivity
//! assumptions are different journal modes and remain different in reports.
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::Path};

const MAX_RULES: usize = 16_384;
const MAX_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleEvidence {
    HistoricalMarketSnapshot,
    ModeledSensitivity,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MarketRule {
    pub token: String,
    pub valid_from_ns: u64,
    pub valid_until_ns: u64,
    pub taker_hold_ms: u64,
    pub evidence: RuleEvidence,
    pub provenance: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    schema_version: u32,
    mode: String,
    rules: Vec<MarketRule>,
}

#[derive(Default)]
pub struct MarketRules {
    by_token: HashMap<String, Vec<MarketRule>>,
    pub historical_rules: usize,
    pub sensitivity_rules: usize,
}

impl MarketRules {
    pub fn from_optional_path(path: &str) -> anyhow::Result<Self> {
        if path.is_empty() {
            return Ok(Self::default());
        }
        let path = Path::new(path);
        anyhow::ensure!(
            std::fs::metadata(path)?.len() <= MAX_BYTES,
            "market rules journal exceeds capacity"
        );
        Self::from_json(&std::fs::read(path)?)
    }

    pub fn from_json(bytes: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(
            bytes.len() <= MAX_BYTES as usize,
            "market rules journal exceeds capacity"
        );
        let journal: Journal = serde_json::from_slice(bytes)?;
        anyhow::ensure!(
            journal.schema_version == 1
                && matches!(journal.mode.as_str(), "historical" | "sensitivity"),
            "unsupported market rules version/mode"
        );
        anyhow::ensure!(journal.rules.len() <= MAX_RULES, "too many market rules");
        let mut out = Self::default();
        for rule in journal.rules {
            anyhow::ensure!(
                !rule.token.is_empty()
                    && rule.token.len() <= 256
                    && !rule.provenance.trim().is_empty(),
                "market rule lacks token/provenance"
            );
            anyhow::ensure!(
                rule.valid_from_ns < rule.valid_until_ns && rule.taker_hold_ms <= 10_000,
                "invalid market rule time/hold"
            );
            anyhow::ensure!(
                (journal.mode == "historical")
                    == (rule.evidence == RuleEvidence::HistoricalMarketSnapshot),
                "market rule evidence does not match journal mode"
            );
            match rule.evidence {
                RuleEvidence::HistoricalMarketSnapshot => out.historical_rules += 1,
                RuleEvidence::ModeledSensitivity => out.sensitivity_rules += 1,
            }
            out.by_token
                .entry(rule.token.clone())
                .or_default()
                .push(rule);
        }
        for rules in out.by_token.values_mut() {
            rules.sort_unstable_by_key(|rule| rule.valid_from_ns);
            anyhow::ensure!(
                rules
                    .windows(2)
                    .all(|w| w[0].valid_until_ns <= w[1].valid_from_ns),
                "overlapping market rule intervals"
            );
        }
        Ok(out)
    }

    pub fn at(&self, token: &str, now_ns: u64) -> Option<&MarketRule> {
        let rules = self.by_token.get(token)?;
        let i = rules.partition_point(|rule| rule.valid_from_ns <= now_ns);
        let rule = rules.get(i.checked_sub(1)?)?;
        (now_ns < rule.valid_until_ns).then_some(rule)
    }
}

/// Reserve a known hold INSIDE a complete HTTP budget. The remainder includes
/// unobserved network/gateway processing, not claimed pure network transit.
/// Recorded budgets smaller than the rule are contradictory, never stretched.
pub fn partition_hold_budget(
    total_ns: u64,
    hold_ns: u64,
    outbound_bps: u16,
) -> anyhow::Result<(u64, u64)> {
    anyhow::ensure!(
        (1..10_000).contains(&outbound_bps),
        "invalid outbound fraction"
    );
    anyhow::ensure!(
        total_ns >= hold_ns.saturating_add(2),
        "observed RTT cannot contain the configured taker hold and two positive legs"
    );
    let remaining = total_ns - hold_ns;
    let l1 = ((remaining as u128 * outbound_bps as u128) / 10_000) as u64;
    let l1 = l1.clamp(1, remaining - 1);
    Ok((l1, remaining - l1))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn journal(mode: &str, evidence: &str) -> String {
        format!(
            r#"{{"schema_version":1,"mode":"{mode}","rules":[{{"token":"yes","valid_from_ns":100,"valid_until_ns":200,"taker_hold_ms":250,"evidence":"{evidence}","provenance":"fixture"}}]}}"#
        )
    }
    #[test]
    fn rules_do_not_leak_across_tokens_or_effective_intervals() {
        let rules =
            MarketRules::from_json(journal("historical", "historical_market_snapshot").as_bytes())
                .unwrap();
        assert!(rules.at("yes", 99).is_none());
        assert_eq!(rules.at("yes", 100).unwrap().taker_hold_ms, 250);
        assert!(rules.at("yes", 200).is_none());
        assert!(rules.at("no", 150).is_none());
    }
    #[test]
    fn sensitivity_cannot_masquerade_as_historical_evidence() {
        assert!(
            MarketRules::from_json(journal("historical", "modeled_sensitivity").as_bytes())
                .is_err()
        );
        let rules =
            MarketRules::from_json(journal("sensitivity", "modeled_sensitivity").as_bytes())
                .unwrap();
        assert_eq!((rules.historical_rules, rules.sensitivity_rules), (0, 1));
    }
    #[test]
    fn hold_partition_preserves_odd_rtt_and_rejects_impossible_budget() {
        let (l1, l2) = partition_hold_budget(300_000_001, 250_000_000, 3000).unwrap();
        assert!(l1 > 0 && l2 > 0);
        assert_eq!(l1 + 250_000_000 + l2, 300_000_001);
        assert!(partition_hold_budget(200_000_000, 250_000_000, 5000).is_err());
        assert!(partition_hold_budget(300_000_000, 250_000_000, 10_000).is_err());
    }
}
