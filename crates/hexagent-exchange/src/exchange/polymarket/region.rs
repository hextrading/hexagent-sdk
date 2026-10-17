//! Owner-local region pause and cold, read-only geographic eligibility checks.
//! Each event owns a fresh guard. Five consecutive distinct regional placement
//! failures pause that event only. GET checks are diagnostic; manual early
//! recovery requires fresh allowed evidence. Other risk gates remain in force.
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

pub const REJECT_LIMIT: u32 = 5;

pub const GEOBLOCK_URL: &str = "https://polymarket.com/api/geoblock";
pub const CHECK_INTERVAL_NS: u64 = 30_000_000_000;
pub const EVIDENCE_TTL_NS: u64 = 30_000_000_000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Eligibility {
    Allowed,
    Blocked,
    #[default]
    Unknown,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Observation {
    pub eligibility: Eligibility,
    pub checked_ns: u64,
    pub http_status: u16,
    pub ip: Option<IpAddr>,
    pub country: [u8; 2],
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct RegionGuard {
    pub consecutive_rejects: u32,
    pub paused: bool,
    pub epoch: u64,
    pub rejected_ns: u64,
    pub observation: Observation,
    pub last_command_ns: u64,
    pub last_resume: ResumeResult,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResumeResult {
    #[default]
    None,
    Applied,
    Duplicate,
    WrongEpoch,
    ExpiredCommand,
    NotEligible,
    StaleEvidence,
    NotPaused,
}

impl RegionGuard {
    pub fn reject(&mut self, now_ns: u64) {
        self.consecutive_rejects = self.consecutive_rejects.saturating_add(1);
        self.paused |= self.consecutive_rejects >= REJECT_LIMIT;
        self.epoch = self.epoch.saturating_add(1);
        self.rejected_ns = now_ns;
        self.observation = Observation::default();
        self.last_resume = ResumeResult::None;
    }

    /// Only the first authoritative placement outcome for an order is applied.
    /// Non-region rejection also breaks a *consecutive regional* failure streak.
    /// An in-flight success cannot undo an already latched event pause.
    pub fn non_region_outcome(&mut self) {
        if !self.paused {
            self.consecutive_rejects = 0;
        }
    }

    pub fn observe(&mut self, epoch: u64, value: Observation, now_ns: u64) -> bool {
        if epoch != self.epoch
            || value.checked_ns < self.rejected_ns
            || value.checked_ns < self.observation.checked_ns
            || value.checked_ns > now_ns
        {
            return false;
        }
        self.observation = value;
        true
    }

    pub fn resume(&mut self, epoch: u64, command_ns: u64, now_ns: u64) -> ResumeResult {
        let result = if command_ns <= self.last_command_ns {
            ResumeResult::Duplicate
        } else if command_ns > now_ns || now_ns - command_ns > EVIDENCE_TTL_NS {
            ResumeResult::ExpiredCommand
        } else if epoch != self.epoch || self.epoch == u64::MAX {
            ResumeResult::WrongEpoch
        } else if !self.paused {
            ResumeResult::NotPaused
        } else if self.observation.eligibility != Eligibility::Allowed {
            ResumeResult::NotEligible
        } else if self.observation.checked_ns < command_ns
            || self.observation.checked_ns < self.rejected_ns
            || self.observation.checked_ns > now_ns
            || now_ns - self.observation.checked_ns > EVIDENCE_TTL_NS
        {
            ResumeResult::StaleEvidence
        } else {
            self.paused = false;
            self.consecutive_rejects = 0;
            ResumeResult::Applied
        };
        // Invalid future timestamps must not poison deduplication.
        if command_ns <= now_ns {
            self.last_command_ns = self.last_command_ns.max(command_ns);
        }
        if result != ResumeResult::Duplicate {
            self.last_resume = result;
        }
        result
    }
}

#[derive(Deserialize)]
struct GeoResponse {
    blocked: bool,
    ip: IpAddr,
    country: String,
    region: String,
}

fn parse(body: &[u8], status: u16, now_ns: u64) -> Observation {
    let unknown = Observation {
        checked_ns: now_ns,
        http_status: status,
        ..Default::default()
    };
    if status != 200 || body.len() > 4096 {
        return unknown;
    }
    let Ok(r) = serde_json::from_slice::<GeoResponse>(body) else {
        return unknown;
    };
    if r.country.len() != 2
        || !r.country.bytes().all(|b| b.is_ascii_uppercase())
        || r.region.len() > 64
    {
        return unknown;
    }
    Observation {
        eligibility: if r.blocked {
            Eligibility::Blocked
        } else {
            Eligibility::Allowed
        },
        ip: Some(r.ip),
        country: [r.country.as_bytes()[0], r.country.as_bytes()[1]],
        ..unknown
    }
}

/// Called exclusively by an existing background worker. Fixed official HTTPS
/// endpoint, no redirects/proxies, bounded body and whole-operation deadline.
pub fn check_region() -> Observation {
    crate::async_rt::block_on_runtime(async {
        match tokio::time::timeout(std::time::Duration::from_secs(5), fetch()).await {
            Ok(value) => value,
            Err(_) => Observation {
                checked_ns: crate::types::now_ns(),
                ..Default::default()
            },
        }
    })
}

async fn fetch() -> Observation {
    let unknown = || Observation {
        checked_ns: crate::types::now_ns(),
        ..Default::default()
    };
    let Ok(client) = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(5))
        .build()
    else {
        return unknown();
    };
    let Ok(mut response) = client.get(GEOBLOCK_URL).send().await else {
        return unknown();
    };
    let status = response.status().as_u16();
    if status != 200 {
        return parse(&[], status, crate::types::now_ns());
    }
    let mut body = Vec::with_capacity(4096);
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) if body.len() + chunk.len() <= 4096 => body.extend_from_slice(&chunk),
            Ok(None) => break,
            _ => return parse(&[], status, crate::types::now_ns()),
        }
    }
    parse(&body, status, crate::types::now_ns())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn allowed(t: u64) -> Observation {
        parse(
            br#"{"blocked":false,"ip":"203.0.113.1","country":"IE","region":"D"}"#,
            200,
            t,
        )
    }
    #[test]
    fn malformed_http_and_missing_fields_fail_closed() {
        assert_eq!(allowed(3).eligibility, Eligibility::Allowed);
        for b in [
            b"{}".as_slice(),
            b"null",
            b"{\"blocked\":false}",
            b"<html>200 OK</html>",
        ] {
            assert_eq!(parse(b, 200, 3).eligibility, Eligibility::Unknown);
        }
        assert_eq!(parse(b"{}", 403, 3).eligibility, Eligibility::Unknown);
        assert_eq!(
            parse(
                br#"{"blocked":true,"ip":"16.61.189.122","country":"GB","region":"ENG"}"#,
                200,
                3
            )
            .eligibility,
            Eligibility::Blocked
        );
    }
    fn paused() -> RegionGuard {
        let mut g = RegionGuard::default();
        for t in 1..=5 {
            g.reject(t);
        }
        g
    }
    #[test]
    fn fifth_consecutive_reject_pauses_only_this_event() {
        let mut a = RegionGuard::default();
        for t in 1..5 {
            a.reject(t);
            assert!(!a.paused);
        }
        a.non_region_outcome();
        assert_eq!(a.consecutive_rejects, 0);
        for t in 5..=9 {
            a.reject(t);
        }
        assert!(a.paused);
        a.non_region_outcome();
        assert!(a.paused);
        let next_event = RegionGuard::default();
        assert!(!next_event.paused);
        assert_eq!(next_event.consecutive_rejects, 0);
    }
    #[test]
    fn fresh_manual_resume_only_and_instance_isolation() {
        let mut a = paused();
        let b = paused();
        assert!(a.observe(5, allowed(30), 30));
        assert!(a.paused);
        assert_eq!(a.resume(5, 20, 30), ResumeResult::Applied);
        assert!(!a.paused);
        assert!(b.paused);
        assert_eq!(a.consecutive_rejects, 0);
        assert_eq!(a.resume(5, 20, 31), ResumeResult::Duplicate);
    }
    #[test]
    fn rejection_fences_inflight_checks_commands_and_replay() {
        let mut a = paused();
        a.reject(20);
        assert!(!a.observe(5, allowed(30), 30));
        assert_eq!(a.resume(5, 21, 30), ResumeResult::WrongEpoch);
        assert!(a.paused);
        a.observe(6, allowed(30), 30);
        assert_eq!(a.resume(6, 31, 32), ResumeResult::StaleEvidence);
        assert_eq!(
            a.resume(6, 33, 33 + EVIDENCE_TTL_NS + 1),
            ResumeResult::ExpiredCommand
        );
        a.observe(
            6,
            Observation {
                checked_ns: 40,
                ..Default::default()
            },
            40,
        );
        assert_eq!(a.resume(6, 35, 40), ResumeResult::NotEligible);
        assert!(a.paused);
    }
    #[test]
    fn blocked_or_unknown_checks_never_resume() {
        for eligibility in [Eligibility::Blocked, Eligibility::Unknown] {
            let mut a = paused();
            a.observe(
                5,
                Observation {
                    eligibility,
                    checked_ns: 30,
                    ..Default::default()
                },
                30,
            );
            assert_eq!(a.resume(5, 20, 30), ResumeResult::NotEligible);
            assert!(a.paused);
        }
    }
    #[test]
    fn future_and_out_of_order_evidence_cannot_clear_or_poison() {
        let mut a = paused();
        assert!(!a.observe(5, allowed(31), 30));
        a.observe(5, allowed(30), 30);
        assert!(!a.observe(5, allowed(20), 31));
        assert_eq!(a.resume(5, u64::MAX, 31), ResumeResult::ExpiredCommand);
        assert_eq!(a.resume(5, 25, 31), ResumeResult::Applied);
    }
}
