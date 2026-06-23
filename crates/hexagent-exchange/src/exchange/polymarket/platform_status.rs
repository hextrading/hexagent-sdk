//! Polymarket platform status check (Instatus summary feed).
//!
//! Polls `https://status.polymarket.com/v3/summary.json` (Instatus format)
//! and extracts `page.status`. `"UP"` means all systems operational; any
//! other value (`"HASISSUES"`, `"UNDERMAINTENANCE"`, …) means the platform
//! is degraded, in which case the polymaker pauses quoting + maintenance for
//! the affected event (checked once per event at registration).

use anyhow::{anyhow, Result};
use serde::Deserialize;

/// Default Instatus summary endpoint for Polymarket.
pub const DEFAULT_STATUS_URL: &str = "https://status.polymarket.com/v3/summary.json";

/// The `"UP"` sentinel returned by `page.status` when the platform is healthy.
pub const STATUS_UP: &str = "UP";

#[derive(Debug, Deserialize)]
struct SummaryResponse {
    page: PageStatus,
}

#[derive(Debug, Deserialize)]
struct PageStatus {
    /// Overall platform status, e.g. "UP" / "HASISSUES" / "UNDERMAINTENANCE".
    status: String,
}

/// Fetch the platform `page.status` string (e.g. `"UP"`).
///
/// Blocking; uses the shared query HTTP client with a couple of retries
/// (sub-second on the happy path). Returns the raw status string so the
/// caller can log it verbatim. An empty `url` falls back to
/// [`DEFAULT_STATUS_URL`].
pub fn fetch_platform_status(url: &str) -> Result<String> {
    let url = if url.is_empty() { DEFAULT_STATUS_URL } else { url };
    // 2 attempts × 200 ms base backoff: one request on the happy path, a
    // single quick retry on a transient blip — kept short since this runs
    // inline at event registration.
    let body = crate::async_rt::blocking_get_text_retry(url, 2, 200)?;
    parse_status(&body)
}

/// Extract `page.status` from a raw Instatus summary JSON body. Split out
/// from [`fetch_platform_status`] so the parse is unit-testable without a
/// network round-trip.
fn parse_status(body: &str) -> Result<String> {
    let parsed: SummaryResponse = serde_json::from_str(body).map_err(|e| {
        anyhow!(
            "parse Polymarket status summary: {} (body: {})",
            e,
            &body[..body.len().min(200)]
        )
    })?;
    Ok(parsed.page.status)
}

/// True iff the status string represents the healthy `"UP"` state
/// (case-insensitive).
pub fn is_up(status: &str) -> bool {
    status.eq_ignore_ascii_case(STATUS_UP)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_up_status() {
        let body = r#"{"page":{"name":"Polymarket","url":"https://status.polymarket.com","status":"UP"},"activeMaintenances":[]}"#;
        assert_eq!(parse_status(body).unwrap(), "UP");
        assert!(is_up(&parse_status(body).unwrap()));
    }

    #[test]
    fn parses_non_up_status() {
        // Real shape observed from the live feed during scheduled CLOB maintenance.
        let body = r#"{"page":{"name":"Polymarket","url":"https://status.polymarket.com","status":"UNDERMAINTENANCE"},"activeMaintenances":[{"id":"x","name":"Scheduled CLOB Maintenance","status":"INPROGRESS"}]}"#;
        let status = parse_status(body).unwrap();
        assert_eq!(status, "UNDERMAINTENANCE");
        assert!(!is_up(&status));
    }

    #[test]
    fn is_up_is_case_insensitive() {
        assert!(is_up("UP"));
        assert!(is_up("up"));
        assert!(!is_up("HASISSUES"));
        assert!(!is_up(""));
    }

    #[test]
    fn malformed_body_errors() {
        assert!(parse_status("not json").is_err());
        // Missing `page` field.
        assert!(parse_status(r#"{"activeMaintenances":[]}"#).is_err());
    }
}
