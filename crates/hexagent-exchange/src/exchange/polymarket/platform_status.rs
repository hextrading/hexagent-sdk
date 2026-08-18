//! Polymarket Predictions component-status check.
//!
//! Polymarket's status page covers both Predictions and Perpetuals. Quoting
//! Predictions must therefore depend only on components that either are named
//! `Predictions` or belong to the `Predictions` group. Every matching
//! component must be `OPERATIONAL`; unrelated component failures are ignored.

use anyhow::{anyhow, Result};
use serde::Deserialize;

/// Default Instatus components endpoint for Polymarket.
pub const DEFAULT_STATUS_URL: &str = "https://status.polymarket.com/v3/components.json";

/// The component status required for Predictions trading.
pub const STATUS_OPERATIONAL: &str = "OPERATIONAL";

/// Compatibility alias for callers that previously imported `STATUS_UP`.
#[deprecated(note = "use STATUS_OPERATIONAL")]
pub const STATUS_UP: &str = STATUS_OPERATIONAL;

const PREDICTIONS_NAME: &str = "Predictions";

#[derive(Debug, Deserialize)]
struct ComponentsResponse {
    components: Vec<ComponentStatus>,
}

#[derive(Debug, Deserialize)]
struct ComponentStatus {
    #[serde(default)]
    name: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    group: Option<ComponentGroup>,
}

#[derive(Debug, Deserialize)]
struct ComponentGroup {
    #[serde(default)]
    name: String,
}

/// Aggregated health of all components that serve Predictions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredictionsStatus {
    /// Number of components named `Predictions` or grouped under it.
    pub matched_components: usize,
    /// `name=status` entries for every matching component that is not
    /// `OPERATIONAL`. Missing/malformed status data is returned as an error so
    /// callers can distinguish an unavailable status page from a venue outage.
    pub non_operational: Vec<String>,
}

impl PredictionsStatus {
    /// True only when every matched Predictions component is operational.
    pub fn is_operational(&self) -> bool {
        self.non_operational.is_empty()
    }
}

/// Fetch and evaluate the status of Polymarket's Predictions components.
///
/// Blocking; uses the shared query HTTP client with one quick retry. An empty
/// URL falls back to [`DEFAULT_STATUS_URL`]. A response without any matching
/// Predictions component is an error. Callers should treat fetch/schema errors
/// as status-page unavailability, not as proof that trading is unavailable.
pub fn fetch_predictions_status(url: &str) -> Result<PredictionsStatus> {
    let url = if url.is_empty() {
        DEFAULT_STATUS_URL
    } else {
        url
    };
    let body = crate::async_rt::blocking_get_text_retry(url, 2, 200)?;
    parse_predictions_status(&body)
}

/// Compatibility wrapper for the previous string-returning API.
///
/// Returns `OPERATIONAL` when Predictions trading is healthy, otherwise a
/// comma-separated list of non-operational Predictions components.
pub fn fetch_platform_status(url: &str) -> Result<String> {
    let status = fetch_predictions_status(url)?;
    if status.is_operational() {
        Ok(STATUS_OPERATIONAL.to_string())
    } else {
        Ok(status.non_operational.join(", "))
    }
}

/// Compatibility predicate for the previous `is_up` API.
pub fn is_up(status: &str) -> bool {
    status.eq_ignore_ascii_case(STATUS_OPERATIONAL)
}

fn parse_predictions_status(body: &str) -> Result<PredictionsStatus> {
    let parsed: ComponentsResponse = serde_json::from_str(body).map_err(|error| {
        let excerpt: String = body.chars().take(200).collect();
        anyhow!(
            "parse Polymarket component status: {} (body: {})",
            error,
            excerpt
        )
    })?;

    let mut matched_components = 0;
    let mut matched_parent = false;
    let mut non_operational = Vec::new();

    for component in parsed.components {
        let is_predictions_parent = component.name == PREDICTIONS_NAME;
        let belongs_to_predictions = is_predictions_parent
            || component
                .group
                .as_ref()
                .is_some_and(|group| group.name == PREDICTIONS_NAME);
        if !belongs_to_predictions {
            continue;
        }

        matched_components += 1;
        matched_parent |= is_predictions_parent;
        let Some(status) = component.status.as_deref() else {
            let name = if component.name.is_empty() {
                "<unnamed>"
            } else {
                component.name.as_str()
            };
            return Err(anyhow!(
                "Polymarket Predictions component `{name}` has no status"
            ));
        };
        if status != STATUS_OPERATIONAL {
            let name = if component.name.is_empty() {
                "<unnamed>"
            } else {
                component.name.as_str()
            };
            non_operational.push(format!("{name}={status}"));
        }
    }

    if matched_components == 0 {
        return Err(anyhow!(
            "Polymarket component status contains no Predictions components"
        ));
    }
    if !matched_parent {
        return Err(anyhow!(
            "Polymarket component status contains no Predictions parent component"
        ));
    }

    Ok(PredictionsStatus {
        matched_components,
        non_operational,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn predictions_are_operational_when_all_matching_components_are_operational() {
        let body = r#"{
            "components": [
                {"name":"Predictions","status":"OPERATIONAL","group":null},
                {"name":"Trading API (CLOB)","status":"OPERATIONAL","group":{"name":"Predictions"}},
                {"name":"Perpetuals","status":"MAINTENANCE","group":null},
                {"name":"Perpetuals API","status":"MAINTENANCE","group":{"name":"Perpetuals"}}
            ]
        }"#;

        let status = parse_predictions_status(body).unwrap();
        assert_eq!(status.matched_components, 2);
        assert!(status.is_operational());
    }

    #[test]
    fn non_operational_predictions_parent_blocks_trading() {
        let body = r#"{
            "components": [
                {"name":"Predictions","status":"DEGRADED","group":null},
                {"name":"Clob Websocket","status":"OPERATIONAL","group":{"name":"Predictions"}}
            ]
        }"#;

        let status = parse_predictions_status(body).unwrap();
        assert!(!status.is_operational());
        assert_eq!(status.non_operational, vec!["Predictions=DEGRADED"]);
    }

    #[test]
    fn non_operational_predictions_child_blocks_trading() {
        let body = r#"{
            "components": [
                {"name":"Predictions","status":"OPERATIONAL","group":null},
                {"name":"Clob Websocket","status":"PARTIALOUTAGE","group":{"name":"Predictions"}}
            ]
        }"#;

        let status = parse_predictions_status(body).unwrap();
        assert!(!status.is_operational());
        assert_eq!(status.non_operational, vec!["Clob Websocket=PARTIALOUTAGE"]);
    }

    #[test]
    fn missing_matching_status_is_unknown_not_an_outage() {
        let body = r#"{
            "components": [
                {"name":"Predictions","group":null}
            ]
        }"#;

        assert!(parse_predictions_status(body).is_err());
    }

    #[test]
    fn no_predictions_components_is_an_error() {
        let body = r#"{
            "components": [
                {"name":"Perpetuals","status":"OPERATIONAL","group":null}
            ]
        }"#;

        assert!(parse_predictions_status(body).is_err());
    }

    #[test]
    fn predictions_children_without_parent_are_an_error() {
        let body = r#"{
            "components": [
                {"name":"Clob Websocket","status":"OPERATIONAL","group":{"name":"Predictions"}}
            ]
        }"#;

        assert!(parse_predictions_status(body).is_err());
    }

    #[test]
    fn malformed_components_body_is_an_error() {
        assert!(parse_predictions_status("not json").is_err());
        assert!(parse_predictions_status(r#"{"page":{"status":"UP"}}"#).is_err());
    }

    #[test]
    fn compatibility_api_reports_aggregated_state() {
        assert!(is_up("OPERATIONAL"));
        assert!(is_up("operational"));
        assert!(!is_up("UP"));
        assert!(!is_up("Predictions=DEGRADED"));
    }
}
