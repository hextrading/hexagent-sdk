//! Bounded, cold-worker-only recovery diagnostics. No execution authority.
//!
//! Each round lives on the existing instance recovery worker's stack. Logging
//! uses the existing asynchronous appender; no new queue, thread or shared
//! state. Never pass authentication headers, signed requests or whole replies.

use super::{exact_cancel_acknowledged, FetchOrderResult, OrderOwnership};
use crate::http1_pool::Role;
use serde_json::Value;
use std::time::Instant;

const MAX_DETAIL_BYTES: usize = 768;

fn bounded(value: &str) -> &str {
    let mut end = value.len().min(MAX_DETAIL_BYTES);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

pub(super) struct RecoveryRound<'a> {
    ownership: &'a OrderOwnership,
    order_id: &'a str,
    round_ns: u64,
    started: Instant,
}

impl<'a> RecoveryRound<'a> {
    pub(super) fn new(ownership: &'a OrderOwnership, order_id: &'a str) -> Self {
        Self {
            ownership,
            order_id,
            round_ns: super::now_ns(),
            started: Instant::now(),
        }
    }

    pub(super) fn event(
        &self,
        stage: &'static str,
        result: &'static str,
        location: Option<(Role, usize)>,
        detail: &str,
    ) {
        log::info!(
            "[order_recovery_round] account={} instance={} coid={} orderID={} round_ns={} stage={} result={} location={:?} elapsed_us={} detail={:?} detail_truncated={}",
            self.ownership.account_id,
            self.ownership.instance_id,
            self.ownership.client_order_id,
            self.order_id,
            self.round_ns,
            stage,
            result,
            location,
            self.started.elapsed().as_micros(),
            bounded(detail),
            detail.len() > MAX_DETAIL_BYTES,
        );
    }

    pub(super) fn lookup(
        &self,
        stage: &'static str,
        result: &FetchOrderResult,
        location: Option<(Role, usize)>,
    ) {
        match result {
            FetchOrderResult::Found(order) => self.event(stage, "found", location, &order.status),
            FetchOrderResult::NotFound(evidence) => {
                self.event(stage, "not_found", location, evidence)
            }
            FetchOrderResult::Unavailable(kind) => {
                self.event(stage, "unavailable", location, &format!("{kind:?}"))
            }
        }
    }
}

/// Explain the exact existing admission rule; this does not relax it. Capture
/// only the target OID's reason and known error text, never unrelated orders.
pub(super) fn cancel_detail(response: &Value, order_id: &str) -> String {
    let canceled = response.get("canceled").and_then(Value::as_array);
    let failed = response.get("not_canceled").and_then(Value::as_object);
    let target_canceled = canceled.is_some_and(|ids| {
        ids.iter().any(|id| {
            id.as_str()
                .is_some_and(|id| id.eq_ignore_ascii_case(order_id))
        })
    });
    let reasons: Vec<_> = failed
        .into_iter()
        .flat_map(|rows| rows.iter())
        .filter(|(id, _)| id.eq_ignore_ascii_case(order_id))
        .collect();
    let reason = reasons.first().and_then(|(_, value)| value.as_str());
    let error = response
        .get("errorMsg")
        .or_else(|| response.get("error"))
        .and_then(Value::as_str)
        .unwrap_or("");
    format!(
        "exact_ack={} canceled_schema={} not_canceled_schema={} canceled_count={} not_canceled_count={} target_canceled={} target_reason_count={} target_reason_is_string={} target_reason={:?} error={:?}",
        exact_cancel_acknowledged(response, order_id), canceled.is_some(), failed.is_some(),
        canceled.map_or(0, Vec::len), failed.map_or(0, serde_json::Map::len),
        target_canceled, reasons.len(), reason.is_some(), bounded(reason.unwrap_or("")), bounded(error),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_target_reason_without_other_orders_or_changing_terminal_rule() {
        for reason in [
            "the order is already canceled",
            "order can't be found - already canceled or matched",
            "pending/delayed",
        ] {
            let value = serde_json::json!({"canceled":[],"not_canceled":{"0xOID":reason,"other":"private sibling detail"}});
            let text = cancel_detail(&value, "0xoid");
            assert!(text.contains(reason));
            assert!(!text.contains("private sibling detail"));
            assert!(text.contains("exact_ack=false"));
            assert!(!exact_cancel_acknowledged(&value, "0xoid"));
        }
        let value = serde_json::json!({"canceled":["0xoid"],"not_canceled":{}});
        assert!(cancel_detail(&value, "0xoid").contains("exact_ack=true"));
    }

    #[test]
    fn bounds_unicode_and_escapes_server_line_breaks() {
        let text = "界".repeat(300);
        assert_eq!(bounded(&text).len(), 768);
        let value = serde_json::json!({"canceled":[],"not_canceled":{"oid":"pending\nFORGED\r\t"}});
        let detail = cancel_detail(&value, "oid");
        assert!(!detail.contains('\n'));
        assert!(!detail.contains('\r'));
        assert!(!detail.contains('\t'));
        assert!(detail.contains("\\nFORGED"));
        assert!(cancel_detail(&Value::Null, "oid").contains("canceled_schema=false"));
    }
}
