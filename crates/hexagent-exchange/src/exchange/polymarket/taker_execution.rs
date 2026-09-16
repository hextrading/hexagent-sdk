//! Bounded, allocation-free reduction of an already parsed private trade.
//!
//! The top-level taker price can be the order limit. Actual principal comes
//! from every maker leg, including proven complementary-token mint/merge legs.
//! This runs on the private owner, never the market-data/quote/dispatch path.

use serde_json::Value;

pub const MAX_TAKER_EXECUTION_LEGS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TakerExecution {
    pub quantity: f64,
    pub gross_notional: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TakerExecutionError {
    MissingLegs,
    TooManyLegs,
    InvalidIdentity,
    InvalidNumber,
    InvalidPair,
    DuplicateLeg,
    QuantityMismatch,
}

impl std::fmt::Display for TakerExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::MissingLegs => "taker execution has no complete maker legs",
            Self::TooManyLegs => "taker execution exceeds bounded maker-leg capacity",
            Self::InvalidIdentity => "taker execution has invalid leg identity or side",
            Self::InvalidNumber => "taker execution has invalid quantity or price",
            Self::InvalidPair => "taker execution lacks complementary-token proof",
            Self::DuplicateLeg => "taker execution repeats a maker order",
            Self::QuantityMismatch => "taker execution maker quantities do not match total",
        })
    }
}

impl std::error::Error for TakerExecutionError {}

fn identity(value: Option<&Value>) -> Result<&str, TakerExecutionError> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(TakerExecutionError::InvalidIdentity)
}

fn positive_number(value: Option<&Value>) -> Result<f64, TakerExecutionError> {
    let value = match value {
        Some(Value::String(value)) => value.parse::<f64>().ok(),
        Some(value) => value.as_f64(),
        None => None,
    }
    .filter(|value| value.is_finite() && *value > 0.0)
    .ok_or(TakerExecutionError::InvalidNumber)?;
    Ok(value)
}

fn price(value: Option<&Value>) -> Result<f64, TakerExecutionError> {
    let value = positive_number(value)?;
    if value >= 1.0 {
        return Err(TakerExecutionError::InvalidNumber);
    }
    Ok(value)
}

fn buy_side(value: Option<&Value>) -> Result<bool, TakerExecutionError> {
    let side = identity(value)?;
    if side.eq_ignore_ascii_case("BUY") {
        Ok(true)
    } else if side.eq_ignore_ascii_case("SELL") {
        Ok(false)
    } else {
        Err(TakerExecutionError::InvalidIdentity)
    }
}

fn order_hash(value: &str) -> &str {
    value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value)
}

struct SeenOrders<'a> {
    hashes: [u64; MAX_TAKER_EXECUTION_LEGS * 2],
    ids: [Option<&'a str>; MAX_TAKER_EXECUTION_LEGS * 2],
}

impl<'a> SeenOrders<'a> {
    fn new() -> Self {
        Self {
            hashes: [0; MAX_TAKER_EXECUTION_LEGS * 2],
            ids: [None; MAX_TAKER_EXECUTION_LEGS * 2],
        }
    }

    fn insert(&mut self, id: &'a str) -> bool {
        let hash = id.bytes().fold(0xcbf29ce484222325_u64, |hash, byte| {
            (hash ^ u64::from(byte.to_ascii_lowercase())).wrapping_mul(0x100000001b3)
        });
        let mut slot = hash as usize & (self.ids.len() - 1);
        // At most 64 insertions into 128 slots: an empty slot always exists.
        // Hash collisions are resolved using the complete canonical identity.
        loop {
            match self.ids[slot] {
                None => {
                    self.ids[slot] = Some(id);
                    self.hashes[slot] = hash;
                    return true;
                }
                Some(prior) if self.hashes[slot] == hash && id.eq_ignore_ascii_case(prior) => {
                    return false;
                }
                Some(_) => slot = (slot + 1) & (self.ids.len() - 1),
            }
        }
    }
}

#[derive(Default)]
struct Sum {
    value: f64,
    correction: f64,
}

impl Sum {
    fn add(&mut self, value: f64) {
        let corrected = value - self.correction;
        let next = self.value + corrected;
        self.correction = (next - self.value) - corrected;
        self.value = next;
    }
}

/// Reduce all maker legs, or reject the entire record. `is_binary_pair` must
/// prove both different assets belong to this exact binary condition using an
/// existing immutable publication; a different asset alone is not proof.
/// The callback is never invoked for ordinary same-token opposite-side legs.
pub fn normalize_taker_execution(
    data: &Value,
    mut is_binary_pair: impl FnMut(&str, &str, &str) -> bool,
) -> Result<TakerExecution, TakerExecutionError> {
    let legs = data
        .get("maker_orders")
        .and_then(Value::as_array)
        .filter(|legs| !legs.is_empty())
        .ok_or(TakerExecutionError::MissingLegs)?;
    if legs.len() > MAX_TAKER_EXECUTION_LEGS {
        return Err(TakerExecutionError::TooManyLegs);
    }
    let condition = identity(data.get("market"))?;
    let asset = identity(data.get("asset_id").or_else(|| data.get("token_id")))?;
    let side = buy_side(data.get("side"))?;
    let quantity = positive_number(data.get("size").or_else(|| data.get("matched_amount")))?;
    // Validate the wire price but never use the limit as actual principal.
    price(data.get("price"))?;
    let mut ids = SeenOrders::new();
    let mut total_quantity = Sum::default();
    let mut gross_notional = Sum::default();
    for leg in legs {
        let id = order_hash(identity(leg.get("order_id"))?);
        if id.is_empty() {
            return Err(TakerExecutionError::InvalidIdentity);
        }
        if !ids.insert(id) {
            return Err(TakerExecutionError::DuplicateLeg);
        }
        let leg_asset = identity(leg.get("asset_id"))?;
        let leg_side = buy_side(leg.get("side"))?;
        let leg_quantity = positive_number(leg.get("matched_amount"))?;
        let leg_price = price(leg.get("price"))?;
        let execution_price = if leg_asset == asset {
            if side == leg_side {
                return Err(TakerExecutionError::InvalidPair);
            }
            leg_price
        } else {
            if side != leg_side || !is_binary_pair(condition, asset, leg_asset) {
                return Err(TakerExecutionError::InvalidPair);
            }
            1.0 - leg_price
        };
        if execution_price <= 0.0 {
            return Err(TakerExecutionError::InvalidNumber);
        }
        total_quantity.add(leg_quantity);
        gross_notional.add(leg_quantity * execution_price);
    }
    if !total_quantity.value.is_finite() || !gross_notional.value.is_finite() {
        return Err(TakerExecutionError::InvalidNumber);
    }
    if (total_quantity.value - quantity).abs() > 1e-8_f64.max(quantity.abs() * 1e-8) {
        return Err(TakerExecutionError::QuantityMismatch);
    }
    if gross_notional.value <= 0.0 || gross_notional.value > quantity + 1e-8 {
        return Err(TakerExecutionError::InvalidNumber);
    }
    Ok(TakerExecution {
        quantity,
        gross_notional: gross_notional.value,
    })
}

#[cfg(test)]
#[path = "taker_execution_tests.rs"]
mod tests;
