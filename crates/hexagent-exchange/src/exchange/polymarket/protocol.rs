//! Protocol V2 identifiers and ABI. All decimal decoding/ABI construction is
//! startup/metadata/maintenance work; signing consumes the prepared token word.
use anyhow::{anyhow, bail, Result};
use sha3::{Digest, Keccak256};
fn keccak256(data: &[u8]) -> [u8; 32] {
    Keccak256::digest(data).into()
}
fn u256_bytes(value: u128) -> [u8; 32] {
    let mut word = [0; 32];
    word[16..].copy_from_slice(&value.to_be_bytes());
    word
}

pub const EXCHANGE_V3: &str = "0xe3333700cA9d93003F00f0F71f8515005F6c00Aa";
pub const POSITION_MANAGER: &str = "0x006F54F7f9A22e0000CC2AB60031000000ae9fEF";
pub const ROUTER: &str = "0x12121212006e4CD160D18e3f00711DA5c3372600";

/// Official @polymarket/client 0.16.0 reserves bits 40..103. In addition,
/// require a supported module and binary outcome before admitting this route.
#[inline]
pub fn is_v2_word(word: &[u8; 32]) -> bool {
    crate::types::PreparedToken::is_polymarket_protocol_v2_word(word)
}

pub fn token_word(token: &str) -> Result<[u8; 32]> {
    crate::types::PreparedToken::parse(token)
        .map(|t| t.word())
        .ok_or_else(|| anyhow!("invalid decimal position/token ID"))
}

pub fn is_v2_token(token: &str) -> bool {
    token_word(token).is_ok_and(|word| is_v2_word(&word))
}

pub fn condition_word(condition: &str) -> Result<[u8; 32]> {
    let bytes = hex::decode(
        condition
            .strip_prefix("0x")
            .ok_or_else(|| anyhow!("condition ID requires 0x"))?,
    )?;
    let mut word = [0; 32];
    match bytes.len() {
        31 => word[..31].copy_from_slice(&bytes),
        32 => word.copy_from_slice(&bytes),
        _ => bail!("condition ID must be bytes31 or bytes32"),
    }
    Ok(word)
}

pub fn is_v2_condition(condition: &str) -> bool {
    condition_word(condition).is_ok_and(|w| is_v2_word(&w) && w[31] == 0)
}

/// Cold metadata/diagnostic comparison accepts both documented V2 widths.
pub fn conditions_match(left: &str, right: &str) -> bool {
    if left.eq_ignore_ascii_case(right) { return true; }
    match (condition_word(left), condition_word(right)) {
        (Ok(a),Ok(b)) => a == b && is_v2_word(&a) && a[31] == 0,
        _ => false,
    }
}

pub fn outcome_ids(condition: &str) -> Result<(String, String)> {
    let mut word = condition_word(condition)?;
    if !is_v2_word(&word) || word[31] != 0 {
        bail!("invalid Protocol V2 condition/padding");
    }
    let yes = num_bigint::BigUint::from_bytes_be(&word).to_string();
    word[31] = 1;
    Ok((yes, num_bigint::BigUint::from_bytes_be(&word).to_string()))
}

pub fn router_calldata(
    operation: &str,
    condition: &str,
    outcome: u8,
    amount: u128,
) -> Result<String> {
    let word = condition_word(condition)?;
    if !is_v2_word(&word) || word[31] != 0 {
        bail!("invalid Protocol V2 condition/padding");
    }
    if amount == 0 {
        bail!("Protocol V2 operation requires positive amount");
    }
    let signature = match operation {
        "split" => "split(bytes31,uint256)",
        "merge" => "merge(bytes31,uint256)",
        "redeem" if outcome <= 1 => "redeem(bytes31,uint256,uint256)",
        _ => bail!("unsupported Router operation/outcome"),
    };
    let mut data = Vec::with_capacity(100);
    data.extend_from_slice(&keccak256(signature.as_bytes())[..4]);
    data.extend_from_slice(&word); // bytes31 is LEFT-aligned, zero-padded on right
    if operation == "redeem" {
        data.extend_from_slice(&u256_bytes(outcome as u128));
    }
    data.extend_from_slice(&u256_bytes(amount));
    Ok(format!("0x{}", hex::encode(data)))
}

/// Background-only raw balance read: preserve integer units for redemption.
pub fn balance_units(owner: &str, token: &str) -> Result<u128> {
    let word = token_word(token)?;
    if !is_v2_word(&word) {
        bail!("expected Protocol V2 position");
    }
    let owner = hex::decode(owner.trim_start_matches("0x"))?;
    if owner.len() != 20 {
        bail!("invalid position owner");
    }
    let mut data = Vec::with_capacity(68);
    data.extend_from_slice(&keccak256(b"balanceOf(address,uint256)")[..4]);
    data.extend_from_slice(&[0; 12]);
    data.extend_from_slice(&owner);
    data.extend_from_slice(&word);
    call_units(&data)
}

fn call_units(data: &[u8]) -> Result<u128> {
    let result =
        super::deploy_wallet::eth_call(POSITION_MANAGER, &format!("0x{}", hex::encode(data)))
            .ok_or_else(|| anyhow!("PositionManager RPC failed/unresolved"))?;
    let bytes = hex::decode(result.trim_start_matches("0x"))?;
    if bytes.len() != 32 || bytes[..16].iter().any(|b| *b != 0) {
        bail!("invalid PositionManager uint128 result");
    }
    Ok(u128::from_be_bytes(bytes[16..].try_into().unwrap()))
}

pub fn payout_units(token: &str, amount: u128) -> Result<u128> {
    let word = token_word(token)?;
    if !is_v2_word(&word) {
        bail!("expected Protocol V2 position");
    }
    let mut data = Vec::with_capacity(68);
    data.extend_from_slice(&keccak256(b"getPayout(uint256,uint256)")[..4]);
    data.extend_from_slice(&word);
    data.extend_from_slice(&u256_bytes(amount));
    call_units(&data)
}

/// Async settlement read on the existing I/O runtime. Never submit nested jobs
/// to the bounded blocking pool: the snapshot caller may already own a worker.
pub(crate) async fn resolution(condition: &str) -> Result<std::collections::HashMap<String, f64>> {
    let (yes, no) = outcome_ids(condition)?;
    let client = super::onchain_tx::rpc_http_client();
    let urls = super::onchain_tx::polygon_rpc_urls()?;
    let mut values = std::collections::HashMap::with_capacity(2);
    for token in [yes, no] {
        let mut data = Vec::with_capacity(68);
        data.extend_from_slice(&keccak256(b"getPayout(uint256,uint256)")[..4]);
        data.extend_from_slice(&token_word(&token)?);
        data.extend_from_slice(&u256_bytes(1_000_000));
        let body = serde_json::json!({"jsonrpc":"2.0", "id":1, "method":"eth_call",
            "params":[{"to":POSITION_MANAGER,"data":format!("0x{}",hex::encode(data))},"latest"]});
        let mut payout = None;
        for url in &urls {
            let Ok(response) = client.post(url).json(&body).send().await else {
                continue;
            };
            let Ok(response) = response.error_for_status() else {
                continue;
            };
            let Ok(value) = response.json::<serde_json::Value>().await else {
                continue;
            };
            if value.get("error").is_some() {
                continue;
            }
            let Some(result) = value["result"].as_str() else {
                continue;
            };
            let Ok(bytes) = hex::decode(result.trim_start_matches("0x")) else {
                continue;
            };
            if bytes.len() != 32 || bytes[..16].iter().any(|b| *b != 0) {
                continue;
            }
            let units = u128::from_be_bytes(bytes[16..].try_into().unwrap());
            if units > 1_000_000 {
                bail!("invalid Protocol V2 payout");
            }
            payout = Some(units);
            break;
        }
        let units =
            payout.ok_or_else(|| anyhow!("Protocol V2 payout unresolved or unavailable"))?;
        values.insert(token, units as f64 / 1_000_000.0);
    }
    if (values.values().sum::<f64>() - 1.0).abs() > 1e-9 {
        bail!("Protocol V2 outcome payouts do not sum to one");
    }
    Ok(values)
}

pub fn redemption_calldata(owner: &str, condition: &str) -> Result<Vec<String>> {
    let (yes, no) = outcome_ids(condition)?;
    let mut calls = Vec::with_capacity(2);
    for (index, token) in [yes, no].iter().enumerate() {
        let amount = balance_units(owner, token)?;
        if amount != 0 {
            calls.push(router_calldata("redeem", condition, index as u8, amount)?);
        }
    }
    Ok(calls)
}

/// Receipt-only recovery proof, additionally fenced by the wallet's journaled
/// BatchExecuted nonce and canonical block in deposit_wallet. Transfers must
/// deliver BOTH exact outcomes to the owner in the same successful transaction.
/// Router outputs can be transfers rather than direct mints. Count net receipts
/// so an onward transfer cannot prove the wallet still received the split.
pub fn receipt_proves_split(
    logs: &[serde_json::Value],
    hash: &str,
    owner: &str,
    condition: &str,
    amount: u128,
) -> Result<bool> {
    let (yes, no) = outcome_ids(condition)?;
    let ids = [token_word(&yes)?, token_word(&no)?];
    let single = format!(
        "0x{}",
        hex::encode(keccak256(
            b"TransferSingle(address,address,address,uint256,uint256)"
        ))
    );
    let batch = format!(
        "0x{}",
        hex::encode(keccak256(
            b"TransferBatch(address,address,address,uint256[],uint256[])"
        ))
    );
    let owner_topic = format!(
        "0x{:0>64}",
        owner.trim_start_matches("0x").to_ascii_lowercase()
    );
    let mut quantities = [0_u128; 2];
    let mut outgoing = [0_u128; 2];
    let mut seen = std::collections::HashSet::new();
    for log in logs {
        if log["removed"] == true
            || log["transactionHash"].as_str() != Some(hash)
            || !log["address"]
                .as_str()
                .is_some_and(|a| a.eq_ignore_ascii_case(POSITION_MANAGER))
        {
            continue;
        }
        let received = log["topics"][3]
            .as_str()
            .is_some_and(|a| a.eq_ignore_ascii_case(&owner_topic));
        let sent = log["topics"][2]
            .as_str()
            .is_some_and(|a| a.eq_ignore_ascii_case(&owner_topic));
        if !received && !sent {
            continue;
        }
        let topic = log["topics"][0].as_str().unwrap_or("");
        if topic != single && topic != batch {
            continue;
        }
        let index = log["logIndex"]
            .as_str()
            .ok_or_else(|| anyhow!("mint log missing index"))?;
        if !seen.insert(index) {
            bail!("duplicate mint log in split receipt");
        }
        let data = hex::decode(log["data"].as_str().unwrap_or("").trim_start_matches("0x"))?;
        let read = |offset: usize| -> Result<[u8; 32]> {
            data.get(offset..offset.saturating_add(32))
                .ok_or_else(|| anyhow!("short mint event"))?
                .try_into()
                .map_err(|_| anyhow!("invalid mint word"))
        };
        let small = |word: [u8; 32]| -> Result<u128> {
            if word[..16].iter().any(|b| *b != 0) {
                bail!("mint event value out of range");
            }
            Ok(u128::from_be_bytes(word[16..].try_into().unwrap()))
        };
        let pairs = if topic == single {
            vec![(read(0)?, small(read(32)?)?)]
        } else {
            let offset =
                |word| -> Result<usize> { usize::try_from(small(word)?).map_err(Into::into) };
            let io = offset(read(0)?)?;
            let qo = offset(read(32)?)?;
            let count = offset(read(io)?)?;
            if count != offset(read(qo)?)? || count > 64 {
                bail!("invalid mint batch length");
            }
            let mut pairs = Vec::with_capacity(count);
            for i in 0..count {
                pairs.push((
                    read(io.saturating_add(32 * (i + 1)))?,
                    small(read(qo.saturating_add(32 * (i + 1)))?)?,
                ));
            }
            pairs
        };
        for (id, quantity) in pairs {
            if let Some(index) = ids.iter().position(|value| *value == id) {
                if received {
                    quantities[index] = quantities[index]
                        .checked_add(quantity)
                        .ok_or_else(|| anyhow!("receipt quantity overflow"))?;
                }
                if sent {
                    outgoing[index] = outgoing[index]
                        .checked_add(quantity)
                        .ok_or_else(|| anyhow!("receipt quantity overflow"))?;
                }
            }
        }
    }
    Ok(amount > 0 && (0..2).all(|i| quantities[i].checked_sub(outgoing[i]) == Some(amount)))
}

#[cfg(test)]
mod tests {
    use super::*;
    const CID: &str = "0x01000000000000000000000000000000000000000000000000000000000001";
    #[test]
    fn identifiers_preserve_bytes31_and_outcome_identity() {
        let (yes, no) = outcome_ids(CID).unwrap();
        assert!(is_v2_token(&yes) && is_v2_token(&no));
        assert_eq!(token_word(&yes).unwrap()[31], 0);
        assert_eq!(token_word(&no).unwrap()[31], 1);
        assert_eq!(outcome_ids(&format!("{CID}00")).unwrap(), (yes, no));
        assert!(outcome_ids(&format!("{CID}01")).is_err());
        assert!(!is_v2_token("42"));
        assert!(conditions_match(CID, &format!("{CID}00")));
        assert!(!conditions_match(CID, &format!("{CID}01")));
        assert!(!conditions_match(CID, "0x1234"));
    }
    #[test]
    fn router_encoding_is_left_aligned_and_redeems_explicit_quantity() {
        let data = hex::decode(
            router_calldata("redeem", CID, 1, 1_000_000)
                .unwrap()
                .trim_start_matches("0x"),
        )
        .unwrap();
        assert_eq!(data.len(), 100);
        assert_eq!(&data[4..36], &condition_word(CID).unwrap());
        assert_eq!(data[67], 1);
        assert_eq!(&data[68..], &u256_bytes(1_000_000));
        assert!(router_calldata("redeem", CID, 2, 1).is_err());
        assert!(router_calldata("split", CID, 0, 0).is_err());
    }

    #[test]
    fn split_recovery_requires_both_net_legs_and_rejects_duplicate_replay() {
        use serde_json::json;
        let owner = "0x1111111111111111111111111111111111111111";
        let owner_topic = format!("0x{}", "0".repeat(24) + &"11".repeat(20));
        let router_topic = format!(
            "0x{:0>64}",
            ROUTER.trim_start_matches("0x").to_ascii_lowercase()
        );
        let (yes, no) = outcome_ids(CID).unwrap();
        let logs: Vec<_> = [yes, no].iter().enumerate().map(|(i, id)| {
            let mut data = token_word(id).unwrap().to_vec(); data.extend_from_slice(&u256_bytes(100));
            json!({"address":POSITION_MANAGER,"transactionHash":"tx","logIndex":format!("0x{i:x}"),
                "topics":[format!("0x{}",hex::encode(keccak256(b"TransferSingle(address,address,address,uint256,uint256)"))),
                    router_topic,router_topic,owner_topic],"data":format!("0x{}",hex::encode(data))})
        }).collect();
        assert!(receipt_proves_split(&logs, "tx", owner, CID, 100).unwrap());
        assert!(receipt_proves_split(
            &logs.iter().rev().cloned().collect::<Vec<_>>(),
            "tx",
            owner,
            CID,
            100
        )
        .unwrap());
        assert!(!receipt_proves_split(&logs[..1], "tx", owner, CID, 100).unwrap());
        assert!(!receipt_proves_split(&logs, "other", owner, CID, 100).unwrap());
        assert!(!receipt_proves_split(&logs, "tx", ROUTER, CID, 100).unwrap());
        assert!(!receipt_proves_split(&logs, "tx", owner, CID, 99).unwrap());
        let mut duplicate = logs.clone();
        duplicate.push(logs[0].clone());
        assert!(receipt_proves_split(&duplicate, "tx", owner, CID, 100).is_err());
        let mut removed = logs.clone();
        removed[0]["removed"] = json!(true);
        assert!(!receipt_proves_split(&removed, "tx", owner, CID, 100).unwrap());
        let mut outbound = logs[0].clone();
        outbound["logIndex"] = json!("0x2");
        outbound["topics"][2] = json!(owner_topic);
        outbound["topics"][3] = json!(router_topic);
        let mut sent = logs.clone();
        sent.push(outbound);
        assert!(!receipt_proves_split(&sent, "tx", owner, CID, 100).unwrap());
        let mut batch = logs[0].clone();
        batch["topics"][0] = json!(format!(
            "0x{}",
            hex::encode(keccak256(
                b"TransferBatch(address,address,address,uint256[],uint256[])"
            ))
        ));
        let (yes, no) = outcome_ids(CID).unwrap();
        let words = [
            u256_bytes(64),
            u256_bytes(160),
            u256_bytes(2),
            token_word(&yes).unwrap(),
            token_word(&no).unwrap(),
            u256_bytes(2),
            u256_bytes(100),
            u256_bytes(100),
        ];
        batch["data"] = json!(format!("0x{}", hex::encode(words.concat())));
        assert!(receipt_proves_split(&[batch], "tx", owner, CID, 100).unwrap());
    }
}
