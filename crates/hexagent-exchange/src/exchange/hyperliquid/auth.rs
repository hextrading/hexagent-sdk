//! Hyperliquid credentials + network selection + host URLs.
//!
//! Signing key = the **API agent wallet** key (recommended) or the master
//! EOA key. `account_address` is always the master/owner account that
//! positions/fills are queried and traded under — for an agent wallet it
//! differs from the signer's own address; for EOA signing they coincide.

use anyhow::{anyhow, Result};
use k256::ecdsa::SigningKey;

use super::signer::{derive_eth_address, parse_signing_key};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Network {
    Mainnet,
    Testnet,
}

impl Network {
    pub fn from_str(s: &str) -> Network {
        match s.trim().to_ascii_lowercase().as_str() {
            "mainnet" | "main" | "prod" => Network::Mainnet,
            _ => Network::Testnet, // safe default
        }
    }
    pub fn is_mainnet(&self) -> bool {
        matches!(self, Network::Mainnet)
    }
    /// REST base (no trailing slash). `/info` and `/exchange` are appended.
    pub fn rest_base(&self) -> &'static str {
        match self {
            Network::Mainnet => "https://api.hyperliquid.xyz",
            Network::Testnet => "https://api.hyperliquid-testnet.xyz",
        }
    }
    pub fn ws_url(&self) -> &'static str {
        match self {
            Network::Mainnet => "wss://api.hyperliquid.xyz/ws",
            Network::Testnet => "wss://api.hyperliquid-testnet.xyz/ws",
        }
    }
}

/// Everything the trade/market/info paths need to talk to Hyperliquid.
#[derive(Clone)]
pub struct HlAuth {
    /// Agent (or EOA) signing key.
    pub key: SigningKey,
    /// Master/owner account address (lowercased 0x-hex), used for info,
    /// positions, fills, and as the order owner.
    pub account_address: String,
    /// The signer's own derived address (lowercased) — for logging / EOA check.
    pub signer_address: String,
    pub network: Network,
    /// REST base override (empty → network default).
    pub rest_base: String,
    /// WS URL override (empty → network default).
    pub ws_url: String,
}

impl HlAuth {
    /// Build from config fields. `private_key` is the agent/EOA key;
    /// `account_address` the master account (falls back to the signer's own
    /// address when empty, i.e. plain EOA signing).
    pub fn new(
        private_key: &str,
        account_address: &str,
        network: &str,
        rest_base: &str,
        ws_url: &str,
    ) -> Result<HlAuth> {
        if private_key.trim().is_empty() {
            return Err(anyhow!(
                "hyperliquid: empty private_key (need agent or EOA key)"
            ));
        }
        let key = parse_signing_key(private_key)?;
        let signer_address = derive_eth_address(&key);
        let account = if account_address.trim().is_empty() {
            signer_address.clone()
        } else {
            let a = account_address.trim().to_ascii_lowercase();
            if !a.starts_with("0x") {
                format!("0x{}", a)
            } else {
                a
            }
        };
        Ok(HlAuth {
            key,
            account_address: account,
            signer_address,
            network: Network::from_str(network),
            rest_base: rest_base.trim().to_string(),
            ws_url: ws_url.trim().to_string(),
        })
    }

    pub fn rest_base(&self) -> String {
        if self.rest_base.is_empty() {
            self.network.rest_base().to_string()
        } else {
            self.rest_base.trim_end_matches('/').to_string()
        }
    }
    pub fn ws_url(&self) -> String {
        if self.ws_url.is_empty() {
            self.network.ws_url().to_string()
        } else {
            self.ws_url.clone()
        }
    }
    pub fn info_url(&self) -> String {
        format!("{}/info", self.rest_base())
    }
    pub fn exchange_url(&self) -> String {
        format!("{}/exchange", self.rest_base())
    }
}
