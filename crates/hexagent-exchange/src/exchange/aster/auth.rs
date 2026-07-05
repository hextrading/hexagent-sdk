//! Aster credentials + network selection + host URLs.
//!
//! Signing key = the **API agent wallet** key created via the Aster
//! "Pro API" page (www.asterdex.com/en/api-wallet). `user_address` is the
//! main (login) wallet the account trades under — kept for logging and
//! sanity checks only; V3 requests carry just `signer`+`nonce`+`signature`
//! because the signer→user binding is registered server-side.

use anyhow::{anyhow, Result};
use k256::ecdsa::SigningKey;

use super::super::hyperliquid::signer::{derive_eth_address, parse_signing_key};

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
    /// EIP-712 domain chainId (mainnet 1666, testnet 714 — from the V3 docs).
    pub fn chain_id(&self) -> u64 {
        match self {
            Network::Mainnet => 1666,
            Network::Testnet => 714,
        }
    }
    /// REST base (no trailing slash). `/fapi/v3/...` is appended.
    pub fn rest_base(&self) -> &'static str {
        match self {
            Network::Mainnet => "https://fapi.asterdex.com",
            Network::Testnet => "https://fapi.asterdex-testnet.com",
        }
    }
    /// WS stream base (no trailing slash). `/ws/...` or `/stream?...` appended.
    pub fn ws_base(&self) -> &'static str {
        match self {
            Network::Mainnet => "wss://fstream.asterdex.com",
            Network::Testnet => "wss://fstream.asterdex-testnet.com",
        }
    }
}

/// Everything the trade/market/info paths need to talk to Aster.
#[derive(Clone)]
pub struct AsterAuth {
    /// API agent wallet signing key.
    pub key: SigningKey,
    /// Main (login) wallet address (lowercased 0x-hex) — logging/reference.
    pub user_address: String,
    /// The signer's own derived address (lowercased) — sent as `signer`.
    pub signer_address: String,
    pub network: Network,
    /// REST base override (empty → network default).
    pub rest_base: String,
    /// WS base override (empty → network default).
    pub ws_base: String,
}

impl AsterAuth {
    /// Build from config fields. `private_key` is the API agent wallet key;
    /// `user_address` the main account (may be empty — informational only).
    pub fn new(
        private_key: &str,
        user_address: &str,
        network: &str,
        rest_base: &str,
        ws_base: &str,
    ) -> Result<AsterAuth> {
        if private_key.trim().is_empty() {
            return Err(anyhow!("aster: empty private_key (need the API agent wallet key)"));
        }
        let key = parse_signing_key(private_key)?;
        let signer_address = derive_eth_address(&key);
        let user = {
            let u = user_address.trim().to_ascii_lowercase();
            if u.is_empty() || u.starts_with("0x") {
                u
            } else {
                format!("0x{}", u)
            }
        };
        Ok(AsterAuth {
            key,
            user_address: user,
            signer_address,
            network: Network::from_str(network),
            rest_base: rest_base.trim().to_string(),
            ws_base: ws_base.trim().to_string(),
        })
    }

    pub fn rest_base(&self) -> String {
        if self.rest_base.is_empty() {
            self.network.rest_base().to_string()
        } else {
            self.rest_base.trim_end_matches('/').to_string()
        }
    }
    pub fn ws_base(&self) -> String {
        if self.ws_base.is_empty() {
            self.network.ws_base().to_string()
        } else {
            self.ws_base.trim_end_matches('/').to_string()
        }
    }
    /// `https://…/fapi/v3/<endpoint>`.
    pub fn fapi_url(&self, endpoint: &str) -> String {
        format!("{}/fapi/v3/{}", self.rest_base(), endpoint)
    }
}
