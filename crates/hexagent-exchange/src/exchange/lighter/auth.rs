//! Lighter credentials + network selection + host URLs.
//!
//! Credentials are a 40-byte **API-key private key** (an ECgFp5 scalar
//! registered on-chain via ChangePubKey — not an Ethereum key) plus the
//! `account_index` / `api_key_index` pair it was registered under. All three
//! come from `[lighter.<account_id>]` in secrets.toml; non-secret settings
//! (network, host overrides, symbols) stay in the `[[exchanges]] lighter`
//! config block.

use anyhow::{anyhow, Result};

use super::signer::LighterSigner;

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
    /// REST base (no trailing slash). `/api/v1/...` is appended.
    pub fn rest_base(&self) -> &'static str {
        match self {
            Network::Mainnet => "https://mainnet.zklighter.elliot.ai",
            Network::Testnet => "https://testnet.zklighter.elliot.ai",
        }
    }
    pub fn ws_url(&self) -> &'static str {
        match self {
            Network::Mainnet => "wss://mainnet.zklighter.elliot.ai/stream",
            Network::Testnet => "wss://testnet.zklighter.elliot.ai/stream",
        }
    }
    /// zkLighter L2 chain id used in tx hashing (NOT an EVM chain id).
    pub fn chain_id(&self) -> u32 {
        match self {
            Network::Mainnet => 304,
            Network::Testnet => 300,
        }
    }
}

/// Everything the trade/info/user-feed paths need to talk to Lighter.
pub struct LighterAuth {
    pub signer: LighterSigner,
    pub network: Network,
    /// REST base override (empty → network default).
    pub rest_base: String,
    /// WS URL override (empty → network default).
    pub ws_url: String,
}

impl LighterAuth {
    pub fn new(
        private_key: &str,
        account_index: i64,
        api_key_index: u8,
        network: &str,
        rest_base: &str,
        ws_url: &str,
    ) -> Result<LighterAuth> {
        if private_key.trim().is_empty() {
            return Err(anyhow!("lighter: empty private_key (need 40-byte API key)"));
        }
        if account_index < 0 {
            return Err(anyhow!("lighter: account_index must be set (>= 0)"));
        }
        let network = Network::from_str(network);
        let signer = LighterSigner::new(private_key, account_index, api_key_index, network.chain_id())?;
        Ok(LighterAuth {
            signer,
            network,
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
    pub fn account_index(&self) -> i64 {
        self.signer.account_index
    }
    pub fn api_key_index(&self) -> u8 {
        self.signer.api_key_index
    }
}
