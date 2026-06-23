//! Local implementation of the Hexmarket client — replaces the external
//! `hexmarket_sdk_sync` crate. Uses the shared tokio runtime + reqwest
//! client so connection reuse and TLS session caching are pooled with
//! the rest of the binary.

pub mod auth;
pub mod client;
pub mod types;

pub use auth::ApiCredentials;
pub use client::{HexClient, HexClientConfig};
pub use types::*;
