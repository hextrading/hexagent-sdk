//! Per-instance simulated wallet: USDC balance + per-token share inventory.
//!
//! Mirrors v1's `SimWallet` semantics (`src/exchange/sim/mod.rs`). The wallet
//! stores balances only; "available" (balance − locked-by-resting-orders) is
//! computed by the exchange core, which owns the resting-order set. USDC is
//! `Option`: `None` disables lockup/balance gating (a wallet with no seeded
//! balance never blocks fills — used by tests / un-configured instances).

use std::collections::HashMap;

#[derive(Default, Clone)]
pub struct Wallet {
    pub usdc: Option<f64>,
    pub shares: HashMap<String, f64>,
}

#[derive(Default)]
pub struct WalletBook {
    wallets: HashMap<String, Wallet>,
}

impl WalletBook {
    pub fn new() -> Self {
        Self::default()
    }

    fn wallet_mut(&mut self, iid: &str) -> &mut Wallet {
        self.wallets.entry(iid.to_string()).or_default()
    }

    /// Seed the instance's USDC balance (enables lockup/balance gating).
    pub fn seed_usdc(&mut self, iid: &str, balance: f64) {
        self.wallet_mut(iid).usdc = Some(balance);
    }

    /// `true` if this instance has a seeded USDC balance (gating active).
    pub fn lockup_enabled(&self, iid: &str) -> bool {
        self.wallets.get(iid).map(|w| w.usdc.is_some()).unwrap_or(false)
    }

    pub fn usdc(&self, iid: &str) -> Option<f64> {
        self.wallets.get(iid).and_then(|w| w.usdc)
    }

    pub fn shares(&self, iid: &str, token: &str) -> f64 {
        self.wallets
            .get(iid)
            .and_then(|w| w.shares.get(token).copied())
            .unwrap_or(0.0)
    }

    /// Drop a token's share entry from every instance wallet. Called when an
    /// event is retired (its tokens are long settled and never traded again)
    /// to bound memory over long backtests / paper sessions. The per-`iid`
    /// `wallets` map itself is a small fixed set and is left intact.
    pub fn retire_token(&mut self, token: &str) {
        for w in self.wallets.values_mut() {
            w.shares.remove(token);
        }
    }

    /// Adjust USDC by `delta` (signed) — only if the instance has a seeded
    /// (lockup-enabled) wallet; a no-op otherwise. Used for the virtual-split
    /// cash cost at seed (−) and the settlement payout at retire (+).
    pub fn adjust_usdc(&mut self, iid: &str, delta: f64) {
        if let Some(b) = self.wallet_mut(iid).usdc.as_mut() {
            *b += delta;
        }
    }

    pub fn credit_shares(&mut self, iid: &str, token: &str, qty: f64) {
        if qty <= 0.0 {
            return;
        }
        *self.wallet_mut(iid).shares.entry(token.to_string()).or_insert(0.0) += qty;
    }

    pub fn debit_shares(&mut self, iid: &str, token: &str, qty: f64) {
        if qty <= 0.0 {
            return;
        }
        let e = self.wallet_mut(iid).shares.entry(token.to_string()).or_insert(0.0);
        *e = (*e - qty).max(0.0);
    }

    /// Settle a BUY fill: spend `cost` USDC and credit `qty` shares.
    pub fn settle_buy(&mut self, iid: &str, token: &str, qty: f64, cost: f64) {
        let w = self.wallet_mut(iid);
        if let Some(b) = w.usdc.as_mut() {
            *b -= cost;
        }
        *w.shares.entry(token.to_string()).or_insert(0.0) += qty;
    }

    /// Settle a SELL fill: debit `qty` shares and add `proceeds` USDC.
    pub fn settle_sell(&mut self, iid: &str, token: &str, qty: f64, proceeds: f64) {
        let w = self.wallet_mut(iid);
        let e = w.shares.entry(token.to_string()).or_insert(0.0);
        *e = (*e - qty).max(0.0);
        if let Some(b) = w.usdc.as_mut() {
            *b += proceeds;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_and_settle_buy() {
        let mut wb = WalletBook::new();
        wb.seed_usdc("a", 1000.0);
        assert!(wb.lockup_enabled("a"));
        wb.settle_buy("a", "up", 10.0, 6.0); // buy 10 up for 6 USDC
        assert_eq!(wb.usdc("a"), Some(994.0));
        assert_eq!(wb.shares("a", "up"), 10.0);
    }

    #[test]
    fn settle_sell_debits_shares_adds_usdc() {
        let mut wb = WalletBook::new();
        wb.seed_usdc("a", 1000.0);
        wb.credit_shares("a", "up", 30.0);
        wb.settle_sell("a", "up", 10.0, 4.0);
        assert_eq!(wb.shares("a", "up"), 20.0);
        assert_eq!(wb.usdc("a"), Some(1004.0));
    }

    #[test]
    fn no_seed_means_no_gating() {
        let wb = WalletBook::new();
        assert!(!wb.lockup_enabled("a"));
        assert_eq!(wb.usdc("a"), None);
    }
}
