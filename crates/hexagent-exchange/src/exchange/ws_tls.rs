//! Feed-owned TLS configuration, prepared before the WebSocket runtime receives work.
//! tokio-tungstenite's default connector loads native certificates synchronously
//! on every connect. Rotation and standby reconnects share the active reader's
//! runtime, so even asynchronous handshakes must not take that default path.
use anyhow::{bail, Result};
use rustls::{ClientConfig, RootCertStore};
use std::sync::Arc;
use tokio_tungstenite::Connector;

#[derive(Clone)]
pub(crate) struct WsTlsConfig(Arc<ClientConfig>);

impl WsTlsConfig {
    pub(crate) fn load_native() -> Result<Self> {
        let loaded = rustls_native_certs::load_native_certs();
        if !loaded.errors.is_empty() {
            log::warn!("WebSocket native root CA loading errors: {:?}", loaded.errors);
        }
        let mut roots = RootCertStore::empty();
        roots.add_parsable_certificates(loaded.certs);
        Self::from_roots(roots)
    }

    fn from_roots(roots: RootCertStore) -> Result<Self> {
        if roots.is_empty() {
            bail!("WebSocket TLS initialization failed: no usable native root CA certificates");
        }
        let mut config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        // Previously each connection had a fresh session cache. Preserve full
        // handshakes and avoid introducing a shared mutable resumption cache.
        config.resumption = rustls::client::Resumption::disabled();
        Ok(Self(Arc::new(config)))
    }

    pub(crate) fn connector(&self) -> Connector {
        Connector::Rustls(self.0.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_or_unparseable_native_roots_fail_closed() {
        assert!(WsTlsConfig::from_roots(RootCertStore::empty()).is_err());
        let mut roots = RootCertStore::empty();
        roots.add_parsable_certificates([vec![0_u8; 32].into()]);
        assert!(WsTlsConfig::from_roots(roots).is_err());
    }

    #[test]
    #[ignore = "manual native trust-store preparation benchmark; performs cold-path filesystem I/O"]
    fn native_roots_and_reconnect_connector_benchmark() {
        use std::{hint::black_box, time::Instant};
        let prepared = WsTlsConfig::load_native().unwrap();
        let mut baseline = Vec::with_capacity(256);
        let mut reused = Vec::with_capacity(256);
        for _ in 0..256 {
            let start = Instant::now();
            black_box(WsTlsConfig::load_native().unwrap());
            baseline.push(start.elapsed().as_nanos());
            let start = Instant::now();
            let Connector::Rustls(config) = black_box(prepared.connector()) else {
                panic!("WebSocket must retain TLS certificate verification");
            };
            reused.push(start.elapsed().as_nanos());
            assert!(Arc::ptr_eq(&config, &prepared.0));
        }
        for (name, mut samples) in [("native_roots_per_connect", baseline), ("prepared_connector", reused)] {
            samples.sort_unstable();
            println!("{name}: n={} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth=0 overflow=0 boundary=config_preparation_only_excludes_network_handshake",
                samples.len(), samples[127], samples[253], samples[255], samples[255]);
        }
    }
}
