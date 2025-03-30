//! Configuration structures for the PDS.
use std::{net::SocketAddr, path::PathBuf};

use serde::Deserialize;
use url::Url;

/// The metrics configuration.
pub(crate) mod metrics {
    use super::*;

    #[derive(Deserialize, Debug, Clone)]
    /// The Prometheus configuration.
    pub(crate) struct PrometheusConfig {
        /// The URL of the Prometheus server's exporter endpoint.
        pub url: Url,
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub(crate) enum MetricConfig {
    /// The Prometheus push gateway.
    PrometheusPush(metrics::PrometheusConfig),
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct FirehoseConfig {
    /// A list of upstream relays that this PDS will try to reach out to.
    pub relays: Vec<Url>,
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct RepoConfig {
    /// The path to the repository storage.
    pub path: PathBuf,
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct PlcConfig {
    /// The path to the local PLC cache.
    pub path: PathBuf,
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct BlobConfig {
    /// The path to store blobs into.
    pub path: PathBuf,
    /// The maximum size limit of blobs.
    pub limit: u64,
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct AppConfig {
    /// The primary signing keys for all PLC/DID operations.
    pub key: PathBuf,
    /// The hostname of the PDS. Typically a domain name.
    pub host_name: String,
    /// The listen address for the PDS.
    pub listen_address: Option<SocketAddr>,
    /// The metrics configuration block.
    pub metrics: Option<MetricConfig>,
    /// The firehose configuration block.
    pub firehose: FirehoseConfig,
    /// The PLC configuration block.
    pub plc: PlcConfig,
    /// The repo configuration block.
    pub repo: RepoConfig,
    /// The blob configuration block.
    pub blob: BlobConfig,
    /// The sqlite database connection options.
    pub db: String,
    /// Test mode.
    pub test: bool,
}
