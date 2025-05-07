//! Configuration structures for the PDS.
/// The metrics configuration.
pub(crate) mod metrics {
    use super::{Deserialize, Url};

    #[derive(Deserialize, Debug, Clone)]
    /// The Prometheus configuration.
    pub(crate) struct PrometheusConfig {
        /// The URL of the Prometheus server's exporter endpoint.
        pub url: Url,
    }
}

use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf};
use url::Url;

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "type")]
/// Configuration for metrics.
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
    /// Use SQLite for repository storage instead of CAR files.
    #[serde(default)]
    pub use_sqlite: bool,
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct PlcConfig {
    /// The path to the local PLC cache.
    pub path: PathBuf,
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct BlobConfig {
    /// The maximum size limit of blobs.
    pub limit: u64,
    /// The path to store blobs into.
    pub path: PathBuf,
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct AppConfig {
    /// The blob configuration block.
    pub blob: BlobConfig,
    /// The sqlite database connection options.
    pub db: String,
    /// The firehose configuration block.
    pub firehose: FirehoseConfig,
    /// The hostname of the PDS. Typically a domain name.
    pub host_name: String,
    /// The primary signing keys for all PLC/DID operations.
    pub key: PathBuf,
    /// The listen address for the PDS.
    pub listen_address: Option<SocketAddr>,
    /// The metrics configuration block.
    pub metrics: Option<MetricConfig>,
    /// The PLC configuration block.
    pub plc: PlcConfig,
    /// The repo configuration block.
    pub repo: RepoConfig,
    /// Test mode.
    pub test: bool,
}
