use std::{net::SocketAddr, path::PathBuf};

use serde::Deserialize;
use url::Url;

#[derive(Deserialize, Debug, Clone)]
pub struct FirehoseConfig {
    /// A list of upstream relays that this PDS will try to reach out to.
    pub relays: Vec<Url>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct RepoConfig {
    /// The path to the repository storage.
    pub path: PathBuf,
}

#[derive(Deserialize, Debug, Clone)]
pub struct PlcConfig {
    /// The path to the local PLC cache.
    pub path: PathBuf,
}

#[derive(Deserialize, Debug, Clone)]
pub struct AppConfig {
    /// The primary signing keys for all PLC/DID operations.
    pub key: PathBuf,
    /// The hostname of the PDS. Typically a domain name.
    pub host_name: String,
    /// The listen address for the PDS.
    pub listen_address: Option<SocketAddr>,
    /// The firehose configuration block.
    pub firehose: FirehoseConfig,
    /// The PLC configuration block.
    pub plc: PlcConfig,
    /// The repo configuration block.
    pub repo: RepoConfig,
    /// The sqlite database connection options.
    pub db: String,
    /// Test mode.
    pub test: bool,
}
