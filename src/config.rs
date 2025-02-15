use std::{net::SocketAddr, path::PathBuf};

use serde::Deserialize;
use url::Url;

#[derive(Deserialize, Debug, Clone)]
pub struct RepoConfig {
    /// The path to the repository storage.
    pub path: PathBuf,
}

#[derive(Deserialize, Debug, Clone)]
pub struct PlcConfig {
    /// The path to the local PLC cache.
    pub path: PathBuf,
    /// Whether or not to write updates to the global PLC directory.
    pub update: bool,
}

#[derive(Deserialize, Debug, Clone)]
pub struct AppConfig {
    /// The local administration password.
    pub admin_pw: String,
    /// The primary signing keys for all PLC/DID operations.
    pub key: PathBuf,
    /// The hostname of the PDS. Typically a domain name.
    pub host_name: String,
    /// The listen address for the PDS.
    pub listen_address: Option<SocketAddr>,
    /// The PLC configuration block.
    pub plc: PlcConfig,
    /// The repo configuration block.
    pub repo: RepoConfig,
    /// The sqlite database connection options.
    pub db: String,
}
