use std::{net::SocketAddr, path::PathBuf};

use serde::Deserialize;
use url::Url;

#[derive(Deserialize, Debug, Clone)]
pub struct DidConfig {
    pub path: PathBuf,
}

#[derive(Deserialize, Debug, Clone)]
pub struct PlcConfig {
    pub path: PathBuf,
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
    /// The DID configuration block.
    pub did: DidConfig,
    /// The sqlite database connection options.
    pub db: String,
}
