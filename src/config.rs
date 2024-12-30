use std::net::SocketAddr;

use atrium_api::types::string::Did;
use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct AppConfig {
    pub listen_address: Option<SocketAddr>,
    pub did: Did,
}
