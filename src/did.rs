use anyhow::{bail, Context, Result};
use atrium_api::types::string::Did;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::Client;

/// URL whitelist for DID document resolution.
const ALLOWED_URLS: &[&str] = &["bsky.app", "bsky.chat"];

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DidVerificationMethod {
    pub id: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub controller: String,
    pub public_key_multibase: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DidService {
    pub id: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub service_endpoint: Url,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DidDocument {
    #[serde(rename = "@context", skip_serializing_if = "Vec::is_empty")]
    pub context: Vec<Url>,
    pub id: Did,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub verification_method: Vec<DidVerificationMethod>,
    pub service: Vec<DidService>,
}

/// Resolve a DID document using the specified reqwest client.
pub async fn resolve(client: &Client, did: Did) -> Result<DidDocument> {
    let url = match did.method() {
        "did:web" => {
            // N.B: This is a potentially hostile operation, so we are only going to allow
            // certain URLs for now.
            let host = did
                .as_str()
                .strip_prefix("did:web:")
                .context("invalid DID format")?;

            if !ALLOWED_URLS.iter().any(|u| host.ends_with(u)) {
                bail!("forbidden URL {host}");
            }

            format!("https://{}/.well-known/did.json", host)
        }
        "did:plc" => {
            format!("https://plc.directory/{}", did.as_str())
        }
        m => bail!("unknown did method {m}"),
    }
    .parse::<Url>()
    .context("failed to resolve DID URL")?;

    client
        .get(url)
        .send()
        .await
        .context("failed to fetch DID document")?
        .json()
        .await
        .context("failed to decode DID document")
}
