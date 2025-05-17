//! DID utilities.

use anyhow::{Context as _, Result, bail};
use atrium_api::types::string::Did;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::serve::Client;

/// URL whitelist for DID document resolution.
const ALLOWED_URLS: &[&str] = &["bsky.app", "bsky.chat"];

#[expect(
    clippy::arbitrary_source_item_ordering,
    reason = "serialized data might be structured"
)]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
/// DID verification method.
pub(crate) struct DidVerificationMethod {
    pub id: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub controller: String,
    pub public_key_multibase: String,
}

#[expect(
    clippy::arbitrary_source_item_ordering,
    reason = "serialized data might be structured"
)]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DidService {
    pub id: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub service_endpoint: Url,
}

#[expect(
    clippy::arbitrary_source_item_ordering,
    reason = "serialized data might be structured"
)]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
/// DID document.
pub(crate) struct DidDocument {
    #[serde(rename = "@context", skip_serializing_if = "Vec::is_empty")]
    pub context: Vec<Url>,
    pub id: Did,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub verification_method: Vec<DidVerificationMethod>,
    pub service: Vec<DidService>,
}

/// Resolve a DID document using the specified reqwest client.
pub(crate) async fn resolve(client: &Client, did: Did) -> Result<DidDocument> {
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

            format!("https://{host}/.well-known/did.json")
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
