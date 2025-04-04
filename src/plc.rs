//! PLC operations.
use std::collections::HashMap;

use anyhow::{Context as _, bail};
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{Client, RotationKey};

/// The URL of the public PLC directory.
const PLC_DIRECTORY: &str = "https://plc.directory/";

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase", tag = "type")]
/// A PLC service.
pub(crate) enum PlcService {
    #[serde(rename = "AtprotoPersonalDataServer")]
    /// A personal data server.
    Pds {
        /// The URL of the PDS.
        endpoint: String,
    },
}

#[expect(
    clippy::arbitrary_source_item_ordering,
    reason = "serialized data might be structured"
)]
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PlcOperation {
    #[serde(rename = "type")]
    pub typ: String,
    pub rotation_keys: Vec<String>,
    pub verification_methods: HashMap<String, String>,
    pub also_known_as: Vec<String>,
    pub services: HashMap<String, PlcService>,
    pub prev: Option<String>,
}

impl PlcOperation {
    /// Sign an operation with the provided signature.
    pub(crate) fn sign(self, sig: Vec<u8>) -> SignedPlcOperation {
        SignedPlcOperation {
            typ: self.typ,
            rotation_keys: self.rotation_keys,
            verification_methods: self.verification_methods,
            also_known_as: self.also_known_as,
            services: self.services,
            prev: self.prev,
            sig: base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(sig),
        }
    }
}

#[expect(
    clippy::arbitrary_source_item_ordering,
    reason = "serialized data might be structured"
)]
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
/// A signed PLC operation.
pub(crate) struct SignedPlcOperation {
    #[serde(rename = "type")]
    pub typ: String,
    pub rotation_keys: Vec<String>,
    pub verification_methods: HashMap<String, String>,
    pub also_known_as: Vec<String>,
    pub services: HashMap<String, PlcService>,
    pub prev: Option<String>,
    pub sig: String,
}

pub(crate) fn sign_op(rkey: &RotationKey, op: PlcOperation) -> anyhow::Result<SignedPlcOperation> {
    let bytes = serde_ipld_dagcbor::to_vec(&op).context("failed to encode op")?;
    let bytes = rkey.sign(&bytes).context("failed to sign op")?;

    Ok(op.sign(bytes))
}

/// Submit a PLC operation to the public directory.
pub(crate) async fn submit(
    client: &Client,
    did: &str,
    op: &SignedPlcOperation,
) -> anyhow::Result<()> {
    debug!(
        "submitting {} {}",
        did,
        serde_json::to_string(&op).context("should serialize")?
    );

    let res = client
        .post(format!("{PLC_DIRECTORY}{did}"))
        .json(&op)
        .send()
        .await
        .context("failed to send directory request")?;

    if res.status().is_success() {
        Ok(())
    } else {
        let e = res
            .json::<serde_json::Value>()
            .await
            .context("failed to read error response")?;

        bail!(
            "error from PLC directory: {}",
            serde_json::to_string(&e).context("should serialize")?
        );
    }
}
