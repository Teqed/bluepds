use std::collections::HashMap;

use anyhow::Context;
use base64::Engine;
use serde::{Deserialize, Serialize};

/// The URL of the public PLC directory.
const PLC_DIRECTORY: &str = "https://plc.directory/";

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum PlcService {
    #[serde(rename = "AtprotoPersonalDataServer")]
    Pds { service_endpoint: String },
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PlcVerificationMethod {
    pub atproto: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PlcOperation {
    #[serde(rename = "type")]
    pub typ: String,
    pub rotation_keys: Vec<String>,
    pub verification_methods: Vec<PlcVerificationMethod>,
    pub also_known_as: Vec<String>,
    pub services: HashMap<String, PlcService>,
    pub prev: Option<String>,
}

impl PlcOperation {
    pub fn sign(self, sig: Vec<u8>) -> SignedPlcOperation {
        SignedPlcOperation {
            typ: self.typ,
            rotation_keys: self.rotation_keys,
            verification_methods: self.verification_methods,
            also_known_as: self.also_known_as,
            services: self.services,
            prev: self.prev,
            sig: base64::prelude::BASE64_URL_SAFE.encode(sig),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SignedPlcOperation {
    #[serde(rename = "type")]
    pub typ: String,
    pub rotation_keys: Vec<String>,
    pub verification_methods: Vec<PlcVerificationMethod>,
    pub also_known_as: Vec<String>,
    pub services: HashMap<String, PlcService>,
    pub prev: Option<String>,
    pub sig: String,
}

/// Submit a PLC operation to the public directory.
pub async fn submit(did: &str, op: &SignedPlcOperation) -> anyhow::Result<()> {
    let res = reqwest::Client::new()
        .post(format!("{PLC_DIRECTORY}{did}"))
        .json(op)
        .send()
        .await
        .context("failed to send directory request")?;

    res.error_for_status()
        .context("plc directory returned an error")?;
    Ok(())
}
