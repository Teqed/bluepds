//! Authentication layers

use std::path::PathBuf;

use anyhow::Context;
use atrium_api::types::string::Cid;
use atrium_repo::blockstore::{AsyncBlockStoreRead, AsyncBlockStoreWrite, CarStore};
use axum::extract::FromRequestParts;
use serde::{Deserialize, Serialize};

use crate::{AppState, Result};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct LogonClaims {
    /// Target audience
    pub aud: String,
    /// Issued at
    pub iat: usize,
    /// Expiration time (UTC timestamp)
    pub exp: usize,
    /// Subject (DID of authenticated user)
    pub sub: String,
    /// Scope of the token
    pub scope: String,
}

pub struct AuthenticatedUser {
    did: String,
    storage: PathBuf,
}

impl AuthenticatedUser {
    /// Retrieve a handle to the backing storage for the user
    pub async fn storage(&self) -> Result<(impl AsyncBlockStoreRead + AsyncBlockStoreWrite, Cid)> {
        let store = CarStore::open(
            tokio::fs::File::open(self.storage.clone())
                .await
                .context("failed to open backing storage")?,
        )
        .await
        .map_err(anyhow::Error::new)?;
        let root = store.roots().next().context("no roots found in storage")?;

        Ok((store, root))
    }
}

#[axum::async_trait]
impl FromRequestParts<AppState> for AuthenticatedUser {
    type Rejection = crate::Error;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &AppState,
    ) -> std::result::Result<Self, Self::Rejection> {
        todo!()
    }
}
