//! Authentication layers

use std::path::PathBuf;

use anyhow::Context;
use atrium_repo::{
    blockstore::{AsyncBlockStoreRead, AsyncBlockStoreWrite, CarStore},
    Cid,
};
use axum::extract::FromRequestParts;
use serde::{Deserialize, Serialize};

use crate::{AppState, Result};

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

impl FromRequestParts<AppState> for AuthenticatedUser {
    type Rejection = crate::Error;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &AppState,
    ) -> std::result::Result<Self, Self::Rejection> {
        let session_id = parts.headers.get("authorization").and_then(|auth| {
            auth.to_str()
                .ok()
                .and_then(|auth| auth.strip_prefix("Bearer "))
        });

        let session_id = session_id.context("missing authorization header")?;

        let did = sqlx::query_scalar!(r#"SELECT did FROM sessions WHERE id = ?"#, session_id)
            .fetch_optional(&state.db)
            .await
            .context("failed to query session")?;

        let did = did.context("session not found")?;

        Ok(AuthenticatedUser {
            storage: state.config.did.path.join(&did),
            did,
        })
    }
}
