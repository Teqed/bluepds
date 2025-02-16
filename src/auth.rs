//! Authentication layers

use anyhow::{anyhow, Context};
use axum::{extract::FromRequestParts, http::StatusCode};

use crate::{AppState, Error};

pub struct AuthenticatedUser {
    did: String,
    session: String,
}

impl AuthenticatedUser {
    pub fn did(&self) -> String {
        self.did.clone()
    }

    pub fn session(&self) -> String {
        self.session.clone()
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

        if let Some(session_id) = session_id {
            let did = sqlx::query_scalar!(r#"SELECT did FROM sessions WHERE id = ?"#, session_id)
                .fetch_optional(&state.db)
                .await
                .context("failed to query session")?;

            let did = did.context("session not found")?;

            Ok(AuthenticatedUser {
                session: session_id.to_string(),
                did,
            })
        } else {
            Err(Error::with_status(
                StatusCode::UNAUTHORIZED,
                anyhow!("no authorization token provided"),
            ))
        }
    }
}
