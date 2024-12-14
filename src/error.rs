use axum::{http::StatusCode, response::IntoResponse};
use thiserror::Error;
use tracing::error;

/// `axum`-compatible error handler.
#[derive(Debug, Error)]
#[error(transparent)]
pub struct Error(#[from] anyhow::Error);

impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        error!("{:?}", self.0);

        // N.B: Forward out the error message to the requester if this is a debug build.
        // This is insecure for production builds, so we'll return an empty body if this
        // is a release build.
        if cfg!(debug_assertions) {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("{:?}", self.0)).into_response()
        } else {
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
