use axum::{
    body::Body,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use thiserror::Error;
use tracing::error;

/// `axum`-compatible error handler.
#[derive(Error)]
pub struct Error {
    status: StatusCode,
    err: anyhow::Error,
    message: Option<String>,
}

impl Error {
    pub fn unimplemented(err: impl Into<anyhow::Error>) -> Self {
        Self::with_status(StatusCode::NOT_IMPLEMENTED, err)
    }

    pub fn with_status(status: StatusCode, err: impl Into<anyhow::Error>) -> Self {
        Self {
            status,
            err: err.into(),
            message: None,
        }
    }

    pub fn with_message(status: StatusCode, err: impl Into<anyhow::Error>, message: impl Into<String>) -> Self {
        Self {
            status,
            err: err.into(),
            message: Some(message.into()),            
        }
    }
}

impl From<anyhow::Error> for Error {
    fn from(err: anyhow::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            err,
            message: None,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.status, self.err)
    }
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.err.fmt(f)
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        error!("{:?}", self.err);

        // N.B: Forward out the error message to the requester if this is a debug build.
        // This is insecure for production builds, so we'll return an empty body if this
        // is a release build, unless a message was explicitly set.
        if cfg!(debug_assertions) {
            Response::builder()
                .status(self.status)
                .body(Body::new(format!("{:?}", self.err)))
                .unwrap()
        } else {
            Response::builder()
                .status(self.status)
                .body(Body::new(self.message.unwrap_or_default()))
                .unwrap()
        }
    }
}
