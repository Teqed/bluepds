//! Error handling for the application.
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
    message: Option<ErrorMessage>,
}

#[derive(Default, serde::Serialize)]
/// A JSON error message.
pub(crate) struct ErrorMessage {
    error: String,
    message: String,
}
impl std::fmt::Display for ErrorMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            r#"{{"error":"{}","message":"{}"}}"#,
            self.error, self.message
        )
    }
}
impl ErrorMessage {
    /// Create a new error message to be returned as JSON body.
    pub(crate) fn new(error: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            message: message.into(),
        }
    }
}

impl Error {
    /// Returned when a route is not yet implemented.
    pub fn unimplemented(err: impl Into<anyhow::Error>) -> Self {
        Self::with_status(StatusCode::NOT_IMPLEMENTED, err)
    }

    /// Returned when just providing a status code.
    pub fn with_status(status: StatusCode, err: impl Into<anyhow::Error>) -> Self {
        Self {
            status,
            err: err.into(),
            message: None,
        }
    }

    /// Returned when providing a status code and a JSON message body.
    pub(crate) fn with_message(
        status: StatusCode,
        err: impl Into<anyhow::Error>,
        message: impl Into<ErrorMessage>,
    ) -> Self {
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
    fn into_response(self) -> Response {
        error!("{:?}", self.err);

        // N.B: Forward out the error message to the requester if this is a debug build.
        // This is insecure for production builds, so we'll return an empty body if this
        // is a release build, unless a message was explicitly set.
        if cfg!(debug_assertions) {
            Response::builder()
                .status(self.status)
                .body(Body::new(format!("{:?}", self.err)))
                .expect("should be a valid response")
        } else {
            Response::builder()
                .status(self.status)
                .header("Content-Type", "application/json")
                .body(Body::new(self.message.unwrap_or_default().to_string()))
                .expect("should be a valid response")
        }
    }
}
