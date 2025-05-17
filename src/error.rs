//! Error handling for the application.
use axum::{
    body::Body,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use rsky_pds::handle::{self, errors::ErrorKind};
use thiserror::Error;
use tracing::error;

/// `axum`-compatible error handler.
#[derive(Error)]
#[expect(clippy::error_impl_error, reason = "just one")]
pub struct Error {
    /// The actual error that occurred.
    err: anyhow::Error,
    /// The error message to be returned as JSON body.
    message: Option<ErrorMessage>,
    /// The HTTP status code to be returned.
    status: StatusCode,
}

#[derive(Default, serde::Serialize)]
/// A JSON error message.
pub(crate) struct ErrorMessage {
    /// The error type.
    /// This is used to identify the error in the client.
    /// E.g. `InvalidRequest`, `ExpiredToken`, `InvalidToken`, `HandleNotFound`.
    error: String,
    /// The error message.
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
    pub fn unimplemented<T: Into<anyhow::Error>>(err: T) -> Self {
        Self::with_status(StatusCode::NOT_IMPLEMENTED, err)
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
    /// Returned when just providing a status code.
    pub fn with_status<T: Into<anyhow::Error>>(status: StatusCode, err: T) -> Self {
        Self {
            status,
            err: err.into(),
            message: None,
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

/// API error types that can be returned to clients
#[derive(Clone, Debug)]
pub enum ApiError {
    RuntimeError,
    InvalidLogin,
    AccountTakendown,
    InvalidRequest(String),
    ExpiredToken,
    InvalidToken,
    RecordNotFound,
    InvalidHandle,
    InvalidEmail,
    InvalidPassword,
    InvalidInviteCode,
    HandleNotAvailable,
    EmailNotAvailable,
    UnsupportedDomain,
    UnresolvableDid,
    IncompatibleDidDoc,
    WellKnownNotFound,
    AccountNotFound,
    BlobNotFound,
    BadRequest(String, String),
    AuthRequiredError(String),
}

impl ApiError {
    /// Get the appropriate HTTP status code for this error
    const fn status_code(&self) -> StatusCode {
        match self {
            Self::RuntimeError => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidLogin
            | Self::ExpiredToken
            | Self::InvalidToken
            | Self::AuthRequiredError(_) => StatusCode::UNAUTHORIZED,
            Self::AccountTakendown => StatusCode::FORBIDDEN,
            Self::RecordNotFound
            | Self::WellKnownNotFound
            | Self::AccountNotFound
            | Self::BlobNotFound => StatusCode::NOT_FOUND,
            // All bad requests grouped together
            _ => StatusCode::BAD_REQUEST,
        }
    }

    /// Get the error type string for API responses
    fn error_type(&self) -> String {
        match self {
            Self::RuntimeError => "InternalServerError",
            Self::InvalidLogin => "InvalidLogin",
            Self::AccountTakendown => "AccountTakendown",
            Self::InvalidRequest(_) => "InvalidRequest",
            Self::ExpiredToken => "ExpiredToken",
            Self::InvalidToken => "InvalidToken",
            Self::RecordNotFound => "RecordNotFound",
            Self::InvalidHandle => "InvalidHandle",
            Self::InvalidEmail => "InvalidEmail",
            Self::InvalidPassword => "InvalidPassword",
            Self::InvalidInviteCode => "InvalidInviteCode",
            Self::HandleNotAvailable => "HandleNotAvailable",
            Self::EmailNotAvailable => "EmailNotAvailable",
            Self::UnsupportedDomain => "UnsupportedDomain",
            Self::UnresolvableDid => "UnresolvableDid",
            Self::IncompatibleDidDoc => "IncompatibleDidDoc",
            Self::WellKnownNotFound => "WellKnownNotFound",
            Self::AccountNotFound => "AccountNotFound",
            Self::BlobNotFound => "BlobNotFound",
            Self::BadRequest(error, _) => error,
            Self::AuthRequiredError(_) => "AuthRequiredError",
        }
        .to_owned()
    }

    /// Get the user-facing error message
    fn message(&self) -> String {
        match self {
            Self::RuntimeError => "Something went wrong",
            Self::InvalidLogin => "Invalid identifier or password",
            Self::AccountTakendown => "Account has been taken down",
            Self::InvalidRequest(msg) => msg,
            Self::ExpiredToken => "Token is expired",
            Self::InvalidToken => "Token is invalid",
            Self::RecordNotFound => "Record could not be found",
            Self::InvalidHandle => "Handle is invalid",
            Self::InvalidEmail => "Invalid email",
            Self::InvalidPassword => "Invalid Password",
            Self::InvalidInviteCode => "Invalid invite code",
            Self::HandleNotAvailable => "Handle not available",
            Self::EmailNotAvailable => "Email not available",
            Self::UnsupportedDomain => "Unsupported domain",
            Self::UnresolvableDid => "Unresolved Did",
            Self::IncompatibleDidDoc => "IncompatibleDidDoc",
            Self::WellKnownNotFound => "User not found",
            Self::AccountNotFound => "Account could not be found",
            Self::BlobNotFound => "Blob could not be found",
            Self::BadRequest(_, msg) => msg,
            Self::AuthRequiredError(msg) => msg,
        }
        .to_owned()
    }
}

impl From<Error> for ApiError {
    fn from(_value: Error) -> Self {
        Self::RuntimeError
    }
}

impl From<handle::errors::Error> for ApiError {
    fn from(value: handle::errors::Error) -> Self {
        match value.kind {
            ErrorKind::InvalidHandle => Self::InvalidHandle,
            ErrorKind::HandleNotAvailable => Self::HandleNotAvailable,
            ErrorKind::UnsupportedDomain => Self::UnsupportedDomain,
            ErrorKind::InternalError => Self::RuntimeError,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let error_type = self.error_type();
        let message = self.message();

        if cfg!(debug_assertions) {
            error!("API Error: {}: {}", error_type, message);
        }

        // Create the error message and serialize to JSON
        let error_message = ErrorMessage::new(error_type, message);
        let body = serde_json::to_string(&error_message).unwrap_or_else(|_| {
            r#"{"error":"InternalServerError","message":"Error serializing response"}"#.to_owned()
        });

        // Build the response
        Response::builder()
            .status(status)
            .header("Content-Type", "application/json")
            .body(Body::new(body))
            .expect("should be a valid response")
    }
}
