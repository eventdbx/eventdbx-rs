use std::fmt;
use std::str::Utf8Error;

use capnp::NotInSchema;
use thiserror::Error;

use crate::noise::NoiseError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("cap'n proto error: {0}")]
    Capnp(#[from] capnp::Error),
    #[error("json serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("authentication rejected: {0}")]
    Authentication(String),
    #[error("server responded with an error: {0}")]
    Server(#[from] ServerError),
    #[error("protocol violation: {0}")]
    Protocol(String),
    #[error("request timed out")]
    Timeout,
}

impl From<NotInSchema> for Error {
    fn from(value: NotInSchema) -> Self {
        Self::Protocol(format!("value not in schema: {value}"))
    }
}

impl From<Utf8Error> for Error {
    fn from(err: Utf8Error) -> Self {
        Self::Protocol(format!("invalid utf-8: {err}"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerError {
    pub code: String,
    pub message: String,
}

impl ServerError {
    pub(crate) fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ServerError {}

impl From<NoiseError> for Error {
    fn from(err: NoiseError) -> Self {
        match err {
            NoiseError::Io(inner) => Self::Io(inner),
            NoiseError::Protocol(msg) => Self::Protocol(msg),
        }
    }
}
