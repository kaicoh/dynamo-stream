use axum::{
    body,
    http::{
        header::{HeaderValue, CONTENT_TYPE},
        StatusCode,
    },
    response::{IntoResponse, Response},
};
use std::sync::{MutexGuard, PoisonError};
use thiserror::Error;
use tracing::error;

pub fn from_guard<T>(error: PoisonError<MutexGuard<'_, T>>) -> anyhow::Error {
    error!("{:#?}", error);
    anyhow::anyhow!("{}", error)
}

#[derive(Debug, Error)]
pub enum HttpError {
    #[error("Not found: `{0}`")]
    NotFound(String),
    #[error("Internal Server Error")]
    Server(#[from] anyhow::Error),
}

impl HttpError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Server(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        Response::builder()
            .status(self.status_code())
            .header(CONTENT_TYPE, HeaderValue::from_static("text/plain"))
            .body(body::boxed(format!("{self}")))
            .unwrap()
    }
}
