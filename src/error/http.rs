use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use thiserror::Error;
use tracing::error;
use validator::{ValidationErrors, ValidationErrorsKind};

#[derive(Debug, Error)]
pub enum HttpError {
    #[error("validation error")]
    Validation(Option<ValidationErrors>),
    #[error("Not found: `{0}`")]
    NotFound(String),
    #[error("Internal Server Error")]
    Server(#[from] anyhow::Error),
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        match serde_json::to_vec(&self.body()) {
            Ok(body) => {
                let status = self.status_code();
                let header = [("content-type", "application/json")];
                (status, header, body).into_response()
            }
            Err(err) => {
                error!("{:#?}", err);
                let status = StatusCode::INTERNAL_SERVER_ERROR;
                let header = [("content-type", "text/plain")];
                (status, header, format!("{err}")).into_response()
            }
        }
    }
}

impl HttpError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Validation(_) => StatusCode::BAD_REQUEST,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Server(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn body(&self) -> Body {
        Body {
            message: format!("{self}"),
            errors: self.validation_errors(),
        }
    }

    fn validation_errors(&self) -> Vec<ValidationErrorContent> {
        match self {
            Self::Validation(Some(errors)) => ValidationErrorContent::from_errors(errors),
            _ => vec![],
        }
    }
}

#[derive(Debug, Serialize)]
struct Body {
    message: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    errors: Vec<ValidationErrorContent>,
}

#[derive(Debug, Serialize)]
struct ValidationErrorContent {
    field: String,
    messages: Vec<String>,
}

impl ValidationErrorContent {
    fn from_errors(errors: &ValidationErrors) -> Vec<Self> {
        let mut results: Vec<Self> = vec![];
        reduce(errors, "", &mut results);
        results
    }
}

fn reduce(errors: &ValidationErrors, prefix: &str, acc: &mut Vec<ValidationErrorContent>) {
    for (key, val) in errors.errors() {
        match val {
            ValidationErrorsKind::Struct(e) => {
                let p = format!("{}{}.", prefix, key);
                reduce(e, &p, acc);
            }
            ValidationErrorsKind::List(m) => {
                for (n, e) in m {
                    let p = format!("{}{}[{}].", prefix, key, n);
                    reduce(e, &p, acc);
                }
            }
            ValidationErrorsKind::Field(e) => {
                let field = format!("{}{}", prefix, key);
                let messages = e
                    .iter()
                    .map(|err| {
                        if err.code == "required" {
                            err.code.to_string()
                        } else {
                            match err.message.as_ref() {
                                Some(message) => message.to_string(),
                                None => "Invalid value".to_string(),
                            }
                        }
                    })
                    .collect();
                acc.push(ValidationErrorContent { field, messages });
            }
        }
    }
}
