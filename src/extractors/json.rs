use crate::error::HttpError;

use async_trait::async_trait;
use axum::{extract::FromRequest, http::Request, RequestExt};
use serde::Deserialize;
use tracing::warn;
use validator::Validate;

pub struct Json<J>(pub J);

#[async_trait]
impl<S, B, J> FromRequest<S, B> for Json<J>
where
    B: Send + 'static,
    S: Send + Sync,
    J: FromValidate + 'static,
    <J as FromValidate>::Validatable: Validate + for<'de> Deserialize<'de>,
    axum::Json<<J as FromValidate>::Validatable>: FromRequest<(), B>,
    <axum::Json<<J as FromValidate>::Validatable> as FromRequest<(), B>>::Rejection:
        std::fmt::Debug,
{
    type Rejection = HttpError;

    async fn from_request(req: Request<B>, _state: &S) -> Result<Self, Self::Rejection> {
        let axum::Json(req) = req
            .extract::<axum::Json<<J as FromValidate>::Validatable>, _>()
            .await
            .map_err(|err| {
                warn!("{:#?}", err);
                HttpError::Validation(None)
            })?;
        req.validate()
            .map_err(|err| HttpError::Validation(Some(err)))?;
        Ok(Self(FromValidate::from(req)))
    }
}

pub trait FromValidate {
    type Validatable;

    fn from(value: Self::Validatable) -> Self;
}
