use crate::error::HttpError;

use async_trait::async_trait;
use axum::{extract::FromRequest, http::Request, RequestExt};
use tracing::warn;
use validator::Validate;

pub struct Json<J>(pub J);

#[async_trait]
impl<S, B, J> FromRequest<S, B> for Json<J>
where
    B: Send + 'static,
    S: Send + Sync,
    J: IntoValid + 'static,
    axum::Json<J>: FromRequest<(), B>,
    <axum::Json<J> as FromRequest<(), B>>::Rejection: std::fmt::Debug,
{
    type Rejection = HttpError;

    async fn from_request(req: Request<B>, _state: &S) -> Result<Self, Self::Rejection> {
        let axum::Json(data) = req.extract::<axum::Json<J>, _>().await.map_err(|err| {
            warn!("{:#?}", err);
            HttpError::Validation(None)
        })?;
        data.validate()
            .map_err(|err| HttpError::Validation(Some(err)))?;
        Ok(Self(data))
    }
}

pub trait IntoValid: Validate {
    type Valid;

    fn into_valid(self) -> Self::Valid;
}
