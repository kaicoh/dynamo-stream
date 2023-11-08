use crate::error::HttpError;

use axum::{async_trait, extract::FromRequest, http::Request, RequestExt};
use serde::Deserialize;
use std::fmt;
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
    <axum::Json<<J as FromValidate>::Validatable> as FromRequest<(), B>>::Rejection: fmt::Display,
{
    type Rejection = HttpError;

    async fn from_request(req: Request<B>, _state: &S) -> Result<Self, Self::Rejection> {
        let axum::Json(req) = req
            .extract::<axum::Json<<J as FromValidate>::Validatable>, _>()
            .await
            .map_err(|err| HttpError::Unprocessable(format!("{err}")))?;
        req.validate().map_err(HttpError::Validation)?;
        Ok(Self(FromValidate::from(req)))
    }
}

pub trait FromValidate {
    type Validatable;

    fn from(value: Self::Validatable) -> Self;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Deserialize, Validate)]
    struct RawTest {
        #[validate(required)]
        name: Option<String>,
    }

    #[derive(Debug, PartialEq)]
    struct Test {
        name: String,
    }

    impl FromValidate for Test {
        type Validatable = RawTest;

        fn from(value: RawTest) -> Test {
            Test {
                name: value.name.expect("`name` should be Some"),
            }
        }
    }

    impl fmt::Debug for Json<Test> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
            write!(f, "{:?}", self.0)
        }
    }

    #[async_trait]
    impl FromRequest<(), RawTest> for axum::Json<RawTest> {
        type Rejection = HttpError;

        async fn from_request(req: Request<RawTest>, _state: &()) -> Result<Self, Self::Rejection> {
            let body = req.body().clone();
            Ok(Self(body))
        }
    }

    #[tokio::test]
    async fn it_gets_raw_request_and_returns_validated_struct() {
        let body = RawTest {
            name: Some("Tanaka".into()),
        };

        let req = Request::builder()
            .method("POST")
            .uri("http://foo.bar")
            .body(body)
            .unwrap();

        let result = Json::<Test>::from_request(req, &()).await;
        assert!(result.is_ok());

        let inner = result.unwrap().0;
        assert_eq!(inner.name, "Tanaka");
    }

    #[tokio::test]
    async fn it_returns_error_if_it_violates_validation() {
        let body = RawTest { name: None };

        let req = Request::builder()
            .method("POST")
            .uri("http://foo.bar")
            .body(body)
            .unwrap();

        let result = Json::<Test>::from_request(req, &()).await;
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(matches!(err, HttpError::Validation(_)));
    }
}
