use crate::web::error::HttpError;

use axum::{
    async_trait,
    extract::{rejection::JsonRejection, FromRequest},
    http::Request,
};
use serde::Deserialize;
use validator::Validate;

pub struct Json<J>(pub J);

#[async_trait]
impl<S, B, J> FromRequest<S, B> for Json<J>
where
    B: Send + 'static,
    S: Send + Sync,
    J: FromValidate + 'static,
    axum::Json<<J as FromValidate>::Validatable>: FromRequest<S, B, Rejection = JsonRejection>,
{
    type Rejection = HttpError;

    async fn from_request(req: Request<B>, _state: &S) -> Result<Self, Self::Rejection> {
        let axum::Json(req) =
            axum::Json::<<J as FromValidate>::Validatable>::from_request(req, _state)
                .await
                .map_err(|err| HttpError::Unprocessable(format!("{err}")))?;
        req.validate().map_err(HttpError::Validation)?;
        Ok(Self(FromValidate::from(req)))
    }
}

pub trait FromValidate {
    type Validatable: Validate + for<'de> Deserialize<'de>;

    fn from(value: Self::Validatable) -> Self;
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use serde::Serialize;

    #[derive(Debug, Clone, Deserialize, Serialize, Validate)]
    struct RawTest {
        #[validate(required)]
        name: Option<String>,
    }

    impl From<RawTest> for Body {
        fn from(value: RawTest) -> Body {
            Body::from(serde_json::to_string(&value).unwrap())
        }
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

    #[tokio::test]
    async fn it_gets_raw_request_and_returns_validated_struct() {
        let raw = RawTest {
            name: Some("Tanaka".into()),
        };

        let req = Request::builder()
            .method("POST")
            .header("Content-Type", "application/json")
            .uri("http://foo.bar")
            .body(Body::from(raw))
            .unwrap();

        let result = Json::<Test>::from_request(req, &()).await;
        assert!(result.is_ok());

        let inner = result.unwrap().0;
        assert_eq!(inner.name, "Tanaka");
    }

    #[tokio::test]
    async fn it_returns_error_if_it_violates_validation() {
        let raw = RawTest { name: None };

        let req = Request::builder()
            .method("POST")
            .header("Content-Type", "application/json")
            .uri("http://foo.bar")
            .body(Body::from(raw))
            .unwrap();

        let result = Json::<Test>::from_request(req, &()).await;

        match result {
            Ok(_) => {
                unreachable!("The result shoud be an error");
            }
            Err(err) => {
                assert!(matches!(err, HttpError::Validation(_)));
            }
        }
    }

    #[tokio::test]
    async fn it_returns_unprocessable_error_if_it_fails_deserializing() {
        let req = Request::builder()
            .method("POST")
            .header("Content-Type", "application/json")
            .uri("http://foo.bar")
            .body(Body::from("{\"foo\":\"bar}"))
            .unwrap();

        let result = Json::<Test>::from_request(req, &()).await;

        match result {
            Ok(_) => {
                unreachable!("The result shoud be an error");
            }
            Err(err) => {
                assert!(matches!(err, HttpError::Unprocessable(_)));
            }
        }
    }

    #[tokio::test]
    async fn it_returns_unprocessable_error_if_there_are_no_content_type_header() {
        let raw = RawTest {
            name: Some("Tanaka".into()),
        };

        let req = Request::builder()
            .method("POST")
            .uri("http://foo.bar")
            .body(Body::from(raw))
            .unwrap();

        let result = Json::<Test>::from_request(req, &()).await;

        match result {
            Ok(_) => {
                unreachable!("The result shoud be an error");
            }
            Err(err) => {
                assert!(matches!(err, HttpError::Unprocessable(_)));
            }
        }
    }
}
