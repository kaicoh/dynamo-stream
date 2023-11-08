use crate::{
    error::{from_guard, HttpError},
    extractors::{FromValidate, Json},
    state::Entry,
    SharedState,
};
use axum::{
    extract::{Path, State},
    response::IntoResponse,
    routing::{delete, get, post},
    Router,
};
use serde::Deserialize;
use ulid::Ulid;
use validator::Validate;

#[derive(Debug, Deserialize, Validate)]
struct RawEntryBody {
    #[validate(required, length(max = 255))]
    table_name: Option<String>,
    #[validate(required, length(max = 255))]
    url: Option<String>,
}

#[derive(Debug)]
struct EntryBody {
    table_name: String,
    url: String,
}

impl FromValidate for EntryBody {
    type Validatable = RawEntryBody;

    fn from(b: RawEntryBody) -> EntryBody {
        EntryBody {
            table_name: b.table_name.expect("`table_name` should be Some"),
            url: b.url.expect("`url` should be Some"),
        }
    }
}

async fn index(State(state): State<SharedState>) -> Result<impl IntoResponse, HttpError> {
    let state = state.lock().map_err(from_guard)?;
    Ok(axum::response::Json(state.entry_states()))
}

async fn register(
    State(state): State<SharedState>,
    Json(body): Json<EntryBody>,
) -> Result<impl IntoResponse, HttpError> {
    let EntryBody { table_name, url } = body;
    let id = Ulid::new().to_string();

    let mut state = state.lock().map_err(from_guard)?;
    let notifier = state.get_notifier();
    let entry = Entry::new(table_name, url, notifier);
    state.insert(&id, entry);

    Ok(id)
}

async fn deregister(
    State(state): State<SharedState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, HttpError> {
    let mut state = state.lock().map_err(from_guard)?;

    state
        .set_removed(&id)
        .ok_or(HttpError::NotFound(format!("Entry id: {id}")))
        .map(|_| id)
}

pub fn router(state: SharedState) -> Router {
    Router::new()
        .route("/:id", delete(deregister))
        .route("/", get(index))
        .route("/", post(register))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{state::EntryStatus, AppState};
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        response::Response,
    };
    use std::sync::{Arc, Mutex};
    use tokio::sync::mpsc;
    use tower::ServiceExt;

    #[tokio::test]
    async fn index_handler_returns_current_entries() {
        let state = build_state();
        let app = router(state);
        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = json_body(response).await;
        let expected = serde_json::json!({
            "entry_0": {
                "status": "CREATED",
                "table_name": "People",
                "url": "http://test.com",
            },
        });
        assert_eq!(body, expected);
    }

    #[tokio::test]
    async fn register_handler_adds_entry() {
        let state = build_state();
        let app = router(Arc::clone(&state));

        let body = request_body(serde_json::json!({
            "table_name": "People",
            "url": "http://test2.co.jp",
        }));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/")
                    .header("Content-Type", "application/json")
                    .body(body)
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let id = text_body(response).await;

        let mut state = state.lock().unwrap();
        assert_eq!(state.iter_mut().len(), 2);

        let opt = state
            .iter_mut()
            .find(|(_id, _)| _id.as_str() == id.as_str());
        assert!(opt.is_some());

        let (_, entry) = opt.unwrap();
        assert_eq!(entry.table_name(), "People");
        assert_eq!(entry.url(), "http://test2.co.jp");
        assert_eq!(entry.status(), EntryStatus::Created);
    }

    #[tokio::test]
    async fn deregister_handler_marks_removed_to_entry() {
        let state = build_state();
        let app = router(Arc::clone(&state));

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/entry_0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = text_body(response).await;
        assert_eq!(body, "entry_0");

        let mut state = state.lock().unwrap();
        assert_eq!(state.iter_mut().len(), 1);

        match state.iter_mut().next() {
            Some((id, entry)) => {
                assert_eq!(id, "entry_0");
                assert_eq!(entry.status(), EntryStatus::Removed);
            }
            None => {
                unreachable!("There are no entry!");
            }
        }
    }

    #[tokio::test]
    async fn deregister_handler_returns_not_found() {
        let state = build_state();
        let app = router(Arc::clone(&state));

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/unknown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let body = json_body(response).await;
        let expected = serde_json::json!({
            "message": "Not found: `Entry id: unknown`",
        });
        assert_eq!(body, expected);
    }

    fn build_state() -> SharedState {
        let (tx, _rx) = mpsc::channel::<_>(10);
        let mut state = AppState::new(tx.clone());
        let entry = Entry::new("People", "http://test.com", tx);
        state.insert("entry_0", entry);
        Arc::new(Mutex::new(state))
    }

    async fn json_body(response: Response) -> serde_json::Value {
        let bytes = hyper::body::to_bytes(response.into_body()).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    async fn text_body(response: Response) -> String {
        let bytes = hyper::body::to_bytes(response.into_body()).await.unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    fn request_body(value: serde_json::Value) -> Body {
        Body::from(value.to_string())
    }
}
