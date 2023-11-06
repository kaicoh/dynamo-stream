use crate::{
    error::{from_guard, HttpError},
    extractors::{IntoValid, Json},
    types::Entry,
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
struct Request {
    #[validate(required, length(max = 255))]
    table_name: Option<String>,
    #[validate(required, length(max = 255))]
    url: Option<String>,
}

#[derive(Debug)]
struct ValidRequest {
    table_name: String,
    url: String,
}

impl IntoValid for Request {
    type Valid = ValidRequest;

    fn into_valid(self) -> Self::Valid {
        ValidRequest {
            table_name: self.table_name.unwrap(),
            url: self.url.unwrap(),
        }
    }
}

async fn index(State(state): State<SharedState>) -> Result<impl IntoResponse, HttpError> {
    let state = state.lock().map_err(from_guard)?;
    Ok(axum::response::Json(state.serialize()))
}

async fn register(
    State(state): State<SharedState>,
    Json(body): Json<Request>,
) -> Result<impl IntoResponse, HttpError> {
    let ValidRequest { table_name, url } = body.into_valid();
    let id = Ulid::new().to_string();
    let entry = Entry::new(table_name, url);
    let mut state = state.lock().map_err(from_guard)?;
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
