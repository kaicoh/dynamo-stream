use crate::{
    error::{from_guard, HttpError},
    extractors::{FromValidate, Json},
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
struct ValidatableRequest {
    #[validate(required, length(max = 255))]
    table_name: Option<String>,
    #[validate(required, length(max = 255))]
    url: Option<String>,
}

#[derive(Debug)]
struct Request {
    table_name: String,
    url: String,
}

impl FromValidate for Request {
    type Validatable = ValidatableRequest;

    fn from(req: ValidatableRequest) -> Request {
        Request {
            table_name: req.table_name.expect("`table_name` should be Some"),
            url: req.url.expect("`url` should be Some"),
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
    let Request { table_name, url } = body;
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
