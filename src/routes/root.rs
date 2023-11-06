use crate::{
    error::{from_guard, HttpError},
    types::Entry,
    SharedState,
};
use axum::{
    extract::{Json, Path, State},
    response::IntoResponse,
    routing::{delete, get, post},
    Router,
};
use serde::Deserialize;
use ulid::Ulid;

#[derive(Debug, Deserialize)]
struct Request {
    table_name: String,
    url: String,
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

async fn remove(
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
        .route("/:id", delete(remove))
        .route("/", get(index))
        .route("/", post(register))
        .with_state(state)
}
