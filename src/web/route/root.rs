use super::{from_guard, FromValidate, HttpError, Json, SharedState, Subscription};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{self, IntoResponse},
    routing::{delete, get, post},
    Router,
};
use serde::Deserialize;
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
    Ok(response::Json(state.serialize()))
}

async fn register(
    State(state): State<SharedState>,
    Json(body): Json<EntryBody>,
) -> Result<impl IntoResponse, HttpError> {
    let EntryBody { table_name, url } = body;

    let (client, new_sub) = {
        let mut state = state.lock().map_err(from_guard)?;
        (state.client(), !state.has_sub(&table_name))
    };

    let sub = if new_sub {
        let sub = Subscription::builder()
            .set_client(client)
            .set_table(&table_name)
            .build()
            .await?;
        Some(sub)
    } else {
        None
    };

    let mut state = state.lock().map_err(from_guard)?;

    if let Some(sub) = sub {
        state.add_sub(sub);
    }

    state
        .add_listener(table_name, url)
        .ok_or(HttpError::NotFound("Subscription".to_owned()))
        .map(response::Json)
}

async fn deregister_url(
    State(state): State<SharedState>,
    Path(table): Path<String>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, HttpError> {
    let mut state = state.lock().map_err(from_guard)?;
    state.remove_listener(table, id);
    Ok((StatusCode::NO_CONTENT, "No content"))
}

async fn unsubscribe_table(
    State(state): State<SharedState>,
    Path(table): Path<String>,
) -> Result<impl IntoResponse, HttpError> {
    let mut state = state.lock().map_err(from_guard)?;
    state.remove_sub(table);
    Ok((StatusCode::NO_CONTENT, "No content"))
}

pub fn router(state: SharedState) -> Router {
    Router::new()
        .route("/:table/:id", delete(deregister_url))
        .route("/:table", delete(unsubscribe_table))
        .route("/", get(index))
        .route("/", post(register))
        .with_state(state)
}
