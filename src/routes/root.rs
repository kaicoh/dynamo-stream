use axum::{
    extract::{Json, State},
    routing::{get, post},
    response::IntoResponse,
    Router,
};
use crate::{AppState, Subscription};
use serde::Deserialize;
use std::sync::{Arc, Mutex};

#[derive(Debug, Deserialize)]
struct Request {
    arn: String,
    url: String,
}

async fn retrieve(
    State(state): State<Arc<Mutex<AppState>>>,
) -> impl IntoResponse {
    let state = state.lock().unwrap();
    axum::response::Json(state.clone())
}

async fn register(
    State(state): State<Arc<Mutex<AppState>>>,
    Json(body): Json<Request>,
) -> impl IntoResponse {
    let mut state = state.lock().unwrap();

    let subscription = Subscription::new(&body.url);
    state.insert(&body.arn, subscription);

    "OK"
}

pub fn router(state: Arc<Mutex<AppState>>) -> Router {
    Router::new()
        .route("/", get(retrieve))
        .route("/", post(register))
        .with_state(state)
}
