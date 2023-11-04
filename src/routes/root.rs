use crate::{Subscription, Subscriptions};
use axum::{
    extract::{Json, State},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use serde::Deserialize;
use std::sync::{Arc, Mutex};

#[derive(Debug, Deserialize)]
struct Request {
    arn: String,
    url: String,
}

async fn retrieve(State(state): State<Arc<Mutex<Subscriptions>>>) -> impl IntoResponse {
    let state = state.lock().unwrap();
    axum::response::Json(state.clone())
}

async fn register(
    State(state): State<Arc<Mutex<Subscriptions>>>,
    Json(body): Json<Request>,
) -> impl IntoResponse {
    let mut state = state.lock().unwrap();

    let subscription = Subscription::new(&body.url);
    state.insert(&body.arn, subscription);

    "OK"
}

pub fn router(state: Arc<Mutex<Subscriptions>>) -> Router {
    Router::new()
        .route("/", get(retrieve))
        .route("/", post(register))
        .with_state(state)
}
