use dynamo_stream::{routes::root, AppState};
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() {
    let shared_state = Arc::new(Mutex::new(AppState::new()));

    let app = root::router(shared_state);

    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
