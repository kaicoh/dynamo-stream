use dynamo_stream::{routes::root, Subscriptions};
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() {
    let state = Arc::new(Mutex::new(Subscriptions::new()));
    let shared_state = Arc::clone(&state);

    tokio::spawn(async move {
        dynamo_stream::subscribe(state).await;
    });

    let app = root::router(shared_state);

    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
