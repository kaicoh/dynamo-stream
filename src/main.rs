use dynamo_stream::{
    web::{route::root, AppState, SharedState},
    DynamodbClient, ENV_DYNAMODB_ENDPOINT_URL,
};
use std::net::SocketAddr;
use tower_http::{
    trace::{DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse, TraceLayer},
    LatencyUnit,
};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let client = DynamodbClient::builder()
        .await
        .endpoint_url(std::env::var(ENV_DYNAMODB_ENDPOINT_URL).ok())
        .build();

    let state: SharedState = AppState::new(client).into();

    let app = root::router(state).layer(
        TraceLayer::new_for_http()
            .make_span_with(
                DefaultMakeSpan::new()
                    .level(Level::INFO)
                    .include_headers(true),
            )
            .on_request(DefaultOnRequest::new().level(Level::INFO))
            .on_response(
                DefaultOnResponse::new()
                    .level(Level::INFO)
                    .latency_unit(LatencyUnit::Micros)
                    .include_headers(true),
            ),
    );

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    info!("listening on {addr}");

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
