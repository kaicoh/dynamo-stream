use dynamo_stream::{routes::root, AppState, Config, DynamodbClient, Event};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tower_http::{
    trace::{DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse, TraceLayer},
    LatencyUnit,
};
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let config = Config::new();
    let client = DynamodbClient::builder()
        .await
        .endpoint_url(config.endpoint_url())
        .build();

    let (tx, rx) = mpsc::channel::<Event>(100);

    let state = Arc::new(Mutex::new(AppState::new(tx, config.entries())));
    let shared_state = Arc::clone(&state);

    tokio::spawn(async move {
        if let Err(err) = dynamo_stream::start_notification(rx).await {
            error!("Unexpected error from notification process.");
            error!("{:#?}", err);
        }
    });

    tokio::spawn(async move {
        if let Err(err) = dynamo_stream::subscribe(state, Arc::new(client)).await {
            error!("Unexpected error from polling process.");
            error!("{:#?}", err);
        }
    });

    let app = root::router(shared_state).layer(
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

    let addr = SocketAddr::from(([0, 0, 0, 0], config.port()));
    info!("listening on {addr}");

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
