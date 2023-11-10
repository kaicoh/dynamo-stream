mod state;
mod stream;
mod utils;
mod web;

use std::sync::{Arc, Mutex};

pub use state::AppState;
pub use stream::{start as start_notification, subscribe, DynamodbClient, Event};
pub use web::routes;

pub type SharedState = Arc<Mutex<AppState>>;

pub const ENV_DYNAMODB_ENDPOINT_URL: &str = "DYNAMODB_ENDPOINT_URL";
