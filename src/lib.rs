mod config;
mod state;
mod stream;
mod utils;
mod web;

use config::EntryConfig;
use std::sync::{Arc, Mutex};

pub use config::Config;
pub use state::AppState;
pub use stream::{start as start_notification, subscribe, DynamodbClient, Event};
pub use web::routes;

pub type SharedState = Arc<Mutex<AppState>>;

pub const ENV_DYNAMODB_ENDPOINT_URL: &str = "DYNAMODB_ENDPOINT_URL";
pub const ENV_PORT: &str = "PORT";
pub const ENV_CONFIG_PATH: &str = "CONFIG_PATH";
