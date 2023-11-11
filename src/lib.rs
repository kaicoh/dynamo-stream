mod config;
mod notification;
mod state;
mod stream;
mod utils;
mod web;

use anyhow::Result;
use config::EntryConfig;
use state::EntryStatus;
use std::sync::{Arc, Mutex};
use stream::client::Client;
use tokio::time::{sleep, Duration};

pub use config::Config;
pub use notification::{start as start_notification, Event};
pub use state::AppState;
pub use stream::DynamodbClient;
pub use web::routes;

pub type SharedState = Arc<Mutex<AppState>>;

pub const ENV_DYNAMODB_ENDPOINT_URL: &str = "DYNAMODB_ENDPOINT_URL";
pub const ENV_PORT: &str = "PORT";
pub const ENV_CONFIG_PATH: &str = "CONFIG_PATH";

pub async fn subscribe(state: SharedState, client: Arc<dyn Client>) -> Result<()> {
    loop {
        sleep(Duration::from_secs(3)).await;

        let mut ids_to_remove: Vec<String> = vec![];
        let mut state = state.lock().map_err(utils::from_guard)?;

        for (id, entry) in state.iter_mut() {
            match entry.status() {
                EntryStatus::Created => {
                    let client = Arc::clone(&client);
                    entry.start_polling(client);
                }
                EntryStatus::Running => {
                    entry.check_polling();
                }
                EntryStatus::Removed => {
                    entry.finish_polling();
                    ids_to_remove.push(id.into());
                }
                _ => {}
            }
        }

        for id in ids_to_remove {
            state.remove(id);
        }
    }
}
