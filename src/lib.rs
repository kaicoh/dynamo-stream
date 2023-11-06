mod client;
mod error;
pub mod routes;
mod state;
mod types;

pub use client::DynamodbClient;
pub use state::AppState;

pub type SharedState = Arc<Mutex<AppState>>;

pub const ENV_DYNAMODB_ENDPOINT_URL: &str = "DYNAMODB_ENDPOINT_URL";

use anyhow::Result;
use client::Client;
use error::from_guard;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};
use types::EntryStatus;

pub async fn subscribe(state: SharedState, client: Arc<dyn Client>) -> Result<()> {
    loop {
        sleep(Duration::from_secs(3)).await;

        let mut ids_to_remove: Vec<String> = vec![];
        let mut state = state.lock().map_err(from_guard)?;

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
