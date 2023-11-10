pub mod client;
pub mod notification;
mod types;

use crate::{state::EntryStatus, utils::from_guard, SharedState};

pub use client::DynamodbClient;
pub use notification::{start, Event, EventFactory};
pub use types::Records;

use anyhow::Result;
use client::Client;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

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
