use crate::types::Records;
use tokio::sync::mpsc::Receiver;

use anyhow::Result;
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub enum Event {
    Http { url: String, records: Records },
}

impl Event {
    pub fn http<T: Into<String>>(url: T, records: Records) -> Self {
        Event::Http {
            url: url.into(),
            records,
        }
    }

    async fn notify(self) -> Result<()> {
        match self {
            Event::Http { url, records } => {
                info!("Http Notification Event to {url}");
                info!("{:#?}", records);
            }
        }

        Ok(())
    }
}

pub async fn start(mut receiver: Receiver<Event>) -> Result<()> {
    while let Some(event) = receiver.recv().await {
        if let Err(err) = event.notify().await {
            warn!("{:#?}", err);
        }
    }

    Err(anyhow::anyhow!(
        "It seems all event senders have been dropped"
    ))
}
