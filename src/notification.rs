use crate::types::Records;
use tokio::sync::mpsc::Receiver;

use anyhow::Result;
use tracing::{error, info};

#[derive(Debug, Clone)]
pub enum Event {
    Http { url: String, records: Records },
    Error(String),
}

impl Event {
    pub fn http<T: Into<String>>(url: T, records: Records) -> Self {
        Event::Http {
            url: url.into(),
            records,
        }
    }

    pub fn error<T: Into<String>>(message: T) -> Self {
        Event::Error(message.into())
    }

    async fn notify(self) -> Result<()> {
        match self {
            Event::Http { url, records } => {
                info!("Http Notification Event to {url}");
                info!("{:#?}", records);
            }
            Event::Error(message) => {
                error!("Something went wrong. You have to notify to subscriber: {message}");
            }
        }

        Ok(())
    }
}

pub async fn start(mut receiver: Receiver<Event>) -> Result<()> {
    while let Some(event) = receiver.recv().await {
        tokio::spawn(async move {
            if let Err(err) = event.notify().await {
                error!("{:#?}", err);
            }
        });
    }

    Err(anyhow::anyhow!(
        "It seems all event senders have been dropped"
    ))
}
