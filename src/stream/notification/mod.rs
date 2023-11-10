use super::types::Records;

use anyhow::Result;
use serde::Serialize;
use tokio::sync::mpsc::Receiver;
use tracing::error;

#[derive(Debug)]
pub struct Event {
    pub table_name: String,
    pub url: String,
    pub payload: Payload,
}

impl Event {
    pub fn factory<T, U>(table_name: T, url: U) -> EventFactory
    where
        T: Into<String>,
        U: Into<String>,
    {
        EventFactory::new(table_name.into(), url.into())
    }

    async fn notify(self) -> Result<()> {
        let url = self.url;
        let table_name = self.table_name;

        match self.payload {
            Payload::Records(records) => send(&url, &records).await,
            Payload::Error(message) => {
                let body = serde_json::json!({
                    "table_name": table_name,
                    "message": message,
                });
                send(&url, &body).await
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum Payload {
    Records(Records),
    Error(String),
}

#[derive(Debug, Clone)]
pub struct EventFactory {
    table_name: String,
    url: String,
}

impl EventFactory {
    fn new(table_name: String, url: String) -> Self {
        Self { table_name, url }
    }

    pub fn records(&self, records: Records) -> Event {
        Event {
            table_name: self.table_name.to_string(),
            url: self.url.to_string(),
            payload: Payload::Records(records),
        }
    }

    pub fn error(&self, message: String) -> Event {
        Event {
            table_name: self.table_name.to_string(),
            url: self.url.to_string(),
            payload: Payload::Error(message),
        }
    }
}

async fn send<B: Serialize>(url: &str, body: &B) -> Result<()> {
    reqwest::Client::new()
        .post(url)
        .json(body)
        .send()
        .await
        .map(drop)
        .map_err(anyhow::Error::from)
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
